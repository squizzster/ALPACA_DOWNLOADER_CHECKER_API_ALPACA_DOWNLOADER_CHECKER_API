"""Separate competing unit occupations without transferring common-stock or warrant prices."""

import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

from libs.alpaca_lineage_build import occupation_continuation, unit_occupations
from libs.alpaca_lineage_build.contract import RecoveryRequired


def bars(pairs):
    return {
        day: {
            "timestamp": day + "T04:00:00Z",
            **dict.fromkeys(("open", "high", "low", "close", "vwap"), str(price)),
            "volume": str(price * 100),
            "trade_count": "5",
        }
        for day, price in pairs
    }


class NativeUnitTests(unittest.TestCase):
    def setUp(self):
        self.first, self.last, self.restart, self.end = "2020-06-01", "2020-08-27", "2020-12-03", "2021-04-05"
        self.next_day = "2020-08-28"
        self.unit = {
            "id": "unit-owner",
            "symbol": "ACME.U",
            "name": "ACME Units, one share of common stock and one warrant",
            "class": "us_equity",
        }
        self.owner = {"member_alpaca_asset_ids": ["unit-owner"]}
        self.lookups = [
            {"lookup": "ACME.U", "found": True, "asset": self.unit},
            {"lookup": "123456789", "found": True, "asset": self.unit},
        ]
        self.split = {
            "id": "sep",
            "action_type": "unit_split",
            "old_symbol": "ACME.U",
            "old_cusip": "123456789",
            "old_rate": 1,
            "new_symbol": "ACME",
            "new_cusip": "stock-issue",
            "new_rate": 1,
            "alternate_symbol": "ACME.WS",
            "alternate_cusip": "warrant-issue",
            "alternate_rate": 0.5,
            "effective_date": self.next_day,
        }
        self.mergers = [
            {
                "id": suffix,
                "action_type": "stock_merger",
                "effective_date": self.next_day,
                "acquiree_symbol": self.split[suffix + "_symbol"],
                "acquiree_cusip": self.split[suffix + "_cusip"],
                "acquirer_cusip": "another-" + suffix,
            }
            for suffix in ("new", "alternate")
        ]
        self.events = [self.split, *self.mergers]
        self.days = ["2020-06-24", self.last, self.next_day, "2021-01-20", self.end]
        self.raw = bars(zip(self.days, [8, 9, 10, 11, 12]))
        self.new = {d: r for d, r in self.raw.items() if d > self.restart}
        self.old = {d: r for d, r in self.raw.items() if d < self.restart}
        self.old.update(bars([("2020-09-01", 81), ("2020-09-02", 82), ("2021-01-20", 83), (self.end, 84)]))
        self.routes = {("ACME", "-"): self.raw, ("ACME", self.last): self.old, ("ACME", self.restart): self.new}
        self.unit_rows = bars([(self.first, 10), (self.last, 11), (self.restart, 12), (self.end, 13)])
        self.history = Mock(side_effect=AssertionError("unselected corporate history"))
        self.route = Mock(side_effect=lambda s, d: self.routes[s, d])

    def select(self):
        return unit_occupations.select(
            [{"asset_lookups": self.lookups, "corporate_actions": self.events}],
            "ACME.U",
            self.owner,
            self.unit_rows,
            self.last,
            self.restart,
            self.route,
            self.history,
        )

    def test_identified_old_unit_retains_pre_separation_tape(self):
        result = self.select()
        self.assertEqual((result["retained_side"], result["old_asset_id"]), ("prior", "unit-owner"))
        self.assertEqual(result["component_action_ids"], ["alternate", "new"])
        self.history.assert_not_called()

    def test_no_merger_of_warrant_or_conflicting_cusip_retires_neither_unit(self):
        self.mergers[-1]["acquiree_cusip"] = "unrelated-warrant"
        self.assertIsNone(self.select())
        self.route.assert_not_called()
        self.mergers[-1]["acquiree_cusip"] = "warrant-issue"
        self.lookups[-1]["asset"] = {**self.unit, "id": "unrelated-unit"}
        self.assertIsNone(self.select())

    def test_fund_with_shared_stem_and_prices_is_not_a_unit(self):
        self.unit["name"] = "ACME Equity ETF"
        self.assertIsNone(self.select())
        self.route.assert_not_called()

    def test_single_common_with_resumed_trades_and_identical_older_route_keeps_continuity(self):
        self.old.update(self.new)
        self.assertIsNone(self.select())

    def test_common_before_unit_or_trading_through_unit_hiatus_is_insufficient(self):
        self.raw.update(bars([("2020-05-08", 5)]))
        self.assertIsNone(self.select())
        self.raw.pop("2020-05-08")
        self.raw.update(bars([("2020-09-01", 7)]))
        self.assertIsNone(self.select())

    def test_distinct_common_and_dated_successor_witness_keep_later_unit_id(self):
        self.events = [
            {"id": "split", "symbol": "ACME.U", "action_type": "reverse_split", "ex_date": self.next_day}
        ]
        self.raw.pop(self.next_day)
        self.old.pop(self.next_day)
        terminal = {
            "id": "rename",
            "action_type": "name_change",
            "old_symbol": "ACME",
            "new_symbol": "NEWC",
            "process_date": "2021-04-07",
            "old_cusip": "class-one",
            "new_cusip": "class-one",
        }
        self.history = Mock(return_value=[terminal])
        self.routes["NEWC", "2021-04-07"] = self.new
        self.assertEqual(self.select()["retained_side"], "later")
        self.routes["NEWC", "2021-04-07"] = {**self.new, **bars([("2021-01-20", 88)])}
        self.assertIsNone(self.select())

    def test_canceled_term_does_not_break_unit(self):
        self.mergers[-1]["status"] = "cancelled"
        self.assertIsNone(self.select())
        self.route.assert_not_called()


class TerminalContinuationTests(unittest.TestCase):
    def setUp(self):
        self.evidence = {"corporate_actions": [], "asset_lookups": []}
        self.item = {
            "retained_side": "prior",
            "symbol": "XYZ.U",
            "terminal_action": {
                "id": "a",
                "old_symbol": "XYZ.U",
                "old_cusip": "old",
                "new_symbol": "XYZ",
                "new_cusip": "share",
                "alternate_symbol": "XYZ.W",
                "alternate_cusip": "warrant",
                "effective_date": "2020-02-04",
            },
            "last_prior_trade": "2020-02-03",
            "current_asof": "2021-07-01",
            "component_action_ids": ["m1", "m2"],
        }
        self.item["terminal_evidence"] = {"XYZ.U": occupation_continuation.terms(self.evidence, self.item)}
        self.previous = {
            "evidence": self.evidence,
            "source_sha256": "pricing",
            "dated_occupations": {"pid": self.item},
        }
        self.view = SimpleNamespace(
            symbol="XYZ.U", source_sha256="appended", source={"history_changes": []}, correction_from=None
        )
        self.index = {"raw": SimpleNamespace(sessions=("2025-10-02",))}
        seg = {
            "adjustment": "raw",
            "row_start": 1,
            "row_end": 1,
            "row_count": 1,
            "first_timestamp": "2025-10-02T04:00:00Z",
            "last_timestamp": "2025-10-02T04:00:00Z",
            "security_identity": {"security_id": "pid"},
            "historical_symbol": "XYZ.U",
        }
        self.report = {
            "correlated": {"row_count": 1, "segments": [seg]},
            "uncorrelated": {"row_count": 0, "segments": []},
            "observation_routes": {"pid": ["2025-10-02T04:00:00Z", {}]},
        }

    def apply(self, evidence=None, continued=True):
        return occupation_continuation.apply(
            self.previous,
            self.evidence if evidence is None else evidence,
            self.view,
            self.report,
            self.index,
            continued=continued,
        )

    def test_normal_appended_trade_cannot_reopen_retired_issuer_or_route(self):
        decision = self.apply()
        self.assertEqual(decision["correlated"], {"segments": [], "row_count": 0})
        self.assertEqual(
            decision["uncorrelated"]["segments"][0]["reason_codes"], ["later_unit_occupation_owner_unresolved"]
        )
        self.assertFalse(decision["observation_routes"])

    def test_later_unit_owner_and_unrelated_class_keep_normal_assignment(self):
        self.item["retained_side"] = "later"
        self.assertIs(self.apply(), self.report)
        self.item["retained_side"] = "prior"
        self.report["correlated"]["segments"][0]["security_identity"]["security_id"] = "peer"
        self.assertIs(self.apply(), self.report)

    def test_changed_selected_term_or_cusip_and_intersecting_price_reset_require_owning_recovery(self):
        changed = deepcopy(self.evidence)
        changed["corporate_actions"].append(
            {**self.item["terminal_action"], "action_type": "unit_split", "new_rate": 2}
        )
        with self.assertRaises(RecoveryRequired):
            self.apply(changed)
        with self.assertRaises(RecoveryRequired):
            self.apply(continued=False)
        self.view.correction_from = self.item["last_prior_trade"]
        with self.assertRaises(RecoveryRequired):
            self.apply()

    def test_materialized_append_accepts_pricing_ancestry_and_splits_only_selected_positions(self):
        self.view.source = {"history_changes": [{"source_sha256": "pricing", "changed_from": "2025-10-02"}]}
        self.view.views = {"raw": [("2020-02-03T05:00:00Z",), ("2025-10-02T04:00:00Z",)]}
        self.index["raw"].sessions = ("2020-02-03", "2025-10-02")
        seg = self.report["correlated"]["segments"][0]
        seg.update(row_count=2, row_end=2, first_timestamp=self.view.views["raw"][0][0])
        self.report["correlated"]["row_count"] = 2
        result = self.apply(continued=False)
        self.assertEqual(result["correlated"]["row_count"], 1)
        self.assertEqual(result["uncorrelated"]["row_count"], 1)
        self.assertEqual(result["correlated"]["segments"][0]["last_session_date"], "2020-02-03")
        self.assertEqual(result["uncorrelated"]["segments"][0]["row_start"], 2)
        self.assertEqual(result["uncorrelated"]["segments"][0]["first_timestamp"], "2025-10-02T04:00:00Z")
        self.view.source["history_changes"][0]["changed_from"] = "2020-02-01"
        with self.assertRaises(RecoveryRequired):
            self.apply(continued=False)

    def test_unrelated_native_event_does_not_reopen_review(self):
        changed = {
            **self.evidence,
            "corporate_actions": [
                {"id": "unrelated", "action_type": "cash_dividend", "symbol": "Z", "process_date": "2025-10-02"}
            ],
        }
        self.assertEqual(self.apply(changed)["uncorrelated"]["row_count"], 1)

"""A completed closed-unit occupation bounds use of a later split adjustment."""

import unittest
from copy import deepcopy
from decimal import Decimal
from unittest.mock import Mock, patch
from uuid import uuid4

from libs.alpaca_lineage_build import dated_price_publication as dated
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.retired_unit import applies, inputs, valid_scope
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.rename_split_basis import scaled
from libs.alpaca_pricing_download.retired_unit import assess
from libs.alpaca_pricing_download.shifted_split import _PAIRS

FIRST, LAST, SEPARATE, REVIEW, NEXT = "2025-05-15", "2025-05-16", "2025-05-19", "2025-06-02", "2025-06-16"


class RetiredUnitTests(unittest.TestCase):
    def setUp(self):
        self.pid = "a" * 64
        self.symbol = "FUT.U"
        self.occ = dict(
            token=uuid4().hex,
            retained_side="prior",
            symbol=self.symbol,
            prior_first_trade=FIRST,
            last_prior_trade=LAST,
            first=NEXT,
            terminal_action=dict(id="terminal", effective_date=SEPARATE),
        )
        self.action = dict(
            id="later",
            action_type="reverse_split",
            ex_date=REVIEW,
            symbol=self.symbol,
            old_rate=1,
            new_rate=0.333333,
        )
        self.receipt = {
            "dated_occupations": {self.pid: self.occ},
            "evidence": {"corporate_actions": [self.action]},
        }
        self.ranges = [dict(ticker=self.symbol, first_session=FIRST, last_session=LAST)]
        self.security = Mock(public_id=self.pid)
        self.snapshot = Mock()
        self.snapshot._selected_receipts = {self.symbol: self.receipt}
        self.raw = [
            dict(
                session=d,
                tickers=[self.symbol],
                timestamp=d + "T04:00:00Z",
                open="10.35",
                high="10.95",
                low="9.95",
                close="10.60",
                vwap="10.698333",
                volume="1227",
                trade_count="18",
            )
            for d in (FIRST, LAST)
        ]
        self.views = {
            m: {
                "bars": [
                    scaled(r, Decimal(1) / Decimal(".333333")) if m in _PAIRS else dict(r) for r in self.raw
                ]
            }
            for m in ADJUSTMENTS
        }
        self.snapshot.bounded_acquisition_views.return_value = self.views

    def test_accepted_terminal_owner_and_exact_native_terms_restore_old_units(self):
        selected = inputs(self.snapshot, self.security, REVIEW, self.ranges)
        self.snapshot.bounded_acquisition_views.assert_called_once_with(self.pid, FIRST, LAST)
        self.assertEqual(self.views["split"]["bars"][0]["volume"], "408")
        g, outcome = assess(selected)
        self.assertEqual(outcome["cut"], SEPARATE)
        self.assertEqual(outcome["witness_end"], REVIEW)
        for mode, bars in g.items():
            for row in self.raw:
                self.assertEqual(bars[row["session"]], row)
        scope = outcome["retired_unit_scope"]
        value = {**outcome, "public_id": self.pid, "members": [self.symbol]}
        self.assertTrue(valid_scope(value))
        self.assertTrue(applies(self.receipt, dict(self.receipt), value))
        new = deepcopy(self.receipt)
        new["dated_occupations"][self.pid]["token"] = uuid4().hex
        self.assertFalse(applies(self.receipt, new, value))
        new = deepcopy(self.receipt)
        new["evidence"]["corporate_actions"][0]["new_rate"] = 0.3
        self.assertFalse(applies(self.receipt, new, value))
        self.assertEqual(scope["occupation_token"], self.occ["token"])

    def _rebuild(self, member):
        self.occ["current_asof"] = NEXT
        _, interval = assess(inputs(self.snapshot, self.security, REVIEW, self.ranges))
        interval.update(public_id=self.pid, token=uuid4().hex, members=[self.symbol, "PEER"], unavailable=[])
        source = {
            "latest_request": {"asof": NEXT, "end_exclusive": NEXT, "feed": "sip"},
            "history_changes": [{"source_sha256": "prior", "changed_from": NEXT}],
        }
        previous = {
            **self.receipt,
            "symbol": member,
            "source_sha256": "prior",
            "boundaries": {"closed": "a"},
            "pricing_request": {**source["latest_request"], "history_start": None},
            dated.FIELD: {self.pid: interval},
        }
        rebuilt = {
            **self.receipt,
            "symbol": member,
            "source_sha256": "appended",
            "boundaries": {"closed": "b"},
            "input_sources": {self.symbol: "appended", "PEER": "appended"},
        }
        return previous, rebuilt, source

    def test_full_rebuild_keeps_exact_selected_fragment_for_owner_and_peer(self):
        for name in (self.symbol, "PEER"):
            previous, rebuilt, source = self._rebuild(name)
            if name == self.symbol:
                # The assignment producer already accepted the owner's ancestry.
                source["history_changes"] = Mock(side_effect=AssertionError("replayed owner ancestry"))
            with (
                self.subTest(member=name),
                patch(
                    "libs.alpaca_lineage_build.retired_unit.inputs",
                    side_effect=AssertionError("reassessed prices"),
                ),
                patch(
                    "libs.alpaca_lineage_build.retired_unit._other_actions",
                    side_effect=AssertionError("repeated accepted evidence"),
                ),
                patch(
                    "libs.alpaca_pricing_download.dated_publication.assemble",
                    side_effect=AssertionError("opened prices"),
                ),
            ):
                dated.retain(previous, rebuilt, full_materialization=True, source=source)
            self.assertIs(rebuilt[dated.FIELD][self.pid], previous[dated.FIELD][self.pid])

    def test_full_rebuild_refuses_stale_or_missing_retired_price_binding(self):
        for changed in (
            "last_witness",
            "unknown_ancestry",
            "missing_source",
            "mapping",
            "action",
            "occupation",
            "member",
            "unavailable",
        ):
            previous, rebuilt, source = self._rebuild("PEER")
            if changed == "last_witness":
                source["history_changes"][0]["changed_from"] = REVIEW
            elif changed == "unknown_ancestry":
                source["history_changes"][0]["source_sha256"] = "other"
            elif changed == "missing_source":
                source = None
            elif changed == "mapping":
                source["latest_request"]["asof"] = FIRST
            elif changed == "action":
                rebuilt["evidence"] = {"corporate_actions": [{**self.action, "new_rate": 2}]}
            elif changed == "occupation":
                rebuilt["dated_occupations"] = {self.pid: {**self.occ, "token": uuid4().hex}}
            elif changed == "member":
                rebuilt["input_sources"].pop(self.symbol)
            else:
                rebuilt["unavailable_pricing"] = {"NEW": {}}
            with self.subTest(changed=changed), self.assertRaisesRegex(RecoveryRequired, "pricing recovery"):
                dated.retain(previous, rebuilt, full_materialization=True, source=source)
            self.assertNotIn(dated.FIELD, rebuilt)

    def test_price_coverage_ends_at_the_last_owned_session_even_when_separation_was_later(self):
        for effective in (LAST, "2025-05-23"):
            with self.subTest(effective=effective):
                self.occ["terminal_action"]["effective_date"] = effective
                selected = inputs(self.snapshot, self.security, REVIEW, self.ranges)
                self.assertEqual(assess(selected)[1]["cut"], SEPARATE)

    def test_terminal_decision_and_component_mergers_are_not_reproved(self):
        from libs.alpaca_security_lineage.inference_evidence import reject_assertions

        evidence = {
            "provider_corporate_actions": [
                self.action,
                {"id": "terminal", "old_symbol": self.symbol, "effective_date": SEPARATE},
                {"id": "component", "acquiree_symbol": "FUT", "effective_date": SEPARATE},
            ]
        }
        self.receipt["evidence"] = evidence
        observed = []

        def restricted(value, **kwargs):
            self.assertEqual(value["id"], "later")
            observed.append(value["id"])
            return reject_assertions(value, **kwargs)

        with patch("libs.alpaca_security_lineage.inference_evidence.reject_assertions", side_effect=restricted):
            selected = inputs(self.snapshot, self.security, REVIEW, self.ranges)
        self.assertEqual(observed, ["later"])
        self.assertEqual(selected["plan"]["terminal_action_id"], "terminal")
        self.receipt["evidence"] = {"corporate_actions": [self.action], "event_supplements": [self.action]}
        with self.assertRaisesRegex(ValueError, "assertion|research"):
            inputs(self.snapshot, self.security, REVIEW, self.ranges)

    def test_active_unit_unqualified_terminal_or_separate_distribution_never_read_prices(self):
        for failure in ("active", "no_termination", "unclipped", "wrong_factor", "unresolved", "cash"):
            with self.subTest(failure=failure):
                self.setUp()
                if failure == "active":
                    self.occ["retained_side"] = "later"
                elif failure == "no_termination":
                    del self.occ["terminal_action"]
                elif failure == "unclipped":
                    self.ranges[0]["last_session"] = NEXT
                elif failure == "unresolved":
                    self.snapshot._selected_receipts = {self.symbol: {"evidence": {}}}
                elif failure == "wrong_factor":
                    self.action["new_rate"] = 1
                else:
                    self.receipt["evidence"]["corporate_actions"].append(
                        dict(id="genuine", symbol=self.symbol, action_type="cash_dividend", ex_date=LAST)
                    )
                with self.assertRaises(UnsupportedInterval):
                    inputs(self.snapshot, self.security, REVIEW, self.ranges)
                self.snapshot.bounded_acquisition_views.assert_not_called()

    def test_competing_monetary_or_share_basis_declines(self):
        selected = inputs(self.snapshot, self.security, REVIEW, self.ranges)
        for mode, key, value in (
            ("all", "vwap", "32.09"),
            ("split", "volume", "409"),
            ("dividend", "close", "10.11"),
            ("spin-off", "trade_count", "80"),
            ("raw", "tickers", ["OTHER"]),
        ):
            row = deepcopy(selected)
            row["views"][mode]["bars"][0][key] = value
            with self.subTest(mode=mode), self.assertRaises(UnsupportedInterval):
                assess(row)

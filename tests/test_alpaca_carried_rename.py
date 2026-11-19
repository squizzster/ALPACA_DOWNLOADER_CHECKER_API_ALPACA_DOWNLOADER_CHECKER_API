"""A native provider rename can precede the published process date on the literal tape."""

import unittest
from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

from libs.alpaca_lineage_build.carried_rename import inputs_header, valid_changes
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_pricing_download.carried_rename import preceding_scope
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval, acquire
from libs.alpaca_serving_catalogue.carried_labels import project_ownership, project_ranges

DAYS = [(date(2031, 2, 3) + timedelta(days=i)).isoformat() for i in range(10)]
CUT = DAYS[5]


def bar(day, ticker, price, traded):
    return dict(
        timestamp=day + "T05:00:00Z",
        session=day,
        tickers=[ticker],
        open=price,
        high=price,
        low=price,
        close=price,
        volume="80" if traded else "0",
        trade_count="5" if traded else "0",
        vwap=price,
    )


class CarriedRenameTests(unittest.TestCase):
    def setUp(self):
        self.views, self.mapped = {}, {}
        for mode in ADJUSTMENTS:
            scale = "30" if "split" in mode or mode == "all" else "10"
            self.views[mode] = {
                "bars": [
                    bar(
                        day,
                        "FUT" if day >= CUT else "OLD",
                        "10" if DAYS[3] <= day < CUT else scale,
                        day < DAYS[3] or day >= CUT,
                    )
                    for day in DAYS
                ]
            }
            self.mapped[mode] = {day: bar(day, "FUT", scale, True) for day in DAYS}
        self.old = {b["session"]: b for b in self.views["raw"]["bars"] if b["session"] < CUT}
        self.new = {d: self.mapped["raw"][d] for d in DAYS[3:]}
        self.ranges = [
            {"ticker": "OLD", "first_session": DAYS[0], "last_session": DAYS[4]},
            {"ticker": "FUT", "first_session": CUT, "last_session": DAYS[-1]},
        ]
        self.header = {"ranges": self.ranges, "actions": [["OLD", "FUT", CUT, None]], "competitors": []}
        self.calls = []

    def observe(self, ticker, asof, mode, start, end):
        key = ticker, asof, mode, start, end
        self.assertNotIn(key, self.calls, "repeated an accepted provider request")
        self.calls.append(key)
        if asof == "-":
            self.assertEqual(mode, "raw")
            rows = self.old if ticker == "OLD" else self.new
        else:
            self.assertEqual((ticker, asof), ("FUT", CUT))
            rows = self.mapped[mode]
        return {d: r for d, r in rows.items() if start <= d <= end}

    def run_trial(self, **kwargs):
        return acquire(self.views, self.header, CUT, self.observe, carried=True, **kwargs)

    def test_interrupted_future_security_lends_only_unfinished_views(self):
        saved = {}

        def checkpoint(key, value):
            saved[key] = value
            if key == "confirmation:0":
                raise OSError("interrupted before next view")

        with self.assertRaisesRegex(OSError, "interrupted"):
            self.run_trial(checkpoint=checkpoint)
        for mode in ("raw", "all", "split"):
            self.views[mode] = Mock(side_effect=AssertionError("reopened accepted input"))
        bundles, proof = self.run_trial(completed=saved)
        self.assertEqual(proof["first"], DAYS[3])
        self.assertEqual(proof["sessions"], 2)
        self.assertEqual(proof["repair_kind"], "carried_rename")
        self.assertEqual(len(proof["raw_revisions"]), 2)
        self.assertEqual([r["tickers"] for b in bundles.values() for r in b.values()], [["FUT"]] * 16)
        self.assertEqual(len(self.calls), 10)
        self.assertEqual({req[3:] for req in self.calls if req[1] == CUT}, {(DAYS[0], DAYS[7])})
        self.assertEqual({req[3:] for req in self.calls if req[1] == "-"}, {(DAYS[2], DAYS[4]), (DAYS[2], CUT)})
        self.assertEqual(acquire(None, None, CUT, self.observe, carried=True, completed=saved)[1], proof)

    def test_actual_return_on_first_published_successor_does_not_mask_earlier_carry(self):
        for mode in ADJUSTMENTS:
            for day in DAYS[5:]:
                for bar_ in (self.mapped[mode][day], self.views[mode]["bars"][DAYS.index(day)]):
                    for field in ("open", "high", "low", "close", "vwap"):
                        bar_[field] = str(int(bar_[field]) * 2)
        bars, proof = self.run_trial()
        self.assertEqual(proof["first"], DAYS[3])
        self.assertEqual(bars["split"][DAYS[4]]["close"], "30")
        self.assertNotIn(CUT, bars["split"])
        self.assertEqual(len(self.calls), 10)

    def test_ticker_claims_and_ambiguous_literal_observations_decline(self):
        for failure in (
            "competing_owner",
            "missing_action",
            "competing_action",
            "both_traded",
            "missing_literal",
            "partial_carry",
            "left_anchor",
            "other_view_label",
        ):
            with self.subTest(failure=failure):
                self.setUp()
                if failure == "competing_owner":
                    self.header["competitors"] = [["FUT", DAYS[0], CUT]]
                elif failure == "missing_action":
                    self.header["actions"] = []
                elif failure == "competing_action":
                    self.header["actions"].append(["THIRD", "FUT", CUT, None])
                elif failure == "both_traded":
                    self.new[DAYS[2]] = self.mapped["raw"][DAYS[2]]
                elif failure == "missing_literal":
                    del self.new[DAYS[4]]
                elif failure == "partial_carry":
                    self.mapped["raw"][DAYS[4]] = self.old[DAYS[4]]
                elif failure == "left_anchor":
                    self.mapped["all"][DAYS[0]]["close"] = "31"
                else:
                    self.views["dividend"]["bars"][3]["tickers"].append("THIRD")
                with self.assertRaises(UnsupportedInterval):
                    self.run_trial()
                if failure in ("competing_owner", "missing_action", "competing_action"):
                    self.assertEqual(self.calls, [])

    def test_prior_scope_limits_borrowed_history(self):
        rows = self.views["raw"]["bars"]
        calls = []

        def read(lo, hi):
            self.assertFalse(any(a <= hi and lo <= b for a, b in calls))
            calls.append((lo, hi))
            return [r for r in rows if lo.isoformat() <= r["session"] <= hi.isoformat()]

        first = date(2028, 1, 1)
        got = preceding_scope(first, date.fromisoformat(DAYS[4]), read)
        self.assertEqual(got, rows[:5])
        self.assertEqual(len(calls), 1)
        calls.clear()
        self.assertEqual(preceding_scope(first, date.fromisoformat(DAYS[-1]), read), rows)
        self.assertEqual(len(calls), 2)

    def test_only_raw_provider_actions_are_admissible(self):
        evidence = {"event_supplements": ["authored"], "corporate_actions": [Mock()]}
        snap = SimpleNamespace(_selected_receipts={"OLD": {"evidence": evidence}}, _assignments={})
        with self.assertRaises(RecoveryRequired):
            inputs_header(snap, "pid", self.ranges)
        evidence["provider_corporate_actions"] = [
            dict(action_type="name_change", old_symbol="OLD", new_symbol="FUT", process_date=CUT)
        ]
        self.assertEqual(inputs_header(snap, "pid", self.ranges)["actions"], [("OLD", "FUT", CUT, None)])
        evidence["provider_corporate_actions"][0]["research_certificates"] = ["assertion"]
        with self.assertRaisesRegex(ValueError, "injection"):
            inputs_header(snap, "pid", self.ranges)

    def test_retention_scopes_actions_and_ownership_changes(self):
        from libs.alpaca_lineage_build import dated_price_publication as publication

        _, interval = self.run_trial()
        interval.update(members=["FUT", "OLD"], unavailable=[])
        action = dict(action_type="name_change", process_date=CUT, old_symbol="OLD", new_symbol="FUT")
        prior = dict(
            evidence={"corporate_actions": [action]},
            source_sha256="source",
            boundaries={"era": "A"},
            dated_price_intervals={"pid": interval},
            input_sources={"FUT": "new", "OLD": "old"},
        )
        for change in ("unrelated", "removed", "competing", "ownership", "late_append", "changed_member"):
            with self.subTest(change=change):
                receipt = deepcopy(prior)
                receipt.pop(publication.FIELD)
                if change == "unrelated":
                    receipt["evidence"]["corporate_actions"].append({**action, "process_date": DAYS[-1]})
                elif change == "removed":
                    receipt["evidence"]["corporate_actions"].clear()
                elif change == "competing":
                    receipt["evidence"]["corporate_actions"].append(
                        {**action, "old_symbol": "THIRD", "process_date": DAYS[4]}
                    )
                elif change == "changed_member":
                    receipt["input_sources"]["THIRD"] = "third"
                else:
                    receipt["boundaries"] = {"era": "B"}
                    publication.continue_closed(prior, receipt, DAYS[-1] if change == "late_append" else DAYS[3])
                publication.retain(prior, receipt, continued=True)
                self.assertEqual(publication.FIELD in receipt, change in {"unrelated", "late_append"})

    def test_ranges_transfer_once_preserving_partial_projection_and_cusips(self):
        _, interval = self.run_trial()
        changes = interval["label_changes"]
        interval["members"] = ["FUT", "OLD"]
        self.assertTrue(valid_changes(interval))
        corrupt = deepcopy(interval)
        corrupt["label_changes"][0]["next_session"] = DAYS[6]
        self.assertFalse(valid_changes(corrupt))
        expected = [
            dict(ticker="FUT", first_session=DAYS[3], last_session=DAYS[-1]),
            dict(ticker="OLD", first_session=DAYS[0], last_session=DAYS[2]),
        ]
        self.assertEqual(project_ranges(self.ranges, changes), expected)
        self.assertEqual(project_ranges(expected, changes), expected)
        self.assertEqual(
            project_ranges([{**self.ranges[0], "first_session": DAYS[4]}], changes),
            [dict(ticker="FUT", first_session=DAYS[4], last_session=DAYS[4])],
        )
        occupancy = project_ownership(
            [
                dict(ticker="OLD", start_inclusive=None, end_exclusive=CUT, cusip="one"),
                dict(ticker="FUT", start_inclusive=CUT, end_exclusive=None, cusip="two"),
            ],
            changes,
        )
        self.assertEqual(
            [(r["ticker"], r["start_inclusive"], r["end_exclusive"], r["cusip"]) for r in occupancy],
            [("OLD", None, DAYS[3], "one"), ("FUT", DAYS[3], CUT, "one"), ("FUT", CUT, None, "two")],
        )

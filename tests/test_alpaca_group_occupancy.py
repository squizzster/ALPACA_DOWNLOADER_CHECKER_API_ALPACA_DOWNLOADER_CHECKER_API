import copy
import unittest
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.correlation import (
    _without_reconciled_occupancy_actions,
)
from libs.alpaca_end_of_day_security_correlation.evidence import (
    build_ticker_ownership_intervals,
)
from libs.alpaca_lineage_build.occupancy import reconcile_duplicate_foreign_occupancy
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from tests.test_alpaca_end_of_day_security_correlation import (
    complete_market_session_rows,
    occupancy,
    same_symbol_split_lineage,
)
from tests.test_alpaca_name_change_evidence import bars, inputs_for


class DuplicateForeignOccupancyTests(unittest.TestCase):
    def setUp(self):
        self.current_id = "current-label-owner"
        self.foreign_id = "foreign-cusip-owner"
        self.current_cusip = "111111111"
        self.foreign_cusip = "222222222"
        self.current_asset = self.asset(self.current_id, "CURR")
        self.foreign_asset = self.asset(self.foreign_id, "OWN")
        self.foreign_action = self.dividend("foreign-copy", "CURR", self.foreign_cusip, "2024-12-02", 0.559028)
        self.own_action = self.dividend("own-action", "OWN", self.foreign_cusip, "2024-12-02", 0.5590278)
        self.evidence = {
            "CURR": {
                "corporate_actions": [
                    self.dividend("before", "CURR", self.current_cusip, "2024-09-05", 0.1),
                    self.foreign_action,
                    self.dividend("after", "CURR", self.current_cusip, "2025-06-17", 0.1),
                ],
                "asset_lookups": [
                    self.lookup("symbol", "CURR", self.current_asset),
                    self.lookup("cusip", self.current_cusip, self.current_asset),
                    self.lookup("cusip", self.foreign_cusip, self.foreign_asset),
                ],
                "symbol_occupancy_evidence": [
                    self.occupancy("CURR", self.current_cusip, self.current_id, "before", "after"),
                    self.occupancy("CURR", self.foreign_cusip, self.foreign_id, "foreign-copy"),
                ],
                "input_symbol_occupants": [
                    self.occupancy("CURR", self.current_cusip, self.current_id, "before", "after"),
                    self.occupancy("CURR", self.foreign_cusip, self.foreign_id, "foreign-copy"),
                ],
            },
            "OWN": {
                "corporate_actions": [self.own_action],
                "asset_lookups": [
                    self.lookup("symbol", "OWN", self.foreign_asset),
                    self.lookup("cusip", self.foreign_cusip, self.foreign_asset),
                ],
                "symbol_occupancy_evidence": [
                    self.occupancy("OWN", self.foreign_cusip, self.foreign_id, "own-action")
                ],
                "input_symbol_occupants": [
                    self.occupancy("OWN", self.foreign_cusip, self.foreign_id, "own-action")
                ],
            },
        }
        row = ("2024-12-02T05:00:00Z",)
        self.inputs = {
            "CURR": SimpleNamespace(views={name: (row,) for name in ADJUSTMENTS}),
            "OWN": SimpleNamespace(views={name: (row,) for name in ADJUSTMENTS}),
        }
        segments = [
            {
                "adjustment": name,
                "row_start": 1,
                "row_end": 1,
                "canonical_security": {"alpaca_asset_id": self.foreign_id},
            }
            for name in ADJUSTMENTS
        ]
        self.preliminary = {
            "CURR": {
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 1, "segments": []},
            },
            "OWN": {
                "correlated": {"row_count": 8, "segments": segments},
                "uncorrelated": {"row_count": 0, "segments": []},
            },
        }

    @staticmethod
    def asset(asset_id, symbol):
        return {
            "id": asset_id,
            "symbol": symbol,
            "class": "us_equity",
            "exchange": "NYSE",
            "status": "active",
            "tradable": True,
        }

    @staticmethod
    def lookup(kind, value, selected_asset):
        return {
            "lookup_kind": kind,
            "lookup": value,
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": selected_asset,
        }

    @staticmethod
    def dividend(action_id, symbol, cusip, when, rate):
        return {
            "id": action_id,
            "action_type": "cash_dividend",
            "symbol": symbol,
            "cusip": cusip,
            "ex_date": when,
            "record_date": when,
            "payable_date": when,
            "process_date": when,
            "rate": rate,
        }

    @staticmethod
    def occupancy(symbol, cusip, asset_id, *action_ids):
        return {
            "symbol": symbol,
            "cusip": cusip,
            "asset_id": asset_id,
            "evidence": [{"action_id": action_id} for action_id in action_ids],
        }

    def decisions(self):
        return reconcile_duplicate_foreign_occupancy(self.inputs, self.evidence, self.preliminary)

    def test_complete_own_source_supersedes_only_the_foreign_label_copy(self):
        decision = self.decisions()["CURR"][0]

        self.assertEqual("foreign-copy", decision["action_id"])
        self.assertEqual("own-action", decision["authoritative_action_id"])
        self.assertEqual(self.foreign_id, decision["authoritative_asset_id"])
        self.assertEqual(["before", "after"], decision["bracketing_current_action_ids"])

        supplied = copy.deepcopy(self.evidence["CURR"])
        supplied["symbol_occupancy_evidence"].append(self.occupancy("CURR", "333333333", "unrelated"))
        original = copy.deepcopy(supplied)
        interpreted = _without_reconciled_occupancy_actions(supplied, (decision,))
        self.assertEqual(original, supplied)
        self.assertEqual(
            ["before", "after"],
            [item["id"] for item in interpreted["corporate_actions"]],
        )
        self.assertEqual(
            {self.current_cusip, "333333333"},
            {item["cusip"] for item in interpreted["symbol_occupancy_evidence"]},
        )

    def test_missing_or_unassigned_authoritative_view_keeps_ambiguity(self):
        for change in ("missing_view", "unassigned", "wrong_uuid"):
            with self.subTest(change=change):
                inputs = copy.deepcopy(self.inputs)
                reports = copy.deepcopy(self.preliminary)
                if change == "missing_view":
                    inputs["OWN"].views.pop("all")
                elif change == "unassigned":
                    reports["OWN"]["correlated"]["segments"][-1]["row_start"] = 2
                else:
                    reports["OWN"]["correlated"]["segments"][-1]["canonical_security"] = {
                        "alpaca_asset_id": "different"
                    }
                self.assertEqual({}, reconcile_duplicate_foreign_occupancy(inputs, self.evidence, reports))

    def test_foreign_label_prices_do_not_supply_action_provenance(self):
        inputs = copy.deepcopy(self.inputs)
        inputs["CURR"].views["raw"] = (("2024-12-02T05:00:00Z", "different"),)

        decision = reconcile_duplicate_foreign_occupancy(inputs, self.evidence, self.preliminary)
        self.assertEqual("foreign-copy", decision["CURR"][0]["action_id"])

    def test_complete_foreign_label_source_preserves_its_accepted_decision(self):
        reports = copy.deepcopy(self.preliminary)
        reports["CURR"]["uncorrelated"]["row_count"] = 0
        self.assertEqual({}, reconcile_duplicate_foreign_occupancy(self.inputs, self.evidence, reports))

    def test_completed_reconciliation_remains_fixed_when_it_resolves_the_source(self):
        inputs = inputs_for(
            {
                "CURR": bars(["2025-06-17"]),
                "OWN": bars(["2024-12-02"]),
            },
            asofs={"CURR": "2025-06-18", "OWN": "2025-06-18"},
        )
        for view in inputs.values():
            for download in view.downloads.values():
                download.manifest["request_to"] = "2025-06-18"
            view.source["latest_request"]["end_exclusive"] = "2025-06-18"

        reports = correlate(Path("."), inputs, self.evidence)

        self.assertEqual(0, reports["CURR"]["uncorrelated"]["row_count"])
        self.assertEqual(len(ADJUSTMENTS), reports["CURR"]["correlated"]["row_count"])
        self.assertEqual(
            {self.current_id},
            {
                segment["canonical_security"]["alpaca_asset_id"]
                for segment in reports["CURR"]["correlated"]["segments"]
            },
        )

    def test_unbracketed_or_structurally_related_label_keeps_ambiguity(self):
        for change in ("unbracketed", "structural"):
            with self.subTest(change=change):
                evidence = copy.deepcopy(self.evidence)
                if change == "unbracketed":
                    evidence["CURR"]["corporate_actions"] = evidence["CURR"]["corporate_actions"][:-1]
                else:
                    evidence["CURR"]["corporate_actions"].append(
                        {
                            "id": "rename",
                            "action_type": "name_change",
                            "old_symbol": "CURR",
                            "new_symbol": "OWN",
                            "old_cusip": self.current_cusip,
                            "new_cusip": self.foreign_cusip,
                            "process_date": "2024-12-02",
                        }
                    )
                self.assertEqual(
                    {}, reconcile_duplicate_foreign_occupancy(self.inputs, evidence, self.preliminary)
                )

    def test_action_dates_and_exact_lookups_must_agree(self):
        for change in ("date", "lookup", "ambiguous"):
            with self.subTest(change=change):
                evidence = copy.deepcopy(self.evidence)
                if change == "date":
                    evidence["OWN"]["corporate_actions"][0]["payable_date"] = "2024-12-03"
                elif change == "lookup":
                    evidence["OWN"]["asset_lookups"][0]["asset"] = self.current_asset
                else:
                    evidence["OWN"]["asset_lookups"].append(copy.deepcopy(evidence["OWN"]["asset_lookups"][0]))
                self.assertEqual(
                    {}, reconcile_duplicate_foreign_occupancy(self.inputs, evidence, self.preliminary)
                )


class ProducerOccupancyFallbackTests(unittest.TestCase):
    def test_completed_identity_wins_before_action_anchored_suffix_fallback(self):
        evidence = same_symbol_split_lineage()
        evidence["corporate_actions"].extend(
            (
                self.dividend("previous-distribution", "111111111", "2026-09-26"),
                self.dividend("current-distribution", "222222222", "2026-10-01"),
            )
        )
        evidence["symbol_occupancy_evidence"].extend(
            (
                occupancy(
                    "SPLT",
                    "111111111",
                    "old-split-asset-id",
                    action_type="cash_dividend",
                    action_id="previous-distribution",
                    process_date="2026-09-26",
                    role="symbol",
                ),
                occupancy(
                    "SPLT",
                    "222222222",
                    "new-split-asset-id",
                    action_type="cash_dividend",
                    action_id="current-distribution",
                    process_date="2026-10-01",
                    role="symbol",
                ),
            )
        )
        intervals, issues = build_ticker_ownership_intervals(
            evidence,
            include_action_anchored_current_suffix=True,
        )
        self.assertEqual([], issues)
        self.assertEqual("2026-10-01", intervals[0].start_date_inclusive.isoformat())

        rows = complete_market_session_rows("2026-09-25", "2026-10-02")
        for index, row in enumerate(rows):
            value = str(10 + index)
            row.update(o=value, h=value, l=value, c=value, vw=value, v="100", n="10")
        inputs = inputs_for({"SPLT": rows}, asofs={"SPLT": "2026-10-01"})
        for download in inputs["SPLT"].downloads.values():
            download.manifest.update(
                request_from="2026-09-25",
                request_to="2026-10-02",
            )

        report = correlate(Path("."), inputs, {"SPLT": evidence})["SPLT"]

        self.assertEqual(len(rows) * len(ADJUSTMENTS), report["correlated"]["row_count"])
        self.assertEqual(0, report["uncorrelated"]["row_count"])
        raw_segments = [
            segment for segment in report["correlated"]["segments"] if segment["adjustment"] == "raw"
        ]
        self.assertEqual(1, len(raw_segments))
        self.assertEqual(
            "stable_continuous_ticker_identity_set",
            raw_segments[0]["correlation_rule"]["name"],
        )

    @staticmethod
    def dividend(action_id, cusip, when):
        return {
            "id": action_id,
            "action_type": "cash_dividend",
            "symbol": "SPLT",
            "cusip": cusip,
            "ex_date": when,
            "process_date": when,
            "rate": 0.1,
        }


if __name__ == "__main__":
    unittest.main()

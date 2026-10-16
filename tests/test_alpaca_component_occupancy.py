import copy
import unittest
from dataclasses import replace
from datetime import date

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.occupancy_anchor import dated_occupancy_anchors
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge, correlation_source


class ComponentOccupancyTests(unittest.TestCase):
    def setUp(self):
        self.lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        for asset_id, cusip, successor, when in (
            ("prior", "111111111", "PRIOR", "2024-01-03"),
            ("component", "222222222", "NEXT", "2024-01-08"),
        ):
            add_name_change_edge(
                self.lineage,
                action_id=asset_id,
                process_date=when,
                predecessor=(asset_id, "REUS", cusip, asset_id),
                successor=(asset_id, successor, cusip, asset_id),
            )
        self.action = {
            "id": "separation",
            "action_type": "unit_split",
            "effective_date": "2024-01-04",
            "old_symbol": "UNIT",
            "old_cusip": "333333333",
            "old_rate": 1,
            "new_symbol": "REUS",
            "new_cusip": "222222222",
            "new_rate": 1,
            "alternate_symbol": "WARRANT",
            "alternate_cusip": "444444444",
            "alternate_rate": 0.5,
        }
        self.lineage["corporate_actions"].append(self.action)
        self.lookup = {
            "lookup": "222222222",
            "lookup_kind": "cusip",
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": {"id": "component", "symbol": "REUS", "class": "us_equity", "exchange": "NASDAQ"},
        }
        self.lineage["asset_lookups"] = [self.lookup]
        self.intervals, _ = build_ticker_ownership_intervals(self.lineage)

    def correlate(
        self, symbol="REUS", adjustment="raw", dates=("2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")
    ):
        return correlate_download_rows(
            [{"t": value + "T12:00:00Z"} for value in dates],
            {**correlation_source(symbol, asof="2024-01-09"), "adjustment": adjustment},
            self.lineage,
            self.intervals,
        )

    def test_bounded_positive_anchor_preserves_unsupported_prefix_and_exact_cutovers(self):
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            with self.subTest(adjustment=adjustment):
                result = self.correlate(adjustment=adjustment)
                assigned = result["correlated"]
                self.assertEqual(1, len(assigned))
                self.assertEqual(
                    ("2024-01-04", "2024-01-05", 2),
                    (
                        assigned[0]["first_session_date"],
                        assigned[0]["last_session_date"],
                        assigned[0]["row_count"],
                    ),
                )
                self.assertEqual("component", assigned[0]["canonical_security"]["alpaca_asset_id"])
                self.assertEqual(["component"], assigned[0]["security_identity"]["member_alpaca_asset_ids"])
                self.assertEqual("requested_symbol_dated_uuid_interval", assigned[0]["correlation_rule"]["name"])
                self.assertIn("dated_action_occupancy", assigned[0]["warnings"])
                self.assertEqual(2, sum(item["row_count"] for item in result["uncorrelated"]))

    def test_alternate_component_is_supported_without_joining_units_or_other_component(self):
        for key in ("symbol", "cusip", "rate"):
            self.action["new_" + key], self.action["alternate_" + key] = (
                self.action["alternate_" + key],
                self.action["new_" + key],
            )
        self.assertEqual(2, self.correlate()["correlated"][0]["row_count"])
        self.assertEqual({}, dated_occupancy_anchors("UNIT", self.lineage, self.intervals))

    def test_missing_failed_ambiguous_mismatched_or_otc_lookup_does_not_anchor(self):
        for change in (
            {"found": False},
            {"http_status": 404},
            {"lookup_kind": "symbol"},
            {"asset": {**self.lookup["asset"], "id": "other"}},
            {"asset": {**self.lookup["asset"], "symbol": "OTHER"}},
            {"asset": {**self.lookup["asset"], "exchange": "OTC"}},
        ):
            with self.subTest(change=change):
                self.lineage["asset_lookups"] = [{**self.lookup, **change}]
                self.assertEqual({}, dated_occupancy_anchors("REUS", self.lineage, self.intervals))
        self.lineage["asset_lookups"] = [self.lookup, copy.deepcopy(self.lookup)]
        self.assertEqual({}, dated_occupancy_anchors("REUS", self.lineage, self.intervals))

    def test_malformed_old_role_or_out_of_interval_event_does_not_anchor(self):
        for change in (
            {"effective_date": "2024-01-08"},
            {"effective_date": "bad"},
            {"new_rate": 0},
            {"new_rate": "NaN"},
            {"alternate_cusip": None},
            {"old_symbol": "REUS"},
            {"new_cusip": "333333333"},
            {"action_type": "stock_merger"},
        ):
            with self.subTest(change=change):
                self.lineage["corporate_actions"][-1] = {**self.action, **change}
                self.assertEqual({}, dated_occupancy_anchors("REUS", self.lineage, self.intervals))

    def test_competing_or_recurrent_episode_vetoes_anchor_even_after_competitor_ends(self):
        target = next(
            item for item in self.intervals if item.historical_symbol == "REUS" and item.asset_id == "component"
        )
        for other in (
            replace(
                target,
                asset_id="other",
                start_date_inclusive=date(2024, 1, 4),
                end_date_exclusive=date(2024, 1, 5),
            ),
            replace(target, start_date_inclusive=date(2023, 1, 1), end_date_exclusive=date(2023, 2, 1)),
        ):
            self.assertEqual({}, dated_occupancy_anchors("REUS", self.lineage, [*self.intervals, other]))
        self.assertEqual(
            {}, dated_occupancy_anchors("REUS", self.lineage, [replace(target, end_date_exclusive=None)])
        )

    def test_earlier_asof_ownership_and_gap_guard_retain_precedence(self):
        result = self.correlate(symbol="NEXT")
        self.assertEqual([], result["uncorrelated"])
        self.assertTrue(
            all(
                s["correlation_rule"]["name"] == "asof_selected_uuid_lineage_interval"
                for s in result["correlated"]
            )
        )
        self.assertIsNone(
            next(
                i for i in self.intervals if i.asset_id == "component" and i.historical_symbol == "REUS"
            ).start_date_inclusive
        )
        result = self.correlate(dates=("2020-01-02", "2024-01-04"))
        self.assertEqual([], result["correlated"])

    def test_dividend_ex_date_anchors_bounded_occupancy_without_backdating_to_payment(self):
        dividend = {
            "id": "distribution",
            "action_type": "cash_dividend",
            "symbol": "REUS",
            "cusip": "222222222",
            "ex_date": "2024-01-04",
            "rate": 0.1,
            "payable_date": "2024-01-09",
            "process_date": "2024-01-09",
        }
        self.lineage["corporate_actions"][-1] = dividend
        self.assertEqual(2, self.correlate()["correlated"][0]["row_count"])
        for change in (
            {"cusip": "111111111"},
            {"symbol": "OTHER"},
            {"rate": 0},
            {"rate": "Infinity"},
            {"ex_date": None},
            {"ex_date": "2024-01-08"},
        ):
            with self.subTest(change=change):
                self.lineage["corporate_actions"][-1] = {**dividend, **change}
                self.assertEqual({}, dated_occupancy_anchors("REUS", self.lineage, self.intervals))

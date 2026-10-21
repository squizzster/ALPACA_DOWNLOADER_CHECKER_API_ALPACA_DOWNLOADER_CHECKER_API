import copy
import unittest

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    correlate_download_rows,
    download_session_dates,
)
from libs.alpaca_end_of_day_security_correlation.ticker_reuse_gap import adjustment_session_consensus
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from tests.test_alpaca_end_of_day_security_correlation import (
    complete_market_session_rows,
    correlation_source,
    occupancy,
    same_symbol_split_lineage,
)


class ClosedSplitHistoryTests(unittest.TestCase):
    def setUp(self):
        self.lineage = same_symbol_split_lineage()
        for item in self.lineage["identities"]:
            item["status"] = "inactive"
            self.lineage["symbol_occupancy_evidence"].append(
                occupancy(
                    "SPLT",
                    item["lookup_cusips"][0],
                    item["id"],
                    action_type="reverse_split",
                    action_id="same-symbol-reverse-split",
                    process_date="2026-09-29",
                    role="old" if item["id"].startswith("old") else "new",
                )
            )
        self.departure = {
            "id": "departure",
            "action_type": "stock_merger",
            "effective_date": "2026-10-02",
            "acquiree_symbol": "SPLT",
            "acquiree_cusip": "222222222",
            "acquirer_symbol": "ESCROW",
            "acquirer_cusip": "333333333",
        }
        self.lineage["corporate_actions"].append(self.departure)
        self.rows = complete_market_session_rows("2026-09-25", "2026-10-02")
        for index, row in enumerate(self.rows):
            row.update(
                o=str(10 + index),
                h=str(10 + index),
                l=str(10 + index),
                c=str(10 + index),
                v="100",
                n="10",
                vw=str(10 + index),
            )

    def resolve(self, *, adjustments=ALPACA_ADJUSTMENT_MODES):
        intervals, _ = build_ticker_ownership_intervals(self.lineage)
        sessions = download_session_dates(self.rows)
        consensus = adjustment_session_consensus((a, sessions) for a in adjustments)
        return correlate_download_rows(
            self.rows,
            correlation_source("SPLT", asof="2026-10-06"),
            self.lineage,
            intervals,
            adjustment_consensus=consensus,
        )

    def test_terminal_event_closes_component_history_without_uuid_vintage_or_escrow_join(self):
        result = self.resolve()
        self.assertEqual([], result["uncorrelated"])
        self.assertEqual(len(self.rows), result["correlated"][0]["row_count"])
        segment = result["correlated"][0]
        self.assertEqual(
            ["new-split-asset-id", "old-split-asset-id"], segment["security_identity"]["member_alpaca_asset_ids"]
        )
        self.assertIsNone(segment["canonical_security"]["alpaca_asset_id"])
        coverage = segment["terminal_correlation_fallback"]["session_coverage"]
        self.assertEqual("2026-10-02", coverage["evaluated_end_date_exclusive"])
        self.assertEqual("2026-10-07T00:00:00Z", coverage["actual_request_end_exclusive"])

    def test_remote_acquirer_only_or_missing_departure_does_not_close_history(self):
        for change in (
            {"effective_date": "2026-11-01"},
            {"acquiree_symbol": "OTHER"},
            {"acquiree_cusip": "other"},
        ):
            saved = copy.deepcopy(self.departure)
            self.departure.update(change)
            with self.subTest(change=change):
                self.assertTrue(self.resolve()["uncorrelated"])
            self.departure.clear()
            self.departure.update(saved)

    def test_unlinked_third_occupant_and_missing_view_do_not_qualify(self):
        self.assertTrue(self.resolve(adjustments=("raw",))["uncorrelated"])
        self.lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "SPLT",
                "444444444",
                "other",
                action_type="cash_dividend",
                action_id="other",
                process_date="2026-09-28",
                role="symbol",
            )
        )
        self.assertTrue(self.resolve()["uncorrelated"])

    def test_internal_missing_session_remains_unresolved(self):
        self.rows.pop(2)
        self.assertTrue(self.resolve()["uncorrelated"])

    def test_synthetic_bridge_is_not_continuous_history_proof(self):
        rows = complete_market_session_rows("2025-01-02", "2026-10-01")
        self.rows = [
            {**row, "o": "10", "h": "10", "l": "10", "c": "10", "v": "0", "n": "0", "vw": "10"} for row in rows
        ]
        self.assertTrue(self.resolve()["uncorrelated"])

    def test_inactive_existing_component_does_not_require_missing_terminal_metadata(self):
        self.lineage["corporate_actions"].remove(self.departure)
        result = self.resolve()
        self.assertEqual([], result["uncorrelated"])
        segment = result["correlated"][0]
        self.assertIsNone(segment["canonical_security"]["alpaca_asset_id"])
        coverage = segment["terminal_correlation_fallback"]["session_coverage"]
        self.assertNotIn("explained_terminal_departure", coverage)
        self.assertFalse(coverage["observed_inactive_split_component"]["termination_date_inferred"])

    def test_active_metadata_future_only_split_and_unreported_extra_identity_veto_closure(self):
        self.lineage["corporate_actions"].remove(self.departure)
        saved = copy.deepcopy(self.lineage)
        self.lineage["identities"][-1]["status"] = "active"
        self.assertTrue(self.resolve()["uncorrelated"])
        self.lineage = copy.deepcopy(saved)
        self.lineage["corporate_actions"][0]["ex_date"] = "2026-11-01"
        self.assertTrue(self.resolve()["uncorrelated"])
        self.lineage = copy.deepcopy(saved)
        self.lineage["input_symbol_occupants"] = [{"symbol": "SPLT", "asset_id": "unexplained"}]
        self.assertTrue(self.resolve()["uncorrelated"])

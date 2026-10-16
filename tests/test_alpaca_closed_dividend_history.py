import copy
import unittest

from tests import test_alpaca_closed_split_history as split_fixtures


class ClosedDividendHistoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = split_fixtures.ClosedSplitHistoryTests()
        self.fixture.setUp()
        self.lineage = self.fixture.lineage
        self.old_dividend = {
            "id": "old-dividend",
            "action_type": "cash_dividend",
            "symbol": "SPLT",
            "cusip": "111111111",
            "ex_date": "2026-09-25",
            "rate": "0.1",
        }
        self.new_dividend = {
            **self.old_dividend,
            "id": "new-dividend",
            "cusip": "222222222",
            "ex_date": "2026-09-30",
        }
        self.terminal = {
            **self.fixture.departure,
            "action_type": "stock_and_cash_merger",
            "effective_date": "2026-10-06",
            "cash_rate": "12",
        }
        self.lineage["corporate_actions"] = [self.old_dividend, self.new_dividend, self.terminal]
        self.lineage["symbol_occupancy_evidence"] = [
            split_fixtures.occupancy(
                "SPLT",
                action["cusip"],
                asset_id,
                action_type="cash_dividend",
                action_id=action["id"],
                process_date=action["ex_date"],
                role="symbol",
            )
            for action, asset_id in (
                (self.old_dividend, "old-split-asset-id"),
                (self.new_dividend, "new-split-asset-id"),
            )
        ]

    def test_complete_ceased_history_joins_candidates_without_inventing_uuid_or_trading_cutovers(self):
        result = self.fixture.resolve()
        self.assertEqual([], result["uncorrelated"])
        segment = result["correlated"][0]
        self.assertIsNone(segment["canonical_security"]["alpaca_asset_id"])
        self.assertIsNone(segment["canonical_security"]["cusip"])
        self.assertEqual(
            ["new-split-asset-id", "old-split-asset-id"],
            segment["security_identity"]["member_alpaca_asset_ids"],
        )
        coverage = segment["terminal_correlation_fallback"]["session_coverage"]
        proof = coverage["observed_inactive_successive_dividend_history"]
        self.assertFalse(proof["termination_date_inferred"])
        self.assertFalse(proof["uuid_cutover_dates_inferred"])
        self.assertGreater(coverage["actual_request_missing_session_count"], 0)

    def test_interleaving_future_occupancy_or_wrong_terminal_endpoint_does_not_decide(self):
        for target, change in (
            (self.old_dividend, {"ex_date": "2026-10-01"}),
            (self.new_dividend, {"ex_date": "2026-11-01"}),
            (self.terminal, {"acquiree_cusip": "111111111"}),
            (self.terminal, {"cash_rate": "NaN"}),
        ):
            with self.subTest(change=change):
                saved = copy.deepcopy(target)
                target.update(change)
                self.assertTrue(self.fixture.resolve()["uncorrelated"])
                target.clear()
                target.update(saved)

    def test_extra_occupant_or_foreign_predecessor_vetoes_same_backfilled_names(self):
        self.lineage["input_symbol_occupants"] = [{"symbol": "SPLT", "asset_id": "unexplained"}]
        self.assertTrue(self.fixture.resolve()["uncorrelated"])
        del self.lineage["input_symbol_occupants"]
        self.lineage["corporate_actions"].append(
            {
                "action_type": "name_change",
                "old_symbol": "FOREIGN",
                "new_symbol": "SPLT",
            }
        )
        self.assertTrue(self.fixture.resolve()["uncorrelated"])

    def test_missing_views_internal_gaps_and_active_identity_do_not_decide(self):
        self.assertTrue(self.fixture.resolve(adjustments=("raw",))["uncorrelated"])
        self.lineage["identities"][-1]["status"] = "active"
        self.assertTrue(self.fixture.resolve()["uncorrelated"])
        self.lineage["identities"][-1]["status"] = "inactive"
        self.fixture.rows.pop(2)
        self.assertTrue(self.fixture.resolve()["uncorrelated"])

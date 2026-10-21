import copy
import unittest
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from tests import test_alpaca_closed_split_history as split_fixtures
from tests.test_alpaca_end_of_day_security_correlation import asset


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

    def overlapping_history(self):
        payment = {
            **self.old_dividend,
            "ex_date": "2026-09-28",
            "record_date": "2026-09-29",
            "payable_date": "2026-09-30",
            "foreign": False,
            "special": False,
        }
        old = {**payment, "id": "old-overlap"}
        new = {**payment, "id": "new-overlap", "cusip": self.new_dividend["cusip"]}
        self.lineage["corporate_actions"].extend((old, new))
        return old, new

    def stock_departure(self):
        self.terminal.update(action_type="stock_merger", acquiree_rate="1", acquirer_rate="1.61514")
        self.terminal.pop("cash_rate")
        acquirer = asset("acquirer-id", "ESCROW", "Distinct acquiring security")
        lookup = {
            "lookup": "333333333",
            "lookup_kind": "cusip",
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": acquirer,
        }
        self.lineage["asset_lookups"].append(lookup)
        self.lineage["identities"].append(acquirer)
        return lookup

    def test_matching_complete_overlap_and_stock_exit_recover_only_predecessor(self):
        self.overlapping_history()
        self.stock_departure()
        result = self.fixture.resolve()
        self.assertFalse(result["uncorrelated"])
        segment = result["correlated"][0]
        self.assertEqual("stable_continuous_ticker_identity_set", segment["correlation_rule"]["name"])
        self.assertEqual(
            ["new-split-asset-id", "old-split-asset-id"],
            segment["security_identity"]["member_alpaca_asset_ids"],
        )
        self.assertIsNone(segment["canonical_security"]["alpaca_asset_id"])
        self.assertIsNone(segment["canonical_security"]["cusip"])

    def test_production_three_pass_path_includes_stock_departure_in_closed_history_precheck(self):
        self.overlapping_history()
        self.stock_departure()
        downloads, descriptors = {}, []
        for mode in ALPACA_ADJUSTMENT_MODES:
            filename = mode + ".csv.gz"
            downloads[filename] = SimpleNamespace(
                rows=copy.deepcopy(self.fixture.rows),
                path=Path(filename),
                manifest=dict(
                    symbol="SPLT",
                    adjustment=mode,
                    sha256=mode,
                    asof="2026-10-06",
                    request_from="2016-01-01",
                    request_to="2026-10-07",
                    feed="sip",
                    timeframe="1Day",
                    rows=len(self.fixture.rows),
                ),
                pipeline_state="source_published",
                integrity_failure=None,
            )
            descriptors.append(dict(file=filename, rows=len(self.fixture.rows)))
        view = SimpleNamespace(
            downloads=downloads,
            local=dict(directory="SPLT", downloads=descriptors, present=True),
            reader=lambda _root, _directory, descriptor: downloads[descriptor["file"]],
        )
        report = correlate(Path("."), {"SPLT": view}, {"SPLT": self.lineage})["SPLT"]
        self.assertEqual(len(self.fixture.rows) * 8, report["correlated"]["row_count"])
        self.assertEqual(0, report["uncorrelated"]["row_count"])
        self.assertEqual([], report["uuid_lineage_edges"])
        for segment in report["correlated"]["segments"]:
            self.assertEqual(
                ["new-split-asset-id", "old-split-asset-id"],
                segment["security_identity"]["member_alpaca_asset_ids"],
            )

    def test_stock_exit_also_closes_existing_nonoverlapping_dividend_shape(self):
        self.stock_departure()
        self.assertFalse(self.fixture.resolve()["uncorrelated"])

    def test_matching_overlap_also_preserves_cash_exit(self):
        self.overlapping_history()
        self.assertFalse(self.fixture.resolve()["uncorrelated"])

    def test_conflicting_or_incomplete_overlap_payment_cannot_be_ignored(self):
        _, payment = self.overlapping_history()
        for change in (
            {"rate": "0.11"},
            {"record_date": "2026-09-30"},
            {"payable_date": "2026-10-01"},
            {"special": True},
            {"foreign": True},
            {"foreign": None},
            {"payable_date": None},
            {"record_date": "2026-09-27"},
        ):
            with self.subTest(change=change):
                saved = copy.deepcopy(payment)
                payment.update(change)
                self.assertTrue(self.fixture.resolve()["uncorrelated"])
                payment.clear()
                payment.update(saved)

    def test_equal_intersection_with_unmatched_payment_in_overlap_does_not_decide(self):
        old, new = self.overlapping_history()
        self.lineage["corporate_actions"].append(
            {**old, "id": "old-later", "ex_date": "2026-09-29", "record_date": "2026-09-29"}
        )
        self.assertTrue(self.fixture.resolve()["uncorrelated"])
        self.lineage["corporate_actions"].append(
            {**new, "id": "new-later", "ex_date": "2026-09-29", "record_date": "2026-09-29"}
        )
        self.assertFalse(self.fixture.resolve()["uncorrelated"])

    def test_tied_or_contained_epochs_cannot_invent_order(self):
        old, new = self.overlapping_history()
        self.lineage["corporate_actions"].remove(self.new_dividend)
        self.assertTrue(self.fixture.resolve()["uncorrelated"])
        self.lineage["corporate_actions"].remove(self.old_dividend)
        self.assertTrue(self.fixture.resolve()["uncorrelated"])

    def test_stock_exit_requires_exact_distinct_endpoint_positive_rates_and_nearby_date(self):
        lookup = self.stock_departure()
        for target, change in (
            (self.terminal, {"effective_date": "2026-11-01"}),
            (self.terminal, {"effective_date": "2026-09-29"}),
            (self.terminal, {"acquirer_rate": "NaN"}),
            (self.terminal, {"acquiree_rate": "0"}),
            (self.terminal, {"acquirer_cusip": "222222222"}),
            (self.terminal, {"acquirer_symbol": "SPLT"}),
            (lookup, {"found": False}),
        ):
            with self.subTest(change=change):
                saved = copy.deepcopy(target)
                target.update(change)
                self.assertTrue(self.fixture.resolve()["uncorrelated"])
                target.clear()
                target.update(saved)

    def test_qualified_class_names_and_unexplained_occupants_still_veto(self):
        self.overlapping_history()
        self.stock_departure()
        self.lineage["identities"][0]["name"] = "Split Test Security Series B"
        self.assertTrue(self.fixture.resolve()["uncorrelated"])
        self.lineage["identities"][0]["name"] = self.lineage["identities"][1]["name"]
        self.lineage["input_symbol_occupants"] = [{"symbol": "SPLT", "asset_id": "unexplained"}]
        self.assertTrue(self.fixture.resolve()["uncorrelated"])

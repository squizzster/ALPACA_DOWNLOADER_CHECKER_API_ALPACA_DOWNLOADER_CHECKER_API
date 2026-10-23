import copy
import unittest

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from tests.test_alpaca_end_of_day_security_correlation import (
    add_name_change_edge,
    asset,
    certified_row_inputs,
    correlation_source,
    occupancy,
)


def evidence():
    result = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": [], "asset_lookups": []}
    for key, symbol, cusip in (
        ("buyer", "BUYER", "111111111"),
        ("first", "FIRST", "222222222"),
        ("second", "SECOND", "333333333"),
    ):
        item = asset(key, symbol, symbol)
        result["identities"].append({**item, "lookup_cusips": [cusip]})
        result["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": cusip,
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": item,
            }
        )
        if key == "buyer":
            result["asset_lookups"].append(
                {**result["asset_lookups"][-1], "lookup_kind": "symbol", "lookup": symbol}
            )
        else:
            result["corporate_actions"].append(
                {
                    "id": key,
                    "action_type": "cash_merger",
                    "effective_date": "2024-01-08",
                    "acquiree_symbol": symbol,
                    "acquiree_cusip": cusip,
                    "acquirer_symbol": "BUYER",
                    "acquirer_cusip": "111111111",
                    "rate": 10,
                }
            )
            result["symbol_occupancy_evidence"].append(
                occupancy(
                    symbol,
                    cusip,
                    key,
                    action_type="cash_merger",
                    action_id=key,
                    process_date="2024-01-08",
                    role="acquiree",
                )
            )
    return result


def resolve(lineage):
    intervals, _ = build_ticker_ownership_intervals(lineage)
    return correlate_download_rows(
        [{"t": "2024-01-02T12:00:00Z"}],
        correlation_source("BUYER", asof="2024-02-01"),
        lineage,
        intervals,
        **certified_row_inputs(
            [{"t": "2024-01-02T12:00:00Z"}], correlation_source("BUYER", asof="2024-02-01"), lineage, intervals
        ),
    )


class CashAcquirerScopeTests(unittest.TestCase):
    def test_cash_targets_cannot_block_independent_buyer_identity_or_join_membership(self):
        result = resolve(evidence())
        self.assertEqual([], result["uncorrelated"])
        segment = result["correlated"][0]
        self.assertEqual("buyer", segment["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(["buyer"], segment["security_identity"]["member_alpaca_asset_ids"])
        self.assertEqual("unique_uuid_candidate_with_warnings", segment["correlation_rule"]["name"])

    def test_acquiree_or_incomplete_mismatched_purchaser_evidence_does_not_exempt(self):
        for change in ({"acquirer_symbol": "OTHER"}, {"acquirer_cusip": "other"}, {"acquiree_cusip": "other"}):
            lineage = evidence()
            lineage["corporate_actions"][0].update(change)
            # A mismatched acquiree action may fail to build an interval at all,
            # so use the helper's existing approved interval for that conflict.
            from libs.alpaca_end_of_day_security_correlation.source_continuity import (
                cash_acquiree_intervals_outside_source,
            )

            intervals, _ = build_ticker_ownership_intervals(evidence())
            exempt = cash_acquiree_intervals_outside_source("BUYER", lineage, intervals)
            self.assertNotIn(next(i for i in intervals if i.historical_symbol == "FIRST").interval_key(), exempt)
        lineage = evidence()
        lineage["asset_lookups"][0]["asset"]["id"] = "wrong"
        self.assertTrue(resolve(lineage)["uncorrelated"])

    def test_missing_or_duplicate_exact_current_anchor_preserves_guard(self):
        for change in ("missing", "duplicate", "failed"):
            lineage = evidence()
            lookup = next(i for i in lineage["asset_lookups"] if i["lookup_kind"] == "symbol")
            if change == "missing":
                lineage["asset_lookups"].remove(lookup)
            elif change == "duplicate":
                lineage["asset_lookups"].append(copy.deepcopy(lookup))
            else:
                lookup["http_status"] = 404
            self.assertTrue(resolve(lineage)["uncorrelated"])

    def test_mixed_real_foreign_predecessor_keeps_original_competition(self):
        lineage = evidence()
        lineage["corporate_actions"][0] = {
            "id": "first",
            "action_type": "name_change",
            "process_date": "2024-01-08",
            "old_symbol": "FIRST",
            "old_cusip": "222222222",
            "new_symbol": "RENAMED",
            "new_cusip": "222222222",
        }
        lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "RENAMED",
                "222222222",
                "first",
                action_type="name_change",
                action_id="first",
                process_date="2024-01-08",
                role="new",
            )
        )
        self.assertTrue(resolve(lineage)["uncorrelated"])

    def test_other_source_occupant_is_not_silently_merged_by_later_fallback(self):
        lineage = evidence()
        lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "BUYER",
                "444444444",
                "other-buyer",
                action_type="cash_dividend",
                action_id="other",
                process_date="2024-01-02",
                role="symbol",
            )
        )
        self.assertTrue(resolve(lineage)["uncorrelated"])

    def test_real_name_change_predecessor_retains_earlier_asof_precedence(self):
        lineage = evidence()
        add_name_change_edge(
            lineage,
            action_id="continuation",
            process_date="2024-01-03",
            predecessor=("predecessor", "OLD", "444444444", "Old buyer"),
            successor=("buyer", "BUYER", "111111111", "Buyer"),
        )
        segment = resolve(lineage)["correlated"][0]
        self.assertEqual("predecessor", segment["canonical_security"]["alpaca_asset_id"])
        self.assertEqual("asof_selected_uuid_lineage_interval", segment["correlation_rule"]["name"])

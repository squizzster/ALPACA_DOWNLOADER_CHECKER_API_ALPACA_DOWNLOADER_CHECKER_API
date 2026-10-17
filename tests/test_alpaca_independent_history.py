import copy
import unittest
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import TickerOwnershipInterval
from libs.alpaca_end_of_day_security_correlation.predecessor_prefix import (
    INDEPENDENT_HISTORY_WARNING,
    assess_independent_historical_intervals,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import UuidLineageEdge
from tests import test_alpaca_predecessor_prefix as prefix_fixtures


class IndependentHistoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = prefix_fixtures.PredecessorPrefixTests()
        self.fixture.setUp()
        for _, source, _ in self.fixture.work:
            source["requested_symbol"] = "NEXT"
        for segment in self.fixture.segments:
            segment.update(
                adjustment=segment["data_file"][:-7],
                first_session_date="2024-08-14",
                last_session_date="2024-08-15",
                warnings=["dated_action_occupancy"],
                correlation_rule={"name": "requested_symbol_dated_uuid_interval"},
            )

    def assess(self):
        return assess_independent_historical_intervals(
            "NEXT",
            self.fixture.lineage,
            self.fixture.intervals,
            self.fixture.edges,
            self.fixture.work,
            correlation_loader=lambda symbol: SimpleNamespace(
                correlated={"segments": self.fixture.segments}, sources_by_filename=self.fixture.descriptors
            ),
            source_rows_loader=lambda symbol, filename: (self.fixture.donor_rows[filename], "generation"),
        )

    def resolve(self, proofs):
        phantom = TickerOwnershipInterval("NEXT", "new-cusip1", "new1", None, date(2024, 8, 16), ())
        return correlate_download_rows(
            prefix_fixtures.rows(),
            self.fixture.work[0][1],
            self.fixture.lineage,
            [*self.fixture.intervals, phantom],
            independent_history=proofs,
        )

    def test_independent_bounded_interval_resolves_only_competing_historical_span(self):
        proofs = self.assess()
        self.assertEqual(1, len(proofs))
        before, after = self.resolve(()), self.resolve(proofs)
        self.assertEqual(2, before["uncorrelated"][0]["row_count"])
        self.assertEqual([], after["uncorrelated"])
        first = after["correlated"][0]
        self.assertEqual("old1", first["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(2, first["row_count"])
        self.assertIn(INDEPENDENT_HISTORY_WARNING, first["warnings"])
        self.assertEqual("new1", after["correlated"][-1]["canonical_security"]["alpaca_asset_id"])

    def test_every_view_requires_independent_dated_ownership(self):
        segment = self.fixture.segments[-1]
        for change in (
            {"warnings": []},
            {"correlation_rule": {"name": "asof_selected_uuid_lineage_interval"}},
            {"warnings": ["dated_action_occupancy", INDEPENDENT_HISTORY_WARNING]},
        ):
            saved = copy.deepcopy(segment)
            segment.update(change)
            self.assertEqual((), self.assess())
            segment.clear()
            segment.update(saved)

    def test_raw_mismatch_missing_grid_or_stale_generation_does_not_qualify(self):
        filename = self.fixture.work[0][1]["data_file"]
        saved = copy.deepcopy(self.fixture.donor_rows[filename])
        self.fixture.donor_rows[filename][0]["c"] = "999"
        self.assertEqual((), self.assess())
        self.fixture.donor_rows[filename] = saved
        self.fixture.descriptors[filename]["download_sha256"] = "stale"
        self.assertEqual((), self.assess())
        self.fixture.descriptors[filename]["download_sha256"] = "generation"
        self.fixture.work.pop()
        self.assertEqual((), self.assess())

    def test_adjustment_values_may_differ_while_grids_must_match(self):
        self.fixture.donor_rows["split.csv.gz"][0]["c"] = "999"
        self.assertEqual(1, len(self.assess()))
        self.fixture.donor_rows["split.csv.gz"][0]["t"] = "2024-08-13T04:00:00Z"
        self.assertEqual((), self.assess())

    def test_conflicting_independent_owners_preserve_asof_ambiguity(self):
        proofs = self.assess()
        other = TickerOwnershipInterval("NEXT", "new-cusip1", "new1", None, date(2024, 8, 16), ())
        result = self.resolve((*proofs, replace(proofs[0], interval=other)))
        self.assertEqual(2, result["uncorrelated"][0]["row_count"])

    def test_transfer_cannot_be_laundered_into_original_prefix_donor(self):
        fixture = prefix_fixtures.PredecessorPrefixTests()
        fixture.setUp()
        for segment in fixture.segments:
            segment["warnings"] = [INDEPENDENT_HISTORY_WARNING]
        self.assertEqual((), fixture.assess())

    def split_chain_donor(self):
        self.fixture.lineage["symbol_occupancy_evidence"] = [
            item
            for item in self.fixture.lineage["symbol_occupancy_evidence"]
            if item.get("symbol") != "OLD" or item.get("asset_id") == "old1"
        ]
        self.fixture.lineage["identities"] = [
            item
            for item in self.fixture.lineage["identities"]
            if item.get("symbol") != "OLD" or item.get("id") == "old1"
        ]
        self.fixture.lineage["symbol_occupancy_evidence"].append(
            {"symbol": "OLD", "asset_id": "old0", "cusip": "old-cusip0"}
        )
        split = UuidLineageEdge(
            "reverse_split", "split", date(2023, 1, 3), "old0", "OLD", "old-cusip0", "old1", "OLD", "old-cusip1"
        )
        self.fixture.edges = [split, *self.fixture.edges]
        for segment in self.fixture.segments:
            segment["warnings"] = ["inferred_lower_ownership_boundary"]
            segment["security_identity"] = {"member_alpaca_asset_ids": ["old0", "old1", "new1"]}
        return split

    def test_direct_split_chain_donor_works_after_requested_label_expires(self):
        self.split_chain_donor()
        proofs = self.assess()
        self.assertEqual(1, len(proofs))
        intervals = [
            replace(item, end_date_exclusive=date(2024, 8, 20)) if item.historical_symbol == "NEXT" else item
            for item in self.fixture.intervals
        ]
        result = correlate_download_rows(
            prefix_fixtures.rows(),
            self.fixture.work[0][1],
            self.fixture.lineage,
            intervals,
            independent_history=proofs,
        )
        first = result["correlated"][0]
        self.assertEqual(2, first["row_count"])
        self.assertEqual(9, first["correlation_rule"]["order"])
        self.assertEqual("old1", first["canonical_security"]["alpaca_asset_id"])
        self.assertIn(INDEPENDENT_HISTORY_WARNING, first["warnings"])
        self.assertEqual("new1", result["correlated"][-1]["canonical_security"]["alpaca_asset_id"])

    def test_shared_future_membership_cannot_replace_direct_split_chronology(self):
        split = self.split_chain_donor()
        for change in (
            {"edge_type": "stock_merger"},
            {"predecessor_symbol": "FOREIGN"},
            {"effective_date": date(2025, 1, 3)},
        ):
            with self.subTest(change=change):
                self.fixture.edges[0] = replace(split, **change)
                self.assertEqual((), self.assess())
        self.fixture.edges[0] = split
        self.fixture.lineage["symbol_occupancy_evidence"].append(
            {"symbol": "OLD", "asset_id": "unexplained", "cusip": "another"}
        )
        self.assertEqual((), self.assess())

    def test_split_chain_branch_or_recursive_donor_does_not_qualify(self):
        split = self.split_chain_donor()
        self.fixture.edges.append(replace(split, successor_asset_id="other", successor_symbol="OTHER"))
        self.assertEqual((), self.assess())
        self.fixture.edges.pop()
        self.fixture.segments[-1]["warnings"].append(INDEPENDENT_HISTORY_WARNING)
        self.assertEqual((), self.assess())

    def test_later_membership_does_not_erase_sole_dated_predecessor_provenance(self):
        self.split_chain_donor()
        self.fixture.lineage["symbol_occupancy_evidence"] = [
            item for item in self.fixture.lineage["symbol_occupancy_evidence"] if item.get("asset_id") != "old0"
        ]
        self.fixture.edges[0] = replace(
            self.fixture.edges[0],
            effective_date=date(2025, 1, 3),
            predecessor_symbol="NEXT",
            successor_symbol="NEXT",
        )
        proofs = self.assess()
        self.assertEqual(1, len(proofs))
        self.assertEqual("OLD", proofs[0].interval.historical_symbol)
        result = self.resolve(proofs)
        self.assertEqual("OLD", result["correlated"][0]["historical_symbol"])
        self.assertEqual("old1", result["correlated"][0]["canonical_security"]["alpaca_asset_id"])

        # A real extra occupant cannot be explained by a split after the handoff.
        self.fixture.lineage["symbol_occupancy_evidence"].append(
            {"symbol": "OLD", "asset_id": "old0", "cusip": "old-cusip0"}
        )
        self.assertEqual((), self.assess())

    def test_earlier_foreign_arrival_still_needs_its_own_provenance(self):
        self.split_chain_donor()
        self.fixture.lineage["symbol_occupancy_evidence"] = [
            item for item in self.fixture.lineage["symbol_occupancy_evidence"] if item.get("asset_id") != "old0"
        ]
        self.fixture.edges[0] = replace(self.fixture.edges[0], predecessor_symbol="FOREIGN")
        self.assertEqual((), self.assess())

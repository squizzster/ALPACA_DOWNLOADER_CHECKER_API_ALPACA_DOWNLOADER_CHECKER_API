import copy
import unittest
from datetime import date

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.predecessor_prefix import IndependentHistoricalInterval
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from tests import test_alpaca_predecessor_prefix as prefix_fixtures
from tests.test_alpaca_end_of_day_security_correlation import (
    add_name_change_edge,
    asset,
    certified_edges,
    correlation_source,
    occupancy,
)


class TargetLabelAdoptionTests(unittest.TestCase):
    def setUp(self):
        self.lineage = {"identities": [], "corporate_actions": [], "symbol_occupancy_evidence": []}
        add_name_change_edge(
            self.lineage,
            action_id="rename",
            process_date="2024-08-16",
            predecessor=("survivor", "BEFORE", "before-cusip", "Earlier surviving bank"),
            successor=("continuing", "SAME", "new-cusip", "Adopted name"),
        )
        target = asset("target", "SAME", "Adopted name")
        target["status"] = "inactive"
        self.lineage["identities"].append(target)
        self.merger = {
            "id": "acquire",
            "action_type": "stock_merger",
            "effective_date": "2024-08-16",
            "acquiree_symbol": "SAME",
            "acquiree_cusip": "target-cusip",
            "acquiree_rate": 1,
            "acquirer_symbol": "SAME",
            "acquirer_cusip": "new-cusip",
            "acquirer_rate": 0.79,
        }
        self.lineage["corporate_actions"].append(self.merger)
        self.lineage["symbol_occupancy_evidence"].extend(
            [
                occupancy(
                    "SAME",
                    "target-cusip",
                    "target",
                    action_type="stock_merger",
                    action_id="acquire",
                    process_date="2024-08-16",
                    role="acquiree",
                ),
                occupancy(
                    "SAME",
                    "new-cusip",
                    "continuing",
                    action_type="stock_merger",
                    action_id="acquire",
                    process_date="2024-08-16",
                    role="acquirer",
                ),
            ]
        )
        identities = {item["id"]: item for item in self.lineage["identities"]}
        self.lineage["asset_lookups"] = [
            {
                "lookup": cusip,
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": identities[asset_id],
            }
            for cusip, asset_id in (
                ("before-cusip", "survivor"),
                ("new-cusip", "continuing"),
                ("target-cusip", "target"),
            )
        ]
        self.intervals, _ = build_ticker_ownership_intervals(self.lineage)

    def resolve(self, proofs=()):
        return correlate_download_rows(
            prefix_fixtures.rows(),
            correlation_source("SAME", asof="2026-10-06"),
            self.lineage,
            self.intervals,
            independent_history=proofs,
            uuid_lineage_edges=certified_edges(self.lineage)[0],
        )

    def certificate(self, owner):
        interval = next(item for item in self.intervals if item.asset_id == owner)
        return IndependentHistoricalInterval(date(2024, 8, 14), date(2024, 8, 15), interval)

    def test_target_terminal_interval_survives_without_joining_surviving_component(self):
        edges, issues = certified_edges(self.lineage)
        self.assertEqual(
            [("survivor", "continuing")],
            [(edge.predecessor_asset_id, edge.successor_asset_id) for edge in edges],
        )
        self.assertEqual("acquirer_adopted_target_symbol", issues[0]["reason_code"])
        self.assertTrue(
            any(
                item.asset_id == "target" and item.end_date_exclusive == date(2024, 8, 16)
                for item in self.intervals
            )
        )

    def test_independent_survivor_or_target_proof_controls_historical_owner(self):
        for owner in ("survivor", "target"):
            with self.subTest(owner=owner):
                result = self.resolve((self.certificate(owner),))
                first = result["correlated"][0]
                self.assertEqual(owner, first["canonical_security"]["alpaca_asset_id"])
                self.assertEqual(2, first["row_count"])
                if owner == "survivor":
                    self.assertEqual(
                        ["continuing", "survivor"], first["security_identity"]["member_alpaca_asset_ids"]
                    )
                else:
                    self.assertEqual(["target"], first["security_identity"]["member_alpaca_asset_ids"])
                self.assertEqual("continuing", result["correlated"][-1]["canonical_security"]["alpaca_asset_id"])

    def test_no_proof_or_conflicting_proofs_do_not_mechanically_select_target(self):
        for proofs in ((), (self.certificate("survivor"), self.certificate("target"))):
            result = self.resolve(proofs)
            self.assertEqual(2, result["uncorrelated"][0]["row_count"])
            self.assertEqual(1, sum(item["row_count"] for item in result["correlated"]))

    def test_one_for_one_remote_rename_and_missing_exact_endpoint_do_not_assert_separation(self):
        saved = copy.deepcopy(self.lineage)
        for change in ("one-for-one", "remote", "missing"):
            with self.subTest(change=change):
                self.lineage = copy.deepcopy(saved)
                if change == "one-for-one":
                    self.lineage["corporate_actions"][-1]["acquirer_rate"] = 1
                elif change == "remote":
                    self.lineage["corporate_actions"][0]["process_date"] = "2024-08-19"
                else:
                    self.lineage["asset_lookups"].pop()
                edges, issues = build_uuid_lineage_edges(self.lineage)
                self.assertTrue(any(edge.edge_type == "stock_merger" for edge in edges))
                self.assertFalse(
                    any(issue.get("reason_code") == "acquirer_adopted_target_symbol" for issue in issues)
                )

    def test_distinct_successor_endpoint_is_not_ticker_adoption_certificate(self):
        self.lineage["corporate_actions"][0]["new_cusip"] = "different"
        edges, issues = build_uuid_lineage_edges(self.lineage)
        self.assertTrue(any(edge.edge_type == "stock_merger" for edge in edges))
        self.assertFalse(any(issue.get("reason_code") == "acquirer_adopted_target_symbol" for issue in issues))

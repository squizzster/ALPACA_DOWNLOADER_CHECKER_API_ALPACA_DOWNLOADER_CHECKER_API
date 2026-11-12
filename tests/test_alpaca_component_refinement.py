"""A containing component needs same-security applicability, not mere overlap."""

import copy
import unittest
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import component_refinement
from libs.alpaca_end_of_day_security_correlation.component_refinement import membership_pairs, refine
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def identity(public_id, members):
    return {
        "security_id": public_id,
        "member_alpaca_asset_ids": members,
        "member_tickers": ["OLD", "NEW"],
        "basis": "explicit_uuid_lineage",
    }


def fixture():
    old, current = identity("old", ["a", "b"]), identity("current", ["a", "b", "c"])
    edge = {
        "edge_type": "reverse_split",
        "corporate_action_id": "split",
        "effective_date": "2026-11-09",
        "predecessor": {"asset_id": "b", "symbol": "NEW", "cusip": "B"},
        "successor": {"asset_id": "c", "symbol": "NEW", "cusip": "C"},
    }

    def report(component):
        return {
            "row_partition_complete": True,
            "uuid_lineage_edges": [edge] if component is current else [],
            "correlated": {
                "row_count": 32,
                "segments": [
                    {
                        "adjustment": adjustment,
                        "row_start": 1,
                        "row_end": 4,
                        "first_session_date": "2023-06-14",
                        "last_session_date": "2023-06-20",
                        "historical_symbol": "OLD",
                        "canonical_security": {
                            "security_id": component["security_id"],
                            "alpaca_asset_id": "a",
                            "cusip": "A",
                        },
                        "security_identity": component,
                        "correlation_rule": {"name": "accepted-owner"},
                        "warnings": ["retained"],
                        "observation_selection": "selected",
                        "observation_tickers": ["OLD"],
                    }
                    for adjustment in ADJUSTMENTS
                ],
            },
            "uncorrelated": {"row_count": 0, "segments": []},
        }

    reports = {"OLD": report(old), "NEW": report(current)}
    evidence = {
        "corporate_actions": [
            {
                "action_type": "name_change",
                "id": "rename-21",
                "process_date": "2023-06-21",
                "old_symbol": "OLD",
                "new_symbol": "NEW",
                "old_cusip": "A",
                "new_cusip": "A",
            },
            {
                "action_type": "name_change",
                "id": "rename-22",
                "process_date": "2023-06-22",
                "old_symbol": "OLD",
                "new_symbol": "NEW",
                "old_cusip": "A",
                "new_cusip": "A",
            },
        ],
        "asset_lookups": [{"lookup_kind": "cusip", "lookup": "A", "outcome": "found", "asset": {"id": "a"}}],
    }
    selected = {
        "receipts": {
            name: {"identity_memberships": [component]} for name, component in (("OLD", old), ("NEW", current))
        },
        "selected_group": ["NEW", "OLD"],
        "pricing_generations": {"OLD": "old-prices", "NEW": "new-prices"},
        "receipt_generations": {"OLD": "old-receipt", "NEW": "new-receipt"},
    }
    return reports, {"OLD": evidence, "NEW": evidence}, selected


class ComponentRefinementTests(unittest.TestCase):
    def test_eight_views_consume_one_completed_applicability_proof(self):
        reports, evidence, selected = fixture()
        with (
            patch.object(
                component_refinement, "_same_issue_route", wraps=component_refinement._same_issue_route
            ) as route,
            patch.object(component_refinement, "_extends", wraps=component_refinement._extends) as extension,
        ):
            _, trace = refine(reports, evidence, selected)
        self.assertEqual(len(trace["changes"]), 8)
        self.assertEqual(route.call_count, 1)
        self.assertEqual(extension.call_count, 1)

    def test_control_created_component_without_accepted_source_binding_cannot_refine(self):
        reports, evidence, selected = fixture()
        selected["receipts"]["OLD"]["identity_memberships"] = []
        result, trace = refine(reports, evidence, selected)
        self.assertFalse(trace["changes"])
        self.assertIs(result["OLD"], reports["OLD"])

    def test_missing_membership_capability_is_unknown_to_the_metadata_screen(self):
        _, _, selected = fixture()
        self.assertEqual(len(membership_pairs(selected["receipts"])), 1)
        selected["receipts"]["OLD"].pop("identity_memberships")
        self.assertIsNone(membership_pairs(selected["receipts"]))
        reports, evidence, _ = fixture()
        with self.assertRaisesRegex(SecurityCorrelationError, "owning metadata recovery"):
            refine(reports, evidence, selected)

    def test_exact_owner_and_same_issue_path_use_completed_containing_identity(self):
        reports, evidence, selected = fixture()
        before = copy.deepcopy(reports)
        with (
            patch(
                "libs.alpaca_end_of_day_security_correlation.uuid_lineage.build_uuid_lineage_edges",
                side_effect=AssertionError("accepted edges replayed"),
            ),
            patch(
                "libs.alpaca_end_of_day_security_correlation.security_identity.security_identity_for_alpaca_assets",
                side_effect=AssertionError("accepted component rebuilt"),
            ),
        ):
            result, trace = refine(reports, evidence, selected)
        target = selected["receipts"]["NEW"]["identity_memberships"][0]
        self.assertEqual(reports, before)
        self.assertIs(result["NEW"], reports["NEW"])
        self.assertEqual(len(trace["changes"]), 8)
        self.assertEqual(trace["changed_source_view_rows"], {"OLD": 32})
        for old_segment, new_segment in zip(
            reports["OLD"]["correlated"]["segments"], result["OLD"]["correlated"]["segments"]
        ):
            self.assertIs(new_segment["security_identity"], target)
            for key in old_segment.keys() - {"security_identity", "canonical_security"}:
                self.assertEqual(old_segment[key], new_segment[key])
            self.assertEqual(new_segment["canonical_security"]["alpaca_asset_id"], "a")
            self.assertEqual(new_segment["canonical_security"]["cusip"], "A")

    def test_missing_or_unlike_extension_proof_keeps_accepted_owner(self):
        for case in ("no-edge", "spin_off", "stock_merger", "foreign-label", "fork"):
            with self.subTest(case=case):
                reports, evidence, selected = fixture()
                edges = reports["NEW"]["uuid_lineage_edges"]
                if case == "no-edge":
                    edges.clear()
                elif case == "foreign-label":
                    edges[0]["successor"]["symbol"] = "PARENT"
                elif case == "fork":
                    edges.append(
                        {
                            **copy.deepcopy(edges[0]),
                            "successor": {"asset_id": "foreign", "symbol": "NEW", "cusip": "X"},
                        }
                    )
                else:
                    edges[0]["edge_type"] = case
                result, trace = refine(reports, evidence, selected)
                self.assertFalse(trace["changes"])
                self.assertIs(result["OLD"], reports["OLD"])

    def test_name_similarity_containment_or_conflicting_mapping_is_insufficient(self):
        for case in (
            "no-rename",
            "changed-cusip",
            "different-uuid",
            "failed-lookup",
            "late-interval",
            "competing-successor",
        ):
            with self.subTest(case=case):
                reports, evidence, selected = fixture()
                facts = evidence["OLD"]
                if case == "no-rename":
                    facts["corporate_actions"].clear()
                elif case == "changed-cusip":
                    facts["corporate_actions"][0]["new_cusip"] = "X"
                elif case == "different-uuid":
                    facts["asset_lookups"][0]["asset"]["id"] = "foreign"
                elif case == "failed-lookup":
                    facts["asset_lookups"][0]["outcome"] = "failed"
                elif case == "competing-successor":
                    facts["corporate_actions"].append({**facts["corporate_actions"][0], "new_symbol": "PARENT"})
                else:
                    for segment in reports["OLD"]["correlated"]["segments"]:
                        segment["last_session_date"] = "2023-06-21"
                result, trace = refine(reports, evidence, selected)
                self.assertFalse(trace["changes"])
                self.assertIs(result["OLD"], reports["OLD"])

    def test_incomparable_and_competing_components_never_silently_union(self):
        reports, evidence, selected = fixture()
        selected["receipts"]["NEW"]["identity_memberships"][0] = identity("other", ["a", "c"])
        result, trace = refine(reports, evidence, selected)
        self.assertFalse(trace["changes"])
        self.assertIs(result["OLD"], reports["OLD"])

    def test_boundaries_and_input_generations_are_reported_without_revalidation(self):
        reports, evidence, selected = fixture()
        _, trace = refine(reports, evidence, selected)
        self.assertEqual(trace["scope_binding"]["pricing_generations"], selected["pricing_generations"])
        self.assertTrue(all(change["receipt_generation"] == "old-receipt" for change in trace["changes"]))
        self.assertTrue(
            all(change["route"]["action_ids"] == ["rename-21", "rename-22"] for change in trace["changes"])
        )


if __name__ == "__main__":
    unittest.main()

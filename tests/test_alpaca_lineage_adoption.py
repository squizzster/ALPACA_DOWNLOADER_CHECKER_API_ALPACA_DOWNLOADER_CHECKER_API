"""Select legacy lineage work from accepted controls and preserve successful observations."""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_component_refinement import fixture

from libs.alpaca_lineage_build import membership_updates, retained_membership
from libs.alpaca_lineage_build.contract import LineageError, RecoveryRequired
from libs.alpaca_lineage_build.universe_plan import retired_lookup_due


class LineageAdoptionTests(TestCase):
    def test_selected_legacy_pair_is_bound_to_unchanged_group_and_assessed_once(self):
        _, _, selected = fixture()
        pricing = selected["pricing_generations"]
        receipts = {
            name: {**item, "input_sources": pricing, "versions": {"correlation": 93}}
            for name, item in selected["receipts"].items()
        }
        index = {name: {"sha256": value} for name, value in selected["receipt_generations"].items()}
        policy = SimpleNamespace(implementation_correlation_version=94)
        cache = {}
        with patch.object(
            retained_membership, "membership_pairs", wraps=retained_membership.membership_pairs
        ) as screen:
            chosen = retained_membership.selected_pairs(receipts, pricing, policy, cache, index)
            self.assertEqual([(p["source"], p["donor"]) for p in chosen], [("OLD", "NEW")])
            self.assertIs(
                chosen, retained_membership.selected_pairs(dict(receipts), pricing, policy, cache, index)
            )
            self.assertIsNone(
                retained_membership.selected_pairs(
                    receipts, {**pricing, "NEW": "replacement"}, policy, cache, index
                )
            )
            for receipt in receipts.values():
                receipt["retained_membership_assessed"] = pricing
            self.assertIsNone(retained_membership.selected_pairs(receipts, pricing, policy, cache, index))
        self.assertEqual(screen.call_count, 1)

    def test_missing_legacy_edge_handoff_uses_only_applicable_continuity_decisions(self):
        evidence, cache = {}, {}
        unresolved = {
            "corporate_action_id": "different-UUID-rename",
            "reason_code": "name_change_requires_independent_continuity",
        }
        with patch.object(
            retained_membership, "build_uuid_lineage_edges", return_value=([], [unresolved])
        ) as producer:
            edges = retained_membership.donor_edges(
                evidence, "NEW", cache, receipt_generation="a", accepted_issues=[unresolved]
            )
            self.assertEqual(edges, [])
            self.assertIs(
                edges,
                retained_membership.donor_edges(
                    evidence, "NEW", cache, receipt_generation="a", accepted_issues=[unresolved]
                ),
            )
            self.assertEqual(producer.call_count, 1)
            with self.assertRaises(RecoveryRequired):
                retained_membership.donor_edges(
                    evidence, "NEW", cache, receipt_generation="b", accepted_issues=[]
                )
        self.assertEqual(producer.call_count, 2)

    def test_completed_no_pair_and_competing_donor_expire_at_their_group_bindings(self):
        for competing in (False, True):
            reports, evidence, selected = fixture()
            selected["store"] = "/synthetic"
            selected["receipt_generations"]["PEER"] = "receipt-1"
            selected["pricing_generations"]["PEER"] = "prices"
            selected["selected_group"] = sorted(selected["pricing_generations"])
            selected["receipts"]["PEER"] = {"identity_memberships": []}
            reports["NEW"]["lineage_evidence_sha256"] = "metadata-1"
            if competing:
                edge = reports["NEW"]["uuid_lineage_edges"][0]
                reports["NEW"]["uuid_lineage_edges"].append(
                    {**edge, "successor": {**edge["successor"], "asset_id": "another"}}
                )
            else:
                reports["NEW"]["correlated"]["segments"] = []
            ruleset = SimpleNamespace(membership_refinement=membership_updates.NAME)
            _, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
            self.assertEqual(trace["changes"], [])
            if competing:
                self.assertEqual(len(trace["issues"]), 1)
                self.assertEqual(trace["issues"][0]["reason"], "competing_uuid_transitions")
                self.assertEqual(trace["issues"][0]["asset_id"], "b")
            handoff = {"store": selected["store"], "plan": trace}
            with patch.object(membership_updates, "membership_pairs", side_effect=AssertionError("replayed")):
                reused, _ = membership_updates.update_reports(
                    reports, evidence, selected, ruleset, completed=handoff
                )
                self.assertEqual(reused, reports)
                with self.assertRaisesRegex(LineageError, "receipt changed"):
                    membership_updates.apply_completed(
                        reports,
                        {
                            **selected,
                            "receipt_generations": {**selected["receipt_generations"], "PEER": "receipt-2"},
                        },
                        handoff,
                    )
                reports["NEW"]["lineage_evidence_sha256"] = "metadata-2"
                with self.assertRaisesRegex(LineageError, "metadata changed"):
                    membership_updates.apply_completed(reports, selected, handoff)

    def test_retained_no_pair_recovery_requires_no_assignment_bodies(self):
        reports, _, selected = fixture()
        selected.update(store="/synthetic", membership_version=94)
        trace = membership_updates._empty_trace(selected, {})
        handoff = {"store": selected["store"], "plan": trace}

        def forbidden(name):
            self.fail(f"opened {name} for a bound negative decision")

        result, accepted = membership_updates.apply_completed({}, selected, handoff, load_report=forbidden)
        self.assertEqual(result, {})
        self.assertIs(accepted, trace)
        changed = {**selected, "pricing_generations": {**selected["pricing_generations"], "NEW": "later"}}
        with self.assertRaisesRegex(LineageError, "pricing changed"):
            membership_updates.apply_completed(reports, changed, handoff, load_report=forbidden)

    def test_retirement_selects_only_applicable_unassessed_or_changed_uuid(self):
        identifier = "cfa5033a-50de-40f9-b0aa-764a80c413aa"
        old = {"id": identifier, "symbol": "OLD", "class": "us_equity"}
        negative = {"lookup": "OLD", "outcome": "not_found", "http_status": 404}
        projection = {
            "source_rows": 8,
            "symbol_lookups": [negative],
            "last_observed_symbol_lookup": {
                "lookup": "OLD",
                "lookup_kind": "symbol",
                "outcome": "found",
                "http_status": 200,
                "asset": old,
            },
        }
        client = SimpleNamespace(observed_uuid_lookup=lambda _: None, catalogue_uuid_conflicts={})
        self.assertTrue(retired_lookup_due("OLD", projection, client))
        for change in (
            {"symbol_lookups": []},
            {"last_observed_symbol_lookup": None},
            {"source_rows": 0},
            {"symbol_lookups": [{**negative, "http_status": 503}]},
            {"symbol_lookups": [{**negative, "lookup": "PEER"}]},
        ):
            self.assertFalse(retired_lookup_due("OLD", {**projection, **change}, client))
        done = {**projection, "retired_uuid_lookup": {**negative, "lookup": identifier, "asset": None}}
        self.assertFalse(retired_lookup_due("OLD", done, client))
        successor = {"outcome": "found", "http_status": 200, "asset": {**old, "symbol": "NEW"}}
        client.observed_uuid_lookup = lambda _: successor
        self.assertTrue(retired_lookup_due("OLD", done, client))
        done["retired_uuid_lookup"] = {**successor, "lookup": identifier}
        self.assertFalse(retired_lookup_due("OLD", done, client))

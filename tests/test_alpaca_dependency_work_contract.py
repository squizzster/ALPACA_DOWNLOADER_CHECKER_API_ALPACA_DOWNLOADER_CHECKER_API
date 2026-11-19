"""Pricing and workflow scopes for previously published lineage groups."""

from unittest import TestCase

from test_alpaca_durable_maintenance import fixture, seed_group

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.work_contract import (
    WorkPolicy,
    conditional_dependency,
    obligation,
    pricing_target,
)


class DependencyWorkContractTests(TestCase):
    def test_only_lineage_only_legacy_work_can_use_conditional_coverage(self):
        item = {
            "reasons": ["dependency_reconciliation", "unfinished_lineage"],
            "acquire": True,
            "end_exclusive": "2026-10-20",
            "pricing_end_exclusive": "2026-10-20",
        }
        covered = "2026-10-17"
        self.assertEqual(covered, pricing_target(item, lineage_coverage=covered))
        for extra in (
            {"reasons": [*item["reasons"], "advance_retained_coverage"]},
            {"reasons": [*item["reasons"], "explicit_pricing_acquisition"]},
            {"reasons": [*item["reasons"], "unknown_requirement"]},
            {"invalidate_pricing": True},
            {"required_history_check": True},
            {"required_pricing_acquisition": True},
            {"correction_id": "unfinished-replacement"},
        ):
            with self.subTest(extra=extra):
                distinct = {**item, **extra}
                self.assertFalse(conditional_dependency(distinct))
                self.assertEqual("2026-10-20", pricing_target(distinct, lineage_coverage=covered))

    def test_later_command_cutoff_keeps_an_unfinished_unchanged_dependency_request(self):
        prior = {
            "obligation_id": "first",
            "status": "blocked",
            "reasons": ["unfinished_lineage"],
            "pricing_end_exclusive": "2026-10-17",
            "end_exclusive": "2026-10-20",
        }
        receipt = {"version": 1, "owner": "PEER", "obligation_id": "first", "source_sha256": "same-source"}
        prior["dependency_suffixes"] = {"PEER": receipt}
        later = {**prior, "end_exclusive": "2026-10-21"}
        continued = obligation(later, prior)
        self.assertNotEqual("first", continued["obligation_id"])
        self.assertEqual(
            {**receipt, "obligation_id": continued["obligation_id"]}, continued["dependency_suffixes"]["PEER"]
        )
        for change in (
            {"required_pricing_acquisition": True, "pricing_end_exclusive": "2026-10-21"},
            {"correction_id": "new-correction"},
            {"invalidate_pricing": True},
        ):
            with self.subTest(change=change):
                self.assertEqual({}, obligation({**later, **change}, prior)["dependency_suffixes"])
        self.assertEqual({}, obligation(later, {**prior, "status": "complete"})["dependency_suffixes"])

    def test_compact_acknowledgement_requires_a_current_complete_group_and_exact_empty_scope(self):
        with fixture() as case:
            store = seed_group(case, inactive_second=True)
            index = storage.read_index(store / "end_of_day/lineage")
            entries = index["symbols"]
            end = "2026-10-20"
            group = {"AAA", "BBB"}
            # AAA's new published suffix extends only this group's checking scope.
            entries["AAA"]["projection"] = {**entries["AAA"]["projection"], "processed_through": end}
            peer = entries["BBB"]
            item = obligation(
                {
                    "status": "pending",
                    "required_group": sorted(group),
                    "reasons": ["unfinished_lineage"],
                    "end_exclusive": end,
                    "pricing_end_exclusive": peer["projection"]["processed_through"][:10],
                }
            )
            policy = WorkPolicy(store, index, {"BBB": item}, [], end)
            scope = ("BBB", peer["source_sha256"], end)

            def accepted(pending=None, publications=None, owned=None):
                return policy.accept_indexed_dependencies(
                    group, end, owned or entries, pending or {}, publications or {}
                )

            for binding in (("BBB", "other-source", end), ("BBB", scope[1], "2026-10-19"), ("AAA", *scope[1:])):
                policy.empty_suffixes = {binding}
                self.assertFalse(accepted())
            policy.empty_suffixes = {scope}
            self.assertFalse(accepted(pending={"AAA": "new-source"}))
            self.assertFalse(accepted(publications={"BBB": {"source_sha256": "new"}}))
            self.assertFalse(accepted(owned={"BBB": peer}))
            self.assertFalse(accepted(owned={**entries, "BBB": {**peer, "group_members": ["BBB"]}}))
            self.assertFalse(accepted(owned={**entries, "BBB": {**peer, "computation_revision": 0}}))
            for change in (
                {"refresh_metadata": True},
                {"pricing_start": "2000-01-01"},
                {"required_group": ["AAA", "BBB", "MISSING"]},
                {"required_lookups": {"UNKNOWN": {"outcome": "not_found"}}},
                {"required_numerical_completion": {"version": 1, "operation_id": "pending"}},
                {"required_pricing_acquisition": True},
            ):
                with self.subTest(change=change):
                    policy.work["BBB"] = {**item, **change}
                    self.assertFalse(accepted())
            self.assertFalse(policy.completed_decisions)
            policy.work["BBB"] = item
            self.assertTrue(accepted())
            self.assertIs(peer, policy.satisfying_entry("BBB", entries, {}))
            self.assertIsNone(
                policy.satisfying_entry("BBB", {**entries, "AAA": {**entries["AAA"], "sha256": "new"}}, {})
            )

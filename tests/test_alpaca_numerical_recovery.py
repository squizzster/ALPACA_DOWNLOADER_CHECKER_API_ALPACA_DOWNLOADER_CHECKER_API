"""Scoped numerical recovery retains accepted work and completed milestones."""

import copy
import unittest
from unittest.mock import patch

import test_alpaca_numerical_completion_integration as integration
from test_alpaca_end_of_day_api import NativeCase

from libs.alpaca_lineage_build import (
    numerical_completion,
    numerical_recovery,
    numerical_revision,
    numerical_scope,
    source_observations,
    storage,
    universe_state,
)
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def selection_value():
    return {
        "schema_version": 1,
        "boundary": numerical_recovery.BOUNDARY,
        "mode": "numerical_only",
        "store": "/tmp/numerical-recovery-contract-test",
        "correlation_policy_sha256": "c" * 64,
        "groups": [
            {
                "members": ["AAA", "BBB"],
                "pricing_generations": {"AAA": "a" * 64, "BBB": "b" * 64},
                "receipt_generations": {"AAA": "d" * 64, "BBB": "e" * 64},
                "scopes": {
                    name: [
                        {
                            "adjustment": view,
                            "boundary": "completed_years",
                            "public_id": "f" * 64,
                            "row_start": 1,
                            "row_end": 3,
                        }
                        for view in ADJUSTMENTS
                    ]
                    for name in ("AAA", "BBB")
                },
            }
        ],
    }


class NumericalRecoveryContractTests(unittest.TestCase):
    def test_scope_admission_accepts_only_proven_disjoint_omitted_peer(self):
        value = selection_value()
        del value["groups"][0]["scopes"]["BBB"]
        group = numerical_recovery.accept_selection(value, value["store"]).groups[0]
        pid = "f" * 64
        documents = {
            "AAA": {
                "completed_years": {
                    "adjustments": [
                        {
                            "adjustment": view,
                            "segments": [
                                {
                                    "branch": "correlated",
                                    "security_identity": {"security_id": pid},
                                    "row_start": 1,
                                    "row_end": 3,
                                    "from": "2025-01-02",
                                    "to": "2025-01-06",
                                }
                            ],
                        }
                        for view in ADJUSTMENTS
                    ]
                }
            }
        }
        previous = {
            name: {
                "anchor_month": "2026-01",
                "serving_ownership": {
                    "views": {view: {"ownership": [{"owner": [pid, name], "rows": 3}]} for view in ADJUSTMENTS}
                },
                "assignment_summary": {
                    view: {"first_timestamp": "2024-01-02T21:00:00Z", "last_timestamp": "2024-01-06T21:00:00Z"}
                    for view in ADJUSTMENTS
                },
            }
            for name in group.members
        }
        # The omitted peer is nonempty and owns this PID, but its accepted
        # complete-source row extent proves it outside every opened date.
        selected = numerical_scope.admit(group, previous, documents)
        self.assertEqual(len(selected), len(ADJUSTMENTS))
        for summary in previous["BBB"]["assignment_summary"].values():
            summary["last_timestamp"] = "2025-01-06T21:00:00Z"
        with self.assertRaisesRegex(LineageError, "numerical scope omits.*contributor"):
            numerical_scope.admit(group, previous, documents)
        # Missing extent proof must not turn into an implicit disjoint claim.
        previous["BBB"]["assignment_summary"] = {}
        with self.assertRaisesRegex(LineageError, "numerical scope omits.*contributor"):
            numerical_scope.admit(group, previous, documents)

    def test_scope_is_part_of_completion_identity_and_order_is_not(self):
        value = selection_value()
        original = numerical_recovery.accept_selection(value, value["store"])
        reordered = copy.deepcopy(value)
        reordered["groups"][0]["scopes"]["AAA"].reverse()
        same = numerical_recovery.accept_selection(reordered, value["store"])
        self.assertEqual(original.groups[0].operation_id, same.groups[0].operation_id)
        changed = copy.deepcopy(value)
        for intervals in changed["groups"][0]["scopes"].values():
            for interval in intervals:
                interval["row_end"] = 2
        other = numerical_recovery.accept_selection(changed, value["store"])
        self.assertNotEqual(original.groups[0].operation_id, other.groups[0].operation_id)

    def test_invalid_positions_partial_views_and_authored_fields_are_rejected(self):
        for change in ("bool", "overlap", "view", "source", "owner", "fact", "mode"):
            with self.subTest(change=change):
                value = selection_value()
                scopes = value["groups"][0]["scopes"]
                if change == "bool":
                    scopes["AAA"][0]["row_start"] = True
                elif change == "overlap":
                    scopes["AAA"].append(dict(scopes["AAA"][0]))
                elif change == "view":
                    scopes["AAA"].pop()
                elif change == "source":
                    scopes["OTHER"] = scopes.pop("AAA")
                elif change == "owner":
                    scopes["AAA"][0]["public_id"] = "research-owner"
                elif change == "fact":
                    value["groups"][0]["research_facts"] = []
                else:
                    value["mode"] = "full"
                with self.assertRaises(LineageError):
                    numerical_recovery.accept_selection(value, value["store"])

    def test_compact_completion_keeps_exact_group_and_generation_binding(self):
        value = selection_value()
        selection = numerical_recovery.accept_selection(value, value["store"])
        group = selection.groups[0]
        index = {
            "symbols": {
                name: {
                    "group_members": list(group.members),
                    "source_sha256": group.pricing[name],
                    "sha256": "0" * 64,
                    "projection": {"retained_recovery": group.completion()},
                }
                for name in group.members
            }
        }
        self.assertTrue(group.select(index))
        index["symbols"]["AAA"]["source_sha256"] = "1" * 64
        with self.assertRaisesRegex(LineageError, "changed"):
            group.select(index)

    def test_enrollment_does_not_revive_completed_upstream_requirements(self):
        value = selection_value()
        selection = numerical_recovery.accept_selection(value, value["store"])
        group = selection.groups[0]
        index = {
            "symbols": {
                name: {
                    "group_members": list(group.members),
                    "source_sha256": group.pricing[name],
                    "sha256": group.receipts[name],
                    "projection": {"processed_through": "2020-01-04T00:00:00Z"},
                }
                for name in group.members
            }
        }
        old = {
            name: {
                "status": "complete",
                "end_exclusive": "2030-01-04",
                "pricing_end_exclusive": "2030-01-04",
                "required_history_check": True,
                "refresh_metadata": True,
                "pricing_correction": {"previously": "completed"},
                "correction_id": "completed-id",
            }
            for name in group.members
        }
        with patch.object(universe_state, "put_work_group", side_effect=lambda _store, work: work):
            enrolled = numerical_recovery.enroll(selection, index=index, work=old)
        for item in enrolled.values():
            self.assertEqual(item["end_exclusive"], "2020-01-04")
            self.assertEqual(item["pricing_end_exclusive"], "2020-01-04")
            self.assertFalse(item["required_history_check"])
            self.assertFalse(item["refresh_metadata"])
            self.assertNotIn("pricing_correction", item)
            self.assertNotIn("correction_id", item)

    def test_generation_applicability_distinguishes_preserved_changed_and_unknown_dates(self):
        selected = numerical_recovery.accept_selection(selection_value(), selection_value()["store"])
        group = selected.groups[0]
        sources = {"AAA": {"history_changes": [{"source_sha256": "0" * 64, "changed_from": "2020-01-03"}]}}
        self.assertTrue(
            numerical_recovery._generation_applies(group, sources, "AAA", group.pricing["AAA"], "2020-01-04")
        )
        self.assertTrue(numerical_recovery._generation_applies(group, sources, "AAA", "0" * 64, "2020-01-02"))
        self.assertFalse(numerical_recovery._generation_applies(group, sources, "AAA", "0" * 64, "2020-01-03"))
        self.assertIsNone(numerical_recovery._generation_applies(group, sources, "AAA", "9" * 64, "2020-01-02"))


class NumericalRecoveryMilestoneTests(NativeCase):
    legacy_group = integration.NumericalCompletionIntegrationTests.legacy_group
    selection = integration.NumericalCompletionIntegrationTests.selection
    no_ownership_or_pricing_work = integration.NumericalCompletionIntegrationTests.no_ownership_or_pricing_work

    def equality_receipts(self, selected, *, day=None, generation=None, extra_donor=None):
        from libs.alpaca_lineage_build.source_equivalence import validate

        group = selected.groups[0]
        frame = {
            # These are the original donor anchors, deliberately not the
            # current receipt tokens after metadata-only publications.
            "previous_receipt_generations": {name: "1" * 64 for name in group.receipts},
            "pricing_generations": {**group.pricing, **({"MAIN": generation} if generation else {})},
            "previous_pricing_generations": dict(group.pricing),
            "identities": {
                self.public_id: {
                    day or "2025-01-02": {
                        "selected_source": "MAIN",
                        "donors": [{"source": "OLD", "public_id": self.public_id}],
                    }
                }
            },
        }
        if extra_donor is not None:
            frame["identities"][self.public_id][day or "2025-01-02"]["donors"].append(
                {"source": extra_donor, "public_id": self.public_id}
            )
        receipts = copy.deepcopy(self.receipts)
        for receipt in receipts.values():
            receipt["numerical_source_equivalence_version"] = 1
            receipt["numerical_source_equivalence"] = {"version": 1, "decisions": [frame]}
            validate(receipt)
        return receipts

    def test_applicable_completed_equality_is_not_reopened_or_retired(self):
        self.legacy_group()
        selected = self.selection()
        receipts = self.equality_receipts(selected)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "read_receipt", side_effect=lambda _root, name, *_args, **_kw: receipts[name]),
            patch.object(
                source_observations, "counterpart_positions", side_effect=AssertionError("closed prices opened")
            ),
            patch.object(
                numerical_completion, "complete", side_effect=AssertionError("closed equality replayed")
            ),
            self.assertRaisesRegex(LineageError, "completed source-equivalence.*direct carry"),
        ):
            numerical_recovery.resume(selected)

    def test_unrelated_completed_equality_does_not_block_or_get_discarded(self):
        self.legacy_group()
        selected, context = self.selection(), OperationContext(self.store)
        receipts = self.equality_receipts(selected, day="2025-01-08")
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "read_receipt", side_effect=lambda _root, name, *_args, **_kw: receipts[name]),
        ):
            result = numerical_recovery.resume(selected, context=context)
        self.assertTrue(result["complete"], result)
        receipt = storage.read_receipt(self.root, "MAIN", storage.read_index(self.root))
        frames = receipt["numerical_source_equivalence"]["decisions"]
        self.assertEqual(set(frames[0]["identities"][self.public_id]), {"2025-01-08"})

    def test_one_changed_donor_does_not_reopen_another_applicable_closed_donor(self):
        self.legacy_group()
        selected = self.selection()
        receipts = self.equality_receipts(selected, extra_donor="ZERO")
        applies = numerical_recovery._generation_applies

        def changed(group, sources, name, generation, day):
            return False if name == "ZERO" else applies(group, sources, name, generation, day)

        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "read_receipt", side_effect=lambda _root, name, *_args, **_kw: receipts[name]),
            patch.object(numerical_recovery, "_generation_applies", side_effect=changed),
            patch.object(
                source_observations, "counterpart_positions", side_effect=AssertionError("closed prices opened")
            ),
            self.assertRaisesRegex(LineageError, "contains.*completed source-equivalence"),
        ):
            numerical_recovery.resume(selected)

    def test_missing_equality_ancestry_does_not_authorize_new_selection(self):
        self.legacy_group()
        selected = self.selection()
        receipts = self.equality_receipts(selected, generation="9" * 64)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "read_receipt", side_effect=lambda _root, name, *_args, **_kw: receipts[name]),
            patch.object(
                source_observations, "counterpart_positions", side_effect=AssertionError("closed prices opened")
            ),
            self.assertRaisesRegex(LineageError, "may overlap.*completed source-equivalence"),
        ):
            numerical_recovery.resume(selected)

    def test_accepted_publication_does_not_rehash_selected_source_handoffs(self):
        self.legacy_group()
        selected = self.selection()
        digest = numerical_recovery.document_digest
        observed = {}

        def once(value):
            if isinstance(value, dict) and "latest_request" in value and "history_start" in value:
                name = value["symbol"]
                observed[name] = observed.get(name, 0) + 1
                self.assertEqual(observed[name], 1, "accepted source generation rehashed")
            return digest(value)

        with (
            self.no_ownership_or_pricing_work(),
            patch.object(numerical_recovery, "document_digest", side_effect=once),
            patch.object(storage, "document_digest", side_effect=once),
        ):
            result = numerical_recovery.resume(selected)
        self.assertTrue(result["complete"], result)
        self.assertEqual(observed, {name: 1 for name in self.members})

    def test_full_accepted_metadata_completes_missing_route_without_outside_prices(self):
        self.legacy_group()
        selected = self.selection()
        receipts = copy.deepcopy(self.receipts)
        for receipt in receipts.values():
            receipt["observation_routes"] = {}
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "read_receipt", side_effect=lambda _root, name, *_args, **_kw: receipts[name]),
        ):
            result = numerical_recovery.resume(selected)
        self.assertTrue(result["complete"], result)
        receipt = storage.read_receipt(self.root, "MAIN", storage.read_index(self.root))
        membership = receipt["numerical_source_revision"]["source_membership"][self.public_id]
        self.assertEqual(membership, {view: ["MAIN"] for view in ADJUSTMENTS})

    def test_failure_after_selection_keeps_scoped_inputs_and_peer_patches(self):
        self.legacy_group()
        selected, context = self.selection(), OperationContext(self.store)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(numerical_revision, "build", side_effect=RuntimeError("revision interrupted")),
            self.assertRaisesRegex(RuntimeError, "revision interrupted"),
        ):
            numerical_recovery.resume(selected, context=context)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(numerical_completion, "complete", side_effect=AssertionError("selection repeated")),
            patch.object(
                source_observations, "counterpart_positions", side_effect=AssertionError("prices reopened")
            ),
            patch.object(source_observations, "apply_selection", side_effect=AssertionError("patch repeated")),
            patch.object(storage, "read_receipt", side_effect=AssertionError("receipt reread")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("assignment reread")),
        ):
            result = numerical_recovery.resume(selected, context=context)
        self.assertTrue(result["complete"], result)
        self.assertFalse(any(key[0] == "numerical_only" for key in context.group_completions))

    def test_successful_peer_patch_survives_another_peer_failure(self):
        self.legacy_group()
        selected, context = self.selection(), OperationContext(self.store)
        apply = source_observations.apply_selection
        calls = []

        def interrupted(document, *args):
            calls.append(document["symbol"])
            if document["symbol"] == "OLD" and calls.count("OLD") == 1:
                raise RuntimeError("peer interrupted")
            if calls.count("MAIN") > 1:
                raise AssertionError("successful peer patch repeated")
            return apply(document, *args)

        with (
            self.no_ownership_or_pricing_work(),
            patch.object(source_observations, "apply_selection", side_effect=interrupted),
        ):
            with self.assertRaisesRegex(RuntimeError, "peer interrupted"):
                numerical_recovery.resume(selected, context=context)
            with patch.object(
                numerical_completion, "complete", side_effect=AssertionError("selection repeated")
            ):
                result = numerical_recovery.resume(selected, context=context)
        self.assertTrue(result["complete"], result)
        self.assertEqual(calls.count("MAIN"), 1)
        self.assertEqual(calls.count("OLD"), 2)

    def test_public_recovery_enrolls_and_acknowledges_the_exact_numerical_requirement(self):
        from libs.alpaca_lineage_build.retained_recovery import recover

        self.legacy_group()
        universe_state.initialize(self.store)
        selected = self.selection()
        with self.no_ownership_or_pricing_work():
            result = recover(self.store, None, selection=selected)
        self.assertTrue(result["complete"], result)
        state = universe_state.read_state(self.store, symbols=selected.symbols)
        for name, item in state["work"].items():
            self.assertEqual(item["status"], "complete", (name, item))
            self.assertEqual(
                item["required_numerical_completion"]["operation_id"], selected.groups[0].operation_id
            )
        self.assertEqual(universe_state.count_pending(self.store), 0)

"""Reviewed research publication consumes completed decisions, including retries."""

import copy
import json
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_documented_markets import documented_fact
from test_alpaca_durable_maintenance import fixture
from test_alpaca_prepared_lineage_recovery import candidate_for

from libs.alpaca_end_of_day_security_correlation import documented_markets
from libs.alpaca_lineage_build import assignment_handoff, group_engine, pipeline, storage, transform
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import document_digest
from libs.alpaca_lineage_build.prepared_recovery import publish_prepared
from libs.alpaca_lineage_build.research_admission import APPROVAL_SCHEMA, approve_admission, prepare_admission
from libs.alpaca_lineage_build.ruleset import load_ruleset
from libs.alpaca_pricing_download import storage as pricing_storage


def research_for(store, *, facts=False):
    completed = candidate_for(store)
    ruleset = load_ruleset(authorize=True)
    policy = copy.deepcopy(ruleset.policy.as_report_object())
    policy.update(policy_id="synthetic_research", policy_role="research_candidate", semantic_sha256="f" * 64)
    policy["research"] = {
        "membership_refinement": ruleset.membership_refinement,
        "control_mode": "documented_scopes",
    }
    receipt = completed["candidate_receipts"]["AAA"]
    if facts:
        fact = documented_fact(
            "same-common",
            ("AAA", "111111111"),
            ("AAA", "111111111"),
            [("AAA", "111111111", "2016-01-01", None, "Continuing Common")],
        )
        policy["research"]["documented_facts"] = [{"symbols": ["AAA"], "supplements": [fact]}]
        evidence = receipt["evidence"]
        asset = next(item["asset"] for item in evidence["asset_lookups"] if item.get("asset"))
        evidence["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": "111111111",
                "outcome": "found",
                "http_status": 200,
                "asset": asset,
            }
        )
        evidence["sourced_market_occupancy"] = {"schema_version": 1, "facts": [fact]}
    receipt["correlation_policy"] = {
        **{key: policy[key] for key in receipt["correlation_policy"]},
        "research": policy["research"],
    }
    comparison = {"mapping_corrections": {"control": {}, "candidate": {}}, "candidate_partition_complete": True}
    report = {
        "schema": "alpaca.lineage.debug.report",
        "schema_version": 1,
        "operation": "run",
        "store": str(store.absolute()),
        "implementation_binding": {
            "debug": 3,
            "policy_lab": 22,
            "correlation_runtime": 95,
            "lineage_runtime": 98,
        },
        "groups": [
            {
                "group": ["AAA"],
                "candidate_policy": policy,
                "control_policy": ruleset.policy.as_report_object(),
                "control_kind": "accepted_publication_no_replay",
                "control_correlation_reused": True,
                "comparison": comparison,
                "candidate_handoff": {
                    "boundary": "alpaca_lineage_research_candidate",
                    "schema_version": 1,
                    "publication_authorized": False,
                    "store": str(store.absolute()),
                    "policy": policy,
                    "pricing_generations": completed["pricing_generations"],
                    "baseline_receipt_generations": completed["receipt_generations"],
                    "receipts": completed["candidate_receipts"],
                    "boundaries": completed["candidate_boundaries"],
                    "equality_capture": {"requested": False, "complete": False},
                },
            }
        ],
    }
    return json.loads(json.dumps(report)), ruleset, completed["baseline_receipts"]


def approval_for(admission):
    return {
        "schema": APPROVAL_SCHEMA,
        "schema_version": 1,
        "status": "approved",
        "approval_id": "synthetic-exact-review",
        "conclusion": "Reviewed exact scoped canonical equivalence.",
        "scope": admission.scope,
    }


@contextmanager
def no_replay():
    with ExitStack() as stack:
        for owner, name in (
            (pipeline, "load_pricing"),
            (pipeline, "correlate"),
            (pipeline, "select_observations"),
            (pipeline, "download_pricing"),
            (pipeline, "acquire"),
            (pipeline, "local_symbols"),
            (transform, "make_publication"),
            (group_engine, "make_publication"),
            (assignment_handoff, "produce"),
            (assignment_handoff, "comparison"),
            (assignment_handoff, "_valid_run"),
            (documented_markets, "bind"),
            (pricing_storage, "read_validator"),
            (storage, "selected_document"),
        ):
            stack.enter_context(patch.object(owner, name, side_effect=AssertionError(name)))
        yield


class ResearchAdmissionTests(TestCase):
    def test_reviewed_output_publishes_without_replay_and_exact_retry_skips_publication(self):
        with fixture() as case:
            report, ruleset, baseline = research_for(case.store)
            original = copy.deepcopy(report)
            context = OperationContext(case.store)
            context._ruleset = ruleset
            context._correlation_policy = context._authorized_policy = ruleset.policy
            with no_replay():
                admission = prepare_admission(report, ruleset, case.store, baseline)
                self.assertIsNone(admission.selection.prepared)
                selection = approve_admission(admission, approval_for(admission))
                result = publish_prepared(selection, context)
            self.assertEqual(original, report)
            self.assertEqual(0, result["recomputed_groups"])
            self.assertEqual(1, result["published_prepared_groups"])
            receipt = storage.read_receipt(
                case.store / "end_of_day/lineage", "AAA", storage.read_index(case.store / "end_of_day/lineage")
            )
            self.assertEqual("production", receipt["correlation_policy"]["policy_role"])
            self.assertEqual(
                "research_candidate",
                receipt["research_admission"]["approved_scope"]["candidate_policy"]["policy_role"],
            )
            self.assertIs(False, receipt["research_admission"]["original_publication_authorized"])
            self.assertIsNone(selection.prepared.observations)
            with (
                no_replay(),
                patch.object(
                    pipeline, "_current_sources", side_effect=AssertionError("completed source reread")
                ),
                patch.object(storage, "publish", side_effect=AssertionError("completed publication repeated")),
            ):
                repeated = publish_prepared(selection, context)
            self.assertEqual(1, repeated["previously_completed_groups"])
            self.assertEqual(0, repeated["published_prepared_groups"])

    def test_missing_baseline_correction_or_canonical_equivalence_fails_before_io(self):
        with fixture() as case:
            report, ruleset, baseline = research_for(case.store)
            with no_replay():
                for mutation in ("baseline", "cascade", "correction", "projection"):
                    changed = copy.deepcopy(report)
                    group = changed["groups"][0]
                    previous = baseline
                    if mutation == "baseline":
                        previous = {}
                    elif mutation == "cascade":
                        group["candidate_policy"]["row_rules"].reverse()
                    elif mutation == "correction":
                        group["comparison"]["mapping_corrections"]["candidate"] = {"AAA": {}}
                    else:
                        receipt = group["candidate_handoff"]["receipts"]["AAA"]
                        kind = next(
                            kind
                            for kind, value in receipt["boundaries"].items()
                            if value != baseline["AAA"]["boundaries"][kind]
                        )
                        receipt["assignment_handoffs"][kind].pop("_projection")
                    with (
                        self.subTest(mutation=mutation),
                        self.assertRaisesRegex(RuntimeError, "research admission"),
                    ):
                        prepare_admission(changed, ruleset, case.store, previous)
                admission = prepare_admission(report, ruleset, case.store, baseline)
                for field, invalid in (
                    ("status", "unapproved"),
                    ("scope", {}),
                    ("approval_id", ""),
                    ("schema_version", True),
                ):
                    approval = {**approval_for(admission), field: invalid}
                    with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "exact review scope"):
                        approve_admission(admission, approval)

    def test_real_receipt_invalidation_stops_publication_without_replaying_accepted_output(self):
        with fixture() as case:
            report, ruleset, baseline = research_for(case.store)
            with no_replay():
                admission = prepare_admission(report, ruleset, case.store, baseline)
                selection = approve_admission(admission, approval_for(admission))
            context = OperationContext(case.store)
            context._ruleset = ruleset
            context._correlation_policy = context._authorized_policy = ruleset.policy
            index = storage.read_index(case.store / "end_of_day/lineage")
            index["symbols"]["AAA"]["sha256"] = "a" * 64
            with (
                no_replay(),
                patch.object(storage, "refresh_index", return_value=index),
                patch.object(storage, "publish", side_effect=AssertionError("stale publication")),
                self.assertRaisesRegex(RuntimeError, "receipt generation changed"),
            ):
                publish_prepared(selection, context)

    def test_unchanged_legacy_body_and_interrupted_journal_need_no_reconstruction(self):
        class Interrupted(BaseException):
            pass

        for installed in (False, True):
            with self.subTest(installed=installed), fixture() as case:
                report, ruleset, baseline = research_for(case.store)
                receipt = report["groups"][0]["candidate_handoff"]["receipts"]["AAA"]
                kind = "completed_years"
                self.assertEqual(baseline["AAA"]["boundaries"][kind], receipt["boundaries"][kind])
                handoff = receipt["assignment_handoffs"][kind]
                for key in ("_projection", "observation_choices", "observation_choices_version"):
                    handoff.pop(key, None)
                with no_replay():
                    admission = prepare_admission(report, ruleset, case.store, baseline)
                    selection = approve_admission(admission, approval_for(admission))
                self.assertNotIn(kind, selection.prepared.boundaries["AAA"])
                self.assertIn(kind, selection.prepared.assignments["AAA"])
                install = storage._install

                def interrupted(*args, **kwargs):
                    if installed:
                        install(*args, **kwargs)
                    raise Interrupted()

                def accepted_context():
                    context = OperationContext(case.store)
                    context._ruleset = ruleset
                    context._correlation_policy = context._authorized_policy = ruleset.policy
                    return context

                with no_replay(), patch.object(storage, "_install", side_effect=interrupted):
                    with self.assertRaises(Interrupted):
                        publish_prepared(selection, accepted_context())
                with no_replay():
                    result = publish_prepared(selection, accepted_context())
                self.assertEqual(0, result["recomputed_groups"])
                self.assertEqual(1, result["previously_completed_groups"])
                self.assertEqual(0, result["published_prepared_groups"])

    def test_changed_generic_producer_binding_stops_without_replaying_output(self):
        with fixture() as case:
            report, ruleset, baseline = research_for(case.store)
            with no_replay():
                admission = prepare_admission(report, ruleset, case.store, baseline)
                selection = approve_admission(admission, approval_for(admission))
            context = OperationContext(case.store)
            context._correlation_policy = context._authorized_policy = ruleset.policy
            context._ruleset = replace(
                ruleset,
                membership_refinement=None,
                execution_sha256=document_digest({"membership_refinement": None}),
            )
            with (
                no_replay(),
                patch.object(storage, "publish", side_effect=AssertionError("changed recipe published")),
                self.assertRaisesRegex(RuntimeError, "producer selection changed"),
            ):
                publish_prepared(selection, context)

    def test_fact_backed_candidate_is_rejected_even_with_review_approval(self):
        from libs.alpaca_lineage_build.prepared_recovery import accept_candidate

        with fixture() as case:
            report, ruleset, baseline = research_for(case.store, facts=True)
            with (
                no_replay(),
                self.assertRaisesRegex(RuntimeError, "research admission.*(fact injection|assertion)"),
            ):
                prepare_admission(report, ruleset, case.store, baseline)
            completed = candidate_for(case.store)
            completed["candidate_receipts"]["AAA"]["evidence"]["sourced_market_occupancy"] = {
                "schema_version": 1,
                "facts": [{"authored": True}],
            }
            self.assertEqual("approved", completed["review"]["status"])
            with no_replay(), self.assertRaisesRegex(RuntimeError, "fact injection|assertion"):
                accept_candidate(completed, case.store)

    def test_receipt_assertions_fail_when_candidate_policy_omits_them(self):
        with fixture() as case:
            report, ruleset, baseline = research_for(case.store, facts=True)
            group = report["groups"][0]
            for policy in (
                group["candidate_policy"],
                group["candidate_handoff"]["policy"],
                group["candidate_handoff"]["receipts"]["AAA"]["correlation_policy"],
            ):
                policy["research"].pop("documented_facts", None)
            with (
                no_replay(),
                self.assertRaisesRegex(RuntimeError, "research admission.*(fact injection|assertion)"),
            ):
                prepare_admission(report, ruleset, case.store, baseline)

    def test_existing_closed_membership_is_not_revalidated_as_new_evidence(self):
        with fixture() as case:
            report, ruleset, baseline = research_for(case.store)
            closed = {"version": 1, "owners": [{"accepted": True}], "source_routes": {}}
            baseline["AAA"]["documented_identity_membership"] = copy.deepcopy(closed)
            report["groups"][0]["candidate_handoff"]["receipts"]["AAA"]["documented_identity_membership"] = (
                closed
            )
            with no_replay():
                admission = prepare_admission(report, ruleset, case.store, baseline)
                selection = approve_admission(admission, approval_for(admission))
            self.assertEqual(closed, selection.prepared.receipts["AAA"]["documented_identity_membership"])

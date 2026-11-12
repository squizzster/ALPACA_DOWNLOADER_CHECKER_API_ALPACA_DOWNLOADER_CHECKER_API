"""Generic containing-component updates retain scoped producer decisions."""

import copy
import json
import tempfile
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_component_refinement as fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, direct_adjustment_observations

from libs.alpaca_end_of_day_security_correlation import component_refinement
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import (
    membership_updates,
    metadata,
    observations,
    pipeline,
    source_work,
    storage,
    transform,
)
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.pricing import metadata_source_control
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_lineage_build.source_observations import _protect_reports
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_lineage_debug.paired import _retained
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pipeline.state import export_state, import_state
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source


def fixture():
    reports, evidence, selected = fixtures.fixture()
    controls = {
        name: SimpleNamespace(
            symbol=name,
            store="/synthetic",
            source={},
            source_sha256=selected["pricing_generations"][name],
            previous=selected["receipts"][name],
            previous_receipt_generation=selected["receipt_generations"][name],
            accepted_reuse=(True, True),
            local={},
        )
        for name in reports
    }
    identities = {
        identifier: identity
        for receipt in selected["receipts"].values()
        for identity in receipt["identity_memberships"]
        for identifier in identity["member_alpaca_asset_ids"]
    }
    plan = SourceWorkPlan(dict.fromkeys(reports, "retained"), identities, {}, {}, {}, {}, {}, False)
    ruleset = SimpleNamespace(membership_refinement=component_refinement.NAME, execution_sha256="enabled")
    return reports, evidence, selected, controls, plan, ruleset


class MembershipUpdateTests(TestCase):
    def test_fresh_completed_edges_feed_the_shared_hook_and_exact_invalidation(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        days = ("2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20")
        rows = tuple((day + "T04:00:00Z",) for day in days)
        inputs = {"OLD": SimpleNamespace(views=dict.fromkeys(ADJUSTMENTS, rows))}
        with (
            patch(
                "libs.alpaca_end_of_day_security_correlation.uuid_lineage.build_uuid_lineage_edges",
                side_effect=AssertionError("fresh edge recertified"),
            ),
            patch(
                "libs.alpaca_end_of_day_security_correlation.security_identity.security_identity_for_alpaca_assets",
                side_effect=AssertionError("fresh component rebuilt"),
            ),
        ):
            actual, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
            invalidated = membership_updates.invalidations(inputs, actual, trace)
        self.assertEqual(set(invalidated), set(days))
        self.assertTrue(all(value == {"old", "current"} for value in invalidated.values()))
        self.assertEqual(
            actual["OLD"]["correlated"]["segments"][0]["canonical_security"]["alpaca_asset_id"], "a"
        )

    def test_invalidation_uses_new_york_session_and_one_shared_timestamp_projection(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        actual, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
        rows = tuple(
            (value,)
            for value in (
                "2023-06-15T01:00:00Z",
                "2023-06-16T01:00:00Z",
                "2023-06-17T01:00:00Z",
                "2023-06-21T01:00:00Z",
            )
        )
        inputs = {"OLD": SimpleNamespace(views=dict.fromkeys(ADJUSTMENTS, rows))}
        instants = {}
        with patch.object(membership_updates, "timestamp", wraps=membership_updates.timestamp) as parse:
            result = membership_updates.invalidations(inputs, actual, trace, instants=instants)
        self.assertEqual(set(result), {"2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20"})
        self.assertEqual(parse.call_count, 4)
        with patch.object(
            membership_updates, "timestamp", side_effect=AssertionError("held timestamp reparsed")
        ):
            self.assertEqual(result, membership_updates.invalidations(inputs, actual, trace, instants=instants))

    def test_disabled_producer_performs_no_screen_proof_or_report_read(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        ruleset.membership_refinement = None
        with patch.object(membership_updates, "membership_pairs", side_effect=AssertionError("screen")):
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, {}, lambda _: self.fail("assignment read")
            )
        self.assertIs(actual, plan)
        self.assertEqual(set(reports), set(actual.dispositions))

    def test_optional_missing_membership_capability_preserves_good_reports_and_reports_unknown_scope(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        selected["receipts"]["OLD"].pop("identity_memberships")
        with patch.object(membership_updates, "refine", side_effect=AssertionError("legacy membership rebuilt")):
            actual, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertIs(actual, reports)
        self.assertFalse(trace["changes"])
        self.assertEqual(trace["applicability"], "unknown")
        self.assertEqual(trace["issues"], [{"reason": "accepted_membership_scope_unknown", "sources": ["OLD"]}])
        with self.assertRaises(LineageError):
            membership_updates.update_reports(reports, evidence, selected, ruleset, completed={})

    def test_retained_containing_component_is_scoped_and_cached(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        cache = {}
        with patch.object(component_refinement, "_extends", wraps=component_refinement._extends) as proof:
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, cache, reports.__getitem__
            )
        self.assertEqual(1, proof.call_count)
        self.assertEqual(actual.dispositions, {"OLD": "B", "NEW": "retained"})
        self.assertEqual(actual.membership_refinement["changed_source_view_rows"], {"OLD": 32})
        with patch.object(membership_updates, "refine", side_effect=AssertionError("completed proof repeated")):
            repeated = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, cache, lambda _: self.fail("assignment repeated")
            )
        self.assertIs(repeated, actual)
        self.assertEqual(actual.membership_refinement["input_evidence"], evidence)

    def test_completed_applicability_crosses_json_without_repeated_proof(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        cache = {}
        membership_updates.prepare(controls, evidence, {}, plan, ruleset, cache, reports.__getitem__)
        context = OperationContext("/synthetic")
        owner = object()
        context.sourced_operations[owner] = {"source_work": cache}
        encoder = Encoder()
        packed = export_state(context, None, encoder.encode, evidence=owner)
        objects, packed = json.loads(json.dumps([encoder.objects, packed]))
        replacement = object()
        restored, _ = import_state("/synthetic", packed, Decoder(objects).decode, evidence=replacement)
        cache = restored.sourced_operations[replacement]["source_work"]
        accepted = next(iter(cache["membership_plans"].values()))
        evidence = accepted.membership_refinement["input_evidence"]
        with patch.object(membership_updates, "refine", side_effect=AssertionError("transport repeated proof")):
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, cache, lambda _: self.fail("transport reread assignments")
            )
        self.assertIs(actual, accepted)

    def test_explicit_research_plan_consumes_no_route_or_edge_proof(self):
        reports, evidence, selected, controls, plan, ruleset = fixture()
        expected, trace = component_refinement.refine(reports, evidence, selected)
        completed = {"store": "/synthetic", "plan": trace}
        with (
            patch.object(membership_updates, "refine", side_effect=AssertionError("rule replay")),
            patch.object(membership_updates, "membership_pairs", side_effect=AssertionError("screen replay")),
            patch.object(component_refinement, "_extends", side_effect=AssertionError("edge replay")),
            patch.object(component_refinement, "_same_issue_route", side_effect=AssertionError("route replay")),
        ):
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, {}, reports.__getitem__, completed=completed
            )
        for old, new in zip(
            expected["OLD"]["correlated"]["segments"], actual.prepared_reports["OLD"]["correlated"]["segments"]
        ):
            self.assertEqual(
                old, {key: value for key, value in new.items() if key != "completed_membership_refinement"}
            )

    def test_actual_changed_store_or_generation_rejects_import_without_reproof(self):
        reports, _, selected, _, _, _ = fixture()
        _, trace = component_refinement.refine(*fixtures.fixture())
        selected["store"] = "/synthetic"
        for changed in ("store", "pricing", "receipt", "donor"):
            current = copy.deepcopy(selected)
            completed = {"store": "/elsewhere" if changed == "store" else "/synthetic", "plan": trace}
            if changed == "pricing":
                current["pricing_generations"]["OLD"] = "changed"
            elif changed == "receipt":
                current["receipt_generations"]["OLD"] = "changed"
            elif changed == "donor":
                current["receipt_generations"]["NEW"] = "changed"
            with self.subTest(changed=changed), self.assertRaises(LineageError):
                membership_updates.apply_completed(reports, current, completed)

    def test_missing_edges_preserve_accepted_owner_and_cached_negative(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        reports["NEW"]["uuid_lineage_edges"] = []
        cache = {}
        actual = membership_updates.prepare(controls, evidence, {}, plan, ruleset, cache, reports.__getitem__)
        self.assertFalse(actual.membership_refinement["changes"])
        self.assertEqual(actual.dispositions, plan.dispositions)
        with patch.object(membership_updates, "refine", side_effect=AssertionError("negative replay")):
            self.assertIs(
                actual,
                membership_updates.prepare(
                    controls, evidence, {}, plan, ruleset, cache, lambda _: self.fail("negative read")
                ),
            )

    def test_partial_source_scope_is_not_a_whole_identity_rebind(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        late = copy.deepcopy(reports["OLD"]["correlated"]["segments"][0])
        late.update(row_start=5, row_end=5, first_session_date="2023-06-21", last_session_date="2023-06-21")
        reports["OLD"]["correlated"]["segments"].append(late)
        actual = membership_updates.prepare(controls, evidence, {}, plan, ruleset, {}, reports.__getitem__)
        selected = source_work.closed_source(controls["OLD"], actual).accepted_report
        self.assertEqual(selected["correlated"]["segments"][-1]["security_identity"]["security_id"], "old")
        self.assertEqual(selected["correlated"]["segments"][0]["security_identity"]["security_id"], "current")

    def test_only_changed_source_positions_reopen_numerical_selection(self):
        reports, evidence, selected, controls, plan, ruleset = fixture()
        actual = membership_updates.prepare(controls, evidence, {}, plan, ruleset, {}, reports.__getitem__)
        choices = _retained(reports, [], selected)
        retained = {
            name: (
                {
                    "views": {
                        adjustment: {"ownership": [{"rows": 4, "owner": [owner, [name]]}]}
                        for adjustment in ADJUSTMENTS
                    }
                },
                {owner: receipt["identity_memberships"][0]},
                set(),
                choices[name],
            )
            for name, receipt in selected["receipts"].items()
            for owner in (receipt["identity_memberships"][0]["security_id"],)
        }
        active, protected = _protect_reports(actual.prepared_reports, retained)
        self.assertEqual(len(active["OLD"]["correlated"]["segments"]), 8)
        self.assertFalse(protected["OLD"])
        self.assertFalse(active["NEW"]["correlated"]["segments"])
        self.assertEqual(len(protected["NEW"]), 8)

    def test_public_source_work_entry_invokes_opt_in_owner(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        ruleset.policy = SimpleNamespace()
        with (
            patch.object(source_work, "_prepare_group_evidence_and_source_work", return_value=plan),
            patch.object(
                source_work,
                "retained_ownership_report",
                side_effect=lambda prior, *_: reports["OLD" if prior is controls["OLD"].previous else "NEW"],
            ),
            patch("libs.alpaca_lineage_build.transform.retained_decision_report", return_value={}),
        ):
            actual = source_work.prepare_group_evidence_and_source_work(
                controls, evidence, {}, cache={}, ruleset=ruleset
            )
        self.assertEqual(actual.dispositions["OLD"], "B")
        self.assertEqual(Path(controls["OLD"].store), Path("/synthetic"))


class OldRowsClient(FakeClient):
    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        rows = super().fetch(symbol, adjustment, start, end, asof=asof)
        return [row for row in rows if symbol != "OLD" or row["t"][:10] < "2024-01-08"]


class ProductionMembershipTests(TestCase):
    def test_researched_four_session_plan_publishes_and_retry_keeps_completed_choices(self):
        self.exercise()

    def test_construction_interruption_keeps_completed_applicability_and_numerical_decisions(self):
        self.exercise(interrupted=True)

    def test_subsequent_append_preserves_the_repaired_membership(self):
        self.exercise(append=True)

    def test_context_plan_is_consumed_before_unchanged_repeat_and_append(self):
        self.exercise(controller=True, append=True)

    def test_context_plan_consumption_survives_postcommit_json_resume(self):
        self.exercise(controller=True, delivery_json=True)

    def test_success_preserves_a_replaced_context_plan(self):
        self.exercise(controller=True, renewed=True)

    def exercise(self, *, interrupted=False, append=False, controller=False, delivery_json=False, renewed=False):
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory) / "store"
            root = store / "end_of_day/lineage"
            old = security_identity_for_alpaca_assets(
                ["a"], [], tickers=["OLD", "NEW"], basis="accepted"
            ).as_report_object()
            target = security_identity_for_alpaca_assets(
                ["a", "b"], [], tickers=["OLD", "NEW"], basis="accepted"
            ).as_report_object()
            days = (
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
                "2024-01-08",
                "2024-01-09",
                "2024-01-10",
                "2024-01-11",
            )
            bars = [bar(day, "10") for day in days]
            client = IdentityClient(
                {
                    "OLD": asset("OLD", "a"),
                    "NEW": asset("NEW", "b"),
                    "111111111": asset("OLD", "a"),
                    "222222222": asset("NEW", "b"),
                },
                [
                    {
                        "id": "rename",
                        "action_type": "name_change",
                        "old_symbol": "OLD",
                        "new_symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "111111111",
                        "process_date": "2024-01-08",
                    },
                    {
                        "id": "split",
                        "action_type": "reverse_split",
                        "symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                        "old_rate": 7,
                        "new_rate": 1,
                        "effective_date": "2024-01-10",
                        "process_date": "2024-01-10",
                    },
                ],
            )
            with (
                patch(
                    "libs.alpaca_pricing_download.download.utc_now",
                    return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                ),
                direct_adjustment_observations(),
            ):
                for name in ("OLD", "NEW"):
                    download_pricing(
                        store, name, "2024-01-12", from_date="2024-01-01", client=OldRowsClient(bars)
                    )
            context, held = OperationContext(store), {}
            evidence_client = metadata.EvidenceClient(client)
            correlate = pipeline.correlate

            def prior_fixture(*args, **kwargs):
                reports = correlate(*args, **kwargs)
                for name, report in reports.items():
                    segments = []
                    identity = old if name == "OLD" else target
                    for adjustment, rows in args[1][name].views.items():
                        for position, row in enumerate(rows, 1):
                            earlier = row[0][:10] < "2024-01-10"
                            segments.append(
                                {
                                    "adjustment": adjustment,
                                    "row_start": position,
                                    "row_end": position,
                                    "row_count": 1,
                                    "first_timestamp": row[0],
                                    "last_timestamp": row[0],
                                    "first_session_date": row[0][:10],
                                    "last_session_date": row[0][:10],
                                    "historical_symbol": name,
                                    "security_identity": identity,
                                    "canonical_security": {
                                        "security_id": identity["security_id"],
                                        "alpaca_asset_id": "a" if earlier else "b",
                                        "cusip": "111111111" if earlier else "222222222",
                                    },
                                    "ticker_ownership_interval": {
                                        "historical_symbol": name,
                                        "start_date_inclusive": "2024-01-01",
                                        "end_date_exclusive": "2024-01-08" if name == "OLD" else None,
                                    },
                                    "correlation_rule": {"name": "accepted-fixture"},
                                    "warnings": [],
                                }
                            )
                    report["correlated"] = {"row_count": len(segments), "segments": segments}
                    report["uncorrelated"] = {"row_count": 0, "segments": []}
                    held[name] = report
                return reports

            with patch.object(pipeline, "correlate", side_effect=prior_fixture):
                initial = pipeline.run_lineage(
                    store,
                    ["NEW", "OLD"],
                    identity_client=client,
                    pricing_client=OldRowsClient(bars),
                    evidence_client=evidence_client,
                    context=context,
                )
            self.assertEqual(initial["failed"], 0, initial)
            index = storage.read_index(root)
            receipts = {name: storage.read_receipt(root, name, index) for name in ("OLD", "NEW")}
            controls = {}
            for name in receipts:
                source = load_source(store / "end_of_day/pricing" / name, name)
                controls[name] = metadata_source_control(
                    store,
                    name,
                    source,
                    receipts[name]["source_sha256"],
                    receipts[name],
                    accepted_reuse=(True, True),
                )
                controls[name].previous_receipt_generation = index["symbols"][name]["sha256"]
            selected = membership_updates._selected(controls)
            reports = {
                name: {
                    **receipt,
                    **source_work.retained_ownership_report(receipt, controls[name].local, {}, root, {}),
                }
                for name, receipt in receipts.items()
            }
            reports["NEW"]["uuid_lineage_edges"] = [
                {
                    "edge_type": "reverse_split",
                    "corporate_action_id": "split",
                    "effective_date": "2024-01-10",
                    "predecessor": {"asset_id": "a", "symbol": "NEW", "cusip": "111111111"},
                    "successor": {"asset_id": "b", "symbol": "NEW", "cusip": "222222222"},
                }
            ]
            evidence = {name: receipt["evidence"] for name, receipt in receipts.items()}
            _, research = component_refinement.refine(reports, evidence, selected)
            self.assertEqual(len(research["changes"]), 32)
            ruleset = replace(
                context.accept_ruleset(),
                membership_refinement=component_refinement.NAME,
                execution_sha256="selected-membership",
            )
            if controller:
                context._ruleset = ruleset
                key = tuple(sorted(controls))
                accepted_plan = {"store": str(store), "plan": research}
                context.completed_membership_plans[key] = accepted_plan

                def controlled_run():
                    return pipeline.run_lineage(
                        store,
                        ["NEW"],
                        identity_client=client,
                        evidence_client=evidence_client,
                        context=context,
                    )

                with (
                    operation(store),
                    patch.object(
                        membership_updates,
                        "refine",
                        side_effect=AssertionError("research applicability repeated"),
                    ),
                    patch.object(
                        component_refinement,
                        "_extends",
                        side_effect=AssertionError("accepted extension proof repeated"),
                    ),
                    patch.object(
                        transform, "correlate", side_effect=AssertionError("accepted owner cascade repeated")
                    ),
                ):
                    if delivery_json:
                        with patch.object(
                            storage.ReceiptCache,
                            "published",
                            side_effect=OSError("postcommit delivery interrupted"),
                        ):
                            failed = controlled_run()
                        self.assertEqual(failed["failed"], 1, failed)
                        self.assertIs(context.completed_membership_plans[key], accepted_plan)
                        held = next(
                            value
                            for value in context.group_completions.values()
                            if value.get("effects", {}).get("published")
                        )
                        self.assertIs(held["accepted_membership_plan"][1], accepted_plan)
                        from libs.alpaca_pipeline.codec import Decoder, Encoder
                        from libs.alpaca_pipeline.state import export_state, import_state

                        encoder = Encoder()
                        state = export_state(context, None, encoder.encode, evidence=evidence_client)
                        document = json.loads(json.dumps({"state": state, "objects": encoder.objects}))
                        decoder = Decoder(document["objects"])
                        context, _ = import_state(
                            store, document["state"], decoder.decode, evidence=evidence_client
                        )
                        accepted_plan = context.completed_membership_plans[key]
                        held = next(
                            value
                            for value in context.group_completions.values()
                            if value.get("effects", {}).get("published")
                        )
                        self.assertIs(held["accepted_membership_plan"][1], accepted_plan)
                        with (
                            patch.object(
                                storage, "publish", side_effect=AssertionError("completed publication repeated")
                            ),
                            patch.object(
                                pipeline, "load_pricing", side_effect=AssertionError("completed prices reopened")
                            ),
                            patch.object(
                                observations,
                                "_select_new_observations",
                                side_effect=AssertionError("completed numerical decision repeated"),
                            ),
                        ):
                            result = controlled_run()
                    elif renewed:
                        replacement_plan = dict(accepted_plan)
                        published = storage.ReceiptCache.published

                        def renew_after_publication(cache, receipts):
                            context.completed_membership_plans[key] = replacement_plan
                            return published(cache, receipts)

                        with patch.object(storage.ReceiptCache, "published", new=renew_after_publication):
                            result = controlled_run()
                    else:
                        result = controlled_run()
                    self.assertEqual(result["failed"], 0, result)
                    if renewed:
                        self.assertIs(context.completed_membership_plans[key], replacement_plan)
                    else:
                        self.assertNotIn(key, context.completed_membership_plans)
                    self.assertFalse(
                        any("accepted_membership_plan" in value for value in context.group_completions.values())
                    )
                    if not renewed:
                        with (
                            patch.object(
                                membership_updates,
                                "apply_completed",
                                side_effect=AssertionError("consumed plan reapplied"),
                            ),
                            patch.object(
                                pipeline, "load_pricing", side_effect=AssertionError("unchanged prices reopened")
                            ),
                        ):
                            repeated = controlled_run()
                        self.assertEqual(repeated["failed"], 0, repeated)
            else:
                cache = {}
                numerical = observations._select_new_observations

                def new_contest_only(inputs, reports, **kwargs):
                    for report in reports.values():
                        for segment in report["correlated"]["segments"]:
                            self.assertLessEqual(segment["last_timestamp"][:10], "2024-01-05")
                    return numerical(inputs, reports, **kwargs)

                with (
                    operation(store),
                    patch.object(
                        membership_updates,
                        "refine",
                        side_effect=AssertionError("research applicability repeated"),
                    ),
                    patch.object(
                        transform, "correlate", side_effect=AssertionError("accepted ownership repeated")
                    ),
                    patch.object(observations, "_select_new_observations", side_effect=new_contest_only),
                ):
                    plan = source_work.prepare_group_evidence_and_source_work(
                        controls,
                        evidence,
                        {},
                        cache=cache,
                        ruleset=ruleset,
                        accepted_membership_plan={"store": str(store), "plan": research},
                    )
                    if interrupted:

                        class Interrupted(BaseException):
                            pass

                        with patch.object(transform, "make_publication", side_effect=Interrupted):
                            with self.assertRaises(Interrupted):
                                source_work.execute_source_work(
                                    store,
                                    root,
                                    controls,
                                    evidence,
                                    {},
                                    plan,
                                    ruleset.policy,
                                    cache,
                                    loader=pipeline.load_pricing,
                                )
                    decisions = (
                        patch.object(
                            source_work,
                            "_decide_source_work",
                            side_effect=AssertionError("completed numerical work repeated"),
                        )
                        if interrupted
                        else nullcontext()
                    )
                    with decisions:
                        outputs = source_work.execute_source_work(
                            store,
                            root,
                            controls,
                            evidence,
                            {},
                            plan,
                            ruleset.policy,
                            cache,
                            loader=pipeline.load_pricing,
                        )
                    with patch.object(
                        source_work,
                        "_execute_source_work",
                        side_effect=AssertionError("completed result repeated"),
                    ):
                        self.assertIs(
                            outputs,
                            source_work.execute_source_work(
                                store,
                                root,
                                controls,
                                evidence,
                                {},
                                plan,
                                ruleset.policy,
                                cache,
                                loader=lambda *_args, **_kwargs: self.fail("accepted rows reread"),
                            ),
                        )
                    new_receipts, documents, _, _ = outputs
                    for receipt in new_receipts.values():
                        receipt["input_sources"] = selected["pricing_generations"]
                    storage.publish(
                        store,
                        root,
                        index,
                        new_receipts,
                        documents,
                        {},
                        previous=receipts,
                        sources={name: control.source for name, control in controls.items()},
                    )
            actual = storage.read_receipt(root, "OLD", storage.read_index(root))
            self.assertEqual(
                {item["security_id"] for item in actual["identity_memberships"]}, {target["security_id"]}
            )
            self.assertEqual(actual["source_sha256"], receipts["OLD"]["source_sha256"])
            if append:
                appended = [*bars, bar("2024-01-12", "10"), bar("2024-01-16", "10")]
                with (
                    patch(
                        "libs.alpaca_pricing_download.download.utc_now",
                        return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                    ),
                    direct_adjustment_observations(),
                ):
                    for name in ("OLD", "NEW"):
                        download_pricing(
                            store, name, "2024-01-17", from_date="2024-01-01", client=OldRowsClient(appended)
                        )
                resumed = context if controller else OperationContext(store)
                resumed._correlation_policy = ruleset.policy
                resumed._ruleset = ruleset
                with patch.object(
                    component_refinement,
                    "_extends",
                    side_effect=AssertionError("accepted extension proof repeated"),
                ):
                    continued = pipeline.run_lineage(
                        store,
                        ["NEW"],
                        identity_client=client,
                        pricing_client=OldRowsClient(appended),
                        context=resumed,
                    )
                self.assertEqual(continued["failed"], 0, continued)
                after = storage.read_receipt(root, "OLD", storage.read_index(root))
                self.assertEqual(
                    {item["security_id"] for item in after["identity_memberships"]}, {target["security_id"]}
                )

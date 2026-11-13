"""Numerical requirements reach their owner before ordinary lineage effects."""

from contextlib import ExitStack
from unittest.mock import patch

import test_alpaca_numerical_completion_integration as integration
from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import IdentityClient

from libs.alpaca_lineage_build import (
    group_engine,
    numerical_completion,
    numerical_recovery,
    pipeline,
    storage,
    universe_state,
)
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.work_contract import WorkPolicy


class NumericalPipelineTests(NativeCase):
    legacy_group = integration.NumericalCompletionIntegrationTests.legacy_group
    selection = integration.NumericalCompletionIntegrationTests.selection
    no_ownership_or_pricing_work = integration.NumericalCompletionIntegrationTests.no_ownership_or_pricing_work

    def enrolled(self):
        self.legacy_group()
        universe_state.initialize(self.store)
        selected = self.selection()
        index = storage.read_index(self.root)
        work = numerical_recovery.enroll_selection(selected, index=index)
        context = OperationContext(self.store)
        context.index = index
        policy = WorkPolicy(
            self.store,
            index,
            work,
            [],
            max(item["end_exclusive"] for item in work.values()),
            context=context,
        )
        return selected, context, policy

    def forbid_ordinary_effects(self):
        stack = ExitStack()
        for name in ("_select_pending_pricing", "_catch_up_published_dependencies", "_prefetch_metadata"):
            stack.enter_context(patch.object(pipeline, name, side_effect=AssertionError(name)))
        return stack

    def test_lineage_cohort_drains_the_job_before_pricing_or_provider_work(self):
        _, context, work = self.enrolled()
        processed = []
        with (
            self.no_ownership_or_pricing_work(),
            self.forbid_ordinary_effects(),
            patch.object(pipeline, "process_group", side_effect=AssertionError("ordinary group replayed")),
            patch.object(numerical_completion, "complete", wraps=numerical_completion.complete) as producer,
        ):
            result = pipeline.run_lineage(
                self.store,
                list(self.members),
                identity_client=IdentityClient({}),
                context=context,
                work_policy=work,
                on_processed=processed.append,
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(producer.call_count, 1)
        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(set(processed[0]), set(self.members))

    def test_direct_group_dispatch_reuses_the_held_writer_lock(self):
        _, context, work = self.enrolled()
        with storage.writer_lock(self.store, accepted_store=context.accept_store()) as root:
            with (
                self.no_ownership_or_pricing_work(),
                patch.object(storage, "writer_lock", side_effect=AssertionError("nested lineage writer")),
                patch.object(numerical_completion, "complete", wraps=numerical_completion.complete) as producer,
            ):
                result = pipeline.process_group(
                    self.store,
                    root,
                    "MAIN",
                    identity_client=IdentityClient({}),
                    index=context.index,
                    context=context,
                    work_policy=work,
                )
        self.assertEqual(producer.call_count, 1)
        self.assertEqual(result["status"], "processed")
        self.assertEqual(set(result["processed"]), set(self.members))

    def test_future_ordinary_target_is_not_discarded_after_numerical_completion(self):
        _, context, work = self.enrolled()
        work.end = "2026-10-15"
        work.require("MAIN", "2026-10-15", reason="later_session", acquire=True, group=self.members)
        ordinary = []

        def later_group(store, root, symbol, **options):
            self.assertIsNone(work.numerical_selection(symbol))
            self.assertIsNone(work.satisfying_entry(symbol, context.index["symbols"], {}))
            ordinary.append(symbol)
            return {"symbol": symbol, "status": "waiting_for_pricing", "processed": []}

        catchup = {"failed": 0, "requested": 0, "succeeded": 0, "skipped": 0, "results": [], "requests": {}}
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(pipeline, "_catch_up_published_dependencies", return_value=catchup),
            patch.object(pipeline, "_prefetch_metadata"),
            patch.object(pipeline, "process_group", side_effect=later_group),
            patch.object(numerical_completion, "complete", wraps=numerical_completion.complete) as producer,
        ):
            result = pipeline.run_lineage(
                self.store,
                ["MAIN"],
                identity_client=IdentityClient({}),
                context=context,
                work_policy=work,
            )
        self.assertEqual(producer.call_count, 1)
        self.assertEqual(ordinary, ["MAIN"])
        self.assertEqual(result["failed"], 1, result)
        self.assertEqual(work.work["MAIN"]["end_exclusive"], "2026-10-15")

    def test_numerical_failure_blocks_only_its_group_before_ordinary_effects(self):
        _, context, work = self.enrolled()
        ordinary = []

        def independent(store, root, symbol, **options):
            self.assertEqual(symbol, "TEST")
            ordinary.append(symbol)
            return {"symbol": symbol, "status": "unchanged", "processed": [symbol]}

        def catchup(store, labels, *args, **options):
            self.assertEqual(labels, ["TEST"])
            return {"failed": 0, "requested": 0, "succeeded": 0, "skipped": 0, "results": [], "requests": {}}

        with (
            self.no_ownership_or_pricing_work(),
            patch.object(
                numerical_recovery, "resume", side_effect=LineageError("selected numerical binding changed")
            ),
            patch.object(pipeline, "_catch_up_published_dependencies", side_effect=catchup),
            patch.object(pipeline, "_prefetch_metadata"),
            patch.object(pipeline, "process_group", side_effect=independent),
        ):
            result = pipeline.run_lineage(
                self.store,
                ["MAIN", "TEST"],
                identity_client=IdentityClient({}),
                context=context,
                work_policy=work,
            )
        self.assertEqual(ordinary, ["TEST"])
        self.assertEqual(result["failed"], 1, result)
        self.assertTrue(set(self.members) <= work.blocked)
        self.assertNotIn("TEST", work.blocked)

    def test_pending_pricing_notice_is_not_discarded_by_a_completed_numerical_marker(self):
        _, context, work = self.enrolled()
        numerical_recovery.resume(self.selection(), context=context, work_policy=work)
        pending = {"MAIN": {"sha256": "pending-selected-pricing"}}
        result = group_engine.complete_numerical_group(
            self.store,
            self.root,
            "MAIN",
            set(self.members),
            context.index,
            context=context,
            work_policy=work,
            pending=pending,
        )
        self.assertIsNone(result)

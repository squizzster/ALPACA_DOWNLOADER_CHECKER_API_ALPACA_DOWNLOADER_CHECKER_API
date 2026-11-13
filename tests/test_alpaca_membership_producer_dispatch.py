"""Ordinary append dispatches the configured producer without a supplied plan."""

from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_build as fixture

from libs.alpaca_end_of_day_security_correlation.component_refinement import NAME
from libs.alpaca_lineage_build import membership_updates, pipeline, policy_engine
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.metadata import EvidenceClient


class MembershipProducerDispatchTests(TestCase):
    def test_normal_append_evaluates_enabled_producer_and_preserves_accepted_prefix(self):
        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        before, after = "2026-10-14", "2026-10-15"
        case.download(end=before, bars=fixture.calendar_bars("2026-10-01", before))
        context = OperationContext(case.store)
        client = fixture.IdentityClient()
        evidence = EvidenceClient(client)
        case.run_build(context=context, client=client, evidence_client=evidence)
        prior = case.receipt()
        self.assertEqual(NAME, context.accept_ruleset().membership_refinement)
        self.assertFalse(context.completed_membership_plans)
        case.download(end=after, bars=fixture.calendar_bars("2026-10-01", after))
        added = {bar["t"] for bar in fixture.calendar_bars(before, after)}
        build = policy_engine.build_security_correlation_report

        def only_added(report, *args, **kwargs):
            for descriptor in report["local"]["downloads"]:
                rows = kwargs["authenticated_download_reader"](None, None, descriptor).rows
                self.assertEqual(added, {row["t"] for row in rows})
            return build(report, *args, **kwargs)

        with (
            patch.object(
                membership_updates, "update_reports", wraps=membership_updates.update_reports
            ) as producer,
            patch.object(
                pipeline,
                "prepare_group_evidence_and_source_work",
                side_effect=AssertionError("manual source-work route selected"),
            ),
            patch.object(policy_engine, "build_security_correlation_report", side_effect=only_added),
        ):
            result = case.run_build(context=context, client=client, evidence_client=evidence)
        self.assertEqual(1, producer.call_count)
        self.assertNotIn("completed", producer.call_args.kwargs)
        self.assertIs(context.accept_ruleset(), producer.call_args.kwargs["ruleset"])
        completed = result["results"][0]
        self.assertTrue(completed["reused_closed_boundaries"])
        self.assertEqual(prior["source_rows"], completed["historical_rows_reused"])
        self.assertEqual(NAME, completed["membership_refinement"]["implementation"])
        self.assertEqual([], completed["membership_refinement"]["changes"])
        with (
            patch.object(
                membership_updates, "update_reports", side_effect=AssertionError("completed producer repeated")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("completed pricing reopened")),
        ):
            case.run_build(context=context, client=client, evidence_client=evidence)

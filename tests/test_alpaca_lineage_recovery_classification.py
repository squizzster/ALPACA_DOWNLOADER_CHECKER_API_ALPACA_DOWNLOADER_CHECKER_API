"""Deterministic internal recovery is blocked; transient provider work can retry."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from urllib.error import HTTPError

from libs.alpaca_lineage_build import pipeline, storage, universe_state
from libs.alpaca_lineage_build.contract import RecoveryRequired, failure_outcome
from libs.alpaca_lineage_build.work_contract import WorkPolicy, restriction


class RecoveryClassificationTests(TestCase):
    def test_missing_internal_capability_persists_blocked_without_retry_timer(self):
        with TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            policy = WorkPolicy(store, {"symbols": {}}, {}, [], "2026-01-01")
            policy.require("FUTURE27", "2026-01-01", reason="selected_numerical_work")
            error = RecoveryRequired("missing contribution-grid capability; owning recovery required")
            policy.fail(["FUTURE27"], error, stage="lineage")
            item = universe_state.read_state(store, symbols=["FUTURE27"])["work"]["FUTURE27"]
            self.assertEqual("blocked", item["status"])
            self.assertIsNone(item["retry_after_utc"])
            self.assertEqual("recovery_required", item["outcome"]["outcome"]["outcome"])
            self.assertEqual("blocked", restriction(item, "2026-01-01"))
            self.assertIsNone(restriction(item, "2026-01-01", retry_blocked=True))

    def test_transient_transport_failure_keeps_existing_retry_classification(self):
        with TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            policy = WorkPolicy(store, {"symbols": {}}, {}, [], "2026-01-01")
            policy.require("FUTURE27", "2026-01-01", reason="selected_numerical_work")
            error = HTTPError("https://example.invalid", 503, "temporarily unavailable", None, None)
            policy.fail(["FUTURE27"], error, stage="lineage")
            item = universe_state.read_state(store, symbols=["FUTURE27"])["work"]["FUTURE27"]
            self.assertEqual("retry", item["status"])
            self.assertIsNotNone(item["retry_after_utc"])
            self.assertEqual("request_failed", item["outcome"]["outcome"]["outcome"])
            self.assertEqual(503, item["outcome"]["outcome"]["http_status"])
            self.assertEqual(
                "request_failed", failure_outcome(RuntimeError("unclassified execution failure"))["outcome"]
            )

    def test_pipeline_report_and_durable_work_keep_recovery_classification(self):
        from test_alpaca_lineage_build import IdentityClient, LineageBuildTests, asset

        helper = LineageBuildTests("runTest")
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.download("FUTURE27")
        identities = IdentityClient({"FUTURE27": asset("FUTURE27")})
        helper.run_build(["FUTURE27"], client=identities)
        universe_state.initialize(helper.store)
        policy = WorkPolicy(helper.store, storage.read_index(helper.root), {}, [], "2026-10-14")
        policy.require("FUTURE27", "2026-10-14", reason="explicit_lineage_refresh", refresh=True)

        def selected_owner(*args, **kwargs):
            policy.current_group = {"FUTURE27"}
            raise RecoveryRequired("declared contribution-grid capability is broken; owning recovery required")

        with patch.object(pipeline, "process_group", side_effect=selected_owner):
            report = pipeline.run_lineage(
                helper.store, ["FUTURE27"], identity_client=identities, work_policy=policy
            )
        self.assertEqual(1, report["failed"])
        self.assertEqual("failed", report["results"][0]["status"])
        self.assertEqual("recovery_required", report["results"][0]["outcome"]["outcome"])
        item = universe_state.read_state(helper.store, symbols=["FUTURE27"])["work"]["FUTURE27"]
        self.assertEqual("blocked", item["status"])
        self.assertIsNone(item["retry_after_utc"])

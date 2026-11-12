"""Completed corrections survive an append and its interrupted acknowledgement."""

import copy
import unittest
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import universe_state, update
from libs.alpaca_lineage_build.context import accept_context
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import refresh
from libs.alpaca_pricing_download.download import download_pricing


class CompletionCarryTests(unittest.TestCase):
    def seed(self, case):
        context = accept_context(case.store)
        policy = WorkPolicy(
            case.store, context.select_index(context.accept_store().root), {}, [], "2026-10-20", context=context
        )
        prepared = refresh.prepare_refresh(case.store, ["AAA"])
        policy.require_correction(prepared)
        result = download_pricing(
            case.store,
            "AAA",
            "2026-10-17",
            client=FakeClient(case.old),
            refresh_all=True,
            history_correction_id=policy.work["AAA"]["correction_id"],
        )
        policy.accept_correction_completions([result])
        policy.require("AAA", "2026-10-20", reason="advance_summary_coverage", history_check=True)
        case.live["AAA"] = [*case.old, bar("2026-10-19")]
        case.pages.clear()
        return policy, context

    def run_maintenance(self, case, policy, context):
        return update.update_everything(
            case.store,
            "2026-10-20",
            pricing_client=case.pricing,
            identity_client=case.identity,
            work_policy=policy,
            context=context,
            recover_pending=False,
        )

    def test_equal_correction_then_append_completes_without_historical_requests(self):
        with fixture() as case:
            policy, context = self.seed(case)
            previous = copy.deepcopy(policy.work["AAA"]["correction_completion"])
            result = self.run_maintenance(case, policy, context)
            self.assertTrue(result["complete"], result)
            policy.acknowledge()
            work = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertEqual(work["status"], "complete")
            self.assertEqual(work["correction_completion"]["correction_id"], previous["correction_id"])
            self.assertNotEqual(work["correction_completion"]["source_sha256"], previous["source_sha256"])
            self.assertTrue(case.pages)
            self.assertTrue(all(p["start"][:10] >= "2026-10-17" for p in case.pages), case.pages)

    def test_append_commit_before_acknowledgement_recovers_without_provider_requests(self):
        class Interrupted(BaseException):
            pass

        with fixture() as case:
            policy, context = self.seed(case)
            with patch.object(policy, "carry_correction_completions", side_effect=Interrupted):
                with self.assertRaises(Interrupted):
                    self.run_maintenance(case, policy, context)
            context = accept_context(case.store)
            policy = WorkPolicy(
                case.store,
                context.select_index(context.accept_store().root),
                universe_state.read_state(case.store)["work"],
                [],
                "2026-10-20",
                context=context,
            )
            with patch.object(
                case.pricing, "execute_plan", side_effect=AssertionError("completed prices requested again")
            ):
                result = self.run_maintenance(case, policy, context)
            self.assertTrue(result["complete"], result)
            policy.acknowledge()
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")

    def test_changed_retained_history_or_renewed_obligation_cannot_inherit_completion(self):
        with fixture() as case:
            policy, _ = self.seed(case)
            prior = policy.work["AAA"]["correction_completion"]["source_sha256"]
            source = {
                "latest_request": {"start": "2026-10-17T00:00:00Z"},
                "history_changes": [{"source_sha256": prior, "changed_from": "2026-10-19"}],
            }
            self.assertIsNotNone(policy.correction_completion_for("AAA", "next", source))
            for changed in ("2016-01-04", "2026-10-16"):
                altered = copy.deepcopy(source)
                altered["history_changes"][0]["changed_from"] = changed
                self.assertIsNone(policy.correction_completion_for("AAA", "next", altered))
            self.assertIsNone(policy.correction_completion_for("AAA", "next", {**source, "history_changes": []}))
            policy.work["AAA"]["correction_id"] = "renewed"
            self.assertIsNone(policy.correction_completion_for("AAA", "next", source))

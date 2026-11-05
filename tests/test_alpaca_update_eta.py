"""Reporting consumes held work without changing requests or publication outcomes."""

import io
import math
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from libs.alpaca_cli.command_progress import PHASES, progress_callback
from libs.alpaca_cli.update_eta import UpdateEstimate, measured_request
from libs.alpaca_heartbeat import LifecycleReporter, parse_record


def selected(low=100, high=300, metadata=20, late=200):
    return {
        "expected_io": {
            "http_pages_minimum": low,
            "http_pages_dense_history_estimate": high,
            "selection_metadata_http_requests_observed": metadata,
            "late_bar_probe_actions_upper_bound": late,
        }
    }


class UpdateETATests(unittest.TestCase):
    def trained(self, **options):
        estimate = UpdateEstimate()
        estimate.observe("selected", selected(**options))
        for n in range(8):
            estimate.request({"started": n, "finished": n + 1, "succeeded": True})
        return estimate

    def test_estimate_excludes_completed_selection_and_labels_conditional_scope(self):
        estimate = self.trained()
        with patch("builtins.open", side_effect=AssertionError("ETA storage read")):
            context = estimate.context()
        self.assertEqual(context["eta_scope"], "known_pricing_requests")
        self.assertEqual(context["eta_status"], "conditional")
        self.assertEqual(context["estimated_remaining_lower_seconds"], 72)
        self.assertEqual(context["estimated_remaining_upper_seconds"], 272)
        self.assertEqual(context["estimated_remaining_seconds"], 172)

    def test_actual_late_plan_replaces_upper_assumption_once(self):
        estimate = self.trained()
        estimate.observe("provider_late_bar_check", {"state": "started", "minimum_pages": 30})
        self.assertEqual(estimate.context()["estimated_remaining_lower_seconds"], 101)
        self.assertEqual(estimate.context()["estimated_remaining_upper_seconds"], 102)
        before = estimate.context()
        estimate.observe("provider_late_bar_check", {"state": "started", "minimum_pages": 30})
        self.assertEqual(estimate.context(), before)

    def test_exhausted_estimate_or_component_completion_cannot_complete_update(self):
        estimate = self.trained(low=8, high=8, metadata=0, late=0)
        estimate.observe("checked", {"complete": True})
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], "TBD")
        self.assertEqual(estimate.context()["eta_status"], "awaiting_remaining_scope")
        estimate.observe("completion", {"complete": False, "pending_work": 1})
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], "TBD")
        estimate.observe("completion", {"complete": True, "pending_work": 0})
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], 0)
        self.assertEqual(estimate.context()["eta_scope"], "data_update")

    def test_newly_selected_request_scope_expands_exhausted_budget(self):
        estimate = self.trained(low=8, high=8, metadata=0, late=0)
        estimate.observe("changed_data", {"state": "started", "minimum_pages": 100})
        self.assertEqual(estimate.context()["estimated_remaining_lower_seconds"], 100)
        self.assertEqual(estimate.context()["estimated_remaining_upper_seconds"], 100)

    def test_phase_eta_uses_observed_counts_and_resets_for_different_work(self):
        now = [0]
        estimate = UpdateEstimate(clock=lambda: now[0])
        estimate.observe("lineage", {"done": 0, "total": 10})
        now[0] = 4
        estimate.observe("lineage", {"done": 2, "total": 10})
        self.assertEqual(estimate.context()["estimated_phase_remaining_seconds"], 16)
        now[0] = 6
        estimate.observe("lineage", {"done": 3, "total": 12})
        self.assertEqual(estimate.context()["estimated_phase_remaining_seconds"], 18)
        estimate.observe("lineage", {"done": 0, "total": 12})
        self.assertNotIn("estimated_phase_remaining_seconds", estimate.context())
        estimate.observe("lineage", {"done": 12, "total": 12})
        self.assertEqual(estimate.context()["estimated_phase_remaining_seconds"], 0)
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], "TBD")
        estimate.observe("serving_catalogue", {"state": "started"})
        self.assertNotIn("estimated_phase_remaining_seconds", estimate.context())

    def test_parallel_durations_are_not_added_and_shared_rate_cap_applies(self):
        for duration, remaining in ((10, 115), (0.01, math.ceil(92 * 60 / 199))):
            estimate = UpdateEstimate()
            estimate.observe("selected", selected(low=100, high=100, metadata=0))
            for _ in range(8):
                estimate.request({"started": 0, "finished": duration, "succeeded": True})
            self.assertEqual(estimate.context()["estimated_remaining_seconds"], remaining)

    def test_slow_retry_changes_rate_without_counting_attempts_as_finished_pages(self):
        estimate = self.trained()
        before = estimate.context()["estimated_remaining_seconds"]
        estimate.request({"started": 8, "finished": 68, "succeeded": True})
        after = estimate.context()
        self.assertEqual(after["eta_pricing_pages_completed"], 9)
        self.assertGreater(after["estimated_remaining_seconds"], before)
        estimate.request({"started": 68, "finished": 70, "succeeded": False})
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], "TBD")
        self.assertEqual(estimate.context()["eta_status"], "request_failed")

    def test_bad_or_unobserved_inputs_do_not_fabricate_eta(self):
        estimate = UpdateEstimate()
        estimate.observe("selected", selected(low=1, high=0))
        for value in (None, True, float("nan"), float("inf"), "10"):
            estimate.request({"started": value, "finished": 1, "succeeded": True})
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], "TBD")
        self.assertEqual(estimate.completed, 0)

    def test_observer_preserves_response_identity_exception_and_exact_call_count(self):
        from unittest.mock import Mock

        response = object()
        source = Mock(return_value=response)
        observer = Mock(side_effect=OSError("reporting unavailable"))
        self.assertIs(measured_request(source, observer)("original", asof="fixed"), response)
        source.assert_called_once_with("original", asof="fixed")
        error = ValueError("provider failure")
        source = Mock(side_effect=error)
        with self.assertRaises(ValueError) as raised:
            measured_request(source, observer)({"symbols": "AAA"})
        self.assertIs(raised.exception, error)
        source.assert_called_once()

    def test_request_observations_preserve_phase_and_heartbeat_cadence(self):
        now, stream = [0.0], io.StringIO()
        reporter = LifecycleReporter(
            PHASES,
            stream=stream,
            clock=lambda: now[0],
            first_heartbeat_context={"estimated_remaining_seconds": "TBD"},
        )
        with patch("threading.Thread.start"):
            reporter.start()
        estimate = UpdateEstimate()
        progress = progress_callback(reporter, estimate=estimate)
        progress("selected", selected())
        progress("checking", {"tickers": 9000})
        with ThreadPoolExecutor(max_workers=3) as workers:
            list(
                workers.map(
                    lambda n: progress("eta_request", {"started": n, "finished": n + 1, "succeeded": True}),
                    range(8),
                )
            )
        # Request reporting updates held memory; it does not print a heartbeat.
        self.assertEqual(len(stream.getvalue().splitlines()), 2)
        for time in (10, 70):
            now[0] = time
            reporter._emit_due()
        records = [parse_record(line) for line in stream.getvalue().splitlines(keepends=True)]
        self.assertEqual(records[-2]["context"]["estimated_remaining_seconds"], "TBD")
        self.assertEqual(records[-1]["context"]["estimated_remaining_seconds"], 172)
        self.assertEqual(records[-1]["phase"], "PRICING")
        self.assertEqual(records[-1]["context"]["tickers"], 9000)
        self.assertEqual(records[-1]["context"]["eta_pricing_pages_completed"], 8)
        reporter.finish("success", 0)

    def test_real_update_preserves_requests_histories_and_serving(self):
        from test_alpaca_durable_maintenance import fixture
        from test_alpaca_lineage_build import asset
        from test_alpaca_pricing_download import bar

        from libs.alpaca_pricing_download.download import read_pricing_views

        outcomes = []
        for observed in (False, True):
            with fixture() as case:
                case.records[:] = [asset("AAA")]
                case.live["AAA"] = [*case.old, bar("2026-10-19")]
                reporter = LifecycleReporter(PHASES, stream=io.StringIO())
                with patch("threading.Thread.start"):
                    reporter.start()
                estimate = UpdateEstimate()
                progress = progress_callback(reporter, estimate=estimate) if observed else None
                if observed:
                    case.pricing._get = measured_request(case.pricing._get, progress)
                result = case.update("2026-10-20", progress=progress)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["pending_work"], 0)
                self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
                outcomes.append((case.pages, read_pricing_views(case.store, "AAA")))
                if observed:
                    self.assertEqual(estimate.completed, len(case.pages))
                    self.assertEqual(estimate.context()["estimated_remaining_seconds"], 0)
                reporter.finish("success", 0)
        self.assertEqual(outcomes[0], outcomes[1])

    def test_cli_installs_observer_without_debug_or_extra_requests(self):
        from types import SimpleNamespace
        from unittest.mock import Mock

        from libs import alpaca_cli

        args = alpaca_cli.build_parser().parse_args(["update"])
        reporter = LifecycleReporter(PHASES, stream=io.StringIO())
        with patch("threading.Thread.start"):
            reporter.start()
        args._lifecycle = reporter
        response, result = object(), {"complete": True}
        source = Mock(return_value=response)
        pricing = SimpleNamespace(_get=source)

        def update(*args, progress, pricing_client, **kwargs):
            progress("selected", selected())
            progress("checking", {"tickers": 100})
            for _ in range(8):
                self.assertIs(pricing_client._get({"symbols": "AAA"}), response)
            return result

        with (
            patch.object(alpaca_cli, "clients", return_value=(pricing, object())),
            patch.object(alpaca_cli, "_pricing_end", return_value="2026-10-20"),
            patch("libs.alpaca_lineage_build.universe_update.run_update", side_effect=update),
            patch.object(reporter, "update", wraps=reporter.update) as reported,
        ):
            self.assertIs(alpaca_cli.dispatch(args), result)
        self.assertEqual(source.call_count, 8)
        self.assertEqual(reported.call_args.kwargs["context"]["eta_pricing_pages_completed"], 8)
        self.assertEqual(reported.call_args.kwargs["context"]["eta_status"], "conditional")
        reporter.finish("success", 0)

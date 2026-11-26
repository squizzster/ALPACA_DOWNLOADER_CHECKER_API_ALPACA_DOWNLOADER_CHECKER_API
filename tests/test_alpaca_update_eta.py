"""Reporting consumes held work without changing requests or publication outcomes."""

import io
import math
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
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
        self.assertEqual(estimate.context()["eta_status"], "unknown")
        self.assertEqual(estimate.context()["eta_reason"], "request_failed")

    def test_bad_or_unobserved_inputs_do_not_fabricate_eta(self):
        estimate = UpdateEstimate()
        estimate.observe("selected", selected(low=1, high=0))
        for value in (None, True, float("nan"), float("inf"), "10"):
            estimate.request({"started": value, "finished": 1, "succeeded": True})
        self.assertEqual(estimate.context()["estimated_remaining_seconds"], "TBD")
        self.assertEqual(estimate.completed, 0)

    def test_only_the_current_groups_exact_auxiliary_rejection_leaves_the_unresolved_count(self):
        estimate = self.trained()
        request = {
            "symbols": "AUX",
            "asof": "2026-10-15",
            "adjustment": "raw",
            "feed": "sip",
            "timeframe": "1Day",
            "start": "2016-01-01T00:00:00Z",
            "end": "2026-10-14T23:59:59.999999Z",
        }

        def reject(*, first_page=True, **scope):
            estimate.request(
                {
                    "succeeded": False,
                    "error_type": "BarsSymbolUnavailable",
                    "http_status": 400,
                    "first_page": first_page,
                    "request": {**request, **scope},
                }
            )

        reject()
        required = {"AUX": {"from": "2016-01-01", "end_exclusive": "2026-10-15"}}
        estimate.observe("recovery_dependency_acquisition", {"wave": 1, "required": required})
        reject()
        reject(symbols="PRIMARY")
        reject(asof="2026-10-14")
        reject(end="2026-10-13T23:59:59.999999Z")
        reject(first_page=False)
        estimate.observe("recovery_dependency_pricing", {"symbol": "AUX", "state": "unavailable"})
        reject()  # A later exception cannot appropriate the producer's completed request.
        estimate.observe("recovery_dependency_acquisition", {"wave": 2, "required": {"PEER": required["AUX"]}})
        completed = {"status": "processed", "unavailable_pricing": ["AUX"]}
        estimate.observe("discovery_lineage_lineage_group", completed)
        self.assertEqual(estimate.context()["eta_unresolved_request_failures"], 7)
        estimate.observe("recovery_lineage_group", completed)
        self.assertEqual(estimate.unresolved, {1, 3, 4, 5, 6, 7})
        self.assertEqual(estimate.context()["eta_unresolved_request_failures"], 6)
        self.assertEqual(estimate.context()["eta_request_failures"], 7)
        self.assertEqual(estimate.context()["eta_status"], "unknown")

    def test_malformed_optional_diagnostics_leave_failed_pages_observed(self):
        estimate = self.trained()
        failure = {
            "succeeded": False,
            "started": 8,
            "finished": 9,
            "error_type": "BarsSymbolUnavailable",
            "http_status": 400,
            "first_page": True,
        }
        required = {"from": "2016-01-01", "end_exclusive": "malformed"}
        page = {
            "symbols": "AUX",
            "adjustment": "raw",
            "feed": "sip",
            "timeframe": "1Day",
            "asof": "malformed",
            "start": "2016-01-01T00:00:00Z",
            "end": "bad date",
        }
        inputs = (
            (None, page),
            ({"AUX": ["malformed"]}, page),
            ({"AUX": required}, None),
            ({"AUX": required}, {**page, "symbols": ["AUX", "PEER"]}),
            ({"AUX": required}, page),
        )
        for scope, request in inputs:
            estimate.observe("dependency_acquisition", {"wave": 1, "required": scope})
            estimate.request({**failure, "request": request})
            estimate.observe("lineage_group", {"status": "processed", "unavailable_pricing": [[], "AUX"]})
        context = estimate.context()
        self.assertEqual(context["eta_unresolved_request_failures"], len(inputs))
        self.assertEqual(context["eta_pricing_pages_completed"], 8 + len(inputs))

    def test_real_group_retires_the_rejection_only_after_its_disposition_completes(self):
        from test_alpaca_lineage_acquisition import AuxiliaryAcquisitionTests

        from libs.alpaca_lineage_build import storage
        from libs.alpaca_lineage_build.pipeline import run_lineage

        for seed, interrupted, expected in (("BUYER", False, 0), ("BUYER", True, 1), ("ENTITLE", False, 1)):
            with self.subTest(seed=seed, interrupted=interrupted):
                case = AuxiliaryAcquisitionTests()
                case.setUp()
                self.addCleanup(case.doCleanups)
                identities = case.prepare_entitlement()
                pricing, requests = case.rejected_client()
                estimate = self.trained()
                observed = []

                def report(stage, values):
                    if stage == "eta_request":
                        estimate.request(values)
                    else:
                        estimate.observe(stage, values)
                    if stage == "dependency_pricing" and values.get("state") == "unavailable":
                        observed.append(estimate.context()["eta_unresolved_request_failures"])

                pricing._get = measured_request(pricing._get, report)
                stop = patch.object(storage, "finish_publication", side_effect=OSError("interrupted"))
                with stop if interrupted else nullcontext():
                    result = run_lineage(
                        case.store,
                        [seed],
                        identity_client=identities,
                        pricing_client=pricing,
                        end_date="2026-10-15",
                        progress=report,
                    )
                self.assertEqual(result["failed"], expected, result)
                self.assertEqual(observed, [1] if seed == "BUYER" else [])
                self.assertEqual(len(requests), 1)
                self.assertEqual(estimate.context()["eta_unresolved_request_failures"], expected)
                self.assertEqual(estimate.context()["eta_request_failures"], 1)
                self.assertEqual(estimate.context()["eta_status"], "unknown" if expected else "conditional")

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

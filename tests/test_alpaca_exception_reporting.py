"""Failure diagnostics preserve owning recovery and never repeat provider work."""

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

from libs.alpaca_cli.command_progress import PHASES, progress_callback
from libs.alpaca_cli.update_eta import UpdateEstimate, measured_request
from libs.alpaca_heartbeat import LifecycleReporter
from libs.alpaca_lineage_build import numerical_mapping, numerical_mapping_acquisition, universe_state
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download.adaptive import acquire_adjustments
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, DownloadError
from libs.alpaca_pricing_download.failures import AcquisitionFailure
from libs.alpaca_pricing_download.http import BarsClient
from libs.common.debug_trace import DebugTrace, trace_http


class ExceptionReportingTests(TestCase):
    def test_mapping_journal_recovery_is_blocked_without_provider_calls_or_cooldown(self):
        with TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            policy = WorkPolicy(store, {"symbols": {}}, {}, [], "2026-10-12")
            policy.require("FUTURE27", "2026-10-12", reason="selected_numerical_work")
            client, download = Mock(), Mock()
            request = {"numerical_mapping": {"candidate_id": "candidate", "binding": {}}}
            with (
                patch.object(numerical_mapping_acquisition.storage, "path_at") as path_at,
                patch.object(
                    numerical_mapping_acquisition, "read_document", return_value={"version": 999}
                ) as read,
                self.assertRaises(RecoveryRequired) as raised,
            ):
                path_at.return_value.exists.return_value = True
                numerical_mapping_acquisition.execute(
                    store,
                    store,
                    "FUTURE27",
                    request,
                    client=client,
                    download=download,
                    context=None,
                    cache={},
                    required_sessions=None,
                )
            read.assert_called_once()
            client.fetch.assert_not_called()
            download.assert_not_called()
            policy.fail(["FUTURE27"], raised.exception, stage="lineage")
            item = universe_state.read_state(store, symbols=["FUTURE27"])["work"]["FUTURE27"]
            self.assertEqual("blocked", item["status"])
            self.assertIsNone(item["retry_after_utc"])
            self.assertEqual("recovery_required", item["outcome"]["outcome"]["outcome"])

    def test_malformed_selected_stage_blocks_without_fetch_replay_or_cooldown(self):
        from test_alpaca_numerical_mapping import fixture

        with TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            request = numerical_mapping.select(store, *fixture(), selected=True)["NOVA27"]
            need = request["numerical_mapping"]
            envelope = {"version": 1, "candidate_id": need["candidate_id"]}
            bound = {**envelope, "binding": need["binding"]}
            start, end = numerical_mapping.witness_window(need)
            witness = dict(
                symbol="NOVA27",
                adjustment="all",
                asof=request["mapping_asof"],
                start=start.isoformat(),
                end=end.isoformat(),
                rows=[],
                singleton=False,
            )
            for document in (
                envelope,
                {**bound, "binding": []},
                {**bound, "witness": {**witness, "start": "invalid-time"}},
                {**bound, "witness": {**witness, "symbol": "OTHER"}},
                {**bound, "witness": {**witness, "rows": None}},
                {**bound, "witness": witness, "acquisition": []},
                {**bound, "assessment": {}},
            ):
                policy = WorkPolicy(store, {"symbols": {}}, {}, [], "2026-10-12")
                policy.require("NOVA27", "2026-10-12", reason="selected_numerical_work")
                with (
                    self.subTest(document=document),
                    patch.object(numerical_mapping_acquisition.storage, "path_at"),
                    patch.object(numerical_mapping_acquisition, "read_document", return_value=document) as read,
                    self.assertRaises(RecoveryRequired) as raised,
                ):
                    forbidden = Mock(side_effect=AssertionError("malformed journal reached acquisition"))
                    numerical_mapping_acquisition.execute(
                        store,
                        store,
                        "NOVA27",
                        request,
                        client=SimpleNamespace(fetch=forbidden),
                        download=forbidden,
                        context=None,
                        cache={},
                        required_sessions=None,
                        grid_provider=forbidden,
                    )
                read.assert_called_once()
                self.assertIsNotNone(raised.exception.__cause__)
                policy.fail(["NOVA27"], raised.exception, stage="lineage")
                item = universe_state.read_state(store, symbols=["NOVA27"])["work"]["NOVA27"]
                self.assertEqual("blocked", item["status"])
                self.assertIsNone(item["retry_after_utc"])

    def test_broken_witness_and_changed_journal_binding_require_owning_recovery(self):
        from test_alpaca_numerical_mapping import select

        selected = select()["NOVA27"]
        window = numerical_mapping.witness_window(selected["numerical_mapping"])
        with self.assertRaises(RecoveryRequired):
            numerical_mapping_acquisition._restore_witness({}, selected, window)
        request = {"numerical_mapping": {"candidate_id": "candidate", "binding": {"selected": True}}}
        cache = {"stages": {"candidate": {"path": Mock(), "value": {"binding": {"selected": False}}}}}
        with (
            patch.object(numerical_mapping_acquisition, "read_document", side_effect=AssertionError("reread")),
            self.assertRaises(RecoveryRequired),
        ):
            numerical_mapping_acquisition.execute(
                "/tmp/synthetic",
                Path("/tmp/synthetic"),
                "FUTURE27",
                request,
                client=Mock(),
                download=Mock(),
                context=None,
                cache=cache,
                required_sessions=None,
            )

    def test_selector_failure_preserves_its_cause_without_payload_fallback(self):
        for error in (PermissionError("selector access denied"), RuntimeError("selector malformed")):
            context = SimpleNamespace(index={}, select_serving=Mock(side_effect=error))
            with (
                patch(
                    "libs.alpaca_serving_catalogue.storage.read_groups", side_effect=AssertionError("fallback")
                ),
                self.assertRaises(RecoveryRequired) as raised,
            ):
                numerical_mapping.checkpoint_sessions(context, {}, "owner", {})
            self.assertIs(error, raised.exception.__cause__)
            self.assertIn(str(error), str(raised.exception))
            context.select_serving.assert_called_once()

    def test_provider_and_transport_reasons_cross_scalar_failure_handoffs(self):
        provider_message = "bad parameter " + "private-key private-secret\n" + "x" * 2000
        # The provider body is bounded at ingress; keep this message inside that
        # bound so the complete JSON document itself remains admissible.
        payload = json.dumps({"message": provider_message[:1000]}).encode()
        opener = Mock(
            side_effect=lambda *a, **k: (_ for _ in ()).throw(
                HTTPError("https://example.invalid", 400, "Bad Request", {}, io.BytesIO(payload))
            )
        )
        client = BarsClient("private-key", "private-secret", opener=opener, limiter=Mock())
        with self.assertRaises(DownloadError) as raised:
            client._get({"symbols": ",".join(f"S{n}" for n in range(1000)), "adjustment": "all"})
        retained = AcquisitionFailure.from_error(raised.exception).to_outcome()
        self.assertEqual(400, retained["http_status"])
        self.assertIn("1000 symbols/all", retained["error"])
        self.assertIn("bad parameter", retained["error"])
        self.assertNotIn("private-key", retained["error"])
        self.assertNotIn("private-secret", retained["error"])
        self.assertNotIn("\n", retained["error"])
        self.assertLess(len(retained["error"]), 650)
        opener.assert_called_once()

        opener = Mock(side_effect=URLError("DNS resolution failed for private-secret"))
        client = BarsClient("private-key", "private-secret", opener=opener, limiter=Mock(), sleep=Mock())
        with self.assertRaises(DownloadError) as raised:
            client._get({"symbols": "AAA", "adjustment": "all"})
        retained = AcquisitionFailure.from_error(raised.exception).to_outcome()
        self.assertIn("DNS resolution failed", retained["error"])
        self.assertNotIn("private-secret", retained["error"])
        self.assertEqual(6, opener.call_count)
        self.assertIsNone(retained["http_status"])

    def test_debug_trace_preserves_wrapped_http_status_and_original_exception(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "debug.jsonl"
            trace = DebugTrace(path)
            original = HTTPError("https://example.invalid", 503, "unavailable", {}, None)
            wrapped = DownloadError("bounded provider diagnostic")
            wrapped.__cause__ = original
            source = Mock(side_effect=wrapped)
            client = SimpleNamespace(request_count=0)
            try:
                with trace.activate(), self.assertRaises(DownloadError) as raised:
                    trace_http(source, lambda: "https://example.invalid/bars", client)()
            finally:
                trace.close("error", 1)
            self.assertIs(wrapped, raised.exception)
            source.assert_called_once()
            records = [json.loads(line) for line in path.read_text().splitlines()]
            details = next(
                r["details"] for r in records if r["stage"] == "http_request" and r["event"] == "exit"
            )
            self.assertEqual(503, details["http_status"])
            self.assertEqual("DownloadError", details["error_type"])
            self.assertNotIn("bounded provider diagnostic", path.read_text())
            original.close()

    def test_recovered_batch_exception_has_normal_diagnostic_and_owner_scoped_eta(self):
        calls = []

        def opener(request, *, timeout):
            params = {k: v[0] for k, v in parse_qs(urlsplit(request.full_url).query).items()}
            calls.append(params)
            roster = params["symbols"].split(",")
            if len(roster) > 1:
                raise HTTPError(
                    request.full_url, 400, "Bad Request", {}, io.BytesIO(b'{"message":"batch rejected"}')
                )
            row = {"t": "2026-10-10T04:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 2, "n": 1, "vw": 1}
            response = io.BytesIO(json.dumps({"bars": {s: [row] for s in roster}}).encode())
            response.headers = {}
            return response

        estimate, stream = UpdateEstimate(), io.StringIO()
        estimate.observe(
            "selected",
            {
                "expected_io": {
                    "http_pages_minimum": 100,
                    "http_pages_dense_history_estimate": 100,
                    "selection_metadata_http_requests_observed": 0,
                }
            },
        )
        for n in range(8):
            estimate.request({"started": n, "finished": n + 1, "succeeded": True})
        reporter = LifecycleReporter(PHASES, stream=stream)
        with patch("threading.Thread.start"):
            reporter.start()
        progress = progress_callback(reporter, estimate=estimate)
        client = BarsClient("private-key", "private-secret", opener=opener, limiter=Mock())
        client._get = measured_request(client._get, progress)
        progress("refresh", {"state": "started", "request_scope": True})
        needs = [
            {
                "kind": "bars",
                "why": "retained_history_acquisition",
                "symbols": [symbol],
                "asof": "2026-10-12",
                "adjustments": list(ADJUSTMENTS),
                "ranges": [{"start": "2026-10-10T04:00:00Z", "end_exclusive": "2026-10-11T04:00:00Z"}],
            }
            for symbol in ("AAA", "BBB")
        ]
        states, _ = acquire_adjustments(needs, client, progress=progress)
        self.assertEqual("unknown", estimate.context()["eta_status"])
        for state in states:
            state.finish(lambda name: client.fetch(state.symbol, name, state.start, state.end, asof=state.asof))
        self.assertTrue(all(s.completed is not None for s in states))
        self.assertEqual(5, len(calls))
        self.assertEqual("unknown", estimate.context()["eta_status"])
        self.assertEqual("request_failed", estimate.context()["eta_reason"])
        progress("unrelated", {"state": "started"})
        progress("unrelated", {"state": "completed", "failed": 0})
        self.assertEqual("unknown", estimate.context()["eta_status"])
        progress("refresh", {"state": "completed", "failed": 0})
        self.assertEqual("conditional", estimate.context()["eta_status"])
        self.assertEqual(0, estimate.context()["eta_unresolved_request_failures"])
        self.assertEqual(1, estimate.context()["eta_request_failures"])
        reporter.finish("success", 0)
        records = [json.loads(line) for line in stream.getvalue().splitlines()]
        failures = [
            json.loads(r["message"])["pricing_request_failure"]
            for r in records
            if r.get("schema") == "alpaca.lifecycle.diagnostic" and "pricing_request_failure" in r["message"]
        ]
        self.assertEqual(1, len(failures))
        self.assertEqual(400, failures[0]["http_status"])
        self.assertEqual("AAA,BBB", failures[0]["request"]["symbols"])
        self.assertIn("batch rejected", failures[0]["error"])
        self.assertNotIn("private-secret", stream.getvalue())

    def test_failed_scope_and_unrelated_success_cannot_clear_unfinished_failure(self):
        estimate = UpdateEstimate(clock=lambda: 0)
        estimate.observe("owner", {"state": "started", "request_scope": True})
        estimate.request({"started": 0, "finished": 1, "succeeded": False})
        estimate.observe("owner", {"state": "completed", "failed": 1})
        estimate.observe("peer", {"state": "started", "request_scope": True})
        for n in range(10):
            estimate.request({"started": n + 1, "finished": n + 2, "succeeded": True})
        estimate.observe("peer", {"state": "completed", "failed": 0})
        self.assertEqual("unknown", estimate.context()["eta_status"])
        estimate.observe("completion", {"complete": True, "pending_work": 0})
        self.assertEqual("complete", estimate.context()["eta_status"])
        self.assertEqual(0, estimate.context()["eta_unresolved_request_failures"])

    def test_transport_failure_updates_progress_without_inventing_http_status(self):
        estimate = UpdateEstimate()
        reporter = Mock()
        progress = progress_callback(reporter, estimate=estimate)
        progress("owner", {"state": "started", "request_scope": True})
        error = DownloadError("transport unavailable")
        source = Mock(side_effect=error)
        with self.assertRaises(DownloadError) as raised:
            measured_request(source, progress)({"symbols": "AAA", "adjustment": "all"})
        self.assertIs(error, raised.exception)
        context = reporter.update.call_args.kwargs["context"]
        self.assertEqual("unknown", context["eta_status"])
        self.assertEqual("request_failed", context["eta_reason"])
        self.assertNotIn("eta_last_http_status", context)
        reporter.diagnostic.assert_called_once()
        source.assert_called_once()

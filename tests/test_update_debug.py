"""Trace coverage and terminal behavior before using a long production run."""

import io
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from libs import alpaca_cli
from libs.common.debug_trace import DebugTrace, in_context, progress_callback, trace_http, traced
from libs.common.store_lock import StoreBusyError


class DebugTraceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "debug.jsonl"

    def records(self):
        lines = self.path.read_text().splitlines()
        self.assertTrue(all(len(line.encode()) < 8192 for line in lines))
        records = [json.loads(line) for line in lines]
        self.assertEqual([r["seq"] for r in records], list(range(1, len(records) + 1)))
        enters = {r["span"]: r for r in records if r["event"] == "enter"}
        exits = {r["span"]: r for r in records if r["event"] == "exit"}
        self.assertEqual(enters.keys(), exits.keys())
        for key, record in exits.items():
            self.assertEqual(record["stage"], enters[key]["stage"])
            self.assertGreaterEqual(record["duration_seconds"], 0)
            if record["parent"]:
                self.assertIn(record["parent"], enters)
        return records

    def test_http_targets_are_complete_debug_only_and_do_not_log_responses(self):
        class Client:
            request_count = 0

        client = Client()
        target = "https://example.test/bars?symbols=" + ",".join(f"S{i}" for i in range(3000))
        calls = []

        def request(url):
            calls.append(url)
            client.request_count += 1
            return {"response_secret": "NEVER_LOG_RESPONSE_DATA"}

        wrapped = trace_http(request, lambda url: url, client)
        with patch("libs.common.debug_trace.urlsplit", side_effect=AssertionError("debug work when disabled")):
            self.assertEqual(wrapped(target), {"response_secret": "NEVER_LOG_RESPONSE_DATA"})
        self.assertFalse(self.path.exists())
        trace = DebugTrace(self.path)
        try:
            with trace.activate():
                wrapped(target)
        finally:
            trace.close("success", 0)
        records = self.records()
        from urllib.parse import parse_qs, urlsplit

        recovered = "".join(r["url_part"] for r in records if r["event"] == "request")
        self.assertEqual(parse_qs(urlsplit(recovered).query), parse_qs(urlsplit(target).query))
        self.assertEqual(calls, [target, target])
        self.assertNotIn("NEVER_LOG_RESPONSE_DATA", self.path.read_text())

    def test_worker_spans_close_on_success_failure_and_interruption(self):
        @traced("worker")
        def worker(symbol):
            if symbol == "fail":
                raise ValueError("secret must not be logged")
            if symbol == "interrupt":
                raise KeyboardInterrupt
            return {"status": "unchanged", "pricing_rows_processed": 0}

        trace = DebugTrace(self.path)
        try:
            with trace.activate(), ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(in_context(worker), name) for name in ("ok", "fail", "interrupt")]
                self.assertEqual(futures[0].result()["status"], "unchanged")
                with self.assertRaises(ValueError):
                    futures[1].result()
                with self.assertRaises(KeyboardInterrupt):
                    futures[2].result()
        finally:
            trace.close("success", 0)
        records = self.records()
        self.assertEqual(
            {r["details"]["status"] for r in records if r["event"] == "exit"}, {"unchanged", "error", "aborted"}
        )
        self.assertNotIn("secret", self.path.read_text())

    def test_bounded_progress_and_failed_trace_do_not_change_work(self):
        warnings = []
        trace = DebugTrace(self.path, warning=warnings.append)
        try:
            with trace.activate():
                callback = progress_callback(lambda stage, values: 42)
                self.assertEqual(callback("selected", {"symbols": ["A"] * 100000}), 42)
                with patch.object(trace.stream, "write", side_effect=OSError("disk full")):
                    self.assertEqual(callback("failed_sink", {}), 42)
                    self.assertEqual(callback("failed_sink_again", {}), 42)
        finally:
            trace.close("success", 0)
        self.assertEqual(len(warnings), 1)
        records = self.records()
        self.assertEqual(records[1]["details"]["symbols"]["count"], 100000)
        self.assertEqual(len(records[1]["details"]["symbols"]["sample"]), 8)

    def test_cli_terminal_paths_and_exclusive_destination(self):
        for value, code, status in (
            ({"complete": True}, 0, "success"),
            ({"complete": False, "blockers": ["pending"]}, 1, "error"),
            (ValueError("invalid"), 1, "error"),
            (OSError("unreadable"), 1, "error"),
            (StoreBusyError("busy"), 3, "error"),
            (KeyboardInterrupt(), 130, "aborted"),
            (SystemExit(2), 2, "aborted"),
        ):
            with self.subTest(value=value), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.path = self.path.with_name(f"trace-{code}-{type(value).__name__}.jsonl")
                with patch.object(
                    alpaca_cli,
                    "dispatch",
                    **({"side_effect": value} if isinstance(value, BaseException) else {"return_value": value}),
                ):
                    arguments = ["update", "--plan", "--debug", str(self.path)]
                    if isinstance(value, (KeyboardInterrupt, SystemExit)):
                        with self.assertRaises(type(value)):
                            alpaca_cli.main(arguments)
                    else:
                        self.assertEqual(alpaca_cli.main(arguments), code)
                records = self.records()
                self.assertEqual(records[-1]["status"], status)
                self.assertEqual(records[-1]["exit_code"], code)
                before = self.path.read_bytes()
                with patch.object(alpaca_cli, "dispatch", side_effect=AssertionError("work before log")):
                    self.assertEqual(alpaca_cli.main(arguments), 1)
                self.assertEqual(before, self.path.read_bytes())

    def test_synthetic_daily_update_covers_nested_work_and_append_workers(self):
        from test_alpaca_universe_update import UniverseUpdateTests, bar

        case = UniverseUpdateTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        with (
            patch.object(alpaca_cli, "clients", return_value=(case.pricing, case.identity)),
            patch.object(alpaca_cli, "DEFAULT_REGISTRY", case.registry),
            patch.object(alpaca_cli, "commit_registry"),
            patch("libs.alpaca_lineage_build.universe_update.DEFAULT_REGISTRY", case.registry),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            # run_update's default registry is bound at definition; the adapter
            # below supplies this fixture's registry explicitly.
            from libs.alpaca_lineage_build import universe_update

            run = universe_update.run_update

            def fixture_update(*args, **kwargs):
                return run(*args, registry=case.registry, **kwargs)

            with patch.object(universe_update, "run_update", side_effect=fixture_update):
                self.assertEqual(
                    alpaca_cli.main(
                        [
                            "update",
                            "--store",
                            str(case.store),
                            "--through",
                            "2026-10-16",
                            "--debug",
                            str(self.path),
                        ]
                    ),
                    0,
                )
                stages = {r["stage"] for r in self.records() if r["event"] == "enter"}
                self.assertTrue(
                    {
                        "command",
                        "store_operation",
                        "dispatch",
                        "update",
                        "recovery",
                        "boundaries",
                        "catalogue",
                        "selection",
                        "work_accept",
                        "registry_register",
                        "pricing_acquisition",
                        "lineage",
                        "lineage_group",
                        "correlation",
                        "lineage_publish",
                        "maintenance",
                        "live_check_and_correction",
                        "completion_boundary",
                        "serving",
                        "serving_evaluation",
                        "serving_publish",
                        "result_output",
                    }
                    <= stages,
                    stages,
                )
                self.path = self.path.with_name("append.jsonl")
                case.live["AAA"].append(bar("2026-10-19"))
                case.live["NEW"].append(bar("2026-10-19"))
                self.assertEqual(
                    alpaca_cli.main(
                        [
                            "update",
                            "--store",
                            str(case.store),
                            "--through",
                            "2026-10-19",
                            "--debug",
                            str(self.path),
                        ]
                    ),
                    0,
                )
                records = self.records()
                publications = [
                    r for r in records if r["stage"] == "pricing_publication" and r["event"] == "enter"
                ]
                self.assertTrue(publications)
                self.assertTrue(
                    all(r["thread"].startswith("pricing-append") and r["parent"] for r in publications)
                )
                self.assertTrue(any(r["stage"] == "lineage_reuse_gate" for r in records))
                # Tracing itself must preserve the completed-boundary I/O rule.
                # Repeating the same cutoff may check live prices, but must not
                # discover paths or reopen accepted lineage receipt bodies.
                from libs.alpaca_lineage_build import storage

                self.path = self.path.with_name("covered.jsonl")
                with (
                    patch.object(storage, "read_receipt", side_effect=AssertionError("receipt reopened")),
                    patch.object(Path, "iterdir", side_effect=AssertionError("store crawl")),
                    patch.object(storage, "check_store", wraps=storage.check_store) as acceptance,
                ):
                    self.assertEqual(
                        alpaca_cli.main(
                            [
                                "update",
                                "--store",
                                str(case.store),
                                "--through",
                                "2026-10-19",
                                "--debug",
                                str(self.path),
                            ]
                        ),
                        0,
                    )
                self.assertEqual(acceptance.call_count, 1)
                self.assertEqual(self.records()[-1]["status"], "success")


if __name__ == "__main__":
    unittest.main()

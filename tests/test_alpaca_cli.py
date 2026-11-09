"""Canonical dispatch and inclusive dates for the standalone commands."""

import argparse
import io
import json
import subprocess
import sys
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_cli import _store_operation_mode, build_parser, dispatch, end_exclusive, main
from libs.alpaca_heartbeat import LifecycleReporter, run_cli
from libs.common.store_lock import StoreBusyError


class CanonicalCliTests(unittest.TestCase):
    def test_store_operation_modes_cover_every_writer_and_csv_export(self):
        cases = [
            ("update", None, False, "exclusive"),
            ("update", None, True, None),
            ("pricing", "acquire", False, "exclusive"),
            ("pricing", "refresh", False, "exclusive"),
            ("pricing", "check", False, None),
            ("lineage", "update", False, "exclusive"),
            ("lineage", "refresh", False, "exclusive"),
            ("lineage", "migrate-index", False, "exclusive"),
            ("lineage", "migrate-policy", False, "exclusive"),
            ("lineage", "migrate-assignments", False, "exclusive"),
            ("lineage", "inspect", False, None),
            ("universe", "reconcile", False, "exclusive"),
            ("universe", "rebuild", False, "exclusive"),
            ("summary", "rebuild", False, "exclusive"),
            ("catalogue", "bootstrap", False, "exclusive"),
            ("catalogue", "bootstrap", True, None),
            ("catalogue", "rebuild", False, "exclusive"),
            ("catalogue", "update", False, "exclusive"),
            ("catalogue", "status", False, None),
            ("export", "csv", False, "shared"),
            ("export", "csv", True, None),
            ("export", "bloodhound", False, None),
            ("read", None, False, None),
            ("status", None, False, None),
        ]
        for command, operation, plan, expected in cases:
            with self.subTest(command=command, operation=operation, plan=plan):
                args = SimpleNamespace(command=command, operation=operation, plan=plan)
                self.assertEqual(_store_operation_mode(args), expected)

    def test_writer_reports_waiting_before_work_and_restores_operation_after_acquisition(self):
        errors, output = io.StringIO(), io.StringIO()
        reporters, sequence = [], []
        clock = [1000.0]
        message = "Waiting for the store lock; press Ctrl-C to cancel."

        def reporter(*args, **kwargs):
            value = LifecycleReporter(*args, **kwargs, clock=lambda: clock[0])
            reporters.append(value)
            return value

        @contextmanager
        def gate(store, *, exclusive, wait=False, on_wait=None):
            self.assertEqual(store, Path("/unused/test-store"))
            self.assertTrue(exclusive)
            self.assertTrue(wait)
            self.assertTrue(callable(on_wait))
            sequence.append("attempt")
            clients.assert_not_called()
            dispatched.assert_not_called()
            on_wait()
            sequence.append("waiting")
            records = [json.loads(line) for line in errors.getvalue().splitlines()]
            self.assertEqual(records[-1]["schema"], "alpaca.lifecycle.diagnostic")
            self.assertEqual(records[-1]["phase"], "WAIT")
            self.assertEqual(records[-1]["message"], message)
            # Sample the registered waiting phase without a real clock delay.
            clock[0] += 10
            reporters[0]._emit_due()
            heartbeat = json.loads(errors.getvalue().splitlines()[-1])
            self.assertEqual(heartbeat["phase"], "WAIT")
            self.assertEqual(heartbeat["context"]["estimated_remaining_seconds"], "TBD")
            clients.assert_not_called()
            dispatched.assert_not_called()
            sequence.append("acquired")
            yield
            sequence.append("released")

        def work(args):
            self.assertEqual(sequence, ["attempt", "waiting", "acquired"])
            self.assertEqual(args._lifecycle._held["phase"], "UPDATE")
            sequence.append("dispatch")
            return {"complete": True}

        with (
            patch("libs.alpaca_cli.LifecycleReporter", side_effect=reporter),
            patch("threading.Thread.start"),
            patch("libs.alpaca_cli.operation_lock", side_effect=gate) as locked,
            patch(
                "libs.alpaca_cli.clients", side_effect=AssertionError("clients before acquired gate")
            ) as clients,
            patch("libs.alpaca_cli.dispatch", side_effect=work) as dispatched,
            patch("libs.alpaca_cli.commit_registry"),
            redirect_stderr(errors),
            redirect_stdout(output),
        ):
            self.assertEqual(main(["update", "--store", "/unused/test-store"]), 0)
        locked.assert_called_once()
        clients.assert_not_called()
        dispatched.assert_called_once()
        self.assertEqual(sequence, ["attempt", "waiting", "acquired", "dispatch", "released"])
        records = [json.loads(line) for line in errors.getvalue().splitlines()]
        self.assertEqual(sum(item["schema"] == "alpaca.lifecycle.diagnostic" for item in records), 1)
        self.assertEqual(records[-1]["last_phase"], "UPDATE")
        self.assertEqual(records[-1]["exit_code"], 0)

    def test_cancelled_wait_reports_terminal_130_without_dispatch_or_clients(self):
        errors = io.StringIO()

        @contextmanager
        def cancelled(_store, *, exclusive, wait=False, on_wait=None):
            self.assertTrue(exclusive)
            self.assertTrue(wait)
            on_wait()
            clients.assert_not_called()
            dispatched.assert_not_called()
            raise KeyboardInterrupt
            yield  # pragma: no cover - make the cancelled gate a context manager.

        with (
            patch("threading.Thread.start"),
            patch("libs.alpaca_cli.operation_lock", side_effect=cancelled),
            patch(
                "libs.alpaca_cli.clients", side_effect=AssertionError("cancelled waiter created clients")
            ) as clients,
            patch(
                "libs.alpaca_cli.dispatch", side_effect=AssertionError("cancelled waiter dispatched")
            ) as dispatched,
            patch("libs.alpaca_cli.commit_registry", side_effect=AssertionError("cancelled waiter committed")),
            redirect_stderr(errors),
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(run_cli(lambda: main(["update", "--store", "/unused/test-store"])), 130)
        clients.assert_not_called()
        dispatched.assert_not_called()
        records = [json.loads(line) for line in errors.getvalue().splitlines()]
        self.assertEqual(records[-1]["phase"], "FINAL_EXIT")
        self.assertEqual(records[-1]["last_phase"], "WAIT")
        self.assertEqual(records[-1]["status"], "aborted")
        self.assertEqual(records[-1]["exit_code"], 130)

    def test_csv_store_gate_still_fails_fast_without_wait_callback(self):
        errors = io.StringIO()

        @contextmanager
        def busy(_store, *, exclusive, wait=False, on_wait=None):
            self.assertFalse(exclusive)
            self.assertFalse(wait)
            self.assertIsNone(on_wait)
            raise StoreBusyError("store is busy with an export or update; retry later")
            yield  # pragma: no cover - make the busy gate a context manager.

        with (
            patch("threading.Thread.start"),
            patch("libs.alpaca_cli.operation_lock", side_effect=busy),
            patch(
                "libs.alpaca_cli.dispatch", side_effect=AssertionError("busy exporter dispatched")
            ) as dispatched,
            redirect_stderr(errors),
        ):
            self.assertEqual(main(["export", "csv", "--store", "/unused/test-store"]), 3)
        dispatched.assert_not_called()
        records = [json.loads(line) for line in errors.getvalue().splitlines()]
        self.assertFalse(any(item["phase"] == "WAIT" for item in records))
        self.assertEqual(records[-1]["exit_code"], 3)

    def test_policy_migration_dispatches_before_dates_or_clients(self):
        store = Path("/data2/alpaca/test-store")
        args = build_parser().parse_args(["lineage", "migrate-policy", "--store", str(store)])
        expected = {"complete": True, "failed": 0}
        with (
            patch("libs.alpaca_cli._pricing_end", side_effect=AssertionError("resolved live cutoff")),
            patch("libs.alpaca_cli.clients", side_effect=AssertionError("constructed provider clients")),
            patch(
                "libs.alpaca_lineage_build.migration.migrate_correlation_policy",
                return_value=expected,
            ) as migrate,
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(expected, dispatch(args))
        self.assertEqual(store, migrate.call_args.args[0])

    def test_assignment_migration_requires_scope_and_dispatches_without_live_clients(self):
        parser = build_parser()
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["lineage", "migrate-assignments"])
        for scope, selected, whole in (
            (["--symbol", "AAA", "--symbol", "BBB"], ["AAA", "BBB"], False),
            (["--all-published"], None, True),
        ):
            with (
                self.subTest(scope=scope),
                patch("libs.alpaca_cli._pricing_end", side_effect=AssertionError("resolved live cutoff")),
                patch("libs.alpaca_cli.clients", side_effect=AssertionError("constructed provider clients")),
                patch("libs.alpaca_lineage_build.assignment_migration.migrate", return_value={}) as migrate,
                redirect_stderr(io.StringIO()),
            ):
                dispatch(parser.parse_args(["lineage", "migrate-assignments", *scope]))
                self.assertEqual(migrate.call_args.args[1], selected)
                self.assertEqual(migrate.call_args.kwargs["all_published"], whole)
                self.assertEqual(migrate.call_args.kwargs["limit"], 128)

    def test_retired_experiment_interface_fails_with_canonical_command(self):
        result = subprocess.run(
            [sys.executable, "-B", "-m", "libs.alpaca_pricing_download.live_check_experiment", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("alpaca-cli-eod pricing check", result.stderr)

    def test_every_command_help_and_removed_abbreviations(self):
        parser = build_parser()
        paths = []

        def visit(current, prefix):
            paths.append(prefix)
            for action in current._actions:
                if isinstance(action, argparse._SubParsersAction):
                    for name, child in action.choices.items():
                        visit(child, [*prefix, name])

        visit(parser, [])
        for path in paths:
            with self.subTest(path=path), redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as error:
                parser.parse_args([*path, "--help"])
            self.assertEqual(error.exception.code, 0)
        for arguments in (
            ["legacy"],
            ["--update"],
            ["update", "--thr", "2026-10-16"],
            ["pricing", "acquire", "--all"],
        ):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                parser.parse_args(arguments)
            self.assertEqual(error.exception.code, 2)

    def test_month_end_conversion_reaches_daily_producer(self):
        self.assertEqual(end_exclusive("2026-09-30"), "2026-10-01")
        args = build_parser().parse_args(["update", "--through", "2026-09-30"])
        with (
            patch("libs.alpaca_cli.clients", return_value=(object(), object())),
            patch("libs.alpaca_lineage_build.universe_update.run_update", return_value={}) as run,
            redirect_stderr(io.StringIO()),
        ):
            dispatch(args)
        self.assertEqual(run.call_args.args[1], "2026-10-01")
        self.assertTrue(run.call_args.kwargs["coalesce_late_bars"])

    def test_exact_late_bar_override_reaches_plan_and_execution(self):
        for planning in (False, True):
            args = build_parser().parse_args(
                ["update", "--through", "2026-09-30", "--exact-late-bars", *(["--plan"] if planning else [])]
            )
            owner = "plan_update" if planning else "run_update"
            with (
                patch("libs.alpaca_cli.clients", return_value=(object(), object())),
                patch("libs.alpaca_lineage_build.universe_update." + owner, return_value={}) as run,
                redirect_stderr(io.StringIO()),
            ):
                dispatch(args)
            self.assertFalse(run.call_args.kwargs["coalesce_late_bars"])

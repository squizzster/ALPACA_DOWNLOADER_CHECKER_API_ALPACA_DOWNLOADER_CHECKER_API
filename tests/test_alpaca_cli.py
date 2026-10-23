"""Canonical dispatch and inclusive dates for the standalone commands."""

import argparse
import io
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_cli import _store_operation_mode, build_parser, dispatch, end_exclusive, main
from libs.alpaca_pricing_download.storage import initialize_store
from libs.common.store_lock import operation_lock


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

    def test_update_is_busy_before_clients_or_dispatch_when_export_holds_store(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary) / "data_pricing"
            initialize_store(store)
            errors = io.StringIO()
            with (
                operation_lock(store, exclusive=False),
                patch("libs.alpaca_cli.clients", side_effect=AssertionError("writer passed store gate")),
                redirect_stderr(errors),
            ):
                self.assertEqual(main(["update", "--store", str(store)]), 3)
            self.assertIn("store is busy with an export or update", errors.getvalue())

    def test_retired_experiment_interface_fails_with_canonical_command(self):
        result = subprocess.run(
            [sys.executable, "-B", "-m", "libs.alpaca_pricing_download.live_check_experiment", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("alpaca-eod pricing check", result.stderr)

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

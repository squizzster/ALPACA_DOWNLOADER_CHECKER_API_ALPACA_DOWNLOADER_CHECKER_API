"""Canonical dispatch and inclusive dates for the standalone commands."""

import argparse
import io
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from libs.alpaca_cli import build_parser, dispatch, end_exclusive


class CanonicalCliTests(unittest.TestCase):
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

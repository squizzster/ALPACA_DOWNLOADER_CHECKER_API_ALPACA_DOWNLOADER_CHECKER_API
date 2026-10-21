"""Canonical dispatch, inclusive dates and combined-store cutoff protection."""

import argparse
import io
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from test_alpaca_end_of_day_clean_room import _resolution
from test_alpaca_pricing_download import snapshot

from libs.alpaca_cli import build_parser, dispatch, end_exclusive
from libs.alpaca_end_of_day_v6.errors import V6Error
from libs.alpaca_end_of_day_v6.pipeline import run_v6_pipeline


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
        for arguments in (["--update"], ["update", "--thr", "2026-10-16"], ["pricing", "acquire", "--all"]):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                parser.parse_args(arguments)
            self.assertEqual(error.exception.code, 2)

    def test_month_end_conversion_reaches_combined_producer_as_utc_boundary(self):
        self.assertEqual(end_exclusive("2026-09-30"), "2026-10-01")
        args = build_parser().parse_args(
            ["legacy", "catalogue", "update", "--symbol", "AAA", "--through", "2026-09-30"]
        )
        with patch("libs.alpaca_end_of_day_v6.pipeline.run_v6_pipeline", return_value={}) as run:
            dispatch(args)
        self.assertEqual(run.call_args.kwargs["request_end_timestamp"], "2026-10-01T00:00:00Z")
        self.assertIsNone(run.call_args.kwargs["request_start_timestamp"])

    def test_combined_store_rejects_backward_empty_coverage_before_network_or_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory) / "combined"
            arguments = dict(
                store_root=store,
                environment={"ALPACA_API_KEY_ID": "key", "ALPACA_API_SECRET_KEY": "secret"},
                request_start_timestamp="2016-01-01T00:00:00Z",
                request_end_timestamp="2026-10-17T00:00:00Z",
                symbol_resolutions=[_resolution("EMPTY", "EMPTY")],
                accept_failed_validation=True,
            )
            with patch("libs.alpaca_end_of_day_v6.pipeline.request_alpaca_daily_bars", return_value=[]):
                run_v6_pipeline(["EMPTY"], **arguments)
            before = snapshot(store)
            arguments["request_end_timestamp"] = "2026-10-16T00:00:00Z"
            with patch(
                "libs.alpaca_end_of_day_v6.pipeline.request_alpaca_daily_bars",
                side_effect=AssertionError("network"),
            ):
                with self.assertRaisesRegex(V6Error, "precedes retained source coverage"):
                    run_v6_pipeline(["EMPTY"], **arguments)
            self.assertEqual(before, snapshot(store))

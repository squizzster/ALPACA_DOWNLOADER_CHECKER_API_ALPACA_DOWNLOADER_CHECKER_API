"""The CLI cannot lend researched assertions to fresh lineage inference."""

import io
import unittest
from contextlib import ExitStack, nullcontext, redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_cli import build_parser, dispatch, main


class CliAssertionRejectionTests(unittest.TestCase):
    def test_removed_option_rejects_before_files_lifecycle_store_or_provider_work(self):
        for operation in ("update", "refresh"):
            for option in (
                ["--event-supplements", "/unused/research.json"],
                ["--event-supplements=/unused/research.json"],
            ):
                with self.subTest(operation=operation, option=option), ExitStack() as guards:
                    for target in (
                        "pathlib.Path.open",
                        "pathlib.Path.read_bytes",
                        "pathlib.Path.read_text",
                        "libs.alpaca_cli.LifecycleReporter",
                        "libs.alpaca_cli.operation_lock",
                        "libs.alpaca_cli.clients",
                        "libs.alpaca_cli.dispatch",
                    ):
                        guards.enter_context(
                            patch(target, side_effect=AssertionError("forbidden work: " + target))
                        )
                    errors = guards.enter_context(redirect_stderr(io.StringIO()))
                    with self.assertRaises(SystemExit) as stopped:
                        main(["lineage", operation, "--symbol", "TEST", *option])
                    self.assertEqual(stopped.exception.code, 2)
                    self.assertIn("unrecognized arguments", errors.getvalue())

    def test_nonempty_legacy_dispatch_input_rejects_before_dates_files_or_clients(self):
        for value in (Path("/unused/research.json"), "research.json", [{"kind": "authored_assertion"}]):
            with self.subTest(value=value), ExitStack() as guards:
                for target in (
                    "pathlib.Path.open",
                    "pathlib.Path.read_bytes",
                    "pathlib.Path.read_text",
                    "libs.alpaca_cli._pricing_end",
                    "libs.alpaca_cli.operation_lock",
                    "libs.alpaca_cli.clients",
                ):
                    guards.enter_context(patch(target, side_effect=AssertionError("forbidden work: " + target)))
                with self.assertRaisesRegex(ValueError, "research fact injection is inadmissible"):
                    dispatch(SimpleNamespace(event_supplements=value))

    def test_provider_only_dispatch_does_not_forward_a_supplement_capability(self):
        args = build_parser().parse_args(
            ["lineage", "update", "--store", "/unused/test-store", "--symbol", "TEST"]
        )
        self.assertFalse(hasattr(args, "event_supplements"))
        # An empty placeholder from an older already parsed command grants no
        # capability and is not forwarded to the inference owner.
        args.event_supplements = ()
        policy = SimpleNamespace(pending_work=())
        with (
            patch("libs.alpaca_cli._pricing_end", return_value="2026-11-12"),
            patch("libs.alpaca_cli.clients", return_value=(object(), object())),
            patch(
                "libs.alpaca_lineage_build.registration.standalone_registration",
                return_value=nullcontext((object(), policy)),
            ),
            patch(
                "libs.alpaca_lineage_build.pipeline.run_lineage", return_value={"failed": 0, "results": []}
            ) as run,
            patch("libs.alpaca_serving_catalogue.storage.path") as catalogue,
        ):
            catalogue.return_value.exists.return_value = False
            result = dispatch(args)
        self.assertTrue(result["complete"])
        run.assert_called_once()
        self.assertNotIn("event_supplements", run.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()

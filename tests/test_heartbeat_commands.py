"""Reporting stays outside command results, exit behavior and storage work."""

import argparse
import io
import json
import unittest
from contextlib import nullcontext, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from libs import alpaca_cli
from libs.alpaca_cli.command_progress import progress_callback
from libs.alpaca_heartbeat import (
    DIAGNOSTIC_SCHEMA,
    ERROR_SCHEMA,
    VERSION,
    LifecycleReporter,
    Phase,
    parse_record,
    run_cli,
)
from libs.alpaca_lineage_policy_lab import command_line as lab


class HeartbeatCommandTests(unittest.TestCase):
    def records(self, stream):
        return [parse_record(line) for line in stream.getvalue().splitlines(keepends=True)]

    def leaves(self, parser, path=()):
        children = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
        if children:
            for name, child in children[0].choices.items():
                yield from self.leaves(child, (*path, name))
        else:
            arguments = list(path)
            for action in parser._actions:
                if action.required:
                    arguments.extend([action.option_strings[0], "AAA"] if action.option_strings else ["AAA"])
            for group in parser._mutually_exclusive_groups:
                if group.required:
                    arguments.extend([group._group_actions[0].option_strings[0], "AAA"])
            yield arguments

    def test_every_canonical_leaf_has_one_lifecycle_and_identical_stdout(self):
        # Patch only the operation boundary: parsing, lifecycle, serialization and
        # exit selection remain real, without opening a runtime store.
        result = {"processing_complete": True, "complete": True, "result": "unchanged"}
        for arguments in self.leaves(alpaca_cli.build_parser()):
            with self.subTest(arguments=arguments):
                output, errors = io.StringIO(), io.StringIO()
                with (
                    patch.object(alpaca_cli, "_store_operation", return_value=nullcontext()),
                    patch.object(alpaca_cli, "dispatch", return_value=result) as dispatch,
                    patch.object(alpaca_cli, "commit_registry"),
                    redirect_stdout(output),
                    redirect_stderr(errors),
                ):
                    code = alpaca_cli.main(arguments)
                self.assertEqual(code, 0)
                self.assertEqual(output.getvalue(), json.dumps(result, separators=(",", ":")) + "\n")
                dispatch.assert_called_once()
                records = self.records(errors)
                self.assertEqual([r["phase"] for r in records], ["INIT", "FINAL_EXIT"])
                self.assertEqual(records[-1]["status"], "success")
                self.assertEqual(records[0]["next_heart_beat_in_seconds"], 10)

    def test_every_parser_rejects_cadence_overrides_before_work(self):
        for module in (alpaca_cli, lab):
            for arguments in self.leaves(module.build_parser()):
                for position in range(len(arguments) + 1):
                    overridden = arguments[:position] + ["--heartbeat-seconds", "60"] + arguments[position:]
                    with (
                        self.subTest(module=module.__name__, arguments=overridden),
                        redirect_stderr(io.StringIO()) as errors,
                        patch.object(
                            module, "LifecycleReporter", side_effect=AssertionError("started lifecycle")
                        ),
                        self.assertRaises(SystemExit) as raised,
                    ):
                        module.main(overridden)
                    self.assertEqual(raised.exception.code, 2)
                    self.assertNotIn("alpaca.lifecycle", errors.getvalue())

    def test_existing_exit_decisions_and_exception_propagation(self):
        for arguments, result, expected in (
            (["update", "--plan"], {"failed": 2}, 1),
            (["status"], {"processing_complete": False}, 1),
            (["status"], 3, 3),
        ):
            output, errors = io.StringIO(), io.StringIO()
            with (
                patch.object(alpaca_cli, "dispatch", return_value=result),
                redirect_stdout(output),
                redirect_stderr(errors),
            ):
                self.assertEqual(alpaca_cli.main(arguments), expected)
            self.assertEqual(self.records(errors)[-1]["exit_code"], expected)
        for error, status, code in (
            (KeyboardInterrupt(), "aborted", 130),
            (SystemExit(7), "aborted", 7),
            (TypeError("unexpected operation bug"), "error", 1),
        ):
            errors = io.StringIO()
            with (
                patch.object(alpaca_cli, "dispatch", side_effect=error),
                redirect_stderr(errors),
                self.assertRaises(type(error)),
            ):
                alpaca_cli.main(["status"])
            records = self.records(errors)
            self.assertEqual(records[-1]["status"], status)
            self.assertEqual(records[-1]["exit_code"], code)

    def test_broken_stderr_does_not_change_success_or_stdout(self):
        class BrokenStream:
            def write(self, _value):
                raise OSError("closed diagnostic pipe")

        output = io.StringIO()
        with (
            patch.object(alpaca_cli, "dispatch", return_value={"processing_complete": True}),
            redirect_stderr(BrokenStream()),
            redirect_stdout(output),
        ):
            self.assertEqual(alpaca_cli.main(["status"]), 0)
        self.assertEqual(output.getvalue(), '{"processing_complete":true}\n')

    def test_progress_only_samples_existing_scalar_observations(self):
        from libs.alpaca_cli.command_progress import PHASES

        now = [0.0]
        errors = io.StringIO()
        reporter = LifecycleReporter(PHASES, stream=errors, clock=lambda: now[0])
        with patch("threading.Thread.start"):
            reporter.start()
        progress = progress_callback(reporter)
        progress("lineage_publication", {"done": 2, "total": 5, "symbol": "AAA", "symbols": ["AAA"]})
        self.assertEqual(len(self.records(errors)), 1)
        now[0] = 10.0
        reporter._emit_due()
        record = self.records(errors)[-1]
        self.assertEqual(record["phase"], "LINEAGE")
        self.assertEqual((record["current"], record["total"]), (2, 5))
        self.assertEqual(record["context"]["symbol"], "AAA")
        self.assertNotIn("symbols", record["context"])
        self.assertEqual(record["context"]["estimated_remaining_seconds"], "TBD")
        reporter.finish("success", 0)

    def test_diagnostics_are_bounded_immediate_and_do_not_replace_work(self):
        errors = io.StringIO()
        reporter = LifecycleReporter({"WORK": Phase("Working.")}, stream=errors)
        with patch("threading.Thread.start"):
            reporter.start()
        reporter.update("WORK")
        self.assertTrue(reporter.diagnostic("\N{HORSE FACE}" * 10000, level="warning"))
        records = self.records(errors)
        self.assertEqual(records[-1]["schema"], DIAGNOSTIC_SCHEMA)
        self.assertEqual(records[-1]["phase"], "WORK")
        self.assertTrue(records[-1]["truncated"])
        reporter.finish("success", 0)
        self.assertFalse(reporter.diagnostic("too late"))
        self.assertEqual(self.records(errors)[-1]["last_phase"], "WORK")

    def test_lab_preserves_reports_parity_and_timeout_exit_codes(self):
        for command, report, code in (
            ("compare", {"comparison": {"parity": True}}, 0),
            ("compare", {"comparison": {"parity": False}}, 1),
            ("audit", {"summary": {"parity": True}}, 0),
            ("audit", {"summary": {"parity": False}}, 1),
            ("audit", {"status": "timed_out"}, 124),
        ):
            output, errors = io.StringIO(), io.StringIO()
            arguments = [command, "--policy", "unused.yaml"]
            if command == "compare":
                arguments.extend(["--symbol", "AAA"])
            with (
                patch.object(lab, "load_policy", return_value=object()),
                patch.object(lab, command, return_value=report),
                redirect_stdout(output),
                redirect_stderr(errors),
            ):
                self.assertEqual(lab.main(arguments), code)
            self.assertEqual(output.getvalue(), json.dumps(report, sort_keys=True, indent=2) + "\n")
            self.assertEqual(self.records(errors)[-1]["exit_code"], code)
        errors = io.StringIO()
        with (
            patch.object(lab, "load_policy", side_effect=ValueError("bad policy")),
            redirect_stderr(errors),
        ):
            self.assertEqual(lab.main(["audit", "--policy", "unused.yaml"]), 2)
        self.assertEqual(self.records(errors)[1]["schema"], ERROR_SCHEMA)
        self.assertEqual(self.records(errors)[-1]["exit_code"], 2)

    def test_consumer_rejects_unsupported_and_oversized_input(self):
        for line in ("[]", "invalid", '{"schema":"unknown"}', '{"schema":[]}', " " * 16385):
            with self.subTest(line=line[:40]), self.assertRaises(ValueError):
                parse_record(line)
        entries = json.loads(Path("resources/alpaca_heartbeat_change_log.json").read_text())
        self.assertEqual(len(entries), VERSION)
        self.assertTrue(all("version" not in entry and entry["modifications"] for entry in entries))

    def test_parquet_adapter_samples_progress_and_preserves_immediate_rejections(self):
        from libs.alpaca_bloodhound_export import command_line as parquet

        result = {"output": "unused.parquet", "rows": 2, "identities": 1, "diagnostic_errors": []}

        def export(_reader, _output, **options):
            options["progress"]("collecting accepted Arrow data in RAM")
            options["progress"]("rejected source=BAD public_id=one: conflicting observations")
            options["progress"]("processed 1/1 source locators; converted 1 identities")
            return result

        output, errors = io.StringIO(), io.StringIO()
        with (
            patch.object(parquet, "StandaloneReader") as reader,
            patch.object(parquet, "export", side_effect=export),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            reader.return_value.cache_info.return_value = {"maximum_bytes": 0}
            self.assertEqual(alpaca_cli.main(["export", "bloodhound", "--output", "unused.parquet"]), 0)
        self.assertEqual(json.loads(output.getvalue()), result)
        records = self.records(errors)
        warnings = [record for record in records if record.get("level") == "warning"]
        self.assertEqual(len(warnings), 1)
        self.assertIn("conflicting observations", warnings[0]["message"])
        self.assertEqual(records[-1]["last_phase"], "EXPORT")

    def test_executable_adapter_keeps_interpreter_codes_without_plain_tracebacks(self):
        for error, expected in ((TypeError("bug"), 1), (KeyboardInterrupt(), 130), (SystemExit(7), 7)):
            with patch.object(alpaca_cli, "dispatch", side_effect=error), redirect_stderr(io.StringIO()):
                self.assertEqual(run_cli(lambda: alpaca_cli.main(["status"])), expected)

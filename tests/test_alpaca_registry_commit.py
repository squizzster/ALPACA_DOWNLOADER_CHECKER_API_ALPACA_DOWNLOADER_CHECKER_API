"""Automatic registry commits use real isolated Git repositories, never the working checkout."""

import io
import subprocess
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from libs import alpaca_cli
from libs.alpaca_cli import registry_commit
from libs.alpaca_heartbeat import LifecycleReporter, parse_record


class RegistryCommitTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=alpaca_cli.REPOSITORY / ".git")
        self.addCleanup(temporary.cleanup)
        self.repository = Path(temporary.name)
        self.registry = self.repository / "resources/ticker.csv"
        self.registry.parent.mkdir()
        self.registry.write_text("AAA\n")
        changelog = alpaca_cli.REPOSITORY / "resources/alpaca_cli_change_log.json"
        self.registry.with_name(changelog.name).write_bytes(changelog.read_bytes())
        self.other = self.repository / "other.txt"
        self.other.write_text("original\n")
        self.git("init", "-q")
        self.git("config", "user.name", "Registry Test")
        self.git("config", "user.email", "registry-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", str(self.repository / ".git/hooks"))
        self.git("add", ".")
        self.git("commit", "-qm", "Initial fixture")
        self.initial = self.git("rev-parse", "HEAD")
        self.enterContext(patch.object(alpaca_cli, "REPOSITORY", self.repository))
        self.enterContext(patch.object(alpaca_cli, "DEFAULT_REGISTRY", self.registry))

    def git(self, *arguments):
        return subprocess.check_output(
            ["git", "-C", str(self.repository), *arguments], stderr=subprocess.STDOUT, text=True
        ).strip()

    def invoke(self, arguments=None, *, result=None, error=None):
        output, errors = io.StringIO(), io.StringIO()
        self.operation_held = False

        @contextmanager
        def operation(_args):
            self.operation_held = True
            try:
                yield
            finally:
                self.operation_held = False

        actual_commit = registry_commit.commit_registry

        def commit(*args):
            self.assertFalse(self.operation_held, "Git ran while the data operation lock was held")
            return actual_commit(*args)

        with (
            patch.object(alpaca_cli, "_store_operation", side_effect=operation),
            patch.object(
                alpaca_cli,
                "dispatch",
                return_value=result if result is not None else {"complete": True},
                side_effect=error,
            ),
            patch.object(alpaca_cli, "commit_registry", side_effect=commit) as automatic,
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            code = alpaca_cli.main(arguments or ["update"])
        records = [parse_record(line) for line in errors.getvalue().splitlines(keepends=True)]
        return code, output.getvalue(), records, automatic.call_count

    def test_success_commits_only_registry_and_preserves_other_work_and_stdout(self):
        # Preserve an unrelated staged edit and a later unstaged edit to it.
        self.other.write_text("staged\n")
        self.git("add", "other.txt")
        self.other.write_text("unstaged\n")
        # Commit the current registry, including changes after it was staged.
        self.registry.write_text("AAA,BBB\n")
        self.git("add", "resources/ticker.csv")
        self.registry.write_text("AAA,BBB,CCC\n")
        code, output, records, calls = self.invoke()
        self.assertEqual((code, calls), (0, 1))
        self.assertEqual(output, '{"complete":true}\n')
        self.assertNotEqual(self.git("rev-parse", "HEAD"), self.initial)
        self.assertEqual(
            self.git("diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"), "resources/ticker.csv"
        )
        self.assertEqual(self.git("show", "HEAD:resources/ticker.csv"), "AAA,BBB,CCC")
        self.assertEqual(self.git("show", "HEAD:other.txt"), "original")
        self.assertEqual(self.git("show", ":other.txt"), "staged")
        self.assertEqual(self.other.read_text(), "unstaged\n")
        self.assertEqual(self.git("diff", "HEAD", "--", "resources/ticker.csv"), "")
        diagnostics = [r for r in records if r["schema"] == "alpaca.lifecycle.diagnostic"]
        self.assertTrue(all(r["phase"] == "COMMIT" for r in diagnostics))
        self.assertIn(self.git("rev-parse", "HEAD"), diagnostics[-1]["message"])
        self.assertEqual(records[-1]["last_phase"], "COMMIT")
        self.assertEqual(records[-1]["status"], "success")
        self.assertEqual(records[-1]["exit_code"], 0)

    def test_unchanged_registry_makes_no_empty_commit(self):
        code, output, records, _ = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(output, '{"complete":true}\n')
        self.assertEqual(self.git("rev-parse", "HEAD"), self.initial)
        self.assertIn("unchanged", records[-2]["message"])
        self.assertEqual(records[-1]["status"], "success")

    def test_git_hook_failure_warns_in_protocol_and_keeps_successful_data_result(self):
        self.registry.write_text("AAA,BBB\n")
        hook = self.repository / ".git/hooks/pre-commit"
        hook.write_text("#!/bin/sh\necho 'fixture rejects commit' >&2\nexit 1\n")
        hook.chmod(0o755)
        code, output, records, _ = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(output, '{"complete":true}\n')
        self.assertEqual(self.git("rev-parse", "HEAD"), self.initial)
        self.assertEqual(self.registry.read_text(), "AAA,BBB\n")
        warning = records[-2]
        self.assertEqual(warning["schema"], "alpaca.lifecycle.diagnostic")
        self.assertEqual((warning["phase"], warning["level"]), ("COMMIT", "warning"))
        self.assertIn("fixture rejects commit", warning["message"])
        self.assertEqual(records[-1]["status"], "success")
        self.assertEqual(records[-1]["exit_code"], 0)

    def test_missing_git_warns_without_changing_the_completed_result(self):
        self.registry.write_text("AAA,BBB\n")
        with patch.object(registry_commit, "_git", side_effect=FileNotFoundError("git unavailable")):
            code, output, records, _ = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(output, '{"complete":true}\n')
        self.assertIn("git unavailable", records[-2]["message"])
        self.assertEqual(records[-2]["level"], "warning")
        self.assertEqual(self.git("rev-parse", "HEAD"), self.initial)

    def test_plans_failures_and_other_commands_do_not_attempt_git(self):
        self.registry.write_text("AAA,BBB\n")
        for arguments, result, error, expected in (
            (["update", "--plan"], {"complete": True}, None, 0),
            (["update"], {"complete": False}, None, 1),
            (["update"], {"failed": 1}, None, 1),
            (["update"], None, RuntimeError("data failed"), 1),
            (["status"], {"processing_complete": True}, None, 0),
        ):
            with self.subTest(arguments=arguments, result=result, error=error):
                code, _, _, calls = self.invoke(arguments, result=result, error=error)
                self.assertEqual((code, calls), (expected, 0))
                self.assertEqual(self.git("rev-parse", "HEAD"), self.initial)

    def test_custom_registry_and_untracked_registry_are_not_committed(self):
        custom = self.repository / "custom.csv"
        custom.write_text("AAA,BBB\n")
        with patch.object(alpaca_cli, "DEFAULT_REGISTRY", custom):
            self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(self.git("rev-parse", "HEAD"), self.initial)
        self.git("rm", "--cached", "resources/ticker.csv")
        code, _, records, _ = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(records[-2]["level"], "warning")
        self.assertEqual(self.git("rev-parse", "HEAD"), self.initial)

    def test_commit_phase_is_held_for_timer_samples(self):
        self.registry.write_text("AAA,BBB\n")
        actual_git = registry_commit._git
        clock = [0.0]
        reporters = []

        def reporter(*args, **kwargs):
            instance = LifecycleReporter(*args, **kwargs, clock=lambda: clock[0])
            reporters.append(instance)
            return instance

        def inspect(repository, *args, **kwargs):
            if args[0] == "commit":
                # The subprocess runs inside the existing registry lock.
                import fcntl

                with self.registry.with_name(".ticker.csv.lock").open("rb") as lock:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                clock[0] = 10.0
                reporters[0]._emit_due()
            return actual_git(repository, *args, **kwargs)

        with (
            patch.object(registry_commit, "_git", side_effect=inspect),
            patch.object(alpaca_cli, "LifecycleReporter", side_effect=reporter),
            patch("threading.Thread.start"),
        ):
            code, _, records, _ = self.invoke()
        self.assertEqual(code, 0)
        sample = next(
            r for r in records if r["schema"] == "alpaca.lifecycle.progress" and r["phase"] == "COMMIT"
        )
        self.assertEqual(sample["context"]["status"], "committing")
        self.assertEqual(sample["context"]["path"], "resources/ticker.csv")

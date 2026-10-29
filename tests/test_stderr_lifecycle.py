from __future__ import annotations

import io
import json
import threading
import unittest
from unittest import mock

from libs.alpaca_cli.csv_progress import PHASES, progress_callback
from libs.alpaca_heartbeat import LifecycleReporter, Phase


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingStream(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.flush_count = 0
        self.write_count = 0
        self.heartbeat = threading.Event()

    def write(self, value: str) -> int:
        self.write_count += 1
        result = super().write(value)
        if json.loads(value)["phase"] == "EXPORT":
            self.heartbeat.set()
        return result

    def flush(self) -> None:
        self.flush_count += 1
        super().flush()

    def records(self) -> list[dict]:
        return [json.loads(line) for line in self.getvalue().splitlines()]


class LifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.stream = RecordingStream()
        self.phases = {
            "EXPORT": Phase("Writing CSVs", {"public_id": str, "rows": int}),
            "PUBLISH": Phase("Publishing manifest", {"path": str}),
        }

    def reporter(self, **kwargs) -> LifecycleReporter:
        reporter = LifecycleReporter(
            self.phases,
            stream=self.stream,
            clock=self.clock,
            **kwargs,
        )
        self.addCleanup(reporter.finish, "aborted", None)
        with mock.patch("threading.Thread.start"):
            reporter.start()
        return reporter

    def test_init_is_immediate_compact_versioned_and_flushed(self) -> None:
        self.reporter()
        self.assertEqual(self.stream.flush_count, 1)
        self.assertEqual(self.stream.write_count, 1)
        records = self.stream.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["schema"], "alpaca.lifecycle.progress")
        self.assertEqual(records[0]["schema_version"], 4)
        self.assertEqual(records[0]["next_heart_beat_in_seconds"], 10)
        self.assertEqual(records[0]["phase"], "INIT")
        self.assertEqual(records[0]["elapsed_seconds"], 0)
        self.assertEqual(
            records[0]["message"],
            "Operation started.",
        )
        self.assertNotIn("current", records[0])
        self.assertNotIn("total", records[0])
        self.assertNotIn(": ", self.stream.getvalue())

    def test_monitoring_guidance_appears_only_in_initial_record(self) -> None:
        reporter = self.reporter()
        guidance = self.stream.records()[0]["ai_agent_read_this"]
        self.assertIn("more than 10 seconds late", guidance)
        self.assertIn("autonomously", guidance)
        self.assertIn(
            "You should update the user **only** at each heartbeat unless the user has directed you otherwise.",
            guidance,
        )
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["phase"], "INIT")
        reporter.update("EXPORT", current=0, total=1)
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        reporter.finish("error", 1, error=ValueError("export failed"))
        for record in self.stream.records()[1:]:
            self.assertNotIn("ai_agent_read_this", record)

    def test_ramped_deadlines_are_anchored_and_plateau(self) -> None:
        reporter = self.reporter()
        initial = self.clock.now
        for deadline, next_interval in (
            (10, 60),
            (70, 120),
            (190, 180),
            (370, 240),
            (610, 300),
            (910, 300),
            (1210, 300),
            (1510, 300),
        ):
            self.clock.now = initial + deadline - 0.25
            count = len(self.stream.records())
            self.assertTrue(reporter.update("EXPORT", current=0, total=10))
            reporter._emit_due()
            self.assertEqual(len(self.stream.records()), count)
            self.clock.advance(0.25)
            reporter._emit_due()
            self.assertEqual(len(self.stream.records()), count + 1)
            self.assertEqual(self.stream.records()[-1]["elapsed_seconds"], deadline)
            self.assertEqual(self.stream.records()[-1]["next_heart_beat_in_seconds"], next_interval)
        self.assertEqual(self.stream.flush_count, 9)

    def test_csv_export_estimate_is_tbd_until_progress_supplies_seconds(self) -> None:
        self.phases = PHASES
        reporter = self.reporter()
        progress = progress_callback(reporter)
        self.assertTrue(progress("csv_export_plan", {"identities": 2}))
        self.clock.advance(10)
        reporter._emit_due()
        first = self.stream.records()[-1]
        self.assertEqual(first["elapsed_seconds"], 10)
        self.assertEqual(first["context"]["estimated_remaining_seconds"], "TBD")
        self.assertTrue(progress("csv_export", {"done": 1, "total": 2, "estimated_remaining_seconds": 42.5}))
        self.clock.advance(60)
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["context"]["estimated_remaining_seconds"], 42)

    def test_first_heartbeat_overrides_estimate_without_changing_held_status(self) -> None:
        self.phases = PHASES
        for phase in ("INIT", "CATALOGUE", "PLAN", "EXPORT", "PUBLISH"):
            with self.subTest(phase=phase):
                self.stream = RecordingStream()
                reporter = self.reporter(first_heartbeat_context={"estimated_remaining_seconds": "TBD"})
                if phase == "EXPORT":
                    reporter.update(phase, context={"estimated_remaining_seconds": 42, "rows": 100})
                elif phase != "INIT":
                    reporter.update(phase)
                self.assertNotIn("context", self.stream.records()[0])
                self.clock.advance(10)
                reporter._emit_due()
                self.assertEqual(self.stream.records()[-1]["context"]["estimated_remaining_seconds"], "TBD")
                self.clock.advance(60)
                reporter._emit_due()
                if phase == "EXPORT":
                    self.assertEqual(
                        self.stream.records()[-1]["context"], {"estimated_remaining_seconds": 42, "rows": 100}
                    )
                else:
                    self.assertNotIn("context", self.stream.records()[-1])
                reporter.finish("success", 0)
                self.assertNotIn("context", self.stream.records()[-1])

    def test_updates_hold_only_latest_status_and_heartbeats_repeat_it(self) -> None:
        reporter = self.reporter()
        self.clock.advance(9.75)
        self.assertTrue(reporter.update("EXPORT", current=1, total=5))
        self.assertTrue(reporter.update("EXPORT", current=2, total=5))
        self.assertEqual(len(self.stream.records()), 1)
        self.clock.advance(0.25)
        reporter._emit_due()
        self.clock.advance(60.9)
        reporter._emit_due()
        first, second = self.stream.records()[1:]
        self.assertEqual(first["current"], 2)
        self.assertEqual(second["current"], 2)
        self.assertEqual(first["elapsed_seconds"], 10)
        self.assertEqual(second["elapsed_seconds"], 70)
        self.assertEqual(first["next_heart_beat_in_seconds"], 60)
        self.assertEqual(second["next_heart_beat_in_seconds"], 120)
        self.assertEqual(first["message"], "Writing CSVs")
        self.assertEqual(
            {k: v for k, v in first.items() if k not in {"elapsed_seconds", "next_heart_beat_in_seconds"}},
            {k: v for k, v in second.items() if k not in {"elapsed_seconds", "next_heart_beat_in_seconds"}},
        )

    def test_delayed_heartbeat_samples_once_and_skips_missed_deadlines(self) -> None:
        reporter = self.reporter()
        self.clock.advance(1000)
        reporter._emit_due()
        reporter._emit_due()
        self.assertEqual(len(self.stream.records()), 2)
        self.assertEqual(self.stream.records()[-1]["elapsed_seconds"], 1000)
        self.assertEqual(self.stream.records()[-1]["next_heart_beat_in_seconds"], 210)
        self.clock.advance(209)
        reporter._emit_due()
        self.assertEqual(len(self.stream.records()), 2)
        self.clock.advance(1)
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["elapsed_seconds"], 1210)

    def test_next_heartbeat_rounds_up_remaining_seconds(self) -> None:
        reporter = self.reporter()
        self.assertEqual(self.stream.records()[0]["next_heart_beat_in_seconds"], 10)
        self.clock.advance(12.25)
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["next_heart_beat_in_seconds"], 58)

    def test_context_is_copied_and_never_carried_to_other_work(self) -> None:
        reporter = self.reporter()
        context = {"public_id": "one", "rows": 12}
        self.assertTrue(reporter.update("EXPORT", current=1, total=2, context=context, scope="one"))
        context["public_id"] = "modified"
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["context"]["public_id"], "one")
        self.assertTrue(reporter.update("EXPORT", scope="two"))
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        self.assertNotIn("context", self.stream.records()[-1])
        self.assertNotIn("current", self.stream.records()[-1])
        self.assertTrue(reporter.update("PUBLISH", context={"path": "manifest"}))
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        self.assertNotIn("scope", self.stream.records()[-1])
        self.assertEqual(self.stream.records()[-1]["context"], {"path": "manifest"})

    def test_invalid_update_preserves_the_entire_accepted_status(self) -> None:
        reporter = self.reporter()
        self.assertTrue(reporter.update("EXPORT", current=1, total=3, context={"public_id": "accepted"}))
        invalid = [
            ("UNKNOWN", {}),
            ("INIT", {}),
            ("FINAL_EXIT", {}),
            ("EXPORT", {"message": 1}),
            ("EXPORT", {"current": 1}),
            ("EXPORT", {"total": 3}),
            ("EXPORT", {"current": -1, "total": 3}),
            ("EXPORT", {"current": 4, "total": 3}),
            ("EXPORT", {"current": True, "total": 3}),
            ("EXPORT", {"current": 1.0, "total": 3}),
            ("EXPORT", {"context": {"unregistered": "value"}}),
            ("EXPORT", {"context": {"rows": True}}),
            ("EXPORT", {"context": {"public_id": 123}}),
            ("EXPORT", {"message": "x" * 5000}),
        ]
        for phase, kwargs in invalid:
            with self.subTest(phase=phase, kwargs=kwargs):
                self.assertFalse(reporter.update(phase, **kwargs))
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        held = self.stream.records()[-1]
        self.assertEqual(held["phase"], "EXPORT")
        self.assertEqual(held["current"], 1)
        self.assertEqual(held["total"], 3)
        self.assertEqual(held["context"], {"public_id": "accepted"})

    def test_size_limit_counts_utf8_bytes_not_characters(self) -> None:
        reporter = self.reporter(progress_limit=1024)
        self.assertTrue(reporter.update("EXPORT", message="x" * 180))
        self.assertFalse(reporter.update("EXPORT", message="\U0001f434" * 180))
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["message"], "x" * 180)
        for line in self.stream.getvalue().splitlines():
            self.assertLessEqual(len(line.encode("utf-8")), 1024)

    def test_success_and_aborted_finish_immediately_once(self) -> None:
        for status, exit_code in (("success", 0), ("aborted", None)):
            with self.subTest(status=status):
                self.stream = RecordingStream()
                reporter = self.reporter()
                reporter.update("EXPORT", current=1, total=2)
                self.clock.advance(0.5)
                reporter.finish(status, exit_code)
                reporter.finish("error", 1, error=ValueError("too late"))
                self.clock.advance(1000)
                reporter._emit_due()
                records = self.stream.records()
                self.assertEqual(len(records), 2)
                self.assertEqual(records[-1]["phase"], "FINAL_EXIT")
                self.assertEqual(records[-1]["last_phase"], "EXPORT")
                self.assertEqual(records[-1]["status"], status)
                self.assertEqual(records[-1]["exit_code"], exit_code)
                self.assertIsNone(records[-1]["next_heart_beat_in_seconds"])
                self.assertNotIn("current", records[-1])
                self.assertEqual(self.stream.flush_count, 2)

    def test_failure_emits_error_with_cause_before_final(self) -> None:
        reporter = self.reporter()
        reporter.update("EXPORT", context={"public_id": "one"})
        try:
            try:
                raise OSError("source unavailable")
            except OSError as cause:
                raise RuntimeError("export failed") from cause
        except RuntimeError as error:
            reporter.finish("error", 7, error=error)
        records = self.stream.records()
        self.assertEqual(len(records), 3)
        self.assertEqual(records[1]["schema"], "alpaca.lifecycle.error")
        self.assertEqual(records[1]["schema_version"], 4)
        self.assertIsNone(records[1]["next_heart_beat_in_seconds"])
        self.assertIsNone(records[2]["next_heart_beat_in_seconds"])
        self.assertEqual(records[1]["phase"], "EXPORT")
        self.assertEqual(records[1]["error_type"], "RuntimeError")
        self.assertEqual(records[1]["message"], "export failed")
        self.assertIn("source unavailable", json.dumps(records[1]["causes"]))
        self.assertIn("test_failure_emits_error", records[1]["traceback"])
        self.assertEqual(records[2]["status"], "error")
        self.assertEqual(records[2]["exit_code"], 7)
        self.assertEqual(records[2]["last_phase"], "EXPORT")
        self.assertEqual(self.stream.flush_count, 3)

    def test_error_has_separate_bounded_valid_json_size_policy(self) -> None:
        reporter = self.reporter(progress_limit=1024, error_limit=2048)
        reporter.update("EXPORT")
        reporter.finish("error", 1, error=ValueError("\U0001f434" * 10000))
        records = self.stream.records()
        error = records[1]
        self.assertEqual(error["schema"], "alpaca.lifecycle.error")
        self.assertTrue(error["truncated"])
        self.assertEqual(error["phase"], "EXPORT")
        for line, record in zip(self.stream.getvalue().splitlines(), records):
            limit = 2048 if record["schema"] == "alpaca.lifecycle.error" else 1024
            self.assertLessEqual(len(line.encode("utf-8")), limit)

    def test_real_timer_reports_while_application_work_is_idle(self) -> None:
        # Keep the real timer/thread; accelerate only elapsed time, not its cadence.
        import time

        origin = time.monotonic()
        reporter = LifecycleReporter(
            self.phases, stream=self.stream, clock=lambda: (time.monotonic() - origin) * 1000
        )
        self.addCleanup(reporter.finish, "aborted", None)
        stop = reporter._stop
        real_wait = stop.wait
        with mock.patch.object(stop, "wait", side_effect=lambda seconds: real_wait(seconds / 1000)):
            reporter.start()
            reporter.update("EXPORT", current=0, total=1)
            self.assertTrue(self.stream.heartbeat.wait(timeout=2))
        reporter.finish("success", 0)
        records = self.stream.records()
        self.assertEqual(records[0]["phase"], "INIT")
        self.assertIn("EXPORT", [record["phase"] for record in records[1:-1]])
        self.assertEqual(records[-1]["phase"], "FINAL_EXIT")
        self.assertEqual(self.stream.write_count, self.stream.flush_count)

    def test_concurrent_finish_and_due_heartbeat_cannot_emit_after_final(self) -> None:
        reporter = self.reporter()
        reporter.update("EXPORT", current=0, total=1)
        self.clock.advance(10)
        barrier = threading.Barrier(4)

        def invoke(action) -> None:
            barrier.wait(timeout=2)
            action()

        workers = [
            threading.Thread(target=invoke, args=(reporter._emit_due,)),
            threading.Thread(target=invoke, args=(lambda: reporter.finish("success", 0),)),
            threading.Thread(target=invoke, args=(lambda: reporter.finish("aborted", None),)),
        ]
        for worker in workers:
            worker.start()
        barrier.wait(timeout=2)
        for worker in workers:
            worker.join(timeout=2)
            self.assertFalse(worker.is_alive())
        records = self.stream.records()
        finals = [record for record in records if record["phase"] == "FINAL_EXIT"]
        self.assertEqual(len(finals), 1)
        self.assertIs(records[-1], finals[0])
        self.assertEqual(self.stream.write_count, self.stream.flush_count)

    def test_broken_write_or_flush_does_not_raise(self) -> None:
        for operation in ("write", "flush"):
            with self.subTest(operation=operation):
                self.stream = RecordingStream()
                with mock.patch.object(self.stream, operation, side_effect=OSError("broken stderr")):
                    reporter = self.reporter()
                    reporter.update("EXPORT")
                    self.clock.now = reporter._next_deadline
                    reporter._emit_due()
                    reporter.finish("error", 1, error=ValueError("actual failure"))
                reporter.finish("success", 0)

    def test_exception_with_broken_string_formatter_still_finalizes(self) -> None:
        class BrokenMessageError(Exception):
            def __str__(self) -> str:
                raise RuntimeError("cannot format")

        reporter = self.reporter()
        reporter.update("EXPORT")
        reporter.finish("error", 1, error=BrokenMessageError())
        records = self.stream.records()
        self.assertEqual(records[-1]["phase"], "FINAL_EXIT")
        self.assertEqual(records[-1]["status"], "error")
        self.assertEqual(records[-1]["exit_code"], 1)

    def test_worker_start_failure_does_not_change_operation_outcome(self) -> None:
        reporter = LifecycleReporter(self.phases, stream=self.stream)
        self.addCleanup(reporter.finish, "aborted", None)
        with mock.patch("threading.Thread.start", side_effect=RuntimeError("thread")):
            reporter.start()
        reporter.update("EXPORT", current=1, total=1)
        reporter.finish("success", 0)

    def test_registered_context_types_reject_nonfinite_numbers(self) -> None:
        self.phases["RATE"] = Phase("Measuring rate", {"rate": float})
        reporter = self.reporter()
        self.assertTrue(reporter.update("RATE", context={"rate": 1.25}))
        for value in (float("inf"), float("-inf"), float("nan"), True, "1.25"):
            with self.subTest(value=value):
                self.assertFalse(reporter.update("RATE", context={"rate": value}))
        self.clock.now = reporter._next_deadline
        reporter._emit_due()
        self.assertEqual(self.stream.records()[-1]["context"], {"rate": 1.25})

    def test_reporter_rejects_interval_override_even_matching_fixed_schedule(self) -> None:
        for intervals in ((1,), (60,), (10, 60, 120, 180, 240, 300)):
            with self.subTest(intervals=intervals), self.assertRaises(TypeError):
                LifecycleReporter(self.phases, heartbeat_intervals=intervals)

    def test_configuration_rejects_invalid_limits(self) -> None:
        for kwargs in (
            {"progress_limit": 1},
            {"progress_limit": 512},
            {"error_limit": 1},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    LifecycleReporter(self.phases, stream=self.stream, **kwargs)


if __name__ == "__main__":
    unittest.main()

"""Bounded STDERR lifecycle records over one in-memory, sampled status."""

import json
import math
import sys
import threading
import time
import traceback
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

DEFAULT_HEARTBEAT_INTERVALS = (60, 120, 180, 240, 300)
PROGRESS_SCHEMA = "alpaca.lifecycle.progress"
ERROR_SCHEMA = "alpaca.lifecycle.error"
SCHEMA_VERSION = 1
_SCALAR_TYPES = (str, int, float, bool, type(None))
_MAX_ELAPSED = 2**63 - 1
_FINAL_MESSAGES = {
    "success": "Operation completed.",
    "error": "Operation failed.",
    "aborted": "Operation interrupted.",
}


@dataclass(frozen=True)
class Phase:
    """A registered phase and its optional, scalar context fields."""

    message: str
    fields: Mapping[str, type] = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.message, str) or not self.message:
            raise ValueError("phase message must be a nonempty string")
        if not isinstance(self.fields, Mapping) or any(
            not isinstance(name, str) or not name or kind not in _SCALAR_TYPES
            for name, kind in self.fields.items()
        ):
            raise ValueError("phase fields must declare named scalar types")
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))


class LifecycleReporter:
    """Sample a held status independently of work; reporting failures stay local.

    Intervals are anchored at activation; the last interval repeats indefinitely.
    Updates take only the state lock. Serialized writes use a separate lock, so a
    slow STDERR cannot block status updates. Finalization serializes behind any
    in-flight write to preserve the final-record ordering contract.
    """

    def __init__(
        self,
        phases,
        *,
        stream=None,
        heartbeat_intervals=DEFAULT_HEARTBEAT_INTERVALS,
        progress_limit=4096,
        error_limit=16384,
        clock=time.monotonic,
    ):
        intervals = tuple(heartbeat_intervals)
        if not intervals or any(
            type(value) not in (int, float) or not math.isfinite(value) or value <= 0 for value in intervals
        ):
            raise ValueError("heartbeat intervals must be positive finite seconds")
        if any(type(value) is not int or value < 256 for value in (progress_limit, error_limit)):
            raise ValueError("record size limits must be integers of at least 256 bytes")
        if not callable(clock):
            raise ValueError("clock must be callable")
        if not isinstance(phases, Mapping) or any(
            not isinstance(name, str)
            or not name
            or len(name.encode("utf-8")) > 64
            or name in {"INIT", "FINAL_EXIT"}
            or not isinstance(phase, Phase)
            for name, phase in phases.items()
        ):
            raise ValueError("phases must have unique nonreserved identifiers up to 64 UTF-8 bytes")
        self._phases = dict(phases)
        self._phases["INIT"] = Phase("Reporting initialized.")
        self._stream = sys.stderr if stream is None else stream
        self._intervals = intervals
        self._progress_limit = progress_limit
        self._error_limit = error_limit
        self._clock = clock
        self._state_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self._started = False
        self._finalized = False
        self._writable = True
        self._observed_time = None
        self._origin = 0.0
        self._interval_index = 0
        self._next_deadline = 0.0
        self._held = {"phase": "INIT", "message": self._phases["INIT"].message}
        self._thread = None
        for name, phase in self._phases.items():
            if not self._fits_progress({"phase": name, "message": phase.message}):
                raise ValueError("phase default message exceeds progress record size limit")
            final = self._final_record("aborted", 130, name, _MAX_ELAPSED)
            if self._encode(final, progress_limit) is None:
                raise ValueError("phase identifier exceeds final record size limit")
            error = self._error_base(name, _MAX_ELAPSED)
            error["truncated"] = True
            if self._encode(error, error_limit) is None:
                raise ValueError("phase identifier exceeds error record size limit")

    @staticmethod
    def _encode(record, limit):
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n"
        return line if len(line) <= limit else None

    def _now(self):
        try:
            value = self._clock()
            if type(value) in (int, float) and math.isfinite(value):
                self._observed_time = value if self._observed_time is None else max(self._observed_time, value)
        except Exception:
            pass
        return 0.0 if self._observed_time is None else self._observed_time

    def _elapsed(self, now):
        return max(0, int(now - self._origin))

    @staticmethod
    def _progress_record(status, elapsed):
        return {
            "schema": PROGRESS_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "elapsed_seconds": elapsed,
            **status,
        }

    def _fits_progress(self, status):
        return self._encode(self._progress_record(status, _MAX_ELAPSED), self._progress_limit) is not None

    def _write(self, record, limit):
        if not self._writable:
            return
        try:
            line = self._encode(record, limit)
            if line is None:
                return
            self._stream.write(line)
            self._stream.flush()
        except Exception:
            self._writable = False
            self._stop.set()

    def start(self):
        """Activate once and immediately flush INIT; execution may then begin."""
        try:
            with self._write_lock:
                with self._state_lock:
                    if self._started or self._finalized:
                        return
                    self._started = True
                    self._origin = self._now()
                    self._next_deadline = self._origin + self._intervals[0]
                    initial = self._progress_record(self._held, 0)
                self._write(initial, self._progress_limit)
            with self._state_lock:
                if self._finalized or self._stop.is_set():
                    return
                self._thread = threading.Thread(target=self._run, name="stderr-lifecycle", daemon=True)
                self._thread.start()
        except Exception:
            self._stop.set()

    def update(self, phase, *, message=None, current=None, total=None, context=None, scope=None):
        """Replace the complete held status, or return False without changing it."""
        try:
            if phase == "INIT" or phase not in self._phases:
                return False
            definition = self._phases[phase]
            message = definition.message if message is None else message
            if not isinstance(message, str) or not message:
                return False
            status = {"phase": phase, "message": message}
            if current is not None or total is not None:
                if type(current) is not int or type(total) is not int or not 0 <= current <= total:
                    return False
                status.update(current=current, total=total)
            if scope is not None:
                if not isinstance(scope, str) or not scope:
                    return False
                status["scope"] = scope
            if context is not None:
                if not isinstance(context, Mapping):
                    return False
                copied = dict(context)
                for name, value in copied.items():
                    if name not in definition.fields or type(value) is not definition.fields[name]:
                        return False
                    if type(value) is float and not math.isfinite(value):
                        return False
                if copied:
                    status["context"] = copied
            if not self._fits_progress(status):
                return False
            with self._state_lock:
                if not self._started or self._finalized:
                    return False
                self._held = status
            return True
        except Exception:
            return False

    def _advance_deadline(self, now):
        while self._next_deadline <= now:
            if self._interval_index + 1 < len(self._intervals):
                self._interval_index += 1
                self._next_deadline += self._intervals[self._interval_index]
            else:
                interval = self._intervals[-1]
                skipped = int((now - self._next_deadline) // interval) + 1
                self._next_deadline += skipped * interval

    def _emit_due(self):
        """Emit at most one due sample, preserving the anchored timer schedule."""
        try:
            with self._write_lock:
                with self._state_lock:
                    if not self._started or self._finalized or self._stop.is_set():
                        return
                    now = self._now()
                    if now < self._next_deadline:
                        return
                    self._advance_deadline(now)
                    record = self._progress_record(self._held, self._elapsed(now))
                self._write(record, self._progress_limit)
        except Exception:
            self._stop.set()

    def _run(self):
        try:
            while not self._stop.is_set():
                with self._state_lock:
                    if self._finalized:
                        return
                    delay = max(0.0, self._next_deadline - self._now())
                if self._stop.wait(delay):
                    return
                self._emit_due()
        except Exception:
            self._stop.set()

    @staticmethod
    def _final_record(status, exit_code, last_phase, elapsed):
        return LifecycleReporter._progress_record(
            {
                "phase": "FINAL_EXIT",
                "message": _FINAL_MESSAGES[status],
                "status": status,
                "exit_code": exit_code,
                "last_phase": last_phase,
            },
            elapsed,
        )

    @staticmethod
    def _error_base(phase, elapsed):
        return {
            "schema": ERROR_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "elapsed_seconds": elapsed,
            "phase": phase,
            "error_type": "",
            "message": "",
        }

    @staticmethod
    def _error_message(error):
        try:
            return str(error)
        except Exception:
            return "Exception message unavailable."

    def _bounded_error(self, error, phase, elapsed):
        record = self._error_base(phase, elapsed)
        record.update(error_type=type(error).__name__, message=self._error_message(error))
        causes, seen, cause = [], {id(error)}, error
        for _ in range(8):
            explicit = cause.__cause__
            cause = (
                explicit if explicit is not None else (None if cause.__suppress_context__ else cause.__context__)
            )
            if cause is None or id(cause) in seen:
                break
            seen.add(id(cause))
            causes.append({"error_type": type(cause).__name__, "message": self._error_message(cause)})
        if causes:
            record["causes"] = causes
        try:
            if error.__traceback__ is not None:
                record["traceback"] = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__, limit=20, chain=False)
                )
        except Exception:
            pass
        if self._encode(record, self._error_limit) is not None:
            return record
        record["truncated"] = True
        # Preserve the latest phase exactly; trim optional diagnostics before the
        # primary error message. Each result remains a complete JSON object.
        for name in ("traceback", "causes"):
            record.pop(name, None)
            if self._encode(record, self._error_limit) is not None:
                return record
        for name in ("message", "error_type"):
            original = record[name]
            record[name] = ""
            if self._encode(record, self._error_limit) is None:
                continue
            low, high = 0, len(original)
            while low < high:
                middle = (low + high + 1) // 2
                record[name] = original[:middle]
                if self._encode(record, self._error_limit) is None:
                    high = middle - 1
                else:
                    low = middle
            record[name] = original[:low]
            return record
        return record

    def finish(self, status, exit_code, error=None):
        """Stop sampling immediately, then emit any error and exactly one final."""
        try:
            if status not in _FINAL_MESSAGES or (exit_code is not None and type(exit_code) is not int):
                return
            if (status == "success" and exit_code != 0) or (status != "success" and exit_code == 0):
                return
            if error is not None and (status != "error" or not isinstance(error, BaseException)):
                return
            with self._state_lock:
                if not self._started or self._finalized:
                    return
                elapsed = self._elapsed(self._now())
                phase = self._held["phase"]
                final = self._final_record(status, exit_code, phase, elapsed)
                if self._encode(final, self._progress_limit) is None:
                    return
                self._finalized = True
                self._stop.set()
            with self._write_lock:
                if error is not None:
                    try:
                        record = self._bounded_error(error, phase, elapsed)
                    except Exception:
                        record = self._error_base(phase, elapsed)
                        record.update(error_type="Exception", message="Error details unavailable.")
                    self._write(record, self._error_limit)
                self._write(final, self._progress_limit)
        except Exception:
            self._stop.set()

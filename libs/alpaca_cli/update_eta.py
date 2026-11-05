"""Conditional update estimates from accepted plans and completed HTTP calls.

This observer owns no data authority. It performs no filesystem or provider I/O,
does not retain request arguments/results, and cannot authorize or finish work.
"""

import math
from collections import deque
from functools import wraps
from time import monotonic

from libs.alpaca_pricing_download.http import REQUESTS_PER_MINUTE


class UpdateEstimate:
    """Estimate the known request workload, explicitly excluding unknown work.

    Page estimates are conditional planning assumptions, never hard deadlines. A
    producer's exact late-bar plan replaces its earlier provisional contribution.
    Completed pages include time spent in retries and rate limiting. Wall-time
    throughput, rather than summed worker durations, handles concurrent requests.
    The progress adapter serializes this object's observations across workers.
    """

    def __init__(self, *, clock=monotonic):
        self.clock = clock
        self.completed = 0
        self.selected_at = 0
        self.bounds = None
        self.late_bounds = None
        self.samples = deque(maxlen=64)
        self.first_started = None
        self.last_finished = None
        self.failed = False
        self.complete = False
        self.phase = None
        self.phase_baseline = None
        self.phase_remaining = None
        self.phase_done = None

    def request(self, values):
        start, finish = values.get("started"), values.get("finished")
        if not all(type(value) in (int, float) and math.isfinite(value) for value in (start, finish)):
            return
        if finish < start:
            return
        self.completed += 1
        self.first_started = start if self.first_started is None else min(start, self.first_started)
        self.last_finished = finish if self.last_finished is None else max(finish, self.last_finished)
        self.samples.append((start, finish))
        if values.get("succeeded") is not True:
            self.failed = True

    def observe(self, stage, values):
        done = values.get("done", values.get("completed"))
        total = values.get("total", values.get("selected"))
        if stage != self.phase or values.get("state") == "started":
            self.phase, self.phase_baseline, self.phase_remaining, self.phase_done = stage, None, None, None
        if type(done) is int and type(total) is int and 0 <= done <= total:
            now = self.clock()
            if self.phase_baseline is None or (self.phase_done is not None and done < self.phase_done):
                self.phase_baseline, self.phase_remaining = (now, done), None
            self.phase_done = done
            elapsed, completed = now - self.phase_baseline[0], done - self.phase_baseline[1]
            if done == total:
                self.phase_remaining = 0
            elif elapsed > 0 and completed > 0:
                self.phase_remaining = math.ceil((total - done) * elapsed / completed)
        expected = values.get("expected_io")
        if isinstance(expected, dict):
            names = (
                "http_pages_minimum",
                "http_pages_dense_history_estimate",
                "selection_metadata_http_requests_observed",
            )
            low, high, metadata = (expected.get(name) for name in names)
            if (
                all(type(value) is int and value >= 0 for value in (low, high, metadata))
                and metadata <= low <= high
            ):
                self.bounds = [low - metadata, high - metadata]
                self.selected_at = self.completed
                late = expected.get("late_bar_probe_actions_upper_bound")
                self.late_bounds = (int(bool(late)), late) if type(late) is int and late >= 0 else None
        if stage.endswith("provider_late_bar_check") and values.get("state") == "started":
            pages = values.get("minimum_pages")
            if self.bounds is not None and self.late_bounds is not None and type(pages) is int and pages >= 0:
                self.bounds = [max(0, bound - old + pages) for bound, old in zip(self.bounds, self.late_bounds)]
                self.late_bounds = None  # Replace this provisional scope only once.
        pages = values.get("minimum_pages")
        if self.bounds is not None and values.get("state") == "started" and type(pages) is int and pages >= 0:
            # Corrections, dependencies and pagination can exceed selection's
            # estimate. A newly accepted plan cannot inherit an exhausted budget.
            required = self.completed - self.selected_at + pages
            self.bounds = [max(bound, required) for bound in self.bounds]
        if stage == "completion":
            self.complete = values.get("complete") is True and values.get("pending_work") == 0

    def context(self):
        base = {
            "estimated_remaining_seconds": "TBD",
            "eta_scope": "known_pricing_requests",
            "eta_status": "learning",
            "eta_pricing_pages_completed": self.completed,
        }
        if self.phase_remaining is not None:
            base.update(eta_phase=self.phase, estimated_phase_remaining_seconds=self.phase_remaining)
        if self.complete:
            return {
                **base,
                "estimated_remaining_seconds": 0,
                "eta_scope": "data_update",
                "eta_status": "complete",
            }
        if self.failed:
            return {**base, "eta_status": "request_failed"}
        if self.bounds is None or len(self.samples) < 8:
            return base
        completed = self.completed - self.selected_at
        low, high = (max(0, bound - completed) for bound in self.bounds)
        if high == 0:
            # Exhausting a provisional page budget is not command completion.
            return {**base, "eta_status": "awaiting_remaining_scope"}
        recent = (max(end for _, end in self.samples) - min(start for start, _ in self.samples)) / len(
            self.samples
        )
        overall = (self.last_finished - self.first_started) / self.completed
        floor = 60 / REQUESTS_PER_MINUTE
        fast, slow = max(floor, min(recent, overall)), max(floor, recent, overall)
        lower, upper = math.ceil(low * fast), math.ceil(high * slow)
        return {
            **base,
            "estimated_remaining_seconds": max(1, math.ceil((lower + upper) / 2)),
            "estimated_remaining_lower_seconds": lower,
            "estimated_remaining_upper_seconds": upper,
            "eta_status": "conditional",
        }


def measured_request(function, progress, *, clock=monotonic):
    """Observe one original call, preserving its result, errors and retry policy."""

    @wraps(function)
    def call(*args, **kwargs):
        try:
            started = clock()
        except Exception:
            started = None
        succeeded = False
        try:
            result = function(*args, **kwargs)
            succeeded = True
            return result
        finally:
            try:
                progress("eta_request", {"started": started, "finished": clock(), "succeeded": succeeded})
            except Exception:
                pass  # Reporting failure must not alter the provider outcome.

    return call

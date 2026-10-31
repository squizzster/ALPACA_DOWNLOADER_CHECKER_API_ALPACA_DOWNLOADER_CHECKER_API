"""Opt-in, bounded operation tracing from held values only; never inspect stores."""

import json
import threading
import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from datetime import datetime, timezone
from functools import wraps
from inspect import signature
from itertools import islice
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_active = ContextVar("alpaca_debug_trace", default=None)
_parent = ContextVar("alpaca_debug_parent", default=None)
_SCALARS = frozenset(
    "symbol seed status complete failed failed_stage mode state seconds end_exclusive "
    "reused_decision reused_closed_boundaries historical_rows_reused pricing_rows_processed "
    "unresolved_rows processed evaluated_groups reused_groups reused_history_prefixes "
    "full_history_evaluations checked_bars published_components provider_requests "
    "identity_rule_executions pricing_writes workers requested succeeded deferred_full_range".split()
)
_ARGUMENTS = frozenset(
    "symbol seed symbols labels end_date start end refresh bootstrap rebuild resume_closed "
    "active_only resume_from from_date required inputs sources receipts entries prepared selected".split()
)


def enabled():
    return _active.get() is not None


def trace_http(function, url_for, client):
    """Log the complete GET target in bounded chunks, never headers or bodies."""

    @wraps(function)
    def call(*args, **kwargs):
        trace = _active.get()
        if trace is None:
            return function(*args, **kwargs)
        target = urlsplit(url_for(*args, **kwargs))
        query = [
            (key, value)
            for key, value in parse_qsl(target.query, keep_blank_values=True)
            if key.lower() not in {"api_key", "api_secret", "authorization", "access_token"}
        ]
        url = urlunsplit((target.scheme, target.netloc.rsplit("@", 1)[-1], target.path, urlencode(query), ""))
        request_id = uuid.uuid4().hex[:16]
        before = client.request_count
        with span("http_request", request_id=request_id, method="GET") as result:
            parts = max(1, (len(url) + 1023) // 1024)
            for part in range(parts):
                trace.emit(
                    "request",
                    "http_target",
                    parent=_parent.get(),
                    request_id=request_id,
                    part=part,
                    parts=parts,
                    url_part=url[part * 1024 : (part + 1) * 1024],
                )
            try:
                return function(*args, **kwargs)
            except BaseException as error:
                status = getattr(error, "status", getattr(error, "code", None))
                if type(status) is int:
                    result["http_status"] = status
                raise
            finally:
                result["attempts"] = client.request_count - before

    return call


def compact(value, depth=0):
    """Bound reports without walking large collections or serializing payloads."""
    if value is None or type(value) in (bool, int, float):
        return value
    if isinstance(value, str):
        return value[:256]
    if depth >= 3:
        return {"count": len(value)} if isinstance(value, (dict, list, tuple, set)) else None
    if isinstance(value, dict):
        result = {}
        for key in islice(value, 32):
            result[str(key)[:80]] = compact(value[key], depth + 1)
        if len(value) > 32:
            result["omitted_keys"] = len(value) - 32
        return result
    if isinstance(value, (list, tuple)):
        return {"count": len(value), "sample": [compact(v, depth + 1) for v in value[:8]]}
    if isinstance(value, set):
        return {"count": len(value)}
    return None


def outcome(value):
    if isinstance(value, dict):
        result = {k: value[k] for k in _SCALARS & value.keys() if type(value[k]) in (str, int, float, bool)}
        for key in (
            "results",
            "processed",
            "blockers",
            "pending_work",
            "pending_symbols",
            "unavailable_pricing",
        ):
            if isinstance(value.get(key), (list, tuple, dict, set)):
                result[key + "_count"] = len(value[key])
        for key in ("requests", "validation_work", "dependency_pricing"):
            if key in value:
                result[key] = compact(value[key])
        return result
    return {"exit_code": value} if type(value) is int else {}


class DebugTrace:
    def __init__(self, path, *, warning=None):
        # An explicit destination is opened before any update work. Never replace
        # an existing log or create missing parent directories implicitly.
        self.stream = open(path, "x", encoding="utf-8", buffering=1)
        self.warning = warning
        self.lock = threading.Lock()
        self.started = time.monotonic()
        self.run_id = uuid.uuid4().hex
        self.sequence = 0
        self.disabled = False

    def emit(self, event, stage, **values):
        if self.disabled:
            return
        with self.lock:
            if self.disabled:
                return
            try:
                self.sequence += 1
                record = {
                    "schema": "alpaca.update.debug",
                    "schema_version": 1,
                    "run_id": self.run_id,
                    "seq": self.sequence,
                    "elapsed_seconds": round(time.monotonic() - self.started, 6),
                    "thread": threading.current_thread().name,
                    "event": event,
                    "stage": stage,
                    **values,
                }
                line = json.dumps(record, separators=(",", ":"), allow_nan=False)
                if len(line.encode("utf-8")) > 8191:
                    record["details"] = {"truncated": True}
                    line = json.dumps(record, separators=(",", ":"), allow_nan=False)
                self.stream.write(line + "\n")
            except (OSError, ValueError, TypeError):
                self.disabled = True
                if self.warning:
                    self.warning("Debug trace write failed; tracing disabled for this run.")

    @contextmanager
    def activate(self):
        token = _active.set(self)
        self.emit("start", "run", utc=datetime.now(timezone.utc).isoformat())
        try:
            yield self
        finally:
            _active.reset(token)

    def close(self, status, code):
        self.emit("end", "run", status=status, exit_code=code)
        try:
            self.stream.close()
        except OSError:
            if self.warning and not self.disabled:
                self.warning("Debug trace close failed; final records may be missing.")


@contextmanager
def span(stage, **details):
    trace = _active.get()
    if trace is None:
        yield {}
        return
    span_id = uuid.uuid4().hex[:16]
    parent = _parent.get()
    started = time.monotonic()
    trace.emit("enter", stage, span=span_id, parent=parent, details=compact(details))
    token = _parent.set(span_id)
    result = {}
    try:
        yield result
    except BaseException as error:
        # Error text/arguments can contain request URLs or credentials. The
        # normal error channel owns those diagnostics; tracing retains type only.
        result.update(
            status="aborted" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "error",
            error_type=type(error).__name__,
        )
        raise
    finally:
        _parent.reset(token)
        result.setdefault("status", "returned")
        trace.emit(
            "exit",
            stage,
            span=span_id,
            parent=parent,
            duration_seconds=round(time.monotonic() - started, 6),
            details=compact(result),
        )


def event(stage, **details):
    trace = _active.get()
    if trace is not None:
        trace.emit("decision", stage, parent=_parent.get(), details=compact(details))


def traced(stage):
    def decorate(function):
        parameters = signature(function)

        @wraps(function)
        def call(*args, **kwargs):
            if _active.get() is None:
                return function(*args, **kwargs)
            bound = parameters.bind_partial(*args, **kwargs).arguments
            details = {}
            for name in _ARGUMENTS & bound.keys():
                value = bound[name]
                if type(value) in (str, bool, int, float) or value is None:
                    details[name] = value
                elif isinstance(value, (list, tuple, set, dict)):
                    details[name + "_count"] = len(value)
            with span(stage, **details) as result:
                value = function(*args, **kwargs)
                result.update(outcome(value))
                return value

        return call

    return decorate


def in_context(function):
    """Carry the invoking span into one worker task (one context per task)."""
    context = copy_context()
    return lambda *args, **kwargs: context.run(function, *args, **kwargs)


def traced_context(stage):
    def decorate(function):
        @wraps(function)
        @contextmanager
        def call(*args, **kwargs):
            with span(stage):
                with function(*args, **kwargs) as value:
                    event(stage + "_acquired")
                    yield value

        return call

    return decorate


def progress_callback(callback):
    trace, parent = _active.get(), _parent.get()
    if trace is None:
        return callback

    def progress(stage, values):
        trace.emit("progress", stage, parent=_parent.get() or parent, details=compact(values))
        if callback is not None:
            return callback(stage, values)

    return progress

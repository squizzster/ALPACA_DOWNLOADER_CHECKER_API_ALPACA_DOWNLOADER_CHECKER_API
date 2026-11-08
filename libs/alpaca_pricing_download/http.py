"""Only the historical stock-bars endpoint; no asset or identity lookups."""

from __future__ import annotations

import http.client
import json
import math
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from email.utils import parsedate_to_datetime

from libs.alpaca_data_api_solver import BAR_URL, build_plan, validate_version
from libs.alpaca_data_api_solver import VERSION as PLAN_VERSION
from libs.common.provider_http import (
    RETRY,
    fetch_symbol_batches,
    open_provider_request,
    paginated_pages,
    request_with_policy,
)

from .contract import BarsSymbolUnavailable, DownloadError, timestamp, transform
from .responses import CompletedBars

REQUESTS_PER_MINUTE = 199
REQUEST_TIMEOUT_SECONDS = 300


class RequestLimiter:
    """One in-memory request budget and cooldown shared by every ticker worker."""

    def __init__(self, *, sleep=time.sleep, monotonic=time.monotonic):
        self.sleep = sleep
        self.monotonic = monotonic
        self._lock = threading.Lock()
        self._starts = deque()
        self._next_request = 0.0
        self._cooldown_until = 0.0

    def acquire(self) -> float:
        while True:
            with self._lock:
                now = self.monotonic()
                while self._starts and self._starts[0] + 60 <= now:
                    self._starts.popleft()
                ready = max(self._next_request, self._cooldown_until)
                if len(self._starts) >= REQUESTS_PER_MINUTE:
                    ready = max(ready, self._starts[0] + 60)
                delay = ready - now
                if delay <= 0:
                    self._starts.append(now)
                    self._next_request = now + 60 / REQUESTS_PER_MINUTE
                    return now
            # Never hold the mutex while sleeping or performing network I/O.
            # Recheck after waking: another worker may have extended the cooldown.
            self.sleep(min(delay, 60.0))

    def defer(self, seconds: float) -> None:
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, self.monotonic() + seconds)


def retry_after_seconds(value: str | None, now: float) -> float:
    """Accept delta seconds or an HTTP date; malformed headers get a safe fallback."""
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            retry_date = parsedate_to_datetime(value)
            if retry_date.tzinfo is None:
                return 60.0
            seconds = max(0.0, retry_date.timestamp() - now)
        except (TypeError, ValueError, OverflowError, AttributeError):
            return 60.0
    return seconds if math.isfinite(seconds) and seconds >= 0 else 60.0


def _object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise DownloadError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _nonfinite(value: str):
    raise DownloadError(f"non-finite JSON token: {value}")


def parse_response(data: bytes) -> dict:
    try:
        result = json.loads(data, parse_float=Decimal, parse_constant=_nonfinite, object_pairs_hook=_object)
    except (ValueError, UnicodeError) as error:
        raise DownloadError("invalid provider JSON") from error
    if not isinstance(result, dict):
        raise DownloadError("provider response is not an object")
    return result


class BarsClient:
    def __init__(
        self,
        key: str,
        secret: str,
        *,
        opener=None,
        sleep=time.sleep,
        monotonic=time.monotonic,
        wall_time=time.time,
        limiter: RequestLimiter | None = None,
    ):
        if not key or not secret:
            raise DownloadError("ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY are required")
        validate_version()
        self.key = key
        self.secret = secret
        self.opener = opener or open_provider_request
        self.sleep = sleep
        self.wall_time = wall_time
        self.limiter = limiter if limiter is not None else RequestLimiter(sleep=sleep, monotonic=monotonic)
        self.request_count = 0
        self._request_lock = threading.Lock()

    def _get(self, parameters: dict) -> dict:
        request = urllib.request.Request(
            BAR_URL + "?" + urllib.parse.urlencode(parameters),
            headers={"APCA-API-KEY-ID": self.key, "APCA-API-SECRET-KEY": self.secret},
        )

        def before_attempt():
            # Every page, endpoint probe, and retry uses this same shared budget.
            self.limiter.acquire()
            with self._request_lock:
                self.request_count += 1

        def on_error(error, attempt, last):
            if isinstance(error, urllib.error.HTTPError):
                retry = error.code == 429 or 500 <= error.code < 600
                if retry:
                    delay = retry_after_seconds((error.headers or {}).get("Retry-After"), self.wall_time())
                    # Even an exhausted ticker must leave the other workers paused.
                    self.limiter.defer(delay)
                unavailable = False
                if error.code == 400:
                    try:
                        body = parse_response(error.read(2048))
                        symbol = parameters.get("symbols")
                        unavailable = (
                            isinstance(symbol, str)
                            and "," not in symbol
                            and body.get("message") == f"invalid symbol: {symbol}"
                        )
                    except (DownloadError, OSError, http.client.HTTPException):
                        pass  # A malformed error is not proof of symbol rejection.
                if unavailable:
                    raise BarsSymbolUnavailable(symbol, error.code) from error
                if not retry or last:
                    target = parameters.get("symbols")
                    if isinstance(target, str) and "," in target:
                        target = f"{target.count(',') + 1} symbols"
                    raise DownloadError(
                        f"Alpaca bars request failed for {target}/{parameters.get('adjustment')}: HTTP {error.code}"
                    ) from error
            else:
                if last:
                    raise DownloadError(f"Alpaca bars transport failed: {type(error).__name__}") from error
                self.sleep(min(2**attempt, 30))
            return RETRY

        return request_with_policy(
            request,
            opener=self.opener,
            timeout=REQUEST_TIMEOUT_SECONDS,
            before_attempt=before_attempt,
            read_response=lambda response: parse_response(response.read()),
            on_error=on_error,
        )

    def fetch(
        self, symbol: str, adjustment: str, start: datetime, end: datetime, *, asof: str | None = None
    ) -> CompletedBars:
        need = {
            "kind": "bars",
            "why": "boundary_check" if asof is not None else "history_download",
            "symbols": [symbol],
            "adjustments": [adjustment],
            "asof": asof if asof is not None else end.date().isoformat(),
            "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
        }
        return self.execute_plan(build_plan({"steps": [{"needs": [need]}]}))[(0, 0, 0, symbol, adjustment)]

    def execute_plan(self, plan, *, metrics=None, on_failure=None, on_completed=None):
        return execute_bar_plan(
            plan, self._get, metrics=metrics, on_failure=on_failure, on_completed=on_completed
        )


def execute_bar_plan(plan, get_page, *, metrics=None, on_failure=None, on_completed=None):
    """Execute the solver's ordered bar actions and route only requested slices."""
    if plan.get("schema_version") != PLAN_VERSION:
        raise DownloadError("unsupported API action-plan version")
    strategies = {"bars_single_range", "bars_multi_range", "bars_boundary_batch"}
    for step in plan["steps"]:
        for action in step["actions"]:
            if action["strategy"] not in strategies or action["request"]["url"] != BAR_URL:
                raise DownloadError("bar executor received a different API strategy")
    metrics = metrics if metrics is not None else {}
    metrics.setdefault("page_requests", 0)
    metrics.setdefault("oversize_splits", 0)
    output = {}
    failed_steps = set()

    def execute(step_index, action):
        parameters = dict(action["request"]["params"])

        def fetch_batch(params):
            bars = {symbol: [] for symbol in params["symbols"].split(",")}

            def page_error(error, has_pages):
                if isinstance(error, BarsSymbolUnavailable) and has_pages:
                    raise DownloadError("symbol rejected after a successful pricing page") from error

            for document in paginated_pages(
                params,
                action["pagination"],
                get_page,
                error_type=DownloadError,
                metrics=metrics,
                on_error=page_error,
            ):
                values = document.get("bars") if isinstance(document, dict) else None
                if not isinstance(values, dict) or set(values) - bars.keys():
                    raise DownloadError("bar response does not belong to the requested ticker set")
                for symbol, rows in values.items():
                    if not isinstance(rows, list):
                        raise DownloadError("provider bars are not an array")
                    bars[symbol].extend(rows)
            start = timestamp(params["start"])
            end = timestamp(params["end"]) + timedelta(microseconds=1)
            # Normalize only completed batches, retaining them privately until the
            # whole original action succeeds. Consumers never see partial success.
            return {
                symbol: CompletedBars(
                    symbol,
                    params["adjustment"],
                    start,
                    end,
                    params["asof"],
                    transform(rows, start, end),
                    singleton="," not in params["symbols"],
                )
                for symbol, rows in bars.items()
            }

        def status_code(error):
            cause = error.__cause__ or error
            return cause.code if isinstance(cause, urllib.error.HTTPError) else None

        try:
            normalized = {
                symbol: rows
                for batch in fetch_symbol_batches(
                    parameters,
                    fetch_batch,
                    status_code=status_code,
                    metrics=metrics,
                )
                for symbol, rows in batch.items()
            }
        except (DownloadError, OSError) as error:
            if isinstance(error, BarsSymbolUnavailable) and "," in parameters["symbols"]:
                incomplete = DownloadError(f"Alpaca bars action incomplete: member {error.symbol} was rejected")
                incomplete.http_status = error.http_status
                if on_failure is None:
                    raise incomplete from error.__cause__
                error = incomplete
            if on_failure is None:
                raise
            failed_steps.add(step_index)
            on_failure(step_index, action, error)
            return
        adjustment = parameters["adjustment"]
        # After every page completes, omitted symbols have no bars in this range.
        # Never fan normal omissions out into exact requests: empty/inactive labels
        # would multiply one batch into symbols x adjustment views requests.
        # Transport, schema and pagination failures above still fail the action.
        delivered = {}
        for target in action["deliver"]:
            lower, upper = timestamp(target["start"]), timestamp(target["end_exclusive"])
            for symbol in target["symbols"]:
                delivered[(step_index, target["need"], target["range"], symbol, adjustment)] = normalized[
                    symbol
                ].select(symbol, adjustment, lower, upper, parameters["asof"])
        output.update(delivered)
        if on_completed is not None:
            # Outside the transport handler: a failed durable handoff must stop
            # execution, never masquerade as a failed provider action.
            on_completed(step_index, action, delivered)

    for step_index, step in enumerate(plan["steps"]):
        for action in step["actions"]:
            execute(step_index, action)
        # Callers that continue independent failures must not execute a dependent step.
        if step_index in failed_steps:
            break
    return output

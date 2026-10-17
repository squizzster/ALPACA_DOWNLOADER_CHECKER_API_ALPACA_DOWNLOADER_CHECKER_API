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

from .contract import BarsSymbolUnavailable, DownloadError

REQUESTS_PER_MINUTE = 199


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
        self.key = key
        self.secret = secret
        self.opener = opener or urllib.request.urlopen
        self.sleep = sleep
        self.wall_time = wall_time
        self.limiter = limiter if limiter is not None else RequestLimiter(sleep=sleep, monotonic=monotonic)

    def _get(self, parameters: dict) -> dict:
        request = urllib.request.Request(
            "https://data.alpaca.markets/v2/stocks/bars?" + urllib.parse.urlencode(parameters),
            headers={"APCA-API-KEY-ID": self.key, "APCA-API-SECRET-KEY": self.secret},
        )
        for attempt in range(6):
            # Every page, endpoint probe, and retry uses this same shared budget.
            self.limiter.acquire()
            try:
                with self.opener(request, timeout=60) as response:
                    return parse_response(response.read())
            except urllib.error.HTTPError as error:
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
                error.close()
                if unavailable:
                    raise BarsSymbolUnavailable(symbol, error.code) from error
                if not retry or attempt == 5:
                    raise DownloadError(
                        f"Alpaca bars request failed for {parameters.get('symbols')}/"
                        f"{parameters.get('adjustment')}: HTTP {error.code}"
                    ) from error
            except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException) as error:
                if attempt == 5:
                    raise DownloadError(f"Alpaca bars transport failed: {type(error).__name__}") from error
                self.sleep(min(2**attempt, 30))
        raise DownloadError("provider request exhausted retries")

    def fetch(
        self, symbol: str, adjustment: str, start: datetime, end: datetime, *, asof: str | None = None
    ) -> list[dict]:
        # Alpaca's end is inclusive; our durable coverage end is exclusive.
        parameters = {
            "symbols": symbol,
            "timeframe": "1Day",
            "feed": "sip",
            "currency": "USD",
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": (end - timedelta(microseconds=1)).isoformat().replace("+00:00", "Z"),
            # Short consistency probes must use the same symbol mapping as the update.
            "asof": asof if asof is not None else end.date().isoformat(),
            "adjustment": adjustment,
            "limit": 10000,
            "sort": "asc",
        }
        rows = []
        tokens = set()
        while True:
            try:
                document = self._get(parameters)
            except BarsSymbolUnavailable as error:
                if tokens:
                    raise DownloadError(f"symbol rejected after a successful pricing page: {symbol}") from error
                raise
            bars = document.get("bars")
            if not isinstance(bars, dict) or set(bars) - {symbol}:
                raise DownloadError("bar response does not belong to the requested ticker")
            values = bars.get(symbol, [])
            if not isinstance(values, list):
                raise DownloadError("provider bars are not an array")
            rows.extend(values)
            token = document.get("next_page_token")
            if token is None:
                return rows
            if not isinstance(token, str) or not token or token in tokens:
                raise DownloadError("invalid or repeated pagination token")
            tokens.add(token)
            parameters["page_token"] = token

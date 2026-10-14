import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from .settings import (
    ALPACA_BAR_TIMEFRAME,
    ALPACA_DATA_FEED,
    EARLIEST_REQUEST_TIMESTAMP,
    MIN_REQUEST_INTERVAL_SECONDS,
)


class AlpacaRequestRateLimiter:
    def __init__(self) -> None:
        self.next_request_monotonic = 0.0
        self._request_condition = threading.Condition()

    def wait_until_request_allowed(self) -> None:
        with self._request_condition:
            while True:
                delay_seconds = self.next_request_monotonic - time.monotonic()
                if delay_seconds <= 0:
                    self.next_request_monotonic = time.monotonic() + MIN_REQUEST_INTERVAL_SECONDS
                    return
                self._request_condition.wait(timeout=delay_seconds)

    def defer_requests_for_seconds(self, delay_seconds: float) -> None:
        with self._request_condition:
            self.next_request_monotonic = max(
                self.next_request_monotonic,
                time.monotonic() + delay_seconds,
            )
            self._request_condition.notify_all()


def rate_limit_delay_seconds(response_headers, default_seconds: float = 60.0) -> float:
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0) + 0.25
        except ValueError:
            pass

    reset_timestamp = response_headers.get("X-RateLimit-Reset")
    if reset_timestamp:
        try:
            return max(float(reset_timestamp) - time.time(), 0.0) + 0.25
        except ValueError:
            pass
    return default_seconds


def request_alpaca_daily_bars(
    symbol: str,
    adjustment_mode: str,
    api_key_id: str,
    api_secret_key: str,
    request_end_timestamp: str,
    rate_limiter: AlpacaRequestRateLimiter,
    *,
    request_start_timestamp: str = EARLIEST_REQUEST_TIMESTAMP,
) -> list[dict]:
    query_parameters = urllib.parse.urlencode(
        {
            "symbols": symbol,
            "timeframe": ALPACA_BAR_TIMEFRAME,
            "start": request_start_timestamp,
            "end": request_end_timestamp,
            "feed": ALPACA_DATA_FEED,
            "adjustment": adjustment_mode,
            "asof": request_end_timestamp[:10],
            "limit": 10000,
            "sort": "asc",
        }
    )
    http_request = urllib.request.Request(
        f"https://data.alpaca.markets/v2/stocks/bars?{query_parameters}",
        headers={
            "APCA-API-KEY-ID": api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key,
        },
    )

    for retry_attempt in range(6):
        rate_limiter.wait_until_request_allowed()
        try:
            with urllib.request.urlopen(http_request, timeout=60) as http_response:
                response_document = json.load(http_response)
                if http_response.headers.get("X-RateLimit-Remaining") == "0":
                    rate_limiter.defer_requests_for_seconds(rate_limit_delay_seconds(http_response.headers))
                break
        except urllib.error.HTTPError as http_error:
            if http_error.code != 429 or retry_attempt == 5:
                raise
            delay_seconds = rate_limit_delay_seconds(http_error.headers)
            http_error.close()
            print(
                f"{adjustment_mode}: rate limited; retrying in {delay_seconds:.2f}s",
                file=sys.stderr,
            )
            rate_limiter.defer_requests_for_seconds(delay_seconds)

    if response_document.get("next_page_token"):
        raise RuntimeError("response exceeded the single-request 10,000-bar limit")

    daily_bars_by_symbol = response_document.get("bars", {})
    if symbol in daily_bars_by_symbol:
        return daily_bars_by_symbol[symbol]
    if len(daily_bars_by_symbol) == 1:
        return next(iter(daily_bars_by_symbol.values()))
    return []

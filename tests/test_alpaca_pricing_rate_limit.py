import io
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from email.utils import formatdate
from urllib.error import HTTPError

from libs.alpaca_pricing_download.contract import DownloadError, failure_outcome, timestamp
from libs.alpaca_pricing_download.http import BarsClient, RequestLimiter, retry_after_seconds


class FakeClock:
    """Advance virtual time rather than sleeping or contacting the provider."""

    epoch = 1_800_000_000.0

    def __init__(self):
        self._lock = threading.Lock()
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        with self._lock:
            return self.now

    def wall_time(self):
        return self.epoch + self.monotonic()

    def sleep(self, seconds):
        with self._lock:
            self.sleeps.append(seconds)
            self.now += seconds

    def client(self, **kwargs):
        return BarsClient(
            "test", "test", sleep=self.sleep, monotonic=self.monotonic, wall_time=self.wall_time, **kwargs
        )


class PricingRateLimitTests(unittest.TestCase):
    def assert_budget(self, starts):
        starts = sorted(starts)
        for previous, current in zip(starts, starts[1:]):
            self.assertGreaterEqual(current - previous + 1e-10, 60 / 199)
        for previous, current in zip(starts, starts[199:]):
            self.assertGreaterEqual(current - previous + 1e-10, 60)

    def test_smooth_pacing_and_rolling_minute_budget_without_idle_bursts(self):
        clock = FakeClock()
        limiter = RequestLimiter(sleep=clock.sleep, monotonic=clock.monotonic)
        starts = [limiter.acquire() for _ in range(450)]
        self.assert_budget(starts)
        clock.sleep(600)
        after_idle = [limiter.acquire() for _ in range(3)]
        self.assert_budget(after_idle)
        self.assertAlmostEqual(after_idle[-1] - after_idle[0], 120 / 199)

    def test_three_concurrent_workers_share_one_budget(self):
        clock = FakeClock()
        limiter = RequestLimiter(sleep=clock.sleep, monotonic=clock.monotonic)
        barrier = threading.Barrier(3, timeout=5)

        def worker():
            barrier.wait()
            return [limiter.acquire() for _ in range(150)]

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(worker) for _ in range(3)]
            starts = [start for future in futures for start in future.result(timeout=5)]
        self.assertEqual(len(starts), 450)
        self.assert_budget(starts)

    def test_waiting_worker_rechecks_extended_shared_cooldown(self):
        clock = FakeClock()
        waiting, release = threading.Event(), threading.Event()

        def sleep(seconds):
            if not waiting.is_set():
                waiting.set()
                if not release.wait(5):
                    raise AssertionError("test waiter was not released")
            clock.sleep(seconds)

        limiter = RequestLimiter(sleep=sleep, monotonic=clock.monotonic)
        limiter.acquire()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(limiter.acquire)
            try:
                self.assertTrue(waiting.wait(5))
                limiter.defer(20)
                limiter.defer(1)  # A later, shorter response cannot shorten the pause.
            finally:
                release.set()
            self.assertEqual(future.result(timeout=5), 20)

    def test_long_cooldown_uses_bounded_sleeps_and_no_catch_up_burst(self):
        clock = FakeClock()
        limiter = RequestLimiter(sleep=clock.sleep, monotonic=clock.monotonic)
        limiter.defer(125)
        self.assertEqual(limiter.acquire(), 125)
        self.assertEqual(clock.sleeps, [60, 60, 5])
        self.assertAlmostEqual(limiter.acquire(), 125 + 60 / 199)

    def test_early_wakeup_does_not_admit_request(self):
        clock = FakeClock()
        wakes = []

        def sleep(seconds):
            wakes.append(seconds)
            if len(wakes) > 1:
                clock.sleep(seconds)

        limiter = RequestLimiter(sleep=sleep, monotonic=clock.monotonic)
        limiter.acquire()
        self.assertAlmostEqual(limiter.acquire(), 60 / 199)
        self.assertEqual(len(wakes), 2)

    def test_retry_after_accepts_seconds_and_http_dates_with_safe_fallback(self):
        now = FakeClock.epoch
        self.assertEqual(retry_after_seconds("12", now), 12)
        self.assertEqual(retry_after_seconds(formatdate(now + 90, usegmt=True), now), 90)
        self.assertEqual(retry_after_seconds(formatdate(now - 90, usegmt=True), now), 0)
        for value in (None, "", "garbage", "NaN", "inf", "-10", "1e999"):
            with self.subTest(value=value):
                self.assertEqual(retry_after_seconds(value, now), 60)

    def test_429_and_503_respect_retry_after(self):
        for status, header, delay in (
            (429, "12", 12),
            (429, formatdate(FakeClock.epoch + 90, usegmt=True), 90),
            (429, None, 60),
            (429, "malformed", 60),
            (429, "0", 60 / 199),
            (503, "7", 7),
        ):
            with self.subTest(status=status, header=header):
                clock = FakeClock()
                starts = []
                error = HTTPError("https://example.invalid", status, "retry", {"Retry-After": header}, None)

                def opener(*_args, **_kwargs):
                    starts.append(clock.monotonic())
                    if len(starts) == 1:
                        raise error
                    return io.BytesIO(b'{"bars":{}}')

                self.assertEqual(clock.client(opener=opener)._get({}), {"bars": {}})
                self.assertEqual(len(starts), 2)
                self.assertAlmostEqual(starts[1], delay)
                self.assertTrue(error.closed)

    def test_exhausted_429_keeps_other_ticker_paused(self):
        clock = FakeClock()
        starts = []

        def opener(*_args, **_kwargs):
            starts.append(clock.monotonic())
            if len(starts) <= 6:
                raise HTTPError("https://example.invalid", 429, "retry", {"Retry-After": "10"}, None)
            return io.BytesIO(b'{"bars":{}}')

        client = clock.client(opener=opener)
        with self.assertRaisesRegex(DownloadError, "HTTP 429"):
            client._get({"symbols": "FIRST"})
        client._get({"symbols": "SECOND"})
        self.assertEqual(starts, [0, 10, 20, 30, 40, 50, 60])

    def test_exhausted_batch_errors_stay_compact_and_keep_http_status(self):
        for count in (1, 2000, 8000):
            with self.subTest(symbols=count):
                clock = FakeClock()

                def opener(*_args, **_kwargs):
                    raise HTTPError("https://example.invalid", 504, "timeout", {"Retry-After": "0"}, None)

                client = clock.client(opener=opener)
                with self.assertRaises(DownloadError) as raised:
                    client._get({"symbols": ",".join(f"S{i}" for i in range(count)), "adjustment": "raw"})
                outcome = failure_outcome(raised.exception)
                self.assertEqual(outcome["http_status"], 504)
                self.assertEqual(outcome["outcome"], "request_failed")
                self.assertIn("S0/raw" if count == 1 else f"{count} symbols/raw", outcome["error"])
                self.assertLess(len(outcome["error"]), 200)
                self.assertEqual(client.request_count, 6)

    def test_zero_retry_after_cannot_bypass_shared_minute_budget(self):
        clock = FakeClock()
        starts = []

        def opener(*_args, **_kwargs):
            starts.append(clock.monotonic())
            if len(starts) % 2:
                raise HTTPError("https://example.invalid", 429, "retry", {"Retry-After": "0"}, None)
            return io.BytesIO(b'{"bars":{}}')

        client = clock.client(opener=opener)
        for _ in range(110):
            client._get({})
        self.assertEqual(len(starts), 220)
        self.assert_budget(starts)

    def test_network_requests_can_overlap_without_holding_limiter_mutex(self):
        clock = FakeClock()
        barrier = threading.Barrier(3, timeout=5)

        def opener(*_args, **_kwargs):
            barrier.wait()  # All three must enter I/O before any response can finish.
            return io.BytesIO(b'{"bars":{}}')

        client = clock.client(opener=opener)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(client._get, {"symbols": symbol}) for symbol in ("GOOG", "MSFT", "AAPL")]
            self.assertTrue(all(future.result(timeout=5) == {"bars": {}} for future in futures))

    def test_nonretryable_http_error_fails_after_one_attempt(self):
        clock = FakeClock()
        starts = []

        def opener(*_args, **_kwargs):
            starts.append(clock.monotonic())
            raise HTTPError("https://example.invalid", 403, "denied", {}, None)

        with self.assertRaisesRegex(DownloadError, "HTTP 403"):
            clock.client(opener=opener)._get({})
        self.assertEqual(starts, [0])

    def test_pagination_and_retries_use_the_same_budget(self):
        clock = FakeClock()
        starts = []
        responses = [
            HTTPError("https://example.invalid", 429, "retry", {"Retry-After": "0"}, None),
            b'{"bars":{"GOOG":[]},"next_page_token":"second"}',
            b'{"bars":{"GOOG":[]},"next_page_token":null}',
        ]

        def opener(*_args, **_kwargs):
            starts.append(clock.monotonic())
            response = responses.pop(0)
            if isinstance(response, HTTPError):
                raise response
            return io.BytesIO(response)

        client = clock.client(opener=opener)
        self.assertEqual(
            client.fetch(
                "GOOG", "raw", timestamp("2026-10-01T00:00:00Z"), timestamp("2026-10-14T00:00:00Z")
            ).rows,
            (),
        )
        self.assertEqual(len(starts), 3)
        self.assertEqual(client.request_count, len(starts))
        self.assert_budget(starts)

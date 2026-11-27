"""Transfer completed literal SIP auction coverage across changed ownership request ranges."""

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from libs.alpaca_data_api_solver import auction_request
from libs.alpaca_lineage_build.historical_listing import screen_range
from libs.alpaca_lineage_build.listing_backfill import _screen
from libs.alpaca_lineage_build.listing_observations import Observations
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError


def day(n):
    return f"2023-03-{n:02d}"


def page(symbol, dates, token=None):
    return {"auctions": {symbol: [{"d": day(n), "o": [], "c": []} for n in dates]}, "next_page_token": token}


class AuctionRangeTests(TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Path(tmp.name)
        (self.store / "end_of_day/lineage").mkdir(parents=True)
        self.http = Mock()
        self.cache = self.open_cache()

    def open_cache(self):
        session = Observations(self.store, lambda: self.http)
        self.addCleanup(session.close)
        return session

    def expect(self, calls):
        def request(url):
            symbol, start, stop, token, result = calls.pop(0)
            expected = auction_request(symbol, day(start), day(stop))
            actual = urlsplit(url)
            self.assertEqual(url.split("?")[0], expected["url"])
            params = {k: [str(v)] for k, v in expected["params"].items()}
            if token:
                params["page_token"] = [token]
            self.assertEqual(parse_qs(actual.query), params)
            if isinstance(result, BaseException):
                raise result
            return result

        self.http.request_json.side_effect = request
        return calls

    def test_extended_and_disjoint_coverage_requests_only_uncovered_market_dates_after_reopen(self):
        calls = self.expect(
            [
                ("T", 6, 8, None, page("T", [6], "P")),
                ("T", 6, 8, "P", page("T", [7, 8])),
                ("T", 13, 14, None, page("T", [])),  # native 200 with zero rows covers both dates
                ("T", 3, 3, None, page("T", [3])),
                ("T", 9, 10, None, page("T", [9, 10])),
                ("T", 15, 15, None, page("T", [15])),
            ]
        )
        self.cache.auction_days("T", day(6), day(8))
        self.cache.auction_days("T", day(13), day(14))
        reopened = self.open_cache()
        self.assertEqual(
            [r["d"] for r in reopened.auction_days("T", day(3), day(15))],
            list(map(day, [3, 6, 7, 8, 9, 10, 15])),
        )
        self.assertEqual(calls, [])
        self.http.request_json.side_effect = AssertionError("covered dates requested again")
        read = reopened.get

        def days_7_8_only(kind, key):
            if kind == "auction_page":
                self.assertEqual(key, ["T", day(6), day(8), "P"])
            return read(kind, key)

        with patch.object(reopened, "get", side_effect=days_7_8_only):
            self.assertEqual([r["d"] for r in reopened.auction_days("T", day(7), day(7))], [day(7)])
            self.assertEqual(reopened.auction_days("T", day(13), day(13)), [])

    def test_old_stage_screen_adopts_selected_physical_route_once_and_mapped_owner_is_irrelevant(self):
        for version, symbol in enumerate(("FIRST", "SECOND", "THIRD"), 1):
            key = [symbol, day(6), day(9)]
            self.cache.put(
                "auction_page", [*key, ""], {"rows": page(symbol, [6])["auctions"][symbol], "next": "P"}
            )
            self.cache.put(
                "auction_page", [*key, "P"], {"rows": page(symbol, [9])["auctions"][symbol], "next": None}
            )
            self.cache.put(
                "screen", key, {"schema_version": version, "auction_available": True, "candidates": [0, 3]}
            )
        self.http.request_json.side_effect = AssertionError("completed physical dates requested again")
        reopened = self.open_cache()
        for symbol in ("FIRST", "SECOND", "THIRD"):
            key = [symbol, day(6), day(9)]
            get = reopened.get
            reads = []

            def selected(kind, arg):
                self.assertEqual(arg[0], symbol)
                reads.append((kind, arg))
                return get(kind, arg)

            with patch.object(reopened, "get", side_effect=selected):
                self.assertEqual([r["d"] for r in reopened.auction_days(symbol, day(9), day(9))], [day(9)])
                self.assertEqual([arg for kind, arg in reads if kind == "screen"], [key])
                reads.clear()
                self.assertEqual([r["d"] for r in reopened.auction_days(symbol, day(6), day(6))], [day(6)])
                self.assertEqual([arg for kind, arg in reads if kind == "auction_page"], [[*key, ""]])
                self.assertFalse(any(kind == "screen" for kind, _ in reads))

    def test_old_missing_evidence_partial_403_and_404_never_establish_native_range_coverage(self):
        scope = ["T", day(6), day(9)]
        unavailable = {"schema_version": 2, "auction_available": False, "candidates": [0, 3]}
        self.cache.put("screen", scope, unavailable)
        self.cache.put("auction_page", [*scope, ""], {"unavailable": True})
        self.cache.put("auction_page", ["T", day(7), day(9), ""], {"rows": [], "next": "unfinished"})
        calls = self.expect(
            [
                ("T", 8, 8, None, IdentityHttpError(403, "forbidden")),
                ("T", 7, 8, None, page("T", [7])),
                ("T", 9, 9, None, IdentityHttpError(404, "unknown route")),
                ("T", 9, 10, None, page("T", [10])),
            ]
        )
        with self.assertRaises(IdentityHttpError):
            self.cache.auction_days("T", day(8), day(8))
        self.cache.auction_days("T", day(7), day(8))
        mixed = self.cache.auction_days("T", day(7), day(9))
        self.assertFalse(mixed.available)
        self.assertEqual([r["d"] for r in mixed], [day(7)])
        self.assertFalse(screen_range("T", day(7), day(9), mixed).auction_available)
        self.assertEqual([r["d"] for r in self.cache.auction_days("T", day(7), day(10))], [day(7), day(10)])
        self.assertEqual(calls, [])

    def test_exact_screen_skips_native_routes_and_missing_completed_legacy_page_fails_without_http(self):
        self.expect([("T", 6, 8, None, page("T", [6, 7]))])
        screen = _screen(self.cache, "T", day(6), day(8))
        with patch.object(self.cache, "auction_days", side_effect=AssertionError("complete screen reopened")):
            self.assertEqual(_screen(self.cache, "T", day(6), day(8)), screen)
        key = ["BROKEN", day(6), day(8)]
        self.cache.put("screen", key, {"schema_version": 2, "auction_available": True, "candidates": [0, 2]})
        with self.assertRaisesRegex(ValueError, "missing completed"):
            self.cache.auction_days("BROKEN", day(7), day(7))

    def test_changed_calendar_reuses_the_already_held_legacy_completion(self):
        symbol = "LEGACY"
        key = [symbol, day(6), day(7)]
        native = page(symbol, [6, 7])["auctions"][symbol]
        legacy = screen_range(*key, native).to_document()
        self.cache.put("screen", key, {**legacy, "schema_version": 2, "calendar_sha256": "previous"})
        self.cache.put("auction_page", [*key, ""], {"rows": native, "next": None})
        read = self.cache.get
        selected = []

        def once(kind, key):
            if kind == "screen":
                self.assertFalse(selected, "reopened selected legacy screen")
                selected.append(key)
            return read(kind, key)

        self.http.request_json.side_effect = AssertionError("reauthenticated physical coverage")
        with patch.object(self.cache, "get", side_effect=once):
            self.assertEqual(_screen(self.cache, *key).to_document(), legacy)
        self.assertEqual(selected, [key])

    def test_concurrent_wide_and_narrow_scopes_share_one_physical_request(self):
        started, release = Event(), Event()

        def fetch(_):
            started.set()
            self.assertTrue(release.wait(3))
            return page("T", [6, 7, 8, 9])

        self.http.request_json.side_effect = fetch
        with ThreadPoolExecutor(max_workers=2) as pool:
            wide = pool.submit(self.cache.auction_days, "T", day(6), day(9))
            self.assertTrue(started.wait(3))
            narrow = pool.submit(self.cache.auction_days, "T", day(7), day(8))
            release.set()
            self.assertEqual([r["d"] for r in wide.result()], list(map(day, [6, 7, 8, 9])))
            self.assertEqual([r["d"] for r in narrow.result()], list(map(day, [7, 8])))
        self.http.request_json.assert_called_once()

    def test_rejected_continuation_retries_only_that_token(self):
        calls = self.expect(
            [
                ("T", 6, 7, None, page("T", [6], "P")),
                ("T", 6, 7, "P", page("T", [7], "P")),
                ("T", 6, 7, "P", page("T", [7])),
            ]
        )
        with self.assertRaisesRegex(ValueError, "repeated historical auction page token"):
            self.cache.auction_days("T", day(6), day(7))
        self.assertFalse(self.cache.has("auction_route", ["T", day(6), day(7)]))
        copied = self.open_cache().auction_days("T", day(6), day(7))
        self.assertEqual([r["d"] for r in copied], list(map(day, [6, 7])))
        self.assertEqual(calls, [])

    def test_covered_weekend_gap_and_symbol_index_seek(self):
        calls = self.expect([("T", 10, 10, None, page("T", [10])), ("T", 13, 13, None, page("T", [13]))])
        for n in (10, 13):
            self.cache.auction_days("T", day(n), day(n))
        statements = []
        self.cache.db.set_trace_callback(statements.append)
        self.assertEqual(
            [r["d"] for r in self.cache.auction_days("T", day(10), day(13))], list(map(day, [10, 13]))
        )
        self.cache.db.set_trace_callback(None)
        self.assertEqual(calls, [])
        query = next(s for s in statements if "UNION ALL" in s)
        plan = [p[-1] for p in self.cache.db.execute("EXPLAIN QUERY PLAN " + query)]
        self.assertTrue(all("SCAN observations" not in p for p in plan))
        self.assertTrue(any("request>? AND request<?" in p for p in plan), plan)

    def test_staged_key_discovery_uses_only_keys_of_selected_rule_owners_symbols_and_starts(self):
        scope = ("pid", "T", day(6), day(9), False)
        expected = [1, "pid", "T", day(6), day(8), False, "binding"]
        excluded = [
            [1, "other", "T", day(6), day(8), False],
            [1, "pid", "TT", day(6), day(8), False],
            [2, "pid", "T", day(6), day(8), False],
            [1, "pid", "T", day(3), day(7), False],
            [1, "pid", "T", day(10), day(10), False],
        ]
        with self.cache.db:
            self.cache.db.executemany(
                "INSERT INTO observations VALUES('assessment',?,?)",
                [(json.dumps(key), b"unreadable body") for key in [expected, *excluded]],
            )
        self.assertEqual(self.cache.staged_assessment_keys(1, [scope, scope]), [expected])
        self.assertEqual(self.cache.assessment_key(1, scope)[0], 1)
        self.assertEqual(self.cache.assessment_key(2, scope)[0], 2)

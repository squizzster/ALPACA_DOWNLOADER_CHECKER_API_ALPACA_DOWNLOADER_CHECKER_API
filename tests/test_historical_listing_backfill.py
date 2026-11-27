import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from test_alpaca_end_of_day_api import NativeCase

from libs.alpaca_cli import build_parser
from libs.alpaca_data_api_solver import auction_request
from libs.alpaca_lineage_build.listing_backfill import _assess, _screen
from libs.alpaca_lineage_build.listing_observations import Observations, _auction_page
from libs.alpaca_lineage_build.listing_ranges import _slices


class HistoricalListingBackfillTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = Path(tmp.name)
        (self.store / "end_of_day/lineage").mkdir(parents=True)
        self.client = Mock()
        self.cache = Observations(self.store, lambda: self.client)
        self.addCleanup(self.cache.close)

    def test_partial_page_commits_before_retry(self):
        first, last = "2023-03-10", "2023-03-13"
        req = auction_request("TEST", first, last)["params"]
        self.assertEqual(req["asof"], "-")
        self.assertEqual(req["start"], "2023-03-10T05:00:00Z")
        self.assertEqual(req["end"], "2023-03-14T03:59:59.999999999Z")
        before = {"auctions": {"TEST": [{"d": first, "o": [], "c": None}]}, "next_page_token": "p2"}
        after = {"auctions": {}, "next_page_token": None}
        self.client.request_json.side_effect = [before, ConnectionError("interruption"), after]
        with self.assertRaises(ConnectionError):
            self.cache.auction_days("TEST", first, last)
        self.assertEqual(len(self.cache.auction_days("TEST", first, last)), 1)
        urls = [c.args[0] for c in self.client.request_json.call_args_list]
        self.assertEqual(len(urls), 3)
        self.assertTrue("page_token=" not in urls[0] and urls[1] == urls[2])
        self.assertIn("page_token=p2", urls[2])

    def test_complete_screen_and_unresolved_assessment_need_no_more_requests(self):
        self.client.request_json.return_value = {"auctions": None, "next_page_token": None}
        date = "2023-03-13"
        self.client.get_trade_tape_sample.return_value = {
            "symbol": "TEST",
            "session": date,
            "feed": "sip",
            "asof": "-",
            "http_status": 200,
            "outcome": "no_trades",
        }
        _screen(self.cache, "TEST", date, date)
        self.client.request_json.side_effect = AssertionError("completed auction screen reopened")
        scope = "pid", "TEST", date, date, False
        g = {"group_key": "g"}
        a = _assess((g, [scope]), self.cache)
        self.assertEqual(a[1]["pid"][0]["status"], "unresolved")
        self.client.get_trade_tape_sample.side_effect = AssertionError("completed trade request repeated")
        self.assertEqual(_assess((g, [scope]), self.cache), a)
        self.assertEqual(self.client.request_json.call_count, 1)
        self.assertEqual(self.client.get_trade_tape_sample.call_count, 1)

    def test_disjoint_ownership_and_covered_prefix(self):
        rows = {"OLD": [("a", "2023-03-01", "2023-03-14"), ("b", "2023-03-08", "2023-03-09")]}
        completed = [{"symbol": "OLD", "first_session": "2023-03-01", "last_session": "2023-03-06"}]
        parts = list(_slices("a", "OLD", "2023-03-01", "2023-03-14", rows, completed))
        self.assertEqual(
            parts,
            [
                ("2023-03-07", "2023-03-07", False),
                ("2023-03-08", "2023-03-09", True),
                ("2023-03-10", "2023-03-14", False),
            ],
        )
        self.cache.client_factory = Mock(side_effect=AssertionError("competing ownership queried"))
        row = _assess(({"group_key": "g"}, [("a", "OLD", *parts[1])]), self.cache)[1]["a"][0]
        self.assertEqual(row["limits"], ["competing_published_owners"])
        self.cache.client_factory.assert_not_called()

    def test_invalid_route_cannot_be_accepted(self):
        with self.assertRaises(ValueError):
            _auction_page(
                {"auctions": {"OTHER": []}, "next_page_token": None}, "TEST", "2023-03-01", "2023-03-02"
            )
        for op in ("--plan", "--dry-run", "--apply", "--stage-auctions", "--publish-staged"):
            args = build_parser().parse_args(["lineage", "backfill-listings", "--symbol", "T", op])
            self.assertEqual(args.symbol, ["T"])


class ListingWindowTests(unittest.TestCase):
    def test_one_native_page_serves_all_selected_owners_and_retry_uses_zero(self):
        from concurrent.futures import ThreadPoolExecutor
        from types import SimpleNamespace
        from unittest.mock import patch

        from libs.alpaca_lineage_build.listing_batch import prepare_points

        day = "2023-03-13"
        sec = "2023-03-13T20:00:00Z"
        labels = ["TEST" + chr(65 + i) for i in range(12)]
        screens = [
            SimpleNamespace(symbol=s, initial_sessions=[day], auction_seconds={day: [sec]}) for s in labels
        ]

        def native(symbols, session, client, *, second, regular_minute=False):
            self.assertEqual((session, second, regular_minute), (day, sec, False))
            self.assertEqual(set(symbols), set(labels))
            return {
                s: {
                    "symbol": s,
                    "session": day,
                    "returned_symbol": s,
                    "feed": "sip",
                    "asof": "-",
                    "http_status": 200,
                    "outcome": "observed",
                    "trade": {"t": sec, "z": "A"},
                }
                for s in symbols[:-1]
            }

        with tempfile.TemporaryDirectory() as location, ThreadPoolExecutor(max_workers=2) as pool:
            store = Path(location)
            (store / "end_of_day/lineage").mkdir(parents=True)
            native_client = Mock(request_count=1)
            cache = Observations(store, lambda: native_client)
            try:
                with patch(
                    "libs.alpaca_lineage_build.listing_batch.batch_trade_points", side_effect=native
                ) as call:
                    prepare_points(screens, cache, pool)
                    prepare_points(screens, cache, pool)
                self.assertEqual(call.call_count, 1)
                self.assertEqual(len(cache.retained_points), 11)
                self.assertIsNone(cache.known_point(labels[-1], day))
            finally:
                cache.close()

    def test_truncated_sparse_request_drops_every_already_observed_label(self):
        from unittest.mock import patch

        from libs.alpaca_lineage_build.listing_batch import _observe

        day = "2023-03-13"
        calls = []

        def native(labels, session, client):
            calls.append(list(labels))
            used = labels[:1] if len(calls) == 1 else labels
            return {
                s: {
                    "symbol": s,
                    "session": session,
                    "feed": "sip",
                    "asof": "-",
                    "http_status": 200,
                    "outcome": "no_trades",
                }
                for s in used
            }

        with tempfile.TemporaryDirectory() as location:
            store = Path(location)
            (store / "end_of_day/lineage").mkdir(parents=True)
            cache = Observations(store, Mock)
            try:
                args = ["FIRST", "SECOND", "THIRD"], day, "sparse_day"
                with patch("libs.alpaca_lineage_build.listing_batch.sparse_day_points", side_effect=native):
                    _observe(args, cache)
                    _observe(args, cache)
                self.assertEqual(calls, [["FIRST", "SECOND", "THIRD"], ["SECOND", "THIRD"]])
                self.assertEqual(cache.get_trade_tape_sample("SECOND", day)["outcome"], "no_trades")
            finally:
                cache.close()


class ListingMigrationCommandTests(NativeCase):
    def setUp(self):
        super().setUp()
        self.add_symbol("SECOND")
        self.bootstrap()

    def client(self):
        from urllib.parse import parse_qs, urlsplit

        client = Mock(request_count=0)

        def request(url):
            client.request_count += 1
            label = parse_qs(urlsplit(url).query)["symbols"][0]
            if label == "SECOND":
                return {"auctions": [], "next_page_token": None}
            return {"auctions": None, "next_page_token": None}

        def sample(symbol, session):
            client.request_count += 1
            return {
                "symbol": symbol,
                "returned_symbol": symbol,
                "session": session,
                "asof": "-",
                "feed": "sip",
                "http_status": 200,
                "outcome": "observed",
                "trade": {"t": session + "T15:00:00Z", "z": "A"},
            }

        client.request_json.side_effect = request
        client.get_trade_tape_sample.side_effect = sample
        return client

    def test_staged_apply_and_repeat_use_no_provider_or_historical_prices(self):
        from unittest.mock import patch

        from libs.alpaca_lineage_build.listing_backfill import run
        from libs.alpaca_serving_catalogue import storage

        pre = storage.selector_state(self.store)["generation"]
        offline = Mock(side_effect=AssertionError("established provider inputs queried"))
        args = {"symbols": ["TEST"], "workers": 2}
        with patch(
            "libs.alpaca_lineage_build.storage.load_source", side_effect=AssertionError("read historical prices")
        ):
            plan = run(self.store, plan=True, client_factory=offline, **args)
            self.assertEqual(plan["missing_assessments"], 1)
            preview = run(self.store, client_factory=self.client, **args)
            self.assertEqual(pre, storage.selector_state(self.store)["generation"])
            self.assertGreater(preview["provider_requests"], 0)
            applied = run(self.store, apply=True, client_factory=offline, **args)
            self.assertEqual(applied["assessments"], preview["assessments"])
            self.assertEqual(applied["provider_requests"], 0)
            self.assertEqual(applied["groups_published"], 1)
            repeat = run(self.store, apply=True, client_factory=offline, **args)
            self.assertEqual((repeat["groups_selected"], repeat["provider_requests"]), (0, 0))
            offline.assert_not_called()

    def test_one_invalid_provider_response_allows_independent_publication(self):
        from libs.alpaca_lineage_build.listing_backfill import run

        result = run(self.store, all_published=True, apply=True, client_factory=self.client)
        self.assertFalse(result["complete"])
        self.assertEqual((result["groups_published"], result["failed_groups"]), (1, 1))
        self.assertIn("SECOND", result["failures"])
        follow = run(self.store, all_published=True, plan=True, client_factory=lambda: None)
        self.assertEqual(follow["ranges_selected"], 1)

    def test_interruption_preserves_whole_groups_already_eligible_for_publication(self):
        from unittest.mock import patch

        from libs.alpaca_lineage_build.listing_backfill import run

        run(self.store, symbols=["TEST"], client_factory=self.client)

        def interrupted(screens, cache, pool, *, progress, checkpoint):
            checkpoint()
            raise KeyboardInterrupt()

        with patch("libs.alpaca_lineage_build.listing_batch.prepare_points", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                run(self.store, all_published=True, apply=True, client_factory=self.client)
        completed = run(self.store, symbols=["TEST"], plan=True, client_factory=None)
        pending = run(self.store, symbols=["SECOND"], plan=True, client_factory=None)
        self.assertEqual(completed["groups_selected"], 0)
        self.assertEqual(pending["groups_selected"], 1)

    def test_publish_staged_only_releases_completed_groups_without_credentials(self):
        from libs.alpaca_lineage_build.listing_backfill import run

        run(self.store, symbols=["TEST"], client_factory=self.client)
        disabled = Mock(side_effect=AssertionError("staged publication obtained credentials"))
        result = run(self.store, all_published=True, publish_staged=True, client_factory=disabled)
        disabled.assert_not_called()
        self.assertEqual(result["mode"], "publish_staged")
        self.assertEqual(result["groups_published"], 1)
        self.assertEqual(result["groups_waiting_for_assessment"], 1)
        self.assertFalse(result["complete"])

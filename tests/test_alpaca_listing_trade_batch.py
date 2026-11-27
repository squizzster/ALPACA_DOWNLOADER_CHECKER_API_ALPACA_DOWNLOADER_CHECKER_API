"""Restricted native SIP batch positives may stand in for dated listing points only."""

from unittest import TestCase
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

from libs.alpaca_data_api_solver.listing_trade_batch import listing_trade_request
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
from libs.alpaca_security_lineage.listing_trade_batch import (
    batch_trade_points,
    combine_listing_samples,
    sparse_day_points,
)

DAY = "2024-05-24"


def point(tape="A", stamp="2024-05-24T13:30:00.000000002Z"):
    return {"t": stamp, "z": tape}


class ListingTradeBatchTests(TestCase):
    def test_one_second_fixed_scope_timezone_uses_only_first_page(self):
        client = Mock()
        client.request_json.side_effect = [
            {"trades": {"ALPHA": [point()], "BETA": [point("C")]}, "next_page_token": "opaque"},
            AssertionError("followed a page token for a positive-only sample"),
        ]
        result = batch_trade_points(["GAMMA", "ALPHA", "BETA", "ALPHA"], DAY, client)
        self.assertEqual(set(result), {"ALPHA", "BETA"})
        self.assertEqual(result["BETA"]["trade"], point("C"))
        self.assertEqual(result["BETA"]["sample_scope"], "regular_open_second")
        urls = [urlsplit(c.args[0]) for c in client.request_json.call_args_list]
        self.assertEqual([u.path for u in urls], ["/v2/stocks/trades"])
        parsed = parse_qs(urls[0].query)
        self.assertEqual(
            parsed,
            {
                "symbols": ["ALPHA,BETA,GAMMA"],
                "feed": ["sip"],
                "asof": ["-"],
                "sort": ["asc"],
                "limit": ["10000"],
                "start": ["2024-05-24T13:30:00Z"],
                "end": ["2024-05-24T13:30:00.999999999Z"],
            },
        )
        winter = listing_trade_request(["ALPHA", "BETA"], "2024-11-29")
        self.assertEqual(winter["params"]["start"], "2024-11-29T14:30:00Z")

    def test_no_print_is_no_daywide_conclusion_and_sparse_page_never_requests_another(self):
        client = Mock()
        client.request_json.return_value = {
            "trades": {"ALPHA": [point("Z"), point()]},
            "next_page_token": "rest",
        }
        result = batch_trade_points(["ALPHA", "BETA", "GAMMA"], DAY, client)
        self.assertEqual(set(result), {"ALPHA"})
        self.assertNotIn("BETA", result)
        client.request_json.assert_called_once()
        for empty in (None, {"trades": None}, {"trades": {}}):
            client.request_json.return_value = empty
            self.assertEqual(batch_trade_points(["ALPHA", "BETA"], DAY, client), {})
        for labels in ([], ["ALPHA"], ["ALPHA", "ALPHA"]):
            self.assertEqual(batch_trade_points(labels, DAY, Mock(spec=[])), {})

    def test_rejected_response_routes_timestamps_pagination_and_entitlement(self):
        for document in (
            {"trades": {"WRONG": [point()]}},
            {"trades": {"ALPHA": [point(stamp="2024-05-24T13:30:01Z")]}},
            {"trades": {"ALPHA": [point(stamp="2024-05-24T13:29:59.999999999Z")]}},
            {"trades": {"ALPHA": [point(2)]}},
            {"trades": []},
            {"trades": {}, "next_page_token": []},
            {},
        ):
            with self.subTest(document=document):
                client = Mock()
                client.request_json.return_value = document
                with self.assertRaises(RuntimeError):
                    batch_trade_points(["ALPHA", "BETA"], DAY, client)
        client = Mock()
        client.request_json.side_effect = IdentityHttpError(403, "subscription denied")
        with self.assertRaises(IdentityHttpError):
            batch_trade_points(["ALPHA", "BETA"], DAY, client)

    def test_url_injection_fails_before_http(self):
        with self.assertRaises(ValueError):
            batch_trade_points(["ALPHA", "../trades?feed=otc"], DAY, Mock(spec=[]))
        with self.assertRaises(ValueError):
            batch_trade_points([f"EXAMPLE{i}" for i in range(49)], DAY, Mock(spec=[]))

    def test_conflicting_recognized_prints_on_one_bounded_page_have_one_witness_per_tape(self):
        client = Mock()
        a = point("A")
        c = point("C", "2024-05-24T13:30:00.000000009Z")
        client.request_json.return_value = {
            "trades": {"ALPHA": [a, point("A", "2024-05-24T13:30:00.000000005Z"), c], "BETA": [c]},
            "next_page_token": "other-data",
        }
        result = batch_trade_points(["ALPHA", "BETA"], DAY, client)
        conflict = result["ALPHA"]
        self.assertEqual(conflict["outcome"], "conflicting_tapes")
        self.assertEqual(conflict["conflicting_trades"], [a, c])
        self.assertNotIn("trade", conflict)
        self.assertEqual(result["BETA"]["trade"], c)
        client.request_json.assert_called_once()

    def test_combined_literal_observations_preserve_conflict_and_reject_different_mapping_or_scope(self):
        base = {
            "symbol": "ALPHA",
            "session": DAY,
            "http_status": 200,
            "feed": "sip",
            "asof": "-",
            "returned_symbol": "ALPHA",
            "outcome": "observed",
        }
        a, b, c = ({**base, "trade": point(z)} for z in ("A", "B", "C"))
        conflict = combine_listing_samples(a, c)
        self.assertEqual(conflict, combine_listing_samples(c, a))
        self.assertEqual(conflict["conflicting_trades"], [a["trade"], c["trade"]])
        self.assertIs(combine_listing_samples(conflict, c), conflict)
        expanded = combine_listing_samples(conflict, b)
        self.assertEqual(expanded["conflicting_trades"], [a["trade"], b["trade"], c["trade"]])
        self.assertIs(combine_listing_samples(a, {**a, "trade": point("A", DAY + "T15:00:00Z")}), a)
        self.assertIs(combine_listing_samples(None, a), a)
        negative = {**base, "outcome": "no_trades"}
        self.assertIs(combine_listing_samples(negative, conflict), conflict)
        self.assertIs(combine_listing_samples(conflict, negative), conflict)
        unknown = {**base, "trade": {"t": DAY + "T17:00:00Z"}}
        self.assertIs(combine_listing_samples(negative, unknown), unknown)
        self.assertIs(combine_listing_samples(unknown, negative), unknown)
        for changed in ({"session": "2024-05-28"}, {"asof": DAY}, {"feed": "otc"}, {"returned_symbol": "OTHER"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                combine_listing_samples(a, {**c, **changed})

    def test_selected_auction_second_and_cohort_minute_have_distinct_literal_scope(self):
        client = Mock()
        close = "2024-05-24T20:00:00Z"
        client.request_json.return_value = {"trades": {"ALPHA": [point("B", close)]}}
        selected = batch_trade_points(["ALPHA", "BETA"], DAY, client, second=close)
        self.assertEqual(
            (selected["ALPHA"]["sample_scope"], selected["ALPHA"]["start"]), ("auction_second", close)
        )
        labels = ["ALPHA", *[f"S{i}" for i in range(7)]]
        client.request_json.return_value = {"trades": {"ALPHA": [point("A", "2024-05-24T13:30:59.999999999Z")]}}
        minute = batch_trade_points(labels, DAY, client, regular_minute=True)["ALPHA"]
        self.assertEqual(minute["sample_scope"], "regular_open_minute")
        self.assertEqual(minute["end"], "2024-05-24T13:30:59.999999999Z")
        self.assertEqual(batch_trade_points(labels[:-1], DAY, Mock(spec=[]), regular_minute=True), {})
        for flags in (
            {"regular_minute": True, "second": close},
            {"second": "2024-05-24T02:30:00Z"},  # still May 23 in New York
            {"second": "2024-05-24T20:00:00.000000009Z"},
        ):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                batch_trade_points(labels, DAY, Mock(spec=[]), **flags)

    def test_selected_auction_window_admits_only_ordered_whole_seconds_within_one_new_york_date(self):
        labels = ["ALPHA", "BETA"]
        client = Mock()
        first, last = DAY + "T15:53:22Z", DAY + "T15:54:21Z"
        client.request_json.side_effect = [
            {"trades": {"ALPHA": [point("B", DAY + "T15:54:21.999999999Z")]}, "next_page_token": "ignored"},
            AssertionError("retrieved a second page for a bounded witness"),
        ]
        row = batch_trade_points(labels, DAY, client, window_start=first, window_end=last)["ALPHA"]
        self.assertEqual(
            (row["sample_scope"], row["start"], row["end"]),
            ("auction_window", first, DAY + "T15:54:21.999999999Z"),
        )
        client.request_json.assert_called_once()
        for flags in (
            {"window_start": first},
            {"window_end": last},
            {"window_start": last, "window_end": first},
            {"window_start": first, "window_end": DAY + "T15:54:22Z"},
            {"window_start": "2024-05-25T03:59:50Z", "window_end": "2024-05-25T04:00:10Z"},
            {"window_start": first, "window_end": last, "second": first},
        ):
            with self.subTest(flags=flags), self.assertRaises(ValueError):
                batch_trade_points(labels, DAY, Mock(spec=[]), **flags)

    def test_sparse_terminal_native_page_establishes_empty_unknown_and_conflicting_occupations(self):
        client = Mock()
        client.request_json.return_value = {
            "trades": {
                "ALPHA": [point("A"), point("C")],
                "BETA": [{"t": DAY + "T16:00:01Z"}],
                "GAMMA": [],
            },
            "next_page_token": None,
        }
        labels = ["ALPHA", "BETA", "GAMMA", "DELTA"]
        result = sparse_day_points(labels, DAY, client)
        self.assertEqual(set(result), set(labels))
        self.assertEqual(result["ALPHA"]["outcome"], "conflicting_tapes")
        self.assertEqual(result["BETA"]["trade"], {"t": DAY + "T16:00:01Z"})
        self.assertEqual(result["BETA"]["outcome"], "observed")
        for symbol in ("GAMMA", "DELTA"):
            self.assertEqual(result[symbol]["outcome"], "no_trades")
            self.assertNotIn("trade", result[symbol])
        self.assertTrue(
            all(row["page_complete"] and row["sample_scope"] == "sparse_day" for row in result.values())
        )
        params = parse_qs(urlsplit(client.request_json.call_args.args[0]).query)
        self.assertEqual(
            (params["limit"], params["start"], params["end"], params["asof"], params["feed"]),
            (["8"], [DAY + "T04:00:00Z"], ["2024-05-25T03:59:59.999999999Z"], ["-"], ["sip"]),
        )
        client.request_json.assert_called_once()

    def test_sparse_truncated_page_touches_remaining_labels_only_on_a_new_selection_and_never_follows_token(
        self,
    ):
        labels = {"ALPHA", "BETA", "DELTA", "GAMMA"}
        client = Mock()
        client.request_json.side_effect = [
            {"trades": {"ALPHA": [point("Z")], "BETA": [point("B")]}, "next_page_token": "keep-paging"},
            {"trades": None, "next_page_token": None},
            AssertionError("repeated a completed sparse route or followed full-day trades"),
        ]
        first = sparse_day_points(labels, DAY, client)
        self.assertEqual(set(first), {"ALPHA", "BETA"})
        self.assertEqual(first["ALPHA"]["outcome"], "observed")
        self.assertFalse(first["BETA"]["page_complete"])
        remaining = sparse_day_points(labels - first.keys(), DAY, client)
        self.assertEqual(
            {s: r["outcome"] for s, r in remaining.items()}, {"DELTA": "no_trades", "GAMMA": "no_trades"}
        )
        params = [parse_qs(urlsplit(c.args[0]).query) for c in client.request_json.call_args_list]
        self.assertEqual([r["symbols"] for r in params], [["ALPHA,BETA,DELTA,GAMMA"], ["DELTA,GAMMA"]])
        self.assertEqual([r["limit"] for r in params], [["8"], ["4"]])
        self.assertTrue(all("page_token" not in r for r in params))
        self.assertEqual(sparse_day_points(["ONLY"], DAY, Mock(spec=[])), {})

    def test_sparse_empty_nonterminal_malformed_overfull_and_denied_are_operational(self):
        labels = ["ALPHA", "BETA"]
        client = Mock()
        for document in (
            {"trades": None, "next_page_token": "more"},
            {"trades": {"ALPHA": [{"z": "Z"}]}},
            {"trades": {"OTHER": []}},
            {"trades": {"BETA": [point()] * 5}},
            {"trades": {"ALPHA": [point(), point("A", DAY + "T02:00:00Z")]}},
        ):
            with self.subTest(document=document):
                client.request_json.return_value = document
                with self.assertRaises(RuntimeError):
                    sparse_day_points(labels, DAY, client)
        client.request_json.return_value = None  # allow_not_found=True contract
        self.assertEqual(sparse_day_points(labels, DAY, client), {})
        client.request_json.side_effect = IdentityHttpError(403, "forbidden")
        with self.assertRaises(IdentityHttpError):
            sparse_day_points(labels, DAY, client)

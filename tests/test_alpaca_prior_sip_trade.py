"""A bounded mapped SIP search returns one last native trade with its actual date."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from test_alpaca_pipeline_evidence import transport

from libs.alpaca_data_api_solver import build_plan, trade_tape_request
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError


class PriorSipTransportTests(TestCase):
    def setUp(self):
        self.api = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        self.symbol = "ARCHIVE.A"
        self.start, self.through, self.asof = "2025-03-07", "2025-03-10", "2025-07-01"
        self.need = {
            "kind": "historical_trade_tape",
            "why": "dated_ticker_tape",
            "symbol": self.symbol,
            "session": self.start,
            "through_session": self.through,
            "asof": self.asof,
            "feed": "sip",
            "sort": "desc",
        }
        self.trade = {"t": "2025-03-08T00:10:39.123456789Z", "x": "P", "z": "A", "s": 1, "c": ["N", "I"]}

    def sample(self, *, through=None):
        return self.api.get_trade_tape_sample(
            self.symbol,
            self.start,
            through_session=through or self.through,
            asof=self.asof,
            feed="sip",
            sort="desc",
        )

    def test_one_mapped_sip_page_preserves_native_trade_even_when_utc_and_local_date_differ(self):
        plan = build_plan({"steps": [{"needs": [self.need, dict(self.need)]}]})
        (action,) = plan["steps"][0]["actions"]
        self.assertEqual(action["deliver"], [{"need": 0}, {"need": 1}])
        self.assertNotIn("pagination", action)
        with patch.object(
            self.api,
            "request_json",
            return_value={"symbol": self.symbol, "trades": [self.trade], "next_page_token": "older-page"},
        ) as get:
            result = self.sample()
        get.assert_called_once()
        native = urlsplit(get.call_args.args[0])
        self.assertEqual((native.netloc, native.path), ("data.alpaca.markets", "/v2/stocks/ARCHIVE.A/trades"))
        self.assertEqual(
            parse_qs(native.query),
            {
                "asof": [self.asof],
                "feed": ["sip"],
                "start": ["2025-03-07T05:00:00Z"],
                "end": ["2025-03-11T03:59:59.999999999Z"],
                "sort": ["desc"],
                "limit": ["1"],
            },
        )
        self.assertEqual(result["trade"], self.trade)
        self.assertEqual(
            (
                result["symbol"],
                result["session"],
                result["through_session"],
                result["observed_session"],
                result["asof"],
                result["sort"],
                result["feed"],
                result["tape_network"],
            ),
            (self.symbol, self.start, self.through, "2025-03-07", self.asof, "desc", "sip", "CTA network A"),
        )
        self.assertEqual(
            (result["start"], result["end"], result["limit"]),
            (
                "2025-03-07T05:00:00Z",
                "2025-03-11T03:59:59.999999999Z",
                1,
            ),
        )

    def test_incomplete_empty_denied_and_transient_responses_retain_distinct_outcomes(self):
        for trades in (None, []):
            doc = {"symbol": self.symbol, "trades": trades, "next_page_token": None}
            with self.subTest(trades=trades), patch.object(self.api, "request_json", return_value=doc):
                empty = self.sample(through=self.start)
                self.assertEqual(
                    (empty["outcome"], empty["http_status"], empty["sort"], empty["through_session"]),
                    ("no_trades", 200, "desc", self.start),
                )
                self.assertNotIn("observed_session", empty)
                self.assertNotIn("trade", empty)
                doc["next_page_token"] = "another-page"
                with self.assertRaisesRegex(RuntimeError, "empty first trade page"):
                    self.sample(through=self.start)
        for status in (403, 404, 503):
            with (
                self.subTest(status=status),
                patch.object(self.api, "request_json", side_effect=IdentityHttpError(status, "native status")),
            ):
                if status == 503:
                    with self.assertRaises(IdentityHttpError):
                        self.sample()
                else:
                    denied = self.sample()
                    self.assertEqual(
                        (denied["http_status"], denied["outcome"], denied["sort"]),
                        (status, "unavailable", "desc"),
                    )
                    self.assertNotIn("observed_session", denied)

    def test_preserves_optional_size_conditions_or_absence_and_rejects_malformed_shapes(self):
        minimal = {"t": self.trade["t"]}
        for optional in (
            {},
            {"p": 13.7425, "s": 1, "c": ["I"]},
            {"s": 0, "c": []},
            {"s": 2**58 + 1, "c": ["@", "I", "@"]},
            {"s": 0.25},
            {"s": None, "c": None},
        ):
            trade = {**minimal, **optional}
            with (
                self.subTest(optional=optional),
                patch.object(self.api, "request_json", return_value={"symbol": self.symbol, "trades": [trade]}),
            ):
                self.assertEqual(self.sample()["trade"], trade)
        for optional in (
            {"p": "12.50"},
            {"p": float("inf")},
            {"s": True},
            {"s": "1"},
            {"s": -1},
            {"s": float("nan")},
            {"s": float("inf")},
            {"c": "NI"},
            {"c": [1]},
        ):
            with (
                self.subTest(optional=optional),
                patch.object(
                    self.api,
                    "request_json",
                    return_value={"symbol": self.symbol, "trades": [{**minimal, **optional}]},
                ),
                self.assertRaises(RuntimeError),
            ):
                self.sample()

    def test_only_authorized_range_directions_and_mappings_can_reach_the_credentialed_destination(self):
        for change in (
            {"asof": "-"},
            {"feed": "otc", "asof": "-"},
            {"sort": "asc"},
            {"sort": "other"},
            {"through_session": "2025-03-01"},
            {"through_session": None},
            {"sort": False},
            {"session": "2025-03-07T12:00:00Z"},
            {"symbol": "OTHER/../../trades"},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                build_plan({"steps": [{"needs": [{**self.need, **change}]}]})
        with self.assertRaises(ValueError):
            trade_tape_request(self.symbol, self.start, self.asof, "sip", sort="desc")
        plan = build_plan({"steps": [{"needs": [self.need]}]})
        for field, value in (
            ("url", "https://elsewhere.invalid/trades"),
            ("sort", "asc"),
            ("feed", "otc"),
            ("limit", 25),
            ("end", "2030-01-01"),
            ("asof", "-"),
            ("page_token", "tampered"),
        ):
            bad = deepcopy(plan)
            request = bad["steps"][0]["actions"][0]["request"]
            (request if field == "url" else request["params"])[field] = value
            with (
                self.subTest(field=field),
                patch.object(self.api, "request_json", side_effect=AssertionError("unauthorized request")),
                self.assertRaises(ValueError),
            ):
                self.api.execute_plan(bad)
        bad = deepcopy(plan)
        bad["steps"][0]["actions"][0]["pagination"] = {"parameter": "page_token"}
        with (
            patch.object(self.api, "request_json", side_effect=AssertionError("unbounded request")),
            self.assertRaises(ValueError),
        ):
            self.api.execute_plan(bad)

    def test_handoff_preserves_distinct_prior_sip_point_and_forward_otc_observations(self):
        cache = EvidenceClient(self.api)
        base = {"asof": self.asof, "mapping_asset_id": "same-issue", "feed": "sip"}
        descending = {**base, "sort": "desc", "through_session": self.through}
        forward = {"feed": "otc", "through_session": self.through}
        with patch.object(
            self.api, "request_json", return_value={"symbol": self.symbol, "trades": []}
        ) as requests:
            held = [
                cache.get_trade_tape_sample(self.symbol, self.start, **scope)
                for scope in (base, forward, descending)
            ]
            self.assertEqual(requests.call_count, 3)
            self.assertEqual(
                set(cache.trade_tape_samples),
                {
                    (self.symbol, self.start, self.asof, "same-issue", "sip"),
                    (self.symbol, self.start, "-", None, "otc", self.through),
                    (self.symbol, self.start, self.asof, "same-issue", "sip", self.through, "desc"),
                },
            )
            restored, originals = transport(cache, extra=held)
            for query, original in zip((base, forward, descending), originals, strict=True):
                self.assertIs(restored.get_trade_tape_sample(self.symbol, self.start, **query), original)
            for change in (
                {"mapping_asset_id": "different-issue"},
                {"asof": "2025-04-01"},
                {"through_session": "2025-03-09"},
            ):
                cache.get_trade_tape_sample(self.symbol, self.start, **{**descending, **change})
            self.assertEqual(requests.call_count, 6)
            with self.assertRaises(ValueError):
                cache.get_trade_tape_sample(self.symbol, self.start, **{**descending, "mapping_asset_id": None})
            self.assertEqual(requests.call_count, 6)

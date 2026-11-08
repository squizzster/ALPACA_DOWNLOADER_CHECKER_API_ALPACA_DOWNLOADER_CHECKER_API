"""Exercise the real endpoint adapters through synthetic physical HTTP responses."""

import copy
import io
import json
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_pricing_download.contract import DownloadError, failure_outcome
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.live_check import verify
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError
from libs.common.provider_http import balanced_batches
from tests.test_alpaca_data_api_solver import need


def response(document):
    result = io.BytesIO(json.dumps(document).encode())
    result.headers = {}
    return result


def symbols(count):
    return [f"S{i:04}" for i in range(count)]


def action_need(roster):
    return {
        "kind": "corporate_actions",
        "why": "lineage",
        "symbols": roster,
        "start": "2026-10-10",
        "end": "2026-10-12",
    }


def row():
    return {"t": "2026-10-10T04:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 2, "n": 1, "vw": 1}


class HttpBatchingTests(unittest.TestCase):
    def run_client(self, owner, count, handler, *, metrics=None, on_failure=None, dependent=False):
        roster = symbols(count)
        item = need(roster) if owner == "bars" else action_need(roster)
        plan = build_plan({"steps": [{"needs": [item]}] + ([{"needs": [item]}] if dependent else [])})
        original = copy.deepcopy(plan)
        calls, limiter = [], Mock()

        def opener(request, *, timeout):
            params = {key: values[0] for key, values in parse_qs(urlsplit(request.full_url).query).items()}
            self.assertEqual(timeout, 300 if owner == "bars" else 60)
            self.assertEqual(request.get_header("Apca-api-key-id"), "test")
            self.assertEqual(request.get_header("Apca-api-secret-key"), "secret")
            self.assertLessEqual(len(params["symbols"].split(",")), 1000)
            calls.append(params)
            return handler(params, calls)

        client = (
            BarsClient("test", "secret", opener=opener, limiter=limiter)
            if owner == "bars"
            else AlpacaIdentityClient("test", "secret", rate_limiter=limiter)
        )
        try:
            with patch("libs.alpaca_security_lineage.alpaca_http.open_provider_request", side_effect=opener):
                result = client.execute_plan(
                    plan, metrics=metrics, **({"on_failure": on_failure} if owner == "bars" else {})
                )
            return result, calls
        finally:
            self.assertEqual(plan, original)
            self.assertEqual(client.request_count, len(calls))
            acquire = limiter.acquire if owner == "bars" else limiter.wait_until_request_allowed
            self.assertEqual(acquire.call_count, len(calls))

    def test_balanced_boundaries_preserve_every_symbol_once(self):
        for count, expected in (
            (0, []),
            (1, [1]),
            (999, [999]),
            (1000, [1000]),
            (1001, [500, 501]),
            (2001, [667] * 3),
            (3386, [846, 846, 847, 847]),
        ):
            with self.subTest(count=count):
                roster = symbols(count)
                batches = balanced_batches(roster)
                self.assertEqual([len(batch) for batch in batches], expected)
                self.assertEqual([s for batch in batches for s in batch], roster)
        for invalid in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                balanced_batches(["A"], invalid)

    def test_real_clients_balance_paginate_retry_and_keep_parameters(self):
        for owner in ("bars", "metadata"):
            for count in (1, 1000, 1001, 2001, 3386):
                with self.subTest(owner=owner, count=count):
                    metrics, rejected = {}, set()

                    def handler(params, calls):
                        roster = params["symbols"].split(",")
                        # A retry in every batch, followed by two pages using the
                        # same token text across batches (tokens are batch-local).
                        if params["symbols"] not in rejected:
                            rejected.add(params["symbols"])
                            raise HTTPError("test", 429, "retry", {"Retry-After": "0"}, io.BytesIO())
                        self.assertEqual(
                            params["start"], "2026-10-10T04:00:00Z" if owner == "bars" else "2026-10-10"
                        )
                        self.assertEqual(
                            params["end"], "2026-10-12T03:59:59.999999Z" if owner == "bars" else "2026-10-12"
                        )
                        if owner == "bars":
                            self.assertEqual(
                                {
                                    k: params[k]
                                    for k in (
                                        "asof",
                                        "adjustment",
                                        "feed",
                                        "currency",
                                        "timeframe",
                                        "limit",
                                        "sort",
                                    )
                                },
                                {
                                    "asof": "2026-10-15",
                                    "adjustment": "all",
                                    "feed": "sip",
                                    "currency": "USD",
                                    "timeframe": "1Day",
                                    "limit": "10000",
                                    "sort": "asc",
                                },
                            )
                            data = (
                                {"bars": {s: [row()] for s in roster}}
                                if "page_token" in params
                                else {"bars": {}}
                            )
                        else:
                            self.assertEqual(
                                {k: params[k] for k in ("region", "limit", "data_quality", "sort")},
                                {"region": "us", "limit": "1000", "data_quality": "complete", "sort": "asc"},
                            )
                            data = (
                                {"corporate_actions": {"cash_dividends": [{"symbol": s} for s in roster]}}
                                if "page_token" in params
                                else {"corporate_actions": {}}
                            )
                        return response({**data, "next_page_token": None if "page_token" in params else "next"})

                    result, calls = self.run_client(owner, count, handler, metrics=metrics)
                    batches = balanced_batches(symbols(count))
                    self.assertEqual([c["symbols"].split(",") for c in calls[::3]], batches)
                    self.assertEqual([c.get("page_token") for c in calls], [None, None, "next"] * len(batches))
                    self.assertEqual(
                        metrics,
                        {"batches": len(batches), "oversize_splits": 0, "page_requests": 2 * len(batches)},
                    )
                    if owner == "bars":
                        self.assertEqual({k[3] for k in result}, set(symbols(count)))
                        self.assertTrue(all(len(value.rows) == 1 for value in result.values()))
                    else:
                        self.assertEqual(
                            result, [[[{"symbol": s, "action_type": "cash_dividend"} for s in symbols(count)]]]
                        )

    def test_empty_histories_complete_without_single_symbol_fanout(self):
        for owner in ("bars", "metadata"):
            key = "bars" if owner == "bars" else "corporate_actions"
            result, calls = self.run_client(owner, 3386, lambda *_: response({key: {}}))
            self.assertEqual(len(calls), 4)
            if owner == "bars":
                self.assertEqual(len(result), 3386)
                self.assertTrue(all(not value.rows for value in result.values()))
            else:
                self.assertEqual(result, [[[]]])

    def test_later_batch_failures_preserve_completed_independent_subsets(self):
        for owner in ("bars", "metadata"):
            for failure in ("http", "json", "shape", "token", "repeat"):
                with self.subTest(owner=owner, failure=failure):
                    calls_seen = []
                    key = "bars" if owner == "bars" else "corporate_actions"

                    def handler(params, calls):
                        calls_seen.append(params)
                        if params["symbols"].split(",")[0] == "S0000":
                            return response({key: {}})
                        if failure == "http":
                            raise HTTPError("test", 503, "failed", {"Retry-After": "0"}, io.BytesIO())
                        if failure == "json":
                            return io.BytesIO(b'{"broken":')
                        if failure == "shape":
                            return response({key: []})
                        return response({key: {}, "next_page_token": "repeat" if failure == "repeat" else False})

                    expected = DownloadError if owner == "bars" else RuntimeError
                    with self.assertRaises(expected):
                        self.run_client(owner, 1001, handler, dependent=True)
                    # Neither the first batch nor a dependent step is restarted.
                    self.assertEqual(sum(c["symbols"].startswith("S0000,") for c in calls_seen), 1)
                    if owner == "bars":
                        errors = []
                        result, _ = self.run_client(
                            owner,
                            1001,
                            handler,
                            on_failure=lambda step, action, error: errors.append((step, action)),
                            dependent=True,
                        )
                        self.assertEqual(len(result), 500)
                        self.assertTrue(all(not value.rows for value in result.values()))
                        self.assertEqual(len(errors), 1)
                        self.assertEqual(len(errors[0][1]["request"]["params"]["symbols"].split(",")), 501)

    def test_414_after_a_page_restarts_only_that_batch_and_discards_its_partial_rows(self):
        for owner in ("bars", "metadata"):
            key = "bars" if owner == "bars" else "corporate_actions"
            metrics = {}

            def handler(params, calls):
                roster = params["symbols"].split(",")
                data = (
                    {s: [row()] for s in roster}
                    if owner == "bars"
                    else {"cash_dividends": [{"symbol": s} for s in roster]}
                )
                if len(roster) > 1:
                    if "page_token" in params:
                        raise HTTPError("test", 414, "too long", {}, io.BytesIO())
                    return response({key: data, "next_page_token": "next"})
                return response({key: data})

            result, calls = self.run_client(owner, 3, handler, metrics=metrics)
            self.assertEqual(
                [(c["symbols"], c.get("page_token")) for c in calls],
                [
                    ("S0000,S0001,S0002", None),
                    ("S0000,S0001,S0002", "next"),
                    ("S0000", None),
                    ("S0001,S0002", None),
                    ("S0001,S0002", "next"),
                    ("S0001", None),
                    ("S0002", None),
                ],
            )
            self.assertEqual(metrics, {"batches": 5, "oversize_splits": 2, "page_requests": 7})
            if owner == "bars":
                self.assertEqual(len(result), 3)
                self.assertTrue(all(len(value.rows) == 1 for value in result.values()))
            else:
                self.assertEqual(result, [[[{"symbol": s, "action_type": "cash_dividend"} for s in symbols(3)]]])

    def test_rejected_exact_split_child_does_not_mark_siblings_unavailable(self):
        errors = []

        def handler(params, calls):
            if "," in params["symbols"]:
                raise HTTPError("test", 414, "too long", {}, io.BytesIO())
            if params["symbols"] == "S0001":
                raise HTTPError("test", 400, "invalid", {}, response({"message": "invalid symbol: S0001"}))
            return response({"bars": {}})

        result, calls = self.run_client(
            "bars", 2, handler, on_failure=lambda step, action, error: errors.append(failure_outcome(error))
        )
        self.assertEqual({key[3] for key in result}, {"S0000"})
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["outcome"], "invalid_symbol")
        self.assertEqual(errors[0]["http_status"], 400)
        with self.assertRaises(DownloadError) as caught:
            self.run_client("bars", 2, handler)
        self.assertEqual(failure_outcome(caught.exception)["outcome"], "invalid_symbol")

    def test_universe_estimates_include_each_physical_boundary_batch(self):
        from libs.alpaca_lineage_build.universe_plan import preview

        stored = ["2026-10-09T04:00:00Z", "1", "1", "1", "1", "2", "1", "1"]
        accepted = {
            "summary": {
                "entries": [
                    {
                        "symbol": s,
                        "asof": "2026-10-10",
                        "first_all": stored,
                        "last_all": stored,
                        "tail_all": [stored],
                        "tail_version": 32,
                        "end_exclusive": "2026-10-10T00:00:00Z",
                    }
                    for s in symbols(3386)
                ]
            },
            "late_bar_repairs": {"sources": {}},
            "index": {"symbols": {}},
            "serving_catalogue": {"pending_symbols": []},
        }
        report = preview(
            accepted,
            {"deferred": [], "work": {}},
            {"catalogue_observed_at_utc": "2026-10-10T00:00:00Z", "records": []},
            "2026-10-10",
        )
        self.assertEqual(report["expected_io"]["history_check_actions"], 1)
        self.assertEqual(report["expected_io"]["http_pages_minimum"], 5)
        self.assertEqual(report["expected_io"]["http_pages_dense_history_estimate"], 5)

    def test_late_bar_preview_uses_mapped_selected_runs_and_execution_reuses_them(self):
        from libs.alpaca_lineage_build.universe_plan import preview
        from libs.alpaca_pricing_download.late_bars import session_ranges

        sessions = ["2026-03-06", "2026-03-09", "2026-03-11"]
        ranges = session_ranges(sessions, max_sessions=1)
        self.assertEqual(len(ranges), 3)
        self.assertEqual(len(session_ranges(sessions)), 2)
        self.assertEqual(len(session_ranges(sessions, coalesce=False)), 3)
        accepted = {
            "summary": {"entries": []},
            "index": {"symbols": {}},
            "serving_catalogue": {"pending_symbols": []},
            "late_bar_repairs": {
                "window_policy": "consecutive_sessions",
                "sources": {
                    symbol: {"sessions": sessions, "planned": {"asof": asof, "ranges": session_ranges(sessions)}}
                    for symbol, asof in (("A", "2026-03-12"), ("B", "2026-03-12"), ("C", "2026-03-13"))
                },
            },
        }
        report = preview(
            accepted,
            {"deferred": [{"symbol": "B"}], "work": {}},
            {"catalogue_observed_at_utc": "now", "records": []},
            "2026-03-14",
        )
        io = report["expected_io"]
        self.assertEqual(io["late_bar_probe_sessions"], 6)
        self.assertEqual(io["late_bar_probe_actions_upper_bound"], 4)
        self.assertEqual(io["http_pages_minimum"], 5)  # Catalogue plus four mapped bar actions.
        self.assertEqual(io["http_pages_dense_history_estimate"], 5)
        self.assertEqual(io["late_bar_window_policy"], "consecutive_sessions")

    def test_metadata_preserves_nonretryable_transport_and_404_policies(self):
        for error in (
            HTTPError("test", 504, "timeout", {}, io.BytesIO()),
            URLError("offline"),
            TimeoutError("slow"),
        ):
            client = AlpacaIdentityClient("test", "secret", rate_limiter=Mock())
            with patch(
                "libs.alpaca_security_lineage.alpaca_http.open_provider_request", side_effect=error
            ) as opener:
                with self.assertRaises((RuntimeError, TimeoutError)):
                    client.get_asset_lookup("S0000")
                self.assertEqual(opener.call_count, 1)
        client = AlpacaIdentityClient("test", "secret", rate_limiter=Mock())
        with patch(
            "libs.alpaca_security_lineage.alpaca_http.open_provider_request",
            side_effect=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                HTTPError("test", 404, "absent", {}, io.BytesIO())
            ),
        ):
            self.assertEqual(client.get_asset_lookup("S0000")["outcome"], "not_found")
            with self.assertRaises(IdentityHttpError):
                client.get_corporate_actions("S0000", "2026-01-01", "2026-01-02")

    def test_daily_check_3386_symbols_completes_through_real_batched_client(self):
        stored = ["2026-10-10T04:00:00Z", "1", "1", "1", "1", "2", "1", "1"]
        snapshot = {
            "sha256": "synthetic",
            "entries": [
                {
                    "symbol": s,
                    "asof": "2026-10-15",
                    "first_all": stored,
                    "last_all": stored,
                    "tail_all": [stored],
                    "tail_version": 32,
                    "end_exclusive": "2026-10-15T00:00:00Z",
                }
                for s in symbols(3386)
            ],
        }
        calls = []

        def opener(request, *, timeout):
            params = parse_qs(urlsplit(request.full_url).query)
            roster = params["symbols"][0].split(",")
            calls.append(roster)
            return response({"bars": {s: [row()] for s in roster}})

        client = BarsClient("test", "secret", opener=opener, limiter=Mock())
        result = verify(snapshot, client._get)
        self.assertTrue(result["complete"])
        self.assertEqual(result["stats"]["matched"], 3386)
        self.assertEqual([len(c) for c in calls], [846, 846, 847, 847])
        self.assertEqual(
            result["requests"], {"planned_batches": 4, "batches": 4, "page_requests": 4, "oversize_splits": 0}
        )

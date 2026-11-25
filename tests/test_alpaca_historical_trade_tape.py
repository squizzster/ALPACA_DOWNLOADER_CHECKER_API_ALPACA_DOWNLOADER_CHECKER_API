"""A dated SIP trade is selected, observed and carried separately from asset snapshots."""

import copy
import io
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, call, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pipeline_evidence import NoRequests, transport

from libs.alpaca_data_api_solver import build_plan, trade_tape_request
from libs.alpaca_lineage_build import metadata, trade_tape
from libs.alpaca_lineage_build.resume import _evidence_facts
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pipeline.evidence import _V1_FIELDS, export_evidence, import_evidence
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError
from libs.alpaca_security_lineage.inference_evidence import provider_evidence


class TradeTapeTransportTests(TestCase):
    def setUp(self):
        self.client = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        self.need = {
            "kind": "historical_trade_tape",
            "why": "dated_ticker_tape",
            "symbol": "EXAMPLE.A",
            "session": "2024-06-20",
            "asof": "-",
            "feed": "sip",
        }

    def test_sip_tape_execution_venue_and_unknowns_on_exact_single_page(self):
        for code, name in (
            ("A", "CTA network A"),
            ("B", "CTA network B"),
            ("C", "Nasdaq UTP"),
            ("Q", "unknown"),
            (None, "unknown"),
        ):
            with self.subTest(code=code):
                trade = {"t": "2024-06-20T13:30:01.251497986Z", "x": "P"}
                if code is not None:
                    trade["z"] = code
                response = {"symbol": "EXAMPLE.A", "trades": [trade], "next_page_token": "unused"}
                with patch.object(self.client, "request_json", return_value=response) as get:
                    sample = self.client.get_trade_tape_sample("EXAMPLE.A", "2024-06-20")
                get.assert_called_once()
                url = urlsplit(get.call_args.args[0])
                self.assertEqual((url.netloc, url.path), ("data.alpaca.markets", "/v2/stocks/EXAMPLE.A/trades"))
                self.assertEqual(
                    parse_qs(url.query),
                    {
                        "feed": ["sip"],
                        "asof": ["-"],
                        "start": ["2024-06-20T04:00:00Z"],
                        "end": ["2024-06-21T03:59:59.999999999Z"],
                        "sort": ["asc"],
                        "limit": ["1"],
                    },
                )
                self.assertEqual(sample["trade"], trade)
                self.assertEqual((sample["outcome"], sample["tape_network"]), ("observed", name))
                self.assertNotIn("exchange", sample)

    def test_empty_and_denied_supply_only_their_observed_outcomes(self):
        for empty in ([], None):
            with (
                self.subTest(empty=empty),
                patch.object(
                    self.client,
                    "request_json",
                    return_value={"symbol": "EXAMPLE.A", "trades": empty, "next_page_token": None},
                ) as get,
            ):
                sample = self.client.get_trade_tape_sample("EXAMPLE.A", "2024-06-20")
            get.assert_called_once()
            self.assertEqual((sample["http_status"], sample["outcome"]), (200, "no_trades"))
            self.assertNotIn("tape_network", sample)
        for status in (403, 404):
            with (
                self.subTest(status=status),
                patch.object(
                    self.client, "request_json", side_effect=IdentityHttpError(status, "unavailable")
                ) as get,
            ):
                sample = self.client.get_trade_tape_sample("EXAMPLE.A", "2024-06-20")
                get.assert_called_once()
                self.assertEqual((sample["http_status"], sample["outcome"]), (status, "unavailable"))
                self.assertNotIn("trade", sample)
                self.assertNotIn("tape_network", sample)
                self.assertEqual(
                    (sample["symbol"], sample["session"], sample["feed"]), ("EXAMPLE.A", "2024-06-20", "sip")
                )
        with (
            patch.object(self.client, "request_json", side_effect=IdentityHttpError(503, "retry")),
            self.assertRaises(IdentityHttpError),
        ):
            self.client.get_trade_tape_sample("EXAMPLE.A", "2024-06-20")

    def test_wrong_symbol_date_or_shape_fails_at_ingress(self):
        for response in (
            {"symbol": "NEW", "trades": []},
            {"symbol": "EXAMPLE.A"},
            {"symbol": "EXAMPLE.A", "trades": None, "next_page_token": "more"},
            {"symbol": "EXAMPLE.A", "trades": {}},
            {"symbol": "EXAMPLE.A", "trades": [{}, {}]},
            {"symbol": "EXAMPLE.A", "trades": [{"t": "2024-06-20T03:59:00Z"}]},
            {"symbol": "EXAMPLE.A", "trades": [{"t": "2024-06-20"}]},
            {"symbol": "EXAMPLE.A", "trades": [{"t": "2024-06-20T14:00:00Z", "z": 9}]},
        ):
            with (
                self.subTest(response=response),
                patch.object(self.client, "request_json", return_value=response),
                self.assertRaises(RuntimeError),
            ):
                self.client.get_trade_tape_sample("EXAMPLE.A", "2024-06-20")

    def test_explicit_otc_subscription_denial_is_shared_by_credential_and_operation_only(self):
        cache = metadata.EvidenceClient(self.client)
        started, release = Event(), Event()

        def rejected(request, **_):
            started.set()
            if not release.wait(4):
                raise AssertionError("OTC denial probe was not released")
            raise HTTPError(
                request.full_url,
                403,
                "Forbidden",
                {},
                io.BytesIO(b'{"message":"subscription does not permit querying OTC data"}'),
            )

        with (
            patch(
                "libs.alpaca_security_lineage.alpaca_http.open_provider_request", side_effect=rejected
            ) as http,
            ThreadPoolExecutor(max_workers=2) as pool,
        ):
            first = pool.submit(cache.get_trade_tape_sample, "ONE", "2025-01-21", feed="otc")
            self.assertTrue(started.wait(4))
            other = pool.submit(cache.get_trade_tape_sample, "TWO", "2025-01-22", feed="otc")
            release.set()
            observed = first.result()
            self.assertEqual(
                (observed["http_status"], observed["reason"], observed["symbol"]),
                (403, "otc_subscription_denied", "ONE"),
            )
            self.assertIsNone(other.result())
            self.assertIs(cache.get_trade_tape_sample("ONE", "2025-01-21", feed="otc"), observed)
            http.assert_called_once()
        encoder = Encoder()
        doc = export_evidence(cache, encoder.encode)
        wire = json.dumps((doc, encoder.objects))

        def restore(raw):
            doc, objects = json.loads(wire)
            return import_evidence(doc, raw, Decoder(objects).decode)

        same = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        with patch.object(same, "request_json", side_effect=AssertionError("repeated peer denial")):
            self.assertIsNone(restore(same).get_trade_tape_sample("THREE", "2025-01-21", feed="otc"))
        new = AlpacaIdentityClient("different", "changed-secret", rate_limiter=Mock())
        with patch.object(new, "request_json", return_value={"symbol": "ONE", "trades": []}) as http:
            self.assertEqual(
                restore(new).get_trade_tape_sample("ONE", "2025-01-21", feed="otc")["outcome"], "no_trades"
            )
            http.assert_called_once()
        with patch.object(same, "request_json", return_value={"symbol": "ONE", "trades": []}) as http:
            self.assertEqual(
                metadata.EvidenceClient(same).get_trade_tape_sample("ONE", "2025-01-21", feed="otc")["outcome"],
                "no_trades",
            )
            http.assert_called_once()

    def test_generic_otc_denial_does_not_suppress_other_symbols(self):
        cache = metadata.EvidenceClient(self.client)
        with patch.object(
            self.client,
            "request_json",
            side_effect=[IdentityHttpError(403, "symbol restricted"), {"symbol": "TWO", "trades": []}],
        ) as get:
            first = cache.get_trade_tape_sample("ONE", "2025-01-21", feed="otc")
            other = cache.get_trade_tape_sample("TWO", "2025-01-21", feed="otc")
        self.assertNotIn("reason", first)
        self.assertEqual(other["outcome"], "no_trades")
        self.assertEqual(get.call_count, 2)

    def test_plan_deduplicates_bound_requests_and_executor_rejects_tampering_before_http(self):
        plan = build_plan({"steps": [{"needs": [self.need, self.need]}]})
        (action,) = plan["steps"][0]["actions"]
        self.assertEqual(action["deliver"], [{"need": 0}, {"need": 1}])
        for key, value in (("asof", "2024-06-20"), ("limit", 25), ("feed", "otc"), ("start", "1900-01-01")):
            bad = copy.deepcopy(plan)
            bad["steps"][0]["actions"][0]["request"]["params"][key] = value
            with (
                self.subTest(key=key),
                patch.object(self.client, "request_json", side_effect=AssertionError("unsafe request")),
                self.assertRaises(ValueError),
            ):
                self.client.execute_plan(bad)
        for symbol, day in (("a", "2024-06-20"), ("A/../X", "2024-06-20"), ("A", "2024-06-20T00:00:00")):
            with self.subTest(symbol=symbol, day=day), self.assertRaises(ValueError):
                trade_tape_request(symbol, day)


def fresh_inputs():
    old = {**asset("ARCHIVED", "older"), "exchange": "OTC", "status": "inactive", "tradable": False}
    current = asset("SUCCESSOR", "later")
    api = IdentityClient(
        {"ARCHIVED": old, "SUCCESSOR": current, "111111111": old, "222222222": current},
        [
            {
                "action_type": "cash_dividend",
                "id": "native-dividend",
                "symbol": "SHARED",
                "cusip": "111111111",
                "ex_date": "2024-06-20",
                "process_date": "2024-06-27",
                "rate": "0.37",
            },
            {
                "action_type": "name_change",
                "id": "new-occupant",
                "process_date": "2025-09-12",
                "new_symbol": "SHARED",
                "old_symbol": "PREVIOUS",
                "new_cusip": "222222222",
                "old_cusip": "222222222",
            },
        ],
    )
    api.get_trade_tape_sample = Mock(
        side_effect=lambda symbol, day, *, asof, feed: {
            "symbol": symbol,
            "session": day,
            "feed": feed,
            "asof": asof,
            "limit": 1,
            "http_status": 403,
            "outcome": "unavailable",
        }
    )
    view = SimpleNamespace(
        symbol="SHARED",
        local={"present": True, "counts_known": False, "downloads": []},
        source={"history_start": "2024-01-01", "latest_request": {"end_exclusive": "2025-10-01T04:00:00Z"}},
    )
    return view, api


class TradeTapeMetadataTests(TestCase):
    def test_metadata_only_adapter_preserves_native_actions_without_creating_trade_receipts(self):
        view, api = fresh_inputs()
        del api.get_trade_tape_sample
        view.source["latest_request"].update(asof="2025-10-01", timeframe="1Day", feed="sip")
        view.local["downloads"] = [
            {"adjustment": "raw", "rows": 270, "from": "2024-01-02T05:00:00Z", "to": "2025-01-03T05:00:00Z"}
        ]
        client = metadata.EvidenceClient(api)
        former = metadata.acquire(view, client)
        self.assertTrue(former["corporate_actions"])
        self.assertNotIn("historical_trade_tape", former)
        view.symbol = "ARCHIVED"
        archive = metadata.acquire(view, client)
        self.assertNotIn("mapped_historical_trade_tape", archive)
        self.assertNotIn("otc_historical_trade_tape", archive)
        self.assertFalse(client.trade_tape_samples)

    def test_stock_paying_acquirers_literal_sample_uses_its_effective_date(self):
        view, api = fresh_inputs()
        api.actions = [
            {
                "action_type": "stock_and_cash_merger",
                "id": "acquisition",
                "process_date": "2024-07-05",
                "effective_date": "2024-07-02",
                "acquirer_symbol": view.symbol,
                "acquirer_cusip": "111111111",
                "acquirer_rate": ".05",
                "acquiree_symbol": "OTHER",
                "acquiree_cusip": "222222222",
                "acquiree_rate": "1",
                "cash_rate": "2",
            }
        ]
        report = metadata.acquire(view, metadata.EvidenceClient(api))
        self.assertEqual(report["historical_trade_tape"]["session"], "2024-07-02")
        api.get_trade_tape_sample.assert_called_once_with("SHARED", "2024-07-02", asof="-", feed="sip")
        with patch.object(api, "get_trade_tape_sample", side_effect=AssertionError("repeated acquirer sample")):
            prior = {"anchor_month": "2025-09", "evidence": report}
            self.assertEqual(
                metadata.acquire(view, metadata.EvidenceClient(api), prior)["historical_trade_tape"],
                report["historical_trade_tape"],
            )

    def test_literal_dividend_day_must_be_wholly_within_the_native_request(self):
        for start, end, expected in (
            ("2024-06-20T04:00:00Z", "2024-06-21T04:00:00Z", True),
            ("2024-06-20T04:00:00.000001Z", "2024-06-21T04:00:00Z", False),
            ("2024-06-19T08:00:00Z", "2024-06-20T13:00:00Z", False),
            ("2024-06-19T08:00:00Z", "2024-06-20T04:00:00Z", False),
        ):
            with self.subTest(start=start, end=end):
                view, api = fresh_inputs()
                api.actions = [{**api.actions[0], "process_date": "2024-06-20"}]
                view.source["history_start"] = start
                view.source["latest_request"]["end_exclusive"] = end
                report = metadata.acquire(view, metadata.EvidenceClient(api))
                self.assertEqual("historical_trade_tape" in report, expected)
                self.assertEqual(api.get_trade_tape_sample.call_count, int(expected))

    def test_completed_denial_or_peer_suppression_does_not_reopen_an_unchanged_otc_source(self):
        view, api = fresh_inputs()
        view.symbol = "ARCHIVED"
        view.local["downloads"] = [{"adjustment": "raw", "to": "2025-01-03T05:00:00Z"}]
        view.source["latest_request"].update(asof="2025-10-01", timeframe="1Day", feed="sip")
        original = metadata.acquire(view, metadata.EvidenceClient(api))
        self.assertEqual(original["otc_historical_trade_tape"]["outcome"], "unavailable")
        for requested in (True, False):
            with self.subTest(prior_was_request=requested):
                source = dict(original)
                if not requested:
                    del source["otc_historical_trade_tape"]
                prior = {"anchor_month": "2025-09", "evidence": source}
                client = metadata.EvidenceClient(api)
                with patch.object(
                    api, "get_trade_tape_sample", side_effect=AssertionError("reopened unchanged OTC source")
                ):
                    held = metadata.acquire(view, client, prior)
                self.assertEqual("otc_historical_trade_tape" in held, requested)
                api.get_trade_tape_sample.reset_mock()
                metadata.acquire(view, client, prior, refresh=True)
                api.get_trade_tape_sample.assert_called_once_with("ARCHIVED", "2025-01-06", asof="-", feed="otc")

    def test_legacy_authored_report_cannot_supply_dated_trade_annotations(self):
        view, api = fresh_inputs()
        native = metadata.acquire(view, metadata.EvidenceClient(api))
        legacy = {
            **native,
            "sourced_facts": {"facts": ["research"]},
            "provider_corporate_actions": native["corporate_actions"],
            "historical_trade_tape": {"tape_network": "researched network"},
            "mapped_historical_trade_tape": {
                "pricing_route_binding": {"observed_symbol_asset_id": "researched"},
                "last_trade_search": {"outcome": "observed"},
            },
            "otc_historical_trade_tape": {"following_trade_search": {"outcome": "observed"}},
            "terminal_cash_merger_trade": {"observed_symbol_asset_id": "researched", "outcome": "observed"},
        }
        selected = provider_evidence(legacy, local_report=view.local)
        self.assertNotIn("historical_trade_tape", selected)
        self.assertNotIn("mapped_historical_trade_tape", selected)
        self.assertNotIn("otc_historical_trade_tape", selected)
        self.assertNotIn("terminal_cash_merger_trade", selected)

    def test_first_exact_ex_date_retains_literal_scope_and_current_snapshot_separately(self):
        view, api = fresh_inputs()
        dividend = api.actions[0]
        api.actions.extend(
            [
                {**dividend, "id": "earlier", "ex_date": "2024-05-03"},
                {**dividend, "id": "outside", "ex_date": "2023-12-19"},
                {**dividend, "id": "zero", "ex_date": "2024-02-20", "rate": "0"},
            ]
        )
        client = metadata.EvidenceClient(api)
        report = metadata.acquire(view, client)
        sample = report["historical_trade_tape"]
        api.get_trade_tape_sample.assert_called_once_with("SHARED", "2024-05-03", asof="-", feed="sip")
        self.assertEqual((sample["http_status"], sample["outcome"]), (403, "unavailable"))
        asset_snapshot = next(item["asset"] for item in report["asset_lookups"] if item["lookup"] == "111111111")
        self.assertEqual((asset_snapshot["symbol"], asset_snapshot["exchange"]), ("ARCHIVED", "OTC"))
        self.assertNotIn("exchange", sample)
        previous = {"anchor_month": "2025-09", "evidence": report}
        with (
            patch.object(trade_tape, "sample_session", side_effect=AssertionError("repeated tape selection")),
            patch.object(api, "get_trade_tape_sample", side_effect=AssertionError("repeated sample")),
        ):
            self.assertIs(metadata.retained_metadata(view, client, previous), report)
            self.assertIs(metadata.acquire(view, client, previous)["historical_trade_tape"], sample)
            restored, accepted = transport(client, extra=sample)
            self.assertIs(restored.get_trade_tape_sample("SHARED", "2024-05-03"), accepted)

    def test_nonqualifying_sources_have_no_trade_need(self):
        for reason in ("active", "same_label", "zero_rate", "no_mapping", "other_symbol", "outside"):
            with self.subTest(reason=reason):
                view, api = fresh_inputs()
                dividend = api.actions[0]
                if reason == "active":
                    api.assets["111111111"].update(status="active", exchange="NYSE")
                elif reason == "same_label":
                    api.assets["111111111"]["symbol"] = "SHARED"
                elif reason == "zero_rate":
                    dividend["rate"] = 0
                elif reason == "no_mapping":
                    del api.assets["111111111"]
                elif reason == "other_symbol":
                    view.symbol = "PREVIOUS"
                elif reason == "outside":
                    dividend["ex_date"] = "2023-12-29"
                api.get_trade_tape_sample.side_effect = AssertionError("unselected trade request")
                report = metadata.acquire(view, metadata.EvidenceClient(api))
                self.assertNotIn("historical_trade_tape", report)

    def test_new_native_ex_date_selects_only_its_changed_literal_route(self):
        view, api = fresh_inputs()
        report = metadata.acquire(view, metadata.EvidenceClient(api))
        previous = {"anchor_month": "2024-01", "evidence": report}
        api.actions[0]["ex_date"] = "2024-06-18"
        second = metadata.acquire(view, metadata.EvidenceClient(api), previous)
        self.assertEqual(
            api.get_trade_tape_sample.call_args_list,
            [
                call("SHARED", "2024-06-20", asof="-", feed="sip"),
                call("SHARED", "2024-06-18", asof="-", feed="sip"),
            ],
        )
        self.assertEqual(second["historical_trade_tape"]["session"], "2024-06-18")

    def test_newly_discovered_dividend_on_retained_source_acquires_only_its_literal_tape(self):
        view, api = fresh_inputs()
        dividend = api.actions.pop(0)
        api.actions.clear()
        view.source["latest_request"]["end_exclusive"] = "2025-03-01T05:00:00Z"
        report = metadata.acquire(view, metadata.EvidenceClient(api))
        prior = {"anchor_month": "2025-02", "evidence": report}
        view.source["latest_request"]["end_exclusive"] = "2025-10-01T04:00:00Z"
        api.actions.append({**dividend, "ex_date": "2025-07-10", "process_date": "2025-07-17"})
        cache = metadata.EvidenceClient(api)
        retained = metadata.RetainedReportClient(cache, report)
        updated = metadata.acquire(view, retained, prior)
        self.assertEqual(updated["historical_trade_tape"]["session"], "2025-07-10")
        api.get_trade_tape_sample.assert_called_once_with("SHARED", "2025-07-10", asof="-", feed="sip")

    def test_nyse_dividend_without_reuse_and_terminal_own_route_are_distinct_facts(self):
        view, api = fresh_inputs()
        api.assets["111111111"]["status"] = "active"
        api.actions.pop()  # A different company never used this dividend ticker.
        request = view.source["latest_request"]
        request.update(asof="2025-10-01", timeframe="1Day", feed="sip")
        view.local["downloads"] = [
            {"adjustment": "raw", "rows": 270, "from": "2024-01-02T05:00:00Z", "to": "2025-01-03T05:00:00Z"}
        ]
        http = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        api.get_trade_tape_sample = Mock(wraps=http.get_trade_tape_sample)

        def observed(url):
            route = urlsplit(url)
            symbol = route.path.split("/")[-2]
            params = parse_qs(route.query)
            day = params["start"][0][:10]
            if params["feed"] == ["otc"]:
                self.assertEqual(params["asof"], ["-"])
                self.assertEqual(day, "2025-01-06")
                return {"symbol": symbol, "trades": []}
            self.assertEqual(params["feed"], ["sip"])
            return {"symbol": symbol, "trades": [{"t": day + "T14:30:00Z", "x": "P", "z": "A"}]}

        client = metadata.EvidenceClient(api)
        with patch.object(http, "request_json", side_effect=observed):
            former = metadata.acquire(view, client)
            literal = former["historical_trade_tape"]
            self.assertEqual(
                (literal["returned_symbol"], literal["session"], literal["asof"], literal["trade"]["z"]),
                ("SHARED", "2024-06-20", "-", "A"),
            )
            self.assertNotIn("mapped_historical_trade_tape", former)
            view.symbol = "ARCHIVED"
            archive = metadata.acquire(view, client)
            mapped = archive["mapped_historical_trade_tape"]
            self.assertEqual(archive["otc_historical_trade_tape"]["outcome"], "no_trades")
            self.assertEqual(
                (mapped["returned_symbol"], mapped["session"], mapped["asof"], mapped["tape_network"]),
                ("ARCHIVED", "2025-01-03", request["asof"], "CTA network A"),
            )
            self.assertNotIn("historical_trade_tape", archive)
            self.assertNotIn("asset_id", mapped)
            self.assertEqual(
                next(i["asset"]["exchange"] for i in archive["asset_lookups"] if i["lookup"] == "ARCHIVED"),
                "OTC",
            )
            self.assertEqual(
                _evidence_facts(archive),
                _evidence_facts(
                    {
                        k: v
                        for k, v in archive.items()
                        if k not in {"mapped_historical_trade_tape", "otc_historical_trade_tape"}
                    }
                ),
            )
            prior = {"anchor_month": "2025-09", "evidence": archive}
            self.assertIs(metadata.acquire(view, client, prior)["mapped_historical_trade_tape"], mapped)
            request["asof"] = "2025-10-03"
            remapped = metadata.acquire(view, client, prior)["mapped_historical_trade_tape"]
            self.assertEqual(remapped["asof"], "2025-10-03")
        self.assertEqual(
            api.get_trade_tape_sample.call_args_list,
            [
                call("SHARED", "2024-06-20", asof="-", feed="sip"),
                call("ARCHIVED", "2025-01-03", asof="2025-10-01", feed="sip"),
                call("ARCHIVED", "2025-01-06", asof="-", feed="otc"),
                call("ARCHIVED", "2025-01-03", asof="2025-10-03", feed="sip"),
            ],
        )

    def test_mapped_source_needs_selected_raw_date_sip_and_dated_mapping(self):
        for day, feed, asof in (
            (None, "sip", "2025-10-01"),
            ("2025-01-03T05:00:00Z", "otc", "2025-10-01"),
            ("2025-01-03T05:00:00Z", "sip", "-"),
        ):
            with self.subTest(day=day, feed=feed, asof=asof):
                view, api = fresh_inputs()
                view.symbol = "ARCHIVED"
                view.local["downloads"] = [{"adjustment": "raw", "to": day}]
                view.source["latest_request"].update(feed=feed, timeframe="1Day", asof=asof)
                api.get_trade_tape_sample.side_effect = AssertionError("unselected route")
                report = metadata.acquire(view, metadata.EvidenceClient(api))
                self.assertNotIn("mapped_historical_trade_tape", report)

    def test_unchanged_legacy_terminal_record_does_not_select_backfill(self):
        view, api = fresh_inputs()
        view.symbol = "ARCHIVED"
        view.local["downloads"] = [{"adjustment": "raw", "to": "2025-01-03T05:00:00Z"}]
        view.source["latest_request"].update(feed="sip", timeframe="1Day", asof="2025-10-01")
        api.get_trade_tape_sample.side_effect = AssertionError("unselected legacy backfill")
        report = metadata.acquire(view, metadata.EvidenceClient(api), observe_new_tapes=False)
        prior = {
            "anchor_month": "2025-09",
            "evidence": report,
            "assignment_summary": {"raw": {"last_session": "2025-01-03"}},
        }
        result = metadata.acquire(view, metadata.EvidenceClient(api), prior)
        self.assertNotIn("mapped_historical_trade_tape", result)

    def test_active_otc_requires_historical_gap_and_sliding_mapping_retains_original_scope(self):
        view, api = fresh_inputs()
        view.symbol = "ARCHIVED"
        api.assets["ARCHIVED"]["status"] = "active"
        view.local["downloads"] = [{"adjustment": "raw", "to": "2025-01-17T05:00:00Z"}]
        request = view.source["latest_request"]
        request.update(feed="sip", timeframe="1Day", asof="2025-01-21", end_exclusive="2025-01-21T05:00:00Z")
        client = metadata.EvidenceClient(api)
        recent = metadata.acquire(view, client)
        self.assertNotIn("mapped_historical_trade_tape", recent)  # Holiday Monday followed Friday's raw date.
        api.get_trade_tape_sample.assert_not_called()
        earlier = {
            "anchor_month": "2025-01",
            "evidence": recent,
            "pricing_request": dict(request),
            "assignment_summary": {"raw": {"last_session": "2025-01-17"}},
        }
        request.update(asof="2025-01-24", end_exclusive="2025-01-24T05:00:00Z")
        historical = metadata.acquire(view, client, earlier)
        mapped = historical["mapped_historical_trade_tape"]
        self.assertEqual((mapped["http_status"], mapped["outcome"]), (403, "unavailable"))
        self.assertEqual(mapped["pricing_route_binding"]["observed_symbol_asset_id"], "older")
        key = "ARCHIVED", "2025-01-17", "2025-01-24", "older", "sip"
        restored, completed = transport(client, extra=client.trade_tape_samples[key])
        self.assertIs(
            restored.get_trade_tape_sample(key[0], key[1], asof=key[2], mapping_asset_id=key[3]), completed
        )
        request.update(asof="2025-02-04", end_exclusive="2025-02-04T05:00:00Z")
        prior = {"anchor_month": "2025-01", "evidence": historical}
        carried = metadata.acquire(view, client, prior)
        self.assertIs(carried["mapped_historical_trade_tape"], mapped)
        self.assertEqual(mapped["asof"], "2025-01-24")
        self.assertEqual(
            api.get_trade_tape_sample.call_args_list,
            [
                call("ARCHIVED", "2025-01-17", asof="2025-01-24", feed="sip"),
                call("ARCHIVED", "2025-01-21", asof="-", feed="otc"),
            ],
        )
        # A different current symbol occupant cannot borrow the first occupant's response,
        # even on an otherwise identical URL and within the same provider cache.
        request.update(asof="2025-01-24", end_exclusive="2025-01-24T05:00:00Z")
        old = client.assets["ARCHIVED"]
        client.assets["ARCHIVED"] = {**old, "asset": {**old["asset"], "id": "different-issue"}}
        again = metadata.acquire(view, client, prior)["mapped_historical_trade_tape"]
        self.assertEqual(again["pricing_route_binding"]["observed_symbol_asset_id"], "different-issue")
        self.assertEqual(api.get_trade_tape_sample.call_count, 3)

    def test_point_observation_beyond_local_calendar_horizon(self):
        self.assertEqual(
            trade_tape_request("FUTURE", "2041-04-10", "2041-05-01")["params"]["asof"], "2041-05-01"
        )
        with patch.object(
            trade_tape, "market_sessions_between", side_effect=AssertionError("unnecessary calendar read")
        ):
            self.assertTrue(trade_tape._historical_gap("2041-04-10", "2041-05-01T00:00:00Z"))
        self.assertIsNone(trade_tape.next_market_session_after("1900-01-01"))

    def test_partial_next_local_date_cannot_select_a_full_day_trade_request(self):
        self.assertFalse(trade_tape._historical_gap("2025-01-17", "2025-01-21T12:15:00Z"))
        self.assertTrue(trade_tape._historical_gap("2025-01-17", "2025-01-22T05:00:00Z"))
        view, api = fresh_inputs()
        view.symbol = "ARCHIVED"
        view.source["latest_request"]["end_exclusive"] = "2025-01-21T12:15:00Z"
        trade_tape._record_otc_sample(view, {}, metadata.EvidenceClient(api), "2025-01-17")
        api.get_trade_tape_sample.assert_not_called()

    def test_old_transport_restores_its_accepted_state_without_fetching_a_sample(self):
        _, api = fresh_inputs()
        original = metadata.EvidenceClient(api)
        encoder = Encoder()
        document = export_evidence(
            original,
            lambda state: (
                encoder.encode({k: state[k] for k in _V1_FIELDS})
                if isinstance(state, dict)
                else encoder.encode(state)
            ),
        )
        document["schema_version"] = 1
        restored = import_evidence(document, NoRequests(), Decoder(encoder.objects).decode)
        self.assertEqual(restored.trade_tape_samples, {})

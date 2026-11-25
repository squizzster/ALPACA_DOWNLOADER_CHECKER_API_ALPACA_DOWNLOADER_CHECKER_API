"""Locate a literal OTC print after a documented empty trading date."""

import copy
from unittest import TestCase
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from test_alpaca_historical_trade_tape import fresh_inputs
from test_alpaca_pipeline_evidence import transport

from libs.alpaca_data_api_solver import build_plan, trade_tape_request
from libs.alpaca_lineage_build import metadata, trade_tape
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient, IdentityHttpError


class FirstOtcPrintTests(TestCase):
    def setUp(self):
        self.view, self.api = fresh_inputs()
        self.view.symbol = "ARCHIVED"
        self.view.source_sha256 = "1" * 64
        self.view.local["downloads"] = [{"adjustment": "raw", "to": "2025-01-03T05:00:00Z"}]
        self.view.source["latest_request"].update(
            feed="sip", timeframe="1Day", asof="2025-01-13", end_exclusive="2025-01-13T05:00:00Z"
        )
        self.http = AlpacaIdentityClient("test", "test", rate_limiter=Mock())
        self.api.get_trade_tape_sample = self.http.get_trade_tape_sample
        self.cache = metadata.EvidenceClient(self.api)
        self.responses = {}
        self.routes = []
        self.printed = {"t": "2025-01-11T01:12:54.123456789Z", "x": "D", "z": "A"}
        self.doc = lambda trades, **fields: {"symbol": "ARCHIVED", "trades": trades, **fields}
        self.responses["sip", "2025-01-03", "2025-01-03"] = self.doc([{"t": "2025-01-03T15:00:00Z", "z": "C"}])
        self.responses["otc", "2025-01-06", "2025-01-06"] = self.doc([])
        self.get = patch.object(self.http, "request_json", side_effect=self.response)
        self.get.start()
        self.addCleanup(self.get.stop)

    def response(self, url):
        params = {key: values[0] for key, values in parse_qs(urlsplit(url).query).items()}
        self.assertEqual((params["sort"], params["limit"]), ("asc", "1"))
        self.assertNotIn("page_token", params)
        from libs.alpaca_data_api_solver import NEW_YORK, instant

        start, end = (instant(params[key]).astimezone(NEW_YORK).date().isoformat() for key in ("start", "end"))
        route = (params["feed"], start, end)
        self.routes.append(route)
        response = self.responses.pop(route)
        if isinstance(response, Exception):
            raise response
        return response

    def acquire(self, report=None, **options):
        prior = {"anchor_month": "2025-01", "evidence": report} if report is not None else None
        return metadata.acquire(self.view, self.cache, prior, **options)

    def extend(self, end_date):
        prior = self.view.source_sha256
        self.view.source_sha256 = hex(int(prior, 16) + 1)[2:]
        self.view.source["history_changes"] = [{"source_sha256": prior, "changed_from": None}]
        self.view.source["latest_request"].update(asof=end_date, end_exclusive=end_date + "T05:00:00Z")

    def test_later_first_print_records_actual_local_date_and_one_inclusive_request(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([self.printed], next_page_token="ignore")
        report = self.acquire()
        day = report["otc_historical_trade_tape"]
        search = day["following_trade_search"]
        self.assertEqual((day["session"], day["outcome"]), ("2025-01-06", "no_trades"))
        self.assertEqual((search["outcome"], search["first_observed_session"]), ("observed", "2025-01-10"))
        self.assertEqual(search["empty_through_session"], "2025-01-09")
        self.assertEqual(search["source_generation"], self.view.source_sha256)
        self.assertEqual(search["observed_symbol_asset_id"], self.api.assets["ARCHIVED"]["id"])
        sample = search["last_request"]
        self.assertEqual(
            (
                sample["session"],
                sample["through_session"],
                sample["observed_session"],
                sample["asof"],
                sample["feed"],
            ),
            ("2025-01-07", "2025-01-12", "2025-01-10", "-", "otc"),
        )
        self.assertEqual(
            (sample["start"], sample["end"]), ("2025-01-07T05:00:00Z", "2025-01-13T04:59:59.999999999Z")
        )
        self.assertEqual(sample["trade"], self.printed)
        self.assertEqual(sample["tape_network"], "unknown")
        self.assertEqual(len(self.routes), 3)
        self.assertFalse(self.responses)
        with patch.object(
            trade_tape, "_otc_action_limit", side_effect=AssertionError("repeated scope calculation")
        ):
            self.assertIs(self.acquire(report)["otc_historical_trade_tape"], day)
            self.extend("2025-02-01")
            updated = self.acquire(report)["otc_historical_trade_tape"]["following_trade_search"]
        self.assertIs(updated["last_request"], sample)
        self.assertEqual(updated["source_generation"], self.view.source_sha256)

    def test_empty_interval_extension_requests_only_new_dates_and_survives_transport(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([])
        first = self.acquire()
        initial = first["otc_historical_trade_tape"]["following_trade_search"]
        self.assertEqual(initial["empty_through_session"], "2025-01-12")
        self.extend("2025-01-16")
        self.responses["otc", "2025-01-13", "2025-01-15"] = self.doc([{"t": "2025-01-15T16:00:00Z", "x": "D"}])
        with patch.object(trade_tape, "_otc_action_limit", side_effect=AssertionError("reopened limit")):
            second = self.acquire(first)
        updated = second["otc_historical_trade_tape"]["following_trade_search"]
        self.assertEqual(
            (updated["from_session"], updated["empty_through_session"]), ("2025-01-07", "2025-01-14")
        )
        self.assertEqual(updated["first_observed_session"], "2025-01-15")
        self.assertEqual(updated["last_request"]["session"], "2025-01-13")
        restored, accepted = transport(self.cache, extra=updated["last_request"])
        self.assertIs(
            restored.get_trade_tape_sample("ARCHIVED", "2025-01-13", feed="otc", through_session="2025-01-15"),
            accepted,
        )
        self.assertEqual(len(self.routes), 4)

    def test_native_departure_caps_future_dates_and_projects_existing_first_print(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([self.printed])
        initial = self.acquire()
        action = {
            "id": "departure",
            "action_type": "cash_merger",
            "acquiree_symbol": "ARCHIVED",
            "acquiree_cusip": "111111111",
            "acquirer_symbol": "BUYER",
            "rate": "10",
            "effective_date": "2025-01-10",
            "process_date": "2025-01-10",
        }
        self.api.actions.append(action)
        self.cache = metadata.EvidenceClient(self.api)
        updated = self.acquire(initial, refresh=True)["otc_historical_trade_tape"]["following_trade_search"]
        self.assertEqual((updated["through_session"], updated["outcome"]), ("2025-01-09", "no_trades"))
        self.assertNotIn("first_observed_session", updated)
        self.assertEqual(updated["last_request"]["observed_session"], "2025-01-10")
        self.assertEqual(len(self.routes), 3)

    def test_positive_first_day_entitlement_denial_and_structural_failures_never_select_range(self):
        first = ("otc", "2025-01-06", "2025-01-06")
        for response in (
            self.doc([{"t": "2025-01-06T15:00:00Z"}]),
            IdentityHttpError(403, "unavailable"),
            IdentityHttpError(404, "missing"),
            self.doc([], next_page_token="more"),
        ):
            with self.subTest(response=response):
                self.cache = metadata.EvidenceClient(self.api)
                self.responses["sip", "2025-01-03", "2025-01-03"] = self.doc(
                    [{"t": "2025-01-03T15:00:00Z", "z": "B"}]
                )
                self.responses[first] = response
                if isinstance(response, dict) and response.get("next_page_token"):
                    with self.assertRaisesRegex(RuntimeError, "empty first trade page"):
                        self.acquire()
                else:
                    day = self.acquire()["otc_historical_trade_tape"]
                    self.assertNotIn("following_trade_search", day)
                self.assertEqual(self.routes[-1], first)

    def test_unbound_source_and_missing_endpoint_do_not_select_range(self):
        del self.view.source_sha256
        self.assertNotIn("following_trade_search", self.acquire()["otc_historical_trade_tape"])
        self.view.local["downloads"] = []
        self.assertNotIn("mapped_historical_trade_tape", self.acquire())
        self.assertEqual(len(self.routes), 2)

    def test_disappearing_endpoint_drops_the_old_literal_and_forward_attachment(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([self.printed])
        first = self.acquire()
        self.view.local["downloads"] = []
        later = self.acquire(first)
        self.assertNotIn("otc_historical_trade_tape", later)
        self.assertNotIn("mapped_historical_trade_tape", later)
        self.assertEqual(len(self.routes), 3)

    def test_pricing_endpoint_does_not_close_literal_otc_coverage_but_confirmed_departure_does(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([self.printed])
        initial = self.acquire()
        interval = {
            "historical_symbol": "ARCHIVED",
            "asset_id": self.api.assets["ARCHIVED"]["id"],
            "end_date_exclusive": "2025-01-04",
            "ownership_interval_rules": [
                {
                    "name": "dividend_anchored_former_ticker_occupation",
                    "supporting_corporate_action_ids": ["dividend"],
                }
            ],
        }
        prior = {
            "anchor_month": "2025-01",
            "evidence": initial,
            "observation_routes": {"owner": ["2025-01-03T05:00:00Z", interval]},
        }
        output = metadata.acquire(self.view, self.cache, prior)["otc_historical_trade_tape"][
            "following_trade_search"
        ]
        self.assertEqual(output["first_observed_session"], "2025-01-10")
        interval["ownership_interval_rules"] = [
            {"name": "cash_merger_acquiree_terminal_boundary", "supporting_corporate_action_ids": ["departure"]}
        ]
        capped = metadata.acquire(self.view, self.cache, prior)["otc_historical_trade_tape"][
            "following_trade_search"
        ]
        self.assertEqual(capped["outcome"], "outside_search_bounds")
        self.assertNotIn("first_observed_session", capped)
        self.assertEqual(len(self.routes), 3)

    def test_new_issuer_cannot_inherit_forward_print_when_its_mapped_tape_is_unavailable(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([self.printed])
        initial = self.acquire()
        self.api.assets["ARCHIVED"] = {**self.api.assets["ARCHIVED"], "id": "different-issuer"}
        self.cache = metadata.EvidenceClient(self.api)
        self.responses["sip", "2025-01-03", "2025-01-03"] = IdentityHttpError(404, "new mapping unavailable")
        later = self.acquire(initial, refresh=True)
        self.assertEqual(later["otc_historical_trade_tape"]["outcome"], "no_trades")
        self.assertNotIn("following_trade_search", later["otc_historical_trade_tape"])
        self.assertEqual(later["mapped_historical_trade_tape"]["http_status"], 404)
        self.assertEqual(len(self.routes), 4)

    def test_changed_raw_endpoint_that_has_no_later_eligible_day_drops_old_scope(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = IdentityHttpError(403, "restricted")
        first = self.acquire()
        self.assertEqual(first["otc_historical_trade_tape"]["following_trade_search"]["outcome"], "unavailable")
        self.view.local["downloads"] = [{"adjustment": "raw", "to": "2025-01-10T05:00:00Z"}]
        self.view.source["latest_request"]["end_exclusive"] = "2025-01-11T05:00:00Z"
        updated = self.acquire(first)
        self.assertNotIn("otc_historical_trade_tape", updated)
        self.assertNotIn("mapped_historical_trade_tape", updated)
        self.assertEqual(len(self.routes), 3)

    def test_explicit_refresh_after_unavailable_forward_range_reuses_the_completed_first_day(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = IdentityHttpError(403, "restricted range")
        initial = self.acquire()
        self.cache = metadata.EvidenceClient(self.api)
        carried = self.acquire(initial)
        self.assertEqual(
            carried["otc_historical_trade_tape"]["following_trade_search"]["outcome"], "unavailable"
        )
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([self.printed])
        recovered = self.acquire(carried, refresh=True)["otc_historical_trade_tape"]["following_trade_search"]
        self.assertEqual(recovered["first_observed_session"], "2025-01-10")
        self.assertEqual(self.routes.count(("otc", "2025-01-06", "2025-01-06")), 1)
        self.assertEqual(len(self.routes), 4)

    def test_generation_replacement_with_changed_sip_history_requires_new_selection(self):
        self.responses["otc", "2025-01-07", "2025-01-12"] = self.doc([])
        first = self.acquire()
        self.extend("2025-01-16")
        self.view.source["history_changes"][0]["changed_from"] = "2025-01-03"
        self.cache = metadata.EvidenceClient(self.api)
        self.responses["otc", "2025-01-07", "2025-01-15"] = self.doc([])
        second = self.acquire(first)["otc_historical_trade_tape"]["following_trade_search"]
        self.assertEqual(second["last_request"]["session"], "2025-01-07")
        self.assertEqual(len(self.routes), 4)

    def test_forward_request_restricts_credentialed_executor_and_handles_dst(self):
        need = {
            "kind": "historical_trade_tape",
            "why": "listed halt",
            "symbol": "ARCHIVED",
            "session": "2025-03-08",
            "through_session": "2025-03-11",
            "asof": "-",
            "feed": "otc",
        }
        plan = build_plan({"steps": [{"needs": [need, need]}]})
        (action,) = plan["steps"][0]["actions"]
        self.assertEqual(len(action["deliver"]), 2)
        params = action["request"]["params"]
        self.assertEqual(
            (params["start"], params["end"]), ("2025-03-08T05:00:00Z", "2025-03-12T03:59:59.999999999Z")
        )
        for bad in (
            {**need, "asof": "2025-03-07"},
            {**need, "feed": "sip"},
            {**need, "through_session": "2025-03-07"},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                build_plan({"steps": [{"needs": [bad]}]})
        tampered = copy.deepcopy(plan)
        tampered["steps"][0]["actions"][0]["request"] = trade_tape_request(
            "ARCHIVED", "2025-03-08", "-", "otc", through_session="2025-03-13"
        )
        with self.assertRaises(ValueError):
            self.http.execute_plan(tampered)
        self.assertFalse(self.routes)

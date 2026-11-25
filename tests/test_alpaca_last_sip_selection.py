"""A separately observed historical SIP trade can precede a zero-activity pricing endpoint."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_otc_status_discovery import StatusClient, price_view
from test_alpaca_pipeline_evidence import transport

from libs.alpaca_lineage_build import metadata


class Archive(StatusClient):
    def __init__(self, symbol):
        super().__init__(symbol)
        self.responses = {}
        self.trade_calls = []

    def get_trade_tape_sample(self, symbol, session, *, asof, feed, through_session=None, sort="asc"):
        key = (feed, session, through_session)
        self.trade_calls.append((key, asof, sort))
        answer = self.responses.pop(key)
        result = {
            "symbol": symbol,
            "session": session,
            "asof": asof,
            "feed": feed,
            "http_status": 200,
            "outcome": "no_trades",
            "limit": 1,
            "returned_symbol": symbol,
        }
        if through_session:
            result.update(through_session=through_session, sort=sort)
        if isinstance(answer, int):
            result.update(outcome="unavailable", http_status=answer)
        elif answer:
            observed, code = answer
            result.update(outcome="observed", trade={"t": observed + "T18:15:38.380126262Z", "z": code, "s": 1})
            if through_session:
                result["observed_session"] = observed
        return result


class LastSipSelectionTests(TestCase):
    def start(self, symbol="ARCH"):
        self.view, self.api = price_view(symbol), Archive(symbol)
        self.view.source["latest_request"].update(asof="2024-03-01", end_exclusive="2024-03-01T05:00:00Z")
        self.cache = metadata.EvidenceClient(self.api)
        self.last = ("sip", "2024-02-22", None)
        self.back = ("sip", "2024-02-01", "2024-02-21")
        self.next_day = ("otc", "2024-02-12", None)
        self.forward = ("otc", "2024-02-13", "2024-02-29")
        self.api.responses[self.last] = None

    def acquire(self, prior=None, *, refresh=False):
        previous = {"anchor_month": "2024-02", "evidence": prior} if prior else None
        return metadata.acquire(self.view, self.cache, previous, refresh=refresh)

    def move(self, *, first=None, last=None, changed=None):
        old = self.view.source_sha256
        self.view.source_sha256 += "+"
        self.view.source["history_changes"] = [{"source_sha256": old, "changed_from": changed}]
        raw = self.view.local["downloads"][0]
        if first:
            raw["from"] = first + "T05:00:00Z"
        if last:
            raw["to"] = last + "T05:00:00Z"

    def test_actual_prior_print_drives_one_otc_date_and_followup_without_changing_raw_evidence(self):
        self.start()
        self.api.responses.update(
            {self.back: ("2024-02-09", "C"), self.next_day: None, self.forward: ("2024-02-20", "Z")}
        )
        result = self.acquire()
        mapped = result["mapped_historical_trade_tape"]
        self.assertEqual((mapped["session"], mapped["outcome"]), ("2024-02-22", "no_trades"))
        self.assertNotIn("trade", mapped)
        found = mapped["last_trade_search"]
        self.assertEqual((found["raw_first_session"], found["raw_last_session"]), ("2024-02-01", "2024-02-22"))
        self.assertEqual(found["empty_intervals"], [["2024-02-10", "2024-02-22"]])
        self.assertEqual(found["observed_symbol_asset_id"], self.api.archive["id"])
        self.assertEqual(found["source_generation"], self.view.source_sha256)
        self.assertNotIn("observation", found)
        self.assertEqual(
            found["last_request"]["trade"], {"t": "2024-02-09T18:15:38.380126262Z", "z": "C", "s": 1}
        )
        otc = result["otc_historical_trade_tape"]
        self.assertEqual((otc["after_sip_session"], otc["session"]), ("2024-02-09", "2024-02-12"))
        self.assertEqual(otc["following_trade_search"]["first_observed_session"], "2024-02-20")
        self.assertEqual(
            self.api.trade_calls,
            [
                (self.last, "2024-03-01", "asc"),
                (self.back, "2024-03-01", "desc"),
                (self.next_day, "-", "asc"),
                (self.forward, "-", "asc"),
            ],
        )
        self.cache = metadata.EvidenceClient(self.api)
        with patch.object(
            self.cache, "get_trade_tape_sample", side_effect=AssertionError("repeated tape request")
        ):
            self.assertIs(self.acquire(result)["mapped_historical_trade_tape"], mapped)
            self.assertIs(self.acquire(result, refresh=True)["mapped_historical_trade_tape"], mapped)

    def test_negative_and_unknown_tapes_do_not_imply_a_following_otc_day(self):
        for response, outcome in (
            (None, "no_trades"),
            (403, "unavailable"),
            (404, "unavailable"),
            (("2024-02-09", "Z"), "observed"),
        ):
            with self.subTest(response=response):
                self.start()
                self.api.responses[self.back] = response
                result = self.acquire()
                self.assertNotIn("otc_historical_trade_tape", result)
                search = result["mapped_historical_trade_tape"]["last_trade_search"]
                self.assertEqual(search["outcome"], outcome)
                expected_first = {
                    "no_trades": "2024-02-01",
                    "unavailable": "2024-02-22",
                    "observed": "2024-02-10",
                }[outcome]
                self.assertEqual(search["empty_intervals"], [[expected_first, "2024-02-22"]])
                self.cache = metadata.EvidenceClient(self.api)
                self.assertIs(self.acquire(result)["mapped_historical_trade_tape"]["last_trade_search"], search)
                self.assertEqual(len(self.api.trade_calls), 2)

    def test_legacy_exact_day_requires_explicit_selection_and_does_not_reissue_day(self):
        self.start()
        self.api.responses[self.back] = None
        legacy = deepcopy(self.acquire())
        del legacy["mapped_historical_trade_tape"]["last_trade_search"]
        self.cache = metadata.EvidenceClient(self.api)
        self.assertNotIn("last_trade_search", self.acquire(legacy)["mapped_historical_trade_tape"])
        self.api.responses.update({self.back: ("2024-02-09", "A"), self.next_day: 403})
        mapped = self.acquire(legacy, refresh=True)["mapped_historical_trade_tape"]
        self.assertEqual(mapped["last_trade_search"]["last_request"]["observed_session"], "2024-02-09")
        self.assertEqual([key for key, _, _ in self.api.trade_calls].count(self.last), 1)

    def test_disjoint_extension_does_not_search_closed_empty_scope_or_reobserve_an_earlier_print(self):
        self.start()
        self.api.responses.update({self.back: ("2024-02-09", "A"), self.next_day: ("2024-02-12", "Z")})
        old = self.acquire()
        observation = old["mapped_historical_trade_tape"]["last_trade_search"]["last_request"]
        self.move(first="2024-01-29", last="2024-02-26")
        self.cache = metadata.EvidenceClient(self.api)
        endpoint = ("sip", "2024-02-26", None)
        new_tail = ("sip", "2024-02-23", "2024-02-25")
        self.api.responses.update({endpoint: None, new_tail: None})
        result = self.acquire(old)
        search = result["mapped_historical_trade_tape"]["last_trade_search"]
        self.assertIs(search["observation"], observation)
        self.assertEqual(search["empty_intervals"], [["2024-02-10", "2024-02-26"]])
        self.assertEqual(search["from_session"], "2024-01-29")
        self.assertEqual(search["source_generation"], self.view.source_sha256)
        self.assertEqual([key for key, _, _ in self.api.trade_calls][-2:], [endpoint, new_tail])
        self.assertIs(result["otc_historical_trade_tape"], old["otc_historical_trade_tape"])

    def test_absence_on_newer_suffix_selects_only_unsearched_older_prefix(self):
        self.start()
        self.api.responses[self.back] = None
        initial = self.acquire()
        self.move(first="2024-01-29", last="2024-02-26")
        self.cache = metadata.EvidenceClient(self.api)
        endpoint = ("sip", "2024-02-26", None)
        tail, prefix = ("sip", "2024-02-23", "2024-02-25"), ("sip", "2024-01-29", "2024-01-31")
        self.api.responses.update(
            {endpoint: None, tail: None, prefix: ("2024-01-31", "C"), ("otc", "2024-02-01", None): 403}
        )
        updated = self.acquire(initial)["mapped_historical_trade_tape"]["last_trade_search"]
        self.assertEqual([key for key, _, _ in self.api.trade_calls][-4:-1], [endpoint, tail, prefix])
        self.assertEqual(
            (updated["last_request"]["observed_session"], updated["empty_intervals"]),
            ("2024-01-31", [["2024-02-01", "2024-02-26"]]),
        )

    def test_unavailable_extension_keeps_older_dated_print_and_literal_sample_until_a_selected_retry(self):
        self.start()
        self.api.responses.update({self.back: ("2024-02-09", "C"), self.next_day: ("2024-02-12", "Z")})
        initial = self.acquire()
        self.move(last="2024-02-26")
        self.view.source["latest_request"].update(asof="2024-03-04", end_exclusive="2024-03-04T05:00:00Z")
        self.cache = metadata.EvidenceClient(self.api)
        tail = ("sip", "2024-02-23", "2024-02-25")
        self.api.responses.update({("sip", "2024-02-26", None): None, tail: 404})
        unavailable = self.acquire(initial)
        failed = unavailable["mapped_historical_trade_tape"]["last_trade_search"]
        self.assertEqual(failed["outcome"], "unavailable")
        self.assertEqual(failed["observation"]["observed_session"], "2024-02-09")
        self.assertIs(unavailable["otc_historical_trade_tape"], initial["otc_historical_trade_tape"])
        self.cache = metadata.EvidenceClient(self.api)
        self.assertIs(self.acquire(unavailable)["mapped_historical_trade_tape"]["last_trade_search"], failed)
        self.api.responses[tail] = None
        restored = self.acquire(unavailable, refresh=True)
        found = restored["mapped_historical_trade_tape"]["last_trade_search"]
        self.assertEqual((found["outcome"], found["pricing_asof"]), ("observed", "2024-03-04"))
        self.assertEqual(found["observation"]["asof"], "2024-03-01")
        self.assertEqual(found["empty_intervals"], [["2024-02-10", "2024-02-26"]])
        self.assertIs(restored["otc_historical_trade_tape"], initial["otc_historical_trade_tape"])
        self.assertEqual(self.api.trade_calls[-1], (tail, "2024-03-04", "desc"))
        self.assertFalse(self.api.responses)

    def test_interruption_transports_the_completed_earlier_trade_before_the_first_otc_request(self):
        self.start()
        self.api.responses[self.back] = ("2024-02-09", "A")
        with (
            patch("libs.alpaca_lineage_build.trade_tape._record_otc_sample", side_effect=InterruptedError),
            self.assertRaises(InterruptedError),
        ):
            self.acquire()
        self.cache, _ = transport(self.cache)
        self.cache.client = self.api
        self.api.responses[self.next_day] = 403
        resumed = self.acquire()
        self.assertEqual([key for key, _, _ in self.api.trade_calls], [self.last, self.back, self.next_day])
        self.assertEqual(
            resumed["mapped_historical_trade_tape"]["last_trade_search"]["last_request"]["observed_session"],
            "2024-02-09",
        )
        self.assertEqual(resumed["otc_historical_trade_tape"]["http_status"], 403)

    def test_new_later_print_supersedes_old_date_without_requesting_older_history(self):
        self.start()
        self.api.responses.update({self.back: ("2024-02-09", "C"), self.next_day: ("2024-02-12", "Z")})
        initial = self.acquire()
        self.move(first="2024-01-15", last="2024-02-26")
        self.cache = metadata.EvidenceClient(self.api)
        tail = ("sip", "2024-02-23", "2024-02-25")
        following = ("otc", "2024-02-26", None)
        self.api.responses.update({("sip", "2024-02-26", None): None, tail: ("2024-02-23", "B"), following: 403})
        result = self.acquire(initial)
        found = result["mapped_historical_trade_tape"]["last_trade_search"]
        self.assertEqual(found["last_request"]["observed_session"], "2024-02-23")
        self.assertNotIn("observation", found)
        self.assertEqual(result["otc_historical_trade_tape"]["after_sip_session"], "2024-02-23")
        self.assertFalse(self.api.responses)

    def test_lost_generation_or_changed_owner_discards_prior_association(self):
        for change in ("generation", "owner", "mapping"):
            with self.subTest(change=change):
                self.start()
                self.api.responses.update({self.back: ("2024-02-09", "C"), self.next_day: ("2024-02-12", "Z")})
                initial = self.acquire()
                self.cache = metadata.EvidenceClient(self.api)
                if change == "generation":
                    self.move(changed="2024-02-05")
                else:
                    self.api.responses[self.last] = None
                    if change == "owner":
                        self.api.archive["id"] = "another-issue"
                    else:
                        self.view.source["latest_request"]["asof"] = "2020-01-01"
                self.api.responses[self.back] = None
                result = self.acquire(initial, refresh=True)
                found = result["mapped_historical_trade_tape"]["last_trade_search"]
                self.assertEqual(found["outcome"], "no_trades")
                self.assertNotIn("observation", found)
                self.assertNotIn("otc_historical_trade_tape", result)
                self.assertFalse(self.api.responses)

    def test_unavailable_or_incomplete_raw_scope_never_selects_prior_dates(self):
        for change in ("unavailable", "missing", "partial", "single", "midday"):
            with self.subTest(change=change):
                self.start()
                if change == "unavailable":
                    self.api.responses[self.last] = 403
                    self.api.responses["otc", "2024-02-23", None] = 403
                elif change == "missing":
                    del self.view.local["downloads"][0]["from"]
                elif change == "partial":
                    self.view.materialization_scope = "retained_suffix"
                elif change == "single":
                    self.view.local["downloads"][0]["from"] = self.view.local["downloads"][0]["to"]
                else:
                    self.view.source["history_start"] = "2024-02-01T18:30:00Z"
                    self.api.responses["sip", "2024-02-02", "2024-02-21"] = None
                result = self.acquire()
                search = result["mapped_historical_trade_tape"].get("last_trade_search")
                if change == "midday":
                    self.assertEqual(search["from_session"], "2024-02-02")
                else:
                    self.assertIsNone(search)
                self.assertFalse(self.api.responses)

    def test_status_query_uses_actual_prior_native_trade_and_excludes_untraded_dividend_dates(self):
        for case in (
            "actual_positive",
            "earlier_traded",
            "earlier_carry",
            "later_carry",
            "missing",
            "unavailable",
        ):
            with self.subTest(case=case):
                self.start("WXYZQ")
                day = {
                    "earlier_traded": "2024-02-08",
                    "earlier_carry": "2024-02-08",
                    "later_carry": "2024-02-14",
                }.get(case, "2024-02-09")
                self.api.dividend["ex_date"] = day
                traded = int(case == "earlier_traded")
                self.view.views = {
                    "raw": [
                        (d + "T05:00:00Z", 70, 70, 70, 70, traded, traded, 70)
                        for d in sorted({day, "2024-02-09", "2024-02-22"})
                    ]
                }
                self.api.responses[self.back] = {"missing": None, "unavailable": 403}.get(
                    case, ("2024-02-09", "C")
                )
                if case not in ("missing", "unavailable"):
                    self.api.responses[self.next_day] = 403
                report = self.acquire()
                query = report.get("otc_status_symbol_query")
                if case in ("missing", "unavailable"):
                    self.assertIsNone(query)
                    self.assertNotIn(("actions", "WXYZ"), self.api.events)
                else:
                    self.assertEqual(
                        (query["last_session"], query["mapped_sip_session"]), ("2024-02-22", "2024-02-09")
                    )
                    confirmed = case in ("actual_positive", "earlier_traded")
                    self.assertEqual(query["outcome"], "confirmed" if confirmed else "no_matching_dividend")
                    self.assertEqual("WXYZ" in report["queried_symbols"], confirmed)
                    self.assertEqual(("asset", "111112222") in self.api.events, confirmed)
                self.assertFalse(self.api.responses)

"""A change belongs to the same dated Alpaca asset; only distinct SIP tapes can narrow it."""

from unittest import TestCase
from unittest.mock import Mock, call, patch

from libs.alpaca_lineage_build.exchange_changes import (
    FIELD,
    dating_due,
    next_dating_time,
    record_exchange_changes,
)


def lookup(symbol, exchange, asset_id="asset", *, kind="symbol", **extra):
    return {
        "lookup": symbol if kind == "symbol" else asset_id,
        "lookup_kind": kind,
        "asset": {"id": asset_id, "symbol": symbol, "exchange": exchange, "class": "us_equity"},
        "outcome": "found",
        "http_status": 200,
        "source": "asset_catalogue",
        **extra,
    }


def report(stamp, *lookups):
    return {
        "query": {"input_symbol": "ALPHA"},
        "asset_lookups": list(lookups),
        "catalogue_observed_at_utc": stamp,
    }


def tape(symbol, day, code):
    observation = {"symbol": symbol, "session": day, "asof": "-", "feed": "sip", "http_status": 200}
    if code is None:
        return {**observation, "outcome": "no_trades"}
    return {
        **observation,
        "outcome": "observed",
        "trade": {"t": day + "T14:30:01Z", "z": code, "x": "Q"},
    }


class ListingExchangeChangeTests(TestCase):
    def setUp(self):
        self.prior = {"symbol": "ALPHA", "evidence": report("2024-05-24T21:02:00Z", lookup("ALPHA", "NYSE"))}
        self.new = report("2024-05-28T22:01:00Z", lookup("ALPHA", "NASDAQ"))
        self.http = Mock()
        self.http.get_trade_tape_sample.side_effect = AssertionError("unselected trade request")

    def test_one_asset_after_holiday_and_a_second_transfer_reuses_shared_witness(self):
        self.http.get_trade_tape_sample.side_effect = lambda symbol, day: tape(
            symbol, day, {"2024-05-24": "A", "2024-05-28": "C"}[day]
        )
        record_exchange_changes(self.new, self.prior, self.http)
        (event,) = self.new[FIELD]
        self.assertEqual((event["prior_session"], event["effective_session"]), ("2024-05-24", "2024-05-28"))
        self.assertEqual(event["from_observed_at_utc"], "2024-05-24T21:02:00Z")
        self.assertEqual(event["to_observed_at_utc"], "2024-05-28T22:01:00Z")
        self.assertEqual([s["trade"]["z"] for s in event["tape_samples"]], ["A", "C"])
        self.assertEqual(
            self.http.get_trade_tape_sample.call_args_list,
            [call("ALPHA", "2024-05-28"), call("ALPHA", "2024-05-24")],
        )
        self.http.get_trade_tape_sample.side_effect = AssertionError("repeat of completed comparison")
        record_exchange_changes(self.new, self.prior, self.http)
        stable = report("2024-05-29T12:00:00Z", lookup("ALPHA", "NASDAQ"))
        record_exchange_changes(stable, {"symbol": "ALPHA", "evidence": self.new}, self.http)
        self.assertIs(stable[FIELD], self.new[FIELD])

        reverse = report("2024-05-29T21:00:00Z", lookup("ALPHA", "NYSE"))
        self.http.get_trade_tape_sample.side_effect = lambda symbol, day: tape(
            symbol, day, {"2024-05-29": "A"}[day]
        )
        record_exchange_changes(reverse, {"symbol": "ALPHA", "evidence": stable}, self.http)
        self.assertEqual([e["effective_session"] for e in reverse[FIELD]], ["2024-05-28", "2024-05-29"])
        self.assertIs(reverse[FIELD][0], event)

    def test_bootstrap_unchanged_unknown_reused_unordered_and_conflicting_snapshots(self):
        conflict = report(
            "2024-05-28T22:01:00Z",
            lookup("ALPHA", "NASDAQ"),
            lookup("ALPHA", "ARCA", kind="asset_id"),
        )
        for old, new in (
            (None, self.new),
            (self.prior, report("2024-05-28T22:01:00Z", lookup("ALPHA", "NYSE"))),
            (self.prior, report("2024-05-28T22:01:00Z", lookup("ALPHA", "UNKNOWN"))),
            (self.prior, report("2024-05-28T22:01:00Z", lookup("ALPHA", "NASDAQ", "other"))),
            (self.prior, report("2024-05-23T22:01:00Z", lookup("ALPHA", "NASDAQ"))),
            (self.prior, report("2024-05-28T22:01:00Z", lookup("ALPHA", "NASDAQ", source="exact_uuid"))),
            (self.prior, report("2024-05-28T22:01:00Z", lookup("ALPHA", "NASDAQ", observed_at_utc=None))),
            (self.prior, report("2024-05-28T22:01:00Z", lookup("ALPHA", "NASDAQ", kind="cusip"))),
            (self.prior, conflict),
        ):
            with self.subTest(old=old, new=new):
                record_exchange_changes(new, old, self.http)
                self.assertNotIn(FIELD, new)
        self.http.get_trade_tape_sample.assert_not_called()

    def test_rename_same_network_competing_occupant_and_retained_only_keep_the_bracket(self):
        for new, observe in (
            (report("2024-05-28T22:01:00Z", lookup("BETA", "NASDAQ", kind="asset_id")), True),
            (report("2024-05-28T22:01:00Z", lookup("ALPHA", "ARCA")), True),
            (
                report(
                    "2024-05-28T22:01:00Z",
                    lookup("ALPHA", "NASDAQ", kind="asset_id"),
                    lookup("ALPHA", "NASDAQ", "other"),
                ),
                True,
            ),
            (self.new, False),
        ):
            with self.subTest(new=new, observe=observe):
                record_exchange_changes(new, self.prior, self.http, observe_new_tapes=observe)
                (event,) = new[FIELD]
                self.assertEqual(event["asset_id"], "asset")
                self.assertEqual(event["from_symbol"], "ALPHA")
                self.assertEqual(event["to_symbol"], new["asset_lookups"][0]["asset"]["symbol"])
                self.assertNotIn("effective_session", event)
        self.http.get_trade_tape_sample.assert_not_called()

    def test_exact_lookup_dates_and_last_positive_survive_absence(self):
        old = {
            "symbol": "ALPHA",
            "evidence": report(
                "2024-05-27T22:01:00Z", {"lookup": "ALPHA", "lookup_kind": "symbol", "asset": None}
            ),
            "last_observed_symbol_lookup": lookup(
                "ALPHA", "NYSE", catalogue_observed_at_utc="2024-05-24T21:02:00Z"
            ),
        }
        self.new["asset_lookups"][0].update(source="exact", observed_at_utc="2024-05-28T14:00:00Z")
        record_exchange_changes(self.new, old, self.http, observe_new_tapes=False)
        (event,) = self.new[FIELD]
        self.assertEqual(event["from_observed_at_utc"], "2024-05-24T21:02:00Z")
        self.assertEqual(event["to_observed_at_utc"], "2024-05-28T14:00:00Z")

    def test_preopen_next_day_reuses_both_completed_literal_observations(self):
        self.new["catalogue_observed_at_utc"] = "2024-05-29T06:00:00Z"  # 02:00 New York
        self.prior["evidence"]["historical_trade_tape"] = tape("ALPHA", "2024-05-24", "A")
        self.new["historical_trade_tape"] = tape("ALPHA", "2024-05-28", "C")
        record_exchange_changes(self.new, self.prior, self.http)
        self.http.get_trade_tape_sample.assert_not_called()
        (event,) = self.new[FIELD]
        self.assertEqual(event["effective_session"], "2024-05-28")
        self.assertIsNone(next_dating_time(self.new))

    def test_early_close_selects_one_later_request_and_keeps_original_observation_times(self):
        # Black Friday follows Wednesday's session. The new catalogue observation
        # predates Friday's auction; the next update may see an actual new tape.
        self.prior["evidence"]["catalogue_observed_at_utc"] = "2024-11-27T22:01:00Z"
        self.new["catalogue_observed_at_utc"] = "2024-11-29T08:00:00Z"
        self.http.get_trade_tape_sample.side_effect = lambda symbol, day: tape(
            symbol, day, {"2024-11-27": "A"}[day]
        )
        record_exchange_changes(self.new, self.prior, self.http)
        due = next_dating_time(self.new)
        self.assertEqual(due, "2024-11-29T18:15:00+00:00")
        self.assertFalse(dating_due(due, "2024-11-29T13:14:59-05:00"))
        self.assertTrue(dating_due(due, "2024-11-29T13:15:00-05:00"))
        self.assertNotIn("effective_session", self.new[FIELD][0])
        self.http.get_trade_tape_sample.assert_called_once_with("ALPHA", "2024-11-27")
        self.http.get_trade_tape_sample.side_effect = AssertionError("premature or repeated request")
        early = report("2024-11-29T18:14:59Z", lookup("ALPHA", "NASDAQ"))
        record_exchange_changes(early, {"evidence": self.new}, self.http)
        self.assertIs(early[FIELD][0], self.new[FIELD][0])
        self.http.get_trade_tape_sample.side_effect = lambda symbol, day: tape(
            symbol, day, {"2024-11-29": "C"}[day]
        )
        complete = report("2024-12-06T21:00:00Z", lookup("ALPHA", "NASDAQ"))
        record_exchange_changes(complete, {"evidence": early}, self.http)
        (event,) = complete[FIELD]
        self.assertEqual((event["prior_session"], event["effective_session"]), ("2024-11-27", "2024-11-29"))
        self.assertEqual(event["to_observed_at_utc"], "2024-11-29T08:00:00Z")
        self.assertEqual(event["dating_observed_at_utc"], "2024-12-06T21:00:00Z")
        self.assertIsNone(next_dating_time(complete))
        self.http.get_trade_tape_sample.side_effect = AssertionError("repeated completion")
        record_exchange_changes(
            report("2024-12-10T20:00:00Z", lookup("ALPHA", "NASDAQ")), {"evidence": complete}, self.http
        )

    def test_deferred_selection_closes_on_missing_response_disabled_adapter_or_invalidated_owner(self):
        self.new["catalogue_observed_at_utc"] = "2024-05-25T12:00:00Z"
        self.http.get_trade_tape_sample.side_effect = lambda symbol, day: tape(
            symbol, day, {"2024-05-24": "A"}[day]
        )
        record_exchange_changes(self.new, self.prior, self.http)
        self.assertEqual(next_dating_time(self.new), "2024-05-28T20:15:00+00:00")
        for now, asset, observed, calls in (
            ("2024-05-28T22:00:00Z", lookup("ALPHA", "NASDAQ"), True, 1),
            ("2024-05-28T22:00:00Z", lookup("ALPHA", "NASDAQ"), False, 0),
            ("2024-05-28T22:00:00Z", lookup("ALPHA", "NASDAQ", observed_at_utc="2024-05-25T12:00:00Z"), True, 0),
            ("2024-05-28T10:00:00Z", lookup("ALPHA", "NYSE", "different"), True, 0),
            ("2024-05-28T10:00:00Z", lookup("BETA", "NASDAQ", kind="asset_id"), True, 0),
            ("2024-05-28T10:00:00Z", lookup("ALPHA", "AMEX"), True, 0),
        ):
            with self.subTest(now=now, asset=asset, observed=observed):
                new = report(now, asset)
                api = Mock()
                api.get_trade_tape_sample.return_value = None
                record_exchange_changes(new, {"evidence": self.new}, api, observe_new_tapes=observed)
                self.assertEqual(api.get_trade_tape_sample.call_count, calls)
                self.assertIsNone(next_dating_time(new))
                self.assertNotIn("effective_session", new[FIELD][0])
                if asset["asset"]["exchange"] == "AMEX":
                    self.assertEqual([e["to_exchange"] for e in new[FIELD]], ["NASDAQ", "AMEX"])

    def test_missing_wrong_tape_out_of_time_and_unavailable_have_no_effective_day(self):
        for latest in (
            None,
            {"outcome": "unavailable", "http_status": 403},
            {**tape("ALPHA", "2024-05-28", "C"), "http_status": 404},
            {**tape("ALPHA", "2024-05-28", "C"), "returned_symbol": "BETA"},
            {**tape("ALPHA", "2024-05-28", "C"), "asof": "2024-05-28"},
            tape("ALPHA", "2024-05-28", "B"),
            {**tape("ALPHA", "2024-05-28", "C"), "trade": {"z": "C", "t": "2024-05-28T23:00:00Z"}},
        ):
            with self.subTest(latest=latest):
                new = report("2024-05-28T22:01:00Z", lookup("ALPHA", "NASDAQ"))
                self.http.get_trade_tape_sample.side_effect = lambda symbol, day: (
                    latest if day == "2024-05-28" else tape(symbol, day, "A")
                )
                record_exchange_changes(new, self.prior, self.http)
                self.assertNotIn("effective_session", new[FIELD][0])
                self.assertNotIn("prior_session", new[FIELD][0])

    def test_wide_observation_gap_bisects_only_recent_confirmed_market_sessions(self):
        self.prior["evidence"]["catalogue_observed_at_utc"] = "2022-01-01T22:00:00Z"
        dates = []

        def respond(symbol, day):
            self.assertNotIn(day, dates)
            self.assertGreaterEqual(day, "2024-05-07")
            dates.append(day)
            return tape(symbol, day, "C" if day >= "2024-05-15" else "A")

        self.http.get_trade_tape_sample.side_effect = respond
        record_exchange_changes(self.new, self.prior, self.http)
        (event,) = self.new[FIELD]
        self.assertEqual((event["prior_session"], event["effective_session"]), ("2024-05-14", "2024-05-15"))
        self.assertLessEqual(len(dates), 8)
        self.assertEqual([s["session"] for s in event["tape_samples"]], sorted(dates))

    def test_no_calendar_coverage_records_only_snapshots(self):
        with patch("libs.alpaca_lineage_build.exchange_changes.market_sessions_between", return_value=()):
            record_exchange_changes(self.new, self.prior, self.http)
        self.http.get_trade_tape_sample.assert_not_called()
        self.assertNotIn("effective_session", self.new[FIELD][0])

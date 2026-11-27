"""An auction screens dates. Literal trades own the dated network and listing facts."""

from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock, patch

from libs.alpaca_lineage_build.historical_listing import (
    RangeScreen,
    assess_label_boundaries,
    assess_range,
    screen_range,
)
from libs.common.us_equity_market_session_calendar import market_sessions_between


def auction(day, venue=None, *, opens=(), closes=()):
    return {
        "d": day,
        "o": [{"c": "O", "x": code} for code in ([venue] if venue else opens)],
        "c": [{"c": "6", "x": code} for code in ([venue] if venue else closes)],
    }


class Client:
    def __init__(self, auctions, tapes):
        self.rows = auctions
        self.tapes = tapes
        self.auction_calls = []
        self.trade_calls = []

    def auction_days(self, symbol, first, last):
        self.auction_calls.append((symbol, first, last))
        return iter(self.rows) if self.rows is not None else None

    def get_trade_tape_sample(self, symbol, session):
        key = (symbol, session)
        if key in self.trade_calls:
            raise AssertionError(f"duplicate evidence request {key}")
        self.trade_calls.append(key)
        tape = self.tapes[session]
        wire = {"symbol": symbol, "session": session, "asof": "-", "feed": "sip", "http_status": 200}
        if isinstance(tape, dict):
            return {**wire, **tape}
        if tape is None:
            return {**wire, "outcome": "no_trades"}
        trade = {"z": tape, "t": session + "T14:30:00Z", "x": "Q"}
        return {**wire, "outcome": "observed", "trade": trade}


class HistoricalListingTests(TestCase):
    def setUp(self):
        self.days = market_sessions_between("2024-05-20", "2024-05-31")

    def assess(self, client, days=None):
        dates = self.days if days is None else days
        return assess_range("FUTURE", dates[0], dates[-1], client)

    def test_stable_screen_uses_endpoints_only_and_never_turns_auction_venues_into_exchange(self):
        client = Client([auction(d, "Q") for d in self.days], {self.days[0]: "A", self.days[-1]: "A"})
        result = self.assess(client)
        self.assertEqual(client.auction_calls, [("FUTURE", self.days[0], self.days[-1])])
        self.assertEqual([p["exchange"] for p in result["observations"]], ["NYSE", "NYSE"])
        self.assertEqual(result["changes"], [])
        self.assertEqual(result["status"], "sampled")
        self.assertEqual(result["limits"], ["sampled_sessions_only"])
        self.assertEqual(result["diagnostics"]["trade_sessions_sampled"], 2)

    def test_single_session_reversion_is_preserved_even_when_owned_endpoints_match(self):
        days = self.days
        midpoint = 4
        rows = [auction(d, "Q" if i == midpoint else "N") for i, d in enumerate(days)]
        # Mislabelled/non-auction messages can never create another selection.
        rows[1]["c"].append({"c": "X", "x": "Q"})
        allowed = {0, midpoint - 1, midpoint, midpoint + 1, len(days) - 1}
        client = Client(rows, {days[i]: "C" if i == midpoint else "A" for i in allowed})
        result = self.assess(client)
        self.assertEqual(result["status"], "changes_observed")
        self.assertEqual(
            [(e["from_exchange"], e["to_exchange"], e["effective_session"]) for e in result["changes"]],
            [("NYSE", "NASDAQ", days[midpoint]), ("NASDAQ", "NYSE", days[midpoint + 1])],
        )
        self.assertEqual(result["changes"][1]["from_session"], "2024-05-24")
        self.assertEqual(result["changes"][1]["to_session"], "2024-05-28")
        self.assertEqual(result["diagnostics"]["auction_venue_changes"], 2)
        self.assertEqual(len(client.trade_calls), len(allowed))

    def test_owned_trade_exposes_reversion_with_a_stable_auction_screen_without_refetching_it(self):
        days = self.days[:5]
        midpoint = days[2]
        held = Client([], {midpoint: "C"}).get_trade_tape_sample("FUTURE", midpoint)
        client = Client([auction(d, "N") for d in days], {d: "A" for d in days if d != midpoint})
        result = assess_range("FUTURE", days[0], days[-1], client, retained_points=[held])
        self.assertEqual(
            [(r["from_tape"], r["to_tape"], r["effective_session"]) for r in result["changes"]],
            [("A", "C", midpoint), ("C", "A", days[3])],
        )
        self.assertEqual(result["diagnostics"]["auction_venue_changes"], 0)
        self.assertCountEqual(client.trade_calls, [("FUTURE", d) for d in days if d != midpoint])

    def test_retained_literal_conflict_blocks_refinement_and_invalid_routes_cannot_select_a_probe(self):
        days = self.days[:5]
        midpoint = days[2]
        first = Client([], {midpoint: "A"}).get_trade_tape_sample("FUTURE", midpoint)
        other = Client([], {midpoint: "C"}).get_trade_tape_sample("FUTURE", midpoint)
        wrong = Client([], {days[1]: "B"}).get_trade_tape_sample("FUTURE", days[1])
        held = [
            first,
            other,
            {**wrong, "symbol": "REUSED"},
            {**wrong, "returned_symbol": "REUSED"},
            {**wrong, "feed": "otc"},
            {**wrong, "asof": days[0]},
            {**wrong, "through_session": days[-1]},
            {**wrong, "session": self.days[-1]},
        ]
        client = Client([], {days[0]: "A", days[-1]: "C"})
        result = assess_range("FUTURE", days[0], days[-1], client, retained_points=held)
        self.assertCountEqual(client.trade_calls, [("FUTURE", days[0]), ("FUTURE", days[-1])])
        self.assertEqual([p["session"] for p in result["observations"]], [days[0], days[-1]])
        self.assertEqual(result["diagnostics"]["conflicting_sessions"][0]["session"], midpoint)
        self.assertIn("conflicting_trade_tapes", result["limits"])
        self.assertNotIn("effective_session", result["changes"][0])

    def test_multi_venue_contradiction_and_same_tape_venue_change_do_not_publish_transfer(self):
        dates = self.days[:5]
        rows = [
            auction(dates[0], opens=["N", "Q"], closes=["N", "Q"]),
            auction(dates[1], opens=["N", "Q"], closes=["N"]),
            auction(dates[2], opens=["N"], closes=["Q"]),
            auction(dates[3], opens=["Q", "P"], closes=["Q"]),
            auction(dates[3], opens=["P"]),  # splitting one day over two rows cannot select it twice
            auction(dates[4], "Q"),
        ]
        client = Client(rows, {dates[i]: "B" for i in (0, 1, 3, 4)})
        result = self.assess(client, dates)
        self.assertEqual(result["diagnostics"]["ambiguous_auction_sessions"], 2)
        self.assertEqual(result["diagnostics"]["auction_venue_changes"], 1)
        self.assertEqual(result["changes"], [])
        self.assertIn("multiple_listings_on_tape_b", result["limits"])
        self.assertTrue(all("exchange" not in p for p in result["observations"]))

    def test_refinement_finds_a_third_tape_and_records_only_named_exchange_endpoints(self):
        dates = self.days[:7]
        client = Client([], {d: "A" if i < 2 else "B" if i < 5 else "C" for i, d in enumerate(dates)})
        result = self.assess(client, dates)
        changes = result["changes"]
        self.assertEqual([(v["from_tape"], v["to_tape"]) for v in changes], [("A", "B"), ("B", "C")])
        self.assertEqual([v["effective_session"] for v in changes], [dates[2], dates[5]])
        self.assertNotIn("to_exchange", changes[0])
        self.assertNotIn("from_exchange", changes[1])
        self.assertIn("incomplete_auction_coverage", result["limits"])

    def test_session_without_a_trade_keeps_change_bounded_and_queries_nothing_outside_ownership(self):
        dates = self.days[2:5]
        client = Client(
            [auction(self.days[1], "N"), auction(self.days[5], "Q")],
            {dates[0]: "A", dates[1]: None, dates[2]: "C"},
        )
        result = self.assess(client, dates)
        (change,) = result["changes"]
        self.assertEqual((change["from_session"], change["to_session"]), (dates[0], dates[2]))
        self.assertNotIn("effective_session", change)
        self.assertIn("change_date_bounded", result["limits"])
        self.assertEqual(result["diagnostics"]["trade_outcomes"], {"no_trades": 1, "observed": 2})

    def test_opposing_tapes_on_a_date_are_retained_and_stop_further_search_across_it(self):
        dates = self.days[:5]
        middle = dates[2]
        witnesses = [
            {"t": middle + "T14:30:00Z", "z": "A"},
            {"t": middle + "T14:30:05Z", "z": "C"},
        ]
        ambiguous = {"outcome": "conflicting_tapes", "conflicting_trades": witnesses}
        client = Client([], {dates[0]: "A", middle: ambiguous, dates[-1]: "C"})
        result = self.assess(client, dates)
        self.assertEqual(len(client.trade_calls), 3)
        self.assertEqual([p["session"] for p in result["observations"]], [dates[0], dates[-1]])
        (change,) = result["changes"]
        self.assertNotIn("effective_session", change)
        self.assertIn("conflicting_trade_tapes", result["limits"])
        self.assertEqual(
            result["diagnostics"]["conflicting_sessions"], [{"session": middle, "conflicting_trades": witnesses}]
        )
        self.assertEqual(result["diagnostics"]["trade_outcomes"], {"observed": 2, "conflicting_tapes": 1})
        single = assess_range("FUTURE", middle, middle, Client([], {middle: ambiguous}))
        self.assertEqual(single["status"], "unresolved")
        self.assertEqual(single["observations"], [])
        self.assertNotIn("no_recognized_trade_tape", single["limits"])
        self.assertEqual(assess_label_boundaries([single, {**single, "symbol": "RENAMED"}]), [])

    def test_missing_endpoint_uses_represented_auction_date_without_rechecking_the_day(self):
        dates = self.days[:4]
        client = Client(
            [auction(dates[1], "Q"), auction(dates[2], "Q")],
            {
                dates[0]: None,
                dates[1]: "C",
                dates[2]: "C",
                dates[3]: {"outcome": "unavailable", "http_status": 403},
            },
        )
        result = self.assess(client, dates)
        self.assertEqual([p["session"] for p in result["observations"]], list(dates[1:3]))
        self.assertEqual(result["status"], "sampled")
        self.assertEqual(
            result["diagnostics"]["trade_outcomes"], {"no_trades": 1, "observed": 2, "unavailable": 1}
        )

    def test_wrong_symbol_mapped_routes_unknown_and_unavailable_are_completed_without_claims(self):
        dates = self.days[:2]
        for extra in ({"returned_symbol": "OTHER"}, {"asof": dates[0]}, {"feed": "otc"}):
            client = Client(
                None,
                {
                    dates[0]: {
                        "outcome": "observed",
                        "trade": {"z": "C", "t": dates[0] + "T13:30:00Z"},
                        **extra,
                    },
                    dates[1]: "Z",
                },
            )
            with self.subTest(extra=extra):
                result = self.assess(client, dates)
                self.assertEqual(result["status"], "unresolved")
                self.assertEqual(result["observations"], [])
                self.assertEqual(result["changes"], [])
                self.assertIn("auction_screen_unavailable", result["limits"])
                self.assertEqual(result["diagnostics"]["trade_outcomes"], {"unavailable": 1, "unknown_tape": 1})

    def test_single_session_and_empty_calendar_range_do_no_extra_work(self):
        date = self.days[0]
        client = Client([], {date: "C"})
        result = assess_range("FUTURE", date, date, client)
        self.assertEqual(result["diagnostics"]["trade_sessions_sampled"], 1)
        self.assertNotIn("sampled_sessions_only", result["limits"])
        no_calls = Mock(spec=[])
        result = assess_range("FUTURE", "2024-05-25", "2024-05-27", no_calls)
        self.assertEqual(result["limits"], ["no_calendar_sessions"])
        with self.assertRaises(ValueError):
            assess_range("FUTURE", "2024-05-27", "2024-05-25", no_calls)

    def test_completed_screen_roundtrip_selects_only_positive_open_second_groups(self):
        days = self.days[:4]
        rows = [auction(days[i], "Q") for i in (0, 2, 3)]
        rows[0]["o"][0]["t"] = days[0] + "T13:30:00.000000001Z"
        rows[1]["o"][0]["t"] = days[2] + "T13:30:00Z"  # not a selected probe
        rows[2]["o"][0]["t"] = days[3] + "T13:30:01Z"
        rows[2]["c"].extend(
            [
                {"c": "6", "x": "Q", "t": days[3] + "T16:00:02.12345-04:00"},
                {"c": "6", "x": "Q", "t": days[3] + "T01:30:00Z"},  # different New York date
                {"c": "X", "x": "Q", "t": days[3] + "T20:00:01Z"},
            ]
        )
        screen = screen_range("FUTURE", days[0], days[-1], iter(rows))
        self.assertEqual(screen.initial_sessions, (days[0], days[-1]))
        self.assertEqual(screen.batch_sessions, (days[0],))
        self.assertEqual(
            screen.auction_seconds,
            {
                days[0]: (days[0] + "T13:30:00Z",),
                days[3]: (days[3] + "T13:30:01Z", days[3] + "T20:00:02Z"),
            },
        )
        serialized = screen.to_document()
        self.assertEqual(serialized["auction_runs"], [0, 1, 1, 2])
        restored = RangeScreen.from_document(serialized)
        self.assertEqual(restored, screen)
        legacy = {k: v for k, v in serialized.items() if k != "auction_seconds"}
        legacy["schema_version"] = 1
        self.assertEqual(RangeScreen.from_document(legacy).auction_seconds, {days[0]: (days[0] + "T13:30:00Z",)})
        api = Client(None, {days[0]: "C", days[-1]: "C"})
        api.auction_days = Mock(side_effect=AssertionError("re-screened completed auctions"))
        self.assertEqual(assess_range("FUTURE", days[0], days[-1], api, screen=restored)["status"], "sampled")
        with self.assertRaisesRegex(ValueError, "another selected range"):
            assess_range("OTHER", days[0], days[-1], api, screen=restored)
        with patch(
            "libs.alpaca_lineage_build.historical_listing._calendar_scope",
            side_effect=AssertionError("same calendar"),
        ):
            self.assertEqual(RangeScreen.from_document(serialized), screen)
        with patch(
            "libs.alpaca_lineage_build.historical_listing.us_market_hours_sha256", return_value="future appended"
        ):
            self.assertEqual(RangeScreen.from_document(serialized), screen)
            with self.assertRaisesRegex(ValueError, "calendar changed"):
                RangeScreen.from_document(legacy)
            with (
                patch(
                    "libs.alpaca_lineage_build.historical_listing.market_session_window", return_value=days[1:]
                ),
                self.assertRaisesRegex(ValueError, "calendar changed"),
            ):
                RangeScreen.from_document(serialized)
            with (
                patch(
                    "libs.alpaca_lineage_build.historical_listing.regular_market_hours",
                    return_value=("10:00", "16:00"),
                ),
                self.assertRaisesRegex(ValueError, "calendar changed"),
            ):
                RangeScreen.from_document(serialized)

    def test_shared_session_axis_and_compact_dense_and_segmented_indices_preserve_refinement(self):
        days = self.days
        for indices in ([], [1], list(range(1, len(days) - 1)), [0, 1, 3, 5, 6]):
            with self.subTest(indices=indices):
                rows = [auction(days[i], "Q") for i in indices]
                screen = screen_range("FUTURE", days[0], days[-1], rows)
                document = screen.to_document()
                restored = RangeScreen.from_document(document)
                self.assertEqual(restored, screen)
                self.assertEqual(restored.to_document(), document)
                self.assertEqual(list(restored.auction_days), indices)
                self.assertEqual(tuple(restored.sessions[::-2]), days[::-2])
                shifted = screen_range("OTHER", days[1], days[-1], [])
                self.assertIs(restored.sessions.dates, shifted.sessions.dates)
                if len(document["auction_runs"]) <= 2:
                    self.assertIsInstance(restored.auction_days, range)
                else:
                    self.assertTrue(restored.auction_days.readonly)
                    self.assertEqual(restored.auction_days.nbytes, len(indices) * restored.auction_days.itemsize)
                tapes = {d: "A" if i < len(days) // 2 else "C" for i, d in enumerate(days)}
                ordinary = replace(screen, sessions=days, auction_days=tuple(indices))
                expected = assess_range("FUTURE", days[0], days[-1], Client([], tapes), screen=ordinary)
                result = assess_range("FUTURE", days[0], days[-1], Client([], tapes), screen=restored)
                self.assertEqual(result, expected)

    def test_exact_adjacent_label_transfer_of_the_supplied_identity_and_ambiguous_ranges(self):
        def endpoint(symbol, first, last, tape, *, witnessed=True):
            api = Client([], {first: tape if witnessed else None, last: tape if witnessed else None})
            return assess_range(symbol, first, last, api)

        old = endpoint("OLD", "2024-05-24", "2024-05-24", "A")
        new = endpoint("NEW", "2024-05-28", "2024-05-28", "C")
        later = endpoint("LATER", "2024-05-29", "2024-05-29", "B")
        moves = assess_label_boundaries([later, old, new])
        self.assertEqual([m["effective_session"] for m in moves], ["2024-05-28", "2024-05-29"])
        self.assertEqual((moves[0]["from_symbol"], moves[0]["to_symbol"]), ("OLD", "NEW"))
        self.assertNotIn("to_exchange", moves[1])
        same = {**new, "symbol": "OLD"}
        same_and_renamed = assess_label_boundaries([old, same, later], include_same_symbol=True)
        self.assertEqual(
            same_and_renamed,
            [{**moves[0], "to_symbol": "OLD"}, {**moves[1], "from_symbol": "OLD"}],
        )
        with patch(
            "libs.alpaca_lineage_build.historical_listing.next_market_session_after",
            side_effect=lambda day: {"2024-05-28": "2024-05-29"}[day],
        ):
            self.assertEqual(
                assess_label_boundaries([old, new, later], windows=[("2024-05-29", "2024-05-29")]),
                [moves[1]],
            )
            self.assertEqual(
                assess_label_boundaries(
                    [old, same, later], windows=[("2024-05-29", "2024-05-29")], include_same_symbol=True
                ),
                same_and_renamed[1:],
            )
        for rows in (
            [old, {**new, "symbol": "OLD"}],
            [old, later],
            [old, {**new, "observations": []}],
            [old, new, {**old, "symbol": "OTHER"}],
            [old, new, {**new, "symbol": "OTHER"}],
            [old, new, {**old, "symbol": "OVERLAP", "last_session": "2024-05-30"}],
        ):
            with self.subTest(rows=[r["symbol"] for r in rows]):
                self.assertEqual(assess_label_boundaries(rows), [])
        for invalid in (
            {**same, "limits": ["competing_published_owners"]},
            {**same, "observations": [], "status": "unresolved"},
            {**same, "observations": [{**same["observations"][0], "session": "2024-05-29"}]},
            {**same, "first_session": old["last_session"]},
        ):
            with self.subTest(invalid=invalid):
                self.assertEqual(assess_label_boundaries([old, invalid], include_same_symbol=True), [])

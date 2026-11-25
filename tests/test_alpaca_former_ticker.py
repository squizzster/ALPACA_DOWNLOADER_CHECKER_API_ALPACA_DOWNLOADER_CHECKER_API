"""Former ticker allocations from native dividends and independent mapped price histories."""

from copy import deepcopy
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import former_ticker
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_security_lineage.inference_evidence import accept_provider_evidence
from libs.common.us_equity_market_session_calendar import market_sessions_between


def asset(symbol, identifier, *, exchange="OTC", status="active"):
    return {"id": identifier, "symbol": symbol, "class": "us_equity", "exchange": exchange, "status": status}


def lookup(symbol, kind, record):
    return {
        "lookup_kind": kind,
        "lookup": symbol,
        "found": bool(record),
        "outcome": "found" if record else "not_found",
        "http_status": 200 if record else 404,
        "asset": record,
        **({"observed_at_utc": "2026-08-07T16:00:00Z"} if kind == "symbol" and record is None else {}),
    }


def dividend(symbol, cusip, identifier):
    return {
        "id": identifier,
        "action_type": "cash_dividend",
        "ex_date": "2025-04-23",
        "rate": "0.15",
        "cusip": cusip,
        "symbol": symbol,
    }


def facts(symbol, lookups, actions=()):
    return {
        "query": {
            "input_symbol": symbol,
            "corporate_actions_start": "2024-01-01",
            "corporate_actions_end": "2026-08-07",
            "corporate_actions_data_quality": "complete",
        },
        "asset_lookups": lookups,
        "corporate_actions": list(actions),
    }


def route(symbol):
    dates = tuple(date(2025, 4, day) for day in (21, 22, 23, 24, 25))
    rows = [
        dict(t=d.isoformat(), o=str(i), h=str(i + 4), l=str(i - 1), c=str(i + 1), v="5000", n="500", vw=str(i))
        for i, d in enumerate(dates, 10)
    ]
    return [
        (
            SimpleNamespace(rows=rows),
            {
                "symbol": symbol,
                "adjustment": "raw",
                "asof": "2026-08-07",
                "request_to": "2026-08-07T00:00:00Z",
                "feed": "sip",
                "timeframe": "1Day",
            },
            dates,
        )
    ]


class FormerTickerTests(TestCase):
    def setUp(self):
        self.former, self.terminal = "ORCHARD", "HARBOR"
        self.security = asset(self.terminal, "original-issue")
        self.cusip = "152637489"
        self.native = dividend(self.former, self.cusip, "native-dividend")
        self.sources = {label: route(label) for label in (self.former, self.terminal)}
        self.generations = {self.former: "former-generation", self.terminal: "archive-generation"}
        self.evidence = {
            self.former: facts(
                self.former,
                [lookup(self.former, "symbol", None), lookup(self.cusip, "cusip", self.security)],
                [self.native],
            ),
            self.terminal: facts(self.terminal, [lookup(self.terminal, "symbol", self.security)]),
        }

    def select(self):
        return former_ticker.select(self.sources, self.evidence, pricing_generations=self.generations)

    def literal_edges(self, outcomes):
        direct, request, dates = self.sources[self.former][0]
        self.evidence[self.former]["former_ticker_literal_bars"] = {
            self.terminal: {
                "source_generations": self.generations.copy(),
                "source_asofs": {
                    self.former: request["asof"],
                    self.terminal: self.sources[self.terminal][0][1]["asof"],
                },
                "observations": [
                    {
                        "symbol": self.former,
                        "session": dates[index].isoformat(),
                        "asof": "-",
                        "feed": "sip",
                        "adjustment": "raw",
                        "outcome": outcome,
                        **({"bar": direct.rows[index].copy()} if outcome == "observed" else {}),
                    }
                    for index, outcome in outcomes
                ],
            }
        }

    def test_two_mapped_routes_can_echo_an_unnamed_predecessor(self):
        # The first two market sessions traded under an unreported earlier name.
        # Both mapped routes can reproduce all five prices under their requested label.
        self.sources[self.former][0][1]["asof"] = "2025-04-23"
        self.literal_edges([(0, "no_bars"), (4, "observed")])
        (found,) = self.select()[0][self.former][0].intervals
        self.assertEqual(
            (found.start_date_inclusive, found.end_date_exclusive), (date(2025, 4, 23), date(2025, 4, 26))
        )
        self.literal_edges([(0, "observed"), (4, "observed")])
        (extended,) = self.select()[0][self.former][0].intervals
        self.assertEqual(extended.start_date_inclusive, date(2025, 4, 21))
        self.generations[self.terminal] = "changed-archive-generation"
        (restricted,) = self.select()[0][self.former][0].intervals
        self.assertEqual(
            (restricted.start_date_inclusive, restricted.end_date_exclusive),
            (date(2025, 4, 23), date(2025, 4, 24)),
        )
        bundle = self.evidence[self.former]["former_ticker_literal_bars"][self.terminal]
        bundle["source_generations"] = {key: None for key in (self.former, self.terminal)}
        self.generations = {"UNRELATED": "other-generation"}
        self.assertEqual(self.select()[0][self.former][0].intervals, (restricted,))

    def test_actual_literal_no_trades_contradicts_an_occupied_date_and_denial_is_neutral(self):
        sample = {
            "symbol": self.former,
            "session": "2025-04-23",
            "asof": "-",
            "feed": "sip",
            "outcome": "no_trades",
        }
        self.evidence[self.former]["historical_trade_tape"] = sample
        intervals, limits = self.select()
        self.assertFalse(intervals)
        self.assertEqual(limits[self.former][0]["limit"], "literal_observation_contradicts_mapped_trades")
        self.assertEqual(
            limits[self.former][0]["candidate_actions"],
            [{"date": "2025-04-23", "action_type": "cash_dividend", "action_id": "native-dividend"}],
        )
        sample["outcome"] = "unavailable"
        self.assertIn(self.former, self.select()[0])

    def test_current_lookup_requires_actual_observation_on_the_mapping_date(self):
        lookup_result = self.evidence[self.former]["asset_lookups"][0]
        lookup_result["observed_at_utc"] = "2026-09-05T12:00:00Z"
        self.evidence[self.former]["catalogue_observed_at_utc"] = "2026-08-07T16:00:00Z"
        (period,) = self.select()[0][self.former][0].intervals
        self.assertEqual(period.start_date_inclusive, date(2025, 4, 23))
        self.literal_edges([(0, "observed"), (4, "observed")])
        first = self.evidence[self.former]["former_ticker_literal_bars"][self.terminal]["observations"][0]
        first["bar"]["v"] = "27"
        (period,) = self.select()[0][self.former][0].intervals
        self.assertEqual(
            (period.start_date_inclusive, period.end_date_exclusive), (date(2025, 4, 23), date(2025, 4, 26))
        )

    def test_future_spelling_active_otc_or_inactive_listed_and_ex_date_pin(self):
        for exchange, status, asof in (
            ("OTC", "active", "-"),
            ("NYSE", "inactive", "2026-08-07"),
            ("NASDAQ", "inactive", "2025-04-23"),
        ):
            with self.subTest(exchange=exchange, status=status, asof=asof):
                self.security.update(exchange=exchange, status=status)
                self.sources[self.former][0][1]["asof"] = asof
                if asof == "2025-04-23":
                    self.evidence[self.former]["asset_lookups"][0] = lookup(
                        self.former, "symbol", asset(self.former, "new-issuer")
                    )
                intervals, limits = self.select()
                self.assertEqual(bool(limits), asof == "2025-04-23")
                self.assertIs(intervals[self.former][0], intervals[self.terminal][0])
                (found,) = intervals[self.former][0].intervals
                self.assertEqual(found.identity_key(), (self.former, self.cusip, "original-issue"))
                self.assertEqual(
                    (found.start_date_inclusive, found.end_date_exclusive),
                    (date(2025, 4, 23), date(2025, 4, 24))
                    if asof == "2025-04-23"
                    else (date(2025, 4, 21), date(2025, 4, 26)),
                )
                self.assertEqual(found.applied_rules[0].supporting_action_ids, ("native-dividend",))

    def test_same_api_observations_on_a_future_install_with_an_updated_calendar(self):
        # A later installation supplies its then-current market calendar and provider observations.
        future = tuple(date(2031, 4, day) for day in (21, 22, 23, 24, 25))
        for source, items in self.sources.items():
            download, request, _ = items[0]
            for row, session in zip(download.rows, future, strict=True):
                row["t"] = session.isoformat()
            request.update(asof="2032-06-15", request_to="2032-06-15T00:00:00Z")
            self.sources[source] = [(download, request, future)]
            self.evidence[source]["query"].update(
                corporate_actions_start="2030-01-01", corporate_actions_end="2032-06-15"
            )
        self.native["ex_date"] = "2031-04-23"
        self.evidence[self.former]["asset_lookups"][0]["observed_at_utc"] = "2032-06-15T16:00:00Z"
        with patch.object(
            former_ticker, "market_sessions_between", return_value=tuple(day.isoformat() for day in future)
        ) as calendar:
            choices, limits = self.select()
        calendar.assert_called_once_with("2031-04-21", "2031-04-25")
        self.assertFalse(limits)
        (period,) = choices[self.terminal][0].intervals
        self.assertEqual(
            (period.start_date_inclusive, period.end_date_exclusive), (future[0], date(2031, 4, 26))
        )

    def test_stock_paying_acquirer_can_date_its_own_equity_without_absorbing_the_acquiree(self):
        other = "341257086"
        merger = {
            "id": "native-transaction",
            "action_type": "stock_and_cash_merger",
            "effective_date": "2025-04-23",
            "process_date": "2025-04-25",
            "acquirer_symbol": self.former,
            "acquirer_cusip": self.cusip,
            "acquirer_rate": "0.02",
            "acquiree_symbol": "ELM",
            "acquiree_cusip": other,
            "acquiree_rate": "1",
            "cash_rate": "1.52",
        }
        # The independently priced acquired issue is in the same dependency group.
        self.evidence["ELM"] = facts("ELM", [lookup("ELM", "symbol", asset("ELM", "unrelated-issue"))])
        self.sources["ELM"] = route("ELM")
        data = self.evidence[self.former]
        data["corporate_actions"] = [merger]
        data["asset_lookups"].append(lookup(other, "cusip", asset("ELM", "unrelated-issue")))
        for kind in ("stock_merger", "stock_and_cash_merger"):
            with self.subTest(kind=kind):
                merger["action_type"] = kind
                intervals, limits = self.select()
                self.assertFalse(limits)
                self.assertEqual(set(intervals), {self.former, self.terminal})
                (period,) = intervals[self.former][0].intervals
                self.assertEqual(period.identity_key(), (self.former, self.cusip, "original-issue"))
                self.assertEqual((period.start_date_inclusive.day, period.end_date_exclusive.day), (21, 26))
                self.assertEqual(period.applied_rules[0].supporting_action_ids, ("native-transaction",))
                anchors = former_ticker.native_action_anchors(data, self.former)
                self.assertEqual([(a.action_type, a.day) for a in anchors], [(kind, date(2025, 4, 23))])
        for fields in (
            {"acquirer_symbol": "ELM", "acquiree_symbol": self.former},
            {"acquiree_symbol": self.former},
            {"acquiree_cusip": self.cusip},
            {"effective_date": ""},
            {"cash_rate": "0"},
            {"acquirer_rate": "0"},
            {"acquiree_rate": "NaN"},
            {"action_type": "cash_merger"},
        ):
            with self.subTest(fields=fields):
                data["corporate_actions"] = [{**merger, **fields}]
                self.assertFalse(self.select()[0])
        data["corporate_actions"] = [merger]
        data["asset_lookups"][-1] = lookup(other, "cusip", self.security)
        self.assertFalse(self.select()[0])
        data["asset_lookups"][-1] = lookup(other, "cusip", None)
        self.assertIn(self.terminal, self.select()[0])

    def test_scoped_partial_activity_requires_consistent_full_surrounding_trades(self):
        download = self.sources[self.terminal][0][0]
        original = deepcopy(download.rows)
        partial = {"v": "500", "n": "23", "h": "14", "l": "12", "vw": "12.8"}
        download.rows[2].update(partial)
        self.assertIn(self.former, self.select()[0])
        for index, fields in (
            (0, {"v": "500", "n": "23"}),
            (4, {"v": "500", "n": "23"}),
            (2, {"v": "0", "n": "0"}),
            (2, {**partial, "h": "34"}),
            (2, {**partial, "l": "5"}),
            (2, {**partial, "vw": "99"}),
            (2, {**partial, "o": "14"}),
            (2, {**partial, "c": "14"}),
            (2, {**partial, "n": "500"}),
            (2, {"vw": "12.4"}),
        ):
            with self.subTest(index=index, fields=fields):
                download.rows = deepcopy(original)
                download.rows[index].update(fields)
                intervals, limits = self.select()
                if index == 2:
                    self.assertFalse(intervals)
                else:
                    (found,) = intervals[self.former][0].intervals
                    self.assertEqual(
                        (found.start_date_inclusive, found.end_date_exclusive),
                        (date(2025, 4, 23), date(2025, 4, 24)),
                    )
                self.assertEqual(limits[self.former][0]["limit"], "raw_trading_observations_disagree")
        for items in self.sources.values():
            for row in items[0][0].rows:
                row.update(o="11", h="12", l="10", c="11", vw="11")
        self.assertEqual(self.select()[1][self.former][0]["limit"], "no_varying_full_traded_observations")

    def test_literal_terminal_unrelated_historical_mapping_and_incomplete_dates(self):
        former, terminal = (self.sources[s][0] for s in (self.former, self.terminal))
        for asof in ("-", "2025-04-22"):
            terminal[1]["asof"] = asof
            self.assertFalse(self.select()[0])
        terminal[1]["asof"] = "2026-08-07"
        former[1]["asof"] = "2025-05-01"
        (found,) = self.select()[0][self.former][0].intervals
        self.assertEqual(found.start_date_inclusive, date(2025, 4, 23))
        former[1]["asof"] = "2026-08-07"
        for label, items in self.sources.items():
            source, request, dates = items[0]
            self.sources[label] = [(source, request, dates[:1] + dates[2:])]
        self.assertEqual(
            self.select()[1][self.former][0]["limit"], "different_or_incomplete_observed_session_grids"
        )

    def test_conflicting_endpoint_actions_owners_and_missing_lookups(self):
        self.evidence["PEER"] = peer = facts("PEER", [])
        for symbol, cusip in ((self.former, "919191919"), (self.terminal, self.cusip), (self.terminal, "")):
            with self.subTest(symbol=symbol, cusip=cusip):
                peer["corporate_actions"] = [dividend(symbol, cusip, "other-action")]
                intervals, limits = self.select()
                self.assertFalse(intervals)
                self.assertEqual(limits[self.terminal][0]["limit"], "competing_dated_occupancy")
        peer["corporate_actions"] = []
        self.evidence[self.terminal]["asset_lookups"][0] = lookup(
            self.terminal, "symbol", asset(self.terminal, "rival")
        )
        self.assertFalse(self.select()[0])
        self.evidence[self.former]["asset_lookups"].pop()
        self.assertEqual(self.select(), ({}, {}))

    def test_two_indistinguishable_historical_labels_leave_all_routes_unallocated(self):
        alternate = "FOUNDRY"
        self.sources[alternate] = route(alternate)
        self.evidence[alternate] = facts(
            alternate,
            [lookup(alternate, "symbol", None), lookup(self.cusip, "cusip", self.security)],
            [dividend(alternate, self.cusip, "another-native-dividend")],
        )
        intervals, limits = self.select()
        self.assertFalse(intervals)
        for label in self.sources:
            (issue,) = limits[label]
            self.assertEqual(issue["reason_code"], "competing_former_ticker_occupations")
            self.assertEqual({item["former_symbol"] for item in issue["candidates"]}, {self.former, alternate})

    def test_competing_spelling_on_one_date_preserves_the_uncontested_dates(self):
        alternate = "FOUNDRY"
        self.sources[alternate] = route(alternate)
        self.sources[alternate][0][1]["asof"] = "2025-04-23"
        self.evidence[alternate] = facts(
            alternate,
            [lookup(alternate, "symbol", None), lookup(self.cusip, "cusip", self.security)],
            [dividend(alternate, self.cusip, "competing-spelling")],
        )
        intervals, limits = self.select()
        self.assertNotIn(alternate, intervals)
        self.assertIs(intervals[self.former][0], intervals[self.terminal][0])
        self.assertEqual(
            [
                (p.start_date_inclusive.day, p.end_date_exclusive.day)
                for p in intervals[self.former][0].intervals
            ],
            [(21, 23), (24, 26)],
        )
        tied = [
            issue
            for issue in limits[self.former]
            if issue["reason_code"] == "competing_former_ticker_occupations"
        ]
        self.assertEqual([(x["from"], x["end_exclusive"]) for x in tied], [("2025-04-23", "2025-04-24")])

    def test_disjoint_occupants_of_one_literal_ticker_keep_their_separate_owners(self):
        successor, cusip = "SECOND", "876543219"
        later = asset(successor, "second-issue")
        self.native["ex_date"] = "2025-04-21"
        other_dividend = {**dividend(self.former, cusip, "later-dividend"), "ex_date": "2025-04-25"}
        self.evidence[self.former]["corporate_actions"].append(other_dividend)
        self.evidence[self.former]["asset_lookups"].append(lookup(cusip, "cusip", later))
        self.evidence[successor] = facts(successor, [lookup(successor, "symbol", later)])
        self.sources[self.former][0][1]["asof"] = "-"
        for label, part in ((self.terminal, slice(0, 2)), (successor, slice(3, 5))):
            view, request, days = route(label)[0]
            view.rows = view.rows[part]
            self.sources[label] = [(view, request, days[part])]
        intervals, limits = self.select()
        self.assertFalse(limits)
        actual = {
            (p.identity_key(), p.start_date_inclusive.day, p.end_date_exclusive.day)
            for result in intervals[self.former]
            for p in result.intervals
        }
        self.assertEqual(
            actual,
            {
                ((self.former, self.cusip, "original-issue"), 21, 23),
                ((self.former, cusip, "second-issue"), 24, 26),
            },
        )
        self.assertTrue(all(result.interval_on(date(2025, 4, 23)) is None for result in intervals[self.former]))

    def test_later_occupant_must_have_left_and_stayed_absent_through_the_selected_asof(self):
        following = asset("SECOND", "second-issue")
        cusip = "876543219"
        changes = [
            {
                "id": "entry",
                "action_type": "name_change",
                "old_symbol": "SECOND",
                "new_symbol": self.former,
                "old_cusip": cusip,
                "new_cusip": cusip,
                "process_date": "2025-09-02",
            },
            {
                "id": "exit",
                "action_type": "name_change",
                "old_symbol": self.former,
                "new_symbol": "SECOND",
                "old_cusip": cusip,
                "new_cusip": cusip,
                "process_date": "2025-10-01",
            },
        ]
        body = facts(self.former, [lookup(cusip, "cusip", following)], changes)

        def later():
            return former_ticker._later_occupation(
                body, self.former, "original-issue", date(2025, 9, 2), "2026-08-07"
            )

        self.assertEqual(later(), ("entry", "exit"))
        body["corporate_actions"].append({**self.native, "ex_date": "2026-07-01"})
        self.assertIsNone(later())
        body["corporate_actions"].pop()
        body["query"]["corporate_actions_end"] = "2025-10-15"
        self.assertIsNone(later())

    def test_provider_boundary_and_group_retry_reuse_completed_selections(self):
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            accept_provider_evidence({**self.evidence[self.former], "event_supplements": [self.native]})
        evidence = {s: accept_provider_evidence(body) for s, body in self.evidence.items()}
        selected = former_ticker.native_action_anchors(evidence[self.former], self.former)
        self.assertTrue(selected)
        market_sessions_between("2025-04-21", "2025-04-25")
        cache = CorrelationCache(former_ticker_occupation=True)
        cache.rename_scope_complete = True
        cache.selected_evidence = evidence
        cache._name_change_sources = self.sources
        with (
            patch.object(Path, "open", side_effect=AssertionError("unexpected storage or research read")),
            patch.object(
                former_ticker,
                "exact_cusip_lookup_asset",
                side_effect=AssertionError("repeated lookup selection"),
            ),
        ):
            occupation, _ = cache.former_occupation(self.terminal)
            self.assertTrue(occupation)
        with patch.object(former_ticker, "select", side_effect=AssertionError("repeated group assessment")):
            self.assertIs(cache.former_occupation(self.former)[0][0], occupation[0])
            second_phase = CorrelationCache(former_ticker_occupation=True)
            second_phase.rename_scope_complete = True
            second_phase.inherit_continuity(cache)
            self.assertIs(second_phase.former_occupation(self.terminal)[0], occupation)
        # A newly observed provider generation owns a new immutable body and memo.
        updated = deepcopy(self.evidence[self.former])
        updated["asset_lookups"].pop()
        self.assertFalse(former_ticker.native_action_anchors(accept_provider_evidence(updated), self.former))

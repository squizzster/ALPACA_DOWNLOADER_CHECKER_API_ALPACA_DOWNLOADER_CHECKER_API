"""An unsupported post-split carry can be withheld without hiding the adjacent trades."""

import unittest
from copy import deepcopy
from datetime import date
from unittest.mock import Mock, patch

from alpaca_end_of_day._online_price_spans import assemble, spans
from libs.alpaca_pricing_download import untraded_split as carry
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.shifted_split import _PAIRS
from libs.alpaca_serving_catalogue.price_gaps import project

DAYS = ("2025-05-16", "2025-05-19", "2025-05-20", "2025-05-21", "2025-05-22")
CUT = DAYS[2]


def bar(day, value, traded):
    return dict(
        session=day,
        tickers=["MODEL"],
        timestamp=day + "T04:00:00Z",
        open=value,
        high=value,
        low=value,
        close=value,
        vwap=value if traded else "0",
        volume="100" if traded else "0",
        trade_count="4" if traded else "0",
    )


class UntradedSplitTests(unittest.TestCase):
    def setUp(self):
        self.raw = [bar(d, "0.5" if i < 4 else "2.40", i in (0, 4)) for i, d in enumerate(DAYS)]
        self.split = dict(
            id="action",
            action_type="reverse_split",
            symbol="MODEL",
            old_rate=10,
            new_rate=1,
            old_cusip="123456789",
            new_cusip="987654321",
            ex_date=CUT,
        )
        self.other = {
            m: {"bars": [bar(d, "5" if m in _PAIRS and d < CUT else ".5", False) for d in DAYS[1:4]]}
            for m in ADJUSTMENTS
            if m != "raw"
        }

    def plan(self, terminal=False):
        raw = self.raw[:-1] if terminal else self.raw
        read = Mock(return_value=self.other)
        result = carry.plan(
            raw, read, [self.split], "MODEL", CUT, DAYS[0], raw[-1]["session"], raw[-1]["session"]
        )
        read.assert_called_once_with(DAYS[1], DAYS[3])
        return result

    def test_scope_and_literal_tape_preserve_earlier_valid_carry(self):
        for terminal in (True, False):
            with self.subTest(terminal=terminal):
                plan = self.plan(terminal)
                self.assertEqual(plan["excluded_sessions"], list(DAYS[2:4]))
                self.assertEqual(plan["terminal"], terminal)
                journal = {}

                def put(k, v):
                    journal[k] = v
                    return v

                literal = {b["session"]: b for b in self.raw[2:4]}
                with patch.object(carry, "provider_rows", return_value=literal) as p:
                    result = carry.confirm(plan, None, journal.get, put)
                    p.assert_called_once_with(None, "MODEL", "-", "raw", DAYS[2], DAYS[3])
                self.assertEqual(result["reason"], carry.UNPRICED)
                with patch.object(
                    carry, "provider_rows", side_effect=AssertionError("repeated literal tape read")
                ):
                    self.assertEqual(carry.confirm(plan, None, journal.get, put), result)

    def test_competing_prices_genuine_reopening_and_terminal_scope(self):
        for failure in ("traded_interior", "compatible_reopening", "terminal_guess", "foreign_label"):
            read = Mock(side_effect=AssertionError("unneeded adjusted view read"))
            raw = deepcopy(self.raw)
            if failure == "traded_interior":
                raw[3] = bar(DAYS[3], "0.5", True)
            elif failure == "compatible_reopening":
                raw[-1] = bar(DAYS[-1], "5.02", True)
            elif failure == "terminal_guess":
                raw[-1] = bar(DAYS[-1], "0.5", False)
            else:
                raw[2]["tickers"] = ["OTHER"]
            with self.subTest(failure=failure), self.assertRaises(UnsupportedInterval):
                carry.plan(raw, read, [self.split], "MODEL", CUT, DAYS[0], DAYS[-1], "2025-07-31")
            read.assert_not_called()
        self.other["split"]["bars"][1]["close"] = "5"
        with self.assertRaises(UnsupportedInterval):
            self.plan()

    def test_available_trades_under_changed_symbol_defeat_exclusion(self):
        self.split["new_symbol"] = "NEXT"
        p = self.plan()
        native = {b["session"]: b for b in self.raw[2:4]}
        with patch.object(
            carry, "provider_rows", side_effect=[native, {DAYS[2]: bar(DAYS[2], "5", True)}]
        ) as read:
            self.assertEqual(carry.confirm(p, None, lambda _: None, lambda _, v: v)["outcome"], "unsupported")
            self.assertEqual(read.call_count, 2)

    def test_recovered_eligible_trades_must_cover_every_missing_day(self):
        prior = self.plan()
        self.assertIsNone(carry.restored_trades(prior, self.raw))
        native = deepcopy(self.raw)
        native[2:4] = [bar(DAYS[2], "4.11", True), bar(DAYS[3], "4.01", True)]
        pending = carry.restored_trades(prior, native)
        self.assertEqual(list(pending["witnesses"]), list(DAYS[2:4]))
        with patch.object(carry, "provider_rows", return_value={DAYS[2]: native[2]}):
            result = carry.confirm_recovery(pending, None, lambda k: None, lambda k, v: v)
        self.assertEqual(result["outcome"], "unsupported")
        with patch.object(carry, "provider_rows", return_value=pending["witnesses"]) as provider:
            restored = carry.confirm_recovery(pending, None, lambda k: None, lambda k, v: v)
            provider.assert_called_once_with(None, "MODEL", "-", "raw", DAYS[2], DAYS[3])
        self.assertEqual(restored["outcome"], "restored")

    def test_public_projection_and_physical_reads_intersect_only_supported_dates(self):
        summary = dict(
            first_session=DAYS[0],
            last_session=DAYS[-1],
            ticker_ranges=[dict(ticker="MODEL", first_session=DAYS[0], last_session=DAYS[-1])],
            coverage=dict(session_count=5, explained_missing_sessions=[]),
        )
        limit = {**self.plan(), "reason": carry.UNPRICED}
        partial = project(summary, [limit])
        self.assertEqual(partial["coverage"], dict(session_count=3, explained_missing_sessions=list(DAYS[2:4])))
        fewer = {**summary, "coverage": {"session_count": 4, "explained_missing_sessions": [DAYS[2]]}}
        self.assertEqual(project(fewer, [limit])["coverage"], partial["coverage"])
        t = {**limit, "terminal": True, "excluded_sessions": list(DAYS[2:])}
        truncated = project(summary, [t])
        self.assertEqual(truncated["last_session"], DAYS[1])
        self.assertEqual(truncated["coverage"], dict(session_count=2, explained_missing_sessions=[]))
        self.assertEqual(truncated["ticker_ranges"][0]["last_session"], DAYS[1])
        stale = {**t, "pending": True, "witness_end": DAYS[-1]}
        extended = {
            **summary,
            "last_session": "2025-05-23",
            "coverage": {**summary["coverage"], "session_count": 6},
        }
        projected = project(extended, [stale], [*DAYS, "2025-05-23"])
        self.assertEqual(projected["last_session"], DAYS[1])
        self.assertEqual(projected["coverage"]["session_count"], 2)
        when = [date.fromisoformat(d) for d in DAYS]

        def read(lo, hi):
            self.assertTrue(hi <= when[1] or lo > when[3])
            rows = [{"session": d.isoformat()} for d in when if lo <= d <= hi]
            return {m: {"bars": rows} for m in ("raw", "all")}

        both = assemble(("raw", "all"), spans(when[0], when[-1], partial["price_exclusions"]), read)
        self.assertIs(both["raw"]["bars"], both["all"]["bars"])
        self.assertEqual(both["all"]["bar_count"], 3)
        none = Mock(side_effect=AssertionError("opened prices entirely inside exclusion"))
        result = assemble(("raw",), spans(when[2], when[3], partial["price_exclusions"]), none)
        self.assertEqual(result["raw"]["bar_count"], 0)
        none.assert_not_called()

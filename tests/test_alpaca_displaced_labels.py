"""Select former labels by an indexed native occupation, including legitimate dated reuse and ambiguity."""

import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import occupation_pivots
from libs.alpaca_pricing_download import displaced_labels as prices
from libs.alpaca_pricing_download.contract import COLUMNS


class DisplacedTester(TestCase):
    def setUp(self):
        self.days = ["2020-07-01", "2020-07-02", "2020-07-06", "2020-07-07", "2020-07-08", "2020-07-09"]
        self.first, self.last, self.new = self.days[0], self.days[3], self.days[4]
        self.tape = {
            day: {
                "session": day,
                "timestamp": day + "T04:00:00Z",
                "tickers": ["OWNER"],
                **dict.fromkeys(COLUMNS[1:], "2"),
            }
            for day in self.days
        }
        self.tape[self.days[1]].update(volume="0", trade_count="0", vwap="0")
        self.former = {d: b for d, b in self.tape.items() if d <= self.last and b["volume"] != "0"}
        self.current = {d: b for d, b in self.tape.items() if d >= self.new}
        self.held = {}
        self.peer = False
        self.candidates = ["a"]

    def assess(self):
        def load(lo, hi):
            return [b for d, b in self.tape.items() if lo <= d <= hi]

        def provider(client, route, asof, mode, lo, hi):
            self.assertEqual((route, asof, mode), ("OWNER", "-", "raw"))
            return {d: b for d, b in self.current.items() if lo <= d <= hi}

        def source(token):
            return {
                "symbol": "OLD" + token.upper(),
                "prior_first_trade": self.first,
                "locator": "OCCUPATION",
                "public_id": "b" * 64,
            }, lambda: self.former

        def put(k, v):
            self.held[k] = v
            return v

        with patch.object(prices, "provider_rows", side_effect=provider):
            return prices.assess(
                "OWNER",
                self.first,
                self.new,
                self.last,
                self.days[-1],
                self.candidates,
                load,
                source,
                lambda *a: self.peer,
                None,
                self.held.get,
                put,
            )

    def test_discovery_selects_one_prior_owner_with_anchored_missing_carry(self):
        res = self.assess()
        self.assertEqual(res["change"]["new"], "OLDA")
        self.assertEqual(res["change"]["first"], self.first)
        self.assertEqual(res["change"]["next_session"], self.new)
        with patch.object(prices, "_matches", side_effect=AssertionError("accepted match re-proved")):
            self.assertEqual(self.assess(), res)

    def test_multiple_occupations_and_overlapping_current_trades_are_not_chosen(self):
        self.candidates.append("b")
        self.assertEqual(self.assess()["outcome"], "unsupported")
        self.candidates = ["a"]
        self.held.clear()
        self.peer = True
        self.assertEqual(self.assess()["outcome"], "unsupported")
        self.peer = False
        self.held.clear()
        self.current[self.last] = self.tape[self.last]
        self.assertEqual(self.assess()["outcome"], "unsupported")

    def test_unmatched_price_and_unanchored_carry_do_not_extend_dated_ticker(self):
        self.tape[self.days[1]] = {**self.tape[self.days[1]], "close": "3"}
        self.assertEqual(self.assess()["outcome"], "unsupported")
        self.tape[self.days[1]]["close"] = "2"
        self.former[self.last] = {**self.former[self.last], "trade_count": "4"}
        self.held.clear()
        self.assertEqual(self.assess()["outcome"], "unsupported")

    def test_dated_ownership_keeps_known_cusips_and_fills_only_supported_dates(self):
        from libs.alpaca_serving_catalogue.carried_labels import add_witnessed_occupation

        change = dict(new="OLD", first="2020-01-02", next_session="2020-01-10")
        other = dict(ticker="OTHER", start_inclusive=None, end_exclusive=None, cusip=None)
        known = dict(ticker="OLD", start_inclusive="2020-01-03", end_exclusive="2020-01-09", cusip="123456789")
        intervals = add_witnessed_occupation([other, known], change)
        self.assertEqual(intervals[:2], [other, known])
        self.assertEqual(
            [(r["start_inclusive"], r["end_exclusive"], r["cusip"]) for r in intervals[2:]],
            [("2020-01-02", "2020-01-03", None), ("2020-01-09", "2020-01-10", None)],
        )
        self.assertEqual(add_witnessed_occupation(intervals, change), intervals)

    def test_date_index_add_revoke_and_narrow_query(self):
        with tempfile.TemporaryDirectory() as t:
            store = Path(t)
            root = store / "end_of_day/lineage"
            root.mkdir(parents=True)
            path = root / "index.sqlite3"
            with closing(sqlite3.connect(path)) as db:
                db.execute("CREATE TABLE symbols(symbol TEXT PRIMARY KEY, receipt TEXT NOT NULL)")
                db.execute("INSERT INTO symbols VALUES('GROUP','{}')")
                db.commit()
                occupation_pivots.record(db, {"OTHER": {"pricing": "x"}})
                self.assertIsNone(
                    db.execute("SELECT 1 FROM sqlite_master WHERE name='prior_trading_occupations'").fetchone()
                )
                occupation_pivots.record(
                    db,
                    {
                        "GROUP": {
                            "dated_occupation_pivots": {"i": self.last},
                            "dated_occupation_tokens": {"i": "a" * 32},
                        }
                    },
                )
                db.commit()
                self.assertIn("a" * 32, occupation_pivots.select(store, self.last))
                self.assertFalse(occupation_pivots.select(store, self.first))
                self.assertTrue(
                    any(
                        "prior_trading_date" in r[-1]
                        for r in db.execute(
                            "EXPLAIN QUERY PLAN SELECT p.locator FROM prior_trading_occupations p WHERE p.day=?",
                            (self.last,),
                        )
                    )
                )
                occupation_pivots.record(db, {"GROUP": {"dated_occupation_pivots": {}}})
                db.commit()
                self.assertFalse(occupation_pivots.select(store, self.last))

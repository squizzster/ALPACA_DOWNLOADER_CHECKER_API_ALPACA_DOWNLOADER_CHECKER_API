"""Only an identified product with an observed NAV-relative quote convention goes offline."""

import unittest
from copy import deepcopy
from decimal import Decimal
from unittest.mock import Mock, patch

from libs.alpaca_lineage_build import price_basis_limits
from libs.alpaca_pricing_download import product_quote
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval

DATES = ("2025-05-15", "2025-05-16", "2025-05-19", "2025-05-20", "2025-05-21")


class ProductTests(unittest.TestCase):
    def setUp(self):
        self.pid = "a" * 64
        self.asset = dict(
            id="uid",
            name="Future Fund Exchange-Traded Managed NextShares",
            symbol="FUT",
            exchange="NASDAQ",
            **{"class": "us_equity"},
        )
        self.receipt = dict(
            identity_memberships=[dict(security_id=self.pid, member_alpaca_asset_ids=["uid"])],
            evidence={"asset_lookups": [{"lookup_kind": "symbol", "lookup": "FUT", "asset": self.asset}]},
        )
        self.split = dict(symbol="FUT", action_type="forward_split", old_rate=1, new_rate=2, ex_date=DATES[2])
        self.raw = []
        for i, d in enumerate(DATES):
            traded = i != 2
            price = ["100.02", "99.98", "99.98", "100.01", "99.99"][i]
            self.raw.append(
                dict(
                    session=d,
                    tickers=["FUT"],
                    timestamp=d + "T04:00:00Z",
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    vwap=price if traded else "0",
                    volume="200" if traded else "0",
                    trade_count="2" if traded else "0",
                )
            )
        self.pair = [{**self.raw[1], "close": "49.99"}, self.raw[2]]

    def candidate(self):
        observed = product_quote.assets(self.receipt, self.pid, "FUT")
        return product_quote.candidate(observed, [self.split], "FUT", DATES[2])

    def test_native_membership_and_active_trades_are_required(self):
        factor = self.candidate()
        self.assertEqual(factor, Decimal("0.5"))
        self.receipt["identity_memberships"][0]["member_alpaca_asset_ids"] = ["another"]
        with self.assertRaises(UnsupportedInterval):
            self.candidate()
        self.receipt["identity_memberships"][0]["member_alpaca_asset_ids"] = ["uid"]
        for name in ("Ordinary $100 Preferred Shares", "NAV Index ETF", "Fund Holding NextShares Common Stock"):
            with self.subTest(name=name), self.assertRaises(UnsupportedInterval):
                self.asset["name"] = name
                self.candidate()
        for failure in ("missing_trades", "ordinary_prices", "different_split", "mixed_labels"):
            raw, pair = deepcopy(self.raw), deepcopy(self.pair)
            if failure == "missing_trades":
                raw[0]["volume"] = "0"
            elif failure == "ordinary_prices":
                raw[-1]["close"] = "49.99"
            elif failure == "different_split":
                pair[0]["close"] = "99.98"
            else:
                raw[-1]["tickers"] = ["NEXT"]
            with self.subTest(failure=failure), self.assertRaises(UnsupportedInterval):
                product_quote.plan(raw, pair, "FUT", DATES[2], factor)

    def test_literal_tape_resumes_without_rechecking_prior_witnesses(self):
        selected = product_quote.plan(self.raw, self.pair, "FUT", DATES[2], self.candidate())
        checkpoint = {}
        calls = []

        def put(key, val):
            checkpoint[key] = val
            return val

        def observe(client, symbol, asof, mode, day, last):
            self.assertEqual((symbol, asof, mode, day), ("FUT", "-", "raw", last))
            self.assertNotIn(day, calls)
            calls.append(day)
            if day == DATES[3]:
                raise OSError("transient")
            return {day: next(r for r in self.raw if r["session"] == day)}

        with patch.object(product_quote, "provider_rows", side_effect=observe), self.assertRaises(OSError):
            product_quote.confirm(selected, Mock(), checkpoint.get, put)
        self.assertEqual(set(checkpoint), {"literal_product:" + d for d in DATES[:2]})
        remaining = [r for r in self.raw if r["session"] >= DATES[3]]
        with patch.object(
            product_quote, "provider_rows", side_effect=[{r["session"]: r} for r in remaining]
        ) as provider:
            actual = product_quote.confirm(selected, Mock(), checkpoint.get, put)
            self.assertEqual(provider.call_count, 2)
        self.assertEqual(actual["reason"], product_quote.PROXY)
        with patch.object(product_quote, "provider_rows", side_effect=AssertionError("reopened accepted tape")):
            self.assertEqual(product_quote.confirm(selected, None, checkpoint.get, put), actual)
        checkpoint["literal_product:" + DATES[-1]] = False
        self.assertEqual(product_quote.confirm(selected, None, checkpoint.get, put)["outcome"], "unsupported")

    def test_replaced_dollar_tape_must_demonstrate_the_split_across_eligible_trades(self):
        raw = deepcopy(self.raw)
        for i, row in enumerate(raw):
            price = ("22.02", "21.98", "21.98", "11.02", "11.04")[i]
            for field in ("open", "high", "low", "close"):
                row[field] = price
            if row["volume"] != "0":
                row["vwap"] = price
        # The zero-trade boundary carries its old close, while the next actual
        # trade does realize the native 2-for-1 terms.
        pair = [{**raw[1], "close": "10.99"}, raw[2]]
        selected = product_quote.plan(raw, pair, "FUT", DATES[2], self.candidate(), recheck=True)
        rows = {r["session"]: r for r in raw}
        with patch.object(product_quote, "provider_rows", side_effect=lambda c, s, a, m, lo, hi: {lo: rows[lo]}):
            result = product_quote.confirm(selected, None, lambda k: None, lambda k, v: v)
        self.assertEqual(result["outcome"], "restored")
        raw[-1]["close"] = raw[-1]["high"] = "100"
        with self.assertRaises(UnsupportedInterval):
            product_quote.plan(raw, pair, "FUT", DATES[2], self.candidate(), recheck=True)
        with self.assertRaises(UnsupportedInterval):
            product_quote.plan(
                self.raw, self.pair, "FUT", DATES[2], self.candidate(), recheck=True, named_proxy=False
            )

    def test_price_revisions_anywhere_and_relevant_asset_changes_invalidate(self):
        key = self.pid + ":" + DATES[2]
        item = dict(
            reason=product_quote.PROXY,
            witness_start=DATES[0],
            witness_end=DATES[-1],
            cut=DATES[2],
            public_id=self.pid,
            symbol="FUT",
            members=["FUT"],
            unavailable=[],
        )
        old = {**self.receipt, "source_sha256": "x", "boundaries": {}, price_basis_limits.FIELD: {key: item}}
        new = {**self.receipt, "input_sources": {"FUT": "x"}, "source_sha256": "x", "boundaries": {}}
        price_basis_limits.retain(old, new)
        self.assertEqual(new[price_basis_limits.FIELD][key], item)
        new = deepcopy(new)
        new["evidence"]["asset_lookups"][0]["asset"]["name"] = "Future Equity USD"
        price_basis_limits.retain(old, new)
        self.assertTrue(new[price_basis_limits.FIELD][key]["pending"])
        new = {}
        price_basis_limits.continue_closed(old, new, "2026-11-02")
        self.assertTrue(new[price_basis_limits.FIELD][key]["pending"])

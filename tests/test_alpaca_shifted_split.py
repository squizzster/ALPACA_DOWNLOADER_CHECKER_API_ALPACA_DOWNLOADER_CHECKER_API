"""A dated pricing producer can resolve a split applied one traded session early."""

import unittest
from copy import deepcopy
from decimal import Decimal
from unittest.mock import patch

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.shifted_split import assess, session_window
from libs.common.us_equity_market_session_calendar import MarketCalendarError

DAYS = ("2026-03-17", "2026-03-18", "2026-03-19")
PRICES = ("open", "high", "low", "close", "vwap")


def bar(symbol, day, prices, volume):
    return {
        "session": day,
        "timestamp": day + "T04:00:00Z",
        "tickers": [symbol],
        "trade_count": "77",
        "volume": volume,
        **dict(zip(PRICES, prices.split(), strict=True)),
    }


def fixture(symbol="EXAMPLE", days=DAYS):
    raw = [
        bar(symbol, days[0], "9.655 10.0814 9.475 9.63 9.726383", "1001"),
        bar(symbol, days[1], "9.5 10 9.047 9.05 9.613958", "101"),
        bar(symbol, days[2], "25.75 27.41 24.35 26.41 26", "77"),
    ]
    split = [bar(symbol, days[0], "28.97 30.24 28.43 28.89 29.18", "333"), *raw[1:]]
    views = {
        mode: {"bars": deepcopy(split if mode == "all" or "split" in mode else raw)} for mode in ADJUSTMENTS
    }
    action = dict(
        symbol=symbol, action_type="reverse_split", ex_date=days[1], process_date=days[2], old_rate=3, new_rate=1
    )
    return views, [action]


def forward_with_cash(days=DAYS):
    symbol = "FORWARD"
    raw = [
        bar(symbol, day, " ".join([p] * 5), "111") for day, p in zip(days, ("100", "101", "20.20"), strict=True)
    ]
    views = {}
    for mode in ADJUSTMENTS:
        cash, splits = mode == "all" or "dividend" in mode, mode == "all" or "split" in mode
        bars = deepcopy(raw)
        for i, item in enumerate(bars):
            scale = (Decimal("0.96") if cash else Decimal(1)) / (5 if splits and i == 0 else 1)
            for field in PRICES:
                item[field] = str((Decimal(item[field]) * scale).quantize(Decimal("0.01")))
            if splits and i == 0:
                item["volume"] = "555"
        views[mode] = {"bars": bars}
    return views, [
        dict(
            symbol=symbol,
            action_type="forward_split",
            ex_date=days[1],
            payable_date=days[2],
            old_rate=1,
            new_rate=5,
        )
    ]


class ShiftedSplitTests(unittest.TestCase):
    @staticmethod
    def carried_fixture():
        views, actions = forward_with_cash()
        for mode, payload in views.items():
            current = payload["bars"][1]
            cash = mode == "all" or "dividend" in mode
            current.update(dict.fromkeys(PRICES[:-1], "96" if cash else "100"))
            current.update(volume="0", trade_count="0", vwap="0")
            for row in payload["bars"]:
                row["tickers"] = ["LATER.U"]
        actions[0]["symbol"] = "LATER.U"
        return views, actions

    def test_single_untraded_session_carries_each_views_own_preceding_close(self):
        views, actions = self.carried_fixture()
        bundles, descriptor = assess(views, actions, DAYS[1])
        self.assertEqual(descriptor["reason"], "carried_close_in_pre_split_units")
        for mode, rows in bundles.items():
            current = rows[DAYS[1]]
            self.assertEqual(current["session"], DAYS[1])
            self.assertEqual(current["tickers"], ["LATER.U"])
            self.assertEqual((current["vwap"], current["trade_count"], current["volume"]), ("0", "0", "0"))
            if mode == "all" or "split" in mode:
                self.assertEqual({current[k] for k in PRICES[:-1]}, {views[mode]["bars"][0]["close"]})
            else:
                self.assertEqual(current, views[mode]["bars"][1])
        self.assertEqual(Decimal(bundles["all"][DAYS[1]]["close"]), Decimal("19.20"))
        self.assertEqual(Decimal(bundles["split"][DAYS[1]]["close"]), Decimal("20"))

    def test_partial_carry_changed_cash_long_halt_and_unconfirmed_next_tape_decline(self):
        for mode, index, values in (
            ("all", 1, {"vwap": "0.01"}),
            ("raw", 1, {"trade_count": "1"}),
            ("split", 1, {"volume": "1"}),
            ("dividend", 1, dict.fromkeys(PRICES[:-1], "94")),
            ("split,spin-off", 1, {"high": "107"}),
            ("raw", 0, {"volume": "0", "trade_count": "0", "vwap": "0"}),
            ("raw", 2, {"volume": "0", "trade_count": "0", "vwap": "0"}),
            ("raw", 2, {"open": "100"}),
        ):
            with self.subTest(mode=mode, index=index, values=values):
                views, actions = self.carried_fixture()
                views[mode]["bars"][index].update(values)
                with self.assertRaises(UnsupportedInterval):
                    assess(views, actions, DAYS[1])

    def test_storage_selection_and_producer_share_one_calendar_window(self):
        dates = ("2026-04-02", "2026-04-06", "2026-04-07")
        views, actions = fixture(days=dates)
        self.assertEqual(session_window(dates[1]), dates)
        with patch(
            "libs.alpaca_pricing_download.shifted_split.surrounding_market_sessions",
            side_effect=AssertionError("reselected completed calendar window"),
        ):
            assess(views, actions, dates[1])
        with self.assertRaisesRegex(UnsupportedInterval, "surrounding market"):
            session_window("2026-04-03")
        with (
            patch(
                "libs.alpaca_pricing_download.shifted_split.surrounding_market_sessions",
                side_effect=MarketCalendarError("calendar unavailable"),
            ),
            self.assertRaises(MarketCalendarError),
        ):
            session_window("2026-04-08")

    def test_repair_one_session_using_only_selected_observations(self):
        for symbol, dates in (("EXAMPLE", DAYS), ("FUTURE", ("2028-03-14", "2028-03-15", "2028-03-16"))):
            views, actions = fixture(symbol, dates)
            all_stamp = dates[1] + "T00:00:00-04:00"
            views["all"]["bars"][1].update(timestamp=all_stamp, trade_count="77.0")
            original = deepcopy(views)
            with (
                patch("gzip.open", side_effect=AssertionError("opened historical bars")),
                patch(
                    "libs.alpaca_pricing_download.http.BarsClient.fetch", side_effect=AssertionError("download")
                ),
            ):
                bundles, descriptor = assess(views, actions, dates[1])
            self.assertEqual(views, original)
            self.assertEqual(set(bundles), set(ADJUSTMENTS))
            self.assertEqual(descriptor["repair_kind"], "split_timing")
            self.assertEqual(descriptor["witnesses"], [dates[0], dates[2]])
            self.assertEqual(descriptor["symbol"], symbol)
            self.assertEqual(descriptor["raw_revisions"], [])
            for mode, bundle in bundles.items():
                self.assertEqual(list(bundle), [dates[1]])
                row = bundle[dates[1]]
                if mode != "all" and "split" not in mode:
                    self.assertEqual(row, original[mode]["bars"][1])
                    continue
                self.assertEqual([row[k] for k in PRICES], ["28.50", "30.00", "27.14", "27.15", "28.84"])
                self.assertEqual(row["volume"], "33")
                self.assertEqual(row["trade_count"], original[mode]["bars"][1]["trade_count"])
                self.assertEqual(row["timestamp"], original[mode]["bars"][1]["timestamp"])
                self.assertEqual(row["tickers"], [symbol])

    def test_multiplicative_later_split_and_three_day_window(self):
        views, actions = fixture()
        observed = (
            ("57.93 60.49 56.85 57.78 58.36", "166"),
            ("19.00 20.00 18.09 18.10 19.23", "50"),
            ("51.50 54.82 48.70 52.82 52.00", "38"),
        )
        for mode in ("split", "split,dividend", "split,spin-off", "all"):
            for row, (prices, volume) in zip(views[mode]["bars"], observed, strict=True):
                row.update(dict(zip(PRICES, prices.split(), strict=True)), volume=volume)
        bundles, _ = assess(views, actions, DAYS[1])
        row = bundles["all"][DAYS[1]]
        self.assertEqual([row[k] for k in PRICES], ["57.00", "60.00", "54.28", "54.30", "57.68"])
        self.assertEqual(row["volume"], "16")

    def test_forward_split_preserves_independent_cash_and_split_views(self):
        views, actions = forward_with_cash()
        bundles, _ = assess(views, actions, DAYS[1])
        for mode in ("all", "split,dividend"):
            self.assertEqual(bundles[mode][DAYS[1]]["close"], "19.39")
            self.assertEqual(bundles[mode][DAYS[1]]["volume"], "555")
        self.assertEqual(bundles["dividend"][DAYS[1]]["close"], "96.96")
        self.assertEqual(bundles["split"][DAYS[1]]["close"], "20.20")

    def test_declines_when_action_terms_or_traded_tape_do_not_identify_a_shift(self):
        for change in (
            {"old_rate": 1},
            {"new_rate": "nan"},
            {"action_type": "forward_split"},
            {"ex_date": DAYS[0]},
            {"effective_date": DAYS[0]},
            {"symbol": "DIFFERENT"},
        ):
            with self.subTest(change=change):
                views, actions = fixture()
                actions[0].update(change)
                with self.assertRaises(UnsupportedInterval):
                    assess(views, actions, DAYS[1])
        for index, values in (
            (0, {"volume": "0"}),
            (1, {"trade_count": "0"}),
            (1, {"open": "27", "close": "27"}),
            (2, {"open": "9.1"}),
            (2, {"open": "53"}),
            (0, {"tickers": ["OTHER"]}),
        ):
            with self.subTest(index=index, values=values):
                views, actions = fixture()
                views["raw"]["bars"][index].update(values)
                with self.assertRaises(UnsupportedInterval):
                    assess(views, actions, DAYS[1])

    def test_adjacent_action_scope_and_duplicate_provider_observations(self):
        dates = ("2026-03-19", "2026-03-20", "2026-03-23")
        views, actions = fixture(days=dates)
        # A split dated at Friday's close changes traded units on Monday.
        # An additional provider date is unnecessary if the tape resolves it.
        actions[0]["process_date"] = actions[0]["ex_date"]
        other = dict(symbol="OTHER", action_type="cash_dividend", ex_date=dates[1])
        assess(views, [*actions, dict(actions[0]), other], dates[1])
        with self.assertRaisesRegex(UnsupportedInterval, "one relevant"):
            assess(views, [*actions, {**other, "symbol": "EXAMPLE"}], dates[1])

    def test_rejects_nonadjacent_or_inconsistent_views_and_trade_statistics(self):
        days = ("2026-03-16", DAYS[1], DAYS[2])
        with self.assertRaisesRegex(UnsupportedInterval, "dated scope"):
            assess(*fixture(days=days), DAYS[1])
        for mode, index, values in (
            ("split", 0, {"close": "29"}),
            ("split", 0, {"volume": "334"}),
            ("all", 2, {"trade_count": "75"}),
            ("split,spin-off", 0, {"tickers": ["OTHER"]}),
            ("dividend", 1, {"session": DAYS[0]}),
            ("spin-off", 0, {"volume": "1003"}),
        ):
            with self.subTest(mode=mode, index=index, values=values):
                views, actions = fixture()
                views[mode]["bars"][index].update(values)
                with self.assertRaises(UnsupportedInterval):
                    assess(views, actions, DAYS[1])
        views, actions = fixture()
        del views["dividend"]
        with self.assertRaisesRegex(UnsupportedInterval, "dated scope"):
            assess(views, actions, DAYS[1])

    def test_incompatible_cash_changes_decline(self):
        views, actions = forward_with_cash()
        views["all"]["bars"][0]["open"] = "18.90"
        with self.assertRaisesRegex(UnsupportedInterval, "compatible basis"):
            assess(views, actions, DAYS[1])
        views, actions = forward_with_cash()
        views["all"]["bars"][0]["open"] = "19.2003"
        with self.assertRaisesRegex(UnsupportedInterval, "precision"):
            assess(views, actions, DAYS[1])

    def test_unresolved_subcent_cash_factor_cannot_supply_a_corrected_price(self):
        views, actions = fixture()
        for mode, payload in views.items():
            split = mode == "all" or "split" in mode
            cash = mode == "all" or "dividend" in mode
            first = ("24.30" if cash else "27.00") if split else ("8.100" if cash else "9")
            prices = (first, "8.105" if cash else "9.006", "24.30" if cash else "27")
            for row, price in zip(payload["bars"], prices, strict=True):
                row.update(dict.fromkeys(PRICES, price))
        with self.assertRaisesRegex(UnsupportedInterval, "competing corrected prices"):
            assess(views, actions, DAYS[1])


if __name__ == "__main__":
    unittest.main()

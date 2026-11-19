"""An independently dated share conversion can locate an early split adjustment."""

import unittest
from copy import deepcopy
from decimal import Decimal
from unittest.mock import Mock

from libs.alpaca_lineage_build.conversion_timing import inputs
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.conversion_timing import assess, plan
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.rename_split_basis import scaled
from libs.alpaca_pricing_download.shifted_split import _PAIRS

DAYS = ("2025-05-16", "2025-05-19", "2025-05-20", "2025-05-21")


class ConversionTests(unittest.TestCase):
    def setUp(self):
        self.split = dict(
            id="s",
            action_type="reverse_split",
            ex_date=DAYS[1],
            symbol="FUT",
            old_cusip="123456789",
            new_cusip="123456789",
            old_rate=20,
            new_rate=1,
        )
        self.conversion = dict(
            id="c",
            action_type="stock_merger",
            effective_date=DAYS[-1],
            acquiree_symbol="FUT",
            acquirer_symbol="FUT",
            acquiree_cusip="ABCDEF123",
            acquirer_cusip="123456789",
            acquiree_rate=1,
            acquirer_rate=0.05,
        )
        self.actions = [self.split, self.conversion]
        self.ranges = [dict(first_session=DAYS[0], last_session=DAYS[-1], ticker="FUT")]
        self.views = {mode: {"bars": []} for mode in ADJUSTMENTS}
        for i, (day, price) in enumerate(zip(DAYS, ["0.15", "0.16", "0.17", "3.20"], strict=True)):
            raw = dict(
                session=day,
                tickers=["FUT"],
                timestamp=day + "T04:00:00Z",
                open=price,
                high=price,
                low=price,
                close=price,
                vwap=price,
                trade_count="550",
                volume="128502",
            )
            if i == 3:
                raw.update(open="1.2", low="1.2", high="3.5", vwap="3.3")
            adjusted = scaled(raw, Decimal(400 if not i else 20))
            for mode in self.views:
                self.views[mode]["bars"].append(deepcopy(adjusted if mode in _PAIRS else raw))

    def planned(self):
        return plan(self.actions, "FUT", DAYS[0], DAYS[-1], DAYS[1])

    def test_later_conversion_can_have_a_real_down_open(self):
        snapshot = Mock()
        snapshot._selected_receipts = {"FUT": {"evidence": {"corporate_actions": self.actions}}}
        snapshot.bounded_acquisition_views.return_value = self.views
        selected = inputs(snapshot, Mock(public_id="id"), DAYS[1], self.ranges)
        snapshot.bounded_acquisition_views.assert_called_once_with("id", DAYS[0], DAYS[-1])
        result, proof = assess(selected)
        self.assertEqual(proof["witnesses"], [DAYS[0], DAYS[-1]])
        for mode, bars in result.items():
            self.assertEqual(list(bars), list(DAYS[1:3]))
            self.assertEqual(Decimal(bars[DAYS[1]]["close"]), Decimal("64" if mode in _PAIRS else ".16"))
            self.assertEqual(bars[DAYS[2]]["trade_count"], "550")
            self.assertEqual(bars[DAYS[1]]["volume"], "321" if mode in _PAIRS else "128502")

    def test_competing_currency_or_market_return_cannot_prove_a_unit_change(self):
        info = self.planned()
        for mode, index, field, value in (
            ("split,dividend", 1, "high", "8"),
            ("raw", 2, "open", "3.2"),
            ("raw", -1, "close", "0.16"),
            ("raw", -1, "vwap", "0.16"),
            ("split", 2, "volume", "500"),
            ("raw", 1, "trade_count", "0"),
        ):
            with self.subTest(mode=mode, index=index, field=field):
                views = deepcopy(self.views)
                views[mode]["bars"][index][field] = value
                with self.assertRaises(UnsupportedInterval):
                    assess({"plan": info, "views": views})

    def test_competing_actions_and_identities_decline_before_reading_prices(self):
        for field, value in (
            ("acquirer_symbol", "NEXT"),
            ("acquiree_cusip", "123456789"),
            ("acquirer_cusip", "456789123"),
            ("acquirer_rate", 0.1),
        ):
            with self.subTest(field=field):
                alternate = {**self.conversion, field: value}
                with self.assertRaises(UnsupportedInterval):
                    plan([self.split, alternate], "FUT", DAYS[0], DAYS[-1], DAYS[1])
        for day in DAYS[:3]:
            with self.subTest(day=day), self.assertRaises(UnsupportedInterval):
                plan(
                    [*self.actions, dict(symbol="FUT", action_type="cash_dividend", ex_date=day)],
                    "FUT",
                    DAYS[0],
                    DAYS[-1],
                    DAYS[1],
                )
        snapshot = Mock()
        snapshot._selected_receipts = {"FUT": {"evidence": {"corporate_actions": [self.split]}}}
        with self.assertRaises(UnsupportedInterval):
            inputs(snapshot, Mock(), DAYS[1], self.ranges)
        snapshot.bounded_acquisition_views.assert_not_called()

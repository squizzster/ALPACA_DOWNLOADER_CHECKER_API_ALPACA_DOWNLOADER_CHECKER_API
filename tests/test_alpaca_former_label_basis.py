"""A dated original ticker can arbitrate only an evidenced foreign share factor."""

from copy import deepcopy
from datetime import timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import Mock

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval, _window
from libs.alpaca_pricing_download.former_label_basis import assess
from libs.alpaca_pricing_download.rename_split_basis import scaled

DAYS = [
    "2031-03-03",
    "2031-03-04",
    "2031-03-05",
    "2031-03-06",
    "2031-03-07",
    "2031-03-10",
    "2031-03-11",
    "2031-03-12",
    "2031-03-13",
]
CUT = DAYS[6]


def observations(old, new, *, share_change=False):
    def bar(d, t, price):
        return {
            "session": d,
            "timestamp": _window(d).astimezone(timezone.utc).isoformat(),
            "tickers": [t],
            "volume": "1200",
            "trade_count": "50",
            **dict.fromkeys(("open", "high", "low", "close", "vwap"), price),
        }

    source, views = {}, {}
    actions = [
        dict(action_type="reverse_split", symbol=new, ex_date=DAYS[2], old_rate=4, new_rate=1),
        dict(
            action_type="name_change",
            old_symbol=old,
            new_symbol=new,
            process_date=CUT,
            old_cusip="100",
            new_cusip="100",
        ),
    ]
    if share_change:
        actions.append(
            dict(action_type="reverse_split", symbol=new, ex_date=CUT, old_rate=10, new_rate=1, new_cusip="100")
        )

    for mode in ADJUSTMENTS:
        adjust = mode == "all" or "split" in mode
        rows = [bar(d, old if d < CUT else new, "100" if d >= CUT and share_change else "10") for d in DAYS]
        source[mode] = {b["session"]: deepcopy(b) for b in rows}
        for i, d in enumerate(DAYS):
            factor = (4 if adjust and d < DAYS[2] else 1) * (10 if share_change and adjust and d < CUT else 1)
            rows[i] = scaled(rows[i], Decimal(factor))
        views[mode] = {"bars": rows}
    return dict(
        views=views,
        ranges=[
            dict(ticker=old, first_session=DAYS[0], last_session=DAYS[5]),
            dict(ticker=new, first_session=CUT, last_session=DAYS[-1]),
        ],
        actions=actions,
    ), source


class FormerLabelTests(TestCase):
    def test_dated_owner_and_optional_actual_share_change_preserved(self):
        for old, new, converted in (("EARLY", "LATER", False), ("UNIT.A", "FUTURE", True)):
            with self.subTest(old=old):
                inputs, source = observations(old, new, share_change=converted)

                def select(symbol, asof, mode, first, last):
                    self.assertEqual((symbol, asof, first, last), (old, DAYS[5], DAYS[0], DAYS[-1]))
                    return source[mode]

                state = {}
                bars, desc = assess(inputs, CUT, select, completed=state, checkpoint=lambda k, v: None)
                self.assertEqual(desc["foreign_split_dates"], [DAYS[2]])
                self.assertEqual(desc["raw_revisions"], [])
                self.assertEqual(float(bars["all"][DAYS[0]]["close"]), 100 if converted else 10)
                self.assertEqual(bars["raw"][DAYS[0]]["tickers"], [old])
                self.assertEqual(
                    assess(
                        {},
                        CUT,
                        Mock(side_effect=AssertionError("provider reopened")),
                        completed=state,
                        checkpoint=Mock(side_effect=AssertionError("recomputed")),
                    ),
                    (bars, desc),
                )

    def test_competing_parent_action_market_move_and_distinct_owner_decline(self):
        for case in ("old action", "opening", "second label", "unexplained scale", "wrong conversion"):
            inputs, _ = observations("EARLY", "LATER", share_change=case == "wrong conversion")
            if case == "old action":
                inputs["actions"].append(
                    dict(action_type="forward_split", symbol="EARLY", ex_date=DAYS[2], old_rate=1, new_rate=3)
                )
            if case == "opening":
                inputs["views"]["raw"]["bars"][2]["open"] = "40"
            if case == "second label":
                inputs["views"]["raw"]["bars"][0]["tickers"] = ["ANOTHER"]
            if case == "unexplained scale":
                inputs["actions"][0]["old_rate"] = 8
            if case == "wrong conversion":
                inputs["actions"][-1]["new_cusip"] = "different"
            with self.subTest(case=case), self.assertRaises(UnsupportedInterval):
                assess(
                    inputs,
                    CUT,
                    Mock(side_effect=AssertionError("fetched outside accepted scope")),
                    completed={},
                    checkpoint=lambda k, v: None,
                )

"""A bounded carry preserves the last traded basis without manufacturing trades."""

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_shifted_split import PRICES, forward_with_cash

from libs.alpaca_lineage_build import dated_price_publication as lineage
from libs.alpaca_lineage_build.split_carry import endpoints
from libs.alpaca_pricing_download import dated_publication as pricing
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.split_carry import assess

DATES = ("2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20", "2026-03-23")


def fixture(symbol="FORWARD", dates=DATES):
    first, day, last = dates[0], dates[2], dates[-1]
    views, actions = forward_with_cash((first, day, last))
    actions[0].update(symbol=symbol, old_cusip="123456789", new_cusip="223456789")
    lookups = [("123456789", symbol, "old-issue"), ("223456789", symbol, "new-issue")]
    for mode, payload in views.items():
        before, _, after = payload["bars"]
        before["tickers"] = after["tickers"] = [symbol]
        carried = []
        for session in dates[1:-1]:
            value = before["close"] if session < day else "96" if mode == "all" or "dividend" in mode else "100"
            carried.append(
                {
                    **before,
                    "session": session,
                    "timestamp": session + "T04:00:00Z",
                    **dict.fromkeys(PRICES[:-1], value),
                    "volume": "0",
                    "vwap": "0",
                    "trade_count": "0",
                }
            )
        payload["bars"] = [before, *carried, after]
    return dict(
        views=views, actions=actions, endpoint_observations=lookups, flagged_session=day, first=first, last=last
    )


class SplitCarryTests(TestCase):
    def test_future_matching_security_uses_only_its_scoped_views_and_changes_no_raw_observations(self):
        dates = ("2028-04-03", "2028-04-04", "2028-04-05", "2028-04-06", "2028-04-07", "2028-04-10")
        args = fixture("FUTURE", dates)
        original = deepcopy(args["views"])
        with (
            patch("gzip.open", side_effect=AssertionError("retained history reverified")),
            patch("libs.alpaca_pricing_download.http.BarsClient.fetch", side_effect=AssertionError("download")),
        ):
            bundles, result = assess(**args)
        self.assertEqual(args["views"], original)
        self.assertEqual((result["repair_kind"], result["sessions"]), ("split_carry", 3))
        self.assertEqual(result["witnesses"], [dates[0], dates[-1]])
        for mode, rows in bundles.items():
            self.assertEqual(list(rows), list(dates[2:-1]))
            for idx, (day, row) in enumerate(rows.items(), 2):
                self.assertEqual(row["session"], day)
                self.assertEqual(row["tickers"], ["FUTURE"])
                self.assertEqual((row["vwap"], row["volume"], row["trade_count"]), ("0", "0", "0"))
                if mode == "all" or "split" in mode:
                    self.assertEqual({row[k] for k in PRICES[:-1]}, {original[mode]["bars"][0]["close"]})
                else:
                    self.assertEqual(row, original[mode]["bars"][idx])

    def test_sparse_control_missing_trade_and_mismatched_cusip_or_session_decline(self):
        for key, change in (
            ("views", lambda v: v["raw"]["bars"][-1].update(close="93", vwap="98")),
            ("views", lambda v: v["raw"]["bars"][-1].update(vwap="67")),
            ("views", lambda v: v["raw"]["bars"][-1].update(volume="0", trade_count="0")),
            ("views", lambda v: v["raw"]["bars"][1].update(tickers=["PREVOCCUPANT"])),
            ("views", lambda v: v["raw"]["bars"][3].update(volume="1", trade_count="1", vwap="100")),
            ("views", lambda v: v["split"]["bars"].pop(1)),
            ("views", lambda v: v["dividend"]["bars"][3].update(dict.fromkeys(PRICES[:-1], "98"))),
            ("views", lambda v: v["all"]["bars"][2].update(dict.fromkeys(PRICES[:-1], "55"))),
            ("actions", lambda a: a[0].update(new_cusip="")),
            ("actions", lambda a: a[0].update(new_symbol="REPLACEMENT")),
            (
                "actions",
                lambda a: a.append(
                    dict(action_type="unit_split", old_symbol="FORWARD", effective_date=DATES[1])
                ),
            ),
            ("endpoint_observations", lambda a: a.pop()),
            ("endpoint_observations", lambda a: a.append(("123456789", "OTHER", "issuer"))),
        ):
            with self.subTest(key=key, change=change):
                args = fixture()
                change(args[key])
                with self.assertRaises(UnsupportedInterval):
                    assess(**args)
        for key in ("first", "last"):
            args = fixture()
            args[key] = DATES[2]
            with self.assertRaises(UnsupportedInterval):
                assess(**args)

    def test_input_and_completed_binding_survive_interruption_and_changed_bounds_select_new_work(self):
        args = fixture()
        pid = "a" * 64
        binding = dict(
            public_id=pid,
            cut=args["flagged_session"],
            repair_kind="split_carry",
            carry_window=(DATES[0], DATES[-1]),
            decision_generation="g",
        )
        supplied = Mock(
            return_value=dict(
                views=args["views"], endpoints=args["endpoint_observations"], actions=args["actions"]
            )
        )
        with TemporaryDirectory() as path:
            store = Path(path)
            (store / "end_of_day/pricing").mkdir(parents=True)
            with (
                patch("uuid.uuid4", side_effect=OSError("stopped after completed numerical proof")),
                self.assertRaises(OSError),
            ):
                pricing.acquire_selected(store, binding, supplied, None)
            forbidden = Mock(side_effect=AssertionError("reopened selected artifacts"))
            with patch(
                "libs.alpaca_pricing_download.split_carry.assess", side_effect=AssertionError("repeated proof")
            ):
                result = pricing.acquire_selected(store, binding, forbidden, None)
                self.assertEqual(result, pricing.acquire_selected(store, binding, forbidden, None))
            supplied.assert_called_once_with()
            changed = {**binding, "carry_window": (DATES[1], DATES[-1])}
            with self.assertRaises(UnsupportedInterval):
                pricing.acquire_selected(store, changed, supplied, None)
            with self.assertRaises(UnsupportedInterval):
                pricing.acquire_selected(store, changed, forbidden, None)

    def test_lineage_reuses_accepted_metadata_and_invalidates_a_changed_endpoint(self):
        args = fixture()
        pid = "1" * 64
        code, symbol, uid = args["endpoint_observations"][0]
        look = dict(lookup_kind="cusip", lookup=code, asset={"id": uid, "symbol": symbol})
        interval = dict(
            repair_kind="split_carry",
            symbol=symbol,
            first=DATES[2],
            cut=DATES[-1],
            endpoint_cusips=[item[0] for item in args["endpoint_observations"]],
            witness_start=DATES[0],
            witness_end=DATES[-1],
            members=[symbol],
            unavailable=[],
        )
        old = dict(
            source_sha256="g",
            boundaries={},
            input_sources={symbol: "g"},
            evidence=dict(corporate_actions=args["actions"], asset_lookups=[look]),
        )
        old[lineage.FIELD] = {pid: interval}
        same = dict(old)
        with patch(
            "libs.alpaca_lineage_build.split_carry.endpoints",
            side_effect=AssertionError("checked endpoints again"),
        ):
            lineage.retain(old, same)
        for changed in (False, True):
            new = deepcopy(old)
            new["evidence"]["asset_lookups"][0]["asset"].update(status="inactive")
            if changed:
                new["evidence"]["asset_lookups"][0]["asset"]["id"] = "reassigned"
            lineage.retain(old, new)
            self.assertEqual(lineage.FIELD in new, not changed)
        with self.assertRaises(ValueError):
            endpoints({"asset_lookups": [{**look, "research_certificates": ["untrusted"]}]}, {code})

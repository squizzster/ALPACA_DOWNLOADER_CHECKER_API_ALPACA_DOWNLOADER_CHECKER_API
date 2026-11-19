"""A repeated share event must change the traded share units exactly once."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_shifted_split import PRICES, bar

from libs.alpaca_lineage_build import dated_price_publication as lineage
from libs.alpaca_lineage_build import duplicate_split as selection
from libs.alpaca_pricing_download import dated_publication as pricing
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.duplicate_split import _latent_vwap, assess, plan

DATES = ("2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20", "2026-03-23")


def fixture(*, first_real=False, cusips=True, cash=True, symbol="EXAMPLE", dates=DATES):
    actions = [
        dict(
            id="leg-" + str(i),
            action_type="reverse_split",
            old_rate=4,
            new_rate=1,
            symbol=symbol,
            old_cusip="123456789" if cusips else "",
            new_cusip="223456789" if cusips else "",
            ex_date=dates[i],
        )
        for i in (1, 2)
    ]
    if cash:
        actions.append(dict(id="cash", action_type="cash_dividend", symbol=symbol, ex_date=dates[5], rate="0.8"))
    info = plan(actions, symbol, dates[0], dates[-1], dates[2])
    prices = ("2", "8" if first_real else "2", "8", "8")
    views = {}
    for mode in ADJUSTMENTS:
        rows = []
        for i, (native_share, p) in enumerate(zip((16, 4, 1, 1), prices, strict=True)):
            applies = mode == "all" or "split" in mode
            split = Decimal(native_share if applies else 1)
            adjusted = (
                split * Decimal(p) * (Decimal("0.90") if cash and (mode == "all" or "dividend" in mode) else 1)
            )
            rows.append(bar(symbol, dates[i], " ".join([str(adjusted)] * 5), str(int(Decimal(101) / split))))
        views[mode] = {"bars": rows}
    return actions, dict(plan=info, views=views, later_closes={dates[4]: "8"} if cash else {})


class DuplicateSplitTests(TestCase):
    def test_tape_selects_earlier_or_later_duplicate_and_retains_nonsplit_fields(self):
        for first_real, cusips in ((False, True), (True, False)):
            actions, inputs = fixture(first_real=first_real, cusips=cusips)
            original = deepcopy(inputs)
            with (
                patch("gzip.open", side_effect=AssertionError("retained history reread")),
                patch(
                    "libs.alpaca_pricing_download.http.BarsClient.fetch",
                    side_effect=AssertionError("unscheduled download"),
                ),
            ):
                bundles, result = assess(inputs)
            self.assertEqual(inputs, original)
            self.assertEqual(result["cut"], DATES[2 if first_real else 1])
            self.assertEqual(result["sessions"], 2 if first_real else 1)
            self.assertEqual(Decimal(bundles["split"][DATES[0]]["close"]), 8)
            self.assertEqual(Decimal(bundles["all"][DATES[0]]["close"]), Decimal("7.2"))
            self.assertEqual(bundles["all"][DATES[0]]["volume"], "25")
            self.assertEqual(bundles["all"][DATES[0]]["trade_count"], "77")
            self.assertEqual(bundles["dividend"][DATES[0]], original["views"]["dividend"]["bars"][0])
            self.assertEqual(bundles["raw"][DATES[0]], original["views"]["raw"]["bars"][0])
            if first_real:
                self.assertEqual(Decimal(bundles["all"][DATES[1]]["close"]), Decimal("7.2"))
        future = ("2028-04-03", "2028-04-04", "2028-04-05", "2028-04-06", "2028-04-07", "2028-04-10")
        _, inputs = fixture(symbol="OTHER", dates=future)
        self.assertEqual(assess(inputs)[1]["symbol"], "OTHER")

    def test_two_genuine_splits_missing_records_and_inconsistent_eight_view_prices_decline(self):
        actions, inputs = fixture()
        for key, value in (("old_cusip", "333333333"), ("new_symbol", "OTHER"), ("new_rate", 2)):
            scoped = deepcopy(actions)
            scoped[1][key] = value
            with self.subTest(key=key), self.assertRaises(UnsupportedInterval):
                plan(scoped, "EXAMPLE", DATES[0], DATES[-1], DATES[2])
        for update in (
            lambda v: v["raw"]["bars"][2].update({k: "32" for k in PRICES}),
            lambda v: v["raw"]["bars"][1].update({k: "8" for k in PRICES}),
            lambda v: v["all"]["bars"][0].update(vwap="28.71"),
            lambda v: v["split"]["bars"][0].update(volume="7"),
            lambda v: v["spin-off"]["bars"][0].update(close="1.95"),
            lambda v: v["raw"]["bars"][2].update(volume="0"),
            lambda v: v["raw"]["bars"][1].update(tickers=["REUSED"]),
        ):
            variant = deepcopy(inputs)
            update(variant["views"])
            with self.subTest(update=update), self.assertRaises(UnsupportedInterval):
                assess(variant)
        for supplied in ({}, {DATES[4]: "7"}):
            with self.assertRaises(UnsupportedInterval):
                assess({**inputs, "later_closes": supplied})
        with self.assertRaisesRegex(UnsupportedInterval, "additional action"):
            plan(
                [*actions, dict(action_type="stock_merger", effective_date=DATES[1])],
                "EXAMPLE",
                DATES[0],
                DATES[-1],
                DATES[2],
            )
        with self.assertRaises(UnsupportedInterval):
            plan(actions[:1], "EXAMPLE", DATES[0], DATES[-1], DATES[1])

    def test_reported_precisions_constrain_latent_vwap_without_tolerance_overrides(self):
        self.assertEqual(
            _latent_vwap(Decimal("1.582490"), Decimal("791.24"), Decimal(500), Decimal(50)), "79.12"
        )
        with self.assertRaises(UnsupportedInterval):
            _latent_vwap(Decimal("1.5800"), Decimal("791.24"), Decimal(500), Decimal(50))

    def test_selector_reads_prefix_once_and_only_raw_dividend_anchor_after_it(self):
        actions, wanted = fixture()
        seen = []

        def read(pid, first, last, *, modes=ADJUSTMENTS, cache=None):
            self.assertEqual(pid, "public")
            seen.append((first, last, modes, cache))
            if (first, last) == (DATES[0], DATES[3]):
                self.assertEqual(modes, ADJUSTMENTS)
                return wanted["views"]
            self.assertEqual((first, last, modes), (DATES[4], DATES[4], ("raw",)))
            return {"raw": {"bars": [dict(session=DATES[4], tickers=["EXAMPLE"], close="8")]}}

        group = SimpleNamespace(
            _selected_receipts={"EXAMPLE": {"evidence": {"corporate_actions": actions}}},
            bounded_acquisition_views=read,
        )
        security = SimpleNamespace(public_id="public")
        ranges = [dict(ticker="EXAMPLE", first_session=DATES[0], last_session=DATES[-1])]
        self.assertEqual(selection.inputs(group, security, DATES[2], ranges), wanted)
        self.assertEqual(len(seen), 2)
        self.assertIs(seen[0][3], seen[1][3])
        group._selected_receipts["EXAMPLE"]["evidence"]["event_supplements"] = [{"issuer": "authored"}]
        with self.assertRaises(ValueError):
            selection.inputs(group, security, DATES[2], ranges)
        self.assertEqual(len(seen), 2)

    def test_completed_proof_is_not_repeated_after_interruption(self):
        _, inputs = fixture()
        binding = dict(public_id="1" * 64, cut=DATES[2], decision_generation="g", repair_kind="duplicate_split")
        with TemporaryDirectory() as folder:
            store = Path(folder)
            (store / "end_of_day/pricing").mkdir(parents=True)
            with patch("uuid.uuid4", side_effect=OSError("stopped")), self.assertRaises(OSError):
                pricing.acquire_selected(store, binding, lambda: inputs, None)
            forbidden = Mock(side_effect=AssertionError("reopened inputs"))
            with patch(
                "libs.alpaca_pricing_download.duplicate_split.assess", side_effect=AssertionError("recomputed")
            ):
                result = pricing.acquire_selected(store, binding, forbidden, None)
                self.assertEqual(result, pricing.acquire_selected(store, binding, forbidden, None))
            with self.assertRaisesRegex(AssertionError, "reopened"):
                pricing.acquire_selected(store, {**binding, "decision_generation": "changed"}, forbidden, None)

    def test_action_scope_preserves_irrelevant_cash_dates_and_invalidates_material_term_changes(self):
        actions, inputs = fixture()
        _, result = assess(inputs)
        pid = "1" * 64
        result.update(public_id=pid, source_decision="g", token="2" * 32)
        group = dict(
            decision_generation="g", references=dict(pricing={"EXAMPLE": "g"}, lineage={"EXAMPLE": "g"})
        )
        scope = lineage.bound_descriptor(group, result)
        old = dict(
            source_sha256="g",
            boundaries={},
            input_sources={"EXAMPLE": "g"},
            evidence={"corporate_actions": actions},
        )
        old[lineage.FIELD] = {pid: scope}
        incoming = deepcopy(old)
        incoming["evidence"]["corporate_actions"].append(
            dict(id="outside", symbol="EXAMPLE", action_type="cash_dividend", ex_date="2030-01-02", rate="1")
        )
        lineage.retain(old, incoming)
        self.assertIn(lineage.FIELD, incoming)
        incoming["evidence"]["corporate_actions"][2]["rate"] = "0.9"
        lineage.retain(old, incoming)
        self.assertNotIn(lineage.FIELD, incoming)

"""A new former ticker label cannot introduce a switch in adjustment basis."""

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation.former_ticker import NAME
from libs.alpaca_lineage_build import observations
from libs.alpaca_lineage_build.observation_carry import ChoiceRun, RetainedChoices
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp

DAYS = ("2024-01-02", "2024-01-03", "2024-01-04")


def price_row(index, *, factor, partial=False, share_scale=1):
    close = Decimal(98 + index)
    prices = (
        close - 1,
        close + (1 if partial else 3),
        close - (1 if partial else 4),
        close,
        close - (Decimal(".25") if partial else Decimal(".5")),
    )
    opening, high, low, closing, vwap = (str(price * factor) for price in prices)
    return (
        DAYS[index] + "T05:00:00Z",
        opening,
        high,
        low,
        closing,
        str((20 if partial else 101) * share_scale),
        "2" if partial else "19",
        vwap,
    )


def inputs_and_reports(agreement, *, different_basis=False, split_scale=1):
    inputs, reports = {}, {}
    for symbol in ("FORMER", "MAPPED"):
        mapped = symbol == "MAPPED"
        indices = (0, 1, 2) if mapped else (1,)
        views, segments = {}, []
        for view in ADJUSTMENTS:
            adjusted = view == "all" or "dividend" in view
            factor = Decimal(".5") if adjusted and (mapped or not different_basis) else Decimal(1)
            share_scale = split_scale if not mapped and (view == "all" or "split" in view) else 1
            views[view] = tuple(
                price_row(
                    i,
                    factor=factor,
                    partial=mapped and i == 1 and agreement == "partial",
                    share_scale=share_scale,
                )
                for i in indices
            )
            for position, i in enumerate(indices, 1):
                segment = {
                    "adjustment": view,
                    "row_start": position,
                    "row_end": position,
                    "historical_symbol": "FORMER" if i == 1 else "OTHER_DATED_LABEL",
                    "security_identity": {"security_id": "owner"},
                }
                if i == 1:
                    segment.update(
                        correlation_method=NAME,
                        ticker_ownership_interval={"end_date_exclusive": DAYS[2]},
                    )
                    if mapped:
                        segment["former_ticker_raw_agreement"] = agreement
                segments.append(segment)
        inputs[symbol] = SimpleNamespace(
            views=views,
            source_sha256=symbol.lower(),
            source={
                "activity_consistent": True,
                "history_start": DAYS[0],
                "latest_request": {"asof": "2024-02-01", "end_exclusive": "2024-02-01"},
            },
        )
        reports[symbol] = {"correlated": {"segments": segments}}
    return inputs, reports


def middle_segments(selected, symbol):
    return [
        segment
        for segment in selected[symbol]["correlated"]["segments"]
        if segment["historical_symbol"] == "FORMER"
    ]


class FormerTickerObservationTests(TestCase):
    def assert_middle_bundle(self, result, winner, *, limit):
        selected, corrections = result
        self.assertEqual(corrections, {})
        for symbol in ("FORMER", "MAPPED"):
            middle = middle_segments(selected, symbol)
            self.assertEqual([segment["adjustment"] for segment in middle], list(ADJUSTMENTS))
            for segment in middle:
                self.assertEqual(segment["observation_tickers"], ["FORMER"])
                self.assertEqual(
                    segment["observation_selection"], "selected" if symbol == winner else "superseded"
                )
                self.assertEqual(observations.FORMER_TICKER_BASIS_LIMIT in segment.get("warnings", ()), limit)

    def test_dated_literal_label_cannot_insert_another_adjustment_vintage(self):
        inputs, reports = inputs_and_reports("equal", different_basis=True)
        equal_bundles = {}
        result = observations.select_observations(inputs, reports, equality_outcomes=equal_bundles)
        self.assert_middle_bundle(result, "MAPPED", limit=True)
        self.assertFalse(equal_bundles)
        for view in ("dividend", "all"):
            closes = [
                Decimal(inputs["MAPPED"].views[view][s["row_start"] - 1][4])
                for s in result[0]["MAPPED"]["correlated"]["segments"]
                if s["adjustment"] == view and s["observation_selection"] == "selected"
            ]
            self.assertEqual(closes, [Decimal("49"), Decimal("49.5"), Decimal("50")])

    def test_fuller_raw_aggregate_selects_one_complete_compatible_bundle(self):
        inputs, reports = inputs_and_reports("partial")
        result = observations.select_observations(inputs, reports)
        self.assert_middle_bundle(result, "FORMER", limit=False)
        for view in ADJUSTMENTS:
            self.assertEqual(inputs["FORMER"].views[view][0][5:7], ("101", "19"))
            self.assertEqual(inputs["MAPPED"].views[view][1][5:7], ("20", "2"))

    def test_fuller_raw_aggregate_cannot_splice_incompatible_adjusted_prices_or_share_units(self):
        for options in ({"different_basis": True}, {"split_scale": 2}):
            with self.subTest(options=options):
                inputs, reports = inputs_and_reports("partial", **options)
                self.assert_middle_bundle(
                    observations.select_observations(inputs, reports), "MAPPED", limit=True
                )

    def test_partial_split_volume_rounding_boundary_uses_exact_provider_precision(self):
        for adjustment, expected in ((1, "FORMER"), (2, "MAPPED")):
            with self.subTest(adjustment=adjustment):
                inputs, reports = inputs_and_reports("partial")
                mapped_shares = 10**29 + 1
                for symbol, source in inputs.items():
                    index = 1 if symbol == "MAPPED" else 0
                    for view, rows in source.views.items():
                        raw_volume = mapped_shares if symbol == "MAPPED" else mapped_shares + 1
                        delta = adjustment if symbol == "MAPPED" and (view == "all" or "split" in view) else 0
                        row = rows[index]
                        changed = (*row[:5], str(raw_volume + delta), *row[6:])
                        source.views[view] = (*rows[:index], changed, *rows[index + 1 :])
                self.assert_middle_bundle(
                    observations.select_observations(inputs, reports), expected, limit=(expected == "MAPPED")
                )

    def test_unknown_raw_support_keeps_historical_label_and_existing_bundle(self):
        inputs, reports = inputs_and_reports("unknown")
        for view in ADJUSTMENTS:
            row = inputs["FORMER"].views[view][0]
            inputs["FORMER"].views[view] = ((*row[:5], "500", "30", *row[7:]),)
        self.assert_middle_bundle(observations.select_observations(inputs, reports), "MAPPED", limit=True)

    def test_equal_scope_consumes_raw_handoff_for_all_physical_aliases(self):
        inputs, reports = inputs_and_reports("equal")
        for symbol, source in inputs.items():
            raw = source.views["raw"]
            if symbol == "FORMER":
                # Different provider spellings of equal numbers were already
                # accepted in the raw handoff. All eight views alias that row.
                row = raw[0]
                raw = ((row[0], row[1] + ".0", *row[2:]),)
            source.views = dict.fromkeys(ADJUSTMENTS, raw)
        equal_bundles = {}
        with patch.object(observations, "_same_values", side_effect=AssertionError("repeated raw comparison")):
            result = observations.select_observations(inputs, reports, equality_outcomes=equal_bundles)
        self.assert_middle_bundle(result, "MAPPED", limit=False)
        self.assertEqual(equal_bundles, {("owner", timestamp(DAYS[1] + "T05:00:00Z")): True})

    def test_separate_owner_and_stronger_dated_market_keep_their_own_authority(self):
        inputs, reports = inputs_and_reports("equal", different_basis=True)
        for segment in reports["FORMER"]["correlated"]["segments"]:
            segment["observation_authority"] = {"market": "when_issued", "direct_market_source": True}
        self.assert_middle_bundle(observations.select_observations(inputs, reports), "FORMER", limit=False)
        for segment in reports["FORMER"]["correlated"]["segments"]:
            segment["security_identity"]["security_id"] = "other"
        selected, corrections = observations.select_observations(inputs, reports)
        self.assertFalse(corrections)
        for symbol in inputs:
            for segment in middle_segments(selected, symbol):
                self.assertEqual(segment["observation_selection"], "selected")
                self.assertEqual(
                    segment["security_identity"]["security_id"], "other" if symbol == "FORMER" else "owner"
                )

    def test_new_own_label_does_not_decide_an_earlier_unrelated_price_contest(self):
        inputs, reports = inputs_and_reports("equal", different_basis=True)
        ordinary = []
        for segment in reports["FORMER"]["correlated"]["segments"]:
            view = segment["adjustment"]
            previous = price_row(0, factor=Decimal(1))
            inputs["FORMER"].views[view] = (previous, *inputs["FORMER"].views[view])
            segment.update(row_start=2, row_end=2)
            ordinary.append(
                {
                    "adjustment": view,
                    "row_start": 1,
                    "row_end": 1,
                    "historical_symbol": "MAPPED",
                    "security_identity": {"security_id": "owner"},
                }
            )
        reports["FORMER"]["correlated"]["segments"] = [*ordinary, *reports["FORMER"]["correlated"]["segments"]]
        for segment in reports["MAPPED"]["correlated"]["segments"]:
            if segment["row_start"] == 1:
                segment["historical_symbol"] = "MAPPED"
        result = observations.select_observations(inputs, reports)
        self.assert_middle_bundle(result, "MAPPED", limit=True)
        for symbol in inputs:
            earlier = [
                s for s in result[0][symbol]["correlated"]["segments"] if s["historical_symbol"] == "MAPPED"
            ]
            self.assertEqual(len(earlier), len(ADJUSTMENTS))
            for segment in earlier:
                self.assertEqual(
                    segment["observation_selection"], "selected" if symbol == "MAPPED" else "superseded"
                )
                self.assertNotIn(observations.FORMER_TICKER_BASIS_LIMIT, segment.get("warnings", ()))

    def test_completed_choices_do_not_reopen_the_bound_middle_session(self):
        inputs, reports = inputs_and_reports("equal", different_basis=True)
        first, _ = observations.select_observations(inputs, reports)
        retained = {}
        for symbol, report in first.items():
            runs = {view: [] for view in ADJUSTMENTS}
            for segment in report["correlated"]["segments"]:
                index = segment["row_start"]
                day = inputs[symbol].views[segment["adjustment"]][index - 1][0][:10]
                runs[segment["adjustment"]].append(
                    ChoiceRun(
                        index,
                        index,
                        "owner",
                        segment["observation_selection"],
                        tuple(segment["observation_tickers"]),
                        day,
                        day,
                    )
                )
            retained[symbol] = RetainedChoices(inputs[symbol].source_sha256, "receipt", runs)
            del inputs[symbol].views
        with patch.object(
            observations, "_select_new_observations", side_effect=AssertionError("reopened selection")
        ):
            second = observations.select_observations(inputs, first, retained_choices=retained)
        self.assert_middle_bundle(second, "MAPPED", limit=True)

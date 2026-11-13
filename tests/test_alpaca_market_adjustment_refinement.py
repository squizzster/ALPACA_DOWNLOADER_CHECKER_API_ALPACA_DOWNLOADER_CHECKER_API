"""Numerical changes consume completed labels; researched adjustment recipes reject."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_market_occupancy import completed_market_reports, continuous_case

from libs.alpaca_end_of_day_security_correlation import market_occupancy
from libs.alpaca_lineage_build import market_assignments
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class MarketAdjustmentRefinementTests(TestCase):
    def test_changed_numerical_bundle_reuses_closed_labels(self):
        inputs, reports, _ = continuous_case()
        completed = completed_market_reports(inputs, reports)
        for name, report in completed.items():
            for segment in report["correlated"]["segments"]:
                segment["observation_selection"] = "selected" if name == "MAIN" else "superseded"
        before = deepcopy(completed)
        # A changed source generation owns this new numerical selection. Existing
        # dated labels remain completed input and no fact projector is consulted.
        with patch.object(market_occupancy, "project", side_effect=AssertionError("closed labels repeated")):
            selected, corrections = select_observations(inputs, completed)
        self.assertFalse(corrections)
        self.assertEqual(before, completed)
        first = [
            s for s in selected["ALT"]["correlated"]["segments"] if s["first_timestamp"][:10] == "2026-10-02"
        ]
        self.assertEqual(set(ADJUSTMENTS), {s["adjustment"] for s in first})
        self.assertTrue(
            all(s["historical_symbol"] == "ALT" and s["observation_selection"] == "selected" for s in first)
        )

    def test_completed_labels_cannot_authorize_new_researched_adjustment_input(self):
        _, _, evidence = continuous_case()
        completed = market_assignments.CompletedMarketProjection({}, {})
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            market_assignments.project_group(
                {},
                evidence,
                {},
                {},
                load_authorized_policy(),
                completed_labels=completed,
                date_rows=lambda *_: self.fail("price access"),
            )

    def test_raw_adjustment_plan_cannot_bypass_the_recipe_guard(self):
        plan = market_occupancy.MarketPlan(
            (),
            {},
            {},
            ({"public_id": "public", "continuing_source": "MAIN", "from": "2026-10-02", "to": "2026-10-07"},),
        )
        with self.assertRaisesRegex((ValueError, RuntimeError), "inadmissible|not supported"):
            market_assignments.project_group(
                {},
                {},
                {},
                {},
                load_authorized_policy(),
                plan=plan,
                completed_labels=market_assignments.CompletedMarketProjection({}, {}),
            )

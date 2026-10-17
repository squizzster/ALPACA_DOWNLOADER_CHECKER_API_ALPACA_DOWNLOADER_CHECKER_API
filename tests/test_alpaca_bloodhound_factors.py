import copy
import unittest

from libs.alpaca_bloodhound_export.factors import ADJUSTMENTS, FactorError, infer_split_factors


def views(price="100", volume="100"):
    row = {field: price for field in ("open", "high", "low", "close", "vwap")}
    row.update(session="2024-01-02", volume=volume)
    return {name: {"bars": [dict(row)]} for name in ADJUSTMENTS}


def set_view(data, name, price, volume=None):
    row = data[name]["bars"][0]
    row.update({field: str(price) for field in ("open", "high", "low", "close", "vwap")})
    if volume is not None:
        row["volume"] = str(volume)


class BloodhoundFactorTests(unittest.TestCase):
    def test_unchanged_tiny_prices_choose_one_not_interval_midpoint(self):
        for price in ("0.001", "0.005", "0.000001", "100"):
            with self.subTest(price=price):
                data = views(price, "0")
                before = copy.deepcopy(data)
                factors, report = infer_split_factors(data)
                self.assertEqual(factors, [1.0])
                self.assertEqual(data, before)
                self.assertEqual(report["anchors"]["unchanged"], 1)

    def test_nvda_shape_uses_joint_volume_anchor_without_altering_all(self):
        data = views("32.37", "9777779")
        for name in ("split", "split,spin-off"):
            set_view(data, name, "0.8092", "391111160")
        for name in ("dividend", "dividend,spin-off"):
            set_view(data, name, "31.51")
        for name in ("split,dividend", "all"):
            set_view(data, name, "0.7877", "391111160")
        before = copy.deepcopy(data)
        factors, report = infer_split_factors(data)
        self.assertAlmostEqual(factors[0], 0.025, places=15)
        self.assertEqual(data, before)
        self.assertEqual(report["anchors"]["volume_ratio"], 1)
        self.assertTrue(report["all_ohlc_preserved_without_reconstruction"])

    def test_pairwise_inconsistent_full_factor_is_rejected(self):
        data = views("0.5", "0")
        set_view(data, "all", "0.53")
        with self.assertRaisesRegex(FactorError, "joint"):
            infer_split_factors(data)

    def test_zero_split_volume_retains_lower_factor_bound(self):
        data = views("100", "100")
        data["split"]["bars"][0]["volume"] = "0"
        with self.assertRaisesRegex(FactorError, "joint"):
            infer_split_factors(data)

    def test_split_volume_one_retains_lower_factor_bound(self):
        data = views("100", "100")
        data["split"]["bars"][0]["volume"] = "1"
        with self.assertRaisesRegex(FactorError, "joint"):
            infer_split_factors(data)

    def test_zero_raw_volume_incompatible_with_positive_split_volume(self):
        data = views("100", "0")
        data["split"]["bars"][0]["volume"] = "2"
        with self.assertRaisesRegex(FactorError, "zero raw volume"):
            infer_split_factors(data)

    def test_every_non_split_view_must_preserve_exact_raw_volume(self):
        for name in ("dividend", "spin-off", "dividend,spin-off"):
            data = views(volume="9007199254740993")
            data[name]["bars"][0]["volume"] = "9007199254740992"
            with self.subTest(view=name), self.assertRaisesRegex(FactorError, f"{name} volume differs"):
                infer_split_factors(data)

    def test_every_split_containing_volume_constrains_the_factor(self):
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            data = views("100", "100")
            data[name]["bars"][0]["volume"] = "1000"
            with self.subTest(view=name), self.assertRaisesRegex(FactorError, f"{name} volume incompatible"):
                infer_split_factors(data)

    def test_equal_split_volume_aliases_do_not_artificially_narrow_interval(self):
        data = views("0.1", "1")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "0.03", "3")
        _, report = infer_split_factors(data)
        lower = report["regimes"][0]["feasible_lower"]
        self.assertAlmostEqual(lower, 0.25)

    def test_near_sixteen_prefers_exact_reciprocal_when_feasible(self):
        data = views("100", "1")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "6.25", "16.01011")
        factors, report = infer_split_factors(data)
        self.assertEqual(factors, [0.0625])
        self.assertEqual(report["regimes"][0]["price_factor_exact_rational"], "1/16")
        self.assertEqual(report["regimes"][0]["implied_share_multiplier_exact_rational"], "16")

    def test_exact_sixteen_is_not_forced_outside_the_feasible_interval(self):
        data = views("1000", "1")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "62.460534", "16.01011")
        factors, report = infer_split_factors(data)
        self.assertNotEqual(factors, [0.0625])
        self.assertNotEqual(report["regimes"][0]["price_factor_exact_rational"], "1/16")

    def test_rational_preference_is_reciprocal_symmetric(self):
        data = views("100", "16.01011")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "1600", "1")
        factors, report = infer_split_factors(data)
        self.assertEqual(factors, [16.0])
        self.assertEqual(report["regimes"][0]["price_factor_exact_rational"], "16")
        self.assertEqual(report["regimes"][0]["implied_share_multiplier_exact_rational"], "1/16")

    def test_genuine_fractional_ratio_is_not_rounded_to_an_integer(self):
        data = views("100", "1")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "66.67", "1.50011")
        factors, report = infer_split_factors(data)
        self.assertEqual(factors, [2 / 3])
        self.assertEqual(report["regimes"][0]["price_factor_exact_rational"], "2/3")
        self.assertEqual(report["regimes"][0]["implied_share_multiplier_exact_rational"], "3/2")

    def test_broad_uncertainty_retains_anchor_instead_of_rational_preference(self):
        data = views("0.1", "1")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "0.03", "3.125")
        factors, report = infer_split_factors(data)
        self.assertAlmostEqual(factors[0], 1 / 3.125)
        self.assertIsNone(report["regimes"][0]["price_factor_exact_rational"])

    def test_multiple_simplest_integers_retains_anchor(self):
        data = views("0.001", "10000")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "100", "0.1")
        _, report = infer_split_factors(data)
        self.assertIsNone(report["regimes"][0]["price_factor_exact_rational"])

    def test_fractional_volume_preserved_and_used(self):
        data = views("100", "113.125")
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "5", "2262.5")
        factors, _ = infer_split_factors(data)
        self.assertAlmostEqual(factors[0], 0.05, places=15)
        self.assertEqual(data["raw"]["bars"][0]["volume"], "113.125")

    def test_tiny_factor_changes_hidden_by_rounding_share_declared_regime(self):
        data = views("100", "0")
        for name in ADJUSTMENTS:
            second = dict(data[name]["bars"][0], session="2024-01-03")
            data[name]["bars"].append(second)
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            for field in ("open", "high", "low", "close", "vwap"):
                data[name]["bars"][0][field] = "99.99"
        full, report = infer_split_factors(data)
        self.assertEqual(full[0], full[1])
        self.assertEqual(report["regime_count"], 1)
        self.assertTrue(report["full_history_required_before_date_filter"])
        self.assertFalse(report["all_ohlc_temporal_pooling_or_smoothing"])

    def test_thin_volume_rounding_does_not_invent_daily_share_unit_changes(self):
        data = {name: {"bars": []} for name in ADJUSTMENTS}
        for session, volume, split_volume in (("2024-01-02", "1", "3"), ("2024-01-03", "1.2", "4")):
            for name in ADJUSTMENTS:
                adjusted = name in ("split", "split,dividend", "split,spin-off", "all")
                row = {
                    field: "0.03" if adjusted else "0.1" for field in ("open", "high", "low", "close", "vwap")
                }
                row.update(session=session, volume=split_volume if adjusted else volume)
                data[name]["bars"].append(row)
        factors, report = infer_split_factors(data)
        self.assertEqual(factors[0], factors[1])
        self.assertAlmostEqual(factors[0], 2.2 / 7)
        self.assertEqual(report["regime_count"], 1)
        self.assertGreater(report["maximum_selected_factor_relative_error_bound"], 0.1)

    def test_disjoint_reverse_split_boundary_remains_a_factor_change(self):
        data = views("100", "100")
        for name in ADJUSTMENTS:
            second = dict(data[name]["bars"][0], session="2024-01-03")
            data[name]["bars"].append(second)
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            set_view(data, name, "1000", "10")
        factors, report = infer_split_factors(data)
        self.assertAlmostEqual(factors[0], 10.0)
        self.assertEqual(factors[1], 1.0)
        self.assertEqual(report["regime_count"], 2)

    def test_dividend_change_does_not_smooth_prices_or_create_split_change(self):
        data = views("100", "100")
        for name in ("split", "split,spin-off"):
            set_view(data, name, "50", "200")
        for name in ("dividend", "dividend,spin-off"):
            set_view(data, name, "90")
        for name in ("split,dividend", "all"):
            set_view(data, name, "45", "200")
        for name in ADJUSTMENTS:
            second = dict(data[name]["bars"][0], session="2024-01-03")
            if name in ("dividend", "dividend,spin-off", "split,dividend", "all"):
                second.update(
                    {
                        field: "80" if name in ("dividend", "dividend,spin-off") else "40"
                        for field in ("open", "high", "low", "close", "vwap")
                    }
                )
            data[name]["bars"].append(second)
        before = copy.deepcopy(data)
        factors, report = infer_split_factors(data)
        self.assertEqual(factors[0], factors[1])
        self.assertAlmostEqual(factors[0], 0.5)
        self.assertEqual(report["regime_count"], 1)
        self.assertEqual(data, before)

    def test_missing_view_or_different_grid_is_rejected(self):
        data = views()
        del data["dividend"]
        with self.assertRaisesRegex(FactorError, "eight"):
            infer_split_factors(data)
        data = views()
        data["all"]["bars"][0]["session"] = "2024-01-03"
        with self.assertRaisesRegex(FactorError, "session grid"):
            infer_split_factors(data)

    def test_bad_numeric_or_zero_raw_close_is_rejected(self):
        for value in ("nan", "inf", "-1", "oops", "0"):
            data = views()
            data["raw"]["bars"][0]["close"] = value
            with self.subTest(value=value), self.assertRaises(FactorError):
                infer_split_factors(data)


if __name__ == "__main__":
    unittest.main()

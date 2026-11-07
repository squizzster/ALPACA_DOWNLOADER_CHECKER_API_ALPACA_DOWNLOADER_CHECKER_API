"""Focused numerical and regression tests for split inference.

SciPy is optional and is used only as an independent test oracle, never by
the application.  Run ``python -m unittest discover -s tests -v`` from the
application directory.
"""

import copy
import json
import unittest

import numpy as np
from bloodhound_factors import (
    ADJUSTED_VIEWS,
    FactorInferenceError,
    _project_log_split,
    feasible_split_bounds,
    infer_split_factors,
    validate_split_factors,
)

try:
    from scipy.optimize import linprog
except ImportError:
    linprog = None


MASKS = np.array(
    [[1, 0, 0], [0, 1, 0], [0, 0, 1], [1, 1, 0], [1, 0, 1], [0, 1, 1], [1, 1, 1]],
    dtype=float,
)


def make_views(splits, dividends=1.0, spinoffs=1.0, volumes=3_000_000.25):
    s = np.atleast_1d(splits).astype(float)
    n = len(s)
    d = np.broadcast_to(np.asarray(dividends, dtype=float), (n,))
    o = np.broadcast_to(np.asarray(spinoffs, dtype=float), (n,))
    v = np.broadcast_to(np.asarray(volumes, dtype=float), (n,)).copy()
    raw = {
        "open": np.full(n, 100.0),
        "high": np.full(n, 102.0),
        "low": np.full(n, 99.0),
        "close": np.full(n, 101.0),
        "vwap": np.full(n, 100.5),
        "volume": v,
    }
    views = {"raw": raw}
    for name, mask in zip(ADJUSTED_VIEWS, MASKS):
        factor = s ** mask[0] * d ** mask[1] * o ** mask[2]
        views[name] = {key: np.round(value * factor, 4) for key, value in raw.items() if key != "volume"}
        views[name]["volume"] = v / s if mask[0] else v.copy()
    return views


class FactorInferenceTests(unittest.TestCase):
    def test_forward_reverse_and_compound_price_factors(self):
        expected = np.array([0.025, 0.025, 1.0 / 3.0, 1.0 / 3.0, 8.0, 1.0])
        views = make_views(expected, dividends=0.973, spinoffs=0.812)
        original = copy.deepcopy(views)
        factors, diagnostics = infer_split_factors(views)
        np.testing.assert_allclose(factors, expected, rtol=1e-14, atol=0)
        self.assertEqual(diagnostics["segment_count"], 4)
        self.assertFalse(diagnostics["corporate_actions_certified"])
        self.assertTrue(diagnostics["selected_factors_jointly_feasible"])
        for view in original:
            for field in original[view]:
                np.testing.assert_array_equal(views[view][field], original[view][field])
        json.dumps(diagnostics, allow_nan=False)

    def test_unchanged_tiny_prices_zero_volume_select_neutral_one(self):
        views = make_views([1.0, 1.0], volumes=0.0)
        for view in views.values():
            for field in ("open", "high", "low", "close"):
                view[field][:] = 0.001
            view["vwap"][:] = 0.0
        factors, diagnostics = infer_split_factors(views)
        np.testing.assert_array_equal(factors, [1.0, 1.0])
        segment = diagnostics["segments"][0]
        self.assertEqual(segment["anchor"], "neutral")
        self.assertTrue(segment["ambiguous"])
        self.assertEqual(segment["lower"], 0.0)

    def test_full_segment_informs_leading_ambiguous_row(self):
        views = make_views([1.0 / 3.0, 1.0 / 3.0, 1.0])
        for view in views.values():
            for field in ("open", "high", "low", "close", "vwap"):
                view[field][0] = 0.001
            view["volume"][0] = 0.0
        factors, diagnostics = infer_split_factors(views)
        np.testing.assert_array_equal(factors, [1.0 / 3.0, 1.0 / 3.0, 1.0])
        self.assertEqual(diagnostics["segments"][0]["end_index"], 2)
        self.assertEqual(diagnostics["segments"][0]["anchor"], "volume")

    def test_missing_intermediate_views_are_unconstrained(self):
        views = make_views([0.25, 1.0], dividends=0.84, spinoffs=0.92)
        views = {name: views[name] for name in ("raw", "split", "all")}
        factors, diagnostics = infer_split_factors(views)
        np.testing.assert_array_equal(factors, [0.25, 1.0])
        self.assertEqual(len(diagnostics["missing_optional_views"]), 5)

    def test_jointly_inconsistent_prices_are_rejected(self):
        views = make_views([1.0], volumes=0)
        for field in ("open", "high", "low", "close", "vwap"):
            views["all"][field] *= 1.25
        with self.assertRaisesRegex(FactorInferenceError, "No common positive") as error:
            infer_split_factors(views)
        self.assertEqual(error.exception.row_indices, [0])

    def test_pairwise_overlap_does_not_imply_a_joint_solution(self):
        # Singles are 0..1, each pair 1.5..2 and the triple 0..2 in log
        # space.  Each product interval overlaps products inferred separately
        # from its constituent singles.  The three pair lower bounds require
        # a triple >=2.25, however, contradicting its upper bound of 2.
        lower = np.array([0, 0, 0, 1.5, 1.5, 1.5, 0], dtype=float)[:, None]
        upper = np.array([1, 1, 1, 2, 2, 2, 2], dtype=float)[:, None]
        _, _, feasible = _project_log_split(lower, upper)
        self.assertFalse(feasible[0])

    def test_nonzero_adjusted_volume_with_raw_zero_is_rejected(self):
        views = make_views([1.0], volumes=0)
        views["split"]["volume"][0] = 600_000
        with self.assertRaisesRegex(FactorInferenceError, "raw volume is zero"):
            infer_split_factors(views)

    def test_volume_allowance_zero_and_fractional_volume(self):
        views = make_views([0.25], volumes=3_000_000.25)
        factors, _ = infer_split_factors(views, volume_tolerance=0)
        np.testing.assert_allclose(factors, [0.25], rtol=1e-14)
        views["split"]["volume"][0] = 0
        with self.assertRaises(FactorInferenceError):
            infer_split_factors(views, volume_tolerance=0)

    def test_action_factors_are_jointly_cross_checked(self):
        views = make_views([0.25, 1.0], dividends=0.8, spinoffs=0.9)
        diagnostic = validate_split_factors(views, np.array([0.25, 1.0]))
        self.assertTrue(diagnostic["supplied_factors_jointly_feasible"])
        self.assertFalse(diagnostic["corporate_actions_certified"])
        with self.assertRaisesRegex(FactorInferenceError, "Supplied split factors"):
            validate_split_factors(views, np.array([0.5, 1.0]))

    def test_bounds_include_true_factors_and_support_unbounded_prices(self):
        views = make_views([0.25])
        lower, upper, _ = feasible_split_bounds(views)
        self.assertLessEqual(lower[0], 0.25)
        self.assertGreaterEqual(upper[0], 0.25)
        for view in views.values():
            for value in view.values():
                value[:] = 0
        lower, upper, _ = feasible_split_bounds(views)
        self.assertEqual(lower[0], 0)
        self.assertEqual(upper[0], np.inf)
        factors, diag = infer_split_factors(views)
        self.assertEqual(factors[0], 1)
        self.assertIsNone(diag["segments"][0]["upper"])
        json.dumps(diag, allow_nan=False)

    def test_empty_aligned_arrays(self):
        views = make_views([])
        factors, diag = infer_split_factors(views)
        self.assertEqual(len(factors), 0)
        self.assertEqual(diag["segments"], [])

    def test_invalid_inputs_are_rejected(self):
        views = make_views([1.0])
        with self.assertRaisesRegex(FactorInferenceError, "Required adjustment views"):
            infer_split_factors({"raw": views["raw"], "all": views["all"]})
        with self.assertRaisesRegex(FactorInferenceError, "nonnegative"):
            infer_split_factors(views, volume_tolerance=-1)
        views["raw"]["close"][0] = np.nan
        with self.assertRaisesRegex(FactorInferenceError, "non-finite"):
            infer_split_factors(views)

    @unittest.skipIf(linprog is None, "SciPy is only needed for the independent LP oracle")
    def test_randomized_projection_matches_independent_linear_program(self):
        rng = np.random.default_rng(20261017)
        count = 1000
        centers = rng.normal(0, 1.5, size=(7, count))
        # Half deliberately contain a common point, half use unrelated views.
        centers[:, : count // 2] = MASKS @ rng.normal(0, 1.5, size=(3, count // 2))
        lower = centers - rng.uniform(0.005, 1.5, size=(7, count))
        upper = centers + rng.uniform(0.005, 1.5, size=(7, count))
        lower[rng.random((7, count)) < 0.15] = -np.inf
        upper[rng.random((7, count)) < 0.15] = np.inf
        lower[:, 0] = -np.inf
        upper[:, 0] = np.inf
        actual_lower, actual_upper, feasible = _project_log_split(lower, upper)
        checked_feasible = 0
        checked_infeasible = 0
        for i in range(count):
            rows, rhs = [], []
            for j, mask in enumerate(MASKS):
                if np.isfinite(upper[j, i]):
                    rows.append(mask)
                    rhs.append(upper[j, i])
                if np.isfinite(lower[j, i]):
                    rows.append(-mask)
                    rhs.append(-lower[j, i])
            kwargs = dict(
                A_ub=np.asarray(rows) if rows else None,
                b_ub=np.asarray(rhs) if rhs else None,
                bounds=[(None, None)] * 3,
                method="highs",
            )
            minimum = linprog([1, 0, 0], **kwargs)
            self.assertIn(minimum.status, (0, 2, 3), (i, minimum.message))
            expected_feasible = minimum.status != 2
            self.assertEqual(bool(feasible[i]), expected_feasible, f"case {i}")
            if not expected_feasible:
                checked_infeasible += 1
                continue
            checked_feasible += 1
            maximum = linprog([-1, 0, 0], **kwargs)
            self.assertIn(maximum.status, (0, 3), (i, maximum.message))
            expected_lower = minimum.fun if minimum.status == 0 else -np.inf
            expected_upper = -maximum.fun if maximum.status == 0 else np.inf
            np.testing.assert_allclose(
                actual_lower[i], expected_lower, atol=1e-8, rtol=1e-10, err_msg=f"lower case {i}"
            )
            np.testing.assert_allclose(
                actual_upper[i], expected_upper, atol=1e-8, rtol=1e-10, err_msg=f"upper case {i}"
            )
        self.assertGreater(checked_feasible, 0)
        self.assertGreater(checked_infeasible, 0)


if __name__ == "__main__":
    unittest.main()

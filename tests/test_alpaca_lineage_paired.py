"""Candidate attribution uses one loaded group and completed control decisions."""

import copy
import json
import unittest
from unittest.mock import patch

import test_alpaca_lineage_debug as debug_fixtures
import yaml

from alpaca_end_of_day import LineageDebugError
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_debug import paired
from libs.alpaca_lineage_debug.policy import DEFAULT_POLICY
from libs.alpaca_lineage_policy_lab import comparison


class PairedImpactTests(unittest.TestCase):
    setUp = debug_fixtures.DebugStoreTests.setUp

    def run_pair(self, **kwargs):
        return debug_fixtures.request(
            "run", store=str(self.store), policy=str(self.policy), control_policy=str(DEFAULT_POLICY), **kwargs
        )

    def test_identical_cascade_consumes_control_without_second_evaluation(self):
        with (
            patch.object(comparison, "correlate", wraps=comparison.correlate) as correlate,
            patch.object(comparison, "select_observations", wraps=comparison.select_observations) as numeric,
            patch.object(comparison, "_group_dimensions", wraps=comparison._group_dimensions) as normalize,
            patch.object(storage, "read_index", wraps=storage.read_index) as index,
            patch.object(comparison, "load_pricing", wraps=comparison.load_pricing) as prices,
        ):
            report = self.run_pair(all=True)
        self.assertTrue(report["execution_complete"], report)
        self.assertEqual(index.call_count, 1)
        self.assertEqual(prices.call_count, 2)
        self.assertEqual(correlate.call_count, 2)
        self.assertEqual(numeric.call_count, 2)
        self.assertEqual(normalize.call_count, 4)
        self.assertEqual(report["summary"]["different_groups"], 0)
        self.assertTrue(all(group["control_correlation_reused"] for group in report["groups"]))

    def test_stored_drift_does_not_become_candidate_impact(self):
        original = comparison._group_dimensions
        calls = 0

        def dimensions(*args, **kwargs):
            nonlocal calls
            result = original(*args, **kwargs)
            calls += 1
            if calls == 1:
                key = next(iter(result["explanation"]))
                result["explanation"][key] = {**result["explanation"][key], "warnings": ["stored-drift"]}
            return result

        with patch.object(comparison, "_group_dimensions", side_effect=dimensions):
            report = self.run_pair(symbols=["GOOG"])
        group = report["groups"][0]
        self.assertFalse(group["published_to_control"]["parity"])
        self.assertTrue(group["control_to_candidate"]["parity"])
        self.assertFalse(group["published_to_candidate"]["parity"])
        self.assertEqual(report["summary"]["different_groups"], 0)
        self.assertEqual(report["summary"]["published_control_drift_groups"], 1)

    def test_changed_yaml_replays_candidate_but_reuses_completed_baseline(self):
        value = yaml.safe_load(self.policy.read_text())
        value["execution"]["established_fixed_point"]["row_rules"] = []
        self.policy.write_text(yaml.safe_dump(value))
        with (
            patch.object(comparison, "correlate", wraps=comparison.correlate) as correlate,
            patch.object(comparison, "_group_dimensions", wraps=comparison._group_dimensions) as normalize,
            patch.object(comparison, "load_pricing", wraps=comparison.load_pricing) as prices,
        ):
            report = self.run_pair(symbols=["GOOG"])
        self.assertTrue(report["execution_complete"], report)
        self.assertEqual(correlate.call_count, 2)
        self.assertEqual(normalize.call_count, 3)
        self.assertEqual(prices.call_count, 1)
        self.assertEqual(report["summary"]["different_groups"], 1)
        self.assertFalse(report["groups"][0]["control_correlation_reused"])
        self.assertTrue(
            any(
                dimension["affected_sources"]
                for dimension in report["groups"][0]["comparison"]["differences"].values()
            )
        )

    def test_refinement_requires_named_control_and_is_rejected_by_production(self):
        value = yaml.safe_load(self.policy.read_text())
        value["research"]["membership_refinement"] = "accepted_same_security_containing_component_v1"
        self.policy.write_text(yaml.safe_dump(value))
        with self.assertRaisesRegex(LineageDebugError, "explicit control_policy"):
            debug_fixtures.request("run", store=str(self.store), policy=str(self.policy), symbols=["GOOG"])
        with patch.object(comparison, "correlate", wraps=comparison.correlate) as correlate:
            report = self.run_pair(symbols=["GOOG"])
        self.assertTrue(report["execution_complete"], report)
        self.assertEqual(correlate.call_count, 1)
        self.assertFalse(report["groups"][0]["membership_refinement"]["changes"])
        self.assertEqual(report["summary"]["different_groups"], 0)


class FrameTests(unittest.TestCase):
    def test_diagnostic_change_is_not_a_changed_owner(self):
        first = {
            "dimensions": {
                "semantic": {("AAA", "raw", "t"): {"owner": "same"}},
                "observation": {},
                "explanation": {("AAA", "raw", "t"): {"rule": "before"}},
                "group_diagnostics": {},
            },
            "digests": {name: name for name in ("semantic", "observation", "explanation", "group_diagnostics")},
            "memberships": {},
            "counts": {},
        }
        second = copy.deepcopy(first)
        second["dimensions"]["explanation"]["AAA", "raw", "t"] = {"rule": "after"}
        result = paired.compare_frames(first, second, 0)
        self.assertTrue(result["decision_parity"])
        self.assertTrue(result["diagnostic_only"])
        self.assertFalse(result["parity"])
        self.assertEqual(result["differences"]["explanation"]["count"], 1)
        self.assertTrue(result["differences"]["explanation"]["examples_truncated"])
        self.assertEqual(result["differences"]["explanation"]["affected_sources"]["AAA"]["source_view_rows"], 1)
        json.dumps(result)


if __name__ == "__main__":
    unittest.main()

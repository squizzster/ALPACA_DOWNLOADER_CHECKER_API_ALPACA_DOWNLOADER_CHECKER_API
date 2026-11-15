"""Add/remove/same label producer comparisons consume completed other dimensions."""

from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_labels import fixture

from libs.alpaca_lineage_build import dated_labels
from libs.alpaca_lineage_debug import paired
from libs.alpaca_lineage_debug.policy import DEFAULT_POLICY, load_policy


class LabelComparisonTests(TestCase):
    def fixture(self, enabled):
        inputs, reports, evidence = fixture()
        dimensions = {"semantic": {}, "observation": {}, "explanation": {}, "group_diagnostics": {}}
        indexes = {}
        for source, report in reports.items():
            indexes[source] = (
                {name: (rows, [row[0][:10] for row in rows]) for name, rows in inputs[source].views.items()},
                set(),
            )
            for segment in report["correlated"]["segments"]:
                row = inputs[source].views[segment["adjustment"]][segment["row_start"] - 1]
                key = source, segment["adjustment"], row[0]
                dimensions["semantic"][key] = {
                    "security_identity": segment["security_identity"],
                    "historical_symbol": source,
                }
                dimensions["observation"][key] = {
                    key: segment[key] for key in ("observation_tickers", "observation_selection")
                }
                dimensions["explanation"][key] = {"accepted": True}
        digests = {name: paired._digest_mapping(values) for name, values in dimensions.items()}
        frame = {
            "dimensions": dimensions,
            "digests": digests,
            "memberships": ["owner"],
            "counts": {"rows": 64},
            "mapping_corrections": {},
        }
        candidate, trace = frame, None
        if enabled:
            changed, trace = dated_labels.project_observation_frame(dimensions, reports, evidence, indexes)
            candidate = {
                **frame,
                "dimensions": changed,
                "digests": {**digests, "observation": paired._digest_mapping(changed["observation"])},
            }
        completed = {
            "candidate": candidate,
            "published": {**frame, "row_indexes": indexes},
            "reports": reports,
            "evidence": evidence,
            "unlabelled_dimensions": dimensions,
            "unlabelled_digests": digests,
            "label_trace": trace,
        }
        selected = {"inputs": inputs, "selected_group": sorted(inputs)}
        policy = replace(load_policy(DEFAULT_POLICY), ruleset=None)
        return selected, completed, policy

    def evaluate(self, enabled, candidate_enabled, *, forbid_plan=False):
        selected, completed, policy = self.fixture(enabled)
        control_policy = replace(policy, dated_labels=dated_labels.NAME if enabled else None)
        candidate_policy = replace(policy, dated_labels=dated_labels.NAME if candidate_enabled else None)

        def carry(*args, completed=None, **kwargs):
            completed.update(state)
            return {"baseline": {}, "comparison": {"accepted": True}}

        state = completed
        with (
            patch.object(paired, "_compare_loaded_group", side_effect=carry),
            patch.object(
                paired, "_group_dimensions", side_effect=AssertionError("unrelated dimensions normalized")
            ),
            patch.object(
                paired, "make_publication", side_effect=AssertionError("unnecessary artifact constructed")
            ),
            patch.object(paired, "_memberships", side_effect=AssertionError("completed memberships calculated")),
            patch.object(
                dated_labels,
                "plan",
                side_effect=AssertionError("completed label plan repeated")
                if forbid_plan
                else dated_labels.plan,
            ) as plan,
            patch.object(paired, "_digest_mapping", wraps=paired._digest_mapping) as digest,
        ):
            result = paired.evaluate(None, "OLD", candidate_policy, control_policy, selected, 10)
        return result, plan.call_count, digest.call_count

    def test_identical_enabled_producer_is_not_repeated(self):
        result, plans, digests = self.evaluate(True, True, forbid_plan=True)
        self.assertTrue(result["control_to_candidate"]["parity"])
        self.assertEqual((0, 0), (plans, digests))

    def test_removal_uses_prelabel_frame_without_repeating_other_work(self):
        result, plans, digests = self.evaluate(True, False, forbid_plan=True)
        self.assertGreater(result["control_to_candidate"]["differences"]["observation"]["count"], 0)
        self.assertEqual((0, 0), (plans, digests))
        self.assertEqual(0, result["control_to_candidate"]["differences"]["semantic"]["count"])

    def test_addition_plans_once_and_hashes_only_changed_observations(self):
        result, plans, digests = self.evaluate(False, True)
        self.assertGreater(result["control_to_candidate"]["differences"]["observation"]["count"], 0)
        self.assertEqual((1, 1), (plans, digests))
        self.assertEqual(0, result["control_to_candidate"]["differences"]["explanation"]["count"])

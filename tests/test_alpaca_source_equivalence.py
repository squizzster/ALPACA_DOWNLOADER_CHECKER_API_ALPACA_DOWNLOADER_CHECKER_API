"""Equal values retain only an applicable explicit numerical authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import source_equivalence
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.observation_carry import ChoiceRun
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp
from libs.alpaca_serving_catalogue.contract import policy
from libs.alpaca_serving_catalogue.source_equivalence import accepted_dates

OLD, NEW = "1" * 64, "2" * 64
RECEIPT, PRICES = "3" * 64, "4" * 64
DAY = "2024-01-02"


def frame():
    return {
        "previous_receipt_generations": {"BEFORE": RECEIPT},
        "previous_pricing_generations": {"BEFORE": PRICES},
        "pricing_generations": {"BEFORE": PRICES, "AFTER": "5" * 64},
        "identities": {
            NEW: {DAY: {"selected_source": "AFTER", "donors": [{"source": "BEFORE", "public_id": OLD}]}}
        },
    }


class SourceEquivalenceTests(TestCase):
    def test_unrequested_legacy_dates_do_not_open_or_reconstruct_donor_capabilities(self):
        group = SimpleNamespace(
            numerical_source_equivalences=lambda: self.fail("unused equality handoff opened")
        )
        self.assertEqual({}, accepted_dates(group, [], {}, public_id=NEW, sessions=set()))
        group.numerical_source_equivalences = lambda: {"AFTER": {"version": 1, "decisions": [frame()]}}
        self.assertEqual({}, accepted_dates(group, [], {}, public_id=OLD, sessions={DAY}))

    def test_new_source_can_inherit_only_the_accepted_donor_dates_and_binding(self):
        decision = frame()
        prior = {
            "policy": policy(),
            "references": {"lineage": {"BEFORE": RECEIPT}, "pricing": {"BEFORE": PRICES}},
            "history_validation": {
                "inputs": {"BEFORE": {"pricing_generation": PRICES}},
                "prefix_records": [
                    {"public_id": OLD, "session_dates": [DAY], "validated_through": DAY, "price_reasons": []}
                ],
            },
        }
        group = SimpleNamespace(
            numerical_source_equivalences=lambda: {"AFTER": {"version": 1, "decisions": [decision]}},
            preserves_pricing_prefix=lambda *args, **kwargs: True,
        )
        inputs = {"BEFORE": {}, "AFTER": {}}
        self.assertEqual({NEW: {DAY}}, accepted_dates(group, [prior], inputs))
        changed = deepcopy(prior)
        changed["references"]["lineage"]["BEFORE"] = "6" * 64
        self.assertEqual({}, accepted_dates(group, [changed], inputs))
        group.preserves_pricing_prefix = lambda source, *args, **kwargs: source != "AFTER"
        self.assertEqual({}, accepted_dates(group, [prior], inputs))
        group.preserves_pricing_prefix = lambda source, *args, **kwargs: source != "BEFORE"
        self.assertEqual({}, accepted_dates(group, [prior], inputs))
        decision["identities"][NEW]["2024-01-03"] = decision["identities"][NEW][DAY]
        prior["history_validation"]["prefix_records"][0]["session_dates"].append("2024-01-03")
        group.preserves_pricing_prefix = lambda source, previous, *, through: (
            source != "AFTER" or through < "2024-01-03"
        )
        self.assertEqual({NEW: {DAY}}, accepted_dates(group, [prior], inputs))

    def test_nonnull_owner_without_explicit_selection_is_not_a_numerical_donor(self):
        row = (DAY + "T05:00:00Z", "10", "10", "10", "10", "1", "1", "10")
        inputs = {
            name: SimpleNamespace(views=dict.fromkeys(ADJUSTMENTS, (row,))) for name in ("BEFORE", "AFTER")
        }
        reports = {
            name: {
                "correlated": {
                    "segments": [
                        {
                            "adjustment": view,
                            "row_start": 1,
                            "row_end": 1,
                            "security_identity": {"security_id": NEW},
                            "observation_selection": "selected" if name == "AFTER" else "superseded",
                        }
                        for view in ADJUSTMENTS
                    ]
                }
            }
            for name in inputs
        }
        instant = timestamp(row[0])
        for state in (None, "superseded", "incomplete", "conflict", "selected"):
            choices = SimpleNamespace(
                runs={view: (ChoiceRun(1, 1, OLD, state, ("BEFORE",), DAY, DAY),) for view in ADJUSTMENTS}
            )
            projected = source_equivalence.project(
                inputs,
                reports,
                {},
                {"BEFORE": (None, None, None, choices)},
                {(NEW, instant): True},
                {row[0]: instant},
            )
            self.assertEqual(state == "selected", bool(projected["identities"]))
        choices.runs["all"] = ()
        projected = source_equivalence.project(
            inputs,
            reports,
            {},
            {"BEFORE": (None, None, None, choices)},
            {(NEW, instant): True},
            {row[0]: instant},
        )
        self.assertEqual({}, projected["identities"], "partial donor manufactured an all-view approval")

    def test_reopened_date_retires_only_its_equal_decision_and_keeps_original_binding(self):
        original = frame()
        original["identities"][NEW]["2024-01-03"] = original["identities"][NEW][DAY]
        control = SimpleNamespace(
            previous={
                "numerical_source_equivalence_version": 1,
                "numerical_source_equivalence": {"version": 1, "decisions": [original]},
            }
        )
        carried = source_equivalence.build({"AFTER": control}, {"opened": {(NEW, DAY)}, "identities": {}}, {})
        self.assertEqual({"2024-01-03"}, set(carried["decisions"][0]["identities"][NEW]))
        self.assertEqual(
            original["previous_receipt_generations"], carried["decisions"][0]["previous_receipt_generations"]
        )

    def test_accepted_frame_survives_metadata_projection_without_schema_replay(self):
        receipt = {
            "numerical_source_equivalence_version": 1,
            "numerical_source_equivalence": {"version": 1, "decisions": [frame()]},
        }
        source_equivalence.validate(receipt)
        control = SimpleNamespace(previous=receipt)
        with patch(
            "libs.alpaca_lineage_build.correlation_policy_contract.valid_sha256",
            side_effect=AssertionError("accepted frame authenticated again"),
        ):
            carried = source_equivalence.build({"AFTER": control}, {"opened": set(), "identities": {}}, {})
        self.assertEqual(receipt["numerical_source_equivalence"], carried)

    def test_missing_or_unknown_declared_projection_requires_owning_recovery(self):
        valid = {
            "symbol": "AFTER",
            "numerical_source_equivalence_version": 1,
            "numerical_source_equivalence": {"version": 1, "decisions": [frame()]},
        }
        source_equivalence.validate(valid)
        with patch(
            "libs.alpaca_lineage_build.correlation_policy_contract.valid_sha256",
            side_effect=AssertionError("accepted equality projection validated again"),
        ):
            source_equivalence.validate(valid)
        with self.assertRaises(TypeError):
            valid["numerical_source_equivalence"]["decisions"][0]["identities"].clear()
        for field in ("numerical_source_equivalence", "numerical_source_equivalence_version"):
            invalid = deepcopy(valid)
            invalid.pop(field)
            with self.assertRaisesRegex(LineageError, "source equivalence"):
                source_equivalence.validate(invalid)
        invalid = deepcopy(valid)
        invalid["numerical_source_equivalence_version"] = 2
        with self.assertRaisesRegex(LineageError, "source equivalence"):
            source_equivalence.validate(invalid)

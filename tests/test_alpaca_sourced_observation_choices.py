"""Sourced membership cannot infer a numerical state from serving ownership."""

from types import MappingProxyType, SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import source_observations
from libs.alpaca_lineage_build.observation_carry import ChoiceRun, RetainedChoices
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class SourcedChoiceTests(TestCase):
    def case(self, state, serving_owner):
        segments = [
            {
                "adjustment": view,
                "row_start": 1,
                "row_end": 2,
                "row_count": 2,
                "from": "2024-01-02",
                "to": "2024-01-03",
                "first_timestamp": "2024-01-02T05:00:00Z",
                "last_timestamp": "2024-01-03T05:00:00Z",
                "historical_symbol": "SOURCE",
                "security_identity": {"security_id": "joined"},
            }
            for view in ADJUSTMENTS
        ]
        report = {"correlated": {"row_count": 16, "segments": segments}}
        ownership = {
            "views": {
                view: {"ownership": [{"rows": 2, "owner": [serving_owner, ["WRONG"]]}]} for view in ADJUSTMENTS
            }
        }
        choices = RetainedChoices(
            "generation",
            "receipt",
            MappingProxyType(
                {
                    view: (ChoiceRun(1, 2, "old", state, ("ACCEPTED",), "2024-01-02", "2024-01-03"),)
                    for view in ADJUSTMENTS
                }
            ),
        )
        binding = (ownership, {"old": {"security_id": "joined"}}, set(), choices)
        return report, binding

    def test_explicit_states_and_labels_survive_independently_of_serving_owner(self):
        for state in ("selected", "superseded", "conflict", "incomplete"):
            for owner in (None, "old"):
                with self.subTest(state=state, owner=owner):
                    report, binding = self.case(state, owner)
                    with (
                        patch.object(
                            source_observations, "_preserved_fields", wraps=source_observations._preserved_fields
                        ) as projected,
                        patch.object(
                            source_observations,
                            "select_observations",
                            side_effect=AssertionError("choice replayed"),
                        ),
                        patch.object(
                            source_observations,
                            "counterpart",
                            side_effect=AssertionError("historical payload read"),
                        ),
                        patch.object(
                            source_observations,
                            "_covered",
                            side_effect=AssertionError("accepted choice re-proved"),
                        ),
                    ):
                        result, changes, scopes = source_observations.reconcile(
                            None,
                            {"SOURCE": SimpleNamespace()},
                            {"SOURCE": report},
                            {},
                            {},
                            {},
                            retained={"SOURCE": binding},
                        )
                    self.assertFalse(changes)
                    self.assertFalse(scopes)
                    self.assertEqual(len(ADJUSTMENTS), projected.call_count)
                    for segment in result["SOURCE"]["correlated"]["segments"]:
                        self.assertEqual(state, segment["observation_selection"])
                        self.assertEqual(["ACCEPTED"], segment["observation_tickers"])

    def test_unknown_choice_enters_owner_even_with_nonnull_serving_owner(self):
        report, binding = self.case(None, "old")
        with patch.object(
            source_observations, "select_observations", return_value=({"SOURCE": report}, {})
        ) as choose:
            source_observations.reconcile(
                None, {"SOURCE": object()}, {"SOURCE": report}, {}, {}, {}, retained={"SOURCE": binding}
            )
        self.assertEqual(1, choose.call_count)
        self.assertEqual(
            report["correlated"]["segments"], choose.call_args.args[1]["SOURCE"]["correlated"]["segments"]
        )

    def test_missing_explicit_handoff_does_not_promote_ownership_to_selection(self):
        report, binding = self.case("selected", "old")
        with patch.object(
            source_observations, "select_observations", return_value=({"SOURCE": report}, {})
        ) as choose:
            source_observations.reconcile(
                None, {"SOURCE": object()}, {"SOURCE": report}, {}, {}, {}, retained={"SOURCE": binding[:3]}
            )
        self.assertEqual(1, choose.call_count)

    def test_merged_new_ownership_is_split_at_accepted_choice_boundaries_without_rows(self):
        report, binding = self.case("selected", "old")
        choices = RetainedChoices(
            "generation",
            "receipt",
            MappingProxyType(
                {
                    view: (
                        ChoiceRun(
                            1,
                            1,
                            "old",
                            "selected",
                            ("FIRST",),
                            "2024-01-02",
                            "2024-01-02",
                            "2024-01-02T05:00:00Z",
                            "2024-01-02T05:00:00Z",
                        ),
                        ChoiceRun(
                            2,
                            2,
                            "old",
                            "conflict",
                            ("SECOND",),
                            "2024-01-03",
                            "2024-01-03",
                            "2024-01-03T05:00:00Z",
                            "2024-01-03T05:00:00Z",
                        ),
                    )
                    for view in ADJUSTMENTS
                }
            ),
        )
        with (
            patch.object(
                source_observations,
                "select_observations",
                side_effect=AssertionError("accepted choices replayed"),
            ),
            patch.object(source_observations, "counterpart", side_effect=AssertionError("accepted rows read")),
        ):
            result, changes, scopes = source_observations.reconcile(
                None,
                {"SOURCE": object()},
                {"SOURCE": report},
                {},
                {},
                {},
                retained={"SOURCE": (*binding[:3], choices)},
            )
        self.assertFalse(changes)
        self.assertFalse(scopes)
        segments = [s for s in result["SOURCE"]["correlated"]["segments"] if s["adjustment"] == "raw"]
        self.assertEqual(
            [(1, 1, "selected", ["FIRST"]), (2, 2, "conflict", ["SECOND"])],
            [
                (s["row_start"], s["row_end"], s["observation_selection"], s["observation_tickers"])
                for s in segments
            ],
        )
        self.assertEqual("2024-01-02T05:00:00Z", segments[0]["last_timestamp"])
        self.assertEqual("2024-01-03T05:00:00Z", segments[1]["first_timestamp"])

    def test_recovery_adapter_preserves_each_state_instead_of_owner_inference(self):
        for state in ("selected", "superseded", "conflict", "incomplete"):
            for owner in (None, "old"):
                with self.subTest(state=state, owner=owner):
                    report, binding = self.case(state, owner)
                    documents = {
                        kind: {"adjustments": [{"adjustment": view, "segments": []} for view in ADJUSTMENTS]}
                        for kind in ("completed_years", "current_year", "current_month")
                    }
                    for entry in documents["current_month"]["adjustments"]:
                        segment = next(
                            s for s in report["correlated"]["segments"] if s["adjustment"] == entry["adjustment"]
                        )
                        entry["segments"] = [
                            {**segment, "branch": "correlated", "observation_selection": "wrong"}
                        ]
                    receipt = {
                        "source_sha256": "generation",
                        "anchor_month": "2024-01",
                        "assignment_handoffs": {
                            kind: {
                                "ownership": {
                                    view: {"rows": 2 if kind == "current_month" else 0} for view in ADJUSTMENTS
                                }
                            }
                            for kind in documents
                        },
                    }
                    with (
                        patch.object(
                            source_observations,
                            "select_observations",
                            side_effect=AssertionError("completed choice replayed"),
                        ),
                        patch.object(
                            source_observations,
                            "_covered",
                            side_effect=AssertionError("accepted supersession re-proved"),
                        ),
                    ):
                        changed = source_observations.preserve_selection(
                            "SOURCE",
                            old_serving_ownership=binding[0],
                            accepted_identity_changes=binding[1],
                            current_receipt=receipt,
                            current_documents=documents,
                            preserves_pricing=True,
                            accepted_choices=binding[3],
                        )
                    self.assertEqual(
                        {state},
                        {
                            s["observation_selection"]
                            for entry in changed["current_month"]["adjustments"]
                            for s in entry["segments"]
                        },
                    )
                    self.assertEqual(
                        {("ACCEPTED",)},
                        {
                            tuple(s["observation_tickers"])
                            for entry in changed["current_month"]["adjustments"]
                            for s in entry["segments"]
                        },
                    )

    def test_nested_offline_grid_carries_old_choice_and_covers_new_duplicate_without_rows(self):
        from copy import deepcopy

        from libs.alpaca_lineage_build.source_work import accepted_donor_grids

        donor, binding = self.case("selected", "old")
        nested = {
            "decision_generation": "old-approved",
            "history_validation": {
                "inputs": {"DONOR": {"pricing_generation": "generation", "views": binding[0]["views"]}},
                "prefix_records": [
                    {
                        "public_id": "old",
                        "missing_sessions": [],
                        "price_reasons": [],
                        "coverage": {"meaning": "approved_stored_history"},
                    }
                ],
            },
        }
        current = {
            "decision_generation": "current-offline",
            "history_validation": {"inputs": {}, "prefix_records": []},
            "offline_ownership": {"checkpoints": [nested]},
        }
        control = SimpleNamespace(
            symbol="DONOR", source_sha256="generation", previous={"serving_ownership": binding[0]}
        )
        cache = {"donor_checkpoints": {"selected": current}}
        complete = accepted_donor_grids(control, cache)
        self.assertEqual({"old"}, complete)
        with patch.object(
            source_observations,
            "complete_donor_ids",
            side_effect=AssertionError("completed grid decision repeated"),
        ):
            self.assertIs(complete, accepted_donor_grids(control, cache))
        other = deepcopy(donor)
        other["public_common_continuation_ownership"] = {"first": "2024-01-02", "last": "2024-01-03"}
        unknown = RetainedChoices(
            "generation",
            "receipt",
            MappingProxyType(
                {
                    view: (ChoiceRun(1, 2, None, "unassigned", (), "2024-01-02", "2024-01-03"),)
                    for view in ADJUSTMENTS
                }
            ),
        )
        with (
            patch.object(
                source_observations,
                "select_observations",
                side_effect=AssertionError("accepted donor contest repeated"),
            ),
            patch.object(
                source_observations, "counterpart", side_effect=AssertionError("accepted donor payload opened")
            ),
        ):
            result, changes, scopes = source_observations.reconcile(
                None,
                {"DONOR": object(), "NEW": object()},
                {"DONOR": donor, "NEW": other},
                {},
                {},
                {},
                retained={
                    "DONOR": (binding[0], binding[1], complete, binding[3]),
                    "NEW": (binding[0], binding[1], set(), unknown),
                },
            )
        self.assertFalse(changes)
        self.assertFalse(scopes)
        self.assertEqual(
            {"selected"}, {s["observation_selection"] for s in result["DONOR"]["correlated"]["segments"]}
        )
        self.assertEqual(
            {"superseded"}, {s["observation_selection"] for s in result["NEW"]["correlated"]["segments"]}
        )

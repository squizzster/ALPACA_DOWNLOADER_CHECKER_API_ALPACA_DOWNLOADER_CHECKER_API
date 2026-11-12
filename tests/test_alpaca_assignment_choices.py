"""Published numerical choices remain distinct from row ownership and serving."""

import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from libs.alpaca_lineage_build import assignment_handoff, storage
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class AssignmentChoiceTests(NativeCase):
    def receipt(self):
        index = storage.read_index(self.root)
        return storage.read_receipt(self.root, "TEST", index)

    def mixed_states(self, receipt, boundaries):
        states = ("selected", "superseded", "conflict", "incomplete", "unassigned", None)
        for document in boundaries.values():
            shared = None
            for view in document["adjustments"]:
                if shared is None:
                    shared = []
                    if view["rows"]:
                        template = view["segments"][0]
                        for number, state in enumerate(states, 1):
                            last = view["rows"] if number == len(states) else number
                            segment = {
                                **deepcopy(template),
                                "row_start": number,
                                "row_end": last,
                                "row_count": last - number + 1,
                                "from": self.bars[number - 1]["t"][:10],
                                "to": self.bars[last - 1]["t"][:10],
                                "first_timestamp": self.bars[number - 1]["t"],
                                "last_timestamp": self.bars[last - 1]["t"],
                                "observation_tickers": ["TEST", "TEST.OLD"],
                            }
                            if state == "unassigned":
                                segment["branch"] = "uncorrelated"
                                segment.pop("security_identity", None)
                                segment.pop("observation_selection", None)
                                segment.pop("historical_symbol", None)
                                segment.pop("observation_tickers", None)
                            elif state is None:
                                segment.pop("observation_selection", None)
                            else:
                                segment["observation_selection"] = state
                            shared.append(segment)
                view["segments"] = shared

    def test_publication_records_each_state_owner_labels_and_exact_bounds(self):
        self.publish_change(self.mixed_states)
        receipt = self.receipt()
        handoff = assignment_handoff.selected(receipt, "current_month")
        self.assertEqual(handoff["generation"], receipt["boundaries"]["current_month"]["sha256"])
        self.assertEqual(handoff["observation_choices_version"], 1)
        choices = handoff["observation_choices"]
        self.assertEqual(choices["views"], dict.fromkeys(ADJUSTMENTS, 0))
        self.assertEqual(len(choices["partitions"]), 1)
        part = choices["partitions"][0]
        self.assertEqual(part["rows"], len(self.bars))
        runs = part["runs"]
        self.assertEqual(
            [run["state"] for run in runs],
            ["selected", "superseded", "conflict", "incomplete", "unassigned", None],
        )
        owner = runs[0]["owner"]
        self.assertTrue(owner)
        self.assertEqual([run["owner"] for run in runs], [owner, owner, owner, owner, None, owner])
        self.assertEqual(
            [run["tickers"] for run in runs], [("TEST", "TEST.OLD")] * 4 + [(), ("TEST", "TEST.OLD")]
        )
        self.assertEqual([run["historical_symbol"] for run in runs], ["TEST"] * 4 + [None, "TEST"])
        self.assertEqual([run["row_start"] for run in runs], [1, 2, 3, 4, 5, 6])
        self.assertEqual([run["row_end"] for run in runs], [1, 2, 3, 4, 5, len(self.bars)])
        self.assertEqual([run["row_count"] for run in runs], [1, 1, 1, 1, 1, len(self.bars) - 5])
        for run in runs:
            self.assertEqual(run["from"], self.bars[run["row_start"] - 1]["t"][:10])
            self.assertEqual(run["to"], self.bars[run["row_end"] - 1]["t"][:10])
            self.assertEqual(run["first_timestamp"], self.bars[run["row_start"] - 1]["t"])
            self.assertEqual(run["last_timestamp"], self.bars[run["row_end"] - 1]["t"])

    def test_current_year_choices_include_the_active_overlap_omitted_from_serving_ownership(self):
        receipt = self.receipt()
        handoff = assignment_handoff.selected(receipt, "current_year")
        self.assertEqual(handoff["ownership"]["raw"]["rows"], 0)
        choices = handoff["observation_choices"]
        part = choices["partitions"][choices["views"]["raw"]]
        self.assertEqual(part["rows"], len(self.bars))
        self.assertEqual(part["runs"][0]["row_start"], 1)
        self.assertEqual(part["runs"][-1]["row_end"], len(self.bars))

    def test_physical_aliases_cross_projection_once_and_share_serialized_choice_runs(self):
        digest = assignment_handoff.document_digest
        seen = set()
        physical = set()

        def mutate(receipt, boundaries):
            self.mixed_states(receipt, boundaries)
            physical.update(
                id(document["adjustments"][0]["segments"])
                for document in boundaries.values()
                if document["adjustments"][0]["rows"]
            )

        def once(value):
            if id(value) in physical:
                self.assertNotIn(id(value), seen, "physical assignment alias traversed again")
                seen.add(id(value))
            return digest(value)

        with patch.object(assignment_handoff, "document_digest", side_effect=once):
            self.publish_change(mutate)
        self.assertEqual(seen, physical)
        for handoff in self.receipt()["assignment_handoffs"].values():
            choices = handoff["observation_choices"]
            self.assertEqual(len(choices["partitions"]), 1)
            self.assertEqual(set(choices["views"].values()), {0})

    def test_legacy_absence_is_not_upgraded_or_inferred_from_ownership(self):
        def legacy(receipt, boundaries):
            for handoff in receipt["assignment_handoffs"].values():
                handoff.pop("observation_choices_version")
                handoff.pop("observation_choices")
            boundaries.clear()

        self.publish_change(legacy)
        receipt = self.receipt()
        with (
            patch.object(assignment_handoff, "produce", side_effect=AssertionError("implicit choice upgrade")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("legacy body reopened")),
            patch.object(
                storage, "read_assignment_projection", side_effect=AssertionError("projection reopened")
            ),
        ):
            for kind in receipt["boundaries"]:
                selected = assignment_handoff.selected(receipt, kind)
                self.assertNotIn("observation_choices", selected)
                self.assertNotIn("observation_choices_version", selected)

    def test_declared_incomplete_capability_fails_before_payload_access(self):
        receipt = json.loads(json.dumps(self.receipt()))
        index = storage.read_index(self.root)
        changes = (
            lambda part: part.pop("observation_choices"),
            lambda part: part.pop("observation_choices_version"),
            lambda part: part.update(observation_choices_version=2),
            lambda part: part["observation_choices"].pop("partitions"),
            lambda part: part["observation_choices"]["views"].pop("raw"),
            lambda part: part["observation_choices"]["partitions"][0].pop("runs"),
            lambda part: part["observation_choices"]["partitions"][0]["runs"][0].pop("state"),
            lambda part: part["observation_choices"]["partitions"][0].update(runs=[{}]),
        )
        with (
            patch.object(Path, "read_bytes", side_effect=AssertionError("payload opened")),
            patch.object(assignment_handoff, "produce", side_effect=AssertionError("choice reconstructed")),
            patch.object(assignment_handoff, "document_digest", side_effect=AssertionError("choice rehashed")),
        ):
            for change in changes:
                selected = deepcopy(receipt)
                change(selected["assignment_handoffs"]["current_month"])
                with self.subTest(change=change), self.assertRaisesRegex(LineageError, "observation-choice"):
                    storage.read_receipt(self.root, "TEST", index, selected_loader=lambda *args: selected)

    def test_one_ingress_acceptance_survives_cache_copies_and_later_consumers(self):
        from libs.alpaca_lineage_build import recovery_handoffs

        index = storage.read_index(self.root)
        cache = storage.ReceiptCache(self.root, index)
        with patch.object(assignment_handoff, "_valid_run", wraps=assignment_handoff._valid_run) as validate:
            receipt = cache.read("TEST")
        self.assertEqual(validate.call_count, 2)  # One full-year and one month partition; views share both.
        choices = receipt["assignment_handoffs"]["current_month"]["observation_choices"]
        with self.assertRaises(TypeError):
            choices["partitions"][0]["runs"][0]["state"] = "superseded"
        with self.assertRaises(TypeError):
            choices.generation = "another-generation"
        with (
            patch.object(assignment_handoff, "_valid_run", side_effect=AssertionError("schema checked again")),
            patch.object(assignment_handoff, "produce", side_effect=AssertionError("choice produced again")),
            patch.object(assignment_handoff, "document_digest", side_effect=AssertionError("choice rehashed")),
            patch.object(Path, "read_bytes", side_effect=AssertionError("accepted boundary reopened")),
            recovery_handoffs.operation(self.store),
        ):
            recovery_handoffs.remember(
                self.store, "receipt", ("TEST", index["symbols"]["TEST"]["sha256"]), receipt
            )
            carried = storage.read_receipt(self.root, "TEST", index)
            for selected in (cache.read("TEST"), deepcopy(receipt), carried):
                for kind in receipt["boundaries"]:
                    self.assertIs(
                        assignment_handoff.selected(selected, kind)["observation_choices"],
                        receipt["assignment_handoffs"][kind]["observation_choices"],
                    )

    def test_interrupted_publication_installs_frozen_choices_without_producing_again(self):
        class Interrupted(BaseException):
            pass

        with (
            patch.object(storage, "finish_publication", side_effect=Interrupted),
            self.assertRaises(Interrupted),
        ):
            self.publish_change(self.mixed_states)
        with (
            patch.object(assignment_handoff, "produce", side_effect=AssertionError("choice produced again")),
            patch.object(assignment_handoff, "_choice", side_effect=AssertionError("choice projected again")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("assignment reopened")),
            patch.object(storage, "_recovery_inputs", wraps=storage._recovery_inputs) as select_generations,
            patch("libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("rules replayed")),
            patch(
                "libs.alpaca_pricing_download.storage.read_boundary", side_effect=AssertionError("prices read")
            ),
        ):
            self.assertTrue(storage.recover(self.store, self.root))
        self.assertEqual(select_generations.call_count, 1)
        part = self.receipt()["assignment_handoffs"]["current_month"]["observation_choices"]["partitions"][0]
        self.assertEqual(
            [run["state"] for run in part["runs"]],
            ["selected", "superseded", "conflict", "incomplete", "unassigned", None],
        )

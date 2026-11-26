"""The newly selected winner is lent to serving exactly once for changed complete bundles."""

import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock

from libs.alpaca_lineage_build.candidate_numerical import capture
from libs.alpaca_lineage_build.source_choice_handoff import accept
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp

DAYS = ("2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11")
OWNER = "a" * 64
SOURCES = ("ALDER", "WILLOW")


def history(winners, labels, *, participants=SOURCES):
    semantic, observation = {}, {}
    for source in participants:
        for adjustment in ADJUSTMENTS:
            for day, winner, label in zip(DAYS, winners, labels, strict=True):
                key = source, adjustment, day + "T05:00:00Z"
                semantic[key] = {
                    "branch": "correlated",
                    "security_identity": {"security_id": OWNER},
                    "historical_symbol": label,
                }
                observation[key] = {
                    "observation_selection": "selected" if source == winner else "superseded",
                    "observation_tickers": [label],
                }
    return {"semantic": semantic, "observation": observation}


class SelectedRows:
    def __init__(self, allowed):
        self.allowed, self.calls = allowed, []

    def __getitem__(self, index):
        if index not in self.allowed:
            raise AssertionError(f"unnecessary row assembly at {index}")
        self.calls.append(index)
        return self.allowed[index]


class CandidateNumericalTests(TestCase):
    def setUp(self):
        self.group = SimpleNamespace(
            receipts={"ALDER": "1" * 64, "WILLOW": "2" * 64}, pricing={"ALDER": "3" * 64, "WILLOW": "4" * 64}
        )
        self.before = history(["ALDER"] * 4, ["ALDER"] * 4)

    def rows(self, available):
        selected = SelectedRows(available)
        return selected, {"WILLOW": ({view: (selected, DAYS) for view in ADJUSTMENTS}, set())}

    def test_changed_label_and_two_changed_numerical_dates_lend_only_the_one_genuinely_new_bundle(self):
        after = history(["ALDER", "ALDER", "WILLOW", "WILLOW"], ["FORMER"] * 4)
        changed = DAYS[2:]
        # Original selector decided equality for one changed date, including all eight views.
        equal = {(OWNER, timestamp(changed[0] + "T05:00:00Z")): True}
        row = (DAYS[3] + "T05:00:00Z", "23", "23", "23", "23", "804", "23", "4")
        held, indexes = self.rows({3: row})
        revision, bundles = capture(self.before, after, equal, indexes, self.group)
        self.assertEqual((revision["version"], revision["sessions"][OWNER]), (1, list(changed)))
        self.assertEqual(revision["numeric_reuse_sessions"][OWNER], [changed[0]])
        self.assertEqual(revision["selected_sources"][OWNER], dict.fromkeys(changed, "WILLOW"))
        self.assertEqual(revision["source_membership"][OWNER], dict.fromkeys(ADJUSTMENTS, ["ALDER", "WILLOW"]))
        self.assertEqual(
            revision["identity_episodes"][OWNER],
            [{"symbol": "FORMER", "start_inclusive": DAYS[0], "end_exclusive": "2024-01-12"}],
        )
        self.assertEqual(revision["previous_receipt_generations"], self.group.receipts)
        self.assertEqual(revision["pricing_generations"], self.group.pricing)
        self.assertEqual(held.calls, [3])
        accepted = accept(bundles, revision)
        self.assertEqual(set(accepted.observations[OWNER]), {changed[1]})
        self.assertIs(
            accepted.observations[OWNER][changed[1]]["all"]["row"],
            accepted.observations[OWNER][changed[1]]["raw"]["row"],
        )
        self.assertEqual(len(bundles["identities"][OWNER][changed[1]]["partitions"]), 1)
        transported = accept(json.loads(json.dumps(bundles)), revision)
        self.assertEqual(set(transported.observations[OWNER]), {changed[1]})

    def test_multi_source_label_only_changes_bind_episodes_without_numerical_scopes_or_rows(self):
        winners = ["ALDER", "ALDER", "WILLOW", "WILLOW"]
        original = history(winners, ["ALDER"] * 4)
        projected = history(winners, ["ORCHARD", "ORCHARD", "ALDER", "ALDER"])
        labels = {}
        self.assertEqual(capture(original, projected, {}, {}, self.group, label_output=labels), (None, None))
        self.assertEqual(
            labels,
            {
                source: {
                    OWNER: [{"symbol": "ORCHARD", "start_inclusive": DAYS[0], "end_exclusive": "2024-01-10"}]
                }
                for source in SOURCES
            },
        )
        sole_output = {}
        sole = history(["ALDER"] * 4, ["ORCHARD"] * 4)
        self.assertEqual(capture(self.before, sole, {}, {}, self.group, label_output=sole_output), (None, None))
        self.assertEqual(sole_output, {})

    def test_same_label_full_equal_source_replacement_needs_no_value_from_any_row(self):
        equal = {(OWNER, timestamp(day + "T05:00:00Z")): True for day in DAYS}
        after = history(["WILLOW"] * 4, ["ALDER"] * 4)
        revision, bundles = capture(
            self.before, after, equal, Mock(side_effect=AssertionError("read prices")), self.group
        )
        self.assertEqual((revision["version"], revision["labels_unchanged"]), (2, True))
        self.assertEqual(revision["sessions"], {OWNER: list(DAYS)})
        self.assertEqual(revision["numeric_reuse_sessions"], revision["sessions"])
        self.assertIsNone(bundles)

    def test_partial_or_competing_winners_cannot_claim_complete_grid_and_equal_foreign_pair_cannot_donate(self):
        after = history(["WILLOW"] * 4, ["WILLOW"] * 4)
        key = "ALDER", "all", DAYS[0] + "T05:00:00Z"
        after["observation"][key]["observation_selection"] = "selected"
        self.assertEqual(capture(self.before, after, {}, {}, self.group), (None, None))
        after = history(["WILLOW"] * 4, ["WILLOW"] * 4)
        key = "WILLOW", "all", DAYS[0] + "T05:00:00Z"
        after["observation"][key]["observation_selection"] = "incomplete"
        self.assertEqual(capture(self.before, after, {}, {}, self.group), (None, None))
        after = history(["WILLOW"] * 4, ["ALDER"] * 4, participants=("WILLOW", "COTTON"))
        group = SimpleNamespace(
            receipts={**self.group.receipts, "COTTON": "5" * 64},
            pricing={**self.group.pricing, "COTTON": "6" * 64},
        )
        equal = {(OWNER, timestamp(day + "T05:00:00Z")): True for day in DAYS}
        held, indexes = self.rows(
            {i: (day + "T05:00:00Z", "23", "23", "23", "23", "804", "23", "4") for i, day in enumerate(DAYS)}
        )
        revision, bundles = capture(self.before, after, equal, indexes, group)
        self.assertEqual(revision["numeric_reuse_sessions"], {OWNER: []})
        self.assertEqual(set(bundles.observations[OWNER]), set(DAYS))
        self.assertEqual(set(held.calls), set(range(4)))

"""A membership extension opens only its genuinely new numerical contest."""

import copy
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_component_refinement as fixtures
from test_alpaca_observation_authority import views_of
from test_alpaca_pricing_download import bar

from libs.alpaca_end_of_day_security_correlation.component_refinement import refine
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_debug.paired import _retained
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class SelectedRows:
    def __init__(self, rows):
        self.rows, self.reads = rows, []

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, index):
        if isinstance(index, slice):
            indices = range(*index.indices(len(self.rows)))
            return tuple(self[number] for number in indices)
        if index >= 4:
            raise AssertionError("unrelated retained numerical row read")
        self.reads.append(index)
        return self.rows[index]

    def __iter__(self):
        raise AssertionError("retained history traversed")


class RefinedChoiceTests(TestCase):
    def test_equal_and_different_prices_select_only_newly_joined_sessions(self):
        for old_value in ("10", "20"):
            with self.subTest(old_value=old_value):
                reports, evidence, selected = fixtures.fixture()
                tail = copy.deepcopy(reports["NEW"]["correlated"]["segments"])
                for segment in tail:
                    segment.update(
                        row_start=5,
                        row_end=5,
                        first_session_date="2026-11-10",
                        last_session_date="2026-11-10",
                        historical_symbol="NEW",
                        observation_tickers=["NEW"],
                    )
                reports["NEW"]["correlated"]["segments"].extend(tail)
                reports["OLD"]["observation_routes"] = {
                    "old": ("2023-06-20T04:00:00Z", {"end_date_exclusive": "2023-06-21"})
                }
                reports["NEW"]["observation_routes"] = {
                    "current": ("2026-11-10T05:00:00Z", {"end_date_exclusive": None})
                }
                inputs, readers = {}, {}
                days = ("2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20")
                for source in reports:
                    data = views_of(
                        [bar(day, old_value if source == "OLD" else "10") for day in days]
                        + ([bar("2026-11-10", "10")] if source == "NEW" else [])
                    )
                    rows = SelectedRows(data["raw"])
                    readers[source] = rows
                    inputs[source] = SimpleNamespace(
                        views=dict.fromkeys(ADJUSTMENTS, rows),
                        source_sha256=selected["pricing_generations"][source],
                        source={
                            "activity_consistent": True,
                            "history_start": "2016-01-01",
                            "latest_request": {"asof": "2026-11-12", "end_exclusive": "2026-11-12T00:00:00Z"},
                        },
                    )
                candidate, trace = refine(reports, evidence, selected)
                for source, report in candidate.items():
                    candidate[source] = {
                        **report,
                        "retained_observation_routes": reports[source]["observation_routes"],
                    }
                with patch(
                    "libs.alpaca_pricing_download.activity.validate_activity",
                    side_effect=AssertionError("accepted activity replayed"),
                ):
                    equality = {}
                    result, corrections = select_observations(
                        inputs,
                        candidate,
                        retained_choices=_retained(reports, trace["changes"], selected),
                        equality_outcomes=equality,
                    )
                self.assertEqual(len(equality), 4 if old_value == "10" else 0)
                with patch(
                    "libs.alpaca_lineage_build.observations._select_new_observations",
                    side_effect=AssertionError("completed equality recalculated"),
                ):
                    select_observations(
                        inputs,
                        result,
                        retained_choices=_retained(result, [], selected),
                        equality_outcomes=equality,
                    )
                self.assertFalse(corrections)
                self.assertTrue(
                    all(
                        segment["observation_selection"] == "superseded"
                        for segment in result["OLD"]["correlated"]["segments"]
                    )
                )
                self.assertTrue(
                    all(
                        segment["observation_selection"] == "selected"
                        for segment in result["NEW"]["correlated"]["segments"]
                    )
                )
                self.assertTrue(all(set(reader.reads) <= {0, 1, 2, 3} for reader in readers.values()))
                self.assertTrue(
                    all(
                        segment["security_identity"]["security_id"] == "current"
                        for segment in result["OLD"]["correlated"]["segments"]
                    )
                )

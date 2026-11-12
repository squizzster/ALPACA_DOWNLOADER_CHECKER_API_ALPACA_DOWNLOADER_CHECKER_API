"""Accepted choice carry excludes historical prices and exact invalidated dates."""

from types import MappingProxyType, SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_observation_authority as observation_fixture
from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_observation_authority import views_of

from libs.alpaca_lineage_build import observations
from libs.alpaca_lineage_build.observation_carry import ChoiceRun, RetainedChoices, project_choices
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class ClosedRows:
    def __init__(self, count):
        self.count = count

    def __len__(self):
        return self.count

    def __getitem__(self, index):
        raise AssertionError("frozen historical row accessed")

    def __iter__(self):
        raise AssertionError("frozen historical rows traversed")


class ChoiceConsumptionTests(TestCase):
    def test_all_frozen_states_and_labels_bypass_every_numerical_path(self):
        for state in ("selected", "superseded", "conflict", "incomplete"):
            with self.subTest(state=state):
                inputs, reports = observation_fixture.ObservationSelectionTests().fixture()
                retained = {}
                for name, report in reports.items():
                    by_view = {view: [] for view in ADJUSTMENTS}
                    for segment in report["correlated"]["segments"]:
                        by_view[segment["adjustment"]].append(
                            ChoiceRun(
                                segment["row_start"],
                                segment["row_end"],
                                "id",
                                state,
                                ("ACCEPTED", "AMBIGUOUS"),
                                "2026-10-01",
                                "2026-10-13",
                            )
                        )
                    retained[name] = RetainedChoices(inputs[name].source_sha256, name, MappingProxyType(by_view))
                    inputs[name].views = {
                        view: ClosedRows(len(rows)) for view, rows in inputs[name].views.items()
                    }
                with (
                    patch.object(
                        observations, "_select_new_observations", side_effect=AssertionError("choice repeated")
                    ),
                    patch.object(
                        observations, "_own_interval", side_effect=AssertionError("authority repeated")
                    ),
                    patch(
                        "libs.alpaca_pricing_download.activity.validate_activity",
                        side_effect=AssertionError("activity repeated"),
                    ),
                ):
                    selected, corrections = observations.select_observations(
                        inputs, reports, retained_choices=retained
                    )
                self.assertFalse(corrections)
                for report in selected.values():
                    for segment in report["correlated"]["segments"]:
                        self.assertEqual(segment["observation_selection"], state)
                        self.assertEqual(segment["observation_tickers"], ["ACCEPTED", "AMBIGUOUS"])

    def test_sparse_new_rival_reopens_only_an_exact_present_session(self):
        class NoIteration(tuple):
            def __iter__(self):
                raise AssertionError("retained session grid traversed")

        for day, expected_old_ranges, offset_spelling in (
            ("2026-10-02", [], False),
            ("2026-10-05", [(2, 2)], False),
            ("2026-10-05", [(2, 2)], True),
        ):
            with self.subTest(day=day, offset_spelling=offset_spelling):
                old_bars = [
                    row
                    for row in valid_bars("2026-10-01", "2026-10-08")
                    if row["t"][:10] in {"2026-10-01", "2026-10-05", "2026-10-07"}
                ]
                fresh = [row for row in valid_bars("2026-10-01", "2026-10-08") if row["t"][:10] == day]
                if offset_spelling:
                    fresh[0] = {**fresh[0], "t": "2026-10-05T00:00:00-04:00"}
                inputs, reports = {}, {}
                for name, bars in (("OLD", old_bars), ("NEW", fresh)):
                    views = views_of(bars)
                    shared = NoIteration(views["raw"]) if name == "OLD" else views["raw"]
                    inputs[name] = SimpleNamespace(
                        views=dict.fromkeys(ADJUSTMENTS, shared),
                        source_sha256=name,
                        source={"activity_consistent": True, "latest_request": {"asof": "2026-10-08"}},
                    )
                    reports[name] = {
                        "retained_observation_routes": {"id": ("2026-10-07T04:00:00Z", {})}
                        if name == "OLD"
                        else {},
                        "correlated": {
                            "segments": [
                                {
                                    "adjustment": view,
                                    "row_start": 1,
                                    "row_end": len(bars),
                                    "historical_symbol": name,
                                    "security_identity": {"security_id": "id"},
                                }
                                for view in ADJUSTMENTS
                            ]
                        },
                    }
                retained = {
                    "OLD": RetainedChoices(
                        "OLD",
                        "receipt",
                        MappingProxyType(
                            {
                                view: (ChoiceRun(1, 3, "id", "selected", ("OLD",), "2026-10-01", "2026-10-07"),)
                                for view in ADJUSTMENTS
                            }
                        ),
                    )
                }
                select = observations._select_new_observations
                inspected = []

                def only_active(inputs, active, **kwargs):
                    inspected.extend(
                        (segment["row_start"], segment["row_end"])
                        for segment in active["OLD"]["correlated"]["segments"]
                        if segment["adjustment"] == "raw"
                    )
                    return select(inputs, active, **kwargs)

                with patch.object(observations, "_select_new_observations", side_effect=only_active):
                    selected, corrections = observations.select_observations(
                        inputs, reports, retained_choices=retained
                    )
                self.assertFalse(corrections)
                self.assertEqual(inspected, expected_old_ranges)
                raw = [
                    segment
                    for segment in selected["OLD"]["correlated"]["segments"]
                    if segment["adjustment"] == "raw"
                ]
                self.assertTrue(all(segment["observation_selection"] == "selected" for segment in raw))
                self.assertEqual(
                    selected["NEW"]["correlated"]["segments"][0]["observation_selection"],
                    "superseded" if expected_old_ranges else "selected",
                )

    def test_unknown_choice_does_not_freeze_complementary_legacy_views(self):
        inputs, reports = observation_fixture.ObservationSelectionTests().fixture()
        retained = {}
        for name, report in reports.items():
            names = ADJUSTMENTS[:4] if name == "OLD" else ADJUSTMENTS[4:]
            report["correlated"]["segments"] = [
                segment for segment in report["correlated"]["segments"] if segment["adjustment"] in names
            ]
            by_view = {view: [] for view in ADJUSTMENTS}
            for segment in report["correlated"]["segments"]:
                by_view[segment["adjustment"]].append(
                    ChoiceRun(
                        segment["row_start"], segment["row_end"], "id", None, (name,), "2026-10-01", "2026-10-13"
                    )
                )
            retained[name] = RetainedChoices(inputs[name].source_sha256, name, MappingProxyType(by_view))
        selected, corrections = observations.select_observations(inputs, reports, retained_choices=retained)
        self.assertFalse(corrections)
        self.assertTrue(
            all(
                segment["observation_selection"] == "conflict"
                for segment in selected["OLD"]["correlated"]["segments"]
            )
        )


class ChoiceProjectionTests(NativeCase):
    def held(self, count, *, origin=None, dates=None):
        receipt = self.catalogue_receipt
        return SimpleNamespace(
            source_sha256=receipt["source_sha256"],
            materialization_scope="complete_history" if origin is None else "retained_suffix",
            previous_assignment_summary=None
            if origin is None
            else {name: {"rows": origin} for name in ADJUSTMENTS},
            views={name: ClosedRows(count) for name in ADJUSTMENTS},
            reprojected_sessions=None if dates is None else dict.fromkeys(ADJUSTMENTS, dates),
        )

    def setUp(self):
        super().setUp()
        from libs.alpaca_lineage_build import storage

        self.catalogue_receipt = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))

    def project(self, view, **kwargs):
        return project_choices(
            self.catalogue_receipt,
            view,
            receipt_generation="accepted-receipt",
            legacy_boundary=lambda kind: self.fail("declared capability reconstructed"),
            **kwargs,
        )

    def test_full_history_uses_accepted_offsets_without_year_hot_month_duplication(self):
        carry = self.project(self.held(len(self.bars)))
        for runs in carry.runs.values():
            self.assertEqual([(run.row_start, run.row_end) for run in runs], [(1, len(self.bars))])

    def test_suffix_positions_are_view_local_without_reading_frozen_rows(self):
        carry = self.project(self.held(len(self.bars) - 3, origin=3))
        for runs in carry.runs.values():
            self.assertEqual([(run.row_start, run.row_end) for run in runs], [(1, len(self.bars) - 3)])

    def test_ordinary_suffix_does_not_open_missing_legacy_choice_metadata(self):
        previous = {**self.catalogue_receipt, "assignment_handoffs": {}}
        carry = project_choices(
            previous,
            self.held(1, origin=len(self.bars)),
            receipt_generation="legacy",
            legacy_boundary=lambda kind: self.fail("unneeded old boundary opened"),
        )
        self.assertTrue(all(not runs for runs in carry.runs.values()))

    def test_selected_correction_opens_only_intersecting_legacy_boundary(self):
        from libs.alpaca_lineage_build import storage

        document = storage.read_boundaries(self.root, self.catalogue_receipt, kinds={"current_month"})[
            "current_month"
        ]
        previous = {
            **self.catalogue_receipt,
            "assignment_handoffs": {
                kind: {
                    key: value
                    for key, value in part.items()
                    if key not in {"observation_choices", "observation_choices_version"}
                }
                for kind, part in self.catalogue_receipt["assignment_handoffs"].items()
            },
        }
        reads = []

        def legacy(kind):
            self.assertEqual(kind, "current_month")
            reads.append(kind)
            return document

        carry = project_choices(
            previous,
            self.held(1, origin=len(self.bars) - 1),
            receipt_generation="legacy",
            legacy_boundary=legacy,
        )
        self.assertEqual(reads, ["current_month"])
        self.assertTrue(
            all([(run.row_start, run.row_end) for run in runs] == [(1, 1)] for runs in carry.runs.values())
        )

    def test_exact_correction_scope_keeps_later_accepted_competitor_sessions(self):
        count = len(self.bars)
        held = self.held(count)
        day = self.bars[-2]["t"][:10]
        original = self.held(2, origin=count - 2, dates=tuple(row["t"][:10] for row in self.bars[-2:]))
        owner = self.catalogue_receipt["assignment_handoffs"]["current_month"]["observation_choices"][
            "partitions"
        ][0]["runs"][0]["owner"]
        carry = self.project(held, original_view=original, invalidated={day: {owner}})
        for runs in carry.runs.values():
            self.assertEqual([(run.row_start, run.row_end) for run in runs], [(1, count - 2), (count, count)])

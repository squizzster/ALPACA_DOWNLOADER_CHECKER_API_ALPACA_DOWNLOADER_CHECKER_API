"""Bounded numerical repair keeps accepted ownership and unrelated rows closed."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.numerical_completion import complete
from libs.alpaca_lineage_build.recovery_handoffs import append_partitions, operation, remember_partition
from libs.alpaca_lineage_build.source_observations import apply_selection, counterpart_positions
from libs.alpaca_pricing_download.contract import ADJUSTMENTS

IDENTITY = "accepted-owner"


def boundary_loader(views):
    def load(root, source, kind, *, selected, documents):
        documents[source["boundaries"][kind]["manifest"]] = {"boundary": kind, "symbol": source["symbol"]}
        return views

    return load


def fixture(*, equal=True, retired=False, ambiguous=False):
    inputs, reports, positions, dates = {}, {}, {}, {}
    for source in ("OLD", "NEW"):
        rows = tuple(
            (
                f"2016-01-{day:02d}T05:00:00Z",
                "10",
                "11",
                "9",
                "10" if equal or source == "OLD" else "10.5",
                "100",
                "4",
                "10",
            )
            for day in range(4, 9)
        )
        manifest = {
            "symbol": source,
            "activity_consistent": True,
            "anchor_month": "2026-11",
            "history_start": "2016-01-01T00:00:00Z",
            "latest_request": {"asof": "2026-11-13", "end_exclusive": "2026-11-13T00:00:00Z"},
            "boundaries": {
                kind: {"sha256": kind, "manifest": kind + ".manifest.json"}
                for kind in ("completed_years", "current_year", "current_month")
            },
        }
        inputs[source] = SimpleNamespace(
            source=manifest, source_sha256=source.lower() * 16, views=dict.fromkeys(ADJUSTMENTS, rows)
        )
        reports[source] = {
            "correlated": {
                "row_count": len(rows) * len(ADJUSTMENTS),
                "segments": [
                    {
                        "adjustment": adjustment,
                        "branch": "correlated",
                        "row_start": 1,
                        "row_end": 5,
                        "row_count": 5,
                        "from": "2016-01-04",
                        "to": "2016-01-08",
                        "first_timestamp": rows[0][0],
                        "last_timestamp": rows[-1][0],
                        "historical_symbol": "BORROWED" if ambiguous else "OLD",
                        "security_identity": {"security_id": IDENTITY},
                        "correlation_rule": {"name": "accepted-owner-rule"},
                        "observation_selection": "selected",
                        "observation_tickers": ["OLD"],
                    }
                    for adjustment in ADJUSTMENTS
                ],
            },
            "retained_observation_routes": {}
            if ambiguous
            else {
                IDENTITY: (
                    "2023-10-04T04:00:00Z" if source == "NEW" else "2021-04-21T04:00:00Z",
                    {"end_date_exclusive": "2023-10-05" if retired else None},
                )
            },
        }
        positions[source] = {adjustment: list(range(1, 6)) for adjustment in ADJUSTMENTS}
        dates[source] = {
            adjustment: {number: (row[0][:10], row[0]) for number, row in enumerate(rows, 1)}
            for adjustment in ADJUSTMENTS
        }
    return inputs, reports, positions, dates


class BoundedRows:
    def __init__(self, rows, allowed):
        self.rows, self.allowed, self.reads = rows, set(allowed), []

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return tuple(self[number] for number in range(*index.indices(len(self.rows))))
        if index not in self.allowed:
            raise AssertionError(f"unselected numerical row {index} read")
        self.reads.append(index)
        return self.rows[index]

    def __iter__(self):
        raise AssertionError("whole accepted partition traversed")


class NumericalCompletionTests(TestCase):
    def test_equal_bundles_select_once_and_carry_completed_facts(self):
        inputs, reports, positions, dates = fixture()
        before = deepcopy(reports)
        with patch(
            "libs.alpaca_pricing_download.activity.validate_activity",
            side_effect=AssertionError("accepted activity guarantee replayed"),
        ):
            result = complete(inputs, reports, positions=positions, dates=dates)
        self.assertEqual(reports, before)
        self.assertEqual(result.scopes[IDENTITY], tuple(f"2016-01-{day:02d}" for day in range(4, 9)))
        self.assertEqual(set(result.selected_sources[IDENTITY].values()), {"NEW"})
        self.assertTrue(all(result.equality_outcomes[IDENTITY].values()))
        for adjustment in ADJUSTMENTS:
            self.assertEqual(
                {value["observation_selection"] for value in result.changes["OLD"][adjustment].values()},
                {"superseded"},
            )
            self.assertEqual(result.selected_bundles[IDENTITY]["2016-01-04"][adjustment]["source"], "NEW")
        with patch(
            "libs.alpaca_lineage_build.numerical_completion.select_observations",
            side_effect=AssertionError("completed choice replayed by its consumer"),
        ):
            changes = {**result.changes["OLD"], "dates": dates["OLD"]}
            document = {
                "adjustments": [
                    {"adjustment": adjustment, "segments": [reports["OLD"]["correlated"]["segments"][number]]}
                    for number, adjustment in enumerate(ADJUSTMENTS)
                ]
            }
            patched = apply_selection(document, changes, dict.fromkeys(ADJUSTMENTS, 0))
        self.assertTrue(
            all(
                segment["security_identity"]["security_id"] == IDENTITY
                and segment["correlation_rule"] == {"name": "accepted-owner-rule"}
                and segment["observation_tickers"] == ["OLD"]
                for entry in patched["adjustments"]
                for segment in entry["segments"]
            )
        )

    def test_differing_bundles_cannot_claim_equal_serving_history(self):
        inputs, reports, positions, dates = fixture(equal=False)
        result = complete(inputs, reports, positions=positions, dates=dates)
        self.assertEqual(set(result.selected_sources[IDENTITY].values()), {"NEW"})
        self.assertFalse(any(result.equality_outcomes[IDENTITY].values()))

    def test_researched_authority_cannot_enter_a_reopened_contest(self):
        inputs, reports, positions, dates = fixture()
        control = complete(inputs, reports, positions=positions, dates=dates)
        for segment in reports["OLD"]["correlated"]["segments"]:
            segment["observation_authority"] = {
                "market": "when_issued",
                "direct_market_source": True,
                "source_fact_id": "retained-researched-claim",
            }
        before = deepcopy(reports)
        result = complete(inputs, reports, positions=positions, dates=dates)
        self.assertEqual(result.selected_sources, control.selected_sources)
        self.assertEqual(result.changes, control.changes)
        self.assertEqual(result.equality_outcomes, control.equality_outcomes)
        self.assertEqual(set(result.selected_sources[IDENTITY].values()), {"NEW"})
        self.assertEqual(reports, before, "retained provenance was changed")
        self.assertTrue(
            all(
                "observation_authority" not in segment
                for report in result.reports.values()
                for segment in report["correlated"]["segments"]
            )
        )

    def test_mapping_correction_fails_before_any_publication_result(self):
        inputs, reports, positions, dates = fixture(equal=False, retired=True)
        with self.assertRaisesRegex(LineageError, "owning pricing mapping correction.*NEW"):
            complete(inputs, reports, positions=positions, dates=dates)

    def test_later_accepted_occupant_route_survives_a_clipped_mapping_contest(self):
        inputs, reports, positions, dates = fixture(equal=False, retired=True)
        reports["NEW"]["retained_observation_routes"]["later-owner"] = (
            "2026-11-12T05:00:00Z",
            {"start_date_inclusive": "2025-01-01", "end_date_exclusive": None},
        )
        before = deepcopy(reports)
        with patch(
            "libs.alpaca_pricing_download.storage.read_artifact",
            side_effect=AssertionError("unselected occupancy history read"),
        ):
            result = complete(inputs, reports, positions=positions, dates=dates)
        self.assertEqual(set(result.selected_sources[IDENTITY].values()), {"NEW"})
        self.assertFalse(any(result.equality_outcomes[IDENTITY].values()))
        self.assertEqual(reports, before)
        self.assertNotIn("later-owner", result.scopes)

    def test_earlier_occupant_route_cannot_approve_a_mapping_for_an_old_contest(self):
        inputs, reports, positions, dates = fixture(equal=False, retired=True)
        reports["NEW"]["retained_observation_routes"]["earlier-owner"] = (
            "2022-11-12T05:00:00Z",
            {"start_date_inclusive": "2021-01-01", "end_date_exclusive": "2022-11-13"},
        )
        with self.assertRaisesRegex(
            LineageError, "conflicting observations require an unambiguous mapping route"
        ):
            complete(inputs, reports, positions=positions, dates=dates)

    def test_no_complete_authority_stays_an_explicit_completion_requirement(self):
        inputs, reports, positions, dates = fixture(equal=False, ambiguous=True)
        with self.assertRaisesRegex(LineageError, "one complete selected source bundle"):
            complete(inputs, reports, positions=positions, dates=dates)

    def test_accepted_later_routes_survive_a_historical_slice(self):
        inputs, reports, positions, dates = fixture()
        for name in reports:
            inputs[name].views = dict.fromkeys(ADJUSTMENTS, inputs[name].views["raw"][:2])
            for segment in reports[name]["correlated"]["segments"]:
                segment.update(row_end=2, row_count=2)
            positions[name] = {adjustment: [1, 2] for adjustment in ADJUSTMENTS}
        result = complete(inputs, reports, positions=positions, dates=dates)
        self.assertEqual(set(result.selected_sources[IDENTITY].values()), {"NEW"})
        for report in reports.values():
            report["retained_observation_routes"] = {}
        result = complete(inputs, reports, positions=positions, dates=dates)
        self.assertEqual(set(result.selected_sources[IDENTITY].values()), {"OLD"})


class CounterpartPositionTests(TestCase):
    def prepare(self, first=2, last=3):
        inputs, reports, _, _ = fixture()
        value = inputs["NEW"]
        guarded = BoundedRows(value.views["raw"], {1, 2, 3})
        control = SimpleNamespace(
            symbol="NEW",
            source=value.source,
            source_sha256=value.source_sha256,
            previous={
                "serving_ownership": {
                    "offsets": {
                        adjustment: dict.fromkeys(("completed_years", "current_year", "current_month"), 0)
                        for adjustment in ADJUSTMENTS
                    }
                }
            },
        )
        intervals = [
            {
                "adjustment": adjustment,
                "boundary": "completed_years",
                "public_id": IDENTITY,
                "row_start": first,
                "row_end": last,
            }
            for adjustment in ADJUSTMENTS
        ]
        return control, reports["NEW"], intervals, guarded

    def test_position_scope_aliases_and_only_required_cut_endpoints(self):
        control, report, intervals, guarded = self.prepare()
        with patch(
            "libs.alpaca_lineage_build.source_observations.load_published_boundary",
            side_effect=boundary_loader(dict.fromkeys(ADJUSTMENTS, guarded)),
        ) as load:
            value, projected, positions, dates = counterpart_positions(
                Path("/unused"), control, report, intervals, {}
            )
        self.assertEqual(load.call_count, 1)
        self.assertEqual(load.call_args.args[2], "completed_years")
        self.assertEqual(guarded.reads, [1, 2, 3])
        self.assertTrue(all(value.views[adjustment] is value.views["raw"] for adjustment in ADJUSTMENTS))
        self.assertTrue(all(selected == [2, 3] for selected in positions.values()))
        self.assertTrue(all(set(selected) == {1, 2, 3, 4} for selected in dates.values()))
        self.assertIs(projected["retained_observation_routes"], report["retained_observation_routes"])
        changes = {adjustment: {} for adjustment in ADJUSTMENTS} | {"dates": dates}
        for adjustment in ADJUSTMENTS:
            for index in positions[adjustment]:
                day, stamp = dates[adjustment][index]
                changes[adjustment][index] = {
                    "observation_selection": "superseded",
                    "observation_tickers": ["OLD"],
                    "timestamp": stamp,
                    "day": day,
                }
        document = {
            "adjustments": [
                {"adjustment": adjustment, "segments": [report["correlated"]["segments"][number]]}
                for number, adjustment in enumerate(ADJUSTMENTS)
            ]
        }
        patched = apply_selection(document, changes, dict.fromkeys(ADJUSTMENTS, 0))
        for entry in patched["adjustments"]:
            head, chosen, tail = entry["segments"]
            self.assertEqual(
                (head["row_start"], head["row_end"], head["observation_selection"]), (1, 1, "selected")
            )
            self.assertEqual((chosen["row_start"], chosen["row_end"]), (2, 3))
            self.assertEqual(chosen["observation_selection"], "superseded")
            self.assertEqual(
                (tail["row_start"], tail["row_end"], tail["observation_selection"]), (4, 5, "selected")
            )
            self.assertEqual(tail["from"], "2016-01-07")
            self.assertEqual(tail["to"], "2016-01-08")

    def test_partition_cache_is_used_without_reopening_its_payloads(self):
        control, report, intervals, guarded = self.prepare()
        key = str(Path("/unused/end_of_day/pricing/NEW")), "completed_years", "completed_years"
        cache = {key: dict.fromkeys(ADJUSTMENTS, guarded)}
        with patch(
            "libs.alpaca_lineage_build.source_observations.load_published_boundary",
            side_effect=AssertionError("accepted selected partition reopened"),
        ):
            counterpart_positions(Path("/unused"), control, report, intervals, cache)

    def test_disjoint_position_ranges_keep_the_unselected_middle_fragment(self):
        control, report, intervals, guarded = self.prepare()
        intervals = [
            {**interval, "row_start": number, "row_end": number} for interval in intervals for number in (2, 4)
        ]
        with patch(
            "libs.alpaca_lineage_build.source_observations.load_published_boundary",
            side_effect=boundary_loader(dict.fromkeys(ADJUSTMENTS, guarded)),
        ):
            _, selected, positions, dates = counterpart_positions(
                Path("/unused"), control, report, intervals, {}
            )
        self.assertEqual(guarded.reads, [1, 3, 2])
        self.assertTrue(all(value == [2, 4] for value in positions.values()))
        self.assertTrue(all(set(value) == {1, 2, 3, 4, 5} for value in dates.values()))
        raw = [segment for segment in selected["correlated"]["segments"] if segment["adjustment"] == "raw"]
        self.assertEqual([(segment["row_start"], segment["row_end"]) for segment in raw], [(1, 1), (2, 2)])
        self.assertEqual([segment["first_session_date"] for segment in raw], ["2016-01-05", "2016-01-07"])

    def test_boundary_local_positions_keep_their_accepted_source_offsets(self):
        control, report, intervals, guarded = self.prepare()
        for offsets in control.previous["serving_ownership"]["offsets"].values():
            offsets["completed_years"] = 100
        for segment in report["correlated"]["segments"]:
            segment["row_start"] += 100
            segment["row_end"] += 100
        with patch(
            "libs.alpaca_lineage_build.source_observations.load_published_boundary",
            side_effect=boundary_loader(dict.fromkeys(ADJUSTMENTS, guarded)),
        ):
            _, _, positions, dates = counterpart_positions(Path("/unused"), control, report, intervals, {})
        self.assertTrue(all(value == [102, 103] for value in positions.values()))
        self.assertTrue(all(set(value) == {101, 102, 103, 104} for value in dates.values()))

    def test_operation_partition_handoff_survives_a_new_consumer_cache(self):
        control, report, intervals, _ = self.prepare()
        rows = fixture()[0]["NEW"].views["raw"]
        with operation(Path("/unused")):
            remember_partition(
                Path("/unused"), "NEW", "completed_years", "completed_years", dict.fromkeys(ADJUSTMENTS, rows)
            )
            with patch(
                "libs.alpaca_lineage_build.source_observations.load_published_boundary",
                side_effect=AssertionError("completed producer partition reopened by a new consumer"),
            ):
                value, _, _, _ = counterpart_positions(Path("/unused"), control, report, intervals, {})
        self.assertEqual(len(value.views["raw"]), 2)

    def test_materialized_partition_carries_its_already_read_document_into_append(self):
        control, report, intervals, _ = self.prepare()
        rows = fixture()[0]["NEW"].views["raw"]
        with operation(Path("/unused")):
            with patch(
                "libs.alpaca_lineage_build.source_observations.load_published_boundary",
                side_effect=boundary_loader(dict.fromkeys(ADJUSTMENTS, rows)),
            ) as load:
                counterpart_positions(Path("/unused"), control, report, intervals, {})
            with patch(
                "libs.alpaca_lineage_build.source_observations.load_published_boundary",
                side_effect=AssertionError("append reread the numerical producer's completed partition"),
            ):
                carried = append_partitions(Path("/unused"), {"NEW"})
        self.assertEqual(load.call_count, 1)
        value, document = next(iter(carried.values()))
        self.assertIs(value["raw"], rows)
        self.assertEqual(document, {"boundary": "completed_years", "symbol": "NEW"})

    def test_changed_partition_generation_cannot_reuse_the_old_handoff(self):
        control, report, intervals, _ = self.prepare()
        rows = fixture()[0]["NEW"].views["raw"]
        with operation(Path("/unused")):
            remember_partition(
                Path("/unused"), "NEW", "completed_years", "completed_years", dict.fromkeys(ADJUSTMENTS, rows)
            )
            control.source["boundaries"]["completed_years"]["sha256"] = "changed-boundary"
            with patch(
                "libs.alpaca_lineage_build.source_observations.load_published_boundary",
                side_effect=boundary_loader(dict.fromkeys(ADJUSTMENTS, rows)),
            ) as load:
                counterpart_positions(Path("/unused"), control, report, intervals, {})
        self.assertEqual(load.call_count, 1)

    def test_out_of_boundary_scope_fails_before_reading_any_row(self):
        control, report, intervals, guarded = self.prepare(first=2, last=9)
        with patch(
            "libs.alpaca_lineage_build.source_observations.load_published_boundary",
            side_effect=boundary_loader(dict.fromkeys(ADJUSTMENTS, guarded)),
        ):
            with self.assertRaisesRegex(LineageError, "exceed.*boundary"):
                counterpart_positions(Path("/unused"), control, report, intervals, {})
        self.assertFalse(guarded.reads)

    def test_empty_scope_opens_no_partition(self):
        control, report, _, _ = self.prepare()
        with patch(
            "libs.alpaca_lineage_build.source_observations.load_published_boundary",
            side_effect=AssertionError("empty scope opened prices"),
        ):
            value, selected, positions, dates = counterpart_positions(Path("/unused"), control, report, [], {})
        self.assertFalse(selected["correlated"]["segments"])
        self.assertTrue(all(not rows for rows in value.views.values()))
        self.assertTrue(all(not rows for rows in positions.values()))
        self.assertTrue(all(not rows for rows in dates.values()))

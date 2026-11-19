"""A dated multi-label source can supply one split-adjusted basis across a public identity."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation.mapped_rename_chain import split_adjusted_prefix
from libs.alpaca_lineage_build.adjustment_basis import INCOMPATIBLE_SPLIT_BASIS
from libs.alpaca_lineage_build.observation_carry import ChoiceRun, RetainedChoices
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge
from tests.test_alpaca_name_change_evidence import (
    PRODUCTION_POLICY,
    active_successor_prefix_inputs,
    bars,
    raw_segments,
)
from tests.test_alpaca_observation_carry import ClosedRows

DAYS = (
    "2024-01-02",
    "2024-01-03",
    "2024-01-04",
    "2024-01-05",
    "2024-01-08",
    "2024-01-09",
    "2024-01-10",
    "2024-01-11",
    "2024-01-12",
    "2024-01-16",
    "2024-01-17",
)


def rename_chain():
    facts = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
    add_name_change_edge(
        facts,
        action_id="rename-1",
        process_date=DAYS[3],
        predecessor=("id-A", "FIRST", "CUSIP-A", "Original Co"),
        successor=("id-A", "MIDDLE", "CUSIP-A", "Middle Co"),
    )
    add_name_change_edge(
        facts,
        action_id="rename-2",
        process_date=DAYS[7],
        predecessor=("id-B", "MIDDLE", "CUSIP-B", "Middle Co"),
        successor=("id-B", "LAST", "CUSIP-B", "Last Co"),
    )
    a, b = facts["identities"]
    a.update(symbol="MIDDLE", status="inactive")
    b.update(symbol="LAST", status="inactive")
    c = {**b, "id": "id-C", "status": "active", "lookup_cusips": ["CUSIP-C"]}
    facts["identities"].append(c)
    facts["asset_lookups"] = [
        {
            "lookup_kind": "cusip",
            "lookup": f"CUSIP-{suffix}",
            "asset": v,
            "outcome": "found",
            "found": True,
            "http_status": 200,
        }
        for suffix, v in zip(("A", "B", "C"), (a, b, c), strict=True)
    ]
    for name, old, new, day in (("MIDDLE", "A", "B", DAYS[5]), ("LAST", "B", "C", DAYS[9])):
        facts["corporate_actions"].append(
            {
                "action_type": "reverse_split",
                "id": "split-" + new,
                "symbol": name,
                "old_cusip": "CUSIP-" + old,
                "new_cusip": "CUSIP-" + new,
                "effective_date": day,
                "ex_date": day,
                "process_date": day,
                "old_rate": 10,
                "new_rate": 1,
            }
        )
    facts["queried_symbols"] = ["FIRST", "MIDDLE", "LAST"]
    facts["query"] = {
        "input_symbol": "LAST",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    raw = bars(DAYS)
    histories = {"FIRST": raw[:3], "MIDDLE": raw[3:7], "LAST": deepcopy(raw)}
    return facts, histories


def run_group(facts, histories, version=92, *, scale_for=None):
    adjusted = {}
    for name, rows in histories.items():
        prices = deepcopy(rows)
        for i, row in enumerate(prices):
            scale = scale_for(name, i) if scale_for else {"FIRST": 1, "MIDDLE": 10, "LAST": 100}[name]
            for key in ("o", "h", "l", "c", "vw"):
                row[key] = str(Decimal(row[key]) * scale)
            row["v"] = str(int(row["v"]) // scale)
        adjusted[name] = {mode: prices for mode in ADJUSTMENTS if mode == "all" or "split" in mode.split(",")}
    inputs = active_successor_prefix_inputs(histories, view_histories=adjusted)
    result = correlate(
        Path("."),
        inputs,
        {name: facts for name in histories},
        policy=replace(PRODUCTION_POLICY, implementation_correlation_version=version),
    )
    return inputs, result


class SplitAdjustedRenamePrefixTests(TestCase):
    def test_donor_only_prefix_retains_owner_and_publishes_the_common_eight_view_span(self):
        facts, histories = rename_chain()
        histories["LAST"] = histories["LAST"][1:]
        inputs, reports = run_group(facts, histories)
        selected, corrections = select_observations(inputs, reports)
        self.assertFalse(corrections)
        for mode in ADJUSTMENTS:
            donor = next(s for s in selected["FIRST"]["correlated"]["segments"] if s["adjustment"] == mode)
            self.assertEqual(
                (1, 1, "superseded"), (donor["row_start"], donor["row_end"], donor["observation_selection"])
            )
            self.assertIn(INCOMPATIBLE_SPLIT_BASIS, donor["warnings"])
            self.assertEqual("FIRST", donor["historical_symbol"])
            self.assertEqual("id-A", donor["canonical_security"]["alpaca_asset_id"])
            sessions = [
                row[0][:10]
                for name, report in selected.items()
                for segment in report["correlated"]["segments"]
                if segment["adjustment"] == mode and segment["observation_selection"] == "selected"
                for row in inputs[name].views[mode][segment["row_start"] - 1 : segment["row_end"]]
            ]
            self.assertEqual(list(DAYS[1:]), sessions)
        retained = {}
        for name, report in selected.items():
            runs = {mode: [] for mode in ADJUSTMENTS}
            for segment in report["correlated"]["segments"]:
                runs[segment["adjustment"]].append(
                    ChoiceRun(
                        segment["row_start"],
                        segment["row_end"],
                        segment["security_identity"]["security_id"],
                        segment["observation_selection"],
                        tuple(segment["observation_tickers"]),
                        segment["first_session_date"],
                        segment["last_session_date"],
                    )
                )
            retained[name] = RetainedChoices(inputs[name].source_sha256, name, runs)
            inputs[name].views = {mode: ClosedRows(len(rows)) for mode, rows in inputs[name].views.items()}
        with patch(
            "libs.alpaca_lineage_build.observations.incompatible_prefixes",
            side_effect=AssertionError("replayed basis"),
        ):
            carried, corrections = select_observations(inputs, selected, retained_choices=retained)
        self.assertFalse(corrections)
        self.assertCountEqual(
            selected["FIRST"]["correlated"]["segments"], carried["FIRST"]["correlated"]["segments"]
        )

    def test_equal_bases_or_a_single_corrected_session_keep_the_earlier_bundle(self):
        facts, histories = rename_chain()
        histories["LAST"] = histories["LAST"][1:]
        for corrected in (False, True):
            with self.subTest(one_corrected_day=corrected):
                inputs, reports = run_group(
                    facts,
                    histories,
                    scale_for=lambda name, i: 100 if corrected and name == "LAST" and i == 0 else 1,
                )
                selected, corrections = select_observations(inputs, reports)
                self.assertFalse(corrections)
                for mode in ADJUSTMENTS:
                    first = next(
                        s for s in selected["FIRST"]["correlated"]["segments"] if s["adjustment"] == mode
                    )
                    self.assertEqual("selected", first["observation_selection"])
                    self.assertNotIn(INCOMPATIBLE_SPLIT_BASIS, first["warnings"])

    def test_pending_donor_reports_do_not_repeat_raw_comparisons(self):
        row_reads = []
        grid_reads = []

        def observe(symbol, lineage, edges, sources, correlation, rows, sessions):
            def donor_rows(name, filename):
                self.assertEqual("raw.csv.gz", filename)
                self.assertNotIn(name, row_reads)
                row_reads.append(name)
                return rows(name, filename)

            def donor_sessions(name):
                self.assertNotIn(name, grid_reads)
                grid_reads.append(name)
                return sessions(name)

            return split_adjusted_prefix(
                symbol, lineage, edges, sources, correlation, donor_rows, donor_sessions
            )

        with patch(
            "libs.alpaca_end_of_day_security_correlation.predecessor_prefix.split_adjusted_prefix", observe
        ):
            run_group(*rename_chain())
        self.assertCountEqual(["FIRST", "MIDDLE"], row_reads)
        self.assertCountEqual(["FIRST", "MIDDLE"], grid_reads)

    def test_three_sources_with_intervening_reverse_splits(self):
        facts, histories = rename_chain()
        for version, expected_sources in ((91, ["FIRST", "MIDDLE", "LAST"]), (92, ["LAST"] * 3)):
            with self.subTest(version=version):
                inputs, reports = run_group(facts, histories, version)
                selected, corrections = select_observations(inputs, reports)
                self.assertEqual({}, corrections)
                expected_labels = ["FIRST"] * 3 + ["MIDDLE"] * 4 + ["LAST"] * 4
                expected_donors = (
                    [expected_sources[0]] * 3 + [expected_sources[1]] * 4 + [expected_sources[2]] * 4
                )
                for mode in ADJUSTMENTS:
                    chosen = []
                    for name, report in selected.items():
                        for segment in report["correlated"]["segments"]:
                            if segment["adjustment"] == mode and segment["observation_selection"] == "selected":
                                lo, hi = segment["row_start"] - 1, segment["row_end"]
                                for row in inputs[name].views[mode][lo:hi]:
                                    chosen.append((row[0], name, segment["observation_tickers"], row[4]))
                    chosen.sort()
                    self.assertEqual(expected_donors, [x[1] for x in chosen])
                    self.assertEqual([[label] for label in expected_labels], [x[2] for x in chosen])
                    if version == 92 and mode == "all":
                        self.assertEqual([str(100 * (10 + i)) for i in range(len(DAYS))], [x[3] for x in chosen])

    def test_competing_or_incomplete_evidence_keeps_original_sources(self):
        for case in (
            "raw VWAP mismatch",
            "missing donor day",
            "unreported split endpoint",
            "competing terminal occupant",
            "competing incoming rename",
            "incomplete action coverage",
        ):
            with self.subTest(case=case):
                facts, histories = rename_chain()
                if case == "raw VWAP mismatch":
                    histories["LAST"][1]["vw"] = "10.031"
                elif case == "missing donor day":
                    histories["LAST"].pop(2)
                elif case == "unreported split endpoint":
                    facts["corporate_actions"] = [a for a in facts["corporate_actions"] if a["id"] != "split-B"]
                elif case == "competing terminal occupant":
                    facts["identities"].append(
                        {**facts["identities"][-1], "id": "outside", "lookup_cusips": ["OTHER"]}
                    )
                elif case == "competing incoming rename":
                    facts["corporate_actions"].append(
                        {**facts["corporate_actions"][1], "old_symbol": "RIVAL", "id": "rival"}
                    )
                else:
                    facts["query"]["corporate_actions_data_quality"] = "incomplete"
                _, reports = run_group(facts, histories)
                mapped = [
                    s
                    for s in raw_segments(reports["LAST"])
                    if s["correlation_method"] == "accepted_name_change_mapped_continuation"
                ]
                self.assertEqual([], mapped)

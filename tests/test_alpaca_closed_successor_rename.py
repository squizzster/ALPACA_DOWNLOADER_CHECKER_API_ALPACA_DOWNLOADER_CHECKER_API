"""Independent source evidence can date a retired intermediate ticker before processing."""

import copy
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import name_change
from libs.alpaca_lineage_build import policy_engine
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge, occupancy
from tests.test_alpaca_name_change_evidence import (
    PRODUCTION_POLICY,
    bars,
    early_successor_bridge_evidence,
    inputs_for,
    raw_segments,
)

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
)


def intermediate_case():
    evidence = early_successor_bridge_evidence()
    terminal = evidence["identities"][0]
    terminal.update(exchange="OTC", status="inactive", tradable=False)
    old_lookup = dict(terminal, id="previous-spelling-id", symbol="OLD", exchange="NASDAQ", lookup_cusips=[])
    evidence["asset_lookups"].extend(
        dict(evidence["asset_lookups"][0], lookup_kind="symbol", lookup=asset["symbol"], asset=asset)
        for asset in (terminal, old_lookup)
    )
    evidence["queried_symbols"] = ["OLD", "NEW", "CURRENT", "ESCROW"]
    add_name_change_edge(
        evidence,
        action_id="departure",
        process_date="2024-01-16",
        predecessor=("old-id", "NEW", "old-cusip", "Intermediate security label"),
        successor=("old-id", "CURRENT", "old-cusip", "Terminal label"),
    )
    add_name_change_edge(
        evidence,
        action_id="subsequent-reorganization",
        process_date="2024-01-25",
        predecessor=("old-id", "CURRENT", "old-cusip", "Terminal label"),
        successor=("escrow-id", "ESCROW", "escrow-cusip", "Later escrow"),
    )
    rows = bars(DAYS)
    return evidence, {"OLD": rows[:2], "NEW": rows[2:], "CURRENT": copy.deepcopy(rows)}


def fresh(evidence, histories, *, version, inputs=None, source_evidence=None):
    inputs = inputs or inputs_for(histories)
    reports = correlate(
        Path("/unused"),
        inputs,
        source_evidence or dict.fromkeys(inputs, evidence),
        policy=replace(PRODUCTION_POLICY, implementation_correlation_version=version),
    )
    return inputs, reports


class WindowRows:
    def __init__(self, rows, allowed):
        self.rows = rows
        self.allowed = allowed

    def __getitem__(self, key):
        positions = range(len(self.rows))
        selected = positions[key] if isinstance(key, slice) else (positions[key],)
        if any(position not in self.allowed for position in selected):
            raise AssertionError("source row outside the disputed handoff")
        return self.rows[key]


class ClosedSuccessorRenameTests(unittest.TestCase):
    def test_two_native_name_changes_and_only_the_traded_handoff_date_the_intermediate(self):
        evidence, histories = intermediate_case()
        # Earlier and later price vintages are outside this three-day identity question.
        histories["CURRENT"][0]["v"] = "130"
        histories["CURRENT"][-1]["v"] = "140"
        inputs, frozen = fresh(evidence, histories, version=104)
        checked = []
        compare = name_change._rows_equal
        assess = name_change._closed_successor_source_bridge_history
        source_evidence = {name: copy.deepcopy(evidence) for name in inputs}
        for symbol, receipt in source_evidence.items():
            receipt["query"]["input_symbol"] = symbol

        def compare_window(left, right, fields):
            sessions = [row["t"][:10] for row in left]
            self.assertEqual(list(DAYS[1:6]), sessions)
            self.assertEqual(sessions, [row["t"][:10] for row in right])
            checked.append(sessions)
            return compare(left, right, fields)

        def bounded_rows(lineage, intervals, grids, edge, old, new, fields, peer_evidence):
            if edge.successor_symbol == "NEW":
                terminal = grids["CURRENT"]
                grids = {**grids, "CURRENT": (WindowRows(terminal[0], range(1, 6)), *terminal[1:])}
                old = (WindowRows(old[0], range(1, 2)), *old[1:])
                new = (WindowRows(new[0], range(4)), *new[1:])
            return assess(lineage, intervals, grids, edge, old, new, fields, peer_evidence)

        with (
            patch.object(name_change, "_closed_successor_source_bridge_history", side_effect=bounded_rows),
            patch.object(name_change, "_rows_equal", side_effect=compare_window),
        ):
            _, reports = fresh(evidence, histories, version=105, inputs=inputs, source_evidence=source_evidence)
        self.assertEqual(1, len(checked))
        self.assertEqual(
            [(DAYS[2], DAYS[4])],
            [
                (s["first_session_date"], s["last_session_date"])
                for s in raw_segments(frozen["NEW"], "uncorrelated")
            ],
        )
        self.assertEqual([], raw_segments(reports["NEW"], "uncorrelated"))
        selected, corrections = select_observations(inputs, copy.deepcopy(reports))
        previous, _ = select_observations(inputs, copy.deepcopy(frozen))
        self.assertEqual({}, corrections)
        for adjustment in ADJUSTMENTS:
            segments = [s for s in selected["NEW"]["correlated"]["segments"] if s["adjustment"] == adjustment]
            self.assertEqual(
                (DAYS[2], DAYS[4]), (segments[0]["first_session_date"], segments[0]["last_session_date"])
            )
            self.assertEqual(
                [s for s in previous["NEW"]["correlated"]["segments"] if s["adjustment"] == adjustment],
                segments[1:],
            )
            self.assertEqual({"NEW"}, {s["historical_symbol"] for s in segments})
            self.assertEqual({"selected"}, {s["observation_selection"] for s in segments})
            self.assertEqual(
                {("old-id",)}, {tuple(s["security_identity"]["member_alpaca_asset_ids"]) for s in segments}
            )
            self.assertEqual(DAYS[5], segments[0]["ticker_ownership_interval"]["end_date_exclusive"])
            self.assertEqual(name_change.CLOSED_SUCCESSOR_RENAME_METHOD, segments[0]["correlation_method"])
            self.assertTrue(
                {"observed_trading_dates_only", "effective_rename_date_unknown"}.issubset(
                    segments[0]["warnings"]
                )
            )
            self.assertEqual(
                ["transition", "departure"],
                segments[0]["ticker_ownership_interval"]["supporting_corporate_action_ids"],
            )
        self.assertEqual(DAYS[5], evidence["corporate_actions"][0]["process_date"])
        for label in ("OLD", "CURRENT"):
            for branch in ("correlated", "uncorrelated"):
                self.assertEqual(frozen[label][branch], reports[label][branch])

    def test_an_earlier_same_issue_arrival_and_later_escrow_keep_the_handoff_scoped(self):
        evidence, histories = intermediate_case()
        add_name_change_edge(
            evidence,
            action_id="remote-arrival",
            process_date="2023-12-15",
            predecessor=("old-id", "FIRST", "old-cusip", "Earlier listing"),
            successor=("old-id", "OLD", "old-cusip", "Predecessor"),
        )
        evidence["queried_symbols"].append("FIRST")
        _, result = fresh(evidence, histories, version=105)
        self.assertEqual([], raw_segments(result["NEW"], "uncorrelated"))
        first = raw_segments(result["NEW"])[0]
        self.assertEqual((DAYS[2], DAYS[4]), (first["first_session_date"], first["last_session_date"]))
        for field, value in (
            ("old_symbol", "NEW"),
            ("old_symbol", "CURRENT"),
            ("old_cusip", "other-cusip"),
            ("process_date", DAYS[3]),
        ):
            with self.subTest(field=field, value=value):
                changed = copy.deepcopy(evidence)
                changed["corporate_actions"][-1][field] = value
                _, result = fresh(changed, histories, version=105)
                self.assertTrue(raw_segments(result["NEW"], "uncorrelated"))

    def test_positive_peer_conflicts_are_consumed_before_any_new_price_comparison(self):
        evidence, histories = intermediate_case()
        for contradiction in (
            "peer action date",
            "peer departure identity",
            "named competing action",
            "peer owner",
        ):
            with self.subTest(contradiction=contradiction):
                peers = {name: copy.deepcopy(evidence) for name in histories}
                other = peers["OLD"]
                if contradiction == "peer action date":
                    other["corporate_actions"][0]["effective_date"] = DAYS[5]
                elif contradiction == "peer departure identity":
                    other["corporate_actions"][1]["new_cusip"] = "another-cusip"
                elif contradiction == "named competing action":
                    other["corporate_actions"].append(
                        {
                            "id": "other-exit",
                            "action_type": "name_change",
                            "old_symbol": "NEW",
                            "new_symbol": "ANOTHER",
                            "process_date": DAYS[4],
                        }
                    )
                else:
                    other["asset_lookups"][0]["asset"] = dict(other["identities"][0], id="another-issue")
                with patch.object(name_change, "_rows_equal", side_effect=AssertionError("unselected prices")):
                    _, result = fresh(evidence, histories, version=105, source_evidence=peers)
                self.assertTrue(raw_segments(result["NEW"], "uncorrelated"))

    def test_unknown_group_peer_withholds_only_the_unestablished_early_span(self):
        evidence, histories = intermediate_case()
        inputs = inputs_for(histories)
        with patch.object(
            name_change,
            "_closed_successor_source_bridge_history",
            side_effect=AssertionError("incomplete group"),
        ):
            result = correlate(
                Path("/unused"),
                inputs,
                dict.fromkeys(inputs, evidence),
                policy=PRODUCTION_POLICY,
                selected_group=[*inputs, "UNAVAILABLE"],
                unavailable_pricing=["UNAVAILABLE"],
            )
        self.assertEqual(
            [(DAYS[2], DAYS[4])],
            [
                (s["first_session_date"], s["last_session_date"])
                for s in raw_segments(result["NEW"], "uncorrelated")
            ],
        )
        self.assertEqual(DAYS[5], raw_segments(result["NEW"])[0]["first_session_date"])

    def test_retry_consumes_the_completed_source_decision_until_its_pricing_generation_changes(self):
        evidence, histories = intermediate_case()
        inputs = inputs_for(histories)
        peers = {name: copy.deepcopy(evidence) for name in inputs}
        build = policy_engine.build_security_correlation_report

        def fail_after_new(report, *args, **kwargs):
            result = build(report, *args, **kwargs)
            if report["symbol"] == "NEW":
                raise RuntimeError("interrupted after the successor assessment")
            return result

        def evaluate():
            return correlate(Path("/unused"), inputs, peers, policy=PRODUCTION_POLICY)

        with patch.object(name_change, "_rows_equal", wraps=name_change._rows_equal) as comparison:
            with patch.object(policy_engine, "build_security_correlation_report", side_effect=fail_after_new):
                with self.assertRaisesRegex(RuntimeError, "interrupted after"):
                    evaluate()
            self.assertEqual(1, comparison.call_count)
            self.assertEqual([], raw_segments(evaluate()["NEW"], "uncorrelated"))
            self.assertEqual(1, comparison.call_count)
            histories["NEW"][1]["n"] = "3"
            inputs["NEW"] = inputs_for({"NEW": histories["NEW"]})["NEW"]
            inputs["NEW"].source_sha256 = "new-successor-pricing"
            for _ in range(2):
                self.assertTrue(raw_segments(evaluate()["NEW"], "uncorrelated"))
                self.assertEqual(2, comparison.call_count)

    def test_native_identity_and_source_vetoes_leave_the_early_sessions_unassigned(self):
        for problem in (
            "explicit effective date",
            "incomplete action coverage",
            "returned terminal mismatch",
            "additional departure",
            "dated predecessor rival",
            "structural conflict",
            "raw difference on predecessor",
            "raw difference on processing day",
            "raw difference inside",
            "inert seam",
            "missing bridge session",
            "overlapping direct routes",
            "new source truncated",
            "old request stopped early",
            "stale terminal route",
            "terminal mapping predates departure",
            "literal terminal tape",
            "incomplete view",
        ):
            with self.subTest(problem=problem):
                evidence, histories = intermediate_case()
                if problem == "explicit effective date":
                    evidence["corporate_actions"][0]["effective_date"] = DAYS[5]
                elif problem == "incomplete action coverage":
                    evidence["query"]["corporate_actions_end"] = DAYS[-1]
                elif problem == "returned terminal mismatch":
                    evidence["asset_lookups"][0]["asset"] = dict(evidence["identities"][0], symbol="DIFFERENT")
                elif problem == "additional departure":
                    add_name_change_edge(
                        evidence,
                        action_id="second-exit",
                        process_date="2024-01-17",
                        predecessor=("old-id", "NEW", "old-cusip", "Intermediate"),
                        successor=("old-id", "OTHER", "old-cusip", "Other route"),
                    )
                elif problem == "dated predecessor rival":
                    evidence["symbol_occupancy_evidence"].append(
                        occupancy(
                            "OLD",
                            "other-cusip",
                            "previous-spelling-id",
                            action_type="cash_dividend",
                            action_id="dated-rival",
                            process_date=DAYS[3],
                            role="symbol",
                        )
                    )
                elif problem == "structural conflict":
                    evidence["corporate_actions"].append(
                        {
                            "id": "distribution",
                            "action_type": "reorganization",
                            "process_date": DAYS[3],
                            "outputs": [{"symbol": "CURRENT"}],
                        }
                    )
                elif problem.startswith("raw difference"):
                    pos = {
                        "raw difference on predecessor": 1,
                        "raw difference on processing day": 5,
                        "raw difference inside": 3,
                    }[problem]
                    histories["CURRENT"][pos]["n"] = "2"
                elif problem == "inert seam":
                    for rows, index in ((histories["NEW"], 1), (histories["CURRENT"], 3)):
                        rows[index].update(v="0", n="0")
                elif problem == "missing bridge session":
                    del histories["CURRENT"][3]
                elif problem == "overlapping direct routes":
                    histories["OLD"].append(histories["NEW"][0])
                elif problem == "stale terminal route":
                    histories.pop("CURRENT")
                inputs = inputs_for(histories)
                if problem == "new source truncated":
                    for download in inputs["NEW"].downloads.values():
                        download.manifest["request_from"] = DAYS[2]
                elif problem == "old request stopped early":
                    for download in inputs["OLD"].downloads.values():
                        download.manifest["request_to"] = DAYS[2]
                elif problem in ("terminal mapping predates departure", "literal terminal tape"):
                    asof = DAYS[-1] if problem == "terminal mapping predates departure" else "-"
                    inputs = inputs_for(histories, asofs={"CURRENT": asof})
                elif problem == "incomplete view":
                    inputs = inputs_for(histories, view_histories={"CURRENT": {"all": histories["CURRENT"][1:]}})
                _, reports = fresh(evidence, histories, version=105, inputs=inputs)
                self.assertTrue(raw_segments(reports["NEW"], "uncorrelated"))
                self.assertFalse(
                    any(
                        s["first_session_date"] <= DAYS[2] and s["historical_symbol"] == "NEW"
                        for s in raw_segments(reports["NEW"])
                    )
                )

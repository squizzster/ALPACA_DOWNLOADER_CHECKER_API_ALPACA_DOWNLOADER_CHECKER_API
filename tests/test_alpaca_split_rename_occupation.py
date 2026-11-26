"""The split changes the issue behind the intermediate ticker before the next rename."""

from copy import deepcopy
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from libs.alpaca_end_of_day_security_correlation import row_correlation
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.split_rename_ownership import (
    literal_source_conflict,
    split_rename_occupations,
)
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge
from tests.test_alpaca_name_change_evidence import PRODUCTION_POLICY, bars, inputs_for, raw_segments

DAYS = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10")


def case(labels=("FIRST", "MIDDLE", "LAST")):
    first, middle, last = labels
    evidence = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
    for event, when, old, new, issue in (
        ("arrival", DAYS[1], first, middle, "A"),
        ("departure", DAYS[5], middle, last, "B"),
    ):
        add_name_change_edge(
            evidence,
            action_id=event,
            process_date=when,
            predecessor=("id-" + issue, old, "cusip-" + issue, "Issuer"),
            successor=("id-" + issue, new, "cusip-" + issue, "Issuer"),
        )
    evidence["corporate_actions"].append(
        {
            "action_type": "reverse_split",
            "id": "split",
            "symbol": middle,
            "old_cusip": "cusip-A",
            "new_cusip": "cusip-B",
            "old_rate": 10,
            "new_rate": 1,
            "ex_date": DAYS[3],
            "process_date": DAYS[3],
        }
    )
    evidence["asset_lookups"] = []
    for asset, label, issue, status in zip(
        evidence["identities"], (middle, last), ("A", "B"), ("inactive", "active"), strict=True
    ):
        asset.update(symbol=label, status=status)
        for kind, lookup in (("symbol", label), ("cusip", "cusip-" + issue)):
            evidence["asset_lookups"].append(
                {
                    "lookup_kind": kind,
                    "lookup": lookup,
                    "asset": asset,
                    "found": True,
                    "outcome": "found",
                    "http_status": 200,
                }
            )
    evidence["queried_symbols"] = list(labels)
    evidence["query"] = {
        "input_symbol": last,
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    prices = bars(DAYS)
    return evidence, {first: prices[:1], middle: prices, last: prices[5:]}


def fresh(evidence, histories, version, *, asofs=None):
    first, middle, last = histories
    inputs = inputs_for(histories, asofs=asofs or {first: DAYS[1], middle: DAYS[4], last: DAYS[-1]})
    held = dict.fromkeys(inputs, evidence)
    reports = correlate(
        Path("."), inputs, held, policy=replace(PRODUCTION_POLICY, implementation_correlation_version=version)
    )
    selected, _ = select_observations(inputs, reports, dated_label_evidence=held, correlation_version=version)
    return reports, {
        (mode, row[0][:10]): (
            tuple(segment["observation_tickers"]),
            segment["security_identity"]["security_id"],
            row,
        )
        for source, report in selected.items()
        for segment in report["correlated"]["segments"]
        if segment["observation_selection"] == "selected"
        for mode in (segment["adjustment"],)
        for row in inputs[source].views[mode][segment["row_start"] - 1 : segment["row_end"]]
    }


class SplitRenameOccupationTests(TestCase):
    def test_complete_group_fresh_and_frozen_give_same_prices_and_owner_across_every_view(self):
        for labels in (("FIRST", "MIDDLE", "LAST"), ("PAST", "TEMP", "NOW")):
            with self.subTest(labels=labels):
                first, middle, last = labels
                evidence, histories = case(labels)
                _, old = fresh(evidence, histories, 106)
                with patch.object(
                    row_correlation, "literal_source_conflict", wraps=row_correlation.literal_source_conflict
                ) as scope:
                    reports, new = fresh(evidence, histories, 107)
                self.assertEqual(scope.call_count, 1)
                self.assertEqual(set(old), set(new))
                for mode in ADJUSTMENTS:
                    self.assertEqual([first, *([middle] * 4), last, last], [new[mode, d][0][0] for d in DAYS])
                    for day in DAYS:
                        self.assertEqual(len(new[mode, day][0]), 1)
                        self.assertEqual(old[mode, day][1:], new[mode, day][1:])
                    self.assertEqual(set(old[mode, DAYS[5]][0]), {middle, last})
                split = next(s for s in raw_segments(reports[middle]) if s["first_session_date"] == DAYS[3])
                self.assertEqual(split["canonical_security"]["alpaca_asset_id"], "id-B")
                self.assertEqual(
                    split["ticker_ownership_interval"]["supporting_corporate_action_ids"],
                    ["arrival", "split", "departure"],
                )
                # The one-session mapped prefix still lacks a source-history
                # proof. Its independently observed original-label row survives.
                unresolved = raw_segments(reports[middle], "uncorrelated")
                self.assertEqual(
                    [(row["first_session_date"], row["last_session_date"]) for row in unresolved],
                    [(DAYS[0], DAYS[0])],
                )
                self.assertFalse(raw_segments(reports[first], "uncorrelated"))
                self.assertFalse(raw_segments(reports[last], "uncorrelated"))

    def test_native_and_completed_edges_use_the_same_route_without_reopening_inputs(self):
        evidence, _ = case()
        edges, _ = build_uuid_lineage_edges(evidence)
        encoded = [edge.as_report_object() for edge in reversed(edges)]
        with (
            patch("builtins.open", side_effect=AssertionError("file access in dated ownership")),
            patch.object(Path, "open", side_effect=AssertionError("file access in dated ownership")),
            patch("urllib.request.urlopen", side_effect=AssertionError("provider request in dated ownership")),
            patch.object(row_correlation, "build_uuid_lineage_edges", side_effect=AssertionError("edge replay")),
        ):
            self.assertEqual(
                split_rename_occupations(evidence, edges), split_rename_occupations(evidence, encoded)
            )
            intervals, _ = build_ticker_ownership_intervals(evidence, uuid_lineage_edges=edges)
            self.assertEqual(
                ["id-B"],
                [
                    i.asset_id
                    for i in intervals
                    if i.historical_symbol == "MIDDLE" and i.contains(date.fromisoformat(DAYS[3]))
                ],
            )
            self.assertFalse(
                any(
                    i.historical_symbol == "MIDDLE" and i.contains(date.fromisoformat(DAYS[5]))
                    for i in intervals
                )
            )
            self.assertFalse(
                split_rename_occupations(
                    evidence,
                    [
                        {**edge, "sourced_fact_id": "external"} if edge["edge_type"] == "reverse_split" else edge
                        for edge in encoded
                    ],
                )
            )

    def test_each_split_must_join_the_previous_issue_on_a_strictly_later_date(self):
        evidence, _ = case()
        edges, _ = build_uuid_lineage_edges(evidence)
        split = next(e for e in edges if e.edge_type == "reverse_split")
        first = replace(split, successor_cusip="cusip-T", successor_asset_id="id-T")
        second = replace(
            split,
            predecessor_cusip="cusip-T",
            predecessor_asset_id="id-T",
            action_id="further-split",
            effective_date=date.fromisoformat(DAYS[4]),
        )
        renames = [e for e in edges if e.edge_type == "name_change"]
        paths = split_rename_occupations(evidence, [second, *renames, first])
        self.assertEqual(len(paths), 1)
        self.assertEqual(paths[0].splits, (first, second))
        for invalid in (
            replace(second, predecessor_cusip="unconnected"),
            replace(second, predecessor_asset_id="other-id"),
            replace(second, effective_date=first.effective_date),
            replace(second, effective_date=renames[-1].effective_date),
        ):
            with self.subTest(edge=invalid):
                self.assertFalse(split_rename_occupations(evidence, [first, invalid, *renames]))
        self.assertFalse(split_rename_occupations(evidence, [first, split, second, *renames]))
        self.assertFalse(split_rename_occupations(evidence, [first, *renames]))

    def test_reused_labels_competing_occupants_and_native_conflicts_retain_prior_rules(self):
        for scenario in ("branch", "round trip", "different owner", "unknown structural event", "literal tape"):
            with self.subTest(scenario=scenario):
                evidence, _ = case()
                if scenario == "branch":
                    add_name_change_edge(
                        evidence,
                        action_id="fork",
                        process_date=DAYS[4],
                        predecessor=("id-A", "MIDDLE", "cusip-A", "Issuer"),
                        successor=("id-A", "ANOTHER", "cusip-A", "Issuer"),
                    )
                elif scenario == "round trip":
                    evidence["corporate_actions"][1]["new_symbol"] = "FIRST"
                    evidence["symbol_occupancy_evidence"][3]["symbol"] = "FIRST"
                    evidence["asset_lookups"][2]["asset"]["symbol"] = "FIRST"
                elif scenario == "different owner":
                    evidence["symbol_occupancy_evidence"].append(
                        {
                            "symbol": "MIDDLE",
                            "cusip": "not-this-issue",
                            "asset_id": "different-owner",
                            "evidence": [{"process_date": DAYS[3]}],
                        }
                    )
                elif scenario == "unknown structural event":
                    evidence["corporate_actions"].append(
                        {
                            "action_type": "stock_merger",
                            "id": "unresolved-event",
                            "acquiree_symbol": "MIDDLE",
                            "effective_date": DAYS[4],
                        }
                    )
                else:
                    evidence["historical_trade_tape"] = {
                        "symbol": "MIDDLE",
                        "asof": "-",
                        "feed": "sip",
                        "outcome": "observed",
                        "http_status": 200,
                        "session": DAYS[-1],
                    }
                edges, _ = build_uuid_lineage_edges(evidence)
                self.assertFalse(split_rename_occupations(evidence, edges))
                old, _ = build_ticker_ownership_intervals(
                    evidence, uuid_lineage_edges=edges, bound_split_renames=False
                )
                new, _ = build_ticker_ownership_intervals(evidence, uuid_lineage_edges=edges)
                self.assertEqual(new, old)

    def test_literal_trading_disputing_a_processing_date_defers_to_existing_rules(self):
        for direction in ("early successor", "late predecessor"):
            with self.subTest(direction=direction):
                evidence, histories = case()
                asofs = {"FIRST": DAYS[1], "MIDDLE": DAYS[4], "LAST": DAYS[-1]}
                if direction == "early successor":
                    histories["LAST"] = deepcopy(histories["MIDDLE"][4:])
                    asofs["LAST"] = "-"
                else:
                    histories["MIDDLE"] = histories["MIDDLE"][1:]
                    asofs["MIDDLE"] = "-"
                _, old = fresh(evidence, histories, 106, asofs=asofs)
                reports, new = fresh(evidence, histories, 107, asofs=asofs)
                self.assertEqual(new, old)
                self.assertTrue(
                    any(
                        issue["reason_code"] == "split_rename_contrary_literal_trading"
                        for issue in reports["MIDDLE"]["evidence_issues"]
                    )
                )

    def test_sparse_literal_trading_and_a_separately_dated_later_occupation(self):
        evidence, _ = case()
        return_date = "2025-02-03"
        add_name_change_edge(
            evidence,
            action_id="other-arrival",
            process_date=return_date,
            predecessor=("other-id", "INDEPENDENT", "other-cusip", "Second issuer"),
            successor=("other-id", "MIDDLE", "other-cusip", "Second issuer"),
        )
        evidence["query"]["corporate_actions_end"] = "2025-03-01"
        evidence["historical_trade_tape"] = {
            "symbol": "MIDDLE",
            "asof": "-",
            "feed": "sip",
            "http_status": 200,
            "outcome": "observed",
            "session": "2025-02-04",
        }
        edges, _ = build_uuid_lineage_edges(evidence)
        (occupation,) = split_rename_occupations(evidence, edges)
        intervals, _ = build_ticker_ownership_intervals(evidence, uuid_lineage_edges=edges)
        middle = [i for i in intervals if i.historical_symbol == "MIDDLE"]
        self.assertEqual(["id-B"], [i.asset_id for i in middle if i.contains(date.fromisoformat(DAYS[4]))])
        self.assertEqual(
            ["other-id"], [i.asset_id for i in middle if i.contains(date.fromisoformat(return_date))]
        )
        self.assertFalse(occupation.contradicts_literal("MIDDLE", date.fromisoformat(return_date)))
        # The raw grid has no sessions during the halt. Only its nearest contested
        # result may be read. The later unrelated issuer is outside the decision.
        sessions = tuple(map(date.fromisoformat, (DAYS[4], "2024-05-01", return_date)))
        rows = MagicMock()
        positive = bars(("2024-05-01",))[0]
        rows.__getitem__.side_effect = {1: positive}.__getitem__
        sources = {"MIDDLE": [(SimpleNamespace(rows=rows), {"adjustment": "raw", "asof": "-"}, sessions)]}
        self.assertTrue(literal_source_conflict(occupation, sources))
        rows.__getitem__.assert_called_once_with(1)
        rows.reset_mock()
        rows.__getitem__.side_effect = {1: {**positive, "n": "0", "v": "0", "vw": "0"}}.__getitem__
        self.assertFalse(literal_source_conflict(occupation, sources))
        rows.__getitem__.assert_called_once_with(1)

    def test_a_selected_peer_with_a_competing_dated_holder_vetoes_all_sources(self):
        evidence, histories = case()
        peers = {name: deepcopy(evidence) for name in histories}
        peers["FIRST"]["symbol_occupancy_evidence"].append(
            {
                "symbol": "MIDDLE",
                "cusip": "competing-issue",
                "asset_id": "other-owner",
                "evidence": [{"process_date": DAYS[4]}],
            }
        )
        reports = correlate(
            Path("."),
            inputs_for(histories),
            peers,
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=107),
        )
        for report in reports.values():
            self.assertIn(
                "split_rename_competing_native_occupation",
                {issue["reason_code"] for issue in report["evidence_issues"]},
            )
            self.assertTrue(
                any(
                    i["historical_symbol"] == "MIDDLE" and i["end_date_exclusive"] is None
                    for i in report["ticker_ownership_intervals"]
                )
            )

    def test_retained_literal_trade_observations_use_the_actual_returned_session(self):
        evidence, _ = case()
        edges, _ = build_uuid_lineage_edges(evidence)
        for field, search in (
            ("historical_trade_tape", "last_trade_search"),
            ("otc_historical_trade_tape", "following_trade_search"),
        ):
            for key in ("last_request", "observation"):
                sample = {
                    "symbol": "MIDDLE",
                    "asof": "-",
                    "http_status": 200,
                    "outcome": "observed",
                    "session": DAYS[4],
                    "observed_session": DAYS[-1],
                }
                with self.subTest(field=field, key=key):
                    result = {
                        "outcome": "observed",
                        "from_session": DAYS[4],
                        "through_session": DAYS[-1],
                        key: sample,
                    }
                    held = {**evidence, field: {"outcome": "no_trades", search: result}}
                    self.assertFalse(split_rename_occupations(held, edges))
                    result["through_session"] = DAYS[4]
                    self.assertTrue(split_rename_occupations(held, edges))
                    result["through_session"] = DAYS[-1]
                    sample["asof"] = DAYS[4]
                    self.assertTrue(split_rename_occupations(held, edges))

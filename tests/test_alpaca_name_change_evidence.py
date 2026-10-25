"""Generic rename evidence gates, exercised through the complete group cascade."""

import copy
import unittest
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS
from libs.alpaca_serving_catalogue.policy import _metadata
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge


def bars(days, base=10):
    return [
        {
            "t": day + "T05:00:00Z",
            **dict.fromkeys(("o", "h", "l", "c", "vw"), str(base + index)),
            "v": "100",
            "n": "10",
        }
        for index, day in enumerate(days)
    ]


def rename(*, same_uuid=False, old="OLD", new="NEW", when="2024-01-05"):
    evidence = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
    add_name_change_edge(
        evidence,
        action_id="transition",
        process_date=when,
        predecessor=("old-id", old, "old-cusip", "Original security"),
        successor=("old-id" if same_uuid else "new-id", new, "new-cusip", "Continuing security"),
    )
    evidence["asset_lookups"] = [
        {
            "lookup_kind": "symbol",
            "lookup": asset["symbol"],
            "asset": asset,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for asset in evidence["identities"]
    ]
    return evidence


def inputs_for(histories, *, asofs=None, view_histories=None):
    asofs = asofs or {}
    view_histories = view_histories or {}
    inputs = {}
    for symbol, rows in histories.items():
        downloads, descriptors = {}, []
        for adjustment in ADJUSTMENTS:
            selected_rows = view_histories.get(symbol, {}).get(adjustment, rows)
            filename = adjustment.replace(",", "_") + ".csv.gz"
            manifest = {
                "symbol": symbol,
                "adjustment": adjustment,
                "sha256": symbol + adjustment,
                "asof": asofs.get(symbol, "2024-02-01"),
                "request_from": "2024-01-01",
                "request_to": "2024-01-09",
                "feed": "sip",
                "timeframe": "1Day",
                "rows": len(selected_rows),
            }
            downloads[filename] = SimpleNamespace(
                rows=copy.deepcopy(selected_rows),
                manifest=manifest,
                path=Path(filename),
                pipeline_state="source_published",
                integrity_failure=None,
            )
            descriptors.append({"file": filename, "rows": len(selected_rows)})
        inputs[symbol] = SimpleNamespace(
            downloads=downloads,
            local={"directory": symbol, "downloads": descriptors, "present": True},
            reader=lambda _root, _directory, descriptor, items=downloads: items[descriptor["file"]],
            views={
                adjustment: tuple(
                    tuple(str(row[field]) for field in FIELDS)
                    for row in downloads[adjustment.replace(",", "_") + ".csv.gz"].rows
                )
                for adjustment in ADJUSTMENTS
            },
            source_sha256=symbol.lower() * 16,
            source={
                "history_start": "2024-01-01",
                "latest_request": {
                    "asof": asofs.get(symbol, "2024-02-01"),
                    "end_exclusive": "2024-01-09",
                },
                "activity_consistent": True,
            },
        )
    return inputs


def run(evidence, histories, **input_options):
    return correlate(
        Path("."),
        inputs_for(histories, **input_options),
        {symbol: evidence for symbol in histories},
    )


def raw_segments(report, branch="correlated"):
    return [segment for segment in report[branch]["segments"] if segment["adjustment"] == "raw"]


class NameChangeEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")

    def test_action_alone_cannot_merge_distinct_uuids(self):
        edges, issues = build_uuid_lineage_edges(rename())
        self.assertEqual([], edges)
        self.assertEqual("name_change_requires_independent_continuity", issues[0]["reason_code"])

    def test_historical_mapping_gap_preserves_only_the_complete_predecessor_prefix(self):
        evidence = rename(when="2024-01-09")
        evidence["query"] = {
            "input_symbol": "OLD",
            "corporate_actions_start": "2024-01-01",
            "corporate_actions_end": "2024-01-31",
            "corporate_actions_data_quality": "complete",
        }
        evidence["identities"][0]["status"] = "inactive"
        by_id = {identity["id"]: identity for identity in evidence["identities"]}
        evidence["asset_lookups"].extend(
            {
                "lookup_kind": "cusip",
                "lookup": cusip,
                "asset": by_id[asset_id],
                "found": True,
                "outcome": "found",
                "http_status": 200,
            }
            for cusip, asset_id in (("old-cusip", "old-id"), ("new-cusip", "new-id"))
        )
        days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-08")
        reports = run(evidence, {"OLD": bars(days)}, asofs={"OLD": "2024-01-04"})
        self.assertEqual(
            [("2024-01-02", "2024-01-04", 3)],
            [
                (segment["first_session_date"], segment["last_session_date"], segment["row_count"])
                for segment in raw_segments(reports["OLD"])
            ],
        )
        self.assertEqual(
            [("2024-01-08", ["post_mapping_asof_gap_requires_dated_owner"])],
            [
                (segment["first_session_date"], segment["reason_codes"])
                for segment in raw_segments(reports["OLD"], "uncorrelated")
            ],
        )

    def test_exact_independent_sources_certify_a_rename_and_bound_its_history(self):
        evidence = rename()
        reports = run(evidence, {"OLD": bars(self.days[:3]), "NEW": bars(self.days)})
        self.assertEqual([], raw_segments(reports["NEW"], "uncorrelated"))
        segments = raw_segments(reports["NEW"])
        self.assertEqual(["OLD", "NEW"], [segment["historical_symbol"] for segment in segments])
        self.assertEqual([3, 2], [segment["row_count"] for segment in segments])
        self.assertEqual(["new-id", "old-id"], segments[0]["security_identity"]["member_alpaca_asset_ids"])
        self.assertEqual("2024-01-02", segments[0]["ticker_ownership_interval"]["start_date_inclusive"])

    def test_mapped_predecessor_tail_uses_independently_owned_successor_rows(self):
        evidence = rename()
        histories = {"OLD": bars(self.days), "NEW": bars(self.days)}
        inputs = inputs_for(histories, asofs={"OLD": "2024-01-04"})
        reports = correlate(
            Path("."),
            inputs,
            {symbol: evidence for symbol in histories},
        )

        self.assertEqual([], raw_segments(reports["OLD"], "uncorrelated"))
        segments = raw_segments(reports["OLD"])
        self.assertEqual(["OLD", "NEW"], [segment["historical_symbol"] for segment in segments])
        self.assertEqual([3, 2], [segment["row_count"] for segment in segments])
        self.assertEqual("accepted_name_change_mapped_continuation", segments[1]["correlation_method"])
        self.assertIn("mapped_predecessor_duplicate", segments[1]["warnings"])
        selected, corrections = select_observations(inputs, reports)
        self.assertEqual({}, corrections)
        mapped = [
            segment
            for segment in raw_segments(selected["OLD"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual({"superseded"}, {segment["observation_selection"] for segment in mapped})

    def test_mapped_predecessor_tail_requires_equality_in_every_view(self):
        evidence = rename()
        changed = bars(self.days)
        changed[3]["c"] = "99"
        reports = run(
            evidence,
            {"OLD": bars(self.days), "NEW": bars(self.days)},
            asofs={"OLD": "2024-01-04"},
            view_histories={"OLD": {"split": changed}},
        )

        mapped = [
            segment
            for segment in raw_segments(reports["OLD"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual(
            [("2024-01-08", 1)], [(segment["first_session_date"], segment["row_count"]) for segment in mapped]
        )
        self.assertEqual(
            [("2024-01-05", 1)],
            [
                (segment["first_session_date"], segment["row_count"])
                for segment in raw_segments(reports["OLD"], "uncorrelated")
            ],
        )

    def test_mapped_predecessor_tail_stops_at_successor_source_coverage(self):
        evidence = rename()
        reports = run(
            evidence,
            {"OLD": bars(self.days), "NEW": bars(self.days[:-1])},
            asofs={"OLD": "2024-01-04"},
        )

        mapped = [
            segment
            for segment in raw_segments(reports["OLD"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual(
            [("2024-01-05", 1)],
            [(segment["first_session_date"], segment["row_count"]) for segment in mapped],
        )
        self.assertEqual(
            [("2024-01-08", 1)],
            [
                (segment["first_session_date"], segment["row_count"])
                for segment in raw_segments(reports["OLD"], "uncorrelated")
            ],
        )

    def test_competing_predecessor_branch_vetoes_mapped_continuation(self):
        evidence = rename()
        add_name_change_edge(
            evidence,
            action_id="competing-transition",
            process_date="2024-01-05",
            predecessor=("other-id", "OTHER", "other-cusip", "Other predecessor"),
            successor=("new-id", "NEW", "new-cusip", "Continuing security"),
        )
        reports = run(
            evidence,
            {
                "OLD": bars(self.days),
                "OTHER": bars(self.days[:3]),
                "NEW": bars(self.days),
            },
            asofs={"OLD": "2024-01-04", "OTHER": "2024-01-04"},
        )

        self.assertFalse(
            any(
                segment["correlation_method"] == "accepted_name_change_mapped_continuation"
                for segment in raw_segments(reports["OLD"])
            )
        )
        self.assertEqual([2], [segment["row_count"] for segment in raw_segments(reports["OLD"], "uncorrelated")])

    def test_equal_rows_without_accepted_continuity_do_not_establish_identity(self):
        evidence = rename()
        evidence["corporate_actions"] = []
        reports = run(
            evidence,
            {"OLD": bars(self.days), "NEW": bars(self.days)},
            asofs={"OLD": "2024-01-04"},
        )
        self.assertFalse(
            any(
                segment["correlation_method"] == "accepted_name_change_mapped_continuation"
                for segment in raw_segments(reports["OLD"])
            )
        )

    def test_concurrent_instruments_keep_both_own_histories_and_do_not_merge(self):
        # Conversions, acquisitions and temporary lines have the same evidence
        # shape; names, ticker suffixes and conversion ratios cannot change it.
        for old, new in (("MIDDV", "MIDD"), ("GNTY", "GBCI"), ("ARBKL", "ARBK"), ("CWEN.A", "CWEN")):
            with self.subTest(old=old, new=new):
                reports = run(rename(old=old, new=new), {old: bars(self.days[1:3], 90), new: bars(self.days)})
                old_segments, new_segments = raw_segments(reports[old]), raw_segments(reports[new])
                self.assertEqual([2], [segment["row_count"] for segment in old_segments])
                self.assertEqual([5], [segment["row_count"] for segment in new_segments])
                self.assertEqual(["old-id"], old_segments[0]["security_identity"]["member_alpaca_asset_ids"])
                self.assertEqual(["new-id"], new_segments[0]["security_identity"]["member_alpaca_asset_ids"])
                self.assertEqual(new, new_segments[0]["historical_symbol"])

    def test_shared_uuid_does_not_assign_a_parallel_lines_unrelated_prefix(self):
        for old, new in (("IIVIP", "IIVI"), ("ADEAV", "ADEA")):
            with self.subTest(old=old, new=new):
                reports = run(
                    rename(same_uuid=True, old=old, new=new),
                    {old: bars(self.days[1:3], 90), new: bars(self.days)},
                )
                self.assertEqual(
                    [3], [segment["row_count"] for segment in raw_segments(reports[new], "uncorrelated")]
                )
                self.assertEqual([2], [segment["row_count"] for segment in raw_segments(reports[new])])
                self.assertEqual(
                    "2024-01-03",
                    raw_segments(reports[old])[0]["ticker_ownership_interval"]["start_date_inclusive"],
                )

    def test_shared_uuid_alias_retains_history_despite_interior_price_vintage_difference(self):
        days = ("2023-12-29", *self.days)
        old, new = bars(days[:4]), bars(days)
        old[1].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "11.01"))
        reports = run(rename(same_uuid=True), {"OLD": old, "NEW": new})
        self.assertEqual([], raw_segments(reports["NEW"], "uncorrelated"))
        self.assertEqual(["OLD", "NEW"], [s["historical_symbol"] for s in raw_segments(reports["NEW"])])
        # The same discrepancy cannot certify continuity across different UUIDs.
        reports = run(rename(), {"OLD": old, "NEW": new})
        self.assertEqual([], reports["NEW"]["uuid_lineage_edges"])

    def test_successor_cannot_extend_predecessor_before_its_own_source(self):
        reports = run(
            rename(same_uuid=True),
            {"OLD": bars(self.days[1:3], 11), "NEW": bars(self.days)},
        )
        self.assertEqual([1], [s["row_count"] for s in raw_segments(reports["NEW"], "uncorrelated")])
        self.assertEqual("2024-01-03", raw_segments(reports["NEW"])[0]["first_session_date"])

    def test_missing_or_partial_source_cannot_supply_a_certificate(self):
        evidence = rename()
        inputs = inputs_for({"OLD": bars(self.days[:3]), "NEW": bars(self.days)})
        inputs["OLD"].local["downloads"][0]["rows"] += 1
        reports = correlate(Path("."), inputs, {symbol: evidence for symbol in inputs})
        self.assertEqual([], reports["NEW"]["uuid_lineage_edges"])
        reports = run(evidence, {"NEW": bars(self.days)})
        self.assertEqual([], reports["NEW"]["uuid_lineage_edges"])
        self.assertEqual([3], [s["row_count"] for s in raw_segments(reports["NEW"], "uncorrelated")])

    def test_explicit_date_conflict_fails_closed_in_both_builders_and_rows(self):
        evidence = rename()
        evidence["corporate_actions"][0]["effective_date"] = "2024-01-04"
        reports = run(evidence, {"OLD": bars(self.days[:3]), "NEW": bars(self.days)})
        for report in reports.values():
            self.assertEqual([], report["uuid_lineage_edges"])
            conflicts = [
                s
                for s in raw_segments(report, "uncorrelated")
                if s["correlation_rule"]["name"] == "name_change_boundary_conflict"
            ]
            self.assertEqual(
                [("2024-01-04", "2024-01-04")],
                [(s["first_session_date"], s["last_session_date"]) for s in conflicts],
            )
        self.assertEqual([2], [s["row_count"] for s in raw_segments(reports["OLD"])])
        self.assertEqual([2], [s["row_count"] for s in raw_segments(reports["NEW"])])

    def test_effective_date_alone_establishes_the_boundary(self):
        evidence = rename()
        action = evidence["corporate_actions"][0]
        action["effective_date"] = action.pop("process_date")
        reports = run(evidence, {"OLD": bars(self.days[:3]), "NEW": bars(self.days)})
        self.assertEqual([], raw_segments(reports["NEW"], "uncorrelated"))
        self.assertEqual(["OLD", "NEW"], [s["historical_symbol"] for s in raw_segments(reports["NEW"])])

    def test_component_arrival_conflict_does_not_choose_the_later_process_date(self):
        evidence = rename(old="SMAP", new="MSAI", when="2024-01-08")
        evidence["corporate_actions"].append(
            {
                "id": "component-arrival",
                "action_type": "unit_split",
                "effective_date": "2024-01-05",
                "old_symbol": "UNIT",
                "old_cusip": "unit-cusip",
                "new_symbol": "MSAI",
                "new_cusip": "new-cusip",
                "alternate_symbol": "WARRANT",
                "alternate_cusip": "warrant-cusip",
                "old_rate": "1",
                "new_rate": "1",
                "alternate_rate": "1",
            }
        )
        reports = run(evidence, {"SMAP": bars(self.days[:4]), "MSAI": bars(self.days)})
        self.assertEqual([], reports["MSAI"]["uuid_lineage_edges"])
        self.assertEqual([1], [s["row_count"] for s in raw_segments(reports["MSAI"])])
        self.assertEqual("2024-01-08", raw_segments(reports["MSAI"])[0]["first_session_date"])
        # A malformed component event cannot contradict an otherwise valid date.
        evidence["corporate_actions"][-1]["alternate_rate"] = "0"
        reports = run(evidence, {"SMAP": bars(self.days[:4]), "MSAI": bars(self.days)})
        self.assertEqual(1, len(reports["MSAI"]["uuid_lineage_edges"]))

    def test_current_reused_label_cannot_claim_rows_after_the_old_occupant_departed(self):
        evidence = rename()
        later = {"id": "later-etf", "symbol": "OLD", "name": "Later unrelated fund"}
        evidence["identities"].append(later)
        evidence["asset_lookups"] = [item for item in evidence["asset_lookups"] if item["lookup"] != "OLD"] + [
            {"lookup_kind": "symbol", "lookup": "OLD", "asset": later}
        ]
        reports = run(
            evidence,
            {"OLD": bars(self.days), "NEW": bars(self.days)},
            asofs={"OLD": "2024-01-04"},
        )
        self.assertEqual([3], [s["row_count"] for s in raw_segments(reports["OLD"])])
        self.assertEqual([2], [s["row_count"] for s in raw_segments(reports["OLD"], "uncorrelated")])
        self.assertEqual(
            ["old-id"], raw_segments(reports["OLD"])[0]["security_identity"]["member_alpaca_asset_ids"]
        )

    def test_stopped_parallel_source_does_not_erase_the_successors_history(self):
        reports = run(rename(), {"OLD": bars(self.days[:2], 90), "NEW": bars(self.days)})
        self.assertEqual([], reports["NEW"]["uuid_lineage_edges"])
        self.assertEqual([5], [s["row_count"] for s in raw_segments(reports["NEW"])])

    def test_matching_historical_span_does_not_certify_a_divergent_handoff(self):
        old, new = bars(self.days[:4]), bars(self.days)
        old[-1].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "99"))
        reports = run(rename(when="2024-01-08"), {"OLD": old, "NEW": new})
        self.assertEqual([], reports["NEW"]["uuid_lineage_edges"])
        self.assertEqual(["OLD", "NEW"], [s["historical_symbol"] for s in raw_segments(reports["NEW"])])
        self.assertEqual(
            ["old-id"], raw_segments(reports["NEW"])[0]["security_identity"]["member_alpaca_asset_ids"]
        )
        self.assertEqual([1], [s["row_count"] for s in raw_segments(reports["NEW"], "uncorrelated")])

    def test_inert_successor_padding_is_not_independent_concurrent_history(self):
        new = bars(self.days)
        for row in new[:3]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "5"), v="0", n="0", vw="0")
        reports = run(rename(), {"OLD": bars(self.days[:3], 90), "NEW": new})
        self.assertEqual([3], [s["row_count"] for s in raw_segments(reports["NEW"], "uncorrelated")])

    def test_current_reuse_of_old_label_cannot_erase_bounded_same_uuid_alias_proof(self):
        evidence = rename(same_uuid=True)
        evidence["identities"].append({"id": "later-id", "symbol": "OLD"})
        reports = run(evidence, {"OLD": bars(self.days[:3]), "NEW": bars(self.days)})
        self.assertEqual(["OLD", "NEW"], [s["historical_symbol"] for s in raw_segments(reports["NEW"])])

    def test_bounded_same_uuid_round_trip_keeps_exact_cutovers(self):
        evidence = rename(same_uuid=True, old="HOME", new="TEMP", when="2024-01-03")
        add_name_change_edge(
            evidence,
            action_id="return",
            process_date="2024-01-05",
            predecessor=("old-id", "TEMP", "new-cusip", "Security"),
            successor=("old-id", "HOME", "old-cusip", "Security"),
        )
        reports = run(evidence, {"HOME": bars(self.days), "TEMP": bars(self.days[1:3], 90)})
        self.assertEqual([], raw_segments(reports["HOME"], "uncorrelated"))
        self.assertEqual(
            ["HOME", "TEMP", "HOME"], [s["historical_symbol"] for s in raw_segments(reports["HOME"])]
        )


class NameChangeMetadataTests(unittest.TestCase):
    def test_retired_cusip_descriptors_do_not_pollute_the_continuing_security(self):
        old = {
            "id": "old-id",
            "symbol": "OLD",
            "name": "Unrelated ETF",
            "exchange": "ARCA",
            "class": "us_equity",
        }
        current = {
            "id": "new-id",
            "symbol": "NEW",
            "name": "Continuing company",
            "exchange": "NASDAQ",
            "class": "us_equity",
        }
        group = SimpleNamespace(
            assignments={
                "NEW": [
                    (
                        "raw",
                        "current_month",
                        {
                            "branch": "correlated",
                            "security_identity": {
                                "security_id": "public",
                                "member_alpaca_asset_ids": ["old-id", "new-id"],
                            },
                        },
                    )
                ]
            },
            evidence={
                "sources": {
                    "NEW": {
                        "evidence": {
                            "asset_lookups": [
                                {"lookup_kind": "cusip", "lookup": "old-cusip", "asset": old},
                                {"lookup_kind": "symbol", "lookup": "OLD", "asset": {**old, "id": "etf-id"}},
                                {"lookup_kind": "symbol", "lookup": "NEW", "asset": current},
                            ]
                        }
                    }
                }
            },
        )
        members, observations = _metadata(group, "public")
        self.assertEqual({"old-id", "new-id"}, members)
        self.assertEqual({"Continuing company"}, {o["value"] for o in observations if o["field"] == "name"})
        self.assertEqual({"NASDAQ"}, {o["value"] for o in observations if o["field"] == "exchange"})
        self.assertEqual({"old-cusip"}, {o["value"] for o in observations if o["field"] == "cusip"})

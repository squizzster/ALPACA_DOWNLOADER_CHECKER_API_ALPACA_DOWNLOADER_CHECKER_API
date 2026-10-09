import argparse
import copy
import hashlib
import io
import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import libs.alpaca_end_of_day_security_correlation.security_master as security_master_module
from libs.alpaca_end_of_day_pipeline import validate_and_publish_alpaca_download
from libs.alpaca_end_of_day_security_correlation import (
    CORRELATION_VERSION,
    authenticate_security_master,
    correlate_and_publish_alpaca_end_of_day_security_segments,
    correlated_pipeline_path,
    run_security_correlation_command,
    security_correlation_manifest_path,
    security_id_for_alpaca_asset_ids,
    security_master_manifest_path,
    security_master_path,
    uncorrelated_pipeline_path,
    validate_correlation_change_log,
)
from libs.alpaca_end_of_day_security_correlation.evidence import (
    build_ticker_ownership_intervals,
    ownership_interval_rule_order,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    correlate_download_rows,
    row_correlation_rule_order,
)
from libs.alpaca_end_of_day_security_correlation.terminal_fallback import (
    TERMINAL_FALLBACK_WARNING,
)
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_security_lineage import (
    EXCHANGE_TEST_SYMBOL,
    HISTORICAL_MARKET_DATA_ONLY,
    inspect_local_downloads,
    market_data_series_identity,
)
from libs.common.file_integrity import calculate_file_sha256

TEST_FIXTURES_DIRECTORY = Path(__file__).with_name("fixtures")


def load_bars(name: str) -> list[dict]:
    return json.loads((TEST_FIXTURES_DIRECTORY / name).read_text(encoding="utf-8"))


def correlation_source(symbol: str, *, asof: str) -> dict:
    return {
        "data_file": "raw.csv.gz",
        "download_manifest": "raw.csv.gz.manifest.json",
        "download_sha256": "a" * 64,
        "adjustment": "raw",
        "requested_symbol": symbol,
        "asof": asof,
        "request_from": "2016-01-01T00:00:00Z",
        "request_to": "2026-10-07T00:00:00Z",
        "feed": "sip",
        "timeframe": "1Day",
        "pipeline_state": "cached",
        "validation_pipeline_version": 14,
        "recorded_integrity_failure": None,
    }


def complete_query(symbol: str) -> dict:
    return {
        "input_symbol": symbol,
        "corporate_actions_start": "2016-01-01",
        "corporate_actions_end": "2026-10-06",
        "corporate_actions_data_quality": "complete",
    }


def asset(asset_id: str, symbol: str, name: str) -> dict:
    return {
        "id": asset_id,
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": symbol,
        "name": name,
        "status": "active",
        "tradable": True,
        "lookup_symbols": [symbol],
        "lookup_cusips": [],
    }


def occupancy(
    symbol: str,
    cusip: str,
    asset_id: str,
    *,
    action_type: str,
    action_id: str,
    process_date: str,
    role: str,
) -> dict:
    return {
        "symbol": symbol,
        "cusip": cusip,
        "asset_id": asset_id,
        "evidence": [
            {
                "symbol": symbol,
                "cusip": cusip,
                "role": role,
                "action_type": action_type,
                "action_id": action_id,
                "process_date": process_date,
            }
        ],
    }


def fb_lineage() -> dict:
    meta_id = "meta-id"
    etf_id = "fb-etf-id"
    name_change = {
        "action_type": "name_change",
        "id": "facebook-name-change",
        "process_date": "2022-06-09",
        "old_symbol": "FB",
        "old_cusip": "30303M102",
        "new_symbol": "META",
        "new_cusip": "30303M102",
    }
    dividend = {
        "action_type": "cash_dividend",
        "id": "fb-etf-dividend",
        "process_date": "2025-09-30",
        "ex_date": "2025-09-24",
        "symbol": "FB",
        "cusip": "74349Y613",
    }
    return {
        "query": complete_query("FB"),
        "corporate_actions": [name_change, dividend],
        "symbol_occupancy_evidence": [
            occupancy(
                "FB",
                "30303M102",
                meta_id,
                action_type="name_change",
                action_id=name_change["id"],
                process_date=name_change["process_date"],
                role="old",
            ),
            occupancy(
                "META",
                "30303M102",
                meta_id,
                action_type="name_change",
                action_id=name_change["id"],
                process_date=name_change["process_date"],
                role="new",
            ),
            occupancy(
                "FB",
                "74349Y613",
                etf_id,
                action_type="cash_dividend",
                action_id=dividend["id"],
                process_date=dividend["process_date"],
                role="symbol",
            ),
        ],
        "identities": [
            {**asset(meta_id, "META", "Meta Platforms, Inc."), "lookup_cusips": ["30303M102"]},
            {
                **asset(etf_id, "FB", "ProShares S&P 500 Dynamic Buffer ETF"),
                "lookup_cusips": ["74349Y613"],
            },
        ],
        "asset_lookups": [
            {
                "lookup": "FB",
                "lookup_kind": "symbol",
                "found": True,
                "asset": asset(etf_id, "FB", "ProShares S&P 500 Dynamic Buffer ETF"),
            }
        ],
    }


def twtr_lineage() -> dict:
    twitter_asset_id = "twitter-asset-id"
    cash_merger = {
        "action_type": "cash_merger",
        "id": "twitter-cash-merger",
        "process_date": "2022-10-28",
        "effective_date": "2022-10-28",
        "acquiree_symbol": "TWTR",
        "acquiree_cusip": "90184L102",
        "rate": 54.2,
    }
    twitter_asset = {
        **asset(twitter_asset_id, "TWTR", "Twitter, Inc."),
        "exchange": "NYSE",
        "status": "inactive",
        "tradable": False,
    }
    return {
        "query": complete_query("TWTR"),
        "corporate_actions": [cash_merger],
        "symbol_occupancy_evidence": [
            occupancy(
                "TWTR",
                "90184L102",
                twitter_asset_id,
                action_type="cash_merger",
                action_id=cash_merger["id"],
                process_date=cash_merger["process_date"],
                role="acquiree",
            )
        ],
        "identities": [{**twitter_asset, "lookup_cusips": ["90184L102"]}],
        "asset_lookups": [
            {
                "lookup": "TWTR",
                "lookup_kind": "symbol",
                "found": True,
                "asset": twitter_asset,
            }
        ],
    }


def siri_lineage() -> dict:
    old_siri_asset_id = "old-siri-asset-id"
    current_siri_asset_id = "current-siri-asset-id"
    lsxma_asset_id = "lsxma-asset-id"
    same_ticker_transition = {
        "action_type": "stock_merger",
        "id": "siri-stock-merger",
        "process_date": "2024-09-10",
        "effective_date": "2024-09-10",
        "acquiree_symbol": "SIRI",
        "acquiree_cusip": "82968B103",
        "acquiree_rate": 1,
        "acquirer_symbol": "SIRI",
        "acquirer_cusip": "829933100",
        "acquirer_rate": 0.1,
    }
    unrelated_merger_input = {
        "action_type": "stock_merger",
        "id": "lsxma-stock-merger",
        "process_date": "2024-09-10",
        "effective_date": "2024-09-10",
        "acquiree_symbol": "LSXMA",
        "acquiree_cusip": "531229409",
        "acquiree_rate": 1,
        "acquirer_symbol": "SIRI",
        "acquirer_cusip": "829933100",
        "acquirer_rate": 0.8375,
    }
    current_siri = asset(current_siri_asset_id, "SIRI", "SiriusXM Holdings Inc.")
    return {
        "query": complete_query("SIRI"),
        "corporate_actions": [same_ticker_transition, unrelated_merger_input],
        "symbol_occupancy_evidence": [
            occupancy(
                "SIRI",
                "82968B103",
                old_siri_asset_id,
                action_type="stock_merger",
                action_id=same_ticker_transition["id"],
                process_date=same_ticker_transition["process_date"],
                role="acquiree",
            ),
            occupancy(
                "SIRI",
                "829933100",
                current_siri_asset_id,
                action_type="stock_merger",
                action_id=same_ticker_transition["id"],
                process_date=same_ticker_transition["process_date"],
                role="acquirer",
            ),
            occupancy(
                "LSXMA",
                "531229409",
                lsxma_asset_id,
                action_type="stock_merger",
                action_id=unrelated_merger_input["id"],
                process_date=unrelated_merger_input["process_date"],
                role="acquiree",
            ),
        ],
        "identities": [
            {
                **asset(old_siri_asset_id, "SIRI", "SiriusXM Holdings Inc."),
                "status": "inactive",
                "tradable": False,
                "lookup_cusips": ["82968B103"],
            },
            {**current_siri, "lookup_cusips": ["829933100"]},
            {
                **asset(lsxma_asset_id, "LSXMA", "Liberty SiriusXM Common Stock"),
                "status": "inactive",
                "tradable": False,
                "lookup_cusips": ["531229409"],
            },
        ],
        "asset_lookups": [
            {
                "lookup": "SIRI",
                "lookup_kind": "symbol",
                "found": True,
                "asset": current_siri,
            }
        ],
    }


def dell_lineage() -> dict:
    dell_asset_id = "dell-asset-id"
    split = {
        "action_type": "forward_split",
        "id": "dell-forward-split",
        "process_date": "2021-11-02",
        "symbol": "DELL",
        "cusip": "24703L202",
    }
    dell_asset = {**asset(dell_asset_id, "DELL", "Dell Technologies Inc."), "exchange": "NYSE"}
    return {
        "query": complete_query("DELL"),
        "corporate_actions": [split],
        "symbol_occupancy_evidence": [
            occupancy(
                "DELL",
                "24703L202",
                dell_asset_id,
                action_type="forward_split",
                action_id=split["id"],
                process_date=split["process_date"],
                role="symbol",
            )
        ],
        "identities": [{**dell_asset, "lookup_cusips": ["24703L202"]}],
        "asset_lookups": [
            {
                "lookup": "DELL",
                "lookup_kind": "symbol",
                "found": True,
                "asset": dell_asset,
            }
        ],
    }


def b_lineage() -> dict:
    barnes_id = "barnes-id"
    historical_barrick_id = "historical-barrick-id"
    current_barrick_id = "current-barrick-id"
    merger_one = {
        "action_type": "cash_merger",
        "id": "barnes-merger-one",
        "process_date": "2025-01-27",
        "effective_date": "2025-01-27",
        "acquiree_symbol": "B",
        "acquiree_cusip": "067806109",
    }
    merger_two = {
        **merger_one,
        "id": "barnes-merger-two",
        "process_date": "2025-01-28",
        "effective_date": "2025-01-28",
    }
    name_change = {
        "action_type": "name_change",
        "id": "barrick-name-change",
        "process_date": "2025-05-09",
        "old_symbol": "GOLD",
        "old_cusip": "067901108",
        "new_symbol": "B",
        "new_cusip": "06849F108",
    }
    current_barrick = asset(current_barrick_id, "B", "Barrick Mining Corporation")
    return {
        "query": complete_query("B"),
        "corporate_actions": [merger_one, merger_two, name_change],
        "symbol_occupancy_evidence": [
            occupancy(
                "B",
                "067806109",
                barnes_id,
                action_type="cash_merger",
                action_id=merger_one["id"],
                process_date=merger_one["process_date"],
                role="acquiree",
            ),
            occupancy(
                "GOLD",
                "067901108",
                historical_barrick_id,
                action_type="name_change",
                action_id=name_change["id"],
                process_date=name_change["process_date"],
                role="old",
            ),
            occupancy(
                "B",
                "06849F108",
                current_barrick_id,
                action_type="name_change",
                action_id=name_change["id"],
                process_date=name_change["process_date"],
                role="new",
            ),
        ],
        "identities": [
            {**asset(barnes_id, "B", "Barnes Group Inc."), "lookup_cusips": ["067806109"]},
            {
                **asset(historical_barrick_id, "GOLD", "Barrick Gold Corporation"),
                "lookup_cusips": ["067901108"],
            },
            {**current_barrick, "lookup_cusips": ["06849F108"]},
        ],
        "asset_lookups": [
            {
                "lookup": "B",
                "lookup_kind": "symbol",
                "found": True,
                "asset": current_barrick,
            }
        ],
    }


def fun_like_lineage() -> dict:
    old_fun_id = "old-fun-alpaca-id"
    current_fun_id = "current-fun-alpaca-id"
    old_dividend = {
        "action_type": "cash_dividend",
        "id": "old-fun-dividend",
        "process_date": "2024-06-20",
        "symbol": "FUN",
        "cusip": "150185106",
    }
    merger = {
        "action_type": "stock_merger",
        "id": "fun-stock-merger",
        "process_date": "2024-07-02",
        "effective_date": "2024-07-02",
        "acquiree_symbol": "SIX",
        "acquiree_cusip": "83001A102",
        "acquirer_symbol": "FUN",
        "acquirer_cusip": "83001C108",
    }
    current_fun = asset(current_fun_id, "FUN", "Six Flags Entertainment Corporation")
    old_fun = {
        **asset(old_fun_id, "FUN", "Six Flags Entertainment Corporation"),
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": [],
    }
    return {
        "query": complete_query("FUN"),
        "corporate_actions": [old_dividend, merger],
        "symbol_occupancy_evidence": [
            occupancy(
                "FUN",
                "150185106",
                old_fun_id,
                action_type="cash_dividend",
                action_id=old_dividend["id"],
                process_date=old_dividend["process_date"],
                role="symbol",
            ),
            occupancy(
                "FUN",
                "83001C108",
                current_fun_id,
                action_type="stock_merger",
                action_id=merger["id"],
                process_date=merger["process_date"],
                role="acquirer",
            ),
        ],
        "identities": [
            {**old_fun, "lookup_cusips": ["150185106"]},
            {**current_fun, "lookup_cusips": ["83001C108"]},
        ],
        "asset_lookups": [
            {
                "lookup": "FUN",
                "lookup_kind": "symbol",
                "found": True,
                "asset": current_fun,
            }
        ],
    }


def fun_like_source() -> dict:
    source = correlation_source("FUN", asof="2024-07-03")
    source["request_from"] = "2024-06-28T00:00:00Z"
    source["request_to"] = "2024-07-03T00:00:00Z"
    return source


class RowCorrelationTests(unittest.TestCase):
    def test_unique_uuid_candidate_correlates_dell_with_boundary_warning(self) -> None:
        lineage = dell_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], evidence_issues)
        self.assertEqual([], intervals)

        branches = correlate_download_rows(
            [
                {"t": "2018-12-28T05:00:00Z"},
                {"t": "2026-10-05T04:00:00Z"},
            ],
            correlation_source("DELL", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(2, branches["correlated"][0]["row_count"])
        self.assertEqual(
            "dell-asset-id",
            branches["correlated"][0]["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual("24703L202", branches["correlated"][0]["canonical_security"]["cusip"])
        self.assertEqual(
            {"order": 4, "name": "unique_uuid_candidate_with_warnings"},
            branches["correlated"][0]["correlation_rule"],
        )
        self.assertIn("missing_dated_ticker_ownership_interval", branches["correlated"][0]["warnings"])
        self.assertIsNone(branches["correlated"][0]["ticker_ownership_interval"])

    def test_cash_merger_terminal_rule_correlates_twtr_rows(self) -> None:
        lineage = twtr_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], evidence_issues)
        self.assertEqual(1, len(intervals))
        interval_report = intervals[0].as_report_object()
        self.assertIsNone(interval_report["start_date_inclusive"])
        self.assertEqual("2022-10-28", interval_report["end_date_exclusive"])
        self.assertEqual(
            [
                {
                    "order": 3,
                    "name": "cash_merger_acquiree_terminal_boundary",
                    "supporting_corporate_action_ids": ["twitter-cash-merger"],
                },
            ],
            interval_report["ownership_interval_rules"],
        )

        branches = correlate_download_rows(
            [
                {"t": "2016-01-04T05:00:00Z"},
                {"t": "2022-10-27T04:00:00Z"},
            ],
            correlation_source("TWTR", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(2, branches["correlated"][0]["row_count"])
        self.assertEqual("90184L102", branches["correlated"][0]["canonical_security"]["cusip"])
        self.assertEqual(
            {"order": 3, "name": "requested_symbol_dated_uuid_interval"},
            branches["correlated"][0]["correlation_rule"],
        )

    def test_same_ticker_stock_merger_transitions_between_exact_uuids(self) -> None:
        lineage = siri_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        uuid_edges, edge_issues = build_uuid_lineage_edges(lineage)

        self.assertEqual([], evidence_issues)
        self.assertEqual([], edge_issues)
        self.assertEqual(2, len(intervals))
        old_interval, current_interval = intervals
        self.assertEqual("old-siri-asset-id", old_interval.asset_id)
        self.assertIsNone(old_interval.start_date_inclusive)
        self.assertEqual("2024-09-10", old_interval.end_date_exclusive.isoformat())
        self.assertEqual("current-siri-asset-id", current_interval.asset_id)
        self.assertEqual("2024-09-10", current_interval.start_date_inclusive.isoformat())
        self.assertIsNone(current_interval.end_date_exclusive)
        self.assertEqual(
            [
                {
                    "order": 2,
                    "name": "same_ticker_stock_merger_uuid_transition",
                    "supporting_corporate_action_ids": ["siri-stock-merger"],
                }
            ],
            old_interval.as_report_object()["ownership_interval_rules"],
        )
        self.assertEqual(1, len(uuid_edges))
        self.assertEqual("stock_merger", uuid_edges[0].edge_type)
        self.assertEqual("old-siri-asset-id", uuid_edges[0].predecessor_asset_id)
        self.assertEqual("current-siri-asset-id", uuid_edges[0].successor_asset_id)

        branches = correlate_download_rows(
            [
                {"t": "2024-09-09T04:00:00Z"},
                {"t": "2024-09-10T04:00:00Z"},
            ],
            correlation_source("SIRI", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(2, len(branches["correlated"]))
        old_segment, current_segment = branches["correlated"]
        self.assertEqual(
            {
                "security_id": security_id_for_alpaca_asset_ids(["old-siri-asset-id", "current-siri-asset-id"]),
                "alpaca_asset_id": "old-siri-asset-id",
                "cusip": "82968B103",
            },
            old_segment["canonical_security"],
        )
        self.assertEqual(
            {
                "security_id": old_segment["canonical_security"]["security_id"],
                "alpaca_asset_id": "current-siri-asset-id",
                "cusip": "829933100",
            },
            current_segment["canonical_security"],
        )

    def test_rule_application_order_is_explicit(self) -> None:
        self.assertEqual(
            [
                {"order": 1, "name": "name_change_ticker_transition"},
                {"order": 2, "name": "same_ticker_stock_merger_uuid_transition"},
                {"order": 3, "name": "cash_merger_acquiree_terminal_boundary"},
            ],
            ownership_interval_rule_order(),
        )
        self.assertEqual(
            [
                {"order": 1, "name": "exchange_test_symbol_exclusion"},
                {"order": 2, "name": "asof_selected_uuid_lineage_interval"},
                {"order": 3, "name": "requested_symbol_dated_uuid_interval"},
                {"order": 4, "name": "unique_uuid_candidate_with_warnings"},
                {"order": 5, "name": "unresolved_uuid_candidates"},
                {"order": 6, "name": "two_uuid_continuous_ticker_history"},
            ],
            row_correlation_rule_order(),
        )

    def test_fb_rows_are_assigned_to_exact_meta_and_etf_uuids(self) -> None:
        lineage = fb_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        rows = [
            {"t": "2022-06-08T04:00:00Z"},
            {"t": "2025-06-26T04:00:00Z"},
        ]

        branches = correlate_download_rows(
            rows,
            correlation_source("FB", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], evidence_issues)
        self.assertEqual(2, len(branches["correlated"]))
        self.assertEqual([], branches["uncorrelated"])
        meta_segment, etf_segment = branches["correlated"]
        self.assertEqual("30303M102", meta_segment["canonical_security"]["cusip"])
        self.assertEqual("meta-id", meta_segment["canonical_security"]["alpaca_asset_id"])
        self.assertEqual("FB", meta_segment["historical_symbol"])
        self.assertEqual(
            ["facebook-name-change"],
            meta_segment["ticker_ownership_interval"]["supporting_corporate_action_ids"],
        )
        self.assertEqual("fb-etf-id", etf_segment["canonical_security"]["alpaca_asset_id"])
        self.assertEqual("74349Y613", etf_segment["canonical_security"]["cusip"])
        self.assertIn("missing_dated_ticker_ownership_interval", etf_segment["warnings"])
        self.assertEqual(2, meta_segment["row_count"] + etf_segment["row_count"])

    def test_asof_uuid_branch_routes_reused_b_ticker_to_exact_barrick_uuids(self) -> None:
        lineage = b_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        uuid_edges, edge_issues = build_uuid_lineage_edges(lineage)

        self.assertEqual([], edge_issues)
        self.assertEqual(1, len(uuid_edges))
        self.assertEqual("historical-barrick-id", uuid_edges[0].predecessor_asset_id)
        self.assertEqual("current-barrick-id", uuid_edges[0].successor_asset_id)
        self.assertEqual(
            ["cash_merger_boundary_disagreement"],
            [issue["reason_code"] for issue in evidence_issues],
        )

        branches = correlate_download_rows(
            [
                {"t": "2020-01-02T05:00:00Z"},
                {"t": "2025-01-27T05:00:00Z"},
                {"t": "2025-05-09T04:00:00Z"},
            ],
            correlation_source("B", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(2, len(branches["correlated"]))
        historical_segment, current_segment = branches["correlated"]
        self.assertEqual(
            "historical-barrick-id",
            historical_segment["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual("067901108", historical_segment["canonical_security"]["cusip"])
        self.assertEqual(2, historical_segment["row_count"])
        self.assertIn(
            "asof_uuid_lineage_overrode_requested_symbol_occupancy",
            historical_segment["warnings"],
        )
        self.assertEqual(
            "current-barrick-id",
            current_segment["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual("06849F108", current_segment["canonical_security"]["cusip"])
        self.assertNotIn(
            "barnes-id",
            {segment["canonical_security"]["alpaca_asset_id"] for segment in branches["correlated"]},
        )

    def test_asof_selected_meta_identity_beats_unrelated_historical_meta_ticker(self) -> None:
        lineage = fb_lineage()
        roundhill_change = {
            "action_type": "name_change",
            "id": "roundhill-name-change",
            "process_date": "2022-01-31",
            "old_symbol": "META",
            "old_cusip": "53656F417",
            "new_symbol": "METV",
            "new_cusip": "53656F417",
        }
        lineage["corporate_actions"].insert(0, roundhill_change)
        lineage["symbol_occupancy_evidence"].extend(
            [
                occupancy(
                    "META",
                    "53656F417",
                    "roundhill-id",
                    action_type="name_change",
                    action_id=roundhill_change["id"],
                    process_date=roundhill_change["process_date"],
                    role="old",
                ),
                occupancy(
                    "METV",
                    "53656F417",
                    "roundhill-id",
                    action_type="name_change",
                    action_id=roundhill_change["id"],
                    process_date=roundhill_change["process_date"],
                    role="new",
                ),
            ]
        )
        lineage["identities"].append(
            {
                **asset("roundhill-id", "METV", "Roundhill Metaverse ETF"),
                "lookup_cusips": ["53656F417"],
            }
        )
        intervals, _ = build_ticker_ownership_intervals(lineage)
        rows = [
            {"t": "2021-07-01T04:00:00Z"},
            {"t": "2023-07-03T04:00:00Z"},
        ]

        branches = correlate_download_rows(
            rows,
            correlation_source("META", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(["FB", "META"], [item["historical_symbol"] for item in branches["correlated"]])
        self.assertEqual(
            {"meta-id"},
            {item["canonical_security"]["alpaca_asset_id"] for item in branches["correlated"]},
        )

    def test_gapless_two_id_fun_history_receives_one_internal_security_id(self) -> None:
        lineage = fun_like_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], evidence_issues)
        self.assertEqual([], intervals)
        branches = correlate_download_rows(
            [
                {"t": "2024-06-28T04:00:00Z"},
                {"t": "2024-07-01T04:00:00Z"},
                {"t": "2024-07-02T04:00:00Z"},
            ],
            fun_like_source(),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(1, len(branches["correlated"]))
        segment = branches["correlated"][0]
        expected_security_id = security_id_for_alpaca_asset_ids(["old-fun-alpaca-id", "current-fun-alpaca-id"])
        self.assertEqual(64, len(expected_security_id))
        self.assertEqual(expected_security_id, segment["canonical_security"]["security_id"])
        self.assertIsNone(segment["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            ["current-fun-alpaca-id", "old-fun-alpaca-id"],
            segment["security_identity"]["member_alpaca_asset_ids"],
        )
        self.assertEqual(["FUN"], segment["security_identity"]["member_tickers"])
        self.assertIn(TERMINAL_FALLBACK_WARNING, segment["warnings"])
        fallback = segment["terminal_correlation_fallback"]
        self.assertTrue(fallback["eligible"])
        self.assertTrue(all(fallback["criteria"].values()))
        self.assertEqual(0, fallback["session_coverage"]["missing_session_count"])
        self.assertTrue(
            fallback["supplementary_corroboration"]["potential_identity_change_explanation_available"]
        )
        self.assertEqual(
            "2024-07-02",
            fallback["supplementary_corroboration"]["potential_identity_change_actions"][0]["effective_date"],
        )

    def test_two_id_history_with_one_missing_session_stays_uncorrelated(self) -> None:
        lineage = fun_like_lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)

        branches = correlate_download_rows(
            [
                {"t": "2024-06-28T04:00:00Z"},
                {"t": "2024-07-02T04:00:00Z"},
            ],
            fun_like_source(),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["correlated"])
        self.assertEqual(1, len(branches["uncorrelated"]))
        fallback = branches["uncorrelated"][0]["terminal_correlation_fallback"]
        self.assertFalse(fallback["eligible"])
        self.assertFalse(fallback["criteria"]["no_missing_expected_market_sessions"])
        self.assertEqual(["2024-07-01"], fallback["session_coverage"]["missing_sessions"])

    def test_historical_symbol_without_asset_retains_non_security_series_identity(self) -> None:
        series_identity = market_data_series_identity("CHLN")
        lineage = {
            "identities": [],
            "asset_lookups": [
                {
                    "lookup": "CHLN",
                    "lookup_kind": "symbol",
                    "found": False,
                    "outcome": "not_found",
                    "http_status": 404,
                    "asset": None,
                }
            ],
            "symbol_occupancy_evidence": [],
            "corporate_actions": [],
            "symbol_disposition": {
                "classification": HISTORICAL_MARKET_DATA_ONLY,
                "data_policy": "retain",
                "recurring_pipeline_action": "run",
                "market_data_series_identity": series_identity,
            },
        }

        branches = correlate_download_rows(
            [{"t": "2016-01-04T05:00:00Z"}],
            correlation_source("CHLN", asof="2026-10-06"),
            lineage,
            [],
        )

        self.assertEqual([], branches["correlated"])
        self.assertEqual(1, len(branches["uncorrelated"]))
        segment = branches["uncorrelated"][0]
        self.assertEqual(
            ["historical_market_data_without_alpaca_asset"],
            segment["reason_codes"],
        )
        self.assertEqual(series_identity, segment["market_data_series_identity"])
        self.assertFalse(segment["market_data_series_identity"]["is_security_identity"])

    def test_exchange_test_symbol_is_explicitly_excluded_from_security_correlation(self) -> None:
        test_asset = asset("test-asset-id", "ZXZZT", "NASDAQ TEST STOCK")
        lineage = {
            "identities": [{**test_asset, "lookup_symbols": ["ZXZZT"]}],
            "asset_lookups": [
                {
                    "lookup": "ZXZZT",
                    "lookup_kind": "symbol",
                    "found": True,
                    "outcome": "found",
                    "http_status": 200,
                    "asset": test_asset,
                }
            ],
            "symbol_occupancy_evidence": [],
            "corporate_actions": [],
            "symbol_disposition": {
                "classification": EXCHANGE_TEST_SYMBOL,
                "data_policy": "exclude",
                "recurring_pipeline_action": "skip",
                "market_data_series_identity": market_data_series_identity("ZXZZT"),
            },
        }

        branches = correlate_download_rows(
            [{"t": "2026-09-30T04:00:00Z"}],
            correlation_source("ZXZZT", asof="2026-10-06"),
            lineage,
            [],
        )

        self.assertEqual([], branches["correlated"])
        self.assertEqual(
            ["exchange_test_symbol_excluded"],
            branches["uncorrelated"][0]["reason_codes"],
        )
        self.assertEqual(
            ["test-asset-id"],
            [item["asset_id"] for item in branches["uncorrelated"][0]["candidate_securities"]],
        )

    def test_internal_security_id_is_order_independent_and_membership_sensitive(self) -> None:
        forward = security_id_for_alpaca_asset_ids(["alpaca-b", "alpaca-a"])
        reverse = security_id_for_alpaca_asset_ids(["alpaca-a", "alpaca-b"])

        self.assertEqual(forward, reverse)
        self.assertNotEqual(forward, security_id_for_alpaca_asset_ids(["alpaca-a"]))


class CorrelationPublicationTests(unittest.TestCase):
    def test_version_changelog_matches_correlation_version(self) -> None:
        self.assertEqual(CORRELATION_VERSION, len(validate_correlation_change_log()))

    def test_concurrent_security_master_updates_preserve_every_symbol(self) -> None:
        def source_contribution(
            correlation_report,
            correlation_receipt,
            online_by_master_id,
        ):
            symbol = correlation_report["symbol"]
            digest = hashlib.sha256(symbol.encode("ascii")).hexdigest()
            return {
                "symbol": symbol,
                "correlation_version": CORRELATION_VERSION,
                "correlation_generation_sha256": digest,
                "correlated_output_sha256": digest,
                "uncorrelated_output_sha256": digest,
                "identities": [],
            }

        original_authenticator = security_master_module._authenticate_optional_security_master

        def deliberately_slow_authenticator(repository_root):
            authenticated = original_authenticator(repository_root)
            time.sleep(0.05)
            return authenticated

        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            with (
                patch.object(
                    security_master_module,
                    "_source_contribution",
                    side_effect=source_contribution,
                ),
                patch.object(
                    security_master_module,
                    "_authenticate_optional_security_master",
                    side_effect=deliberately_slow_authenticator,
                ),
                ThreadPoolExecutor(max_workers=2) as executor,
            ):
                futures = [
                    executor.submit(
                        security_master_module.publish_security_master_update,
                        repository_root,
                        {"symbol": symbol},
                        {},
                    )
                    for symbol in ("FIRST", "SECOND")
                ]
                for future in futures:
                    future.result()

            authenticated_master = authenticate_security_master(repository_root)
            authenticated_symbols = [
                source["symbol"] for source in authenticated_master.sources_by_symbol.values()
            ]

        self.assertEqual(
            ["FIRST", "SECOND"],
            authenticated_symbols,
        )

    def test_source_only_update_reuses_the_security_inventory(self) -> None:
        generation = 0

        def source_contribution(
            correlation_report,
            correlation_receipt,
            online_by_master_id,
        ):
            digest = hashlib.sha256(f"TEST:{generation}".encode("ascii")).hexdigest()
            return {
                "symbol": "TEST",
                "correlation_version": CORRELATION_VERSION,
                "correlation_generation_sha256": digest,
                "correlated_output_sha256": digest,
                "uncorrelated_output_sha256": digest,
                "identities": [],
            }

        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            with patch.object(
                security_master_module,
                "_source_contribution",
                side_effect=source_contribution,
            ):
                security_master_module.publish_security_master_update(
                    repository_root,
                    {"symbol": "TEST"},
                    {},
                )
                first = authenticate_security_master(repository_root)
                first_source_inventory = first.master["source_inventory"]
                first_security_inventory = first.master["security_inventory"]
                generation = 1
                security_master_module.publish_security_master_update(
                    repository_root,
                    {"symbol": "TEST"},
                    {},
                )
                second = authenticate_security_master(repository_root)

            root_size = security_master_path(repository_root).stat().st_size

        self.assertNotEqual(first_source_inventory, second.master["source_inventory"])
        self.assertEqual(first_security_inventory, second.master["security_inventory"])
        self.assertEqual(1, second.master["source_count"])
        self.assertEqual(0, second.master["master_id_count"])
        self.assertLess(root_size, 2_500)

    def test_publishes_both_digest_bound_branches_and_then_uses_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            download_path = repository_root / "data" / "end_of_day" / "TEST" / "raw.csv.gz"
            request_identity = {
                "symbol": "TEST",
                "adjustment": "raw",
                "feed": "sip",
                "timeframe": "1Day",
                "request_from": "2024-01-01T00:00:00Z",
                "request_to": "2024-01-17T00:00:00Z",
                "asof": "2024-01-17",
            }
            validate_and_publish_alpaca_download(
                download_path,
                load_bars("ten_valid_daily_bars.json"),
                request_identity,
            )
            lineage_report = self._test_lineage_report(repository_root)
            lineage_calls = []

            def lineage_inspector(symbol, api_key_id, api_secret_key, *, repository_root):
                lineage_calls.append((symbol, api_key_id, api_secret_key, repository_root))
                return copy.deepcopy(lineage_report)

            source_state_before = self._file_state(download_path.parent)
            first_receipt = correlate_and_publish_alpaca_end_of_day_security_segments(
                "TEST",
                "key",
                "secret",
                repository_root=repository_root,
                lineage_inspector=lineage_inspector,
            )
            source_state_after = self._file_state(download_path.parent)

            correlated_path = correlated_pipeline_path(repository_root, "TEST")
            uncorrelated_path = uncorrelated_pipeline_path(repository_root, "TEST")
            receipt_path = security_correlation_manifest_path(repository_root, "TEST")
            self.assertEqual(download_path.parent, receipt_path.parent)
            self.assertEqual(
                source_state_before,
                {filename: source_state_after[filename] for filename in source_state_before},
            )
            self.assertEqual(
                {
                    "correlated.pipeline.json",
                    "security_correlation.manifest.json",
                    "uncorrelated.pipeline.json",
                },
                set(source_state_after) - set(source_state_before),
            )
            self.assertEqual("published", first_receipt["publication_status"])
            self.assertEqual("published", first_receipt["security_master"]["publication_status"])
            self.assertTrue(first_receipt["partition"]["row_partition_complete"])
            self.assertEqual(10, first_receipt["partition"]["correlated_row_count"])
            self.assertEqual(0, first_receipt["partition"]["uncorrelated_row_count"])
            self.assertEqual(
                calculate_file_sha256(correlated_path),
                first_receipt["outputs"]["correlated"]["sha256"],
            )
            self.assertEqual(
                calculate_file_sha256(uncorrelated_path),
                first_receipt["outputs"]["uncorrelated"]["sha256"],
            )
            receipt_document = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertNotIn("publication_status", receipt_document)
            self.assertEqual(CORRELATION_VERSION, receipt_document["correlation_version"])
            self.assertEqual(
                "alpaca_asset_identity_available",
                receipt_document["symbol_disposition"]["classification"],
            )
            self.assertEqual(1, len(receipt_document["uuid_lineage_edges"]))
            self.assertEqual(
                receipt_document["uuid_lineage_edges"],
                json.loads(correlated_path.read_text(encoding="utf-8"))["uuid_lineage_edges"],
            )
            authenticated_master = authenticate_security_master(repository_root)
            self.assertTrue(security_master_path(repository_root).is_file())
            self.assertTrue(security_master_manifest_path(repository_root).is_file())
            self.assertEqual(
                authenticated_master.master_sha256,
                first_receipt["security_master"]["output_sha256"],
            )
            self.assertNotIn(
                "warning",
                json.dumps(list(authenticated_master.identities_by_master_id.values())).lower(),
            )
            self.assertNotIn(
                "error",
                json.dumps(list(authenticated_master.identities_by_master_id.values())).lower(),
            )

            output_state_before_cache = self._file_state(receipt_path.parent)
            lineage_report["generated_at"] = "2099-01-01T00:00:00Z"
            lineage_report["lineage"]["api_request_count"] = 999
            second_receipt = correlate_and_publish_alpaca_end_of_day_security_segments(
                "TEST",
                "key",
                "secret",
                repository_root=repository_root,
                lineage_inspector=lineage_inspector,
            )
            output_state_after_cache = self._file_state(receipt_path.parent)

        self.assertEqual("cached", second_receipt["publication_status"])
        self.assertEqual("cached", second_receipt["security_master"]["publication_status"])
        self.assertEqual(output_state_before_cache, output_state_after_cache)
        self.assertEqual(2, len(lineage_calls))

    def test_command_serializes_the_publication_receipt_to_stdout(self) -> None:
        expected_receipt = {
            "publication_status": "published",
            "correlation_version": CORRELATION_VERSION,
        }

        def correlator(symbol, api_key_id, api_secret_key, *, repository_root):
            self.assertEqual(("FB", "key", "secret"), (symbol, api_key_id, api_secret_key))
            return expected_receipt

        output_stream = io.StringIO()
        receipt = run_security_correlation_command(
            argparse.Namespace(ticker="fb"),
            repository_root=Path("/example"),
            environment={
                "ALPACA_API_KEY_ID": "key",
                "ALPACA_API_SECRET_KEY": "secret",
            },
            output_stream=output_stream,
            correlator=correlator,
        )

        self.assertEqual(expected_receipt, receipt)
        self.assertEqual(expected_receipt, json.loads(output_stream.getvalue()))

    def _test_lineage_report(self, repository_root: Path) -> dict:
        old_asset_id = "old-test-id"
        current_asset_id = "current-test-id"
        name_change = {
            "action_type": "name_change",
            "id": "test-name-change",
            "process_date": "2024-01-10",
            "old_symbol": "TEST",
            "old_cusip": "111111111",
            "new_symbol": "NEW",
            "new_cusip": "111111111",
        }
        current_dividend = {
            "action_type": "cash_dividend",
            "id": "current-dividend",
            "process_date": "2024-01-16",
            "symbol": "TEST",
            "cusip": "222222222",
        }
        current_asset = asset(current_asset_id, "TEST", "Current Test Security")
        return {
            "report_version": 1,
            "generated_at": "2024-01-17T12:00:00Z",
            "symbol": "TEST",
            "local": inspect_local_downloads(repository_root, "TEST"),
            "lineage": {
                "api_request_count": 3,
                "corporate_actions": [name_change, current_dividend],
                "symbol_occupancy_evidence": [
                    occupancy(
                        "TEST",
                        "111111111",
                        old_asset_id,
                        action_type="name_change",
                        action_id=name_change["id"],
                        process_date=name_change["process_date"],
                        role="old",
                    ),
                    occupancy(
                        "NEW",
                        "111111111",
                        old_asset_id,
                        action_type="name_change",
                        action_id=name_change["id"],
                        process_date=name_change["process_date"],
                        role="new",
                    ),
                    occupancy(
                        "TEST",
                        "222222222",
                        current_asset_id,
                        action_type="cash_dividend",
                        action_id=current_dividend["id"],
                        process_date=current_dividend["process_date"],
                        role="symbol",
                    ),
                ],
                "identities": [
                    {
                        **asset(old_asset_id, "NEW", "Old Test Security"),
                        "lookup_cusips": ["111111111"],
                    },
                    {**current_asset, "lookup_cusips": ["222222222"]},
                ],
                "asset_lookups": [
                    {
                        "lookup": "TEST",
                        "lookup_kind": "symbol",
                        "found": True,
                        "outcome": "found",
                        "http_status": 200,
                        "asset": current_asset,
                    }
                ],
                "symbol_disposition": {
                    "classification": "alpaca_asset_identity_available",
                    "data_policy": "process",
                    "recurring_pipeline_action": "run",
                    "input_symbol_asset_lookup": {"outcome": "found", "http_status": 200},
                    "resolved_alpaca_asset_ids": [current_asset_id, old_asset_id],
                    "local_market_data": None,
                    "market_data_series_identity": None,
                    "test_symbol_evidence": None,
                },
            },
        }

    def _file_state(self, directory: Path) -> dict[str, tuple[int, int, str]]:
        return {
            path.name: (
                path.stat().st_size,
                path.stat().st_mtime_ns,
                calculate_file_sha256(path),
            )
            for path in directory.iterdir()
            if path.is_file()
        }


if __name__ == "__main__":
    unittest.main()

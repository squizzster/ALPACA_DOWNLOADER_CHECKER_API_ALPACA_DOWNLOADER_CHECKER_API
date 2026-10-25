import copy
import json
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from libs.alpaca_end_of_day_security_correlation import (
    CORRELATION_VERSION,
    SUPPORTED_CORRELATION_VERSIONS,
    build_security_correlation_report,
    security_id_for_alpaca_asset_ids,
    validate_correlation_change_log,
)
from libs.alpaca_end_of_day_security_correlation.action_identity import complete_stock_merger_endpoints
from libs.alpaca_end_of_day_security_correlation.current_symbol_reuse import (
    CURRENT_SYMBOL_REUSE_RULE_NAME,
)
from libs.alpaca_end_of_day_security_correlation.evidence import (
    build_ticker_ownership_intervals,
    ownership_interval_rule_order,
)
from libs.alpaca_end_of_day_security_correlation.evidence_digest import lineage_evidence_sha256
from libs.alpaca_end_of_day_security_correlation.identity_partition import (
    _assess_candidate,
    _transition_candidates,
)
from libs.alpaca_end_of_day_security_correlation.name_change import NameChangeAssessment, NameChangeHistory
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    AppliedOwnershipIntervalRule,
    TickerOwnershipInterval,
    _constrain_or_append_interval,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    LATEST_DATED_OWNERSHIP_INTERVAL_RULE_NAME,
    _apply_asof_selected_identity_rule,
    _dated_same_symbol_predecessors,
    _RowRuleContext,
    correlate_download_rows,
    row_correlation_rule_order,
)
from libs.alpaca_end_of_day_security_correlation.terminal_fallback import (
    TERMINAL_FALLBACK_WARNING,
)
from libs.alpaca_end_of_day_security_correlation.ticker_reuse_gap import (
    adjustment_session_consensus,
)
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import (
    build_uuid_lineage_edges,
)
from libs.alpaca_lineage_build.pricing import empty_exclusions
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.alpaca_security_lineage import (
    EXCHANGE_TEST_SYMBOL,
    HISTORICAL_MARKET_DATA_ONLY,
    market_data_series_identity,
)
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

TEST_FIXTURES_DIRECTORY = Path(__file__).with_name("fixtures")


def certified_edges(lineage):
    """Supply accepted certificates to unit tests downstream of the evidence gate."""
    return build_uuid_lineage_edges(
        lineage,
        certified_name_changes=frozenset(
            a["id"] for a in lineage["corporate_actions"] if a.get("action_type") == "name_change"
        ),
    )


def certified_row_inputs(rows, source, lineage, intervals):
    """Bound synthetic history proofs to the rows in a row-rule precedence test.

    Full group tests in test_alpaca_name_change_evidence exercise the producer
    of these proofs, including missing, divergent and circular evidence.
    """
    sessions = [date.fromisoformat(row["t"][:10]) for row in rows]
    return {
        "uuid_lineage_edges": certified_edges(lineage)[0],
        "name_change_assessment": NameChangeAssessment(
            histories=tuple(
                NameChangeHistory(source["requested_symbol"], min(sessions), max(sessions), interval, "fixture")
                for interval in intervals
            )
        ),
    }


def load_bars(name: str) -> list[dict]:
    return json.loads((TEST_FIXTURES_DIRECTORY / name).read_text(encoding="utf-8"))


def complete_market_session_rows(start: str, end: str) -> list[dict[str, str]]:
    return [
        {"t": f"{session_date}T12:00:00Z"}
        for session_date in expected_market_session_coverage(start, end).session_dates
    ]


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


def add_name_change_edge(
    lineage: dict,
    *,
    action_id: str,
    process_date: str,
    predecessor: tuple[str, str, str, str],
    successor: tuple[str, str, str, str],
) -> None:
    predecessor_id, predecessor_symbol, predecessor_cusip, predecessor_name = predecessor
    successor_id, successor_symbol, successor_cusip, successor_name = successor
    action = {
        "action_type": "name_change",
        "id": action_id,
        "process_date": process_date,
        "old_symbol": predecessor_symbol,
        "old_cusip": predecessor_cusip,
        "new_symbol": successor_symbol,
        "new_cusip": successor_cusip,
    }
    lineage["corporate_actions"].append(action)
    lineage["symbol_occupancy_evidence"].extend(
        (
            occupancy(
                predecessor_symbol,
                predecessor_cusip,
                predecessor_id,
                action_type="name_change",
                action_id=action_id,
                process_date=process_date,
                role="old",
            ),
            occupancy(
                successor_symbol,
                successor_cusip,
                successor_id,
                action_type="name_change",
                action_id=action_id,
                process_date=process_date,
                role="new",
            ),
        )
    )
    known_asset_ids = {identity["id"] for identity in lineage["identities"]}
    for asset_id, symbol, cusip, name in (predecessor, successor):
        if asset_id in known_asset_ids:
            continue
        lineage["identities"].append(
            {
                **asset(asset_id, symbol, name),
                "lookup_cusips": [cusip],
            }
        )
        known_asset_ids.add(asset_id)


def current_symbol_reuse_lineage() -> dict:
    current_id = "current-reused-symbol-asset-id"
    previous_id = "previous-reused-symbol-asset-id"
    vacated_action = {
        "action_type": "name_change",
        "id": "vacated-reused-symbol",
        "process_date": "2024-01-02",
        "old_symbol": "REUS",
        "old_cusip": "111111111",
        "new_symbol": "OLD",
        "new_cusip": "111111111",
    }
    dividend = {
        "action_type": "cash_dividend",
        "id": "current-reused-symbol-dividend",
        "process_date": "2024-07-03",
        "ex_date": "2024-07-03",
        "symbol": "REUS",
        "cusip": "222222222",
    }
    current_asset = asset(current_id, "REUS", "Current Reused Security")
    current_asset["lookup_cusips"] = ["222222222"]
    previous_asset = asset(previous_id, "OLD", "Previous Reused Security")
    previous_asset["lookup_cusips"] = ["111111111"]
    return {
        "query": complete_query("REUS"),
        "corporate_actions": [vacated_action, dividend],
        "asset_lookups": [
            {
                "lookup": "REUS",
                "lookup_kind": "symbol",
                "found": True,
                "outcome": "found",
                "asset": current_asset,
            },
            {
                "lookup": "222222222",
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "asset": current_asset,
            },
            {
                "lookup": "111111111",
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "asset": previous_asset,
            },
        ],
        "identities": [current_asset, previous_asset],
        "symbol_occupancy_evidence": [
            occupancy(
                "REUS",
                "111111111",
                previous_id,
                action_type="name_change",
                action_id=vacated_action["id"],
                process_date=vacated_action["process_date"],
                role="old",
            ),
            occupancy(
                "REUS",
                "222222222",
                current_id,
                action_type="cash_dividend",
                action_id=dividend["id"],
                process_date=dividend["process_date"],
                role="symbol",
            ),
        ],
        "input_symbol_occupants": [],
        "unresolved_cusips": [],
        "warnings": [],
    }


def superseded_reused_symbol_interval() -> TickerOwnershipInterval:
    return TickerOwnershipInterval(
        historical_symbol="REUS",
        cusip="000000000",
        asset_id="superseded-open-interval-asset-id",
        start_date_inclusive=date(2020, 1, 2),
        end_date_exclusive=None,
        applied_rules=(
            AppliedOwnershipIntervalRule(
                order=1,
                name="name_change_ticker_transition",
                supporting_action_ids=("old-incoming-name-change",),
            ),
        ),
    )


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


def same_symbol_split_lineage() -> dict:
    old_asset = {
        **asset("old-split-asset-id", "SPLT", "Split Test Security"),
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": [],
        "lookup_cusips": ["111111111"],
    }
    new_asset = {
        **asset("new-split-asset-id", "SPLT", "Split Test Security"),
        "lookup_cusips": ["222222222"],
    }
    split = {
        "action_type": "reverse_split",
        "id": "same-symbol-reverse-split",
        "ex_date": "2026-09-29",
        "process_date": "2026-09-29",
        "symbol": "SPLT",
        "old_cusip": "111111111",
        "new_cusip": "222222222",
        "old_rate": 10,
        "new_rate": 1,
    }
    lineage = {
        "query": complete_query("SPLT"),
        "corporate_actions": [split],
        "symbol_occupancy_evidence": [],
        "identities": [old_asset, new_asset],
        "asset_lookups": [
            {
                "lookup": cusip,
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": split_asset,
            }
            for cusip, split_asset in (("111111111", old_asset), ("222222222", new_asset))
        ]
        + [
            {
                "lookup": "SPLT",
                "lookup_kind": "symbol",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": new_asset,
            }
        ],
    }
    return lineage


def dated_terminal_conflict_lineage(terminal_date: str) -> dict:
    requested_symbol = "TERM"
    current_asset = {
        **asset("undated-symbol-asset-id", requested_symbol, "Terminal Test Security"),
        "exchange": "NYSE",
        "status": "inactive",
        "tradable": False,
    }
    dated_asset = {
        **asset("dated-terminal-asset-id", requested_symbol, "Terminal Test Security"),
        "exchange": "NYSE",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": [],
        "lookup_cusips": ["123456789"],
    }
    merger = {
        "action_type": "cash_merger",
        "id": "terminal-test-merger",
        "process_date": terminal_date,
        "effective_date": terminal_date,
        "acquiree_symbol": requested_symbol,
        "acquiree_cusip": "123456789",
    }
    return {
        "query": complete_query(requested_symbol),
        "corporate_actions": [merger],
        "symbol_occupancy_evidence": [
            occupancy(
                requested_symbol,
                "123456789",
                dated_asset["id"],
                action_type="cash_merger",
                action_id=merger["id"],
                process_date=terminal_date,
                role="acquiree",
            )
        ],
        "identities": [current_asset, dated_asset],
        "asset_lookups": [
            {
                "lookup": requested_symbol,
                "lookup_kind": "symbol",
                "found": True,
                "asset": current_asset,
            },
            {
                "lookup": "123456789",
                "lookup_kind": "cusip",
                "found": True,
                "asset": dated_asset,
            },
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


class DistinctUuidTransitionTests(unittest.TestCase):
    @staticmethod
    def _lineage() -> dict:
        old_asset = asset("old-asset-id", "OLD", "Old issuer")
        old_asset["lookup_cusips"] = ["111111111"]
        new_asset = asset("new-asset-id", "NEW", "New issuer")
        new_asset["lookup_cusips"] = ["222222222"]
        return {
            "corporate_actions": [
                {
                    "action_type": "name_change",
                    "id": "transition-1",
                    "process_date": "2024-01-03",
                    "old_symbol": "OLD",
                    "old_cusip": "111111111",
                    "new_symbol": "NEW",
                    "new_cusip": "222222222",
                }
            ],
            "identities": [old_asset, new_asset],
            "asset_lookups": [
                {
                    "lookup_kind": "symbol",
                    "lookup": "OLD",
                    "found": True,
                    "asset": old_asset,
                },
                {
                    "lookup_kind": "symbol",
                    "lookup": "NEW",
                    "found": True,
                    "asset": new_asset,
                },
            ],
            "symbol_occupancy_evidence": [],
        }

    def _assessment(self, *, new_session: date = date(2024, 1, 4)):
        candidate = _transition_candidates(self._lineage())[0]
        supporting = tuple(
            (symbol, adjustment, "a" * 64)
            for symbol in ("OLD", "NEW")
            for adjustment in (
                "raw",
                "split",
                "dividend",
                "spin-off",
                "split,dividend",
                "split,spin-off",
                "dividend,spin-off",
                "all",
            )
        )
        return _assess_candidate(
            candidate,
            {
                "OLD": (date(2024, 1, 2),),
                "NEW": (new_session,),
            },
            supporting,
        )

    def test_one_session_boundary_partitions_both_combined_and_separate_sources(self) -> None:
        transition = self._assessment()
        self.assertIsNotNone(transition)
        lineage = self._lineage()

        combined = correlate_download_rows(
            [
                {"t": "2024-01-02T05:00:00Z"},
                {"t": "2024-01-04T05:00:00Z"},
            ],
            correlation_source("NEW", asof="2024-01-05"),
            lineage,
            [],
            distinct_uuid_transition=transition,
        )
        self.assertEqual([], combined["uncorrelated"])
        self.assertEqual(["OLD", "NEW"], [item["historical_symbol"] for item in combined["correlated"]])
        self.assertEqual(
            ["old-asset-id", "new-asset-id"],
            [item["canonical_security"]["alpaca_asset_id"] for item in combined["correlated"]],
        )
        self.assertEqual(
            ["one_session_distinct_uuid_transition"] * 2,
            [item["correlation_method"] for item in combined["correlated"]],
        )
        self.assertEqual(
            [["old-asset-id"], ["new-asset-id"]],
            [item["security_identity"]["member_alpaca_asset_ids"] for item in combined["correlated"]],
        )

        old_only = correlate_download_rows(
            [{"t": "2024-01-02T05:00:00Z"}],
            correlation_source("OLD", asof="2024-01-03"),
            lineage,
            [],
            distinct_uuid_transition=transition,
        )
        new_only = correlate_download_rows(
            [{"t": "2024-01-04T05:00:00Z"}],
            correlation_source("NEW", asof="2024-01-05"),
            lineage,
            [],
            distinct_uuid_transition=transition,
        )
        self.assertEqual(
            "old-asset-id",
            old_only["correlated"][0]["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual(
            "new-asset-id",
            new_only["correlated"][0]["canonical_security"]["alpaca_asset_id"],
        )

    def test_same_or_third_uuid_and_multi_session_gap_do_not_match(self) -> None:
        same_uuid = self._lineage()
        same_uuid["identities"][1]["id"] = "old-asset-id"
        same_uuid["asset_lookups"][1]["asset"]["id"] = "old-asset-id"
        self.assertEqual((), _transition_candidates(same_uuid))

        third_uuid = self._lineage()
        competing = asset("third-asset-id", "NEW", "New issuer")
        competing["lookup_cusips"] = ["333333333"]
        third_uuid["identities"].append(competing)
        self.assertEqual((), _transition_candidates(third_uuid))

        self.assertIsNone(self._assessment(new_session=date(2024, 1, 5)))

    def test_one_session_candidate_respects_existing_action_supersession(self) -> None:
        lineage = self._lineage()
        lineage["corporate_actions"] = []
        predecessor = ("old-asset-id", "OLD", "111111111", "Old issuer")
        for action_id, when, successor in (
            ("transition-1", "2024-01-03", ("new-asset-id", "NEW", "222222222", "New issuer")),
            ("correction", "2024-01-04", ("old-asset-id", "CORRECT", "333333333", "Old issuer")),
        ):
            add_name_change_edge(
                lineage, action_id=action_id, process_date=when, predecessor=predecessor, successor=successor
            )
        _, issues = build_uuid_lineage_edges(lineage)
        self.assertIn("transition-1", {issue.get("rejected_action_id") for issue in issues})
        self.assertEqual((), _transition_candidates(lineage))
        # The report adapter supplies this existing conclusion; no re-audit.
        with patch(
            "libs.alpaca_end_of_day_security_correlation.identity_partition.build_uuid_lineage_edges",
            side_effect=AssertionError("already calculated"),
        ):
            self.assertEqual((), _transition_candidates(lineage, evidence_issues=issues))

    def test_one_session_candidate_keeps_original_inference_without_corroboration(self) -> None:
        lineage = self._lineage()
        for identity in lineage["identities"]:
            identity["lookup_cusips"] = []
        self.assertEqual(1, len(_transition_candidates(lineage)))

    def test_conflicting_action_cusip_does_not_erase_dated_predecessor_evidence(self) -> None:
        lineage = self._lineage()
        lineage["corporate_actions"][0]["old_cusip"] = "222222222"
        lineage["corporate_actions"].append(
            {
                "action_type": "cash_dividend",
                "id": "earlier-dividend",
                "process_date": "2024-01-02",
                "symbol": "OLD",
                "cusip": "111111111",
            }
        )
        lineage["symbol_occupancy_evidence"] = [
            occupancy(
                "OLD",
                "111111111",
                "old-asset-id",
                action_type="cash_dividend",
                action_id="earlier-dividend",
                process_date="2024-01-02",
                role="symbol",
            ),
            *[
                occupancy(
                    symbol,
                    "222222222",
                    "new-asset-id",
                    action_type="name_change",
                    action_id="transition-1",
                    process_date="2024-01-03",
                    role=role,
                )
                for symbol, role in (("OLD", "old"), ("NEW", "new"))
            ],
        ]
        candidate = _transition_candidates(lineage)[0]
        transition = _assess_candidate(candidate, {"OLD": (date(2024, 1, 2),), "NEW": (date(2024, 1, 4),)}, ())
        self.assertIsNotNone(transition)
        intervals, _ = build_ticker_ownership_intervals(lineage)
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            with self.subTest(adjustment=adjustment):
                branches = correlate_download_rows(
                    [{"t": "2024-01-02T05:00:00Z"}, {"t": "2024-01-04T05:00:00Z"}],
                    {**correlation_source("NEW", asof="2024-01-05"), "adjustment": adjustment},
                    lineage,
                    intervals,
                    distinct_uuid_transition=transition,
                )
                self.assertEqual([], branches["uncorrelated"])
                self.assertEqual(
                    ["old-asset-id", "new-asset-id"],
                    [s["canonical_security"]["alpaca_asset_id"] for s in branches["correlated"]],
                )

    def test_one_session_routing_does_not_cut_an_accepted_continuation(self) -> None:
        lineage = self._lineage()
        lineage["symbol_occupancy_evidence"] = [
            occupancy(
                symbol,
                cusip,
                identifier,
                action_type="name_change",
                action_id="transition-1",
                process_date="2024-01-03",
                role=role,
            )
            for symbol, cusip, identifier, role in (
                ("OLD", "111111111", "old-asset-id", "old"),
                ("NEW", "222222222", "new-asset-id", "new"),
            )
        ]
        intervals, _ = build_ticker_ownership_intervals(lineage)
        expected_id = security_id_for_alpaca_asset_ids(("old-asset-id", "new-asset-id"))
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            with self.subTest(adjustment=adjustment):
                branches = correlate_download_rows(
                    [{"t": "2024-01-02T05:00:00Z"}, {"t": "2024-01-04T05:00:00Z"}],
                    {**correlation_source("NEW", asof="2024-01-05"), "adjustment": adjustment},
                    lineage,
                    intervals,
                    distinct_uuid_transition=self._assessment(),
                    **certified_row_inputs(
                        [{"t": "2024-01-02T05:00:00Z"}, {"t": "2024-01-04T05:00:00Z"}],
                        {**correlation_source("NEW", asof="2024-01-05"), "adjustment": adjustment},
                        lineage,
                        intervals,
                    ),
                )
                self.assertEqual([], branches["uncorrelated"])
                segments = branches["correlated"]
                self.assertEqual(["OLD", "NEW"], [s["historical_symbol"] for s in segments])
                self.assertEqual(
                    ["old-asset-id", "new-asset-id"],
                    [s["canonical_security"]["alpaca_asset_id"] for s in segments],
                )
                self.assertEqual([expected_id] * 2, [s["security_identity"]["security_id"] for s in segments])
                self.assertEqual(
                    ["explicit_uuid_lineage"] * 2, [s["security_identity"]["basis"] for s in segments]
                )


class CertifiedUuidEdgeTests(unittest.TestCase):
    def test_certified_uuid_edge_retains_both_members(self) -> None:
        lineage = DistinctUuidTransitionTests._lineage()
        lineage["symbol_occupancy_evidence"] = [
            occupancy(
                "OLD",
                "111111111",
                "old-asset-id",
                action_type="name_change",
                action_id="transition-1",
                process_date="2024-01-03",
                role="old",
            ),
            occupancy(
                "NEW",
                "222222222",
                "new-asset-id",
                action_type="name_change",
                action_id="transition-1",
                process_date="2024-01-03",
                role="new",
            ),
        ]
        edges, edge_issues = certified_edges(lineage)
        intervals, interval_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], edge_issues)
        self.assertEqual([], interval_issues)
        branches = correlate_download_rows(
            [{"t": "2024-01-02T05:00:00Z"}],
            correlation_source("OLD", asof="2024-01-02"),
            lineage,
            intervals,
            **certified_row_inputs(
                [{"t": "2024-01-02T05:00:00Z"}], correlation_source("OLD", asof="2024-01-02"), lineage, intervals
            ),
        )

        self.assertEqual([], branches["uncorrelated"])
        segment = branches["correlated"][0]
        self.assertEqual("explicit_uuid_lineage", segment["security_identity"]["basis"])
        self.assertEqual(
            ["new-asset-id", "old-asset-id"],
            segment["security_identity"]["member_alpaca_asset_ids"],
        )

    def test_membership_retains_both_name_change_and_split_edges(self) -> None:
        lineage = same_symbol_split_lineage()
        add_name_change_edge(
            lineage,
            action_id="spac-to-split-security",
            process_date="2024-01-03",
            predecessor=("spac-asset-id", "SPAC", "333333333", "Predecessor SPAC"),
            successor=("old-split-asset-id", "SPLT", "111111111", "Split Test Security"),
        )
        edges, edge_issues = certified_edges(lineage)
        intervals, interval_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], edge_issues)
        self.assertEqual([], interval_issues)
        expected_split_members = ["new-split-asset-id", "old-split-asset-id", "spac-asset-id"]
        for timestamp, expected_asset_id in (
            ("2026-09-28T04:00:00Z", "old-split-asset-id"),
            ("2026-09-29T04:00:00Z", "old-split-asset-id"),
        ):
            with self.subTest(timestamp=timestamp):
                branches = correlate_download_rows(
                    [{"t": timestamp}],
                    correlation_source("SPLT", asof="2026-10-06"),
                    lineage,
                    intervals,
                    **certified_row_inputs(
                        [{"t": timestamp}], correlation_source("SPLT", asof="2026-10-06"), lineage, intervals
                    ),
                )
                segment = branches["correlated"][0]
                self.assertEqual(expected_asset_id, segment["canonical_security"]["alpaca_asset_id"])
                self.assertEqual(
                    expected_split_members,
                    segment["security_identity"]["member_alpaca_asset_ids"],
                )
                self.assertIn(
                    "spac-asset-id",
                    segment["security_identity"]["member_alpaca_asset_ids"],
                )


class OwnershipEpisodeBoundaryTests(unittest.TestCase):
    def test_repeated_arrival_keeps_first_positive_start_and_earliest_end(self):
        first = TickerOwnershipInterval("LABEL", "111111111", "issue", date(2023, 12, 13), None, ())
        repeated = replace(first, start_date_inclusive=date(2023, 12, 21))
        terminal = replace(first, start_date_inclusive=None, end_date_exclusive=date(2024, 1, 5))
        later_terminal = replace(terminal, end_date_exclusive=date(2024, 1, 9))
        for constraints in (
            (first, repeated, terminal, later_terminal),
            (terminal, later_terminal, repeated, first),
            (repeated, later_terminal, first, terminal),
        ):
            with self.subTest(constraints=constraints):
                intervals = []
                for constraint in constraints:
                    self.assertTrue(_constrain_or_append_interval(intervals, constraint))
                self.assertEqual([replace(first, end_date_exclusive=date(2024, 1, 5))], intervals)

    def test_disjoint_or_touching_same_identity_episodes_are_not_joined(self):
        first = TickerOwnershipInterval("LABEL", "111111111", "issue", date(2024, 1, 2), date(2024, 1, 5), ())
        for start in (date(2024, 1, 5), date(2024, 2, 1)):
            later = replace(first, start_date_inclusive=start, end_date_exclusive=None)
            intervals = [first]
            self.assertTrue(_constrain_or_append_interval(intervals, later))
            self.assertEqual([first, later], intervals)

    def test_overlapping_constraint_cannot_bridge_two_existing_episodes(self):
        first = TickerOwnershipInterval("LABEL", "111111111", "issue", date(2024, 1, 2), date(2024, 1, 5), ())
        later = replace(first, start_date_inclusive=date(2024, 2, 1), end_date_exclusive=None)
        intervals = [first, later]
        self.assertFalse(_constrain_or_append_interval(intervals, replace(first, end_date_exclusive=None)))
        self.assertEqual([first, later], intervals)

    def test_start_preservation_requires_same_symbol_cusip_and_uuid(self):
        first = TickerOwnershipInterval("LABEL", "111111111", "issue", date(2024, 1, 2), None, ())
        for changed in ({"historical_symbol": "OTHER"}, {"cusip": "222222222"}, {"asset_id": "other-issue"}):
            later = replace(first, start_date_inclusive=date(2024, 1, 3), **changed)
            intervals = [first]
            self.assertTrue(_constrain_or_append_interval(intervals, later))
            self.assertEqual([first, later], intervals)

    def test_repeated_transition_is_independent_of_provider_action_order(self):
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        for when in ("2023-12-13", "2023-12-21"):
            add_name_change_edge(
                lineage,
                action_id=when,
                process_date=when,
                predecessor=("old-id", "OLD", "111111111", "Predecessor"),
                successor=("new-id", "NEW", "222222222", "Successor"),
            )
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        self.assertEqual(
            date(2023, 12, 13), next(i for i in intervals if i.asset_id == "new-id").start_date_inclusive
        )
        self.assertEqual(
            date(2023, 12, 13), next(i for i in intervals if i.asset_id == "old-id").end_date_exclusive
        )
        lineage["corporate_actions"].reverse()
        self.assertEqual((intervals, issues), build_ticker_ownership_intervals(lineage))


class DatedSameSymbolHandoffTests(unittest.TestCase):
    @staticmethod
    def lineage(symbol="HOLD", count=2):
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        stages = [(f"stage-{index}", symbol, str(index + 1) * 9, "Security") for index in range(count)]
        for index, (predecessor, successor) in enumerate(zip(stages, stages[1:])):
            add_name_change_edge(
                lineage,
                action_id=f"handoff-{index}",
                process_date=f"2024-01-0{index + 3}",
                predecessor=predecessor,
                successor=successor,
            )
        add_name_change_edge(
            lineage,
            action_id="rename-away",
            process_date="2024-01-05",
            predecessor=stages[-1],
            successor=(stages[-1][0], "RENAMED", stages[-1][2], "Security"),
        )
        return lineage

    def test_dated_chain_preserves_predecessor_and_exact_cutovers_in_every_view(self):
        for symbol in ("HOLD", "DIFFERENT"):
            for count in (2, 3):
                lineage = self.lineage(symbol, count)
                intervals, issues = build_ticker_ownership_intervals(lineage)
                self.assertEqual([], issues)
                for adjustment in ALPACA_ADJUSTMENT_MODES:
                    with self.subTest(symbol=symbol, count=count, adjustment=adjustment):
                        source = {**correlation_source(symbol, asof="2024-01-08"), "adjustment": adjustment}
                        rows = complete_market_session_rows("2024-01-02", "2024-01-05")
                        branches = correlate_download_rows(
                            rows,
                            source,
                            lineage,
                            intervals,
                            **certified_row_inputs(rows, source, lineage, intervals),
                        )
                        self.assertEqual([], branches["uncorrelated"])
                        expected = [f"stage-{min(index, count - 1)}" for index in range(len(rows))]
                        actual = [
                            segment["canonical_security"]["alpaca_asset_id"]
                            for segment in branches["correlated"]
                            for _ in range(segment["row_count"])
                        ]
                        self.assertEqual(expected, actual)
                        first = branches["correlated"][0]
                        self.assertEqual(
                            {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
                            first["correlation_rule"],
                        )
                        self.assertIn("inferred_lower_ownership_boundary", first["warnings"])
                        self.assertEqual(
                            [f"stage-{index}" for index in range(count)],
                            first["security_identity"]["member_alpaca_asset_ids"],
                        )
                        self.assertEqual(
                            branches,
                            correlate_download_rows(
                                rows,
                                source,
                                lineage,
                                list(reversed(intervals)),
                                **certified_row_inputs(rows, source, lineage, list(reversed(intervals))),
                            ),
                        )

    def test_same_symbol_stock_merger_can_establish_the_same_dated_handoff(self):
        lineage = self.lineage()
        action = lineage["corporate_actions"][0]
        lineage["corporate_actions"][0] = {
            "id": action["id"],
            "action_type": "stock_merger",
            "effective_date": action["process_date"],
            "acquiree_symbol": action["old_symbol"],
            "acquiree_cusip": action["old_cusip"],
            "acquirer_symbol": action["new_symbol"],
            "acquirer_cusip": action["new_cusip"],
        }
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        branches = correlate_download_rows(
            complete_market_session_rows("2024-01-02", "2024-01-05"),
            correlation_source("HOLD", asof="2024-01-08"),
            lineage,
            intervals,
        )
        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual("stage-0", branches["correlated"][0]["canonical_security"]["alpaca_asset_id"])

    def test_incomplete_overlapping_recurrent_or_unrelated_chains_gain_no_exception(self):
        lineage = self.lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)
        edges, _ = build_uuid_lineage_edges(lineage)
        predecessor, successor = [item for item in intervals if item.historical_symbol == "HOLD"]
        cases = {
            "gap": [predecessor, replace(successor, start_date_inclusive=date(2024, 1, 4))],
            "overlap": [predecessor, replace(successor, start_date_inclusive=date(2024, 1, 2))],
            "undated successor": [predecessor, replace(successor, start_date_inclusive=None)],
            "no terminal boundary": [replace(predecessor, end_date_exclusive=None), successor],
            "recurrence": [predecessor, replace(successor, asset_id=predecessor.asset_id)],
            "unrelated occupant": [
                predecessor,
                successor,
                TickerOwnershipInterval("HOLD", "999999999", "unrelated", date(2024, 1, 8), None, ()),
            ],
        }
        for label, candidates in cases.items():
            with self.subTest(label=label):
                self.assertEqual(
                    frozenset(), _dated_same_symbol_predecessors("HOLD", lineage, candidates, edges)
                )

    def test_only_an_accepted_exact_handoff_can_establish_continuity(self):
        lineage = self.lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)
        edges, _ = build_uuid_lineage_edges(lineage)
        handoff = edges[0]
        cases = {
            "missing or superseded edge": [],
            "wrong action": [replace(handoff, action_id="other-action")],
            "wrong CUSIP": [replace(handoff, predecessor_cusip="999999999")],
            "wrong UUID": [replace(handoff, predecessor_asset_id="unrelated")],
            "wrong date": [replace(handoff, effective_date=date(2024, 1, 4))],
            "foreign label": [replace(handoff, predecessor_symbol="OTHER")],
            "split only": [replace(handoff, edge_type="reverse_split")],
        }
        for label, candidates in cases.items():
            with self.subTest(label=label):
                self.assertEqual(
                    frozenset(), _dated_same_symbol_predecessors("HOLD", lineage, intervals, candidates)
                )

    def test_undated_extra_occupancy_or_current_lookup_vetoes_the_dated_subset(self):
        for kind in ("occupancy", "lookup"):
            with self.subTest(kind=kind):
                lineage = self.lineage()
                intervals, _ = build_ticker_ownership_intervals(lineage)
                edges, _ = build_uuid_lineage_edges(lineage)
                if kind == "occupancy":
                    lineage["symbol_occupancy_evidence"].append(
                        {"symbol": "HOLD", "cusip": "999999999", "asset_id": "unrelated", "evidence": []}
                    )
                else:
                    lineage["asset_lookups"] = [
                        {"lookup_kind": "symbol", "lookup": "HOLD", "asset": {"id": "unrelated"}}
                    ]
                self.assertEqual(frozenset(), _dated_same_symbol_predecessors("HOLD", lineage, intervals, edges))
                branches = correlate_download_rows(
                    [{"t": "2024-01-02T12:00:00Z"}],
                    correlation_source("HOLD", asof="2024-01-08"),
                    lineage,
                    intervals,
                )
                self.assertEqual([], branches["correlated"])
                self.assertEqual(
                    ["undated_requested_ticker_reuse_boundary"], branches["uncorrelated"][0]["reason_codes"]
                )

    def test_a_shared_future_successor_is_not_a_same_symbol_handoff(self):
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        for index, when in enumerate(("2024-01-03", "2024-01-05")):
            add_name_change_edge(
                lineage,
                action_id=f"branch-{index}",
                process_date=when,
                predecessor=(f"branch-{index}", "REUSED", str(index + 1) * 9, "Predecessor"),
                successor=("joined-id", "JOINED", "333333333", "Successor"),
            )
        intervals, _ = build_ticker_ownership_intervals(lineage)
        edges, _ = certified_edges(lineage)
        self.assertEqual(2, len(edges))
        self.assertEqual(frozenset(), _dated_same_symbol_predecessors("REUSED", lineage, intervals, edges))
        branches = correlate_download_rows(
            [{"t": "2024-01-04T12:00:00Z"}],
            correlation_source("REUSED", asof="2024-01-08"),
            lineage,
            intervals,
            **certified_row_inputs(
                [{"t": "2024-01-04T12:00:00Z"}],
                correlation_source("REUSED", asof="2024-01-08"),
                lineage,
                intervals,
            ),
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(
            ["undated_requested_ticker_reuse_boundary"], branches["uncorrelated"][0]["reason_codes"]
        )

    def test_pricing_gap_guard_still_precedes_the_supported_handoff(self):
        lineage = self.lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)
        branches = correlate_download_rows(
            [{"t": "2020-01-02T12:00:00Z"}, {"t": "2024-01-02T12:00:00Z"}],
            correlation_source("HOLD", asof="2024-01-08"),
            lineage,
            intervals,
        )
        self.assertEqual([], branches["correlated"])
        self.assertTrue(
            all(
                segment["correlation_rule"]["name"] == "ticker_reuse_gap_guard"
                for segment in branches["uncorrelated"]
            )
        )

    def test_concurrent_asof_predecessors_still_veto_the_supported_handoff(self):
        lineage = self.lineage()
        add_name_change_edge(
            lineage,
            action_id="other-predecessor",
            process_date="2024-01-03",
            predecessor=("other-id", "OTHER", "999999999", "Other security"),
            successor=("stage-1", "HOLD", "222222222", "Security"),
        )
        intervals, _ = build_ticker_ownership_intervals(lineage)
        branches = correlate_download_rows(
            [{"t": "2024-01-02T12:00:00Z"}],
            correlation_source("HOLD", asof="2024-01-04"),
            lineage,
            intervals,
            **certified_row_inputs(
                [{"t": "2024-01-02T12:00:00Z"}],
                correlation_source("HOLD", asof="2024-01-04"),
                lineage,
                intervals,
            ),
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(["ambiguous_asof_uuid_lineage_intervals"], branches["uncorrelated"][0]["reason_codes"])


class OmittedStockMergerEndpointTests(unittest.TestCase):
    @staticmethod
    def lineage():
        old = asset("old-id", "HOLD", "Old legal name")
        new = asset("new-id", "HOLD", "New legal name")
        old.update(status="inactive", tradable=False)
        return {
            "query": complete_query("HOLD"),
            "corporate_actions": [
                {
                    "action_type": "stock_merger",
                    "id": "handoff",
                    "effective_date": "2024-01-03",
                    "process_date": "2024-01-03",
                    "acquiree_symbol": "HOLD",
                    "acquirer_symbol": "",
                    "acquiree_cusip": "111111111",
                    "acquirer_cusip": "222222222",
                    "acquiree_rate": 1,
                    "acquirer_rate": 1,
                }
            ],
            "identities": [old, new],
            "symbol_occupancy_evidence": [],
            "asset_lookups": [
                {
                    "lookup_kind": "cusip",
                    "lookup": cusip,
                    "found": True,
                    "outcome": "found",
                    "http_status": 200,
                    "asset": security,
                }
                for cusip, security in (("111111111", old), ("222222222", new))
            ],
        }

    def test_shared_interpretation_preserves_raw_evidence_and_existing_cascade(self):
        lineage = self.lineage()
        # Unrelated cash acquirees must keep their own intervals, not intercept
        # the source's now-authenticated direct same-symbol handoff.
        for number, symbol in enumerate(("BUYONE", "BUYTWO"), 3):
            lineage["corporate_actions"].append(
                {
                    "action_type": "cash_merger",
                    "id": symbol,
                    "effective_date": "2025-01-03",
                    "process_date": "2025-01-03",
                    "acquiree_symbol": symbol,
                    "acquiree_cusip": str(number) * 9,
                    "acquirer_symbol": "HOLD",
                    "acquirer_cusip": "222222222",
                }
            )
            lineage["symbol_occupancy_evidence"].append(
                {
                    "symbol": symbol,
                    "cusip": str(number) * 9,
                    "asset_id": symbol + "-id",
                }
            )
        original = copy.deepcopy(lineage)
        intervals, issues = build_ticker_ownership_intervals(lineage)
        edges, edge_issues = build_uuid_lineage_edges(lineage)
        self.assertEqual([], issues)
        self.assertEqual([], edge_issues)
        self.assertEqual(4, len(intervals))
        self.assertEqual(1, len(edges))
        branches = correlate_download_rows(
            [{"t": "2024-01-02T05:00:00Z"}, {"t": "2024-01-03T05:00:00Z"}],
            correlation_source("HOLD", asof="2026-10-06"),
            lineage,
            intervals,
        )
        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(
            ["old-id", "new-id"],
            [segment["canonical_security"]["alpaca_asset_id"] for segment in branches["correlated"]],
        )
        for segment in branches["correlated"]:
            self.assertEqual(
                {"order": 9, "name": "asof_selected_uuid_lineage_interval"}, segment["correlation_rule"]
            )
            self.assertEqual(["new-id", "old-id"], segment["security_identity"]["member_alpaca_asset_ids"])
        self.assertEqual(original, lineage)

    def test_one_omitted_endpoint_on_either_side(self):
        for field in ("acquiree_symbol", "acquirer_symbol"):
            for missing in (None, ""):
                with self.subTest(field=field, missing=missing):
                    lineage = self.lineage()
                    action = lineage["corporate_actions"][0]
                    action.update(acquiree_symbol="HOLD", acquirer_symbol="HOLD")
                    action[field] = missing
                    self.assertEqual("HOLD", complete_stock_merger_endpoints(action, lineage).symbol)

    def test_missing_label_never_overrides_conflicting_or_insufficient_evidence(self):
        changes = {
            "both_missing": lambda e: e["corporate_actions"][0].update(acquiree_symbol=None),
            "explicit_other": lambda e: e["corporate_actions"][0].update(acquirer_symbol="OTHER"),
            "malformed_symbol": lambda e: e["corporate_actions"][0].update(acquirer_symbol=123),
            "lookup_missing": lambda e: e["asset_lookups"].pop(),
            "duplicate_lookup": lambda e: e["asset_lookups"].append(copy.deepcopy(e["asset_lookups"][0])),
            "lookup_failed": lambda e: e["asset_lookups"][1].update(found=False),
            "lookup_wrong_status": lambda e: e["asset_lookups"][1].update(http_status=404),
            "lookup_other_label": lambda e: e["asset_lookups"][1]["asset"].update(symbol="OTHER"),
            "lookup_non_equity": lambda e: e["asset_lookups"][1]["asset"].update({"class": "crypto"}),
            "lookup_otc": lambda e: e["asset_lookups"][1]["asset"].update(exchange="OTC"),
            "same_cusip": lambda e: e["corporate_actions"][0].update(acquirer_cusip="111111111"),
            "occupancy_conflict": lambda e: e["symbol_occupancy_evidence"].append(
                {
                    "symbol": "HOLD",
                    "cusip": "222222222",
                    "asset_id": "unrelated-id",
                }
            ),
        }
        for name, change in changes.items():
            with self.subTest(name=name):
                lineage = self.lineage()
                change(lineage)
                self.assertIsNone(complete_stock_merger_endpoints(lineage["corporate_actions"][0], lineage))
                self.assertEqual([], build_uuid_lineage_edges(lineage)[0])
                self.assertEqual([], build_ticker_ownership_intervals(lineage)[0])

    def test_existing_date_and_distinct_uuid_guards_still_apply(self):
        for invalid in ("date", "same_uuid"):
            with self.subTest(invalid=invalid):
                lineage = self.lineage()
                if invalid == "date":
                    lineage["corporate_actions"][0]["effective_date"] = "bad-date"
                else:
                    lineage["asset_lookups"][1]["asset"]["id"] = "old-id"
                self.assertEqual([], build_uuid_lineage_edges(lineage)[0])
                self.assertEqual([], build_ticker_ownership_intervals(lineage)[0])

    def test_completed_endpoint_preserves_prior_rename_chronology_but_keeps_membership(self):
        for case in ("third_uuid_arrival", "third_uuid_departure", "earlier_successor_arrival"):
            with self.subTest(case=case):
                lineage = self.lineage()
                successor = case == "earlier_successor_arrival"
                identity = "new-id" if successor else "alias-id"
                cusip = "222222222" if successor else "333333333"
                departing = case == "third_uuid_departure"
                old_symbol, new_symbol = ("HOLD", "LATER") if departing else ("BEFORE", "HOLD")
                rename_date = "2024-02-01" if departing else "2023-01-03"
                lineage["corporate_actions"].append(
                    {
                        "action_type": "name_change",
                        "id": "earlier-registry-claim",
                        "old_symbol": old_symbol,
                        "new_symbol": new_symbol,
                        "old_cusip": cusip,
                        "new_cusip": cusip,
                        "process_date": rename_date,
                    }
                )
                lineage["symbol_occupancy_evidence"] += [
                    {"symbol": symbol, "cusip": cusip, "asset_id": identity}
                    for symbol in (old_symbol, new_symbol)
                ]
                baseline = copy.deepcopy(lineage)
                baseline["corporate_actions"] = baseline["corporate_actions"][1:]
                expected = build_ticker_ownership_intervals(baseline)[0]
                actual, issues = build_ticker_ownership_intervals(lineage)
                self.assertEqual(expected, actual)
                self.assertEqual(
                    "same_ticker_stock_merger_conflicts_with_existing_ownership_intervals",
                    issues[0]["reason_code"],
                )
                edges, _ = build_uuid_lineage_edges(lineage)
                self.assertTrue(
                    any(
                        edge.predecessor_asset_id == "old-id" and edge.successor_asset_id == "new-id"
                        for edge in edges
                    )
                )

    def test_completed_endpoint_can_constrain_an_undated_successor_rename(self):
        lineage = self.lineage()
        lineage["corporate_actions"].append(
            {
                "action_type": "name_change",
                "id": "later-rename",
                "old_symbol": "HOLD",
                "new_symbol": "LATER",
                "old_cusip": "222222222",
                "new_cusip": "222222222",
                "process_date": "2024-02-01",
            }
        )
        lineage["symbol_occupancy_evidence"] = [
            {"symbol": symbol, "cusip": "222222222", "asset_id": "new-id"} for symbol in ("HOLD", "LATER")
        ]
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        successor = next(i for i in intervals if i.historical_symbol == "HOLD" and i.asset_id == "new-id")
        self.assertEqual(date(2024, 1, 3), successor.start_date_inclusive)
        self.assertEqual(date(2024, 2, 1), successor.end_date_exclusive)


class RowCorrelationTests(unittest.TestCase):
    @staticmethod
    def _overlapping_asof_lineage():
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        selected = ("selected-id", "CURRENT", "111111111", "Selected issue")
        for action_id, when, predecessor, successor in (
            (
                "earlier-label",
                "2019-11-06",
                ("selected-id", "EARLY", "111111111", "Selected issue"),
                ("selected-id", "PRIOR", "111111111", "Selected issue"),
            ),
            ("current-label", "2024-03-04", ("selected-id", "PRIOR", "111111111", "Selected issue"), selected),
            ("other-arrival", "2026-01-30", ("other-id", "CURRENT", "222222222", "Other predecessor"), selected),
        ):
            add_name_change_edge(
                lineage, action_id=action_id, process_date=when, predecessor=predecessor, successor=successor
            )
        return lineage

    def test_exact_asof_uuid_precedes_connected_predecessor_for_all_views(self):
        lineage = self._overlapping_asof_lineage()
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        rows = complete_market_session_rows("2019-11-06", "2026-01-30")
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            with self.subTest(adjustment=adjustment):
                source = {**correlation_source("CURRENT", asof="2026-10-15"), "adjustment": adjustment}
                branches = correlate_download_rows(
                    rows, source, lineage, intervals, **certified_row_inputs(rows, source, lineage, intervals)
                )
                self.assertEqual([], branches["uncorrelated"])
                self.assertEqual(
                    [("PRIOR", 1086), ("CURRENT", 479)],
                    [(s["historical_symbol"], s["row_count"]) for s in branches["correlated"]],
                )
                for segment in branches["correlated"]:
                    self.assertEqual("selected-id", segment["canonical_security"]["alpaca_asset_id"])
                    self.assertEqual(
                        {"order": 9, "name": "asof_selected_uuid_lineage_interval"}, segment["correlation_rule"]
                    )
                self.assertEqual(
                    branches,
                    correlate_download_rows(
                        rows,
                        source,
                        lineage,
                        list(reversed(intervals)),
                        **certified_row_inputs(rows, source, lineage, list(reversed(intervals))),
                    ),
                )

    def test_historical_asof_can_select_the_other_entity(self):
        lineage = self._overlapping_asof_lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)
        branches = correlate_download_rows(
            complete_market_session_rows("2019-11-06", "2024-03-04"),
            correlation_source("CURRENT", asof="2024-02-29"),
            lineage,
            intervals,
        )
        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(1086, branches["correlated"][0]["row_count"])
        self.assertEqual("CURRENT", branches["correlated"][0]["historical_symbol"])
        self.assertEqual("other-id", branches["correlated"][0]["canonical_security"]["alpaca_asset_id"])

    def test_temporary_label_and_following_sessions_use_selected_issue(self):
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        for when, old, new, old_id, old_cusip in (
            ("2024-05-30", "WI", "TEMP", "new-id", "111111111"),
            ("2024-05-31", "TEMP", "CURRENT", "new-id", "111111111"),
            ("2024-06-04", "CURRENT", "CURRENT", "old-id", "222222222"),
            ("2024-06-05", "CURRENT", "CURRENT", "old-id", "222222222"),
        ):
            add_name_change_edge(
                lineage,
                action_id=when,
                process_date=when,
                predecessor=(old_id, old, old_cusip, old_id),
                successor=("new-id", new, "111111111", "New issue"),
            )
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            branches = correlate_download_rows(
                complete_market_session_rows("2024-05-30", "2024-06-04"),
                {**correlation_source("CURRENT", asof="2026-10-15"), "adjustment": adjustment},
                lineage,
                intervals,
                **certified_row_inputs(
                    complete_market_session_rows("2024-05-30", "2024-06-04"),
                    {**correlation_source("CURRENT", asof="2026-10-15"), "adjustment": adjustment},
                    lineage,
                    intervals,
                ),
            )
            self.assertEqual([], branches["uncorrelated"])
            self.assertEqual(
                [("TEMP", 1), ("CURRENT", 2)],
                [(s["historical_symbol"], s["row_count"]) for s in branches["correlated"]],
            )
            self.assertTrue(
                all(s["canonical_security"]["alpaca_asset_id"] == "new-id" for s in branches["correlated"])
            )

    def test_exact_asof_preference_requires_one_dated_interval(self):
        anchor = TickerOwnershipInterval("CURRENT", "111111111", "selected-id", date(2024, 1, 5), None, ())
        earlier = replace(
            anchor, historical_symbol="EARLIER", start_date_inclusive=None, end_date_exclusive=date(2024, 1, 5)
        )
        other = replace(earlier, historical_symbol="OTHER", asset_id="other-id", cusip="222222222")
        context = _RowRuleContext(
            date(2024, 1, 3),
            "CURRENT",
            anchor,
            frozenset(("selected-id", "other-id")),
            [anchor, earlier, other],
            {},
            None,
            None,
        )
        for intervals in (
            context.intervals,
            [
                anchor,
                replace(earlier, start_date_inclusive=date(2024, 1, 2)),
                replace(earlier, historical_symbol="ALIAS"),
                other,
            ],
        ):
            with self.subTest(intervals=intervals):
                decision = _apply_asof_selected_identity_rule(replace(context, intervals=intervals))
                self.assertEqual("uncorrelated", decision.branch)
                self.assertEqual(("ambiguous_asof_uuid_lineage_intervals",), decision.reason_codes)
        self.assertIsNone(_apply_asof_selected_identity_rule(replace(context, selected_asof_interval=None)))

    def test_exchange_exclusion_precedes_exact_asof_uuid(self):
        lineage = self._overlapping_asof_lineage()
        lineage["symbol_disposition"] = {"classification": EXCHANGE_TEST_SYMBOL}
        intervals, _ = build_ticker_ownership_intervals(lineage)
        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}], correlation_source("CURRENT", asof="2026-10-15"), lineage, intervals
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(
            "exchange_test_symbol_exclusion", branches["uncorrelated"][0]["correlation_rule"]["name"]
        )

    def test_preserved_start_exposes_concurrent_predecessors_instead_of_selecting_one(self):
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        middle = ("middle-id", "MIDDLE", "222222222", "First branch")
        for when, predecessor, successor in (
            ("2023-01-20", ("first-id", "FIRST", "111111111", "First branch"), middle),
            ("2024-08-30", ("other-id", "OTHER", "333333333", "Other branch"), middle),
            ("2024-09-06", middle, ("final-id", "FINAL", "444444444", "Successor")),
        ):
            add_name_change_edge(
                lineage, action_id=when, process_date=when, predecessor=predecessor, successor=successor
            )
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        rows = complete_market_session_rows("2023-01-20", "2024-08-30")
        self.assertEqual(405, len(rows))
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            branches = correlate_download_rows(
                rows,
                {**correlation_source("FINAL", asof="2026-10-15"), "adjustment": adjustment},
                lineage,
                intervals,
                **certified_row_inputs(
                    rows,
                    {**correlation_source("FINAL", asof="2026-10-15"), "adjustment": adjustment},
                    lineage,
                    intervals,
                ),
            )
            self.assertEqual([], branches["correlated"])
            self.assertEqual(405, branches["uncorrelated"][0]["row_count"])
            self.assertEqual(
                ["ambiguous_asof_uuid_lineage_intervals"], branches["uncorrelated"][0]["reason_codes"]
            )

    def test_complete_current_run_after_authenticated_ticker_reuse_uses_current_uuid(self) -> None:
        lineage = current_symbol_reuse_lineage()
        sessions = expected_market_session_coverage("2024-07-01", "2024-07-08").session_dates
        parsed_sessions = tuple(date.fromisoformat(value) for value in sessions)
        consensus = adjustment_session_consensus(
            (
                (f"{adjustment}:{adjustment.replace(',', '_')}.csv.gz", parsed_sessions)
                for adjustment in ALPACA_ADJUSTMENT_MODES
            )
        )
        source = correlation_source("REUS", asof="2024-07-07")
        source["request_to"] = "2024-07-08T00:00:00Z"

        branches = correlate_download_rows(
            [{"t": f"{session}T12:00:00Z"} for session in sessions],
            source,
            lineage,
            [superseded_reused_symbol_interval()],
            adjustment_consensus=consensus,
            evidence_issues=[],
        )

        self.assertEqual([], branches["uncorrelated"])
        segment = branches["correlated"][0]
        self.assertEqual(CURRENT_SYMBOL_REUSE_RULE_NAME, segment["correlation_method"])
        self.assertEqual(
            "current-reused-symbol-asset-id",
            segment["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual("222222222", segment["canonical_security"]["cusip"])
        self.assertGreaterEqual(segment["identity_partition"]["vacancy_session_count"], 100)

    def test_current_symbol_reuse_rule_rejects_structural_action_inside_run(self) -> None:
        lineage = current_symbol_reuse_lineage()
        lineage["corporate_actions"].append(
            {
                "action_type": "reverse_split",
                "id": "in-run-structural-action",
                "process_date": "2024-07-03",
                "symbol": "REUS",
                "old_cusip": "222222222",
                "new_cusip": "333333333",
            }
        )
        sessions = expected_market_session_coverage("2024-07-01", "2024-07-08").session_dates
        parsed_sessions = tuple(date.fromisoformat(value) for value in sessions)
        consensus = adjustment_session_consensus(
            (
                (f"{adjustment}:{adjustment.replace(',', '_')}.csv.gz", parsed_sessions)
                for adjustment in ALPACA_ADJUSTMENT_MODES
            )
        )
        source = correlation_source("REUS", asof="2024-07-07")
        source["request_to"] = "2024-07-08T00:00:00Z"

        branches = correlate_download_rows(
            [{"t": f"{session}T12:00:00Z"} for session in sessions],
            source,
            lineage,
            [superseded_reused_symbol_interval()],
            adjustment_consensus=consensus,
            evidence_issues=[],
        )

        self.assertNotEqual(CURRENT_SYMBOL_REUSE_RULE_NAME, branches["correlated"][0]["correlation_method"])

    def test_current_symbol_reuse_preserves_only_accepted_lineage_membership(self) -> None:
        current_id = "current-reused-symbol-asset-id"
        previous_id = "previous-reused-symbol-asset-id"
        sessions = expected_market_session_coverage("2024-07-01", "2024-07-08").session_dates
        consensus = adjustment_session_consensus(
            (adjustment, tuple(date.fromisoformat(value) for value in sessions))
            for adjustment in ALPACA_ADJUSTMENT_MODES
        )
        for linked in (True, False):
            lineage = current_symbol_reuse_lineage()
            action = lineage["corporate_actions"][0]
            successor_id = current_id if linked else "unrelated-successor-id"
            action["new_cusip"] = "222222222" if linked else "333333333"
            lineage["symbol_occupancy_evidence"].append(
                occupancy(
                    "OLD",
                    action["new_cusip"],
                    successor_id,
                    action_type="name_change",
                    action_id=action["id"],
                    process_date=action["process_date"],
                    role="new",
                )
            )
            edges, issues = certified_edges(lineage)
            self.assertEqual([], issues)
            self.assertEqual(1, len(edges))
            expected_members = sorted((current_id, previous_id)) if linked else [current_id]
            for adjustment in ALPACA_ADJUSTMENT_MODES:
                with self.subTest(linked=linked, adjustment=adjustment):
                    branches = correlate_download_rows(
                        [{"t": f"{session}T12:00:00Z"} for session in sessions],
                        {
                            **correlation_source("REUS", asof="2024-07-07"),
                            "request_to": "2024-07-08T00:00:00Z",
                            "adjustment": adjustment,
                        },
                        lineage,
                        [superseded_reused_symbol_interval()],
                        adjustment_consensus=consensus,
                        evidence_issues=[],
                        **certified_row_inputs(
                            [{"t": f"{session}T12:00:00Z"} for session in sessions],
                            {
                                **correlation_source("REUS", asof="2024-07-07"),
                                "request_to": "2024-07-08T00:00:00Z",
                                "adjustment": adjustment,
                            },
                            lineage,
                            [superseded_reused_symbol_interval()],
                        ),
                    )
                    self.assertEqual([], branches["uncorrelated"])
                    segment = branches["correlated"][0]
                    self.assertEqual(CURRENT_SYMBOL_REUSE_RULE_NAME, segment["correlation_method"])
                    self.assertEqual(current_id, segment["canonical_security"]["alpaca_asset_id"])
                    self.assertEqual(expected_members, segment["security_identity"]["member_alpaca_asset_ids"])
                    self.assertEqual(
                        security_id_for_alpaca_asset_ids(expected_members),
                        segment["security_identity"]["security_id"],
                    )

    def test_unique_uuid_candidate_correlates_dell_with_boundary_warning(self) -> None:
        lineage = dell_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], evidence_issues)
        self.assertEqual([], intervals)

        branches = correlate_download_rows(
            [
                {"t": "2018-12-28T05:00:00Z"},
                {"t": "2018-12-31T05:00:00Z"},
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
            {"order": 13, "name": "unique_uuid_candidate_with_warnings"},
            branches["correlated"][0]["correlation_rule"],
        )
        self.assertIn("missing_dated_ticker_ownership_interval", branches["correlated"][0]["warnings"])
        self.assertIsNone(branches["correlated"][0]["ticker_ownership_interval"])

    def test_unique_latest_ownership_start_supersedes_older_open_claim(self) -> None:
        intervals = [
            TickerOwnershipInterval(
                historical_symbol="DELL",
                cusip="111111111",
                asset_id="older-dell-asset-id",
                start_date_inclusive=date(2020, 1, 2),
                end_date_exclusive=None,
                applied_rules=(),
            ),
            TickerOwnershipInterval(
                historical_symbol="DELL",
                cusip="222222222",
                asset_id="newer-dell-asset-id",
                start_date_inclusive=date(2024, 1, 3),
                end_date_exclusive=None,
                applied_rules=(),
            ),
        ]
        source = correlation_source("DELL", asof="2024-01-05")
        source["request_from"] = "2024-01-02T00:00:00Z"
        source["request_to"] = "2024-01-04T00:00:00Z"

        branches = correlate_download_rows(
            [
                {"t": "2024-01-02T12:00:00Z"},
                {"t": "2024-01-03T12:00:00Z"},
            ],
            source,
            dell_lineage(),
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(2, len(branches["correlated"]))
        older, newer = branches["correlated"]
        self.assertEqual("older-dell-asset-id", older["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
            older["correlation_rule"],
        )
        self.assertEqual("newer-dell-asset-id", newer["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            {"order": 11, "name": LATEST_DATED_OWNERSHIP_INTERVAL_RULE_NAME},
            newer["correlation_rule"],
        )
        self.assertEqual(
            ["newer-dell-asset-id"],
            newer["security_identity"]["member_alpaca_asset_ids"],
        )
        self.assertIn("older_overlapping_ownership_claim_superseded", newer["warnings"])

    def test_related_predecessor_branches_do_not_compete_for_historical_rows(self) -> None:
        # BTN-shaped evidence, then the identical shape with entirely different labels.
        for original, renamed, other, successor in (
            ("BTN", "FGH", "PIH", "FGF"),
            ("LEFT", "LEFTNEW", "RIGHT", "JOINED"),
        ):
            lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
            left = ("left-id", original, "111111111", "Left predecessor")
            middle = ("middle-id", renamed, "222222222", "Renamed left predecessor")
            right = ("right-id", other, "333333333", "Right predecessor")
            joined = ("right-id", successor, "333333333", "Right predecessor")
            for action_id, when, predecessor, following in (
                ("right-rename", "2020-12-17", right, joined),
                ("left-rename", "2023-02-03", left, middle),
                ("later-connection", "2024-03-01", middle, joined),
            ):
                add_name_change_edge(
                    lineage,
                    action_id=action_id,
                    process_date=when,
                    predecessor=predecessor,
                    successor=following,
                )
            rows = complete_market_session_rows("2016-01-04", "2022-12-24")
            self.assertEqual(1758, len(rows))
            intervals, issues = build_ticker_ownership_intervals(lineage)
            self.assertEqual([], issues)
            for adjustment in ALPACA_ADJUSTMENT_MODES:
                with self.subTest(symbol=original, adjustment=adjustment):
                    source = {**correlation_source(original, asof="2026-10-15"), "adjustment": adjustment}
                    branches = correlate_download_rows(rows, source, lineage, intervals)
                    self.assertEqual([], branches["uncorrelated"])
                    self.assertEqual(1, len(branches["correlated"]))
                    segment = branches["correlated"][0]
                    self.assertEqual(1758, segment["row_count"])
                    self.assertEqual(original, segment["historical_symbol"])
                    self.assertEqual("left-id", segment["canonical_security"]["alpaca_asset_id"])
                    self.assertEqual(
                        {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
                        segment["correlation_rule"],
                    )
                    self.assertEqual(
                        branches, correlate_download_rows(rows, source, lineage, list(reversed(intervals)))
                    )

    def test_foreign_latest_start_cannot_resolve_ambiguous_same_label_claims(self) -> None:
        intervals = [
            TickerOwnershipInterval("DELL", cusip, asset_id, None, None, ())
            for cusip, asset_id in (("111111111", "first-id"), ("222222222", "second-id"))
        ]
        intervals.append(TickerOwnershipInterval("OTHER", "333333333", "foreign-id", date(2024, 1, 2), None, ()))
        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}],
            correlation_source("DELL", asof="2024-01-03"),
            dell_lineage(),
            intervals,
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(
            ["overlapping_ownership_intervals_without_unique_latest_start"],
            branches["uncorrelated"][0]["reason_codes"],
        )

    def test_foreign_intervals_cannot_supply_missing_source_identity(self) -> None:
        intervals = [
            TickerOwnershipInterval(symbol, cusip, asset_id, start, None, ())
            for symbol, cusip, asset_id, start in (
                ("OTHER", "111111111", "first-id", None),
                ("LATER", "222222222", "second-id", date(2024, 1, 2)),
            )
        ]
        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}],
            correlation_source("UNKNOWN", asof="2024-01-03"),
            {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []},
            intervals,
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(1, branches["uncorrelated"][0]["row_count"])
        self.assertEqual(
            ["overlapping_ownership_intervals_without_source_scope"],
            branches["uncorrelated"][0]["reason_codes"],
        )

    def test_ended_prior_occupant_does_not_date_an_undated_reused_label(self) -> None:
        intervals = [
            TickerOwnershipInterval("REUSED", "111111111", "old-id", None, date(2024, 1, 3), ()),
            TickerOwnershipInterval("REUSED", "222222222", "later-id", None, date(2024, 1, 5), ()),
        ]
        branches = correlate_download_rows(
            [{"t": "2024-01-04T12:00:00Z"}],
            correlation_source("REUSED", asof="2024-01-08"),
            {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []},
            intervals,
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(
            ["undated_requested_ticker_reuse_boundary"], branches["uncorrelated"][0]["reason_codes"]
        )
        self.assertEqual(
            {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
            branches["uncorrelated"][0]["correlation_rule"],
        )

    def test_unscoped_branches_cannot_fall_through_to_one_current_uuid(self) -> None:
        lineage = dell_lineage()  # One exact current candidate is not dated source ownership.
        intervals = [
            TickerOwnershipInterval(symbol, cusip, asset_id, None, None, ())
            for symbol, cusip, asset_id in (
                ("OLD", "111111111", "first-id"),
                ("OTHER", "222222222", "second-id"),
            )
        ]
        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}],
            correlation_source("DELL", asof="2024-01-03"),
            lineage,
            intervals,
        )
        self.assertEqual([], branches["correlated"])
        self.assertEqual(
            ["overlapping_ownership_intervals_without_source_scope"],
            branches["uncorrelated"][0]["reason_codes"],
        )

    def test_concurrent_asof_predecessors_cannot_be_resolved_by_requested_label(self) -> None:
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        for action_id, predecessor in (
            ("left-connection", ("left-id", "JOINED", "111111111", "Left predecessor")),
            ("right-connection", ("right-id", "OTHER", "222222222", "Right predecessor")),
        ):
            add_name_change_edge(
                lineage,
                action_id=action_id,
                process_date="2024-01-04",
                predecessor=predecessor,
                successor=("successor-id", "JOINED", "333333333", "Successor"),
            )
        intervals, issues = build_ticker_ownership_intervals(lineage)
        self.assertEqual([], issues)
        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}, {"t": "2024-01-04T12:00:00Z"}],
            correlation_source("JOINED", asof="2024-01-05"),
            lineage,
            intervals,
            **certified_row_inputs(
                [{"t": "2024-01-03T12:00:00Z"}, {"t": "2024-01-04T12:00:00Z"}],
                correlation_source("JOINED", asof="2024-01-05"),
                lineage,
                intervals,
            ),
        )
        self.assertEqual(1, branches["uncorrelated"][0]["row_count"])
        self.assertEqual(["ambiguous_asof_uuid_lineage_intervals"], branches["uncorrelated"][0]["reason_codes"])
        self.assertEqual(1, branches["correlated"][0]["row_count"])
        self.assertEqual("successor-id", branches["correlated"][0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            {"order": 9, "name": "asof_selected_uuid_lineage_interval"},
            branches["uncorrelated"][0]["correlation_rule"],
        )

    def test_overlapping_ownership_claims_without_unique_latest_start_fail_closed(self) -> None:
        for label, starts in {
            "tied": (date(2024, 1, 2), date(2024, 1, 2)),
            "undated": (None, None),
        }.items():
            with self.subTest(label=label):
                intervals = [
                    TickerOwnershipInterval(
                        historical_symbol="DELL",
                        cusip=cusip,
                        asset_id=asset_id,
                        start_date_inclusive=start,
                        end_date_exclusive=None,
                        applied_rules=(),
                    )
                    for cusip, asset_id, start in (
                        ("111111111", "first-dell-asset-id", starts[0]),
                        ("222222222", "second-dell-asset-id", starts[1]),
                    )
                ]
                source = correlation_source("DELL", asof="2024-01-03")
                source["request_from"] = "2024-01-03T00:00:00Z"
                source["request_to"] = "2024-01-04T00:00:00Z"

                branches = correlate_download_rows(
                    [{"t": "2024-01-03T12:00:00Z"}],
                    source,
                    dell_lineage(),
                    intervals,
                )

                self.assertEqual([], branches["correlated"])
                self.assertEqual(1, len(branches["uncorrelated"]))
                unresolved = branches["uncorrelated"][0]
                self.assertEqual(
                    {"order": 11, "name": LATEST_DATED_OWNERSHIP_INTERVAL_RULE_NAME},
                    unresolved["correlation_rule"],
                )
                self.assertEqual(
                    ["overlapping_ownership_intervals_without_unique_latest_start"],
                    unresolved["reason_codes"],
                )

    def test_same_uuid_overlaps_defer_to_exact_requested_symbol_interval(self) -> None:
        intervals = [
            TickerOwnershipInterval(
                historical_symbol=historical_symbol,
                cusip=cusip,
                asset_id="dell-asset-id",
                start_date_inclusive=None,
                end_date_exclusive=None,
                applied_rules=(),
            )
            for historical_symbol, cusip in (
                ("DELL", "24703L202"),
                ("DVMT", "24703L103"),
            )
        ]

        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}],
            correlation_source("DELL", asof="2024-01-03"),
            dell_lineage(),
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(1, len(branches["correlated"]))
        correlated = branches["correlated"][0]
        self.assertEqual("dell-asset-id", correlated["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
            correlated["correlation_rule"],
        )

    def test_same_uuid_overlaps_for_the_same_requested_symbol_still_fail_closed(self) -> None:
        intervals = [
            TickerOwnershipInterval(
                historical_symbol="DELL",
                cusip=cusip,
                asset_id="dell-asset-id",
                start_date_inclusive=None,
                end_date_exclusive=None,
                applied_rules=(),
            )
            for cusip in ("24703L202", "24703L103")
        ]

        branches = correlate_download_rows(
            [{"t": "2024-01-03T12:00:00Z"}],
            correlation_source("DELL", asof="2024-01-03"),
            dell_lineage(),
            intervals,
        )

        self.assertEqual([], branches["correlated"])
        self.assertEqual(1, len(branches["uncorrelated"]))
        unresolved = branches["uncorrelated"][0]
        self.assertEqual(
            {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
            unresolved["correlation_rule"],
        )
        self.assertEqual(["conflicting_requested_ticker_intervals"], unresolved["reason_codes"])

    def test_exactly_60_session_gap_detects_but_does_not_allocate_either_run(self) -> None:
        lineage = dell_lineage()
        sessions = expected_market_session_coverage("2024-01-02", "2024-07-04").session_dates
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in (sessions[0], *sessions[61:])]
        source = correlation_source("DELL", asof="2024-07-03")
        source["request_to"] = "2024-07-04T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual([], branches["correlated"])
        self.assertEqual(2, len(branches["uncorrelated"]))
        prior, current = branches["uncorrelated"]
        self.assertEqual(1, prior["row_count"])
        self.assertEqual(
            ["prior_pricing_run_before_ticker_reuse_gap"],
            prior["reason_codes"],
        )
        self.assertEqual(len(sessions) - 61, current["row_count"])
        self.assertEqual(sessions[61], current["first_session_date"])
        self.assertEqual(
            ["post_gap_pricing_run_requires_dated_identity"],
            current["reason_codes"],
        )
        self.assertEqual(60, current["ticker_reuse_gap"]["gap_session_count"])
        self.assertTrue(current["ticker_reuse_gap"]["rightmost_run_complete"])

    def test_exactly_100_session_gap_allocates_only_complete_current_suffix(self) -> None:
        lineage = dell_lineage()
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        observed_sessions = (sessions[0], *sessions[101:])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual(1, len(branches["correlated"]))
        suffix = branches["correlated"][0]
        self.assertEqual(sessions[101], suffix["first_session_date"])
        self.assertEqual(len(sessions) - 101, suffix["row_count"])
        self.assertEqual(
            "complete_current_identity_suffix_after_ticker_reuse_gap",
            suffix["correlation_method"],
        )
        self.assertFalse(suffix["ticker_reuse_gap"]["left_prefix_has_current_asset_uuid_evidence"])
        self.assertEqual(1, len(branches["uncorrelated"]))
        self.assertEqual(1, branches["uncorrelated"][0]["row_count"])
        self.assertEqual(
            ["prior_pricing_run_before_ticker_reuse_gap"],
            branches["uncorrelated"][0]["reason_codes"],
        )

    def test_current_suffix_rules_preserve_authenticated_successor_lineage(self) -> None:
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        for expected_rule, terminal_offset in (
            ("complete_current_identity_suffix_after_ticker_reuse_gap", None),
            ("inactive_current_identity_suffix_after_ticker_reuse_gap", -10),
        ):
            with self.subTest(rule=expected_rule):
                lineage = dell_lineage()
                add_name_change_edge(
                    lineage,
                    action_id="dell-successor-name-change",
                    process_date="2025-01-02",
                    predecessor=(
                        "dell-asset-id",
                        "DELL",
                        "24703L202",
                        "Dell Technologies Inc.",
                    ),
                    successor=(
                        "dell-successor-asset-id",
                        "NEXT",
                        "000000001",
                        "Dell Successor Inc.",
                    ),
                )
                if terminal_offset is not None:
                    lineage["asset_lookups"][0]["asset"]["status"] = "inactive"
                    lineage["asset_lookups"][0]["asset"]["tradable"] = False
                    lineage["identities"][0]["status"] = "inactive"
                    lineage["identities"][0]["tradable"] = False
                right_sessions = sessions[101:terminal_offset]
                rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in (sessions[0], *right_sessions)]
                source = correlation_source("DELL", asof="2024-12-30")
                source["request_to"] = "2024-12-31T00:00:00Z"

                branches = correlate_download_rows(
                    rows, source, lineage, [], **certified_row_inputs(rows, source, lineage, [])
                )

                self.assertEqual(1, len(branches["correlated"]))
                suffix = branches["correlated"][0]
                self.assertEqual(expected_rule, suffix["correlation_method"])
                self.assertEqual(expected_rule, suffix["security_identity"]["basis"])
                self.assertEqual(
                    ["dell-asset-id", "dell-successor-asset-id"],
                    suffix["security_identity"]["member_alpaca_asset_ids"],
                )
                self.assertEqual(
                    ["DELL", "NEXT"],
                    suffix["security_identity"]["member_tickers"],
                )
                self.assertEqual(1, len(branches["uncorrelated"]))

    def test_missing_cusip_does_not_block_complete_current_suffix(self) -> None:
        lineage = copy.deepcopy(dell_lineage())
        lineage["identities"][0]["lookup_cusips"] = []
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in (sessions[0], *sessions[101:])]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual(1, len(branches["correlated"]))
        suffix = branches["correlated"][0]
        self.assertIsNone(suffix["canonical_security"]["cusip"])
        self.assertEqual(
            "dell-asset-id",
            suffix["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual(1, len(branches["uncorrelated"]))

    def test_adjustment_session_disagreement_vetoes_complete_suffix(self) -> None:
        lineage = dell_lineage()
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        observed_sessions = (sessions[0], *sessions[101:])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"
        parsed_sessions = tuple(date.fromisoformat(session) for session in observed_sessions)
        consensus = adjustment_session_consensus(
            (
                ("raw:raw.csv.gz", parsed_sessions),
                ("split:split.csv.gz", parsed_sessions[:-1]),
            )
        )

        branches = correlate_download_rows(
            rows,
            source,
            lineage,
            [],
            adjustment_consensus=consensus,
        )

        self.assertEqual([], branches["correlated"])
        self.assertEqual(2, len(branches["uncorrelated"]))
        self.assertFalse(
            branches["uncorrelated"][1]["ticker_reuse_gap"]["adjustment_consensus"][
                "all_authenticated_adjustments_agree"
            ]
        )

    def test_same_uuid_on_dated_left_prefix_vetoes_both_sides(self) -> None:
        lineage = dell_lineage()
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        observed_sessions = (sessions[0], *sessions[101:])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"
        interval = TickerOwnershipInterval(
            historical_symbol="DELL",
            cusip="24703L202",
            asset_id="dell-asset-id",
            start_date_inclusive=date.fromisoformat(sessions[0]),
            end_date_exclusive=date.fromisoformat(sessions[1]),
            applied_rules=(
                AppliedOwnershipIntervalRule(
                    order=1,
                    name="test_dated_identity",
                    supporting_action_ids=("test-action",),
                ),
            ),
        )

        branches = correlate_download_rows(rows, source, lineage, [interval])

        self.assertEqual([], branches["correlated"])
        self.assertEqual(2, len(branches["uncorrelated"]))
        assessment = branches["uncorrelated"][0]["ticker_reuse_gap"]
        self.assertTrue(assessment["left_prefix_has_current_asset_uuid_evidence"])
        self.assertEqual(
            ["test-action"],
            assessment["left_prefix_current_asset_uuid_evidence"][0]["supporting_corporate_action_ids"],
        )

    def test_59_session_gap_does_not_partition_history(self) -> None:
        lineage = dell_lineage()
        sessions = expected_market_session_coverage("2024-01-02", "2024-07-04").session_dates
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in (sessions[0], *sessions[60:])]
        source = correlation_source("DELL", asof="2024-07-03")
        source["request_to"] = "2024-07-04T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(len(rows), branches["correlated"][0]["row_count"])
        self.assertNotIn("ticker_reuse_gap", branches["correlated"][0])

    def test_large_gap_does_not_allocate_an_incomplete_rightmost_run(self) -> None:
        lineage = copy.deepcopy(dell_lineage())
        lineage["asset_lookups"][0]["asset"]["status"] = "inactive"
        lineage["identities"][0]["status"] = "inactive"
        sessions = expected_market_session_coverage("2024-01-02", "2024-07-04").session_dates
        rightmost_with_missing_session = (sessions[61], *sessions[63:])
        rows = [
            {"t": f"{session_date}T12:00:00Z"} for session_date in (sessions[0], *rightmost_with_missing_session)
        ]
        source = correlation_source("DELL", asof="2024-07-03")
        source["request_to"] = "2024-07-04T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual([], branches["correlated"])
        self.assertEqual(2, len(branches["uncorrelated"]))
        prior, unresolved = branches["uncorrelated"]
        self.assertEqual(1, prior["row_count"])
        self.assertEqual(
            ["prior_pricing_run_before_ticker_reuse_gap"],
            prior["reason_codes"],
        )
        self.assertEqual(len(rows) - 1, unresolved["row_count"])
        self.assertEqual(
            ["post_gap_pricing_run_requires_dated_identity"],
            unresolved["reason_codes"],
        )
        self.assertFalse(unresolved["ticker_reuse_gap"]["rightmost_run_complete"])
        self.assertFalse(unresolved["ticker_reuse_gap"]["rightmost_run_complete_through_last_observed"])

    def test_inactive_asset_allocates_contiguous_suffix_ending_before_request_end(self) -> None:
        lineage = copy.deepcopy(dell_lineage())
        lineage["asset_lookups"][0]["asset"]["status"] = "inactive"
        lineage["asset_lookups"][0]["asset"]["tradable"] = False
        lineage["identities"][0]["status"] = "inactive"
        lineage["identities"][0]["tradable"] = False
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        observed_sessions = (sessions[0], *sessions[101:-10])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual(1, len(branches["correlated"]))
        suffix = branches["correlated"][0]
        self.assertEqual(
            "inactive_current_identity_suffix_after_ticker_reuse_gap",
            suffix["correlation_method"],
        )
        self.assertTrue(suffix["ticker_reuse_gap"]["inactive_current_suffix_rule"]["eligible"])
        self.assertEqual(1, len(branches["uncorrelated"]))

    def test_active_asset_keeps_contiguous_suffix_when_only_request_end_advances(self) -> None:
        lineage = dell_lineage()
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        observed_sessions = (sessions[0], *sessions[101:-10])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, [])

        self.assertEqual(1, len(branches["correlated"]))
        self.assertEqual(
            "complete_current_identity_suffix_after_ticker_reuse_gap",
            branches["correlated"][0]["correlation_method"],
        )
        self.assertTrue(branches["correlated"][0]["ticker_reuse_gap"]["active_current_suffix_rule"]["eligible"])
        self.assertEqual(1, len(branches["uncorrelated"]))

    def test_inactive_asset_does_not_override_conflicting_dated_rhs_uuid(self) -> None:
        lineage = copy.deepcopy(dell_lineage())
        lineage["asset_lookups"][0]["asset"]["status"] = "inactive"
        lineage["identities"][0]["status"] = "inactive"
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        observed_sessions = (sessions[0], *sessions[101:-10])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("DELL", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"
        conflicting_interval = TickerOwnershipInterval(
            historical_symbol="DELL",
            cusip="conflicting-cusip",
            asset_id="conflicting-rhs-asset-id",
            start_date_inclusive=date.fromisoformat(sessions[101]),
            end_date_exclusive=None,
            applied_rules=(
                AppliedOwnershipIntervalRule(
                    order=1,
                    name="test_dated_rhs_identity",
                    supporting_action_ids=("test-rhs-action",),
                ),
            ),
        )

        branches = correlate_download_rows(rows, source, lineage, [conflicting_interval])

        self.assertEqual([], branches["correlated"])
        assessment = branches["uncorrelated"][1]["ticker_reuse_gap"]
        self.assertFalse(assessment["inactive_current_suffix_rule"]["eligible"])
        self.assertEqual(
            "conflicting-rhs-asset-id",
            assessment["right_suffix_conflicting_asset_uuid_evidence"][0]["alpaca_asset_id"],
        )

    def test_dated_terminal_identity_allocates_only_complete_right_suffix(self) -> None:
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        terminal_date = sessions[200]
        lineage = dated_terminal_conflict_lineage(terminal_date)
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        observed_sessions = (sessions[0], *sessions[101:200])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("TERM", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, intervals)

        self.assertEqual([], evidence_issues)
        self.assertEqual(1, len(branches["correlated"]))
        suffix = branches["correlated"][0]
        self.assertEqual(sessions[101], suffix["first_session_date"])
        self.assertEqual("dated-terminal-asset-id", suffix["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            "dated_terminal_identity_suffix_after_ticker_reuse_gap",
            suffix["correlation_method"],
        )
        self.assertIn(
            "undated_symbol_lookup_uuid_differs_from_dated_suffix_identity",
            suffix["warnings"],
        )
        assessment = suffix["ticker_reuse_gap"]
        self.assertTrue(assessment["dated_terminal_suffix_rule"]["eligible"])
        self.assertEqual(
            terminal_date, assessment["dated_terminal_suffix_rule"]["identity"]["terminal_boundary_exclusive"]
        )
        self.assertEqual(1, len(branches["uncorrelated"]))
        self.assertEqual(1, branches["uncorrelated"][0]["row_count"])

    def test_dated_terminal_suffix_preserves_authenticated_predecessor_lineage(self) -> None:
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        terminal_date = sessions[200]
        lineage = dated_terminal_conflict_lineage(terminal_date)
        add_name_change_edge(
            lineage,
            action_id="terminal-predecessor-name-change",
            process_date=sessions[101],
            predecessor=(
                "dated-terminal-predecessor-id",
                "PREV",
                "111111111",
                "Terminal Test Predecessor",
            ),
            successor=(
                "dated-terminal-asset-id",
                "TERM",
                "123456789",
                "Terminal Test Security",
            ),
        )
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        observed_sessions = (sessions[0], *sessions[101:200])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("TERM", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(
            rows, source, lineage, intervals, **certified_row_inputs(rows, source, lineage, intervals)
        )

        self.assertEqual([], evidence_issues)
        self.assertEqual(1, len(branches["correlated"]))
        suffix = branches["correlated"][0]
        self.assertEqual(
            "dated_terminal_identity_suffix_after_ticker_reuse_gap",
            suffix["correlation_method"],
        )
        self.assertEqual(
            ["dated-terminal-asset-id", "dated-terminal-predecessor-id"],
            suffix["security_identity"]["member_alpaca_asset_ids"],
        )
        self.assertEqual(1, len(branches["uncorrelated"]))

    def test_dated_terminal_identity_rejects_incomplete_right_suffix(self) -> None:
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        terminal_date = sessions[200]
        lineage = dated_terminal_conflict_lineage(terminal_date)
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        observed_sessions = (sessions[0], *sessions[101:150], *sessions[151:200])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("TERM", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"

        branches = correlate_download_rows(rows, source, lineage, intervals)

        self.assertEqual([], evidence_issues)
        self.assertEqual([], branches["correlated"])
        assessment = branches["uncorrelated"][-1]["ticker_reuse_gap"]
        self.assertFalse(assessment["dated_terminal_suffix_rule"]["eligible"])
        self.assertIsNone(assessment["dated_terminal_suffix_rule"]["identity"])

    def test_dated_terminal_identity_rejects_its_boundary_disagreement(self) -> None:
        sessions = expected_market_session_coverage("2024-01-02", "2024-12-31").session_dates
        terminal_date = sessions[200]
        lineage = dated_terminal_conflict_lineage(terminal_date)
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        observed_sessions = (sessions[0], *sessions[101:200])
        rows = [{"t": f"{session_date}T12:00:00Z"} for session_date in observed_sessions]
        source = correlation_source("TERM", asof="2024-12-30")
        source["request_to"] = "2024-12-31T00:00:00Z"
        boundary_issue = {
            "reason_code": "cash_merger_boundary_disagreement",
            "action_ids": ["terminal-test-merger"],
        }

        branches = correlate_download_rows(
            rows,
            source,
            lineage,
            intervals,
            evidence_issues=[*evidence_issues, boundary_issue],
        )

        self.assertEqual([], branches["correlated"])
        assessment = branches["uncorrelated"][-1]["ticker_reuse_gap"]
        self.assertFalse(assessment["dated_terminal_suffix_rule"]["eligible"])
        self.assertEqual(
            [boundary_issue],
            assessment["dated_terminal_suffix_rule"]["veto_issues"],
        )

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
                {"t": "2022-10-26T04:00:00Z"},
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
            {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
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

    def test_name_change_membership_rejects_immediately_superseded_uuid_edge(self) -> None:
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        predecessor = ("predecessor-id", "OLD", "111111111", "Predecessor")
        add_name_change_edge(
            lineage,
            action_id="first-transition",
            process_date="2026-10-08",
            predecessor=predecessor,
            successor=("first-successor-id", "FIRST", "222222222", "First successor"),
        )
        add_name_change_edge(
            lineage,
            action_id="second-transition",
            process_date="2026-10-09",
            predecessor=predecessor,
            successor=("predecessor-id", "FINAL", "111111111", "Predecessor"),
        )

        edges, issues = build_uuid_lineage_edges(lineage)

        self.assertEqual(["second-transition"], [edge.action_id for edge in edges])
        self.assertEqual(
            [
                {
                    "reason_code": "name_change_uuid_edge_immediately_superseded",
                    "predecessor": {
                        "symbol": "OLD",
                        "cusip": "111111111",
                        "alpaca_asset_id": "predecessor-id",
                    },
                    "rejected_action_id": "first-transition",
                    "rejected_successor": {
                        "symbol": "FIRST",
                        "cusip": "222222222",
                        "alpaca_asset_id": "first-successor-id",
                    },
                    "superseding_action_id": "second-transition",
                    "superseding_successor": {
                        "symbol": "FINAL",
                        "cusip": "111111111",
                        "alpaca_asset_id": "predecessor-id",
                    },
                }
            ],
            issues,
        )

    def test_name_change_membership_keeps_distant_successors(self) -> None:
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        predecessor = ("predecessor-id", "OLD", "111111111", "Predecessor")
        add_name_change_edge(
            lineage,
            action_id="first-transition",
            process_date="2026-10-08",
            predecessor=predecessor,
            successor=("first-successor-id", "FIRST", "222222222", "First successor"),
        )
        add_name_change_edge(
            lineage,
            action_id="later-transition",
            process_date="2026-11-09",
            predecessor=predecessor,
            successor=("predecessor-id", "FINAL", "111111111", "Predecessor"),
        )

        edges, issues = certified_edges(lineage)

        self.assertEqual([], issues)
        self.assertEqual(2, len(edges))

    def test_name_change_membership_allows_successor_aliases_for_one_uuid(self) -> None:
        lineage = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
        predecessor = ("predecessor-id", "OLD", "111111111", "Predecessor")
        add_name_change_edge(
            lineage,
            action_id="first-transition",
            process_date="2026-10-08",
            predecessor=predecessor,
            successor=("successor-id", "NEW.W", "222222222", "Successor"),
        )
        add_name_change_edge(
            lineage,
            action_id="second-transition",
            process_date="2026-10-09",
            predecessor=predecessor,
            successor=("successor-id", "NEWW", "222222222", "Successor"),
        )

        edges, issues = certified_edges(lineage)

        self.assertEqual([], issues)
        self.assertEqual(2, len(edges))
        self.assertEqual({"successor-id"}, {edge.successor_asset_id for edge in edges})

    def test_same_symbol_reverse_split_preserves_identity_without_reassigning_rows(self) -> None:
        lineage = same_symbol_split_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        uuid_edges, edge_issues = build_uuid_lineage_edges(lineage)

        self.assertEqual([], evidence_issues)
        self.assertEqual([], edge_issues)
        split_edges = [edge for edge in uuid_edges if edge.edge_type == "reverse_split"]
        self.assertEqual(1, len(split_edges))
        self.assertEqual(date(2026, 9, 29), split_edges[0].effective_date)
        self.assertEqual([], intervals)

        branches = correlate_download_rows(
            [
                {"t": "2026-09-28T04:00:00Z"},
                {"t": "2026-09-29T04:00:00Z"},
            ],
            correlation_source("SPLT", asof="2026-10-06"),
            lineage,
            intervals,
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(1, len(branches["correlated"]))
        segment = branches["correlated"][0]
        expected_members = ["new-split-asset-id", "old-split-asset-id"]
        self.assertEqual(
            expected_members,
            segment["security_identity"]["member_alpaca_asset_ids"],
        )
        self.assertEqual("new-split-asset-id", segment["canonical_security"]["alpaca_asset_id"])

    def test_same_symbol_split_fails_closed_on_each_material_conflict(self) -> None:
        mutations = {
            "ticker transition": lambda lineage: lineage["corporate_actions"][0].update({"new_symbol": "SPLTD"}),
            "wrong ratio direction": lambda lineage: lineage["corporate_actions"][0].update(
                {"old_rate": 1, "new_rate": 10}
            ),
            "same cusip": lambda lineage: lineage["corporate_actions"][0].update({"new_cusip": "111111111"}),
            "asset symbol mismatch": lambda lineage: lineage["asset_lookups"][1]["asset"].update(
                {"symbol": "OTHER"}
            ),
            "non equity": lambda lineage: lineage["asset_lookups"][1]["asset"].update({"class": "crypto"}),
            "same uuid": lambda lineage: lineage["asset_lookups"][1]["asset"].update(
                {"id": "old-split-asset-id"}
            ),
            "unauthenticated lookup": lambda lineage: lineage["asset_lookups"][1].update(
                {"http_status": 404, "found": False, "outcome": "not_found", "asset": None}
            ),
            "ambiguous lookup": lambda lineage: lineage["asset_lookups"].append(
                copy.deepcopy(lineage["asset_lookups"][1])
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                lineage = same_symbol_split_lineage()
                mutate(lineage)
                edges, issues = build_uuid_lineage_edges(lineage)
                self.assertEqual([], edges)
                self.assertEqual([], issues)

    def test_later_split_extends_membership_without_rewriting_predecessor_attribution(self) -> None:
        # ASCA-shaped evidence: a dated predecessor followed by a later same-symbol split.
        extended = same_symbol_split_lineage()
        add_name_change_edge(
            extended,
            action_id="predecessor-transition",
            process_date="2024-04-04",
            predecessor=("predecessor-id", "ASCA", "333333333", "Predecessor security"),
            successor=("old-split-asset-id", "SPLT", "111111111", "Split Test Security"),
        )
        prior = copy.deepcopy(extended)
        prior["corporate_actions"] = [
            a for a in prior["corporate_actions"] if a["action_type"] != "reverse_split"
        ]
        prior["identities"] = [a for a in prior["identities"] if a["id"] != "new-split-asset-id"]
        prior["asset_lookups"] = [a for a in prior["asset_lookups"] if a["asset"]["id"] != "new-split-asset-id"]
        rows = complete_market_session_rows("2022-03-18", "2024-04-04")
        self.assertEqual(513, len(rows))
        prior_intervals, _ = build_ticker_ownership_intervals(prior)
        extended_intervals, _ = build_ticker_ownership_intervals(extended)
        self.assertEqual(prior_intervals, extended_intervals)
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            with self.subTest(adjustment=adjustment):
                source = {**correlation_source("ASCA", asof="2026-10-15"), "adjustment": adjustment}
                before = correlate_download_rows(
                    rows,
                    source,
                    prior,
                    prior_intervals,
                    **certified_row_inputs(rows, source, prior, prior_intervals),
                )
                after = correlate_download_rows(
                    rows,
                    source,
                    extended,
                    extended_intervals,
                    **certified_row_inputs(rows, source, extended, extended_intervals),
                )
                self.assertEqual([], before["uncorrelated"])
                self.assertEqual([], after["uncorrelated"])
                self.assertEqual(1, len(before["correlated"]))
                self.assertEqual(1, len(after["correlated"]))
                old, new = before["correlated"][0], after["correlated"][0]
                self.assertEqual(513, new["row_count"])
                self.assertEqual("ASCA", new["historical_symbol"])
                self.assertEqual("predecessor-id", new["canonical_security"]["alpaca_asset_id"])
                old_identity, new_identity = old.pop("security_identity"), new.pop("security_identity")
                self.assertEqual(old_identity["security_id"], old["canonical_security"].pop("security_id"))
                self.assertEqual(new_identity["security_id"], new["canonical_security"].pop("security_id"))
                self.assertEqual(old, new)
                self.assertEqual(
                    sorted([*old_identity["member_alpaca_asset_ids"], "new-split-asset-id"]),
                    new_identity["member_alpaca_asset_ids"],
                )
                self.assertNotEqual(old_identity["security_id"], new_identity["security_id"])
                self.assertEqual(
                    security_id_for_alpaca_asset_ids(new_identity["member_alpaca_asset_ids"]),
                    new_identity["security_id"],
                )

    def test_forward_split_uses_the_same_identity_rule(self) -> None:
        lineage = same_symbol_split_lineage()
        lineage["corporate_actions"][0].update(
            {
                "action_type": "forward_split",
                "old_rate": 1,
                "new_rate": 4,
            }
        )

        edges, issues = build_uuid_lineage_edges(lineage)

        self.assertEqual([], issues)
        self.assertEqual(1, len(edges))
        self.assertEqual("forward_split", edges[0].edge_type)

    def test_split_identity_does_not_bootstrap_row_ownership(self) -> None:
        intervals, issues = build_ticker_ownership_intervals(same_symbol_split_lineage())

        self.assertEqual([], issues)
        self.assertEqual([], intervals)

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
                {"order": 2, "name": "name_change_boundary_conflict"},
                {
                    "order": 3,
                    "name": "complete_current_identity_suffix_after_ticker_reuse_gap",
                },
                {
                    "order": 4,
                    "name": "inactive_current_identity_suffix_after_ticker_reuse_gap",
                },
                {
                    "order": 5,
                    "name": "dated_terminal_identity_suffix_after_ticker_reuse_gap",
                },
                {"order": 6, "name": "ticker_reuse_gap_guard"},
                {"order": 7, "name": "one_session_distinct_uuid_transition"},
                {"order": 8, "name": CURRENT_SYMBOL_REUSE_RULE_NAME},
                {"order": 9, "name": "asof_selected_uuid_lineage_interval"},
                {"order": 10, "name": "exact_independently_resolved_predecessor_prefix"},
                {"order": 11, "name": LATEST_DATED_OWNERSHIP_INTERVAL_RULE_NAME},
                {"order": 12, "name": "requested_symbol_dated_uuid_interval"},
                {"order": 13, "name": "unique_uuid_candidate_with_warnings"},
                {"order": 14, "name": "bounded_retired_name_change_continuation"},
                {"order": 15, "name": "unresolved_uuid_candidates"},
                {"order": 16, "name": "stable_continuous_ticker_identity_set"},
            ],
            row_correlation_rule_order(),
        )

    def test_complete_current_suffix_keeps_fb_prefix_uncorrelated(self) -> None:
        lineage = fb_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        rows = [
            {"t": "2022-06-08T04:00:00Z"},
            {"t": "2025-06-26T04:00:00Z"},
        ]

        source = correlation_source("FB", asof="2026-10-06")
        source["request_to"] = "2025-06-27T00:00:00Z"
        branches = correlate_download_rows(
            rows,
            source,
            lineage,
            intervals,
        )

        self.assertEqual([], evidence_issues)
        self.assertEqual(1, len(branches["correlated"]))
        self.assertEqual("2025-06-26", branches["correlated"][0]["first_session_date"])
        self.assertEqual(
            "complete_current_identity_suffix_after_ticker_reuse_gap",
            branches["correlated"][0]["correlation_method"],
        )
        self.assertEqual(1, len(branches["uncorrelated"]))
        meta_segment = branches["uncorrelated"][0]
        self.assertEqual(
            ["prior_pricing_run_before_ticker_reuse_gap"],
            meta_segment["reason_codes"],
        )
        self.assertEqual(2, meta_segment["row_count"] + branches["correlated"][0]["row_count"])

    def test_asof_uuid_branch_routes_reused_b_ticker_to_exact_barrick_uuids(self) -> None:
        lineage = b_lineage()
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)
        uuid_edges, edge_issues = certified_edges(lineage)

        self.assertEqual([], edge_issues)
        self.assertEqual(1, len(uuid_edges))
        self.assertEqual("historical-barrick-id", uuid_edges[0].predecessor_asset_id)
        self.assertEqual("current-barrick-id", uuid_edges[0].successor_asset_id)
        self.assertEqual(
            ["cash_merger_boundary_disagreement"],
            [issue["reason_code"] for issue in evidence_issues],
        )

        rows = complete_market_session_rows("2020-01-02", "2025-05-10")
        branches = correlate_download_rows(
            rows,
            correlation_source("B", asof="2026-10-06"),
            lineage,
            intervals,
            **certified_row_inputs(rows, correlation_source("B", asof="2026-10-06"), lineage, intervals),
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(2, len(branches["correlated"]))
        historical_segment, current_segment = branches["correlated"]
        self.assertEqual(
            "historical-barrick-id",
            historical_segment["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual("067901108", historical_segment["canonical_security"]["cusip"])
        self.assertEqual("2020-01-02", historical_segment["first_session_date"])
        self.assertEqual("2025-05-08", historical_segment["last_session_date"])
        self.assertIn(
            "asof_uuid_lineage_overrode_requested_symbol_occupancy",
            historical_segment["warnings"],
        )
        self.assertEqual(
            "current-barrick-id",
            current_segment["canonical_security"]["alpaca_asset_id"],
        )
        self.assertEqual("06849F108", current_segment["canonical_security"]["cusip"])
        self.assertEqual("2025-05-09", current_segment["first_session_date"])
        self.assertEqual("2025-05-09", current_segment["last_session_date"])
        self.assertEqual(
            len(rows),
            historical_segment["row_count"] + current_segment["row_count"],
        )
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
        rows = complete_market_session_rows("2021-07-01", "2023-07-04")

        branches = correlate_download_rows(
            rows,
            correlation_source("META", asof="2026-10-06"),
            lineage,
            intervals,
            **certified_row_inputs(rows, correlation_source("META", asof="2026-10-06"), lineage, intervals),
        )

        self.assertEqual([], branches["uncorrelated"])
        self.assertEqual(["FB", "META"], [item["historical_symbol"] for item in branches["correlated"]])
        self.assertEqual(
            {"meta-id"},
            {item["canonical_security"]["alpaca_asset_id"] for item in branches["correlated"]},
        )
        self.assertEqual(len(rows), sum(item["row_count"] for item in branches["correlated"]))

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
            closed_history_evidence=True,
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

    def test_gapless_three_unconnected_ids_do_not_receive_one_internal_security_id(self) -> None:
        lineage = fun_like_lineage()
        middle_id = "middle-fun-alpaca-id"
        middle_cusip = "150185205"
        middle = {
            **asset(middle_id, "FUN", "Six Flags Entertainment Corporation"),
            "status": "inactive",
            "tradable": False,
            "lookup_cusips": [middle_cusip],
        }
        lineage["identities"].append(middle)
        lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "FUN",
                middle_cusip,
                middle_id,
                action_type="cash_dividend",
                action_id="middle-fun-dividend",
                process_date="2024-06-21",
                role="symbol",
            )
        )
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], evidence_issues)
        branches = correlate_download_rows(
            [
                {"t": "2024-06-28T04:00:00Z"},
                {"t": "2024-07-01T04:00:00Z"},
                {"t": "2024-07-02T04:00:00Z"},
            ],
            fun_like_source(),
            lineage,
            intervals,
            closed_history_evidence=True,
        )

        self.assertEqual([], branches["correlated"])
        fallback = branches["uncorrelated"][0]["terminal_correlation_fallback"]
        self.assertFalse(fallback["eligible"])
        self.assertFalse(fallback["criteria"]["candidate_identity_continuity_supported"])
        self.assertEqual(3, len(fallback["candidate_identity_components"]))

    def test_disconnected_ids_need_complete_informative_history(self) -> None:
        lineage = fun_like_lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)
        rows = [
            {"t": "2024-06-28T04:00:00Z"},
            {"t": "2024-07-01T04:00:00Z"},
            {"t": "2024-07-02T04:00:00Z"},
        ]

        branches = correlate_download_rows(
            rows,
            fun_like_source(),
            lineage,
            intervals,
            closed_history_evidence=False,
        )

        self.assertEqual([], branches["correlated"])
        fallback = branches["uncorrelated"][0]["terminal_correlation_fallback"]
        self.assertFalse(fallback["criteria"]["candidate_identity_continuity_supported"])

    def test_continuous_ticker_history_excludes_mismatched_asset_ticker_candidate(self) -> None:
        lineage = fun_like_lineage()
        unrelated_id = "unrelated-alpaca-id"
        unrelated_cusip = "G00000101"
        lineage["identities"].append(
            {
                **asset(unrelated_id, "OTHER", "Unrelated Company"),
                "lookup_cusips": [unrelated_cusip],
            }
        )
        lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "FUN",
                unrelated_cusip,
                unrelated_id,
                action_type="cash_dividend",
                action_id="bad-vendor-dividend",
                process_date="2024-06-24",
                role="symbol",
            )
        )
        intervals, evidence_issues = build_ticker_ownership_intervals(lineage)

        self.assertEqual([], evidence_issues)
        branches = correlate_download_rows(
            [
                {"t": "2024-06-28T04:00:00Z"},
                {"t": "2024-07-01T04:00:00Z"},
                {"t": "2024-07-02T04:00:00Z"},
            ],
            fun_like_source(),
            lineage,
            intervals,
            closed_history_evidence=True,
        )

        self.assertEqual([], branches["uncorrelated"])
        segment = branches["correlated"][0]
        self.assertEqual(
            ["current-fun-alpaca-id", "old-fun-alpaca-id"],
            segment["security_identity"]["member_alpaca_asset_ids"],
        )
        fallback = segment["terminal_correlation_fallback"]
        self.assertEqual(
            [unrelated_id],
            fallback["excluded_nonmatching_ticker_candidate_alpaca_asset_ids"],
        )

    def test_same_ticker_candidates_with_different_names_stay_uncorrelated(self) -> None:
        lineage = fun_like_lineage()
        lineage["identities"][0]["name"] = "Different Company"
        intervals, _ = build_ticker_ownership_intervals(lineage)

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

        self.assertEqual([], branches["correlated"])
        fallback = branches["uncorrelated"][0]["terminal_correlation_fallback"]
        self.assertFalse(fallback["eligible"])
        self.assertFalse(fallback["criteria"]["matching_ticker_candidate_asset_names_agree"])

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


class CorrelationReportTests(unittest.TestCase):
    def test_version_changelog_matches_correlation_version(self) -> None:
        self.assertEqual(CORRELATION_VERSION, len(validate_correlation_change_log()))
        self.assertEqual(frozenset(range(8, CORRELATION_VERSION + 1)), SUPPORTED_CORRELATION_VERSIONS)

    def test_report_preserves_partition_and_semantic_evidence_digest(self) -> None:
        rows = load_bars("ten_valid_daily_bars.json")
        source = SimpleNamespace(
            path=Path("raw.csv.gz"),
            rows=rows,
            manifest={
                "symbol": "TEST",
                "adjustment": "raw",
                "feed": "sip",
                "timeframe": "1Day",
                "request_from": "2024-01-01T00:00:00Z",
                "request_to": "2024-01-17T00:00:00Z",
                "asof": "2024-01-17",
                "sha256": "a" * 64,
            },
            pipeline_state="source_published",
            integrity_failure=None,
        )
        lineage_report = self._test_lineage_report()
        root = Path("/unused-correlation-input")
        reader = Mock(return_value=source)
        exclusions = empty_exclusions(root, "TEST")
        exclusion_reader = Mock(return_value=exclusions)
        sessions = Mock(return_value=None)
        first = build_security_correlation_report(
            lineage_report,
            root,
            authenticated_download_reader=reader,
            verified_session_exclusions_reader=exclusion_reader,
            authenticated_symbol_sessions=sessions,
        )
        reader.assert_called_once_with(root, lineage_report["local"]["directory"], {"file": "raw.csv.gz"})
        exclusion_reader.assert_called_once_with(root, "TEST")
        self.assertEqual(first["verified_session_exclusions"], exclusions)
        self.assertTrue(first["row_partition_complete"])
        self.assertEqual(first["source_row_count"], 10)
        self.assertEqual(first["correlated"]["row_count"], 6)
        self.assertEqual(first["uncorrelated"]["row_count"], 4)
        self.assertEqual(first["correlation_version"], CORRELATION_VERSION)
        self.assertEqual(first["symbol_disposition"]["classification"], "alpaca_asset_identity_available")
        self.assertEqual(len(first["uuid_lineage_edges"]), 1)
        self.assertEqual(first["lineage_evidence_sha256"], lineage_evidence_sha256(lineage_report))

        lineage_report["generated_at"] = "2099-01-01T00:00:00Z"
        lineage_report["lineage"]["api_request_count"] = 999
        second = build_security_correlation_report(
            lineage_report,
            root,
            authenticated_download_reader=reader,
            verified_session_exclusions_reader=exclusion_reader,
            authenticated_symbol_sessions=sessions,
        )
        self.assertEqual(first["lineage_evidence_sha256"], second["lineage_evidence_sha256"])
        self.assertEqual(first["correlated"], second["correlated"])
        self.assertEqual(first["uncorrelated"], second["uncorrelated"])
        self.assertEqual(first["uuid_lineage_edges"], second["uuid_lineage_edges"])
        lineage_report["lineage"]["identities"][0]["name"] = "Changed evidence"
        self.assertNotEqual(first["lineage_evidence_sha256"], lineage_evidence_sha256(lineage_report))

    def _test_lineage_report(self) -> dict:
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
            "local": {
                "directory": "end_of_day/pricing/TEST",
                "present": True,
                "downloads": [{"file": "raw.csv.gz"}],
            },
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


if __name__ == "__main__":
    unittest.main()

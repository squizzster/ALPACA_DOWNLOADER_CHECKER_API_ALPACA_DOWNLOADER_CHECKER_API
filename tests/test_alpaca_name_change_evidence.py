"""Generic rename evidence gates, exercised through the complete group cascade."""

import copy
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy, load_policy
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS
from libs.alpaca_serving_catalogue.policy import _metadata
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge, occupancy

PRODUCTION_POLICY = load_authorized_policy()
V59_POLICY = Path(__file__).resolve().parents[1] / "resources/alpaca_security_correlation_v59_legacy_policy.yaml"


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


def rename(*, same_uuid=False, same_cusip=False, old="OLD", new="NEW", when="2024-01-05"):
    evidence = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
    add_name_change_edge(
        evidence,
        action_id="transition",
        process_date=when,
        predecessor=("old-id", old, "old-cusip", "Original security"),
        successor=(
            "old-id" if same_uuid else "new-id",
            new,
            "old-cusip" if same_cusip else "new-cusip",
            "Continuing security",
        ),
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


def exact_same_symbol_handoff():
    evidence = rename(old="HOLD", new="HOLD")
    identities = {item["id"]: item for item in evidence["identities"]}
    predecessor = identities["old-id"]
    successor = identities["new-id"]
    predecessor.update(exchange="NYSE", status="inactive", tradable=False)
    successor.update(exchange="NYSE", status="active", tradable=True)
    evidence["query"] = {
        "input_symbol": "HOLD",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": selected,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value, selected in (
            ("symbol", "HOLD", successor),
            ("cusip", "old-cusip", predecessor),
            ("cusip", "new-cusip", successor),
        )
    ]
    return evidence


def bounded_entry_same_symbol_handoff(
    *,
    entry_asset_id="old-id",
    entry_symbol="ARRIVE",
    entry_cusip="old-cusip",
    entry_date="2024-01-02",
):
    evidence = exact_same_symbol_handoff()
    add_name_change_edge(
        evidence,
        action_id="entry",
        process_date=entry_date,
        predecessor=(entry_asset_id, entry_symbol, entry_cusip, "Earlier label"),
        successor=("old-id", "HOLD", "old-cusip", "Original security"),
    )
    return evidence


def early_successor_bridge_evidence(*, same_uuid=True, same_cusip=True):
    evidence = rename(
        same_uuid=same_uuid,
        same_cusip=same_cusip,
        when="2024-01-09",
    )
    asset_id = "old-id" if same_uuid else "new-id"
    cusip = "old-cusip" if same_cusip else "new-cusip"
    current = {
        "id": asset_id,
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "CURRENT",
        "name": "Current label",
        "status": "active",
        "tradable": True,
        "lookup_symbols": [],
        "lookup_cusips": [cusip],
    }
    evidence["identities"] = [current]
    evidence["asset_lookups"] = [
        {
            "lookup_kind": "cusip",
            "lookup": cusip,
            "asset": current,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
    ]
    evidence["query"] = {
        "input_symbol": "NEW",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    return evidence


def terminal_carry_forward_evidence():
    evidence = rename(same_uuid=True, same_cusip=True, when="2024-01-05")
    successor = {
        "id": "old-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "NEW",
        "name": "Successor ordinary shares",
        "status": "active",
        "tradable": True,
        "lookup_symbols": ["NEW"],
        "lookup_cusips": ["old-cusip"],
    }
    terminal = {
        "id": "terminal-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "OLD",
        "name": "Retired acquiree ordinary shares",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": ["OLD"],
        "lookup_cusips": ["terminal-cusip"],
    }
    evidence["identities"] = [successor, terminal]
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": selected,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value, selected in (
            ("symbol", "OLD", terminal),
            ("symbol", "NEW", successor),
            ("cusip", "terminal-cusip", terminal),
            ("cusip", "old-cusip", successor),
        )
    ]
    evidence["corporate_actions"].append(
        {
            "id": "terminal-stock-merger",
            "action_type": "stock_merger",
            "acquiree_symbol": "OLD",
            "acquiree_cusip": "terminal-cusip",
            "acquiree_rate": 1,
            "acquirer_symbol": "NEW",
            "acquirer_cusip": "old-cusip",
            "acquirer_rate": 0.1,
            "effective_date": "2024-01-05",
            "process_date": "2024-01-05",
            "payable_date": "2024-01-05",
        }
    )
    evidence["symbol_occupancy_evidence"].append(
        {
            "symbol": "OLD",
            "cusip": "terminal-cusip",
            "asset_id": "terminal-id",
            "evidence": [
                {
                    "symbol": "OLD",
                    "cusip": "terminal-cusip",
                    "role": "acquiree",
                    "action_type": "stock_merger",
                    "action_id": "terminal-stock-merger",
                    "process_date": "2024-01-05",
                }
            ],
        }
    )
    evidence["query"] = {
        "input_symbol": "OLD",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    return evidence


def terminal_carry_forward_histories():
    days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")
    predecessor = bars(days)
    final_close = predecessor[2]["c"]
    for row in predecessor[3:]:
        row.update(dict.fromkeys(("o", "h", "l", "c"), final_close), v="0", n="0", vw="0")
    return {"OLD": predecessor, "NEW": bars(days[3:], 40)}


def pre_entry_carry_forward_case():
    sessions = expected_market_session_coverage("2023-01-03", "2024-02-01").session_dates[:110]
    boundary = sessions[-2]
    evidence = rename(
        same_uuid=True,
        same_cusip=True,
        old="OLD",
        new="NEW",
        when=boundary,
    )
    current = evidence["identities"][0]
    current.update(symbol="NEW", status="active", tradable=True)
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": current,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value in (("symbol", "NEW"), ("cusip", "old-cusip"))
    ]
    evidence["query"] = {
        "input_symbol": "NEW",
        "corporate_actions_start": "2023-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    destination = bars(sessions)
    carried = destination[2]["c"]
    for row in destination[3:-2]:
        row.update(dict.fromkeys(("o", "h", "l", "c"), carried), v="0", n="0", vw="0")
    predecessor = bars(sessions[-7:-2], 40)
    return evidence, {"OLD": predecessor, "NEW": destination}, sessions[3], sessions[-3], boundary


def component_anchored_early_successor_case():
    sessions = expected_market_session_coverage("2024-01-02", "2024-02-01").session_dates[:9]
    source_boundary = sessions[3]
    provider_boundary = sessions[6]
    evidence = rename(
        same_uuid=True,
        same_cusip=True,
        old="OLD.WS",
        new="NEW.WS",
        when=provider_boundary,
    )
    current = evidence["identities"][0]
    current.update(symbol="NEW.WS", status="active", tradable=True)
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": current,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value in (("symbol", "NEW.WS"), ("cusip", "old-cusip"))
    ]
    evidence["query"] = {
        "input_symbol": "NEW.WS",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    evidence["corporate_actions"].append(
        {
            "id": "component-separation",
            "action_type": "unit_split",
            "old_symbol": "OLD.U",
            "old_cusip": "unit-cusip",
            "old_rate": 1,
            "new_symbol": "OLD",
            "new_cusip": "common-cusip",
            "new_rate": 1,
            "alternate_symbol": "OLD.WS",
            "alternate_cusip": "legacy-warrant-cusip",
            "alternate_rate": 0.33333,
            "effective_date": source_boundary,
            "process_date": source_boundary,
        }
    )
    evidence["symbol_occupancy_evidence"].append(
        {
            "symbol": "OLD.WS",
            "cusip": "legacy-warrant-cusip",
            "asset_id": None,
            "evidence": [
                {
                    "symbol": "OLD.WS",
                    "cusip": "legacy-warrant-cusip",
                    "role": "alternate",
                    "action_type": "unit_split",
                    "action_id": "component-separation",
                    "process_date": source_boundary,
                }
            ],
        }
    )
    return (
        evidence,
        {"OLD.WS": bars(sessions[:3]), "NEW.WS": bars(sessions[3:], 40)},
        sessions,
    )


def internal_component_handoff_case():
    sessions = expected_market_session_coverage("2024-01-02", "2024-02-01").session_dates[:12]
    component_date = sessions[5]
    rename_date = sessions[8]
    reuse_date = sessions[10]
    evidence = rename(
        same_uuid=True,
        same_cusip=True,
        old="OLD.WS",
        new="NEW.WS",
        when=rename_date,
    )
    continuing = evidence["identities"][0]
    continuing.update(symbol="NEW.WS", status="inactive", tradable=False)
    current = {
        "id": "current-warrant-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "OLD.WS",
        "name": "Reused warrant",
        "status": "active",
        "tradable": True,
        "lookup_symbols": ["OLD.WS"],
        "lookup_cusips": ["current-warrant-cusip"],
    }
    evidence["identities"].append(current)
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": selected,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value, selected in (
            ("symbol", "NEW.WS", continuing),
            ("cusip", "old-cusip", continuing),
            ("symbol", "OLD.WS", current),
            ("cusip", "current-warrant-cusip", current),
        )
    ]
    component_action = {
        "id": "component-separation",
        "action_type": "unit_split",
        "old_symbol": "OLD.U",
        "old_cusip": "unit-cusip",
        "old_rate": 1,
        "new_symbol": "OLD",
        "new_cusip": "common-cusip",
        "new_rate": 1,
        "alternate_symbol": "OLD.WS",
        "alternate_cusip": "legacy-warrant-cusip",
        "alternate_rate": 0.5,
        "effective_date": component_date,
        "process_date": component_date,
    }
    current_action = {
        "id": "current-component-arrival",
        "action_type": "unit_split",
        "old_symbol": "CURRENT.U",
        "old_cusip": "current-unit-cusip",
        "old_rate": 1,
        "new_symbol": "CURRENT",
        "new_cusip": "current-common-cusip",
        "new_rate": 1,
        "alternate_symbol": "OLD.WS",
        "alternate_cusip": "current-warrant-cusip",
        "alternate_rate": 0.5,
        "effective_date": reuse_date,
        "process_date": reuse_date,
    }
    evidence["corporate_actions"].extend((component_action, current_action))
    evidence["symbol_occupancy_evidence"].extend(
        (
            occupancy(
                "OLD.WS",
                "legacy-warrant-cusip",
                None,
                action_type="unit_split",
                action_id=component_action["id"],
                process_date=component_date,
                role="alternate",
            ),
            occupancy(
                "OLD.WS",
                "current-warrant-cusip",
                current["id"],
                action_type="unit_split",
                action_id=current_action["id"],
                process_date=reuse_date,
                role="alternate",
            ),
        )
    )
    evidence["query"] = {
        "input_symbol": "NEW.WS",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    predecessor = bars(sessions)
    carried = predecessor[5]["c"]
    for row in predecessor[6:10]:
        row.update(dict.fromkeys(("o", "h", "l", "c"), carried), v="0", n="0", vw="0")
    predecessor[10:] = bars(sessions[10:], 70)
    successor = [*copy.deepcopy(predecessor[:6]), *bars(sessions[6:], 40)]
    return evidence, {"OLD.WS": predecessor, "NEW.WS": successor}, sessions


def extended_component_handoff_case():
    sessions = expected_market_session_coverage("2023-01-03", "2024-12-01").session_dates[:119]
    source_boundary = sessions[110]
    provider_boundary = sessions[113]
    terminal = sessions[117]
    evidence = rename(
        same_uuid=True,
        same_cusip=True,
        old="OLD",
        new="NEW",
        when=provider_boundary,
    )
    current = evidence["identities"][0]
    current.update(symbol="NEW", status="inactive", tradable=False)
    component = {
        "id": "component-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "OLD",
        "name": "Dated component occupant",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": [],
        "lookup_cusips": ["component-cusip"],
    }
    undated = {
        "id": "undated-symbol-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "OLD",
        "name": "Undated symbol lookup",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": ["OLD"],
        "lookup_cusips": [],
    }
    evidence["identities"].extend((component, undated))
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": selected,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value, selected in (
            ("symbol", "NEW", current),
            ("cusip", "old-cusip", current),
            ("cusip", "component-cusip", component),
            ("symbol", "OLD", undated),
        )
    ]
    evidence["query"] = {
        "input_symbol": "NEW",
        "corporate_actions_start": "2023-01-01",
        "corporate_actions_end": "2024-12-01",
        "corporate_actions_data_quality": "complete",
    }
    evidence["corporate_actions"].extend(
        (
            {
                "id": "component-separation",
                "action_type": "unit_split",
                "old_symbol": "OLD.U",
                "old_cusip": "unit-cusip",
                "old_rate": 1,
                "new_symbol": "OLD",
                "new_cusip": "component-cusip",
                "new_rate": 1,
                "alternate_symbol": "OLD.WS",
                "alternate_cusip": "component-warrant-cusip",
                "alternate_rate": 0.33333,
                "effective_date": source_boundary,
                "process_date": source_boundary,
            },
            {
                "id": "terminal-cash-merger",
                "action_type": "cash_merger",
                "acquiree_symbol": "NEW",
                "acquiree_cusip": "old-cusip",
                "rate": 5,
                "effective_date": terminal,
                "process_date": terminal,
                "payable_date": terminal,
            },
        )
    )
    evidence["symbol_occupancy_evidence"].extend(
        (
            {
                "symbol": "OLD",
                "cusip": "component-cusip",
                "asset_id": "component-id",
                "evidence": [
                    {
                        "symbol": "OLD",
                        "cusip": "component-cusip",
                        "role": "new",
                        "action_type": "unit_split",
                        "action_id": "component-separation",
                        "process_date": source_boundary,
                    }
                ],
            },
            {
                "symbol": "NEW",
                "cusip": "old-cusip",
                "asset_id": "old-id",
                "evidence": [
                    {
                        "symbol": "NEW",
                        "cusip": "old-cusip",
                        "role": "acquiree",
                        "action_type": "cash_merger",
                        "action_id": "terminal-cash-merger",
                        "process_date": terminal,
                    }
                ],
            },
        )
    )
    predecessor = bars(sessions[:110])
    carried = predecessor[0]["c"]
    for row in predecessor[:100]:
        row.update(dict.fromkeys(("o", "h", "l", "c"), carried), v="0", n="0", vw="0")
    return evidence, {"OLD": predecessor, "NEW": bars(sessions[110:117], 40)}, sessions


def post_padding_alias_case(*, same_uuid=True, same_cusip=True):
    sessions = expected_market_session_coverage("2024-01-02", "2024-09-01").session_dates[:115]
    overlap_start = 108
    boundary_index = 111
    boundary = sessions[boundary_index]
    evidence = rename(
        same_uuid=same_uuid,
        same_cusip=same_cusip,
        when=boundary,
    )
    successor_id = "old-id" if same_uuid else "new-id"
    successor_cusip = "old-cusip" if same_cusip else "new-cusip"
    current = {
        "id": successor_id,
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "NEW",
        "name": "Current successor label",
        "status": "active",
        "tradable": True,
        "lookup_symbols": ["NEW"],
        "lookup_cusips": [successor_cusip],
    }
    evidence["identities"] = [current]
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": current,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value in (("symbol", "NEW"), ("cusip", successor_cusip))
    ]
    evidence["query"] = {
        "input_symbol": "NEW",
        "corporate_actions_start": "2024-01-01",
        "corporate_actions_end": "2024-09-01",
        "corporate_actions_data_quality": "complete",
    }

    predecessor = bars(sessions[:boundary_index])
    carried_close = predecessor[2]["c"]
    for row in predecessor[3:overlap_start]:
        row.update(dict.fromkeys(("o", "h", "l", "c"), carried_close), v="0", n="0", vw="0")
    overlap = bars(sessions[overlap_start:boundary_index], 40)
    predecessor[overlap_start:] = copy.deepcopy(overlap)
    successor = [
        *copy.deepcopy(overlap),
        *bars(sessions[boundary_index : boundary_index + 4], 50),
    ]
    return evidence, {"OLD": predecessor, "NEW": successor}, boundary


def successor_prefix_handoff_case(*, same_uuid=True, same_cusip=True):
    evidence, histories, boundary = post_padding_alias_case(
        same_uuid=same_uuid,
        same_cusip=same_cusip,
    )
    predecessor = histories["OLD"]
    prefix_start = 4
    # Begin the successor-labelled source inside the donor's prolonged padding,
    # then retain its exact traded tail and ordinary post-boundary history.
    histories["NEW"] = [
        *copy.deepcopy(predecessor[prefix_start:]),
        *copy.deepcopy(histories["NEW"][3:]),
    ]
    return evidence, histories, boundary, len(predecessor) - prefix_start


def active_successor_prefix_reused_ticker_case():
    sessions = expected_market_session_coverage("2023-01-03", "2024-02-01").session_dates
    boundary_index = len(sessions) - 5
    boundary = sessions[boundary_index]
    prefix_sessions = sessions[boundary_index - 7 : boundary_index]
    evidence = rename(same_uuid=True, same_cusip=True, when=boundary)
    current = evidence["identities"][0]
    current.update(
        symbol="NEW",
        status="active",
        tradable=True,
        lookup_symbols=["NEW"],
        lookup_cusips=["old-cusip"],
    )
    retired = {
        "id": "retired-old-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "OLD",
        "name": "Retired old-label occupant",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": ["OLD"],
        "lookup_cusips": ["retired-cusip"],
    }
    exit_date = sessions[1]
    exit_action = {
        "id": "retired-old-exit",
        "action_type": "cash_merger",
        "acquiree_symbol": "OLD",
        "acquiree_cusip": "retired-cusip",
        "rate": 10,
        "effective_date": exit_date,
        "process_date": exit_date,
    }
    evidence["corporate_actions"].insert(0, exit_action)
    evidence["symbol_occupancy_evidence"].append(
        occupancy(
            "OLD",
            "retired-cusip",
            retired["id"],
            action_type="cash_merger",
            action_id=exit_action["id"],
            process_date=exit_date,
            role="acquiree",
        )
    )
    evidence["identities"].append(retired)
    evidence["asset_lookups"] = [
        {
            "lookup_kind": kind,
            "lookup": value,
            "asset": selected,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
        for kind, value, selected in (
            ("symbol", "NEW", current),
            ("cusip", "old-cusip", current),
            ("symbol", "OLD", retired),
            ("cusip", "retired-cusip", retired),
        )
    ]
    evidence["query"] = {
        "input_symbol": "NEW",
        "corporate_actions_start": "2023-01-01",
        "corporate_actions_end": "2024-02-01",
        "corporate_actions_data_quality": "complete",
    }
    overlap = bars(prefix_sessions, 40)
    histories = {
        "OLD": [*bars(sessions[:1], 5), *copy.deepcopy(overlap)],
        "NEW": [*copy.deepcopy(overlap), *bars(sessions[boundary_index : boundary_index + 4], 50)],
    }
    return evidence, histories, boundary, len(overlap)


def successor_prefix_later_split_case():
    evidence, histories, boundary, prefix_rows = successor_prefix_handoff_case()
    split_date = histories["NEW"][-2]["t"][:10]
    split_action = {
        "id": "later-successor-reverse-split",
        "action_type": "reverse_split",
        "symbol": "NEW",
        "old_cusip": "pre-split-successor-cusip",
        "new_cusip": "old-cusip",
        "old_rate": 8,
        "new_rate": 1,
        "ex_date": split_date,
        "effective_date": split_date,
        "process_date": split_date,
        "payable_date": split_date,
    }
    prior = {
        "id": "pre-split-successor-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "NEW",
        "name": "Pre-split successor label",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": [],
        "lookup_cusips": ["pre-split-successor-cusip"],
    }
    evidence["corporate_actions"].append(split_action)
    evidence["identities"].append(prior)
    evidence["asset_lookups"].append(
        {
            "lookup_kind": "cusip",
            "lookup": "pre-split-successor-cusip",
            "asset": prior,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
    )
    evidence["symbol_occupancy_evidence"].extend(
        (
            occupancy(
                "NEW",
                "pre-split-successor-cusip",
                "pre-split-successor-id",
                action_type="reverse_split",
                action_id=split_action["id"],
                process_date=split_date,
                role="old",
            ),
            occupancy(
                "NEW",
                "old-cusip",
                "old-id",
                action_type="reverse_split",
                action_id=split_action["id"],
                process_date=split_date,
                role="new",
            ),
        )
    )
    return evidence, histories, boundary, prefix_rows


def split_adjusted_post_padding_alias_case():
    evidence, histories, boundary = post_padding_alias_case()
    split_index = 80
    split_date = histories["OLD"][split_index]["t"][:10]
    split_action = {
        "id": "predecessor-reverse-split",
        "action_type": "reverse_split",
        "symbol": "OLD",
        "old_cusip": "pre-split-cusip",
        "new_cusip": "old-cusip",
        "old_rate": 16,
        "new_rate": 1,
        "ex_date": split_date,
        "effective_date": split_date,
        "process_date": split_date,
        "payable_date": split_date,
    }
    evidence["corporate_actions"].insert(0, split_action)
    evidence["symbol_occupancy_evidence"].extend(
        (
            occupancy(
                "OLD",
                "pre-split-cusip",
                "pre-split-id",
                action_type="reverse_split",
                action_id=split_action["id"],
                process_date=split_date,
                role="old",
            ),
            occupancy(
                "OLD",
                "old-cusip",
                "old-id",
                action_type="reverse_split",
                action_id=split_action["id"],
                process_date=split_date,
                role="new",
            ),
        )
    )
    prior = {
        "id": "pre-split-id",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": "OLD",
        "name": "Pre-split predecessor",
        "status": "inactive",
        "tradable": False,
        "lookup_symbols": ["OLD"],
        "lookup_cusips": ["pre-split-cusip"],
    }
    evidence["identities"].append(prior)
    evidence["asset_lookups"].append(
        {
            "lookup_kind": "cusip",
            "lookup": "pre-split-cusip",
            "asset": prior,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
    )

    acquisition_date = histories["NEW"][5]["t"][:10]
    evidence["corporate_actions"].append(
        {
            "id": "later-successor-acquisition",
            "action_type": "stock_merger",
            "acquiree_symbol": "OTHER",
            "acquiree_cusip": "other-cusip",
            "acquiree_rate": 1,
            "acquirer_symbol": "NEW",
            "acquirer_cusip": "old-cusip",
            "acquirer_rate": 0.125,
            "effective_date": acquisition_date,
            "process_date": acquisition_date,
            "payable_date": acquisition_date,
        }
    )

    adjusted = {}
    for adjustment in ("split", "split,dividend", "split,spin-off", "all"):
        rows = copy.deepcopy(histories["OLD"])
        for row in rows[3:split_index]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "192"))
        adjusted[adjustment] = rows
    return evidence, histories, boundary, {"OLD": adjusted}


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


def active_successor_prefix_inputs(histories, *, view_histories=None):
    inputs = inputs_for(
        histories,
        asofs=dict.fromkeys(histories, "2024-01-31"),
        view_histories=view_histories,
    )
    for view in inputs.values():
        for download in view.downloads.values():
            download.manifest["request_from"] = "2023-01-01T00:00:00Z"
            download.manifest["request_to"] = "2024-02-01T00:00:00Z"
        view.source["history_start"] = "2023-01-01T00:00:00Z"
        view.source["latest_request"]["end_exclusive"] = "2024-02-01T00:00:00Z"
    return inputs


def run(evidence, histories, **input_options):
    return correlate(
        Path("."),
        inputs_for(histories, **input_options),
        {symbol: evidence for symbol in histories},
        policy=PRODUCTION_POLICY,
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

    def test_exact_same_symbol_handoff_restores_predecessor_rows_and_one_identity(self):
        evidence = exact_same_symbol_handoff()
        reports = run(evidence, {"HOLD": bars(self.days)})
        report = reports["HOLD"]

        self.assertEqual(len(self.days) * len(ADJUSTMENTS), report["correlated"]["row_count"])
        self.assertEqual(0, report["uncorrelated"]["row_count"])
        self.assertEqual([], raw_segments(report, "uncorrelated"))
        segments = raw_segments(report)
        self.assertEqual(
            ["old-id", "new-id"], [item["canonical_security"]["alpaca_asset_id"] for item in segments]
        )
        self.assertEqual([3, 2], [item["row_count"] for item in segments])
        self.assertEqual(
            {("new-id", "old-id")},
            {tuple(item["security_identity"]["member_alpaca_asset_ids"]) for item in segments},
        )
        self.assertEqual(
            1,
            len({item["security_identity"]["security_id"] for item in segments}),
        )
        self.assertEqual(
            {"explicit_uuid_lineage"},
            {item["security_identity"]["basis"] for item in segments},
        )
        self.assertEqual(1, len(report["uuid_lineage_edges"]))
        self.assertEqual("transition", report["uuid_lineage_edges"][0]["corporate_action_id"])

    def test_exact_same_symbol_handoff_accepts_one_bounded_identity_preserving_entry(self):
        evidence = bounded_entry_same_symbol_handoff()
        days = ("2023-12-29", *self.days)
        report = run(evidence, {"HOLD": bars(days)})["HOLD"]

        self.assertEqual(5 * len(ADJUSTMENTS), report["correlated"]["row_count"])
        self.assertEqual(len(ADJUSTMENTS), report["uncorrelated"]["row_count"])
        self.assertEqual(
            [("2023-12-29", "2023-12-29", 1)],
            [
                (segment["first_session_date"], segment["last_session_date"], segment["row_count"])
                for segment in raw_segments(report, "uncorrelated")
            ],
        )
        segments = raw_segments(report)
        self.assertEqual(
            ["old-id", "new-id"], [item["canonical_security"]["alpaca_asset_id"] for item in segments]
        )
        self.assertEqual([3, 2], [item["row_count"] for item in segments])
        self.assertEqual(
            {("new-id", "old-id")},
            {tuple(item["security_identity"]["member_alpaca_asset_ids"]) for item in segments},
        )
        self.assertEqual(
            {"entry", "transition"},
            {item["corporate_action_id"] for item in report["uuid_lineage_edges"]},
        )

    def test_bounded_entry_same_symbol_handoff_rejects_nearest_counterexamples(self):
        conflicting_date = bounded_entry_same_symbol_handoff()
        next(action for action in conflicting_date["corporate_actions"] if action.get("id") == "entry")[
            "effective_date"
        ] = "2024-01-03"
        competing_branch = bounded_entry_same_symbol_handoff()
        add_name_change_edge(
            competing_branch,
            action_id="competing-entry",
            process_date="2024-01-03",
            predecessor=("old-id", "HOLD", "old-cusip", "Original security"),
            successor=("old-id", "OTHER", "old-cusip", "Other label"),
        )
        cases = {
            "incoming UUID changes": bounded_entry_same_symbol_handoff(entry_asset_id="entry-id"),
            "incoming CUSIP changes": bounded_entry_same_symbol_handoff(entry_cusip="entry-cusip"),
            "same-label recurrence": bounded_entry_same_symbol_handoff(entry_symbol="HOLD"),
            "incoming action follows handoff": bounded_entry_same_symbol_handoff(entry_date="2024-01-08"),
            "incoming dates conflict": conflicting_date,
            "competing branch": competing_branch,
        }
        for label, evidence in cases.items():
            with self.subTest(label=label):
                report = run(evidence, {"HOLD": bars(self.days)})["HOLD"]
                self.assertNotIn(
                    "transition",
                    {item["corporate_action_id"] for item in report["uuid_lineage_edges"]},
                )
                self.assertFalse(
                    any(
                        segment["security_identity"]["member_alpaca_asset_ids"] == ["new-id", "old-id"]
                        for segment in raw_segments(report)
                    )
                )

    def test_exact_same_symbol_handoff_retains_every_material_veto(self):
        def lookup(document, kind, value):
            return next(
                item
                for item in document["asset_lookups"]
                if item["lookup_kind"] == kind and item["lookup"] == value
            )

        def missing_old_lookup(document):
            document["asset_lookups"] = [
                item for item in document["asset_lookups"] if item["lookup"] != "old-cusip"
            ]

        def duplicate_old_lookup(document):
            document["asset_lookups"].append(copy.deepcopy(lookup(document, "cusip", "old-cusip")))

        def mismatched_old_uuid(document):
            lookup(document, "cusip", "old-cusip")["asset"]["id"] = "other-id"

        def duplicate_symbol_lookup(document):
            document["asset_lookups"].append(copy.deepcopy(lookup(document, "symbol", "HOLD")))

        def current_lookup_is_predecessor(document):
            lookup(document, "symbol", "HOLD")["asset"] = lookup(document, "cusip", "old-cusip")["asset"]

        def predecessor_still_active(document):
            lookup(document, "cusip", "old-cusip")["asset"].update(status="active", tradable=True)

        def successor_is_inactive(document):
            for item in document["asset_lookups"]:
                if item.get("asset", {}).get("id") == "new-id":
                    item["asset"].update(status="inactive", tradable=False)

        def exchange_changes(document):
            lookup(document, "cusip", "old-cusip")["asset"]["exchange"] = "NASDAQ"

        def third_occupant(document):
            document["identities"].append(
                {
                    "id": "third-id",
                    "class": "us_equity",
                    "exchange": "NYSE",
                    "symbol": "HOLD",
                    "status": "inactive",
                    "tradable": False,
                }
            )

        def third_input_occupant(document):
            document["input_symbol_occupants"] = [
                {
                    "symbol": "HOLD",
                    "cusip": "third-cusip",
                    "asset_id": "third-id",
                    "evidence": [],
                }
            ]

        def competing_rename(document):
            add_name_change_edge(
                document,
                action_id="competing-transition",
                process_date="2024-01-08",
                predecessor=("new-id", "HOLD", "new-cusip", "Continuing security"),
                successor=("third-id", "OTHER", "third-cusip", "Other security"),
            )

        cases = {
            "incomplete action query": lambda item: item["query"].update(
                corporate_actions_data_quality="partial"
            ),
            "query misses boundary": lambda item: item["query"].update(corporate_actions_end="2024-01-04"),
            "missing old lookup": missing_old_lookup,
            "duplicate old lookup": duplicate_old_lookup,
            "mismatched old UUID": mismatched_old_uuid,
            "duplicate current lookup": duplicate_symbol_lookup,
            "current lookup is predecessor": current_lookup_is_predecessor,
            "predecessor still active": predecessor_still_active,
            "successor inactive": successor_is_inactive,
            "exchange changes": exchange_changes,
            "third occupant": third_occupant,
            "third input occupant": third_input_occupant,
            "competing rename": competing_rename,
            "conflicting action dates": lambda item: item["corporate_actions"][0].update(
                effective_date="2024-01-08"
            ),
        }
        for label, mutate in cases.items():
            with self.subTest(label=label):
                evidence = exact_same_symbol_handoff()
                mutate(evidence)
                report = run(evidence, {"HOLD": bars(self.days)})["HOLD"]
                self.assertEqual([], report["uuid_lineage_edges"])
                self.assertFalse(
                    any(
                        segment["security_identity"]["member_alpaca_asset_ids"] == ["new-id", "old-id"]
                        for segment in raw_segments(report)
                    )
                )
                self.assertTrue(raw_segments(report, "uncorrelated"))

    def test_frozen_v59_replay_does_not_acquire_the_new_certificate(self):
        evidence = exact_same_symbol_handoff()
        inputs = inputs_for({"HOLD": bars(self.days)})
        current = correlate(Path("."), inputs, {"HOLD": evidence}, policy=PRODUCTION_POLICY)["HOLD"]
        frozen = correlate(Path("."), inputs, {"HOLD": evidence}, policy=load_policy(V59_POLICY))["HOLD"]

        self.assertEqual(1, len(current["uuid_lineage_edges"]))
        self.assertEqual([], frozen["uuid_lineage_edges"])
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        self.assertTrue(raw_segments(frozen, "uncorrelated"))

    def test_frozen_v61_replay_keeps_the_bounded_entry_form_disabled(self):
        v61 = replace(PRODUCTION_POLICY, implementation_correlation_version=61)
        direct = correlate(
            Path("."),
            inputs_for({"HOLD": bars(self.days)}),
            {"HOLD": exact_same_symbol_handoff()},
            policy=v61,
        )["HOLD"]
        bounded_evidence = bounded_entry_same_symbol_handoff()
        bounded_inputs = inputs_for({"HOLD": bars(self.days)})
        frozen = correlate(Path("."), bounded_inputs, {"HOLD": bounded_evidence}, policy=v61)["HOLD"]
        current = correlate(Path("."), bounded_inputs, {"HOLD": bounded_evidence}, policy=PRODUCTION_POLICY)[
            "HOLD"
        ]

        self.assertIn("transition", {item["corporate_action_id"] for item in direct["uuid_lineage_edges"]})
        self.assertNotIn("transition", {item["corporate_action_id"] for item in frozen["uuid_lineage_edges"]})
        self.assertIn("transition", {item["corporate_action_id"] for item in current["uuid_lineage_edges"]})

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
            policy=PRODUCTION_POLICY,
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

    def test_successor_prefix_consumes_exact_completed_predecessor_assignments(self):
        evidence, histories, boundary, prefix_rows = successor_prefix_handoff_case()
        inputs = inputs_for(
            histories,
            asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
        )
        current = correlate(
            Path("."),
            inputs,
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=72),
        )
        frozen = correlate(
            Path("."),
            inputs_for(
                histories,
                asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
            ),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=71),
        )

        self.assertEqual(0, current["NEW"]["uncorrelated"]["row_count"])
        mapped = [
            segment
            for segment in raw_segments(current["NEW"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual(1, len(mapped))
        self.assertEqual(prefix_rows, mapped[0]["row_count"])
        self.assertEqual("OLD", mapped[0]["historical_symbol"])
        self.assertEqual("old-id", mapped[0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual("old-cusip", mapped[0]["canonical_security"]["cusip"])
        self.assertEqual("OLD", mapped[0]["ticker_ownership_interval"]["historical_symbol"])
        self.assertEqual(boundary, mapped[0]["ticker_ownership_interval"]["end_date_exclusive"])
        self.assertIn("mapped_predecessor_duplicate", mapped[0]["warnings"])
        self.assertEqual(
            raw_segments(frozen["OLD"]),
            raw_segments(current["OLD"]),
        )
        self.assertEqual(prefix_rows * len(ADJUSTMENTS), frozen["NEW"]["uncorrelated"]["row_count"])
        self.assertFalse(
            any(
                segment.get("correlation_method") == "accepted_name_change_mapped_continuation"
                for segment in raw_segments(frozen["NEW"])
            )
        )

        selected, corrections = select_observations(inputs, current)
        self.assertEqual({}, corrections)
        selected_mapped = [
            segment
            for segment in raw_segments(selected["NEW"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual([["OLD"]], [segment["observation_tickers"] for segment in selected_mapped])

    def test_active_successor_prefix_consumes_only_the_dated_terminal_donor_suffix(self):
        evidence, histories, boundary, prefix_rows = active_successor_prefix_reused_ticker_case()
        current_inputs = active_successor_prefix_inputs(histories)
        current = correlate(
            Path("."),
            current_inputs,
            {symbol: evidence for symbol in histories},
            policy=PRODUCTION_POLICY,
        )
        frozen = correlate(
            Path("."),
            active_successor_prefix_inputs(histories),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=74),
        )

        self.assertEqual(0, current["NEW"]["uncorrelated"]["row_count"])
        mapped = [
            segment
            for segment in raw_segments(current["NEW"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual(1, len(mapped))
        self.assertEqual(prefix_rows, mapped[0]["row_count"])
        self.assertEqual("OLD", mapped[0]["historical_symbol"])
        self.assertEqual("old-id", mapped[0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual("old-cusip", mapped[0]["canonical_security"]["cusip"])
        self.assertEqual(histories["NEW"][0]["t"][:10], mapped[0]["first_session_date"])
        self.assertEqual(
            histories["NEW"][0]["t"][:10],
            mapped[0]["ticker_ownership_interval"]["start_date_inclusive"],
        )
        self.assertEqual(boundary, mapped[0]["ticker_ownership_interval"]["end_date_exclusive"])
        self.assertIn(
            "transition",
            mapped[0]["ticker_ownership_interval"]["supporting_corporate_action_ids"],
        )
        self.assertEqual(
            prefix_rows * len(ADJUSTMENTS),
            frozen["NEW"]["uncorrelated"]["row_count"],
        )
        for branch in ("correlated", "uncorrelated"):
            self.assertEqual(frozen["OLD"][branch], current["OLD"][branch])
        self.assertEqual(frozen["OLD"]["uuid_lineage_edges"], current["OLD"]["uuid_lineage_edges"])
        self.assertEqual(
            frozen["OLD"]["ticker_ownership_intervals"],
            current["OLD"]["ticker_ownership_intervals"],
        )
        old_unresolved = raw_segments(current["OLD"], "uncorrelated")
        self.assertEqual(1, old_unresolved[0]["row_count"])
        self.assertEqual(
            ["prior_pricing_run_before_ticker_reuse_gap"],
            old_unresolved[0]["reason_codes"],
        )

        selected, corrections = select_observations(current_inputs, copy.deepcopy(current))
        self.assertEqual({}, corrections)
        selected_mapped = [
            segment
            for segment in raw_segments(selected["NEW"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual([["OLD"]], [segment["observation_tickers"] for segment in selected_mapped])
        self.assertEqual(["selected"], [segment["observation_selection"] for segment in selected_mapped])
        selected_donor = [
            segment
            for segment in raw_segments(selected["OLD"])
            if segment.get("correlation_method") == "dated_terminal_identity_suffix_after_ticker_reuse_gap"
        ]
        self.assertEqual(["superseded"], [segment["observation_selection"] for segment in selected_donor])

    def test_active_successor_prefix_retains_its_material_vetoes(self):
        cases = []

        evidence, histories, _, _ = active_successor_prefix_reused_ticker_case()
        changed = copy.deepcopy(histories["NEW"])
        changed[2]["c"] = "999"
        cases.append(
            (
                "one adjustment differs",
                evidence,
                histories,
                active_successor_prefix_inputs(
                    histories,
                    view_histories={"NEW": {"dividend": changed}},
                ),
            )
        )

        evidence, histories, _, _ = active_successor_prefix_reused_ticker_case()
        del histories["OLD"][-3]
        cases.append(("incomplete donor suffix", evidence, histories, active_successor_prefix_inputs(histories)))

        evidence, histories, _, _ = active_successor_prefix_reused_ticker_case()
        evidence["query"]["corporate_actions_data_quality"] = "partial"
        cases.append(("incomplete action query", evidence, histories, active_successor_prefix_inputs(histories)))

        evidence, histories, _, prefix_rows = active_successor_prefix_reused_ticker_case()
        for symbol in ("OLD", "NEW"):
            selected = histories[symbol][-prefix_rows:] if symbol == "OLD" else histories[symbol][:prefix_rows]
            for row in selected:
                row.update(dict.fromkeys(("o", "h", "l", "c", "vw"), "40"))
        cases.append(
            ("uninformative traded prefix", evidence, histories, active_successor_prefix_inputs(histories))
        )

        evidence, histories, _, _ = active_successor_prefix_reused_ticker_case()
        incomplete = active_successor_prefix_inputs(histories)
        incomplete["NEW"].local["downloads"][0]["rows"] -= 1
        cases.append(("incomplete successor source", evidence, histories, incomplete))

        for label, evidence, histories, inputs in cases:
            with self.subTest(label=label):
                reports = correlate(
                    Path("."),
                    inputs,
                    {symbol: evidence for symbol in histories},
                    policy=PRODUCTION_POLICY,
                )
                mapped = [
                    segment
                    for segment in raw_segments(reports["NEW"])
                    if segment.get("correlation_method") == "accepted_name_change_mapped_continuation"
                ]
                self.assertEqual([], mapped)
                self.assertGreater(reports["NEW"]["uncorrelated"]["row_count"], 0)

    def test_successor_prefix_handoff_retains_exact_boundary_vetoes(self):
        policy = replace(PRODUCTION_POLICY, implementation_correlation_version=72)

        def mapped_segments(evidence, histories, inputs):
            reports = correlate(
                Path("."),
                inputs,
                {symbol: evidence for symbol in histories},
                policy=policy,
            )
            return [
                segment
                for segment in raw_segments(reports["NEW"])
                if segment.get("correlation_method") == "accepted_name_change_mapped_continuation"
            ], reports["NEW"]["uncorrelated"]["row_count"]

        cases = []

        evidence, histories, _, _ = successor_prefix_handoff_case()
        changed = copy.deepcopy(histories["NEW"])
        changed[10]["c"] = "99"
        cases.append(
            (
                "one adjustment differs",
                evidence,
                histories,
                inputs_for(
                    histories,
                    asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
                    view_histories={"NEW": {"split": changed}},
                ),
            )
        )

        evidence, histories, _, prefix_rows = successor_prefix_handoff_case()
        del histories["NEW"][prefix_rows]
        cases.append(
            (
                "successor boundary is not adjacent",
                evidence,
                histories,
                inputs_for(
                    histories,
                    asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
                ),
            )
        )

        evidence, histories, _, _ = successor_prefix_handoff_case()
        incomplete = inputs_for(
            histories,
            asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
        )
        incomplete["NEW"].local["downloads"][0]["rows"] -= 1
        cases.append(("incomplete successor source", evidence, histories, incomplete))

        evidence, histories, _, _ = successor_prefix_handoff_case()
        other_feed = inputs_for(
            histories,
            asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
        )
        for download in other_feed["OLD"].downloads.values():
            download.manifest["feed"] = "iex"
        cases.append(("different donor feed", evidence, histories, other_feed))

        evidence, histories, _, _ = successor_prefix_handoff_case(same_uuid=False)
        cases.append(
            (
                "different UUID",
                evidence,
                histories,
                inputs_for(
                    histories,
                    asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
                ),
            )
        )

        evidence, histories, boundary, prefix_rows = successor_prefix_handoff_case()
        add_name_change_edge(
            evidence,
            action_id="competing-transition",
            process_date=boundary,
            predecessor=("old-id", "OLD", "old-cusip", "Original security"),
            successor=("old-id", "ALT", "old-cusip", "Alternate successor label"),
        )
        histories["ALT"] = copy.deepcopy(histories["NEW"][prefix_rows:])
        cases.append(
            (
                "competing successor branch",
                evidence,
                histories,
                inputs_for(
                    histories,
                    asofs={
                        "OLD": "2024-02-01",
                        "NEW": "2024-09-01",
                        "ALT": "2024-09-01",
                    },
                ),
            )
        )

        for label, evidence, histories, inputs in cases:
            with self.subTest(label=label):
                mapped, unresolved = mapped_segments(evidence, histories, inputs)
                self.assertEqual([], mapped)
                self.assertGreater(unresolved, 0)

    def test_successor_prefix_accepts_only_a_complete_later_same_label_split_chain(self):
        evidence, histories, _, prefix_rows = successor_prefix_later_split_case()
        split_adjusted = copy.deepcopy(histories["NEW"])
        for row in split_adjusted[:prefix_rows]:
            row.update(dict.fromkeys(("o", "h", "l", "c", "vw"), "1.25"))
        view_histories = {
            "NEW": {
                adjustment: split_adjusted
                for adjustment in ADJUSTMENTS
                if adjustment == "all" or "split" in adjustment.split(",")
            }
        }
        inputs = inputs_for(
            histories,
            asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
            view_histories=view_histories,
        )
        current = correlate(
            Path("."),
            inputs,
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=73),
        )
        frozen = correlate(
            Path("."),
            inputs_for(
                histories,
                asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
                view_histories=view_histories,
            ),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=72),
        )

        mapped = [
            segment
            for segment in raw_segments(current["NEW"])
            if segment["correlation_method"] == "accepted_name_change_mapped_continuation"
        ]
        self.assertEqual([prefix_rows], [segment["row_count"] for segment in mapped])
        self.assertEqual(0, current["NEW"]["uncorrelated"]["row_count"])
        self.assertEqual(prefix_rows * len(ADJUSTMENTS), frozen["NEW"]["uncorrelated"]["row_count"])

        unrelated = copy.deepcopy(evidence)
        unrelated["identities"].append(
            {
                "id": "unrelated-successor-id",
                "class": "us_equity",
                "exchange": "NASDAQ",
                "symbol": "NEW",
                "name": "Unrelated successor-label occupant",
                "status": "inactive",
                "tradable": False,
                "lookup_symbols": [],
                "lookup_cusips": [],
            }
        )
        rejected = correlate(
            Path("."),
            inputs_for(
                histories,
                asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
                view_histories=view_histories,
            ),
            {symbol: unrelated for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=73),
        )
        self.assertFalse(
            any(
                segment.get("correlation_method") == "accepted_name_change_mapped_continuation"
                for segment in raw_segments(rejected["NEW"])
            )
        )
        self.assertEqual(
            prefix_rows * len(ADJUSTMENTS),
            rejected["NEW"]["uncorrelated"]["row_count"],
        )

        changed_raw = copy.deepcopy(histories["NEW"])
        changed_raw[10]["c"] = "99"
        mismatched = correlate(
            Path("."),
            inputs_for(
                histories,
                asofs={"OLD": "2024-02-01", "NEW": "2024-09-01"},
                view_histories={
                    "NEW": {
                        **view_histories["NEW"],
                        "raw": changed_raw,
                    }
                },
            ),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=73),
        )
        self.assertEqual(
            prefix_rows * len(ADJUSTMENTS),
            mismatched["NEW"]["uncorrelated"]["row_count"],
        )

        before_split = correlate(
            Path("."),
            inputs_for(
                histories,
                asofs={
                    "OLD": "2024-02-01",
                    "NEW": histories["NEW"][prefix_rows + 1]["t"][:10],
                },
                view_histories=view_histories,
            ),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=73),
        )
        self.assertEqual(
            prefix_rows * len(ADJUSTMENTS),
            before_split["NEW"]["uncorrelated"]["row_count"],
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

    def test_shared_uuid_alias_accepts_only_its_observed_later_starting_suffix(self):
        days = ("2023-12-29", *self.days)
        old, new = bars(days[:4]), bars(self.days)
        new[:3] = copy.deepcopy(old[1:])
        evidence = rename(same_uuid=True)
        histories = {"OLD": old, "NEW": new}
        current_inputs = inputs_for(histories)
        current_reports = correlate(
            Path("."),
            current_inputs,
            {symbol: evidence for symbol in current_inputs},
            policy=PRODUCTION_POLICY,
        )

        current = current_reports["NEW"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        segments = raw_segments(current)
        self.assertEqual(["OLD", "NEW"], [item["historical_symbol"] for item in segments])
        self.assertEqual([3, 2], [item["row_count"] for item in segments])
        self.assertEqual("2024-01-02", segments[0]["first_session_date"])

        v62 = replace(PRODUCTION_POLICY, implementation_correlation_version=62)
        frozen_inputs = inputs_for(histories)
        frozen_reports = correlate(
            Path("."),
            frozen_inputs,
            {symbol: evidence for symbol in frozen_inputs},
            policy=v62,
        )
        frozen = frozen_reports["NEW"]
        self.assertEqual([3], [item["row_count"] for item in raw_segments(frozen, "uncorrelated")])
        for branch in ("correlated", "uncorrelated"):
            self.assertEqual(frozen_reports["OLD"][branch], current_reports["OLD"][branch])
        _, frozen_corrections = select_observations(frozen_inputs, copy.deepcopy(frozen_reports))
        _, current_corrections = select_observations(current_inputs, copy.deepcopy(current_reports))
        self.assertEqual(frozen_corrections, current_corrections)

    def test_partial_successor_suffix_does_not_join_distinct_uuids_or_a_gapped_overlap(self):
        days = ("2023-12-29", *self.days)
        old, new = bars(days[:4]), bars(self.days)
        new[:3] = copy.deepcopy(old[1:])

        distinct = run(rename(), {"OLD": old, "NEW": new})["NEW"]
        self.assertEqual([], distinct["uuid_lineage_edges"])
        self.assertEqual([3], [item["row_count"] for item in raw_segments(distinct, "uncorrelated")])

        gapped = [new[0], new[2], *new[3:]]
        incomplete = run(rename(same_uuid=True), {"OLD": old, "NEW": gapped})["NEW"]
        self.assertEqual(
            2,
            sum(item["row_count"] for item in raw_segments(incomplete, "uncorrelated")),
        )

    def test_exact_post_padding_alias_suffix_uses_only_observed_predecessor_history(self):
        evidence, histories, boundary = post_padding_alias_case()
        asofs = dict.fromkeys(histories, "2024-09-01")
        current_inputs = inputs_for(histories, asofs=asofs)
        current_reports = correlate(
            Path("."),
            current_inputs,
            {symbol: evidence for symbol in current_inputs},
            policy=PRODUCTION_POLICY,
        )

        current = current_reports["NEW"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        self.assertEqual(
            [("OLD", 3), ("NEW", 4)],
            [(item["historical_symbol"], item["row_count"]) for item in raw_segments(current)],
        )
        self.assertEqual(boundary, raw_segments(current)[0]["ticker_ownership_interval"]["end_date_exclusive"])

        v66 = replace(PRODUCTION_POLICY, implementation_correlation_version=66)
        frozen_inputs = inputs_for(histories, asofs=asofs)
        frozen_reports = correlate(
            Path("."),
            frozen_inputs,
            {symbol: evidence for symbol in frozen_inputs},
            policy=v66,
        )
        self.assertEqual(
            [3], [item["row_count"] for item in raw_segments(frozen_reports["NEW"], "uncorrelated")]
        )
        for branch in ("correlated", "uncorrelated"):
            self.assertEqual(frozen_reports["OLD"][branch], current_reports["OLD"][branch])

        selected, corrections = select_observations(current_inputs, copy.deepcopy(current_reports))
        self.assertEqual({}, corrections)
        self.assertEqual(
            {"superseded"},
            {item["observation_selection"] for item in raw_segments(selected["OLD"])[-1:]},
        )
        self.assertEqual(
            {"selected"},
            {item["observation_selection"] for item in raw_segments(selected["NEW"])},
        )

    def test_post_padding_alias_suffix_retains_identity_and_boundary_vetoes(self):
        def case(**kwargs):
            return post_padding_alias_case(**kwargs)

        incomplete_query, incomplete_query_histories, _ = case()
        incomplete_query["query"]["corporate_actions_data_quality"] = "partial"

        distinct_uuid, distinct_uuid_histories, _ = case(same_uuid=False)
        different_cusip, different_cusip_histories, _ = case(same_cusip=False)

        inactive_current, inactive_current_histories, _ = case()
        inactive_current["identities"][0].update(status="inactive", tradable=False)

        competing_owner, competing_owner_histories, _ = case()
        competing_owner["identities"].append({"id": "other-id", "symbol": "NEW", "class": "us_equity"})

        divergent_overlap, divergent_overlap_histories, _ = case()
        divergent_overlap_histories["NEW"][0]["v"] = "101"

        untraded_overlap_edge, untraded_overlap_edge_histories, _ = case()
        untraded_overlap_edge_histories["OLD"][-3].update(v="0", n="0", vw="0")
        untraded_overlap_edge_histories["NEW"][0].update(v="0", n="0", vw="0")

        detached_padding, detached_padding_histories, _ = case()
        detached_padding_histories["OLD"][-4].update(v="100", n="10", vw="12")

        gapped_overlap, gapped_overlap_histories, _ = case()
        del gapped_overlap_histories["NEW"][1]

        extended_predecessor, extended_predecessor_histories, _ = case()
        extended_predecessor_histories["OLD"].append(copy.deepcopy(extended_predecessor_histories["NEW"][3]))

        uninformative_overlap, uninformative_overlap_histories, _ = case()
        for row in uninformative_overlap_histories["OLD"][-3:]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "40"))
        for row in uninformative_overlap_histories["NEW"][:3]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "40"))

        competing_action, competing_action_histories, _ = case()
        duplicate = copy.deepcopy(competing_action["corporate_actions"][0])
        duplicate["id"] = "competing-transition"
        competing_action["corporate_actions"].append(duplicate)

        incomplete_view, incomplete_view_histories, _ = case()
        incomplete_dividend = copy.deepcopy(incomplete_view_histories["OLD"][:-1])

        cases = {
            "incomplete action query": (incomplete_query, incomplete_query_histories, {}),
            "distinct UUID": (distinct_uuid, distinct_uuid_histories, {}),
            "different CUSIP": (different_cusip, different_cusip_histories, {}),
            "inactive current owner": (inactive_current, inactive_current_histories, {}),
            "competing successor owner": (competing_owner, competing_owner_histories, {}),
            "divergent raw overlap": (divergent_overlap, divergent_overlap_histories, {}),
            "untraded overlap edge": (
                untraded_overlap_edge,
                untraded_overlap_edge_histories,
                {},
            ),
            "padding does not end at overlap": (detached_padding, detached_padding_histories, {}),
            "gapped overlap": (gapped_overlap, gapped_overlap_histories, {}),
            "predecessor reaches boundary": (
                extended_predecessor,
                extended_predecessor_histories,
                {},
            ),
            "uninformative overlap": (
                uninformative_overlap,
                uninformative_overlap_histories,
                {},
            ),
            "competing transition": (competing_action, competing_action_histories, {}),
            "incomplete adjustment view": (
                incomplete_view,
                incomplete_view_histories,
                {"view_histories": {"OLD": {"dividend": incomplete_dividend}}},
            ),
        }
        for label, (evidence, histories, options) in cases.items():
            with self.subTest(label=label):
                options = {**options, "asofs": dict.fromkeys(histories, "2024-09-01")}
                report = run(evidence, histories, **options)["NEW"]
                self.assertTrue(raw_segments(report, "uncorrelated"))
                self.assertNotIn(
                    "OLD",
                    {item["historical_symbol"] for item in raw_segments(report)},
                )

    def test_post_padding_alias_uses_raw_transactions_not_adjustment_vintages(self):
        evidence, histories, _ = post_padding_alias_case()
        changed = copy.deepcopy(histories["NEW"])
        changed[0].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "99"))
        report = run(
            evidence,
            histories,
            asofs=dict.fromkeys(histories, "2024-09-01"),
            view_histories={"NEW": {"dividend": changed}},
        )["NEW"]
        self.assertEqual([], raw_segments(report, "uncorrelated"))

    def test_split_adjusted_padding_and_later_acquisition_do_not_erase_bounded_alias(self):
        evidence, histories, boundary, view_histories = split_adjusted_post_padding_alias_case()
        asofs = dict.fromkeys(histories, "2024-09-01")
        current_inputs = inputs_for(histories, asofs=asofs, view_histories=view_histories)
        current_reports = correlate(
            Path("."),
            current_inputs,
            {symbol: evidence for symbol in current_inputs},
            policy=PRODUCTION_POLICY,
        )

        current = current_reports["NEW"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        self.assertEqual(
            [("OLD", 3), ("NEW", 4)],
            [(item["historical_symbol"], item["row_count"]) for item in raw_segments(current)],
        )
        self.assertEqual(boundary, raw_segments(current)[0]["ticker_ownership_interval"]["end_date_exclusive"])
        self.assertEqual(
            {"old-id", "pre-split-id"},
            {
                asset_id
                for item in raw_segments(current)
                for asset_id in item["security_identity"]["member_alpaca_asset_ids"]
            },
        )
        self.assertNotIn(
            "later-successor-acquisition",
            {item["corporate_action_id"] for item in current["uuid_lineage_edges"]},
        )

        selected, corrections = select_observations(current_inputs, copy.deepcopy(current_reports))
        self.assertEqual({}, corrections)
        self.assertEqual(
            {"selected"},
            {item["observation_selection"] for item in raw_segments(selected["NEW"])},
        )

        v67 = replace(PRODUCTION_POLICY, implementation_correlation_version=67)
        frozen_inputs = inputs_for(histories, asofs=asofs, view_histories=view_histories)
        frozen = correlate(
            Path("."),
            frozen_inputs,
            {symbol: evidence for symbol in frozen_inputs},
            policy=v67,
        )["NEW"]
        self.assertEqual([3], [item["row_count"] for item in raw_segments(frozen, "uncorrelated")])

    def test_split_adjusted_post_padding_alias_retains_temporal_and_split_vetoes(self):
        def case():
            return copy.deepcopy(split_adjusted_post_padding_alias_case())

        unsupported_split, unsupported_histories, _, unsupported_views = case()
        split = next(
            action
            for action in unsupported_split["corporate_actions"]
            if action["id"] == "predecessor-reverse-split"
        )
        unsupported_date = unsupported_histories["OLD"][79]["t"][:10]
        for field in ("ex_date", "effective_date", "process_date", "payable_date"):
            split[field] = unsupported_date

        boundary_action, boundary_histories, boundary, boundary_views = case()
        acquisition = next(
            action
            for action in boundary_action["corporate_actions"]
            if action["id"] == "later-successor-acquisition"
        )
        for field in ("effective_date", "process_date", "payable_date"):
            acquisition[field] = boundary

        wrong_cusip, wrong_cusip_histories, _, wrong_cusip_views = case()
        next(
            action
            for action in wrong_cusip["corporate_actions"]
            if action["id"] == "later-successor-acquisition"
        )["acquirer_cusip"] = "different-cusip"

        missing_process, missing_process_histories, _, missing_process_views = case()
        next(
            action
            for action in missing_process["corporate_actions"]
            if action["id"] == "later-successor-acquisition"
        ).pop("process_date")

        predecessor_acquiree, predecessor_histories, _, predecessor_views = case()
        next(
            action
            for action in predecessor_acquiree["corporate_actions"]
            if action["id"] == "later-successor-acquisition"
        ).update(acquiree_symbol="OLD", acquiree_cusip="old-cusip")

        reused_cusip, reused_cusip_histories, _, reused_cusip_views = case()
        next(
            action
            for action in reused_cusip["corporate_actions"]
            if action["id"] == "later-successor-acquisition"
        )["acquiree_cusip"] = "old-cusip"

        cases = {
            "adjusted value change lacks exact split date": (
                unsupported_split,
                unsupported_histories,
                unsupported_views,
            ),
            "acquisition is not strictly later than rename": (
                boundary_action,
                boundary_histories,
                boundary_views,
            ),
            "continuing acquirer CUSIP differs": (
                wrong_cusip,
                wrong_cusip_histories,
                wrong_cusip_views,
            ),
            "acquisition process date is missing": (
                missing_process,
                missing_process_histories,
                missing_process_views,
            ),
            "acquisition reuses predecessor label": (
                predecessor_acquiree,
                predecessor_histories,
                predecessor_views,
            ),
            "distinct acquiree label reuses continuing CUSIP": (
                reused_cusip,
                reused_cusip_histories,
                reused_cusip_views,
            ),
        }
        for label, (evidence, histories, view_histories) in cases.items():
            with self.subTest(label=label):
                report = run(
                    evidence,
                    histories,
                    asofs=dict.fromkeys(histories, "2024-09-01"),
                    view_histories=view_histories,
                )["NEW"]
                self.assertEqual(
                    [3],
                    [item["row_count"] for item in raw_segments(report, "uncorrelated")],
                )
                self.assertNotIn(
                    "OLD",
                    {item["historical_symbol"] for item in raw_segments(report)},
                )

    def test_single_session_shared_uuid_same_cusip_history_is_bounded(self):
        evidence = rename(same_uuid=True, same_cusip=True, when="2024-01-03")
        histories = {"OLD": bars(self.days[:1]), "NEW": bars(self.days)}
        current_inputs = inputs_for(histories)
        current_reports = correlate(
            Path("."),
            current_inputs,
            {symbol: evidence for symbol in current_inputs},
            policy=PRODUCTION_POLICY,
        )

        current = current_reports["NEW"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        self.assertEqual(
            [("OLD", 1), ("NEW", 4)],
            [(item["historical_symbol"], item["row_count"]) for item in raw_segments(current)],
        )

        v63 = replace(PRODUCTION_POLICY, implementation_correlation_version=63)
        frozen_inputs = inputs_for(histories)
        frozen_reports = correlate(
            Path("."),
            frozen_inputs,
            {symbol: evidence for symbol in frozen_inputs},
            policy=v63,
        )
        self.assertEqual(
            [1], [item["row_count"] for item in raw_segments(frozen_reports["NEW"], "uncorrelated")]
        )
        for branch in ("correlated", "uncorrelated"):
            self.assertEqual(frozen_reports["OLD"][branch], current_reports["OLD"][branch])
        selected, current_corrections = select_observations(current_inputs, copy.deepcopy(current_reports))
        self.assertEqual({}, current_corrections)
        self.assertEqual(
            {"selected"},
            {item["observation_selection"] for item in raw_segments(selected["NEW"])},
        )
        self.assertEqual(
            {"superseded"},
            {item["observation_selection"] for item in raw_segments(selected["OLD"])},
        )

    def test_single_session_certificate_retains_identity_and_boundary_vetoes(self):
        cases = {}
        cases["different_cusip"] = (
            rename(same_uuid=True, when="2024-01-03"),
            bars(self.days[:1]),
            bars(self.days),
        )
        cases["distinct_uuid"] = (
            rename(same_cusip=True, when="2024-01-03"),
            bars(self.days[:1]),
            bars(self.days),
        )
        cases["nonadjacent_boundary"] = (
            rename(same_uuid=True, same_cusip=True),
            bars(self.days[:1]),
            bars(self.days),
        )
        cases["additional_predecessor_source_session"] = (
            rename(same_uuid=True, same_cusip=True, when="2024-01-03"),
            bars(self.days[:2]),
            bars(self.days),
        )
        competing = rename(same_uuid=True, same_cusip=True, when="2024-01-03")
        add_name_change_edge(
            competing,
            action_id="competing-entry",
            process_date="2023-12-29",
            predecessor=("other-id", "OTHER", "other-cusip", "Competing security"),
            successor=("old-id", "OLD", "old-cusip", "Original security"),
        )
        cases["related_structural_edge"] = (competing, bars(self.days[:1]), bars(self.days))
        mismatched = bars(self.days)
        mismatched[0]["v"] = "101"
        cases["different_raw_transaction"] = (
            rename(same_uuid=True, same_cusip=True, when="2024-01-03"),
            bars(self.days[:1]),
            mismatched,
        )
        inert = bars(self.days[:1])
        inert[0].update(v="0", n="0")
        inert_successor = bars(self.days)
        inert_successor[0].update(v="0", n="0")
        cases["no_trading"] = (
            rename(same_uuid=True, same_cusip=True, when="2024-01-03"),
            inert,
            inert_successor,
        )

        for name, (evidence, predecessor, successor) in cases.items():
            with self.subTest(name=name):
                report = run(evidence, {"OLD": predecessor, "NEW": successor})["NEW"]
                unresolved = raw_segments(report, "uncorrelated")
                self.assertTrue(unresolved)
                self.assertEqual("2024-01-02", unresolved[0]["first_session_date"])

    def test_single_session_raw_ownership_ignores_later_adjustment_vintages(self):
        evidence = rename(same_uuid=True, same_cusip=True, when="2024-01-03")
        changed_dividend = bars(self.days)
        changed_dividend[0].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "9.5"))
        report = run(
            evidence,
            {"OLD": bars(self.days[:1]), "NEW": bars(self.days)},
            view_histories={"NEW": {"dividend": changed_dividend}},
        )["NEW"]
        self.assertEqual([], raw_segments(report, "uncorrelated"))

    def test_complete_current_source_bridge_dates_only_the_observed_early_successor(self):
        days = (
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
        )
        complete = bars(days)
        histories = {
            "OLD": complete[:3],
            "NEW": complete[3:],
            "CURRENT": complete,
        }
        evidence = early_successor_bridge_evidence()
        current_inputs = inputs_for(histories)
        current_reports = correlate(
            Path("."),
            current_inputs,
            {symbol: evidence for symbol in current_inputs},
            policy=PRODUCTION_POLICY,
        )

        current = current_reports["NEW"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        self.assertEqual(
            [("2024-01-05", "2024-01-08", 2), ("2024-01-09", "2024-01-10", 2)],
            [
                (item["first_session_date"], item["last_session_date"], item["row_count"])
                for item in raw_segments(current)
            ],
        )
        self.assertEqual(
            ["NEW", "NEW"],
            [item["historical_symbol"] for item in raw_segments(current)],
        )
        self.assertEqual(
            "2024-01-05",
            raw_segments(current)[0]["ticker_ownership_interval"]["start_date_inclusive"],
        )

        v64 = replace(PRODUCTION_POLICY, implementation_correlation_version=64)
        frozen_inputs = inputs_for(histories)
        frozen_reports = correlate(
            Path("."),
            frozen_inputs,
            {symbol: evidence for symbol in frozen_inputs},
            policy=v64,
        )
        self.assertEqual(
            [("2024-01-05", "2024-01-08", 2)],
            [
                (item["first_session_date"], item["last_session_date"], item["row_count"])
                for item in raw_segments(frozen_reports["NEW"], "uncorrelated")
            ],
        )
        for symbol in ("OLD", "CURRENT"):
            for branch in ("correlated", "uncorrelated"):
                self.assertEqual(
                    frozen_reports[symbol][branch],
                    current_reports[symbol][branch],
                )
        selected, corrections = select_observations(current_inputs, copy.deepcopy(current_reports))
        self.assertEqual({}, corrections)
        self.assertEqual(
            {"selected"},
            {item["observation_selection"] for item in raw_segments(selected["NEW"])},
        )

    def test_early_successor_source_bridge_retains_every_material_veto(self):
        days = (
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
        )

        def case(evidence=None, complete=None):
            complete = bars(days) if complete is None else complete
            return (
                early_successor_bridge_evidence() if evidence is None else evidence,
                {"OLD": complete[:3], "NEW": complete[3:], "CURRENT": complete},
            )

        incomplete_query, ordinary = case()
        incomplete_query["query"]["corporate_actions_data_quality"] = "partial"

        query_misses_boundary, query_miss_histories = case()
        query_misses_boundary["query"]["corporate_actions_end"] = "2024-01-08"

        competing, competing_histories = case()
        add_name_change_edge(
            competing,
            action_id="competing-transition",
            process_date="2024-01-08",
            predecessor=("old-id", "OLD", "old-cusip", "Original security"),
            successor=("old-id", "OTHER", "old-cusip", "Other label"),
        )

        ambiguous_owner, ambiguous_histories = case()
        ambiguous_owner["identities"].append({"id": "other-id", "symbol": "CURRENT", "class": "us_equity"})

        mismatched_bridge, mismatched_histories = case()
        mismatched_histories["CURRENT"] = copy.deepcopy(mismatched_histories["CURRENT"])
        mismatched_histories["CURRENT"][3]["v"] = "101"

        uninformative = bars(days)
        for index in (3, 4):
            uninformative[index].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "10"))
        no_trading_information, no_information_histories = case(complete=uninformative)

        uninformative_predecessor = bars(days)
        for index in (0, 1, 2):
            uninformative_predecessor[index].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "10"))
        no_predecessor_information, no_predecessor_information_histories = case(
            complete=uninformative_predecessor
        )

        gapped = bars(days)
        gapped_histories = {
            "OLD": gapped[:2],
            "NEW": gapped[3:],
            "CURRENT": [*gapped[:2], *gapped[3:]],
        }

        no_bridge_evidence, no_bridge_histories = case()
        del no_bridge_histories["CURRENT"]

        missing_boundary_evidence, missing_boundary_histories = case()
        missing_boundary_histories["NEW"] = [
            *missing_boundary_histories["NEW"][:2],
            *missing_boundary_histories["NEW"][3:],
        ]
        missing_boundary_histories["CURRENT"] = [
            *missing_boundary_histories["OLD"],
            *missing_boundary_histories["NEW"],
        ]

        distinct_uuid, distinct_uuid_histories = case(evidence=early_successor_bridge_evidence(same_uuid=False))
        different_cusip, different_cusip_histories = case(
            evidence=early_successor_bridge_evidence(same_cusip=False)
        )

        cases = {
            "incomplete action query": (incomplete_query, ordinary),
            "action query misses boundary": (query_misses_boundary, query_miss_histories),
            "related structural edge": (competing, competing_histories),
            "ambiguous current owner": (ambiguous_owner, ambiguous_histories),
            "different raw bridge": (mismatched_bridge, mismatched_histories),
            "uninformative early span": (no_trading_information, no_information_histories),
            "uninformative predecessor": (
                no_predecessor_information,
                no_predecessor_information_histories,
            ),
            "gapped source handoff": (early_successor_bridge_evidence(), gapped_histories),
            "missing current source": (no_bridge_evidence, no_bridge_histories),
            "missing action-boundary session": (
                missing_boundary_evidence,
                missing_boundary_histories,
            ),
            "distinct UUID": (distinct_uuid, distinct_uuid_histories),
            "different CUSIP": (different_cusip, different_cusip_histories),
        }
        for label, (evidence, histories) in cases.items():
            with self.subTest(label=label):
                report = run(evidence, histories)["NEW"]
                self.assertGreaterEqual(
                    sum(item["row_count"] for item in raw_segments(report, "uncorrelated")),
                    2,
                )

    def test_early_successor_bridge_uses_raw_transactions_not_adjustment_vintages(self):
        days = (
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10",
        )
        complete = bars(days)
        changed = copy.deepcopy(complete)
        changed[3].update(dict.fromkeys(("o", "h", "l", "c", "vw"), "9.5"))
        report = run(
            early_successor_bridge_evidence(),
            {"OLD": complete[:3], "NEW": complete[3:], "CURRENT": complete},
            view_histories={"CURRENT": {"dividend": changed}},
        )["NEW"]
        self.assertEqual([], raw_segments(report, "uncorrelated"))

        incomplete = run(
            early_successor_bridge_evidence(),
            {"OLD": complete[:3], "NEW": complete[3:], "CURRENT": complete},
            view_histories={"CURRENT": {"dividend": complete[:-1]}},
        )["NEW"]
        self.assertEqual(
            2,
            sum(item["row_count"] for item in raw_segments(incomplete, "uncorrelated")),
        )

    def test_pre_entry_carry_forward_is_only_a_non_observation_certificate(self):
        evidence, histories, first, last, _ = pre_entry_carry_forward_case()
        current = run(evidence, histories)["NEW"]
        certified = [
            item
            for item in raw_segments(current, "uncorrelated")
            if item["reason_codes"] == ["certified_pre_entry_carry_forward_non_observation"]
        ]
        self.assertEqual(
            [(first, last, len(histories["NEW"]) - 5)],
            [(item["first_session_date"], item["last_session_date"], item["row_count"]) for item in certified],
        )
        self.assertEqual([], certified[0]["candidate_securities"])

        frozen = correlate(
            Path("."),
            inputs_for(histories),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=69),
        )["NEW"]
        self.assertFalse(
            any(
                item["reason_codes"] == ["certified_pre_entry_carry_forward_non_observation"]
                for item in raw_segments(frozen, "uncorrelated")
            )
        )
        self.assertEqual(frozen["correlated"], current["correlated"])

    def test_pre_entry_carry_forward_retains_material_vetoes(self):
        cases = {}

        evidence, histories, *_ = pre_entry_carry_forward_case()
        evidence["query"]["corporate_actions_data_quality"] = "partial"
        cases["partial action query"] = (evidence, histories, {})

        evidence, histories, *_ = pre_entry_carry_forward_case()
        histories["NEW"][50].update(v="1", n="1", vw=histories["NEW"][50]["c"])
        cases["positive tail activity"] = (evidence, histories, {})

        evidence, histories, *_ = pre_entry_carry_forward_case()
        histories["NEW"][50].update(dict.fromkeys(("o", "h", "l", "c"), "99"))
        cases["changed carried value"] = (evidence, histories, {})

        evidence, histories, *_ = pre_entry_carry_forward_case()
        histories["OLD"] = histories["OLD"][:-1]
        cases["predecessor does not reach boundary"] = (evidence, histories, {})

        evidence, histories, *_ = pre_entry_carry_forward_case()
        evidence["identities"].append({"id": "other-id", "symbol": "NEW", "class": "us_equity"})
        cases["competing destination occupant"] = (evidence, histories, {})

        evidence, histories, *_ = pre_entry_carry_forward_case()
        incomplete = copy.deepcopy(histories["NEW"][:-1])
        cases["misaligned adjustment view"] = (
            evidence,
            histories,
            {"view_histories": {"NEW": {"dividend": incomplete}}},
        )

        for label, (evidence, histories, options) in cases.items():
            with self.subTest(label=label):
                report = run(evidence, histories, **options)["NEW"]
                self.assertFalse(
                    any(
                        item["reason_codes"] == ["certified_pre_entry_carry_forward_non_observation"]
                        for item in raw_segments(report, "uncorrelated")
                    )
                )

    def test_component_event_dates_only_the_observed_early_successor_span(self):
        evidence, histories, sessions = component_anchored_early_successor_case()
        current = run(evidence, histories)["NEW.WS"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        first = raw_segments(current)[0]
        self.assertEqual(sessions[3], first["first_session_date"])
        self.assertEqual(sessions[5], first["last_session_date"])
        self.assertEqual(
            sessions[3],
            first["ticker_ownership_interval"]["start_date_inclusive"],
        )

        frozen = correlate(
            Path("."),
            inputs_for(histories),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=69),
        )["NEW.WS"]
        self.assertEqual(
            [(sessions[3], sessions[5], 3)],
            [
                (item["first_session_date"], item["last_session_date"], item["row_count"])
                for item in raw_segments(frozen, "uncorrelated")
            ],
        )

    def test_component_event_early_successor_retains_material_vetoes(self):
        cases = {}

        evidence, histories, _ = component_anchored_early_successor_case()
        evidence["corporate_actions"] = [
            item for item in evidence["corporate_actions"] if item["id"] != "component-separation"
        ]
        cases["missing component event"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        evidence["symbol_occupancy_evidence"] = [
            item
            for item in evidence["symbol_occupancy_evidence"]
            if not any(record.get("action_id") == "component-separation" for record in item.get("evidence", []))
        ]
        cases["missing component occupancy"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        evidence["query"]["corporate_actions_data_quality"] = "partial"
        cases["partial action query"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        evidence["asset_lookups"][1]["asset"]["class"] = "crypto"
        cases["non-equity endpoint"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        histories["OLD.WS"] = histories["OLD.WS"][:-1]
        cases["gapped source handoff"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        for row in histories["NEW.WS"][:3]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "40"))
        cases["uninformative early successor"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        evidence["corporate_actions"].append(
            {
                "id": "competing",
                "action_type": "reverse_split",
                "symbol": "NEW.WS",
                "old_cusip": "old-cusip",
                "new_cusip": "other-cusip",
                "effective_date": histories["NEW.WS"][1]["t"][:10],
                "process_date": histories["NEW.WS"][1]["t"][:10],
            }
        )
        cases["competing structural event"] = (evidence, histories, {})

        evidence, histories, _ = component_anchored_early_successor_case()
        incomplete = copy.deepcopy(histories["NEW.WS"][:-1])
        cases["misaligned adjustment view"] = (
            evidence,
            histories,
            {"view_histories": {"NEW.WS": {"dividend": incomplete}}},
        )

        for label, (evidence, histories, options) in cases.items():
            with self.subTest(label=label):
                report = run(evidence, histories, **options)["NEW.WS"]
                self.assertEqual(
                    3,
                    sum(item["row_count"] for item in raw_segments(report, "uncorrelated")),
                )

    def test_internal_component_handoff_dates_only_the_post_event_successor_span(self):
        evidence, histories, sessions = internal_component_handoff_case()
        policy = replace(PRODUCTION_POLICY, implementation_correlation_version=74)

        candidate = correlate(
            Path("."),
            inputs_for(histories),
            {symbol: evidence for symbol in histories},
            policy=policy,
        )["NEW.WS"]

        self.assertEqual([], raw_segments(candidate, "uncorrelated"))
        early = [item for item in raw_segments(candidate) if item["first_session_date"] == sessions[6]]
        self.assertEqual(1, len(early))
        self.assertEqual(sessions[7], early[0]["last_session_date"])
        self.assertEqual(2, early[0]["row_count"])
        self.assertEqual("old-id", early[0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(sessions[6], early[0]["ticker_ownership_interval"]["start_date_inclusive"])

        frozen = correlate(
            Path("."),
            inputs_for(histories),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=73),
        )["NEW.WS"]
        self.assertEqual(
            [(sessions[6], sessions[7], 2)],
            [
                (item["first_session_date"], item["last_session_date"], item["row_count"])
                for item in raw_segments(frozen, "uncorrelated")
            ],
        )

    def test_internal_component_handoff_retains_material_vetoes(self):
        cases = []

        evidence, histories, _ = internal_component_handoff_case()
        evidence["corporate_actions"] = [
            action for action in evidence["corporate_actions"] if action["id"] != "current-component-arrival"
        ]
        cases.append(("missing later occupancy anchor", evidence, histories, {}))

        evidence, histories, _ = internal_component_handoff_case()
        changed = copy.deepcopy(histories["NEW.WS"])
        changed[2]["c"] = "999"
        cases.append(
            (
                "nonexact inherited adjustment prefix",
                evidence,
                histories,
                {"view_histories": {"NEW.WS": {"dividend": changed}}},
            )
        )

        evidence, histories, _ = internal_component_handoff_case()
        histories["OLD.WS"][6].update(v="1", n="1", vw=histories["OLD.WS"][6]["c"])
        cases.append(("predecessor is active after component event", evidence, histories, {}))

        evidence, histories, _ = internal_component_handoff_case()
        for row in histories["NEW.WS"][6:8]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "99"), v="0", n="0", vw="0")
        cases.append(("successor early span is not traded", evidence, histories, {}))

        evidence, histories, _ = internal_component_handoff_case()
        evidence["identities"].append({"id": "third-id", "symbol": "OLD.WS", "class": "us_equity"})
        cases.append(("third predecessor-label occupant", evidence, histories, {}))

        evidence, histories, sessions = internal_component_handoff_case()
        evidence["corporate_actions"].append(
            {
                "id": "competing-structure",
                "action_type": "reverse_split",
                "symbol": "OLD.WS",
                "old_cusip": "old-cusip",
                "new_cusip": "other-cusip",
                "effective_date": sessions[7],
                "process_date": sessions[7],
            }
        )
        cases.append(("competing structural action", evidence, histories, {}))

        evidence, histories, _ = internal_component_handoff_case()
        cases.append(
            (
                "incomplete successor adjustment view",
                evidence,
                histories,
                {"view_histories": {"NEW.WS": {"dividend": copy.deepcopy(histories["NEW.WS"][:-1])}}},
            )
        )

        policy = replace(PRODUCTION_POLICY, implementation_correlation_version=74)
        for label, evidence, histories, options in cases:
            with self.subTest(label=label):
                report = correlate(
                    Path("."),
                    inputs_for(histories, **options),
                    {symbol: evidence for symbol in histories},
                    policy=policy,
                )["NEW.WS"]
                self.assertGreaterEqual(
                    sum(item["row_count"] for item in raw_segments(report, "uncorrelated")),
                    2,
                )

    def test_reused_new_component_and_exact_terminal_exit_date_only_observed_prefix(self):
        evidence, histories, sessions = extended_component_handoff_case()
        current = run(evidence, histories)["NEW"]
        self.assertEqual([], raw_segments(current, "uncorrelated"))
        first = raw_segments(current)[0]
        self.assertEqual(sessions[110], first["first_session_date"])
        self.assertEqual(sessions[112], first["last_session_date"])
        self.assertEqual(
            sessions[110],
            first["ticker_ownership_interval"]["start_date_inclusive"],
        )

        frozen = correlate(
            Path("."),
            inputs_for(histories),
            {symbol: evidence for symbol in histories},
            policy=replace(PRODUCTION_POLICY, implementation_correlation_version=70),
        )["NEW"]
        self.assertEqual(
            [(sessions[110], sessions[112], 3)],
            [
                (item["first_session_date"], item["last_session_date"], item["row_count"])
                for item in raw_segments(frozen, "uncorrelated")
            ],
        )

    def test_extended_component_handoff_retains_dated_owner_endpoint_and_terminal_vetoes(self):
        cases = {}

        evidence, histories, _ = extended_component_handoff_case()
        evidence["asset_lookups"] = [
            item
            for item in evidence["asset_lookups"]
            if not (item["lookup_kind"] == "cusip" and item["lookup"] == "component-cusip")
        ]
        cases["missing component CUSIP endpoint"] = (evidence, histories)

        evidence, histories, _ = extended_component_handoff_case()
        evidence["input_symbol_occupants"] = [
            {"symbol": "OLD", "cusip": "third-cusip", "asset_id": "third-dated-id", "evidence": []}
        ]
        cases["third dated predecessor occupant"] = (evidence, histories)

        evidence, histories, _ = extended_component_handoff_case()
        action = next(item for item in evidence["corporate_actions"] if item["id"] == "component-separation")
        action["new_symbol"] = "OTHER"
        cases["predecessor is not an output component"] = (evidence, histories)

        evidence, histories, _ = extended_component_handoff_case()
        action = next(item for item in evidence["corporate_actions"] if item["id"] == "terminal-cash-merger")
        action.pop("rate")
        cases["terminal rate absent"] = (evidence, histories)

        evidence, histories, sessions = extended_component_handoff_case()
        action = next(item for item in evidence["corporate_actions"] if item["id"] == "terminal-cash-merger")
        action.update(
            effective_date=sessions[118],
            process_date=sessions[118],
            payable_date=sessions[118],
        )
        cases["terminal exit is not adjacent"] = (evidence, histories)

        evidence, histories, _ = extended_component_handoff_case()
        action = next(item for item in evidence["corporate_actions"] if item["id"] == "terminal-cash-merger")
        action["acquiree_cusip"] = "other-cusip"
        cases["terminal CUSIP differs"] = (evidence, histories)

        evidence, histories, _ = extended_component_handoff_case()
        histories["OLD"][-1].update(v="0", n="0", vw="0")
        cases["predecessor endpoint is not traded"] = (evidence, histories)

        for label, (evidence, histories) in cases.items():
            with self.subTest(label=label):
                report = run(evidence, histories)["NEW"]
                self.assertEqual(
                    3,
                    sum(item["row_count"] for item in raw_segments(report, "uncorrelated")),
                )

    def test_terminal_carry_forward_remains_unassigned_but_names_only_the_retired_candidate(self):
        evidence = terminal_carry_forward_evidence()
        histories = terminal_carry_forward_histories()
        current = run(evidence, histories)["OLD"]

        self.assertEqual(
            [("2024-01-05", "2024-01-08", 2, ["terminal_nontrading_carry_forward"])],
            [
                (
                    item["first_session_date"],
                    item["last_session_date"],
                    item["row_count"],
                    item["reason_codes"],
                )
                for item in raw_segments(current, "uncorrelated")
            ],
        )
        self.assertEqual(
            ["terminal-id"],
            [item["asset_id"] for item in raw_segments(current, "uncorrelated")[0]["candidate_securities"]],
        )
        self.assertEqual(
            {"old-id"},
            {
                asset_id
                for item in raw_segments(current)
                for asset_id in item["security_identity"]["member_alpaca_asset_ids"]
            },
        )

        v65 = replace(PRODUCTION_POLICY, implementation_correlation_version=65)
        frozen = correlate(
            Path("."),
            inputs_for(histories),
            {symbol: evidence for symbol in histories},
            policy=v65,
        )["OLD"]
        frozen_tail = raw_segments(frozen, "uncorrelated")[0]
        self.assertEqual(["outside_supported_name_change_history"], frozen_tail["reason_codes"])
        self.assertEqual([], frozen_tail["candidate_securities"])

    def test_terminal_carry_forward_exclusion_retains_every_material_veto(self):
        def case():
            return terminal_carry_forward_evidence(), terminal_carry_forward_histories()

        missing_merger, missing_merger_histories = case()
        missing_merger["corporate_actions"] = [
            action for action in missing_merger["corporate_actions"] if action["id"] != "terminal-stock-merger"
        ]

        incomplete_query, incomplete_query_histories = case()
        incomplete_query["query"]["corporate_actions_data_quality"] = "partial"

        active_acquiree, active_acquiree_histories = case()
        next(item for item in active_acquiree["identities"] if item["id"] == "terminal-id").update(
            status="active", tradable=True
        )

        competing_owner, competing_owner_histories = case()
        competing_owner["identities"].append({"id": "other-id", "symbol": "OLD", "class": "us_equity"})

        traded_tail, traded_tail_histories = case()
        traded_tail_histories["OLD"][3].update(v="1", n="1")

        changed_tail, changed_tail_histories = case()
        changed_tail_histories["OLD"][3].update(dict.fromkeys(("o", "h", "l", "c"), "99"))

        untraded_final_close, untraded_final_close_histories = case()
        untraded_final_close_histories["OLD"][2].update(v="0", n="0", vw="0")

        uninformative_predecessor, uninformative_predecessor_histories = case()
        for row in uninformative_predecessor_histories["OLD"][:3]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "12"))

        inert_successor, inert_successor_histories = case()
        for row in inert_successor_histories["NEW"]:
            row.update(dict.fromkeys(("o", "h", "l", "c"), "40"), v="0", n="0", vw="0")

        competing_merger, competing_merger_histories = case()
        extra = copy.deepcopy(competing_merger["corporate_actions"][-1])
        extra["id"] = "competing-stock-merger"
        competing_merger["corporate_actions"].append(extra)

        incomplete_view, incomplete_view_histories = case()
        incomplete_dividend = copy.deepcopy(incomplete_view_histories["OLD"][:-1])

        cases = {
            "missing terminal action": (missing_merger, missing_merger_histories, {}),
            "incomplete action query": (incomplete_query, incomplete_query_histories, {}),
            "acquiree remains active": (active_acquiree, active_acquiree_histories, {}),
            "competing old-label occupant": (competing_owner, competing_owner_histories, {}),
            "positive tail activity": (traded_tail, traded_tail_histories, {}),
            "tail does not carry final close": (changed_tail, changed_tail_histories, {}),
            "final predecessor close is not traded": (
                untraded_final_close,
                untraded_final_close_histories,
                {},
            ),
            "predecessor source is not informative trading": (
                uninformative_predecessor,
                uninformative_predecessor_histories,
                {},
            ),
            "successor is not informative trading": (
                inert_successor,
                inert_successor_histories,
                {},
            ),
            "competing terminal action": (competing_merger, competing_merger_histories, {}),
            "incomplete adjustment view": (
                incomplete_view,
                incomplete_view_histories,
                {"view_histories": {"OLD": {"dividend": incomplete_dividend}}},
            ),
        }
        for label, (evidence, histories, options) in cases.items():
            with self.subTest(label=label):
                tail = raw_segments(run(evidence, histories, **options)["OLD"], "uncorrelated")[-1]
                self.assertNotEqual(["terminal_nontrading_carry_forward"], tail["reason_codes"])
                self.assertEqual([], tail["candidate_securities"])

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
        reports = correlate(Path("."), inputs, {symbol: evidence for symbol in inputs}, policy=PRODUCTION_POLICY)
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

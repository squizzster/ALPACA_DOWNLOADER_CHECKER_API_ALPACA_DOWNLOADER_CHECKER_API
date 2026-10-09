import json
from collections import deque
from typing import Any

from .alpaca_http import AlpacaIdentityClient
from .local_downloads import local_query_dates
from .settings import (
    CUSIP_FIELD_PATTERN,
    MAX_NAME_CHANGE_SYMBOLS,
    SYMBOL_FIELD_PATTERN,
    SYMBOL_PATTERN,
)
from .symbol_disposition import classify_symbol_disposition


def action_identity_key(action: dict[str, Any]) -> tuple[str, str]:
    action_id = action.get("id")
    if isinstance(action_id, str):
        return str(action.get("action_type")), action_id
    return str(action.get("action_type")), json.dumps(action, sort_keys=True, separators=(",", ":"))


def action_date(action: dict[str, Any]) -> str:
    for field_name in ("process_date", "effective_date", "ex_date"):
        value = action.get(field_name)
        if isinstance(value, str):
            return value
    return ""


def related_name_change_symbols(action: dict[str, Any]) -> set[str]:
    if action.get("action_type") != "name_change":
        return set()
    related_symbols = set()
    for field_name in ("old_symbol", "new_symbol"):
        value = action.get(field_name)
        if isinstance(value, str) and SYMBOL_PATTERN.fullmatch(value):
            related_symbols.add(value)
    return related_symbols


def extract_cusips(actions: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            value
            for action in actions
            for field_name, value in action.items()
            if CUSIP_FIELD_PATTERN.search(field_name) and isinstance(value, str) and value
        }
    )


def compact_asset(asset: dict[str, Any]) -> dict[str, Any]:
    fields = ("id", "class", "exchange", "symbol", "name", "status", "tradable")
    return {field_name: asset.get(field_name) for field_name in fields}


def symbol_cusip_evidence(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evidence = {}
    for action in actions:
        for symbol_field, symbol in action.items():
            if not SYMBOL_FIELD_PATTERN.search(symbol_field) or not isinstance(symbol, str):
                continue
            cusip_field = symbol_field.removesuffix("symbol") + "cusip"
            cusip = action.get(cusip_field)
            if not isinstance(cusip, str) or not cusip:
                continue
            role = symbol_field.removesuffix("_symbol")
            item = {
                "symbol": symbol,
                "cusip": cusip,
                "role": role or "security",
                "action_type": action.get("action_type"),
                "action_id": action.get("id"),
                "process_date": action.get("process_date"),
            }
            key = (
                item["symbol"],
                item["cusip"],
                item["role"],
                item["action_type"],
                item["action_id"],
            )
            evidence[key] = item
    return sorted(
        evidence.values(),
        key=lambda item: (
            str(item.get("process_date") or ""),
            str(item["symbol"]),
            str(item["cusip"]),
            str(item["role"]),
        ),
    )


def build_identity_report(
    symbol: str,
    local_report: dict[str, Any],
    client: AlpacaIdentityClient,
) -> dict[str, Any]:
    start_date, end_date = local_query_dates(local_report)
    pending_symbols = deque([symbol])
    queued_symbols = {symbol}
    queried_symbols = []
    actions_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    traversal_limited = False

    while pending_symbols:
        queried_symbol = pending_symbols.popleft()
        queried_symbols.append(queried_symbol)
        actions = client.get_corporate_actions(queried_symbol, start_date, end_date)
        for action in actions:
            actions_by_key[action_identity_key(action)] = action
            for related_symbol in sorted(related_name_change_symbols(action)):
                if related_symbol in queued_symbols:
                    continue
                if len(queued_symbols) >= MAX_NAME_CHANGE_SYMBOLS:
                    traversal_limited = True
                    continue
                queued_symbols.add(related_symbol)
                pending_symbols.append(related_symbol)

    actions = sorted(
        actions_by_key.values(),
        key=lambda item: (action_date(item), str(item.get("action_type")), str(item.get("id"))),
    )
    cusips = extract_cusips(actions)
    asset_lookups = []
    assets_by_id: dict[str, dict[str, Any]] = {}
    asset_id_by_cusip = {}

    for lookup_kind, identifiers in (("symbol", sorted(queued_symbols)), ("cusip", cusips)):
        for identifier in identifiers:
            lookup_result = client.get_asset_lookup(identifier)
            asset = lookup_result["asset"]
            asset_lookups.append(
                {
                    "lookup": identifier,
                    "lookup_kind": lookup_kind,
                    "found": asset is not None,
                    "outcome": lookup_result["outcome"],
                    "http_status": lookup_result["http_status"],
                    "asset": compact_asset(asset) if asset is not None else None,
                }
            )
            if asset is None or not isinstance(asset.get("id"), str):
                continue
            asset_id = asset["id"]
            identity = assets_by_id.setdefault(
                asset_id,
                {
                    **compact_asset(asset),
                    "lookup_symbols": set(),
                    "lookup_cusips": set(),
                },
            )
            identity[f"lookup_{lookup_kind}s"].add(identifier)
            if lookup_kind == "cusip":
                asset_id_by_cusip[identifier] = asset_id

    identities = []
    for identity in assets_by_id.values():
        identities.append(
            {
                **{key: value for key, value in identity.items() if not key.startswith("lookup_")},
                "lookup_symbols": sorted(identity["lookup_symbols"]),
                "lookup_cusips": sorted(identity["lookup_cusips"]),
            }
        )
    identities.sort(key=lambda item: (str(item.get("symbol")), str(item.get("id"))))

    evidence_by_symbol_cusip: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in symbol_cusip_evidence(actions):
        evidence_by_symbol_cusip.setdefault((item["symbol"], item["cusip"]), []).append(item)
    occupancy_evidence = [
        {
            "symbol": evidence_symbol,
            "cusip": cusip,
            "asset_id": asset_id_by_cusip.get(cusip),
            "evidence": evidence_items,
        }
        for (evidence_symbol, cusip), evidence_items in sorted(evidence_by_symbol_cusip.items())
    ]
    input_symbol_occupants = [item for item in occupancy_evidence if item["symbol"] == symbol]
    symbol_changes = [
        {
            key: action.get(key)
            for key in (
                "id",
                "process_date",
                "old_symbol",
                "old_cusip",
                "new_symbol",
                "new_cusip",
            )
        }
        for action in actions
        if action.get("action_type") == "name_change"
    ]

    warnings = [
        "Alpaca bar rows remain symbol-keyed; this report does not bind individual rows to an asset UUID."
    ]
    if local_report["downloads"]:
        warnings.append(
            "Local gzip SHA-256 authentication was not performed; stat_matches_sidecar is only a metadata check."
        )
    if len(input_symbol_occupants) > 1:
        warnings.append(
            f"The API returned evidence for {len(input_symbol_occupants)} CUSIPs occupying {symbol}."
        )
    if traversal_limited:
        warnings.append(f"Name-change traversal stopped at its {MAX_NAME_CHANGE_SYMBOLS}-symbol safety limit.")
    if not local_report["present"]:
        warnings.append(f"No local end-of-day directory exists for {symbol}.")
    if any(not item["stat_matches_sidecar"] for item in local_report["downloads"]):
        warnings.append("At least one local gzip/sidecar pair is missing, unreadable, or stat-inconsistent.")

    symbol_disposition = classify_symbol_disposition(
        symbol,
        local_report,
        asset_lookups,
        identities,
        input_symbol_occupants,
    )

    return {
        "query": {
            "input_symbol": symbol,
            "corporate_actions_start": start_date,
            "corporate_actions_end": end_date,
            "corporate_actions_data_quality": "complete",
        },
        "api_request_count": client.request_count,
        "queried_symbols": queried_symbols,
        "corporate_action_count": len(actions),
        "corporate_actions": actions,
        "asset_lookups": asset_lookups,
        "identities": identities,
        "symbol_changes": symbol_changes,
        "symbol_occupancy_evidence": occupancy_evidence,
        "input_symbol_occupants": input_symbol_occupants,
        "unresolved_cusips": sorted(set(cusips) - set(asset_id_by_cusip)),
        "symbol_disposition": symbol_disposition,
        "warnings": warnings,
    }

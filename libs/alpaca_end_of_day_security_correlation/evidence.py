from datetime import date
from typing import Any

from .ownership_interval_rules import (
    TickerOwnershipInterval,
    build_ticker_ownership_intervals,
    ownership_interval_rule_order,
)

__all__ = [
    "TickerOwnershipInterval",
    "build_ticker_ownership_intervals",
    "canonical_securities_by_asset_id",
    "identity_candidates_for_symbol",
    "ownership_interval_rule_order",
]


def canonical_securities_by_asset_id(lineage: dict[str, Any]) -> dict[str, dict[str, Any]]:
    identities = lineage.get("identities")
    if not isinstance(identities, list):
        return {}
    return {
        identity["id"]: {
            "asset_id": identity["id"],
            "asset_symbol": identity.get("symbol"),
            "asset_name": identity.get("name"),
            "asset_class": identity.get("class"),
            "exchange": identity.get("exchange"),
            "asset_status": identity.get("status"),
        }
        for identity in identities
        if isinstance(identity, dict) and isinstance(identity.get("id"), str)
    }


def identity_candidates_for_symbol(
    lineage: dict[str, Any],
    symbol: str,
    intervals: list[TickerOwnershipInterval],
    session_date: date,
) -> list[dict[str, Any]]:
    securities = canonical_securities_by_asset_id(lineage)
    candidates_by_key = {}
    occupancy_evidence = lineage.get("symbol_occupancy_evidence")
    if not isinstance(occupancy_evidence, list):
        occupancy_evidence = []

    for occupancy in occupancy_evidence:
        if not isinstance(occupancy, dict) or occupancy.get("symbol") != symbol:
            continue
        cusip = occupancy.get("cusip")
        asset_id = occupancy.get("asset_id")
        if not isinstance(cusip, str) or not isinstance(asset_id, str):
            continue
        candidate_intervals = [
            interval
            for interval in intervals
            if interval.historical_symbol == symbol and interval.asset_id == asset_id
        ]
        unknown_prefix = any(
            interval.start_date_inclusive is not None
            and session_date < interval.start_date_inclusive
            and any(rule.name == "action_anchored_current_ticker_occupancy" for rule in interval.applied_rules)
            for interval in candidate_intervals
        )
        if (
            candidate_intervals
            and not unknown_prefix
            and not any(interval.contains(session_date) for interval in candidate_intervals)
        ):
            continue
        evidence = occupancy.get("evidence")
        candidates_by_key[(asset_id, cusip)] = {
            **securities.get(asset_id, {"asset_id": asset_id}),
            "cusip": cusip,
            "evidence": evidence if isinstance(evidence, list) else [],
        }

    asset_lookups = lineage.get("asset_lookups")
    if isinstance(asset_lookups, list):
        for lookup in asset_lookups:
            if (
                not isinstance(lookup, dict)
                or lookup.get("lookup_kind") != "symbol"
                or lookup.get("lookup") != symbol
            ):
                continue
            asset = lookup.get("asset")
            if not isinstance(asset, dict) or not isinstance(asset.get("id"), str):
                continue
            asset_id = asset["id"]
            if any(candidate_asset_id == asset_id for candidate_asset_id, _ in candidates_by_key):
                continue
            candidate_identity = securities.get(asset_id, {"asset_id": asset_id})
            candidates_by_key[(asset_id, None)] = {
                **candidate_identity,
                "cusip": None,
                "evidence": [
                    {
                        "evidence_type": "current_symbol_asset_lookup",
                        "symbol": symbol,
                    }
                ],
            }

    return sorted(
        candidates_by_key.values(),
        key=lambda item: (str(item.get("cusip") or ""), str(item["asset_id"])),
    )

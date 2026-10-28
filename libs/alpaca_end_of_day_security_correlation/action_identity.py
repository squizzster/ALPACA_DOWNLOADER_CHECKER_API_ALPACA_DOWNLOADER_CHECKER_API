"""Exact provider evidence shared by action ownership and membership rules."""

import re
from dataclasses import dataclass
from typing import Any


def exact_cusip_lookup_asset(lineage: dict[str, Any], cusip: str) -> dict[str, Any] | None:
    # Consume the evidence owner's completed exact lookup, including its outcome.
    # This selector performs no HTTP request; a symbol observation cannot stand
    # in for missing/ambiguous CUSIP evidence or establish historical ownership.
    lookups = lineage.get("asset_lookups")
    if not isinstance(lookups, list):
        return None
    matching = [
        item
        for item in lookups
        if isinstance(item, dict) and item.get("lookup_kind") == "cusip" and item.get("lookup") == cusip
    ]
    if len(matching) != 1:
        return None
    lookup = matching[0]
    asset = lookup.get("asset")
    if (
        lookup.get("found") is not True
        or lookup.get("outcome") != "found"
        or lookup.get("http_status") != 200
        or not isinstance(asset, dict)
        or asset.get("class") != "us_equity"
        or not isinstance(asset.get("id"), str)
        or not asset["id"]
    ):
        return None
    return asset


def exact_symbol_lookup_asset(lineage: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    """Return one completed exact symbol lookup, without projecting it backwards."""
    # The observation is authoritative for the observed lookup only. Dated
    # ownership still belongs to the interval rules, not today's ticker spelling.
    lookups = lineage.get("asset_lookups")
    if not isinstance(lookups, list):
        return None
    matching = [
        item
        for item in lookups
        if isinstance(item, dict) and item.get("lookup_kind") == "symbol" and item.get("lookup") == symbol
    ]
    if len(matching) != 1:
        return None
    lookup = matching[0]
    asset = lookup.get("asset")
    if (
        lookup.get("found") is not True
        or lookup.get("outcome") != "found"
        or lookup.get("http_status") != 200
        or not isinstance(asset, dict)
        or asset.get("class") != "us_equity"
        or asset.get("symbol") != symbol
        or not isinstance(asset.get("id"), str)
        or not asset["id"]
    ):
        return None
    return asset


@dataclass(frozen=True)
class CompletedStockMergerEndpoints:
    symbol: str
    predecessor_asset_id: str
    successor_asset_id: str


def complete_stock_merger_endpoints(
    action: dict[str, Any], lineage: dict[str, Any]
) -> CompletedStockMergerEndpoints | None:
    """Fill one omitted label, never override an explicit historical label.

    The dated action supplies the relationship; exact CUSIP lookups supply the
    missing spelling and UUIDs. Raw evidence stays unchanged. Both owners use
    this same interpretation before their existing transition/conflict rules.
    """
    if action.get("action_type") != "stock_merger":
        return None
    old, new = action.get("acquiree_symbol"), action.get("acquirer_symbol")
    if (old in (None, "")) == (new in (None, "")):
        return None
    symbol = new if old in (None, "") else old
    if not isinstance(symbol, str) or re.fullmatch(r"[A-Z][A-Z0-9.-]*", symbol) is None:
        return None
    cusips = (action.get("acquiree_cusip"), action.get("acquirer_cusip"))
    if not all(isinstance(cusip, str) and cusip for cusip in cusips) or cusips[0] == cusips[1]:
        return None
    assets = [exact_cusip_lookup_asset(lineage, cusip) for cusip in cusips]
    if not all(
        asset is not None
        and asset.get("symbol") == symbol
        and asset.get("exchange") not in (None, "", "OTC")
        and symbol not in cusips
        for asset in assets
    ):
        return None
    occupancy = lineage.get("symbol_occupancy_evidence")
    if not isinstance(occupancy, list):
        return None
    for cusip, asset in zip(cusips, assets):
        known_ids = {
            item["asset_id"]
            for item in occupancy
            if isinstance(item, dict)
            and (item.get("symbol"), item.get("cusip")) == (symbol, cusip)
            and isinstance(item.get("asset_id"), str)
            and item["asset_id"]
        }
        if known_ids - {asset["id"]}:
            return None
    return CompletedStockMergerEndpoints(symbol, assets[0]["id"], assets[1]["id"])

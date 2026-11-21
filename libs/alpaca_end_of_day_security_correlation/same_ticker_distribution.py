"""Dated continuity for a one-for-one distribution under the same label."""

from datetime import date
from decimal import Decimal, InvalidOperation

from .action_identity import exact_cusip_lookup_asset, exact_symbol_lookup_asset
from .uuid_lineage import UuidLineageEdge

NAME = "same_ticker_one_for_one_distribution"
CORRELATION_VERSION = 95


def retained_edge(receipt):
    """Lend a completed cutover from a receipt whose prefix was accepted."""
    edge = next((edge for edge in receipt.get("uuid_lineage_edges", ()) if edge["edge_type"] == NAME), None)
    if edge is None:
        return None
    old, new = edge["predecessor"], edge["successor"]
    return UuidLineageEdge(
        NAME,
        edge["corporate_action_id"],
        date.fromisoformat(edge["effective_date"]),
        old["asset_id"],
        old["symbol"],
        old["cusip"],
        new["asset_id"],
        new["symbol"],
        new["cusip"],
    )


def _cutover(action):
    try:
        old, new, distributed = (
            Decimal(str(action[field])) for field in ("old_rate", "new_rate", "alternate_rate")
        )
        if not all(rate.is_finite() and rate > 0 for rate in (old, new, distributed)) or old != new:
            return None, "invalid_or_unequal_distribution_rates"
        effective = date.fromisoformat(action["effective_date"])
        ex = date.fromisoformat(action["ex_date"]) if action.get("ex_date") else None
        processed = date.fromisoformat(action["process_date"]) if action.get("process_date") else None
        payable = date.fromisoformat(action["payable_date"]) if action.get("payable_date") else None
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None, "invalid_distribution_terms"
    # The ex-date governs trading. Without it, the processing date needs the
    # effective date or the payment date to corroborate the market switch.
    boundary = ex or processed
    if boundary is None or boundary < effective or (ex is None and processed not in (effective, payable)):
        return None, "distribution_market_date_uncorroborated"
    return boundary, None


def _competing_action(actions, selected, edge):
    symbol = edge.predecessor_symbol
    identifiers = {edge.predecessor_cusip, edge.successor_cusip}
    for action in actions:
        if action is selected:
            continue
        if action.get("action_type") in ("cash_dividend", "stock_dividend"):
            cusip = action.get("cusip")
            if action.get("symbol") == symbol and cusip in identifiers:
                try:
                    when = date.fromisoformat(action["ex_date"])
                except (KeyError, TypeError, ValueError):
                    return "undated_same_label_dividend"
                if (cusip == edge.predecessor_cusip) != (when < edge.effective_date):
                    return "dated_occupant_conflicts_with_distribution"
            continue
        if any(
            isinstance(value, str) and (value == symbol or value in identifiers)
            for key, value in action.items()
            if key.endswith(("symbol", "cusip"))
        ):
            return "competing_structural_event"
    return None


def _endpoints(lineage, action, symbol, identifiers):
    predecessor, successor, recipient = (exact_cusip_lookup_asset(lineage, cusip) for cusip in identifiers)
    current = exact_symbol_lookup_asset(lineage, symbol)
    if any(asset is None for asset in (predecessor, successor, recipient, current)):
        return None, "missing_or_conflicting_distribution_lookup"
    old, new, alternate = identifiers
    owners = {old: predecessor["id"], new: successor["id"]}
    asset_ids = set(owners.values())
    if (
        len(asset_ids | {recipient["id"]}) != 3
        or predecessor.get("symbol") != symbol
        or successor.get("symbol") != symbol
        or predecessor.get("status") != "inactive"
        or successor.get("status") != "active"
        or current["id"] != successor["id"]
        or current.get("status") != "active"
        or predecessor.get("exchange") != successor.get("exchange")
        or current.get("exchange") != successor.get("exchange")
        or successor.get("exchange") in (None, "", "OTC")
        or not recipient.get("symbol")
        or recipient["symbol"] == symbol
        or action.get("alternate_symbol") not in (recipient["symbol"], alternate)
    ):
        return None, "competing_occupant_or_distribution_recipient"
    if any(
        asset.get("symbol") == symbol and asset.get("id") not in asset_ids
        for asset in lineage.get("identities", ())
    ) or any(
        item.get("cusip") not in owners or (item.get("asset_id") and item["asset_id"] != owners[item["cusip"]])
        for item in lineage.get("symbol_occupancy_evidence", ())
        if item.get("symbol") == symbol
    ):
        return None, "additional_same_label_claim"
    return (predecessor["id"], successor["id"]), None


def assess(lineage, actions):
    """Compare one isolated own-label event against the retained provider facts."""
    query = lineage.get("query")
    symbol = query.get("input_symbol") if isinstance(query, dict) else None
    if not isinstance(symbol, str) or not symbol:
        return None, None
    matches = [
        action
        for action in actions
        if action.get("action_type") == "unit_split"
        and action.get("old_symbol") == action.get("new_symbol") == symbol
    ]
    if not matches:
        return None, None

    def unresolved(reason):
        return None, {
            "reason_code": reason,
            "ownership_interval_rule": NAME,
            "symbol": symbol,
            "supporting_corporate_action_ids": [action["id"] for action in matches if action.get("id")],
        }

    if len(matches) != 1:
        return unresolved("competing_same_ticker_distributions")
    (action,) = matches
    identifiers = tuple(action.get(role + "_cusip") for role in ("old", "new", "alternate"))
    if (
        not isinstance(action.get("id"), str)
        or not action["id"]
        or not all(isinstance(cusip, str) and cusip for cusip in identifiers)
        or len(set(identifiers)) != 3
    ):
        return unresolved("invalid_distribution_endpoints")

    boundary, reason = _cutover(action)
    if reason:
        return unresolved(reason)
    endpoints, reason = _endpoints(lineage, action, symbol, identifiers)
    if reason:
        return unresolved(reason)
    predecessor, successor = endpoints
    edge = UuidLineageEdge(
        edge_type=NAME,
        action_id=action["id"],
        effective_date=boundary,
        predecessor_asset_id=predecessor,
        predecessor_symbol=symbol,
        predecessor_cusip=identifiers[0],
        successor_asset_id=successor,
        successor_symbol=symbol,
        successor_cusip=identifiers[1],
    )
    reason = _competing_action(actions, action, edge)
    return unresolved(reason) if reason else (edge, None)

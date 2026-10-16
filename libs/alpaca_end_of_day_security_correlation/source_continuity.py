"""Distinguish a continuing source-label chain from converging foreign branches."""

from datetime import date, timedelta

from .action_identity import exact_cusip_lookup_asset
from .terminal_episode import shared_inert_bridges


def retired_history_lower_bound(symbol, intervals, work_items):
    """Bound new backward inference, without redefining a dated owner's birth."""
    owners = [item for item in intervals if item.historical_symbol == symbol]
    if len(owners) != 1 or owners[0].start_date_inclusive is None:
        return None
    bridges = shared_inert_bridges(work_items, include_edges=True, before=owners[0].start_date_inclusive)
    if not bridges:
        return None
    sessions = next(sessions for _, source, sessions in work_items if source["adjustment"] == "raw")
    return sessions[bridges[-1][1]] + timedelta(days=1)


def _signature(asset):
    if asset is None or asset.get("exchange") in (None, "", "OTC"):
        return None
    name = asset.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    # Legal-form and share-class words matter; do not reduce to a company stem.
    return " ".join(name.casefold().split()), asset["class"], asset["exchange"]


def retired_source_identity(symbol, lineage, intervals, edges, asof):
    """Infer dated retired-source continuity, never pretend the API mapped it."""
    try:
        when = date.fromisoformat(asof[:10])
    except (TypeError, ValueError):
        return None
    owners = [item for item in intervals if item.historical_symbol == symbol]
    if len(owners) != 1 or owners[0].end_date_exclusive is None or owners[0].end_date_exclusive > when:
        return None
    current = owners[0]
    known = (
        {current.asset_id}
        | {
            item.get("id")
            for item in lineage.get("identities", [])
            if item.get("symbol") == symbol and item.get("id")
        }
        | {
            item.get("asset_id")
            for item in lineage.get("symbol_occupancy_evidence", [])
            if item.get("symbol") == symbol and item.get("asset_id")
        }
        | {
            item.get("asset_id")
            for item in lineage.get("input_symbol_occupants", [])
            if item.get("symbol") == symbol and item.get("asset_id")
        }
        | {
            item["asset"].get("id")
            for item in lineage.get("asset_lookups", [])
            if item.get("found") is True
            and isinstance(item.get("asset"), dict)
            and (
                item["asset"].get("symbol") == symbol
                or item.get("lookup_kind") == "symbol"
                and item.get("lookup") == symbol
            )
        }
    )
    if known != {current.asset_id}:
        return None
    visited = {symbol}
    while not current.contains(when):
        outgoing = {
            edge
            for edge in edges
            if (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            == current.identity_key()
            and edge.effective_date <= when
        }
        destinations = {
            (
                edge.edge_type,
                edge.effective_date,
                edge.successor_symbol,
                edge.successor_cusip,
                edge.successor_asset_id,
            )
            for edge in outgoing
        }
        if len(destinations) != 1:
            return None
        kind, effective, label, cusip, asset_id = next(iter(destinations))
        if (
            kind != "name_change"
            or asset_id != current.asset_id
            or label in visited
            or effective != current.end_date_exclusive
        ):
            return None
        matches = [
            item
            for item in intervals
            if item.identity_key() == (label, cusip, asset_id) and item.start_date_inclusive == effective
        ]
        if len(matches) != 1 or (
            matches[0].end_date_exclusive is not None and matches[0].end_date_exclusive <= effective
        ):
            return None
        visited.add(label)
        current = matches[0]
    endpoints = [
        item for item in intervals if item.historical_symbol == current.historical_symbol and item.contains(when)
    ]
    if endpoints != [current]:
        return None
    lookups = [
        item
        for item in lineage.get("asset_lookups", [])
        if item.get("lookup_kind") == "symbol" and item.get("lookup") == current.historical_symbol
    ]
    if len(lookups) != 1:
        return None
    lookup = lookups[0]
    named = lookup.get("asset", {})
    exact = exact_cusip_lookup_asset(lineage, current.cusip)
    if (
        lookup.get("found") is not True
        or lookup.get("outcome") != "found"
        or lookup.get("http_status") != 200
        or named.get("class") != "us_equity"
        or exact is None
        or (named.get("id"), named.get("symbol")) != (current.asset_id, current.historical_symbol)
        or (exact.get("id"), exact.get("symbol")) != (current.asset_id, current.historical_symbol)
    ):
        return None
    return current


def same_symbol_continuation_ids(symbol, lineage, intervals, selected, certified_predecessors, branch_ids):
    """Prefer comparative continuity evidence, never just a requested label.

    The existing certificate proves the complete dated same-label handoff chain.
    Exact provider endpoint metadata must agree along that chain and distinguish
    competing branches. Processing-date order between sibling branches adds no
    preference. This is a supported inference, not proof of vendor row origin.
    """
    if selected is None or not certified_predecessors:
        return frozenset()
    chain = [item for item in intervals if item.historical_symbol == symbol]
    chain.sort(key=lambda item: item.start_date_inclusive or date.min)
    if chain[-1] != selected:
        return frozenset()
    signatures = set()
    for item in chain:
        asset = exact_cusip_lookup_asset(lineage, item.cusip)
        signature = _signature(asset)
        if signature is None or asset["id"] != item.asset_id or asset.get("symbol") != symbol:
            return frozenset()
        signatures.add(signature)
    if len(signatures) != 1:
        return frozenset()
    signature = next(iter(signatures))
    ids = frozenset(item.asset_id for item in chain)
    if not ids <= branch_ids:
        return frozenset()
    for item in intervals:
        if item.asset_id not in branch_ids or item.historical_symbol == symbol:
            continue
        if item.asset_id in ids:
            # Prior history of a chain member under another label contradicts
            # an inference that its source-label predecessor is the continuation.
            if item.start_date_inclusive is None or item.start_date_inclusive < selected.start_date_inclusive:
                return frozenset()
            continue
        asset = exact_cusip_lookup_asset(lineage, item.cusip)
        competitor = _signature(asset)
        if competitor is None or asset["id"] != item.asset_id or competitor == signature:
            return frozenset()
    return ids


def cash_acquiree_intervals_outside_source(symbol, lineage, intervals):
    """A cash buyer does not inherit the targets' historical pricing episodes."""
    lookups = [
        item
        for item in lineage.get("asset_lookups", [])
        if item.get("lookup_kind") == "symbol" and item.get("lookup") == symbol
    ]
    if len(lookups) != 1:
        return frozenset()
    lookup = lookups[0]
    asset = lookup.get("asset")
    if (
        lookup.get("found") is not True
        or lookup.get("outcome") != "found"
        or lookup.get("http_status") != 200
        or not isinstance(asset, dict)
        or asset.get("class") != "us_equity"
        or asset.get("symbol") != symbol
        or not asset.get("id")
    ):
        return frozenset()
    source_ids = {item.get("id") for item in lineage.get("identities", []) if item.get("symbol") == symbol} | {
        item.get("asset_id")
        for item in lineage.get("symbol_occupancy_evidence", [])
        if item.get("symbol") == symbol
    }
    if source_ids - {None, asset["id"]}:
        return frozenset()
    actions = {item.get("id"): item for item in lineage.get("corporate_actions", []) if item.get("id")}
    excluded = set()
    for interval in intervals:
        if interval.historical_symbol == symbol or not interval.applied_rules:
            continue
        if any(rule.name != "cash_merger_acquiree_terminal_boundary" for rule in interval.applied_rules):
            continue
        action_ids = {action_id for rule in interval.applied_rules for action_id in rule.supporting_action_ids}
        if not action_ids:
            continue
        for action_id in action_ids:
            action = actions.get(action_id, {})
            acquirer = exact_cusip_lookup_asset(lineage, action.get("acquirer_cusip"))
            acquiree = exact_cusip_lookup_asset(lineage, action.get("acquiree_cusip"))
            if (
                action.get("action_type") != "cash_merger"
                or (action.get("acquiree_symbol"), action.get("acquiree_cusip"))
                != (interval.historical_symbol, interval.cusip)
                or action.get("acquirer_symbol") != symbol
                or acquiree is None
                or acquiree["id"] != interval.asset_id
                or acquiree["id"] == asset["id"]
                or acquirer is None
                or acquirer["id"] != asset["id"]
                or acquirer.get("symbol") != symbol
            ):
                break
        else:
            excluded.add(interval.interval_key())
    return frozenset(excluded)

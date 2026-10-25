"""Producer-owned promotion of narrowly proved, unchanged rule decisions."""

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .contract import versions


def _cash_terminal_decision_unchanged(receipt):
    """Prove the correlation-57 cash-terminal rule cannot change this receipt."""
    asset_ids = receipt.get("asset_ids")
    if (
        not isinstance(asset_ids, list)
        or any(not isinstance(asset_id, str) or not asset_id for asset_id in asset_ids)
        or len(set(asset_ids)) != len(asset_ids)
    ):
        return False
    if len(asset_ids) < 2:
        return True
    evidence = receipt.get("evidence")
    if not isinstance(evidence, dict):
        return False

    from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
        build_ticker_ownership_intervals,
    )

    _, issues = build_ticker_ownership_intervals(evidence)
    return not any(
        issue.get("reason_code") == "cash_merger_unbounded_ownership_conflicts_with_dated_occupancy"
        for issue in issues
    )


def _active_request_horizon_may_change(receipt, evidence):
    """Recognize the compact shape of the new bounded active-suffix decision."""
    if not isinstance(receipt.get("unresolved_rows"), int) or receipt["unresolved_rows"] <= 0:
        return False
    symbol = receipt.get("symbol")
    asset_lookups = evidence.get("asset_lookups")
    if not isinstance(asset_lookups, list):
        return True
    active_lookups = {
        asset.get("id")
        for item in asset_lookups
        if isinstance(item, dict)
        and item.get("lookup_kind") == "symbol"
        and item.get("lookup") == symbol
        and item.get("found") is True
        and isinstance((asset := item.get("asset")), dict)
        and asset.get("symbol") == symbol
        and asset.get("status") == "active"
        and asset.get("id")
    }
    if len(active_lookups) != 1:
        return False
    summary = receipt.get("assignment_summary")
    request = receipt.get("pricing_request")
    if not isinstance(summary, dict) or not isinstance(request, dict):
        return True
    last_sessions = {
        item.get("last_session")
        for item in summary.values()
        if isinstance(item, dict) and item.get("last_session")
    }
    request_end = request.get("end_exclusive")
    if len(last_sessions) != 1 or not isinstance(request_end, str):
        return True
    last_session = next(iter(last_sessions))
    try:
        coverage = expected_market_session_coverage(last_session, request_end[:10])
    except (MarketCalendarError, TypeError, ValueError):
        return True
    return coverage.session_dates != (last_session,)


def _evidence_asset_ids_by_symbol(evidence, retained_asset_ids):
    result = {}

    def add(symbol, asset_id):
        if isinstance(symbol, str) and symbol and isinstance(asset_id, str) and asset_id in retained_asset_ids:
            result.setdefault(symbol, set()).add(asset_id)

    for identity in evidence["identities"]:
        if isinstance(identity, dict):
            add(identity.get("symbol"), identity.get("id"))
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        occupancies = evidence.get(field, [])
        if not isinstance(occupancies, list):
            continue
        for occupancy in occupancies:
            if isinstance(occupancy, dict):
                add(occupancy.get("symbol"), occupancy.get("asset_id"))
    for lookup in evidence["asset_lookups"]:
        if not isinstance(lookup, dict) or not isinstance(lookup.get("asset"), dict):
            continue
        asset = lookup["asset"]
        add(asset.get("symbol"), asset.get("id"))
        if lookup.get("lookup_kind") == "symbol":
            add(lookup.get("lookup"), asset.get("id"))
    return result


def _connected_components(asset_ids, edges):
    components = [{asset_id} for asset_id in asset_ids]
    for edge in edges:
        left = next((item for item in components if edge.predecessor_asset_id in item), None)
        right = next((item for item in components if edge.successor_asset_id in item), None)
        if left is None or right is None or left is right:
            continue
        left.update(right)
        components.remove(right)
    return components


def _correlation_58_decision_may_change(receipt):
    asset_ids_value = receipt.get("asset_ids")
    if not isinstance(asset_ids_value, list) or any(
        not isinstance(asset_id, str) or not asset_id for asset_id in asset_ids_value
    ):
        return True
    asset_ids = set(asset_ids_value)
    if len(asset_ids) != len(set(asset_ids_value)) or not asset_ids:
        return True
    if len(asset_ids) == 1 and receipt.get("unresolved_rows") == 0:
        return False
    evidence = receipt.get("evidence")
    if not isinstance(evidence, dict) or any(
        not isinstance(evidence.get(field), list)
        for field in ("corporate_actions", "identities", "symbol_occupancy_evidence", "asset_lookups")
    ):
        return True
    if _active_request_horizon_may_change(receipt, evidence):
        return True

    from libs.alpaca_end_of_day_security_correlation.uuid_lineage import (
        build_uuid_lineage_edges,
    )

    accepted_edges, _ = build_uuid_lineage_edges(evidence)
    owners_by_symbol = _evidence_asset_ids_by_symbol(evidence, asset_ids)
    if any(
        len(owners) > 1 and len(_connected_components(owners, accepted_edges)) > 1
        for owners in owners_by_symbol.values()
    ):
        return True

    candidate_edges, _ = build_uuid_lineage_edges(evidence, candidates_only=True)
    return any(
        edge.edge_type == "name_change"
        and edge.predecessor_asset_id != edge.successor_asset_id
        and {edge.predecessor_asset_id, edge.successor_asset_id} <= asset_ids
        for edge in candidate_edges
    )


def can_promote_observation_decision(receipt):
    """Prove only the completed 30/56 or 31/57 transition to 31/58.

    The caller already owns a coherent selected group with unchanged inputs.
    This function performs no I/O and never makes an old handoff reader-compatible;
    the producer must publish a current receipt through its existing journal.
    """
    current = versions()
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # These are exact migration proofs, not a rolling compatibility exemption.
    if (current["lineage"], current["correlation"]) != (31, 58):
        return False
    published = receipt.get("versions")
    legacy = receipt.get("schema_version") == 30 and published == {
        **current,
        "lineage": 30,
        "correlation": 56,
    }
    prior = receipt.get("schema_version") == 31 and published == {
        **current,
        "correlation": 57,
    }
    if not legacy and not prior:
        return False

    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Inspect only this selected receipt's already loaded metadata, once during
    # migration. A possibly changed decision sends the COMPLETE group through
    # correlation; an unchanged decision needs no pricing decode, history scan or
    # new API work. Subsequent runs consume the current receipt and skip this proof.
    if legacy and not _cash_terminal_decision_unchanged(receipt):
        return False
    return not _correlation_58_decision_may_change(receipt)

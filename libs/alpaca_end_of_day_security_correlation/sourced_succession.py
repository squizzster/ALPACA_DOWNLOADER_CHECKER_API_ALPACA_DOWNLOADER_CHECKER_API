"""Independent, once-per-group certification of sourced parent substitutions.

The metadata producer owns submitted fact validation and exact lookup evidence.
This owner selects a conservative direct succession from those completed inputs;
it never requests evidence, reads prices, or represents a fact as a provider action.
Its immutable edge is the handoff to dated chronology and UUID membership.
"""

from datetime import date, timedelta
from decimal import InvalidOperation

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
)

from .action_identity import exact_cusip_lookup_asset
from .name_change import (
    _complete_action_query_covers,
    _unknown_occupancy_conflict,
    exact_same_symbol_endpoint_agreement,
)
from .occupancy_anchor import positive_occupancy_action_date
from .uuid_lineage import UuidLineageEdge


def _related(value, identifiers):
    if isinstance(value, dict):
        return any(_related(item, identifiers) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_related(item, identifiers) for item in value)
    return isinstance(value, str) and value in identifiers


def certify_sourced_successions(lineage):
    """Return completed direct certificates, bound to this immutable evidence.

    Only one sourced fact for a label is supported. Competing structural facts,
    additional occupants, incomplete mappings and contradictory dated dividends
    fail closed; no history comparison or source-document revisit is permitted.
    """
    document = lineage.get("sourced_successions")
    if document is None:
        return (), ()
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        return (), ({"reason_code": "unsupported_sourced_succession_evidence"},)
    facts = document.get("facts", ())
    accepted, issues = [], []
    for item in facts:
        match, fact = item["match"], item["facts"]
        symbol, source_id = match["symbol"], match["source_id"]
        reason = "sourced_succession_requires_unambiguous_direct_handoff"
        if sum(other["match"]["symbol"] == symbol for other in facts) != 1:
            issues.append({"reason_code": reason, "sourced_fact_id": source_id})
            continue
        legal = date.fromisoformat(fact["legal_effective_date"])
        boundary = date.fromisoformat(fact["exchange_effective_date"])
        old, new = fact["old_cusip"], fact["new_cusip"]
        predecessor = exact_cusip_lookup_asset(lineage, old)
        successor = exact_cusip_lookup_asset(lineage, new)
        edge = None
        if predecessor is not None and successor is not None:
            edge = UuidLineageEdge(
                edge_type="holding_company_succession",
                action_id=None,
                effective_date=boundary,
                predecessor_asset_id=predecessor["id"],
                predecessor_symbol=symbol,
                predecessor_cusip=old,
                successor_asset_id=successor["id"],
                successor_symbol=symbol,
                successor_cusip=new,
                source_fact_id=source_id,
                legal_effective_date=legal,
            )
        valid = (
            edge is not None
            and old != new
            and predecessor["id"] != successor["id"]
            and legal <= boundary
            and _complete_action_query_covers(lineage, symbol, legal)
            and _complete_action_query_covers(lineage, symbol, boundary)
            and exact_same_symbol_endpoint_agreement(lineage, edge, endpoints=(predecessor, successor))
            and not _unknown_occupancy_conflict(lineage, {symbol})
        )
        if valid:
            try:
                valid = expected_market_session_coverage(
                    boundary.isoformat(), (boundary + timedelta(days=1)).isoformat()
                ).session_dates == (boundary.isoformat(),)
            except MarketCalendarError:
                valid = False
        for action in lineage.get("provider_corporate_actions", lineage.get("corporate_actions", ())):
            if not valid:
                break
            if not _related(action, {symbol, old, new}):
                continue
            if action.get("action_type") not in {
                "cash_dividend",
                "capital_gain",
                "capital_gains_distribution",
            }:
                valid = False
                break
            if action.get("action_type") != "cash_dividend":
                continue
            cusip = action.get("cusip")
            # An unlabelled distribution supplies no dated endpoint ownership.
            # Retain it without converting missing evidence into a conflict.
            if action.get("symbol") == symbol and cusip in (None, ""):
                continue
            if action.get("symbol") != symbol or cusip not in {old, new}:
                valid = False
                break
            try:
                observed = positive_occupancy_action_date(action, symbol, cusip)
            except (KeyError, TypeError, ValueError, InvalidOperation):
                observed = None
            if observed is None or (cusip == old) != (observed < boundary):
                valid = False
        if valid:
            accepted.append(edge)
        else:
            issues.append({"reason_code": reason, "sourced_fact_id": source_id})
    return tuple(accepted), tuple(issues)

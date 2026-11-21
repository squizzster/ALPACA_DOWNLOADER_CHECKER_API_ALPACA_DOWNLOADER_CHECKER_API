"""Select a bounded later source using direct current-security evidence."""

from bisect import bisect_left, bisect_right
from datetime import date

from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .action_identity import exact_cusip_lookup_asset
from .name_change import (
    _action_dates,
    _complete_action_query_covers,
    _known_owners,
    _non_observation_conflict,
    _traded_price,
)
from .occupancy_anchor import positive_occupancy_action_date
from .terminal_episode import has_informative_trading, has_prolonged_carry_forward
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES


def _current_action(symbol, lineage, edges, current, actions):
    splits = [
        edge
        for edge in edges
        if edge.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
        and edge.predecessor_symbol == edge.successor_symbol == symbol
        and edge.successor_asset_id == current["id"]
    ]
    if len(splits) == 1:
        edge = splits[0]
        predecessor = (edge.predecessor_asset_id, edge.predecessor_cusip)
        return edge.effective_date, edge.action_id, edge.successor_cusip, predecessor
    if splits:
        return None

    dividends = {}
    for action in actions.values():
        cusip = action.get("cusip")
        if action.get("action_type") != "cash_dividend" or action.get("symbol") != symbol or not cusip:
            continue
        try:
            day = positive_occupancy_action_date(action, symbol, cusip)
        except (KeyError, TypeError, ValueError, ArithmeticError):
            continue
        if day is not None:
            dividends.setdefault(cusip, {})[day] = action["id"]
    current_dividends = [
        (cusip, dates)
        for cusip, dates in dividends.items()
        if (endpoint := exact_cusip_lookup_asset(lineage, cusip)) is not None
        and all(endpoint.get(key) == current.get(key) for key in ("id", "symbol", "exchange"))
    ]
    if len(current_dividends) != 1:
        return None
    cusip, dates = current_dividends[0]
    if len(dates) < 2:
        return None
    first = min(dates)
    return first, dates[first], cusip, None


def _action_before(action, day):
    dates = _action_dates({**action, "effective_date": action.get("ex_date") or action.get("effective_date")})
    return bool(dates and max(dates) < day)


def _older_issues(issues, actions, first):
    for issue in issues:
        direct = issue.get("action_id") or issue.get("corporate_action_id")
        references = (direct,) if direct else issue.get("action_ids", ())
        facts = [actions.get(identifier) for identifier in references] or [issue.get("action")]
        if not all(fact is not None and _action_before(fact, first) for fact in facts):
            return False
    return True


def _separate_unit_components(lineage, actions, symbol, current, first, overlapping):
    """A completed earlier unit action can distinguish co-traded units and warrants."""
    owners = _known_owners(lineage, symbol) | {current["id"]}
    separate = set()
    for action in actions.values():
        common_cusip = action.get("new_cusip")
        if (
            action.get("action_type") != "unit_split"
            or action.get("new_symbol") != symbol
            or not _action_before(action, first)
        ):
            continue
        try:
            if positive_occupancy_action_date(action, symbol, common_cusip) is None:
                continue
        except (KeyError, TypeError, ValueError, ArithmeticError):
            continue
        for role in ("old", "alternate"):
            label, cusip = action.get(f"{role}_symbol"), action.get(f"{role}_cusip")
            if label not in overlapping:
                continue
            endpoint = exact_cusip_lookup_asset(lineage, cusip)
            if (
                endpoint is not None
                and endpoint.get("symbol") == label
                and not (_known_owners(lineage, label) & owners)
                and endpoint["id"] not in owners
            ):
                separate.add(label)
    return separate


def current_anchor_bounds(
    symbol, lineage, source, sessions, current, edges, work_items, informative, evidence_issues, complete_sources
):
    """Date the later source without extending the evidence into the disputed prefix."""
    rivals = _known_owners(lineage, symbol) - {current["id"]}
    inactive = {item["id"] for item in lineage.get("identities", ()) if item.get("status") == "inactive"}
    if not rivals or not rivals <= inactive:
        return None
    actions = {action.get("id"): action for action in lineage.get("corporate_actions", ())}
    anchor = _current_action(symbol, lineage, edges, current, actions)
    if anchor is None:
        return None
    first, action_id, cusip, predecessor = anchor
    end = date.fromisoformat(source["request_to"][:10])
    if not all(_complete_action_query_covers(lineage, symbol, day) for day in (first, end)):
        return None
    position = bisect_left(sessions, first)
    if not 0 < position < len(sessions) or sessions[position] != first:
        return None

    # An undated or newer unresolved issue may apply to the selected span.
    if not _older_issues(evidence_issues, actions, first):
        return None
    owners = {(current["id"], cusip)}
    if predecessor is not None:
        for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
            for item in lineage.get(field, ()):
                if item.get("symbol") != symbol or (item.get("asset_id"), item.get("cusip")) != predecessor:
                    continue
                records = item.get("evidence") or ()
                if not records or any(
                    record.get("action_id") != action_id and not _action_before(record, first)
                    for record in records
                ):
                    return None
        owners.add(predecessor)
    if _non_observation_conflict(lineage, symbol, first, end, owners, {action_id}):
        return None
    overlapping = {
        name
        for name, work in complete_sources.items()
        if name != symbol
        for _, _, days in work
        if days and bisect_left(days, first) < bisect_right(days, sessions[-1])
    }
    if overlapping and overlapping != _separate_unit_components(
        lineage, actions, symbol, current, first, overlapping
    ):
        return None
    raw = next(download.rows for download, item, _ in work_items if item["adjustment"] == "raw")
    if not (_traded_price(raw[position]) and _traded_price(raw[-1])) or (
        not informative
        and (not has_informative_trading(raw[position:]) or has_prolonged_carry_forward(raw[position:]))
    ):
        return None
    expected = expected_market_session_coverage(first.isoformat(), source["request_to"])
    if tuple(day.isoformat() for day in sessions[position:]) != expected.session_dates:
        return None
    return first.isoformat(), sessions[-1].isoformat(), cusip, action_id

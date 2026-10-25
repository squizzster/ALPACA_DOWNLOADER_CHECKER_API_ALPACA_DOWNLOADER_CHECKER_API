"""Dated rename candidates and bounded, independent source evidence.

The action supplies endpoints. Own-label observations supply historical bounds;
the action's UUID component must never be used to authenticate those observations.
All inputs are the selected group's in-memory pricing handoffs.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import InvalidOperation
from itertools import groupby
from typing import TYPE_CHECKING

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import MarketCalendarError, expected_market_session_coverage

from .occupancy_anchor import positive_occupancy_action_date

if TYPE_CHECKING:
    from .ownership_interval_rules import TickerOwnershipInterval


def name_change_dates(action, actions=()):
    """Collect endpoint dates without silently preferring one conflicting date."""
    values = {key: action.get(key) for key in ("effective_date", "process_date") if action.get(key) is not None}
    try:
        dates = {date.fromisoformat(value) for value in values.values()}
    except (TypeError, ValueError):
        return (), "invalid_name_change_boundary_date"
    if not dates:
        return (), "invalid_name_change_boundary_date"
    # A component-separation event positively dates the arriving component.
    # It does not date any preceding rows, but can contradict a late rename.
    for other in actions:
        if other.get("action_type") != "unit_split":
            continue
        try:
            effective = positive_occupancy_action_date(other, action.get("new_symbol"), action.get("new_cusip"))
        except (KeyError, TypeError, ValueError, InvalidOperation):
            continue
        if effective is not None:
            dates.add(effective)
    if len(dates) != 1:
        return tuple(sorted(dates)), "conflicting_name_change_boundary_dates"
    return tuple(dates), None


def name_change_date(action, actions=()):
    dates, issue = name_change_dates(action, actions)
    return (None if issue else dates[0]), issue


@dataclass(frozen=True)
class NameChangeHistory:
    symbol: str
    first: date
    last: date
    interval: "TickerOwnershipInterval"
    action_id: str


@dataclass(frozen=True)
class NameChangeAssessment:
    accepted: frozenset[str] = frozenset()
    histories: tuple[NameChangeHistory, ...] = ()
    independent_successors: frozenset[str] = frozenset()


def _grid(work):
    if len(work) != len(ADJUSTMENTS):
        return None
    views = {source["adjustment"]: (download.rows, source, sessions) for download, source, sessions in work}
    if set(views) != set(ADJUSTMENTS):
        return None
    rows, source, sessions = views["raw"]
    if not sessions or any(
        grid != sessions or (context["feed"], context["timeframe"]) != (source["feed"], source["timeframe"])
        for _, context, grid in views.values()
    ):
        return None
    return rows, source, sessions


def _known_owners(lineage, symbol):
    return (
        {
            item.get("asset_id")
            for item in lineage.get("symbol_occupancy_evidence", [])
            if item.get("symbol") == symbol and item.get("asset_id")
        }
        | {
            item.get("id")
            for item in lineage.get("identities", [])
            if item.get("symbol") == symbol and item.get("id")
        }
        | {
            item["asset"].get("id")
            for item in lineage.get("asset_lookups", [])
            if isinstance(item.get("asset"), dict)
            and item["asset"].get("id")
            and item["asset"].get("symbol") == symbol
        }
    )


def _independent_members(asset_id, edges):
    members = {asset_id}
    while True:
        previous = members.copy()
        for edge in edges:
            if edge.edge_type != "name_change" and members.intersection(
                (edge.predecessor_asset_id, edge.successor_asset_id)
            ):
                members.update((edge.predecessor_asset_id, edge.successor_asset_id))
        if members == previous:
            return members


def _own_interval(lineage, intervals, edge, sessions):
    matches = [
        item
        for item in intervals
        if item.identity_key() == (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
        and item.end_date_exclusive == edge.effective_date
    ]
    if len(matches) != 1:
        return None
    owner = matches[0]
    occupied = [item for item in intervals if item.historical_symbol == edge.predecessor_symbol]
    # An unbounded claim cannot defeat another, undated occupant of the label.
    if edge.predecessor_asset_id != edge.successor_asset_id and _known_owners(
        lineage, edge.predecessor_symbol
    ) - {item.asset_id for item in occupied}:
        return None
    selected = [day for day in sessions if owner.contains(day)]
    if not selected or any(
        other.asset_id != owner.asset_id and any(other.contains(day) for day in selected) for other in occupied
    ):
        return None
    return owner


def assess_name_change_history(lineage, intervals, candidates, sources):
    """Certify source bounds without reading reports produced by these edges."""
    # Import here to keep the ownership/edge builders independent of price rules.
    from .terminal_episode import has_informative_trading, has_prolonged_carry_forward

    fields = ("t", "o", "h", "l", "c", "v", "n", "vw")
    grids = {symbol: _grid(work) for symbol, work in sources.items()}
    accepted, histories, independent = set(), [], set()
    for edge in candidates:
        if edge.edge_type != "name_change" or edge.predecessor_symbol == edge.successor_symbol:
            continue
        old, new = grids.get(edge.predecessor_symbol), grids.get(edge.successor_symbol)
        if old is None or new is None:
            continue
        old_rows, old_source, old_sessions = old
        new_rows, new_source, new_sessions = new
        if (old_source["feed"], old_source["timeframe"]) != (new_source["feed"], new_source["timeframe"]):
            continue
        owner = _own_interval(lineage, intervals, edge, old_sessions)
        if owner is None:
            continue
        prefix = [(day, row) for day, row in zip(old_sessions, old_rows) if owner.contains(day)]
        if not prefix:
            continue
        first, last = prefix[0][0], prefix[-1][0]
        try:
            expected = expected_market_session_coverage(
                first.isoformat(), (last + timedelta(days=1)).isoformat()
            )
            tail = expected_market_session_coverage(last.isoformat(), edge.effective_date.isoformat())
        except MarketCalendarError:
            continue
        if tuple(day.isoformat() for day, _ in prefix) != expected.session_dates:
            continue
        own_rows = [row for _, row in prefix]
        if not has_informative_trading(own_rows) or has_prolonged_carry_forward(own_rows):
            continue
        other = [(day, row) for day, row in zip(new_sessions, new_rows) if first <= day <= last]
        if [day for day, _ in other] != [day for day, _ in prefix]:
            continue
        equal = [
            all(left.get(key) is not None and left.get(key) == right.get(key) for key in fields)
            for (_, left), (_, right) in zip(prefix, other)
        ]
        same_uuid = edge.predecessor_asset_id == edge.successor_asset_id
        # A shared UUID already establishes identity, but not the label's dates.
        # Its independent own-label source bounds the interval, and an exact,
        # informative suffix connects the two sources. Earlier price revisions
        # cannot invalidate that connection or extend its observed lower bound.
        suffix_start = max((index + 1 for index, match in enumerate(equal) if not match), default=0)
        matching_suffix = own_rows[suffix_start:]
        reaches_boundary = tail.session_dates == (last.isoformat(),)
        proven = (
            same_uuid
            and has_informative_trading(matching_suffix)
            or not same_uuid
            and all(equal)
            and reaches_boundary
        )
        if proven:
            accepted.add(edge.action_id)
            spans = [(first, last)]
        else:
            # Exact observed spans can establish row provenance even when the
            # full handoff cannot certify a UUID join. Keep gaps unresolved.
            spans = []
            for matches, run in groupby(enumerate(equal), key=lambda item: item[1]):
                indexes = [index for index, _ in run]
                if matches and has_informative_trading(own_rows[indexes[0] : indexes[-1] + 1]):
                    spans.append((prefix[indexes[0]][0], prefix[indexes[-1]][0]))
        for start, end in spans:
            for symbol in (edge.predecessor_symbol, edge.successor_symbol):
                histories.append(NameChangeHistory(symbol, start, end, owner, edge.action_id))
        if not same_uuid and not any(equal) and has_informative_trading([row for _, row in other]):
            # Both sources have independent own-label observations on the same
            # dates. The relationship cannot erase the destination's history.
            if _known_owners(lineage, edge.successor_symbol) <= _independent_members(
                edge.successor_asset_id, candidates
            ):
                independent.add(edge.action_id)
    return NameChangeAssessment(frozenset(accepted), tuple(histories), frozenset(independent))

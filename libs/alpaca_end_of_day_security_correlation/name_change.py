"""Dated rename candidates and bounded, independent source evidence.

The action supplies endpoints. Own-label observations supply historical bounds;
the action's UUID component must never be used to authenticate those observations.
All inputs are the selected group's in-memory pricing handoffs.
"""

from dataclasses import dataclass, replace
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from itertools import groupby
from typing import TYPE_CHECKING

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import MarketCalendarError, expected_market_session_coverage

from .action_identity import exact_cusip_lookup_asset, exact_symbol_lookup_asset
from .occupancy_anchor import positive_occupancy_action_date

if TYPE_CHECKING:
    from .ownership_interval_rules import TickerOwnershipInterval


EXACT_SAME_SYMBOL_HANDOFF_CORRELATION_VERSION = 61
BOUNDED_ENTRY_SAME_SYMBOL_HANDOFF_CORRELATION_VERSION = 62
PARTIAL_SHARED_UUID_SUFFIX_CORRELATION_VERSION = 63
SINGLE_SESSION_SHARED_UUID_HISTORY_CORRELATION_VERSION = 64
EARLY_SUCCESSOR_SOURCE_BRIDGE_CORRELATION_VERSION = 65
TERMINAL_CARRY_FORWARD_EXCLUSION_CORRELATION_VERSION = 66
POST_PADDING_SHARED_UUID_SUFFIX_CORRELATION_VERSION = 67
SPLIT_ADJUSTED_POST_PADDING_ALIAS_CORRELATION_VERSION = 68
PRE_ENTRY_AND_COMPONENT_HANDOFF_CORRELATION_VERSION = 70
EXTENDED_COMPONENT_HANDOFF_CORRELATION_VERSION = 71
INTERNAL_COMPONENT_HANDOFF_CORRELATION_VERSION = 74
COMPLETE_CERTIFICATE_CONFLICT_CORRELATION_VERSION = 77
COMPLETED_EVENT_SCOPE_CORRELATION_VERSION = 78

PRE_ENTRY_CARRY_FORWARD_REASON = "certified_pre_entry_carry_forward_non_observation"


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
class TerminalCarryForwardExclusion:
    symbol: str
    first: date
    last: date
    asset_id: str
    cusip: str
    action_ids: tuple[str, ...]


@dataclass(frozen=True)
class PreEntryCarryForwardExclusion:
    symbol: str
    first: date
    last: date
    action_id: str


@dataclass(frozen=True)
class ComponentSplitAnchor:
    action_id: str
    role: str
    cusip: str
    asset_id: str | None


@dataclass(frozen=True)
class NameChangeAssessment:
    accepted: frozenset[str] = frozenset()
    histories: tuple[NameChangeHistory, ...] = ()
    independent_successors: frozenset[str] = frozenset()
    terminal_carry_forward_exclusions: tuple[TerminalCarryForwardExclusion, ...] = ()
    pre_entry_carry_forward_exclusions: tuple[PreEntryCarryForwardExclusion, ...] = ()


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
            item.get("asset_id")
            for item in lineage.get("input_symbol_occupants", [])
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


def _dated_owners(lineage, symbol):
    """Return only owners placed on a label by a dated producer record."""
    return {
        item.get("asset_id")
        for field in ("symbol_occupancy_evidence", "input_symbol_occupants")
        for item in lineage.get(field, ())
        if item.get("symbol") == symbol and item.get("asset_id")
    }


def action_labels(value):
    """Structural evidence includes nested reorganization recipients."""
    if isinstance(value, list):
        return set().union(*(action_labels(item) for item in value))
    if not isinstance(value, dict):
        return set()
    result = {
        item
        for key, item in value.items()
        if (key == "symbol" or key.endswith("_symbol")) and isinstance(item, str) and item
    }
    for item in value.values():
        if isinstance(item, (dict, list)):
            result.update(action_labels(item))
    return result


def _unknown_occupancy_conflict(lineage, symbols, first=None, last=None, *, allowed=()):
    """An unresolved positive occupant is not evidence that a label is vacant.

    Only this certificate's label/date scope is tested. A validated component
    anchor can authorize its exact unresolved component evidence, not another
    distribution for that CUSIP or an unrelated unknown occupant.
    """
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        for occupant in lineage.get(field, ()):
            if occupant.get("symbol") not in symbols or occupant.get("asset_id"):
                continue
            records = occupant.get("evidence") or ()
            if not records:
                return True
            for record in records:
                if (occupant.get("symbol"), occupant.get("cusip"), record.get("action_id")) in allowed:
                    continue
                dates = _action_dates(record)
                if not dates or any(
                    (first is None or first <= day) and (last is None or day <= last) for day in dates
                ):
                    return True
    return False


def _bridge_structural_conflict(lineage, symbols, first, last, action_id):
    structural = {
        "cash_merger",
        "forward_split",
        "name_change",
        "reorganization",
        "reverse_split",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
        "unit_split",
    }
    for action in lineage.get("corporate_actions", ()):
        if action.get("id") == action_id or action.get("action_type") not in structural:
            continue
        if not action_labels(action).intersection(symbols):
            continue
        dates = _action_dates(action)
        if not dates or any(first <= day <= last for day in dates):
            return True
    return False


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


def _is_unique_direct_edge(candidates, edge):
    symbols = {edge.predecessor_symbol, edge.successor_symbol}
    related = [
        candidate
        for candidate in candidates
        if candidate.predecessor_symbol in symbols or candidate.successor_symbol in symbols
    ]
    return related == [edge]


def _complete_action_query_covers(lineage, symbol, effective):
    query = lineage.get("query")
    if (
        not isinstance(query, dict)
        or (query.get("input_symbol") != symbol and symbol not in lineage.get("queried_symbols", ()))
        or query.get("corporate_actions_data_quality") != "complete"
    ):
        return False
    try:
        start = date.fromisoformat(query["corporate_actions_start"])
        end = date.fromisoformat(query["corporate_actions_end"])
    except (KeyError, TypeError, ValueError):
        return False
    return start <= effective <= end


def _rows_equal(left, right, fields):
    return len(left) == len(right) and all(
        all(a.get(field) is not None and a.get(field) == b.get(field) for field in fields)
        for a, b in zip(left, right)
    )


def _positive_decimal(value):
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _terminal_carry_value(row):
    try:
        prices = tuple(Decimal(str(row[key])) for key in ("o", "h", "l", "c"))
        volume = Decimal(str(row["v"]))
        trades = Decimal(str(row["n"]))
        vwap = Decimal(str(row["vw"]))
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return None
    if (
        all(value.is_finite() for value in (*prices, volume, trades, vwap))
        and len(set(prices)) == 1
        and volume == trades == vwap == 0
    ):
        return prices[0]
    return None


def _traded_price(row):
    try:
        prices = tuple(Decimal(str(row[key])) for key in ("o", "h", "l", "c"))
        volume = Decimal(str(row["v"]))
        trades = Decimal(str(row["n"]))
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return None
    if all(value.is_finite() for value in (*prices, volume, trades)) and volume > 0 and trades > 0:
        return prices
    return None


def _action_dates(action):
    values = [action.get(key) for key in ("effective_date", "process_date", "payable_date")]
    try:
        return {date.fromisoformat(value) for value in values if value is not None}
    except (TypeError, ValueError):
        return set()


def _non_observation_conflict(lineage, symbol, first, last, allowed_owners, allowed_actions):
    """New certificate scope must not contain an unaccounted dated occupant.

    Unknown UUIDs are unresolved evidence, not evidence of absence. This check
    belongs to the non-observation producer; serving trusts its disposition.
    """
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        for occupant in lineage.get(field, ()):
            if (
                occupant.get("symbol") != symbol
                or (occupant.get("asset_id"), occupant.get("cusip")) in allowed_owners
            ):
                continue
            records = occupant.get("evidence") or ()
            if not records:
                return True
            for record in records:
                dates = _action_dates(record)
                if not dates or any(first <= day <= last for day in dates):
                    return True
    for action in lineage.get("corporate_actions", ()):
        if action.get("id") in allowed_actions or not any(
            value == symbol for key, value in action.items() if key == "symbol" or key.endswith("_symbol")
        ):
            continue
        # Distribution evidence for the already identified owner is compatible;
        # unknown CUSIPs and every additional structural event need allocation.
        distribution = action.get("action_type") in {"cash_dividend", "capital_gain"}
        if distribution and any(cusip == action.get("cusip") for _, cusip in allowed_owners):
            continue
        dates = _action_dates(
            {**action, "effective_date": action.get("ex_date") or action.get("effective_date")}
        )
        if not dates or any(first <= day <= last for day in dates):
            return True
    return False


def _terminal_carry_forward_exclusion(
    lineage,
    intervals,
    candidates,
    sources,
    grids,
    edge,
    owner,
):
    """Identify a terminal non-trading pad without assigning its rows.

    The name-change edge dates the provider's continuing series. A separate,
    exact stock-merger acquiree identifies the retired issue whose final close
    was carried forward. The completed source group proves only that the suffix
    is non-trading padding; row routing retains it as explicitly uncorrelated.
    """
    if (
        edge.predecessor_symbol == edge.successor_symbol
        or edge.predecessor_asset_id != edge.successor_asset_id
        or edge.predecessor_cusip != edge.successor_cusip
        or not _complete_action_query_covers(lineage, edge.predecessor_symbol, edge.effective_date)
    ):
        return None
    # A returning/reused predecessor label is not a terminal pad candidate.
    if any(
        candidate != edge
        and edge.predecessor_symbol in (candidate.predecessor_symbol, candidate.successor_symbol)
        for candidate in candidates
    ):
        return None

    mergers = [
        action
        for action in lineage.get("corporate_actions", ())
        if isinstance(action, dict)
        and action.get("action_type") == "stock_merger"
        and action.get("acquiree_symbol") == edge.predecessor_symbol
        and action.get("acquirer_symbol") == edge.successor_symbol
        and action.get("acquirer_cusip") == edge.successor_cusip
        and action.get("acquiree_cusip") != edge.successor_cusip
        and _action_dates(action) == {edge.effective_date}
        and isinstance(action.get("id"), str)
        and action["id"]
        and _positive_decimal(action.get("acquiree_rate")) is not None
        and _positive_decimal(action.get("acquirer_rate")) is not None
    ]
    if len(mergers) != 1:
        return None
    merger = mergers[0]
    acquiree_cusip = merger["acquiree_cusip"]
    acquiree = exact_cusip_lookup_asset(lineage, acquiree_cusip)
    current_old = exact_symbol_lookup_asset(lineage, edge.predecessor_symbol)
    successor = exact_cusip_lookup_asset(lineage, edge.successor_cusip)
    if (
        acquiree is None
        or current_old is None
        or successor is None
        or acquiree.get("id") == edge.successor_asset_id
        or acquiree.get("id") != current_old.get("id")
        or acquiree.get("symbol") != edge.predecessor_symbol
        or acquiree.get("class") != "us_equity"
        or acquiree.get("status") != "inactive"
        or acquiree.get("tradable") is not False
        or acquiree.get("exchange") in (None, "", "OTC")
        or successor.get("id") != edge.successor_asset_id
        or successor.get("class") != "us_equity"
        or current_old.get("symbol") != edge.predecessor_symbol
        or current_old.get("status") != "inactive"
        or current_old.get("tradable") is not False
        or current_old.get("exchange") != acquiree.get("exchange")
        or _known_owners(lineage, edge.predecessor_symbol) != {acquiree["id"], edge.successor_asset_id}
        or _known_owners(lineage, edge.successor_symbol) != {edge.successor_asset_id}
    ):
        return None

    own_intervals = [interval for interval in intervals if interval.historical_symbol == edge.predecessor_symbol]
    successor_intervals = [
        interval for interval in intervals if interval.historical_symbol == edge.successor_symbol
    ]
    if own_intervals != [owner] or len(successor_intervals) != 1:
        return None
    successor_interval = successor_intervals[0]
    if (
        successor_interval.identity_key()
        != (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        or successor_interval.start_date_inclusive != edge.effective_date
    ):
        return None

    # Other replacement events can change what the flat rows mean.
    structural = {
        "cash_merger",
        "stock_merger",
        "stock_and_cash_merger",
        "spin_off",
        "reorganization",
    }
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or action.get("id") in {edge.action_id, merger["id"]}:
            continue
        affected = {
            action.get(key)
            for key in ("symbol", "old_symbol", "new_symbol", "acquiree_symbol", "acquirer_symbol")
        }
        if action.get("action_type") in structural and affected.intersection(
            {edge.predecessor_symbol, edge.successor_symbol}
        ):
            return None

    old = grids.get(edge.predecessor_symbol)
    new = grids.get(edge.successor_symbol)
    old_work = sources.get(edge.predecessor_symbol)
    if old is None or new is None or old_work is None:
        return None
    _, old_source, old_sessions = old
    new_rows, new_source, new_sessions = new
    if (old_source["feed"], old_source["timeframe"]) != (
        new_source["feed"],
        new_source["timeframe"],
    ) or edge.effective_date not in old_sessions:
        return None
    suffix_start = old_sessions.index(edge.effective_date)
    if suffix_start == 0:
        return None
    suffix_sessions = old_sessions[suffix_start:]
    if _non_observation_conflict(
        lineage,
        edge.predecessor_symbol,
        suffix_sessions[0],
        suffix_sessions[-1],
        {(acquiree["id"], acquiree_cusip), (edge.predecessor_asset_id, edge.predecessor_cusip)},
        {edge.action_id, merger["id"]},
    ):
        return None
    if any(not successor_interval.contains(day) for day in suffix_sessions):
        return None
    try:
        expected_suffix = expected_market_session_coverage(
            suffix_sessions[0].isoformat(),
            (suffix_sessions[-1] + timedelta(days=1)).isoformat(),
        )
        handoff = expected_market_session_coverage(
            old_sessions[suffix_start - 1].isoformat(),
            (suffix_sessions[0] + timedelta(days=1)).isoformat(),
        )
    except MarketCalendarError:
        return None
    if expected_suffix.session_dates != tuple(
        day.isoformat() for day in suffix_sessions
    ) or handoff.session_dates != (old_sessions[suffix_start - 1].isoformat(), suffix_sessions[0].isoformat()):
        return None

    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in old_work}
    if len(views) != len(old_work) or set(views) != set(ADJUSTMENTS):
        return None
    for values, sessions in views.values():
        try:
            carried_close = Decimal(str(values[suffix_start - 1]["c"]))
        except (InvalidOperation, KeyError, TypeError, ValueError, IndexError):
            return None
        if (
            sessions != old_sessions
            or not carried_close.is_finite()
            or _traded_price(values[suffix_start - 1]) is None
            or any(_terminal_carry_value(row) != carried_close for row in values[suffix_start:])
        ):
            return None
    predecessor_trades = [_traded_price(row) for row in views["raw"][0][:suffix_start]]
    if len({value for value in predecessor_trades if value is not None}) < 2:
        return None

    suffix_set = set(suffix_sessions)
    successor_rows = {day: row for day, row in zip(new_sessions, new_rows) if day in suffix_set}
    traded = [_traded_price(successor_rows.get(day, {})) for day in suffix_sessions]
    if None in traded or len(set(traded)) < 2:
        return None
    return TerminalCarryForwardExclusion(
        edge.predecessor_symbol,
        suffix_sessions[0],
        suffix_sessions[-1],
        acquiree["id"],
        acquiree_cusip,
        (edge.action_id, merger["id"]),
    )


def _pre_entry_carry_forward_exclusion(
    lineage,
    intervals,
    candidates,
    sources,
    grids,
    edge,
    owner,
    has_informative_trading,
    has_prolonged_carry_forward,
):
    """Certify only the inert tail before a proved different-label entry.

    The independently owned predecessor source overlaps the destination-label
    carry-forward and reaches the accepted rename boundary.  That proves the
    tail is not an observation of the arriving identity; it does not identify
    the retired issue or assign any of its rows.
    """
    if (
        edge.predecessor_symbol == edge.successor_symbol
        or edge.predecessor_asset_id != edge.successor_asset_id
        or edge.predecessor_cusip != edge.successor_cusip
        or not _is_unique_direct_edge(candidates, edge)
        or not _complete_action_query_covers(lineage, edge.successor_symbol, edge.effective_date)
        or _known_owners(lineage, edge.successor_symbol) != {edge.successor_asset_id}
    ):
        return None
    endpoint = exact_cusip_lookup_asset(lineage, edge.successor_cusip)
    old = grids.get(edge.predecessor_symbol)
    new = grids.get(edge.successor_symbol)
    new_work = sources.get(edge.successor_symbol)
    if (
        endpoint is None
        or endpoint.get("id") != edge.successor_asset_id
        or endpoint.get("class") != "us_equity"
        or old is None
        or new is None
        or new_work is None
    ):
        return None
    old_rows, old_source, old_sessions = old
    new_rows, new_source, new_sessions = new
    if (
        not old_sessions
        or not new_sessions
        or edge.effective_date not in new_sessions
        or (old_source["feed"], old_source["timeframe"]) != (new_source["feed"], new_source["timeframe"])
        or any(not owner.contains(day) for day in old_sessions)
    ):
        return None
    successor_intervals = [item for item in intervals if item.historical_symbol == edge.successor_symbol]
    if len(successor_intervals) != 1:
        return None
    successor = successor_intervals[0]
    if (
        successor.identity_key() != (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        or successor.start_date_inclusive != edge.effective_date
    ):
        return None
    entry = new_sessions.index(edge.effective_date)
    if entry == 0:
        return None
    views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in new_work}
    if len(views) != len(new_work) or set(views) != set(ADJUSTMENTS):
        return None
    start = entry
    while start and all(_terminal_carry_value(values[start - 1]) is not None for values, _ in views.values()):
        start -= 1
    if entry - start < 100 or start == 0:
        return None
    tail_sessions = new_sessions[start:entry]
    if _non_observation_conflict(
        lineage,
        edge.successor_symbol,
        tail_sessions[0],
        tail_sessions[-1],
        {(edge.successor_asset_id, edge.successor_cusip)},
        {edge.action_id},
    ):
        return None
    try:
        tail_grid = expected_market_session_coverage(
            tail_sessions[0].isoformat(), edge.effective_date.isoformat()
        )
        handoff = expected_market_session_coverage(
            old_sessions[-1].isoformat(), (edge.effective_date + timedelta(days=1)).isoformat()
        )
    except MarketCalendarError:
        return None
    if (
        tail_grid.session_dates != tuple(day.isoformat() for day in tail_sessions)
        or handoff.session_dates != (old_sessions[-1].isoformat(), edge.effective_date.isoformat())
        or not tail_sessions[0] <= old_sessions[0]
        or tuple(day for day in new_sessions if old_sessions[0] <= day <= old_sessions[-1])
        != tuple(old_sessions)
    ):
        return None
    for values, sessions in views.values():
        if sessions != new_sessions:
            return None
        try:
            carried = Decimal(str(values[start - 1]["c"]))
        except (InvalidOperation, KeyError, TypeError, ValueError, IndexError):
            return None
        if (
            not carried.is_finite()
            or _traded_price(values[start - 1]) is None
            or any(_terminal_carry_value(row) != carried for row in values[start:entry])
        ):
            return None
    old_views = {source["adjustment"]: download.rows for download, source, _ in sources[edge.predecessor_symbol]}
    if (
        set(old_views) != set(ADJUSTMENTS)
        or any(
            not has_informative_trading(values) or has_prolonged_carry_forward(values)
            for values in old_views.values()
        )
        or not has_informative_trading(new_rows[:start])
        or has_prolonged_carry_forward(new_rows[:start])
        or not has_informative_trading(new_rows[entry:])
        or has_prolonged_carry_forward(new_rows[entry:])
    ):
        return None
    return PreEntryCarryForwardExclusion(
        edge.successor_symbol,
        tail_sessions[0],
        tail_sessions[-1],
        edge.action_id,
    )


def _component_split_anchor(lineage, edge, first, *, extended_component_handoffs):
    matches = []
    for action in lineage.get("corporate_actions", ()):
        if (
            not isinstance(action, dict)
            or action.get("action_type") != "unit_split"
            or action.get("effective_date") != first.isoformat()
            or action.get("process_date") != first.isoformat()
            or not isinstance(action.get("id"), str)
            or not action["id"]
            or any(
                not isinstance(action.get(key), str) or not action[key]
                for key in (
                    "old_symbol",
                    "old_cusip",
                    "new_symbol",
                    "new_cusip",
                    "alternate_symbol",
                    "alternate_cusip",
                )
            )
            or len({action["old_symbol"], action["new_symbol"], action["alternate_symbol"]}) != 3
            or len({action["old_cusip"], action["new_cusip"], action["alternate_cusip"]}) != 3
            or any(
                _positive_decimal(action.get(key)) is None for key in ("old_rate", "new_rate", "alternate_rate")
            )
        ):
            continue
        roles = (
            tuple(role for role in ("new", "alternate") if action[f"{role}_symbol"] == edge.predecessor_symbol)
            if extended_component_handoffs
            else (("alternate",) if action["alternate_symbol"] == edge.predecessor_symbol else ())
        )
        if len(roles) != 1:
            continue
        role = roles[0]
        component_cusip = action[f"{role}_cusip"]
        if component_cusip == edge.predecessor_cusip:
            continue
        occupancy = [
            item
            for item in lineage.get("symbol_occupancy_evidence", ())
            if item.get("symbol") == edge.predecessor_symbol
            and item.get("cusip") == component_cusip
            and any(
                record.get("action_id") == action["id"]
                and record.get("action_type") == "unit_split"
                and record.get("role") == role
                and record.get("process_date") == first.isoformat()
                for record in item.get("evidence", ())
                if isinstance(record, dict)
            )
        ]
        if len(occupancy) != 1:
            continue
        asset_id = occupancy[0].get("asset_id")
        component = exact_cusip_lookup_asset(lineage, component_cusip)
        if asset_id is None:
            if component is not None:
                continue
        elif (
            not isinstance(asset_id, str)
            or not asset_id
            or component is None
            or component.get("id") != asset_id
            or component.get("symbol") != edge.predecessor_symbol
        ):
            continue
        matches.append(ComponentSplitAnchor(action["id"], role, component_cusip, asset_id))
    return matches[0] if len(matches) == 1 else None


def _later_terminal_component_exit(lineage, action, edge, successor, new_sessions):
    if (
        action.get("action_type") != "cash_merger"
        or not isinstance(action.get("id"), str)
        or not action["id"]
        or action.get("acquiree_symbol") != edge.successor_symbol
        or action.get("acquiree_cusip") != edge.successor_cusip
        or _positive_decimal(action.get("rate")) is None
    ):
        return False
    dates = _action_dates(action)
    if len(dates) != 1:
        return False
    terminal = next(iter(dates))
    if (
        terminal <= edge.effective_date
        or successor.end_date_exclusive != terminal
        or not _complete_action_query_covers(lineage, edge.successor_symbol, terminal)
    ):
        return False
    try:
        adjacent = expected_market_session_coverage(
            new_sessions[-1].isoformat(), (terminal + timedelta(days=1)).isoformat()
        )
    except MarketCalendarError:
        return False
    return adjacent.session_dates == (new_sessions[-1].isoformat(), terminal.isoformat())


def _component_anchored_early_successor_history(
    lineage,
    intervals,
    candidates,
    grids,
    edge,
    owner,
    has_informative_trading,
    has_prolonged_carry_forward,
    extended_component_handoffs,
    complete_certificate_conflicts=True,
):
    """Date an observed component rename prefix from its structural event.

    The retained provider rename date remains unchanged.  The certificate owns
    only the complete successor-label sessions between an exact adjacent source
    handoff and the independently dated component-separation action.
    """
    if (
        edge.predecessor_symbol == edge.successor_symbol
        or edge.predecessor_asset_id != edge.successor_asset_id
        or edge.predecessor_cusip != edge.successor_cusip
        or not _complete_action_query_covers(lineage, edge.successor_symbol, edge.effective_date)
        or _known_owners(lineage, edge.successor_symbol) != {edge.successor_asset_id}
    ):
        return None
    old = grids.get(edge.predecessor_symbol)
    new = grids.get(edge.successor_symbol)
    endpoint = exact_cusip_lookup_asset(lineage, edge.successor_cusip)
    if (
        old is None
        or new is None
        or endpoint is None
        or endpoint.get("id") != edge.successor_asset_id
        or endpoint.get("class") != "us_equity"
    ):
        return None
    old_rows, old_source, old_sessions = old
    new_rows, new_source, new_sessions = new
    if (
        not old_sessions
        or not new_sessions
        or not new_sessions[0] < edge.effective_date
        or edge.effective_date not in new_sessions
        or (old_source["feed"], old_source["timeframe"]) != (new_source["feed"], new_source["timeframe"])
        or any(not owner.contains(day) for day in old_sessions)
    ):
        return None
    successor_intervals = [item for item in intervals if item.historical_symbol == edge.successor_symbol]
    if len(successor_intervals) != 1:
        return None
    successor = successor_intervals[0]
    if (
        successor.identity_key() != (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        or successor.start_date_inclusive != edge.effective_date
    ):
        return None
    early_count = new_sessions.index(edge.effective_date)
    early_sessions = new_sessions[:early_count]
    if not early_sessions or any(
        successor.end_date_exclusive is not None and day >= successor.end_date_exclusive for day in new_sessions
    ):
        return None
    try:
        handoff = expected_market_session_coverage(
            old_sessions[-1].isoformat(), (new_sessions[0] + timedelta(days=1)).isoformat()
        )
        early = expected_market_session_coverage(new_sessions[0].isoformat(), edge.effective_date.isoformat())
    except MarketCalendarError:
        return None
    if (
        handoff.session_dates != (old_sessions[-1].isoformat(), new_sessions[0].isoformat())
        or early.session_dates != tuple(day.isoformat() for day in early_sessions)
        or not has_informative_trading(old_rows)
        or (
            has_prolonged_carry_forward(old_rows)
            and (
                not extended_component_handoffs
                or len(old_rows) < 5
                or any(_traded_price(row) is None for row in old_rows[-5:])
                or len({_traded_price(row) for row in old_rows[-5:]}) < 2
            )
        )
        or not has_informative_trading(new_rows[:early_count])
        or has_prolonged_carry_forward(new_rows[:early_count])
    ):
        return None
    anchor = _component_split_anchor(
        lineage,
        edge,
        new_sessions[0],
        extended_component_handoffs=extended_component_handoffs,
    )
    if anchor is None or not _complete_action_query_covers(lineage, edge.successor_symbol, new_sessions[0]):
        return None
    predecessor_owners = (
        _dated_owners(lineage, edge.predecessor_symbol)
        if extended_component_handoffs
        else _known_owners(lineage, edge.predecessor_symbol)
    )
    expected_predecessor_owners = {edge.predecessor_asset_id}
    if extended_component_handoffs and anchor.asset_id is not None:
        expected_predecessor_owners.add(anchor.asset_id)
    if predecessor_owners != expected_predecessor_owners:
        return None
    for candidate in candidates:
        if candidate == edge:
            continue
        affected = {candidate.predecessor_symbol, candidate.successor_symbol}
        if not affected.intersection({edge.predecessor_symbol, edge.successor_symbol}):
            continue
        later_linear_exit = (
            candidate.edge_type == "name_change"
            and candidate.predecessor_symbol == edge.successor_symbol
            and candidate.predecessor_asset_id == candidate.successor_asset_id == edge.successor_asset_id
            and candidate.predecessor_cusip == candidate.successor_cusip == edge.successor_cusip
            and candidate.effective_date > edge.effective_date
        )
        if not later_linear_exit:
            return None
    structural = {
        "cash_merger",
        "forward_split",
        "name_change",
        "reorganization",
        "reverse_split",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
        "unit_split",
    }
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or action.get("id") in {edge.action_id, anchor.action_id}:
            continue
        affected = {
            action.get(key)
            for key in (
                "symbol",
                "old_symbol",
                "new_symbol",
                "alternate_symbol",
                "acquiree_symbol",
                "acquirer_symbol",
            )
        }
        if action.get("action_type") not in structural or not affected.intersection(
            {edge.predecessor_symbol, edge.successor_symbol}
        ):
            continue
        dates = _action_dates(action)
        later_linear_exit = (
            action.get("action_type") == "name_change"
            and action.get("old_symbol") == edge.successor_symbol
            and action.get("old_cusip") == action.get("new_cusip") == edge.successor_cusip
            and dates
            and min(dates) > edge.effective_date
        )
        later_terminal_exit = extended_component_handoffs and _later_terminal_component_exit(
            lineage,
            action,
            edge,
            successor,
            new_sessions,
        )
        if not later_linear_exit and not later_terminal_exit:
            return None
    if complete_certificate_conflicts and _unknown_occupancy_conflict(
        lineage,
        {edge.predecessor_symbol, edge.successor_symbol},
        old_sessions[0],
        early_sessions[-1],
        allowed={(edge.predecessor_symbol, anchor.cusip, anchor.action_id)},
    ):
        return None
    observed = replace(successor, start_date_inclusive=early_sessions[0])
    return NameChangeHistory(
        edge.successor_symbol,
        early_sessions[0],
        early_sessions[-1],
        observed,
        edge.action_id,
    )


def _internal_component_anchored_early_successor_history(
    lineage,
    intervals,
    candidates,
    sources,
    grids,
    edge,
    owner,
    complete_certificate_conflicts=True,
):
    """Date a post-component successor span after an exact inherited prefix.

    Some provider successor sources retain the predecessor's complete history
    before exposing the new-label component.  The exact prefix remains owned by
    the predecessor certificate; this producer dates only the traded sessions
    after the component event and before the accepted same-UUID rename.
    """
    if (
        edge.predecessor_symbol == edge.successor_symbol
        or edge.predecessor_asset_id != edge.successor_asset_id
        or edge.predecessor_cusip != edge.successor_cusip
        or not _complete_action_query_covers(lineage, edge.successor_symbol, edge.effective_date)
        or _known_owners(lineage, edge.successor_symbol) != {edge.successor_asset_id}
    ):
        return None
    old = grids.get(edge.predecessor_symbol)
    new = grids.get(edge.successor_symbol)
    old_work = sources.get(edge.predecessor_symbol)
    new_work = sources.get(edge.successor_symbol)
    endpoint = exact_cusip_lookup_asset(lineage, edge.successor_cusip)
    if (
        old is None
        or new is None
        or old_work is None
        or new_work is None
        or endpoint is None
        or endpoint.get("id") != edge.successor_asset_id
        or endpoint.get("class") != "us_equity"
    ):
        return None
    old_rows, old_source, old_sessions = old
    new_rows, new_source, new_sessions = new
    if (
        not old_sessions
        or not new_sessions
        or edge.effective_date not in new_sessions
        or (old_source["feed"], old_source["timeframe"]) != (new_source["feed"], new_source["timeframe"])
        or owner.identity_key() != (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
        or owner.end_date_exclusive != edge.effective_date
    ):
        return None
    successor_intervals = [
        item
        for item in intervals
        if item.identity_key() == (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        and item.start_date_inclusive == edge.effective_date
    ]
    if len(successor_intervals) != 1:
        return None
    successor = successor_intervals[0]

    anchor_dates = set()
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or action.get("action_type") != "unit_split":
            continue
        try:
            effective = date.fromisoformat(action["effective_date"])
            processed = date.fromisoformat(action["process_date"])
        except (KeyError, TypeError, ValueError):
            continue
        if effective == processed and effective < edge.effective_date:
            anchor_dates.add(effective)
    anchors = [
        (when, anchor)
        for when in sorted(anchor_dates)
        if when in old_sessions and when in new_sessions
        for anchor in (
            _component_split_anchor(
                lineage,
                edge,
                when,
                extended_component_handoffs=True,
            ),
        )
        if anchor is not None
    ]
    if len(anchors) != 1:
        return None
    anchor_date, anchor = anchors[0]
    if not _complete_action_query_covers(lineage, edge.successor_symbol, anchor_date):
        return None
    try:
        span = tuple(
            date.fromisoformat(value)
            for value in expected_market_session_coverage(
                anchor_date.isoformat(), edge.effective_date.isoformat()
            ).session_dates
        )
    except (MarketCalendarError, ValueError):
        return None
    if not span or span[0] != anchor_date or len(span) < 2:
        return None
    early_sessions = span[1:]
    if (
        tuple(day for day in new_sessions if anchor_date < day < edge.effective_date) != early_sessions
        or tuple(day for day in old_sessions if anchor_date < day < edge.effective_date) != early_sessions
        or any(not owner.contains(day) for day in (anchor_date, *early_sessions))
    ):
        return None

    old_prefix = tuple(day for day in old_sessions if day <= anchor_date)
    new_prefix = tuple(day for day in new_sessions if day <= anchor_date)
    if old_prefix != new_prefix or len(old_prefix) < 5:
        return None
    try:
        expected_prefix = expected_market_session_coverage(
            old_prefix[0].isoformat(), (anchor_date + timedelta(days=1)).isoformat()
        )
    except MarketCalendarError:
        return None
    if expected_prefix.session_dates != tuple(day.isoformat() for day in old_prefix):
        return None

    old_views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in old_work}
    new_views = {source["adjustment"]: (download.rows, sessions) for download, source, sessions in new_work}
    if (
        len(old_views) != len(old_work)
        or len(new_views) != len(new_work)
        or set(old_views) != set(ADJUSTMENTS)
        or set(new_views) != set(ADJUSTMENTS)
    ):
        return None
    prefix_count = len(old_prefix)
    anchor_index = prefix_count - 1
    early_count = len(early_sessions)
    for adjustment in ADJUSTMENTS:
        prior_values, prior_sessions = old_views[adjustment]
        successor_values, successor_sessions = new_views[adjustment]
        if (
            prior_sessions != old_sessions
            or successor_sessions != new_sessions
            or not _rows_equal(
                prior_values[:prefix_count],
                successor_values[:prefix_count],
                fields=(
                    "t",
                    "o",
                    "h",
                    "l",
                    "c",
                    "v",
                    "n",
                    "vw",
                ),
            )
            or _traded_price(prior_values[anchor_index]) is None
        ):
            return None
        try:
            carried = Decimal(str(prior_values[anchor_index]["c"]))
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return None
        prior_early = prior_values[anchor_index + 1 : anchor_index + 1 + early_count]
        successor_early = successor_values[anchor_index + 1 : anchor_index + 1 + early_count]
        if (
            any(_terminal_carry_value(row) != carried for row in prior_early)
            or any(_traded_price(row) is None for row in successor_early)
            or anchor_index + 1 + early_count >= len(successor_values)
            or _traded_price(successor_values[anchor_index + 1 + early_count]) is None
        ):
            return None
    if len({_traded_price(row) for row in new_rows[anchor_index + 1 : anchor_index + 1 + early_count]}) < 2:
        return None

    later_intervals = [
        item
        for item in intervals
        if item.historical_symbol == edge.predecessor_symbol
        and item.asset_id != edge.predecessor_asset_id
        and item.start_date_inclusive is not None
        and item.start_date_inclusive > edge.effective_date
    ]
    if any(item.end_date_exclusive is not None for item in later_intervals) or any(
        item.historical_symbol == edge.predecessor_symbol
        and item.asset_id != edge.predecessor_asset_id
        and item not in later_intervals
        for item in intervals
    ):
        return None
    expected_predecessor_owners = {edge.predecessor_asset_id}
    if anchor.asset_id is not None:
        expected_predecessor_owners.add(anchor.asset_id)
    later_owner_ids = {item.asset_id for item in later_intervals}
    if _known_owners(lineage, edge.predecessor_symbol) != expected_predecessor_owners | later_owner_ids:
        return None

    for candidate in candidates:
        if candidate == edge:
            continue
        if not {candidate.predecessor_symbol, candidate.successor_symbol}.intersection(
            {edge.predecessor_symbol, edge.successor_symbol}
        ):
            continue
        if candidate.effective_date <= edge.effective_date:
            return None
    allowed_later_action_ids = {
        action_id
        for interval in later_intervals
        for rule in interval.applied_rules
        if rule.name == "action_anchored_current_ticker_occupancy"
        for action_id in rule.supporting_action_ids
    }
    allowed_action_ids = {edge.action_id, anchor.action_id, *allowed_later_action_ids}
    structural = {
        "cash_merger",
        "forward_split",
        "name_change",
        "reorganization",
        "reverse_split",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
        "unit_split",
    }
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or action.get("action_type") not in structural:
            continue
        affected = {
            action.get(key)
            for key in (
                "symbol",
                "old_symbol",
                "new_symbol",
                "alternate_symbol",
                "acquiree_symbol",
                "acquirer_symbol",
            )
        }
        if not affected.intersection({edge.predecessor_symbol, edge.successor_symbol}):
            continue
        dates = _action_dates(action)
        if not dates or (action.get("id") not in allowed_action_ids and min(dates) <= edge.effective_date):
            return None

    if complete_certificate_conflicts and _unknown_occupancy_conflict(
        lineage,
        {edge.predecessor_symbol, edge.successor_symbol},
        old_sessions[0],
        early_sessions[-1],
        allowed={(edge.predecessor_symbol, anchor.cusip, anchor.action_id)},
    ):
        return None
    observed = replace(successor, start_date_inclusive=early_sessions[0])
    return NameChangeHistory(
        edge.successor_symbol,
        early_sessions[0],
        early_sessions[-1],
        observed,
        edge.action_id,
    )


def _early_successor_source_bridge_history(
    lineage,
    intervals,
    candidates,
    grids,
    edge,
    owner,
    fields,
    complete_certificate_conflicts=True,
):
    """Date only an observed early successor span through a complete current source."""
    if (
        edge.predecessor_asset_id != edge.successor_asset_id
        or edge.predecessor_cusip != edge.successor_cusip
        or not _is_unique_direct_edge(candidates, edge)
        or not _complete_action_query_covers(lineage, edge.successor_symbol, edge.effective_date)
        or _known_owners(lineage, edge.successor_symbol) != {edge.successor_asset_id}
    ):
        return None

    current = exact_cusip_lookup_asset(lineage, edge.successor_cusip)
    current_symbol = current.get("symbol") if current is not None else None
    if (
        current is None
        or current.get("id") != edge.successor_asset_id
        or not isinstance(current_symbol, str)
        or not current_symbol
        or current_symbol in {edge.predecessor_symbol, edge.successor_symbol}
        or _known_owners(lineage, current_symbol) != {edge.successor_asset_id}
    ):
        return None

    old = grids.get(edge.predecessor_symbol)
    new = grids.get(edge.successor_symbol)
    bridge = grids.get(current_symbol)
    if old is None or new is None or bridge is None:
        return None
    old_rows, old_source, old_sessions = old
    new_rows, new_source, new_sessions = new
    bridge_rows, bridge_source, bridge_sessions = bridge
    if (
        not old_sessions
        or not new_sessions
        or (old_source["feed"], old_source["timeframe"]) != (new_source["feed"], new_source["timeframe"])
        or (old_source["feed"], old_source["timeframe"]) != (bridge_source["feed"], bridge_source["timeframe"])
        or bridge_sessions != (*old_sessions, *new_sessions)
        or not _rows_equal([*old_rows, *new_rows], bridge_rows, fields)
    ):
        return None

    predecessor_intervals = [item for item in intervals if item.historical_symbol == edge.predecessor_symbol]
    successor_intervals = [item for item in intervals if item.historical_symbol == edge.successor_symbol]
    if predecessor_intervals != [owner] or len(successor_intervals) != 1:
        return None
    successor = successor_intervals[0]
    if (
        successor.identity_key() != (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        or successor.start_date_inclusive != edge.effective_date
        or successor.end_date_exclusive is not None
        or any(not owner.contains(day) for day in old_sessions)
        or not new_sessions[0] < edge.effective_date
        or edge.effective_date not in new_sessions
    ):
        return None

    early_count = next(
        (index for index, day in enumerate(new_sessions) if day >= edge.effective_date),
        len(new_sessions),
    )
    early_sessions = new_sessions[:early_count]
    if not early_sessions:
        return None
    try:
        handoff = expected_market_session_coverage(
            old_sessions[-1].isoformat(),
            (new_sessions[0] + timedelta(days=1)).isoformat(),
        )
        early = expected_market_session_coverage(
            new_sessions[0].isoformat(),
            edge.effective_date.isoformat(),
        )
    except MarketCalendarError:
        return None
    if handoff.session_dates != (
        old_sessions[-1].isoformat(),
        new_sessions[0].isoformat(),
    ) or early.session_dates != tuple(day.isoformat() for day in early_sessions):
        return None

    certificate_symbols = {edge.predecessor_symbol, edge.successor_symbol, current_symbol}
    if complete_certificate_conflicts and (
        _unknown_occupancy_conflict(lineage, certificate_symbols, old_sessions[0], early_sessions[-1])
        or _bridge_structural_conflict(
            lineage, certificate_symbols, old_sessions[0], early_sessions[-1], edge.action_id
        )
    ):
        return None
    observed = replace(successor, start_date_inclusive=early_sessions[0])
    return (
        NameChangeHistory(
            edge.successor_symbol,
            early_sessions[0],
            early_sessions[-1],
            observed,
            edge.action_id,
        ),
        new_rows[:early_count],
    )


def _post_padding_shared_uuid_suffix_history(
    lineage,
    intervals,
    candidates,
    sources,
    grids,
    edge,
    owner,
    fields,
    has_informative_trading,
    has_prolonged_carry_forward,
    shared_inert_bridges,
    split_adjusted_post_padding_aliases,
):
    """Certify only an exact successor-source alias of predecessor rows.

    Earlier provider padding is neither continuity nor absence-of-trading
    evidence. It may not erase a later complete, exact overlap, but this
    certificate consumes only that observed overlap and keeps the action's
    ticker boundary unchanged.
    """
    if (
        edge.predecessor_asset_id != edge.successor_asset_id
        or edge.predecessor_cusip != edge.successor_cusip
        or not _complete_action_query_covers(lineage, edge.successor_symbol, edge.effective_date)
    ):
        return None

    old = grids.get(edge.predecessor_symbol)
    new = grids.get(edge.successor_symbol)
    if old is None or new is None:
        return None
    old_rows, old_source, old_sessions = old
    new_rows, new_source, new_sessions = new
    if (
        (old_source["feed"], old_source["timeframe"]) != (new_source["feed"], new_source["timeframe"])
        or not old_sessions
        or not new_sessions
        or edge.effective_date not in new_sessions
    ):
        return None

    boundary_index = new_sessions.index(edge.effective_date)
    early_sessions = new_sessions[:boundary_index]
    if (
        not early_sessions
        or len(early_sessions) >= len(old_sessions)
        or tuple(old_sessions[-len(early_sessions) :]) != tuple(early_sessions)
    ):
        return None
    overlap_start = len(old_sessions) - len(early_sessions)
    predecessor_members = _independent_members(edge.predecessor_asset_id, candidates)
    split_dates = {
        candidate.effective_date
        for candidate in candidates
        if split_adjusted_post_padding_aliases
        and candidate.edge_type in {"forward_split", "reverse_split"}
        and candidate.predecessor_symbol == candidate.successor_symbol == edge.predecessor_symbol
        and {candidate.predecessor_asset_id, candidate.successor_asset_id} <= predecessor_members
        and candidate.effective_date < early_sessions[0]
    }
    bridges = shared_inert_bridges(
        sources[edge.predecessor_symbol],
        allowed_value_change_dates=frozenset(split_dates),
    )
    if (
        len(bridges) != 1
        or bridges[0][1] != overlap_start - 1
        or _traded_price(old_rows[bridges[0][0] - 1]) is None
    ):
        return None
    old_overlap = old_rows[-len(early_sessions) :]
    new_overlap = new_rows[:boundary_index]
    if (
        not _rows_equal(old_overlap, new_overlap, fields)
        or _traded_price(new_overlap[0]) is None
        or _traded_price(new_overlap[-1]) is None
        or not has_informative_trading(new_overlap)
        or has_prolonged_carry_forward(new_overlap)
    ):
        return None
    try:
        early = expected_market_session_coverage(early_sessions[0].isoformat(), edge.effective_date.isoformat())
        handoff = expected_market_session_coverage(old_sessions[-1].isoformat(), edge.effective_date.isoformat())
    except MarketCalendarError:
        return None
    if early.session_dates != tuple(day.isoformat() for day in early_sessions) or handoff.session_dates != (
        old_sessions[-1].isoformat(),
    ):
        return None

    current_cusip = exact_cusip_lookup_asset(lineage, edge.successor_cusip)
    current_symbol = exact_symbol_lookup_asset(lineage, edge.successor_symbol)
    if (
        current_cusip is None
        or current_symbol is None
        or current_cusip.get("id") != edge.successor_asset_id
        or current_symbol.get("id") != edge.successor_asset_id
        or current_cusip.get("symbol") != edge.successor_symbol
        or current_symbol.get("symbol") != edge.successor_symbol
        or current_cusip.get("class") != "us_equity"
        or current_symbol.get("class") != "us_equity"
        or current_cusip.get("status") != "active"
        or current_symbol.get("status") != "active"
        or current_cusip.get("tradable") is not True
        or current_symbol.get("tradable") is not True
        or current_cusip.get("exchange") in (None, "", "OTC")
        or current_symbol.get("exchange") != current_cusip.get("exchange")
        or _known_owners(lineage, edge.successor_symbol) != {edge.successor_asset_id}
    ):
        return None
    predecessor_owners = _known_owners(lineage, edge.predecessor_symbol)
    if edge.predecessor_asset_id not in predecessor_owners or not predecessor_owners <= predecessor_members:
        return None

    successor_intervals = [item for item in intervals if item.historical_symbol == edge.successor_symbol]
    if len(successor_intervals) != 1:
        return None
    successor = successor_intervals[0]
    if (
        successor.identity_key() != (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
        or successor.start_date_inclusive != edge.effective_date
        or successor.end_date_exclusive is not None
        or any(not owner.contains(day) for day in early_sessions)
    ):
        return None

    related_symbols = {edge.predecessor_symbol, edge.successor_symbol}
    for candidate in candidates:
        if candidate == edge:
            continue
        affected = {candidate.predecessor_symbol, candidate.successor_symbol}
        if edge.successor_symbol in affected or (
            edge.predecessor_symbol in affected and candidate.effective_date >= early_sessions[0]
        ):
            return None
    structural = {
        "cash_merger",
        "forward_split",
        "name_change",
        "reorganization",
        "reverse_split",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
        "unit_split",
    }
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or action.get("id") == edge.action_id:
            continue
        affected = {
            action.get(key)
            for key in ("symbol", "old_symbol", "new_symbol", "acquiree_symbol", "acquirer_symbol")
        }
        if action.get("action_type") not in structural or not affected.intersection(related_symbols):
            continue
        dates = _action_dates(action)
        later_successor_acquisition = (
            split_adjusted_post_padding_aliases
            and action.get("action_type") == "stock_merger"
            and action.get("acquirer_symbol") == edge.successor_symbol
            and action.get("acquirer_cusip") == edge.successor_cusip
            and action.get("acquiree_symbol") not in (None, "", edge.predecessor_symbol, edge.successor_symbol)
            and action.get("acquiree_cusip") not in (None, "", edge.predecessor_cusip, edge.successor_cusip)
            and _positive_decimal(action.get("acquiree_rate")) is not None
            and _positive_decimal(action.get("acquirer_rate")) is not None
            and action.get("effective_date") is not None
            and action.get("process_date") is not None
            and dates
            and min(dates) > edge.effective_date
        )
        if later_successor_acquisition:
            continue
        if edge.successor_symbol in affected or not dates or max(dates) >= early_sessions[0]:
            return None

    return NameChangeHistory(
        edge.successor_symbol,
        early_sessions[0],
        early_sessions[-1],
        owner,
        edge.action_id,
    )


def _earlier_continuing_acquirer(action, edge, predecessor_interval):
    """An incumbent's earlier stock acquisition is not a replacement of its equity."""
    dates = _action_dates(action)
    return (
        action.get("action_type") == "stock_merger"
        and bool(action.get("id"))
        and action.get("acquirer_symbol") == edge.predecessor_symbol
        and action.get("acquirer_cusip") == edge.predecessor_cusip
        and action.get("acquiree_symbol") not in (None, "", edge.predecessor_symbol, edge.successor_symbol)
        and action.get("acquiree_cusip") not in (None, "", edge.predecessor_cusip, edge.successor_cusip)
        and _positive_decimal(action.get("acquiree_rate")) is not None
        and _positive_decimal(action.get("acquirer_rate")) is not None
        and action.get("effective_date") is not None
        and action.get("process_date") is not None
        and bool(dates)
        and max(dates) < edge.effective_date
        and (
            predecessor_interval.start_date_inclusive is None
            or min(dates) >= predecessor_interval.start_date_inclusive
        )
    )


def _same_symbol_handoff_action_shape(
    actions,
    related_candidates,
    edge,
    predecessor_interval,
    *,
    bounded_entry_same_symbol_handoffs,
    continuing_acquirer_handoffs=True,
):
    if continuing_acquirer_handoffs:
        # The surrounding certificate already owns the exact incumbent endpoint.
        # Keep acquisition evidence and candidate membership unchanged; only its
        # role in this substitution's conflict test differs.
        actions = [
            action for action in actions if not _earlier_continuing_acquirer(action, edge, predecessor_interval)
        ]
    if predecessor_interval.start_date_inclusive is None:
        return len(actions) == 1 and actions[0].get("id") == edge.action_id and related_candidates == [edge]
    if not bounded_entry_same_symbol_handoffs or len(actions) != 2 or len(related_candidates) != 2:
        return False

    incoming = [candidate for candidate in related_candidates if candidate != edge]
    if len(incoming) != 1:
        return False
    incoming = incoming[0]
    return (
        incoming.predecessor_symbol != edge.predecessor_symbol
        and incoming.successor_symbol == edge.predecessor_symbol
        and incoming.predecessor_cusip == incoming.successor_cusip == edge.predecessor_cusip
        and incoming.predecessor_asset_id == incoming.successor_asset_id == edge.predecessor_asset_id
        and incoming.effective_date == predecessor_interval.start_date_inclusive
        and incoming.effective_date < edge.effective_date
        and {action.get("id") for action in actions} == {incoming.action_id, edge.action_id}
    )


def exact_same_symbol_endpoint_agreement(lineage, edge, *, endpoints=None):
    """The shared independent endpoint gate for same-label substitutions.

    Call only at the continuity producer. Completed certificates carry this
    decision to chronology and membership without another lookup projection.
    """
    symbol = edge.predecessor_symbol
    predecessor, successor = (
        endpoints
        if endpoints is not None
        else (
            exact_cusip_lookup_asset(lineage, edge.predecessor_cusip),
            exact_cusip_lookup_asset(lineage, edge.successor_cusip),
        )
    )
    current = exact_symbol_lookup_asset(lineage, symbol)
    return not (
        predecessor is None
        or successor is None
        or current is None
        or predecessor.get("id") != edge.predecessor_asset_id
        or successor.get("id") != edge.successor_asset_id
        or current.get("id") != edge.successor_asset_id
        or predecessor.get("symbol") != symbol
        or successor.get("symbol") != symbol
        or predecessor.get("status") != "inactive"
        or predecessor.get("tradable") is not False
        or successor.get("status") != "active"
        or successor.get("tradable") is not True
        or current.get("status") != "active"
        or current.get("tradable") is not True
        or predecessor.get("exchange") in (None, "", "OTC")
        or predecessor.get("exchange") != successor.get("exchange")
        or current.get("exchange") != successor.get("exchange")
        or _known_owners(lineage, symbol) != {edge.predecessor_asset_id, edge.successor_asset_id}
    )


def _exact_same_symbol_handoff(
    lineage,
    intervals,
    candidates,
    edge,
    *,
    bounded_entry_same_symbol_handoffs,
    complete_certificate_conflicts=True,
    continuing_acquirer_handoffs=True,
):
    """Certify one closed predecessor/current-successor substitution.

    This producer consumes completed action and lookup evidence once. Row routing
    and UUID membership consume the resulting accepted action ID; neither repeats
    these endpoint checks or infers a boundary from returned prices.
    """
    symbol = edge.predecessor_symbol
    if (
        edge.edge_type != "name_change"
        or symbol != edge.successor_symbol
        or edge.predecessor_cusip == edge.successor_cusip
        or edge.predecessor_asset_id == edge.successor_asset_id
        or not _complete_action_query_covers(lineage, symbol, edge.effective_date)
    ):
        return False

    actions = [
        action
        for action in lineage.get("corporate_actions", ())
        if isinstance(action, dict)
        and action.get("action_type") not in {"cash_dividend", "capital_gain"}
        and any(value == symbol for key, value in action.items() if key == "symbol" or key.endswith("_symbol"))
    ]
    related_candidates = [
        candidate
        for candidate in candidates
        if symbol in (candidate.predecessor_symbol, candidate.successor_symbol)
    ]
    if not exact_same_symbol_endpoint_agreement(lineage, edge):
        return False

    own_intervals = [item for item in intervals if item.historical_symbol == symbol]
    predecessor_intervals = [
        item
        for item in own_intervals
        if item.identity_key() == (symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
        and item.end_date_exclusive == edge.effective_date
    ]
    successor_intervals = [
        item
        for item in own_intervals
        if item.identity_key() == (symbol, edge.successor_cusip, edge.successor_asset_id)
        and item.start_date_inclusive == edge.effective_date
        and item.end_date_exclusive is None
    ]
    if len(own_intervals) != 2 or len(predecessor_intervals) != 1 or len(successor_intervals) != 1:
        return False
    if complete_certificate_conflicts and _unknown_occupancy_conflict(
        lineage,
        {symbol},
        predecessor_intervals[0].start_date_inclusive,
        successor_intervals[0].end_date_exclusive,
    ):
        return False
    return _same_symbol_handoff_action_shape(
        actions,
        related_candidates,
        edge,
        predecessor_intervals[0],
        bounded_entry_same_symbol_handoffs=bounded_entry_same_symbol_handoffs,
        continuing_acquirer_handoffs=continuing_acquirer_handoffs,
    )


def assess_name_change_history(
    lineage,
    intervals,
    candidates,
    sources,
    *,
    exact_same_symbol_handoffs=True,
    bounded_entry_same_symbol_handoffs=True,
    partial_shared_uuid_suffixes=True,
    single_session_shared_uuid_histories=True,
    early_successor_source_bridges=True,
    terminal_carry_forward_exclusions=True,
    post_padding_shared_uuid_suffixes=True,
    split_adjusted_post_padding_aliases=True,
    pre_entry_carry_forward_exclusions=True,
    component_anchored_early_successors=True,
    extended_component_handoffs=True,
    internal_component_handoffs=True,
    complete_certificate_conflicts=True,
    completed_event_scope=True,
):
    """Certify source bounds without reading reports produced by these edges."""
    if not any(edge.edge_type == "name_change" for edge in candidates):
        # A completed sourced substitution needs no numerical rename proof.
        # Do not prepare price grids when this group has no rename candidate.
        return NameChangeAssessment()
    # This is the independent continuity producer. Its complete selected source
    # inputs may prove a bounded rename; assignments derived from that same
    # disputed edge cannot prove it. Downstream adapters consume the resulting
    # certificate instead of repeating these comparisons per row or view.
    # Import here to keep the ownership/edge builders independent of price rules.
    from .terminal_episode import (
        has_informative_trading,
        has_prolonged_carry_forward,
        has_traded_observation,
        shared_inert_bridges,
    )

    if not completed_event_scope:
        # Frozen historical policies recognized only the original query label.
        # Current certificates consume the producer's completed queried-symbol
        # set; this projection preserves historical replay without changing facts.
        lineage = {**lineage, "queried_symbols": ()}
    fields = ("t", "o", "h", "l", "c", "v", "n", "vw")
    grids = {symbol: _grid(work) for symbol, work in sources.items()}
    accepted, histories, independent, excluded, pre_entry_excluded = set(), [], set(), [], []
    for edge in candidates:
        if edge.edge_type != "name_change":
            continue
        if edge.predecessor_symbol == edge.successor_symbol:
            if exact_same_symbol_handoffs and _exact_same_symbol_handoff(
                lineage,
                intervals,
                candidates,
                edge,
                bounded_entry_same_symbol_handoffs=bounded_entry_same_symbol_handoffs,
                complete_certificate_conflicts=complete_certificate_conflicts,
                continuing_acquirer_handoffs=completed_event_scope,
            ):
                accepted.add(edge.action_id)
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
        prolonged_padding = has_prolonged_carry_forward(own_rows)
        post_padding_history = (
            _post_padding_shared_uuid_suffix_history(
                lineage,
                intervals,
                candidates,
                sources,
                grids,
                edge,
                owner,
                fields,
                has_informative_trading,
                has_prolonged_carry_forward,
                shared_inert_bridges,
                split_adjusted_post_padding_aliases,
            )
            if prolonged_padding and post_padding_shared_uuid_suffixes
            else None
        )
        if prolonged_padding:
            if post_padding_history is not None:
                histories.append(post_padding_history)
            component_history = (
                _component_anchored_early_successor_history(
                    lineage,
                    intervals,
                    candidates,
                    grids,
                    edge,
                    owner,
                    has_informative_trading,
                    has_prolonged_carry_forward,
                    extended_component_handoffs,
                    complete_certificate_conflicts=complete_certificate_conflicts,
                )
                if component_anchored_early_successors and extended_component_handoffs
                else None
            )
            if component_history is not None:
                histories.append(component_history)
            internal_component_history = (
                _internal_component_anchored_early_successor_history(
                    lineage,
                    intervals,
                    candidates,
                    sources,
                    grids,
                    edge,
                    owner,
                    complete_certificate_conflicts=complete_certificate_conflicts,
                )
                if internal_component_handoffs
                else None
            )
            if internal_component_history is not None:
                histories.append(internal_component_history)
            continue
        terminal_exclusion = (
            _terminal_carry_forward_exclusion(
                lineage,
                intervals,
                candidates,
                sources,
                grids,
                edge,
                owner,
            )
            if terminal_carry_forward_exclusions
            else None
        )
        if terminal_exclusion is not None:
            excluded.append(terminal_exclusion)
        pre_entry_exclusion = (
            _pre_entry_carry_forward_exclusion(
                lineage,
                intervals,
                candidates,
                sources,
                grids,
                edge,
                owner,
                has_informative_trading,
                has_prolonged_carry_forward,
            )
            if pre_entry_carry_forward_exclusions
            else None
        )
        if pre_entry_exclusion is not None:
            pre_entry_excluded.append(pre_entry_exclusion)
        early_bridge = (
            _early_successor_source_bridge_history(
                lineage,
                intervals,
                candidates,
                grids,
                edge,
                owner,
                fields,
                complete_certificate_conflicts=complete_certificate_conflicts,
            )
            if early_successor_source_bridges
            else None
        )
        if (
            early_bridge is not None
            and has_informative_trading(own_rows)
            and has_informative_trading(early_bridge[1])
            and not has_prolonged_carry_forward(early_bridge[1])
        ):
            histories.append(early_bridge[0])
        component_history = (
            _component_anchored_early_successor_history(
                lineage,
                intervals,
                candidates,
                grids,
                edge,
                owner,
                has_informative_trading,
                has_prolonged_carry_forward,
                extended_component_handoffs,
                complete_certificate_conflicts=complete_certificate_conflicts,
            )
            if component_anchored_early_successors
            else None
        )
        if component_history is not None:
            histories.append(component_history)
        internal_component_history = (
            _internal_component_anchored_early_successor_history(
                lineage,
                intervals,
                candidates,
                sources,
                grids,
                edge,
                owner,
                complete_certificate_conflicts=complete_certificate_conflicts,
            )
            if internal_component_handoffs
            else None
        )
        if internal_component_history is not None:
            histories.append(internal_component_history)
        other = [(day, row) for day, row in zip(new_sessions, new_rows) if first <= day <= last]
        prefix_days = [day for day, _ in prefix]
        other_days = [day for day, _ in other]
        same_uuid = edge.predecessor_asset_id == edge.successor_asset_id
        if other_days == prefix_days:
            compared = prefix
            partial_successor_suffix = False
        elif (
            partial_shared_uuid_suffixes
            and same_uuid
            and other_days
            and len(other_days) < len(prefix_days)
            and other_days == prefix_days[-len(other_days) :]
        ):
            # A shared UUID establishes identity, not unseen row provenance. A
            # later-starting destination source may consume only its observed,
            # contiguous exact suffix of the independently owned predecessor.
            compared = prefix[-len(other_days) :]
            partial_successor_suffix = True
        else:
            continue
        equal = [
            all(left.get(key) is not None and left.get(key) == right.get(key) for key in fields)
            for (_, left), (_, right) in zip(compared, other)
        ]
        # A shared UUID already establishes identity, but not the label's dates.
        # Its independent own-label source bounds the interval, and an exact,
        # informative suffix connects the two sources. Earlier price revisions
        # cannot invalidate that connection or extend its observed lower bound.
        suffix_start = max((index + 1 for index, match in enumerate(equal) if not match), default=0)
        compared_rows = [row for _, row in compared]
        matching_suffix = compared_rows[suffix_start:]
        reaches_boundary = tail.session_dates == (last.isoformat(),)
        single_session_history = (
            single_session_shared_uuid_histories
            and same_uuid
            and edge.predecessor_cusip == edge.successor_cusip
            and len(old_sessions) == 1
            and len(prefix) == len(other) == 1
            and other_days == prefix_days
            and reaches_boundary
            and _is_unique_direct_edge(candidates, edge)
            and all(equal)
            and has_traded_observation(own_rows)
        )
        if not has_informative_trading(own_rows) and not single_session_history:
            continue
        proven = (
            same_uuid
            and (has_informative_trading(matching_suffix) or single_session_history)
            or not same_uuid
            and all(equal)
            and reaches_boundary
        )
        if proven:
            accepted.add(edge.action_id)
            spans = [(compared[0][0], compared[-1][0])]
        else:
            # Exact observed spans can establish row provenance even when the
            # full handoff cannot certify a UUID join. Keep gaps unresolved.
            spans = []
            for matches, run in groupby(enumerate(equal), key=lambda item: item[1]):
                indexes = [index for index, _ in run]
                if matches and has_informative_trading(compared_rows[indexes[0] : indexes[-1] + 1]):
                    spans.append((compared[indexes[0]][0], compared[indexes[-1]][0]))
        history_symbols = (
            (edge.successor_symbol,)
            if partial_successor_suffix or single_session_history
            else (edge.predecessor_symbol, edge.successor_symbol)
        )
        for start, end in spans:
            for symbol in history_symbols:
                histories.append(NameChangeHistory(symbol, start, end, owner, edge.action_id))
        if not same_uuid and not any(equal) and has_informative_trading([row for _, row in other]):
            # Both sources have independent own-label observations on the same
            # dates. The relationship cannot erase the destination's history.
            if _known_owners(lineage, edge.successor_symbol) <= _independent_members(
                edge.successor_asset_id, candidates
            ):
                independent.add(edge.action_id)
    return NameChangeAssessment(
        frozenset(accepted),
        tuple(histories),
        frozenset(independent),
        tuple(excluded),
        tuple(pre_entry_excluded),
    )

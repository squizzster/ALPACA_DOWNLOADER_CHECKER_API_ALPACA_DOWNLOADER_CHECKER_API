"""Dated ticker ownership after a stock merger replaces a former issue."""

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from .action_identity import exact_cusip_lookup_asset, exact_symbol_lookup_asset
from .name_change import (
    _action_dates,
    _complete_action_query_covers,
    _non_observation_conflict,
    _positive_decimal,
    action_labels,
)

if TYPE_CHECKING:
    from .ownership_interval_rules import TickerOwnershipInterval

NAME = "distinct_stock_merger_ticker_occupation"
CORRELATION_VERSION = 101
DEPARTURE_NAME = "stock_merger_acquiree_terminal_boundary"
DEPARTURE_VERSION = 103


@dataclass(frozen=True)
class StockMergerOccupation:
    predecessor: "TickerOwnershipInterval"
    date: date
    cusip: str
    asset_id: str
    action_id: str


def _same_issue_distribution(action, symbol, cusip, day):
    return (
        action.get("action_type") == "spin_off"
        and (action.get("source_symbol"), action.get("new_symbol")) == (symbol, symbol)
        and (action.get("source_cusip"), action.get("new_cusip")) == (cusip, cusip)
        and action.get("ex_date") == day.isoformat()
        and _positive_decimal(action.get("source_rate")) == _positive_decimal(action.get("new_rate")) == 1
    )


def select_departure(lineage, intervals, actions):
    """Close established acquiree ownership using exact, corroborated provider evidence."""
    query = lineage.get("query") or {}
    symbol = query.get("input_symbol")
    candidates = []
    for action in actions:
        acquirer = action.get("acquirer_symbol")
        if action.get("action_type") != "stock_merger" or action.get("acquiree_symbol") != symbol:
            continue
        if not acquirer or acquirer == symbol or not action.get("id"):
            continue
        try:
            day = date.fromisoformat(action["effective_date"])
            through = date.fromisoformat(query["corporate_actions_end"])
        except (KeyError, TypeError, ValueError):
            continue
        if any(
            action.get(field, day.isoformat()) != day.isoformat() for field in ("process_date", "payable_date")
        ):
            continue
        if not all(_positive_decimal(action.get(field)) for field in ("acquiree_rate", "acquirer_rate")):
            continue
        if not all(_complete_action_query_covers(lineage, symbol, point) for point in (day, through)):
            continue
        cusip, destination = action.get("acquiree_cusip"), action.get("acquirer_cusip")
        old = exact_cusip_lookup_asset(lineage, cusip)
        new = exact_cusip_lookup_asset(lineage, destination)
        if (
            old is None
            or new is None
            or cusip == destination
            or old["id"] == new["id"]
            or old.get("symbol") != symbol
            or new.get("symbol") != acquirer
            or old.get("status") != "inactive"
            or old.get("tradable") is not False
        ):
            continue
        preceding = [
            interval for interval in intervals if interval.historical_symbol == symbol and interval.contains(day)
        ]
        if len(preceding) != 1:
            continue
        (prior,) = preceding
        if (prior.cusip, prior.asset_id) != (cusip, old["id"]) or not (
            prior.start_date_inclusive is not None and prior.start_date_inclusive < day
        ):
            continue
        if any(
            other.get("id") != action["id"]
            and symbol in action_labels(other)
            and (not (dates := _action_dates(other)) or max(dates) >= day)
            and not _same_issue_distribution(other, symbol, cusip, day)
            for other in actions
        ):
            continue
        candidates.append((prior, day, action["id"]))
    return candidates[0] if len(candidates) == 1 else None


def _same_acquirer_events(lineage, actions, current, cusip, predecessor_id):
    """Separate later transfers into the same issue from a new label occupant."""
    result = set()
    for action in actions:
        role = {"stock_merger": "acquirer", "spin_off": "new"}.get(action.get("action_type"))
        if role is None or (action.get(f"{role}_symbol"), action.get(f"{role}_cusip")) != (
            current["symbol"],
            cusip,
        ):
            continue
        other = "acquiree" if role == "acquirer" else "source"
        endpoint = exact_cusip_lookup_asset(lineage, action.get(f"{other}_cusip"))
        action_id = action.get("id")
        if (
            action_id
            and endpoint is not None
            and endpoint["id"] not in {current["id"], predecessor_id}
            and endpoint.get("symbol") == action.get(f"{other}_symbol")
            and endpoint.get("symbol") != current["symbol"]
        ):
            result.add(action_id)
    return result


def select_occupation(lineage, intervals, actions):
    """Keep two different securities and date their successive own-label spans."""
    query = lineage.get("query") or {}
    symbol = query.get("input_symbol")
    if not symbol:
        return None
    current = exact_symbol_lookup_asset(lineage, symbol)
    if not current or current.get("status") != "active" or current.get("tradable") is not True:
        return None
    try:
        through = date.fromisoformat(query["corporate_actions_end"])
    except (KeyError, TypeError, ValueError):
        return None
    own = tuple(interval for interval in intervals if interval.historical_symbol == symbol)
    if not own or any(interval.asset_id == current["id"] for interval in own):
        return None
    candidates = []
    for action in actions:
        former = action.get("acquiree_symbol")
        if (
            action.get("action_type") != "stock_merger"
            or not former
            or former == symbol
            or action.get("acquirer_symbol") != symbol
        ):
            continue
        action_id = action.get("id")
        effective = action.get("effective_date")
        corroborating = {action.get(field) for field in ("payable_date", "process_date")} - {None}
        old_cusip, cusip = action.get("acquiree_cusip"), action.get("acquirer_cusip")
        if (
            not action_id
            or not effective
            or corroborating != {effective}
            or not old_cusip
            or not cusip
            or old_cusip == cusip
            or not all(_positive_decimal(action.get(field)) for field in ("acquiree_rate", "acquirer_rate"))
        ):
            continue
        try:
            day = date.fromisoformat(effective)
        except (TypeError, ValueError):
            continue
        if not all(_complete_action_query_covers(lineage, symbol, d) for d in (day, through)):
            continue
        old = exact_cusip_lookup_asset(lineage, old_cusip)
        new = exact_cusip_lookup_asset(lineage, cusip)
        if (
            old is None
            or old["id"] == current["id"]
            or old.get("symbol") != former
            or old.get("status") != "inactive"
            or old.get("tradable") is not False
            or new is None
            or any(new.get(field) != current.get(field) for field in ("id", "symbol", "exchange", "status"))
        ):
            continue
        old_intervals = [interval for interval in own if interval.contains(day)]
        if len(old_intervals) != 1:
            continue
        (preceding,) = old_intervals
        if (
            (preceding.asset_id, preceding.cusip) != (old["id"], old_cusip)
            or preceding.start_date_inclusive is None
            or preceding.start_date_inclusive >= day
            or any(
                interval != preceding
                and (interval.end_date_exclusive is None or interval.end_date_exclusive > day)
                for interval in own
            )
        ):
            continue
        candidates.append(StockMergerOccupation(preceding, day, cusip, current["id"], action_id))
    if len(candidates) != 1:
        return None
    (occupation,) = candidates
    allowed = _same_acquirer_events(lineage, actions, current, occupation.cusip, occupation.predecessor.asset_id)
    allowed.add(occupation.action_id)
    if _non_observation_conflict(
        lineage, symbol, occupation.date, through, {(occupation.asset_id, occupation.cusip)}, allowed
    ):
        return None
    return occupation

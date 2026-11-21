"""Bind one latest traded session to a contemporaneous own-symbol observation."""

from datetime import date, datetime, time

from libs.alpaca_pricing_download.contract import NEW_YORK
from libs.common.us_equity_market_session_calendar import regular_market_hours, surrounding_market_sessions

from .name_change import (
    _action_dates,
    _complete_action_query_covers,
    _known_owners,
    _traded_price,
    action_labels,
)


def observed_session(symbol, lineage, source, last, asset_id, work):
    """A directly observed current occupant can own the latest literal bar."""
    rivals = _known_owners(lineage, symbol) - {asset_id}
    former = {
        item["id"]
        for item in lineage.get("identities", ())
        if item.get("status") == "inactive" or item.get("symbol") != symbol
    }
    if not rivals or not rivals <= former:
        return None
    lookup = next(
        item
        for item in lineage["asset_lookups"]
        if item.get("lookup_kind") == "symbol" and item.get("lookup") == symbol
    )
    if lookup.get("source") != "asset_catalogue":
        return None
    try:
        observed = datetime.fromisoformat(lineage["catalogue_observed_at_utc"])
        cutoff = date.fromisoformat(source["asof"])
    except (KeyError, TypeError, ValueError):
        return None
    if observed.tzinfo is None:
        return None
    local = observed.astimezone(NEW_YORK)
    if not last <= local.date() <= cutoff:
        return None
    neighbours = surrounding_market_sessions(last.isoformat())
    if not neighbours:
        return None
    next_session = date.fromisoformat(neighbours[-1])
    opening = time.fromisoformat(regular_market_hours(next_session.isoformat())[0])
    if (local.date(), local.time()) >= (next_session, opening) or cutoff > next_session:
        return None
    if not all(_complete_action_query_covers(lineage, symbol, day) for day in (last, cutoff)):
        return None

    def concurrent(action):
        if symbol not in action_labels(action):
            return False
        days = _action_dates({**action, "effective_date": action.get("ex_date") or action.get("effective_date")})
        return not days or any(last <= day <= cutoff for day in days)

    if any(concurrent(action) for action in lineage.get("corporate_actions", ())):
        return None
    if any(not rows.rows or _traded_price(rows.rows[-1]) is None for rows, _, _ in work):
        return None
    return observed.isoformat()

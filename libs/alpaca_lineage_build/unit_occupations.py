"""Separate unit issues from the dated underlying common, preserving class-specific ownership."""

from datetime import date, timedelta
from decimal import Decimal

from libs.alpaca_pricing_download.dated_rename import _matches
from libs.alpaca_security_lineage.inference_evidence import provider_actions
from libs.common.us_equity_market_session_calendar import surrounding_market_sessions

from .occupation_boundary import traded


def _active(rows):
    return {d for d, row in rows.items() if traded(row)}


def _issued(event):
    return event.get("id") and str(event.get("status", "")).lower() not in {
        "canceled",
        "cancelled",
        "withdrawn",
        "pending",
        "postponed",
    }


def _family(evidence, symbol, members):
    quotes = {}
    events = {}
    for report in evidence:
        for lookup in report["asset_lookups"]:
            if lookup.get("found"):
                asset = lookup["asset"]
                quotes.setdefault(lookup["lookup"], {})[asset["id"]] = asset
        for event in provider_actions(report):
            identifier = event.get("id")
            if identifier in events and events[identifier] != event:
                return None
            events[identifier] = event
    candidates = list(quotes.get(symbol, {}).values())
    if len(candidates) != 1:
        return None
    unit = candidates[0]
    name = unit.get("name", "").lower()
    if (
        unit["id"] not in members
        or unit.get("symbol") != symbol
        or unit.get("class") != "us_equity"
        or "unit" not in name
        or not ("common stock" in name or "ordinary share" in name)
        or len(symbol) < 3
        or not symbol.endswith(".U")
    ):
        return None
    return unit, quotes, [e for e in events.values() if _issued(e)]


def _old_separation(events, unit, common, prev, earliest, quotes):
    upcoming = surrounding_market_sessions(prev)
    if not upcoming:
        return None
    actions = [
        e
        for e in events
        if e["action_type"] == "unit_split"
        and e.get("old_symbol") == unit["symbol"]
        and prev <= (e.get("effective_date") or "") < earliest
    ]
    if not actions:
        return {}
    if len(actions) != 1:
        return None
    action = actions[0]
    when = action["effective_date"]
    if (
        when not in upcoming[1:]
        or action.get("new_symbol") != common
        or not action.get("alternate_symbol")
        or action["alternate_symbol"] in (common, unit["symbol"])
        or not action.get("old_cusip")
        or set(quotes.get(action["old_cusip"], {})) != {unit["id"]}
    ):
        return None
    components = []
    for side in ("new", "alternate"):
        cusip, symbol = action.get(side + "_cusip"), action[side + "_symbol"]
        if not cusip or Decimal(str(action.get(side + "_rate") or 0)) <= 0:
            return None
        matches = [
            e
            for e in events
            if e["action_type"] == "stock_merger"
            and e.get("effective_date") == when
            and e.get("acquiree_cusip") == cusip
            and e.get("acquiree_symbol") == symbol
            and e.get("acquirer_cusip")
            and e["acquirer_cusip"] != cusip
        ]
        if len(matches) != 1:
            return None
        components.append(matches[0]["id"])
    return {
        "retained_side": "prior",
        "terminal_action": action,
        "old_asset_id": unit["id"],
        "component_action_ids": sorted(components),
    }


def _paired_tapes(literal, old, new, first, restart, last, common_end):
    dates = _active(literal)
    prior = sorted(d for d in dates if d <= common_end)
    later = sorted(d for d in dates if d >= restart)
    if (
        len(prior) < 2
        or len(later) < 2
        or not first < prior[0] < prior[-1]
        or prior[-1] != common_end
        or not restart < later[0] < later[-1]
        or later[-1] != last
        or set(prior + later) != dates
        or _active(new) != set(later)
    ):
        return False
    old_active = _active(old)
    if (
        set(prior) != {day for day in old_active if day <= common_end}
        or any(not _matches(old[d], literal[d]) for d in prior)
        or any(not _matches(new[d], literal[d]) for d in later)
    ):
        return False
    during = {old[d]["close"] for d in old_active if common_end < d < restart}
    concurrent = old_active.intersection(later)
    return (
        len(during) >= 2
        and len(concurrent) >= 2
        and min(concurrent) <= later[1]
        and all(not _matches(literal[d], old[d]) for d in concurrent)
    )


def select(evidence, symbol, owner, unit_rows, previous, restart, route, history, *, prior_anchor=False):
    family = _family(evidence, symbol, set(owner["member_alpaca_asset_ids"]))
    if not family:
        return None
    unit, quotes, events = family
    first, last = min(_active(unit_rows)), max(_active(unit_rows))
    common = symbol[:-2]
    old = _old_separation(events, unit, common, previous, restart, quotes)
    if old is None or prior_anchor and not old:
        return None
    # A native common/warrant separation plus both component mergers provides
    # a dated earlier unit UUID. A later common rename instead locates the
    # second common issue independently of today's symbol or exchange.
    until = (date.fromisoformat(last) + timedelta(days=10)).isoformat()
    if not old:
        if not any(
            e["action_type"] == "reverse_split"
            and e.get("symbol") == symbol
            and e.get("ex_date") in surrounding_market_sessions(previous)[1:]
            for e in events
        ):
            return None
        terms = [
            e
            for e in history(common, last, until)
            if _issued(e)
            and e.get("action_type") == "name_change"
            and e.get("old_symbol") == common
            and e.get("new_symbol")
            and e["new_symbol"] != common
            and e.get("old_cusip")
            and e["old_cusip"] == e.get("new_cusip")
            and last <= e.get("process_date", "") <= until
        ]
        if len(terms) != 1:
            return None
        (terminal,) = terms
    primary = route(common, "-")
    old_common = route(common, previous)
    new_common = route(common, restart)
    common_end = old["terminal_action"]["effective_date"] if old else previous
    if not _paired_tapes(primary, old_common, new_common, first, restart, last, common_end):
        return None
    if old:
        return {**old, "reason": "terminal_unit_separation_and_distinct_later_common", "common_symbol": common}
    # The named successor's route establishes the same later common trades.
    successor = route(terminal["new_symbol"], terminal["process_date"])
    late = {d for d in _active(primary) if d >= restart}
    if _active(successor) != late or any(not _matches(primary[d], successor[d]) for d in late):
        return None
    return {
        "reason": "reissued_unit_and_distinct_dated_common",
        "retained_side": "later",
        "common_symbol": common,
        "common_terminal_action_id": terminal["id"],
    }

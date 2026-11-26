"""Interpret a paired cash distribution and a later departure of the paying issue."""

from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, timedelta

from libs.common.us_equity_market_session_calendar import next_market_session_after

from .action_identity import exact_cusip_lookup_asset, exact_symbol_lookup_asset
from .current_symbol_reuse import _complete_corporate_action_query, _consensus_has_every_adjustment
from .name_change import (
    _bridge_structural_conflict,
    _known_owners,
    _non_observation_conflict,
    _positive_decimal,
    name_change_date,
)
from .ownership_interval_rules import AppliedOwnershipIntervalRule, TickerOwnershipInterval

VERSION = 106
METHOD = "observed_trading_after_paired_cash_distribution"


@dataclass(frozen=True)
class DueBillCashTerms:
    symbol: str
    cusip: str
    asset_id: str
    ex_date: date
    departure_date: date
    replacement_symbol: str
    replacement_cusip: str
    replacement_asset_id: str
    cash_merger_id: str
    dividend_id: str
    departure_id: str
    timing_basis: str

    @property
    def action_ids(self):
        return self.cash_merger_id, self.dividend_id, self.departure_id


def positive_activity(volume, trade_count):
    return _positive_decimal(volume) is not None and _positive_decimal(trade_count) is not None


def _date(value):
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _distribution_timing(dividend, ex_date):
    dates = {}
    for key in ("due_bill_on_date", "due_bill_off_date", "record_date", "payable_date"):
        value = dividend.get(key)
        if value is not None:
            if (day := _date(value)) is None:
                return None
            dates[key] = day
    on, off = dates.get("due_bill_on_date"), dates.get("due_bill_off_date")
    record, payable = dates.get("record_date"), dates.get("payable_date")
    if (
        (off is not None and off != ex_date)
        or (on is not None and on >= ex_date)
        or (record is not None and record > ex_date)
        or (
            payable is not None
            and (
                payable > ex_date
                or (record is not None and record > payable)
                or (on is not None and on > payable)
            )
        )
    ):
        return None
    if on is not None and off is not None:
        return "native_due_bill_dates"
    if record is not None and payable is not None and record <= payable < ex_date:
        return "special_distribution_payable_before_ex_date"
    return None


def _retired_listed(asset, label):
    return (
        asset is not None
        and asset.get("symbol") == label
        and asset.get("status") == "inactive"
        and asset.get("exchange") not in (None, "", "OTC")
    )


def _contrary_evidence(lineage, symbol, cusip, asset_id, first, last, terms, exact_endpoints):
    ids = tuple(action["id"] for action in terms)
    if (
        _known_owners(lineage, symbol) - {asset_id}
        or _non_observation_conflict(lineage, symbol, first, last, {(asset_id, cusip)}, ids)
        or _bridge_structural_conflict(lineage, {symbol}, first, last, ids[0], other_allowed=ids)
    ):
        return True
    selected = {action["id"]: action for action in terms}
    for action in lineage.get("corporate_actions", ()):
        held = selected.get(action.get("id"))
        if (
            held is not None
            and held is not action
            and any(
                action[key] != held[key]
                and not (key == "rate" and _positive_decimal(action[key]) == _positive_decimal(held[key]))
                for key in held.keys() & action.keys()
            )
        ):
            return True
        if (
            held is None
            and action.get("action_type") == "cash_dividend"
            and action.get("symbol") == symbol
            and action.get("special") is True
            and action.get("ex_date") == first.isoformat()
        ):
            return True
    for lookup in lineage.get("asset_lookups", ()):
        expected = exact_endpoints.get((lookup.get("lookup_kind"), lookup.get("lookup")))
        if expected is not None and lookup.get("found") is True:
            asset = lookup.get("asset") or {}
            if (asset.get("id"), asset.get("symbol")) != expected:
                return True
    return False


def due_bill_cash_terms(lineage, symbol, *, peers=()):
    """Compare held native terms and exact endpoints; this does not select pricing dates or merge UUIDs."""
    actions = lineage.get("corporate_actions", ())
    if not isinstance(actions, (list, tuple)) or any(not isinstance(action, dict) for action in actions):
        return None
    mergers = [
        a for a in actions if a.get("action_type") == "cash_merger" and a.get("acquiree_symbol") == symbol
    ]
    if len(mergers) != 1:
        return None
    merger = mergers[0]
    cusip = merger.get("acquiree_cusip")
    ex_date = _date(merger.get("effective_date"))
    rate = _positive_decimal(merger.get("rate"))
    if not cusip or not ex_date or rate is None:
        return None
    dividends = [
        a
        for a in actions
        if a.get("action_type") == "cash_dividend"
        and a.get("symbol") == symbol
        and a.get("special") is True
        and a.get("ex_date") == ex_date.isoformat()
    ]
    if len(dividends) != 1:
        return None
    dividend = dividends[0]
    timing = _distribution_timing(dividend, ex_date)
    if (
        timing is None
        or dividend.get("cusip") != cusip
        or _positive_decimal(dividend.get("rate")) != rate
        or (merger.get("currency") and dividend.get("currency") and merger["currency"] != dividend["currency"])
    ):
        return None
    exits = [
        a
        for a in actions
        if a.get("action_type") == "name_change"
        and a.get("old_symbol") == symbol
        and a.get("old_cusip") == cusip
    ]
    if len(exits) != 1:
        return None
    departure = exits[0]
    later, issue = name_change_date(departure, actions)
    new_symbol, new_cusip = departure.get("new_symbol"), departure.get("new_cusip")
    ids = (merger.get("id"), dividend.get("id"), departure.get("id"))
    if (
        issue
        or not ex_date < later
        or new_symbol in (None, "", symbol)
        or new_cusip in (None, "", cusip)
        or not all(isinstance(value, str) and value for value in ids)
        or len(set(ids)) != len(ids)
        or not isinstance(lineage.get("query"), dict)
        or lineage["query"].get("input_symbol") != symbol
        or not _complete_corporate_action_query(lineage, ex_date, later)
    ):
        return None
    named = exact_symbol_lookup_asset(lineage, symbol)
    old, new = exact_cusip_lookup_asset(lineage, cusip), exact_cusip_lookup_asset(lineage, new_cusip)
    if (
        not _retired_listed(named, symbol)
        or not _retired_listed(old, symbol)
        or (named["id"], named["exchange"]) != (old["id"], old["exchange"])
        or new is None
        or new.get("symbol") != new_symbol
        or new.get("status") != "inactive"
        or new["id"] == old["id"]
    ):
        return None
    endpoints = {
        ("symbol", symbol): (old["id"], symbol),
        ("cusip", cusip): (old["id"], symbol),
        ("cusip", new_cusip): (new["id"], new_symbol),
    }
    if any(
        _contrary_evidence(
            item, symbol, cusip, old["id"], ex_date, later, (merger, dividend, departure), endpoints
        )
        for item in (lineage, *peers)
    ):
        return None
    return DueBillCashTerms(
        symbol, cusip, old["id"], ex_date, later, new_symbol, new_cusip, new["id"], *ids, timing
    )


def traded_intervals(symbol, lineage, intervals, edges, work_items, consensus, *, peers=()):
    """Keep only observed active sessions inside the cash-to-replacement window, using the held raw grid."""
    terminals = [
        item
        for item in intervals
        if item.historical_symbol == symbol
        and item.end_date_exclusive is not None
        and {rule.name for rule in item.applied_rules}
        >= {"name_change_ticker_transition", "cash_merger_acquiree_terminal_boundary"}
    ]
    if len(terminals) != 1:
        return ()
    terminal = terminals[0]
    terms = due_bill_cash_terms(lineage, symbol, peers=peers)
    if terms is None or (
        terminal.identity_key() != (symbol, terms.cusip, terms.asset_id)
        or terminal.end_date_exclusive != terms.ex_date
        or any(edge.action_id == terms.departure_id for edge in edges)
        or not {terms.cash_merger_id, terms.departure_id}
        <= {action_id for rule in terminal.applied_rules for action_id in rule.supporting_action_ids}
    ):
        return ()
    if consensus is None or not _consensus_has_every_adjustment(consensus):
        return ()
    routes = {
        (source.get("symbol"), source.get("timeframe"), source.get("feed"), source.get("asof"))
        for _, source, _ in work_items
    }
    if len(routes) != 1:
        return ()
    label, timeframe, feed, asof = routes.pop()
    if (label, timeframe, feed) != (symbol, "1Day", "sip") or (
        asof != "-" and (_date(asof) or date.min) < terms.ex_date
    ):
        return ()
    rows, sessions = next(
        (download.rows, grid) for download, source, grid in work_items if source["adjustment"] == "raw"
    )
    start, stop = bisect_left(sessions, terms.ex_date), bisect_left(sessions, terms.departure_date)
    if start == stop or sessions[stop - 1] <= terms.ex_date:
        return ()
    ranges = []
    for index in range(start, stop):
        row = rows[index]
        if positive_activity(row.get("v"), row.get("n")):
            day = sessions[index]
            if ranges and next_market_session_after(ranges[-1][1].isoformat()) == day.isoformat():
                ranges[-1] = (ranges[-1][0], day)
            else:
                ranges.append((day, day))
    if not ranges or ranges[-1][1] <= terms.ex_date:
        return ()
    cash_rule = next(
        rule for rule in terminal.applied_rules if rule.name == "cash_merger_acquiree_terminal_boundary"
    )
    provenance = AppliedOwnershipIntervalRule(cash_rule.order, METHOD, terms.action_ids)
    return tuple(
        TickerOwnershipInterval(
            symbol, terms.cusip, terms.asset_id, first, last + timedelta(days=1), (provenance,)
        )
        for first, last in ranges
    )

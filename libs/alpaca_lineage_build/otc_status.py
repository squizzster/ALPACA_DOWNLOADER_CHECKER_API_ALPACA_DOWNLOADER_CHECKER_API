"""A bounded OTC status-symbol action filter for fresh historical SIP routes."""

import re
from bisect import bisect_left
from decimal import Decimal, InvalidOperation

from libs.alpaca_data_api_solver import NEW_YORK, instant, valid_cusip
from libs.alpaca_end_of_day_security_correlation.former_ticker import VERSION as FORMER_TICKER_VERSION
from libs.alpaca_end_of_day_security_correlation.occupancy_anchor import positive_occupancy_action_date
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
from libs.alpaca_security_lineage.cusip_discovery import missing_intervals
from libs.alpaca_security_lineage.lineage import action_date, action_identity_key

from .trade_tape import _same_pricing_mapping, listed_sip_trade, record_mapped_samples


def _covered_parts(provider, label, start, end):
    parts = []
    for (a, b), rows in provider.action_intervals.get(label, {}).items():
        if a <= start and end <= b:
            return [(a, b, rows)]
        if a <= end and start <= b:
            parts.append((a, b, rows))
    return parts


def _rows(parts, start, end):
    return {
        action_identity_key(row): row for _, _, rows in parts for row in rows if start <= action_date(row) <= end
    }


def _accept_full_window(provider, label, start, end):
    """Confirmed symbols enter ordinary traversal with their completed action coverage."""
    key = label, start, end
    if provider._covered(key):
        return
    parts = _covered_parts(provider, label, start, end)
    for a, b in missing_intervals(start, end, [(a, b) for a, b, _ in parts]):
        parts.append((a, b, provider.get_corporate_actions(label, a, b)))
    joined = list(_rows(parts, start, end).values())
    provider.actions[key] = joined
    provider.action_intervals[label][start, end] = joined


def _same_issue(lookup, symbol, owner, cusip):
    asset = lookup.get("asset") or {}
    return (
        lookup.get("outcome") == "found"
        and lookup.get("http_status") == 200
        and (asset.get("class"), asset.get("id"), asset.get("symbol")) == ("us_equity", owner, symbol)
        and (not valid_cusip(asset.get("cusip")) or asset["cusip"] == cusip)
    )


def _matched(window, query, rows, seen):
    raw = getattr(window.status_view, "views", {}).get("raw")
    for row in sorted(rows, key=action_date):
        cusip = row.get("cusip")
        if (
            row.get("action_type") != "cash_dividend"
            or row.get("symbol") != query["filter_symbol"]
            or not valid_cusip(cusip)
        ):
            continue
        try:
            ex = positive_occupancy_action_date(row, query["filter_symbol"], cusip)
        except (KeyError, ValueError, TypeError, InvalidOperation):
            continue
        if not ex or cusip in seen:
            continue
        day = ex.isoformat()
        if not query["first_session"] <= day <= query["mapped_sip_session"]:
            continue
        if raw is not None and day != query["mapped_sip_session"]:
            pos = bisect_left(raw, day, key=lambda bar: bar[0][:10])
            if (
                pos == len(raw)
                or raw[pos][0][:10] != day
                or len(raw[pos]) < 7
                or not any(Decimal(value) > 0 for value in raw[pos][5:7])
            ):
                continue
        seen.add(cusip)
        try:
            lookup = window.client.get_asset_lookup(cusip)
        except IdentityHttpError as error:
            if error.status not in (403, 404):
                raise
            lookup = {"outcome": "unavailable", "http_status": error.status, "asset": None}
        asset = lookup.get("asset") or {}
        query["dividend_lookups"].append(
            {
                "action_id": row["id"],
                "action_type": row["action_type"],
                "symbol": row["symbol"],
                "ex_date": day,
                "process_date": row.get("process_date"),
                "rate": row["rate"],
                "cusip": cusip,
                "lookup": cusip,
                "lookup_kind": "cusip",
                "outcome": lookup.get("outcome"),
                "http_status": lookup.get("http_status"),
                "asset_id": asset.get("id"),
                "asset_symbol": asset.get("symbol"),
                "asset_class": asset.get("class"),
            }
        )
        if _same_issue(lookup, query["symbol"], query["asset_id"], cusip):
            query["confirmed_cusip"] = cusip
            query["confirmed_ex_date"] = day
            return row["symbol"]
    return None


def _observe(window, query, start, end):
    provider = window.client
    label = query["filter_symbol"]
    seen = set()
    parts = _covered_parts(provider, label, start, end)
    query["action_ranges"] = [
        {"start": a, "end": b, "http_status": 200, "source": "completed_symbol_actions"} for a, b, _ in parts
    ]
    rows = _rows(parts, start, end)
    if provider.action_conflicts:
        provider._check_action_conflicts(rows.values())
    match = _matched(window, query, rows.values(), seen)
    missing = list(missing_intervals(start, end, [(a, b) for a, b, _ in parts]))
    if not match and missing:
        # A fragmented earlier response may leave several windows. Select at most
        # one unobserved range overlapping the priced episode for discovery.
        span = next(
            ((a, b) for a, b in missing if a <= query["mapped_sip_session"] and query["first_session"] <= b),
            None,
        )
        if span:
            a, b = span
            entry = {"start": a, "end": b, "source": "symbol_actions"}
            query["action_ranges"].append(entry)
            try:
                values = provider.get_corporate_actions(label, a, b)
            except IdentityHttpError as error:
                if error.status not in (403, 404):
                    raise
                entry["http_status"] = error.status
                query["outcome"] = "unavailable"
                return None, query
            entry["http_status"] = 200
            rows.update((action_identity_key(row), row) for row in values)
            match = _matched(window, query, values, seen)
            missing.remove(span)
    if match:
        query["outcome"] = "confirmed"
    elif query["dividend_lookups"]:
        query["outcome"] = (
            "cusip_mismatch"
            if any(item["outcome"] == "found" for item in query["dividend_lookups"])
            else "cusip_unavailable"
        )
    elif missing:
        query["outcome"] = "partial_action_coverage"
    else:
        query["outcome"] = "no_matching_dividend" if rows else "empty"
    return match, query


def discover(window, own, start, end):
    symbol, view = window.symbol, window.status_view
    asset = own.get("asset") or {}
    if not (
        re.fullmatch(r"[A-Z]{4}Q", symbol)
        and own.get("outcome") == "found"
        and own.get("http_status") == 200
        and (asset.get("symbol"), asset.get("class"), asset.get("exchange")) == (symbol, "us_equity", "OTC")
        and asset.get("id")
    ):
        return None, None
    if window.previous:
        previous_policy = window.previous.get("versions", {}).get("correlation")
        if isinstance(previous_policy, int) and previous_policy < FORMER_TICKER_VERSION:
            return None, None
        previous = window.previous["evidence"]
        saved, mapped = previous.get("otc_status_symbol_query"), previous.get("mapped_historical_trade_tape")
        if (
            not saved
            or saved.get("filter_symbol") != symbol[:-1]
            or saved.get("asset_id") != asset["id"]
            or not mapped
        ):
            return None, None
        if view is None or not _same_pricing_mapping(mapped, view.source["latest_request"], asset["id"]):
            return None, None
        if saved["outcome"] == "confirmed" and saved["filter_symbol"] in previous["queried_symbols"]:
            if getattr(view, "source_sha256", None) != saved["source_generation"]:
                raw = next((d for d in view.local["downloads"] if d.get("adjustment") == "raw"), {})
                if raw.get("rows") == 0:
                    return None, None
                if (
                    raw.get("from")
                    and raw.get("to")
                    and not raw["from"][:10] <= saved["confirmed_ex_date"] <= raw["to"][:10]
                ):
                    return None, None
            cusip = saved["confirmed_cusip"]
            observed = window.client.assets.get(cusip)
            if observed is not None and (
                not isinstance(observed, dict) or not _same_issue(observed, symbol, asset["id"], cusip)
            ):
                return None, None
            return saved["filter_symbol"], saved
        return None, saved
    if (
        view is None
        or not window.allow_status_search
        or getattr(view, "materialization_scope", "complete_history") != "complete_history"
        or getattr(view, "previous", None) is not None
        or getattr(view, "accepted_report", None) is not None
        or getattr(view, "retained_split", None) is not None
    ):
        return None, None
    # This is the ordinary mapped-tape producer. Carry its completed positive or
    # negative observation directly into the report assembled by the caller.
    tapes = window.early_mapped_samples = {}
    record_mapped_samples(view, tapes, window, refresh=False)
    mapped = tapes.get("mapped_historical_trade_tape") or {}
    listed = listed_sip_trade(mapped)
    if not listed:
        return None, None
    raw = next((item for item in view.local["downloads"] if item.get("adjustment") == "raw"), {})
    if not raw.get("from"):
        return None, None
    first, last = instant(raw["from"]).astimezone(NEW_YORK).date().isoformat(), mapped["session"]
    generation = getattr(view, "source_sha256", None)
    if not generation:
        return None, None
    binding = symbol, asset["id"], generation, mapped["asof"], first, last, start, end
    if listed[0] != last:
        binding += listed
    provider = window.client
    pending = ("otc_status_symbol_query", *binding)
    with provider._trade_condition:
        while pending in provider._trade_pending:
            provider._trade_condition.wait()
        provider._trade_pending.add(pending)
    try:
        if binding not in provider.otc_status_queries:
            query = {
                "symbol": symbol,
                "asset_id": asset["id"],
                "filter_symbol": symbol[:-1],
                "first_session": first,
                "last_session": last,
                "pricing_asof": mapped["asof"],
                "source_generation": generation,
                "actions_start": start,
                "actions_end": end,
                "mapped_sip_session": listed[0],
                "mapped_sip_tape": listed[1],
                "dividend_lookups": [],
            }
            provider.otc_status_queries[binding] = _observe(window, query, start, end)
        match, query = provider.otc_status_queries[binding]
        if match and "confirmed_symbol_action_coverage" not in query:
            _accept_full_window(provider, match, start, end)
            query["confirmed_symbol_action_coverage"] = {"start": start, "end": end}
        return match, query
    finally:
        with provider._trade_condition:
            provider._trade_pending.remove(pending)
            provider._trade_condition.notify_all()

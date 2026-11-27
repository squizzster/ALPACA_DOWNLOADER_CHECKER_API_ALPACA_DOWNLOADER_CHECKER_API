"""Reuse completed physical literal SIP auction routes on their inclusive date ranges."""

import json
from datetime import date, timedelta

from libs.common.us_equity_market_session_calendar import market_session_window


class PartialAuctionRows(list):
    """Positive native rows from covered slices when another slice returned 404."""

    available = False


def _day(value, offset):
    return (date.fromisoformat(value) + timedelta(days=offset)).isoformat()


def _overlaps(cache, symbol, first, last):
    # JSON request keys use the observation cache's fixed json.dumps spelling. Seek
    # within the existing (kind,request) primary key even on an earlier stage database.
    prefix = json.dumps([symbol])[:-1] + ", "
    where = """request BETWEEN ? AND ? AND json_extract(request,'$[1]')<=?
               AND json_extract(request,'$[2]')>=?"""
    args = prefix, prefix + "\uffff", last, first
    with cache.lock:
        rows = list(
            cache.db.execute(
                f"""SELECT request,kind FROM observations WHERE kind='auction_route' AND {where}
                UNION ALL SELECT request,kind FROM observations s WHERE kind='screen' AND {where}
                AND NOT EXISTS (SELECT 1 FROM observations n WHERE n.kind IN ('auction_route','auction_nonroute')
                                AND n.request=s.request)""",
                (*args, *args),
            )
        )
    return {tuple(json.loads(request)[1:]): kind for request, kind in rows}


def _covered(cache, symbol, source, kind, first, last):
    key = [symbol, *source]
    if kind == "legacy":
        # Before interval reuse every completed available screen used exactly one
        # physical route. Calendar-empty screens acquired no physical pages.
        return cache.read_auction_route(symbol, *source, first, last, completed=True)
    route = cache.get("auction_route", key)
    result = []
    for token, low, high in route["pages"]:
        if low is not None and low <= last and first <= high:
            page = cache.get("auction_page", [*key, token])
            if page is None:
                raise ValueError("missing completed historical auction page")
            if first <= low and high <= last:
                result.extend(page["rows"])
            else:
                result.extend(r for r in page["rows"] if first <= r["d"] <= last)
    return result


def auction_days(cache, symbol, first, last, selected_screen=None):
    if first > last:
        raise ValueError("unordered auction dates")
    candidates = _overlaps(cache, symbol, first, last)
    exact = first, last

    def available(scope):
        if candidates[scope] != "screen":
            return True
        key = [symbol, *scope]
        legacy = selected_screen if scope == exact and selected_screen is not None else cache.get("screen", key)
        if not legacy["auction_available"] or not legacy["candidates"]:
            cache.put("auction_nonroute", key, True)
            del candidates[scope]
            return False
        candidates[scope] = "legacy"
        return True

    if exact in candidates and available(exact):
        return _covered(cache, symbol, exact, candidates[exact], first, last)
    rows = []
    unavailable = False
    cursor = first
    ordered = sorted(candidates)
    i = 0
    while cursor <= last:
        possible = []
        while i < len(ordered) and ordered[i][0] <= cursor:
            if ordered[i][1] >= cursor:
                possible.append(ordered[i])
            i += 1
        # After the longest prefix, all other currently eligible spans are exhausted.
        # Disjoint and nested ranges therefore consume each selector at most once.
        source = next(
            (s for s in sorted(possible, key=lambda r: (r[1], r[0]), reverse=True) if available(s)), None
        )
        if source:
            end = min(source[1], last)
            held = _covered(cache, symbol, source, candidates[source], cursor, end)
        else:
            while i < len(ordered) and not available(ordered[i]):
                i += 1
            end = _day(ordered[i][0], -1) if i < len(ordered) else last
            if (cursor, end) == (first, last):
                return cache.read_auction_route(symbol, first, last, first, last)
            days = market_session_window(cursor, end)
            held = cache.read_auction_route(symbol, days[0], days[-1], days[0], days[-1]) if days else []
            if held is None:
                unavailable = True
        rows.extend(held or ())
        if end == last:
            break
        cursor = _day(end, 1)
    # A derived screen can no longer attest a single physical route. This tag
    # also keeps mixed/404 results outside the positive native coverage selector.
    cache.put("auction_nonroute", [symbol, first, last], True)
    if unavailable:
        return PartialAuctionRows(rows) if rows else None
    return rows

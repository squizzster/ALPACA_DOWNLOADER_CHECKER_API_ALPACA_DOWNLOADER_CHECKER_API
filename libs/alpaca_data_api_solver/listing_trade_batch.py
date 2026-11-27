"""One literal SIP trade window inside an already selected market session."""

from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache

from libs.common.us_equity_market_session_calendar import regular_market_hours

from . import NEW_YORK, PAGE_LIMIT, STOCK_URL, _canonical_equity_symbol, instant, iso, trade_tape_request

MAX_BATCH_SYMBOLS = 48


@lru_cache(maxsize=8192)
def opening_second(session):
    """The inclusive native trade query bounds in UTC for this calendar session."""
    if not isinstance(session, str) or date.fromisoformat(session).isoformat() != session:
        raise ValueError("historical listing batch requires one ISO session")
    opening, _ = regular_market_hours(session)
    local_open = datetime.combine(date.fromisoformat(session), time.fromisoformat(opening), NEW_YORK)
    start = iso(local_open.astimezone(timezone.utc))
    return start, start.removesuffix("Z") + ".999999999Z"


def _labels(symbols):
    if isinstance(symbols, str):
        raise ValueError("listing trade request requires a collection of tickers")
    symbols = tuple(symbols)
    if not symbols or any(not _canonical_equity_symbol(symbol) for symbol in symbols):
        raise ValueError("historical listing batch requires canonical US-equity tickers")
    return sorted(set(symbols))


def _request(labels, start, end, limit):
    return {
        "url": f"{STOCK_URL}/trades",
        "params": {
            "symbols": ",".join(labels),
            "feed": "sip",
            "asof": "-",
            "start": start,
            "end": end,
            "sort": "asc",
            "limit": limit,
        },
    }


def _session_second(value, session):
    stamp = instant(value)
    if stamp.microsecond or iso(stamp) != value or stamp.astimezone(NEW_YORK).date().isoformat() != session:
        raise ValueError("auction bounds must be canonical UTC whole seconds inside the selected session date")
    return stamp


def listing_trade_request(
    symbols, session, *, second=None, regular_minute=False, window_start=None, window_end=None
):
    ranged = window_start is not None or window_end is not None
    if type(regular_minute) is not bool or sum((regular_minute, second is not None, ranged)) > 1:
        raise ValueError("select one listing trade window")
    labels = _labels(symbols)
    start, end = opening_second(session)
    if ranged:
        first, last = _session_second(window_start, session), _session_second(window_end, session)
        if not timedelta(0) <= last - first <= timedelta(seconds=59):
            raise ValueError("a listing trade window covers at most sixty ordered seconds")
        start, end = window_start, window_end.removesuffix("Z") + ".999999999Z"
    elif regular_minute:
        final_second = iso(instant(start) + timedelta(seconds=59))
        end = final_second.removesuffix("Z") + ".999999999Z"
    elif second is not None:
        _session_second(second, session)
        start, end = second, second.removesuffix("Z") + ".999999999Z"
    return _request(labels, start, end, PAGE_LIMIT)


def sparse_day_trade_request(symbols, session):
    labels = _labels(symbols)
    if not 2 <= len(labels) <= MAX_BATCH_SYMBOLS:
        raise ValueError("a sparse listing trade page requires between two and 48 tickers")
    day = trade_tape_request(labels[0], session)["params"]
    return _request(labels, day["start"], day["end"], 2 * len(labels))

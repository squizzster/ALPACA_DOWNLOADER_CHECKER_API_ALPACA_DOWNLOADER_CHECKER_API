"""Positive dated tape points from one restricted page for co-selected tickers."""

from urllib.parse import urlencode

from libs.alpaca_data_api_solver import instant
from libs.alpaca_data_api_solver.listing_trade_batch import (
    MAX_BATCH_SYMBOLS,
    listing_trade_request,
    sparse_day_trade_request,
)


def literal_listing_key(sample):
    """Return the scope of a point observation, including a completed conflicting result."""
    if (
        sample
        and sample.get("feed") == "sip"
        and sample.get("asof") == "-"
        and not sample.get("through_session")
        and sample.get("symbol")
        and sample.get("session")
        and sample.get("returned_symbol", sample["symbol"]) == sample["symbol"]
    ):
        return sample["symbol"], sample["session"]
    return None


def _recognized_prints(sample):
    if sample.get("http_status") == 200:
        if sample["outcome"] == "conflicting_tapes":
            return sample["conflicting_trades"]
        trade = sample.get("trade") or {}
        if sample["outcome"] == "observed" and trade.get("z") in ("A", "B", "C"):
            return [trade]
    return ()


def combine_listing_samples(previous, current):
    """Merge already observed literal prints on one symbol/date; retain a witness for each different tape."""
    if previous is None:
        return current
    if current is None or previous is current:
        return previous
    key = literal_listing_key(previous)
    if key is None or key != literal_listing_key(current):
        raise ValueError("cannot combine historical listing evidence from different literal routes")
    old, new = _recognized_prints(previous), _recognized_prints(current)
    if not new:
        if not old and previous.get("outcome") != "observed" and current.get("outcome") == "observed":
            return current  # A dated print can establish activity even when its tape is unknown.
        return previous
    if not old:
        return current
    witnesses = {}
    for trade in (*old, *new):
        tape, stamp = trade["z"], trade["t"]
        if tape not in witnesses or stamp < witnesses[tape]:
            witnesses[tape] = stamp
    if len(witnesses) == 1:
        return previous
    fingerprints = [{"z": tape, "t": witnesses[tape]} for tape in sorted(witnesses)]
    if previous.get("conflicting_trades") == fingerprints:
        return previous
    symbol, session = key
    return {
        "symbol": symbol,
        "returned_symbol": symbol,
        "session": session,
        "feed": "sip",
        "asof": "-",
        "http_status": 200,
        "sample_scope": "combined_literal_observations",
        "outcome": "conflicting_tapes",
        "conflicting_trades": fingerprints,
    }


def batch_trade_points(
    symbols, session, client, *, second=None, regular_minute=False, window_start=None, window_end=None
):
    """Omitted or paginated-away tickers retain their full-day fallback."""
    if isinstance(symbols, str):
        raise ValueError("listing trade batch requires a collection of tickers")
    labels = set(symbols)
    ranged = window_start is not None or window_end is not None
    if type(regular_minute) is not bool or sum((regular_minute, second is not None, ranged)) > 1:
        raise ValueError("select one listing trade window")
    if len(labels) < (8 if regular_minute else 2):
        return {}
    if len(labels) > MAX_BATCH_SYMBOLS:
        raise ValueError("too many tickers in one historical listing point batch")
    request = listing_trade_request(
        labels,
        session,
        second=second,
        regular_minute=regular_minute,
        window_start=window_start,
        window_end=window_end,
    )
    if ranged:
        scope = "auction_window"
    elif regular_minute:
        scope = "regular_open_minute"
    else:
        scope = "regular_open_second" if second is None else "auction_second"
    return _one_page(labels, session, request, scope, client)


def sparse_day_points(symbols, session, client):
    """One small symbol-major page; only a terminal page establishes full-day absence."""
    if isinstance(symbols, str):
        raise ValueError("listing trade batch requires a collection of tickers")
    labels = set(symbols)
    if len(labels) < 2:
        return {}
    return _one_page(labels, session, sparse_day_trade_request(labels, session), "sparse_day", client)


def _one_page(labels, session, request, scope, client):
    params = request["params"]
    url = request["url"] + "?" + urlencode(params)
    if len(url) > 7900:
        raise ValueError("historical listing point batch exceeds its single URL budget")
    document = client.request_json(url, allow_not_found=True)
    if document is None:
        return {}
    if not isinstance(document, dict):
        raise RuntimeError("invalid literal listing trade page")
    trades = document.get("trades")
    if trades is None and "trades" in document:
        trades = {}
    if not isinstance(trades, dict) or not trades.keys() <= labels:
        raise RuntimeError("invalid symbols in literal listing trade batch")
    # Each physical request ends at this page. A terminal response covers
    # the entire requested day only on the explicit sparse-day route.
    token = document.get("next_page_token")
    if token is not None and (not isinstance(token, str) or not token):
        raise RuntimeError("invalid listing trade page token")
    start, end = instant(params["start"]), instant(params["end"])
    full_day = scope == "sparse_day"
    common = {
        "session": session,
        "feed": "sip",
        "asof": "-",
        "sample_scope": scope,
        "start": params["start"],
        "end": params["end"],
        "limit": params["limit"],
        "http_status": 200,
    }
    if full_day:
        common["page_complete"] = token is None
    observed = {}
    size = 0
    for symbol, records in trades.items():
        if not isinstance(records, list):
            raise RuntimeError("invalid trade records in listing trade batch")
        size += len(records)
        witnesses = {}
        first_unknown = None
        for trade in records:
            if not isinstance(trade, dict):
                raise RuntimeError("invalid trade in listing trade batch")
            tape = trade.get("z")
            if tape is not None and not isinstance(tape, str):
                raise RuntimeError("invalid tape code in listing trade batch")
            recognized = tape in ("A", "B", "C")
            if not full_day and (not recognized or tape in witnesses):
                continue  # These extra records do not contribute to a bounded-window result.
            stamp = trade.get("t")
            if not isinstance(stamp, str) or len(stamp) <= 10 or not start <= instant(stamp) <= end:
                raise RuntimeError("listing trade outside the selected bounded window")
            if recognized:
                if tape not in witnesses:
                    witnesses[tape] = {"t": stamp, "z": tape}
            elif first_unknown is None:
                first_unknown = {"t": stamp, **({"z": tape} if tape is not None else {})}
        if witnesses or first_unknown:
            sample = {"symbol": symbol, "returned_symbol": symbol, **common}
            if not witnesses:
                sample.update(outcome="observed", trade=first_unknown)
            elif len(witnesses) == 1:
                sample.update(outcome="observed", trade=next(iter(witnesses.values())))
            else:
                sample.update(
                    outcome="conflicting_tapes", conflicting_trades=[witnesses[z] for z in sorted(witnesses)]
                )
            observed[symbol] = sample
    if size > params["limit"]:
        raise RuntimeError("oversized literal listing trade page")
    if full_day and not size and token is not None:
        raise RuntimeError("empty nonterminal sparse listing trade page")
    if full_day and token is None:
        for symbol in sorted(labels - observed.keys()):
            observed[symbol] = {"symbol": symbol, **common, "outcome": "no_trades"}
    return observed

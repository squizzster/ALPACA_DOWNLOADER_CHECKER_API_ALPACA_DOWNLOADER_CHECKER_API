"""Bind a repeated split to the selected price prefix and its provider action terms."""

from datetime import date

from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.duplicate_split import plan
from libs.alpaca_pricing_download.shifted_split import session_window

from .dated_price_publication import action_window
from .metadata import action_symbols


def _actions(evidence, symbol, first, through):
    for action in action_window(evidence, first, through):
        day = action.get("ex_date") or action.get("effective_date") or action.get("process_date")
        if day and first <= day <= through and symbol in action_symbols(action):
            yield action


def inputs(snapshot, security, review, ranges):
    if len(ranges) != 1:
        raise UnsupportedInterval("a cross-label prefix needs a separate share-basis acquisition")
    item = ranges[0]
    first, through, symbol = item["first_session"], item["last_session"], item["ticker"]
    observed = {}
    for receipt in snapshot._selected_receipts.values():
        for action in _actions(receipt["evidence"], symbol, first, through):
            key = action.get("id")
            if not key or key in observed and observed[key] != action:
                raise UnsupportedInterval("conflicting provider corporate-action observations")
            observed[key] = action
    info = plan(list(observed.values()), symbol, first, through, review)
    cache = {}
    views = snapshot.bounded_acquisition_views(security.public_id, first, info["witness_last"], cache=cache)
    closes = {}
    for _, prior, _ in info["cash"]:
        if prior > info["witness_last"] and prior not in closes:
            exact = snapshot.bounded_acquisition_views(
                security.public_id, prior, prior, modes=("raw",), cache=cache
            )
            bars = exact["raw"]["bars"]
            if len(bars) != 1 or bars[0]["tickers"] != [symbol] or bars[0]["session"] != prior:
                raise UnsupportedInterval("later cash term lacks the selected raw close")
            closes[prior] = bars[0]["close"]
    return {"plan": info, "views": views, "later_closes": closes}


def relevant_actions(evidence, interval):
    scope = interval["duplicate_scope"]
    first, last = scope["pair"]
    lo, hi = session_window(first)[0], session_window(last)[2]
    material = []
    for a in _actions(evidence, scope["symbol"], interval["first"], scope["through"]):
        day = a.get("ex_date") or a.get("effective_date") or a.get("process_date")
        if a.get("action_type") in ("reverse_split", "forward_split", "cash_dividend") or lo <= day <= hi:
            material.append(a)
    return sorted(material, key=lambda a: a["id"])


def valid_scope(interval):
    if interval.get("repair_kind") != "duplicate_split":
        return "duplicate_scope" not in interval
    scope = interval["duplicate_scope"]
    return (
        set(scope) == {"symbol", "review", "through", "pair"}
        and scope["symbol"] in interval["members"]
        and isinstance(scope["pair"], list)
        and len(scope["pair"]) == 2
        and interval["first"]
        < scope["pair"][0]
        < scope["pair"][1]
        <= interval["witness_end"]
        <= scope["through"]
        and interval["cut"] in scope["pair"]
        and scope["review"] in scope["pair"]
        and all(date.fromisoformat(value).isoformat() == value for value in (*scope["pair"], scope["through"]))
    )

"""Identify a historical split observation by date, terms and its literal traded tape."""

from decimal import Decimal

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import _positive_rate
from libs.alpaca_lineage_build.metadata import action_symbols
from libs.alpaca_security_lineage.inference_evidence import reject_assertions

from .dated_rename import _matches, provider_rows


def select(load, pivot, recheck, client, identity, get, put):
    from .price_basis_review import plan

    def stage(key, make):
        value = get(key)
        return put(key, make()) if value is None else value

    inputs = stage("historical:inputs", load)
    dates = inputs["dates"]
    if not dates or pivot not in dates[1:-1]:
        return {"outcome": "unsupported", "reason": "incomplete historical ticker window"}
    views = inputs["views"]
    raw = views["raw"]["bars"]
    if not raw or len(raw[0]["tickers"]) != 1:
        return {"outcome": "unsupported", "reason": "ambiguous published ticker"}
    published = raw[0]["tickers"][0]
    if [b["session"] for b in raw] != list(dates) or any(b["tickers"] != [published] for b in raw):
        return {
            "outcome": "unsupported",
            "reason": "historical review window has competing labels or missing dates",
        }
    base = stage("historical:existing_plan", lambda: plan(inputs, pivot, recheck))
    own_action = any(
        published in action_symbols(a)
        and (a.get("ex_date") or a.get("effective_date") or a.get("process_date")) == pivot
        for a in inputs["actions"]
    )
    if base.get("reason") != "missing or competing corporate-action explanation" or own_action:
        return base

    def literal(symbol):
        return provider_rows(client, symbol, "-", "raw", dates[0], dates[-1])

    if stage(
        "historical:published_trades", lambda: any(Decimal(v["volume"]) > 0 for v in literal(published).values())
    ):
        return {
            "outcome": "unsupported",
            "reason": "published literal ticker already trades within the disputed window",
        }
    previous, current = views["split"]["bars"]
    by_day = {b["session"]: b for b in raw}
    multiple = (Decimal(previous["close"]) / Decimal(by_day[previous["session"]]["close"])) / (
        Decimal(current["close"]) / Decimal(by_day[current["session"]]["close"])
    )
    if max(multiple, 1 / multiple) < 2 or sum(Decimal(b["volume"]) > 0 for b in raw) < 3:
        return {
            "outcome": "unsupported",
            "reason": "historical window lacks a material share step or enough traded observations",
        }
    actions = stage("historical:market_terms", lambda: identity.get_market_splits(pivot))
    possible = {}
    for a in actions:
        reject_assertions(a, where="historical provider action")
        if a.get("ex_date") != pivot or a.get("action_type") not in {"forward_split", "reverse_split"}:
            continue

        old, new = (_positive_rate(a.get(k)) for k in ("old_rate", "new_rate"))
        if (
            old
            and new
            and Decimal(".98") <= old / new / multiple <= Decimal("1.02")
            and a.get("symbol") != published
        ):
            possible.setdefault(a["symbol"], []).append(a)

    def matches(ticker):
        bars = literal(ticker)
        return all(b["session"] in bars and _matches(b, bars[b["session"]]) for b in raw)

    winners = [s for s in sorted(possible) if stage("historical:literal:" + s, lambda: matches(s))]
    if len(winners) != 1:
        return {
            "outcome": "unsupported",
            "reason": "historical actions and literal prices do not select one traded ticker",
        }
    symbol = winners[0]
    projected = {m: {"bars": [{**b, "tickers": [symbol]} for b in pack["bars"]]} for m, pack in views.items()}
    return {
        **plan(dict(dates=dates, views=projected, actions=possible[symbol]), pivot, recheck),
        "published_symbol": published,
    }

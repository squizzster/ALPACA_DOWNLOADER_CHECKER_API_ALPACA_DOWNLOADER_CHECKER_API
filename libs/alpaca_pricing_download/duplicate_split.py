"""Remove a repeated provider split when the tape records one change of share units."""

from datetime import date, timedelta
from decimal import Decimal

from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .carried_rename import _traded
from .dated_rename import UnsupportedInterval, _fields
from .rename_split_basis import _SPLITS, _factor, scaled
from .shifted_split import _PAIRS, _PRICES, _quantum, _rounded, _scoped_rows, session_window


def plan(actions, symbol, first, through, review):
    """Select native factor events and the exact bar requests needed by this assessment."""
    splits = sorted(
        (a for a in actions if a.get("symbol") == symbol and a.get("action_type") in _SPLITS),
        key=lambda a: a.get("ex_date") or "",
    )
    terms = [(a.get("ex_date"), str(_factor(a))) for a in splits]
    if len(set(day for day, _ in terms)) != len(terms):
        raise UnsupportedInterval("competing split records share an effective date")
    pairs = []
    for earlier, later in zip(splits, splits[1:]):
        left, right = earlier["ex_date"], later["ex_date"]
        if (
            review not in (left, right)
            or earlier["action_type"] != later["action_type"]
            or _factor(earlier) != _factor(later)
        ):
            continue
        if left not in (session_window(right)[0], session_window(session_window(right)[0])[0]):
            continue
        old, new = earlier.get("old_cusip") or "", earlier.get("new_cusip") or ""
        if (old, new) != (later.get("old_cusip") or "", later.get("new_cusip") or ""):
            continue
        if (old or new) and (not old or not new or old == new):
            continue
        if any(a.get("new_symbol") not in (None, "", symbol) for a in (earlier, later)):
            continue
        pairs.append((left, right, str(_factor(earlier))))
    if len(pairs) != 1:
        raise UnsupportedInterval("no unique close pair of repeated split terms applies")
    left, right, factor = pairs[0]
    witness_first, witness_last = session_window(left)[0], session_window(right)[2]
    if first > witness_first or witness_last > through:
        raise UnsupportedInterval("repeated terms lack bracketing published sessions")
    cash = {}
    for a in actions:
        kind = a.get("action_type")
        day = a.get("ex_date") or a.get("effective_date") or a.get("process_date")
        if kind in _SPLITS:
            continue
        if witness_first <= day <= witness_last:
            raise UnsupportedInterval("an additional action competes inside the duplicate split window")
        if kind == "cash_dividend" and a.get("symbol") == symbol and a.get("ex_date"):
            rate = Decimal(str(a.get("rate") or 0))
            if not rate.is_finite() or rate <= 0:
                raise UnsupportedInterval("cash distribution lacks usable provider terms")
            cash[day] = cash.get(day, Decimal(0)) + rate
    payments = [[day, session_window(day)[0], str(rate)] for day, rate in sorted(cash.items()) if first < day]
    return dict(
        symbol=symbol,
        first=first,
        through=through,
        pair=[left, right],
        factor=factor,
        splits=terms,
        cash=payments,
        witness_first=witness_first,
        witness_last=witness_last,
        review=review,
    )


def _removed(raw, plan):
    left, right = plan["pair"]
    step = Decimal(plan["factor"])
    direct, unchanged = [], []
    for d in (left, right):
        before = raw[session_window(d)[0]]
        current = raw[d]
        if not _traded(before) or not _traded(current):
            raise UnsupportedInterval("a repeated split needs actual trades on each side of both event dates")
        moves = [Decimal(current[k]) / Decimal(before["close"]) for k in ("open", "close", "vwap")]
        if all(Decimal("0.75") <= m / step <= Decimal("1.25") for m in moves):
            direct.append(d)
        if all(Decimal("0.75") <= m <= Decimal("1.25") for m in moves):
            unchanged.append(d)
    if len(direct) != len(unchanged) or len(direct) != 1 or direct == unchanged:
        raise UnsupportedInterval("traded prices do not identify exactly one realized split")
    return unchanged[0]


def _prices(bar, factor):
    result = scaled(bar, factor)
    return {k: result[k] for k in _PRICES}


def _latent_vwap(raw, observed, native, corrected):
    # The published raw VWAP has at most six decimal places. A scaled provider
    # VWAP can retain more information about the underlying weighted price.
    lo = max(raw - Decimal("0.0000005"), (observed - _quantum(observed.next_minus()) / 2) / native)
    hi = min(raw + Decimal("0.0000005"), (observed + _quantum(observed) / 2) / native)
    if lo >= hi or _rounded(lo * corrected) != _rounded(hi * corrected):
        raise UnsupportedInterval("weighted price observations do not determine one corrected VWAP")
    return str(_rounded(lo * corrected))


def assess(inputs):
    from libs.alpaca_serving_catalogue.policy import _bar_reasons

    info = inputs["plan"]
    first, last = info["first"], info["witness_last"]
    dates = tuple(b["session"] for b in inputs["views"]["raw"]["bars"])
    required = expected_market_session_coverage(
        info["witness_first"], (date.fromisoformat(last) + timedelta(days=1)).isoformat()
    ).session_dates
    if (
        not dates
        or dates[0] != first
        or dates[-len(required) :] != required
        or list(dates) != sorted(set(dates))
    ):
        raise UnsupportedInterval("duplicate split witnesses do not cover the selected trading sessions")
    rows, symbol = _scoped_rows(inputs["views"], dates)
    if symbol != info["symbol"]:
        raise UnsupportedInterval("repeated split crosses a different ticker")
    raw = {r["session"]: r for r in rows["raw"]}
    cut = _removed(raw, info)
    anchors = {day: Decimal(value) for day, value in inputs["later_closes"].items()}
    events = []
    for day, ratio in info["splits"]:
        events.append((day, Decimal(ratio), False))
    for day, preceding, rate in info["cash"]:
        prior = Decimal(raw[preceding]["close"]) if preceding in raw else anchors.get(preceding)
        if not prior or not Decimal(0) < Decimal(rate) < prior:
            raise UnsupportedInterval("dividend-adjusted prices lack a usable preceding raw session")
        events.append((day, 1 - Decimal(rate) / prior, True))
    events.sort(reverse=True)
    cash = share = Decimal(1)
    cursor = 0
    factor = Decimal(info["factor"])
    bundles = {mode: {} for mode in rows}
    for index in range(len(dates) - 1, -1, -1):
        day = dates[index]
        while cursor < len(events) and events[cursor][0] > day:
            _, value, dividend = events[cursor]
            if dividend:
                cash *= value
            else:
                share *= value
            cursor += 1
        r = raw[day]
        new_share = share / factor if day < cut else share
        native_split = scaled(r, share)
        native = rows["split"][index]
        corrected = scaled(r, new_share) if day < cut else native_split
        if _fields(native_split) != _fields(native):
            for k in _PRICES[:-1]:
                if Decimal(native_split[k]) == Decimal(native[k]):
                    continue
                value = Decimal(r[k]) * share
                if day < cut or abs(value - Decimal(native[k])) != _quantum(value) / 2:
                    raise UnsupportedInterval("native split prices contradict the provider share terms")
                # An unmodified witness at an exact half-quantum can exhibit
                # a different binary tie direction under compound scaling.
                native_split[k] = native[k]
            if any(Decimal(native_split[k]) != Decimal(native[k]) for k in ("volume", "trade_count")):
                raise UnsupportedInterval("native split views contradict the provider share terms")
            if Decimal(native_split["vwap"]) != Decimal(native["vwap"]):
                if cash != 1:
                    raise UnsupportedInterval("weighted price needs joint cash and share precision evidence")
                corrected["vwap"] = _latent_vwap(Decimal(r["vwap"]), Decimal(native["vwap"]), share, new_share)
                native_split["vwap"] = native["vwap"]
        # A cash dividend affects prices only. The spin-only view must retain
        # raw terms; a separate spin basis cannot be inferred from cash terms.
        cash_prices = _prices(r, cash) if cash != 1 else r
        old_combined = _prices(r, cash * share) if cash != 1 else native_split
        new_combined = _prices(r, cash * new_share) if day < cut and cash != 1 else corrected
        for mode, bars in rows.items():
            b = bars[index]
            has_cash = mode == "all" or "dividend" in mode
            has_split = mode in _PAIRS
            if mode not in ("raw", "split"):
                expected = (
                    (old_combined if has_cash else native_split)
                    if has_split
                    else (cash_prices if has_cash else r)
                )
                units = native_split if has_split else r
                if any(Decimal(b[k]) != Decimal(expected[k]) for k in _PRICES) or any(
                    Decimal(b[k]) != Decimal(units[k]) for k in ("volume", "trade_count")
                ):
                    raise UnsupportedInterval(
                        "other adjustment views do not support the observed split and cash terms"
                    )
            if day < cut:
                replacement = new_combined if has_cash else corrected
                bundles[mode][day] = (
                    {**b, **{k: replacement[k] for k in _PRICES}, "volume": corrected["volume"]}
                    if has_split
                    else b
                )
                if has_split and _bar_reasons(bundles[mode][day], mode):
                    raise UnsupportedInterval("corrected bar violates the price or activity domain")
    # The standalone reader consumes chronological bundles.
    bundles = {m: dict(reversed(list(days.items()))) for m, days in bundles.items()}
    return bundles, dict(
        first=first,
        cut=cut,
        witness_end=max([last, *anchors]),
        witnesses=[info["witness_first"], last],
        symbol=symbol,
        sessions=len(bundles["raw"]),
        raw_revisions=[],
        repair_kind="duplicate_split",
        duplicate_scope={
            "symbol": symbol,
            "through": info["through"],
            "review": info["review"],
            "pair": info["pair"],
        },
        reason="two_adjustments_but_one_observed_share_change",
    )

"""Repair a one-session disagreement between split-adjusted prices and traded share units."""

from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import _positive_rate
from libs.alpaca_lineage_build.metadata import action_symbols
from libs.common.us_equity_market_session_calendar import surrounding_market_sessions

from .carried_rename import _carried, _traded
from .contract import ADJUSTMENTS
from .dated_rename import UnsupportedInterval, _fields

_PRICES = ("open", "high", "low", "close", "vwap")
_PAIRS = {"split": "raw", "split,dividend": "dividend", "split,spin-off": "spin-off", "all": "dividend,spin-off"}


@lru_cache(maxsize=256)
def session_window(flagged_session):
    dates = surrounding_market_sessions(flagged_session)
    if not dates:
        raise UnsupportedInterval("selected date lacks two surrounding market sessions")
    return dates


def _action(actions, symbol, previous, day, following):
    scoped = []
    for action in actions:
        if symbol not in action_symbols(action):
            continue
        effective = action.get("ex_date") or action.get("effective_date") or action.get("process_date")
        if effective and previous <= effective <= following and action not in scoped:
            scoped.append(action)
    if len(scoped) != 1:
        raise UnsupportedInterval("expected one relevant provider action in the price window")
    action = scoped[0]
    kind = action.get("action_type")
    if (
        kind not in {"reverse_split", "forward_split"}
        or action.get("symbol") != symbol
        or action.get("ex_date") != day
        or action.get("effective_date") not in (None, day, following)
    ):
        raise UnsupportedInterval("split terms do not apply to the selected trading boundary")
    old, new = _positive_rate(action.get("old_rate")), _positive_rate(action.get("new_rate"))
    if old is None or new is None or old == new or (old > new) != (kind == "reverse_split"):
        raise UnsupportedInterval("incompatible provider split rates")
    return action, old, new


def _quantum(price):
    # Observed adjusted US share prices retain cents, with four significant
    # digits below $10. A witness across all priced fields must fit this model.
    return Decimal(1).scaleb(min(-2, price.adjusted() - 3))


def _rounded(price):
    return price.quantize(_quantum(price), rounding=ROUND_HALF_UP)


def _scale(bar, factor):
    if factor == 1:
        return dict(bar)
    return {
        **bar,
        **{field: str(_rounded(Decimal(bar[field]) * factor)) for field in _PRICES},
        "volume": str(int(Decimal(bar["volume"]) / factor)),
    }


def _split_factors(raw, split, step, *, scale=_scale):
    # The other retained splits may leave an integer/reciprocal share scale
    # on both sides. Only the observations can select that scale. Try the
    # nearest simple rate and its neighbours, including unchanged units.
    ratio = Decimal(split[-1]["close"]) / Decimal(raw[-1]["close"])
    reciprocal = ratio < 1
    anchor = int((1 / ratio if reciprocal else ratio).to_integral_value())
    candidates = {Decimal(1) / n if reciprocal else Decimal(n) for n in range(max(1, anchor - 1), anchor + 2)}
    compatible = []
    for after in candidates:
        factors = (after * step,) + (after,) * (len(raw) - 1)
        if all(_fields(scale(r, f)) == _fields(s) for r, s, f in zip(raw, split, factors, strict=True)):
            compatible.append(factors)
    if len(compatible) != 1:
        raise UnsupportedInterval("split views do not resolve a unique compatible share scale")
    return compatible[0]


def _carry(previous, current):
    return {**current, **dict.fromkeys(_PRICES[:-1], previous["close"])}


def _combined(raw, split, base, combined, factors, corrected_split, carried):
    if all(_fields(r) == _fields(b) for r, b in zip(raw, base, strict=True)):
        if any(_fields(c) != _fields(s) for c, s in zip(combined, split, strict=True)):
            raise UnsupportedInterval("combined view differs from the selected split-only scale")
        return corrected_split
    # A simultaneous cash/spin basis must remain the same through this
    # window. Every rounded price supplies bounds on its common multiplier.
    # Scaling an already rounded dividend bar could amplify its error.
    lower, upper = Decimal(0), Decimal("Infinity")
    for r, s, b, c, share in zip(raw, split, base, combined, factors, strict=True):
        if (
            Decimal(b["volume"]) != Decimal(r["volume"])
            or Decimal(c["volume"]) != Decimal(s["volume"])
            or Decimal(b["trade_count"]) != Decimal(r["trade_count"])
            or Decimal(c["trade_count"]) != Decimal(r["trade_count"])
        ):
            raise UnsupportedInterval("combined views disagree on share units or trade counts")
        for field in _PRICES:
            raw_price = Decimal(r[field])
            if not raw_price:
                # The zero-activity midpoint already established zero VWAP
                # in every view; it supplies no price multiplier observation.
                continue
            for observation, scale in ((b, 1), (c, share)):
                value = Decimal(observation[field])
                if _rounded(value) != value:
                    raise UnsupportedInterval(
                        "combined price precision disagrees with the observed rounding model"
                    )
                denominator = raw_price * scale
                # Immediately below a power of ten the provider can retain
                # another decimal place. The upper rounding limit is open.
                lower = max(lower, (value - _quantum(value.next_minus()) / 2) / denominator)
                upper = min(upper, (value + _quantum(value) / 2) / denominator)
    if lower >= upper:
        raise UnsupportedInterval("non-split adjustments do not have one compatible basis in the window")
    if carried:
        return _carry(*combined[:2])
    result = dict(corrected_split)
    for field in _PRICES:
        price = Decimal(raw[1][field]) * factors[0]
        low, high = _rounded(price * lower), _rounded(price * upper)
        if low != high:
            raise UnsupportedInterval("rounded observations admit competing corrected prices")
        result[field] = str(low)
    return result


def _scoped_rows(views, dates):
    rows = {mode: views.get(mode, {}).get("bars", []) for mode in ADJUSTMENTS}
    if any(tuple(r["session"] for r in bars) != dates for bars in rows.values()):
        raise UnsupportedInterval("price views disagree on their dated scope")
    raw = rows["raw"]
    labels = raw[0]["tickers"]
    if len(labels) != 1 or any(r["tickers"] != labels for bars in rows.values() for r in bars):
        raise UnsupportedInterval("price window crosses an ambiguous or different ticker")
    return rows, labels[0]


def assess(views, actions, flagged_session):
    """Consume the owning reader's three selected sessions and admissible provider actions."""
    from libs.alpaca_serving_catalogue.policy import _bar_reasons

    dates = session_window(flagged_session)
    previous, day, following = dates
    rows, symbol = _scoped_rows(views, dates)
    raw = rows["raw"]
    _, old, new = _action(actions, symbol, previous, day, following)
    if not _traded(raw[0]) or not _traded(raw[2]):
        raise UnsupportedInterval("price window lacks traded anchors on both sides")
    carried = not _traded(raw[1])
    if carried and any(
        not _carried(bars[1])
        or Decimal(bars[1]["vwap"]) != 0
        or (mode not in _PAIRS and Decimal(bars[1]["close"]) != Decimal(bars[0]["close"]))
        for mode, bars in rows.items()
    ):
        raise UnsupportedInterval("zero-activity row does not carry the preceding close on each non-split basis")
    step = old / new
    previous_close = Decimal(raw[0]["close"])
    opening, close = (Decimal(raw[1][field]) for field in ("open", "close"))
    next_open = Decimal(raw[2]["open"])
    # One interval still trades in the old units; the next open changes in
    # the direction and size of the split. A genuinely large economic return
    # or a competing price scale must not qualify just by crossing a flag.
    if any(
        not Decimal("0.75") <= move <= Decimal("1.25")
        for move in (opening / previous_close, close / previous_close, next_open / (close * step))
    ) or any(
        Decimal("0.75") <= move <= Decimal("1.25")
        for move in (opening / (previous_close * step), next_open / close)
    ):
        raise UnsupportedInterval("traded prices do not isolate a next-session change of share units")
    factors = _split_factors(raw, rows["split"], step)
    bundles = {}
    corrected_split = _carry(*rows["split"][:2]) if carried else _scale(raw[1], factors[0])
    for mode in ADJUSTMENTS:
        current = rows[mode][1]
        if mode in _PAIRS:
            corrected = (
                corrected_split
                if mode == "split"
                else _combined(
                    raw, rows["split"], rows[_PAIRS[mode]], rows[mode], factors, corrected_split, carried
                )
            )
            current = {**current, **{key: corrected[key] for key in (*_PRICES, "volume")}}
            if _bar_reasons(current, mode):
                raise UnsupportedInterval("corrected split view is outside eligible price or volume bounds")
        bundles[mode] = {day: current}
    return bundles, {
        "first": day,
        "cut": following,
        "witness_end": following,
        "symbol": symbol,
        "sessions": 1,
        "witnesses": [previous, following],
        "raw_revisions": [],
        "repair_kind": "split_timing",
        "split_terms": {"old_rate": str(old), "new_rate": str(new)},
        "reason": "carried_close_in_pre_split_units"
        if carried
        else "traded_share_units_changed_on_next_session",
    }

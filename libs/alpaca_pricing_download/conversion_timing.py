"""Repair early split adjustments paired with a later same-label security conversion."""

from datetime import date, timedelta
from decimal import Decimal

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import _positive_rate
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .carried_rename import _traded
from .dated_rename import UnsupportedInterval, _fields
from .rename_split_basis import _SPLITS, _factor, scaled
from .shifted_split import _PAIRS, _scoped_rows, _split_factors, session_window


def plan(actions, symbol, first, through, review):
    previous = session_window(review)[0]
    if previous < first:
        raise UnsupportedInterval("conversion lacks an immediately preceding traded label")
    by_date = {}
    for action in actions:
        day = action.get("ex_date") or action.get("effective_date") or action.get("process_date")
        if day and previous <= day <= through:
            by_date.setdefault(day, []).append(action)
    days = sorted(by_date)
    if len(days) < 2 or days[0] != review or any(len(by_date[d]) != 1 for d in days[:2]):
        raise UnsupportedInterval("split and subsequent conversion lack exclusive provider actions")
    split, conversion = (by_date[d][0] for d in days[:2])
    final = days[1]
    destination = split.get("new_cusip")
    original = conversion.get("acquiree_cusip")
    if not (
        split.get("action_type") in _SPLITS
        and split.get("symbol") == symbol
        and split.get("new_symbol") in (None, "", symbol)
        and split.get("ex_date") == review
        and split.get("effective_date") in (None, review, final)
        and isinstance(destination, str)
        and len(destination) == 9
        and split.get("old_cusip") == destination
        and conversion.get("action_type") == "stock_merger"
        and conversion.get("effective_date") == final
        and conversion.get("acquirer_symbol") == conversion.get("acquiree_symbol") == symbol
        and conversion.get("acquirer_cusip") == destination
        and isinstance(original, str)
        and len(original) == 9
        and original != destination
    ):
        raise UnsupportedInterval("events do not identify one same-label conversion into the split security")
    factor = _factor(split)
    old = _positive_rate(conversion.get("acquiree_rate"))
    new = _positive_rate(conversion.get("acquirer_rate"))
    if not old or not new or old / new != factor:
        raise UnsupportedInterval("conversion terms disagree with the observed split")
    return dict(first=review, previous=previous, final=final, symbol=symbol, factor=str(factor))


def assess(inputs):
    info, views = inputs["plan"], inputs["views"]
    first, previous, final = (info[n] for n in ("first", "previous", "final"))
    end = (date.fromisoformat(final) + timedelta(days=1)).isoformat()
    dates = expected_market_session_coverage(previous, end).session_dates
    if dates[:2] != (previous, first) or dates[-1] != final:
        raise UnsupportedInterval("conversion terms do not fall on traded sessions")
    rows, symbol = _scoped_rows(views, dates)
    raw, split = rows["raw"], rows["split"]
    if symbol != info["symbol"] or not all(_traded(r) for r in raw):
        raise UnsupportedInterval("conversion lacks exclusive traded prices in one accepted label")
    factor = Decimal(info["factor"])
    anchor = Decimal(raw[0]["close"])

    def comparable(value):
        return Decimal("0.75") <= value <= Decimal("1.25")

    for bar in raw[1:-1]:
        for field in ("open", "close", "vwap"):
            move = Decimal(bar[field]) / anchor
            if not comparable(move) or comparable(move / factor):
                raise UnsupportedInterval("interior trades do not isolate the original share units")
    for field in ("close", "vwap"):
        move = Decimal(raw[-1][field]) / Decimal(raw[-2]["close"])
        if not comparable(move / factor) or comparable(move):
            raise UnsupportedInterval("final traded close and VWAP do not confirm the security conversion")

    factors = _split_factors(raw, split, factor, scale=scaled)
    corrected = [scaled(r, factors[0]) for r in raw[1:-1]]
    bundles = {}
    for mode, bars in rows.items():
        reference = split if mode in _PAIRS else raw
        if mode not in ("raw", "split") and any(
            _fields(r) != _fields(b) for r, b in zip(reference, bars, strict=True)
        ):
            raise UnsupportedInterval("independent cash or spin basis requires separate conversion terms")
        bundles[mode] = {r["session"]: r for r in (corrected if mode in _PAIRS else bars[1:-1])}
    return bundles, dict(
        first=first,
        cut=final,
        witness_end=final,
        symbol=symbol,
        sessions=len(corrected),
        witnesses=[previous, final],
        raw_revisions=[],
        repair_kind="conversion_timing",
        split_factor=info["factor"],
        reason="traded_share_units_changed_on_same_label_conversion",
    )

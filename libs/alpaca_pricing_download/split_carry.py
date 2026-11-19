"""Adjust last-trade marks during a bracketed interval without eligible SIP trades."""

from datetime import date, timedelta
from decimal import Decimal

from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .carried_rename import _carried, _traded
from .dated_rename import UnsupportedInterval
from .shifted_split import _PAIRS, _action, _carry, _combined, _scoped_rows, _split_factors


def assess(views, actions, endpoint_observations, flagged_session, first, last):
    """The bounds select an interval. Its observations must establish both traded anchors."""
    if not first < flagged_session < last:
        raise UnsupportedInterval("selected split must be inside the bounded carry interval")
    end = date.fromisoformat(last) + timedelta(days=1)
    dates = expected_market_session_coverage(first, end.isoformat()).session_dates
    if len(dates) < 4 or dates[0] != first or dates[-1] != last or flagged_session not in dates:
        raise UnsupportedInterval("select the two traded sessions bracketing an extended exchange-session carry")
    rows, symbol = _scoped_rows(views, dates)
    raw = rows["raw"]
    if not _traded(raw[0]) or not _traded(raw[-1]):
        raise UnsupportedInterval("selected bounds are not both traded sessions")
    action, old, new = _action(actions, symbol, first, flagged_session, last)
    cusips = [action.get("old_cusip"), action.get("new_cusip")]
    if (
        not all(isinstance(code, str) and len(code) == 9 for code in cusips)
        or cusips[0] == cusips[1]
        or action.get("new_symbol") not in (None, "", symbol)
    ):
        raise UnsupportedInterval("split does not establish distinct CUSIP endpoints for the selected ticker")
    for code in cusips:
        matches = {(label, uid) for cusip, label, uid in endpoint_observations if cusip == code}
        if len(matches) != 1 or not any(label == symbol and uid for label, uid in matches):
            raise UnsupportedInterval(
                "CUSIP observations do not resolve each split endpoint to the traded label"
            )

    pivot = dates.index(flagged_session)
    for mode, bars in rows.items():
        before = Decimal(bars[0]["close"])
        after = Decimal(bars[pivot]["close"]) if mode in _PAIRS else before
        if any(
            not _carried(bar)
            or Decimal(bar["vwap"]) != 0
            or Decimal(bar["close"]) != (before if index < pivot else after)
            for index, bar in enumerate(bars[1:-1], 1)
        ):
            raise UnsupportedInterval("interior rows do not form one flat carry in each observed price basis")

    step = old / new
    last_trade = Decimal(raw[0]["close"])
    # A reopening can print a volatile opening price following a long halt.
    # Both its eligible closing trade and its volume-weighted prices must
    # support the same share units, and exclude unchanged units.
    for field in ("close", "vwap"):
        move = Decimal(raw[-1][field]) / last_trade
        if not Decimal("0.75") <= move / step <= Decimal("1.25") or Decimal("0.75") <= move <= Decimal("1.25"):
            raise UnsupportedInterval("the resumed close and VWAP do not confirm the adjusted last-trade basis")

    samples = {mode: (bars[0], bars[pivot], bars[-1]) for mode, bars in rows.items()}
    factors = _split_factors(samples["raw"], samples["split"], step)
    corrected = _carry(*samples["split"][:2])
    for mode, base in _PAIRS.items():
        if mode != "split":
            _combined(samples["raw"], samples["split"], samples[base], samples[mode], factors, corrected, True)
    # Each corrected OHLC field receives this exact view's last traded close.
    # The carried rows already established flat prices and zero activity.
    bundles = {
        mode: {bar["session"]: _carry(bars[0], bar) if mode in _PAIRS else bar for bar in bars[pivot:-1]}
        for mode, bars in rows.items()
    }
    return bundles, {
        "first": flagged_session,
        "cut": last,
        "witness_end": last,
        "symbol": symbol,
        "sessions": len(dates) - pivot - 1,
        "witnesses": [first, last],
        "raw_revisions": [],
        "repair_kind": "split_carry",
        "endpoint_cusips": cusips,
        "split_terms": {"old_rate": str(old), "new_rate": str(new)},
        "reason": "split_adjusted_last_trade_carried_until_trading_resumed",
    }

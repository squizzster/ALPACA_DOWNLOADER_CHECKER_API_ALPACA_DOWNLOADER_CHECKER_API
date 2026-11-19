"""Select unpriced sessions where a carried quote remains in pre-action share units."""

from datetime import date, timedelta
from decimal import Decimal

from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .carried_rename import _carried, _traded
from .dated_rename import UnsupportedInterval, provider_rows
from .rename_split_basis import _SPLITS, _factor
from .shifted_split import _PAIRS, _scoped_rows

UNPRICED = "unpriced_post_split_carry"


def plan(raw, other_views, actions, symbol, cut, first, last, coverage_end):
    if not first < cut <= last:
        raise UnsupportedInterval("split must be after the preceding trade and inside the bounded history")
    end = (date.fromisoformat(last) + timedelta(days=1)).isoformat()
    days = expected_market_session_coverage(first, end).session_dates
    if not days or days[0] != first or days[-1] != last or cut not in days:
        raise UnsupportedInterval("review bounds are not complete market sessions")
    if (
        tuple(b["session"] for b in raw) != days
        or any(b["tickers"] != [symbol] for b in raw)
        or not _traded(raw[0])
    ):
        raise UnsupportedInterval("first witness lacks a trade under the selected ticker")
    terminal = not _traded(raw[-1])
    if terminal and last != coverage_end:
        raise UnsupportedInterval("a missing reopening requires the actual end of the accepted price history")
    stop = len(days) if terminal else len(days) - 1
    i = days.index(cut)
    if i >= stop:
        raise UnsupportedInterval("there is no untraded session after the split")
    if (
        len(actions) != 1
        or actions[0].get("action_type") not in _SPLITS
        or actions[0].get("symbol") != symbol
        or actions[0].get("ex_date") != cut
    ):
        raise UnsupportedInterval("no exclusive provider split identifies the untraded interval")
    action = actions[0]
    factor = _factor(action)
    if any(not action.get(n) for n in ("old_cusip", "new_cusip")) or action["old_cusip"] == action["new_cusip"]:
        raise UnsupportedInterval("split does not identify distinct native securities at the share change")
    anchor = Decimal(raw[0]["close"])
    if any(not _carried(b) or Decimal(b["vwap"]) != 0 or Decimal(b["close"]) != anchor for b in raw[1:stop]):
        raise UnsupportedInterval("raw prices do not carry the last eligible trade through the proposed span")
    if not terminal and all(
        Decimal("0.75") <= Decimal(raw[-1][field]) / anchor / factor <= Decimal("1.25")
        for field in ("close", "vwap")
    ):
        raise UnsupportedInterval("resumed trade supports a numerical split-carry correction")
    checked = days[i - 1 : stop]
    views = other_views(checked[0], checked[-1])
    rows, _ = _scoped_rows({**views, "raw": {"bars": raw[i - 1 : stop]}}, checked)
    for mode, bars in rows.items():
        before, after = (Decimal(bars[k]["close"]) for k in (0, 1))
        step = before / after
        expected = factor if mode in _PAIRS else Decimal(1)
        if (mode not in _PAIRS and step != 1) or not Decimal("0.98") <= step / expected <= Decimal("1.02"):
            raise UnsupportedInterval("price views do not isolate a stale mark in the original share units")
        if any(not _carried(b) or Decimal(b["vwap"]) != 0 or Decimal(b["close"]) != after for b in bars[1:]):
            raise UnsupportedInterval("affected price views contain non-carried activity or another basis")
    return dict(
        outcome="check_untraded_tape",
        symbol=symbol,
        cut=cut,
        previous_session=days[i - 1],
        witness_start=first,
        witness_end=last,
        excluded_sessions=list(days[i:stop]),
        terminal=terminal,
        close=str(anchor),
        alternate=action.get("new_symbol") or symbol,
        factor=str(factor),
    )


def restored_trades(previous, raw):
    """A replacement can resolve a prior invalid mark by supplying actual eligible trades."""
    removed = set(previous["excluded_sessions"])
    observed = {b["session"]: b for b in raw if b["session"] in removed}
    if observed.keys() != removed or not all(
        b["tickers"] == [previous["symbol"]] and _traded(b) for b in observed.values()
    ):
        return None
    return {
        **{k: previous[k] for k in ("cut", "previous_session", "symbol", "witness_start", "witness_end")},
        "outcome": "check_recovered_trades",
        "witnesses": observed,
    }


def confirm_recovery(selected, client, get, put):
    from .dated_rename import _matches
    from .price_basis_review import RESTORED

    expected = selected["witnesses"]
    accepted = get("literal_recovered")
    if accepted is None:
        observed = provider_rows(client, selected["symbol"], "-", "raw", min(expected), max(expected))
        accepted = put(
            "literal_recovered",
            all(day in observed and _matches(bar, observed[day]) for day, bar in expected.items()),
        )
    context = {k: v for k, v in selected.items() if k != "witnesses"}
    return (
        {**context, "outcome": "restored", "reason": RESTORED}
        if accepted
        else {**context, "outcome": "unsupported", "reason": "recovered trades disagree with literal prices"}
    )


def confirm(selected, client, get, put):
    days = selected["excluded_sessions"]
    for symbol in dict.fromkeys((selected["symbol"], selected["alternate"])):
        key = "literal_untraded:" + symbol
        accepted = get(key)
        if accepted is None:
            rows = provider_rows(client, symbol, "-", "raw", days[0], days[-1])
            if symbol == selected["symbol"]:
                accepted = all(
                    day in rows
                    and _carried(rows[day])
                    and Decimal(rows[day]["vwap"]) == 0
                    and Decimal(rows[day]["close"]) == Decimal(selected["close"])
                    for day in days
                )
            else:
                accepted = not any(_traded(b) for b in rows.values())
            accepted = put(key, accepted)
        if not accepted:
            return {
                **selected,
                "outcome": "unsupported",
                "reason": "literal tape offers competing prices or lacks the retained carry",
            }
    return {**selected, "outcome": "restricted", "reason": UNPRICED}

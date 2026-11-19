"""Identify an observed ticker handover when the outgoing ticker carried its last trade."""

from datetime import timedelta
from decimal import Decimal

from .dated_rename import UnsupportedInterval, _matches


def _traded(bar):
    return min(Decimal(bar["volume"]), Decimal(bar["trade_count"])) > 0


def _carried(bar):
    return (
        Decimal(bar["volume"]) == Decimal(bar["trade_count"]) == 0
        and len({Decimal(bar[k]) for k in ("open", "high", "low", "close")}) == 1
    )


def preceding_scope(first, end, read):
    """Read an interior carry and its traded anchors, or a required full prefix."""
    raw = []
    while end >= first:
        lower = max(first, end - timedelta(days=6))
        raw = [*read(lower, end), *raw]
        end = lower - timedelta(days=1)
        if not raw:
            continue
        if not _carried(raw[-1]):
            return [*read(first, end), *raw] if first <= end else raw
        traded = [i for i, bar in enumerate(raw) if _traded(bar)]
        if len(traded) >= 3:
            return raw[traded[-3] :]
    raise UnsupportedInterval("carried interval lacks a traded predecessor history")


def _actions_support(header, old, new, first, cut):
    related = {
        (outgoing, incoming, process, effective)
        for outgoing, incoming, process, effective in header["actions"]
        if any(first <= value <= cut for value in (process, effective) if value)
        and (outgoing == old or incoming == new)
    }
    return (
        bool(related)
        and all(
            outgoing == old and incoming == new and process == cut and effective in (None, first)
            for outgoing, incoming, process, effective in related
        )
        and not any(
            ticker in (old, new) and lo <= cut and first <= hi for ticker, lo, hi in header["competitors"]
        )
    )


def select(plan, raw, header):
    """Only a terminal run of flat untraded outgoing bars can change labels."""
    days = list(raw)
    candidates = []
    for i in range(1, len(days)):
        day = days[i]
        if day > plan["cut"]:
            break
        before, after = raw[days[i - 1]], raw[day]
        old, new = before["tickers"], after["tickers"]
        if len(old) != 1 or len(new) != 1 or old == new or not _carried(before) or not _traded(after):
            continue
        j = i - 1
        while j > 0 and raw[days[j - 1]]["tickers"] == old and _carried(raw[days[j - 1]]):
            j -= 1
        if not j:
            continue
        first, prior = days[j], days[j - 1]
        anchor = raw[prior]
        exclusive = [r for r in header["ranges"] if r["first_session"] <= day and r["last_session"] >= first]
        if (
            anchor["tickers"] != old
            or not _traded(anchor)
            or any(Decimal(raw[d]["close"]) != Decimal(anchor["close"]) for d in days[j:i])
            or sorted(
                (r["ticker"], r["last_session"] == days[i - 1], r["first_session"] == day) for r in exclusive
            )
            != sorted([(old[0], True, False), (new[0], False, True)])
            or not _actions_support(header, old[0], new[0], first, day)
        ):
            continue
        change = dict(
            old=old[0], new=new[0], first=first, last=days[i - 1], prior_session=prior, next_session=day
        )
        candidates.append(change)
        if day == plan["cut"]:
            left = days[:j]
            if len(left) != 3 or any(not _traded(raw[d]) or raw[d]["tickers"] != old for d in left):
                raise UnsupportedInterval(
                    "interior correction lacks three adjacent traded predecessor witnesses"
                )
            plan["left_witnesses"] = left
            plan["earlier"] = days[j:i]
    if not candidates:
        raise UnsupportedInterval("no uniquely owned carried predecessor at a provider rename")
    plan["label_changes"] = candidates


def confirmation_groups(plan, previous, bars, groups):
    supported = []
    for change in plan["label_changes"]:
        days = [d for d in bars if change["first"] <= d <= change["last"]]
        group = next((g for g in groups if g["days"] == days), None)
        if group is None or not all(_traded(bars[d]) for d in days):
            raise UnsupportedInterval("incomplete traded successor coverage of carried interval")
        group["label_change"] = change
        group["outgoing"] = {d: previous[d] for d in [change["prior_session"], *days]}
        group["next_raw"] = previous[change["next_session"]]
        supported.append(change)
    return supported


def confirm(group, bars, observe):
    change = group["label_change"]
    old, new = change["old"], change["new"]
    first, last = change["prior_session"], change["next_session"]
    outgoing = observe(old, "-", "raw", first, change["last"])
    incoming = observe(new, "-", "raw", first, last)
    if any(d not in outgoing or not _matches(outgoing[d], b) for d, b in group["outgoing"].items()):
        raise UnsupportedInterval("literal predecessor disagrees with carried history")
    if first in incoming and _traded(incoming[first]):
        raise UnsupportedInterval("both literal tickers traded before the proposed handover")
    if last not in incoming or not _matches(incoming[last], group["next_raw"]):
        raise UnsupportedInterval("literal successor disagrees with its first published trade")
    for day in group["days"]:
        if day not in incoming or not _matches(incoming[day], bars[day]):
            raise UnsupportedInterval("literal successor does not corroborate mapped trade")
    return [{"session": day, "ticker": new, "asof": "-"} for day in group["days"]]

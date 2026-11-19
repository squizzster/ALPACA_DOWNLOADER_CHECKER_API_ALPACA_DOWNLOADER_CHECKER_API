"""Extend an accepted literal ticker witness onto new dated tape without comparing it again."""

from datetime import date, timedelta
from decimal import Decimal

from .dated_rename import _matches, provider_rows


def _traded(b):
    return b is not None and min(Decimal(b["trade_count"]), Decimal(b["volume"])) > 0


def assess(accepted, published, upper, load, peer_ranges, client, get, put):
    former = accepted["symbol"]
    first = accepted["original_first_session"]
    opening, through = accepted["witness_start"], accepted["witness_end"]

    def stage(key, make):
        prior = get(key)
        return put(key, make()) if prior is None else prior

    def provider(symbol, lo, hi):
        return stage(f"literal:{symbol}:{lo}:{hi}", lambda: provider_rows(client, symbol, "-", "raw", lo, hi))

    def observed(lo, hi):
        return stage(f"owned:{lo}:{hi}", lambda: load(lo, hi))

    def moved(b, literal, other):
        return (
            b["tickers"] == [published]
            and _traded(b)
            and _traded(literal)
            and _matches(b, literal)
            and not _traded(other)
        )

    def offset(day, n):
        return (date.fromisoformat(day) + timedelta(days=n)).isoformat()

    def declined(reason):
        return {"outcome": "unsupported", "reason": reason}

    # Resolve the first continuous run on the right in bounded quarterly blocks.
    # Once two consecutive genuine successor observations follow the former
    # label, nothing further can extend this original occupation.
    def right():
        lo = offset(through, 1)
        final = through
        witnesses = []
        while lo <= upper:
            hi = min(upper, offset(lo, 91))

            def block():
                nonlocal final
                newer = provider(published, lo, hi)
                endpoints = sorted(day for day, bar in newer.items() if _traded(bar))
                end = endpoints[1 - len(witnesses)] if len(endpoints) >= 2 - len(witnesses) else hi
                raw = observed(lo, end)
                old = provider(former, lo, end)
                if not raw:
                    return declined("no subsequent owner-selected observations")
                for b in raw:
                    day = b["session"]
                    if not witnesses and moved(b, old.get(day), newer.get(day)):
                        final = day
                    elif moved(b, newer.get(day), old.get(day)):
                        witnesses.append(day)
                        if len(witnesses) == 2:
                            break
                    else:
                        return declined("the literal trading paths do not identify a unique handover")
                if len(witnesses) < 2 and end < hi:
                    return declined("the selected owner lacks a subsequent literal trade witness")
                return {"last": final, "witnesses": list(witnesses)}

            done = stage("right_block:" + lo, block)
            if "outcome" in done:
                return done
            final, witnesses = done["last"], done["witnesses"]
            if len(witnesses) == 2:
                return dict(last=final, next_session=witnesses[0], witness_end=witnesses[1])
            lo = offset(hi, 1)
        return declined("missing a subsequent literal trading occupation")

    after = stage("right_occupation", right)
    if "outcome" in after:
        return after

    def left():
        start = opening
        prior = offset(opening, -1)
        hi = prior
        if first <= hi:
            raw = observed(first, hi)
            older = provider(former, first, hi)
            newer = provider(published, first, hi)
            for b in reversed(raw):
                day = b["session"]
                if not moved(b, older.get(day), newer.get(day)):
                    prior = day
                    break
                start = day
                prior = offset(day, -1)
        return dict(first=start, prior_session=prior)

    before = stage("left_occupation", left)
    if any(r["first_session"] <= after["last"] and r["last_session"] >= before["first"] for r in peer_ranges()):
        return declined("an independent published identity occupies the former ticker on those dates")
    return dict(
        outcome="projected",
        witness_end=after["witness_end"],
        change=dict(old=published, new=former, **before, last=after["last"], next_session=after["next_session"]),
    )

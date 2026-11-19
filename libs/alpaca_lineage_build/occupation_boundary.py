"""Decide which dates an accepted ticker owner can support across an earlier use."""

from decimal import Decimal

from libs.alpaca_pricing_download.dated_rename import _matches


def traded(row):
    return Decimal(row["volume"]) > 0 and Decimal(row["trade_count"]) > 0


def _positive(rows):
    return {day for day, bar in rows.items() if traded(bar)}


def _flat_carry(bar, close):
    return (
        Decimal(bar["trade_count"]) == 0
        and Decimal(bar["volume"]) == 0
        and all(Decimal(bar[f]) == close for f in ("open", "high", "low", "close"))
    )


def select(held, literal, current, old_asof, *, anchored_before=lambda _: False, unit_occupation=None):
    """Compare two plausible occupations and an uninterrupted returning issuer.

    A mapping can start late after an ordinary suspension; the historical
    route must independently distinguish the two traded periods.
    """
    active = sorted(_positive(literal))
    if len(active) < 4 or _positive(held) - set(active):
        return None
    current_active = _positive(current)
    if not current_active:
        return None
    earliest = min(current_active)
    if current_active != {d for d in active if d >= earliest} or any(
        not _matches(literal[d], current[d]) for d in current_active
    ):
        return None

    def flat(prev, start):
        close = Decimal(literal[prev]["close"])
        bridge = [b for d, b in held.items() if prev < d < start]
        return bool(bridge) and all(_flat_carry(b, close) for b in bridge)

    def result(start, end, reason):
        if any(d not in held for d in active if d >= start) or any(
            not _matches(held[d], literal[d]) for d in _positive(held) if d < start
        ):
            return None
        differences = [d for d in active if d >= start and not _matches(held[d], literal[d])]
        return {"first": start, "last_prior_trade": end, "reason": reason, "price_revision_dates": differences}

    if earliest > active[0]:
        earlier = [d for d in active if d < earliest]
        if len(earlier) < 2 or len(current_active) < 2 or not flat(earlier[-1], earliest):
            return None
        if any(
            not _flat_carry(row, Decimal(literal[earlier[-1]]["close"]))
            for d, row in current.items()
            if d < earliest
        ):
            return None
        prev = earlier[-1]
        if anchored_before(prev):
            return None
        historical = old_asof(prev)
        historical_active = _positive(historical)
        prior_trades = sorted(d for d in historical_active if d <= prev)
        if (
            len(prior_trades) < 2
            or prior_trades != [d for d in earlier if d >= prior_trades[0]]
            or any(not _matches(literal[d], historical[d]) for d in prior_trades)
        ):
            return None
        later = set(d for d in historical_active if d >= earliest)
        bridge = [d for d in held if prev < d < earliest]
        if later:
            if (
                not current_active <= later
                or any(_matches(historical[d], literal[d]) for d in current_active)
                or len({historical[d]["close"] for d in bridge if d in historical_active}) < 2
            ):
                return None
        # With no concurrent competing tape, a short return from suspension
        # remains plausible. Require at least a normal trading month's worth
        # of inactive sessions in addition to reciprocal, dated exclusion.
        elif len(bridge) < 21 or historical_active.intersection(bridge):
            return None
        return result(earliest, prev, "current_route_excludes_previous_trades")

    # A resumed listing also has carry rows. The old route must continue
    # trading against the frozen price, then coexist with a distinct later tape.
    windows = []
    positions = {day: i for i, day in enumerate(sorted(literal))}
    for prev, start in zip(active, active[1:]):
        # The comparison requires two distinct intervening traded prices on
        # the competing route. A single intervening session cannot supply them.
        if positions[start] - positions[prev] < 3:
            continue
        if flat(prev, start):
            windows.append((positions[start] - positions[prev], prev, start))
    if not windows:
        return None
    longest = max(w[0] for w in windows)
    targets = [w for w in windows if w[0] == longest]
    if len(targets) != 1:
        return None
    _, prev, start = targets[0]
    prior_anchor = anchored_before(prev)
    if prior_anchor and unit_occupation is None:
        return None
    historical = old_asof(prev)
    if not historical:
        return None
    prior = [d for d in active if d <= prev]
    later = [d for d in active if d >= start]
    if (
        len(prior) < 2
        or len(later) < 2
        or any(d not in historical or not _matches(literal[d], historical[d]) for d in prior)
    ):
        return None
    if unit_occupation and all(d in historical and _matches(literal[d], historical[d]) for d in later):
        component = unit_occupation(prev, start, prior_anchor)
        if component:
            decision = result(start, prev, component["reason"])
            return {**decision, **component} if decision else None
        return None
    if prior_anchor:
        return None
    during = [d for d, b in historical.items() if prev < d < start and traded(b)]
    if len({historical[d]["close"] for d in during}) < 2:
        return None
    if any(
        d not in historical or not traded(historical[d]) or _matches(literal[d], historical[d]) for d in later
    ):
        return None
    return result(start, prev, "historical_route_keeps_trading_against_different_later_tape")

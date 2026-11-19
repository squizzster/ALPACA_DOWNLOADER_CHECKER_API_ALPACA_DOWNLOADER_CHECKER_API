"""Select an older label's mapped tape when a later occupant's action distorts it."""

from decimal import Decimal

from libs.alpaca_lineage_build.metadata import action_symbols

from .contract import ADJUSTMENTS
from .dated_rename import UnsupportedInterval, _basis_jump, _matches, _scope
from .rename_split_basis import _SPLIT, _SPLITS, _day, _factor, _single_rename, scaled


def _plan(inputs, cut, held):
    leading = {m: held(m) for m in ("raw", "split", "all")}
    plan = _scope(leading, inputs["ranges"], cut, False, require_jump=False)
    raw, earlier, successor = leading["raw"], plan["earlier"], plan["symbol"]
    (old,) = raw[earlier[-1]]["tickers"]
    if not all(raw[d]["tickers"] == [old] for d in earlier):
        raise UnsupportedInterval("earlier history spans multiple ticker occupations")
    runs = [r for r in inputs["ranges"] if r["first_session"] < cut]
    if len(runs) != 1 or runs[0] != {"ticker": old, "first_session": earlier[0], "last_session": earlier[-1]}:
        raise UnsupportedInterval("historical ticker does not own the complete opening interval")
    actions = inputs["actions"]
    dates = {d: prev for prev, d in zip(earlier, earlier[1:])}
    foreign = set()
    for a in actions:
        d = _day(a)
        if a.get("action_type") in _SPLITS and a.get("symbol") == successor and d in dates:
            prev = dates[d]
            if (
                not _basis_jump(leading, prev, d)
                or any(min(Decimal(raw[k]["volume"]), Decimal(raw[k]["trade_count"])) <= 0 for k in (prev, d))
                or not Decimal(".75") <= Decimal(raw[d]["open"]) / Decimal(raw[prev]["close"]) <= Decimal("1.25")
            ):
                continue
            before = Decimal(leading["split"][prev]["close"]) / Decimal(raw[prev]["close"])
            after = Decimal(leading["split"][d]["close"]) / Decimal(raw[d]["close"])
            if Decimal(".98") <= before / after / _factor(a) <= Decimal("1.02"):
                if any(_day(other) == d and old in action_symbols(other) for other in actions):
                    raise UnsupportedInterval("former ticker has competing action terms on the observed step")
                foreign.add(d)
    if not foreign:
        raise UnsupportedInterval("no split of the future ticker explains the earlier basis step")
    seam = [
        a for a in actions if _day(a) == cut and a.get("symbol") == successor and a.get("action_type") in _SPLITS
    ]
    if len(seam) > 1:
        raise UnsupportedInterval("competing share terms at the actual ticker transition")
    # A documented rename plus a concurrent share change can preserve ownership.
    # The actual trading tape must select its economic units independently.
    if seam:
        names = [
            a
            for a in actions
            if _day(a) == cut
            and a.get("old_symbol") == old
            and a.get("new_symbol") == successor
            and a.get("action_type") == "name_change"
        ]
        if len(names) != 1 or not names[0].get("new_cusip") or names[0]["new_cusip"] != seam[0].get("new_cusip"):
            raise UnsupportedInterval("concurrent share terms do not bind the accepted rename")
        for a in actions:
            if (
                a not in [*names, *seam]
                and earlier[-1] <= _day(a) <= cut
                and {old, successor} & action_symbols(a)
                and a.get("action_type") != "cash_dividend"
            ):
                raise UnsupportedInterval("additional unit terms compete at the actual transition")
    else:
        _single_rename(actions, earlier[-1], cut, old, successor)
    factor = _factor(seam[0]) if seam else Decimal(1)
    left = [d for d in earlier if min(Decimal(raw[d]["volume"]), Decimal(raw[d]["trade_count"])) > 0][-3:]
    if len(left) != 3 or min(Decimal(raw[cut]["volume"]), Decimal(raw[cut]["trade_count"])) <= 0:
        raise UnsupportedInterval("actual ticker transition lacks traded price witnesses")
    move = Decimal(raw[cut]["open"]) / Decimal(raw[left[-1]]["close"]) / factor
    if not Decimal(".75") <= move <= Decimal("1.25"):
        raise UnsupportedInterval("traded prices do not support reported units at the actual rename")
    return {**plan, "old": old, "left": left, "factor": str(factor), "foreign": sorted(foreign)}


def assess(inputs, cut, observe, *, completed, checkpoint):
    from libs.alpaca_serving_catalogue.policy import _bar_reasons

    def save(step, value):
        checkpoint(step, value)
        completed[step] = value

    if "declined" in completed:
        raise UnsupportedInterval(completed["declined"])
    if "result" in completed:
        return {m: completed["view:" + m]["bars"] for m in ADJUSTMENTS}, completed["result"]
    cache = {}

    def previous(mode):
        if mode not in cache:
            cache[mode] = {b["session"]: b for b in inputs["views"][mode]["bars"]}
        return cache[mode]

    try:
        if "scope" not in completed:
            save("scope", _plan(inputs, cut, previous))
        p = completed["scope"]
        old, dates, left, witnesses = p["old"], p["earlier"], p["left"], p["witnesses"]
        factor = Decimal(p["factor"])
        bundles = {}
        for mode in ("raw", *(m for m in ADJUSTMENTS if m != "raw")):
            key = "view:" + mode
            if key not in completed:
                source = observe(old, dates[-1], mode, dates[0], witnesses[-1])
                held = previous(mode)
                if any(d not in source or not _matches(source[d], held[d]) for d in witnesses):
                    raise UnsupportedInterval(f"former label mapping disagrees on a {mode} successor witness")
                bars, changes = {}, []
                for d in dates:
                    if d not in source or held[d]["tickers"] != [old]:
                        raise UnsupportedInterval("former label lacks dated coverage")
                    bar = source[d]
                    if factor != 1 and mode != "raw":
                        if not _matches(bar, bundles["raw"][d]):
                            raise UnsupportedInterval(
                                "distinct non-raw adjustments compete with the rename share terms"
                            )
                        if mode in _SPLIT:
                            bar = scaled(bar, factor)
                    if _bar_reasons(bar, mode):
                        raise UnsupportedInterval(f"former label supplied an ineligible {mode} bar on {d}")
                    if d in left and not _matches(held[d], bar):
                        raise UnsupportedInterval(f"traded prior {mode} witnesses disagree at the actual rename")
                    if mode == "raw" and not _matches(held[d], bar):
                        if d in p["foreign"]:
                            raise UnsupportedInterval(
                                "the two routes disagree on raw prices at the flagged action"
                            )
                        changes.append(d)
                    bars[d] = {**bar, "session": d, "tickers": [old]}
                save(key, {"bars": bars, "changes": changes})
            bundles[mode] = completed[key]["bars"]
        changes = completed["view:raw"]["changes"]
        for d in changes:
            key = "literal:" + d
            if key not in completed:
                literal = observe(old, "-", "raw", d, d)
                if d not in literal or not _matches(literal[d], bundles["raw"][d]):
                    raise UnsupportedInterval("literal historical label does not corroborate raw revision")
                save(key, True)
        held = {m: previous(m) for m in ("all", "split", "raw")}
        for prior, d in zip(dates, dates[1:]):
            if d in p["foreign"]:
                for mode in ("split", "all"):
                    before = Decimal(bundles[mode][prior]["close"]) / Decimal(bundles["raw"][prior]["close"])
                    after = Decimal(bundles[mode][d]["close"]) / Decimal(bundles["raw"][d]["close"])
                    if max(before, after) > min(before, after) * Decimal("1.05"):
                        raise UnsupportedInterval(
                            "mapped historical basis still changes at the selected foreign action"
                        )
            if _basis_jump(bundles, prior, d) and not _basis_jump(held, prior, d):
                raise UnsupportedInterval("former label leaves the selected step or introduces another")
        desc = dict(
            first=dates[0],
            cut=cut,
            witness_end=witnesses[-1],
            witnesses=witnesses,
            symbol=p["symbol"],
            sessions=len(dates),
            repair_kind="former_label_basis",
            former_symbol=old,
            share_basis=p["factor"],
            foreign_split_dates=p["foreign"],
            raw_revisions=[{"session": d, "ticker": old, "asof": "-"} for d in changes],
        )
        save("result", desc)
        return bundles, desc
    except UnsupportedInterval as exc:
        save("declined", str(exc))
        raise

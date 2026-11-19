"""Extend a witnessed native split basis across an already owned ticker rename."""

import math
from decimal import Decimal
from itertools import groupby

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import _positive_rate
from libs.alpaca_lineage_build.metadata import action_symbols

from .contract import ADJUSTMENTS
from .dated_rename import UnsupportedInterval, _matches, _scope
from .shifted_split import _quantum

_SPLIT = {"split", "split,dividend", "split,spin-off", "all"}
_SPLITS = {"forward_split", "reverse_split"}


def scaled(raw, factor):
    if factor == 1:
        return dict(raw)
    adjusted = {**raw, "volume": str(int(Decimal(raw["volume"]) / factor))}
    # Alpaca divides by a binary64 share scale before rounding a positive
    # price to display precision. Multiplying rounded adjusted prices (or
    # decimal half-up at exact ties) can move an OHLC/VWAP by a cent.
    binary_factor = float(factor)
    if not math.isfinite(binary_factor) or binary_factor == 0:
        raise UnsupportedInterval("share factor exceeds the supported price precision")
    divisor = 1 / binary_factor
    for field in ("open", "high", "low", "close", "vwap"):
        value = float(raw[field]) / divisor
        if not math.isfinite(value) or abs(value) > 2**52:
            raise UnsupportedInterval("share price exceeds the supported price precision")
        precision = -_quantum(Decimal(str(value))).as_tuple().exponent if value else 2
        unit = 10**precision
        if abs(value * unit) > 2**52:
            raise UnsupportedInterval("share price exceeds the supported rounding precision")
        adjusted[field] = str(Decimal(math.floor(value * unit + 0.5)) / unit)
    return adjusted


def _day(a):
    return a.get("ex_date") or a.get("effective_date") or a.get("process_date")


def _factor(a):
    old, new = _positive_rate(a.get("old_rate")), _positive_rate(a.get("new_rate"))
    if (
        not old
        or not new
        or not a.get("ex_date")
        or old == new
        or (old > new) != (a["action_type"] == "reverse_split")
    ):
        raise UnsupportedInterval("incompatible provider share terms")
    return old / new


def _single_rename(actions, prev, day, old, new):
    names = [
        a
        for a in actions
        if a.get("action_type") == "name_change"
        and prev <= _day(a) <= day
        and (a.get("old_symbol") == old or a.get("new_symbol") == new)
    ]
    if (
        len(names) != 1
        or _day(names[0]) != day
        or names[0].get("old_symbol") != old
        or names[0].get("new_symbol") != new
    ):
        raise UnsupportedInterval("provider actions do not uniquely bind the dated rename")
    rename = names[0]
    equivalent = False
    for a in actions:
        if not prev <= _day(a) <= day:
            continue
        if a.get("action_type") in _SPLITS and a.get("symbol") in (old, new):
            raise UnsupportedInterval("a split at the rename competes with the proposed mapping")
        if a.get("acquiree_symbol") == old:
            old_rate = _positive_rate(a.get("acquiree_rate"))
            if (
                a.get("action_type") != "stock_merger"
                or not old_rate
                or old_rate != _positive_rate(a.get("acquirer_rate"))
                or a.get("acquirer_symbol") not in (None, "", new)
            ):
                raise UnsupportedInterval("rename has competing merger terms")
            equivalent = True
            continue
        kind = a.get("action_type")
        if kind in {"name_change", "cash_dividend"} or not action_symbols(a) & {old, new}:
            continue
        if kind == "unit_split" and a.get("old_symbol") and a["old_symbol"] not in (old, new):
            # An independent unit separating into the common or warrant does
            # not change the number of shares of an existing holder of either.
            continue
        raise UnsupportedInterval("unresolved share-unit change at the proposed rename")
    if (
        rename.get("old_cusip")
        and rename.get("new_cusip")
        and rename["old_cusip"] != rename["new_cusip"]
        and not equivalent
    ):
        raise UnsupportedInterval("changed rename CUSIP lacks compatible share conversion terms")


def _split_terms(actions, ranges, plan):
    """Select successive share factors only along the accepted historical labels."""
    earlier, cut, new = plan["earlier"], plan["cut"], plan["symbol"]
    old = next((r["ticker"] for r in ranges if r["last_session"] == earlier[-1]), None)
    _single_rename(actions, earlier[-1], cut, old, new)
    history = [r for r in ranges if r["first_session"] < cut]
    later = next(r for r in ranges if r["ticker"] == new and r["first_session"] == cut)
    early_terms, future = [], []
    for a in sorted(actions, key=lambda a: (_day(a), a.get("id", ""))):
        symbol, day = a.get("symbol"), _day(a)
        if a.get("action_type") not in _SPLITS:
            continue
        if any(r["ticker"] == symbol and r["first_session"] <= day <= r["last_session"] for r in history):
            early_terms.append([day, str(_factor(a))])
        elif day >= cut and symbol == new:
            if day <= plan["witnesses"][-1]:
                raise UnsupportedInterval("share units change inside the rename witness window")
            if day > later["last_session"]:
                # A distinct later occupation of the same text is no authority.
                # Follow only a provider-linked same-label CUSIP transition.
                previous_cusip = future[-1].get("new_cusip") if future else None
                link = [
                    v
                    for v in actions
                    if v.get("action_type") == "name_change"
                    and v.get("old_symbol") == v.get("new_symbol") == new
                    and previous_cusip
                    and v.get("old_cusip") == previous_cusip
                    and v.get("new_cusip") == a.get("old_cusip")
                    and later["last_session"] < _day(v) <= day
                ]
                if len(link) != 1:
                    continue
            future.append(a)
    if not future or len({d for d, _ in early_terms}) != len(early_terms):
        raise UnsupportedInterval("provider actions do not define unique share terms")
    return old, early_terms, future


def _plan(inputs, cut, previous):
    views = {mode: previous(mode) for mode in ("raw", "all", "split")}
    plan = _scope(views, inputs["ranges"], cut, False)
    prior, held = plan["earlier"][-1], views["raw"]
    if any(min(Decimal(held[d]["volume"]), Decimal(held[d]["trade_count"])) <= 0 for d in (prior, cut)):
        raise UnsupportedInterval("the proposed split-basis join lacks a trade on each side")
    old, terms, future = _split_terms(inputs["actions"], inputs["ranges"], plan)
    # The accepted successor fixes the target horizon. Select the earliest
    # observed cumulative split terms that reproduce its prices and share
    # units. Later events cannot extend that established horizon.
    factor = Decimal(1)
    for through, dated in groupby(future, key=_day):
        leg, *alternatives = dated
        if alternatives:
            raise UnsupportedInterval("competing provider split terms on one date")
        factor *= _factor(leg)
        if factor != 1 and all(_matches(scaled(held[d], factor), views["split"][d]) for d in plan["witnesses"]):
            break
    else:
        raise UnsupportedInterval("successor observations do not establish a provider share basis")
    for d in plan["witnesses"]:
        for mode in ADJUSTMENTS:
            if mode in {"raw", "split"}:
                continue
            anchor = views["split"][d] if mode in _SPLIT else held[d]
            witness = previous(mode).get(d)
            if witness is None or witness["tickers"] != held[d]["tickers"] or not _matches(witness, anchor):
                raise UnsupportedInterval("successor has a distinct non-split adjustment")
    spans = [
        [r["ticker"], r["first_session"], through if r["first_session"] == cut else r["last_session"]]
        for r in inputs["ranges"]
        if r["first_session"] <= cut
    ]
    scope = dict(
        symbols=sorted({s[0] for s in spans}), through=through, spans=spans, rename=[old, plan["symbol"], prior]
    )
    plan.update(old=old, earlier_terms=terms, factor=str(factor), action_scope=scope)
    return plan


def _mapped(plan, previous, observe):
    """A direct rename with compatible tape belongs to mapped price acquisition."""
    new, cut, day = plan["symbol"], plan["cut"], plan["earlier"][-1]
    rows = observe(new, cut, "raw", day, cut)
    if cut not in rows or not _matches(rows[cut], previous("raw")[cut]):
        raise UnsupportedInterval("mapped successor does not corroborate the selected raw successor")
    prior = rows.get(day)
    if prior is None:
        return "disjoint_mapped_tapes"
    if _matches(prior, previous("raw")[day]):
        return "matching_predecessor"
    if Decimal(prior["volume"]) == Decimal(prior["trade_count"]) == 0:
        return "competing_carry"
    raise UnsupportedInterval("a different traded predecessor competes with the selected raw tape")


def _mapping_basis(plan, previous, observe, mapped):
    new, cut, day = plan["symbol"], plan["cut"], plan["earlier"][-1]
    if mapped == "matching_predecessor":
        adjusted = observe(new, cut, "all", cut, cut)
        if cut not in adjusted:
            raise UnsupportedInterval("mapped successor adjustment witness absent")
        if _matches(adjusted[cut], previous("all")[cut]):
            raise UnsupportedInterval("earlier tape is available through the ordinary dated mapping")
    elif mapped == "competing_carry":
        literal = observe(plan["old"], "-", "raw", day, day)
        if day not in literal or not _matches(literal[day], previous("raw")[day]):
            raise UnsupportedInterval("literal predecessor does not corroborate the contested trade")


def _prices(plan, previous):
    from libs.alpaca_serving_catalogue.policy import _bar_reasons

    factor = Decimal(plan["factor"])
    base = previous("raw")
    others = {m: previous(m) for m in ADJUSTMENTS if m != "raw"}
    bundles = {m: {} for m in ADJUSTMENTS}
    terms = [(when, Decimal(rate)) for when, rate in plan["earlier_terms"]]
    for d in plan["earlier"]:
        raw = base[d]
        if len(raw["tickers"]) != 1:
            raise UnsupportedInterval("ambiguous dated price label")
        historical = math.prod((scale for when, scale in terms if d < when), start=Decimal(1))
        existing = scaled(raw, historical)
        corrected = scaled(raw, factor * historical)
        if _bar_reasons(corrected, "split"):
            raise UnsupportedInterval("derived share-adjusted bar is outside the supported price bounds")
        bundles["raw"][d] = raw
        for mode, rows in others.items():
            held = rows.get(d)
            expected = existing if mode in _SPLIT else raw
            if held is None or held["tickers"] != raw["tickers"] or not _matches(held, expected):
                raise UnsupportedInterval("existing eight-view observations do not isolate share adjustments")
            bundles[mode][d] = corrected if mode in _SPLIT else held
    return bundles


def assess(inputs, cut, observe, *, completed=None, checkpoint=None):
    state = {} if completed is None else completed

    def save(key, value):
        if checkpoint is not None:
            checkpoint(key, value)
        state[key] = value

    if "declined" in state:
        raise UnsupportedInterval(state["declined"])
    if "result" in state:
        return state["result"]
    held = {}

    def previous(mode):
        if mode not in held:
            held[mode] = {b["session"]: b for b in inputs["views"][mode]["bars"]}
        return held[mode]

    try:
        if "scope" not in state:
            save("scope", _plan(inputs, cut, previous))
        plan = state["scope"]
        if "mapping" not in state:
            save("mapping", _mapped(plan, previous, observe))
        if "route" not in state:
            _mapping_basis(plan, previous, observe, state["mapping"])
            save("route", True)
        bundles = _prices(plan, previous)
        result = dict(
            first=plan["earlier"][0],
            cut=cut,
            witness_end=plan["witnesses"][-1],
            symbol=plan["symbol"],
            witnesses=plan["witnesses"],
            sessions=len(plan["earlier"]),
            raw_revisions=[],
            repair_kind="rename_split_basis",
            share_basis=plan["factor"],
            basis_action_scope=plan["action_scope"],
        )
        save("result", [bundles, result])
        return bundles, result
    except UnsupportedInterval as error:
        save("declined", str(error))
        raise

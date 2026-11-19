"""Acquire a price interval for an already published, dated identity."""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from itertools import islice, pairwise

from .contract import ADJUSTMENTS, COLUMNS, NEW_YORK, timestamp
from .responses import fetch_rows


class UnsupportedInterval(ValueError):
    """The available mapped observations do not support this correction."""


def _fields(bar):
    return tuple(Decimal(bar[field]) for field in COLUMNS[1:])


def _window(day):
    return datetime.combine(date.fromisoformat(day), time.min, NEW_YORK)


def provider_rows(client, symbol, asof, view, first, last):
    rows = fetch_rows(client, symbol, view, _window(first), _window(last) + timedelta(days=1), asof=asof)
    by_day = {}
    for row in rows:
        day = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
        if day in by_day:
            raise UnsupportedInterval("ambiguous daily provider observation")
        by_day[day] = dict(zip(COLUMNS, row, strict=True))
    return by_day


def _matches(left, right):
    return timestamp(left["timestamp"]) == timestamp(right["timestamp"]) and _fields(left) == _fields(right)


def _basis_jump(views, before, after):
    """A split-driven price change while the unadjusted close stays comparable."""

    def ratio(view):
        rows = views[view]
        return Decimal(rows[after]["close"]) / Decimal(rows[before]["close"])

    raw_move = ratio("raw")
    if not Decimal("0.75") <= raw_move <= Decimal("1.25"):
        return False
    split_move = ratio("split")
    basis_move = split_move / raw_move
    adjusted_move = ratio("all")
    return (
        (basis_move >= 2 or basis_move <= Decimal("0.5"))
        and (adjusted_move >= 2 or adjusted_move <= Decimal("0.5"))
        and max(adjusted_move, split_move) <= min(adjusted_move, split_move) * Decimal("1.05")
    )


def _confirmation_ranges(changes, ranges, previous, successor, cut):
    """Group contiguous revised sessions under their independently dated ticker."""
    groups = []
    previous_index = -2
    for index, day in changes:
        labels = [r for r in ranges if r["first_session"] <= day <= r["last_session"]]
        if len(labels) != 1:
            raise UnsupportedInterval("ambiguous historical ticker for revised raw bar")
        label = labels[0]
        route = [label["ticker"], min(label["last_session"], previous)]
        if route == [successor, cut]:
            raise UnsupportedInterval("raw revision lacks a distinct historical ticker route")
        if index != previous_index + 1 or groups[-1]["route"] != route:
            groups.append({"route": route, "days": []})
        groups[-1]["days"].append(day)
        previous_index = index
    return groups


def _scope(held, ranges, cut, carried, same_label=False, *, require_jump=True):
    header = ranges if carried else None
    if carried:
        ranges = header["ranges"]
    raw = held["raw"]
    earlier = [d for d in raw if d < cut]
    labels = [r for r in ranges if r["first_session"] <= cut <= r["last_session"]]
    if (
        not earlier
        or cut not in raw
        or len(labels) != 1
        or (not same_label and labels[0]["first_session"] != cut)
    ):
        raise UnsupportedInterval("select the first published session under a new ticker")
    successor = labels[0]["ticker"]
    previous = raw[earlier[-1]]["tickers"]
    if same_label and (
        labels[0]["first_session"] != earlier[0] or any(raw[d]["tickers"] != [successor] for d in earlier)
    ):
        raise UnsupportedInterval("selected prefix does not belong to one dated ticker range")
    if raw[cut]["tickers"] != [successor] or len(previous) != 1 or (previous == [successor]) != same_label:
        raise UnsupportedInterval("ambiguous or unchanged ticker at the requested session")
    if require_jump and not carried and not _basis_jump(held, earlier[-1], cut):
        raise UnsupportedInterval("selected transition has no comparable split-basis discontinuity")
    traded = list(
        islice(
            (
                d
                for d in raw
                if cut <= d <= labels[0]["last_session"]
                and raw[d]["tickers"] == [successor]
                and Decimal(raw[d]["volume"]) > 0
                and Decimal(raw[d]["trade_count"]) > 0
            ),
            3,
        )
    )
    if len(traded) < 2:
        raise UnsupportedInterval("insufficient traded successor sessions")
    if same_label:
        before = next(
            (
                raw[d]
                for d in reversed(earlier)
                if min(Decimal(raw[d]["volume"]), Decimal(raw[d]["trade_count"])) > 0
            ),
            None,
        )
        if before is None or not Decimal("0.75") <= Decimal(raw[traded[0]]["open"]) / Decimal(
            before["close"]
        ) <= Decimal("1.25"):
            raise UnsupportedInterval(
                "traded opening does not establish comparable raw units across the adjustment step"
            )
    witnesses = sorted({cut, *traded})
    plan = {"earlier": earlier, "cut": cut, "witnesses": witnesses, "symbol": successor}
    if same_label:
        plan["same_label"] = True
    if carried:
        from .carried_rename import select

        select(plan, raw, header)
        if require_jump and not _basis_jump(held, earlier[-1], cut):
            # A genuine return on the first published successor day can mask
            # a basis break where the outgoing tape first stopped trading.
            if not any(
                _basis_jump(held, change["prior_session"], change["first"]) for change in plan["label_changes"]
            ):
                raise UnsupportedInterval("neither endpoint of the carried interval exhibits a basis break")
    return plan


def _view(mode, plan, previous, ranges, observe):
    from libs.alpaca_serving_catalogue.policy import _bar_reasons

    earlier, witnesses = plan["earlier"], plan["witnesses"]
    left = plan.get("left_witnesses", ())
    mapped = observe(plan["symbol"], plan["cut"], mode, left[0] if left else earlier[0], witnesses[-1])
    if any(d not in mapped or not _matches(mapped[d], previous[d]) for d in left):
        raise UnsupportedInterval(f"{mode} disagrees on a traded predecessor witness")
    if any(d not in mapped for d in earlier) or any(d not in mapped for d in witnesses):
        raise UnsupportedInterval(f"{mode} lacks a published session")
    if any(not _matches(mapped[d], previous[d]) for d in witnesses):
        raise UnsupportedInterval(f"{mode} disagrees on a successor session")
    bars, changes = {}, []
    for index, day in enumerate(earlier):
        bar = mapped[day]
        if _bar_reasons(bar, mode):
            raise UnsupportedInterval(f"{mode} has an ineligible mapped bar on {day}")
        if mode == "raw" and not _matches(bar, previous[day]):
            if plan.get("same_label"):
                raise UnsupportedInterval(f"mapped raw history differs from the selected ticker on {day}")
            changes.append((index, day))
        labels = previous[day]["tickers"]
        for change in plan.get("label_changes", ()):
            if change["first"] <= day <= change["last"]:
                if labels != [change["old"]]:
                    raise UnsupportedInterval(f"{mode} has a competing dated ticker")
                labels = [change["new"]]
                break
        bars[day] = {**bar, "session": day, "tickers": labels}
    accepted = {"bars": bars}
    if mode == "raw":
        accepted["confirmations"] = _confirmation_ranges(
            changes, ranges, earlier[-1], plan["symbol"], plan["cut"]
        )
        if "label_changes" in plan:
            from .carried_rename import confirmation_groups

            confirmation_groups(plan, previous, bars, accepted["confirmations"])
    return accepted


def _check_basis(plan, bundles, held):
    earlier, cut = plan["earlier"], plan["cut"]
    for mode in ("split", "all"):
        left = Decimal(bundles[mode][earlier[-1]]["close"]) / Decimal(bundles["raw"][earlier[-1]]["close"])
        right = Decimal(held[mode][cut]["close"]) / Decimal(held["raw"][cut]["close"])
        if max(left, right) > min(left, right) * Decimal("1.05"):
            raise UnsupportedInterval(f"{mode} leaves an incompatible rename basis")
    left_witnesses = plan.get("left_witnesses")
    if left_witnesses:
        first = earlier[0]
        last = left_witnesses[-1]
        boundary = {m: {last: held[m][last], first: bundles[m][first]} for m in ("all", "split", "raw")}
        if _basis_jump(boundary, last, first):
            raise UnsupportedInterval("mapped prices leave a discontinuity at the start of the interval")
    for before, after in pairwise(earlier):
        if _basis_jump(bundles, before, after) and not _basis_jump(held, before, after):
            raise UnsupportedInterval(f"mapped prices introduce a split-basis discontinuity on {after}")


def _confirm(group, bars, observe):
    if "label_change" in group:
        from .carried_rename import confirm

        return confirm(group, bars, observe)
    ticker, asof = group["route"]
    days = group["days"]
    actual = observe(ticker, asof, "raw", days[0], days[-1])
    for day in days:
        if day not in actual or not _matches(actual[day], bars[day]):
            raise UnsupportedInterval(f"uncorroborated raw change on {day}")
    return [{"session": d, "ticker": ticker, "asof": asof} for d in days]


def acquire(views, ranges, cut, observe, *, completed=None, checkpoint=None, carried=False, same_label=False):
    """Produce the dated bundle, resuming only stages bound by the owning journal.

    The caller binds completed and checkpoint to the store, published group
    generation, identity, cut and operation format. A stage becomes reusable
    after its observation and comparison both finish. Transient transport or
    journal failures never become completed evidence dispositions.
    """
    state = {} if completed is None else completed
    if "declined" in state:
        raise UnsupportedInterval(state["declined"])
    if "result" in state:
        return {mode: state["view:" + mode]["bars"] for mode in ADJUSTMENTS}, state["result"]

    def save(key, value):
        if checkpoint is not None:
            checkpoint(key, value)
        state[key] = value

    # The owning reader may provide a lazy mapping during recovery. Only newly
    # selected downstream work needs the original published rows.
    held = {}

    def previous(mode):
        if mode not in held:
            held[mode] = {b["session"]: b for b in views[mode]["bars"]}
        return held[mode]

    try:
        leading = ("raw", "all", "split")
        if "scope" not in state:
            save("scope", _scope({mode: previous(mode) for mode in leading}, ranges, cut, carried, same_label))
        plan = state["scope"]
        bundles = {}
        revisions = []
        for mode in (*leading, *(view for view in ADJUSTMENTS if view not in leading)):
            key = "view:" + mode
            if key not in state:
                save(
                    key,
                    _view(
                        mode, plan, previous(mode), ranges["ranges"] if carried and ranges else ranges, observe
                    ),
                )
            bundles[mode] = state[key]["bars"]
            if mode == "split":
                if "basis" not in state:
                    _check_basis(plan, bundles, {v: previous(v) for v in leading})
                    save("basis", True)
                for index, group in enumerate(state["view:raw"]["confirmations"]):
                    confirmation = f"confirmation:{index}"
                    if confirmation not in state:
                        save(confirmation, _confirm(group, bundles["raw"], observe))
                    revisions.extend(state[confirmation])
        earlier, witnesses = plan["earlier"], plan["witnesses"]
        result = dict(
            first=earlier[0],
            cut=cut,
            witness_end=witnesses[-1],
            symbol=plan["symbol"],
            witnesses=witnesses,
            raw_revisions=revisions,
            sessions=len(earlier),
        )
        if same_label:
            result["repair_kind"] = "same_label_basis"
        if carried:
            result.update(repair_kind="carried_rename", label_changes=plan["label_changes"])
        save("result", result)
        return bundles, result
    except UnsupportedInterval as error:
        save("declined", str(error))
        raise

"""Pricing-owned positional changes for retained rows across selected-session insertions."""

from bisect import bisect_left
from datetime import date

from .changes import MAX_ANCESTORS
from .contract import ADJUSTMENTS, NEW_YORK, timestamp


def session(row):
    return timestamp(row[0]).astimezone(NEW_YORK).date()


def partition(month, anchor):
    if month == anchor:
        return "current_month"
    return "current_year" if month[:4] == anchor[:4] else "completed_years"


def _reposition(positions, anchor, views, inserted, *, changed_from):
    """Retain exact row coordinates; only a changed partition layout needs new coordinates."""
    result = {}
    for old_kind, adjustments in positions.items():
        for adjustment, pairs in adjustments.items():
            for pos, day in pairs:
                if changed_from is not None and day >= changed_from:
                    # Later ordinary pricing work owns this suffix. No row there
                    # retains positional authority from this earlier insertion.
                    continue
                kind = partition(day[:7], anchor)
                if kind != old_kind:
                    # Rollover has already assembled the destination partition.
                    # Locate this known day without opening a boundary or validating prices.
                    pos = 1 + bisect_left(views[kind][adjustment], date.fromisoformat(day), key=session)
                elif inserted and kind in inserted:
                    pos += bisect_left(inserted[kind][adjustment], day, key=lambda pair: pair[1])
                result.setdefault(kind, {}).setdefault(adjustment, []).append([pos, day])
    return result


def publication(previous, generation, request, anchor, views, update, *, inserted, replacement):
    """Bind each insertion to its exact input generation and installed partition positions."""
    if previous is None:
        return None
    old = previous.get("session_insertions", ())
    if not (inserted or old):
        return None
    old_request = previous["latest_request"]
    same_mapping = old_request["asof"] == request["asof"] or (
        old_request["asof"] == old_request["end_exclusive"][:10]
        and request["asof"] == request["end_exclusive"][:10]
    )
    if not same_mapping or replacement and update:
        return None
    changed_from = update["from"] if update else None
    change = (
        {"positions": inserted, "last_inserted_session": update["to"]}
        if inserted
        else {"changed_from": changed_from}
    )
    chain = [{"source_sha256": generation, **change}]
    for item in old[: MAX_ANCESTORS - 1]:
        positions = item.get("positions")
        if positions and (
            previous["anchor_month"] != anchor
            or changed_from is not None
            and changed_from <= item["last_inserted_session"]
        ):
            current = _reposition(
                positions, anchor, views, inserted, changed_from=None if inserted else changed_from
            )
            item = {**item, "positions": current}
        chain.append(item)
    return chain if any(item.get("positions") for item in chain) else None


def retained_insertions(source, old_generation, *, through):
    """Return inserted positions in today's partitions when all other changes are later."""
    positions = {}
    for item in source.get("session_insertions", ()):
        changed_from = item.get("changed_from")
        if changed_from is not None and changed_from <= through:
            return None
        for kind, adjustments in item.get("positions", {}).items():
            views = positions.setdefault(kind, {name: [] for name in ADJUSTMENTS})
            for adjustment, pairs in adjustments.items():
                views[adjustment].extend(pairs)
        if item["source_sha256"] == old_generation:
            for views in positions.values():
                for pairs in views.values():
                    pairs.sort()
            return positions or None
    return None

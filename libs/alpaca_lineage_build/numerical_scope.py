"""Admit complete numerical contests from accepted row-selection metadata."""

from bisect import bisect_left

from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

from .contract import LineageError

KINDS = ("completed_years", "current_year", "current_month")


def _intervals(group):
    selected = {}
    for name, scopes in group.scopes.items():
        for scope in scopes:
            selected.setdefault((name, scope["adjustment"], scope["boundary"]), []).append(scope)
    return selected


def _covered(position, intervals):
    return any(scope["row_start"] <= position <= scope["row_end"] for scope in intervals)


def _segments(documents, previous):
    for name, parts in documents.items():
        for kind, document in parts.items():
            for entry in document["adjustments"]:
                for segment in entry["segments"]:
                    if (
                        segment["branch"] == "correlated"
                        and segment.get("observation_selection") != "superseded"
                        and not (
                            kind == "current_year" and segment["from"][:7] >= previous[name]["anchor_month"]
                        )
                    ):
                        yield name, kind, entry["adjustment"], segment


def _branch_overlap(receipt, kind, first, last, window):
    left, right = max(first, window[0]), min(last, window[1])
    if left > right:
        return False
    anchor = receipt["anchor_month"]
    year, month = anchor[:4] + "-01-01", anchor + "-01"
    return (
        left < year
        if kind == "completed_years"
        else right >= year and left < month
        if kind == "current_year"
        else right >= month
    )


def _possible_unread(receipt, kind, view, pid, windows, date_cache):
    handoff = receipt.get("assignment_handoffs", {}).get(kind, {})
    ownership = handoff.get("ownership", {}).get(view)
    if ownership is not None and not any(
        run["owner"][0] == pid and run["rows"] for run in ownership["ownership"]
    ):
        return False
    whole = (receipt.get("serving_ownership") or {}).get("views", {}).get(view)
    if whole is not None and not any(run["owner"][0] == pid and run["rows"] for run in whole["ownership"]):
        return False
    choices = handoff.get("observation_choices")
    if choices is not None:
        runs = choices["partitions"][choices["views"][view]]["runs"]
        bounds = [
            (run["from"], run["to"])
            for run in runs
            if run["owner"] == pid
            and run["state"] != "superseded"
            and not (kind == "current_year" and run["from"][:7] >= receipt["anchor_month"])
        ]
        if bounds:
            return any(
                _branch_overlap(receipt, kind, first, last, window)
                for first, last in bounds
                for window in windows
            )
    # Legacy issuer/ownership chronology is not a complete row-date guarantee.
    # The complete source's accepted first/last observations can still prove
    # it wholly disjoint, without opening an omitted assignment body.
    summary = receipt.get("assignment_summary", {}).get(view, {})
    bounds = []
    for field in ("first_timestamp", "last_timestamp"):
        value = summary.get(field)
        if value is None:
            bounds.append("" if field == "first_timestamp" else "9999-12-31")
        else:
            if value not in date_cache:
                date_cache[value] = timestamp(value).astimezone(NEW_YORK).date().isoformat()
            bounds.append(date_cache[value])
    return any(_branch_overlap(receipt, kind, *bounds, window) for window in windows)


def admit(group, previous, documents):
    """Reject omitted possible contributors before numerical payload reads."""
    selected, windows, endpoints = _intervals(group), {}, {}
    for name, kind, view, segment in _segments(documents, previous):
        pid = segment["security_identity"]["security_id"]
        for scope in selected.get((name, view, kind), ()):
            if (
                scope["public_id"] != pid
                or scope["row_end"] < segment["row_start"]
                or segment["row_end"] < scope["row_start"]
            ):
                continue
            windows.setdefault((pid, view), []).append((segment["from"], segment["to"]))
            known = endpoints.setdefault((pid, view), set())
            if scope["row_start"] <= segment["row_start"] <= scope["row_end"]:
                known.add(segment["from"])
            if scope["row_start"] <= segment["row_end"] <= scope["row_end"]:
                known.add(segment["to"])
    date_cache = {}
    for name, receipt in previous.items():
        for (pid, view), ranges in windows.items():
            for kind in KINDS:
                if kind not in documents.get(name, {}) and _possible_unread(
                    receipt, kind, view, pid, ranges, date_cache
                ):
                    raise LineageError(
                        f"numerical scope omits a potentially overlapping contributor: {name}/{kind}/{view}"
                    )
    for name, kind, view, segment in _segments(documents, previous):
        pid = segment["security_identity"]["security_id"]
        intervals = selected.get((name, view, kind), ())
        for position, day in ((segment["row_start"], segment["from"]), (segment["row_end"], segment["to"])):
            if day in endpoints.get((pid, view), ()) and not _covered(position, intervals):
                raise LineageError(
                    f"numerical scope omits a competing selected endpoint: {name}/{kind}/{view}/{day}"
                )
    return selected


def _remaining(first, last, intervals):
    for scope in intervals:
        if scope["row_end"] < first:
            continue
        if scope["row_start"] > last:
            break
        if first < scope["row_start"]:
            yield first, min(last, scope["row_start"] - 1)
        first = max(first, scope["row_end"] + 1)
        if first > last:
            return
    if first <= last:
        yield first, last


def complete(group, previous, documents, scoped, selected):
    """Close included cut interiors using only already required timestamps."""
    opened = {}
    for name, (_inputs, report, positions, dates) in scoped.items():
        for segment in report["correlated"]["segments"]:
            pid, view = segment["security_identity"]["security_id"], segment["adjustment"]
            days = opened.setdefault((pid, view), set())
            for index in range(segment["row_start"] - 1, segment["row_end"]):
                days.add(dates[view][positions[view][index]][0])
    opened = {key: sorted(days) for key, days in opened.items()}
    for name, kind, view, segment in _segments(documents, previous):
        pid = segment["security_identity"]["security_id"]
        days = opened.get((pid, view))
        if not days:
            continue
        offsets = previous[name]["serving_ownership"]["offsets"][view]
        dates = scoped[name][3][view]
        for first, last in _remaining(
            segment["row_start"], segment["row_end"], selected.get((name, view, kind), ())
        ):
            lower = (
                segment["from"]
                if first == segment["row_start"]
                else dates.get(offsets[kind] + first, (segment["from"],))[0]
            )
            upper = (
                segment["to"]
                if last == segment["row_end"]
                else dates.get(offsets[kind] + last, (segment["to"],))[0]
            )
            index = bisect_left(days, lower)
            if index < len(days) and days[index] <= upper:
                raise LineageError(
                    f"numerical scope omits a possible competing row: {name}/{kind}/{view}/{days[index]}"
                )

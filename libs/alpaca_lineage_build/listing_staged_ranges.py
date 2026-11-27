"""Carry applicable, completed assessment stages across growth of an unpublished ticker range."""

from bisect import bisect_left, bisect_right
from collections import defaultdict
from heapq import heappop, heappush

from libs.common.us_equity_market_session_calendar import market_session_window


def _partition(scope, candidates, cache, version):
    """Cover the selected sessions with whole stages and only the remaining gaps."""
    owner, symbol, first, last, competing = scope
    days = market_session_window(first, last)
    intervals = []
    for candidate, keys in candidates:
        start = bisect_left(days, candidate[2])
        stop = bisect_right(days, candidate[3])
        if start < stop:
            intervals.append((start, -stop, candidate, keys))
    if not intervals:
        return [(scope, None)]
    intervals.sort(key=lambda item: item[:3])
    parts, available = [], []
    cursor = i = 0
    while cursor < len(days):
        # When old independently assessed stages overlap, retain an original
        # result only if it reaches beyond the last selection. Shorter nested
        # stages need neither a retained-point calculation nor a body read.
        while i < len(intervals) and intervals[i][0] <= cursor:
            start, negative_stop, candidate, keys = intervals[i]
            heappush(available, (negative_stop, start, candidate, keys))
            i += 1
        while available and -available[0][0] > cursor:
            negative_stop, _, candidate, keys = heappop(available)
            bound = cache.assessment_key(version, candidate)
            if tuple(bound) in keys:
                parts.append((candidate, bound))
                cursor = -negative_stop
                break
        else:
            available.clear()
            stop = intervals[i][0] if i < len(intervals) else len(days)
            start = parts.pop()[0][2] if parts and parts[-1][1] is None else days[cursor]
            parts.append(((owner, symbol, start, days[stop - 1], competing), None))
            cursor = stop
    return [(scope, None)] if len(parts) == 1 and parts[0][1] is None else parts


def _select(scope, dates, keys, cache, version):
    first, last = scope[2:4]
    exact = keys.get((first, last))
    if exact:
        bound = cache.assessment_key(version, scope)
        if tuple(bound) in exact:
            return [(scope, bound)]
    candidates = []
    for start, end in dates[bisect_left(dates, (first,)) : bisect_right(dates, (last, last))]:
        if end > last or (start, end) == (first, last):
            continue
        candidate = scope[0], scope[1], start, end, scope[4]
        candidates.append((candidate, keys[start, end]))
    return _partition(scope, candidates, cache, version) if candidates else [(scope, None)]


def select_staged_ranges(selected, cache, version):
    """Refine current group work in place, preserving its owner, conflict class and atomic publication.

    A stage supplies its original authenticated result only inside the selected
    owner/label range and with the current rule and retained-point binding.
    Published rows carried by the ownership planner already have their result.
    """
    selected = list(selected)
    wanted = [
        scope
        for group, work in selected
        for scope in work
        if scope not in group.get("carried_listing_assessments", {})
    ]
    scopes = defaultdict(lambda: defaultdict(set))
    for key in cache.staged_assessment_keys(version, wanted) if wanted else ():
        rule, owner, symbol, first, last, competing, *_ = key
        if rule == version:
            scopes[owner, symbol, competing][first, last].add(tuple(key))
    ordered = {label: sorted(keys) for label, keys in scopes.items()}
    result = []
    for group, work in selected:
        refined, staged = [], {}
        carried = group.get("carried_listing_assessments", {})
        for scope in work:
            label = scope[0], scope[1], scope[4]
            if scope in carried or label not in scopes:
                refined.append(scope)
                continue
            for item, key in _select(scope, ordered[label], scopes[label], cache, version):
                refined.append(item)
                if key is not None:
                    staged[item] = key
        group["listing_work_scopes"] = refined
        group["staged_listing_assessment_keys"] = staged
        result.append((group, refined))
    return result

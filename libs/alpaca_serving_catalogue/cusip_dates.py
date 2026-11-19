"""Constrain public CUSIP dates using provider splits of the accepted security."""

from collections import defaultdict
from datetime import date, timedelta

from libs.alpaca_security_lineage.inference_evidence import provider_actions


def split_evidence(sources):
    """Collect endpoint dates and exact lookup identities once per selected group."""
    changes = set()
    for source in sources.values():
        evidence = source["evidence"]
        if "provider_corporate_actions" not in evidence and evidence.get("event_supplements"):
            continue
        for action in provider_actions(evidence):
            if action.get("action_type") not in {"forward_split", "reverse_split"}:
                continue
            symbol, old, new = (action.get(field) for field in ("symbol", "old_cusip", "new_cusip"))
            when = action.get("ex_date") or action.get("effective_date")
            if (
                not all(isinstance(v, str) and v for v in (symbol, old, new, when))
                or old == new
                or action.get("new_symbol") not in (None, "", symbol)
            ):
                continue
            try:
                day = date.fromisoformat(when).isoformat()
            except ValueError:
                continue
            changes.add((symbol, day, old, new))
    assets = {cusip: set() for _, _, old, new in changes for cusip in (old, new)}
    if assets:
        for source in sources.values():
            for lookup in source["evidence"]["asset_lookups"]:
                asset = lookup.get("asset") or {}
                if (
                    lookup.get("lookup_kind") == "cusip"
                    and lookup.get("lookup") in assets
                    and lookup.get("outcome") == "found"
                    and lookup.get("found") is True
                    and lookup.get("http_status") == 200
                    and asset.get("class") == "us_equity"
                    and asset.get("id")
                ):
                    assets[lookup["lookup"]].add(asset["id"])
    return assets, changes


def supported_periods(evidence, members, tickers):
    """Existing dated claims can survive only in compatible half-open periods."""
    assets, changes = evidence
    events = defaultdict(dict)
    transitions = defaultdict(set)
    for symbol, day, old, new in changes:
        if symbol not in tickers or any(
            len(assets[value]) != 1 or not assets[value] <= members for value in (old, new)
        ):
            continue
        transitions[day, old].add((old, new))
        transitions[day, new].add((old, new))
        events[old][day] = "exit"
        events[new][day] = "entry"
    ambiguous = {
        endpoint
        for same_day in transitions.values()
        if len(same_day) > 1
        for change in same_day
        for endpoint in change
    }
    periods = {}
    for value, dated in events.items():
        windows = periods[value] = []
        if value in ambiguous:
            continue
        start = previous = None
        for day, direction in sorted(dated.items()):
            if direction == previous:
                windows.clear()
                break
            if direction == "exit":
                windows.append((start, day))
            else:
                start = day
            previous = direction
        else:
            if previous == "entry":
                windows.append((start, None))
    return periods


def _pieces(first, end, windows):
    """Keep the owner over the entire interval, including undated endpoints."""
    boundaries = sorted(
        {
            point
            for window in windows
            for point in window
            if point and (first is None or point > first) and (end is None or point < end)
        }
    )
    for left, right in zip([first, *boundaries], [*boundaries, end]):
        allowed = any(
            (start is None or left is not None and start <= left)
            and (stop is None or right is not None and right <= stop)
            for start, stop in windows
        )
        yield left, right, allowed


def constrain(intervals, observations, periods):
    if not periods:
        return intervals, observations
    owners = []
    for item in intervals:
        value = item["cusip"]
        if value not in periods:
            owners.append(item)
            continue
        for first, end, allowed in _pieces(item["start_inclusive"], item["end_exclusive"], periods[value]):
            owners.append(
                {**item, "start_inclusive": first, "end_exclusive": end, "cusip": value if allowed else None}
            )
    dated = []
    for item in observations:
        if item["field"] != "cusip" or item["effective_from"] is None or item["value"] not in periods:
            dated.append(item)
            continue
        end = (date.fromisoformat(item["effective_through"]) + timedelta(days=1)).isoformat()
        for first, stop, allowed in _pieces(item["effective_from"], end, periods[item["value"]]):
            if allowed:
                through = (date.fromisoformat(stop) - timedelta(days=1)).isoformat()
                dated.append({**item, "effective_from": first, "effective_through": through})
    return owners, dated

"""Retain an accepted recent history when an older extension is separated by a gap."""

from bisect import bisect_right

from alpaca_end_of_day._history_coverage import coverage_sources
from libs.alpaca_end_of_day_security_correlation.action_identity import exact_symbol_lookup_asset
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import market_sessions_between

SUPPORTED_SUFFIX_VERSION = 1


def supported_current_source(sources, inputs, members, public_id, last_session, *, symbols=None):
    """Require a selected all-view own-label endpoint for the current security."""
    for symbol, source in inputs.items():
        if symbol not in sources or (symbols is not None and symbol not in symbols):
            continue
        views = source["views"]
        if set(views) != set(ADJUSTMENTS) or any(
            view["last_session"] != last_session
            or not view["ownership"]
            or view["ownership"][-1]["owner"][0] != public_id
            or symbol not in view["ownership"][-1]["owner"][1]
            for view in views.values()
        ):
            continue
        asset = exact_symbol_lookup_asset(sources[symbol]["evidence"], symbol)
        if (
            asset
            and asset["id"] in members
            and asset.get("status") == "active"
            and asset.get("tradable") is True
        ):
            return True
    return False


def clip_history(history, dates, cutoff):
    """Project the first supported range from accepted session dates and labels."""
    start = bisect_right(dates, cutoff)
    first = dates[start]
    return {
        **history,
        "first_session": first,
        "ticker_ranges": [
            {**run, "first_session": max(first, run["first_session"])}
            for run in history["ticker_ranges"]
            if run["last_session"] >= first
        ],
        "coverage": {
            **history["coverage"],
            "session_count": len(dates) - start,
            "explained_missing_sessions": [
                day for day in history["coverage"]["explained_missing_sessions"] if day > first
            ],
        },
    }


def legacy_session_dates(first, last, missing):
    """Assemble dates from an accepted clean calendar outcome's exact missing list."""
    return [day for day in market_sessions_between(first, last) if day not in missing]


def legacy_start(record, decision, *, absent_sessions):
    """Locate the distinct later label range, applying prior physical evidence when available."""
    if decision["reasons"] != ["unexplained_missing_sessions"]:
        return None
    missing = set(record["missing_sessions"]).difference(decision["explained_missing_sessions"])
    if not missing:
        return None
    witnesses = record["gap_bars"]
    right = bisect_right(witnesses, max(missing), key=lambda bar: bar["session"])
    if not 0 < right < len(witnesses):
        return None
    left, resumed = witnesses[right - 1 : right + 1]
    if len(left["tickers"]) != 1 or len(resumed["tickers"]) != 1 or left["tickers"] == resumed["tickers"]:
        return None
    start, label = resumed["session"], resumed["tickers"][0]
    if absent_sessions is not None and any(
        day not in absent_sessions for day in missing if left["session"] < day < start
    ):
        return None
    # Every view's labels contribute to these completed ranges. A reused label
    # or a second label anywhere in the later block does not isolate that block.
    relevant = [
        item for item in record["ticker_ranges"] if item["ticker"] == label or item["last_session"] >= start
    ]
    if relevant != [{"ticker": label, "first_session": start, "last_session": record["last_session"]}]:
        return None
    return start


def retained_range(group, record, latest_gap, last_session):
    """Select a supported suffix from the actual same-ID serving checkpoint."""
    if group is None:
        return None, None
    public_id = record["public_id"]
    accepted = next((item for item in group["securities"] if item["public_id"] == public_id), None)
    if accepted is not None:
        if latest_gap < accepted["first_session"] and accepted["last_session"] <= last_session:
            return accepted["first_session"], accepted
        return None, None
    if group["history_validation"].get("gap_suffix_version") is not None:
        return None, None
    if "_legacy_gap_starts" in group:
        start = group["_legacy_gap_starts"].get(public_id)
    else:
        decision = next(item for item in group["decisions"] if item["public_id"] == public_id)
        history = group["history_validation"]
        record = next(item for item in history["prefix_records"] if item["public_id"] == public_id)
        # Older policies can lack source-specific raw provenance. The current
        # contributors' exact seam presence is resolved by applicable_absence.
        start = legacy_start(record, decision, absent_sessions=None)
    return start if start is not None and latest_gap < start else None, None


def applicable_absence(snapshot, previous, group, record, start, unresolved, coverage, inputs, observed_raw):
    """Keep a legacy screening result only while its separating gap is physically absent."""
    from .gaps import resolve_raw_presence

    old = group["history_validation"]
    old_sources = {s for view in old["identities"][record["public_id"]].values() for s in coverage_sources(view)}
    sources = {s for view in coverage.values() for s in coverage_sources(view)}
    changed = {
        s: inputs[s]["pricing_generation"]
        for s in sources
        if "_legacy_gap_starts" not in group
        or s not in old_sources
        or inputs[s]["pricing_generation"] != old["inputs"][s]["pricing_generation"]
    }
    if not changed:
        return True
    held = next(item for item in old["prefix_records"] if item["public_id"] == record["public_id"])
    left = max(bar["session"] for bar in held["gap_bars"] if bar["session"] < start)
    seam = {day for day in unresolved if left < day < start}
    if not seam:
        return True
    facts = resolve_raw_presence(snapshot, dict.fromkeys(changed, seam), previous, changed, observed_raw)
    for source, selected in facts.items():
        present = set(selected["present_sessions"])
        observed_raw.setdefault(source, {}).update((day, day in present) for day in seam)
    return not any(selected["present_sessions"] for selected in facts.values())

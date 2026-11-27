"""Select fresh and carried historical listing ranges from the accepted public ownership roster."""

from collections import defaultdict
from datetime import date, timedelta

from libs.common.us_equity_market_session_calendar import market_sessions_between

COMPETING = "competing_published_owners"


def _day(day, offset):
    return (date.fromisoformat(day) + timedelta(days=offset)).isoformat()


def _overlaps(a, b, first, last):
    return a <= last and first <= b


def _runs(spans):
    """Union overlapping inclusive spans, preserving disjoint published windows."""
    ranges = []
    for first, last in sorted(spans):
        if ranges and first <= ranges[-1][1]:
            ranges[-1] = ranges[-1][0], max(last, ranges[-1][1])
        else:
            ranges.append((first, last))
    return ranges


def _parts(first, last, windows):
    """Cut only where a window changes on an actual market session."""
    cuts = set()
    for a, b in windows:
        if first < a <= last:
            cuts.add(a)
        if first <= b < last:
            cuts.add(_day(b, 1))
    if not cuts:
        yield first, last
        return
    edges = [first, *sorted(cuts), _day(last, 1)]
    for start, stop in zip(edges, edges[1:]):
        sessions = market_sessions_between(start, _day(stop, -1))
        if sessions:
            yield sessions[0], sessions[-1]


def _pieces(symbol, owned, rivals, previous, superseded, *, replacements_only=False):
    # Accepted ranges can overlap. Replace the union of the affected intervals
    # together so that an unchanged overlapping result cannot conceal obsolete facts.
    previous = [r for r in previous if r["symbol"] == symbol]
    scopes = [(r["first_session"], r["last_session"]) for r in previous]
    was_competing = [COMPETING in r.get("limits", ()) for r in previous]
    changed = set()
    for i, (first, last) in enumerate(scopes):
        for a, _ in _parts(first, last, rivals):
            if was_competing[i] != any(x <= a <= y for x, y in rivals):
                changed.add(i)
                break
    if replacements_only and not changed:
        return
    for start, end in _runs(scopes) if changed else ():
        if any(start <= scopes[i][0] <= end for i in changed):
            changed.update(i for i, (a, _) in enumerate(scopes) if start <= a <= end)
    superseded.update((symbol, *scopes[i]) for i in changed)

    for first, last in _runs(owned):
        relevant = [(a, b) for a, b in rivals if _overlaps(a, b, first, last)]
        covered = [i for i, (a, b) in enumerate(scopes) if _overlaps(a, b, first, last)]
        if not relevant and not covered:
            if not replacements_only:
                yield first, last, False, ()
            continue
        # Adjacent pieces with the same status and donor set can be published
        # together even if one rival leaves precisely where a different rival enters.
        pending = None
        for a, b in _parts(first, last, [*relevant, *(scopes[i] for i in covered)]):
            matching = tuple(i for i in covered if scopes[i][0] <= a <= scopes[i][1])
            if (replacements_only and not matching) or any(i not in changed for i in matching):
                if pending:
                    yield pending[0], pending[1], pending[2], tuple(previous[i] for i in pending[3])
                    pending = None
                continue
            competing = any(x <= a <= y for x, y in relevant)
            donors = tuple(i for i in matching if was_competing[i] == competing)
            if pending and pending[2:] == (competing, donors):
                pending = pending[0], b, competing, donors
            else:
                if pending:
                    yield pending[0], pending[1], pending[2], tuple(previous[i] for i in pending[3])
                pending = a, b, competing, donors
        if pending:
            yield pending[0], pending[1], pending[2], tuple(previous[i] for i in pending[3])


def _carry(rows, symbol, first, last, competing):
    # Projection of accepted observations only. A transition needs both of
    # its original dated endpoints inside this still applicable piece.
    points, changes = {}, {}
    limits = set()
    for row in rows:
        for point in row["observations"]:
            if first <= point["session"] <= last:
                points[tuple(sorted(point.items()))] = point
        for change in row["changes"]:
            if first <= change["from_session"] and change["to_session"] <= last:
                changes[tuple(sorted(change.items()))] = change
        if "auction_screen_unavailable" in row["limits"]:
            limits.add("auction_screen_unavailable")
    observations = sorted(points.values(), key=lambda p: (p["session"], p["trade_at"], p["tape"]))
    transitions = sorted(
        changes.values(), key=lambda c: (c["from_session"], c["to_session"], c["from_tape"], c["to_tape"])
    )
    if competing:
        limits.add(COMPETING)
    else:
        if len({p["session"] for p in observations}) < len(market_sessions_between(first, last)):
            limits.add("sampled_sessions_only")
        if any(p["tape"] == "B" for p in observations):
            limits.add("multiple_listings_on_tape_b")
        if any("effective_session" not in c for c in transitions):
            limits.add("change_date_bounded")
        if not observations and all("no_recognized_trade_tape" in row["limits"] for row in rows):
            limits.add("no_recognized_trade_tape")
    return {
        "symbol": symbol,
        "first_session": first,
        "last_session": last,
        "status": "changes_observed" if transitions else "sampled" if observations else "unresolved",
        "observations": observations,
        "changes": transitions,
        "limits": sorted(limits),
        "diagnostics": {
            "retained_from": [
                {k: row[k] for k in ("symbol", "first_session", "last_session", "status", "limits")}
                for row in rows
            ]
        },
    }


def _rivals(pid, symbol, owners):
    return _runs((a, b) for other, a, b in owners[symbol] if other != pid)


def _slices(pid, symbol, first, last, owners, completed):
    """Expose the selected calendar slices with their current competition classification."""
    for a, b, competing, _ in _pieces(symbol, [(first, last)], _rivals(pid, symbol, owners), completed, set()):
        yield a, b, competing


def _work(groups, owners, facts, public_ids=None, recovered_owners_by_group=None):
    requested = {pid.lower() for pid in public_ids} if public_ids else None
    for key, group in sorted(groups.items()):
        work, carried, retired = [], {}, {}
        for pid, spans in sorted(group["ticker_ranges"].items()):
            replacements_only = requested is not None and pid not in requested
            if replacements_only and pid not in (recovered_owners_by_group or {}).get(key, ()):
                continue
            previous = facts[key].get(pid, {}).get("historical_listings", ())
            by_symbol = defaultdict(list)
            for span in spans:
                by_symbol[span["ticker"]].append((span["first_session"], span["last_session"]))
            replaced = set()
            for symbol, owned in by_symbol.items():
                for first, last, competing, donors in _pieces(
                    symbol,
                    owned,
                    _rivals(pid, symbol, owners),
                    previous,
                    replaced,
                    replacements_only=replacements_only,
                ):
                    scope = pid, symbol, first, last, competing
                    work.append(scope)
                    if donors:
                        carried[scope] = _carry(donors, symbol, first, last, competing)
            if replaced:
                retired[pid] = sorted(replaced)
        group["superseded_listing_scopes"] = retired
        group["carried_listing_assessments"] = carried
        group["listing_work_scopes"] = work
        if work:
            yield group, work

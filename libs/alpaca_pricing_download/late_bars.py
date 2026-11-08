"""Exact selected-session transport, never an enclosing symbol/date rectangle."""

from datetime import date, datetime, time, timedelta, timezone

from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import NEW_YORK, DownloadError, timestamp


def session_ranges(sessions, *, coalesce=True, max_sessions=10000, cutoff=None):
    """Coalesce adjacent selected exchange sessions, including holiday/weekend seams.

    Unselected exchange sessions break a run. Callers keep each source's mapping
    date; the ordinary solver batches only identical resulting ranges.
    """
    if not sessions:
        return []
    days = sorted(set(sessions))
    after = (date.fromisoformat(days[-1]) + timedelta(days=1)).isoformat()
    calendar = expected_market_session_coverage(days[0], after).session_dates
    positions = {day: index for index, day in enumerate(calendar)}
    if any(day not in positions for day in days):
        raise DownloadError("late-bar selection contains a non-market session")
    runs = []
    first = last = days[0]
    count = 1
    for day in days[1:]:
        if not coalesce or count >= max_sessions or positions[day] != positions[last] + 1:
            runs.append((first, last))
            first = day
            count = 0
        last = day
        count += 1
    runs.append((first, last))
    ranges = [
        {
            "start": datetime.combine(date.fromisoformat(first), time.min, tzinfo=NEW_YORK)
            .astimezone(timezone.utc)
            .isoformat(),
            "end_exclusive": datetime.combine(
                date.fromisoformat(last) + timedelta(days=1), time.min, tzinfo=NEW_YORK
            )
            .astimezone(timezone.utc)
            .isoformat(),
        }
        for first, last in runs
    ]
    if cutoff is not None:
        for window in ranges:
            window["end_exclusive"] = min(timestamp(window["end_exclusive"]), timestamp(cutoff)).isoformat()
    return ranges

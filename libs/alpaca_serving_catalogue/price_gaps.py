"""Project accepted unpriced quote sessions onto public coverage, independently of ownership."""

from libs.alpaca_pricing_download.untraded_split import UNPRICED


def active(limits):
    return [
        v
        for v in limits.values()
        if v["reason"] == UNPRICED or v.get("restored_review") == UNPRICED and v.get("pending")
    ]


def project(summary, selected, session_dates=None):
    selected = list(selected)
    if not selected:
        return summary
    first, last = summary["first_session"], summary["last_session"]
    # Every excluded date names an actual stored row accepted by this producer.
    deleted = {d for item in selected for d in item["excluded_sessions"] if first <= d <= last}
    for item in selected:
        if item.get("pending") and item["terminal"] and last > item["witness_end"]:
            # A new suffix needs a new comparison with the resumed tape. Its
            # accepted date projection supplies the exact provisional span.
            if session_dates is None:
                raise ValueError("terminal price-gap review requires the extended date projection")
            deleted.update(day for day in session_dates if max(first, item["witness_end"]) < day <= last)
    existing = set(summary["coverage"]["explained_missing_sessions"])
    # A replacement can physically omit an earlier excluded mark. Its accepted
    # missing-date projection has already removed that date from the row count.
    deleted -= existing
    if not deleted:
        return summary
    allowed_last = last
    for item in sorted(selected, key=lambda v: v["cut"], reverse=True):
        high = last if item.get("pending") and item["terminal"] else item["excluded_sessions"][-1]
        if item["cut"] <= allowed_last <= high:
            allowed_last = item["previous_session"]
    if first in deleted:
        # An independent pre-event restriction may overlap the opening of a gap.
        following = [
            item["witness_end"]
            for item in selected
            if first in item["excluded_sessions"] and not item["terminal"]
        ]
        if following:
            first = max(following)
    ranges = [
        {
            **r,
            "first_session": max(first, r["first_session"]),
            "last_session": min(allowed_last, r["last_session"]),
        }
        for r in summary["ticker_ranges"]
        if r["last_session"] >= first and r["first_session"] <= allowed_last
    ]
    return {
        **summary,
        "first_session": first,
        "last_session": allowed_last,
        "ticker_ranges": ranges,
        "price_exclusions": sorted(
            [v["cut"], v["excluded_sessions"][-1]]
            for v in selected
            if v["cut"] <= last and v["excluded_sessions"][-1] >= first
        ),
        "coverage": {
            **summary["coverage"],
            "session_count": summary["coverage"]["session_count"] - len(deleted),
            "explained_missing_sessions": sorted(d for d in existing | deleted if first < d < allowed_last),
        },
    }

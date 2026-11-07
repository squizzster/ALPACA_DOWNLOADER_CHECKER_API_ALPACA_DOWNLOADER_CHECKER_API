"""Retain closed assignments before a producer-scoped latest-session correction."""

from copy import deepcopy

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import LineageError


def correction_from(source, previous):
    """Accept the pricing cause; unresolved ownership retains its broader owner."""
    if not previous or previous.get("unresolved_rows") or previous.get("row_partition_complete") is not True:
        return None
    correction = source.get("latest_session_correction")
    if not correction or previous["source_sha256"] not in {
        correction.get("source_sha256"),
        *correction.get("unchanged_ancestors", ()),
    }:
        return None
    day = timestamp(correction["session"]).astimezone(NEW_YORK).date().isoformat()
    if not any(
        change["source_sha256"] == previous["source_sha256"] and change["changed_from"] == day
        for change in source.get("history_changes", ())
    ):
        return None
    summary = previous.get("assignment_summary", {})
    if set(summary) != set(ADJUSTMENTS) or any(item["last_session"] != day for item in summary.values()):
        return None
    return day


def prefix_assignments(previous, boundaries, day, prefix_ends):
    """Clip the single corrected row from accepted metadata for output assembly.

    The source producer certifies one replaced session per view. Earlier
    timestamps come from the already decoded affected pricing partition or an
    accepted earlier assignment boundary. No ownership rule is rerun here.
    """
    summaries = deepcopy(previous["assignment_summary"])
    clipped = deepcopy(boundaries)
    for name in ADJUSTMENTS:
        old = summaries[name]
        count = old["rows"] - 1
        candidates = [prefix_ends[name]] if prefix_ends.get(name) else []
        candidates.extend(
            segment["last_timestamp"]
            for kind, document in boundaries.items()
            for entry in document["adjustments"]
            if entry["adjustment"] == name
            for segment in entry["segments"]
            if segment["to"] < day and (kind != "current_year" or segment["from"][:7] < previous["anchor_month"])
        )
        last = max(candidates, key=timestamp) if candidates else None
        if count < 0 or count and last is None:
            raise LineageError("latest correction is missing its retained assignment prefix")
        old.update(
            rows=count,
            first_timestamp=old["first_timestamp"] if count else None,
            last_timestamp=last if count else None,
            last_session=timestamp(last).astimezone(NEW_YORK).date().isoformat() if count else None,
        )
        for document in clipped.values():
            entry = next(item for item in document["adjustments"] if item["adjustment"] == name)
            segments = []
            for segment in entry["segments"]:
                if segment["from"] >= day:
                    continue
                if segment["to"] >= day:
                    segment.update(
                        row_count=segment["row_count"] - 1,
                        row_end=segment["row_end"] - 1,
                        last_timestamp=last,
                        to=old["last_session"],
                    )
                if segment["row_count"]:
                    segments.append(segment)
            entry.update(segments=segments, rows=sum(segment["row_count"] for segment in segments))
    return summaries, clipped


def complete_prefix_boundaries(root, previous, loaded, day):
    """Select earlier assignment metadata only if the affected partition has one row."""
    from . import storage

    kind = (
        "current_month"
        if day[:7] == previous["anchor_month"]
        else "current_year"
        if day[:4] == previous["anchor_month"][:4]
        else "completed_years"
    )
    if all(entry["rows"] > 1 for entry in loaded[kind]["adjustments"]):
        return
    for older in (
        ("current_year", "completed_years")
        if kind == "current_month"
        else ("completed_years",)
        if kind == "current_year"
        else ()
    ):
        if older not in loaded:
            loaded.update(storage.read_boundaries(root, previous, kinds={older}))
        if any(segment["to"] < day for entry in loaded[older]["adjustments"] for segment in entry["segments"]):
            break

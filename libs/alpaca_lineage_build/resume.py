"""Conservative reuse of approved, single-identity history.

This is a cache gate, not an identity rule. When continuity or evidence can change
an earlier decision, the established complete-chronology transform owns that work.
"""

from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import reusable_decision


def unchanged_source(source, previous, *, selected=None):
    """The pricing producer's completed proof covers every retained row."""
    return (
        previous is not None
        and reusable_decision(previous)
        and previous["processed_through"] <= source["latest_request"]["end_exclusive"]
        and (
            selected == previous["source_sha256"]
            or any(
                change["source_sha256"] == previous["source_sha256"] and change["changed_from"] is None
                for change in source.get("history_changes", ())
            )
        )
    )


def eligible_source(source, previous, *, refresh):
    return (
        not refresh
        and previous is not None
        and reusable_decision(previous)
        and previous["processed_through"] <= source["latest_request"]["end_exclusive"]
        and previous["source_rows"] > 0
        and previous["unresolved_rows"] == 0
        and not previous["dependencies"]
        and len(previous["asset_ids"]) == 1
        and any(
            change["source_sha256"] == previous["source_sha256"]
            and (change["changed_from"] is None or change["changed_from"] >= previous["processed_through"][:10])
            for change in source.get("history_changes", ())
        )
    )


def assignment_summary(previous, boundaries):
    """Consume published assignments when an older receipt lacks their summary.

    This is a projection of accepted results, not another rule execution or
    pricing audit. New receipts carry it so closed assignment bodies stay closed.
    """
    if "assignment_summary" in previous:
        return previous["assignment_summary"]
    result = {}
    for adjustment in ADJUSTMENTS:
        segments = [
            segment
            for kind, document in boundaries.items()
            for item in document["adjustments"]
            if item["adjustment"] == adjustment
            for segment in item["segments"]
            if kind != "current_year" or segment["from"][:7] < previous["anchor_month"]
        ]
        result[adjustment] = {
            "rows": sum(segment["row_count"] for segment in segments),
            "first_timestamp": min((s["first_timestamp"] for s in segments), default=None),
            "last_timestamp": max((s["last_timestamp"] for s in segments), default=None),
            "last_session": max((s["to"] for s in segments), default=None),
        }
    return result


def stable_evidence(previous, current):
    fields = (
        "identities",
        "corporate_actions",
        "symbol_occupancy_evidence",
        "input_symbol_occupants",
        "unresolved_cusips",
        "queried_symbols",
    )

    def lookup_facts(evidence):
        return [
            {key: value for key, value in lookup.items() if key != "source"}
            if lookup.get("source") == "asset_catalogue"
            else lookup
            for lookup in evidence["asset_lookups"]
        ]

    return lookup_facts(previous) == lookup_facts(current) and all(
        previous[field] == current[field] for field in fields
    )


def unique_assignment(segments, asset_id):
    return bool(segments) and all(
        segment.get("correlation_method") == "unique_uuid_candidate_with_warnings"
        and segment.get("security_identity", {}).get("member_alpaca_asset_ids") == [asset_id]
        for segment in segments
    )


def can_resume(view, previous, evidence):
    if not stable_evidence(previous["evidence"], evidence):
        return False
    if not any(view.views.values()):
        return True
    summary = view.previous_assignment_summary
    checked, dates, calendars = set(), {}, {}
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        last = summary[adjustment]["last_session"]
        if last is None:
            return False
        key = id(rows), last
        if key in checked:
            continue
        checked.add(key)
        for row in rows:
            if row[0] not in dates:
                dates[row[0]] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
        sessions = tuple(dates[row[0]] for row in rows)
        # Only the added range and its predecessor seam are new evidence.
        first = (date.fromisoformat(last) + timedelta(days=1)).isoformat()
        if first not in calendars:
            calendars[first] = expected_market_session_coverage(
                first, view.source["latest_request"]["end_exclusive"]
            )
        expected = calendars[first]
        if sessions != expected.session_dates:
            return False
    return True

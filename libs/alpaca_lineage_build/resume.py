"""Conservative reuse of approved, single-identity history.

This is a cache gate, not an identity rule. When continuity or evidence can change
an earlier decision, the established complete-chronology transform owns that work.
"""

from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import versions


def eligible_source(source, previous, notice, *, refresh):
    return (
        not refresh
        and previous is not None
        and previous["versions"] == versions()
        and previous["anchor_month"] == source["anchor_month"]
        and previous["source_rows"] > 0
        and previous["unresolved_rows"] == 0
        and not previous["dependencies"]
        and len(previous["asset_ids"]) == 1
        and (notice is None or notice["from"] >= source["anchor_month"] + "-01")
        and all(
            previous["pricing_boundaries"][kind] == source["boundaries"][kind]
            for kind in ("completed_years", "current_year")
        )
    )


def stable_evidence(previous, current):
    fields = (
        "asset_lookups",
        "identities",
        "corporate_actions",
        "symbol_occupancy_evidence",
        "input_symbol_occupants",
        "unresolved_cusips",
        "queried_symbols",
    )
    return all(previous[field] == current[field] for field in fields)


def unique_assignment(segments, asset_id):
    return bool(segments) and all(
        segment.get("correlation_method") == "unique_uuid_candidate_with_warnings"
        and segment.get("security_identity", {}).get("member_alpaca_asset_ids") == [asset_id]
        for segment in segments
    )


def can_resume(view, previous, old, evidence):
    if not stable_evidence(previous["evidence"], evidence):
        return False
    asset_id = previous["asset_ids"][0]
    anchor = view.source["anchor_month"]
    for adjustment in ADJUSTMENTS:
        segments = [
            segment
            for document in old.values()
            for item in document["adjustments"]
            if item["adjustment"] == adjustment
            for segment in item["segments"]
        ]
        if not unique_assignment(segments, asset_id):
            return False
        rows = view.views[adjustment]
        if not rows:
            return False
        sessions = tuple(timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows)
        prior = [segment["to"] for segment in segments if segment["from"][:7] < anchor]
        # Check only the newly acquired month and its single predecessor seam.
        # Previously published sessions/decisions are neither reopened nor audited.
        first = (date.fromisoformat(max(prior)) + timedelta(days=1)).isoformat() if prior else sessions[0]
        expected = expected_market_session_coverage(first, view.source["latest_request"]["end_exclusive"])
        if sessions != expected.session_dates:
            return False
    return True

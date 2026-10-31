"""Conservative reuse of approved, single-identity history.

This is a cache gate, not an identity rule. When continuity or evidence can change
an earlier decision, the established complete-chronology transform owns that work.
"""

from collections import defaultdict
from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.debug_trace import event
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import reusable_decision


def unchanged_source(source, previous, *, selected=None):
    """The pricing producer's completed proof covers every retained row."""
    # DO NOT compare historical rows to confirm an unchanged generation/ancestry
    # handoff. Pricing owns that guarantee across all views, including removals.
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
    return eligible_prefix(source, previous, refresh=refresh) and not previous["dependencies"]


def eligible_prefix(source, previous, *, refresh, selected=None):
    """Pricing ancestry proves a resolved single-identity prefix is unchanged."""
    # The completed lineage receipt owns the prefix decision. Pricing ancestry
    # supplies its exact invalidation condition; a later generation is safe only
    # when its earliest possible change is at/after the exclusive lineage cutoff.
    reason = None
    if refresh:
        reason = "explicit_refresh"
    elif previous is None:
        reason = "no_prior_receipt"
    elif not reusable_decision(previous):
        reason = "incompatible_decision"
    elif previous["processed_through"] > source["latest_request"]["end_exclusive"]:
        reason = "coverage_regressed"
    elif previous["source_rows"] == 0:
        reason = "empty_prefix"
    elif previous["unresolved_rows"]:
        reason = "unresolved_prefix"
    elif len(previous["asset_ids"]) != 1:
        reason = "multiple_prefix_identities"
    elif not (
        selected == previous["source_sha256"]
        or any(
            change["source_sha256"] == previous["source_sha256"]
            and (change["changed_from"] is None or change["changed_from"] >= previous["processed_through"][:10])
            for change in source.get("history_changes", ())
        )
    ):
        reason = "ancestry_missing_or_prefix_changed"
    event("prefix_eligibility", symbol=source.get("symbol"), reason=reason or "accepted")
    return reason is None


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


def stable_observation_priority(inputs, previous, old_boundaries, reports):
    """Consume accepted last assignments; a changed route rank needs full work.

    Shared-identity sources can compete for older numerical observations even
    when pricing ancestry is unchanged. Continue only an equal-ranked group
    whose accepted own-label endpoints and new endpoints remain equal. Missing
    endpoint assignments in the already-required bodies are not permission to
    open additional history to prove this gate.
    """
    identities = defaultdict(list)
    for name, prior in previous.items():
        identities[prior["asset_ids"][0]].append(name)
    for members in identities.values():
        if len(members) < 2:
            continue
        old_last, new_last = set(), set()
        for name in members:
            summary = previous[name].get("assignment_summary", {}).get("raw", {})
            last = summary.get("last_timestamp")
            if not last:
                return False
            accepted = [
                segment
                for body in old_boundaries[name].values()
                for adjustment in body["adjustments"]
                if adjustment["adjustment"] == "raw"
                for segment in adjustment["segments"]
                if segment["last_timestamp"] == last and segment.get("historical_symbol") == name
            ]
            if not accepted:
                return False
            old_last.add(last)
            added = inputs[name].views["raw"]
            if added and any(
                s.get("historical_symbol") != name
                for s in reports[name]["correlated"]["segments"]
                if s["adjustment"] == "raw"
            ):
                return False
            new_last.add(added[-1][0] if added else last)
        if len(old_last) != 1 or len(new_last) != 1:
            return False
    return True


def can_resume(view, previous, evidence):
    # This owner checks changed evidence and the NEW suffix/seam only. Failure
    # delegates the selected history to full reconciliation; it is not permission
    # to authenticate unrelated stored groups or add checks to successful reuse.
    if not stable_evidence(previous["evidence"], evidence):
        event("continuation_rejected", reason="evidence_changed")
        return False
    if not any(view.views.values()):
        return True
    summary = view.previous_assignment_summary
    checked, dates, calendars = set(), {}, {}
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        last = summary[adjustment]["last_session"]
        if last is None:
            event("continuation_rejected", reason="missing_assignment_seam", adjustment=adjustment)
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
            event(
                "continuation_rejected",
                reason="incomplete_session_seam",
                adjustment=adjustment,
                observed_sessions=len(sessions),
                expected_sessions=len(expected.session_dates),
            )
            return False
    return True

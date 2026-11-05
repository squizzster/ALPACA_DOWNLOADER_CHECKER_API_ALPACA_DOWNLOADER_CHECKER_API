"""A yes/no boundary over accepted lineage inputs, without identity rules."""

from copy import deepcopy
from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.debug_trace import event
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import reusable_decision


def request_context(request):
    # The latest HTTP page/window start is not the source's retained start.
    # history_start is the producer's effective request_from handoff.
    return {key: value for key, value in request.items() if key != "start"}


def unchanged_source(source, previous, *, selected=None):
    """Unchanged pricing stays unchanged when only accepted coverage advances."""
    if (
        not previous
        or not reusable_decision(previous)
        or not unchanged_rows(source, previous, selected=selected)
    ):
        return False
    before = request_context(previous.get("pricing_request", {}))
    after = request_context({**source["latest_request"], "history_start": source.get("history_start")})
    old_end, new_end = before.pop("end_exclusive", None), after.pop("end_exclusive", None)
    old_asof, new_asof = before.pop("asof", None), after.pop("asof", None)
    if not old_end or not new_end or new_end < old_end:
        return False
    # Normal current-label mappings advance with coverage; historical mappings
    # remain pinned. A genuinely changed mapping still selects reconciliation.
    if old_asof != new_asof and (old_asof != old_end[:10] or new_asof != new_end[:10]):
        return False
    return before == after


def unchanged_rows(source, previous, *, selected=None):
    """Consume pricing's completed all-view equality, independently of requests."""
    return previous is not None and (
        selected == previous["source_sha256"]
        or any(
            change["source_sha256"] == previous["source_sha256"] and change["changed_from"] is None
            for change in source.get("history_changes", ())
        )
    )


def reusable_prefix(source, previous, *, selected=None, refresh=False):
    """Select an unchanged isolated prefix, without predicting new assignments.

    Pricing owns equality of every retained view. The completed lineage receipt
    owns its assignments. Only an isolated, nonempty, fully resolved single
    identity with distributions/splits can retain that prefix across an append.
    Cross-label history, structural events, ambiguity and unknown action types
    select complete-group work. No ownership rules are executed here.
    """

    def selected_prefix(accepted, reason):
        event("retained_prefix", symbol=source["symbol"], accepted=accepted, reason=reason)
        return accepted

    if refresh or not previous or not reusable_decision(previous):
        return selected_prefix(False, "refresh_or_missing_compatible_receipt")
    facts = previous["evidence"]
    symbol = source["symbol"]
    if previous["dependencies"] or facts["queried_symbols"] != [symbol]:
        return selected_prefix(False, "cross_label_history")
    if previous["source_rows"] == 0:
        return selected_prefix(False, "empty_prefix")
    if previous["unresolved_rows"] or len(previous["asset_ids"]) != 1:
        return selected_prefix(False, "unresolved_or_multiple_identities")
    if any(item.get("symbol") != symbol for item in facts["identities"]):
        return selected_prefix(False, "foreign_identity_label")
    if any(
        item.get("action_type") not in {"cash_dividend", "forward_split", "reverse_split"}
        for item in facts["corporate_actions"]
    ):
        return selected_prefix(False, "structural_or_unknown_action")
    before = request_context(previous.get("pricing_request", {}))
    after = request_context({**source["latest_request"], "history_start": source.get("history_start")})
    # Advancing the normal observation/mapping cutoff adds work. Changing any
    # other effective pricing input invalidates the retained-history handoff.
    for field in ("end_exclusive", "asof"):
        old, new = before.pop(field, None), after.pop(field, None)
        if old is None or new is None or new < old:
            return selected_prefix(False, "mapping_or_coverage_regressed")
    if before != after:
        return selected_prefix(False, "effective_pricing_context_changed")
    accepted = selected == previous["source_sha256"] or any(
        change["source_sha256"] == previous["source_sha256"]
        and (change["changed_from"] is None or change["changed_from"] >= previous["processed_through"][:10])
        for change in source.get("history_changes", ())
    )
    return selected_prefix(accepted, "accepted_pricing_ancestry" if accepted else "changed_or_unproven_prefix")


def complete_append(view):
    """Only the new session grid and its accepted seam require validation."""
    summaries = view.previous_assignment_summary
    if not any(view.views.values()):
        return True
    checked = set()
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        last = summaries[adjustment]["last_session"]
        if last is None:
            return False
        key = id(rows), last
        if key in checked:
            continue
        checked.add(key)
        first = (date.fromisoformat(last) + timedelta(days=1)).isoformat()
        expected = expected_market_session_coverage(first, view.source["latest_request"]["end_exclusive"])
        sessions = tuple(timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows)
        if sessions != expected.session_dates:
            return False
    return True


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


def stable_evidence(previous, current, *, append=False):
    """Compare every evidence fact; exclude only acquisition bookkeeping.

    New producer fields participate automatically. This boundary has no map of
    ownership rules, candidates, certificates or likely correlation outcomes.
    """

    def facts(value):
        result = deepcopy(value)
        result.pop("api_request_count", None)
        result.pop("catalogue_observed_at_utc", None)
        # Metadata owns these lookup-invalidation baselines and completes any
        # required acquisition before this handoff. Adding/changing its bookkeeping
        # alone is not new lineage evidence; actual lookup/action facts stay below.
        result.pop("mapping_endpoint_observations", None)
        for lookup in result.get("asset_lookups", ()):
            lookup.pop("source", None)
        if append:
            # The accepted pricing handoff already covers this advancement.
            # Actual actions, disposition and coverage quality still participate;
            # acquisition bookkeeping cannot invalidate accepted assignments.
            query = result.get("query", {})
            query.pop("corporate_actions_start", None)
            query.pop("corporate_actions_end", None)
            local = result.get("symbol_disposition", {}).get("local_market_data")
            if isinstance(local, dict):
                local.pop("maximum_rows", None)
        return result

    return facts(previous) == facts(current)


def requires_lineage(previous, evidence, *, pricing_unchanged, refresh=False, append=False):
    """Unknown or changed inputs select the existing complete lineage pipeline."""
    return (
        refresh
        or not pricing_unchanged
        or previous.keys() != evidence.keys()
        or any(
            prior is None
            or not reusable_decision(prior)
            or not stable_evidence(prior["evidence"], evidence[symbol], append=append)
            for symbol, prior in previous.items()
        )
    )

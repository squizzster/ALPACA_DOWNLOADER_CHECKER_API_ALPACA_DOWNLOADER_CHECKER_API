"""A yes/no boundary over accepted lineage inputs, without identity rules."""

from copy import deepcopy
from datetime import date, timedelta

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.debug_trace import event
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .contract import reusable_decision
from .correction_prefix import correction_from


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


def reusable_prefix(source, previous, *, selected=None, refresh=False, selected_correction=...):
    """Accept pricing's unchanged-prefix handoff without replaying lineage rules.

    Pricing owns retained-row equality and the completed lineage receipt owns its
    assignments. Group topology, ambiguity, an empty history and event complexity
    describe that accepted result; none proves its retained prices changed. Actual
    evidence changes and append effects on unresolved decisions are selected later,
    after their producers have supplied the new facts. No ownership rule executes
    at this boundary.
    """

    def selected_prefix(accepted, reason):
        event("retained_prefix", symbol=source["symbol"], accepted=accepted, reason=reason)
        return accepted

    if refresh or not previous or not reusable_decision(previous):
        return selected_prefix(False, "refresh_or_missing_compatible_receipt")
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
    accepted = (
        (correction_from(source, previous) if selected_correction is ... else selected_correction) is not None
        or selected == previous["source_sha256"]
        or any(
            change["source_sha256"] == previous["source_sha256"]
            and (change["changed_from"] is None or change["changed_from"] >= previous["processed_through"][:10])
            for change in source.get("history_changes", ())
        )
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
            if summaries[adjustment]["rows"] == 0:
                continue  # The producer validates first rows; there is no retained seam.
            return False
        key = id(rows), last
        if key in checked:
            continue
        checked.add(key)
        first = (
            view.previous_coverage_end
            if view.correction_from
            else (date.fromisoformat(last) + timedelta(days=1)).isoformat()
        )
        expected = expected_market_session_coverage(first, view.source["latest_request"]["end_exclusive"])
        sessions = tuple(timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows)
        accepted_sessions = (
            (view.correction_from, *expected.session_dates) if view.correction_from else expected.session_dates
        )
        if sessions != accepted_sessions:
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


def _evidence_facts(value, *, append=False, historical=False):
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
    if historical:
        # Names and current active/tradable presentation describe a fresh asset
        # observation. With the same provider UUID, symbol, class and exchange,
        # they affect new suffix projection but cannot revoke completed row owners.
        def without_descriptions(item):
            if isinstance(item, list):
                return [without_descriptions(value) for value in item]
            if not isinstance(item, dict):
                return item
            projected = {key: without_descriptions(value) for key, value in item.items()}
            if {"id", "symbol", "class", "exchange"} <= projected.keys():
                for field in ("name", "status", "tradable"):
                    projected.pop(field, None)
            return projected

        result = without_descriptions(result)
    return result


def evidence_delta(previous, current, *, append=False):
    """Classify held metadata facts by the narrowest lineage scope they can change."""
    before = _evidence_facts(previous, append=append)
    after = _evidence_facts(current, append=append)
    if before == after:
        return {"scope": "none", "changed_fields": []}
    changed = sorted(key for key in before.keys() | after.keys() if before.get(key) != after.get(key))
    if append and _evidence_facts(previous, append=True, historical=True) == _evidence_facts(
        current, append=True, historical=True
    ):
        return {"scope": "added_sessions", "changed_fields": changed}
    return {"scope": "complete_group", "changed_fields": changed}


def stable_evidence(previous, current, *, append=False):
    """Return whether metadata preserves completed historical assignments."""
    return evidence_delta(previous, current, append=append)["scope"] != "complete_group"


def append_history_invalidation(previous, inputs):
    """Name accepted prior decisions that new rows can actually reconsider.

    Resolved assignments are closed. An unresolved decision is deliberately
    conditional on later facts from its complete dependency group, so new rows in
    that group select the existing complete-group owner until a finer producer
    certificate can bound the unresolved interval.
    """
    added = sorted(symbol for symbol, view in inputs.items() if any(rows for rows in view.views.values()))
    if not added:
        return None
    unresolved = sorted(
        symbol for symbol, prior in previous.items() if prior and prior.get("unresolved_rows", 0)
    )
    incomplete = sorted(
        symbol
        for symbol, prior in previous.items()
        if prior
        and (
            prior.get("row_partition_complete") is not True
            or prior.get("source_rows") != prior.get("correlated_rows", 0) + prior.get("unresolved_rows", 0)
        )
    )
    if not unresolved and not incomplete:
        return None
    return {"added_symbols": added, "unresolved_symbols": unresolved, "incomplete_symbols": incomplete}


def requires_lineage(previous, evidence, *, pricing_unchanged, refresh=False, append=False):
    """Unknown or changed inputs select the existing complete lineage pipeline."""
    if refresh or not pricing_unchanged or previous.keys() != evidence.keys():
        return True
    required = False
    empty_history = append and all(prior and prior.get("source_rows") == 0 for prior in previous.values())
    for symbol, prior in previous.items():
        if prior is None or not reusable_decision(prior):
            required = True
            continue
        delta = evidence_delta(prior["evidence"], evidence[symbol], append=append)
        if empty_history and delta["scope"] == "complete_group":
            delta = {**delta, "scope": "added_sessions", "reason": "no_retained_rows"}
        event("lineage_evidence_delta", symbol=symbol, **delta)
        required |= delta["scope"] == "complete_group"
    return required

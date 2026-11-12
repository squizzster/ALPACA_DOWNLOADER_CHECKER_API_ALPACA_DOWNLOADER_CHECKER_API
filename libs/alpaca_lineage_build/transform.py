"""The existing rule cascade, followed by three compact chronological outputs."""

from bisect import bisect_left, bisect_right
from copy import deepcopy
from dataclasses import dataclass

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.alpaca_pricing_download.storage import boundary_paths
from libs.common.debug_trace import traced

from .assignment_handoff import comparison, produce, selected
from .contract import COMPUTATION_REVISION, VERSION, LineageError, document_digest, versions
from .policy_engine import evaluate_policy


@traced("correlation")
def correlate(
    store,
    inputs,
    evidence,
    *,
    policy,
    selected_group=None,
    unavailable_pricing=None,
    evidence_generations=None,
    pricing_generations=None,
    execution_trace=None,
    evidence_projection=None,
    auxiliary_evidence=None,
    closed_sources=None,
    closed_evidence=None,
    closed_report_loader=None,
):
    """Run the shared compiled policy against one already-loaded complete group."""
    from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND, items

    if policy.implementation_correlation_version < 87 and any(
        fact["facts"]["schema_version"] in (3, 4, 5)
        for facts in (*evidence.values(), *(auxiliary_evidence or {}).values())
        for fact in items(facts, MARKET_KIND)
    ):
        raise LineageError("documented public-security ownership requires correlation policy 87")
    if policy.implementation_correlation_version < 90 and any(
        fact["facts"]["schema_version"] == 5
        for facts in (*evidence.values(), *(auxiliary_evidence or {}).values())
        for fact in items(facts, MARKET_KIND)
    ):
        raise LineageError("documented public holding exchange requires correlation policy 90")
    if policy.implementation_correlation_version < 89 and any(
        fact["facts"]["schema_version"] == 4
        for facts in (*evidence.values(), *(auxiliary_evidence or {}).values())
        for fact in items(facts, MARKET_KIND)
    ):
        raise LineageError("documented public reclassification requires correlation policy 89")
    # Correlation owns NEW identity decisions. Inputs/evidence already crossed
    # their producer boundaries; this adapter must not reopen paths to authenticate
    # them, and readers must consume its published assignments instead of rerunning it.
    reports = evaluate_policy(
        store,
        inputs,
        evidence,
        policy=policy,
        selected_group=selected_group,
        unavailable_pricing=unavailable_pricing,
        evidence_generations=evidence_generations,
        pricing_generations=pricing_generations,
        execution_trace=execution_trace,
        evidence_projection=evidence_projection,
        auxiliary_evidence=auxiliary_evidence,
        closed_sources=closed_sources,
        closed_evidence=closed_evidence,
        closed_report_loader=closed_report_loader,
    )
    if policy.implementation_correlation_version >= 83:
        from libs.alpaca_end_of_day_security_correlation.market_occupancy import project

        reports = project(
            inputs,
            reports,
            evidence,
            auxiliary_evidence,
            allow_adjustment_continuity=policy.implementation_correlation_version >= 84,
            inactive_memberships={
                name: getattr(inputs[name], "accepted_identity_memberships", None) or {}
                for name, report in reports.items()
                if report["source_row_count"] == 0
            },
        )
    if policy.implementation_correlation_version >= 86:
        from libs.alpaca_end_of_day_security_correlation.recapitalizations import project as recapitalizations

        reports = recapitalizations(inputs, reports, evidence)
    if policy.implementation_correlation_version >= 87:
        from libs.alpaca_end_of_day_security_correlation.documented_markets import project as documented

        reports = documented(
            inputs,
            reports,
            evidence,
            allow_containing_membership=policy.implementation_correlation_version >= 88,
            allow_reclassification=policy.implementation_correlation_version >= 89,
            allow_holding_exchange=policy.implementation_correlation_version >= 90,
            refine_accepted_dates=policy.implementation_correlation_version >= 91
            or policy.implementation_correlation_version >= 90
            and any(
                fact["facts"]["schema_version"] == 5
                for report in evidence.values()
                for fact in items(report, MARKET_KIND)
            ),
            allow_report_memberships=policy.implementation_correlation_version >= 91,
        )
    return reports


def retained_decision_report(previous, *, policy):
    """Adapt an accepted decision to publication when there are no new rows."""
    # Reuse has already been selected by the owning pipeline. Empty added rows
    # do not erase retained assignments or justify recomputing historical rules.
    return {
        **{
            key: previous[key]
            for key in (
                "evidence_issues",
                "ownership_interval_rule_order",
                "row_correlation_rule_order",
                "row_partition_complete",
            )
        },
        "correlation_policy": policy.provenance() if policy is not None else previous["correlation_policy"],
        "source_row_count": 0,
        "current_source_ownership": previous.get("current_source_ownership"),
        "sourced_rename_ownership": previous.get("sourced_rename_ownership"),
        "sourced_rename_membership": previous.get("sourced_rename_membership"),
        "public_common_continuation_membership": previous.get("public_common_continuation_membership"),
        "public_common_continuation_ownership": previous.get("public_common_continuation_ownership"),
        "documented_identity_membership": previous.get("documented_identity_membership"),
        "security_relations": previous.get("security_relations", {"version": 1, "relations": [], "issues": []}),
        "correlated": {"row_count": 0, "segments": []},
        "uncorrelated": {"row_count": 0, "segments": []},
    }


# Persist the decision, not a second audit trail of the rule's changing whole-
# history diagnostics. These assignments plus normalized evidence are the boundary.
ASSIGNMENT_FIELDS = {
    "historical_symbol",
    "canonical_security",
    "security_identity",
    "identity_confidence",
    "correlation_method",
    "ticker_ownership_interval",
    "correlation_rule",
    "warnings",
    "reason_codes",
    "candidate_securities",
    "market_data_series_identity",
    "observation_selection",
    "observation_tickers",
    "observation_authority",
    "dated_identity_refinement",
}


@dataclass(frozen=True)
class _RowIndex:
    sessions: tuple[str, ...]
    months: tuple[str, ...]


def _row_indexes(view):
    sessions, indexes = {}, {}
    for adjustment in ADJUSTMENTS:
        dates = []
        for row in view.views[adjustment]:
            if row[0] not in sessions:
                sessions[row[0]] = timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
            dates.append(sessions[row[0]])
        indexes[adjustment] = _RowIndex(tuple(dates), tuple(day[:7] for day in dates))
    return indexes


def boundary(view, report, kind, anchor, indexes):
    year = anchor[:4]
    adjustments = []
    for adjustment in ADJUSTMENTS:
        full_rows = view.views[adjustment]
        index = indexes[adjustment]
        if kind == "completed_years":
            lo, hi = 0, bisect_left(index.months, year + "-01")
        elif kind == "current_year":
            lo, hi = bisect_left(index.months, year + "-01"), bisect_right(index.months, year + "-12")
        elif kind == "current_month":
            lo, hi = bisect_left(index.months, anchor), bisect_right(index.months, anchor)
        else:
            raise KeyError(kind)
        segments = []
        for branch in ("correlated", "uncorrelated"):
            for segment in report[branch]["segments"]:
                if segment["adjustment"] != adjustment:
                    continue
                first_index = max(lo + 1, segment["row_start"])
                last_index = min(hi, segment["row_end"])
                if first_index > last_index:
                    continue
                first, last = full_rows[first_index - 1], full_rows[last_index - 1]
                segments.append(
                    {
                        **{key: deepcopy(value) for key, value in segment.items() if key in ASSIGNMENT_FIELDS},
                        "branch": branch,
                        "row_start": first_index - lo,
                        "row_end": last_index - lo,
                        "row_count": last_index - first_index + 1,
                        "first_timestamp": first[0],
                        "last_timestamp": last[0],
                        "from": index.sessions[first_index - 1],
                        "to": index.sessions[last_index - 1],
                    }
                )
        segments.sort(key=lambda segment: segment["row_start"])
        if sum(segment["row_count"] for segment in segments) != hi - lo:
            raise LineageError("lineage boundary does not exhaust its source rows")
        adjustments.append({"adjustment": adjustment, "rows": hi - lo, "segments": segments})
    return {
        "schema_version": VERSION,
        "boundary": "alpaca_lineage_" + kind,
        "symbol": view.symbol,
        "anchor_month": anchor,
        "adjustments": adjustments,
    }


def _prefix(document, anchor):
    """Ignore the retained active-month overlap when testing the year snapshot."""
    prefix = deepcopy(document)
    for entry in prefix["adjustments"]:
        # Publication segments are split at month boundaries, making overlap
        # replacement an exact segment selection rather than inferred row slicing.
        entry["segments"] = [item for item in entry["segments"] if item["from"][:7] < anchor]
        entry["rows"] = sum(item["row_count"] for item in entry["segments"])
    prefix.pop("anchor_month")
    return prefix


def _split_months(report, indexes):
    result = deepcopy(report)
    for branch in ("correlated", "uncorrelated"):
        split = []
        for segment in report[branch]["segments"]:
            months = indexes[segment["adjustment"]].months
            start = segment["row_start"]
            while start <= segment["row_end"]:
                end = min(bisect_right(months, months[start - 1], lo=start - 1), segment["row_end"])
                split.append({**segment, "row_start": start, "row_end": end})
                start = end + 1
        result[branch]["segments"] = split
    return result


def _continued_documents(view, report, indexes, previous, old):
    """Append new decisions to accepted assignments; regroup only at rollover."""
    anchor = view.source["anchor_month"]
    same_month = previous["anchor_month"] == anchor
    same_year = previous["anchor_month"][:4] == anchor[:4]
    if same_month and not any(view.views.values()):
        return {}
    documents = {}
    for kind in boundary_paths(anchor):
        corrected_kind = (
            (
                "current_month"
                if view.correction_from[:7] == anchor
                else "current_year"
                if view.correction_from[:4] == anchor[:4]
                else "completed_years"
            )
            if view.correction_from
            else None
        )
        if kind != corrected_kind and (
            (kind == "completed_years" and same_year) or (kind == "current_year" and same_month)
        ):
            continue
        document = boundary(view, report, kind, anchor, indexes)
        for entry in document["adjustments"]:
            prefix = [
                segment
                for old_kind, prior in old.items()
                for item in prior["adjustments"]
                if item["adjustment"] == entry["adjustment"]
                for segment in item["segments"]
                if (old_kind != "current_year" or segment["from"][:7] < previous["anchor_month"])
                and (
                    (kind == "completed_years" and segment["from"][:4] < anchor[:4])
                    or (kind == "current_year" and segment["from"][:4] == anchor[:4])
                    or (kind == "current_month" and segment["from"][:7] == anchor)
                )
            ]
            offset, segments = 0, []
            for segment in sorted(prefix, key=lambda s: s["from"]) + entry["segments"]:
                segments.append({**segment, "row_start": offset + 1, "row_end": offset + segment["row_count"]})
                offset += segment["row_count"]
            entry.update(rows=offset, segments=segments)
        documents[kind] = document
    return documents


@traced("lineage_prepare_publication")
def make_publication(view, evidence, report, deps, previous=None, old_boundaries=None, *, resume_closed=False):
    # This producer joins accepted pricing and completed identity decisions into
    # a new handoff. Preserve unchanged closed references; do not require consumers
    # to reconstruct ownership, coverage or numerical selection from raw evidence.
    from .projection import last_observed_symbol_lookup

    anchor = view.source["anchor_month"]
    indexes = _row_indexes(view)
    report = _split_months(report, indexes)
    old_boundaries = old_boundaries or {}
    documents = (
        _continued_documents(view, report, indexes, previous, view.prefix_boundaries or old_boundaries)
        if resume_closed
        else {kind: boundary(view, report, kind, anchor, indexes) for kind in boundary_paths(anchor)}
    )
    comparisons = {}
    for kind, new in list(documents.items()):
        accepted = selected(previous, kind) if previous else None
        if accepted is not None and (
            kind == "completed_years" or kind == "current_year" and previous["anchor_month"] == anchor
        ):
            comparisons[kind] = comparison(new)
            if comparisons[kind] == accepted["comparison"]:
                del documents[kind]
    for kind, old in old_boundaries.items():
        if kind not in documents:
            continue
        new = documents[kind]
        same_anchor = previous["anchor_month"] == anchor
        if kind == "current_year" and same_anchor and _prefix(old, anchor) == _prefix(new, anchor):
            documents[kind] = old
        elif kind == "completed_years" and {k: v for k, v in old.items() if k != "anchor_month"} == {
            k: v for k, v in new.items() if k != "anchor_month"
        }:
            documents[kind] = old
    closed_rows = sum(item["rows"] for item in view.previous_assignment_summary.values()) if resume_closed else 0
    removed_rows = previous["source_rows"] - closed_rows if resume_closed else 0
    assets = {identity["id"] for identity in evidence["identities"] if identity.get("id")}
    memberships = {
        identity["security_id"]: identity
        for identity in (previous or {}).get("identity_memberships", ())
        if resume_closed
    }
    routes = deepcopy((previous or {}).get("observation_routes", {})) if resume_closed else {}
    intervals = deepcopy((previous or {}).get("identity_intervals", [])) if resume_closed else []
    if resume_closed and view.correction_from:
        intervals = [
            {**item, "to": min(item["to"], view.previous_assignment_summary["raw"]["last_session"])}
            for item in intervals
            if item["from"] < view.correction_from
        ]
    for segment in report["correlated"]["segments"]:
        identity = segment["security_identity"]
        memberships[identity["security_id"]] = identity
        rows = view.views[segment["adjustment"]]
        intervals.append(
            {
                "from": timestamp(rows[segment["row_start"] - 1][0]).astimezone(NEW_YORK).date().isoformat(),
                "to": timestamp(rows[segment["row_end"] - 1][0]).astimezone(NEW_YORK).date().isoformat(),
                "security_id": identity["security_id"],
            }
        )
        if segment["adjustment"] == "raw" and segment["historical_symbol"] == view.symbol:
            pid = identity["security_id"]
            if pid not in routes or routes[pid][0] < segment["last_timestamp"]:
                routes[pid] = [segment["last_timestamp"], segment.get("ticker_ownership_interval") or {}]
    for pid, route in report.get("observation_routes", {}).items():
        if pid not in routes or routes[pid][0] <= route[0]:
            routes[pid] = route
    receipt = {
        "schema_version": VERSION,
        "boundary": "alpaca_standalone_lineage",
        "symbol": view.symbol,
        "versions": versions(),
        "computation_revision": COMPUTATION_REVISION,
        "correlation_policy": report["correlation_policy"],
        "source_sha256": view.source_sha256,
        "pricing_boundaries": view.source["boundaries"],
        "anchor_month": anchor,
        "processed_through": view.source["latest_request"]["end_exclusive"],
        "pricing_request": {**view.source["latest_request"], "history_start": view.source.get("history_start")},
        "acquisition_evidence_sha256": view.source.get("acquisition_evidence_sha256"),
        "history_correction_id": view.source.get("history_correction_id"),
        "update_obligation_id": (previous or {}).get("update_obligation_id"),
        "last_observed_symbol_lookup": last_observed_symbol_lookup(previous) if previous else None,
        "dependencies": deps,
        "asset_ids": sorted(assets),
        "identity_memberships": list(memberships.values()),
        "observation_routes": routes,
        "identity_intervals": [
            dict(zip(("from", "to", "security_id"), key))
            for key in sorted({(item["from"], item["to"], item["security_id"]) for item in intervals})
        ],
        "evidence": evidence,
        "current_source_ownership": report.get("current_source_ownership"),
        "sourced_rename_ownership": report.get("sourced_rename_ownership"),
        "sourced_rename_membership": report.get("sourced_rename_membership"),
        "public_common_continuation_membership": report.get("public_common_continuation_membership"),
        "public_common_continuation_ownership": report.get("public_common_continuation_ownership"),
        "documented_identity_membership": report.get("documented_identity_membership"),
        "security_relations": report.get("security_relations", {"version": 1, "relations": [], "issues": []}),
        "membership_only_rebind": None,
        "evidence_issues": report["evidence_issues"],
        "ownership_interval_rule_order": report["ownership_interval_rule_order"],
        "row_correlation_rule_order": report["row_correlation_rule_order"],
        "row_partition_complete": report["row_partition_complete"],
        "source_rows": report["source_row_count"] + closed_rows,
        "correlated_rows": report["correlated"]["row_count"]
        + (previous["correlated_rows"] - removed_rows if resume_closed else 0),
        "unresolved_rows": report["uncorrelated"]["row_count"]
        + (previous["unresolved_rows"] if resume_closed else 0),
        "assignment_summary": {
            item["adjustment"]: {
                "rows": item["rows"],
                "first_timestamp": item["from"],
                "last_timestamp": item["to"],
                "last_session": timestamp(item["to"]).astimezone(NEW_YORK).date().isoformat()
                if item["to"]
                else None,
            }
            for item in view.local["downloads"]
        },
        "boundaries": {
            kind: {
                "manifest": relative,
                "sha256": (
                    previous["boundaries"][kind]["sha256"]
                    if kind not in documents or documents[kind] is old_boundaries.get(kind)
                    else document_digest(documents[kind])
                ),
            }
            for kind, relative in boundary_paths(anchor).items()
        },
    }
    if resume_closed and (
        "numerical_source_equivalence" in previous or "numerical_source_equivalence_version" in previous
    ):
        from .source_equivalence import carry_prefix

        through = [item["last_session"] for item in view.previous_assignment_summary.values()]
        equality = carry_prefix(previous, min(through) if through and all(through) else None)
        if equality is not None:
            receipt["numerical_source_equivalence_version"] = 1
            receipt["numerical_source_equivalence"] = equality
    from libs.alpaca_end_of_day_security_correlation.security_relations import project

    relation = receipt["security_relations"]
    if any(
        "security_id" not in item[role]
        for item in relation["relations"]
        for role in ("predecessor", "successor")
    ):
        receipt["security_relations"] = project(
            relation,
            {
                identifier: identity
                for identity in memberships.values()
                for identifier in identity["member_alpaca_asset_ids"]
            },
        )
    if resume_closed and "identity_memberships" not in previous:
        receipt.pop("identity_memberships")
    if resume_closed and "identity_intervals" not in previous:
        receipt.pop("identity_intervals")
    if not resume_closed or "continuation_handoff_version" in previous:
        from .continuation import HANDOFF_VERSION

        receipt["continuation_handoff_version"] = HANDOFF_VERSION
    if resume_closed and "observation_routes" not in previous and not report["correlated"]["segments"]:
        receipt.pop("observation_routes")
    handoffs = dict((previous or {}).get("assignment_handoffs", {}))
    for kind, document in documents.items():
        generation = receipt["boundaries"][kind]["sha256"]
        if handoffs.get(kind, {}).get("generation") != generation:
            handoffs[kind] = produce(document, generation, compared=comparisons.get(kind))
    if handoffs:
        receipt["assignment_handoffs"] = handoffs
        receipt["assignment_handoff_kinds"] = sorted(handoffs)
    if resume_closed and previous.get("membership_only_rebind") is not None:
        from .membership_transfer import carry_prefix as carry_membership_prefix

        receipt["membership_only_rebind"] = carry_membership_prefix(
            previous, receipt, view.previous_assignment_summary, unchanged_selection=True
        )
        receipt["membership_transfer_handoff_version"] = 1
    return receipt, documents

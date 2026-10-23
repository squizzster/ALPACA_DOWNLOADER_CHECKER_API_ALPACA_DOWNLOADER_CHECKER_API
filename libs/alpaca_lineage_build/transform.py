"""The existing rule cascade, followed by three compact chronological outputs."""

from bisect import bisect_left, bisect_right
from copy import deepcopy
from dataclasses import dataclass

from libs.alpaca_end_of_day_security_correlation.correlation import build_security_correlation_report
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.alpaca_pricing_download.storage import boundary_paths

from .contract import VERSION, LineageError, document_digest, versions
from .pricing import empty_exclusions, rule_loaders


def correlate(store, inputs, evidence):
    previous = {}
    cache = CorrelationCache()
    # Identity rules consume only this selected group's already-loaded rows.
    # Missing complete evidence withholds a certificate; never open more histories
    # or fetch provider bars here to make a downstream rule succeed.
    for symbol, view in inputs.items():
        cache.register_name_change_source(
            symbol,
            view.downloads.values(),
            complete=all(
                descriptor.get("rows", len(view.downloads[descriptor["file"]].rows))
                == len(view.downloads[descriptor["file"]].rows)
                for descriptor in view.local["downloads"]
            ),
        )
    for iteration in range(3):
        sessions, correlation, rows = rule_loaders(inputs, previous, cache)
        reports = {
            symbol: build_security_correlation_report(
                {"symbol": symbol, "local": view.local, "lineage": evidence[symbol]},
                store,
                authenticated_download_reader=view.reader,
                verified_session_exclusions_reader=empty_exclusions,
                authenticated_symbol_sessions=sessions,
                correlation_loader=correlation,
                source_rows_loader=rows,
                cache=cache,
            )
            for symbol, view in sorted(inputs.items())
        }
        if iteration and reports == previous:
            return reports
        previous = reports
    raise LineageError("lineage correlation did not reach its established fixed point")


def retained_decision_report(previous):
    """Adapt an accepted decision to publication when there are no new rows."""
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
        "source_row_count": 0,
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
        if (kind == "completed_years" and same_year) or (kind == "current_year" and same_month):
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


def make_publication(view, evidence, report, deps, previous=None, old_boundaries=None, *, resume_closed=False):
    from .projection import last_observed_symbol_lookup

    anchor = view.source["anchor_month"]
    indexes = _row_indexes(view)
    report = _split_months(report, indexes)
    old_boundaries = old_boundaries or {}
    documents = (
        _continued_documents(view, report, indexes, previous, old_boundaries)
        if resume_closed
        else {kind: boundary(view, report, kind, anchor, indexes) for kind in boundary_paths(anchor)}
    )
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
    closed_rows = previous["source_rows"] if resume_closed else 0
    assets = {identity["id"] for identity in evidence["identities"] if identity.get("id")}
    receipt = {
        "schema_version": VERSION,
        "boundary": "alpaca_standalone_lineage",
        "symbol": view.symbol,
        "versions": versions(),
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
        "evidence": evidence,
        "evidence_issues": report["evidence_issues"],
        "ownership_interval_rule_order": report["ownership_interval_rule_order"],
        "row_correlation_rule_order": report["row_correlation_rule_order"],
        "row_partition_complete": report["row_partition_complete"],
        "source_rows": report["source_row_count"] + closed_rows,
        "correlated_rows": report["correlated"]["row_count"]
        + (previous["correlated_rows"] if resume_closed else 0),
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
    return receipt, documents

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
                assess_episode_partitions=iteration > 0,
                cache=cache,
            )
            for symbol, view in sorted(inputs.items())
        }
        if iteration and reports == previous:
            return reports
        previous = reports
    raise LineageError("lineage correlation did not reach its established fixed point")


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


def make_publication(view, evidence, report, deps, previous=None, old_boundaries=None, *, resume_closed=False):
    anchor = view.source["anchor_month"]
    indexes = _row_indexes(view)
    report = _split_months(report, indexes)
    old_boundaries = old_boundaries or {}
    documents = {
        kind: old_boundaries[kind]
        if resume_closed and kind != "current_month"
        else boundary(view, report, kind, anchor, indexes)
        for kind in boundary_paths(anchor)
    }
    for kind, old in old_boundaries.items():
        new = documents[kind]
        same_anchor = previous["anchor_month"] == anchor
        if kind == "current_year" and same_anchor and _prefix(old, anchor) == _prefix(new, anchor):
            documents[kind] = old
        elif kind == "completed_years" and {k: v for k, v in old.items() if k != "anchor_month"} == {
            k: v for k, v in new.items() if k != "anchor_month"
        }:
            documents[kind] = old
    closed_rows = 0
    if resume_closed:
        for kind in ("completed_years", "current_year"):
            documents[kind] = old_boundaries[kind]
            closed_rows += sum(
                segment["row_count"]
                for entry in documents[kind]["adjustments"]
                for segment in entry["segments"]
                if segment["from"][:7] < anchor
            )
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
        "dependencies": deps,
        "asset_ids": sorted(assets),
        "evidence": evidence,
        "evidence_issues": report["evidence_issues"],
        "ownership_interval_rule_order": report["ownership_interval_rule_order"],
        "row_correlation_rule_order": report["row_correlation_rule_order"],
        "row_partition_complete": report["row_partition_complete"],
        "source_rows": report["source_row_count"] + closed_rows,
        "correlated_rows": report["correlated"]["row_count"] + closed_rows,
        "unresolved_rows": report["uncorrelated"]["row_count"],
        "boundaries": {
            kind: {
                "manifest": relative,
                "sha256": (
                    previous["boundaries"][kind]["sha256"]
                    if documents[kind] is old_boundaries.get(kind)
                    else document_digest(documents[kind])
                ),
            }
            for kind, relative in boundary_paths(anchor).items()
        },
    }
    return receipt, documents

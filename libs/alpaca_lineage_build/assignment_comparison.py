"""Normalize selected assignment outputs for explicit production and research comparisons."""

import hashlib
import json
from bisect import bisect_left, bisect_right

from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import CorrelationPolicyError

_SEMANTIC_FIELDS = (
    "branch",
    "historical_symbol",
    "canonical_security",
    "security_identity",
    "reason_codes",
    "candidate_securities",
    "market_data_series_identity",
)
_EXPLANATION_FIELDS = (
    "identity_confidence",
    "correlation_method",
    "ticker_ownership_interval",
    "warnings",
    "correlation_rule",
)
_OBSERVATION_FIELDS = ("observation_selection", "observation_tickers")
_GROUP_DIAGNOSTIC_FIELDS = (
    "evidence_issues",
    "ownership_interval_rule_order",
    "row_correlation_rule_order",
    "row_partition_complete",
)


def _canonical_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest_mapping(mapping) -> str:
    digest = hashlib.sha256()
    for key in sorted(mapping):
        item = _canonical_bytes({"key": list(key), "value": mapping[key]})
        digest.update(len(item).to_bytes(8, "big"))
        digest.update(item)
    return digest.hexdigest()


def _segment_value(segment, fields):
    return {field: segment.get(field) for field in fields}


def _included(kind: str, day: str, anchor: str) -> bool:
    if kind == "completed_years":
        return day[:4] < anchor[:4]
    if kind == "current_year":
        return day[:4] == anchor[:4] and day[:7] < anchor
    if kind == "current_month":
        return day[:7] == anchor
    raise CorrelationPolicyError(f"unknown assignment boundary {kind!r}")


def _normalize_boundaries(symbol, boundaries, view, anchor, *, row_index=None):
    expected_kinds = {"completed_years", "current_year", "current_month"}
    if set(boundaries) != expected_kinds:
        raise CorrelationPolicyError(f"{symbol}: assignment boundaries are incomplete")
    dimensions = {"semantic": {}, "explanation": {}, "observation": {}}
    if row_index is None:
        expected_keys, rows_by_adjustment, dates_by_rows = set(), {}, {}
        for adjustment in ADJUSTMENTS:
            rows = view.views[adjustment]
            if id(rows) not in dates_by_rows:
                dates_by_rows[id(rows)] = tuple(
                    timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows
                )
            rows_by_adjustment[adjustment] = (rows, dates_by_rows[id(rows)])
            expected_keys.update((symbol, adjustment, row[0]) for row in rows)
        row_index = rows_by_adjustment, expected_keys
    rows_by_adjustment, expected_keys = row_index

    for kind in ("completed_years", "current_year", "current_month"):
        document = boundaries[kind]
        if not isinstance(document, dict) or document.get("symbol") != symbol:
            raise CorrelationPolicyError(f"{symbol}: invalid {kind} assignment boundary")
        adjustments = document.get("adjustments")
        if not isinstance(adjustments, list):
            raise CorrelationPolicyError(f"{symbol}: invalid {kind} adjustments")
        by_name = {
            item.get("adjustment"): item
            for item in adjustments
            if isinstance(item, dict) and item.get("adjustment") in ADJUSTMENTS
        }
        if set(by_name) != set(ADJUSTMENTS) or len(by_name) != len(adjustments):
            raise CorrelationPolicyError(f"{symbol}: {kind} does not contain each adjustment once")
        for adjustment in ADJUSTMENTS:
            item = by_name[adjustment]
            segments = item.get("segments")
            if not isinstance(segments, list):
                raise CorrelationPolicyError(f"{symbol}: invalid {kind}/{adjustment} segments")
            rows, dates = rows_by_adjustment[adjustment]
            for segment in segments:
                if (
                    not isinstance(segment, dict)
                    or segment.get("branch") not in {"correlated", "uncorrelated"}
                    or not isinstance(segment.get("from"), str)
                    or not isinstance(segment.get("to"), str)
                    or segment["from"] > segment["to"]
                ):
                    raise CorrelationPolicyError(f"{symbol}: invalid {kind}/{adjustment} segment")
                lo = bisect_left(dates, segment["from"])
                hi = bisect_right(dates, segment["to"])
                semantic = _segment_value(segment, _SEMANTIC_FIELDS)
                explanation = _segment_value(segment, _EXPLANATION_FIELDS)
                observation = _segment_value(segment, _OBSERVATION_FIELDS)
                for index in range(lo, hi):
                    if not _included(kind, dates[index], anchor):
                        continue
                    key = (symbol, adjustment, rows[index][0])
                    if key in dimensions["semantic"]:
                        raise CorrelationPolicyError(f"{symbol}: overlapping assignment at {key[2]}")
                    dimensions["semantic"][key] = semantic
                    dimensions["explanation"][key] = explanation
                    dimensions["observation"][key] = observation
    actual_keys = set(dimensions["semantic"])
    if actual_keys != expected_keys:
        raise CorrelationPolicyError(
            f"{symbol}: effective assignments do not exhaust pricing rows "
            f"({len(actual_keys)} != {len(expected_keys)})"
        )
    return dimensions


def _group_dimensions(boundaries, inputs, receipts, *, row_indexes=None):
    result = {"semantic": {}, "explanation": {}, "observation": {}}
    row_indexes = {} if row_indexes is None else row_indexes
    for symbol in sorted(inputs):
        view = inputs[symbol]
        if symbol not in row_indexes:
            dates_by_rows, rows_by_adjustment, expected_keys = {}, {}, set()
            for adjustment in ADJUSTMENTS:
                rows = view.views[adjustment]
                if id(rows) not in dates_by_rows:
                    dates_by_rows[id(rows)] = tuple(
                        timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows
                    )
                rows_by_adjustment[adjustment] = rows, dates_by_rows[id(rows)]
                expected_keys.update((symbol, adjustment, row[0]) for row in rows)
            row_indexes[symbol] = rows_by_adjustment, expected_keys
        normalized = _normalize_boundaries(
            symbol,
            boundaries[symbol],
            inputs[symbol],
            receipts[symbol]["anchor_month"],
            row_index=row_indexes[symbol],
        )
        for dimension in result:
            result[dimension].update(normalized[dimension])
    return result


def _difference(baseline, candidate, maximum, *, baseline_sha256=None, candidate_sha256=None):
    changed = []
    count = 0
    for key in sorted(set(baseline) | set(candidate)):
        before = baseline.get(key)
        after = candidate.get(key)
        if before == after:
            continue
        count += 1
        if len(changed) < maximum:
            changed.append({"key": list(key), "baseline": before, "candidate": after})
    return {
        "count": count,
        "baseline_sha256": _digest_mapping(baseline) if baseline_sha256 is None else baseline_sha256,
        "candidate_sha256": _digest_mapping(candidate) if candidate_sha256 is None else candidate_sha256,
        "examples": changed,
    }


def _memberships(semantic):
    memberships = {}
    for value in semantic.values():
        if value.get("branch") != "correlated":
            continue
        identity = value.get("security_identity")
        if not isinstance(identity, dict) or not isinstance(identity.get("security_id"), str):
            raise CorrelationPolicyError("correlated assignment has no security identity")
        member_ids = identity.get("member_alpaca_asset_ids")
        if not isinstance(member_ids, list) or any(not isinstance(item, str) for item in member_ids):
            raise CorrelationPolicyError("correlated assignment has invalid identity membership")
        key = (identity["security_id"], tuple(sorted(member_ids)))
        memberships[key] = {
            "security_id": key[0],
            "member_alpaca_asset_ids": list(key[1]),
        }
    return [memberships[key] for key in sorted(memberships)]


def _row_counts(semantic):
    result = {"total": len(semantic), "correlated": 0, "unresolved": 0, "excluded": 0}
    for value in semantic.values():
        if value.get("branch") == "correlated":
            result["correlated"] += 1
            continue
        reasons = value.get("reason_codes") or []
        bucket = "excluded" if "exchange_test_symbol_excluded" in reasons else "unresolved"
        result[bucket] += 1
    return result


def _group_diagnostics(values):
    return {
        (symbol, field): value.get(field)
        for symbol, value in sorted(values.items())
        for field in _GROUP_DIAGNOSTIC_FIELDS
    }


def _published_counts(receipts):
    return {
        "total": sum(receipt["source_rows"] for receipt in receipts.values()),
        "correlated": sum(receipt["correlated_rows"] for receipt in receipts.values()),
        "uncorrelated": sum(receipt["unresolved_rows"] for receipt in receipts.values()),
    }


def _candidate_counts(reports):
    return {
        "total": sum(report["source_row_count"] for report in reports.values()),
        "correlated": sum(report["correlated"]["row_count"] for report in reports.values()),
        "uncorrelated": sum(report["uncorrelated"]["row_count"] for report in reports.values()),
    }


def _counts_match_rows(reported, rows):
    return reported == {
        "total": rows["total"],
        "correlated": rows["correlated"],
        "uncorrelated": rows["unresolved"] + rows["excluded"],
    }

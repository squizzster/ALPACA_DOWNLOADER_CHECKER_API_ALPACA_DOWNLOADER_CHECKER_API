"""Read one trusted complete group and compare a candidate policy in memory."""

import hashlib
import json
from bisect import bisect_left, bisect_right
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_security_correlation.correlation_version import (
    CORRELATION_VERSION,
)
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    OWNERSHIP_INTERVAL_RULES,
    ownership_interval_rule_order,
)
from libs.alpaca_lineage_build.acquisition import (
    check_unavailable_pricing,
    unavailable_pricing,
)
from libs.alpaca_lineage_build.contract import (
    VERSION as LINEAGE_VERSION,
)
from libs.alpaca_lineage_build.contract import document_digest, reusable_decision, versions
from libs.alpaca_lineage_build.correlation_policy import authorize_production_policy
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.pricing import load_pricing
from libs.alpaca_lineage_build.storage import reader_snapshot, select_inputs
from libs.alpaca_lineage_build.transform import correlate, make_publication
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, symbol_label, timestamp
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path

from .contract import (
    LAB_REPORT_BOUNDARY,
    LAB_SCHEMA_VERSION,
    LAB_VERSION,
    PolicyLabError,
    replayable_lineage_versions,
    validate_change_log,
)
from .fallbacks import CORRECTED_PROFILE
from .policy import CompiledPolicy

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
    raise PolicyLabError(f"unknown assignment boundary {kind!r}")


def _normalize_boundaries(symbol, boundaries, view, anchor):
    expected_kinds = {"completed_years", "current_year", "current_month"}
    if set(boundaries) != expected_kinds:
        raise PolicyLabError(f"{symbol}: assignment boundaries are incomplete")
    dimensions = {"semantic": {}, "explanation": {}, "observation": {}}
    expected_keys = set()
    rows_by_adjustment = {}
    for adjustment in ADJUSTMENTS:
        rows = view.views[adjustment]
        dates = [timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() for row in rows]
        rows_by_adjustment[adjustment] = (rows, dates)
        expected_keys.update((symbol, adjustment, row[0]) for row in rows)

    for kind in ("completed_years", "current_year", "current_month"):
        document = boundaries[kind]
        if not isinstance(document, dict) or document.get("symbol") != symbol:
            raise PolicyLabError(f"{symbol}: invalid {kind} assignment boundary")
        adjustments = document.get("adjustments")
        if not isinstance(adjustments, list):
            raise PolicyLabError(f"{symbol}: invalid {kind} adjustments")
        by_name = {
            item.get("adjustment"): item
            for item in adjustments
            if isinstance(item, dict) and item.get("adjustment") in ADJUSTMENTS
        }
        if set(by_name) != set(ADJUSTMENTS) or len(by_name) != len(adjustments):
            raise PolicyLabError(f"{symbol}: {kind} does not contain each adjustment once")
        for adjustment in ADJUSTMENTS:
            item = by_name[adjustment]
            segments = item.get("segments")
            if not isinstance(segments, list):
                raise PolicyLabError(f"{symbol}: invalid {kind}/{adjustment} segments")
            rows, dates = rows_by_adjustment[adjustment]
            for segment in segments:
                if (
                    not isinstance(segment, dict)
                    or segment.get("branch") not in {"correlated", "uncorrelated"}
                    or not isinstance(segment.get("from"), str)
                    or not isinstance(segment.get("to"), str)
                    or segment["from"] > segment["to"]
                ):
                    raise PolicyLabError(f"{symbol}: invalid {kind}/{adjustment} segment")
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
                        raise PolicyLabError(f"{symbol}: overlapping assignment at {key[2]}")
                    dimensions["semantic"][key] = semantic
                    dimensions["explanation"][key] = explanation
                    dimensions["observation"][key] = observation
    actual_keys = set(dimensions["semantic"])
    if actual_keys != expected_keys:
        raise PolicyLabError(
            f"{symbol}: effective assignments do not exhaust pricing rows "
            f"({len(actual_keys)} != {len(expected_keys)})"
        )
    return dimensions


def _group_dimensions(boundaries, inputs, receipts):
    result = {"semantic": {}, "explanation": {}, "observation": {}}
    for symbol in sorted(inputs):
        normalized = _normalize_boundaries(
            symbol,
            boundaries[symbol],
            inputs[symbol],
            receipts[symbol]["anchor_month"],
        )
        for dimension in result:
            result[dimension].update(normalized[dimension])
    return result


def _difference(baseline, candidate, maximum):
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
        "baseline_sha256": _digest_mapping(baseline),
        "candidate_sha256": _digest_mapping(candidate),
        "examples": changed,
    }


def _memberships(semantic):
    memberships = {}
    for value in semantic.values():
        if value.get("branch") != "correlated":
            continue
        identity = value.get("security_identity")
        if not isinstance(identity, dict) or not isinstance(identity.get("security_id"), str):
            raise PolicyLabError("correlated assignment has no security identity")
        member_ids = identity.get("member_alpaca_asset_ids")
        if not isinstance(member_ids, list) or any(not isinstance(item, str) for item in member_ids):
            raise PolicyLabError("correlated assignment has invalid identity membership")
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


def _exact_legacy_33_59_receipt(receipt: dict[str, Any]) -> bool:
    legacy = {**versions(), "lineage": 33, "correlation": 59}
    legacy.pop("correlation_policy_sha256", None)
    return (
        isinstance(receipt, dict)
        and receipt.get("schema_version") == 33
        and receipt.get("versions") == legacy
        and "correlation_policy" not in receipt
    )


def _migration_rule_order_receipt_state(receipt: dict[str, Any]) -> str | None:
    if _exact_legacy_33_59_receipt(receipt):
        return "policyless_33_59"
    if isinstance(receipt, dict) and receipt.get("schema_version") == 34 and reusable_decision(receipt):
        return "canonical_34_60"
    return None


def _migration_preserved_rule_order_provenance(
    policy: CompiledPolicy,
    receipts: dict[str, dict[str, Any]],
    reports: dict[str, dict[str, Any]],
    fallback_execution: dict[str, Any] | None,
) -> bool:
    """Recognize only inert rule-order provenance preserved by exact promotion."""
    try:
        authorize_production_policy(policy)
    except RuntimeError:
        return False
    if not receipts or set(receipts) != set(reports):
        return False
    states = {_migration_rule_order_receipt_state(receipt) for receipt in receipts.values()}
    if len(states) != 1 or None in states:
        return False
    first_three = ownership_interval_rule_order(rules=OWNERSHIP_INTERVAL_RULES[:3])
    first_four = ownership_interval_rule_order(rules=OWNERSHIP_INTERVAL_RULES)
    if any(receipt.get("ownership_interval_rule_order") != first_three for receipt in receipts.values()) or any(
        report.get("ownership_interval_rule_order") != first_four for report in reports.values()
    ):
        return False
    other_fields = tuple(field for field in _GROUP_DIAGNOSTIC_FIELDS if field != "ownership_interval_rule_order")
    if any(
        any(receipts[symbol].get(field) != reports[symbol].get(field) for field in other_fields)
        for symbol in receipts
    ):
        return False
    if not isinstance(fallback_execution, dict):
        return False
    reconciliation = fallback_execution.get("reconciliation")
    ownership = fallback_execution.get("ownership_fallback")
    return (
        isinstance(reconciliation, dict)
        and reconciliation.get("decision_count") == 0
        and isinstance(ownership, dict)
        and ownership.get("selected_sources") == []
        and ownership.get("applied_intervals") == []
        and fallback_execution.get("second_fixed_point_executed") is False
    )


@contextmanager
def _load_group(store: Path, seed_symbol: str):
    # Explicit comparison owns this selected complete-group replay. The snapshot
    # selects and locks its publications; input generation checks bind the replay
    # to those facts. DO NOT repair pending work, fetch replacement evidence or
    # expand into unrelated groups when an input cannot be consumed.
    with reader_snapshot(store, lambda root: select_inputs(root, seed_symbol)) as snapshot:
        seed_receipt = snapshot["receipts"].get(seed_symbol)
        if not isinstance(seed_receipt, dict):
            raise PolicyLabError(f"no published lineage for {seed_symbol}")
        expected_sources = seed_receipt.get("input_sources")
        if not isinstance(expected_sources, dict) or seed_symbol not in expected_sources:
            raise PolicyLabError("selected lineage receipt has invalid input_sources")
        unavailable = unavailable_pricing(seed_receipt)
        selected_group = set(snapshot["entries"])
        if selected_group != set(expected_sources) | set(unavailable):
            raise PolicyLabError("selected lineage group does not match its trusted receipt")
        check_unavailable_pricing(store, unavailable, snapshot["locked"])

        receipts = {}
        for symbol in sorted(expected_sources):
            receipt = snapshot["receipts"].get(symbol)
            if not isinstance(receipt, dict):
                raise PolicyLabError(f"selected group has no lineage receipt for {symbol}")
            if not replayable_lineage_versions(receipt.get("versions")):
                raise PolicyLabError(f"{symbol}: published lineage version is not reusable")
            if receipt.get("row_partition_complete") is not True:
                raise PolicyLabError(f"{symbol}: published row partition is incomplete")
            if receipt.get("input_sources") != expected_sources:
                raise PolicyLabError("complete-group receipts select different pricing generations")
            if not isinstance(receipt.get("evidence"), dict):
                raise PolicyLabError(f"{symbol}: published receipt has no normalized evidence")
            receipts[symbol] = receipt

        sources = {}
        inputs = {}
        for symbol, expected_generation in sorted(expected_sources.items()):
            if symbol not in snapshot["locked"]:
                raise PolicyLabError(f"{symbol}: selected pricing source is not locked")
            pricing_root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
            if read_update(pricing_root) is not None:
                raise PolicyLabError(f"{symbol}: lineage awaits a pending pricing update")
            source = load_source(pricing_root, symbol)
            if source is None or document_digest(source) != expected_generation:
                raise PolicyLabError(f"{symbol}: selected pricing generation changed")
            sources[symbol] = source
            inputs[symbol] = load_pricing(store, symbol, source=source)

        boundaries = {symbol: snapshot["boundaries"][symbol] for symbol in sorted(expected_sources)}
        # These are report provenance tokens, not instructions for a consumer to
        # rehash upstream files or republish the compared production baseline.
        receipt_generations = {
            symbol: snapshot["entries"][symbol]["sha256"] for symbol in sorted(expected_sources)
        }
        evidence_generations = {
            symbol: document_digest(receipts[symbol]["evidence"]) for symbol in sorted(expected_sources)
        }
        yield {
            "inputs": inputs,
            "receipts": receipts,
            "boundaries": boundaries,
            "receipt_generations": receipt_generations,
            "evidence_generations": evidence_generations,
            "pricing_generations": dict(sorted(expected_sources.items())),
            "unavailable_pricing": sorted(unavailable),
            "selected_group": sorted(selected_group),
        }


def _compare_loaded_group(
    store: Path,
    seed_symbol: str,
    policy: CompiledPolicy,
    selected: dict[str, Any],
    *,
    max_differences: int,
) -> dict[str, Any]:
    """Evaluate one already selected complete group without further storage reads."""
    inputs = selected["inputs"]
    receipts = selected["receipts"]
    evidence_generations = selected.get("evidence_generations")
    if evidence_generations is None:
        evidence_generations = {
            symbol: document_digest(receipt["evidence"]) for symbol, receipt in sorted(receipts.items())
        }
    if policy.policy_role in {
        "historical_reference",
        "published_reference",
        "corrected_candidate",
        "production",
    }:
        allowed_baselines = {(33, 59), (LINEAGE_VERSION, CORRELATION_VERSION)}
        if any(
            (
                receipt["versions"].get("lineage"),
                receipt["versions"].get("correlation"),
            )
            not in allowed_baselines
            for receipt in receipts.values()
        ):
            raise PolicyLabError(
                "policy comparison requires a completed 33/59 or current publication as baseline-only data"
            )
    else:
        if any(
            receipt["versions"].get("correlation") != policy.implementation_correlation_version
            for receipt in receipts.values()
        ):
            raise PolicyLabError("baseline and candidate implementation versions differ")
        if policy.implementation_lineage_version is not None and any(
            receipt["versions"].get("lineage") != policy.implementation_lineage_version
            for receipt in receipts.values()
        ):
            raise PolicyLabError("baseline and candidate lineage versions differ")
    raw_evidence = {symbol: receipt["evidence"] for symbol, receipt in receipts.items()}
    fallback_execution = None
    if policy.schema_version == 4:
        evidence = raw_evidence
        evidence_quality = {
            "fact_producers": [],
            "rules": [],
            "raw_evidence_sha256": {},
            "candidate_evidence_sha256": {},
            "quarantine_count": 0,
            "quarantines": [],
            "trace_truncated": False,
        }
        active_suffix = {
            "fact_producers": [],
            "rules": [],
            "certificate_count": 0,
            "certificates": [],
            "certificate_sha256": None,
            "evaluations": [],
            "trace_truncated": False,
        }
        fallback_execution = {}
        reports = correlate(
            store,
            inputs,
            evidence,
            policy=policy,
            selected_group=selected["selected_group"],
            unavailable_pricing=selected["unavailable_pricing"],
            evidence_generations=evidence_generations,
            pricing_generations=selected["pricing_generations"],
            execution_trace=fallback_execution,
        )
        if not isinstance(fallback_execution.get("reconciliation"), dict) or not isinstance(
            fallback_execution.get("ownership_fallback"), dict
        ):
            raise PolicyLabError("schema-v4 policy evaluation returned an incomplete execution trace")
        quality_trace = fallback_execution["reconciliation"].get("quality_trace")
        if isinstance(quality_trace, dict) and fallback_execution["profile"] == CORRECTED_PROFILE:
            evidence_quality = quality_trace
        certificates = fallback_execution["ownership_fallback"]["certificates"]
        active_suffix = certificates
    else:
        execution = {}
        evidence = {}
        reports = correlate(
            store,
            inputs,
            raw_evidence,
            policy=policy,
            selected_group=selected["selected_group"],
            unavailable_pricing=selected["unavailable_pricing"],
            evidence_generations=evidence_generations,
            pricing_generations=selected["pricing_generations"],
            execution_trace=execution,
            evidence_projection=evidence,
        )
        evidence_quality = execution.get("evidence_quality")
        active_suffix = execution.get("active_suffix")
        if not isinstance(evidence_quality, dict) or not isinstance(active_suffix, dict):
            raise PolicyLabError("policy evaluation returned an incomplete execution trace")
    reports, mapping_corrections = select_observations(inputs, reports)
    candidate_boundaries = {}
    for symbol, view in inputs.items():
        _, candidate_boundaries[symbol] = make_publication(
            view,
            evidence[symbol],
            reports[symbol],
            receipts[symbol]["dependencies"],
        )

    baseline = _group_dimensions(selected["boundaries"], inputs, receipts)
    candidate = _group_dimensions(candidate_boundaries, inputs, receipts)
    differences = {
        name: _difference(baseline[name], candidate[name], max_differences)
        for name in ("semantic", "observation", "explanation")
    }
    differences["group_diagnostics"] = _difference(
        _group_diagnostics(receipts),
        _group_diagnostics(reports),
        max_differences,
    )
    baseline_memberships = _memberships(baseline["semantic"])
    candidate_memberships = _memberships(candidate["semantic"])
    baseline_counts = _row_counts(baseline["semantic"])
    candidate_counts = _row_counts(candidate["semantic"])
    baseline_reported_counts = _published_counts(receipts)
    candidate_reported_counts = _candidate_counts(reports)
    memberships_match = baseline_memberships == candidate_memberships
    counts_match = baseline_counts == candidate_counts
    baseline_counts_valid = _counts_match_rows(baseline_reported_counts, baseline_counts)
    candidate_counts_valid = _counts_match_rows(candidate_reported_counts, candidate_counts)
    reports_complete = all(report.get("row_partition_complete") is True for report in reports.values())
    decision_parity = (
        all(differences[name]["count"] == 0 for name in ("semantic", "observation", "explanation"))
        and memberships_match
        and counts_match
        and baseline_counts_valid
        and candidate_counts_valid
        and reports_complete
        and not mapping_corrections
    )
    diagnostic_snapshot_match = differences["group_diagnostics"]["count"] == 0
    exact_snapshot_parity = decision_parity and diagnostic_snapshot_match
    migrated_rule_order = not diagnostic_snapshot_match and _migration_preserved_rule_order_provenance(
        policy, receipts, reports, fallback_execution
    )
    migration_classification = "migration_preserved_rule_order_provenance" if migrated_rule_order else None
    migration_compatible = decision_parity and (diagnostic_snapshot_match or migrated_rule_order)
    return {
        "schema_version": LAB_SCHEMA_VERSION,
        "lab_version": LAB_VERSION,
        "boundary": LAB_REPORT_BOUNDARY,
        "mode": "read_only_one_complete_group",
        "seed_symbol": seed_symbol,
        "group": selected["selected_group"],
        "candidate_policy": policy.as_report_object(),
        "evidence_quality": evidence_quality,
        "active_suffix": active_suffix,
        "fallback_execution": fallback_execution,
        "baseline": {
            "versions": {symbol: receipt["versions"] for symbol, receipt in sorted(receipts.items())},
            "receipt_generations": selected["receipt_generations"],
            "evidence_generations": evidence_generations,
            "pricing_generations": selected["pricing_generations"],
            "unavailable_pricing": selected["unavailable_pricing"],
        },
        "comparison": {
            # `parity` remains the strict historical result used by the CLI.
            "parity": exact_snapshot_parity,
            "decision_parity": decision_parity,
            "diagnostic_snapshot_match": diagnostic_snapshot_match,
            "exact_snapshot_parity": exact_snapshot_parity,
            "migration_compatible": migration_compatible,
            "migration_classification": migration_classification,
            "differences": differences,
            "memberships": {
                "match": memberships_match,
                "baseline": baseline_memberships,
                "candidate": candidate_memberships,
            },
            "row_counts": {
                "match": counts_match,
                "baseline": baseline_counts,
                "candidate": candidate_counts,
                "baseline_reported": baseline_reported_counts,
                "candidate_reported": candidate_reported_counts,
                "baseline_invariant": baseline_counts_valid,
                "candidate_invariant": candidate_counts_valid,
            },
            "candidate_partition_complete": reports_complete,
            "mapping_corrections": mapping_corrections,
        },
    }


def compare(
    store: str | Path,
    seed_symbol: str,
    policy: CompiledPolicy,
    *,
    max_differences: int = 20,
) -> dict[str, Any]:
    """Compare one YAML policy with one completed published group; write nothing."""
    if type(max_differences) is not int or not 0 <= max_differences <= 1000:
        raise PolicyLabError("max_differences must be an integer between 0 and 1000")
    if not isinstance(policy, CompiledPolicy):
        raise PolicyLabError("policy must be a compiled policy")
    validate_change_log()
    store = Path(store).absolute()
    seed_symbol = symbol_label(seed_symbol)

    with _load_group(store, seed_symbol) as selected:
        return _compare_loaded_group(
            store,
            seed_symbol,
            policy,
            selected,
            max_differences=max_differences,
        )

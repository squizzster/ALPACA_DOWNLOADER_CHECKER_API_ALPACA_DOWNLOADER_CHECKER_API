"""One-pass, read-only audit of the current policy across published lineage."""

import hashlib
import json
import resource
import signal
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from libs.alpaca_lineage_build.acquisition import (
    check_unavailable_pricing,
    unavailable_pricing,
)
from libs.alpaca_lineage_build.contract import MAX_CLOSURE, document_digest
from libs.alpaca_lineage_build.occupancy import (
    may_have_action_anchored_current_suffix,
    may_have_duplicate_foreign_occupancy,
)
from libs.alpaca_lineage_build.pricing import load_pricing
from libs.alpaca_lineage_build.storage import (
    assert_index_current,
    check_store,
    path_at,
    publication_lock,
    read_boundaries,
    read_index,
    read_receipt,
)
from libs.alpaca_pricing_download.storage import (
    load_source,
    read_update,
    regular_path,
    ticker_lock,
)

from .active_suffix import _evaluate_symbol
from .comparison import _compare_loaded_group
from .contract import (
    LAB_AUDIT_BOUNDARY,
    LAB_SCHEMA_VERSION,
    LAB_VERSION,
    PolicyLabError,
    replayable_lineage_versions,
    validate_change_log,
)
from .evidence_quality import _candidate_pairs, _facts_for_pair
from .fallbacks import (
    CORRECTED_PROFILE,
    PUBLISHED_PROFILE,
)
from .policy import CompiledPolicy

MAX_AUDIT_SECONDS = 3600
MAX_GROUP_TRACE_ITEMS = 10
_QUALITY_DEFERRED_FACTS = frozenset(("quality.projection_closure_complete",))
_SUFFIX_DEFERRED_FACTS = frozenset(
    (
        "suffix.adjustment_set_complete",
        "suffix.adjustment_sessions_consistent",
        "suffix.suffix_session_coverage_complete",
        "suffix.row_candidates_exact",
    )
)


class _AuditTimedOut(RuntimeError):
    pass


@dataclass(frozen=True)
class _PreparedGroup:
    symbols: tuple[str, ...]
    receipts: dict[str, dict[str, Any]]
    unavailable: dict[str, dict[str, Any]]
    receipt_generations: dict[str, str]
    evidence_generations: dict[str, str]
    pricing_generations: dict[str, str]
    preliminary_rules: tuple[str, ...] = ()


class _Deadline:
    def __init__(self, seconds: int | float):
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise PolicyLabError("audit timeout_seconds must be a number")
        if not 0 < seconds <= MAX_AUDIT_SECONDS:
            raise PolicyLabError(
                f"audit timeout_seconds must be greater than zero and at most {MAX_AUDIT_SECONDS}"
            )
        self.seconds = seconds
        self.started = time.monotonic()
        self.phase = "initialization"

    def check(self, phase: str) -> None:
        self.phase = phase
        if time.monotonic() - self.started >= self.seconds:
            raise _AuditTimedOut(phase)


@contextmanager
def _hard_timeout(deadline: _Deadline):
    try:
        previous = signal.getitimer(signal.ITIMER_REAL)
        previous_handler = signal.getsignal(signal.SIGALRM)
    except (AttributeError, ValueError) as error:
        raise PolicyLabError("all-lineage audit requires a main-thread POSIX alarm") from error
    if previous != (0.0, 0.0):
        raise PolicyLabError("all-lineage audit cannot replace an active process alarm")

    def expired(_signum, _frame):
        raise _AuditTimedOut(deadline.phase)

    try:
        signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, deadline.seconds)
    except ValueError as error:
        raise PolicyLabError("all-lineage audit requires a main-thread POSIX alarm") from error
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _selector_groups(index: dict[str, Any]) -> tuple[list[tuple[str, ...]], int]:
    # The completed index owns membership. An explicit audit may inspect its
    # declared scope; missing/invalid projections fail here, never trigger a
    # directory census or receipt crawl to reconstruct membership implicitly.
    entries = index.get("symbols")
    state = index.get("projection_state")
    if not isinstance(entries, dict) or not entries:
        raise PolicyLabError("lineage audit requires a nonempty selector")
    if not isinstance(state, dict) or state.get("complete") is not True:
        raise PolicyLabError("lineage audit requires a completed selector projection")
    groups = set()
    for symbol, entry in sorted(entries.items()):
        members = entry.get("group_members") if isinstance(entry, dict) else None
        if (
            not isinstance(members, list)
            or not 1 <= len(members) <= MAX_CLOSURE
            or any(not isinstance(item, str) or not item for item in members)
            or members != sorted(set(members))
            or symbol not in members
        ):
            raise PolicyLabError(f"{symbol}: selector has an invalid group projection")
        groups.add(tuple(members))
    incomplete = 0
    for group in groups:
        available = [symbol for symbol in group if symbol in entries]
        if len(available) != len(group):
            incomplete += 1
        if any(tuple(entries[symbol].get("group_members", ())) != group for symbol in available):
            raise PolicyLabError(f"{group[0]}: selector group members disagree")
    return sorted(groups), incomplete


def _missing_suffix_hint(entry: dict[str, Any]) -> bool:
    projection = entry.get("projection")
    return (
        not isinstance(projection, dict)
        or type(projection.get("unresolved_rows")) is not int
        or not isinstance(entry.get("asset_ids"), list)
        or any(not isinstance(item, str) or not item for item in entry.get("asset_ids", ()))
    )


def _index_prefilter(
    index: dict[str, Any],
    groups: list[tuple[str, ...]],
    policy: CompiledPolicy,
) -> list[tuple[str, ...]]:
    entries = index["symbols"]
    selected = []
    for group in groups:
        available = [symbol for symbol in group if symbol in entries]
        quality_candidate = bool(policy.evidence_rules) and len(available) > 1
        suffix_candidate = False
        if policy.schema_version == 4 and policy.fallback_profile == PUBLISHED_PROFILE:
            quality_candidate = len(available) > 1
            suffix_candidate = any(
                _missing_suffix_hint(entries[symbol]) or len(set(entries[symbol]["asset_ids"])) >= 2
                for symbol in available
            )
        elif policy.schema_version == 4 and policy.fallback_profile == CORRECTED_PROFILE:
            if len(group) == len(available) == 1:
                entry = entries[group[0]]
                suffix_candidate = _missing_suffix_hint(entry) or (
                    len(entry["asset_ids"]) == 2 and len(set(entry["asset_ids"])) == 2
                )
        elif policy.active_suffix_rules and len(group) == len(available) == 1:
            entry = entries[group[0]]
            suffix_candidate = _missing_suffix_hint(entry) or (
                entry["projection"]["unresolved_rows"] > 0
                and len(entry["asset_ids"]) == 2
                and len(set(entry["asset_ids"])) == 2
            )
        if quality_candidate or suffix_candidate:
            selected.append(group)
    return selected


def _validate_receipt_group(
    group: tuple[str, ...],
    receipts: dict[str, dict[str, Any]],
    index: dict[str, Any],
    policy: CompiledPolicy,
) -> _PreparedGroup:
    available = tuple(symbol for symbol in group if symbol in index["symbols"])
    if not receipts or set(receipts) != set(available):
        raise PolicyLabError(f"{group[0]}: selected receipt group is incomplete")
    first = receipts[available[0]]
    expected_sources = first.get("input_sources")
    absent = unavailable_pricing(first)
    if (
        not isinstance(expected_sources, dict)
        or not isinstance(absent, dict)
        or set(expected_sources) != set(available)
        or set(expected_sources).intersection(absent)
        or set(expected_sources) | set(absent) != set(group)
    ):
        raise PolicyLabError(f"{group[0]}: receipt closure differs from its selector group")
    if any(not isinstance(generation, str) or len(generation) != 64 for generation in expected_sources.values()):
        raise PolicyLabError(f"{group[0]}: receipt has invalid pricing generations")
    for symbol in available:
        receipt = receipts[symbol]
        if (
            receipt.get("input_sources") != expected_sources
            or unavailable_pricing(receipt) != absent
            or not replayable_lineage_versions(receipt.get("versions"))
            or receipt.get("row_partition_complete") is not True
            or not isinstance(receipt.get("evidence"), dict)
            or not isinstance(receipt.get("dependencies"), list)
            or not isinstance(receipt.get("boundaries"), dict)
        ):
            raise PolicyLabError(f"{symbol}: receipt is not a complete reusable audit input")
        if index["symbols"][symbol].get("source_sha256") != expected_sources[symbol]:
            raise PolicyLabError(f"{symbol}: selector and receipt pricing generations differ")
    return _PreparedGroup(
        symbols=available,
        receipts=receipts,
        unavailable=absent,
        receipt_generations={symbol: index["symbols"][symbol]["sha256"] for symbol in available},
        evidence_generations={symbol: document_digest(receipts[symbol]["evidence"]) for symbol in available},
        pricing_generations=dict(sorted(expected_sources.items())),
    )


def _quality_prefilter(group: _PreparedGroup, policy: CompiledPolicy) -> bool:
    if policy.schema_version == 4:
        if len(group.symbols) < 2:
            return False
        evidence = {symbol: receipt["evidence"] for symbol, receipt in group.receipts.items()}
        if policy.fallback_profile == PUBLISHED_PROFILE:
            return any(
                may_have_duplicate_foreign_occupancy(evidence[symbol], symbol) for symbol in group.symbols
            )
        if policy.fallback_profile != CORRECTED_PROFILE:
            raise PolicyLabError("schema-v4 audit received an unknown fallback profile")
        local_reports = {symbol: None for symbol in evidence}
        rule = policy.evidence_rules[0]
        for pair in _candidate_pairs(evidence):
            facts, _ = _facts_for_pair(
                pair,
                evidence,
                group.evidence_generations,
                local_reports,
            )
            if all(
                name in _QUALITY_DEFERRED_FACTS or facts.get(name) is expected
                for name, expected in rule.conditions
            ):
                return True
        return False
    if not policy.evidence_rules or len(group.symbols) < 2:
        return False
    evidence = {symbol: receipt["evidence"] for symbol, receipt in group.receipts.items()}
    local_reports = {symbol: None for symbol in evidence}
    for pair in _candidate_pairs(evidence):
        facts, _ = _facts_for_pair(
            pair,
            evidence,
            group.evidence_generations,
            local_reports,
        )
        for rule in policy.evidence_rules:
            if all(
                name in _QUALITY_DEFERRED_FACTS or facts.get(name) is expected
                for name, expected in rule.conditions
            ):
                return True
    return False


def _suffix_prefilter(
    group: _PreparedGroup,
    index: dict[str, Any],
    policy: CompiledPolicy,
) -> bool:
    if policy.schema_version == 4:
        if policy.fallback_profile == PUBLISHED_PROFILE:
            candidates = [
                may_have_action_anchored_current_suffix(group.receipts[symbol]["evidence"])
                for symbol in group.symbols
            ]
            return any(candidates)
        if policy.fallback_profile != CORRECTED_PROFILE:
            raise PolicyLabError("schema-v4 audit received an unknown fallback profile")
        if len(group.symbols) != 1 or group.unavailable:
            return False
    if not policy.active_suffix_rules or len(group.symbols) != 1 or group.unavailable:
        if policy.schema_version != 4:
            return False
    symbol = group.symbols[0]
    projection = index["symbols"][symbol].get("projection")
    request = projection.get("pricing_request") if isinstance(projection, dict) else None
    if not isinstance(request, dict):
        return True
    view = SimpleNamespace(source={"latest_request": request}, views={}, local={})
    report = {
        "row_partition_complete": False,
        "correlated": {"segments": []},
        "uncorrelated": {"segments": []},
    }
    evidence_quality = {
        "raw_evidence_sha256": {symbol: "prefilter"},
        "candidate_evidence_sha256": {symbol: "prefilter"},
        "quarantine_count": 0,
    }
    _, facts, _, _ = _evaluate_symbol(
        symbol,
        group.receipts[symbol]["evidence"],
        view,
        report,
        evidence_sha256=group.evidence_generations[symbol],
        pricing_generation=group.pricing_generations[symbol],
        evidence_quality=evidence_quality,
        single_receipt_group=True,
    )
    rules = policy.active_suffix_rules
    return any(
        all(name in _SUFFIX_DEFERRED_FACTS or facts.get(name) is expected for name, expected in rule.conditions)
        for rule in rules
    )


def _preflight_file_descriptors(lock_count: int) -> None:
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft_limit != resource.RLIM_INFINITY and lock_count + 64 > soft_limit:
        raise PolicyLabError(
            f"audit requires {lock_count} pricing locks but the file-descriptor limit is {soft_limit}"
        )


@contextmanager
def _shared_pricing_locks(store: Path, symbols: list[str]):
    ordered = sorted(set(symbols))
    _preflight_file_descriptors(len(ordered))
    with ExitStack() as stack:
        for symbol in ordered:
            root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
            try:
                stack.enter_context(ticker_lock(root, exclusive=False))
            except FileNotFoundError as error:
                raise PolicyLabError(f"{symbol}: selected pricing lock is missing") from error
        yield set(ordered)


def _compact_group_result(report: dict[str, Any], preliminary_rules: tuple[str, ...]) -> dict[str, Any]:
    comparison = report["comparison"]
    quality = report["evidence_quality"]
    suffix = report["active_suffix"]
    differences = {
        name: {
            "count": value["count"],
            "baseline_sha256": value["baseline_sha256"],
            "candidate_sha256": value["candidate_sha256"],
        }
        for name, value in sorted(comparison["differences"].items())
    }
    quarantines = [
        {
            "rule_id": item["rule_id"],
            "subject": item["subject"],
            "reference": item["reference"],
            "projection_removals": item["projection_removals"],
        }
        for item in quality["quarantines"][:MAX_GROUP_TRACE_ITEMS]
    ]
    certificates = suffix["certificates"][:MAX_GROUP_TRACE_ITEMS]
    fallback = report.get("fallback_execution")
    compact_fallback = None
    if isinstance(fallback, dict):
        reconciliation = fallback["reconciliation"]
        ownership = fallback["ownership_fallback"]
        compact_fallback = {
            "profile": fallback["profile"],
            "producer_dependency": fallback["producer_dependency"],
            "frozen_plan_sha256": fallback["frozen_plan_sha256"],
            "phase_one": fallback["phase_one"],
            "reconciliation": {
                "producer": reconciliation["producer"],
                "decision_count": reconciliation["decision_count"],
                "decisions": reconciliation["decisions"][:MAX_GROUP_TRACE_ITEMS],
                "rate_evaluations": reconciliation["rate_evaluations"][:MAX_GROUP_TRACE_ITEMS],
            },
            "ownership_fallback": {
                "producer": ownership["producer"],
                "selected_sources": ownership["selected_sources"],
                "legacy_reconciliation_enabled_sources": ownership["legacy_reconciliation_enabled_sources"],
                "applied_interval_count": len(ownership["applied_intervals"]),
                "applied_intervals": ownership["applied_intervals"][:MAX_GROUP_TRACE_ITEMS],
            },
            "second_fixed_point_executed": fallback["second_fixed_point_executed"],
            "phase_two": fallback["phase_two"],
            "phase_invariants": fallback["phase_invariants"],
        }
    return {
        "group": report["group"],
        "preliminary_rules": list(preliminary_rules),
        "parity": comparison["parity"],
        "decision_parity": comparison["decision_parity"],
        "diagnostic_snapshot_match": comparison["diagnostic_snapshot_match"],
        "exact_snapshot_parity": comparison["exact_snapshot_parity"],
        "migration_compatible": comparison["migration_compatible"],
        "migration_classification": comparison["migration_classification"],
        "evidence_quality": {
            "quarantine_count": quality["quarantine_count"],
            "trace_truncated": quality["trace_truncated"] or quality["quarantine_count"] > len(quarantines),
            "quarantines": quarantines,
        },
        "active_suffix": {
            "certificate_count": suffix["certificate_count"],
            "certificate_sha256": suffix["certificate_sha256"],
            "trace_truncated": suffix["trace_truncated"] or suffix["certificate_count"] > len(certificates),
            "certificates": certificates,
        },
        "fallback_execution": compact_fallback,
        "differences": differences,
        "memberships": comparison["memberships"],
        "row_counts": comparison["row_counts"],
        "candidate_partition_complete": comparison["candidate_partition_complete"],
        "mapping_corrections": comparison["mapping_corrections"],
    }


def _timeout_report(
    policy: CompiledPolicy,
    timeout_seconds: int | float,
    progress: dict[str, Any],
    phase: str,
) -> dict[str, Any]:
    return {
        "schema_version": LAB_SCHEMA_VERSION,
        "lab_version": LAB_VERSION,
        "boundary": LAB_AUDIT_BOUNDARY,
        "mode": "read_only_all_lineage_policy_audit",
        "status": "timed_out",
        "complete": False,
        "audit_conclusion": None,
        "timeout_seconds": timeout_seconds,
        "phase": phase,
        "candidate_policy": {
            "policy_id": policy.policy_id,
            "policy_version": policy.policy_version,
            "policy_role": policy.policy_role,
            "execution_profile": policy.execution_profile,
            "implementation_correlation_version": policy.implementation_correlation_version,
            "implementation_lineage_version": policy.implementation_lineage_version,
            "file_sha256": policy.file_sha256,
            "semantic_sha256": policy.semantic_sha256,
        },
        "selector": progress.get("selector"),
        "scope": {
            "index_candidate_group_count": progress.get("index_candidate_group_count", 0),
            "selected_receipt_count": progress.get("selected_receipt_count", 0),
            "preliminary_group_count": progress.get("preliminary_group_count", 0),
            "evaluated_group_count": progress.get("evaluated_group_count", 0),
        },
    }


def _completed_report(
    policy: CompiledPolicy,
    timeout_seconds: int | float,
    index: dict[str, Any],
    selector_groups: list[tuple[str, ...]],
    incomplete_group_count: int,
    index_candidate_groups: list[tuple[str, ...]],
    selected_receipt_count: int,
    retained: list[_PreparedGroup],
    lock_symbols: list[str],
    group_results: list[dict[str, Any]],
) -> dict[str, Any]:
    changed = [item for item in group_results if not item["parity"]]
    decision_mismatches = [item for item in group_results if not item["decision_parity"]]
    diagnostic_mismatches = [item for item in group_results if not item["diagnostic_snapshot_match"]]
    migrated_compatible = [
        item
        for item in group_results
        if item["migration_classification"] == "migration_preserved_rule_order_provenance"
        and item["migration_compatible"]
    ]
    migration_incompatible = [item for item in group_results if not item["migration_compatible"]]
    dimension_differences = {
        name: {
            "total": sum(item["differences"][name]["count"] for item in group_results),
            "group_count": sum(item["differences"][name]["count"] > 0 for item in group_results),
        }
        for name in ("semantic", "observation", "explanation", "group_diagnostics")
    }
    membership_mismatches = sum(not item["memberships"]["match"] for item in group_results)
    row_count_mismatches = sum(not item["row_counts"]["match"] for item in group_results)
    invariant_failures = sum(
        not item["row_counts"]["baseline_invariant"] or not item["row_counts"]["candidate_invariant"]
        for item in group_results
    )
    partition_failures = sum(not item["candidate_partition_complete"] for item in group_results)
    mapping_failures = sum(bool(item["mapping_corrections"]) for item in group_results)
    quarantine_count = sum(item["evidence_quality"]["quarantine_count"] for item in group_results)
    certificate_count = sum(item["active_suffix"]["certificate_count"] for item in group_results)
    reconciliation_count = sum(
        (item.get("fallback_execution") or {}).get("reconciliation", {}).get("decision_count", 0)
        for item in group_results
    )
    fallback_interval_count = sum(
        (item.get("fallback_execution") or {}).get("ownership_fallback", {}).get("applied_interval_count", 0)
        for item in group_results
    )
    aggregate_rows = {
        side: {
            field: sum(item["row_counts"][side][field] for item in group_results)
            for field in ("total", "correlated", "unresolved", "excluded")
        }
        for side in ("baseline", "candidate")
    }
    receipt_generations = {
        symbol: generation for group in retained for symbol, generation in group.receipt_generations.items()
    }
    pricing_generations = {
        symbol: generation for group in retained for symbol, generation in group.pricing_generations.items()
    }
    if not changed:
        conclusion = "exact_parity"
    elif not migration_incompatible:
        conclusion = "decision_parity_with_migrated_diagnostics"
    else:
        conclusion = "differences"
    report = {
        "schema_version": LAB_SCHEMA_VERSION,
        "lab_version": LAB_VERSION,
        "boundary": LAB_AUDIT_BOUNDARY,
        "mode": "read_only_all_lineage_policy_audit",
        "status": "complete",
        "complete": True,
        "audit_conclusion": conclusion,
        "timeout_seconds": timeout_seconds,
        "selector": index["projection_state"],
        "candidate_policy": {
            "policy_id": policy.policy_id,
            "policy_version": policy.policy_version,
            "policy_role": policy.policy_role,
            "execution_profile": policy.execution_profile,
            "implementation_correlation_version": policy.implementation_correlation_version,
            "implementation_lineage_version": policy.implementation_lineage_version,
            "file_sha256": policy.file_sha256,
            "semantic_sha256": policy.semantic_sha256,
        },
        "scope": {
            "indexed_symbol_count": len(index["symbols"]),
            "selector_group_count": len(selector_groups),
            "incomplete_group_count": incomplete_group_count,
            "index_candidate_group_count": len(index_candidate_groups),
            "selected_receipt_count": selected_receipt_count,
            "preliminary_group_count": len(retained),
            "selected_pricing_lock_count": len(lock_symbols),
            "evaluated_group_count": len(group_results),
            "prefiltered_no_change_group_count": len(selector_groups) - len(retained),
            "receipt_generations_sha256": _canonical_digest(receipt_generations),
            "pricing_generations_sha256": _canonical_digest(pricing_generations),
        },
        "summary": {
            "parity": not changed,
            "decision_parity": not decision_mismatches,
            "diagnostic_snapshot_match": not diagnostic_mismatches,
            "exact_snapshot_parity": not changed,
            "migration_compatible": not migration_incompatible,
            "changed_group_count": len(changed),
            "decision_mismatch_group_count": len(decision_mismatches),
            "diagnostic_mismatch_group_count": len(diagnostic_mismatches),
            "migrated_compatible_group_count": len(migrated_compatible),
            "migration_incompatible_group_count": len(migration_incompatible),
            "membership_mismatch_group_count": membership_mismatches,
            "row_count_mismatch_group_count": row_count_mismatches,
            "invariant_failure_group_count": invariant_failures,
            "partition_failure_group_count": partition_failures,
            "mapping_correction_group_count": mapping_failures,
            "dimension_differences": dimension_differences,
            "quarantine_count": quarantine_count,
            "reconciliation_decision_count": reconciliation_count,
            "ownership_fallback_interval_count": fallback_interval_count,
            "active_suffix_certificate_count": certificate_count,
            "row_counts": aggregate_rows,
            "groups_sha256": _canonical_digest(group_results),
        },
        "groups": group_results,
    }
    semantic = {key: value for key, value in report.items() if key != "timeout_seconds"}
    report["semantic_sha256"] = _canonical_digest(semantic)
    return report


def _run_audit(
    store: Path,
    policy: CompiledPolicy,
    timeout_seconds: int | float,
    deadline: _Deadline,
    progress: dict[str, Any],
) -> dict[str, Any]:
    deadline.check("selector read")
    # This broad read is authorized by the explicit audit command, not a daily
    # update fallback. Prefilter from the selector, then load only selected groups;
    # the audit reports pending publication and never recovers or writes it.
    check_store(store)
    root = regular_path(store, "end_of_day/lineage")
    with publication_lock(root, wait=False):
        if path_at(root, ".pending.json").exists():
            raise PolicyLabError("lineage publication is incomplete; resume lineage first")
        index = read_index(root)
        selector_groups, incomplete_group_count = _selector_groups(index)
        index_candidate_groups = _index_prefilter(index, selector_groups, policy)
    progress["selector"] = index["projection_state"]
    progress["index_candidate_group_count"] = len(index_candidate_groups)

    retained: list[_PreparedGroup] = []
    selected_receipt_count = 0
    for group_symbols in index_candidate_groups:
        deadline.check("receipt loading and evidence prefilter")
        available = [symbol for symbol in group_symbols if symbol in index["symbols"]]
        receipts = {symbol: read_receipt(root, symbol, index) for symbol in available}
        if any(receipt is None for receipt in receipts.values()):
            raise PolicyLabError(f"{group_symbols[0]}: selected receipt disappeared")
        selected_receipt_count += len(receipts)
        progress["selected_receipt_count"] = selected_receipt_count
        prepared = _validate_receipt_group(group_symbols, receipts, index, policy)
        preliminary = []
        if _quality_prefilter(prepared, policy):
            preliminary.append(
                "unresolved_complete_group_reconciliation" if policy.schema_version == 4 else "evidence_quality"
            )
        if _suffix_prefilter(prepared, index, policy):
            preliminary.append(
                "optional_fourth_ownership_fallback"
                if policy.schema_version == 4
                else "active_suffix_assignment"
            )
        if preliminary:
            retained.append(
                _PreparedGroup(
                    symbols=prepared.symbols,
                    receipts=prepared.receipts,
                    unavailable=prepared.unavailable,
                    receipt_generations=prepared.receipt_generations,
                    evidence_generations=prepared.evidence_generations,
                    pricing_generations=prepared.pricing_generations,
                    preliminary_rules=tuple(preliminary),
                )
            )
            progress["preliminary_group_count"] = len(retained)

    deadline.check("pricing lock planning")
    pricing_generations: dict[str, str] = {}
    unavailable_symbols = set()
    for group in retained:
        unavailable_symbols.update(group.unavailable)
        for symbol, generation in group.pricing_generations.items():
            previous = pricing_generations.setdefault(symbol, generation)
            if previous != generation:
                raise PolicyLabError(f"{symbol}: groups select different pricing generations")
    lock_symbols = sorted(set(pricing_generations) | unavailable_symbols)
    sources: dict[str, dict[str, Any]] = {}
    inputs: dict[str, Any] = {}
    boundaries: dict[str, dict[str, Any]] = {}
    group_results = []

    deadline.check("pricing lock acquisition")
    with _shared_pricing_locks(store, lock_symbols) as locked:
        deadline.check("selector token recheck")
        with publication_lock(root, wait=False):
            # Locks were acquired after selection: compare the compact revision
            # once, rather than authenticating every upstream publication again.
            assert_index_current(root, index)

        for symbol in lock_symbols:
            deadline.check("pricing source loading")
            pricing_root = regular_path(
                store,
                f"end_of_day/pricing/{symbol}",
                published=True,
            )
            if regular_path(pricing_root, ".pending.json", published=True).exists():
                raise PolicyLabError(f"{symbol}: pricing publication is incomplete")
            if read_update(pricing_root) is not None:
                raise PolicyLabError(f"{symbol}: lineage awaits a pending pricing update")
            source = load_source(pricing_root, symbol)
            if source is not None:
                sources[symbol] = source
        check_unavailable_pricing(
            store,
            unavailable_symbols,
            locked,
            sources=sources,
        )
        for symbol, generation in sorted(pricing_generations.items()):
            deadline.check("pricing payload loading")
            source = sources.get(symbol)
            if source is None:
                raise PolicyLabError(f"{symbol}: selected pricing source is missing")
            view = load_pricing(store, symbol, source=source)
            if view.source_sha256 != generation:
                raise PolicyLabError(f"{symbol}: selected pricing generation changed")
            inputs[symbol] = view

        for group in retained:
            deadline.check("lineage boundary loading")
            for symbol, receipt in group.receipts.items():
                if symbol in boundaries:
                    raise PolicyLabError(f"{symbol}: audit selected a receipt more than once")
                boundaries[symbol] = read_boundaries(root, receipt)

        for group in retained:
            deadline.check("group evaluation")
            selected = {
                "inputs": {symbol: inputs[symbol] for symbol in group.symbols},
                "receipts": group.receipts,
                "boundaries": {symbol: boundaries[symbol] for symbol in group.symbols},
                "receipt_generations": group.receipt_generations,
                "evidence_generations": group.evidence_generations,
                "pricing_generations": group.pricing_generations,
                "unavailable_pricing": sorted(group.unavailable),
                "selected_group": sorted(set(group.symbols) | set(group.unavailable)),
            }
            report = _compare_loaded_group(
                store,
                group.symbols[0],
                policy,
                selected,
                max_differences=0,
            )
            group_results.append(_compact_group_result(report, group.preliminary_rules))
            progress["evaluated_group_count"] = len(group_results)
            deadline.check("group evaluation")

        deadline.check("final selector token recheck")
        with publication_lock(root, wait=False):
            assert_index_current(root, index)

    deadline.check("report completion")
    return _completed_report(
        policy,
        timeout_seconds,
        index,
        selector_groups,
        incomplete_group_count,
        index_candidate_groups,
        selected_receipt_count,
        retained,
        lock_symbols,
        group_results,
    )


def audit(
    store: str | Path,
    policy: CompiledPolicy,
    *,
    timeout_seconds: int | float = MAX_AUDIT_SECONDS,
) -> dict[str, Any]:
    """Evaluate the current schema-v4 policy once across the full lineage selector."""
    if not isinstance(policy, CompiledPolicy):
        raise PolicyLabError("policy must be a compiled policy")
    if policy.schema_version != 4 or policy.fallback_profile is None:
        raise PolicyLabError("all-lineage audit requires a current schema-v4 fallback policy")
    validate_change_log()
    deadline = _Deadline(timeout_seconds)
    progress: dict[str, Any] = {
        "index_candidate_group_count": 0,
        "selected_receipt_count": 0,
        "preliminary_group_count": 0,
        "evaluated_group_count": 0,
    }
    try:
        with _hard_timeout(deadline):
            return _run_audit(
                Path(store).absolute(),
                policy,
                timeout_seconds,
                deadline,
                progress,
            )
    except _AuditTimedOut as error:
        phase = str(error) or deadline.phase
        return _timeout_report(policy, timeout_seconds, progress, phase)

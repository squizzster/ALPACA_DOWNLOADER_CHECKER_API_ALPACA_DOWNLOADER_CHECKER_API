"""Shared two-phase correlation policy evaluator and whitelisted profiles."""

import hashlib
import json
from dataclasses import replace
from typing import Any

from libs.alpaca_end_of_day_security_correlation.correlation import (
    build_security_correlation_report,
)
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import OwnershipIntervalRule
from libs.alpaca_end_of_day_security_correlation.policy_rules import (
    CertifiedActiveSuffix,
    certified_active_suffix_ownership_rule,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_lineage_build.occupancy import (
    may_have_action_anchored_current_suffix,
    reconcile_duplicate_foreign_occupancy,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

from .contract import CorrelationPolicyError, LineageError, document_digest
from .policy_active_suffix import (
    FACT_TYPES as ACTIVE_SUFFIX_FACT_TYPES,
)
from .policy_active_suffix import (
    ActiveSuffixCertificate,
    ActiveSuffixRule,
    prepare_active_suffix_policy,
    validate_active_suffix_effect,
    with_active_suffix_rule,
)
from .policy_evidence_quality import (
    EFFECT as EVIDENCE_EFFECT,
)
from .policy_evidence_quality import (
    FACT_TYPES as EVIDENCE_FACT_TYPES,
)
from .policy_evidence_quality import (
    EvidenceQualityRule,
    _candidate_pairs,
    _facts_for_pair,
    _rate_equivalent,
    apply_evidence_quality_policy,
)
from .pricing import empty_exclusions, rule_loaders

PUBLISHED_PROFILE = "published_v59"
CORRECTED_PROFILE = "corrected_v59"
PUBLISHED_RECONCILIATION = "reconcile_duplicate_foreign_occupancy_v1"
CORRECTED_RECONCILIATION = "reconcile_duplicate_foreign_occupancy_v2"
PUBLISHED_SUFFIX = "action_anchored_current_ticker_occupancy_v1"
CORRECTED_SUFFIX = "action_anchored_current_ticker_occupancy_v2"
LEGACY_DEPENDENCY = "legacy_frozen_reconciliation_projection_may_enable_suffix"
INDEPENDENT_DEPENDENCY = "independent_raw_evidence_projection_unchanged"
FOURTH_RULE_NAME = "action_anchored_current_ticker_occupancy"

_NEGATIVE_SUFFIX_FACTS = frozenset(
    (
        "suffix.unresolved_relevant_occupancy_present",
        "suffix.competing_occupancy_on_or_after_anchor",
        "suffix.structural_candidate_link_present",
        "suffix.uuid_candidate_edge_present",
    )
)
_NEGATIVE_RECONCILIATION_FACTS = frozenset(
    (
        "quality.endpoint_lookup_unresolved",
        "quality.selected_structural_link_present",
    )
)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _correlate_until_stable(
    store,
    inputs,
    evidence,
    cache,
    previous,
    *,
    policy,
    suffix_sources=frozenset(),
    occupancy_reconciliations=None,
    phase_trace=None,
):
    """Run only the rule implementations selected by the compiled policy."""
    occupancy_reconciliations = occupancy_reconciliations or {}
    for pass_number in range(1, policy.max_passes + 1):
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
                ownership_interval_rules=policy.ownership_rules,
                row_correlation_rules=policy.row_rules,
                occupancy_reconciliations=occupancy_reconciliations.get(symbol, ()),
                include_action_anchored_current_suffix=symbol in suffix_sources,
            )
            for symbol, view in sorted(inputs.items())
        }
        stable = reports == previous
        if phase_trace is not None:
            phase_trace.append(
                {
                    "pass": pass_number,
                    "reports_sha256": document_digest(reports),
                    "stable": stable,
                }
            )
        if stable:
            return reports
        previous = reports
    raise LineageError("lineage correlation did not reach its policy fixed point")


def _register_sources(cache: CorrelationCache, inputs: dict[str, Any]) -> None:
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


def _unchanged_evidence_report(
    evidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    digests = {symbol: _digest(document) for symbol, document in sorted(evidence.items())}
    return {
        "fact_producers": [],
        "rules": [],
        "raw_evidence_sha256": digests,
        "candidate_evidence_sha256": dict(digests),
        "quarantine_count": 0,
        "quarantines": [],
        "trace_truncated": False,
    }


def _action_by_id(document: dict[str, Any], action_id: Any) -> dict[str, Any] | None:
    if not isinstance(action_id, str) or not action_id:
        return None
    matches = [
        item
        for item in document.get("corporate_actions", ())
        if isinstance(item, dict) and item.get("id") == action_id
    ]
    return matches[0] if len(matches) == 1 else None


def _corrected_reconciliation_rule() -> EvidenceQualityRule:
    return EvidenceQualityRule(
        rule_id="RECONCILE_PROVED_DUPLICATE_FOREIGN_OCCUPANCY",
        description="Reconcile only an exact, complete and structurally unexplained own-label duplicate.",
        scope="cross_receipt_action_pair",
        conditions=tuple((name, name not in _NEGATIVE_RECONCILIATION_FACTS) for name in EVIDENCE_FACT_TYPES),
        effect=tuple(EVIDENCE_EFFECT.items()),
    )


def _corrected_reconciliations(
    inputs: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    baseline: dict[str, dict[str, Any]],
    evidence_generations: dict[str, str],
    local_reports: dict[str, dict[str, Any]],
    quality_rule: EvidenceQualityRule | None = None,
) -> tuple[
    dict[str, tuple[dict[str, Any], ...]],
    list[dict[str, Any]],
    dict[str, Any],
]:
    """Retain published v59 gates and additionally require bounded rate equality."""
    published = reconcile_duplicate_foreign_occupancy(inputs, evidence, baseline)
    quality_rule = _corrected_reconciliation_rule() if quality_rule is None else quality_rule
    _, quality_report = apply_evidence_quality_policy(
        evidence,
        (quality_rule,),
        evidence_generations=evidence_generations,
        local_reports=local_reports,
    )
    proved = set()
    for pair in _candidate_pairs(evidence):
        facts, _ = _facts_for_pair(pair, evidence, evidence_generations, local_reports)
        if quality_rule.matches(facts):
            proved.add(
                (
                    pair.subject.receipt_symbol,
                    pair.subject.action["id"],
                    pair.reference.receipt_symbol,
                    pair.reference.action["id"],
                )
            )
    accepted: dict[str, list[dict[str, Any]]] = {}
    evaluations = []
    for symbol, decisions in sorted(published.items()):
        for decision in decisions:
            owner = decision.get("authoritative_symbol")
            subject_action = _action_by_id(evidence[symbol], decision.get("action_id"))
            owner_document = evidence.get(owner) if isinstance(owner, str) else None
            owner_action = (
                _action_by_id(owner_document, decision.get("authoritative_action_id"))
                if isinstance(owner_document, dict)
                else None
            )
            equivalent = (
                _rate_equivalent(subject_action.get("rate"), owner_action.get("rate"))
                if subject_action is not None and owner_action is not None
                else None
            )
            full_quality_match = (
                symbol,
                decision.get("action_id"),
                owner,
                decision.get("authoritative_action_id"),
            ) in proved
            expected_rate_equivalence = dict(quality_rule.conditions)["quality.rate_precision_equivalent"]
            rate_gate_matches = equivalent is expected_rate_equivalence
            evaluations.append(
                {
                    "subject_symbol": symbol,
                    "subject_evidence_sha256": evidence_generations.get(symbol),
                    "subject_action_id": decision.get("action_id"),
                    "authoritative_symbol": owner,
                    "authoritative_evidence_sha256": evidence_generations.get(owner),
                    "authoritative_action_id": decision.get("authoritative_action_id"),
                    "rate_precision_equivalent": equivalent,
                    "full_quality_match": full_quality_match,
                    "accepted": rate_gate_matches and full_quality_match,
                }
            )
            if rate_gate_matches and full_quality_match:
                accepted.setdefault(symbol, []).append(decision)
    return (
        {
            symbol: tuple(sorted(values, key=lambda item: (item["effective_date"], item["action_id"])))
            for symbol, values in accepted.items()
        },
        evaluations,
        quality_report,
    )


def _corrected_suffix_rule() -> ActiveSuffixRule:
    return ActiveSuffixRule(
        rule_id="ASSIGN_BOUNDED_ACTIVE_UUID_SUFFIX",
        description=(
            "Assign only the generation-bound observed suffix proved by complete, uncontested occupancy."
        ),
        scope="single_symbol_observed_suffix",
        conditions=tuple((name, name not in _NEGATIVE_SUFFIX_FACTS) for name in ACTIVE_SUFFIX_FACT_TYPES),
        effect=(
            ("type", "assign_observed_suffix"),
            ("identity", "exact_active_asset"),
            ("start", "positive_evidence_anchor"),
            ("membership", "single_asset"),
        ),
    )


def _certificate_ownership_rule(
    certificates: tuple[ActiveSuffixCertificate, ...],
) -> OwnershipIntervalRule:
    if len({item.symbol for item in certificates}) != len(certificates):
        raise CorrelationPolicyError("corrected suffix certificates contain duplicate symbols")
    return certified_active_suffix_ownership_rule(
        tuple(
            CertifiedActiveSuffix(
                symbol=item.symbol,
                asset_id=item.asset_id,
                cusip=item.cusip,
                start_date_inclusive=item.anchor_date,
                end_date_exclusive=item.end_exclusive,
                supporting_action_ids=item.anchor_action_ids,
                session_dates=item.suffix_sessions,
            )
            for item in certificates
        ),
        rule_name=FOURTH_RULE_NAME,
    )


def _applied_fourth_rule_intervals(reports: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    applied = []
    for symbol, report in sorted(reports.items()):
        for interval in report.get("ticker_ownership_intervals", ()):
            rules = interval.get("ownership_interval_rules") if isinstance(interval, dict) else None
            if not isinstance(rules, list) or not any(
                isinstance(rule, dict) and rule.get("name") == FOURTH_RULE_NAME for rule in rules
            ):
                continue
            applied.append({"symbol": symbol, **interval})
    return applied


_POSITION_FIELDS = frozenset(
    (
        "adjustment",
        "row_start",
        "row_end",
        "row_count",
        "first_timestamp",
        "last_timestamp",
        "from",
        "to",
    )
)


def _report_rows(report: dict[str, Any]) -> dict[tuple[str, int], tuple[str, dict[str, Any]]]:
    rows = {}
    for branch in ("correlated", "uncorrelated"):
        for segment in report[branch]["segments"]:
            value = {key: item for key, item in segment.items() if key not in _POSITION_FIELDS}
            adjustment = segment["adjustment"]
            for index in range(segment["row_start"], segment["row_end"] + 1):
                key = (adjustment, index)
                if key in rows:
                    raise CorrelationPolicyError("correlation phase output contains overlapping rows")
                rows[key] = (branch, value)
    if len(rows) != report["source_row_count"]:
        raise CorrelationPolicyError("correlation phase output does not exhaust source rows")
    return rows


def _validate_phase_monotonicity(
    baseline: dict[str, dict[str, Any]],
    final: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    def semantics(value):
        return value

    if set(baseline) != set(final):
        raise CorrelationPolicyError("second fixed point changed the selected source set")
    completed = []
    preserved_correlated = 0
    newly_resolved = 0
    for symbol in sorted(baseline):
        before = baseline[symbol]
        after = final[symbol]
        if before["uncorrelated"]["row_count"] == 0:
            if before != after:
                raise CorrelationPolicyError("second fixed point changed a completed baseline report")
            completed.append(symbol)
            continue
        before_rows = _report_rows(before)
        after_rows = _report_rows(after)
        if set(before_rows) != set(after_rows):
            raise CorrelationPolicyError("second fixed point changed the baseline row set")
        for key, before_value in before_rows.items():
            after_value = after_rows[key]
            if before_value[0] == "correlated":
                if semantics(after_value) != semantics(before_value):
                    raise CorrelationPolicyError("second fixed point changed an established correlated row")
                preserved_correlated += 1
            elif after_value[0] == "uncorrelated":
                # A second pass may add incidental diagnostics to a row that
                # remains unresolved. It still cannot turn that row into any
                # branch other than unresolved or a proved correlation.
                continue
            elif after_value[0] == "correlated":
                newly_resolved += 1
            else:
                raise CorrelationPolicyError("second fixed point produced an unknown row branch")
    return {
        "completed_sources_unchanged": completed,
        "preserved_correlated_row_count": preserved_correlated,
        "newly_resolved_row_count": newly_resolved,
        "only_baseline_unresolved_rows_resolved": True,
    }


def _validate_corrected_certificate_effect(
    baseline: dict[str, dict[str, Any]],
    reports: dict[str, dict[str, Any]],
    inputs: dict[str, Any],
    certificates: tuple[ActiveSuffixCertificate, ...],
) -> None:
    for certificate in certificates:
        report = reports[certificate.symbol]
        expected = {item.isoformat() for item in certificate.suffix_sessions}
        assigned = {adjustment: set() for adjustment in ADJUSTMENTS}
        before_rows = _report_rows(baseline[certificate.symbol])
        after_rows = _report_rows(report)
        for (adjustment, row_number), after_value in sorted(after_rows.items()):
            before_value = before_rows[(adjustment, row_number)]
            if before_value[0] == "correlated" or after_value[0] != "correlated":
                continue
            if adjustment not in assigned:
                raise CorrelationPolicyError("corrected ownership fallback produced an unknown adjustment")
            rows = inputs[certificate.symbol].views[adjustment]
            session_date = timestamp(rows[row_number - 1][0]).astimezone(NEW_YORK).date()
            if (
                session_date < certificate.anchor_date
                or session_date >= certificate.end_exclusive
                or session_date.isoformat() not in expected
            ):
                raise CorrelationPolicyError(
                    "corrected ownership fallback exceeded its exact certified session set"
                )
            value = after_value[1]
            asset_id = (value.get("canonical_security") or {}).get("alpaca_asset_id")
            identity = value.get("security_identity")
            if asset_id != certificate.asset_id:
                raise CorrelationPolicyError("corrected ownership fallback assigned another asset")
            if (
                not isinstance(identity, dict)
                or identity.get("member_alpaca_asset_ids") != [certificate.asset_id]
                or identity.get("basis") != "single_alpaca_asset_id"
            ):
                raise CorrelationPolicyError(
                    "corrected ownership fallback exceeded its single-asset certificate"
                )
            assigned[adjustment].add(session_date.isoformat())
        if any(sessions != expected for sessions in assigned.values()):
            raise CorrelationPolicyError("corrected ownership fallback exceeded its exact certified session set")
        matching_intervals = [
            item
            for item in _applied_fourth_rule_intervals({certificate.symbol: report})
            if item.get("historical_symbol") == certificate.symbol
            and item.get("start_date_inclusive") == certificate.anchor_date.isoformat()
            and item.get("end_date_exclusive") == certificate.end_exclusive.isoformat()
        ]
        if len(matching_intervals) != 1:
            raise CorrelationPolicyError("corrected ownership fallback did not preserve its certified interval")


def correlate_schema_four(
    store,
    inputs: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    policy,
    *,
    selected_group: list[str],
    unavailable_pricing: list[str],
    evidence_generations: dict[str, str],
    pricing_generations: dict[str, str],
    include_trace: bool = True,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Execute the schema-v4 frozen-baseline/two-phase contract in memory."""
    cache = CorrelationCache(
        ownership_interval_rules=policy.ownership_rules,
        row_correlation_rules=policy.row_rules,
    )
    _register_sources(cache, inputs)
    phase_one_passes = [] if include_trace else None
    baseline = _correlate_until_stable(
        store,
        inputs,
        evidence,
        cache,
        {},
        policy=policy,
        phase_trace=phase_one_passes,
    )
    unresolved = {symbol for symbol, report in baseline.items() if report["uncorrelated"]["row_count"] > 0}
    raw_suffix_sources = frozenset(
        symbol for symbol in unresolved if may_have_action_anchored_current_suffix(evidence[symbol])
    )
    rate_evaluations: list[dict[str, Any]] = []
    quality_report = _unchanged_evidence_report(evidence) if include_trace else {}
    certificates: tuple[ActiveSuffixCertificate, ...] = ()
    certificate_trace: dict[str, Any] = {
        "fact_producers": [],
        "rules": [],
        "certificate_count": 0,
        "certificates": [],
        "certificate_sha256": None,
        "evaluations": [],
        "trace_truncated": False,
    }
    runtime_policy = policy

    if policy.fallback_profile == PUBLISHED_PROFILE:
        reconciliations = reconcile_duplicate_foreign_occupancy(inputs, evidence, baseline)
        suffix_sources = raw_suffix_sources | reconciliations.keys()
        legacy_enabled = sorted(set(reconciliations) - set(raw_suffix_sources))
    elif policy.fallback_profile == CORRECTED_PROFILE:
        reconciliations, rate_evaluations, quality_report = _corrected_reconciliations(
            inputs,
            evidence,
            baseline,
            evidence_generations,
            {symbol: getattr(view, "local", {}) for symbol, view in inputs.items()},
            policy.evidence_rules[0],
        )
        certificates, certificate_trace = prepare_active_suffix_policy(
            evidence,
            inputs,
            baseline,
            policy.active_suffix_rules,
            selected_group=selected_group,
            unavailable_pricing=unavailable_pricing,
            evidence_generations=evidence_generations,
            pricing_generations=pricing_generations,
            evidence_quality=quality_report,
        )
        suffix_sources = frozenset(item.symbol for item in certificates)
        legacy_enabled = []
        if certificates:
            corrected_fourth = _certificate_ownership_rule(certificates)
            runtime_policy = replace(
                policy,
                ownership_rules=(*policy.established_ownership_rules, corrected_fourth),
            )
            cache = CorrelationCache(
                ownership_interval_rules=runtime_policy.ownership_rules,
                row_correlation_rules=runtime_policy.row_rules,
            )
            _register_sources(cache, inputs)
    else:
        raise CorrelationPolicyError(f"unknown schema-v4 fallback profile {policy.fallback_profile!r}")

    phase_two_passes = [] if include_trace else None
    if not reconciliations and not suffix_sources:
        reports = baseline
    else:
        reports = _correlate_until_stable(
            store,
            inputs,
            evidence,
            cache,
            baseline,
            suffix_sources=frozenset(suffix_sources),
            occupancy_reconciliations=reconciliations,
            policy=runtime_policy,
            phase_trace=phase_two_passes,
        )
    phase_invariants = _validate_phase_monotonicity(baseline, reports)
    if policy.fallback_profile == CORRECTED_PROFILE and certificates:
        _validate_corrected_certificate_effect(baseline, reports, inputs, certificates)

    if not include_trace:
        return reports, {}

    reconciliation_items = [
        {"symbol": symbol, **decision}
        for symbol, decisions in sorted(reconciliations.items())
        for decision in decisions
    ]
    baseline_completed = {
        symbol: report for symbol, report in sorted(baseline.items()) if report["uncorrelated"]["row_count"] == 0
    }
    baseline_correlated = {
        f"{symbol}:{adjustment}:{index}": value
        for symbol, report in sorted(baseline.items())
        for (adjustment, index), value in sorted(_report_rows(report).items())
        if value[0] == "correlated"
    }
    plan_object = {
        "phase_one_sha256": _digest(baseline),
        "reconciliations": reconciliation_items,
        "suffix_sources": sorted(suffix_sources),
        "certificates": certificate_trace["certificates"],
        "producer_dependency": policy.producer_dependency,
    }
    trace = {
        "profile": policy.fallback_profile,
        "baseline_frozen": True,
        "baseline_sha256": _digest(baseline),
        "phase_one": {
            "pass_count": len(phase_one_passes),
            "passes": phase_one_passes,
            "result_sha256": _digest(baseline),
            "row_counts": {
                symbol: {
                    "total": report["source_row_count"],
                    "correlated": report["correlated"]["row_count"],
                    "unresolved": report["uncorrelated"]["row_count"],
                }
                for symbol, report in sorted(baseline.items())
            },
            "protected_completed_sources_sha256": _digest(baseline_completed),
            "protected_correlated_rows_sha256": _digest(baseline_correlated),
        },
        "producer_evaluation": "once_from_frozen_baseline",
        "producer_dependency": policy.producer_dependency,
        "frozen_plan_sha256": _digest(plan_object),
        "reconciliation": {
            "producer": policy.reconciliation_producer,
            "decision_count": len(reconciliation_items),
            "decisions": reconciliation_items,
            "rate_evaluations": rate_evaluations,
            "quality_trace": quality_report,
        },
        "ownership_fallback": {
            "producer": policy.ownership_fallback_producer,
            "raw_candidate_sources": sorted(raw_suffix_sources),
            "selected_sources": sorted(suffix_sources),
            "enabled_rule_by_source": {symbol: FOURTH_RULE_NAME for symbol in sorted(suffix_sources)},
            "legacy_reconciliation_enabled_sources": legacy_enabled,
            "certificates": certificate_trace,
            "applied_intervals": _applied_fourth_rule_intervals(reports),
        },
        "second_fixed_point_executed": bool(reconciliations or suffix_sources),
        "phase_two": {
            "seed_sha256": _digest(baseline),
            "pass_count": len(phase_two_passes),
            "passes": phase_two_passes,
            "result_sha256": _digest(reports),
        },
        "phase_invariants": phase_invariants,
    }
    return reports, trace


def evaluate_policy(
    store,
    inputs: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    *,
    policy,
    selected_group: list[str] | None = None,
    unavailable_pricing: list[str] | None = None,
    evidence_generations: dict[str, str] | None = None,
    pricing_generations: dict[str, str] | None = None,
    execution_trace: dict[str, Any] | None = None,
    evidence_projection: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Execute one compiled policy; there is no implicit Python-rule fallback."""
    # Mandatory phase/certificate checks establish this NEW decision at its owner.
    # They are not a reader-side audit of completed decisions. Detailed traces and
    # trace digests are opt-in; DO NOT add them to ordinary reuse for reassurance.
    if policy is None or not callable(getattr(policy, "provenance", None)):
        raise CorrelationPolicyError("correlation requires an explicit compiled policy")
    if set(inputs) != set(evidence):
        raise CorrelationPolicyError("correlation policy inputs and evidence must cover the same sources")

    if policy.schema_version < 4:
        candidate_evidence, evidence_quality = apply_evidence_quality_policy(
            evidence,
            policy.evidence_rules,
            evidence_generations=evidence_generations or {},
            local_reports={symbol: getattr(view, "local", {}) for symbol, view in inputs.items()},
        )
        cache = CorrelationCache(
            ownership_interval_rules=policy.ownership_rules,
            row_correlation_rules=policy.row_rules,
        )
        _register_sources(cache, inputs)
        phase = [] if execution_trace is not None else None
        reports = _correlate_until_stable(
            store,
            inputs,
            candidate_evidence,
            cache,
            {},
            policy=policy,
            phase_trace=phase,
        )
        certificates, active_suffix = prepare_active_suffix_policy(
            candidate_evidence,
            inputs,
            reports,
            policy.active_suffix_rules,
            selected_group=selected_group or sorted(inputs),
            unavailable_pricing=unavailable_pricing or [],
            evidence_generations=evidence_generations or {},
            pricing_generations=pricing_generations or {},
            evidence_quality=evidence_quality,
        )
        if certificates:
            candidate_policy = replace(
                policy,
                row_rules=with_active_suffix_rule(policy.row_rules, certificates),
            )
            cache = CorrelationCache(
                ownership_interval_rules=candidate_policy.ownership_rules,
                row_correlation_rules=candidate_policy.row_rules,
            )
            _register_sources(cache, inputs)
            reports = _correlate_until_stable(
                store,
                inputs,
                candidate_evidence,
                cache,
                {},
                policy=candidate_policy,
            )
            validate_active_suffix_effect(reports, certificates)
        trace = (
            {
                "profile": policy.execution_profile,
                "single_fixed_point": True,
                "evidence_quality": evidence_quality,
                "active_suffix": active_suffix,
                "phase_one": {
                    "pass_count": len(phase),
                    "passes": phase,
                    "result_sha256": _digest(reports),
                },
            }
            if execution_trace is not None
            else {}
        )
        projected = candidate_evidence
    else:
        reports, trace = correlate_schema_four(
            store,
            inputs,
            evidence,
            policy,
            selected_group=selected_group or sorted(inputs),
            unavailable_pricing=unavailable_pricing or [],
            evidence_generations=evidence_generations or {},
            pricing_generations=pricing_generations or {},
            include_trace=execution_trace is not None,
        )
        projected = evidence

    provenance = policy.provenance()
    reports = {
        symbol: {**report, "correlation_policy": provenance} for symbol, report in sorted(reports.items())
    }
    if execution_trace is not None:
        execution_trace.clear()
        execution_trace.update(trace)
    if evidence_projection is not None:
        evidence_projection.clear()
        evidence_projection.update(projected)
    return reports

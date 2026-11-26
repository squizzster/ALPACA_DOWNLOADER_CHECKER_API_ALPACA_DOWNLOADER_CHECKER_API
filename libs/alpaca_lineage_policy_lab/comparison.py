"""Read one trusted complete group and compare a candidate policy in memory."""

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
from libs.alpaca_lineage_build.assignment_comparison import (
    _GROUP_DIAGNOSTIC_FIELDS,
    _candidate_counts,
    _counts_match_rows,
    _difference,
    _digest_mapping,
    _group_diagnostics,
    _group_dimensions,
    _memberships,
    _published_counts,
    _row_counts,
)
from libs.alpaca_lineage_build.assignment_comparison import (
    _normalize_boundaries as _normalize_boundaries,
)
from libs.alpaca_lineage_build.contract import (
    VERSION as LINEAGE_VERSION,
)
from libs.alpaca_lineage_build.contract import document_digest, reusable_decision, versions
from libs.alpaca_lineage_build.correlation_policy import authorize_production_policy
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.pricing import load_pricing
from libs.alpaca_lineage_build.source_work import retain_split_suffix
from libs.alpaca_lineage_build.storage import reader_snapshot, select_inputs
from libs.alpaca_lineage_build.transform import correlate, make_publication
from libs.alpaca_pricing_download.contract import symbol_label
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path
from libs.alpaca_security_lineage.inference_evidence import (
    ProviderEvidence,
    provider_evidence,
    reject_assertions,
)

from .contract import (
    FROZEN_BASELINE_POLICIES,
    LAB_REPORT_BOUNDARY,
    LAB_SCHEMA_VERSION,
    LAB_VERSION,
    PolicyLabError,
    replayable_lineage_versions,
    validate_change_log,
)
from .fallbacks import CORRECTED_PROFILE
from .policy import CompiledPolicy, require_generic_candidate


def _select_provider_evidence(evidence, *, cache=None):
    cache = {} if cache is None else cache
    selected = {}
    for symbol, original in evidence.items():
        accepted = cache.get(id(original))
        if accepted is None:
            accepted = original, provider_evidence(original)
            cache[id(original)] = accepted
        selected[symbol] = accepted[1]
    return selected


def _candidate_evidence(evidence, *, where):
    # The shared ProviderEvidence values carry their completed acceptance;
    # only previously unaccepted, direct values require the ingress guard.
    reject_assertions(evidence, where=where)
    return evidence


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
def _load_group(
    store: Path,
    seed_symbol: str,
    *,
    selection=None,
    held_receipts=None,
    held_inference_evidence=None,
    held_boundaries=None,
    accepted_store=None,
):
    # Explicit comparison owns this selected complete-group replay. The snapshot
    # selects and locks its publications; input generation checks bind the replay
    # to those facts. DO NOT repair pending work, fetch replacement evidence or
    # expand into unrelated groups when an input cannot be consumed.
    if held_inference_evidence is not None and held_receipts is None:
        raise PolicyLabError("held inference evidence requires its selected completed receipts")
    if held_receipts is not None:
        if held_inference_evidence is None:
            for receipt in held_receipts.values():
                reject_assertions(receipt.get("evidence", {}), where="supplied research receipt evidence")
        elif set(held_inference_evidence) != set(held_receipts) or any(
            not isinstance(value, ProviderEvidence) for value in held_inference_evidence.values()
        ):
            raise PolicyLabError("held inference evidence requires its completed provider-only handoff")
    select = selection if selection is not None else lambda root: select_inputs(root, seed_symbol)
    options = {} if accepted_store is None else {"accepted_store": accepted_store}
    if held_receipts is None:
        context = reader_snapshot(store, select, **options)
    else:
        context = reader_snapshot(store, select, include_receipts=False, include_boundaries=False, **options)
    with context as snapshot:
        if held_receipts is not None:
            from libs.alpaca_lineage_build.storage import read_boundaries

            root = store / "end_of_day/lineage"
            snapshot = {
                **snapshot,
                "receipts": held_receipts,
                "boundaries": held_boundaries
                if held_boundaries is not None
                else {name: read_boundaries(root, receipt) for name, receipt in held_receipts.items()},
            }
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

        projection_cache = {}
        if held_inference_evidence is not None:
            for name, receipt in receipts.items():
                original = receipt["evidence"]
                projection_cache[id(original)] = original, held_inference_evidence[name]
        inference_evidence = held_inference_evidence
        if inference_evidence is None:
            inference_evidence = _select_provider_evidence(
                {symbol: receipt["evidence"] for symbol, receipt in receipts.items()}, cache=projection_cache
            )
        auxiliary_evidence = _select_provider_evidence(
            {name: item["evidence"] for name, item in unavailable.items()}, cache=projection_cache
        )
        needed = set(receipts)
        sources = {}
        inputs = {}
        for symbol, expected_generation in sorted(expected_sources.items()):
            if symbol not in needed:
                continue
            if symbol not in snapshot["locked"]:
                raise PolicyLabError(f"{symbol}: selected pricing source is not locked")
            pricing_root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
            if read_update(pricing_root) is not None:
                raise PolicyLabError(f"{symbol}: lineage awaits a pending pricing update")
            source = load_source(pricing_root, symbol)
            if source is None or document_digest(source) != expected_generation:
                raise PolicyLabError(f"{symbol}: selected pricing generation changed")
            sources[symbol] = source
            inputs[symbol] = load_pricing(store, symbol, source=source, source_sha256=expected_generation)
            # An on-store completed suffix already owns the split observation
            # and later grid. The candidate can assess the earlier positions
            # using that handoff. Supplied research receipts cannot seed it.
            suffix = receipts[symbol].get("current_source_ownership")
            if (
                held_receipts is None
                and selected_group == {symbol}
                and isinstance(suffix, dict)
                and suffix.get("version") == 3
                and reusable_decision(receipts[symbol])
            ):
                receipt = receipts[symbol]
                accepted_boundaries = {
                    (symbol, kind, receipt["boundaries"][kind]["sha256"]): document
                    for kind, document in snapshot["boundaries"][symbol].items()
                }
                inputs[symbol].retained_split = retain_split_suffix(
                    inputs[symbol], receipt, store / "end_of_day/lineage", accepted_boundaries
                )

        boundaries = {symbol: snapshot["boundaries"][symbol] for symbol in sorted(expected_sources)}
        # These are report provenance tokens, not instructions for a consumer to
        # rehash upstream files or republish the compared production baseline.
        receipt_generations = {
            symbol: snapshot["entries"][symbol]["sha256"] for symbol in sorted(expected_sources)
        }
        evidence_generations = {
            symbol: document_digest(inference_evidence[symbol]) for symbol in sorted(expected_sources)
        }
        yield {
            "inputs": inputs,
            "receipts": receipts,
            "inference_evidence": inference_evidence,
            "boundaries": boundaries,
            "receipt_generations": receipt_generations,
            "evidence_generations": evidence_generations,
            "pricing_generations": dict(sorted(expected_sources.items())),
            "unavailable_pricing": sorted(unavailable),
            "auxiliary_evidence": auxiliary_evidence,
            "selected_group": sorted(selected_group),
            "materialized_sources": sorted(needed),
        }


def compare_publication(published, frame, policy, receipts, reports, fallback_execution, max_differences):
    """Compare a completed policy frame to the selected publication."""
    differences = {
        name: _difference(
            published["dimensions"][name],
            frame["dimensions"][name],
            max_differences,
            baseline_sha256=published["digests"][name],
            candidate_sha256=frame["digests"][name],
        )
        for name in frame["dimensions"]
    }
    baseline_memberships = published["memberships"]
    candidate_memberships = frame["memberships"]
    baseline_counts = published["counts"]
    candidate_counts = frame["counts"]
    baseline_reported_counts = published["reported_counts"]
    candidate_reported_counts = _candidate_counts(reports)
    memberships_match = baseline_memberships == candidate_memberships
    counts_match = baseline_counts == candidate_counts
    baseline_counts_valid = _counts_match_rows(baseline_reported_counts, baseline_counts)
    candidate_counts_valid = _counts_match_rows(candidate_reported_counts, candidate_counts)
    reports_complete = frame["partition_complete"]
    decision_parity = (
        all(differences[name]["count"] == 0 for name in ("semantic", "observation", "explanation"))
        and memberships_match
        and counts_match
        and baseline_counts_valid
        and candidate_counts_valid
        and reports_complete
        and not frame["mapping_corrections"]
    )
    diagnostic_snapshot_match = differences["group_diagnostics"]["count"] == 0
    exact_snapshot_parity = decision_parity and diagnostic_snapshot_match
    migrated_rule_order = not diagnostic_snapshot_match and _migration_preserved_rule_order_provenance(
        policy, receipts, reports, fallback_execution
    )
    migration_classification = "migration_preserved_rule_order_provenance" if migrated_rule_order else None
    migration_compatible = decision_parity and (diagnostic_snapshot_match or migrated_rule_order)
    return {
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
        "mapping_corrections": frame["mapping_corrections"],
    }


def _compare_loaded_group(
    store: Path,
    seed_symbol: str,
    policy: CompiledPolicy,
    selected: dict[str, Any],
    *,
    max_differences: int,
    comparison_handoff=None,
    completed=None,
    refine=None,
    defer_publication_comparison=False,
) -> dict[str, Any]:
    """Evaluate one already selected complete group without further storage reads."""
    require_generic_candidate(policy)
    if refine is not None:
        from libs.alpaca_end_of_day_security_correlation.component_refinement import refine as generic_refine

        if refine is not generic_refine:
            raise PolicyLabError("research refinement must use the registered generic producer")
    inputs = selected["inputs"]
    receipts = selected["receipts"]
    raw_evidence = selected.get("inference_evidence")
    if raw_evidence is None:
        raw_evidence = {symbol: receipt["evidence"] for symbol, receipt in receipts.items()}
    raw_evidence = _candidate_evidence(raw_evidence, where="supplied research evidence")
    auxiliary_evidence = _candidate_evidence(
        selected.get("auxiliary_evidence", {}), where="supplied auxiliary research evidence"
    )
    evidence_generations = selected.get("evidence_generations")
    if evidence_generations is None:
        evidence_generations = {
            symbol: document_digest(evidence) for symbol, evidence in sorted(raw_evidence.items())
        }
    if policy.policy_role in {
        "historical_reference",
        "published_reference",
        "corrected_candidate",
        "production",
    }:
        allowed_baselines = FROZEN_BASELINE_POLICIES.keys() | {(LINEAGE_VERSION, CORRELATION_VERSION)}
        if any(
            (
                receipt["versions"].get("lineage"),
                receipt["versions"].get("correlation"),
            )
            not in allowed_baselines
            for receipt in receipts.values()
        ):
            raise PolicyLabError(
                "policy comparison requires a supported completed publication as baseline-only data"
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
            auxiliary_evidence=auxiliary_evidence,
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
            auxiliary_evidence=auxiliary_evidence,
            evidence_generations=evidence_generations,
            pricing_generations=selected["pricing_generations"],
            execution_trace=execution,
            evidence_projection=evidence,
        )
        evidence_quality = execution.get("evidence_quality")
        active_suffix = execution.get("active_suffix")
        if not isinstance(evidence_quality, dict) or not isinstance(active_suffix, dict):
            raise PolicyLabError("policy evaluation returned an incomplete execution trace")
    raw_reports = reports
    refinement = None
    if refine is not None:
        reports, refinement = refine(
            reports,
            raw_evidence,
            {
                **selected,
                "membership_version": policy.implementation_correlation_version,
                "current_donors": policy.implementation_correlation_version >= 94,
            },
        )
    reports, mapping_corrections = select_observations(
        inputs, reports, correlation_version=policy.implementation_correlation_version
    )
    candidate_boundaries = {}
    for symbol, view in inputs.items():
        _, candidate_boundaries[symbol] = make_publication(
            view,
            evidence[symbol],
            reports[symbol],
            receipts[symbol]["dependencies"],
        )

    if comparison_handoff is None:
        row_indexes = {}
        baseline = _group_dimensions(selected["boundaries"], inputs, receipts, row_indexes=row_indexes)
        baseline["group_diagnostics"] = _group_diagnostics(receipts)
        comparison_handoff = {
            "dimensions": baseline,
            "row_indexes": row_indexes,
            "digests": {name: _digest_mapping(values) for name, values in baseline.items()},
            "memberships": _memberships(baseline["semantic"]),
            "counts": _row_counts(baseline["semantic"]),
            "reported_counts": _published_counts(receipts),
        }
    else:
        row_indexes = comparison_handoff["row_indexes"]
    candidate = _group_dimensions(candidate_boundaries, inputs, receipts, row_indexes=row_indexes)
    candidate["group_diagnostics"] = _group_diagnostics(reports)
    candidate_digests = {name: _digest_mapping(values) for name, values in candidate.items()}
    unlabelled_dimensions, unlabelled_digests = candidate, candidate_digests
    label_trace = None
    if getattr(policy, "dated_labels", None):
        from libs.alpaca_lineage_build.dated_labels import project_observation_frame

        candidate, label_trace = project_observation_frame(
            candidate,
            reports,
            evidence,
            row_indexes,
            requests={name: view.source.get("latest_request", {}) for name, view in inputs.items()},
        )
        if label_trace["changes"]:
            candidate_digests = {**candidate_digests, "observation": _digest_mapping(candidate["observation"])}

    frame = {
        "dimensions": candidate,
        "digests": candidate_digests,
        "memberships": _memberships(candidate["semantic"]),
        "counts": _row_counts(candidate["semantic"]),
        "mapping_corrections": mapping_corrections,
        "partition_complete": all(report.get("row_partition_complete") is True for report in reports.values()),
    }
    if completed is not None:
        completed.update(
            published=comparison_handoff,
            unlabelled_dimensions=unlabelled_dimensions,
            unlabelled_digests=unlabelled_digests,
            label_trace=label_trace,
            candidate=frame,
            refinement=refinement,
            reports=reports,
            raw_reports=raw_reports,
            refinement_evidence=raw_evidence,
            candidate_boundaries=candidate_boundaries,
            evidence=evidence,
        )
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
        "comparison": None
        if defer_publication_comparison
        else compare_publication(
            comparison_handoff, frame, policy, receipts, reports, fallback_execution, max_differences
        ),
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
    require_generic_candidate(policy)
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

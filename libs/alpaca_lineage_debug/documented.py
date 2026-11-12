"""Explicit primary chronology against accepted publications, without rule replay."""

from libs.alpaca_end_of_day_security_correlation.component_refinement import refine
from libs.alpaca_lineage_build import source_work
from libs.alpaca_lineage_build.documented_updates import apply_updates
from libs.alpaca_lineage_build.documented_updates import merge_plan as merge_plan
from libs.alpaca_lineage_build.documented_updates import numeric_changes as numeric_changes
from libs.alpaca_lineage_build.transform import retained_decision_report
from libs.alpaca_lineage_policy_lab.comparison import (
    _digest_mapping,
    _group_diagnostics,
    _group_dimensions,
    _memberships,
    _row_counts,
    make_publication,
)


def scoped_facts(policy, group):
    selected = []
    for entry in policy.documented_facts:
        scope = set(entry["symbols"])
        if scope.intersection(group):
            if not scope <= set(group):
                raise ValueError("documented fact scope crosses the selected complete group")
            selected.append(entry)
    return selected


def accepted_reports(selected, policy):
    reports = {}
    for name in selected["inputs"]:
        receipt = selected["receipts"][name]
        cache = {
            (name, kind, reference["sha256"]): selected["boundaries"][name][kind]
            for kind, reference in receipt["boundaries"].items()
        }
        reports[name] = {
            **receipt,
            **retained_decision_report(receipt, policy=policy),
            **source_work.retained_ownership_report(receipt, selected["inputs"][name].local, {}, None, cache),
        }
    return reports


def materialize_sources(policy, receipts, group, *, membership_pairs_handoff=None):
    """Select possible owner peers from accepted metadata before price assembly."""
    entries = scoped_facts(policy, group)
    scope = {name for entry in entries for name in entry["symbols"]}
    if policy.membership_refinement:
        if membership_pairs_handoff is None:
            return set(receipts)  # Unknown membership scope is not an exclusion proof.
        scope.update(name for pair in membership_pairs_handoff for name in (pair["source"], pair["donor"]))
    cusips = {
        episode["cusip"]
        for entry in entries
        for fact in entry["supplements"]
        for episode in fact["facts"]["episodes"]
    } | {
        fact["facts"][role]["cusip"]
        for entry in entries
        for fact in entry["supplements"]
        for role in ("predecessor", "successor")
    }
    identifiers = {
        lookup["asset"]["id"]
        for receipt in receipts.values()
        for lookup in receipt["evidence"].get("asset_lookups", ())
        if lookup.get("lookup_kind") == "cusip"
        and lookup.get("lookup") in cusips
        and lookup.get("asset")
        and lookup["asset"].get("id")
    }
    for name, receipt in receipts.items():
        members = receipt.get("identity_memberships")
        if members is None or any(
            identifiers.intersection(identity["member_alpaca_asset_ids"]) for identity in members
        ):
            scope.add(name)
    return scope


def frame(dimensions, reports, *, corrections=None):
    return {
        "dimensions": dimensions,
        "digests": {name: _digest_mapping(value) for name, value in dimensions.items()},
        "memberships": _memberships(dimensions["semantic"]),
        "counts": _row_counts(dimensions["semantic"]),
        "mapping_corrections": corrections or {},
        "partition_complete": all(value["row_partition_complete"] for value in reports.values()),
    }


def evaluate(store, seed, policy, control_policy, selected, maximum):
    from libs.alpaca_lineage_build.observations import select_observations

    from .paired import _retained, compare_frames, same_cascade

    if not same_cascade(policy, control_policy):
        raise ValueError("accepted-publication control cannot change the executable rule cascade")
    before = accepted_reports(selected, control_policy)
    evidence = {name: receipt["evidence"] for name, receipt in selected["receipts"].items()}
    reports, membership = refine(before, evidence, selected) if policy.membership_refinement else (before, None)
    reports, evidence, plans, documented_affected = apply_updates(
        selected["inputs"],
        reports,
        evidence,
        selected["receipts"],
        scoped_facts(policy, selected["selected_group"]),
    )
    affected = {change["source"] for change in (membership or {}).get("changes", ())} | documented_affected
    changes = numeric_changes(before, reports)
    corrections, equality = {}, {}
    if changes:
        component_ids = {
            segment["security_identity"]["security_id"]
            for change in changes
            for segment in reports[change["source"]]["correlated"]["segments"]
            if segment["adjustment"] == change["adjustment"]
            and segment["row_start"] <= change["row_end"]
            and change["row_start"] <= segment["row_end"]
        }
        affected.update(
            name
            for name, report in reports.items()
            if any(
                segment["security_identity"]["security_id"] in component_ids
                for segment in report["correlated"]["segments"]
            )
        )
        for name, report in reports.items():
            reports[name] = {**report, "retained_observation_routes": before[name].get("observation_routes", {})}
        reports, corrections = select_observations(
            selected["inputs"],
            reports,
            retained_choices=_retained(reports, changes, selected),
            equality_outcomes=equality,
        )
    indexes = {}
    baseline_dimensions = _group_dimensions(
        selected["boundaries"], selected["inputs"], selected["receipts"], row_indexes=indexes
    )
    baseline_dimensions["group_diagnostics"] = _group_diagnostics(
        {name: selected["receipts"][name] for name in before}
    )
    baseline = frame(baseline_dimensions, before)
    receipts, boundaries = dict(selected["receipts"]), dict(selected["boundaries"])
    for name in sorted(affected):
        reports[name] = {**reports[name], "correlation_policy": policy.provenance()}
        receipts[name], boundaries[name] = make_publication(
            selected["inputs"][name], evidence[name], reports[name], selected["receipts"][name]["dependencies"]
        )
        receipts[name]["input_sources"] = selected["pricing_generations"]
        if "unavailable_pricing" in selected["receipts"][name]:
            receipts[name]["unavailable_pricing"] = selected["receipts"][name]["unavailable_pricing"]
    dimensions = {
        key: {
            position: value
            for position, value in values.items()
            if key == "group_diagnostics" or position[0] not in affected
        }
        for key, values in baseline_dimensions.items()
    }
    fresh = _group_dimensions(
        {name: boundaries[name] for name in affected},
        {name: selected["inputs"][name] for name in affected},
        selected["receipts"],
        row_indexes=indexes,
    )
    for key, values in fresh.items():
        dimensions[key].update(values)
    dimensions["group_diagnostics"] = _group_diagnostics(reports)
    candidate = frame(dimensions, reports, corrections=corrections)
    impact = compare_frames(baseline, candidate, maximum)
    # Numerical selection splits positions. The constructed boundaries own the
    # precise clipped timestamps/counts; assemble that completed representation
    # once for the handoff, without reopening outputs or replaying any decision.
    handoff_reports = {
        **before,
        **accepted_reports(
            {
                **selected,
                "inputs": {name: selected["inputs"][name] for name in affected},
                "receipts": {name: receipts[name] for name in affected},
                "boundaries": {name: boundaries[name] for name in affected},
            },
            policy,
        ),
    }
    return {
        "group": selected["selected_group"],
        "seed_symbol": seed,
        "mode": "paired_accepted_publication",
        "control_kind": "accepted_publication_no_replay",
        "comparison_scope": {
            "materialized_sources": sorted(selected["inputs"]),
            "unchanged_metadata_only_sources": sorted(set(selected["receipts"]) - set(selected["inputs"])),
            "proof": "No accepted component intersects the scoped documented endpoints.",
        },
        "control_policy": control_policy.as_report_object(),
        "candidate_policy": policy.as_report_object(),
        "baseline": {
            name: selected[name]
            for name in ("receipt_generations", "pricing_generations", "evidence_generations")
        },
        "published_to_control": compare_frames(baseline, baseline, maximum),
        "control_to_candidate": impact,
        "published_to_candidate": impact,
        "comparison": impact,
        "control_correlation_reused": True,
        "membership_refinement": membership,
        "documented_plans": plans,
        "new_numerical_scope": changes,
        "candidate_handoff": {
            "boundary": "alpaca_lineage_research_candidate",
            "schema_version": 1,
            "publication_authorized": False,
            "store": str(store),
            "policy": policy.as_report_object(),
            "receipts": receipts,
            "boundaries": boundaries,
            "reports": handoff_reports,
            "pricing_generations": selected["pricing_generations"],
            "baseline_receipt_generations": selected["receipt_generations"],
            "completed_numerical_equality": [
                {"public_id": key[0], "timestamp": key[1].isoformat(), "equal": value}
                for key, value in sorted(equality.items())
            ],
            "equality_capture": {"requested": True, "complete": True},
        },
        "limits": [
            "Control consumes accepted publication; it does not establish full replay parity.",
            "Foreign current source contexts retain unresolved rows; no provider lookup or acquisition is invented.",
        ],
    }

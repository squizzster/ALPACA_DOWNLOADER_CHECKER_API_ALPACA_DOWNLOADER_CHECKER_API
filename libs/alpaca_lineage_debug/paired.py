"""Compare published, unchanged control and candidate on one held group.

Membership-only research consumes completed control ownership and choices. A
different YAML cascade receives its own explicit evaluation on the same inputs.
"""

from collections import Counter, defaultdict
from dataclasses import fields
from types import MappingProxyType

from libs.alpaca_end_of_day_security_correlation.component_refinement import refine
from libs.alpaca_lineage_build.correlation_policy import CompiledPolicy
from libs.alpaca_lineage_build.observation_carry import ChoiceRun, RetainedChoices
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_policy_lab.comparison import (
    _compare_loaded_group,
    _digest_mapping,
    _group_diagnostics,
    _group_dimensions,
    _memberships,
    _row_counts,
    make_publication,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


def same_cascade(left, right):
    descriptive = {"policy_id", "policy_version", "policy_role", "file_sha256", "semantic_sha256", "source"}
    return all(
        getattr(left, field.name) == getattr(right, field.name)
        for field in fields(CompiledPolicy)
        if field.name not in descriptive
    )


def _difference(before, after, maximum, first_digest, second_digest, *, row_keys):
    examples, sources, changed_fields = [], {}, Counter()
    count = 0
    for key in sorted(before.keys() | after.keys()):
        left, right = before.get(key), after.get(key)
        if left == right:
            continue
        count += 1
        if len(examples) < maximum:
            examples.append({"key": list(key), "control": left, "candidate": right})
        if row_keys:
            source, adjustment, instant = key
            item = sources.setdefault(
                source, {"source_view_rows": 0, "first_timestamp": instant, "last_timestamp": instant}
            )
            item["source_view_rows"] += 1
            item["first_timestamp"] = min(item["first_timestamp"], instant)
            item["last_timestamp"] = max(item["last_timestamp"], instant)
        if isinstance(left, dict) and isinstance(right, dict):
            changed_fields.update(
                name for name in left.keys() | right.keys() if left.get(name) != right.get(name)
            )
    return {
        "count": count,
        "control_sha256": first_digest,
        "candidate_sha256": second_digest,
        "examples": examples,
        "examples_truncated": count > len(examples),
        "affected_sources": sources,
        "changed_fields": dict(sorted(changed_fields.items())),
    }


def compare_frames(control, candidate, maximum):
    differences = {
        name: _difference(
            control["dimensions"][name],
            candidate["dimensions"][name],
            maximum,
            control["digests"][name],
            candidate["digests"][name],
            row_keys=name != "group_diagnostics",
        )
        for name in control["dimensions"]
    }
    decision_parity = (
        not any(differences[name]["count"] for name in ("semantic", "observation"))
        and control["memberships"] == candidate["memberships"]
        and control["counts"] == candidate["counts"]
        and control.get("mapping_corrections", {}) == candidate.get("mapping_corrections", {})
        and candidate.get("partition_complete", True)
    )
    return {
        "parity": decision_parity and not any(item["count"] for item in differences.values()),
        "decision_parity": decision_parity,
        "diagnostic_only": decision_parity and any(item["count"] for item in differences.values()),
        "differences": differences,
        "memberships": {"control": control["memberships"], "candidate": candidate["memberships"]},
        "mapping_corrections": {
            "control": control.get("mapping_corrections", {}),
            "candidate": candidate.get("mapping_corrections", {}),
        },
        "candidate_partition_complete": candidate.get("partition_complete", True),
    }


def _retained(control, changes, selected):
    excluded = defaultdict(list)
    for change in changes:
        excluded[change["source"], change["adjustment"]].append((change["row_start"], change["row_end"]))
    choices = {}
    for source, report in control.items():
        runs = {name: [] for name in ADJUSTMENTS}
        for segment in report["correlated"]["segments"]:
            name = segment["adjustment"]
            first, last = segment["row_start"], segment["row_end"]
            cuts = sorted(excluded[source, name])
            parts, cursor = [], first
            for left, right in cuts:
                if right < cursor or left > last:
                    continue
                if cursor < left:
                    parts.append((cursor, min(last, left - 1)))
                cursor = max(cursor, right + 1)
            if cursor <= last:
                parts.append((cursor, last))
            for left, right in parts:
                runs[name].append(
                    ChoiceRun(
                        left,
                        right,
                        segment["security_identity"]["security_id"],
                        segment.get("observation_selection"),
                        tuple(segment.get("observation_tickers", ())),
                        segment.get("first_session_date", segment.get("from")),
                        segment.get("last_session_date", segment.get("to")),
                    )
                )
        choices[source] = RetainedChoices(
            selected["pricing_generations"][source],
            selected["receipt_generations"][source],
            MappingProxyType(
                {name: tuple(sorted(parts, key=lambda run: run.row_start)) for name, parts in runs.items()}
            ),
        )
    return choices


def _refined_candidate(selected, control, maximum):
    reports, trace = refine(control["reports"], control["evidence"], selected)
    if not trace["changes"]:
        return control["candidate"], trace
    for source, report in reports.items():
        # Completed latest own-label routes must travel into this narrow contest.
        reports[source] = {
            **report,
            "retained_observation_routes": control["reports"][source].get("observation_routes", {}),
        }
    reports, corrections = select_observations(
        selected["inputs"],
        reports,
        retained_choices=_retained(control["reports"], trace["changes"], selected),
    )
    affected = {item["old_public_id"] for item in trace["changes"]} | {
        item["new_public_id"] for item in trace["changes"]
    }
    sources = {
        source
        for source, report in reports.items()
        if any(
            segment["security_identity"]["security_id"] in affected
            for segment in report["correlated"]["segments"]
        )
    }
    boundaries = {}
    for source in sources:
        _, boundaries[source] = make_publication(
            selected["inputs"][source],
            control["evidence"][source],
            reports[source],
            selected["receipts"][source]["dependencies"],
        )
    dimensions = {
        name: {
            key: value for key, value in values.items() if name == "group_diagnostics" or key[0] not in sources
        }
        for name, values in control["candidate"]["dimensions"].items()
    }
    fresh = _group_dimensions(
        boundaries,
        {source: selected["inputs"][source] for source in sources},
        selected["receipts"],
        row_indexes=control["published"]["row_indexes"],
    )
    for name, values in fresh.items():
        dimensions[name].update(values)
    dimensions["group_diagnostics"] = _group_diagnostics(reports)
    frame = {
        "dimensions": dimensions,
        "digests": {name: _digest_mapping(values) for name, values in dimensions.items()},
        "memberships": _memberships(dimensions["semantic"]),
        "counts": _row_counts(dimensions["semantic"]),
        "mapping_corrections": {**control["candidate"]["mapping_corrections"], **corrections},
        "partition_complete": all(report.get("row_partition_complete") is True for report in reports.values()),
    }
    return frame, trace


def evaluate(store, seed, policy, control_policy, selected, maximum):
    mode = getattr(policy, "control_mode", "replay")
    if mode in {"accepted_publication", "documented_scopes"}:
        from .documented import evaluate as evaluate_accepted
        from .documented import scoped_facts

        if mode == "accepted_publication" or scoped_facts(policy, selected["selected_group"]):
            return evaluate_accepted(store, seed, policy, control_policy, selected, maximum)
    control = {}
    control_report = _compare_loaded_group(
        store, seed, control_policy, selected, max_differences=maximum, completed=control
    )
    trace = None
    if same_cascade(control_policy, policy):
        if getattr(policy, "membership_refinement", None):
            candidate, trace = _refined_candidate(selected, control, maximum)
        else:
            candidate = control["candidate"]
    else:
        candidate_completed = {}
        callback = refine if getattr(policy, "membership_refinement", None) else None
        _compare_loaded_group(
            store,
            seed,
            policy,
            selected,
            max_differences=maximum,
            comparison_handoff=control["published"],
            completed=candidate_completed,
            refine=callback,
        )
        candidate = candidate_completed["candidate"]
        trace = candidate_completed["refinement"]
    impact = compare_frames(control["candidate"], candidate, maximum)
    return {
        "group": selected["selected_group"],
        "seed_symbol": seed,
        "mode": "paired_same_inputs",
        "control_policy": control_policy.as_report_object(),
        "candidate_policy": policy.as_report_object(),
        "baseline": control_report["baseline"],
        "published_to_control": control_report["comparison"],
        "control_to_candidate": impact,
        "published_to_candidate": compare_frames(control["published"], candidate, maximum),
        "comparison": impact,
        "membership_refinement": trace,
        "control_correlation_reused": same_cascade(control_policy, policy),
    }

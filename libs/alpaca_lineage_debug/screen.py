"""Metadata-only applicability planning for the named membership experiment."""

from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.component_refinement import NAME, membership_pairs
from libs.alpaca_lineage_build import storage

from . import accept_report
from .paired import same_cascade
from .policy import policy_applies, policy_applies_to_group, require_generic_policy
from .research import failure, select


def screen(store, policy, control):
    require_generic_policy(policy)
    require_generic_policy(control)
    if (
        control is None
        or getattr(control, "membership_refinement", None) is not None
        or not same_cascade(control, policy)
        or getattr(policy, "membership_refinement", None) != NAME
    ):
        raise ValueError(
            "metadata screening requires the named membership candidate and the same unchanged control cascade"
        )
    store, root, index = select(store)
    groups = sorted({tuple(entry["group_members"]) for entry in index["symbols"].values()})
    unaffected, candidates, unknown, failures = [], [], [], []
    reads = 0
    with storage.publication_lock(root):
        storage.assert_index_current(root, index)
        for group in groups:
            available = [source for source in group if source in index["symbols"]]
            try:
                if len(available) < 2:
                    unaffected.append({"group": list(group), "reason": "no_different_indexed_donor_source"})
                    continue
                receipts = {}
                for source in available:
                    reads += 1
                    receipts[source] = storage.read_receipt(root, source, index)
                pairs = membership_pairs(receipts)
                record = {"group": list(group), "receipts": receipts}
                if pairs is None:
                    unknown.append(record)
                elif pairs:
                    record["membership_pairs"] = [
                        {
                            "source": pair["source"],
                            "old_public_id": pair["old"]["security_id"],
                            "donor": pair["donor"],
                            "new_public_id": pair["target"]["security_id"],
                            "old_members": sorted(pair["old_members"]),
                            "target_members": sorted(pair["target_members"]),
                        }
                        for pair in pairs
                    ]
                    candidates.append(record)
                else:
                    unaffected.append(
                        {"group": list(group), "reason": "no_accepted_cross_source_containing_component"}
                    )
            except (RuntimeError, ValueError, OSError) as error:
                failures.append({"group": list(group), "error": failure(error)})
    return {
        "status": "complete" if not failures else "incomplete",
        "execution_complete": not failures,
        "mode": "membership_applicability_screen",
        "store": str(store),
        "candidate_policy": policy.as_report_object(),
        "control_policy": control.as_report_object(),
        "index_handoff": index,
        "summary": {
            "indexed_groups": len(groups),
            "proven_candidate_unaffected_groups": len(unaffected),
            "candidate_groups": len(candidates),
            "unknown_groups": len(unknown),
            "failed_groups": len(failures),
            "receipt_reads": reads,
            "pricing_payload_reads": 0,
        },
        "unaffected_groups": unaffected,
        "candidate_groups": candidates,
        "unknown_groups": unknown,
        "failures": failures,
        "expected_io": {
            "pricing_sources_if_evaluated": len(
                {source for item in candidates + unknown for source in item["receipts"]}
            ),
            "provider_requests": 0,
            "production_writes": 0,
        },
        "limits": [
            "A positive pair is a possibility, not proof that the candidate changes a decision.",
            "Candidate-unaffected proofs do not establish stored/control parity or overall lineage correctness.",
            "Unknown membership capabilities remain selected for evaluation; they are never silently excluded.",
        ],
    }


def accept_screen(value, store, policy, control):
    accept_report(value, "screen", allow_previous=True)
    if value.get("operation") != "screen" or value.get("mode") != "membership_applicability_screen":
        raise ValueError("expected a completed lineage membership screen")
    if value.get("store") != str(Path(store).absolute()) or not value.get("execution_complete"):
        raise ValueError("screen has an incompatible store binding or incomplete coverage")
    if not policy_applies_to_group(value["candidate_policy"], policy, []) or not policy_applies(
        value["control_policy"], control
    ):
        raise ValueError("screen belongs to another control/candidate policy")
    root = Path(store).absolute() / "end_of_day/lineage"
    index = value["index_handoff"]
    with storage.publication_lock(root):
        storage.assert_index_current(root, index)
    return root, index

"""Install reviewed, generation-bound producer outputs without recomputing them.

The completed candidate owns correlation, numerical choice and assignment
construction. Ingress accepts its envelope once. Locked selector checks establish
applicability; they never authorize another comparison or historical payload read.
The ordinary publication journal owns installation and interruption recovery.
"""

from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path

from . import storage
from .assignment_handoff import accept_observation_choices, selected
from .continuation import validate_handoff
from .contract import COMPUTATION_REVISION, LineageError
from .retained_recovery import RECOVERY_BOUNDARY, accept_selection

CANDIDATE_BOUNDARY = "alpaca_retained_candidate_handoff"
CANDIDATE_VERSION = 1


@dataclass(frozen=True)
class PreparedCandidate:
    receipts: dict
    boundaries: dict
    previous: dict
    policy: dict
    assignments: dict
    observations: object = None


def accept_candidate(value, store):
    """Accept saved output and its completed review, without replaying the lab."""
    policy = value.get("policy")
    review = value.get("review")
    if (
        type(value.get("version")) is not int
        or value["version"] != CANDIDATE_VERSION
        or value.get("boundary") != CANDIDATE_BOUNDARY
        or value.get("computation_revision") != COMPUTATION_REVISION
        or not isinstance(policy, dict)
        or policy.get("policy_role") != "production"
        or not isinstance(review, dict)
        or review.get("version") != 1
        or review.get("status") != "approved"
        or not isinstance(review.get("conclusion"), str)
        or not review["conclusion"]
    ):
        raise LineageError("prepared candidate requires a complete approved review and production policy")
    selection = accept_selection(
        {
            "schema_version": 1,
            "boundary": RECOVERY_BOUNDARY,
            "store": value.get("store"),
            "correlation_policy_sha256": policy.get("semantic_sha256"),
            "groups": [
                {
                    "members": value.get("members"),
                    "pricing_generations": value.get("pricing_generations"),
                    "receipt_generations": value.get("receipt_generations"),
                }
            ],
        },
        store,
    )
    group = selection.groups[0]
    receipts, boundaries, previous = (
        value.get("candidate_receipts"),
        value.get("candidate_boundaries"),
        value.get("baseline_receipts"),
    )
    if any(
        not isinstance(items, dict) or items.keys() != group.receipts.keys()
        for items in (receipts, boundaries, previous)
    ):
        raise LineageError("prepared candidate output does not cover its selected sources")
    comparison = value.get("comparison", {}).get("comparison", {})
    if comparison.get("mapping_corrections") != {} or comparison.get("candidate_partition_complete") is not True:
        raise LineageError("prepared candidate requires completed assignments and no pricing correction")
    provenance = {
        key: policy[key] for key in receipts[group.seed].get("correlation_policy", {}) if key in policy
    }
    for name, receipt in receipts.items():
        old = previous[name]
        if (
            receipt.get("symbol") != name
            or receipt.get("source_sha256") != group.pricing[name]
            or receipt.get("correlation_policy") != provenance
            or receipt.get("computation_revision") != COMPUTATION_REVISION
            or not isinstance(boundaries[name], dict)
            or not boundaries[name].keys() <= receipt.get("boundaries", {}).keys()
            or any(
                receipt["boundaries"][kind] != old.get("boundaries", {}).get(kind)
                for kind in receipt.get("boundaries", {})
                if kind not in boundaries[name]
            )
            or old.get("symbol") != name
            or old.get("source_sha256") != group.pricing[name]
        ):
            raise LineageError(f"{name}: incomplete prepared candidate binding")
        validate_handoff(receipt)
        accept_observation_choices(receipt)
        for kind in boundaries[name]:
            handoff = selected(receipt, kind)
            if handoff is None or not isinstance(handoff.get("_projection"), dict):
                raise LineageError(f"{name}: prepared assignment projection is missing")
    accepted = {
        name: {
            **{
                kind: body
                for kind, body in value.get("baseline_boundaries", {}).get(name, {}).items()
                if receipt["boundaries"].get(kind) == previous[name]["boundaries"].get(kind)
            },
            **boundaries[name],
        }
        for name, receipt in receipts.items()
    }
    observations = None
    if "selected_observation_bundles" in value:
        from .source_choice_handoff import accept

        revisions = {
            receipt["numerical_source_revision"]["id"]: receipt["numerical_source_revision"]
            for receipt in receipts.values()
            if receipt.get("numerical_source_revision")
        }
        if len(revisions) != 1:
            raise LineageError("prepared selected observations require one completed source revision")
        observations = accept(value["selected_observation_bundles"], next(iter(revisions.values())))
    return replace(
        selection, prepared=PreparedCandidate(receipts, boundaries, previous, provenance, accepted, observations)
    )


def publish_prepared(selection, context, *, progress=None):
    """Check current bindings, finalize bookkeeping and journal completed output."""
    from .pipeline import _current_sources
    from .recovery_handoffs import remember

    store = Path(selection.store)
    prepared = selection.prepared
    for name, receipt in prepared.receipts.items():
        for kind, handoff in receipt["assignment_handoffs"].items():
            if "observation_choices" in handoff:
                remember(store, "choices", (name, kind, handoff["generation"]), handoff["observation_choices"])
    with storage.writer_lock(store, accepted_store=context.accept_store()) as root:
        recovered = context.recover_lineage(root)
        policy = context.accept_correlation_policy()
        # Exact implementation/semantic provenance matters; YAML formatting does
        # not revoke a completed calculation under the same authorized policy.
        current = policy.provenance()
        if {k: v for k, v in current.items() if k != "file_sha256"} != {
            k: v for k, v in prepared.policy.items() if k != "file_sha256"
        }:
            raise LineageError("prepared recovery policy changed from accepted comparison")
        if any(receipt.get("research_admission") is not None for receipt in prepared.receipts.values()):
            from .research_admission import require_canonical_applicability

            require_canonical_applicability(prepared, context.accept_ruleset())
        with storage.publication_lock(root):
            index = context.select_index(root)
        pending, completed = selection.select(index)
        for group in pending:
            with storage.pricing_locks(store, group.members, known=index["symbols"]):
                sources = _current_sources(store, group.members, known=index["symbols"])
                group.accept_sources(
                    group.members, {name: storage.document_digest(source) for name, source in sources.items()}
                )
                if prepared.observations is not None:
                    remember(
                        store,
                        "source_choice_bundles",
                        prepared.observations["revision_id"],
                        prepared.observations.observations,
                    )
                receipts = deepcopy(prepared.receipts)
                boundaries = deepcopy(prepared.boundaries)
                for name, receipt in receipts.items():
                    old = prepared.previous[name]
                    receipt["input_sources"] = dict(group.pricing)
                    for key in (
                        "unavailable_pricing",
                        "unavailable_obligations",
                        "update_obligation_id",
                        "evidence_requirements",
                        "last_observed_symbol_lookup",
                    ):
                        if key in old:
                            receipt[key] = deepcopy(old[key])
                    receipt["retained_recovery"] = group.completion()
                if progress:
                    progress("lineage_publication", {"symbols": sorted(receipts), "prepared_decision": True})
                storage.publish(
                    store,
                    root,
                    index,
                    receipts,
                    boundaries,
                    {},
                    previous=prepared.previous,
                    sources=sources,
                )
                for name, receipt in receipts.items():
                    remember(store, "receipt", (name, index["symbols"][name]["sha256"]), receipt)
                    for kind, document in prepared.assignments[name].items():
                        remember(
                            store, "assignment", (name, kind, receipt["boundaries"][kind]["sha256"]), document
                        )
        return {
            "failed": 0,
            "recovered": recovered,
            "previously_completed_groups": len(completed),
            "published_prepared_groups": len(pending),
            "recomputed_groups": 0,
        }

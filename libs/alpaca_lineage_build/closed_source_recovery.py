"""Owning recovery of the correlation-82 legacy closed-source envelope lift."""

from . import storage
from .assignment_handoff import accept_observation_choices
from .continuation import validate_handoff
from .contract import LineageError, selected_document
from .selection import DependencyGraph


def recover(store, root, index, pending_serving, applications):
    """Repair only selected pending publications, preserving all completed work."""
    graph = DependencyGraph(index["symbols"])
    groups, seen = [], set()
    for application in applications:
        for symbol in set(application["symbols"]) & index["symbols"].keys():
            entry = index["symbols"][symbol]
            if (
                entry["projection"].get("sourced_fact_applications", {}).get(application["id"])
                != application["fingerprint"]
            ):
                continue
            group = graph.closure({symbol})
            if group & pending_serving.keys() and not group & seen:
                groups.append(group)
                seen.update(group)
    repaired = []
    for group in groups:
        repaired.extend(_recover_group(store, root, index, group))
    return sorted(repaired)


def _recover_group(store, root, index, group):
    selected = group & index["symbols"].keys()
    fields = {"current_source_ownership", "sourced_rename_ownership"}
    fixed, previous, receipts = {}, {}, {}
    with storage.pricing_locks(store, group, known=index["symbols"]):
        with storage.publication_lock(root):
            storage.assert_index_current(root, index)
        for symbol in sorted(selected):
            entry = index["symbols"][symbol]
            old = selected_document(
                storage.path_at(root, f"{symbol}/lineage.manifest.json", published=True), entry["sha256"]
            )
            accept_observation_choices(old)
            previous[symbol] = receipts[symbol] = old
            from .recovery_handoffs import remember

            missing = fields - old.keys()
            if not missing:
                validate_handoff(old)
                remember(store, "receipt", (symbol, entry["sha256"]), old)
                continue
            if (
                old.get("schema_version") != 37
                or old.get("versions", {}).get("correlation") != 82
                or "closed_source_handoff_version" in old
                or old.get("public_common_continuation_ownership") is not None
                or old.get("sourced_rename_ownership") is not None
                or any(
                    c["new_symbol"] == symbol
                    for c in (old.get("sourced_rename_membership") or {}).get("certificates", ())
                )
                or not {
                    "public_common_continuation_membership",
                    "public_common_continuation_ownership",
                    "security_relations",
                    "membership_only_rebind",
                }
                <= old.keys()
            ):
                raise LineageError(f"{symbol}: source envelope is outside legacy-lift recovery")
            new = {**old, **{key: None for key in missing}, "closed_source_handoff_version": 1}
            validate_handoff(new)
            fixed[symbol] = receipts[symbol] = new
        if fixed:
            storage.publish(
                store,
                root,
                index,
                receipts,
                {s: {} for s in receipts},
                {},
                previous=previous,
                unchanged_decision=True,
                source_envelope_recovery=True,
            )
            for symbol, receipt in receipts.items():
                remember(store, "receipt", (symbol, index["symbols"][symbol]["sha256"]), receipt)
    return sorted(fixed)

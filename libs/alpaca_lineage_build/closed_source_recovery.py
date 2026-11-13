"""Owning recovery of the correlation-82 legacy closed-source envelope lift."""

from . import storage
from .assignment_handoff import accept_observation_choices
from .continuation import validate_handoff
from .contract import LineageError, selected_document
from .recovery_handoffs import recall, remember


def recover(store, root, index, pending_serving):
    """Repair only selected pending publications, preserving all completed work."""
    groups, seen = [], set()
    entries = index["symbols"]
    for symbol, generation in pending_serving.items():
        entry = entries.get(symbol)
        if entry is None:
            continue
        if entry["sha256"] != generation:
            raise LineageError(f"{symbol}: pending serving generation changed before envelope recovery")
        if (
            entry.get("versions", {}).get("lineage") != 37
            or entry.get("versions", {}).get("correlation") != 82
            or entry.get("projection", {}).get("closed_source_handoff_version") == 1
            or entry.get("projection", {}).get("source_ownership_envelopes_complete") is True
        ):
            continue
        members = entry.get("group_members")
        if not isinstance(members, list) or not members or symbol not in members:
            raise LineageError(f"{symbol}: envelope recovery requires completed group membership")
        group = set(members)
        if not group & seen:
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
            old = recall(store, "receipt", (symbol, entry["sha256"]))
            held = old is not None
            if old is None:
                old = selected_document(
                    storage.path_at(root, f"{symbol}/lineage.manifest.json", published=True), entry["sha256"]
                )
                accept_observation_choices(old)
            previous[symbol] = receipts[symbol] = old

            missing = fields - old.keys()
            if not missing:
                if (
                    not held
                    and entry.get("projection", {}).get("source_ownership_envelopes_complete") is not True
                ):
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

"""Repair selected membership transfers from completed numerical checkpoints."""

from copy import deepcopy

from alpaca_end_of_day._history_coverage import coverage_sources
from libs.alpaca_serving_catalogue import storage as serving_storage

from . import storage
from .assignment_handoff import produce
from .checkpoint_choices import select as checkpoint_choices
from .continuation import validate_handoff
from .contract import LineageError
from .membership_transfer import build
from .recovery_handoffs import remember
from .source_observations import preserve_selection, selected_donor_scopes
from .source_work import assignment_documents


def recover(store, root, index, pending_serving, serving):
    """Select only compact pending groups advertising incomplete closed transfers."""
    entries = index["symbols"]
    groups, seen = [], set()
    for symbol in sorted(pending_serving.keys() & entries.keys()):
        if entries[symbol]["sha256"] != pending_serving[symbol]:
            raise LineageError(f"{symbol}: pending serving generation changed before transfer recovery")
        if symbol in seen:
            continue
        group = set(entries[symbol]["group_members"])
        seen.update(group)
        selected = group & entries.keys()
        if any(entries[name]["projection"].get("has_membership_rebind") for name in selected) and any(
            not entries[name]["projection"].get("membership_transfers_complete") for name in selected
        ):
            groups.append(group)
    repaired = []
    for group in groups:
        repaired.extend(_recover_group(store, root, index, group, serving))
    return sorted(repaired)


def _recover_group(store, root, index, group, serving):
    selected = group & index["symbols"].keys()
    keys = {serving["members"][name] for name in group if name in serving["members"]}
    prior_groups = serving_storage.read_groups(store, keys, generation=serving["state"]["generation"])
    for key, prior in prior_groups.items():
        remember(store, "serving_group", (serving["state"]["generation"], key), prior)
    with storage.pricing_locks(store, group, known=index["symbols"]):
        with storage.publication_lock(root):
            storage.assert_index_current(root, index)
        previous = {name: storage.read_receipt(root, name, index) for name in sorted(selected)}
        for name, receipt in previous.items():
            remember(store, "receipt", (name, index["symbols"][name]["sha256"]), receipt)
        changes = {}
        for receipt in previous.values():
            for old, identity in (receipt.get("membership_only_rebind") or {}).get("identities", {}).items():
                if old in changes and changes[old] != identity:
                    raise LineageError("membership recovery has conflicting accepted identity transfers")
                changes[old] = identity
        if not changes:
            return []
        receipts, assignments, expected = deepcopy(previous), {}, {}
        changed_documents, selection_inputs = {name: {} for name in receipts}, {}
        priors, metadata, choices = {}, {}, {}
        for name in receipts:
            matches = [prior for prior in prior_groups.values() if name in prior["history_validation"]["inputs"]]
            if len(matches) != 1:
                raise LineageError(f"{name}: membership recovery lacks one selected numerical checkpoint")
            priors[name] = matches[0]
            choices[name] = checkpoint_choices(matches[0], name)

        def documents_for(name):
            if name not in metadata:
                metadata[name] = assignment_documents(root, previous[name], assignments)
                for kind, value in metadata[name].items():
                    remember(
                        store, "assignment", (name, kind, previous[name]["boundaries"][kind]["sha256"]), value
                    )
            return metadata[name]

        donor_scopes = []
        for name, receipt in receipts.items():
            old_input = priors[name]["history_validation"]["inputs"][name]
            donor_scopes.extend(
                selected_donor_scopes(
                    name,
                    old_serving_ownership={"views": old_input["views"]},
                    accepted_identity_changes=changes,
                    current_receipt=receipt,
                    current_documents=lambda name=name: documents_for(name),
                    accepted_checkpoint=priors[name],
                    accepted_choices=choices[name],
                )
            )
        for prior in prior_groups.values():
            checkpoint = prior["history_validation"]
            for old in changes.keys() & checkpoint["identities"].keys():
                expected.setdefault(old, set()).update(
                    source
                    for coverage in checkpoint["identities"][old].values()
                    for source in coverage_sources(coverage)
                )
        for name, receipt in receipts.items():
            prior = priors[name]
            checkpoint = prior["history_validation"]
            old_input = checkpoint["inputs"][name]

            def documents():
                return documents_for(name)

            preserved = preserve_selection(
                name,
                old_serving_ownership={"views": old_input["views"]},
                accepted_identity_changes=changes,
                current_receipt=receipt,
                current_documents=documents,
                preserves_pricing=old_input["pricing_generation"] == receipt["source_sha256"],
                accepted_donor_scopes=donor_scopes,
                accepted_choices=choices[name],
            )
            if preserved:
                from .contract import document_digest

                originals = documents()
                selection_inputs[name] = {kind: originals[kind] for kind in preserved}
                for kind, document in preserved.items():
                    generation = document_digest(document)
                    changed_documents[name][kind] = document
                    receipt["boundaries"][kind] = {**receipt["boundaries"][kind], "sha256": generation}
                    receipt["assignment_handoffs"][kind] = produce(document, generation)

            def selected_documents():
                return {**documents(), **changed_documents[name]}

            transfer = build(
                name,
                old_identity_memberships=[],
                accepted_identity_changes=changes,
                old_serving_ownership={"views": old_input["views"]},
                old_receipt_generation=prior["references"]["lineage"][name],
                old_pricing_generation=old_input["pricing_generation"],
                old_boundaries=None,
                current_receipt=receipt,
                current_documents=selected_documents,
                preserves_pricing=old_input["pricing_generation"] == receipt["source_sha256"],
            )
            if transfer is not None:
                transfer = {k: v for k, v in transfer.items() if k != "previous_boundaries"}
                transfer.update(
                    version=2,
                    previous_selection={
                        "kind": "accepted_serving_checkpoint",
                        "generation": prior["decision_generation"],
                    },
                )
            receipt["membership_only_rebind"] = transfer
            receipt["membership_transfer_handoff_version"] = 1
            validate_handoff(receipt)
        actual = {old: set() for old in expected}
        for name, receipt in receipts.items():
            for old in (receipt.get("membership_only_rebind") or {}).get("identities", {}):
                actual.setdefault(old, set()).add(name)
        if actual != expected:
            raise LineageError(
                f"membership recovery lacks applicable retained source transfers: expected={expected}, actual={actual}"
            )
        storage.publish(
            store,
            root,
            index,
            receipts,
            changed_documents,
            {},
            previous=previous,
            membership_transfer_recovery=True,
            selection_inputs=selection_inputs,
        )
        for name, receipt in receipts.items():
            remember(store, "receipt", (name, index["symbols"][name]["sha256"]), receipt)
            for kind, document in changed_documents[name].items():
                remember(store, "assignment", (name, kind, receipt["boundaries"][kind]["sha256"]), document)
    return sorted(selected)

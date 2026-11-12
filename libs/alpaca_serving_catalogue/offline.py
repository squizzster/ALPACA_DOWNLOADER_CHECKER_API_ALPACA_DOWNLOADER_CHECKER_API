"""Completed, generation-bound offline ownership decisions and retained checkpoints."""

from .contract import CatalogueError
from .ownership_loss import AcceptedOwnershipLost, retained_owner_losses

VERSION = 1
REASON = "accepted_ownership_recovery_required"


def previous_checkpoints(previous):
    """Borrow accepted checkpoints; never reopen or authenticate their sources."""
    selected = {}
    for group in previous:
        for checkpoint in group.get("offline_ownership", {}).get("checkpoints", ()):
            selected[checkpoint["decision_generation"]] = checkpoint
        selected[group["decision_generation"]] = group
    return tuple(selected.values())


def select_offline(group, previous, inputs, offsets, *, securities=None):
    losses = retained_owner_losses(group, previous, inputs, offsets, securities=securities)
    if not losses:
        return set(), None
    memberships = {}
    for assignments in group.assignments.values():
        for _, _, segment in assignments:
            identity = segment.get("security_identity")
            if identity:
                memberships[identity["security_id"]] = set(identity["member_alpaca_asset_ids"])
    affected = {item["public_id"] for item in losses}
    # A conflicting singleton/union representation of the same UUID is part of
    # this uncertainty. Independent rights/units keep their accepted identity.
    by_uuid = {}
    for pid, members in memberships.items():
        for uuid in members:
            by_uuid.setdefault(uuid, set()).add(pid)
    pending = list(affected)
    while pending:
        for uuid in memberships.get(pending.pop(), ()):
            for pid in by_uuid.pop(uuid, ()):
                if pid not in affected:
                    affected.add(pid)
                    pending.append(pid)
    checkpoints = []
    for prior in previous:
        history = prior["history_validation"]
        records = [item for item in history["prefix_records"] if item["public_id"] in affected]
        if not records:
            continue
        ids = {item["public_id"] for item in records}
        checkpoints.append(
            {
                **{k: prior[k] for k in ("locator", "policy", "references", "decision_generation")},
                "securities": [item for item in prior["securities"] if item["public_id"] in ids],
                "decisions": [item for item in prior["decisions"] if item["public_id"] in ids],
                "history_validation": {
                    **history,
                    "prefix_records": records,
                    "identities": {pid: history["identities"][pid] for pid in ids},
                },
            }
        )
    return affected, {
        "version": VERSION,
        "public_ids": sorted(affected),
        "reason": REASON,
        "outcome": AcceptedOwnershipLost(losses).outcome,
        "checkpoints": checkpoints,
    }


def available_checkpoints(previous, excluded):
    """Do not select numerical prefixes for an already selected offline result."""
    if not excluded:
        return previous
    return tuple(
        {
            **prior,
            "history_validation": {
                **prior["history_validation"],
                "prefix_records": [
                    item
                    for item in prior["history_validation"]["prefix_records"]
                    if item["public_id"] not in excluded
                ],
            },
        }
        for prior in previous
    )


def validate(group):
    envelope = group.get("offline_ownership")
    if envelope is None:
        return
    decisions = {item["public_id"]: item for item in group["decisions"]}
    if (
        envelope.get("version") != VERSION
        or envelope.get("reason") != REASON
        or not envelope.get("public_ids")
        or not envelope.get("checkpoints")
        or any(
            pid not in decisions or decisions[pid]["eligible"] or REASON not in decisions[pid]["reasons"]
            for pid in envelope["public_ids"]
        )
    ):
        raise CatalogueError("invalid new offline ownership envelope")
    # Frozen checkpoints are accepted producer results. Their envelope is new;
    # their numerical, calendar and historical ownership proofs stay closed.


def projection(group):
    envelope = group.get("offline_ownership")
    return (
        None
        if envelope is None
        else {
            "version": VERSION,
            "public_ids": envelope["public_ids"],
            "reason": envelope["reason"],
        }
    )

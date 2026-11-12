"""Explicit recovery of selected current-revision decisions from accepted inputs."""

import json
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

from libs.common.debug_trace import event
from libs.common.work_scope import selected_work

from . import storage, universe_state
from .context import accept_context
from .contract import MAX_CLOSURE, LineageError, document_digest
from .correlation_policy_contract import valid_sha256
from .projection import require_complete
from .rebuild import RetainedOnlyClient
from .recovery_handoffs import operation

RECOVERY_VERSION = 1
RECOVERY_BOUNDARY = "alpaca_retained_lineage_recovery"


@dataclass(frozen=True)
class RecoveryGroup:
    members: tuple
    pricing: dict
    receipts: dict
    operation_id: str

    @property
    def seed(self):
        return min(self.receipts)

    def completion(self):
        return {"version": RECOVERY_VERSION, "operation_id": self.operation_id}

    def select(self, index):
        """The compact selector binds evidence/mappings through receipt generations."""
        completed = []
        for name in self.members:
            entry = index["symbols"].get(name)
            if name not in self.receipts:
                if entry is not None:
                    raise LineageError(f"{name}: recovery group acquired a published member")
                continue
            if (
                entry is None
                or tuple(entry.get("group_members", ())) != self.members
                or entry["source_sha256"] != self.pricing[name]
            ):
                raise LineageError(f"{name}: recovery membership or pricing generation changed")
            marker = entry.get("projection", {}).get("retained_recovery")
            done = marker == self.completion()
            if not done and entry["sha256"] != self.receipts[name]:
                raise LineageError(f"{name}: recovery receipt generation changed")
            completed.append(done)
        if any(completed) and not all(completed):
            raise LineageError("incomplete recovery completion handoff; finish owning journal recovery")
        return all(completed)

    def accept_sources(self, members, selected):
        # Called once with the pricing owner's selection under its group locks.
        # No payload, validator, comparison or evidence authentication belongs here.
        if tuple(sorted(members)) != self.members or selected != self.pricing:
            raise LineageError(f"{self.seed}: locked recovery inputs changed")


@dataclass(frozen=True)
class RecoverySelection:
    store: Path
    policy_sha256: str
    groups: tuple
    prepared: object = None

    @property
    def symbols(self):
        return {name for group in self.groups for name in group.members}

    def select(self, index):
        require_complete(index)
        pending, completed = [], []
        for group in self.groups:
            (completed if group.select(index) else pending).append(group)
        return pending, completed


def load_selection(path, store):
    """Accept one explicit versioned scope; saved comparisons need no replay."""
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(value, dict) and value.get("boundary") == "alpaca_retained_candidate_handoff":
        from .prepared_recovery import accept_candidate

        return accept_candidate(value, store)
    return accept_selection(value, store)


def accept_selection(value, store):
    """Accept the compact scope independently of its file transport."""
    store = Path(store).absolute()
    if (
        not isinstance(value, dict)
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != RECOVERY_VERSION
        or value.get("boundary") != RECOVERY_BOUNDARY
        or value.get("store") != str(store)
        or not valid_sha256(value.get("correlation_policy_sha256"))
        or not isinstance(value.get("groups"), list)
        or not value["groups"]
    ):
        raise LineageError("invalid retained recovery selection or store binding")
    groups, seen = [], set()
    from libs.alpaca_pricing_download.contract import symbol_label

    for item in value["groups"]:
        if not isinstance(item, dict):
            raise LineageError("invalid retained recovery group")
        members = item.get("members")
        pricing, receipts = item.get("pricing_generations"), item.get("receipt_generations")
        if (
            not isinstance(members, list)
            or not members
            or len(members) > MAX_CLOSURE
            or any(not isinstance(name, str) or symbol_label(name) != name for name in members)
            or members != sorted(set(members))
            or seen.intersection(members)
            or not isinstance(pricing, dict)
            or not pricing
            or not isinstance(receipts, dict)
            or pricing.keys() != receipts.keys()
            or not pricing.keys() <= set(members)
            or not all(valid_sha256(v) for v in (*pricing.values(), *receipts.values()))
        ):
            raise LineageError("invalid or overlapping retained recovery group")
        seen.update(members)
        operation_id = document_digest(
            {
                "version": RECOVERY_VERSION,
                "store": str(store),
                "policy": value["correlation_policy_sha256"],
                "members": members,
                "pricing": pricing,
                "receipts": receipts,
            }
        )
        groups.append(RecoveryGroup(tuple(members), pricing, receipts, operation_id))
    return RecoverySelection(store, value["correlation_policy_sha256"], tuple(groups))


def recover(store, selection_path, *, plan=False, progress=None, selection=None):
    """Finish selected decisions, consuming prepared producer output when supplied."""
    store = Path(store).absolute()
    selection = load_selection(selection_path, store) if selection is None else selection
    if selection.store != store:
        raise LineageError("retained recovery selection belongs to another store")
    context = accept_context(store)
    universe_state.require_initialized(store)
    scope = {
        "selected_groups": len(selection.groups),
        "selected_symbols": len(selection.symbols),
        "provider_requests": 0,
        "pricing_writes": 0,
    }
    event("retained_recovery_scope", **scope)
    with (
        nullcontext() if plan else universe_state.update_lock(store),
        selected_work(store, selection.symbols, phase="retained_lineage_recovery"),
        operation(store),
    ):
        if plan:
            if context.accept_correlation_policy().semantic_sha256 != selection.policy_sha256:
                raise LineageError("retained recovery policy changed from accepted comparison")
            root = context.accept_store().root
            with storage.publication_lock(root):
                if storage.path_at(root, ".pending.json").exists():
                    raise LineageError("lineage publication pending; finish owning recovery")
                index = context.select_index(root)
            pending, completed = selection.select(index)
            return {
                "mode": "plan",
                **scope,
                "pending_groups": len(pending),
                "completed_groups": len(completed),
                "symbols": sorted(selection.symbols),
            }
        if selection.prepared is not None:
            from .prepared_recovery import publish_prepared

            result = publish_prepared(selection, context, progress=progress)
        else:
            from .pipeline import run_lineage

            result = run_lineage(
                store,
                [group.seed for group in selection.groups],
                identity_client=RetainedOnlyClient(),
                context=context,
                retained_rebuild=True,
                retained_recovery=selection,
                progress=progress,
            )
        result.update(scope)
        result["complete"] = not result["failed"]
        if result["complete"]:
            from libs.alpaca_serving_catalogue.producer import maintain

            serving = maintain(store, symbols=selection.symbols, context=context, progress=progress)
            result["serving_catalogue"] = serving
            result["failed"] += serving["failed"]
            result["complete"] = serving["complete"]
        return result

"""Explicit transient acceptance state for Prepare's next owner.

The shared codec preserves references between records, scopes and selected
objects. Import consumes those acceptances; it does not run their constructors,
validators, dependency selection or storage ingress again. Current-generation
applicability is the caller's responsibility after acquiring the owning locks.
"""

from pathlib import Path

from libs.alpaca_lineage_build import recovery_handoffs
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.selection import DependencyGraph
from libs.alpaca_lineage_build.storage import AcceptedStore
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download.context import PricingContext

CONTEXT_FIELDS = (
    "_accepted_store",
    "_ruleset",
    "_correlation_policy",
    "_authorized_policy",
    "_lineage_recovery_complete",
    "index",
    "summary",
    "serving",
    "history_comparisons",
    "history_checks",
    "_history_scopes",
    "completed_membership_plans",
    "group_completions",
)
PRICING_FIELDS = (
    "endpoint_selections",
    "full_publications",
    "approved_appends",
    "correction_acquisitions",
    "suffix_acquisitions",
    "late_bar_observations",
    "late_bar_requirements",
)
POLICY_FIELDS = (
    "end",
    "index",
    "work",
    "required_groups",
    "blocked",
    "failures",
    "command_failures",
    "history_approved",
    "completed_decisions",
    "completed_corrections",
    "restricted_identifiers",
    "current_group",
    "rule_applications",
)
GRAPH_FIELDS = ("dependencies", "assets", "dependants", "members")


def _require_fields(document, fields, label):
    if not isinstance(document, dict) or set(document) != set(fields):
        raise LineageError(f"incomplete {label} handoff")


def export_state(context, policy, encode, *, evidence=None):
    """Carry only enumerated accepted facts, retaining their shared references.

    Sourced calculations belong to the evidence client used for this operation.
    The transport names that client explicitly rather than serializing a Python
    client as a dictionary key. Unknown peer clients are a contract defect.
    """
    sourced = context.sourced_operations
    if sourced and (evidence is None or set(sourced) != {evidence}):
        raise LineageError("sourced-operation handoff has an unbound evidence owner")
    if sourced:
        from .source_state import export_sourced

        recovery = recovery_handoffs._selected.get()
        # The selected producer caches retain their actual metadata and control
        # objects. Universe selectors/catalogue rows are not identity anchors.
        roots = []
        if recovery is not None and recovery[0] == context.store:
            roots.extend(body for (kind, _), body in recovery[1].items() if kind == "receipt")
        packed_sourced = export_sourced(sourced[evidence], roots=roots)
    else:
        packed_sourced = None
    pricing = context._pricing
    return {
        "store": str(context.store),
        "context": {name: encode(getattr(context, name)) for name in CONTEXT_FIELDS},
        "pricing": None
        if pricing is None
        else {name: encode(getattr(pricing, name)) for name in PRICING_FIELDS},
        "policy": None if policy is None else {name: encode(getattr(policy, name)) for name in POLICY_FIELDS},
        "graph": None
        if policy is None
        else {name: encode(getattr(policy.graph, name)) for name in GRAPH_FIELDS},
        "pending_work": {
            "present": policy is not None and hasattr(policy, "pending_work"),
            "value": encode(policy.pending_work)
            if policy is not None and hasattr(policy, "pending_work")
            else None,
        },
        "sourced_operations": {
            "present": bool(sourced),
            "value": encode(packed_sourced) if sourced else None,
        },
    }


def import_state(store, document, decode, *, evidence=None):
    """Restore accepted records without producer work or graph reconstruction."""
    _require_fields(
        document,
        ("store", "context", "pricing", "policy", "graph", "pending_work", "sourced_operations"),
        "operation state",
    )
    store = Path(store).absolute()
    if document["store"] != str(store):
        raise LineageError("operation state handoff belongs to another store")
    _require_fields(document["context"], CONTEXT_FIELDS, "accepted context")
    context = object.__new__(OperationContext)
    context.store = store
    for name in CONTEXT_FIELDS:
        setattr(context, name, decode(document["context"][name]))
    pricing = document["pricing"]
    context._pricing = None
    if pricing is not None:
        _require_fields(pricing, PRICING_FIELDS, "pricing acceptance")
        context._pricing = PricingContext(store, **{name: decode(pricing[name]) for name in PRICING_FIELDS})
    sourced = document["sourced_operations"]
    _require_fields(sourced, ("present", "value"), "sourced operations")
    if type(sourced["present"]) is not bool:
        raise LineageError("invalid sourced-operation presence")
    if sourced["present"] and evidence is None:
        raise LineageError("sourced-operation handoff requires its evidence owner")
    context.sourced_operations = {evidence: decode(sourced["value"])} if sourced["present"] else {}
    policy_document = document["policy"]
    policy = None
    if policy_document is not None:
        _require_fields(policy_document, POLICY_FIELDS, "work policy")
        _require_fields(document["graph"], GRAPH_FIELDS, "selected dependency graph")
        policy = object.__new__(WorkPolicy)
        policy.store, policy.context = store, context
        for name in POLICY_FIELDS:
            setattr(policy, name, decode(policy_document[name]))
        policy.graph = object.__new__(DependencyGraph)
        for name in GRAPH_FIELDS:
            setattr(policy.graph, name, decode(document["graph"][name]))
    elif document["graph"] is not None:
        raise LineageError("selected graph lacks a work policy")
    pending = document["pending_work"]
    _require_fields(pending, ("present", "value"), "pending work")
    if type(pending["present"]) is not bool or pending["present"] and policy is None:
        raise LineageError("invalid pending-work presence")
    if pending["present"]:
        policy.pending_work = decode(pending["value"])
    return context, policy


def export_recovery(store, encode):
    """Export the active owner's held values without rereading their artifacts."""
    selected = recovery_handoffs._selected.get()
    if selected is None or selected[0] != Path(store).absolute():
        raise LineageError("recovery handoff requires the active owning operation")
    return {"store": str(selected[0]), "values": encode(selected[1])}


def restore_recovery(store, document, decode):
    """Install carried values into an empty operation; never merge stale state."""
    _require_fields(document, ("store", "values"), "recovery")
    store = Path(store).absolute()
    selected = recovery_handoffs._selected.get()
    if document["store"] != str(store) or selected is None or selected[0] != store:
        raise LineageError("recovery handoff belongs to another store or inactive operation")
    if selected[1]:
        raise LineageError("recovery handoff requires an empty consuming operation")
    values = decode(document["values"])
    if not isinstance(values, dict):
        raise LineageError("invalid recovery handoff values")
    selected[1].update(values)


def export_serving_context(context, encode):
    """Carry only the completed facts whose remaining consumer is serving.

    Pricing comparisons, acquisition plans, provider evidence and source-work
    caches have finished their job. Receipts, assignments and decoded partitions
    travel separately through the recovery value handoff, under their exact keys.
    """
    return {name: encode(getattr(context, name)) for name in ("_accepted_store", "index", "serving")}


def import_serving_context(store, document, decode):
    _require_fields(document, ("_accepted_store", "index", "serving"), "serving context")
    context = object.__new__(OperationContext)
    context.store = Path(store).absolute()
    for name in CONTEXT_FIELDS:
        setattr(context, name, None)
    context._lineage_recovery_complete = True
    context._pricing = None
    context.sourced_operations = {}
    context.history_comparisons, context.history_checks, context._history_scopes = {}, {}, {}
    for name, value in document.items():
        setattr(context, name, decode(value))
    if (
        not isinstance(context._accepted_store, AcceptedStore)
        or context._accepted_store.store != context.store
        or context._accepted_store.root != context.store / "end_of_day/lineage"
        or not isinstance(context.index, dict)
        or not isinstance(context.serving, dict)
    ):
        raise LineageError("incomplete serving baseline acceptance")
    return context

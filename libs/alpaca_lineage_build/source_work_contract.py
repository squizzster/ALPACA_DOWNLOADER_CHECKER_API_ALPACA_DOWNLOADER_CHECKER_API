"""Source-work value contracts and accepted component/binding projections.

Shared by planners and transport without importing the source-work executor.
Accepted producer bindings are consumed and memoized, never re-established here.
"""

from dataclasses import dataclass
from dataclasses import field as dataclass_field

from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets

from .contract import reusable_decision
from .resume import continuing_request, unchanged_rows


@dataclass(frozen=True)
class SourceWorkPlan:
    dispositions: dict
    memberships: dict
    rename_membership: dict
    common_membership: dict
    relation_certificate: dict
    relation_projection: dict
    common_bounds: dict
    projection_required: bool
    market_projection: bool = False
    market_binding: object = None
    market_labels_complete: bool = False
    recapitalization_graphs: object = None
    documented_membership: object = None
    refine_accepted_dates: bool = False
    refinement_fact_ids: frozenset | None = None
    refinement_policy_version: int = 90
    prepared_reports: dict | None = None
    closed_report_projections: dict = dataclass_field(default_factory=dict, repr=False, compare=False)
    stable_numerical_identities: frozenset = dataclass_field(default_factory=frozenset)
    membership_refinement: dict | None = None


def binding_applies(control):
    if control.accepted_reuse is not None:
        return control.accepted_reuse[0]
    if hasattr(control, "_accepted_binding_applies"):
        return control._accepted_binding_applies
    prior = control.previous
    control._accepted_binding_applies = bool(
        prior
        and reusable_decision(prior)
        and unchanged_rows(control.source, prior, selected=control.source_sha256)
        and continuing_request(control.source, prior)
    )
    return control._accepted_binding_applies


def compose_memberships(prior_members, edges):
    """Join complete accepted components, retaining all member and label facts."""
    edges = tuple(edges)
    if not edges:
        return dict(prior_members)
    graph, labels, components, known, descriptions = {}, {}, {}, {}, set()
    for identity in prior_members.values():
        if id(identity) in descriptions:
            continue
        descriptions.add(id(identity))
        public_id = identity["security_id"]
        if public_id not in components:
            members = components[public_id] = frozenset(identity["member_alpaca_asset_ids"])
            known.setdefault(members, identity)
            # A completed component is undirected even when a shared UUID's
            # local dictionary entry names another accepted source scope.
            for identifier in members:
                graph.setdefault(identifier, set()).update(members)
        for identifier in components[public_id]:
            labels.setdefault(identifier, set()).update(identity["member_tickers"])
    touched = set()
    for edge in edges:
        left, right = edge.predecessor_asset_id, edge.successor_asset_id
        graph.setdefault(left, set()).add(right)
        graph.setdefault(right, set()).add(left)
        labels.setdefault(left, set()).add(edge.predecessor_symbol)
        labels.setdefault(right, set()).add(edge.successor_symbol)
        touched.update((left, right))
    memberships = dict(prior_members)
    while touched:
        pending, members, tickers = [min(touched)], set(), set()
        while pending:
            identifier = pending.pop()
            if identifier in members:
                continue
            members.add(identifier)
            tickers.update(labels.get(identifier, ()))
            pending.extend(graph.get(identifier, ()) - members)
        touched.difference_update(members)
        old = known.get(frozenset(members))
        if old is not None:
            completed = (
                old if set(old["member_tickers"]) == tickers else {**old, "member_tickers": sorted(tickers)}
            )
        else:
            completed = security_identity_for_alpaca_assets(
                members, [], tickers=tickers, basis="sourced_public_common_or_unchanged_security"
            ).as_report_object()
        for identifier in members:
            memberships[identifier] = completed
    return memberships

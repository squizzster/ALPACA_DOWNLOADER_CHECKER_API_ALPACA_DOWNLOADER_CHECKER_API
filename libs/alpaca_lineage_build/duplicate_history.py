"""Resolve an explicitly selected overlap between published histories.

A dated corporate action supplies the owner relationship. A complete matching
traded raw prefix identifies which of the already-published observations is the
duplicate. Numerical output consists of slices of previously accepted histories.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from itertools import combinations

from libs.alpaca_end_of_day_security_correlation.action_identity import exact_cusip_lookup_asset
from libs.alpaca_security_lineage.inference_evidence import provider_actions, reject_assertions


@dataclass(frozen=True)
class AcceptedHistory:
    public_id: str
    members: frozenset
    labels: dict
    raw: dict


@dataclass(frozen=True)
class HistorySlice:
    source_id: str
    first: date
    last: date


@dataclass(frozen=True)
class Reconciliation:
    sources: tuple
    selection: tuple
    supporting_action_ids: tuple
    reason: str


@dataclass(frozen=True)
class Split:
    day: date
    old_label: str
    new_label: str
    predecessor: str
    successor: str


def _action(action):
    kind = action.get("action_type")
    if kind not in ("forward_split", "reverse_split"):
        return None
    state = str(action.get("status", "")).strip().lower()
    if state in {"canceled", "cancelled", "withdrawn", "postponed", "pending"}:
        return None
    old_label = action.get("symbol")
    fields = (
        action.get("ex_date"),
        old_label,
        action.get("new_symbol", old_label),
        action.get("old_cusip"),
        action.get("new_cusip"),
    )
    if any(not isinstance(value, str) or not value for value in fields) or fields[-2] == fields[-1]:
        return None
    try:
        day = date.fromisoformat(fields[0])
        old_rate, new_rate = (Decimal(str(action[key])) for key in ("old_rate", "new_rate"))
        if (
            not old_rate.is_finite()
            or not new_rate.is_finite()
            or min(old_rate, new_rate) <= 0
            or old_rate == new_rate
            or (kind == "forward_split") != (new_rate > old_rate)
        ):
            return None
    except (ValueError, KeyError, InvalidOperation):
        return None
    return (day, *fields[1:], old_rate, new_rate)


def _resolved_splits(evidence):
    """Current label spelling alone cannot supply either CUSIP endpoint."""
    reject_assertions(evidence, where="provider split ownership observations")
    actions = defaultdict(set)
    for facts in evidence:
        for action in provider_actions(facts):
            identifier = action.get("id")
            if identifier and action.get("action_type") in {"forward_split", "reverse_split"}:
                actions[identifier].add(_action(action))
    unique = {key: next(iter(items)) for key, items in actions.items() if len(items) == 1 and None not in items}
    endpoints = {cusip for fields in unique.values() for cusip in fields[3:5]}
    assets = defaultdict(set)
    for facts in evidence:
        for cusip in endpoints:
            asset = exact_cusip_lookup_asset(facts, cusip)
            if asset:
                assets[cusip].add((asset.get("symbol"), asset["id"]))
    result = {}
    for key, (day, old_label, new_label, old_cusip, new_cusip, _, _) in unique.items():
        left, right = assets[old_cusip], assets[new_cusip]
        if len(left) != 1 or len(right) != 1:
            continue
        ((predecessor_label, predecessor),) = left
        ((successor_label, successor),) = right
        if predecessor_label == old_label and successor_label == new_label and predecessor != successor:
            result[key] = Split(day, old_label, new_label, predecessor, successor)
    return result


def _unique_edge(ident, edges):
    edge = edges[ident]
    return not any(
        other_id != ident and (other.predecessor == edge.predecessor or other.successor == edge.successor)
        for other_id, other in edges.items()
    )


def _nested_route(smaller, larger, edges, last):
    reached = set(smaller.members)
    proof = []
    for ident, edge in sorted(edges.items(), key=lambda item: item[1].day):
        if edge.predecessor not in reached or edge.successor in reached:
            continue
        if (
            edge.successor not in larger.members
            or edge.old_label != edge.new_label
            or edge.day <= last
            or not _unique_edge(ident, edges)
        ):
            return None
        reached.add(edge.successor)
        proof.append(ident)
    return tuple(proof) if reached == larger.members and proof else None


def _pair(left, right, edges):
    if not left.raw or not right.raw or left.public_id == right.public_id:
        return None
    if max(left.raw) > max(right.raw):
        left, right = right, left
    first, last = min(left.raw), max(left.raw)
    if first != min(right.raw) or last >= max(right.raw):
        return None
    if left.raw.keys() != {day for day in right.raw if day <= last}:
        return None
    if left.labels.keys() != left.raw.keys() or right.labels.keys() != right.raw.keys():
        return None
    if any(bar != right.raw[day] for day, bar in left.raw.items()):
        return None
    if len({bar.close for bar in left.raw.values() if bar.volume > 0}) < 2:
        return None
    ids = left.public_id, right.public_id
    if left.members < right.members:
        if any(labels != right.labels[day] for day, labels in left.labels.items()):
            return None
        proof = _nested_route(left, right, edges, last)
        if proof:
            return Reconciliation(
                ids,
                (HistorySlice(right.public_id, first, max(right.raw)),),
                proof,
                "A later split extended the same dated owner and the accepted successor covers every earlier session.",
            )
    if not left.members.isdisjoint(right.members):
        return None
    start = min(day for day in right.raw if day > last)
    matches = [
        ident
        for ident, edge in edges.items()
        if edge.day == start
        and edge.old_label != edge.new_label
        and left.labels[last] == (edge.old_label,)
        and right.labels[start] == (edge.new_label,)
        and edge.predecessor in left.members
        and edge.successor in right.members
    ]
    if len(matches) != 1 or not _unique_edge(matches[0], edges):
        return None
    return Reconciliation(
        ids,
        (HistorySlice(left.public_id, first, last), HistorySlice(right.public_id, start, max(right.raw))),
        tuple(matches),
        "A dated split joins two exact CUSIP endpoints and the incoming raw prefix duplicates the predecessor.",
    )


def select_group(histories, evidence):
    """A third plausible match leaves the competing prior IDs unchanged."""
    edges = _resolved_splits(tuple(evidence))
    if not edges:
        return ()
    possible = [
        choice for left, right in combinations(histories, 2) if (choice := _pair(left, right, edges)) is not None
    ]
    occurrences = Counter(ident for choice in possible for ident in choice.sources)
    return tuple(choice for choice in possible if all(occurrences[ident] == 1 for ident in choice.sources))


def retain_selected_fragments(previous, receipt, completed):
    from .contract import LineageError

    inherited = {}
    for old, fragment in previous.get("dated_price_intervals", {}).items():
        if receipt["source_sha256"] != previous["source_sha256"]:
            raise LineageError("dated selection requires its original pricing generation")
        if old not in completed["sources"]:
            # Ownership and observation positions outside this producer's scope were copied verbatim.
            inherited[old] = fragment
            continue
        covering = [
            item
            for item in completed["selection"]
            if item["source_id"] == old
            and item["first"] <= fragment["first"]
            and fragment["witness_end"] <= item["last"]
        ]
        if len(covering) != 1:
            raise LineageError("dated price selection crosses a changed source scope")
        inherited[old] = fragment
    if inherited:
        receipt["dated_price_intervals"] = inherited

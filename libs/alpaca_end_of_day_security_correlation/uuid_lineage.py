from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from .action_identity import complete_stock_merger_endpoints, exact_cusip_lookup_asset

IDENTITY_PRESERVING_SPLIT_EDGE_TYPES = frozenset(("forward_split", "reverse_split"))


@dataclass(frozen=True)
class UuidLineageEdge:
    edge_type: str
    action_id: str
    effective_date: date
    predecessor_asset_id: str
    predecessor_symbol: str
    predecessor_cusip: str
    successor_asset_id: str
    successor_symbol: str
    successor_cusip: str

    def as_report_object(self) -> dict[str, Any]:
        return {
            "edge_type": self.edge_type,
            "corporate_action_id": self.action_id,
            "effective_date": self.effective_date.isoformat(),
            "predecessor": {
                "asset_id": self.predecessor_asset_id,
                "symbol": self.predecessor_symbol,
                "cusip": self.predecessor_cusip,
            },
            "successor": {
                "asset_id": self.successor_asset_id,
                "symbol": self.successor_symbol,
                "cusip": self.successor_cusip,
            },
        }


@dataclass(frozen=True)
class _UuidLineageEdgeCandidate:
    edge_type: str
    unresolved_reason_code: str
    action_id: str
    effective_date: str
    predecessor_symbol: str
    predecessor_cusip: str
    successor_symbol: str
    successor_cusip: str


def _asset_ids_by_symbol_and_cusip(lineage: dict[str, Any]) -> dict[tuple[str, str], set[str]]:
    asset_ids: dict[tuple[str, str], set[str]] = {}
    occupancy_evidence = lineage.get("symbol_occupancy_evidence")
    if not isinstance(occupancy_evidence, list):
        return asset_ids
    for occupancy in occupancy_evidence:
        if not isinstance(occupancy, dict):
            continue
        symbol = occupancy.get("symbol")
        cusip = occupancy.get("cusip")
        asset_id = occupancy.get("asset_id")
        if not all(isinstance(value, str) and value for value in (symbol, cusip, asset_id)):
            continue
        asset_ids.setdefault((symbol, cusip), set()).add(asset_id)
    return asset_ids


def _positive_rate(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        rate = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not rate.is_finite() or rate <= 0:
        return None
    return rate


def _identity_preserving_split_edge(
    action: dict[str, Any],
    lineage: dict[str, Any],
) -> UuidLineageEdge | None:
    action_type = action.get("action_type")
    if action_type not in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES:
        return None
    action_id = action.get("id")
    effective_date_value = action.get("ex_date") or action.get("effective_date")
    symbol = action.get("symbol")
    old_cusip = action.get("old_cusip")
    new_cusip = action.get("new_cusip")
    if not all(
        isinstance(value, str) and value
        for value in (action_id, effective_date_value, symbol, old_cusip, new_cusip)
    ):
        return None
    if old_cusip == new_cusip:
        return None
    if "new_symbol" in action and action.get("new_symbol") != symbol:
        return None

    old_rate = _positive_rate(action.get("old_rate"))
    new_rate = _positive_rate(action.get("new_rate"))
    if old_rate is None or new_rate is None:
        return None
    if action_type == "reverse_split" and old_rate <= new_rate:
        return None
    if action_type == "forward_split" and new_rate <= old_rate:
        return None
    try:
        effective_date = date.fromisoformat(effective_date_value)
    except ValueError:
        return None

    predecessor_asset = exact_cusip_lookup_asset(lineage, old_cusip)
    successor_asset = exact_cusip_lookup_asset(lineage, new_cusip)
    if predecessor_asset is None or successor_asset is None:
        return None
    if predecessor_asset.get("symbol") != symbol or successor_asset.get("symbol") != symbol:
        return None
    predecessor_asset_id = predecessor_asset["id"]
    successor_asset_id = successor_asset["id"]
    if predecessor_asset_id == successor_asset_id:
        return None
    return UuidLineageEdge(
        edge_type=action_type,
        action_id=action_id,
        effective_date=effective_date,
        predecessor_asset_id=predecessor_asset_id,
        predecessor_symbol=symbol,
        predecessor_cusip=old_cusip,
        successor_asset_id=successor_asset_id,
        successor_symbol=symbol,
        successor_cusip=new_cusip,
    )


def _name_change_edge_candidate(action: dict[str, Any]) -> _UuidLineageEdgeCandidate | None:
    if action.get("action_type") != "name_change":
        return None
    candidate = _UuidLineageEdgeCandidate(
        edge_type="name_change",
        unresolved_reason_code="unresolved_name_change_uuid_edge",
        action_id=action.get("id"),
        effective_date=action.get("process_date"),
        predecessor_symbol=action.get("old_symbol"),
        predecessor_cusip=action.get("old_cusip"),
        successor_symbol=action.get("new_symbol"),
        successor_cusip=action.get("new_cusip"),
    )
    if not all(isinstance(value, str) and value for value in candidate.__dict__.values()):
        return None
    return candidate


def _same_ticker_stock_merger_edge_candidate(
    action: dict[str, Any],
) -> _UuidLineageEdgeCandidate | None:
    if action.get("action_type") != "stock_merger":
        return None
    predecessor_symbol = action.get("acquiree_symbol")
    successor_symbol = action.get("acquirer_symbol")
    if (
        not isinstance(predecessor_symbol, str)
        or not predecessor_symbol
        or predecessor_symbol != successor_symbol
    ):
        return None
    candidate = _UuidLineageEdgeCandidate(
        edge_type="stock_merger",
        unresolved_reason_code="unresolved_same_ticker_stock_merger_uuid_edge",
        action_id=action.get("id"),
        effective_date=action.get("effective_date") or action.get("process_date"),
        predecessor_symbol=predecessor_symbol,
        predecessor_cusip=action.get("acquiree_cusip"),
        successor_symbol=successor_symbol,
        successor_cusip=action.get("acquirer_cusip"),
    )
    if not all(isinstance(value, str) and value for value in candidate.__dict__.values()):
        return None
    return candidate


def _immediately_superseded_name_change_action_ids(
    corporate_actions: list[Any],
    asset_ids_by_symbol_and_cusip: dict[tuple[str, str], set[str]],
) -> tuple[frozenset[str], list[dict[str, Any]]]:
    """Reject a different-UUID edge corrected by a next-day same-UUID rename."""
    candidates_by_predecessor: dict[
        tuple[str, str],
        list[_UuidLineageEdgeCandidate],
    ] = {}
    for action in corporate_actions:
        if not isinstance(action, dict):
            continue
        candidate = _name_change_edge_candidate(action)
        if candidate is None:
            continue
        predecessor = (candidate.predecessor_symbol, candidate.predecessor_cusip)
        candidates_by_predecessor.setdefault(predecessor, []).append(candidate)

    superseded_action_ids = set()
    evidence_issues = []
    for (predecessor_symbol, predecessor_cusip), candidates in sorted(candidates_by_predecessor.items()):
        predecessor_asset_ids = asset_ids_by_symbol_and_cusip.get(
            (predecessor_symbol, predecessor_cusip),
            set(),
        )
        if len(predecessor_asset_ids) != 1:
            continue
        predecessor_asset_id = next(iter(predecessor_asset_ids))
        resolved_successors = []
        for candidate in candidates:
            successor_asset_ids = asset_ids_by_symbol_and_cusip.get(
                (candidate.successor_symbol, candidate.successor_cusip),
                set(),
            )
            if len(successor_asset_ids) != 1:
                continue
            try:
                effective_date = date.fromisoformat(candidate.effective_date)
            except ValueError:
                continue
            resolved_successors.append((candidate, next(iter(successor_asset_ids)), effective_date))
        for earlier, earlier_asset_id, earlier_date in resolved_successors:
            if earlier_asset_id == predecessor_asset_id:
                continue
            superseding = [
                (later, later_asset_id, later_date)
                for later, later_asset_id, later_date in resolved_successors
                if later_asset_id == predecessor_asset_id and (later_date - earlier_date).days == 1
            ]
            if len(superseding) != 1:
                continue
            later, later_asset_id, _ = superseding[0]
            superseded_action_ids.add(earlier.action_id)
            evidence_issues.append(
                {
                    "reason_code": "name_change_uuid_edge_immediately_superseded",
                    "predecessor": {
                        "symbol": predecessor_symbol,
                        "cusip": predecessor_cusip,
                        "alpaca_asset_id": predecessor_asset_id,
                    },
                    "rejected_action_id": earlier.action_id,
                    "rejected_successor": {
                        "symbol": earlier.successor_symbol,
                        "cusip": earlier.successor_cusip,
                        "alpaca_asset_id": earlier_asset_id,
                    },
                    "superseding_action_id": later.action_id,
                    "superseding_successor": {
                        "symbol": later.successor_symbol,
                        "cusip": later.successor_cusip,
                        "alpaca_asset_id": later_asset_id,
                    },
                }
            )
    return frozenset(superseded_action_ids), evidence_issues


def build_uuid_lineage_edges(
    lineage: dict[str, Any],
) -> tuple[list[UuidLineageEdge], list[dict[str, Any]]]:
    corporate_actions = lineage.get("corporate_actions")
    if not isinstance(corporate_actions, list):
        return [], [{"reason_code": "malformed_corporate_actions_for_uuid_lineage"}]

    asset_ids_by_symbol_and_cusip = _asset_ids_by_symbol_and_cusip(lineage)
    edges = []
    superseded_name_change_action_ids, evidence_issues = _immediately_superseded_name_change_action_ids(
        corporate_actions,
        asset_ids_by_symbol_and_cusip,
    )
    for action in corporate_actions:
        if not isinstance(action, dict):
            continue
        split_edge = _identity_preserving_split_edge(action, lineage)
        if split_edge is not None:
            edges.append(split_edge)
            continue
        edge_candidate = _name_change_edge_candidate(action)
        completed = complete_stock_merger_endpoints(action, lineage)
        if edge_candidate is None:
            interpreted_action = (
                {**action, "acquiree_symbol": completed.symbol, "acquirer_symbol": completed.symbol}
                if completed is not None
                else action
            )
            edge_candidate = _same_ticker_stock_merger_edge_candidate(interpreted_action)
        if edge_candidate is None:
            continue
        if (
            edge_candidate.edge_type == "name_change"
            and edge_candidate.action_id in superseded_name_change_action_ids
        ):
            continue
        predecessor_asset_ids = asset_ids_by_symbol_and_cusip.get(
            (edge_candidate.predecessor_symbol, edge_candidate.predecessor_cusip),
            set(),
        )
        successor_asset_ids = asset_ids_by_symbol_and_cusip.get(
            (edge_candidate.successor_symbol, edge_candidate.successor_cusip),
            set(),
        )
        if completed is not None:
            predecessor_asset_ids = {completed.predecessor_asset_id}
            successor_asset_ids = {completed.successor_asset_id}
        if len(predecessor_asset_ids) != 1 or len(successor_asset_ids) != 1:
            evidence_issues.append(
                {
                    "reason_code": edge_candidate.unresolved_reason_code,
                    "corporate_action_id": edge_candidate.action_id,
                    "predecessor_asset_ids": sorted(predecessor_asset_ids),
                    "successor_asset_ids": sorted(successor_asset_ids),
                }
            )
            continue
        predecessor_asset_id = next(iter(predecessor_asset_ids))
        successor_asset_id = next(iter(successor_asset_ids))
        if edge_candidate.edge_type == "stock_merger" and predecessor_asset_id == successor_asset_id:
            evidence_issues.append(
                {
                    "severity": "warning",
                    "reason_code": "same_ticker_stock_merger_resolves_to_one_uuid",
                    "corporate_action_id": edge_candidate.action_id,
                    "asset_id": predecessor_asset_id,
                }
            )
            continue
        try:
            effective_date = date.fromisoformat(edge_candidate.effective_date)
        except ValueError:
            continue
        edges.append(
            UuidLineageEdge(
                edge_type=edge_candidate.edge_type,
                action_id=edge_candidate.action_id,
                effective_date=effective_date,
                predecessor_asset_id=predecessor_asset_id,
                predecessor_symbol=edge_candidate.predecessor_symbol,
                predecessor_cusip=edge_candidate.predecessor_cusip,
                successor_asset_id=successor_asset_id,
                successor_symbol=edge_candidate.successor_symbol,
                successor_cusip=edge_candidate.successor_cusip,
            )
        )
    edges.sort(key=lambda edge: (edge.effective_date, edge.edge_type, edge.action_id))
    return edges, evidence_issues


def predecessor_uuid_branch(
    selected_asset_id: str,
    edges: list[UuidLineageEdge],
) -> frozenset[str]:
    branch_asset_ids = {selected_asset_id}
    pending_asset_ids = [selected_asset_id]
    while pending_asset_ids:
        successor_asset_id = pending_asset_ids.pop()
        for edge in edges:
            if edge.successor_asset_id != successor_asset_id:
                continue
            if edge.predecessor_asset_id in branch_asset_ids:
                continue
            branch_asset_ids.add(edge.predecessor_asset_id)
            pending_asset_ids.append(edge.predecessor_asset_id)
    return frozenset(branch_asset_ids)

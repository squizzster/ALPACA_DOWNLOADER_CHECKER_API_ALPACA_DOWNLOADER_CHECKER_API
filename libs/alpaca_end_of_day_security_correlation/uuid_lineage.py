from dataclasses import dataclass
from datetime import date
from typing import Any


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


def build_uuid_lineage_edges(
    lineage: dict[str, Any],
) -> tuple[list[UuidLineageEdge], list[dict[str, Any]]]:
    corporate_actions = lineage.get("corporate_actions")
    if not isinstance(corporate_actions, list):
        return [], [{"reason_code": "malformed_corporate_actions_for_uuid_lineage"}]

    asset_ids_by_symbol_and_cusip = _asset_ids_by_symbol_and_cusip(lineage)
    edges = []
    evidence_issues = []
    for action in corporate_actions:
        if not isinstance(action, dict):
            continue
        edge_candidate = _name_change_edge_candidate(action)
        if edge_candidate is None:
            edge_candidate = _same_ticker_stock_merger_edge_candidate(action)
        if edge_candidate is None:
            continue
        predecessor_asset_ids = asset_ids_by_symbol_and_cusip.get(
            (edge_candidate.predecessor_symbol, edge_candidate.predecessor_cusip),
            set(),
        )
        successor_asset_ids = asset_ids_by_symbol_and_cusip.get(
            (edge_candidate.successor_symbol, edge_candidate.successor_cusip),
            set(),
        )
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

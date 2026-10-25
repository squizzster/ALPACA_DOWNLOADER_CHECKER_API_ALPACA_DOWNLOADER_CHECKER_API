"""Typed, evidence-referenced candidate projections for policy evaluation."""

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import (
    build_uuid_lineage_edges,
)
from libs.alpaca_security_lineage.lineage import (
    compact_asset,
    extract_cusips,
    symbol_cusip_evidence,
)
from libs.alpaca_security_lineage.symbol_disposition import classify_symbol_disposition

from .contract import CorrelationPolicyError

FACT_PRODUCER = "cross_receipt_cash_dividend_pair_v1"
FACT_TYPES = {
    "quality.cash_dividend_pair": "boolean",
    "quality.query_coverage_complete": "boolean",
    "quality.unique_reference_match": "boolean",
    "quality.subject_symbol_matches_input": "boolean",
    "quality.reference_symbol_matches_input": "boolean",
    "quality.distinct_action_ids": "boolean",
    "quality.distinct_action_symbols": "boolean",
    "quality.same_cusip": "boolean",
    "quality.nonidentity_payload_exact": "boolean",
    "quality.rate_precision_equivalent": "boolean",
    "quality.lookup_routes_consistent": "boolean",
    "quality.resolved_assets_distinct": "boolean",
    "quality.subject_history_brackets_event": "boolean",
    "quality.owner_history_brackets_event": "boolean",
    "quality.endpoint_lookup_unresolved": "boolean",
    "quality.selected_structural_link_present": "boolean",
    "quality.projection_closure_complete": "boolean",
}
EFFECT = {
    "type": "lab_quarantine",
    "target": "subject_action",
    "closure": "derived_evidence",
}
MAX_TRACE_EXAMPLES = 100
_OCCUPANT_WARNING = re.compile(r"The API returned evidence for [0-9]+ CUSIPs occupying .+\.")
_DISTRIBUTION_TYPES = frozenset(("cash_dividend", "capital_gains_distribution"))
_RATE_QUANTUM = Decimal("0.000001")
_RATE_TOLERANCE = Decimal("0.0000005")


@dataclass(frozen=True)
class EvidenceQualityRule:
    rule_id: str
    description: str
    scope: str
    conditions: tuple[tuple[str, bool], ...]
    effect: tuple[tuple[str, str], ...]

    def matches(self, facts: dict[str, bool | None]) -> bool:
        return all(facts.get(name) is expected for name, expected in self.conditions)

    def as_report_object(self) -> dict[str, Any]:
        return {
            "id": self.rule_id,
            "description": self.description,
            "scope": self.scope,
            "when": {"all": [{"fact": name, "eq": expected} for name, expected in self.conditions]},
            "effect": dict(self.effect),
        }


@dataclass(frozen=True)
class _ActionRecord:
    receipt_symbol: str
    action: dict[str, Any]


@dataclass(frozen=True)
class _ActionPairCandidate:
    subject: _ActionRecord
    reference: _ActionRecord
    unique_reference_match: bool
    equivalent_reference_ids: tuple[tuple[str, str], ...]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _reference(kind: str, receipt_symbol: str, generation: str, **values) -> dict[str, Any]:
    return {
        "kind": kind,
        "receipt_symbol": receipt_symbol,
        "evidence_sha256": generation,
        **values,
    }


def _valid_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _rate_equivalent(left: Any, right: Any) -> bool | None:
    if isinstance(left, bool) or isinstance(right, bool):
        return None
    try:
        left_decimal = Decimal(str(left))
        right_decimal = Decimal(str(right))
        if (
            not left_decimal.is_finite()
            or not right_decimal.is_finite()
            or left_decimal <= 0
            or right_decimal <= 0
        ):
            return None
        return (
            left_decimal.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_EVEN)
            == right_decimal.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_EVEN)
            and abs(left_decimal - right_decimal) <= _RATE_TOLERANCE
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


def _payload(action: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in action.items() if key not in {"id", "symbol", "rate"}}


def _valid_lookup(document: dict[str, Any], kind: str, value: str) -> list[dict[str, Any]]:
    lookups = document.get("asset_lookups")
    if not isinstance(lookups, list):
        return []
    return [
        lookup["asset"]
        for lookup in lookups
        if isinstance(lookup, dict)
        and lookup.get("lookup_kind") == kind
        and lookup.get("lookup") == value
        and lookup.get("found") is True
        and lookup.get("outcome") == "found"
        and lookup.get("http_status") == 200
        and isinstance(lookup.get("asset"), dict)
        and lookup["asset"].get("class") == "us_equity"
        and isinstance(lookup["asset"].get("id"), str)
        and bool(lookup["asset"]["id"])
    ]


def _one_asset(document: dict[str, Any], kind: str, value: str) -> dict[str, Any] | None:
    assets = _valid_lookup(document, kind, value)
    identities = {(asset["id"], asset.get("symbol")) for asset in assets}
    return assets[0] if len(assets) == 1 and len(identities) == 1 else None


def _query_covers(document: dict[str, Any], action: dict[str, Any]) -> bool | None:
    query = document.get("query")
    if not isinstance(query, dict):
        return None
    start = _valid_date(query.get("corporate_actions_start"))
    end = _valid_date(query.get("corporate_actions_end"))
    event_dates = [
        _valid_date(action.get(field)) for field in ("ex_date", "record_date", "payable_date", "process_date")
    ]
    if start is None or end is None or any(value is None for value in event_dates):
        return None
    return query.get("corporate_actions_data_quality") == "complete" and all(
        start <= value <= end for value in event_dates
    )


def _both_complete(left: bool | None, right: bool | None) -> bool | None:
    if left is None or right is None:
        return None
    return left and right


def _own_receipt_label(document: dict[str, Any], receipt_symbol: str, action_symbol: Any) -> bool | None:
    query = document.get("query")
    if not isinstance(query, dict) or not isinstance(action_symbol, str) or not action_symbol:
        return None
    input_symbol = query.get("input_symbol")
    if not isinstance(input_symbol, str) or not input_symbol:
        return None
    return receipt_symbol == action_symbol == input_symbol


def _distinct_nonempty_strings(left: Any, right: Any) -> bool | None:
    if not all(isinstance(value, str) and value for value in (left, right)):
        return None
    return left != right


def _unique_action_id(document: dict[str, Any], action_id: Any) -> bool | None:
    actions = document.get("corporate_actions")
    if not isinstance(actions, list) or not isinstance(action_id, str) or not action_id:
        return None
    return sum(1 for action in actions if isinstance(action, dict) and action.get("id") == action_id) == 1


def _same_nonempty_string(left: Any, right: Any) -> bool | None:
    if not all(isinstance(value, str) and value for value in (left, right)):
        return None
    return left == right


def _history_brackets(
    document: dict[str, Any], symbol: str, asset_id: str, event_date: date, *, cusip: str | None = None
) -> tuple[bool | None, list[str]]:
    actions = document.get("corporate_actions")
    if not isinstance(actions, list):
        return None, []
    before = []
    after = []
    for action in actions:
        if (
            not isinstance(action, dict)
            or action.get("action_type") != "cash_dividend"
            or action.get("symbol") != symbol
            or not isinstance(action.get("cusip"), str)
            or not action["cusip"]
            or cusip is not None
            and action["cusip"] != cusip
        ):
            continue
        owner = _one_asset(document, "cusip", action["cusip"])
        when = _valid_date(action.get("ex_date"))
        if owner is None or owner["id"] != asset_id or when is None:
            continue
        action_id = action.get("id")
        if not isinstance(action_id, str) or not action_id:
            continue
        if when < event_date:
            before.append((when, action_id))
        elif when > event_date:
            after.append((when, action_id))
    if not before or not after:
        return False, []
    return True, [max(before)[1], min(after)[1]]


def _touches(action: dict[str, Any], symbols: set[str], cusips: set[str]) -> bool:
    for key, value in action.items():
        if not isinstance(value, str):
            continue
        if (key == "symbol" or key.endswith("_symbol")) and value in symbols:
            return True
        if (key == "cusip" or key.endswith("_cusip")) and value in cusips:
            return True
    return False


def _endpoint_cusips(evidence: dict[str, dict[str, Any]], endpoint_asset_ids: frozenset[str]) -> set[str] | None:
    cusips = set()
    for document in evidence.values():
        lookups = document.get("asset_lookups")
        if not isinstance(lookups, list) or any(not isinstance(lookup, dict) for lookup in lookups):
            return None
        for lookup in lookups:
            asset = lookup.get("asset")
            identifier = lookup.get("lookup")
            if (
                lookup.get("lookup_kind") == "cusip"
                and isinstance(identifier, str)
                and identifier
                and isinstance(asset, dict)
                and asset.get("id") in endpoint_asset_ids
            ):
                cusips.add(identifier)
    return cusips


def _structural_link(
    evidence: dict[str, dict[str, Any]],
    symbols: set[str],
    cusips: set[str],
    endpoint_asset_ids: frozenset[str],
) -> tuple[bool | None, dict[str, dict[str, list[str]]]]:
    found: dict[str, dict[str, list[str]]] = {}
    for receipt_symbol, document in sorted(evidence.items()):
        actions = document.get("corporate_actions")
        changes = document.get("symbol_changes")
        if not isinstance(actions, list) or not isinstance(changes, list):
            return None, {}
        action_ids = set()
        for action in actions:
            if not isinstance(action, dict):
                return None, {}
            if action.get("action_type") not in _DISTRIBUTION_TYPES and _touches(action, symbols, cusips):
                action_id = action.get("id")
                if not isinstance(action_id, str) or not action_id:
                    return None, {}
                action_ids.add(action_id)
        for change in changes:
            if not isinstance(change, dict):
                return None, {}
            if _touches(change, symbols, cusips):
                action_id = change.get("id")
                if not isinstance(action_id, str) or not action_id:
                    return None, {}
                action_ids.add(action_id)
        try:
            edges, issues = build_uuid_lineage_edges(document, candidates_only=True)
        except (AttributeError, KeyError, TypeError, ValueError):
            return None, {}
        if issues:
            return None, {}
        edge_ids = {
            edge.action_id
            for edge in edges
            if frozenset((edge.predecessor_asset_id, edge.successor_asset_id)) == endpoint_asset_ids
        }
        found[receipt_symbol] = {
            "structural_action_ids": sorted(action_ids),
            "uuid_edge_action_ids": sorted(edge_ids),
        }
    return any(selected for by_kind in found.values() for selected in by_kind.values()), found


def _action_records(evidence: dict[str, dict[str, Any]]) -> list[_ActionRecord]:
    records = []
    for receipt_symbol, document in sorted(evidence.items()):
        actions = document.get("corporate_actions")
        if not isinstance(actions, list):
            continue
        records.extend(
            _ActionRecord(receipt_symbol, action)
            for action in actions
            if isinstance(action, dict) and action.get("action_type") == "cash_dividend"
        )
    return records


def _candidate_pairs(evidence: dict[str, dict[str, Any]]) -> list[_ActionPairCandidate]:
    records = _action_records(evidence)
    pairs = []
    for subject in records:
        cusip = subject.action.get("cusip")
        if not isinstance(cusip, str) or not cusip:
            continue
        owner_assets = [
            asset for document in evidence.values() for asset in _valid_lookup(document, "cusip", cusip)
        ]
        owner_keys = {(asset["id"], asset.get("symbol")) for asset in owner_assets}
        if len(owner_keys) != 1:
            continue
        owner_symbol = next(iter(owner_keys))[1]
        if not isinstance(owner_symbol, str) or not owner_symbol or subject.action.get("symbol") == owner_symbol:
            continue
        references = [
            item
            for item in records
            if item.receipt_symbol != subject.receipt_symbol
            and item.action.get("symbol") == owner_symbol
            and item.action.get("cusip") == cusip
        ]
        equivalent = [
            item
            for item in references
            if _payload(subject.action) == _payload(item.action)
            and _rate_equivalent(subject.action.get("rate"), item.action.get("rate")) is True
            and _distinct_nonempty_strings(subject.action.get("id"), item.action.get("id")) is True
        ]
        equivalent_ids = tuple(sorted((item.receipt_symbol, item.action["id"]) for item in equivalent))
        for reference in references:
            pairs.append(
                _ActionPairCandidate(
                    subject=subject,
                    reference=reference,
                    unique_reference_match=len(equivalent) == 1,
                    equivalent_reference_ids=equivalent_ids,
                )
            )
    return sorted(
        {
            (
                pair.subject.receipt_symbol,
                str(pair.subject.action.get("id")),
                pair.reference.receipt_symbol,
                str(pair.reference.action.get("id")),
            ): pair
            for pair in pairs
        }.values(),
        key=lambda pair: (
            pair.subject.receipt_symbol,
            str(pair.subject.action.get("id")),
            pair.reference.receipt_symbol,
            str(pair.reference.action.get("id")),
        ),
    )


def _rebuild_document(
    document: dict[str, Any], symbol: str, removed_ids: set[str], local_report: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    actions = document.get("corporate_actions")
    lookups = document.get("asset_lookups")
    if not isinstance(actions, list) or not isinstance(lookups, list) or not isinstance(local_report, dict):
        raise CorrelationPolicyError(
            "candidate evidence projection requires normalized actions, lookups, and local report"
        )
    candidate = copy.deepcopy(document)
    retained_actions = [
        copy.deepcopy(action)
        for action in actions
        if isinstance(action, dict) and action.get("id") not in removed_ids
    ]
    referenced_cusips = set(extract_cusips(retained_actions))
    retained_lookups = [
        copy.deepcopy(lookup)
        for lookup in lookups
        if isinstance(lookup, dict)
        and (
            lookup.get("lookup_kind") == "symbol"
            or lookup.get("lookup_kind") == "cusip"
            and lookup.get("lookup") in referenced_cusips
        )
    ]
    if any(
        not isinstance(lookup, dict) or lookup.get("lookup_kind") not in {"symbol", "cusip"}
        for lookup in lookups
    ):
        raise CorrelationPolicyError("candidate evidence projection found an unsupported lookup kind")

    assets_by_id: dict[str, dict[str, Any]] = {}
    asset_id_by_cusip: dict[str, str] = {}
    for lookup in retained_lookups:
        asset = lookup.get("asset")
        if not isinstance(asset, dict) or not isinstance(asset.get("id"), str):
            continue
        asset_id = asset["id"]
        identity = assets_by_id.setdefault(
            asset_id,
            {**compact_asset(asset), "lookup_symbols": set(), "lookup_cusips": set()},
        )
        kind = lookup["lookup_kind"]
        identifier = lookup.get("lookup")
        if isinstance(identifier, str):
            identity[f"lookup_{kind}s"].add(identifier)
            if kind == "cusip":
                asset_id_by_cusip[identifier] = asset_id
    identities = [
        {
            **{key: value for key, value in identity.items() if not key.startswith("lookup_")},
            "lookup_symbols": sorted(identity["lookup_symbols"]),
            "lookup_cusips": sorted(identity["lookup_cusips"]),
        }
        for identity in assets_by_id.values()
    ]
    identities.sort(key=lambda item: (str(item.get("symbol")), str(item.get("id"))))
    by_symbol_cusip: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in symbol_cusip_evidence(retained_actions):
        by_symbol_cusip.setdefault((item["symbol"], item["cusip"]), []).append(item)
    occupancy = [
        {
            "symbol": evidence_symbol,
            "cusip": cusip,
            "asset_id": asset_id_by_cusip.get(cusip),
            "evidence": items,
        }
        for (evidence_symbol, cusip), items in sorted(by_symbol_cusip.items())
    ]
    input_occupants = [item for item in occupancy if item["symbol"] == symbol]
    symbol_changes = [
        {
            key: action.get(key)
            for key in ("id", "process_date", "old_symbol", "old_cusip", "new_symbol", "new_cusip")
        }
        for action in retained_actions
        if action.get("action_type") == "name_change"
    ]
    warnings = [
        warning
        for warning in candidate.get("warnings", [])
        if not isinstance(warning, str) or _OCCUPANT_WARNING.fullmatch(warning) is None
    ]
    if len(input_occupants) > 1:
        warnings.append(f"The API returned evidence for {len(input_occupants)} CUSIPs occupying {symbol}.")
    candidate.update(
        corporate_actions=retained_actions,
        corporate_action_count=len(retained_actions),
        asset_lookups=retained_lookups,
        identities=identities,
        symbol_changes=symbol_changes,
        symbol_occupancy_evidence=occupancy,
        input_symbol_occupants=input_occupants,
        unresolved_cusips=sorted(referenced_cusips - set(asset_id_by_cusip)),
        symbol_disposition=classify_symbol_disposition(
            symbol, local_report, retained_lookups, identities, input_occupants
        ),
        warnings=warnings,
    )
    removed = {
        "actions": sorted(
            action.get("id")
            for action in actions
            if isinstance(action, dict) and action.get("id") in removed_ids
        ),
        "lookups": sorted(
            (str(item.get("lookup_kind")), str(item.get("lookup")))
            for item in lookups
            if item not in retained_lookups
        ),
        "identities": sorted(
            item.get("id")
            for item in document.get("identities", [])
            if isinstance(item, dict) and item.get("id") not in {identity.get("id") for identity in identities}
        ),
    }
    return candidate, removed


def _facts_for_pair(
    pair: _ActionPairCandidate,
    evidence: dict[str, dict[str, Any]],
    generations: dict[str, str],
    local_reports: dict[str, dict[str, Any]],
) -> tuple[dict[str, bool | None], dict[str, list[dict[str, Any]]]]:
    subject = pair.subject
    reference = pair.reference
    subject_doc = evidence[subject.receipt_symbol]
    reference_doc = evidence[reference.receipt_symbol]
    action = subject.action
    owner_action = reference.action
    action_id = action.get("id")
    owner_id = owner_action.get("id")
    symbol = action.get("symbol")
    owner_symbol = owner_action.get("symbol")
    cusip = action.get("cusip")
    base_refs = [
        _reference(
            "corporate_action", subject.receipt_symbol, generations[subject.receipt_symbol], action_id=action_id
        ),
        _reference(
            "corporate_action",
            reference.receipt_symbol,
            generations[reference.receipt_symbol],
            action_id=owner_id,
        ),
    ]
    facts: dict[str, bool | None] = {
        "quality.cash_dividend_pair": (
            action.get("action_type") == "cash_dividend" and owner_action.get("action_type") == "cash_dividend"
        ),
        "quality.query_coverage_complete": _both_complete(
            _query_covers(subject_doc, action),
            _query_covers(reference_doc, owner_action),
        ),
        "quality.unique_reference_match": pair.unique_reference_match,
        "quality.subject_symbol_matches_input": _own_receipt_label(subject_doc, subject.receipt_symbol, symbol),
        "quality.reference_symbol_matches_input": _own_receipt_label(
            reference_doc, reference.receipt_symbol, owner_symbol
        ),
        "quality.distinct_action_ids": (
            _distinct_nonempty_strings(action_id, owner_id)
            if _unique_action_id(subject_doc, action_id) is True
            and _unique_action_id(reference_doc, owner_id) is True
            else None
        ),
        "quality.distinct_action_symbols": _distinct_nonempty_strings(symbol, owner_symbol),
        "quality.same_cusip": _same_nonempty_string(cusip, owner_action.get("cusip")),
        "quality.nonidentity_payload_exact": _payload(action) == _payload(owner_action),
        "quality.rate_precision_equivalent": _rate_equivalent(action.get("rate"), owner_action.get("rate")),
        "quality.lookup_routes_consistent": None,
        "quality.resolved_assets_distinct": None,
        "quality.subject_history_brackets_event": None,
        "quality.owner_history_brackets_event": None,
        "quality.endpoint_lookup_unresolved": None,
        "quality.selected_structural_link_present": None,
        "quality.projection_closure_complete": None,
    }
    evidence_refs = {name: list(base_refs) for name in facts}
    evidence_refs["quality.unique_reference_match"] = [
        _reference(
            "reference_selection",
            receipt_symbol,
            generations[receipt_symbol],
            equivalent_action_ids=[
                action_id
                for selected_symbol, action_id in pair.equivalent_reference_ids
                if selected_symbol == receipt_symbol
            ],
        )
        for receipt_symbol in sorted(evidence)
    ]
    if not all(isinstance(value, str) and value for value in (symbol, owner_symbol, cusip)):
        return facts, evidence_refs
    subject_asset = _one_asset(subject_doc, "symbol", symbol)
    owner_by_cusip_subject = _one_asset(subject_doc, "cusip", cusip)
    owner_by_cusip_reference = _one_asset(reference_doc, "cusip", cusip)
    owner_by_symbol = _one_asset(reference_doc, "symbol", owner_symbol)
    endpoint_assets = (
        subject_asset,
        owner_by_cusip_subject,
        owner_by_cusip_reference,
        owner_by_symbol,
    )
    exact = all(asset is not None for asset in endpoint_assets)
    facts["quality.endpoint_lookup_unresolved"] = False if exact else None
    if exact:
        owner_ids = {
            owner_by_cusip_subject["id"],
            owner_by_cusip_reference["id"],
            owner_by_symbol["id"],
        }
        facts["quality.lookup_routes_consistent"] = (
            len(owner_ids) == 1
            and subject_asset.get("symbol") == symbol
            and owner_by_symbol.get("symbol") == owner_symbol
            and owner_by_cusip_subject.get("symbol") == owner_symbol
            and owner_by_cusip_reference.get("symbol") == owner_symbol
        )
        facts["quality.resolved_assets_distinct"] = subject_asset["id"] not in owner_ids
        lookup_refs = [
            _reference(
                "asset_lookup",
                subject.receipt_symbol,
                generations[subject.receipt_symbol],
                lookup_kind="symbol",
                lookup=symbol,
            ),
            _reference(
                "asset_lookup",
                subject.receipt_symbol,
                generations[subject.receipt_symbol],
                lookup_kind="cusip",
                lookup=cusip,
            ),
            _reference(
                "asset_lookup",
                reference.receipt_symbol,
                generations[reference.receipt_symbol],
                lookup_kind="symbol",
                lookup=owner_symbol,
            ),
            _reference(
                "asset_lookup",
                reference.receipt_symbol,
                generations[reference.receipt_symbol],
                lookup_kind="cusip",
                lookup=cusip,
            ),
        ]
        for name in (
            "quality.lookup_routes_consistent",
            "quality.resolved_assets_distinct",
            "quality.endpoint_lookup_unresolved",
        ):
            evidence_refs[name] = lookup_refs
        event_date = _valid_date(action.get("ex_date"))
        if event_date is not None:
            subject_bracket, subject_ids = _history_brackets(
                subject_doc, symbol, subject_asset["id"], event_date
            )
            owner_bracket, owner_ids_for_history = _history_brackets(
                reference_doc, owner_symbol, owner_by_symbol["id"], event_date, cusip=cusip
            )
            facts["quality.subject_history_brackets_event"] = subject_bracket
            facts["quality.owner_history_brackets_event"] = owner_bracket
            evidence_refs["quality.subject_history_brackets_event"] = [
                _reference(
                    "bracketing_actions",
                    subject.receipt_symbol,
                    generations[subject.receipt_symbol],
                    action_ids=subject_ids,
                )
            ]
            evidence_refs["quality.owner_history_brackets_event"] = [
                _reference(
                    "bracketing_actions",
                    reference.receipt_symbol,
                    generations[reference.receipt_symbol],
                    action_ids=owner_ids_for_history,
                )
            ]
        endpoint_asset_ids = frozenset((subject_asset["id"], owner_by_symbol["id"]))
        relevant_cusips = _endpoint_cusips(evidence, endpoint_asset_ids)
        if relevant_cusips is not None:
            relevant_cusips.add(cusip)
            structural, structural_refs = _structural_link(
                evidence,
                {symbol, owner_symbol},
                relevant_cusips,
                endpoint_asset_ids,
            )
            facts["quality.selected_structural_link_present"] = structural
            evidence_refs["quality.selected_structural_link_present"] = [
                _reference(
                    "completed_selected_evidence",
                    receipt_symbol,
                    generations[receipt_symbol],
                    **structural_refs.get(
                        receipt_symbol,
                        {"structural_action_ids": [], "uuid_edge_action_ids": []},
                    ),
                )
                for receipt_symbol in sorted(evidence)
            ]
    if isinstance(action_id, str):
        try:
            _rebuild_document(
                subject_doc,
                subject.receipt_symbol,
                {action_id},
                local_reports[subject.receipt_symbol],
            )
            facts["quality.projection_closure_complete"] = True
        except (KeyError, CorrelationPolicyError, TypeError, ValueError):
            facts["quality.projection_closure_complete"] = None
    evidence_refs["quality.projection_closure_complete"] = [
        _reference(
            "projection_inputs", subject.receipt_symbol, generations[subject.receipt_symbol], action_id=action_id
        )
    ]
    return facts, evidence_refs


def apply_evidence_quality_policy(
    evidence: dict[str, dict[str, Any]],
    rules: tuple[EvidenceQualityRule, ...],
    *,
    evidence_generations: dict[str, str],
    local_reports: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Apply a candidate projection; never mutate or replace trusted evidence."""
    raw_digests = {symbol: _digest(document) for symbol, document in sorted(evidence.items())}
    if not rules:
        return evidence, {
            "fact_producers": [],
            "rules": [],
            "raw_evidence_sha256": raw_digests,
            "candidate_evidence_sha256": dict(raw_digests),
            "quarantine_count": 0,
            "quarantines": [],
            "trace_truncated": False,
        }
    if set(evidence) != set(evidence_generations) or set(evidence) != set(local_reports):
        raise CorrelationPolicyError("candidate evidence projection inputs do not cover the selected group")

    matches = []
    for pair in _candidate_pairs(evidence):
        facts, fact_evidence = _facts_for_pair(pair, evidence, evidence_generations, local_reports)
        for rule in rules:
            if not rule.matches(facts):
                continue
            matches.append((pair.subject, pair.reference, rule, facts, fact_evidence))
            break

    removed_by_symbol: dict[str, set[str]] = {}
    for subject, _, _, _, _ in matches:
        removed_by_symbol.setdefault(subject.receipt_symbol, set()).add(subject.action["id"])
    candidate = dict(evidence)
    removal_details = {}
    for symbol, removed_ids in sorted(removed_by_symbol.items()):
        candidate[symbol], removal_details[symbol] = _rebuild_document(
            evidence[symbol], symbol, removed_ids, local_reports[symbol]
        )

    quarantines = []
    for subject, reference, rule, facts, fact_evidence in matches:
        quarantines.append(
            {
                "rule_id": rule.rule_id,
                "effect": dict(rule.effect),
                "subject": {
                    "receipt_symbol": subject.receipt_symbol,
                    "evidence_sha256": evidence_generations[subject.receipt_symbol],
                    "action_id": subject.action["id"],
                    "action_symbol": subject.action.get("symbol"),
                },
                "reference": {
                    "receipt_symbol": reference.receipt_symbol,
                    "evidence_sha256": evidence_generations[reference.receipt_symbol],
                    "action_id": reference.action["id"],
                    "action_symbol": reference.action.get("symbol"),
                },
                "facts": dict(sorted(facts.items())),
                "fact_evidence": {key: value for key, value in sorted(fact_evidence.items())},
                "projection_removals": removal_details[subject.receipt_symbol],
            }
        )
    quarantines.sort(
        key=lambda item: (
            item["subject"]["receipt_symbol"],
            item["subject"]["action_id"],
            item["reference"]["receipt_symbol"],
            item["reference"]["action_id"],
        )
    )
    candidate_digests = {symbol: _digest(document) for symbol, document in sorted(candidate.items())}
    return candidate, {
        "fact_producers": [FACT_PRODUCER],
        "rules": [rule.as_report_object() for rule in rules],
        "raw_evidence_sha256": raw_digests,
        "candidate_evidence_sha256": candidate_digests,
        "quarantine_count": len(quarantines),
        "quarantines": quarantines[:MAX_TRACE_EXAMPLES],
        "trace_truncated": len(quarantines) > MAX_TRACE_EXAMPLES,
    }

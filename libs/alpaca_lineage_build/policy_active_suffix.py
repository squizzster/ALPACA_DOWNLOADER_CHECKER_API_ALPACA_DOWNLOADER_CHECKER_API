"""Typed positive-evidence certificates for bounded active-UUID suffixes."""

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any

from libs.alpaca_end_of_day_security_correlation.policy_rules import (
    CertifiedActiveSuffix,
    certified_active_suffix_row_rule,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import RowCorrelationRule
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp
from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
    market_session_dates_sha256,
)

from .contract import CorrelationPolicyError

FACT_PRODUCER = "active_cusip_dividend_suffix_v2"
ROW_RULE_NAME = "evidence_anchored_active_uuid_suffix"
FACT_TYPES = {
    "suffix.single_receipt_complete_group": "boolean",
    "suffix.evidence_projection_unchanged": "boolean",
    "suffix.complete_covering_query": "boolean",
    "suffix.current_lookup_matches_cutoff": "boolean",
    "suffix.current_symbol_lookup_exact": "boolean",
    "suffix.current_asset_active": "boolean",
    "suffix.current_cusip_route_exact": "boolean",
    "suffix.candidate_assets_exactly_two": "boolean",
    "suffix.candidate_lookup_closure_complete": "boolean",
    "suffix.histories_non_interleaving": "boolean",
    "suffix.boundary_overlap_consistent": "boolean",
    "suffix.positive_anchor_available": "boolean",
    "suffix.repeated_active_anchors": "boolean",
    "suffix.unresolved_relevant_occupancy_present": "boolean",
    "suffix.competing_occupancy_on_or_after_anchor": "boolean",
    "suffix.post_anchor_occupancy_closure_exact": "boolean",
    "suffix.structural_candidate_link_present": "boolean",
    "suffix.uuid_candidate_edge_present": "boolean",
    "suffix.adjustment_set_complete": "boolean",
    "suffix.adjustment_sessions_consistent": "boolean",
    "suffix.suffix_session_coverage_complete": "boolean",
    "suffix.row_candidates_exact": "boolean",
}
EFFECT = {
    "type": "assign_observed_suffix",
    "identity": "exact_active_asset",
    "start": "positive_evidence_anchor",
    "membership": "single_asset",
}
MAX_TRACE_EVALUATIONS = 100
_DISTRIBUTION_TYPES = frozenset(("cash_dividend", "capital_gains_distribution"))
_RATE_QUANTUM = Decimal("0.000001")
_RATE_TOLERANCE = Decimal("0.0000005")


@dataclass(frozen=True)
class ActiveSuffixRule:
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
class ActiveSuffixCertificate:
    rule_id: str
    symbol: str
    asset_id: str
    cusip: str
    anchor_date: date
    competing_last_date: date
    anchor_action_ids: tuple[str, ...]
    later_action_ids: tuple[str, ...]
    candidate_pairs: tuple[tuple[str, str], ...]
    evidence_sha256: str
    pricing_generation: str
    end_exclusive: date
    suffix_sessions: tuple[date, ...]

    def as_report_object(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "symbol": self.symbol,
            "active_asset_id": self.asset_id,
            "active_cusip": self.cusip,
            "anchor_date_inclusive": self.anchor_date.isoformat(),
            "competing_last_evidence_date": self.competing_last_date.isoformat(),
            "anchor_action_ids": list(self.anchor_action_ids),
            "later_active_action_ids": list(self.later_action_ids),
            "input_candidates": [
                {"asset_id": asset_id, "cusip": cusip} for asset_id, cusip in self.candidate_pairs
            ],
            "evidence_sha256": self.evidence_sha256,
            "pricing_generation": self.pricing_generation,
            "pricing_end_exclusive": self.end_exclusive.isoformat(),
            "suffix_session_count": len(self.suffix_sessions),
            "suffix_first_session": self.suffix_sessions[0].isoformat(),
            "suffix_last_session": self.suffix_sessions[-1].isoformat(),
            "suffix_session_dates_sha256": market_session_dates_sha256(
                item.isoformat() for item in self.suffix_sessions
            ),
            "uuid_transition_inferred": False,
            "identity_membership_joined": False,
        }


@dataclass(frozen=True)
class _Dividend:
    action_id: str
    cusip: str
    asset_id: str
    ex_date: date
    action: dict[str, Any]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _reference(kind: str, symbol: str, generation: str, **values) -> dict[str, Any]:
    return {"kind": kind, "symbol": symbol, "generation": generation, **values}


def _date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _observed_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _rate(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _rates_equivalent(left: Any, right: Any) -> bool:
    left_rate = _rate(left)
    right_rate = _rate(right)
    if left_rate is None or right_rate is None:
        return False
    try:
        return (
            left_rate.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_EVEN)
            == right_rate.quantize(_RATE_QUANTUM, rounding=ROUND_HALF_EVEN)
            and abs(left_rate - right_rate) <= _RATE_TOLERANCE
        )
    except InvalidOperation:
        return False


def _valid_lookup_assets(document: dict[str, Any], kind: str, value: str) -> list[dict[str, Any]] | None:
    lookups = document.get("asset_lookups")
    if not isinstance(lookups, list) or any(not isinstance(item, dict) for item in lookups):
        return None
    matching = [item for item in lookups if item.get("lookup_kind") == kind and item.get("lookup") == value]
    if any(
        item.get("found") is not True
        or item.get("outcome") != "found"
        or item.get("http_status") != 200
        or not isinstance(item.get("asset"), dict)
        or item["asset"].get("class") != "us_equity"
        or not isinstance(item["asset"].get("id"), str)
        or not item["asset"]["id"]
        for item in matching
    ):
        return None
    return [item["asset"] for item in matching]


def _one_lookup_asset(document: dict[str, Any], kind: str, value: str) -> dict[str, Any] | None:
    assets = _valid_lookup_assets(document, kind, value)
    return assets[0] if assets is not None and len(assets) == 1 else None


def _same_asset(left: dict[str, Any], right: dict[str, Any]) -> bool:
    fields = ("id", "class", "exchange", "symbol", "name", "status", "tradable")
    return all(left.get(field) == right.get(field) for field in fields)


def _ledger_entry(action: dict[str, Any]) -> tuple[bytes, Any]:
    payload = {key: value for key, value in action.items() if key not in {"id", "cusip", "rate"}}
    return _canonical_bytes(payload), action.get("rate")


def _ledgers_match(left: list[_Dividend], right: list[_Dividend]) -> bool:
    left_entries = sorted(
        (_ledger_entry(item.action) for item in left), key=lambda item: (item[0], str(item[1]))
    )
    right_entries = sorted(
        (_ledger_entry(item.action) for item in right), key=lambda item: (item[0], str(item[1]))
    )
    if len(left_entries) != len(right_entries):
        return False
    unmatched = list(right_entries)
    for payload, rate in left_entries:
        matches = [
            index
            for index, (other_payload, other_rate) in enumerate(unmatched)
            if payload == other_payload and _rates_equivalent(rate, other_rate)
        ]
        if len(matches) != 1:
            return False
        unmatched.pop(matches[0])
    return not unmatched


def _strict_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _provided_dates(
    action: dict[str, Any], fields: tuple[str, ...]
) -> tuple[tuple[date, ...] | None, dict[str, str]]:
    values = {}
    for field in fields:
        if field not in action or action[field] is None:
            continue
        parsed = _strict_date(action[field])
        if parsed is None:
            return None, {}
        values[field] = parsed
    return tuple(values.values()), {field: value.isoformat() for field, value in values.items()}


def _occupancy_dates(action: dict[str, Any]) -> tuple[tuple[date, ...] | None, dict[str, str]]:
    action_type = action.get("action_type")
    if action_type in _DISTRIBUTION_TYPES:
        # Ex-date is the provider's ownership-bearing date for distributions.
        # The normalized occupancy record independently closes process-date evidence.
        dates, values = _provided_dates(action, ("ex_date",))
        return (None, {}) if not dates else (dates, values)
    dates, values = _provided_dates(action, ("effective_date", "ex_date", "process_date"))
    return (None, {}) if not dates else (dates, values)


def _structural_date(
    action: dict[str, Any],
) -> tuple[date | None, dict[str, str], bool]:
    dates, values = _provided_dates(action, ("effective_date", "ex_date", "process_date"))
    if dates is None or not dates:
        return None, values, False
    unique = set(dates)
    if len(unique) != 1:
        return None, values, False
    return next(iter(unique)), values, True


def _action_mentions_symbol(action: dict[str, Any], symbol: str) -> bool:
    return any(
        isinstance(value, str) and value == symbol and (key == "symbol" or key.endswith("_symbol"))
        for key, value in action.items()
    )


def _action_endpoint_asset_ids(action: dict[str, Any], document: dict[str, Any]) -> set[str] | None:
    result = set()
    for key, value in action.items():
        if not (key == "cusip" or key.endswith("_cusip")):
            continue
        if value in (None, ""):
            continue
        if not isinstance(value, str):
            return None
        assets = _valid_lookup_assets(document, "cusip", value)
        if assets is None or len(assets) != 1:
            return None
        result.add(assets[0]["id"])
    return result


def _requested_symbol_routes(action: dict[str, Any], symbol: str) -> list[tuple[str, Any]]:
    routes = []
    for key, value in action.items():
        if value != symbol or not (key == "symbol" or key.endswith("_symbol")):
            continue
        prefix = key.removesuffix("symbol")
        routes.append((key, action.get(prefix + "cusip")))
    return routes


def _post_anchor_occupancy(
    document: dict[str, Any],
    symbol: str,
    current_asset_id: str,
    proposed_start: date,
) -> tuple[bool | None, bool | None, dict[str, Any]]:
    """Close every normalized and raw requested-label occupancy route after the anchor."""
    occupancy = document.get("symbol_occupancy_evidence")
    actions = document.get("corporate_actions")
    if not isinstance(occupancy, list) or not isinstance(actions, list):
        return None, None, {}
    normalized = set()
    accepted = set()
    conflicting = set()
    unknown = set()
    for occupant in occupancy:
        if not isinstance(occupant, dict):
            return None, None, {}
        if occupant.get("symbol") != symbol:
            continue
        cusip = occupant.get("cusip")
        asset_id = occupant.get("asset_id")
        evidence = occupant.get("evidence")
        if not isinstance(evidence, list):
            return None, None, {}
        for item in evidence:
            if not isinstance(item, dict):
                return None, None, {}
            dates, _ = _provided_dates(item, ("process_date",))
            action_id = item.get("action_id")
            if dates is None or not dates or not isinstance(action_id, str) or not action_id:
                unknown.add(str(action_id or "<missing-action-id>"))
                continue
            if dates[0] < proposed_start:
                continue
            if not isinstance(cusip, str) or not cusip or not isinstance(asset_id, str) or not asset_id:
                unknown.add(action_id)
                continue
            asset = _one_lookup_asset(document, "cusip", cusip)
            normalized.add((action_id, cusip))
            if asset is None or asset.get("id") != asset_id:
                unknown.add(action_id)
            elif asset_id != current_asset_id:
                conflicting.add(action_id)
            else:
                accepted.add(action_id)

    raw = set()
    raw_dates = {}
    for action in actions:
        if not isinstance(action, dict):
            return None, None, {}
        routes = _requested_symbol_routes(action, symbol)
        if not routes:
            continue
        dates, date_values = _occupancy_dates(action)
        action_id = action.get("id")
        if dates is None or not isinstance(action_id, str) or not action_id:
            unknown.add(str(action_id or "<missing-action-id>"))
            continue
        if not any(when >= proposed_start for when in dates):
            continue
        raw_dates[action_id] = date_values
        for _, cusip in routes:
            if not isinstance(cusip, str) or not cusip:
                unknown.add(action_id)
                continue
            asset = _one_lookup_asset(document, "cusip", cusip)
            raw.add((action_id, cusip))
            if asset is None:
                unknown.add(action_id)
            elif asset.get("id") != current_asset_id:
                conflicting.add(action_id)
            else:
                accepted.add(action_id)

    missing_normalized = raw - normalized
    missing_raw = normalized - raw
    unknown.update(action_id for action_id, _ in missing_normalized | missing_raw)
    trace = {
        "accepted_action_ids": sorted(accepted),
        "conflicting_action_ids": sorted(conflicting),
        "unknown_action_ids": sorted(unknown),
        "normalized_routes": [
            {"action_id": action_id, "cusip": cusip} for action_id, cusip in sorted(normalized)
        ],
        "raw_routes": [{"action_id": action_id, "cusip": cusip} for action_id, cusip in sorted(raw)],
        "raw_action_dates": {action_id: raw_dates[action_id] for action_id in sorted(raw_dates)},
    }
    if unknown:
        return None, None, trace
    return not conflicting, bool(conflicting), trace


def _structural_veto(
    document: dict[str, Any], symbol: str, candidate_asset_ids: set[str], proposed_start: date
) -> tuple[bool | None, list[str], dict[str, Any]]:
    actions = document.get("corporate_actions")
    changes = document.get("symbol_changes")
    if not isinstance(actions, list) or not isinstance(changes, list):
        return None, [], {}
    selected = set()
    dates_by_action = {}
    uncertain = set()
    for action in (*actions, *changes):
        if not isinstance(action, dict):
            return None, [], {}
        if action.get("action_type") in _DISTRIBUTION_TYPES:
            continue
        endpoints = _action_endpoint_asset_ids(action, document)
        if endpoints is None:
            return None, [], {}
        mentions_symbol = _action_mentions_symbol(action, symbol)
        action_id = action.get("id")
        if not isinstance(action_id, str) or not action_id:
            return None, [], {}
        endpoint_touch = bool(endpoints.intersection(candidate_asset_ids))
        if endpoint_touch:
            selected.add(action_id)
            continue
        if not mentions_symbol:
            continue
        when, date_values, dates_consistent = _structural_date(action)
        dates_by_action[action_id] = date_values
        if not dates_consistent or when is None:
            uncertain.add(action_id)
            continue
        touches = when >= proposed_start
        if not touches:
            continue
        selected.add(action_id)
    trace = {
        "action_dates": {action_id: dates_by_action[action_id] for action_id in sorted(dates_by_action)},
        "uncertain_action_ids": sorted(uncertain),
    }
    if uncertain:
        return None, sorted(selected), trace
    return bool(selected), sorted(selected), trace


def _uuid_edge_veto(document: dict[str, Any], candidate_asset_ids: set[str]) -> tuple[bool | None, list[str]]:
    try:
        edges, issues = build_uuid_lineage_edges(document, candidates_only=True)
    except (AttributeError, KeyError, TypeError, ValueError):
        return None, []
    if issues:
        return None, []
    selected = sorted(
        {
            edge.action_id
            for edge in edges
            if edge.predecessor_asset_id in candidate_asset_ids or edge.successor_asset_id in candidate_asset_ids
        }
    )
    return bool(selected), selected


def _session_grid(view: Any, anchor: date) -> tuple[dict[str, bool | None], dict[str, Any] | None]:
    facts = {
        "suffix.adjustment_set_complete": None,
        "suffix.adjustment_sessions_consistent": None,
        "suffix.suffix_session_coverage_complete": None,
    }
    views = getattr(view, "views", None)
    source = getattr(view, "source", None)
    local = getattr(view, "local", None)
    if not isinstance(views, dict) or not isinstance(source, dict) or not isinstance(local, dict):
        return facts, None
    facts["suffix.adjustment_set_complete"] = set(views) == set(ADJUSTMENTS)
    if facts["suffix.adjustment_set_complete"] is not True:
        return facts, None
    grids = {}
    try:
        for adjustment in ADJUSTMENTS:
            rows = views[adjustment]
            if not isinstance(rows, (list, tuple)):
                return facts, None
            sessions = tuple(timestamp(row[0]).astimezone(NEW_YORK).date() for row in rows)
            if not sessions or any(right <= left for left, right in zip(sessions, sessions[1:])):
                facts["suffix.adjustment_sessions_consistent"] = False
                return facts, None
            grids[adjustment] = sessions
    except (IndexError, KeyError, TypeError, ValueError):
        return facts, None
    first_grid = grids[ADJUSTMENTS[0]]
    facts["suffix.adjustment_sessions_consistent"] = all(grid == first_grid for grid in grids.values())
    if facts["suffix.adjustment_sessions_consistent"] is not True:
        return facts, None
    latest = source.get("latest_request")
    end_exclusive = _date(latest.get("end_exclusive")) if isinstance(latest, dict) else None
    suffix = tuple(item for item in first_grid if item >= anchor)
    if end_exclusive is None or not suffix or suffix[0] != anchor:
        facts["suffix.suffix_session_coverage_complete"] = False
        return facts, None
    try:
        expected = expected_market_session_coverage(anchor.isoformat(), end_exclusive.isoformat())
    except MarketCalendarError:
        return facts, None
    facts["suffix.suffix_session_coverage_complete"] = (
        tuple(item.isoformat() for item in suffix) == expected.session_dates
    )
    descriptors = local.get("downloads")
    if not isinstance(descriptors, list):
        facts["suffix.adjustment_set_complete"] = None
        return facts, None
    by_adjustment = {item.get("adjustment"): item for item in descriptors if isinstance(item, dict)}
    facts["suffix.adjustment_set_complete"] = (
        facts["suffix.adjustment_set_complete"] is True
        and len(descriptors) == len(ADJUSTMENTS)
        and set(by_adjustment) == set(ADJUSTMENTS)
        and all(
            item.get("rows") == len(views[adjustment])
            and item.get("sessions") == len(views[adjustment])
            and _date(item.get("request_to")) == end_exclusive
            for adjustment, item in by_adjustment.items()
        )
    )
    return facts, {
        "sessions": first_grid,
        "suffix": suffix,
        "end_exclusive": end_exclusive,
        "session_dates_sha256": market_session_dates_sha256(item.isoformat() for item in first_grid),
        "suffix_session_dates_sha256": market_session_dates_sha256(item.isoformat() for item in suffix),
    }


def _row_candidates_exact(
    report: dict[str, Any], candidate_pairs: set[tuple[str, str]], anchor: date
) -> bool | None:
    if report.get("row_partition_complete") is not True:
        return False
    branches = (report.get("correlated"), report.get("uncorrelated"))
    if any(not isinstance(branch, dict) or not isinstance(branch.get("segments"), list) for branch in branches):
        return None
    if report["correlated"]["segments"]:
        return False
    covered = False
    for segment in report["uncorrelated"]["segments"]:
        if not isinstance(segment, dict):
            return None
        start = _date(segment.get("from") or segment.get("first_timestamp"))
        end = _date(segment.get("to") or segment.get("last_timestamp"))
        if start is None or end is None:
            return None
        covered = covered or end >= anchor
        rule = segment.get("correlation_rule")
        candidates = segment.get("candidate_securities")
        valid_candidates = (
            isinstance(candidates, list)
            and len(candidates) == len(candidate_pairs)
            and all(
                isinstance(item, dict)
                and isinstance(item.get("asset_id"), str)
                and bool(item["asset_id"])
                and isinstance(item.get("cusip"), str)
                and bool(item["cusip"])
                for item in candidates
            )
        )
        actual = {(item["asset_id"], item["cusip"]) for item in candidates} if valid_candidates else set()
        if (
            not isinstance(rule, dict)
            or rule.get("name") != "unresolved_uuid_candidates"
            or segment.get("reason_codes") != ["ambiguous_security_identity_candidates"]
            or not valid_candidates
            or actual != candidate_pairs
        ):
            return False
    return covered


def _evaluate_symbol(
    symbol: str,
    document: dict[str, Any],
    view: Any,
    report: dict[str, Any],
    *,
    evidence_sha256: str,
    pricing_generation: str,
    evidence_quality: dict[str, Any],
    single_receipt_group: bool,
) -> tuple[dict[str, Any], dict[str, bool | None], dict[str, list[dict[str, Any]]], tuple | None]:
    receipt_ref = _reference("evidence_document", symbol, evidence_sha256)
    pricing_ref = _reference("pricing_source", symbol, pricing_generation)
    facts: dict[str, bool | None] = {name: None for name in FACT_TYPES}
    facts["suffix.single_receipt_complete_group"] = single_receipt_group
    refs = {name: [receipt_ref] for name in FACT_TYPES}
    for name in (
        "suffix.adjustment_set_complete",
        "suffix.adjustment_sessions_consistent",
        "suffix.suffix_session_coverage_complete",
    ):
        refs[name] = [pricing_ref]
    raw_digests = evidence_quality.get("raw_evidence_sha256")
    projected_digests = evidence_quality.get("candidate_evidence_sha256")
    quality_count = evidence_quality.get("quarantine_count")
    facts["suffix.evidence_projection_unchanged"] = (
        isinstance(raw_digests, dict)
        and isinstance(projected_digests, dict)
        and type(quality_count) is int
        and quality_count == 0
        and raw_digests.get(symbol) == projected_digests.get(symbol)
    )
    refs["suffix.evidence_projection_unchanged"] = [
        _reference(
            "evidence_quality_projection",
            symbol,
            evidence_sha256,
            raw_sha256=raw_digests.get(symbol) if isinstance(raw_digests, dict) else None,
            candidate_sha256=(projected_digests.get(symbol) if isinstance(projected_digests, dict) else None),
            quarantine_count=quality_count,
        )
    ]
    lookups = document.get("asset_lookups")
    actions = document.get("corporate_actions")
    query = document.get("query")
    if not isinstance(lookups, list) or not isinstance(actions, list) or not isinstance(query, dict):
        return {}, facts, refs, None

    symbol_assets = _valid_lookup_assets(document, "symbol", symbol)
    facts["suffix.current_symbol_lookup_exact"] = None if symbol_assets is None else len(symbol_assets) == 1
    current = symbol_assets[0] if symbol_assets is not None and len(symbol_assets) == 1 else None
    if current is not None:
        facts["suffix.current_asset_active"] = (
            current.get("symbol") == symbol
            and current.get("status") == "active"
            and current.get("class") == "us_equity"
        )
    cusip_lookups = [item for item in lookups if isinstance(item, dict) and item.get("lookup_kind") == "cusip"]
    current_routes = []
    if current is not None:
        current_routes = [
            item
            for item in cusip_lookups
            if item.get("found") is True
            and item.get("outcome") == "found"
            and item.get("http_status") == 200
            and isinstance(item.get("lookup"), str)
            and bool(item["lookup"])
            and isinstance(item.get("asset"), dict)
            and item["asset"].get("id") == current.get("id")
            and _same_asset(item["asset"], current)
        ]
        facts["suffix.current_cusip_route_exact"] = len(current_routes) == 1
    current_cusip = current_routes[0]["lookup"] if len(current_routes) == 1 else None

    latest = getattr(view, "source", {}).get("latest_request")
    end_exclusive = _date(latest.get("end_exclusive")) if isinstance(latest, dict) else None
    asof = _date(latest.get("asof")) if isinstance(latest, dict) else None
    observed = _observed_date(document.get("catalogue_observed_at_utc"))
    symbol_records = [
        item
        for item in lookups
        if isinstance(item, dict) and item.get("lookup_kind") == "symbol" and item.get("lookup") == symbol
    ]
    facts["suffix.current_lookup_matches_cutoff"] = (
        isinstance(latest, dict)
        and end_exclusive is not None
        and asof == end_exclusive
        and observed is not None
        and observed >= asof
        and latest.get("symbol") == symbol
        and query.get("input_symbol") == symbol
        and _date(query.get("corporate_actions_end")) == end_exclusive
        and len(symbol_records) == 1
        and symbol_records[0].get("source") == "asset_catalogue"
    )

    resolved, unresolved = [], []
    for action in actions:
        if not isinstance(action, dict):
            return {}, facts, refs, None
        if action.get("action_type") != "cash_dividend" or action.get("symbol") != symbol:
            continue
        action_id, cusip = action.get("id"), action.get("cusip")
        ex_date = _date(action.get("ex_date"))
        if (
            not isinstance(action_id, str)
            or not action_id
            or ex_date is None
            or _rate(action.get("rate")) is None
        ):
            unresolved.append((ex_date, action_id))
            continue
        asset = _one_lookup_asset(document, "cusip", cusip) if isinstance(cusip, str) and cusip else None
        if asset is None or asset.get("symbol") != symbol:
            unresolved.append((ex_date, action_id))
            continue
        resolved.append(_Dividend(action_id, cusip, asset["id"], ex_date, action))

    pairs = {(item.asset_id, item.cusip) for item in resolved}
    ids = {asset_id for asset_id, _ in pairs}
    facts["suffix.candidate_assets_exactly_two"] = len(ids) == 2 and len(pairs) == 2
    earliest = min((item.ex_date for item in resolved), default=None)
    unresolved_cusips = document.get("unresolved_cusips")
    relevant_unresolved = (
        []
        if earliest is None
        else [(when, action_id) for when, action_id in unresolved if when is None or when >= earliest]
    )
    unresolved_relevant = (
        None
        if earliest is None or not isinstance(unresolved_cusips, list)
        else bool(unresolved_cusips) or bool(relevant_unresolved)
    )
    facts["suffix.unresolved_relevant_occupancy_present"] = unresolved_relevant
    assets = {asset_id: _one_lookup_asset(document, "cusip", cusip) for asset_id, cusip in pairs}
    facts["suffix.candidate_lookup_closure_complete"] = (
        None
        if unresolved_relevant is None
        else not unresolved_relevant
        and all(asset is not None and asset.get("id") == asset_id for asset_id, asset in assets.items())
        and all(asset.get("status") in {"active", "inactive"} for asset in assets.values())
        and len([asset for asset in assets.values() if asset.get("status") == "active"]) == 1
    )
    payload = {
        "symbol": symbol,
        "active_asset_id": current.get("id") if current is not None else None,
        "active_cusip": current_cusip,
        "candidate_asset_ids": sorted(ids),
        "candidate_cusips": sorted(cusip for _, cusip in pairs),
    }
    certificate_inputs = None
    if (
        current is not None
        and isinstance(current_cusip, str)
        and facts["suffix.candidate_assets_exactly_two"] is True
        and current["id"] in ids
    ):
        active = [item for item in resolved if (item.asset_id, item.cusip) == (current["id"], current_cusip)]
        rivals = [item for item in resolved if item.asset_id != current["id"]]
        if active and rivals:
            rival_last = max(item.ex_date for item in rivals)
            active_by_date = {}
            rival_by_date = {}
            for item in active:
                active_by_date.setdefault(item.ex_date, []).append(item)
            for item in rivals:
                rival_by_date.setdefault(item.ex_date, []).append(item)
            overlap = set(active_by_date).intersection(rival_by_date)
            overlap_consistent = all(_ledgers_match(active_by_date[day], rival_by_date[day]) for day in overlap)
            histories_non_interleaving = (
                overlap_consistent
                and all(day >= rival_last or day in overlap for day in active_by_date)
                and all(day <= min(active_by_date) or day in overlap for day in rival_by_date)
            )
            facts["suffix.boundary_overlap_consistent"] = overlap_consistent
            facts["suffix.histories_non_interleaving"] = histories_non_interleaving
            later_dates = sorted(day for day in active_by_date if day > rival_last)
            anchor = later_dates[0] if later_dates else None
            facts["suffix.positive_anchor_available"] = anchor is not None
            if anchor is not None:
                active_dates = sorted(day for day in active_by_date if day >= anchor)
                facts["suffix.repeated_active_anchors"] = len(active_dates) >= 2
                occupancy_closure, competing_occupancy, occupancy_trace = _post_anchor_occupancy(
                    document,
                    symbol,
                    current["id"],
                    anchor,
                )
                facts["suffix.post_anchor_occupancy_closure_exact"] = occupancy_closure
                facts["suffix.competing_occupancy_on_or_after_anchor"] = competing_occupancy
                refs["suffix.post_anchor_occupancy_closure_exact"] = [
                    _reference(
                        "post_anchor_occupancy_closure",
                        symbol,
                        evidence_sha256,
                        **occupancy_trace,
                    )
                ]
                refs["suffix.competing_occupancy_on_or_after_anchor"] = [
                    _reference(
                        "competing_uuid_occupancy",
                        symbol,
                        evidence_sha256,
                        asset_ids=sorted(ids - {current["id"]}),
                        action_ids=occupancy_trace.get("conflicting_action_ids", []),
                    )
                ]
                structural, structural_ids, structural_trace = _structural_veto(document, symbol, ids, anchor)
                edge, edge_ids = _uuid_edge_veto(document, ids)
                facts["suffix.structural_candidate_link_present"] = structural
                facts["suffix.uuid_candidate_edge_present"] = edge
                refs["suffix.structural_candidate_link_present"] = [
                    _reference(
                        "selected_structural_actions",
                        symbol,
                        evidence_sha256,
                        action_ids=structural_ids,
                        **structural_trace,
                    )
                ]
                refs["suffix.uuid_candidate_edge_present"] = [
                    _reference("selected_uuid_edges", symbol, evidence_sha256, action_ids=edge_ids)
                ]
                grid_facts, grid = _session_grid(view, anchor)
                facts.update(grid_facts)
                facts["suffix.row_candidates_exact"] = _row_candidates_exact(report, pairs, anchor)
                if grid is not None:
                    anchor_ids = tuple(sorted(item.action_id for item in active_by_date[anchor]))
                    later_ids = tuple(sorted(item.action_id for item in active if item.ex_date > anchor))
                    payload.update(
                        anchor_date_inclusive=anchor.isoformat(),
                        competing_last_evidence_date=rival_last.isoformat(),
                        overlap_dates=[item.isoformat() for item in sorted(overlap)],
                        anchor_action_ids=list(anchor_ids),
                        later_active_action_ids=list(later_ids),
                        suffix_session_count=len(grid["suffix"]),
                        suffix_first_session=grid["suffix"][0].isoformat(),
                        suffix_last_session=grid["suffix"][-1].isoformat(),
                        suffix_session_dates_sha256=grid["suffix_session_dates_sha256"],
                        evidence_sha256=evidence_sha256,
                        pricing_generation=pricing_generation,
                        pricing_end_exclusive=grid["end_exclusive"].isoformat(),
                    )
                    certificate_inputs = (
                        current["id"],
                        current_cusip,
                        anchor,
                        rival_last,
                        anchor_ids,
                        later_ids,
                        tuple(sorted(pairs)),
                        grid["end_exclusive"],
                        grid["suffix"],
                    )

    start = _date(query.get("corporate_actions_start"))
    end = _date(query.get("corporate_actions_end"))
    facts["suffix.complete_covering_query"] = (
        query.get("corporate_actions_data_quality") == "complete"
        and start is not None
        and earliest is not None
        and start <= earliest
        and end is not None
        and end_exclusive is not None
        and end >= end_exclusive
    )
    action_ids = sorted(item.action_id for item in resolved)
    for name in (
        "suffix.candidate_assets_exactly_two",
        "suffix.candidate_lookup_closure_complete",
        "suffix.histories_non_interleaving",
        "suffix.boundary_overlap_consistent",
        "suffix.positive_anchor_available",
        "suffix.repeated_active_anchors",
        "suffix.unresolved_relevant_occupancy_present",
    ):
        refs[name] = [
            _reference(
                "candidate_dividend_actions",
                symbol,
                evidence_sha256,
                action_ids=action_ids,
                unresolved_action_ids=sorted(
                    action_id for _, action_id in relevant_unresolved if isinstance(action_id, str) and action_id
                ),
                malformed_action_count=len(relevant_unresolved),
                ignored_pre_history_action_ids=sorted(
                    action_id
                    for when, action_id in unresolved
                    if when is not None
                    and earliest is not None
                    and when < earliest
                    and isinstance(action_id, str)
                    and action_id
                ),
                unresolved_cusips=(
                    sorted(unresolved_cusips)
                    if isinstance(unresolved_cusips, list)
                    and all(isinstance(item, str) for item in unresolved_cusips)
                    else None
                ),
            )
        ]
    refs["suffix.row_candidates_exact"] = [_reference("candidate_correlation_report", symbol, evidence_sha256)]
    return payload, facts, refs, certificate_inputs


def prepare_active_suffix_policy(
    evidence: dict[str, dict[str, Any]],
    inputs: dict[str, Any],
    reports: dict[str, dict[str, Any]],
    rules: tuple[ActiveSuffixRule, ...],
    *,
    selected_group: list[str],
    unavailable_pricing: list[str],
    evidence_generations: dict[str, str],
    pricing_generations: dict[str, str],
    evidence_quality: dict[str, Any],
) -> tuple[tuple[ActiveSuffixCertificate, ...], dict[str, Any]]:
    """Evaluate active-suffix rules without mutating evidence, rows, or receipts."""
    if not rules:
        return (), {
            "fact_producers": [],
            "rules": [],
            "certificate_count": 0,
            "certificates": [],
            "certificate_sha256": None,
            "evaluations": [],
            "trace_truncated": False,
        }
    expected = set(evidence)
    if (
        set(inputs) != expected
        or set(reports) != expected
        or set(evidence_generations) != expected
        or set(pricing_generations) != expected
    ):
        raise CorrelationPolicyError("active-suffix inputs do not cover the selected receipt set")
    single_receipt_group = len(expected) == 1 and set(selected_group) == expected and not unavailable_pricing
    certificates = []
    evaluations = []
    for symbol in sorted(evidence):
        payload, facts, fact_evidence, certificate_inputs = _evaluate_symbol(
            symbol,
            evidence[symbol],
            inputs[symbol],
            reports[symbol],
            evidence_sha256=evidence_generations[symbol],
            pricing_generation=pricing_generations[symbol],
            evidence_quality=evidence_quality,
            single_receipt_group=single_receipt_group,
        )
        matched = next((rule for rule in rules if rule.matches(facts)), None)
        if matched is not None and certificate_inputs is not None:
            (
                asset_id,
                cusip,
                anchor,
                rival_last,
                anchor_ids,
                later_ids,
                candidate_pairs,
                end_exclusive,
                sessions,
            ) = certificate_inputs
            certificates.append(
                ActiveSuffixCertificate(
                    rule_id=matched.rule_id,
                    symbol=symbol,
                    asset_id=asset_id,
                    cusip=cusip,
                    anchor_date=anchor,
                    competing_last_date=rival_last,
                    anchor_action_ids=anchor_ids,
                    later_action_ids=later_ids,
                    candidate_pairs=candidate_pairs,
                    evidence_sha256=evidence_generations[symbol],
                    pricing_generation=pricing_generations[symbol],
                    end_exclusive=end_exclusive,
                    suffix_sessions=sessions,
                )
            )
        evaluations.append(
            {
                "symbol": symbol,
                "evidence_sha256": evidence_generations[symbol],
                "pricing_generation": pricing_generations[symbol],
                "matched_rule_id": matched.rule_id if matched is not None else None,
                "candidate": payload,
                "facts": dict(sorted(facts.items())),
                "fact_evidence": {name: references for name, references in sorted(fact_evidence.items())},
            }
        )
    certificates.sort(key=lambda item: (item.symbol, item.anchor_date, item.asset_id))
    objects = [item.as_report_object() for item in certificates]
    return tuple(certificates), {
        "fact_producers": [FACT_PRODUCER],
        "rules": [rule.as_report_object() for rule in rules],
        "certificate_count": len(certificates),
        "certificates": objects,
        "certificate_sha256": _digest(objects) if objects else None,
        "evaluations": evaluations[:MAX_TRACE_EVALUATIONS],
        "trace_truncated": len(evaluations) > MAX_TRACE_EVALUATIONS,
    }


def active_suffix_row_rule(
    certificates: tuple[ActiveSuffixCertificate, ...],
) -> RowCorrelationRule:
    if len({item.symbol for item in certificates}) != len(certificates):
        raise CorrelationPolicyError("active-suffix certificates contain duplicate symbols")
    return certified_active_suffix_row_rule(
        tuple(
            CertifiedActiveSuffix(
                symbol=item.symbol,
                asset_id=item.asset_id,
                cusip=item.cusip,
                start_date_inclusive=item.anchor_date,
                end_date_exclusive=item.end_exclusive,
                supporting_action_ids=item.anchor_action_ids,
                session_dates=item.suffix_sessions,
            )
            for item in certificates
        ),
        rule_name=ROW_RULE_NAME,
        identity_confidence="positive_evidence_bounded_active_uuid_suffix",
        warning_code="identity_start_bounded_by_positive_occupancy_evidence",
    )


def with_active_suffix_rule(
    row_rules: tuple[RowCorrelationRule, ...],
    certificates: tuple[ActiveSuffixCertificate, ...],
) -> tuple[RowCorrelationRule, ...]:
    if not certificates:
        return row_rules
    if not row_rules or row_rules[-1].name != "unresolved_uuid_candidates":
        raise CorrelationPolicyError("active-suffix rule requires the mandatory unresolved fallback")
    if any(rule.name == ROW_RULE_NAME for rule in row_rules):
        raise CorrelationPolicyError("active-suffix row rule is duplicated")
    return (*row_rules[:-1], active_suffix_row_rule(certificates), row_rules[-1])


def validate_active_suffix_effect(
    reports: dict[str, dict[str, Any]],
    certificates: tuple[ActiveSuffixCertificate, ...],
) -> None:
    for certificate in certificates:
        report = reports[certificate.symbol]
        suffix_rows = 0
        for segment in report["correlated"]["segments"]:
            start = _date(segment.get("from") or segment.get("first_timestamp"))
            end = _date(segment.get("to") or segment.get("last_timestamp"))
            if start is None or end is None:
                raise CorrelationPolicyError("active-suffix output has an invalid segment date")
            if end < certificate.anchor_date:
                continue
            identity = segment.get("security_identity")
            if (
                start < certificate.anchor_date
                or segment.get("correlation_rule", {}).get("name") != ROW_RULE_NAME
                or segment.get("ticker_ownership_interval") is not None
                or not isinstance(identity, dict)
                or identity.get("member_alpaca_asset_ids") != [certificate.asset_id]
                or identity.get("basis") != "single_alpaca_asset_id"
            ):
                raise CorrelationPolicyError("active-suffix output exceeded its certificate")
            suffix_rows += segment["row_count"]
        expected_rows = len(certificate.suffix_sessions) * len(ADJUSTMENTS)
        if suffix_rows != expected_rows:
            raise CorrelationPolicyError(
                f"active-suffix output row count is incomplete ({suffix_rows} != {expected_rows})"
            )
        if any(
            (_date(segment.get("from") or segment.get("first_timestamp")) or date.min) < certificate.anchor_date
            for segment in report["correlated"]["segments"]
        ):
            raise CorrelationPolicyError("active-suffix output widened before its anchor")
        for segment in report["uncorrelated"]["segments"]:
            start = _date(segment.get("from") or segment.get("first_timestamp"))
            end = _date(segment.get("to") or segment.get("last_timestamp"))
            candidates = segment.get("candidate_securities")
            if (
                start is None
                or end is None
                or end >= certificate.anchor_date
                or segment.get("correlation_rule", {}).get("name") != "unresolved_uuid_candidates"
                or segment.get("reason_codes") != ["ambiguous_security_identity_candidates"]
                or not isinstance(candidates, list)
                or len(candidates) != len(certificate.candidate_pairs)
                or any(
                    not isinstance(item, dict)
                    or not isinstance(item.get("asset_id"), str)
                    or not isinstance(item.get("cusip"), str)
                    for item in candidates
                )
                or {(item["asset_id"], item["cusip"]) for item in candidates} != set(certificate.candidate_pairs)
            ):
                raise CorrelationPolicyError("active-suffix output changed its unresolved prefix")
        expected_prefix_rows = report["source_row_count"] - expected_rows
        if report["uncorrelated"]["row_count"] != expected_prefix_rows:
            raise CorrelationPolicyError("active-suffix output did not preserve its unresolved prefix")

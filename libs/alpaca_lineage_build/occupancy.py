"""Resolve a contradicted cross-source occupancy claim inside one loaded group."""

from datetime import date
from decimal import InvalidOperation
from typing import Any

from libs.alpaca_end_of_day_security_correlation.action_identity import (
    exact_cusip_lookup_asset,
    exact_symbol_lookup_asset,
)
from libs.alpaca_end_of_day_security_correlation.occupancy_anchor import (
    positive_occupancy_action_date,
)
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, NEW_YORK, timestamp

_DISTRIBUTION_DATE_FIELDS = ("ex_date", "record_date", "payable_date", "process_date")
# Occupancy refinement belongs inside the loaded-group producer. Existing dated
# assignments remain authority; these evidence tests must not become a downstream
# reader's second identity solver or use equal prices alone as ownership proof.


def may_have_action_anchored_current_suffix(lineage: dict[str, Any]) -> bool:
    """Recognize evidence for the producer's bounded-suffix fallback."""
    from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
        build_ticker_ownership_intervals,
    )

    intervals, _ = build_ticker_ownership_intervals(
        lineage,
        include_action_anchored_current_suffix=True,
    )
    return any(
        rule.name == "action_anchored_current_ticker_occupancy"
        for interval in intervals
        for rule in interval.applied_rules
    )


def _distribution_key(action: dict[str, Any]) -> tuple[str, ...] | None:
    values = (action.get("cusip"), *(action.get(field) for field in _DISTRIBUTION_DATE_FIELDS))
    if action.get("action_type") != "cash_dividend" or not all(
        isinstance(value, str) and value for value in values
    ):
        return None
    try:
        for value in values[1:]:
            date.fromisoformat(value)
    except ValueError:
        return None
    return values


def _resolved_pairs(lineage: dict[str, Any], symbol: str) -> list[tuple[str, str]]:
    pairs = {
        (item.get("cusip"), item.get("asset_id"))
        for item in lineage.get("symbol_occupancy_evidence", ())
        if isinstance(item, dict) and item.get("symbol") == symbol
    }
    return sorted(
        (cusip, asset_id)
        for cusip, asset_id in pairs
        if isinstance(cusip, str) and cusip and isinstance(asset_id, str) and asset_id
    )


def _bracketing_current_actions(
    lineage: dict[str, Any], symbol: str, cusip: str, effective: date, rejected_action_id: str
) -> tuple[str, str] | None:
    anchors = []
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or action.get("id") == rejected_action_id:
            continue
        try:
            candidate = positive_occupancy_action_date(action, symbol, cusip)
        except (KeyError, TypeError, ValueError, InvalidOperation):
            continue
        if candidate is not None and isinstance(action.get("id"), str):
            anchors.append((candidate, action["id"]))
    before = max((item for item in anchors if item[0] < effective), default=None)
    after = min((item for item in anchors if item[0] > effective), default=None)
    return (before[1], after[1]) if before is not None and after is not None else None


def _has_related_structural_action(evidence: dict[str, dict[str, Any]], symbols: set[str], cusip: str) -> bool:
    for lineage in evidence.values():
        for action in lineage.get("corporate_actions", ()):
            if not isinstance(action, dict) or action.get("action_type") == "cash_dividend":
                continue
            action_symbols = {
                value
                for key, value in action.items()
                if key.endswith("symbol") and isinstance(value, str) and value
            }
            action_cusips = {
                value
                for key, value in action.items()
                if key.endswith("cusip") and isinstance(value, str) and value
            }
            if action_symbols & symbols or cusip in action_cusips:
                return True
    return False


def may_have_duplicate_foreign_occupancy(lineage: dict[str, Any], symbol: str) -> bool:
    """Recognize the local evidence shape that needs complete-group review.

    This is deliberately only a migration predicate. Resolution still requires
    the other source's matching action, eight complete assigned views, and the
    complete group's structural-action check below.
    """
    current = exact_symbol_lookup_asset(lineage, symbol)
    if current is None or current.get("status") != "active" or current.get("tradable") is not True:
        return False
    current_pairs = [pair for pair in _resolved_pairs(lineage, symbol) if pair[1] == current["id"]]
    if len(current_pairs) != 1:
        return False
    current_cusip, current_asset_id = current_pairs[0]
    current_cusip_asset = exact_cusip_lookup_asset(lineage, current_cusip)
    if current_cusip_asset is None or current_cusip_asset.get("id") != current_asset_id:
        return False
    for action in lineage.get("corporate_actions", ()):
        if not isinstance(action, dict) or not isinstance(action.get("id"), str):
            continue
        key = _distribution_key(action)
        if key is None or action.get("symbol") != symbol:
            continue
        cusip = key[0]
        foreign_asset = exact_cusip_lookup_asset(lineage, cusip)
        if (
            foreign_asset is None
            or foreign_asset.get("symbol") in (None, "", symbol)
            or foreign_asset.get("id") == current_asset_id
        ):
            continue
        try:
            effective = positive_occupancy_action_date(action, symbol, cusip)
        except (KeyError, TypeError, ValueError, InvalidOperation):
            continue
        if (
            effective is not None
            and _bracketing_current_actions(lineage, symbol, current_cusip, effective, action["id"]) is not None
        ):
            return True
    return False


def _assigned_on_every_view(pricing, report: dict[str, Any], day: str, asset_id: str) -> bool:
    if set(pricing.views) != set(ADJUSTMENTS):
        return False
    for adjustment in ADJUSTMENTS:
        indexes = [
            index
            for index, row in enumerate(pricing.views[adjustment], start=1)
            if timestamp(row[0]).astimezone(NEW_YORK).date().isoformat() == day
        ]
        if len(indexes) != 1:
            return False
        index = indexes[0]
        matches = [
            segment
            for segment in report["correlated"]["segments"]
            if segment["adjustment"] == adjustment
            and segment["row_start"] <= index <= segment["row_end"]
            and (segment.get("canonical_security") or {}).get("alpaca_asset_id") == asset_id
        ]
        if len(matches) != 1 or any(
            segment["adjustment"] == adjustment and segment["row_start"] <= index <= segment["row_end"]
            for segment in report["uncorrelated"]["segments"]
        ):
            return False
    return True


def reconcile_duplicate_foreign_occupancy(
    inputs: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    preliminary: dict[str, dict[str, Any]],
) -> dict[str, tuple[dict[str, Any], ...]]:
    """Identify a mislabeled action only when another complete source proves it.

    A CUSIP-bound distribution under a foreign label is superseded only when the
    same completed group contains the corresponding own-label distribution and
    all eight own-label pricing views assign that session to the exact CUSIP UUID.
    The foreign label's current UUID must also have positive actions on both sides.
    Raw provider evidence remains unchanged.
    """
    if not preliminary:
        return {}
    own_actions: dict[tuple[str, ...], list[tuple[str, dict[str, Any]]]] = {}
    for source, lineage in evidence.items():
        for action in lineage.get("corporate_actions", ()):
            if not isinstance(action, dict):
                continue
            key = _distribution_key(action)
            if key is not None and action.get("symbol") == source:
                own_actions.setdefault(key, []).append((source, action))

    decisions: dict[str, list[dict[str, Any]]] = {}
    for source, lineage in evidence.items():
        source_preliminary = preliminary.get(source)
        if (
            not isinstance(source_preliminary, dict)
            or (source_preliminary.get("uncorrelated") or {}).get("row_count") == 0
        ):
            continue
        current = exact_symbol_lookup_asset(lineage, source)
        if current is None or current.get("status") != "active" or current.get("tradable") is not True:
            continue
        current_pairs = [pair for pair in _resolved_pairs(lineage, source) if pair[1] == current["id"]]
        if len(current_pairs) != 1:
            continue
        current_cusip, current_asset_id = current_pairs[0]
        current_cusip_asset = exact_cusip_lookup_asset(lineage, current_cusip)
        if current_cusip_asset is None or current_cusip_asset.get("id") != current_asset_id:
            continue
        for action in lineage.get("corporate_actions", ()):
            if not isinstance(action, dict) or not isinstance(action.get("id"), str):
                continue
            key = _distribution_key(action)
            if key is None or action.get("symbol") != source:
                continue
            cusip, effective_text, *_ = key
            foreign_asset = exact_cusip_lookup_asset(lineage, cusip)
            foreign_symbol = foreign_asset.get("symbol") if foreign_asset is not None else None
            if (
                foreign_asset is None
                or not isinstance(foreign_symbol, str)
                or not foreign_symbol
                or foreign_symbol == source
                or foreign_asset["id"] == current_asset_id
            ):
                continue
            matches = [item for item in own_actions.get(key, ()) if item[0] == foreign_symbol]
            if len(matches) != 1 or foreign_symbol not in inputs or foreign_symbol not in preliminary:
                continue
            authoritative_source, authoritative_action = matches[0]
            authoritative_lineage = evidence.get(authoritative_source)
            if authoritative_lineage is None:
                continue
            own_symbol_asset = exact_symbol_lookup_asset(authoritative_lineage, authoritative_source)
            own_cusip_asset = exact_cusip_lookup_asset(authoritative_lineage, cusip)
            if (
                own_symbol_asset is None
                or own_cusip_asset is None
                or own_symbol_asset["id"] != foreign_asset["id"]
                or own_cusip_asset["id"] != foreign_asset["id"]
                or _has_related_structural_action(evidence, {source, authoritative_source}, cusip)
            ):
                continue
            try:
                effective = positive_occupancy_action_date(action, source, cusip)
                authoritative_effective = positive_occupancy_action_date(
                    authoritative_action, authoritative_source, cusip
                )
            except (KeyError, TypeError, ValueError, InvalidOperation):
                continue
            if effective is None or authoritative_effective != effective:
                continue
            brackets = _bracketing_current_actions(lineage, source, current_cusip, effective, action["id"])
            if brackets is None or not _assigned_on_every_view(
                inputs[authoritative_source],
                preliminary[authoritative_source],
                effective_text,
                foreign_asset["id"],
            ):
                continue
            decisions.setdefault(source, []).append(
                {
                    "reason_code": "resolved_duplicate_foreign_label_occupancy",
                    "action_id": action["id"],
                    "symbol": source,
                    "cusip": cusip,
                    "effective_date": effective_text,
                    "authoritative_symbol": authoritative_source,
                    "authoritative_action_id": authoritative_action["id"],
                    "authoritative_asset_id": foreign_asset["id"],
                    "current_asset_id": current_asset_id,
                    "bracketing_current_action_ids": list(brackets),
                }
            )
    return {
        source: tuple(sorted(values, key=lambda item: (item["effective_date"], item["action_id"])))
        for source, values in decisions.items()
    }

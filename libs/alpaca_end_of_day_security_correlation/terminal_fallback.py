from datetime import date
from typing import Any

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
    us_market_hours_sha256,
)

from .security_identity import security_identity_for_alpaca_assets
from .settings import CORRELATED_BRANCH, UNCORRELATED_BRANCH
from .uuid_lineage import UuidLineageEdge

TERMINAL_FALLBACK_RULE_NAME = "two_uuid_continuous_ticker_history"
TERMINAL_FALLBACK_RULE_ORDER = 6
TERMINAL_FALLBACK_WARNING = (
    "two_alpaca_uuid_candidates_joined_by_continuous_ticker_history_with_unresolved_vendor_boundary"
)

IDENTITY_CHANGE_ACTION_TYPES = frozenset(
    {
        "cash_merger",
        "name_change",
        "reorganization",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
    }
)


def _candidate_state(
    uncorrelated_segments: list[dict[str, Any]],
) -> tuple[list[str], dict[str, list[dict[str, Any]]], bool]:
    candidate_sets = []
    candidates_by_asset_id: dict[str, list[dict[str, Any]]] = {}
    for segment in uncorrelated_segments:
        candidates = segment.get("candidate_securities")
        if not isinstance(candidates, list):
            candidate_sets.append(())
            continue
        segment_asset_ids = []
        for candidate in candidates:
            if not isinstance(candidate, dict) or not isinstance(candidate.get("asset_id"), str):
                continue
            asset_id = candidate["asset_id"]
            segment_asset_ids.append(asset_id)
            candidates_by_asset_id.setdefault(asset_id, []).append(candidate)
        candidate_sets.append(tuple(sorted(segment_asset_ids)))

    candidate_asset_ids = sorted(candidates_by_asset_id)
    stable_pair = bool(candidate_sets) and all(
        candidate_set == candidate_sets[0] for candidate_set in candidate_sets
    )
    stable_pair = stable_pair and len(candidate_sets[0]) == 2 and len(set(candidate_sets[0])) == 2
    return candidate_asset_ids, candidates_by_asset_id, stable_pair


def _asof_symbol_lookup_asset_ids(
    lineage: dict[str, Any],
    requested_symbol: str,
    candidate_asset_ids: list[str],
) -> list[str]:
    matching = set()
    asset_lookups = lineage.get("asset_lookups")
    if not isinstance(asset_lookups, list):
        return []
    for lookup in asset_lookups:
        if (
            not isinstance(lookup, dict)
            or lookup.get("lookup_kind") != "symbol"
            or lookup.get("lookup") != requested_symbol
            or lookup.get("found") is not True
        ):
            continue
        asset = lookup.get("asset")
        asset_id = asset.get("id") if isinstance(asset, dict) else None
        if asset_id in candidate_asset_ids:
            matching.add(asset_id)
    return sorted(matching)


def _potential_identity_change_actions(
    lineage: dict[str, Any],
    candidates_by_asset_id: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    candidate_links_by_action_id: dict[str, list[dict[str, Any]]] = {}
    for asset_id, candidates in candidates_by_asset_id.items():
        for candidate in candidates:
            evidence_items = candidate.get("evidence")
            if not isinstance(evidence_items, list):
                continue
            for evidence in evidence_items:
                if (
                    not isinstance(evidence, dict)
                    or evidence.get("action_type") not in IDENTITY_CHANGE_ACTION_TYPES
                    or not isinstance(evidence.get("action_id"), str)
                ):
                    continue
                action_id = evidence["action_id"]
                candidate_links_by_action_id.setdefault(action_id, []).append(
                    {
                        "alpaca_asset_id": asset_id,
                        "symbol": evidence.get("symbol"),
                        "cusip": evidence.get("cusip"),
                        "role": evidence.get("role"),
                    }
                )

    corporate_actions = lineage.get("corporate_actions")
    if not isinstance(corporate_actions, list):
        corporate_actions = []
    actions_by_id = {
        action["id"]: action
        for action in corporate_actions
        if isinstance(action, dict) and isinstance(action.get("id"), str)
    }
    result = []
    for action_id, candidate_links in sorted(candidate_links_by_action_id.items()):
        action = actions_by_id.get(action_id, {})
        result.append(
            {
                "corporate_action_id": action_id,
                "action_type": action.get("action_type"),
                "process_date": action.get("process_date"),
                "effective_date": action.get("effective_date"),
                "candidate_links": sorted(
                    candidate_links,
                    key=lambda item: (
                        str(item["alpaca_asset_id"]),
                        str(item.get("role") or ""),
                    ),
                ),
            }
        )
    return result


def _session_coverage(
    session_dates: tuple[date, ...],
    source: dict[str, Any],
) -> tuple[dict[str, Any], bool, bool, bool]:
    observed = tuple(session_date.isoformat() for session_date in session_dates)
    coverage = {
        "observed_session_count": len(observed),
        "observed_first_session": observed[0] if observed else None,
        "observed_last_session": observed[-1] if observed else None,
        "evaluated_start_date_inclusive": observed[0] if observed else None,
        "evaluated_end_date_exclusive": source.get("request_to"),
        "expected_session_count": None,
        "expected_session_dates_sha256": None,
        "calendar_sha256": None,
        "missing_session_count": None,
        "missing_sessions": None,
        "unexpected_session_count": None,
        "unexpected_sessions": None,
        "evaluation_error": None,
    }
    if not observed:
        coverage["evaluation_error"] = "the source has no observed sessions"
        return coverage, False, False, False
    try:
        expected, missing, unexpected = compare_observed_to_expected_sessions(
            observed,
            observed[0],
            source.get("request_to"),
        )
        coverage.update(
            {
                "expected_session_count": expected.session_count,
                "expected_session_dates_sha256": expected.session_dates_sha256,
                "calendar_sha256": us_market_hours_sha256(),
                "missing_session_count": len(missing),
                "missing_sessions": list(missing),
                "unexpected_session_count": len(unexpected),
                "unexpected_sessions": list(unexpected),
            }
        )
    except MarketCalendarError as error:
        coverage["evaluation_error"] = str(error)
        return coverage, False, False, False
    return coverage, True, not missing, not unexpected


def apply_terminal_continuous_ticker_fallback(
    branches: dict[str, list[dict[str, Any]]],
    session_dates: tuple[date, ...],
    source: dict[str, Any],
    lineage: dict[str, Any],
    uuid_lineage_edges: list[UuidLineageEdge],
) -> dict[str, list[dict[str, Any]]]:
    """Promote only a fully unresolved, gapless two-UUID ticker history."""
    correlated_segments = branches[CORRELATED_BRANCH]
    uncorrelated_segments = branches[UNCORRELATED_BRANCH]
    candidate_asset_ids, candidates_by_asset_id, stable_pair = _candidate_state(uncorrelated_segments)
    requested_symbol = source["requested_symbol"]
    candidate_symbols_match = stable_pair and all(
        candidates_by_asset_id[asset_id]
        and all(
            candidate.get("asset_symbol") == requested_symbol for candidate in candidates_by_asset_id[asset_id]
        )
        for asset_id in candidate_asset_ids
    )
    initial_uncorrelated_row_count = sum(segment.get("row_count", 0) for segment in uncorrelated_segments)
    all_rows_initially_uncorrelated = (
        bool(session_dates) and not correlated_segments and initial_uncorrelated_row_count == len(session_dates)
    )
    ambiguous_identity_only = bool(uncorrelated_segments) and all(
        segment.get("reason_codes") == ["ambiguous_security_identity_candidates"]
        for segment in uncorrelated_segments
    )
    coverage, calendar_evaluated, no_missing_sessions, no_unexpected_sessions = _session_coverage(
        session_dates,
        source,
    )
    criteria = {
        "all_rows_initially_uncorrelated": all_rows_initially_uncorrelated,
        "ambiguous_identity_is_only_uncorrelated_reason": ambiguous_identity_only,
        "exactly_two_stable_alpaca_asset_id_candidates": stable_pair,
        "both_candidate_asset_symbols_match_requested_ticker": candidate_symbols_match,
        "market_calendar_evaluation_succeeded": calendar_evaluated,
        "no_missing_expected_market_sessions": no_missing_sessions,
        "no_unexpected_market_sessions": no_unexpected_sessions,
    }
    eligible = all(value is True for value in criteria.values())
    current_lookup_asset_ids = _asof_symbol_lookup_asset_ids(
        lineage,
        requested_symbol,
        candidate_asset_ids,
    )
    candidate_names = sorted(
        {
            candidate["asset_name"]
            for candidates in candidates_by_asset_id.values()
            for candidate in candidates
            if isinstance(candidate.get("asset_name"), str) and candidate["asset_name"]
        }
    )
    candidate_statuses = sorted(
        {
            candidate["asset_status"]
            for candidates in candidates_by_asset_id.values()
            for candidate in candidates
            if isinstance(candidate.get("asset_status"), str) and candidate["asset_status"]
        }
    )
    potential_actions = _potential_identity_change_actions(lineage, candidates_by_asset_id)
    evaluation = {
        "rule": {
            "order": TERMINAL_FALLBACK_RULE_ORDER,
            "name": TERMINAL_FALLBACK_RULE_NAME,
        },
        "eligible": eligible,
        "logical_operator": "all",
        "criteria": criteria,
        "requested_symbol": requested_symbol,
        "candidate_alpaca_asset_ids": candidate_asset_ids,
        "asof_symbol_lookup_candidate_asset_ids": current_lookup_asset_ids,
        "session_coverage": coverage,
        "supplementary_corroboration": {
            "candidate_asset_names": candidate_names,
            "candidate_asset_names_match": len(candidate_names) == 1,
            "candidate_asset_statuses": candidate_statuses,
            "potential_identity_change_explanation_available": bool(potential_actions),
            "potential_identity_change_actions": potential_actions,
        },
    }

    if not eligible:
        return {
            CORRELATED_BRANCH: correlated_segments,
            UNCORRELATED_BRANCH: [
                {**segment, "terminal_correlation_fallback": evaluation} for segment in uncorrelated_segments
            ],
        }

    security_identity = security_identity_for_alpaca_assets(
        candidate_asset_ids,
        uuid_lineage_edges,
        tickers=(requested_symbol,),
        basis=TERMINAL_FALLBACK_RULE_NAME,
    )
    promoted_segments = []
    for segment in uncorrelated_segments:
        promoted_segment = dict(segment)
        prior_reason_codes = promoted_segment.pop("reason_codes")
        promoted_segment.update(
            {
                "correlation_rule": evaluation["rule"],
                "warnings": list(
                    dict.fromkeys((*promoted_segment.get("warnings", []), TERMINAL_FALLBACK_WARNING))
                ),
                "historical_symbol": requested_symbol,
                "canonical_security": {
                    "security_id": security_identity.security_id,
                    "alpaca_asset_id": None,
                    "cusip": None,
                },
                "security_identity": security_identity.as_report_object(),
                "identity_confidence": "continuous_ticker_history_with_unresolved_vendor_boundary",
                "correlation_method": TERMINAL_FALLBACK_RULE_NAME,
                "ticker_ownership_interval": None,
                "prior_uncorrelated_reason_codes": prior_reason_codes,
                "terminal_correlation_fallback": evaluation,
            }
        )
        promoted_segments.append(promoted_segment)
    return {
        CORRELATED_BRANCH: promoted_segments,
        UNCORRELATED_BRANCH: [],
    }

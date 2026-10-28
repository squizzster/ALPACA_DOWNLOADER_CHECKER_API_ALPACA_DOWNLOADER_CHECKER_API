from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
    expected_market_session_coverage,
    us_market_hours_sha256,
)

from .action_identity import exact_cusip_lookup_asset
from .security_identity import security_identity_for_alpaca_assets
from .settings import CORRELATED_BRANCH, UNCORRELATED_BRANCH
from .terminal_episode import has_informative_trading, has_prolonged_carry_forward
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES, UuidLineageEdge

TERMINAL_FALLBACK_RULE_NAME = "stable_continuous_ticker_identity_set"
TERMINAL_FALLBACK_RULE_ORDER = 8
TERMINAL_FALLBACK_WARNING = (
    "alpaca_uuid_candidates_joined_by_continuous_ticker_history_with_unresolved_vendor_boundaries"
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
    requested_symbol: str,
) -> tuple[
    list[str],
    list[str],
    dict[str, list[dict[str, Any]]],
    list[str],
    bool,
]:
    matching_candidate_sets = []
    all_candidate_asset_ids = set()
    matching_candidates_by_asset_id: dict[str, list[dict[str, Any]]] = {}
    excluded_candidate_asset_ids = set()
    for segment in uncorrelated_segments:
        candidates = segment.get("candidate_securities")
        if not isinstance(candidates, list):
            matching_candidate_sets.append(())
            continue
        segment_matching_asset_ids = []
        for candidate in candidates:
            if not isinstance(candidate, dict) or not isinstance(candidate.get("asset_id"), str):
                continue
            asset_id = candidate["asset_id"]
            all_candidate_asset_ids.add(asset_id)
            if candidate.get("asset_symbol") != requested_symbol:
                excluded_candidate_asset_ids.add(asset_id)
                continue
            segment_matching_asset_ids.append(asset_id)
            matching_candidates_by_asset_id.setdefault(asset_id, []).append(candidate)
        matching_candidate_sets.append(tuple(sorted(set(segment_matching_asset_ids))))

    stable_matching_set = (
        bool(matching_candidate_sets)
        and bool(matching_candidate_sets[0])
        and all(candidate_set == matching_candidate_sets[0] for candidate_set in matching_candidate_sets)
    )
    matching_candidate_asset_ids = list(matching_candidate_sets[0]) if stable_matching_set else []
    return (
        sorted(all_candidate_asset_ids),
        matching_candidate_asset_ids,
        matching_candidates_by_asset_id,
        sorted(excluded_candidate_asset_ids),
        stable_matching_set,
    )


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


def _candidate_identity_components(candidate_asset_ids, edges):
    remaining = set(candidate_asset_ids)
    components = []
    while remaining:
        connected = {remaining.pop()}
        while True:
            expanded = connected | {
                asset_id
                for edge in edges
                if connected.intersection((edge.predecessor_asset_id, edge.successor_asset_id))
                for asset_id in (edge.predecessor_asset_id, edge.successor_asset_id)
            }
            if expanded == connected:
                break
            connected = expanded
        components.append(tuple(sorted(connected.intersection(candidate_asset_ids))))
        remaining.difference_update(connected)
    return tuple(sorted(components))


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


def _anchored_split_component(symbol, lineage, candidate_ids, edges):
    if len(candidate_ids) < 2:
        return None
    connected = {candidate_ids[0]}
    while True:
        expanded = connected | {
            asset_id
            for edge in edges
            if edge.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
            and edge.predecessor_symbol == edge.successor_symbol == symbol
            and connected.intersection((edge.predecessor_asset_id, edge.successor_asset_id))
            for asset_id in (edge.predecessor_asset_id, edge.successor_asset_id)
        }
        if expanded == connected:
            break
        connected = expanded
    identity = security_identity_for_alpaca_assets(
        candidate_ids, edges, tickers=(symbol,), basis=TERMINAL_FALLBACK_RULE_NAME
    )
    if connected != set(candidate_ids) or set(identity.member_alpaca_asset_ids) != connected:
        return None
    anchors = [
        lookup
        for lookup in lineage.get("asset_lookups", [])
        if lookup.get("lookup_kind") == "symbol"
        and lookup.get("lookup") == symbol
        and lookup.get("found") is True
        and lookup.get("outcome") == "found"
        and lookup.get("http_status") == 200
        and isinstance(lookup.get("asset"), dict)
        and lookup["asset"].get("id") in connected
        and lookup["asset"].get("symbol") == symbol
        and lookup["asset"].get("class") == "us_equity"
    ]
    if len(anchors) != 1:
        return None
    return connected, anchors[0]["asset"]


def _source_label_assets(symbol, lineage):
    assets = [item for item in lineage.get("identities", []) if item.get("symbol") == symbol]
    assets.extend(
        item["asset"]
        for item in lineage.get("asset_lookups", [])
        if item.get("found") is True
        and isinstance(item.get("asset"), dict)
        and (
            item["asset"].get("symbol") == symbol
            or item.get("lookup_kind") == "symbol"
            and item.get("lookup") == symbol
        )
    )
    known = {asset.get("id") for asset in assets}
    for field in ("symbol_occupancy_evidence", "input_symbol_occupants"):
        known.update(
            item.get("asset_id")
            for item in lineage.get(field, [])
            if item.get("symbol") == symbol and item.get("asset_id")
        )
    return assets, known


def _inactive_observed_split_component(symbol, lineage, candidate_ids, edges, first_session, last_session):
    component = _anchored_split_component(symbol, lineage, candidate_ids, edges)
    if component is None:
        return None
    connected, anchor = component
    split_ids = {
        edge.action_id
        for edge in edges
        if edge.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
        and edge.predecessor_symbol == edge.successor_symbol == symbol
        and first_session <= edge.effective_date <= last_session
        and {edge.predecessor_asset_id, edge.successor_asset_id}.issubset(connected)
    }
    if not split_ids or any(
        action.get("action_type") in IDENTITY_CHANGE_ACTION_TYPES
        for action in lineage.get("corporate_actions", [])
    ):
        return None
    assets, known = _source_label_assets(symbol, lineage)
    if known != connected or any(
        asset.get("status") != "inactive" or asset.get("symbol") != symbol or asset.get("class") != "us_equity"
        for asset in assets
    ):
        return None
    # This attributes observations to an existing component. It does not infer
    # a legal termination event or manufacture an unobserved session.
    return {
        "asset_id": anchor["id"],
        "supporting_split_action_ids": sorted(split_ids),
        "observed_last_session": last_session.isoformat(),
        "termination_date_inferred": False,
    }


def _matching_dividend_overlap(earlier, later, start, end):
    """Compare the whole overlap, not just the payments shared by both records."""
    ledgers = []
    for dividends in (earlier, later):
        ledger = {}
        for session, payments in dividends.items():
            if not start <= session <= end:
                continue
            signatures = set()
            for payment in payments:
                try:
                    record = date.fromisoformat(payment["record_date"])
                    payable = date.fromisoformat(payment["payable_date"])
                    rate = Decimal(str(payment["rate"]))
                except (KeyError, TypeError, ValueError, InvalidOperation):
                    return False
                if not isinstance(payment.get("foreign"), bool) or not isinstance(payment.get("special"), bool):
                    return False
                signatures.add((rate, record, payable, payment["foreign"], payment["special"]))
            ledger[session] = signatures
        ledgers.append(ledger)
    return bool(ledgers[0]) and ledgers[0] == ledgers[1]


def _inactive_successive_dividend_history(symbol, lineage, candidate_ids, first_session, last_session):
    assets, known = _source_label_assets(symbol, lineage)
    if (
        len(candidate_ids) < 2
        or known != set(candidate_ids)
        or any(
            asset.get("symbol") != symbol
            or asset.get("status") != "inactive"
            or asset.get("class") != "us_equity"
            for asset in assets
        )
    ):
        return None
    if lineage.get("symbol_changes") or any(
        item.get("asset_id") in known and item.get("symbol") != symbol
        for item in lineage.get("symbol_occupancy_evidence", [])
    ):
        return None
    signatures = {(asset.get("name"), asset.get("class"), asset.get("exchange")) for asset in assets}
    if len(signatures) != 1 or not all(next(iter(signatures))):
        return None
    anchors = [
        item["asset"]
        for item in lineage.get("asset_lookups", [])
        if item.get("lookup_kind") == "symbol"
        and item.get("lookup") == symbol
        and item.get("found") is True
        and item.get("outcome") == "found"
        and item.get("http_status") == 200
        and isinstance(item.get("asset"), dict)
    ]
    if len(anchors) != 1 or anchors[0].get("id") not in known:
        return None
    terminal = [
        action for action in lineage.get("corporate_actions", []) if action.get("action_type") != "cash_dividend"
    ]
    if len(terminal) != 1:
        return None
    action = terminal[0]
    if (
        action.get("action_type") not in ("cash_merger", "stock_and_cash_merger", "stock_merger")
        or action.get("acquiree_symbol") != symbol
    ):
        return None
    endpoint = exact_cusip_lookup_asset(lineage, action.get("acquiree_cusip"))
    if endpoint is None or endpoint.get("id") != anchors[0]["id"]:
        return None
    try:
        when = date.fromisoformat(action["effective_date"])
        rates = [
            Decimal(str(action[field]))
            for field in (
                ("acquiree_rate", "acquirer_rate") if action["action_type"] == "stock_merger" else ("cash_rate",)
            )
        ]
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None
    if when < last_session or any(not rate.is_finite() or rate <= 0 for rate in rates):
        return None
    if action["action_type"] == "stock_merger":
        acquirer = exact_cusip_lookup_asset(lineage, action.get("acquirer_cusip"))
        if (
            acquirer is None
            or acquirer["id"] in known
            or not action.get("acquirer_symbol")
            or action["acquirer_symbol"] == symbol
            or acquirer.get("symbol") != action["acquirer_symbol"]
        ):
            return None
        try:
            if (
                when > last_session
                and expected_market_session_coverage(last_session.isoformat(), when.isoformat()).session_count
                > 4
            ):
                return None
        except MarketCalendarError:
            return None
    epochs = {asset_id: {} for asset_id in candidate_ids}
    for dividend in lineage.get("corporate_actions", []):
        if (
            dividend.get("action_type") != "cash_dividend"
            or dividend.get("symbol") != symbol
            or not dividend.get("cusip")
        ):
            continue
        owner = exact_cusip_lookup_asset(lineage, dividend["cusip"])
        if owner is None or owner.get("id") not in epochs:
            return None
        try:
            session = date.fromisoformat(dividend["ex_date"])
            rate = Decimal(str(dividend["rate"]))
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return None
        if not rate.is_finite() or rate <= 0 or not first_session <= session <= last_session:
            return None
        epochs[owner["id"]].setdefault(session, []).append(dividend)
    if any(not sessions for sessions in epochs.values()):
        return None
    ordered = sorted((min(sessions), max(sessions), asset_id) for asset_id, sessions in epochs.items())
    if ordered[-1][2] != anchors[0]["id"]:
        return None
    for earlier, later in zip(ordered, ordered[1:]):
        if earlier[0] >= later[0] or earlier[1] >= later[1]:
            return None
        if earlier[1] >= later[0] and not _matching_dividend_overlap(
            epochs[earlier[2]], epochs[later[2]], later[0], earlier[1]
        ):
            return None
    # Dated dividends corroborate successive occupants; their dates are not
    # extrapolated into UUID cutovers. Identical overlapping distributions are
    # provider backfill corroboration, not two competing economic payees.
    # Historical UUID/CUSIP fields stay null; the acquirer is not joined.
    return {
        "asset_id": anchors[0]["id"],
        "terminal_corporate_action_id": action.get("id"),
        "successive_dividend_occupants": [item[2] for item in ordered],
        "observed_last_session": last_session.isoformat(),
        "termination_date_inferred": False,
        "uuid_cutover_dates_inferred": False,
    }


def _split_component_departure(symbol, lineage, candidate_ids, edges, last_session):
    component = _anchored_split_component(symbol, lineage, candidate_ids, edges)
    if component is None:
        return None
    connected, _ = component
    departures = []
    for action in lineage.get("corporate_actions", []):
        kind = action.get("action_type")
        role = "old" if kind == "name_change" else "acquiree"
        if kind not in ("name_change", "cash_merger", "stock_merger") or action.get(role + "_symbol") != symbol:
            continue
        asset = exact_cusip_lookup_asset(lineage, action.get(role + "_cusip"))
        if asset is None or asset["id"] not in connected or asset.get("status") != "inactive":
            continue
        try:
            when = date.fromisoformat(action["process_date" if kind == "name_change" else "effective_date"])
            if when < last_session or (
                when > last_session
                and expected_market_session_coverage(last_session.isoformat(), when.isoformat()).session_count
                > 4
            ):
                continue
        except (KeyError, TypeError, ValueError, MarketCalendarError):
            continue
        departures.append((asset["id"], when.isoformat(), action.get("id")))
    if not departures or len({item[0] for item in departures}) != 1:
        return None
    return {
        "asset_id": departures[0][0],
        "departure_dates": sorted({item[1] for item in departures}),
        "supporting_corporate_action_ids": sorted({item[2] for item in departures if item[2]}),
    }


def apply_terminal_continuous_ticker_fallback(
    branches: dict[str, list[dict[str, Any]]],
    session_dates: tuple[date, ...],
    source: dict[str, Any],
    lineage: dict[str, Any],
    uuid_lineage_edges: list[UuidLineageEdge],
    *,
    source_rows=None,
    adjustment_consensus=None,
    closed_history_evidence=None,
    rule_order=TERMINAL_FALLBACK_RULE_ORDER,
) -> dict[str, list[dict[str, Any]]]:
    """Promote only a fully unresolved, anchored, gapless ticker identity set."""
    # Consume established row decisions first: this fallback is not authority to
    # discard resolved history or reopen unrelated sources. Any new membership
    # inference stays subject to the selected group's continuity/evidence gates.
    correlated_segments = branches[CORRELATED_BRANCH]
    uncorrelated_segments = branches[UNCORRELATED_BRANCH]
    requested_symbol = source["requested_symbol"]
    (
        all_candidate_asset_ids,
        matching_candidate_asset_ids,
        matching_candidates_by_asset_id,
        excluded_candidate_asset_ids,
        stable_matching_set,
    ) = _candidate_state(uncorrelated_segments, requested_symbol)
    current_lookup_asset_ids = _asof_symbol_lookup_asset_ids(
        lineage,
        requested_symbol,
        matching_candidate_asset_ids,
    )
    candidate_names = sorted(
        {
            candidate["asset_name"]
            for candidates in matching_candidates_by_asset_id.values()
            for candidate in candidates
            if isinstance(candidate.get("asset_name"), str) and candidate["asset_name"]
        }
    )
    candidate_names_match = len(candidate_names) == 1
    potential_actions = _potential_identity_change_actions(
        lineage,
        matching_candidates_by_asset_id,
    )
    identity_components = _candidate_identity_components(
        matching_candidate_asset_ids,
        uuid_lineage_edges,
    )
    creates_disconnected_union = len(identity_components) > 1
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
    departure = None
    if closed_history_evidence is None:
        closed_history_evidence = (
            source_rows is not None
            and has_informative_trading(source_rows)
            and not has_prolonged_carry_forward(source_rows)
        )
    if (
        calendar_evaluated
        and not no_missing_sessions
        and session_dates
        and not excluded_candidate_asset_ids
        and set(all_candidate_asset_ids) == set(matching_candidate_asset_ids)
        and closed_history_evidence
        and adjustment_consensus is not None
        and adjustment_consensus.authenticated_adjustment_count == 8
        and adjustment_consensus.all_authenticated_adjustments_agree
    ):
        departure = _split_component_departure(
            requested_symbol, lineage, matching_candidate_asset_ids, uuid_lineage_edges, session_dates[-1]
        )
        closure_field = "explained_terminal_departure"
        if departure is None:
            departure = _inactive_observed_split_component(
                requested_symbol,
                lineage,
                matching_candidate_asset_ids,
                uuid_lineage_edges,
                session_dates[0],
                session_dates[-1],
            )
            closure_field = "observed_inactive_split_component"
        if departure is None:
            departure = _inactive_successive_dividend_history(
                requested_symbol, lineage, matching_candidate_asset_ids, session_dates[0], session_dates[-1]
            )
            closure_field = "observed_inactive_successive_dividend_history"
        if departure is not None:
            closed_source = {**source, "request_to": (session_dates[-1] + timedelta(days=1)).isoformat()}
            closed_coverage, closed_evaluated, closed_complete, closed_expected = _session_coverage(
                session_dates, closed_source
            )
            if closed_evaluated and closed_complete and closed_expected:
                coverage = {
                    **closed_coverage,
                    "actual_request_end_exclusive": source.get("request_to"),
                    "actual_request_missing_session_count": coverage["missing_session_count"],
                    closure_field: departure,
                }
                calendar_evaluated = no_missing_sessions = no_unexpected_sessions = True
            else:
                departure = None
    criteria = {
        "all_rows_initially_uncorrelated": all_rows_initially_uncorrelated,
        "ambiguous_identity_is_only_uncorrelated_reason": ambiguous_identity_only,
        "stable_matching_ticker_alpaca_asset_id_candidates": stable_matching_set,
        "exactly_one_asof_symbol_lookup_candidate": len(current_lookup_asset_ids) == 1,
        "matching_ticker_candidate_asset_names_agree": candidate_names_match,
        "market_calendar_evaluation_succeeded": calendar_evaluated,
        "no_missing_expected_market_sessions": no_missing_sessions,
        "no_unexpected_market_sessions": no_unexpected_sessions,
        "candidate_identity_continuity_supported": (
            not creates_disconnected_union
            or departure is not None
            or len(identity_components) == 2
            and closed_history_evidence is True
            and bool(potential_actions)
        ),
    }
    eligible = all(value is True for value in criteria.values())
    candidate_statuses = sorted(
        {
            candidate["asset_status"]
            for candidates in matching_candidates_by_asset_id.values()
            for candidate in candidates
            if isinstance(candidate.get("asset_status"), str) and candidate["asset_status"]
        }
    )
    evaluation = {
        "rule": {
            "order": rule_order,
            "name": TERMINAL_FALLBACK_RULE_NAME,
        },
        "eligible": eligible,
        "logical_operator": "all",
        "criteria": criteria,
        "requested_symbol": requested_symbol,
        "candidate_alpaca_asset_ids": all_candidate_asset_ids,
        "matching_ticker_candidate_alpaca_asset_ids": matching_candidate_asset_ids,
        "excluded_nonmatching_ticker_candidate_alpaca_asset_ids": excluded_candidate_asset_ids,
        "asof_symbol_lookup_candidate_asset_ids": current_lookup_asset_ids,
        "candidate_identity_components": [list(component) for component in identity_components],
        "creates_disconnected_identity_union": creates_disconnected_union,
        "session_coverage": coverage,
        "supplementary_corroboration": {
            "candidate_asset_names": candidate_names,
            "candidate_asset_names_match": candidate_names_match,
            "candidate_asset_statuses": candidate_statuses,
            "potential_identity_change_explanation_available": bool(potential_actions),
            "potential_identity_change_actions": potential_actions,
            "complete_informative_history_without_prolonged_carry_forward": (closed_history_evidence is True),
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
        matching_candidate_asset_ids,
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

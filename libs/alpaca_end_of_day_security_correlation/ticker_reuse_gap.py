from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
    market_session_dates_sha256,
    us_market_hours_sha256,
)

from .evidence import TickerOwnershipInterval

TICKER_REUSE_GAP_MINIMUM_SESSIONS = 60
COMPLETE_CURRENT_SUFFIX_MINIMUM_GAP_SESSIONS = 100
TICKER_REUSE_GAP_RULE_NAME = "ticker_reuse_gap_candidate_detector"
COMPLETE_CURRENT_SUFFIX_RULE_NAME = "complete_current_identity_suffix_after_ticker_reuse_gap"
INACTIVE_CURRENT_SUFFIX_RULE_NAME = "inactive_current_identity_suffix_after_ticker_reuse_gap"
DATED_TERMINAL_SUFFIX_RULE_NAME = "dated_terminal_identity_suffix_after_ticker_reuse_gap"
TERMINAL_OWNERSHIP_INTERVAL_RULE_NAMES = frozenset(
    (
        "name_change_ticker_transition",
        "same_ticker_stock_merger_uuid_transition",
        "cash_merger_acquiree_terminal_boundary",
    )
)


@dataclass(frozen=True)
class AdjustmentSessionConsensus:
    authenticated_adjustment_count: int
    authenticated_adjustments: tuple[str, ...]
    all_authenticated_adjustments_agree: bool
    observed_session_dates_sha256: str | None

    def as_report_object(self) -> dict[str, Any]:
        return {
            "authenticated_adjustment_count": self.authenticated_adjustment_count,
            "authenticated_adjustments": list(self.authenticated_adjustments),
            "all_authenticated_adjustments_agree": self.all_authenticated_adjustments_agree,
            "observed_session_dates_sha256": self.observed_session_dates_sha256,
        }


@dataclass(frozen=True)
class DatedTerminalSuffixIdentity:
    interval: TickerOwnershipInterval
    asset_symbol: str
    asset_name: str
    asset_exchange: str
    asset_status: str | None
    expected_session_count: int
    expected_session_dates_sha256: str
    undated_symbol_lookup_asset_id: str | None

    @property
    def undated_symbol_lookup_uuid_conflict(self) -> bool:
        return (
            self.undated_symbol_lookup_asset_id is not None
            and self.undated_symbol_lookup_asset_id != self.interval.asset_id
        )

    def as_report_object(self) -> dict[str, Any]:
        return {
            "alpaca_asset_id": self.interval.asset_id,
            "cusip": self.interval.cusip,
            "historical_symbol": self.interval.historical_symbol,
            "asset_symbol": self.asset_symbol,
            "name": self.asset_name,
            "exchange": self.asset_exchange,
            "status": self.asset_status,
            "start_date_inclusive": (
                self.interval.start_date_inclusive.isoformat()
                if self.interval.start_date_inclusive is not None
                else None
            ),
            "terminal_boundary_exclusive": self.interval.end_date_exclusive.isoformat(),
            "supporting_corporate_action_ids": list(
                dict.fromkeys(
                    action_id for rule in self.interval.applied_rules for action_id in rule.supporting_action_ids
                )
            ),
            "expected_session_count": self.expected_session_count,
            "expected_session_dates_sha256": self.expected_session_dates_sha256,
            "undated_symbol_lookup_asset_id": self.undated_symbol_lookup_asset_id,
            "undated_symbol_lookup_uuid_conflict": self.undated_symbol_lookup_uuid_conflict,
        }


@dataclass(frozen=True)
class TickerReuseGapAssessment:
    left_observed_session: date
    right_observed_session: date
    gap_first_session: date
    gap_last_session: date
    gap_session_count: int
    gap_session_dates_sha256: str
    rightmost_run_last_session: date
    rightmost_run_session_count: int
    request_end_exclusive: str
    rightmost_run_expected_session_count: int | None
    rightmost_run_expected_session_dates_sha256: str | None
    rightmost_run_complete: bool
    rightmost_run_complete_through_last_observed: bool
    no_unexpected_observed_sessions: bool
    current_asset_id: str | None
    current_asset_symbol: str | None
    current_asset_name: str | None
    current_asset_cusip: str | None
    current_asset_exchange: str | None
    current_asset_status: str | None
    adjustment_consensus: AdjustmentSessionConsensus
    left_prefix_has_current_asset_uuid_evidence: bool
    left_prefix_current_asset_uuid_evidence: tuple[dict[str, Any], ...]
    right_suffix_conflicting_asset_uuid_evidence: tuple[dict[str, Any], ...]
    dated_terminal_suffix_identity: DatedTerminalSuffixIdentity | None
    left_prefix_has_dated_terminal_asset_uuid_evidence: bool
    left_prefix_dated_terminal_asset_uuid_evidence: tuple[dict[str, Any], ...]
    dated_terminal_suffix_veto_issues: tuple[dict[str, Any], ...]

    @property
    def other_publication_gates_pass(self) -> bool:
        return (
            self.gap_session_count >= COMPLETE_CURRENT_SUFFIX_MINIMUM_GAP_SESSIONS
            and self.no_unexpected_observed_sessions
            and self.adjustment_consensus.all_authenticated_adjustments_agree
            and self.current_asset_id is not None
            and self.current_asset_symbol is not None
            and self.current_asset_name is not None
            and self.current_asset_exchange is not None
            and not self.left_prefix_has_current_asset_uuid_evidence
        )

    @property
    def complete_current_suffix_eligible(self) -> bool:
        return self.other_publication_gates_pass and self.rightmost_run_complete

    @property
    def inactive_current_suffix_eligible(self) -> bool:
        return (
            self.other_publication_gates_pass
            and not self.rightmost_run_complete
            and self.rightmost_run_complete_through_last_observed
            and self.current_asset_status == "inactive"
            and not self.right_suffix_conflicting_asset_uuid_evidence
        )

    @property
    def active_current_suffix_flagged(self) -> bool:
        return (
            self.other_publication_gates_pass
            and not self.rightmost_run_complete
            and self.rightmost_run_complete_through_last_observed
            and self.current_asset_status == "active"
            and not self.right_suffix_conflicting_asset_uuid_evidence
        )

    @property
    def dated_terminal_suffix_eligible(self) -> bool:
        return (
            self.gap_session_count >= COMPLETE_CURRENT_SUFFIX_MINIMUM_GAP_SESSIONS
            and self.no_unexpected_observed_sessions
            and self.adjustment_consensus.all_authenticated_adjustments_agree
            and self.dated_terminal_suffix_identity is not None
            and not self.left_prefix_has_dated_terminal_asset_uuid_evidence
        )

    @property
    def eligible(self) -> bool:
        return (
            self.complete_current_suffix_eligible
            or self.inactive_current_suffix_eligible
            or self.dated_terminal_suffix_eligible
        )

    def as_report_object(self) -> dict[str, Any]:
        return {
            "rule_name": TICKER_REUSE_GAP_RULE_NAME,
            "minimum_gap_session_count": TICKER_REUSE_GAP_MINIMUM_SESSIONS,
            "complete_current_suffix_rule": {
                "rule_name": COMPLETE_CURRENT_SUFFIX_RULE_NAME,
                "minimum_gap_session_count": COMPLETE_CURRENT_SUFFIX_MINIMUM_GAP_SESSIONS,
                "cusip_required": False,
                "eligible": self.complete_current_suffix_eligible,
            },
            "inactive_current_suffix_rule": {
                "rule_name": INACTIVE_CURRENT_SUFFIX_RULE_NAME,
                "requires_exact_status": "inactive",
                "eligible": self.inactive_current_suffix_eligible,
                "active_asset_flagged": self.active_current_suffix_flagged,
            },
            "dated_terminal_suffix_rule": {
                "rule_name": DATED_TERMINAL_SUFFIX_RULE_NAME,
                "minimum_gap_session_count": COMPLETE_CURRENT_SUFFIX_MINIMUM_GAP_SESSIONS,
                "requires_exact_cusip_lookup": True,
                "requires_complete_session_grid_to_terminal_boundary": True,
                "eligible": self.dated_terminal_suffix_eligible,
                "identity": (
                    self.dated_terminal_suffix_identity.as_report_object()
                    if self.dated_terminal_suffix_identity is not None
                    else None
                ),
                "left_prefix_has_same_dated_uuid_evidence": (
                    self.left_prefix_has_dated_terminal_asset_uuid_evidence
                ),
                "left_prefix_same_uuid_evidence": list(self.left_prefix_dated_terminal_asset_uuid_evidence),
                "veto_issues": list(self.dated_terminal_suffix_veto_issues),
            },
            "eligible": self.eligible,
            "left_observed_session": self.left_observed_session.isoformat(),
            "right_observed_session": self.right_observed_session.isoformat(),
            "gap_first_session": self.gap_first_session.isoformat(),
            "gap_last_session": self.gap_last_session.isoformat(),
            "gap_session_count": self.gap_session_count,
            "gap_session_dates_sha256": self.gap_session_dates_sha256,
            "rightmost_run_last_session": self.rightmost_run_last_session.isoformat(),
            "rightmost_run_session_count": self.rightmost_run_session_count,
            "request_end_exclusive": self.request_end_exclusive,
            "rightmost_run_expected_session_count": self.rightmost_run_expected_session_count,
            "rightmost_run_expected_session_dates_sha256": (self.rightmost_run_expected_session_dates_sha256),
            "rightmost_run_complete": self.rightmost_run_complete,
            "rightmost_run_complete_through_last_observed": (self.rightmost_run_complete_through_last_observed),
            "no_unexpected_observed_sessions": self.no_unexpected_observed_sessions,
            "adjustment_consensus": self.adjustment_consensus.as_report_object(),
            "left_prefix_has_current_asset_uuid_evidence": (self.left_prefix_has_current_asset_uuid_evidence),
            "left_prefix_current_asset_uuid_evidence": list(self.left_prefix_current_asset_uuid_evidence),
            "right_suffix_conflicting_asset_uuid_evidence": list(
                self.right_suffix_conflicting_asset_uuid_evidence
            ),
            "calendar_sha256": us_market_hours_sha256(),
            "current_asset": (
                {
                    "alpaca_asset_id": self.current_asset_id,
                    "symbol": self.current_asset_symbol,
                    "name": self.current_asset_name,
                    "cusip": self.current_asset_cusip,
                    "exchange": self.current_asset_exchange,
                    "status": self.current_asset_status,
                }
                if self.current_asset_id is not None
                else None
            ),
        }


def adjustment_session_consensus(
    adjustment_sessions: Iterable[tuple[str, tuple[date, ...]]],
) -> AdjustmentSessionConsensus:
    items = tuple(adjustment_sessions)
    session_sets = tuple(sessions for _, sessions in items)
    all_agree = bool(items) and all(sessions == session_sets[0] for sessions in session_sets[1:])
    session_dates_sha256 = (
        market_session_dates_sha256(session.isoformat() for session in session_sets[0]) if all_agree else None
    )
    return AdjustmentSessionConsensus(
        authenticated_adjustment_count=len(items),
        authenticated_adjustments=tuple(name for name, _ in items),
        all_authenticated_adjustments_agree=all_agree,
        observed_session_dates_sha256=session_dates_sha256,
    )


def _unique_named_current_asset(
    lineage: dict[str, Any],
    requested_symbol: str,
) -> tuple[str, str, str, str | None, str, str | None] | None:
    asset_lookups = lineage.get("asset_lookups")
    if not isinstance(asset_lookups, list):
        return None
    lookup_candidates = set()
    for lookup in asset_lookups:
        if (
            not isinstance(lookup, dict)
            or lookup.get("lookup_kind") != "symbol"
            or lookup.get("lookup") != requested_symbol
            or lookup.get("found") is not True
        ):
            continue
        asset = lookup.get("asset")
        if not isinstance(asset, dict) or asset.get("symbol") != requested_symbol:
            continue
        asset_id = asset.get("id")
        asset_name = asset.get("name")
        asset_exchange = asset.get("exchange")
        asset_status = asset.get("status")
        if not all(
            isinstance(value, str) and bool(value.strip()) for value in (asset_id, asset_name, asset_exchange)
        ):
            continue
        lookup_candidates.add(
            (
                asset_id,
                requested_symbol,
                asset_name,
                asset_exchange,
                asset_status if isinstance(asset_status, str) else None,
            )
        )
    if len(lookup_candidates) != 1:
        return None

    asset_id, asset_symbol, asset_name, asset_exchange, asset_status = next(iter(lookup_candidates))
    cusips = set()
    names = {asset_name}
    exchanges = {asset_exchange}
    matching_identity_count = 0
    identities = lineage.get("identities")
    for identity in identities if isinstance(identities, list) else ():
        if not isinstance(identity, dict) or identity.get("id") != asset_id:
            continue
        matching_identity_count += 1
        identity_name = identity.get("name")
        identity_exchange = identity.get("exchange")
        if isinstance(identity_name, str) and identity_name.strip():
            names.add(identity_name)
        if isinstance(identity_exchange, str) and identity_exchange.strip():
            exchanges.add(identity_exchange)
        identity_cusip = identity.get("cusip")
        if isinstance(identity_cusip, str) and identity_cusip:
            cusips.add(identity_cusip)
        lookup_cusips = identity.get("lookup_cusips")
        if isinstance(lookup_cusips, list):
            cusips.update(cusip for cusip in lookup_cusips if isinstance(cusip, str) and cusip)

    if matching_identity_count == 0 or len(names) != 1 or len(exchanges) != 1 or len(cusips) > 1:
        return None
    return (
        asset_id,
        asset_symbol,
        asset_name,
        next(iter(cusips)) if cusips else None,
        asset_exchange,
        asset_status,
    )


def _unique_named_exact_cusip_asset(
    lineage: dict[str, Any],
    interval: TickerOwnershipInterval,
) -> tuple[str, str, str, str, str | None] | None:
    asset_lookups = lineage.get("asset_lookups")
    if not isinstance(asset_lookups, list):
        return None
    candidates = set()
    for lookup in asset_lookups:
        if (
            not isinstance(lookup, dict)
            or lookup.get("lookup_kind") != "cusip"
            or lookup.get("lookup") != interval.cusip
            or lookup.get("found") is not True
        ):
            continue
        asset = lookup.get("asset")
        if not isinstance(asset, dict) or asset.get("id") != interval.asset_id:
            continue
        asset_symbol = asset.get("symbol")
        asset_name = asset.get("name")
        asset_exchange = asset.get("exchange")
        asset_status = asset.get("status")
        if not all(
            isinstance(value, str) and bool(value.strip())
            for value in (asset_symbol, asset_name, asset_exchange)
        ):
            continue
        candidates.add(
            (
                interval.asset_id,
                asset_symbol,
                asset_name,
                asset_exchange,
                asset_status if isinstance(asset_status, str) else None,
            )
        )
    if len(candidates) != 1:
        return None
    return next(iter(candidates))


def _terminal_action_ids(
    lineage: dict[str, Any],
    interval: TickerOwnershipInterval,
) -> tuple[str, ...] | None:
    actions = lineage.get("corporate_actions")
    if not isinstance(actions, list) or interval.end_date_exclusive is None:
        return None
    actions_by_id = {
        action["id"]: action
        for action in actions
        if isinstance(action, dict) and isinstance(action.get("id"), str)
    }
    matched_boundaries = []
    for rule in interval.applied_rules:
        if rule.name not in TERMINAL_OWNERSHIP_INTERVAL_RULE_NAMES:
            continue
        for action_id in rule.supporting_action_ids:
            action = actions_by_id.get(action_id)
            if action is None:
                return None
            action_type = action.get("action_type")
            if rule.name == "name_change_ticker_transition":
                matches_terminal_identity = (
                    action_type == "name_change"
                    and action.get("old_symbol") == interval.historical_symbol
                    and action.get("old_cusip") == interval.cusip
                )
                boundary_value = action.get("process_date")
            elif rule.name == "same_ticker_stock_merger_uuid_transition":
                matches_terminal_identity = (
                    action_type == "stock_merger"
                    and action.get("acquiree_symbol") == interval.historical_symbol
                    and action.get("acquiree_cusip") == interval.cusip
                )
                boundary_value = action.get("effective_date") or action.get("process_date")
            else:
                matches_terminal_identity = (
                    action_type == "cash_merger"
                    and action.get("acquiree_symbol") == interval.historical_symbol
                    and action.get("acquiree_cusip") == interval.cusip
                )
                boundary_value = action.get("effective_date") or action.get("process_date")
            if not matches_terminal_identity:
                continue
            if not isinstance(boundary_value, str):
                return None
            try:
                boundary = date.fromisoformat(boundary_value)
            except ValueError:
                return None
            matched_boundaries.append((action_id, boundary))
    if not matched_boundaries or {boundary for _, boundary in matched_boundaries} != {
        interval.end_date_exclusive
    }:
        return None
    return tuple(dict.fromkeys(action_id for action_id, _ in matched_boundaries))


def _blocking_candidate_evidence_issues(
    evidence_issues: list[dict[str, Any]],
    supporting_action_ids: tuple[str, ...],
) -> tuple[dict[str, Any], ...]:
    supporting_ids = set(supporting_action_ids)
    blocking = []
    for issue in evidence_issues:
        reason_code = issue.get("reason_code") if isinstance(issue, dict) else None
        if not isinstance(reason_code, str) or not (
            reason_code.endswith("_boundary_disagreement")
            or reason_code.endswith("_conflicts_with_existing_ownership_intervals")
        ):
            continue
        issue_action_ids = set()
        action_id = issue.get("action_id")
        if isinstance(action_id, str):
            issue_action_ids.add(action_id)
        action_ids = issue.get("action_ids")
        if isinstance(action_ids, list):
            issue_action_ids.update(item for item in action_ids if isinstance(item, str))
        if supporting_ids & issue_action_ids:
            blocking.append(issue)
    return tuple(blocking)


def _dated_terminal_suffix_identity(
    intervals: list[TickerOwnershipInterval],
    lineage: dict[str, Any],
    requested_symbol: str,
    current_asset_id: str | None,
    right_sessions: tuple[date, ...],
    evidence_issues: list[dict[str, Any]],
) -> tuple[DatedTerminalSuffixIdentity | None, tuple[dict[str, Any], ...]]:
    if not right_sessions:
        return None, ()
    observed = tuple(session.isoformat() for session in right_sessions)
    candidates = []
    veto_issues = []
    for interval in intervals:
        terminal_action_ids = _terminal_action_ids(lineage, interval)
        if (
            interval.historical_symbol != requested_symbol
            or interval.end_date_exclusive is None
            or not interval.cusip
            or terminal_action_ids is None
            or not all(interval.contains(session) for session in right_sessions)
        ):
            continue
        exact_asset = _unique_named_exact_cusip_asset(
            lineage,
            interval,
        )
        if exact_asset is None:
            continue
        try:
            terminal_coverage = expected_market_session_coverage(
                observed[0],
                interval.end_date_exclusive.isoformat(),
            )
        except MarketCalendarError:
            continue
        if observed != terminal_coverage.session_dates:
            continue
        conflicting_intervals = [
            other
            for other in intervals
            if other.historical_symbol == requested_symbol
            and other.asset_id != interval.asset_id
            and any(other.contains(session) for session in right_sessions)
        ]
        if conflicting_intervals:
            veto_issues.append(
                {
                    "reason_code": "conflicting_dated_right_suffix_uuid",
                    "candidate_alpaca_asset_id": interval.asset_id,
                    "conflicting_alpaca_asset_ids": sorted({other.asset_id for other in conflicting_intervals}),
                }
            )
            continue
        candidate_evidence_issues = _blocking_candidate_evidence_issues(
            evidence_issues,
            terminal_action_ids,
        )
        if candidate_evidence_issues:
            veto_issues.extend(candidate_evidence_issues)
            continue
        _, asset_symbol, asset_name, asset_exchange, asset_status = exact_asset
        candidates.append(
            DatedTerminalSuffixIdentity(
                interval=interval,
                asset_symbol=asset_symbol,
                asset_name=asset_name,
                asset_exchange=asset_exchange,
                asset_status=asset_status,
                expected_session_count=terminal_coverage.session_count,
                expected_session_dates_sha256=terminal_coverage.session_dates_sha256,
                undated_symbol_lookup_asset_id=current_asset_id,
            )
        )
    unique_candidates = {candidate.interval.interval_key(): candidate for candidate in candidates}
    if len(unique_candidates) != 1:
        return None, tuple(veto_issues)
    return next(iter(unique_candidates.values())), tuple(veto_issues)


def _right_suffix_conflicting_asset_uuid_evidence(
    intervals: list[TickerOwnershipInterval],
    requested_symbol: str,
    current_asset_id: str | None,
    right_sessions: tuple[date, ...],
) -> tuple[dict[str, Any], ...]:
    if current_asset_id is None:
        return ()
    evidence = []
    for interval in intervals:
        if interval.historical_symbol != requested_symbol or interval.asset_id == current_asset_id:
            continue
        matching_sessions = tuple(session for session in right_sessions if interval.contains(session))
        if not matching_sessions:
            continue
        evidence.append(
            {
                "alpaca_asset_id": interval.asset_id,
                "cusip": interval.cusip,
                "first_session": matching_sessions[0].isoformat(),
                "last_session": matching_sessions[-1].isoformat(),
                "supporting_corporate_action_ids": list(
                    dict.fromkeys(
                        action_id for rule in interval.applied_rules for action_id in rule.supporting_action_ids
                    )
                ),
            }
        )
    return tuple(evidence)


def _left_prefix_current_asset_uuid_evidence(
    intervals: list[TickerOwnershipInterval],
    requested_symbol: str,
    current_asset_id: str | None,
    left_sessions: tuple[date, ...],
) -> tuple[dict[str, Any], ...]:
    if current_asset_id is None:
        return ()
    evidence = []
    for interval in intervals:
        if (
            interval.asset_id != current_asset_id
            or interval.historical_symbol != requested_symbol
            or interval.start_date_inclusive is None
        ):
            continue
        matching_sessions = tuple(session for session in left_sessions if interval.contains(session))
        if not matching_sessions:
            continue
        interval_report = interval.as_report_object()
        evidence.append(
            {
                "alpaca_asset_id": current_asset_id,
                "historical_symbol": interval.historical_symbol,
                "first_matching_left_session": matching_sessions[0].isoformat(),
                "last_matching_left_session": matching_sessions[-1].isoformat(),
                "matching_left_session_count": len(matching_sessions),
                "supporting_corporate_action_ids": sorted(
                    {
                        action_id
                        for rule in interval_report["ownership_interval_rules"]
                        for action_id in rule["supporting_corporate_action_ids"]
                    }
                ),
                "ticker_ownership_interval": interval_report,
            }
        )
    return tuple(evidence)


def assess_ticker_reuse_gap(
    session_dates: tuple[date, ...],
    source: dict[str, Any],
    lineage: dict[str, Any],
    intervals: list[TickerOwnershipInterval] | None = None,
    consensus: AdjustmentSessionConsensus | None = None,
    evidence_issues: list[dict[str, Any]] | None = None,
) -> TickerReuseGapAssessment | None:
    """Describe the rightmost pricing run after the last qualifying ticker-reuse gap."""
    if len(session_dates) < 2:
        return None
    observed = tuple(session_date.isoformat() for session_date in session_dates)
    request_end = source.get("request_to")
    if not isinstance(request_end, str):
        return None
    try:
        full_coverage = expected_market_session_coverage(observed[0], request_end)
    except MarketCalendarError:
        return None
    expected_positions = {
        session_date: position for position, session_date in enumerate(full_coverage.session_dates)
    }
    unexpected_observed = tuple(
        session_date for session_date in observed if session_date not in expected_positions
    )

    qualifying_boundaries: list[tuple[int, tuple[str, ...]]] = []
    for row_index, (left_session, right_session) in enumerate(zip(observed, observed[1:])):
        left_position = expected_positions.get(left_session)
        right_position = expected_positions.get(right_session)
        if left_position is None or right_position is None or right_position <= left_position:
            continue
        gap_sessions = full_coverage.session_dates[left_position + 1 : right_position]
        if len(gap_sessions) >= TICKER_REUSE_GAP_MINIMUM_SESSIONS:
            qualifying_boundaries.append((row_index, gap_sessions))
    if not qualifying_boundaries:
        return None

    boundary_row_index, gap_sessions = qualifying_boundaries[-1]
    rightmost_observed = observed[boundary_row_index + 1 :]
    try:
        rightmost_expected = expected_market_session_coverage(rightmost_observed[0], request_end)
    except MarketCalendarError:
        rightmost_expected = None
    current_asset = _unique_named_current_asset(lineage, source["requested_symbol"])
    (
        current_asset_id,
        current_asset_symbol,
        current_asset_name,
        current_asset_cusip,
        current_asset_exchange,
        current_asset_status,
    ) = current_asset if current_asset is not None else (None, None, None, None, None, None)
    resolved_consensus = consensus or adjustment_session_consensus(
        ((str(source.get("adjustment") or source.get("data_file") or "source"), session_dates),)
    )
    left_uuid_evidence = _left_prefix_current_asset_uuid_evidence(
        intervals or [],
        source["requested_symbol"],
        current_asset_id,
        session_dates[: boundary_row_index + 1],
    )
    right_uuid_conflict_evidence = _right_suffix_conflicting_asset_uuid_evidence(
        intervals or [],
        source["requested_symbol"],
        current_asset_id,
        session_dates[boundary_row_index + 1 :],
    )
    dated_terminal_identity, dated_terminal_veto_issues = _dated_terminal_suffix_identity(
        intervals or [],
        lineage,
        source["requested_symbol"],
        current_asset_id,
        session_dates[boundary_row_index + 1 :],
        evidence_issues or [],
    )
    left_dated_terminal_uuid_evidence = (
        _left_prefix_current_asset_uuid_evidence(
            intervals or [],
            source["requested_symbol"],
            dated_terminal_identity.interval.asset_id,
            session_dates[: boundary_row_index + 1],
        )
        if dated_terminal_identity is not None
        else ()
    )
    return TickerReuseGapAssessment(
        left_observed_session=session_dates[boundary_row_index],
        right_observed_session=session_dates[boundary_row_index + 1],
        gap_first_session=date.fromisoformat(gap_sessions[0]),
        gap_last_session=date.fromisoformat(gap_sessions[-1]),
        gap_session_count=len(gap_sessions),
        gap_session_dates_sha256=market_session_dates_sha256(gap_sessions),
        rightmost_run_last_session=session_dates[-1],
        rightmost_run_session_count=len(rightmost_observed),
        request_end_exclusive=request_end,
        rightmost_run_expected_session_count=(
            rightmost_expected.session_count if rightmost_expected is not None else None
        ),
        rightmost_run_expected_session_dates_sha256=(
            rightmost_expected.session_dates_sha256 if rightmost_expected is not None else None
        ),
        rightmost_run_complete=(
            rightmost_expected is not None
            and not unexpected_observed
            and rightmost_observed == rightmost_expected.session_dates
        ),
        rightmost_run_complete_through_last_observed=(
            rightmost_expected is not None
            and rightmost_observed == rightmost_expected.session_dates[: len(rightmost_observed)]
        ),
        no_unexpected_observed_sessions=not unexpected_observed,
        current_asset_id=current_asset_id,
        current_asset_symbol=current_asset_symbol,
        current_asset_name=current_asset_name,
        current_asset_cusip=current_asset_cusip,
        current_asset_exchange=current_asset_exchange,
        current_asset_status=current_asset_status,
        adjustment_consensus=resolved_consensus,
        left_prefix_has_current_asset_uuid_evidence=bool(left_uuid_evidence),
        left_prefix_current_asset_uuid_evidence=left_uuid_evidence,
        right_suffix_conflicting_asset_uuid_evidence=right_uuid_conflict_evidence,
        dated_terminal_suffix_identity=dated_terminal_identity,
        left_prefix_has_dated_terminal_asset_uuid_evidence=bool(left_dated_terminal_uuid_evidence),
        left_prefix_dated_terminal_asset_uuid_evidence=(left_dated_terminal_uuid_evidence),
        dated_terminal_suffix_veto_issues=dated_terminal_veto_issues,
    )

from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from libs.alpaca_security_lineage import EXCHANGE_TEST_SYMBOL, HISTORICAL_MARKET_DATA_ONLY

from .current_symbol_reuse import (
    CURRENT_SYMBOL_REUSE_RULE_NAME,
    CurrentSymbolReuseAssessment,
    assess_current_symbol_reuse,
)
from .errors import SecurityCorrelationError
from .evidence import (
    TickerOwnershipInterval,
    build_ticker_ownership_intervals,
    identity_candidates_for_symbol,
)
from .identity_partition import OneSessionDistinctUuidTransition
from .name_change import NameChangeAssessment, assess_name_change_history, name_change_dates
from .occupancy_anchor import dated_occupancy_anchors
from .predecessor_prefix import (
    INDEPENDENT_HISTORY_WARNING,
    MAPPED_CONTINUATION_RULE_NAME,
    MAPPED_CONTINUATION_WARNING,
    PREDECESSOR_PREFIX_RULE_NAME,
    IndependentHistoricalInterval,
    MappedContinuation,
    PredecessorPrefix,
)
from .retired_continuation import RETIRED_CONTINUATION_RULE, RetiredContinuation
from .security_identity import security_identity_for_alpaca_assets
from .settings import CORRELATED_BRANCH, UNCORRELATED_BRANCH
from .source_continuity import (
    cash_acquiree_intervals_outside_source,
    retired_source_identity,
    same_symbol_continuation_ids,
)
from .terminal_episode import DatedDeparturePrefix, HistoricalAsOfGap, TerminalTradedEpisode
from .terminal_fallback import (
    TERMINAL_FALLBACK_RULE_NAME,
    apply_terminal_continuous_ticker_fallback,
)
from .ticker_reuse_gap import (
    COMPLETE_CURRENT_SUFFIX_RULE_NAME,
    DATED_TERMINAL_SUFFIX_RULE_NAME,
    INACTIVE_CURRENT_SUFFIX_RULE_NAME,
    AdjustmentSessionConsensus,
    TickerReuseGapAssessment,
    assess_ticker_reuse_gap,
)
from .uuid_lineage import (
    IDENTITY_PRESERVING_SPLIT_EDGE_TYPES,
    UuidLineageEdge,
    build_uuid_lineage_edges,
    certify_name_change_edges,
    predecessor_uuid_branch,
)

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")
LATEST_DATED_OWNERSHIP_INTERVAL_RULE_NAME = "latest_dated_ownership_interval_overlapping_claims"


@dataclass(frozen=True)
class _RowDecision:
    branch: str
    rule_order: int = 0
    rule_name: str = ""
    asset_id: str | None = None
    cusip: str | None = None
    interval: TickerOwnershipInterval | None = None
    identity_confidence: str = ""
    warning_codes: tuple[str, ...] = ()
    reason_codes: tuple[str, ...] = ()
    candidates: tuple[tuple[str, str | None], ...] = ()
    ticker_reuse_gap: TickerReuseGapAssessment | None = None
    distinct_uuid_transition: OneSessionDistinctUuidTransition | None = None
    current_symbol_reuse: CurrentSymbolReuseAssessment | None = None
    historical_symbol: str | None = None
    use_rule_identity_basis: bool = False
    component_only: bool = False
    correlation_method: str | None = None

    def grouping_key(self) -> tuple:
        if self.branch == CORRELATED_BRANCH:
            return (
                self.branch,
                self.rule_order,
                self.rule_name,
                self.asset_id,
                self.cusip,
                self.interval.interval_key() if self.interval is not None else None,
                self.identity_confidence,
                self.warning_codes,
                self.ticker_reuse_gap,
                self.distinct_uuid_transition,
                self.current_symbol_reuse,
                self.historical_symbol,
                self.use_rule_identity_basis,
                self.component_only,
                self.correlation_method,
            )
        return (
            self.branch,
            self.rule_order,
            self.rule_name,
            self.warning_codes,
            self.reason_codes,
            self.candidates,
            self.ticker_reuse_gap,
            self.distinct_uuid_transition,
            self.current_symbol_reuse,
            self.historical_symbol,
            self.use_rule_identity_basis,
        )


@dataclass(frozen=True)
class _RowRuleContext:
    session_date: date
    requested_symbol: str
    selected_asof_interval: TickerOwnershipInterval | None
    selected_asof_identity_chain_asset_ids: frozenset[str]
    intervals: list[TickerOwnershipInterval]
    lineage: dict[str, Any]
    ticker_reuse_gap: TickerReuseGapAssessment | None
    distinct_uuid_transition: OneSessionDistinctUuidTransition | None
    current_symbol_reuse: CurrentSymbolReuseAssessment | None = None
    dated_same_symbol_predecessors: frozenset[tuple] = frozenset()
    predecessor_prefixes: tuple[PredecessorPrefix, ...] = ()
    mapped_continuations: tuple[MappedContinuation, ...] = ()
    occupancy_anchors: tuple[tuple[tuple, date], ...] = ()
    same_symbol_continuation_ids: frozenset[str] = frozenset()
    terminal_episode: TerminalTradedEpisode | None = None
    bridged_episodes: tuple[TerminalTradedEpisode, ...] = ()
    unrelated_cash_acquiree_intervals: frozenset[tuple] = frozenset()
    departure_prefix: DatedDeparturePrefix | None = None
    historical_asof_gap: HistoricalAsOfGap | None = None
    independent_history: tuple[IndependentHistoricalInterval, ...] = ()
    adopted_target_pairs: frozenset[tuple[str, str]] = frozenset()
    retired_source_selection: bool = False
    retired_lower_bound: date | None = None
    retired_continuation: RetiredContinuation | None = None
    bounded_aliases: frozenset[tuple] = frozenset()
    name_change_date_conflicts: tuple[tuple[date, date], ...] = ()


RowRuleFunction = Callable[[_RowRuleContext], _RowDecision | None]


@dataclass(frozen=True)
class RowCorrelationRule:
    name: str
    apply: RowRuleFunction


@dataclass(frozen=True)
class _CorrelatedRow:
    row_number: int
    timestamp: str
    session_date: date
    decision: _RowDecision


def _parse_session_date(timestamp: object, row_number: int) -> tuple[str, date]:
    if not isinstance(timestamp, str):
        raise SecurityCorrelationError(f"row {row_number}: timestamp is not a string")
    normalized_timestamp = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed_timestamp = datetime.fromisoformat(normalized_timestamp)
    except ValueError as error:
        raise SecurityCorrelationError(f"row {row_number}: invalid timestamp {timestamp!r}") from error
    if parsed_timestamp.tzinfo is None:
        raise SecurityCorrelationError(f"row {row_number}: timestamp has no timezone: {timestamp!r}")
    utc_timestamp = parsed_timestamp.astimezone(timezone.utc)
    return timestamp, utc_timestamp.astimezone(NEW_YORK_TIME_ZONE).date()


def _parse_download_rows(
    rows: list[dict], *, session_cache: dict[str, date] | None = None
) -> list[tuple[int, str, date]]:
    parsed_rows = []
    previous_session_date = None
    for row_number, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise SecurityCorrelationError(f"row {row_number}: expected an object")
        timestamp = row.get("t")
        session_date = (
            session_cache.get(timestamp) if session_cache is not None and isinstance(timestamp, str) else None
        )
        if session_date is None:
            timestamp, session_date = _parse_session_date(timestamp, row_number)
            if session_cache is not None:
                session_cache[timestamp] = session_date
        if previous_session_date is not None and session_date <= previous_session_date:
            raise SecurityCorrelationError(f"row {row_number}: session dates are not strictly increasing")
        previous_session_date = session_date
        parsed_rows.append((row_number, timestamp, session_date))
    return parsed_rows


def download_session_dates(rows: list[dict]) -> tuple[date, ...]:
    """Return validated, ordered New York session dates for one authenticated download."""
    return tuple(session_date for _, _, session_date in _parse_download_rows(rows))


class CorrelationCache:
    """Reuse computation within one immutable input group, including its rule passes.

    Keep input rows alive so object IDs cannot be recycled. Discard this cache on
    reload/publication; it is never a cross-run or whole-universe cache.
    """

    def __init__(self):
        self._sessions = {}
        self._downloads = {}
        self._decisions = {}
        self._lineage = {}
        self._name_change_sources = {}
        self._name_changes = {}

    def register_name_change_source(self, symbol, downloads, *, complete):
        if not complete:
            return
        self._name_change_sources[symbol] = tuple(
            (download, download.manifest, self.session_dates(download.rows)) for download in downloads
        )

    def name_change_assessment(self, lineage, *, include_action_anchored_current_suffix=False):
        self.lineage_analysis(
            lineage,
            include_action_anchored_current_suffix=include_action_anchored_current_suffix,
        )
        return self._name_changes[(id(lineage), include_action_anchored_current_suffix)]

    def lineage_analysis(self, lineage, *, include_action_anchored_current_suffix=False):
        """Build immutable evidence structure once for the complete rule group."""
        key = (id(lineage), include_action_anchored_current_suffix)
        cached = self._lineage.get(key)
        if cached is None:
            candidates, edge_issues = build_uuid_lineage_edges(lineage, candidates_only=True)
            # DO NOT REMOVE WITHOUT USER AUTH!#
            # Build each evidence authority once. Interval construction consumes
            # accepted split edges; cross-UUID name candidates still require the
            # subsequent full-group history certificate before membership use.
            intervals, interval_issues = build_ticker_ownership_intervals(
                lineage,
                uuid_lineage_edges=candidates,
                include_action_anchored_current_suffix=include_action_anchored_current_suffix,
            )
            assessment = assess_name_change_history(lineage, intervals, candidates, self._name_change_sources)
            self._name_changes[key] = assessment
            edges, continuity_issues = certify_name_change_edges(candidates, assessment.accepted)
            edge_issues.extend(continuity_issues)
            independent = {
                edge.action_id: edge
                for edge in candidates
                if edge.action_id in assessment.independent_successors
            }
            intervals = [
                replace(
                    interval,
                    start_date_inclusive=self._name_change_sources[interval.historical_symbol][0][2][0],
                )
                if any(
                    interval.identity_key()
                    == (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
                    and interval.start_date_inclusive == edge.effective_date
                    for edge in independent.values()
                )
                else interval
                for interval in intervals
            ]
            cached = (lineage, intervals, tuple(interval_issues), edges, tuple(edge_issues))
            self._lineage[key] = cached
        _, intervals, interval_issues, edges, edge_issues = cached
        return intervals, list(interval_issues), edges, list(edge_issues)

    def _prepare(self, rows):
        key = id(rows)
        if key not in self._downloads:
            parsed = _parse_download_rows(rows, session_cache=self._sessions)
            self._downloads[key] = (rows, parsed, tuple(day for _, _, day in parsed))
        return self._downloads[key]

    def parsed_rows(self, rows):
        return self._prepare(rows)[1]

    def session_dates(self, rows):
        return self._prepare(rows)[2]

    def decisions_for(self, context):
        # Compare every non-date rule input once per view/pass. Adjustment-specific
        # assessments and changed cross-source evidence therefore get separate maps.
        candidates = self._decisions.setdefault(context["requested_symbol"], [])
        for previous, decisions in candidates:
            if previous == context:
                return decisions
        decisions = {}
        candidates.append((deepcopy(context), decisions))
        return decisions


def _interval_matches(
    intervals: list[TickerOwnershipInterval],
    session_date: date,
    *,
    asset_id: str | None = None,
    historical_symbol: str | None = None,
) -> list[TickerOwnershipInterval]:
    return [
        interval
        for interval in intervals
        if interval.contains(session_date)
        and (asset_id is None or interval.asset_id == asset_id)
        and (historical_symbol is None or interval.historical_symbol == historical_symbol)
    ]


def _unique_interval_or_none(
    intervals: list[TickerOwnershipInterval],
) -> TickerOwnershipInterval | None:
    if len(intervals) != 1:
        return None
    return intervals[0]


def _selected_asof_interval(
    requested_symbol: str,
    asof: str,
    intervals: list[TickerOwnershipInterval],
) -> TickerOwnershipInterval | None:
    try:
        asof_date = date.fromisoformat(asof[:10])
    except (TypeError, ValueError):
        return None
    matching_intervals = _interval_matches(
        intervals,
        asof_date,
        historical_symbol=requested_symbol,
    )
    return _unique_interval_or_none(matching_intervals)


def _certified_predecessor_intervals(context: _RowRuleContext):
    return {item.interval for item in context.predecessor_prefixes if item.contains(context.session_date)} | {
        item.interval for item in context.independent_history if item.first <= context.session_date <= item.last
    }


def _apply_predecessor_prefix_rule(context: _RowRuleContext) -> _RowDecision | None:
    matches = _certified_predecessor_intervals(context)
    independent = {
        item.interval for item in context.independent_history if item.first <= context.session_date <= item.last
    }
    matches.update(independent)
    mapped = [item for item in context.mapped_continuations if item.contains(context.session_date)]
    matches.update(item.interval for item in mapped)
    if len(matches) != 1:
        return None
    interval = next(iter(matches))
    mapped_match = bool(mapped)
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=interval.asset_id,
        cusip=interval.cusip,
        interval=interval,
        historical_symbol=interval.historical_symbol if mapped_match else None,
        identity_confidence=(
            "accepted_continuity_and_independent_source" if mapped_match else "action_supported"
        ),
        warning_codes=(
            *(("inferred_lower_ownership_boundary",) if interval.start_date_inclusive is None else ()),
            *((INDEPENDENT_HISTORY_WARNING,) if independent else ()),
            *((MAPPED_CONTINUATION_WARNING,) if mapped_match else ()),
        ),
        correlation_method=MAPPED_CONTINUATION_RULE_NAME if mapped_match else None,
    )


def _apply_name_change_boundary_conflict_rule(context: _RowRuleContext) -> _RowDecision | None:
    if any(first <= context.session_date < last for first, last in context.name_change_date_conflicts):
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("conflicting_name_change_boundary_dates",),
        )
    return None


def _apply_asof_selected_identity_rule(context: _RowRuleContext) -> _RowDecision | None:
    historical_gap = context.historical_asof_gap
    if historical_gap is not None and historical_gap.contains_unresolved_tail(context.session_date):
        mapped = [item for item in context.mapped_continuations if item.contains(context.session_date)]
        if mapped:
            mapped_decision = _apply_predecessor_prefix_rule(context)
            if (
                mapped_decision is not None
                and mapped_decision.correlation_method == MAPPED_CONTINUATION_RULE_NAME
            ):
                return mapped_decision
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("post_mapping_asof_gap_requires_dated_owner",),
            candidates=(
                (historical_gap.interval.asset_id, historical_gap.interval.cusip),
                (historical_gap.successor_asset_id, historical_gap.successor_cusip),
            ),
        )
    if context.selected_asof_interval is not None:
        if (
            context.retired_source_selection
            and context.retired_lower_bound is not None
            and context.session_date < context.retired_lower_bound
        ):
            return None
        if context.retired_source_selection and any(
            item.historical_symbol == context.requested_symbol
            and item.asset_id == context.selected_asof_interval.asset_id
            and item.contains(context.session_date)
            for item in context.intervals
        ):
            # Preserve the existing direct requested-label decisions and donor
            # provenance inside its established epoch. Supplement its missing history.
            return None
        selected_asset_ids = context.selected_asof_identity_chain_asset_ids
        selected_matches = _interval_matches(
            context.intervals,
            context.session_date,
        )
        selected_matches = [interval for interval in selected_matches if interval.asset_id in selected_asset_ids]
        exact_matches = [
            interval
            for interval in selected_matches
            if interval.asset_id == context.selected_asof_interval.asset_id
        ]
        if len(exact_matches) == 1 and exact_matches[0].start_date_inclusive is not None:
            # asof selects an entity, not a price vintage. Its exact dated UUID
            # outranks another predecessor branch connected only by a later
            # action. Undated starts and competing aliases gain no preference.
            selected_matches = exact_matches
            selected_asset_ids = frozenset((context.selected_asof_interval.asset_id,))
        elif context.same_symbol_continuation_ids:
            continuing = [
                interval
                for interval in selected_matches
                if interval.historical_symbol == context.requested_symbol
                and interval.asset_id in context.same_symbol_continuation_ids
            ]
            if len(continuing) == 1:
                selected_matches = continuing
                selected_asset_ids = context.same_symbol_continuation_ids
        independent = False
        if len({interval.asset_id for interval in selected_matches}) > 1:
            proven = {
                item.interval
                for item in context.independent_history
                if item.first <= context.session_date <= item.last
            }
            if len(proven) == 1 and next(iter(proven)) in selected_matches:
                selected_matches = list(proven)
                selected_asset_ids = frozenset((selected_matches[0].asset_id,))
                independent = True
        selected_interval = _unique_interval_or_none(selected_matches)
        if selected_interval is not None:
            if (
                selected_interval.historical_symbol != context.requested_symbol
                and selected_interval.interval_key() not in context.bounded_aliases
                and not any(
                    item.historical_symbol == context.requested_symbol
                    and item.asset_id in selected_asset_ids
                    and item.end_date_exclusive is not None
                    and item.end_date_exclusive <= context.session_date
                    for item in context.intervals
                )
                and selected_interval not in _certified_predecessor_intervals(context)
            ):
                return None
            contested_adoption = selected_interval.historical_symbol != context.requested_symbol and any(
                acquirer == context.selected_asof_interval.asset_id
                and any(
                    item.asset_id == target
                    and item.historical_symbol == context.requested_symbol
                    and item.contains(context.session_date)
                    for item in context.intervals
                )
                for target, acquirer in context.adopted_target_pairs
            )
            if contested_adoption:
                proofs = {
                    item.interval
                    for item in context.independent_history
                    if item.first <= context.session_date <= item.last
                }
                if proofs != {selected_interval}:
                    return None
                independent = True
            requested_symbol_matches = _interval_matches(
                context.intervals,
                context.session_date,
                historical_symbol=context.requested_symbol,
            )
            competing_requested_symbol_occupant = any(
                interval.asset_id not in selected_asset_ids for interval in requested_symbol_matches
            )
            warning_codes = []
            if context.retired_source_selection:
                warning_codes.append("retired_source_same_uuid_continuity")
            if independent:
                warning_codes.append(INDEPENDENT_HISTORY_WARNING)
            if (
                selected_interval.historical_symbol != context.requested_symbol
                or competing_requested_symbol_occupant
            ):
                warning_codes.append("asof_uuid_lineage_overrode_requested_symbol_occupancy")
            if selected_interval.start_date_inclusive is None:
                warning_codes.append("inferred_lower_ownership_boundary")
            return _RowDecision(
                branch=CORRELATED_BRANCH,
                asset_id=selected_interval.asset_id,
                cusip=selected_interval.cusip,
                interval=selected_interval,
                identity_confidence="action_supported",
                warning_codes=tuple(warning_codes),
            )
        if len(selected_matches) > 1:
            if len({interval.asset_id for interval in selected_matches}) > 1:
                # A successor can have concurrent predecessor branches. Neither
                # global recency nor requested-label fallback proves which branch
                # supplied these historical rows. Same-UUID aliases still defer.
                return _RowDecision(
                    branch=UNCORRELATED_BRANCH,
                    reason_codes=("ambiguous_asof_uuid_lineage_intervals",),
                )
            return None
    return None


def _apply_exchange_test_symbol_exclusion_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    disposition = context.lineage.get("symbol_disposition")
    if not isinstance(disposition, dict) or disposition.get("classification") != EXCHANGE_TEST_SYMBOL:
        return None
    candidates = identity_candidates_for_symbol(
        context.lineage,
        context.requested_symbol,
        context.intervals,
        context.session_date,
    )
    return _RowDecision(
        branch=UNCORRELATED_BRANCH,
        reason_codes=("exchange_test_symbol_excluded",),
        candidates=tuple((str(candidate["asset_id"]), candidate.get("cusip")) for candidate in candidates),
    )


def _dated_same_symbol_predecessors(
    requested_symbol: str,
    lineage: dict[str, Any],
    intervals: list[TickerOwnershipInterval],
    edges: list[UuidLineageEdge],
) -> frozenset[tuple]:
    """Recognize a dated handoff chain, never an undirected identity component."""
    occupants = [interval for interval in intervals if interval.historical_symbol == requested_symbol]
    if len(occupants) < 2 or len({interval.asset_id for interval in occupants}) != len(occupants):
        return frozenset()
    # No interval filter: an additional undated current/occupancy UUID is still
    # a competitor, even if the dated subset alone looks like a clean chain.
    known_ids = {
        candidate["asset_id"]
        for candidate in identity_candidates_for_symbol(lineage, requested_symbol, [], date.min)
    }
    if known_ids - {interval.asset_id for interval in occupants}:
        return frozenset()
    occupants.sort(key=lambda interval: interval.start_date_inclusive or date.min)
    if occupants[0].start_date_inclusive is not None:
        return frozenset()
    for predecessor, successor in zip(occupants, occupants[1:]):
        boundary = predecessor.end_date_exclusive
        if boundary is None or successor.start_date_inclusive != boundary:
            return frozenset()
        predecessor_actions = {
            action for rule in predecessor.applied_rules for action in rule.supporting_action_ids
        }
        successor_actions = {action for rule in successor.applied_rules for action in rule.supporting_action_ids}
        if not any(
            edge.edge_type in ("name_change", "stock_merger")
            and edge.effective_date == boundary
            and (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            == predecessor.identity_key()
            and (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
            == successor.identity_key()
            and edge.action_id in predecessor_actions & successor_actions
            for edge in edges
        ):
            return frozenset()
    # Only the original predecessor gains permission to retain its inferred
    # lower bound. A split-only link, recurrence, unrelated reuse, foreign-label
    # path or a common future successor cannot establish this handoff chain.
    return frozenset((occupants[0].interval_key(),))


def _apply_requested_symbol_interval_rule(context: _RowRuleContext) -> _RowDecision | None:
    requested_symbol_matches = _interval_matches(
        context.intervals,
        context.session_date,
        historical_symbol=context.requested_symbol,
    )
    requested_interval = _unique_interval_or_none(requested_symbol_matches)
    if requested_interval is not None:
        undated_reuse = (
            requested_interval.start_date_inclusive is None
            and requested_interval.interval_key() not in context.dated_same_symbol_predecessors
            and any(
                interval.historical_symbol == context.requested_symbol
                and interval.asset_id != requested_interval.asset_id
                for interval in context.intervals
            )
        )
        occupancy_anchored = undated_reuse and any(
            key == requested_interval.interval_key() and anchor <= context.session_date
            for key, anchor in context.occupancy_anchors
        )
        if undated_reuse and not occupancy_anchored:
            # Another occupant ending does not date this occupant's beginning.
            # Proven reuse/as-of rules above can establish the source episode;
            # this fallback cannot fill an unknown reuse boundary retrospectively.
            return _RowDecision(
                branch=UNCORRELATED_BRANCH,
                reason_codes=("undated_requested_ticker_reuse_boundary",),
            )
        warning_codes = (
            ("dated_action_occupancy",)
            if occupancy_anchored
            else (
                ("inferred_lower_ownership_boundary",) if requested_interval.start_date_inclusive is None else ()
            )
        )
        return _RowDecision(
            branch=CORRELATED_BRANCH,
            asset_id=requested_interval.asset_id,
            cusip=requested_interval.cusip,
            interval=requested_interval,
            identity_confidence="action_supported",
            warning_codes=warning_codes,
        )
    if len(requested_symbol_matches) > 1:
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("conflicting_requested_ticker_intervals",),
        )
    return None


def _apply_unique_uuid_candidate_rule(context: _RowRuleContext) -> _RowDecision | None:
    own_intervals = [item for item in context.intervals if item.historical_symbol == context.requested_symbol]
    if any(
        rule.name == "name_change_ticker_transition" for item in own_intervals for rule in item.applied_rules
    ) and not any(item.contains(context.session_date) for item in own_intervals):
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("outside_supported_name_change_history",),
        )
    candidate_objects = identity_candidates_for_symbol(
        context.lineage,
        context.requested_symbol,
        context.intervals,
        context.session_date,
    )
    candidate_keys = tuple(
        (str(candidate["asset_id"]), candidate.get("cusip")) for candidate in candidate_objects
    )
    if len(candidate_keys) != 1:
        return None
    asset_id, cusip = candidate_keys[0]
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=asset_id,
        cusip=cusip,
        identity_confidence="unique_candidate",
        warning_codes=("missing_dated_ticker_ownership_interval",),
    )


def _apply_complete_current_suffix_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    assessment = context.ticker_reuse_gap
    if (
        assessment is None
        or not (assessment.complete_current_suffix_eligible or assessment.active_current_suffix_eligible)
        or context.session_date < assessment.right_observed_session
    ):
        return None
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=assessment.current_asset_id,
        cusip=assessment.current_asset_cusip,
        identity_confidence=(
            "complete_current_identity_suffix"
            if assessment.complete_current_suffix_eligible
            else "bounded_active_current_identity_suffix"
        ),
        ticker_reuse_gap=assessment,
        use_rule_identity_basis=True,
    )


def _apply_inactive_current_suffix_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    assessment = context.ticker_reuse_gap
    if (
        assessment is None
        or not assessment.inactive_current_suffix_eligible
        or context.session_date < assessment.right_observed_session
    ):
        return None
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=assessment.current_asset_id,
        cusip=assessment.current_asset_cusip,
        identity_confidence="inactive_current_identity_suffix",
        ticker_reuse_gap=assessment,
        use_rule_identity_basis=True,
    )


def _apply_dated_terminal_suffix_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    if any(
        episode.preceding_inert_first is not None
        and episode.preceding_inert_first <= context.session_date <= episode.preceding_inert_last
        for episode in context.bridged_episodes
    ):
        # A dated process event can lag the actual departure. Once unrelated
        # reuse is established, never publish its shared synthetic bridge as trades.
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("synthetic_carry_forward_between_unrelated_issues",),
        )
    matches = [
        episode
        for episode in (*context.bridged_episodes, context.terminal_episode)
        if episode is not None and episode.first <= context.session_date <= episode.last
    ]
    if len({(episode.asset_id, episode.cusip) for episode in matches}) > 1:
        return _RowDecision(branch=UNCORRELATED_BRANCH, reason_codes=("conflicting_traded_episode_owners",))
    if matches:
        episode = matches[0]
        return _RowDecision(
            branch=CORRELATED_BRANCH,
            asset_id=episode.asset_id,
            cusip=episode.cusip,
            historical_symbol=context.requested_symbol,
            identity_confidence="dated_terminal_traded_episode",
            warning_codes=(
                "complete_initial_terminal_episode"
                if episode.initial_before_reuse_gap
                else "synthetic_carry_forward_boundary",
                *(("terminal_action_inside_later_reuse_gap",) if episode.terminated_during_reuse_gap else ()),
                *(("bounded_traded_episode",) if episode.bounded_by_inert_bridges else ()),
            ),
        )
    assessment = context.ticker_reuse_gap
    if (
        assessment is None
        or not assessment.dated_terminal_suffix_eligible
        or context.session_date < assessment.right_observed_session
    ):
        return None
    identity = assessment.dated_terminal_suffix_identity
    if identity is None:
        return None
    warning_codes = (
        ("undated_symbol_lookup_uuid_differs_from_dated_suffix_identity",)
        if identity.undated_symbol_lookup_uuid_conflict
        else ()
    )
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=identity.interval.asset_id,
        cusip=identity.interval.cusip,
        interval=identity.interval,
        identity_confidence="dated_terminal_identity_suffix",
        warning_codes=warning_codes,
        ticker_reuse_gap=assessment,
        use_rule_identity_basis=True,
    )


def _apply_ticker_reuse_gap_guard(
    context: _RowRuleContext,
) -> _RowDecision | None:
    assessment = context.ticker_reuse_gap
    if assessment is None:
        return None
    certificates = _certified_predecessor_intervals(context)
    if len(certificates) == 1:
        owner = next(iter(certificates)).asset_id
        # A physical gap cannot erase certified ownership. Release only these
        # dates, and do not expose them to a contradictory earlier row decision.
        earlier = (
            _apply_one_session_distinct_uuid_transition_rule(context),
            _apply_current_symbol_reuse_rule(context),
            _apply_asof_selected_identity_rule(context),
        )
        if all(
            decision is None or decision.branch == CORRELATED_BRANCH and decision.asset_id == owner
            for decision in earlier
        ):
            return None
    candidates = (
        ((assessment.current_asset_id, assessment.current_asset_cusip),)
        if assessment.current_asset_id is not None
        else ()
    )
    return _RowDecision(
        branch=UNCORRELATED_BRANCH,
        reason_codes=(
            (
                "prior_pricing_run_before_ticker_reuse_gap"
                if context.session_date < assessment.right_observed_session
                else ("post_gap_pricing_run_requires_dated_identity")
            ),
        ),
        candidates=candidates,
        ticker_reuse_gap=assessment,
    )


def _apply_one_session_distinct_uuid_transition_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    transition = context.distinct_uuid_transition
    if transition is None or context.requested_symbol not in {
        transition.predecessor.symbol,
        transition.successor.symbol,
    }:
        return None
    identity = (
        transition.predecessor
        if context.session_date < transition.right_observed_session
        else transition.successor
    )
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=identity.asset_id,
        cusip=identity.cusip,
        historical_symbol=identity.symbol,
        identity_confidence="authenticated_distinct_uuid_transition",
        distinct_uuid_transition=transition,
        use_rule_identity_basis=True,
    )


def _apply_current_symbol_reuse_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    assessment = context.current_symbol_reuse
    if assessment is None:
        return None
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=assessment.current_asset_id,
        cusip=assessment.current_cusip,
        historical_symbol=assessment.symbol,
        identity_confidence="authenticated_current_symbol_reuse",
        current_symbol_reuse=assessment,
        use_rule_identity_basis=True,
    )


def _apply_latest_dated_ownership_interval_rule(
    context: _RowRuleContext,
) -> _RowDecision | None:
    prefix = context.departure_prefix
    if prefix is not None and prefix.first <= context.session_date <= prefix.last:
        return _RowDecision(
            branch=CORRELATED_BRANCH,
            asset_id=prefix.interval.asset_id,
            cusip=prefix.interval.cusip,
            interval=prefix.interval,
            identity_confidence="action_supported_complete_prefix",
            warning_codes=("dated_departure_before_synthetic_carry_forward",),
        )
    # Stronger as-of lineage and reuse rules have already had their turn. This
    # fallback compares competing occupants of this dated label, not every branch
    # discovered through a later corporate relationship. A later start on another
    # predecessor's label cannot supersede this source's historical ownership.
    all_matching_intervals = _interval_matches(context.intervals, context.session_date)
    matching_intervals = [
        interval for interval in all_matching_intervals if interval.historical_symbol == context.requested_symbol
    ]
    if not matching_intervals and len({interval.asset_id for interval in all_matching_intervals}) > 1:
        if all(
            interval.interval_key() in context.unrelated_cash_acquiree_intervals
            for interval in all_matching_intervals
        ):
            # These intervals terminate other securities bought for cash. Their
            # relationship to the buyer cannot veto its own identity evidence.
            return None
        # Do not turn unscoped competing branches into a current-UUID guess merely
        # by excluding their labels from the latest-start comparison.
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("overlapping_ownership_intervals_without_source_scope",),
        )
    if len(matching_intervals) < 2:
        return None
    if len({interval.asset_id for interval in matching_intervals}) == 1:
        return None

    dated_intervals = [interval for interval in matching_intervals if interval.start_date_inclusive is not None]
    if not dated_intervals:
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("overlapping_ownership_intervals_without_unique_latest_start",),
        )

    latest_start = max(
        interval.start_date_inclusive
        for interval in dated_intervals
        if interval.start_date_inclusive is not None
    )
    latest_intervals = [
        interval for interval in dated_intervals if interval.start_date_inclusive == latest_start
    ]
    if len(latest_intervals) != 1:
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("overlapping_ownership_intervals_without_unique_latest_start",),
        )

    selected_interval = latest_intervals[0]
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=selected_interval.asset_id,
        cusip=selected_interval.cusip,
        interval=selected_interval,
        identity_confidence="action_supported",
        warning_codes=("older_overlapping_ownership_claim_superseded",),
    )


def _apply_retired_continuation_rule(context: _RowRuleContext) -> _RowDecision | None:
    episode = context.retired_continuation
    if episode is None or not episode.first <= context.session_date <= episode.last:
        return None
    return _RowDecision(
        branch=CORRELATED_BRANCH,
        asset_id=episode.anchor_asset_id,
        historical_symbol=context.requested_symbol,
        identity_confidence="action_and_observed_handoff_supported_continuity",
        warning_codes=("historical_uuid_unknown_successor_component_supported",),
        use_rule_identity_basis=True,
        component_only=True,
    )


def _apply_unresolved_uuid_candidates_rule(context: _RowRuleContext) -> _RowDecision:
    candidate_objects = identity_candidates_for_symbol(
        context.lineage,
        context.requested_symbol,
        context.intervals,
        context.session_date,
    )
    candidate_keys = tuple(
        (str(candidate["asset_id"]), candidate.get("cusip")) for candidate in candidate_objects
    )
    if not candidate_keys:
        disposition = context.lineage.get("symbol_disposition")
        classification = disposition.get("classification") if isinstance(disposition, dict) else None
        if classification == EXCHANGE_TEST_SYMBOL:
            reason_codes = ("exchange_test_symbol_excluded",)
        elif classification == HISTORICAL_MARKET_DATA_ONLY:
            reason_codes = ("historical_market_data_without_alpaca_asset",)
        else:
            reason_codes = ("no_security_identity_evidence",)
    else:
        reason_codes = ("ambiguous_security_identity_candidates",)
    return _RowDecision(
        branch=UNCORRELATED_BRANCH,
        reason_codes=reason_codes,
        candidates=candidate_keys,
    )


ROW_CORRELATION_RULES = (
    RowCorrelationRule(
        name="exchange_test_symbol_exclusion",
        apply=_apply_exchange_test_symbol_exclusion_rule,
    ),
    RowCorrelationRule(
        name="name_change_boundary_conflict",
        apply=_apply_name_change_boundary_conflict_rule,
    ),
    RowCorrelationRule(
        name=COMPLETE_CURRENT_SUFFIX_RULE_NAME,
        apply=_apply_complete_current_suffix_rule,
    ),
    RowCorrelationRule(
        name=INACTIVE_CURRENT_SUFFIX_RULE_NAME,
        apply=_apply_inactive_current_suffix_rule,
    ),
    RowCorrelationRule(
        name=DATED_TERMINAL_SUFFIX_RULE_NAME,
        apply=_apply_dated_terminal_suffix_rule,
    ),
    RowCorrelationRule(
        name="ticker_reuse_gap_guard",
        apply=_apply_ticker_reuse_gap_guard,
    ),
    RowCorrelationRule(
        name="one_session_distinct_uuid_transition",
        apply=_apply_one_session_distinct_uuid_transition_rule,
    ),
    RowCorrelationRule(
        name=CURRENT_SYMBOL_REUSE_RULE_NAME,
        apply=_apply_current_symbol_reuse_rule,
    ),
    RowCorrelationRule(
        name="asof_selected_uuid_lineage_interval",
        apply=_apply_asof_selected_identity_rule,
    ),
    RowCorrelationRule(
        name=PREDECESSOR_PREFIX_RULE_NAME,
        apply=_apply_predecessor_prefix_rule,
    ),
    RowCorrelationRule(
        name=LATEST_DATED_OWNERSHIP_INTERVAL_RULE_NAME,
        apply=_apply_latest_dated_ownership_interval_rule,
    ),
    RowCorrelationRule(
        name="requested_symbol_dated_uuid_interval",
        apply=_apply_requested_symbol_interval_rule,
    ),
    RowCorrelationRule(
        name="unique_uuid_candidate_with_warnings",
        apply=_apply_unique_uuid_candidate_rule,
    ),
    RowCorrelationRule(
        name=RETIRED_CONTINUATION_RULE,
        apply=_apply_retired_continuation_rule,
    ),
    RowCorrelationRule(
        name="unresolved_uuid_candidates",
        apply=_apply_unresolved_uuid_candidates_rule,
    ),
)


def row_correlation_rule_order() -> list[dict[str, Any]]:
    row_rules = [
        {"order": rule_order, "name": rule.name}
        for rule_order, rule in enumerate(ROW_CORRELATION_RULES, start=1)
    ]
    return [
        *row_rules,
        {
            "order": len(row_rules) + 1,
            "name": TERMINAL_FALLBACK_RULE_NAME,
        },
    ]


def _decide_row(context: _RowRuleContext) -> _RowDecision:
    for rule_order, rule in enumerate(ROW_CORRELATION_RULES, start=1):
        decision = rule.apply(context)
        if decision is not None:
            return replace(
                decision,
                rule_order=rule_order,
                rule_name=rule.name,
                warning_codes=tuple(
                    dict.fromkeys((*decision.warning_codes, "response_symbol_label_not_preserved"))
                ),
            )
    raise SecurityCorrelationError("row correlation rule registry produced no decision")


def _canonical_security(
    security_id: str,
    asset_id: str | None,
    cusip: str | None,
) -> dict[str, Any]:
    return {
        "security_id": security_id,
        "alpaca_asset_id": asset_id,
        "cusip": cusip,
    }


def _candidate_report_objects(
    decision: _RowDecision,
    lineage: dict[str, Any],
    requested_symbol: str,
    intervals: list[TickerOwnershipInterval],
    session_date: date,
) -> list[dict[str, Any]]:
    candidates = identity_candidates_for_symbol(
        lineage,
        requested_symbol,
        intervals,
        session_date,
    )
    expected_keys = set(decision.candidates)
    return [
        candidate
        for candidate in candidates
        if (str(candidate["asset_id"]), candidate.get("cusip")) in expected_keys
    ]


def _segment_source(
    source: dict[str, Any],
    first_row: _CorrelatedRow,
    last_row: _CorrelatedRow,
) -> dict[str, Any]:
    return {
        "segment_id": (f"{source['data_file']}:rows:{first_row.row_number}-{last_row.row_number}"),
        "data_file": source["data_file"],
        "download_manifest": source["download_manifest"],
        "download_sha256": source["download_sha256"],
        "adjustment": source["adjustment"],
        "row_start": first_row.row_number,
        "row_end": last_row.row_number,
        "row_count": last_row.row_number - first_row.row_number + 1,
        "first_timestamp": first_row.timestamp,
        "last_timestamp": last_row.timestamp,
        "first_session_date": first_row.session_date.isoformat(),
        "last_session_date": last_row.session_date.isoformat(),
        "requested_symbol": source["requested_symbol"],
        "response_symbol_label": None,
        "response_symbol_label_preserved": False,
        "asof": source["asof"],
        "request_from": source["request_from"],
        "request_to": source["request_to"],
        "feed": source["feed"],
        "timeframe": source["timeframe"],
        "pipeline_state": source["pipeline_state"],
        "validation_pipeline_version": source["validation_pipeline_version"],
        "recorded_integrity_failure": source["recorded_integrity_failure"],
    }


def correlate_download_rows(
    rows: list[dict],
    source: dict[str, Any],
    lineage: dict[str, Any],
    intervals: list[TickerOwnershipInterval],
    *,
    adjustment_consensus: AdjustmentSessionConsensus | None = None,
    evidence_issues: list[dict[str, Any]] | None = None,
    distinct_uuid_transition: OneSessionDistinctUuidTransition | None = None,
    predecessor_prefixes: tuple[PredecessorPrefix, ...] = (),
    mapped_continuations: tuple[MappedContinuation, ...] = (),
    terminal_episode: TerminalTradedEpisode | None = None,
    bridged_episodes: tuple[TerminalTradedEpisode, ...] = (),
    closed_history_evidence: bool | None = None,
    departure_prefix: DatedDeparturePrefix | None = None,
    historical_asof_gap: HistoricalAsOfGap | None = None,
    independent_history: tuple[IndependentHistoricalInterval, ...] = (),
    retired_lower_bound: date | None = None,
    retired_continuation: RetiredContinuation | None = None,
    cache: CorrelationCache | None = None,
    uuid_lineage_edges: list[UuidLineageEdge] | None = None,
    adopted_target_pairs: frozenset[tuple[str, str]] | None = None,
    name_change_assessment: NameChangeAssessment = NameChangeAssessment(),
) -> dict[str, list[dict[str, Any]]]:
    requested_symbol = source["requested_symbol"]
    independent_history = (
        *independent_history,
        *(
            IndependentHistoricalInterval(item.first, item.last, item.interval)
            for item in name_change_assessment.histories
            if item.symbol == requested_symbol
        ),
    )
    selected_interval = _selected_asof_interval(requested_symbol, source["asof"], intervals)
    if uuid_lineage_edges is None or adopted_target_pairs is None:
        calculated_edges, uuid_lineage_issues = build_uuid_lineage_edges(lineage)
        if uuid_lineage_edges is None:
            uuid_lineage_edges = calculated_edges
        if adopted_target_pairs is None:
            adopted_target_pairs = frozenset(
                (issue["target_asset_id"], issue["acquirer_asset_id"])
                for issue in uuid_lineage_issues
                if issue.get("reason_code") == "acquirer_adopted_target_symbol"
            )
    retired_selection = False
    if selected_interval is None:
        selected_interval = retired_source_identity(
            requested_symbol, lineage, intervals, uuid_lineage_edges, source["asof"]
        )
        retired_selection = selected_interval is not None
    row_routing_edges = [
        edge for edge in uuid_lineage_edges if edge.edge_type not in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
    ]
    dated_same_symbol_predecessors = _dated_same_symbol_predecessors(
        requested_symbol, lineage, intervals, row_routing_edges
    )
    occupancy_anchors = tuple(dated_occupancy_anchors(requested_symbol, lineage, intervals).items())
    unrelated_cash_acquiree_intervals = cash_acquiree_intervals_outside_source(
        requested_symbol, lineage, intervals
    )
    selected_identity_chain_asset_ids = (
        predecessor_uuid_branch(selected_interval.asset_id, row_routing_edges)
        if selected_interval is not None
        else frozenset()
    )
    continuation_ids = same_symbol_continuation_ids(
        requested_symbol,
        lineage,
        intervals,
        selected_interval,
        dated_same_symbol_predecessors,
        selected_identity_chain_asset_ids,
    )
    parsed_rows = cache.parsed_rows(rows) if cache is not None else _parse_download_rows(rows)
    bounded_aliases = frozenset(
        interval.interval_key()
        for interval in intervals
        if interval.historical_symbol != requested_symbol
        and interval.start_date_inclusive is not None
        and interval.end_date_exclusive is not None
        and any(
            edge.edge_type == "name_change"
            and edge.predecessor_symbol == requested_symbol
            and edge.successor_symbol == interval.historical_symbol
            and edge.successor_asset_id == interval.asset_id
            and edge.effective_date == interval.start_date_inclusive
            for edge in uuid_lineage_edges
        )
        and any(
            edge.edge_type == "name_change"
            and edge.successor_symbol == requested_symbol
            and edge.predecessor_symbol == interval.historical_symbol
            and edge.predecessor_asset_id == edge.successor_asset_id == interval.asset_id
            and edge.effective_date == interval.end_date_exclusive
            for edge in uuid_lineage_edges
        )
    )

    ticker_reuse_gap = assess_ticker_reuse_gap(
        tuple(session_date for _, _, session_date in parsed_rows),
        source,
        lineage,
        intervals,
        adjustment_consensus,
        evidence_issues,
    )
    current_symbol_reuse = assess_current_symbol_reuse(
        tuple(session_date for _, _, session_date in parsed_rows),
        source,
        lineage,
        intervals,
        adjustment_consensus,
        evidence_issues,
    )
    # Pricing gaps and ticker occupancy cannot revoke an accepted continuation.
    # Unrelated reused-symbol occupants already have no edge to the prior lineage.
    context = dict(
        requested_symbol=requested_symbol,
        selected_asof_interval=selected_interval,
        selected_asof_identity_chain_asset_ids=selected_identity_chain_asset_ids,
        intervals=intervals,
        lineage=lineage,
        ticker_reuse_gap=ticker_reuse_gap,
        distinct_uuid_transition=distinct_uuid_transition,
        current_symbol_reuse=current_symbol_reuse,
        dated_same_symbol_predecessors=dated_same_symbol_predecessors,
        predecessor_prefixes=predecessor_prefixes,
        mapped_continuations=mapped_continuations,
        occupancy_anchors=occupancy_anchors,
        same_symbol_continuation_ids=continuation_ids,
        terminal_episode=terminal_episode,
        bridged_episodes=bridged_episodes,
        unrelated_cash_acquiree_intervals=unrelated_cash_acquiree_intervals,
        departure_prefix=departure_prefix,
        historical_asof_gap=historical_asof_gap,
        independent_history=independent_history,
        adopted_target_pairs=adopted_target_pairs,
        retired_source_selection=retired_selection,
        retired_lower_bound=retired_lower_bound,
        retired_continuation=retired_continuation,
        bounded_aliases=bounded_aliases,
        name_change_date_conflicts=tuple(
            (dates[0], dates[-1]) if dates else (date.min, date.max)
            for action in lineage.get("corporate_actions", [])
            if action.get("action_type") == "name_change"
            and requested_symbol in (action.get("old_symbol"), action.get("new_symbol"))
            for dates, issue in (name_change_dates(action, lineage.get("corporate_actions", [])),)
            if issue
        ),
    )
    decisions = cache.decisions_for(context) if cache is not None else {}
    correlated_rows = []
    for row_number, timestamp, session_date in parsed_rows:
        decision = decisions.get(session_date)
        if decision is None:
            decision = decisions[session_date] = _decide_row(
                _RowRuleContext(session_date=session_date, **context)
            )
        correlated_rows.append(
            _CorrelatedRow(
                row_number=row_number,
                timestamp=timestamp,
                session_date=session_date,
                decision=decision,
            )
        )

    branches = {CORRELATED_BRANCH: [], UNCORRELATED_BRANCH: []}
    segment_start = 0
    while segment_start < len(correlated_rows):
        decision = correlated_rows[segment_start].decision
        grouping_key = decision.grouping_key()
        segment_end = segment_start + 1
        while (
            segment_end < len(correlated_rows)
            and correlated_rows[segment_end].decision.grouping_key() == grouping_key
        ):
            segment_end += 1
        first_row = correlated_rows[segment_start]
        last_row = correlated_rows[segment_end - 1]
        segment = _segment_source(source, first_row, last_row)
        segment["correlation_rule"] = {
            "order": decision.rule_order,
            "name": decision.rule_name,
        }
        segment["warnings"] = list(decision.warning_codes)
        if decision.ticker_reuse_gap is not None:
            segment["ticker_reuse_gap"] = decision.ticker_reuse_gap.as_report_object()
        if decision.distinct_uuid_transition is not None:
            segment["identity_partition"] = decision.distinct_uuid_transition.as_report_object()
        elif decision.current_symbol_reuse is not None:
            segment["identity_partition"] = decision.current_symbol_reuse.as_report_object()
        if decision.branch == CORRELATED_BRANCH:
            if decision.asset_id is None:
                raise SecurityCorrelationError("correlated row decision has no Alpaca asset UUID")
            identity_basis = (
                "explicit_uuid_lineage"
                if any(
                    decision.asset_id in (edge.predecessor_asset_id, edge.successor_asset_id)
                    for edge in uuid_lineage_edges
                )
                else "single_alpaca_asset_id"
            )
            identity_basis = (
                decision.rule_name
                if decision.use_rule_identity_basis
                and not (
                    decision.distinct_uuid_transition is not None and identity_basis == "explicit_uuid_lineage"
                )
                else identity_basis
            )
            historical_symbol = decision.historical_symbol or (
                decision.interval.historical_symbol if decision.interval is not None else requested_symbol
            )
            security_identity = security_identity_for_alpaca_assets(
                (decision.asset_id,),
                uuid_lineage_edges,
                tickers=(historical_symbol,),
                basis=identity_basis,
            )
            segment.update(
                {
                    "historical_symbol": historical_symbol,
                    "canonical_security": _canonical_security(
                        security_identity.security_id,
                        None if decision.component_only else decision.asset_id,
                        decision.cusip,
                    ),
                    "security_identity": security_identity.as_report_object(),
                    "identity_confidence": decision.identity_confidence,
                    "correlation_method": decision.correlation_method or decision.rule_name,
                    "ticker_ownership_interval": (
                        {
                            **replace(
                                decision.interval,
                                start_date_inclusive=(
                                    first_row.session_date
                                    if decision.interval.start_date_inclusive is None
                                    and any(
                                        rule.name == "name_change_ticker_transition"
                                        for rule in decision.interval.applied_rules
                                    )
                                    else decision.interval.start_date_inclusive
                                ),
                            ).as_report_object(),
                            "cusip": decision.interval.cusip,
                            "asset_id": decision.interval.asset_id,
                        }
                        if decision.interval is not None
                        else None
                    ),
                }
            )
        else:
            symbol_disposition = lineage.get("symbol_disposition")
            series_identity = (
                symbol_disposition.get("market_data_series_identity")
                if isinstance(symbol_disposition, dict)
                else None
            )
            segment.update(
                {
                    "reason_codes": list(decision.reason_codes),
                    "candidate_securities": _candidate_report_objects(
                        decision,
                        lineage,
                        requested_symbol,
                        intervals,
                        first_row.session_date,
                    ),
                    "market_data_series_identity": (
                        series_identity if isinstance(series_identity, dict) else None
                    ),
                }
            )
        branches[decision.branch].append(segment)
        segment_start = segment_end

    partitioned_row_count = sum(
        segment["row_count"] for branch_segments in branches.values() for segment in branch_segments
    )
    if partitioned_row_count != len(rows):
        raise SecurityCorrelationError(f"row partition invariant failed: {partitioned_row_count} != {len(rows)}")
    return apply_terminal_continuous_ticker_fallback(
        branches,
        tuple(row.session_date for row in correlated_rows),
        source,
        lineage,
        uuid_lineage_edges,
        source_rows=rows,
        adjustment_consensus=adjustment_consensus,
        closed_history_evidence=closed_history_evidence,
        rule_order=len(ROW_CORRELATION_RULES) + 1,
    )

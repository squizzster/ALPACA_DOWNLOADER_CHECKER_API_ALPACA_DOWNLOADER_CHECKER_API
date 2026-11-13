from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from libs.alpaca_security_lineage import EXCHANGE_TEST_SYMBOL, HISTORICAL_MARKET_DATA_ONLY

from . import public_common_continuation, security_relations, sourced_rename
from .closed_departure import METHOD as CURRENT_SOURCE_METHOD
from .closed_departure import CurrentSourceOwnership, assess_current_source, continue_current_source
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
from .name_change import (
    PRE_ENTRY_CARRY_FORWARD_REASON,
    NameChangeAssessment,
    assess_name_change_history,
    name_change_dates,
)
from .occupancy_anchor import dated_occupancy_anchors
from .ownership_interval_rules import prepare_ticker_ownership
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
from .sourced_succession import certify_sourced_successions
from .terminal_episode import (
    VACANT_TICKER_BRIDGE_REASON,
    DatedDeparturePrefix,
    HistoricalAsOfGap,
    TerminalTradedEpisode,
    VacantTickerBridge,
)
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
    adjustment_session_consensus,
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
    vacant_ticker_bridges: tuple[VacantTickerBridge, ...] = ()
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
    terminal_carry_forward_exclusions: tuple = ()
    pre_entry_carry_forward_exclusions: tuple = ()
    closed_prior_action_anchored_suffixes: bool = False
    current_source_ownership: CurrentSourceOwnership | None = None
    sourced_rename_mapping: sourced_rename.RenameCertificate | sourced_rename.DisputedMapping | None = None


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

    def __init__(
        self,
        *,
        ownership_interval_rules=None,
        row_correlation_rules=None,
        exact_same_symbol_handoffs=True,
        bounded_entry_same_symbol_handoffs=True,
        partial_shared_uuid_suffixes=True,
        single_session_shared_uuid_histories=True,
        early_successor_source_bridges=True,
        complete_certificate_conflicts=True,
        completed_event_scope=True,
        terminal_carry_forward_exclusions=True,
        post_padding_shared_uuid_suffixes=True,
        split_adjusted_post_padding_aliases=True,
        vacant_ticker_bridge_exclusions=True,
        action_anchored_vacant_ticker_bridges=True,
        pre_entry_carry_forward_exclusions=True,
        component_anchored_early_successors=True,
        extended_component_handoffs=True,
        internal_component_handoffs=True,
        mapped_successor_prefixes=True,
        mapped_successor_split_chains=True,
        active_successor_prefixes=True,
        closed_prior_action_anchored_suffixes=True,
        stable_phase_handoffs=True,
        sourced_succession_certificates=False,
        sourced_rename_certificates=False,
        public_common_certificates=False,
        new_public_parent_continuations=False,
        closed_departure_current_sources=True,
    ):
        if any(
            (
                sourced_succession_certificates,
                sourced_rename_certificates,
                public_common_certificates,
                new_public_parent_continuations,
            )
        ):
            from .errors import SecurityCorrelationError

            raise SecurityCorrelationError(
                "researched assertion certificates are inadmissible for new inference"
            )
        self.ownership_interval_rules = ownership_interval_rules
        self.row_correlation_rules = row_correlation_rules
        self.exact_same_symbol_handoffs = exact_same_symbol_handoffs
        self.bounded_entry_same_symbol_handoffs = bounded_entry_same_symbol_handoffs
        self.partial_shared_uuid_suffixes = partial_shared_uuid_suffixes
        self.single_session_shared_uuid_histories = single_session_shared_uuid_histories
        self.early_successor_source_bridges = early_successor_source_bridges
        self.complete_certificate_conflicts = complete_certificate_conflicts
        self.completed_event_scope = completed_event_scope
        self.terminal_carry_forward_exclusions = terminal_carry_forward_exclusions
        self.post_padding_shared_uuid_suffixes = post_padding_shared_uuid_suffixes
        self.split_adjusted_post_padding_aliases = split_adjusted_post_padding_aliases
        self.vacant_ticker_bridge_exclusions = vacant_ticker_bridge_exclusions
        self.action_anchored_vacant_ticker_bridges = action_anchored_vacant_ticker_bridges
        self.pre_entry_carry_forward_exclusions = pre_entry_carry_forward_exclusions
        self.component_anchored_early_successors = component_anchored_early_successors
        self.extended_component_handoffs = extended_component_handoffs
        self.internal_component_handoffs = internal_component_handoffs
        self.mapped_successor_prefixes = mapped_successor_prefixes
        self.mapped_successor_split_chains = mapped_successor_split_chains
        self.active_successor_prefixes = active_successor_prefixes
        self.closed_prior_action_anchored_suffixes = closed_prior_action_anchored_suffixes
        self.stable_phase_handoffs = stable_phase_handoffs
        self.sourced_succession_certificates = sourced_succession_certificates
        self.sourced_rename_certificates = sourced_rename_certificates
        self.public_common_certificates = public_common_certificates
        self.new_public_parent_continuations = new_public_parent_continuations
        self.accepted_public_common_membership = None
        self.accepted_security_relations = None
        self._public_common_membership = None
        self._security_relations = None
        self._public_common_sources = {}
        self.accepted_common_bounds = {}
        self.accepted_common_routes = ()
        self.accepted_common_sources = {}
        self._rename_group = None
        self._rename_sources = {}
        self._rename_mappings = {}
        self._rename_histories = {}
        self.accepted_rename_sources = {}
        self.accepted_rename_membership = None
        self._rename_membership_report = None
        self.emit_empty_rename_membership = True
        self.selected_evidence = None
        self.rename_scope_complete = True
        self.closed_departure_current_sources = closed_departure_current_sources
        self._sessions = {}
        self._downloads = {}
        self._decisions = {}
        self._lineage = {}
        self._provider_evidence_acceptance = {}
        self._name_change_sources = {}
        self._name_changes = {}
        self._continuity = {}
        self._uuid_candidates = {}
        self._sourced_successions = {}
        self._source_work = {}
        self.accepted_identity_memberships = {}
        self.accepted_current_source_ownership = {}
        self._current_source_ownership = {}
        self._session_consensus = {}

    def provider_evidence(self, lineage):
        """Accept fresh evidence once; every phase consumes the same object."""
        from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence, accept_provider_evidence

        if isinstance(lineage, ProviderEvidence):
            return lineage
        key = id(lineage)
        if key not in self._provider_evidence_acceptance:
            self._provider_evidence_acceptance[key] = (lineage, accept_provider_evidence(lineage))
        # Chronology caches are bound to this exact producer input identity.
        # Acceptance is retained by the owner; do not replace that identity.
        return lineage

    def common_membership(self, lineage):
        if self._public_common_membership is None:
            self._public_common_membership = self.accepted_public_common_membership or (
                public_common_continuation.certify_group(
                    (self.selected_evidence or {"selected": lineage}).values(),
                    complete=self.rename_scope_complete,
                    new_public_parent=self.new_public_parent_continuations,
                )
                if self.public_common_certificates
                else {"version": 1, "certificates": [], "issues": []}
            )
        return self._public_common_membership

    def relation_certificate(self, lineage):
        if self._security_relations is None:
            self._security_relations = self.accepted_security_relations or (
                security_relations.certify_group(
                    (self.selected_evidence or {"selected": lineage}).values(),
                    complete=self.rename_scope_complete,
                    extended_terms=self.new_public_parent_continuations,
                )
                if self.public_common_certificates
                else {"version": 1, "relations": [], "issues": []}
            )
        return self._security_relations

    def common_source(self, symbol, lineage, *args, **kwargs):
        if symbol in self.accepted_common_sources:
            if symbol not in self._public_common_sources:
                handoff = self.accepted_common_sources[symbol]
                self._public_common_sources[symbol] = (
                    handoff,
                    public_common_continuation.histories(handoff, self.common_membership(lineage)),
                )
            return self._public_common_sources[symbol]
        if symbol not in self._public_common_sources:
            self._public_common_sources[symbol] = (
                public_common_continuation.source_history(
                    symbol,
                    lineage,
                    self.common_membership(lineage),
                    *args,
                    accepted_bounds=self.accepted_common_bounds,
                    accepted_routes=self.accepted_common_routes,
                    **kwargs,
                )
                if self.public_common_certificates and self.common_membership(lineage)["certificates"]
                else (None, ())
            )
        return self._public_common_sources[symbol]

    def rename_certificates(self, lineage):
        if not self.sourced_rename_certificates:
            return (), ()
        if self._rename_group is None:
            if self.accepted_rename_membership is not None:
                self._rename_group = (
                    tuple(
                        sourced_rename.RenameCertificate.from_report(v)
                        for v in self.accepted_rename_membership["certificates"]
                    ),
                    tuple(self.accepted_rename_membership["issues"]),
                )
            else:
                self._rename_group = sourced_rename.certify_group(
                    (self.selected_evidence or {"selected": lineage}).values(),
                    complete=self.rename_scope_complete,
                    relations=self.relation_certificate(lineage) if self.public_common_certificates else None,
                )
        return self._rename_group

    def rename_membership_report(self, lineage):
        if not self.sourced_rename_certificates:
            if self.emit_empty_rename_membership:
                return {"version": 1, "certificates": [], "issues": []}
            return None
        if self._rename_membership_report is None:
            certificates, issues = self.rename_certificates(lineage)
            self._rename_membership_report = self.accepted_rename_membership or {
                "version": 1,
                "certificates": [c.to_report() for c in certificates],
                "issues": list(issues),
            }
        return self._rename_membership_report

    def rename_source(self, symbol, lineage, *args, **kwargs):
        if not self.sourced_rename_certificates:
            return None
        if symbol not in self._rename_sources:
            accepted = self.accepted_rename_sources.get(symbol)
            self._rename_mappings[symbol] = (
                next(c for c in self.rename_certificates(lineage)[0] if c.action_id == accepted[0]["action_id"])
                if accepted is not None
                else sourced_rename.mapped_certificate(
                    symbol, lineage, self.rename_certificates(lineage)[0], args[0]
                )
            )
            self._rename_sources[symbol] = (
                accepted[0]
                if accepted is not None
                else sourced_rename.source_history(symbol, self._rename_mappings[symbol], *args, **kwargs)
            )
            self._rename_histories[symbol] = sourced_rename.histories(
                self._rename_sources[symbol], self._rename_mappings[symbol]
            )
        return self._rename_sources[symbol]

    def source_session_consensus(self, items):
        ordered = sorted(items, key=lambda item: item[0].path.name)
        key = tuple(id(item[0]) for item in ordered)
        if key not in self._session_consensus:
            self._session_consensus[key] = adjustment_session_consensus(
                (f"{source['adjustment']}:{download.path.name}", sessions)
                for download, source, sessions in ordered
            )
        return self._session_consensus[key]

    def current_source_ownership(self, symbol, lineage, *args, **kwargs):
        if not self.closed_departure_current_sources:
            return None
        # The selected group and its source/evidence bindings are immutable.
        # Preserve even a completed negative assessment across views and phases.
        if symbol not in self._current_source_ownership:
            accepted = self.accepted_current_source_ownership.get(symbol)
            self._current_source_ownership[symbol] = (
                continue_current_source(accepted[0], lineage, accepted[1])
                if accepted is not None
                else assess_current_source(
                    symbol,
                    lineage,
                    *args,
                    complete_sources=self._name_change_sources,
                    source_consensus=self.source_session_consensus,
                    **kwargs,
                )
            )
        return self._current_source_ownership[symbol]

    def prepared_source(self, items):
        from .terminal_episode import PreparedSource

        key = tuple(id(download) for download, _, _ in items)
        if key not in self._source_work:
            self._source_work[key] = PreparedSource(items)
        return self._source_work[key]

    def inherit_continuity(self, established):
        # Same selected inputs and established rules; only the certified fourth
        # rule changes. Reuse the completed chronology and continuity authorities.
        self._public_common_membership = established._public_common_membership
        self._security_relations = established._security_relations
        self._public_common_sources = dict(established._public_common_sources)
        self.accepted_common_bounds = established.accepted_common_bounds
        self.accepted_common_routes = established.accepted_common_routes
        self._rename_group = established._rename_group
        self._rename_membership_report = established._rename_membership_report
        self._rename_sources = dict(established._rename_sources)
        self._rename_mappings = dict(established._rename_mappings)
        self._rename_histories = dict(established._rename_histories)
        self._provider_evidence_acceptance = dict(established._provider_evidence_acceptance)
        self._continuity = dict(established._continuity)
        self._uuid_candidates = dict(established._uuid_candidates)
        self._sourced_successions = dict(established._sourced_successions)
        self._source_work = dict(established._source_work)
        self._current_source_ownership = dict(established._current_source_ownership)
        self._session_consensus = dict(established._session_consensus)

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
        lineage = self.provider_evidence(lineage)
        # Cache identity is safe only while the caller retains these immutable
        # inputs and the bound rules. DO NOT add filesystem freshness probes or
        # replay the analysis for each adjustment view; discard on input reload.
        key = (id(lineage), include_action_anchored_current_suffix)
        cached = self._lineage.get(key)
        if cached is None:
            shared = self._continuity.get(id(lineage)) if self.stable_phase_handoffs else None
            if shared is None:
                if id(lineage) not in self._uuid_candidates:
                    candidates, edge_issues = build_uuid_lineage_edges(lineage, candidates_only=True)
                    # The completed graph is authoritative even when later
                    # preparation fails. Retain immutable producer values and
                    # copy only their containers before adding sourced edges.
                    self._uuid_candidates[id(lineage)] = (lineage, tuple(candidates), tuple(edge_issues))
                _, accepted_candidates, accepted_issues = self._uuid_candidates[id(lineage)]
                candidates, edge_issues = list(accepted_candidates), list(accepted_issues)
                if id(lineage) not in self._sourced_successions:
                    successions, succession_issues = (
                        certify_sourced_successions(lineage)
                        if self.sourced_succession_certificates
                        else ((), ())
                    )
                    # Retain the completed certificate immediately. A peer or
                    # later chronology failure cannot authorize its proof again.
                    self._sourced_successions[id(lineage)] = (lineage, successions, succession_issues)
                _, successions, succession_issues = self._sourced_successions[id(lineage)]
                renames, rename_issues = self.rename_certificates(lineage)
                if renames:
                    renamed_actions = {certificate.action_id for certificate in renames}
                    candidates = [edge for edge in candidates if edge.action_id not in renamed_actions]
                    candidates.extend(certificate.identity_edge() for certificate in renames)
                candidates.extend(edge for c in renames for edge in c.label_edges())
                edge_issues.extend(rename_issues)
                common = self.common_membership(lineage)
                candidates.extend(
                    public_common_continuation.CommonCertificate(**item).identity_edge()
                    for item in common["certificates"]
                )
                edge_issues.extend(common["issues"])
                candidates.extend(successions)
                edge_issues.extend(succession_issues)
            else:
                _, candidates, edges, edge_issues, assessment, prepared = shared
            # DO NOT REMOVE WITHOUT USER AUTH!#
            # Build each evidence authority once. Interval construction consumes
            # accepted split edges; cross-UUID name candidates still require the
            # subsequent full-group history certificate before membership use.
            if self.stable_phase_handoffs:
                # Chronology and continuity are produced once, before either row
                # phase. A dated later occupant may disambiguate a historical
                # rename; that does NOT authorize fallback row allocation early.
                # The established view still exposes only the first three rules.
                prepared = (
                    prepared.with_fallback(self.ownership_interval_rules[3])
                    if shared is not None
                    else prepare_ticker_ownership(
                        lineage,
                        uuid_lineage_edges=candidates,
                        rules=self.ownership_interval_rules,
                        include_action_anchored_current_suffix=True,
                        allow_closed_prior_action_anchored_suffix=self.closed_prior_action_anchored_suffixes,
                        closed_prior_terminal_conflicts=self.completed_event_scope,
                        sourced_successions=successions,
                    )
                )
                if shared is None and renames:
                    prepared = replace(
                        prepared,
                        established=sourced_rename.apply_chronology(prepared.established, renames),
                        complete=sourced_rename.apply_chronology(prepared.complete, renames),
                    )
                intervals = list(prepared.complete)
                phase_intervals = {
                    False: (prepared.established, prepared.established_issues),
                    True: (prepared.complete, prepared.complete_issues),
                }
            else:
                intervals, interval_issues = build_ticker_ownership_intervals(
                    lineage,
                    uuid_lineage_edges=candidates,
                    rules=self.ownership_interval_rules,
                    include_action_anchored_current_suffix=include_action_anchored_current_suffix,
                    allow_closed_prior_action_anchored_suffix=self.closed_prior_action_anchored_suffixes,
                    closed_prior_terminal_conflicts=self.completed_event_scope,
                    sourced_successions=successions,
                )
                phase_intervals = {include_action_anchored_current_suffix: (intervals, interval_issues)}
            assessment = (
                assessment
                if shared is not None
                else assess_name_change_history(
                    lineage,
                    intervals,
                    [edge for edge in candidates if edge.action_id not in {c.action_id for c in renames}],
                    self._name_change_sources,
                    exact_same_symbol_handoffs=self.exact_same_symbol_handoffs,
                    bounded_entry_same_symbol_handoffs=self.bounded_entry_same_symbol_handoffs,
                    partial_shared_uuid_suffixes=self.partial_shared_uuid_suffixes,
                    single_session_shared_uuid_histories=self.single_session_shared_uuid_histories,
                    early_successor_source_bridges=self.early_successor_source_bridges,
                    complete_certificate_conflicts=self.complete_certificate_conflicts,
                    completed_event_scope=self.completed_event_scope,
                    terminal_carry_forward_exclusions=self.terminal_carry_forward_exclusions,
                    post_padding_shared_uuid_suffixes=self.post_padding_shared_uuid_suffixes,
                    split_adjusted_post_padding_aliases=self.split_adjusted_post_padding_aliases,
                    pre_entry_carry_forward_exclusions=(self.pre_entry_carry_forward_exclusions),
                    component_anchored_early_successors=(self.component_anchored_early_successors),
                    extended_component_handoffs=self.extended_component_handoffs,
                    internal_component_handoffs=self.internal_component_handoffs,
                )
            )
            if shared is None:
                if renames:
                    assessment = replace(
                        assessment, accepted=assessment.accepted | frozenset(c.action_id for c in renames)
                    )
                edges, continuity_issues = certify_name_change_edges(candidates, assessment.accepted)
                edge_issues.extend(continuity_issues)
                if self.stable_phase_handoffs:
                    self._continuity[id(lineage)] = (
                        lineage,
                        candidates,
                        edges,
                        tuple(edge_issues),
                        assessment,
                        prepared,
                    )
            independent = {
                edge.action_id: edge
                for edge in candidates
                if edge.action_id in assessment.independent_successors
            }
            for enabled, (intervals, interval_issues) in phase_intervals.items():
                selected_key = (id(lineage), enabled)
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
                self._name_changes[selected_key] = assessment
                self._lineage[selected_key] = (
                    lineage,
                    intervals,
                    tuple(interval_issues),
                    edges,
                    tuple(edge_issues),
                )
            cached = self._lineage[key]
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
    if (
        isinstance(context.sourced_rename_mapping, sourced_rename.DisputedMapping)
        and context.session_date < context.sourced_rename_mapping.boundary_date
    ):
        return _RowDecision(
            branch=UNCORRELATED_BRANCH, reason_codes=("sourced_rename_disputed_predecessor_membership",)
        )
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
        predecessor_facts = {fact for rule in predecessor.applied_rules for fact in rule.supporting_fact_ids}
        successor_facts = {fact for rule in successor.applied_rules for fact in rule.supporting_fact_ids}
        if not any(
            edge.edge_type in ("name_change", "stock_merger", "holding_company_succession")
            and edge.effective_date == boundary
            and (edge.predecessor_symbol, edge.predecessor_cusip, edge.predecessor_asset_id)
            == predecessor.identity_key()
            and (edge.successor_symbol, edge.successor_cusip, edge.successor_asset_id)
            == successor.identity_key()
            and (
                edge.action_id in predecessor_actions & successor_actions
                if edge.source_fact_id is None
                else edge.source_fact_id in predecessor_facts & successor_facts
            )
            for edge in edges
        ):
            return frozenset()
    # Only the original predecessor gains permission to retain its inferred
    # lower bound. A split-only link, recurrence, unrelated reuse, foreign-label
    # path or a common future successor cannot establish this handoff chain.
    return frozenset((occupants[0].interval_key(),))


def _apply_requested_symbol_interval_rule(context: _RowRuleContext) -> _RowDecision | None:
    if (
        context.sourced_rename_mapping is not None
        and context.session_date < context.sourced_rename_mapping.boundary_date
    ):
        # A current entity mapping cannot fall back to an unrelated previous
        # occupant of its request label. Only source-certified prior intervals
        # may allocate this mapped prefix; membership alone does not own rows.
        return _RowDecision(
            branch=UNCORRELATED_BRANCH, reason_codes=("sourced_rename_mapped_history_requires_source_scope",)
        )
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
                and not (
                    context.closed_prior_action_anchored_suffixes
                    and requested_interval.end_date_exclusive is not None
                    and interval.start_date_inclusive is not None
                    and interval.start_date_inclusive >= requested_interval.end_date_exclusive
                    and any(
                        rule.name == "action_anchored_current_ticker_occupancy"
                        for rule in interval.applied_rules
                    )
                )
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
    if any(
        item.symbol == context.requested_symbol and item.first <= context.session_date <= item.last
        for item in context.pre_entry_carry_forward_exclusions
    ):
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=(PRE_ENTRY_CARRY_FORWARD_REASON,),
        )
    excluded = [
        item
        for item in context.terminal_carry_forward_exclusions
        if item.symbol == context.requested_symbol and item.first <= context.session_date <= item.last
    ]
    excluded_candidates = {(item.asset_id, item.cusip) for item in excluded}
    if len(excluded_candidates) == 1:
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=("terminal_nontrading_carry_forward",),
            candidates=tuple(sorted(excluded_candidates)),
        )
    own_intervals = [item for item in context.intervals if item.historical_symbol == context.requested_symbol]
    current_source = context.current_source_ownership
    if current_source is not None and current_source.contains(context.session_date):
        return _RowDecision(
            branch=CORRELATED_BRANCH,
            asset_id=current_source.asset_id,
            identity_confidence="source_bound_current_candidate",
            warning_codes=("missing_dated_ticker_ownership_interval",),
            historical_symbol=current_source.symbol,
            correlation_method=CURRENT_SOURCE_METHOD,
        )
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
    if any(bridge.contains(context.session_date) for bridge in context.vacant_ticker_bridges):
        return _RowDecision(
            branch=UNCORRELATED_BRANCH,
            reason_codes=(VACANT_TICKER_BRIDGE_REASON,),
        )
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


def row_correlation_rule_order(*, rules: tuple[RowCorrelationRule, ...] | None = None) -> list[dict[str, Any]]:
    rules = ROW_CORRELATION_RULES if rules is None else rules
    row_rules = [{"order": rule_order, "name": rule.name} for rule_order, rule in enumerate(rules, start=1)]
    return [
        *row_rules,
        {
            "order": len(row_rules) + 1,
            "name": TERMINAL_FALLBACK_RULE_NAME,
        },
    ]


def _decide_row(
    context: _RowRuleContext,
    *,
    rules: tuple[RowCorrelationRule, ...] | None = None,
) -> _RowDecision:
    rules = ROW_CORRELATION_RULES if rules is None else rules
    for rule_order, rule in enumerate(rules, start=1):
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
    vacant_ticker_bridges: tuple[VacantTickerBridge, ...] = (),
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
    row_rules: tuple[RowCorrelationRule, ...] | None = None,
    established_segments: tuple[dict[str, Any], ...] = (),
    current_source_ownership: CurrentSourceOwnership | None = None,
    sourced_rename_histories=(),
    sourced_rename_mapping=None,
) -> dict[str, list[dict[str, Any]]]:
    from libs.alpaca_security_lineage.inference_evidence import accept_provider_evidence

    if sourced_rename_histories or sourced_rename_mapping is not None:
        raise SecurityCorrelationError("researched rename source recipes are inadmissible for fresh rows")
    lineage = cache.provider_evidence(lineage) if cache is not None else accept_provider_evidence(lineage)
    # This owner allocates rows from supplied intervals and bounded certificates.
    # Those proofs belong to the same selected group; DO NOT reacquire evidence
    # per row/view or promote a dependency link into identity membership. Output
    # remains explicitly unresolved where the ordered rules cannot assign it.
    if cache is not None:
        if row_rules is None:
            row_rules = cache.row_correlation_rules
        elif cache.row_correlation_rules is None:
            raise SecurityCorrelationError("explicit row rules require a policy-bound correlation cache")
    row_rules = ROW_CORRELATION_RULES if row_rules is None else row_rules
    if (
        cache is not None
        and cache.row_correlation_rules is not None
        and tuple(cache.row_correlation_rules) != tuple(row_rules)
    ):
        raise SecurityCorrelationError("correlation cache row policy does not match requested rules")
    requested_symbol = source["requested_symbol"]
    independent_history = (
        *independent_history,
        *(
            IndependentHistoricalInterval(item.first, item.last, item.interval)
            for item in (*name_change_assessment.histories, *sourced_rename_histories)
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

    ticker_reuse_gap = (
        None
        if current_source_ownership is not None
        else assess_ticker_reuse_gap(
            tuple(session_date for _, _, session_date in parsed_rows),
            source,
            lineage,
            intervals,
            adjustment_consensus,
            evidence_issues,
        )
    )
    current_symbol_reuse = (
        None
        if current_source_ownership is not None
        else assess_current_symbol_reuse(
            tuple(session_date for _, _, session_date in parsed_rows),
            source,
            lineage,
            intervals,
            adjustment_consensus,
            evidence_issues,
        )
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
        current_source_ownership=current_source_ownership,
        sourced_rename_mapping=sourced_rename_mapping,
        dated_same_symbol_predecessors=dated_same_symbol_predecessors,
        predecessor_prefixes=predecessor_prefixes,
        mapped_continuations=mapped_continuations,
        occupancy_anchors=occupancy_anchors,
        same_symbol_continuation_ids=continuation_ids,
        terminal_episode=terminal_episode,
        bridged_episodes=bridged_episodes,
        vacant_ticker_bridges=vacant_ticker_bridges,
        unrelated_cash_acquiree_intervals=unrelated_cash_acquiree_intervals,
        departure_prefix=departure_prefix,
        historical_asof_gap=historical_asof_gap,
        independent_history=independent_history,
        adopted_target_pairs=adopted_target_pairs,
        retired_source_selection=retired_selection,
        retired_lower_bound=retired_lower_bound,
        retired_continuation=retired_continuation,
        bounded_aliases=bounded_aliases,
        terminal_carry_forward_exclusions=name_change_assessment.terminal_carry_forward_exclusions,
        pre_entry_carry_forward_exclusions=(name_change_assessment.pre_entry_carry_forward_exclusions),
        closed_prior_action_anchored_suffixes=(
            cache.closed_prior_action_anchored_suffixes if cache is not None else True
        ),
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
    # The caller supplies this phase's immutable, same-input allocation. Reserve
    # its exact segments before invoking rules. Do not reconstruct membership or
    # coalesce these segments with newly resolved neighbours.
    established = iter(sorted(established_segments, key=lambda item: item["row_start"]))
    protected = next(established, None)
    for row_number, timestamp, session_date in parsed_rows:
        while protected is not None and row_number > protected["row_end"]:
            protected = next(established, None)
        if protected is not None and row_number >= protected["row_start"]:
            continue
        decision = decisions.get(session_date)
        if decision is None:
            decision = decisions[session_date] = _decide_row(
                _RowRuleContext(session_date=session_date, **context),
                rules=row_rules,
            )
        correlated_rows.append(
            _CorrelatedRow(
                row_number=row_number,
                timestamp=timestamp,
                session_date=session_date,
                decision=decision,
            )
        )

    branches = {CORRELATED_BRANCH: list(established_segments), UNCORRELATED_BRANCH: []}
    segment_start = 0
    while segment_start < len(correlated_rows):
        decision = correlated_rows[segment_start].decision
        grouping_key = decision.grouping_key()
        segment_end = segment_start + 1
        while (
            segment_end < len(correlated_rows)
            and correlated_rows[segment_end].row_number == correlated_rows[segment_end - 1].row_number + 1
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
            accepted_identity = (
                cache.accepted_identity_memberships.get(requested_symbol, {}).get(decision.asset_id)
                if cache is not None
                else None
            )
            # A dated UUID selection does not revoke that UUID's completed public
            # membership. The continuation owner supplied this applicable handoff;
            # consume it without rebuilding or hashing the accepted identity.
            identity = (
                accepted_identity
                or security_identity_for_alpaca_assets(
                    (decision.asset_id,), uuid_lineage_edges, tickers=(historical_symbol,), basis=identity_basis
                ).as_report_object()
            )
            segment.update(
                {
                    "historical_symbol": historical_symbol,
                    "canonical_security": _canonical_security(
                        identity["security_id"],
                        None if decision.component_only else decision.asset_id,
                        decision.cusip,
                    ),
                    "security_identity": identity,
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
    branches[CORRELATED_BRANCH].sort(key=lambda item: item["row_start"])
    return apply_terminal_continuous_ticker_fallback(
        branches,
        tuple(session_date for _, _, session_date in parsed_rows),
        source,
        lineage,
        uuid_lineage_edges,
        source_rows=rows,
        adjustment_consensus=adjustment_consensus,
        closed_history_evidence=closed_history_evidence,
        rule_order=len(row_rules) + 1,
    )

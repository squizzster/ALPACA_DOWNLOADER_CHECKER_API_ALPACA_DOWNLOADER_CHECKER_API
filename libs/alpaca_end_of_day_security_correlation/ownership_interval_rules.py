from dataclasses import dataclass
from datetime import date
from decimal import InvalidOperation
from typing import Any, Callable

from .action_identity import (
    complete_stock_merger_endpoints,
    exact_cusip_lookup_asset,
    exact_symbol_lookup_asset,
)
from .name_change import name_change_dates
from .occupancy_anchor import competing_terminal_anchors, positive_occupancy_action_date
from .security_identity import security_identity_for_alpaca_assets
from .uuid_lineage import IDENTITY_PRESERVING_SPLIT_EDGE_TYPES, build_uuid_lineage_edges


@dataclass(frozen=True)
class AppliedOwnershipIntervalRule:
    order: int
    name: str
    supporting_action_ids: tuple[str, ...]

    def as_report_object(self) -> dict[str, Any]:
        return {
            "order": self.order,
            "name": self.name,
            "supporting_corporate_action_ids": list(self.supporting_action_ids),
        }


@dataclass(frozen=True)
class TickerOwnershipInterval:
    historical_symbol: str
    cusip: str
    asset_id: str
    start_date_inclusive: date | None
    end_date_exclusive: date | None
    applied_rules: tuple[AppliedOwnershipIntervalRule, ...]

    def contains(self, session_date: date) -> bool:
        return (self.start_date_inclusive is None or session_date >= self.start_date_inclusive) and (
            self.end_date_exclusive is None or session_date < self.end_date_exclusive
        )

    def identity_key(self) -> tuple[str, str, str]:
        return self.historical_symbol, self.cusip, self.asset_id

    def interval_key(self) -> tuple[str, str, str, date | None, date | None]:
        return (*self.identity_key(), self.start_date_inclusive, self.end_date_exclusive)

    def as_report_object(self) -> dict[str, Any]:
        supporting_action_ids = tuple(
            dict.fromkeys(
                action_id
                for applied_rule in self.applied_rules
                for action_id in applied_rule.supporting_action_ids
            )
        )
        return {
            "historical_symbol": self.historical_symbol,
            "start_date_inclusive": (
                self.start_date_inclusive.isoformat() if self.start_date_inclusive is not None else None
            ),
            "end_date_exclusive": (
                self.end_date_exclusive.isoformat() if self.end_date_exclusive is not None else None
            ),
            "supporting_corporate_action_ids": list(supporting_action_ids),
            "ownership_interval_rules": [applied_rule.as_report_object() for applied_rule in self.applied_rules],
        }


@dataclass(frozen=True)
class _OwnershipIntervalRuleContext:
    corporate_actions: tuple[dict[str, Any], ...]
    asset_ids_by_symbol_and_cusip: dict[tuple[str, str], str]
    lineage: dict[str, Any]
    uuid_lineage_edges: tuple = ()


@dataclass(frozen=True)
class _OwnershipIntervalRuleResult:
    intervals: tuple[TickerOwnershipInterval, ...]
    evidence_issues: tuple[dict[str, Any], ...] = ()


OwnershipIntervalRuleFunction = Callable[
    [_OwnershipIntervalRuleContext, tuple[TickerOwnershipInterval, ...], int, str],
    _OwnershipIntervalRuleResult,
]


@dataclass(frozen=True)
class OwnershipIntervalRule:
    name: str
    apply: OwnershipIntervalRuleFunction


@dataclass(frozen=True)
class _NameChange:
    action_id: str
    departure_date: date
    arrival_date: date
    old_symbol: str
    old_cusip: str
    new_symbol: str
    new_cusip: str


@dataclass(frozen=True)
class _CashMerger:
    action_id: str
    boundary_date: date
    acquiree_symbol: str
    acquiree_cusip: str
    asset_id: str


@dataclass(frozen=True)
class _SameTickerStockMergerTransition:
    action_id: str
    boundary_date: date
    symbol: str
    predecessor_cusip: str
    predecessor_asset_id: str
    successor_cusip: str
    successor_asset_id: str
    completed_symbol: bool


def _rule_reference(rule_order: int, rule_name: str) -> dict[str, Any]:
    return {"order": rule_order, "name": rule_name}


def _merge_exact_intervals(
    intervals: tuple[TickerOwnershipInterval, ...] | list[TickerOwnershipInterval],
) -> tuple[TickerOwnershipInterval, ...]:
    intervals_by_key: dict[tuple, TickerOwnershipInterval] = {}
    for interval in intervals:
        existing = intervals_by_key.get(interval.interval_key())
        if existing is None:
            intervals_by_key[interval.interval_key()] = interval
            continue
        applied_rules_by_key = {
            (rule.order, rule.name, rule.supporting_action_ids): rule
            for rule in (*existing.applied_rules, *interval.applied_rules)
        }
        intervals_by_key[interval.interval_key()] = TickerOwnershipInterval(
            historical_symbol=existing.historical_symbol,
            cusip=existing.cusip,
            asset_id=existing.asset_id,
            start_date_inclusive=existing.start_date_inclusive,
            end_date_exclusive=existing.end_date_exclusive,
            applied_rules=tuple(applied_rules_by_key.values()),
        )
    return tuple(
        sorted(
            intervals_by_key.values(),
            key=lambda item: (
                item.start_date_inclusive or date.min,
                item.end_date_exclusive or date.max,
                item.historical_symbol,
                item.cusip,
                item.asset_id,
            ),
        )
    )


def _parse_name_changes(
    context: _OwnershipIntervalRuleContext,
    rule_order: int,
    rule_name: str,
) -> tuple[list[_NameChange], list[dict[str, Any]]]:
    name_changes = []
    evidence_issues = []
    for action in context.corporate_actions:
        if action.get("action_type") != "name_change":
            continue
        required_values = {
            field_name: action.get(field_name)
            for field_name in (
                "id",
                "old_symbol",
                "old_cusip",
                "new_symbol",
                "new_cusip",
            )
        }
        if not all(isinstance(value, str) and value for value in required_values.values()):
            evidence_issues.append(
                {
                    "reason_code": "malformed_name_change",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action": action,
                }
            )
            continue
        dates, issue = name_change_dates(action, context.corporate_actions)
        if issue:
            evidence_issues.append(
                {
                    "reason_code": issue,
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": required_values["id"],
                    "process_date": action.get("process_date"),
                }
            )
            if not dates:
                continue
        name_changes.append(
            _NameChange(
                action_id=required_values["id"],
                departure_date=dates[0],
                arrival_date=dates[-1],
                old_symbol=required_values["old_symbol"],
                old_cusip=required_values["old_cusip"],
                new_symbol=required_values["new_symbol"],
                new_cusip=required_values["new_cusip"],
            )
        )
    name_changes.sort(key=lambda item: (item.departure_date, item.action_id))
    return name_changes, evidence_issues


def _later_start(left: date | None, right: date | None) -> date | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def _earlier_known_date(left: date | None, right: date | None) -> date | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _constrain_or_append_interval(
    intervals: list[TickerOwnershipInterval],
    constraint: TickerOwnershipInterval,
) -> bool:
    overlapping_indexes = []
    for index, interval in enumerate(intervals):
        if interval.identity_key() != constraint.identity_key():
            continue
        combined_start = _later_start(interval.start_date_inclusive, constraint.start_date_inclusive)
        combined_end = _earlier_known_date(interval.end_date_exclusive, constraint.end_date_exclusive)
        if combined_start is None or combined_end is None or combined_start < combined_end:
            overlapping_indexes.append(index)

    if not overlapping_indexes:
        intervals.append(constraint)
        return True
    if len(overlapping_indexes) != 1:
        return False

    interval_index = overlapping_indexes[0]
    existing_interval = intervals[interval_index]
    intervals[interval_index] = TickerOwnershipInterval(
        historical_symbol=existing_interval.historical_symbol,
        cusip=existing_interval.cusip,
        asset_id=existing_interval.asset_id,
        # An arrival cannot erase an earlier positively dated occurrence of
        # this exact symbol/CUSIP/UUID episode. Overlap is checked above: this
        # does not bridge recurrence or choose between different securities.
        start_date_inclusive=_earlier_known_date(
            existing_interval.start_date_inclusive,
            constraint.start_date_inclusive,
        ),
        end_date_exclusive=_earlier_known_date(
            existing_interval.end_date_exclusive,
            constraint.end_date_exclusive,
        ),
        applied_rules=(*existing_interval.applied_rules, *constraint.applied_rules),
    )
    return True


def _apply_name_change_rule(
    context: _OwnershipIntervalRuleContext,
    existing_intervals: tuple[TickerOwnershipInterval, ...],
    rule_order: int,
    rule_name: str,
) -> _OwnershipIntervalRuleResult:
    name_changes, evidence_issues = _parse_name_changes(context, rule_order, rule_name)
    intervals = list(existing_intervals)

    for name_change in name_changes:
        old_asset_id = context.asset_ids_by_symbol_and_cusip.get((name_change.old_symbol, name_change.old_cusip))
        if old_asset_id is None:
            evidence_issues.append(
                {
                    "reason_code": "unresolved_old_name_change_identity",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": name_change.action_id,
                    "symbol": name_change.old_symbol,
                    "cusip": name_change.old_cusip,
                }
            )
        else:
            prior_changes = [
                candidate
                for candidate in name_changes
                if candidate.arrival_date < name_change.departure_date
                and candidate.new_symbol == name_change.old_symbol
                and candidate.new_cusip == name_change.old_cusip
            ]
            prior_change = max(prior_changes, key=lambda item: item.arrival_date, default=None)
            supporting_action_ids = (
                (prior_change.action_id, name_change.action_id)
                if prior_change is not None
                else (name_change.action_id,)
            )
            old_interval = TickerOwnershipInterval(
                historical_symbol=name_change.old_symbol,
                cusip=name_change.old_cusip,
                asset_id=old_asset_id,
                # None is an unknown lower constraint, never permission to copy
                # a successor's prefix. Row ownership binds it to own-label
                # observations or independently certified source history.
                start_date_inclusive=(prior_change.arrival_date if prior_change is not None else None),
                end_date_exclusive=name_change.departure_date,
                applied_rules=(
                    AppliedOwnershipIntervalRule(
                        order=rule_order,
                        name=rule_name,
                        supporting_action_ids=supporting_action_ids,
                    ),
                ),
            )
            if not _constrain_or_append_interval(intervals, old_interval):
                evidence_issues.append(
                    {
                        "reason_code": "name_change_conflicts_with_existing_ownership_intervals",
                        "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                        "action_id": name_change.action_id,
                        "symbol": name_change.old_symbol,
                        "cusip": name_change.old_cusip,
                    }
                )

        new_asset_id = context.asset_ids_by_symbol_and_cusip.get((name_change.new_symbol, name_change.new_cusip))
        if new_asset_id is None:
            evidence_issues.append(
                {
                    "reason_code": "unresolved_new_name_change_identity",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": name_change.action_id,
                    "symbol": name_change.new_symbol,
                    "cusip": name_change.new_cusip,
                }
            )
        else:
            later_changes = [
                candidate
                for candidate in name_changes
                if candidate.departure_date > name_change.arrival_date
                and candidate.old_symbol == name_change.new_symbol
                and candidate.old_cusip == name_change.new_cusip
            ]
            later_change = min(later_changes, key=lambda item: item.departure_date, default=None)
            supporting_action_ids = (
                (name_change.action_id, later_change.action_id)
                if later_change is not None
                else (name_change.action_id,)
            )
            new_interval = TickerOwnershipInterval(
                historical_symbol=name_change.new_symbol,
                cusip=name_change.new_cusip,
                asset_id=new_asset_id,
                start_date_inclusive=name_change.arrival_date,
                end_date_exclusive=(later_change.departure_date if later_change is not None else None),
                applied_rules=(
                    AppliedOwnershipIntervalRule(
                        order=rule_order,
                        name=rule_name,
                        supporting_action_ids=supporting_action_ids,
                    ),
                ),
            )
            if not _constrain_or_append_interval(intervals, new_interval):
                evidence_issues.append(
                    {
                        "reason_code": "name_change_conflicts_with_existing_ownership_intervals",
                        "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                        "action_id": name_change.action_id,
                        "symbol": name_change.new_symbol,
                        "cusip": name_change.new_cusip,
                    }
                )

    return _OwnershipIntervalRuleResult(
        intervals=_merge_exact_intervals(intervals),
        evidence_issues=tuple(evidence_issues),
    )


def _merger_boundary_date(action: dict[str, Any]) -> tuple[str | None, object]:
    effective_date = action.get("effective_date")
    if effective_date is not None:
        return "effective_date", effective_date
    return "process_date", action.get("process_date")


def _parse_cash_mergers(
    context: _OwnershipIntervalRuleContext,
    rule_order: int,
    rule_name: str,
) -> tuple[list[_CashMerger], list[dict[str, Any]]]:
    cash_mergers = []
    evidence_issues = []
    for action in context.corporate_actions:
        if action.get("action_type") != "cash_merger":
            continue
        boundary_field, boundary_value = _merger_boundary_date(action)
        action_id = action.get("id")
        symbol = action.get("acquiree_symbol")
        cusip = action.get("acquiree_cusip")
        if not all(isinstance(value, str) and value for value in (action_id, symbol, cusip)) or not isinstance(
            boundary_value, str
        ):
            evidence_issues.append(
                {
                    "reason_code": "malformed_cash_merger",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action": action,
                }
            )
            continue
        try:
            merger_date = date.fromisoformat(boundary_value)
        except ValueError:
            evidence_issues.append(
                {
                    "reason_code": "invalid_cash_merger_boundary_date",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": action_id,
                    "boundary_field": boundary_field,
                    "boundary_date": boundary_value,
                }
            )
            continue

        asset_id = context.asset_ids_by_symbol_and_cusip.get((symbol, cusip))
        if asset_id is None:
            evidence_issues.append(
                {
                    "reason_code": "unresolved_cash_merger_acquiree_identity",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": action_id,
                    "symbol": symbol,
                    "cusip": cusip,
                }
            )
            continue
        cash_mergers.append(
            _CashMerger(
                action_id=action_id,
                boundary_date=merger_date,
                acquiree_symbol=symbol,
                acquiree_cusip=cusip,
                asset_id=asset_id,
            )
        )
    cash_mergers.sort(key=lambda merger: (merger.boundary_date, merger.action_id))
    return cash_mergers, evidence_issues


def _apply_cash_merger_terminal_rule(
    context: _OwnershipIntervalRuleContext,
    existing_intervals: tuple[TickerOwnershipInterval, ...],
    rule_order: int,
    rule_name: str,
) -> _OwnershipIntervalRuleResult:
    intervals = list(existing_intervals)
    cash_mergers, evidence_issues = _parse_cash_mergers(context, rule_order, rule_name)
    mergers_by_identity: dict[tuple[str, str, str], list[_CashMerger]] = {}
    for cash_merger in cash_mergers:
        identity_key = (
            cash_merger.acquiree_symbol,
            cash_merger.acquiree_cusip,
            cash_merger.asset_id,
        )
        mergers_by_identity.setdefault(identity_key, []).append(cash_merger)

    for identity_key, matching_mergers in mergers_by_identity.items():
        symbol, cusip, asset_id = identity_key
        merger_date = matching_mergers[0].boundary_date
        supporting_action_ids = tuple(merger.action_id for merger in matching_mergers)
        boundary_dates = sorted({merger.boundary_date for merger in matching_mergers})
        if len(boundary_dates) > 1:
            evidence_issues.append(
                {
                    "severity": "warning",
                    "reason_code": "cash_merger_boundary_disagreement",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_ids": list(supporting_action_ids),
                    "boundary_dates": [boundary_date.isoformat() for boundary_date in boundary_dates],
                    "selected_earliest_terminal_boundary": merger_date.isoformat(),
                    "symbol": symbol,
                    "cusip": cusip,
                }
            )
        identity_interval_indexes = [
            index for index, interval in enumerate(intervals) if interval.identity_key() == identity_key
        ]
        active_interval_indexes = [
            index
            for index in identity_interval_indexes
            if (
                intervals[index].start_date_inclusive is None
                or intervals[index].start_date_inclusive < merger_date
            )
            and (
                intervals[index].end_date_exclusive is None or intervals[index].end_date_exclusive >= merger_date
            )
        ]
        applied_rule = AppliedOwnershipIntervalRule(
            order=rule_order,
            name=rule_name,
            supporting_action_ids=supporting_action_ids,
        )

        if len(active_interval_indexes) == 1:
            interval_index = active_interval_indexes[0]
            existing_interval = intervals[interval_index]
            intervals[interval_index] = TickerOwnershipInterval(
                historical_symbol=existing_interval.historical_symbol,
                cusip=existing_interval.cusip,
                asset_id=existing_interval.asset_id,
                start_date_inclusive=existing_interval.start_date_inclusive,
                end_date_exclusive=merger_date,
                applied_rules=(*existing_interval.applied_rules, applied_rule),
            )
            continue

        if len(active_interval_indexes) > 1 or identity_interval_indexes:
            evidence_issues.append(
                {
                    "reason_code": "cash_merger_conflicts_with_existing_ownership_intervals",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_ids": list(supporting_action_ids),
                    "symbol": symbol,
                    "cusip": cusip,
                }
            )
            continue

        # DO NOT REMOVE WITHOUT USER AUTH!#
        # A terminal payment can close an independently established episode (the
        # branches above); it cannot establish an unlimited past for this label
        # across a distinct occupant observed on both sides of that date. A payment date can lag
        # trading or a label's reuse. Keep the action and candidate relation as
        # evidence, but do not manufacture that interval. The first rival event
        # is not an inferred handoff date. Existing row rules decide whether the
        # unchanged complete group supports ownership or must remain ambiguous.
        # This producer-only inference uses already loaded evidence. Readers do
        # not repeat it. Earlier-only occupancy can be a predecessor; same-UUID
        # CUSIP changes and already accepted split continuity are not rivals.
        # Consume the UUID producer's accepted split edges, never reauthenticate
        # them or borrow a dependency/merger edge as proof of identity continuity.
        members = security_identity_for_alpaca_assets(
            (asset_id,),
            [
                edge
                for edge in context.uuid_lineage_edges
                if edge.edge_type in IDENTITY_PRESERVING_SPLIT_EDGE_TYPES
            ],
            tickers=(symbol,),
            basis=rule_name,
        ).member_alpaca_asset_ids
        conflicts = competing_terminal_anchors(
            symbol, members, merger_date, context.lineage, context.asset_ids_by_symbol_and_cusip
        )
        if conflicts:
            evidence_issues.append(
                {
                    "reason_code": "cash_merger_unbounded_ownership_conflicts_with_dated_occupancy",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_ids": list(supporting_action_ids),
                    "symbol": symbol,
                    "cusip": cusip,
                    "asset_id": asset_id,
                    "competing_occupancy": conflicts,
                }
            )
            continue

        intervals.append(
            TickerOwnershipInterval(
                historical_symbol=symbol,
                cusip=cusip,
                asset_id=asset_id,
                start_date_inclusive=None,
                end_date_exclusive=merger_date,
                applied_rules=(applied_rule,),
            )
        )

    return _OwnershipIntervalRuleResult(
        intervals=_merge_exact_intervals(intervals),
        evidence_issues=tuple(evidence_issues),
    )


def _parse_same_ticker_stock_merger_transitions(
    context: _OwnershipIntervalRuleContext,
    rule_order: int,
    rule_name: str,
) -> tuple[list[_SameTickerStockMergerTransition], list[dict[str, Any]]]:
    transitions = []
    evidence_issues = []
    for action in context.corporate_actions:
        if action.get("action_type") != "stock_merger":
            continue
        predecessor_symbol = action.get("acquiree_symbol")
        successor_symbol = action.get("acquirer_symbol")
        completed = complete_stock_merger_endpoints(action, context.lineage)
        if completed is not None:
            predecessor_symbol = successor_symbol = completed.symbol
        if not all(isinstance(symbol, str) and symbol for symbol in (predecessor_symbol, successor_symbol)):
            evidence_issues.append(
                {
                    "reason_code": "malformed_stock_merger_symbols",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action": action,
                }
            )
            continue
        if predecessor_symbol != successor_symbol:
            continue

        action_id = action.get("id")
        predecessor_cusip = action.get("acquiree_cusip")
        successor_cusip = action.get("acquirer_cusip")
        boundary_field, boundary_value = _merger_boundary_date(action)
        if not all(
            isinstance(value, str) and value
            for value in (action_id, predecessor_cusip, successor_cusip, boundary_value)
        ):
            evidence_issues.append(
                {
                    "reason_code": "malformed_same_ticker_stock_merger",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action": action,
                }
            )
            continue
        try:
            transition_date = date.fromisoformat(boundary_value)
        except ValueError:
            evidence_issues.append(
                {
                    "reason_code": "invalid_stock_merger_boundary_date",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": action_id,
                    "boundary_field": boundary_field,
                    "boundary_date": boundary_value,
                }
            )
            continue

        predecessor_asset_id = context.asset_ids_by_symbol_and_cusip.get((predecessor_symbol, predecessor_cusip))
        successor_asset_id = context.asset_ids_by_symbol_and_cusip.get((successor_symbol, successor_cusip))
        if completed is not None:
            predecessor_asset_id = completed.predecessor_asset_id
            successor_asset_id = completed.successor_asset_id
        if predecessor_asset_id is None or successor_asset_id is None:
            evidence_issues.append(
                {
                    "reason_code": "unresolved_same_ticker_stock_merger_identity",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": action_id,
                    "symbol": predecessor_symbol,
                    "predecessor_cusip": predecessor_cusip,
                    "predecessor_asset_id": predecessor_asset_id,
                    "successor_cusip": successor_cusip,
                    "successor_asset_id": successor_asset_id,
                }
            )
            continue
        if predecessor_asset_id == successor_asset_id:
            evidence_issues.append(
                {
                    "severity": "warning",
                    "reason_code": "same_ticker_stock_merger_resolves_to_one_uuid",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_id": action_id,
                    "symbol": predecessor_symbol,
                    "asset_id": predecessor_asset_id,
                    "predecessor_cusip": predecessor_cusip,
                    "successor_cusip": successor_cusip,
                }
            )
            continue

        transitions.append(
            _SameTickerStockMergerTransition(
                action_id=action_id,
                boundary_date=transition_date,
                symbol=predecessor_symbol,
                predecessor_cusip=predecessor_cusip,
                predecessor_asset_id=predecessor_asset_id,
                successor_cusip=successor_cusip,
                successor_asset_id=successor_asset_id,
                completed_symbol=completed is not None,
            )
        )
    transitions.sort(key=lambda transition: (transition.boundary_date, transition.action_id))
    return transitions, evidence_issues


def _apply_same_ticker_stock_merger_transition_rule(
    context: _OwnershipIntervalRuleContext,
    existing_intervals: tuple[TickerOwnershipInterval, ...],
    rule_order: int,
    rule_name: str,
) -> _OwnershipIntervalRuleResult:
    intervals = list(existing_intervals)
    transitions, evidence_issues = _parse_same_ticker_stock_merger_transitions(
        context,
        rule_order,
        rule_name,
    )
    transitions_by_identity_pair: dict[
        tuple[str, str, str, str, str],
        list[_SameTickerStockMergerTransition],
    ] = {}
    for transition in transitions:
        identity_pair = (
            transition.symbol,
            transition.predecessor_cusip,
            transition.predecessor_asset_id,
            transition.successor_cusip,
            transition.successor_asset_id,
        )
        transitions_by_identity_pair.setdefault(identity_pair, []).append(transition)

    for identity_pair, matching_transitions in transitions_by_identity_pair.items():
        symbol, predecessor_cusip, predecessor_asset_id, successor_cusip, successor_asset_id = identity_pair
        boundary_date = matching_transitions[0].boundary_date
        supporting_action_ids = tuple(transition.action_id for transition in matching_transitions)
        boundary_dates = sorted({transition.boundary_date for transition in matching_transitions})
        # Completing a missing label establishes the direct identity link, not
        # permission to contradict a name-change boundary already established
        # earlier in this registry. Retain that ownership chronology; the UUID
        # edge owner can still preserve the independently supported continuity.
        conflicting_prior_intervals = [
            interval
            for interval in existing_intervals
            if interval.historical_symbol == symbol
            and interval.contains(boundary_date)
            and (
                interval.asset_id not in {predecessor_asset_id, successor_asset_id}
                or (
                    interval.asset_id == successor_asset_id
                    and interval.start_date_inclusive is not None
                    and interval.start_date_inclusive < boundary_date
                )
            )
        ]
        if (
            all(transition.completed_symbol for transition in matching_transitions)
            and conflicting_prior_intervals
        ):
            evidence_issues.append(
                {
                    "reason_code": "same_ticker_stock_merger_conflicts_with_existing_ownership_intervals",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_ids": list(supporting_action_ids),
                    "symbol": symbol,
                    "detail": "Completed endpoint retains membership; earlier name-change dates retain ownership.",
                }
            )
            continue
        if len(boundary_dates) > 1:
            evidence_issues.append(
                {
                    "severity": "warning",
                    "reason_code": "same_ticker_stock_merger_boundary_disagreement",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_ids": list(supporting_action_ids),
                    "boundary_dates": [item.isoformat() for item in boundary_dates],
                    "selected_earliest_transition_boundary": boundary_date.isoformat(),
                    "symbol": symbol,
                    "predecessor_cusip": predecessor_cusip,
                    "successor_cusip": successor_cusip,
                }
            )

        applied_rule = AppliedOwnershipIntervalRule(
            order=rule_order,
            name=rule_name,
            supporting_action_ids=supporting_action_ids,
        )
        constrained_intervals = (
            TickerOwnershipInterval(
                historical_symbol=symbol,
                cusip=predecessor_cusip,
                asset_id=predecessor_asset_id,
                start_date_inclusive=None,
                end_date_exclusive=boundary_date,
                applied_rules=(applied_rule,),
            ),
            TickerOwnershipInterval(
                historical_symbol=symbol,
                cusip=successor_cusip,
                asset_id=successor_asset_id,
                start_date_inclusive=boundary_date,
                end_date_exclusive=None,
                applied_rules=(applied_rule,),
            ),
        )
        for role, constrained_interval in zip(
            ("predecessor", "successor"),
            constrained_intervals,
            strict=True,
        ):
            if _constrain_or_append_interval(intervals, constrained_interval):
                continue
            evidence_issues.append(
                {
                    "reason_code": "same_ticker_stock_merger_conflicts_with_existing_ownership_intervals",
                    "ownership_interval_rule": _rule_reference(rule_order, rule_name),
                    "action_ids": list(supporting_action_ids),
                    "role": role,
                    "symbol": constrained_interval.historical_symbol,
                    "cusip": constrained_interval.cusip,
                    "asset_id": constrained_interval.asset_id,
                }
            )

    return _OwnershipIntervalRuleResult(
        intervals=_merge_exact_intervals(intervals),
        evidence_issues=tuple(evidence_issues),
    )


def _apply_action_anchored_current_suffix_rule(
    context: _OwnershipIntervalRuleContext,
    existing_intervals: tuple[TickerOwnershipInterval, ...],
    rule_order: int,
    rule_name: str,
) -> _OwnershipIntervalRuleResult:
    """Bound a current reused ticker from positive provider evidence onward.

    The first current-UUID action after every competing UUID action is a lower
    ownership bound, not a ticker-change date. The unsupported prefix remains
    ambiguous, and no UUID continuity is inferred.
    """
    intervals = list(existing_intervals)
    symbol_lookups = context.lineage.get("asset_lookups")
    if not isinstance(symbol_lookups, list):
        return _OwnershipIntervalRuleResult(intervals=existing_intervals)
    symbols = sorted(
        {
            lookup.get("lookup")
            for lookup in symbol_lookups
            if isinstance(lookup, dict)
            and lookup.get("lookup_kind") == "symbol"
            and isinstance(lookup.get("lookup"), str)
            and lookup["lookup"]
        }
    )
    for symbol in symbols:
        if any(interval.historical_symbol == symbol for interval in existing_intervals):
            continue
        current = exact_symbol_lookup_asset(context.lineage, symbol)
        if (
            current is None
            or current.get("status") != "active"
            or current.get("tradable") is not True
            or current.get("exchange") in (None, "", "OTC")
        ):
            continue
        pairs = sorted(
            (
                cusip,
                asset_id,
            )
            for (candidate_symbol, cusip), asset_id in context.asset_ids_by_symbol_and_cusip.items()
            if candidate_symbol == symbol
        )
        if len({asset_id for _, asset_id in pairs}) < 2:
            continue
        current_pairs = [(cusip, asset_id) for cusip, asset_id in pairs if asset_id == current["id"]]
        if len(current_pairs) != 1:
            continue
        current_cusip, current_asset_id = current_pairs[0]
        current_cusip_asset = exact_cusip_lookup_asset(context.lineage, current_cusip)
        if (
            current_cusip_asset is None
            or current_cusip_asset.get("id") != current_asset_id
            or current_cusip_asset.get("symbol") != symbol
            or current_cusip_asset.get("exchange") in (None, "", "OTC")
        ):
            continue

        anchors = []
        for action in context.corporate_actions:
            for cusip, asset_id in pairs:
                try:
                    effective = positive_occupancy_action_date(action, symbol, cusip)
                except (KeyError, TypeError, ValueError, InvalidOperation):
                    continue
                if effective is not None:
                    anchors.append((effective, str(action["id"]), cusip, asset_id))
        competing_dates = [effective for effective, _, _, asset_id in anchors if asset_id != current_asset_id]
        last_competing = max(competing_dates, default=None)
        current_anchors = sorted(
            item
            for item in anchors
            if item[3] == current_asset_id and (last_competing is None or item[0] > last_competing)
        )
        if not current_anchors:
            continue
        lower_bound = current_anchors[0][0]
        supporting_action_ids = tuple(
            action_id
            for effective, action_id, cusip, asset_id in current_anchors
            if effective == lower_bound and cusip == current_cusip and asset_id == current_asset_id
        )
        constraint = TickerOwnershipInterval(
            historical_symbol=symbol,
            cusip=current_cusip,
            asset_id=current_asset_id,
            start_date_inclusive=lower_bound,
            end_date_exclusive=None,
            applied_rules=(
                AppliedOwnershipIntervalRule(
                    order=rule_order,
                    name=rule_name,
                    supporting_action_ids=supporting_action_ids,
                ),
            ),
        )
        _constrain_or_append_interval(intervals, constraint)
    return _OwnershipIntervalRuleResult(intervals=_merge_exact_intervals(intervals))


OWNERSHIP_INTERVAL_RULES = (
    OwnershipIntervalRule(
        name="name_change_ticker_transition",
        apply=_apply_name_change_rule,
    ),
    OwnershipIntervalRule(
        name="same_ticker_stock_merger_uuid_transition",
        apply=_apply_same_ticker_stock_merger_transition_rule,
    ),
    OwnershipIntervalRule(
        name="cash_merger_acquiree_terminal_boundary",
        apply=_apply_cash_merger_terminal_rule,
    ),
    OwnershipIntervalRule(
        name="action_anchored_current_ticker_occupancy",
        apply=_apply_action_anchored_current_suffix_rule,
    ),
)


def ownership_interval_rule_order() -> list[dict[str, Any]]:
    return [
        {"order": rule_order, "name": rule.name}
        for rule_order, rule in enumerate(OWNERSHIP_INTERVAL_RULES, start=1)
    ]


def _asset_ids_by_symbol_and_cusip(
    lineage: dict[str, Any],
) -> tuple[dict[tuple[str, str], str], list[dict[str, Any]]]:
    # Ownership needs one unambiguous UUID and explicit issues for conflicting candidates.
    asset_ids: dict[tuple[str, str], str] = {}
    ambiguous_keys = set()
    evidence_issues = []
    occupancy_evidence = lineage.get("symbol_occupancy_evidence")
    if not isinstance(occupancy_evidence, list):
        return {}, [{"reason_code": "malformed_symbol_occupancy_evidence", "detail": "expected a list"}]

    for occupancy in occupancy_evidence:
        if not isinstance(occupancy, dict):
            evidence_issues.append(
                {
                    "reason_code": "malformed_symbol_occupancy_item",
                    "item": repr(occupancy),
                }
            )
            continue
        symbol = occupancy.get("symbol")
        cusip = occupancy.get("cusip")
        asset_id = occupancy.get("asset_id")
        if not all(isinstance(value, str) and value for value in (symbol, cusip, asset_id)):
            continue
        key = (symbol, cusip)
        existing_asset_id = asset_ids.get(key)
        if existing_asset_id is not None and existing_asset_id != asset_id:
            ambiguous_keys.add(key)
            continue
        asset_ids[key] = asset_id

    for key in sorted(ambiguous_keys):
        asset_ids.pop(key, None)
        evidence_issues.append(
            {
                "reason_code": "conflicting_asset_ids_for_symbol_cusip",
                "symbol": key[0],
                "cusip": key[1],
            }
        )
    return asset_ids, evidence_issues


def build_ticker_ownership_intervals(
    lineage: dict[str, Any],
    *,
    uuid_lineage_edges=None,
    include_action_anchored_current_suffix: bool = False,
) -> tuple[list[TickerOwnershipInterval], list[dict[str, Any]]]:
    corporate_actions = lineage.get("corporate_actions")
    if not isinstance(corporate_actions, list):
        return [], [{"reason_code": "malformed_corporate_actions", "detail": "expected a list"}]
    normalized_actions = tuple(action for action in corporate_actions if isinstance(action, dict))
    malformed_action_issues = [
        {"reason_code": "malformed_corporate_action_item", "item": repr(action)}
        for action in corporate_actions
        if not isinstance(action, dict)
    ]
    asset_ids, evidence_issues = _asset_ids_by_symbol_and_cusip(lineage)
    evidence_issues = [*malformed_action_issues, *evidence_issues]
    context = _OwnershipIntervalRuleContext(
        corporate_actions=normalized_actions,
        asset_ids_by_symbol_and_cusip=asset_ids,
        lineage=lineage,
        uuid_lineage_edges=tuple(
            build_uuid_lineage_edges(lineage)[0] if uuid_lineage_edges is None else uuid_lineage_edges
        ),
    )
    intervals: tuple[TickerOwnershipInterval, ...] = ()

    for rule_order, rule in enumerate(OWNERSHIP_INTERVAL_RULES, start=1):
        if (
            rule.name == "action_anchored_current_ticker_occupancy"
            and not include_action_anchored_current_suffix
        ):
            continue
        rule_result = rule.apply(context, intervals, rule_order, rule.name)
        intervals = rule_result.intervals
        evidence_issues.extend(rule_result.evidence_issues)

    return list(intervals), evidence_issues

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from libs.alpaca_security_lineage import EXCHANGE_TEST_SYMBOL, HISTORICAL_MARKET_DATA_ONLY

from .errors import SecurityCorrelationError
from .evidence import (
    TickerOwnershipInterval,
    identity_candidates_for_symbol,
)
from .security_identity import security_identity_for_alpaca_assets
from .settings import CORRELATED_BRANCH, UNCORRELATED_BRANCH
from .terminal_fallback import (
    TERMINAL_FALLBACK_RULE_NAME,
    apply_terminal_continuous_ticker_fallback,
)
from .uuid_lineage import build_uuid_lineage_edges, predecessor_uuid_branch

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")


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
            )
        return (
            self.branch,
            self.rule_order,
            self.rule_name,
            self.warning_codes,
            self.reason_codes,
            self.candidates,
        )


@dataclass(frozen=True)
class _RowRuleContext:
    session_date: date
    requested_symbol: str
    selected_asof_interval: TickerOwnershipInterval | None
    selected_asof_identity_chain_asset_ids: frozenset[str]
    intervals: list[TickerOwnershipInterval]
    lineage: dict[str, Any]


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


def _apply_asof_selected_identity_rule(context: _RowRuleContext) -> _RowDecision | None:
    if context.selected_asof_interval is not None:
        selected_matches = _interval_matches(
            context.intervals,
            context.session_date,
        )
        selected_matches = [
            interval
            for interval in selected_matches
            if interval.asset_id in context.selected_asof_identity_chain_asset_ids
        ]
        selected_interval = _unique_interval_or_none(selected_matches)
        if selected_interval is not None:
            requested_symbol_matches = _interval_matches(
                context.intervals,
                context.session_date,
                historical_symbol=context.requested_symbol,
            )
            competing_requested_symbol_occupant = any(
                interval.asset_id not in context.selected_asof_identity_chain_asset_ids
                for interval in requested_symbol_matches
            )
            warning_codes = []
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
            return _RowDecision(
                branch=UNCORRELATED_BRANCH,
                reason_codes=("conflicting_asof_identity_intervals",),
            )
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


def _apply_requested_symbol_interval_rule(context: _RowRuleContext) -> _RowDecision | None:
    requested_symbol_matches = _interval_matches(
        context.intervals,
        context.session_date,
        historical_symbol=context.requested_symbol,
    )
    requested_interval = _unique_interval_or_none(requested_symbol_matches)
    if requested_interval is not None:
        warning_codes = (
            ("inferred_lower_ownership_boundary",) if requested_interval.start_date_inclusive is None else ()
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
        name="asof_selected_uuid_lineage_interval",
        apply=_apply_asof_selected_identity_rule,
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
    asset_id: str,
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
) -> dict[str, list[dict[str, Any]]]:
    requested_symbol = source["requested_symbol"]
    selected_interval = _selected_asof_interval(requested_symbol, source["asof"], intervals)
    uuid_lineage_edges, _ = build_uuid_lineage_edges(lineage)
    selected_identity_chain_asset_ids = (
        predecessor_uuid_branch(selected_interval.asset_id, uuid_lineage_edges)
        if selected_interval is not None
        else frozenset()
    )
    correlated_rows = []
    previous_session_date = None

    for row_number, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise SecurityCorrelationError(f"row {row_number}: expected an object")
        timestamp, session_date = _parse_session_date(row.get("t"), row_number)
        if previous_session_date is not None and session_date <= previous_session_date:
            raise SecurityCorrelationError(f"row {row_number}: session dates are not strictly increasing")
        previous_session_date = session_date
        decision = _decide_row(
            _RowRuleContext(
                session_date=session_date,
                requested_symbol=requested_symbol,
                selected_asof_interval=selected_interval,
                selected_asof_identity_chain_asset_ids=selected_identity_chain_asset_ids,
                intervals=intervals,
                lineage=lineage,
            )
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
        segment_end = segment_start + 1
        while (
            segment_end < len(correlated_rows)
            and correlated_rows[segment_end].decision.grouping_key() == decision.grouping_key()
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
            security_identity = security_identity_for_alpaca_assets(
                (decision.asset_id,),
                uuid_lineage_edges,
                tickers=(
                    decision.interval.historical_symbol if decision.interval is not None else requested_symbol,
                ),
                basis=identity_basis,
            )
            segment.update(
                {
                    "historical_symbol": (
                        decision.interval.historical_symbol
                        if decision.interval is not None
                        else requested_symbol
                    ),
                    "canonical_security": _canonical_security(
                        security_identity.security_id,
                        decision.asset_id,
                        decision.cusip,
                    ),
                    "security_identity": security_identity.as_report_object(),
                    "identity_confidence": decision.identity_confidence,
                    "correlation_method": decision.rule_name,
                    "ticker_ownership_interval": (
                        {
                            **decision.interval.as_report_object(),
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
    )

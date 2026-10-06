import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
)

from .errors import WarningPipelineError
from .findings import INFORMATION, WARNING, FindingCollector
from .models import AnalyzableDailyBar, WarningPipelineInputs
from .settings import WarningPipelineConfiguration

SECURITY_SEGMENT_STATISTICAL_RULES = (
    (
        1,
        "alpaca_uuid_partition",
        "Build independent histories using only correlated Alpaca asset UUID segments.",
    ),
    (
        2,
        "uuid_segment_session_coverage",
        "Check missing sessions only inside one correlated UUID ownership segment.",
    ),
    (3, "primary_activity_view", "Prefer raw activity data and choose one fully covering adjusted price view."),
    (4, "corroborated_activity_collapse", "Compare activity with a robust trailing history inside one UUID."),
    (5, "one_trade_nonflat", "Flag a one-trade bar with a non-flat price range as contextual evidence."),
    (6, "repeated_liquid_bar", "Find repeated complete high-activity bar signatures."),
    (7, "isolated_price_level_reversal", "Find a full price-level jump that immediately reverses."),
    (8, "extreme_unsupported_wick", "Find an isolated extreme unsupported by body, VWAP, or neighbors."),
    (9, "raw_discontinuity_explanation", "Record large raw discontinuities removed by an adjusted view."),
)


def security_segment_statistical_rule_order() -> list[dict[str, Any]]:
    return [
        {"order": order, "name": name, "purpose": purpose}
        for order, name, purpose in SECURITY_SEGMENT_STATISTICAL_RULES
    ]


@dataclass(frozen=True)
class _SecurityCoverageSegment:
    data_file: str
    row_start: int
    row_end: int
    historical_symbol: str | None
    ownership_start_date_inclusive: str | None
    ownership_end_date_exclusive: str | None
    observed_session_dates: tuple[str, ...]


@dataclass(frozen=True)
class _SecurityAdjustmentHistory:
    asset_id: str
    cusips: tuple[str, ...]
    request_cohort: tuple[str, ...]
    adjustment: str
    adjustment_actions: frozenset[str]
    data_files: tuple[str, ...]
    bars: tuple[AnalyzableDailyBar, ...]
    source_positions: dict[datetime, tuple[str, int]]
    coverage_segments: tuple[_SecurityCoverageSegment, ...]

    @property
    def bars_by_timestamp(self) -> dict[datetime, AnalyzableDailyBar]:
        return {bar.timestamp: bar for bar in self.bars}

    @property
    def canonical_security(self) -> dict[str, Any]:
        return {"asset_id": self.asset_id, "cusips": list(self.cusips)}


def _iso_timestamp(timestamp: datetime) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")


def _build_security_histories(
    inputs: WarningPipelineInputs,
) -> tuple[list[_SecurityAdjustmentHistory], dict[str, Any]]:
    views_by_filename = {view.data_file: view for view in inputs.download_views}
    histories: dict[
        tuple[str, tuple[str, ...], str],
        dict[str, Any],
    ] = {}
    correlated_segments = inputs.correlation.correlated["segments"]
    correlated_analyzable_rows = 0
    excluded_integrity_rows = 0
    cusips_by_asset_id: dict[str, set[str]] = defaultdict(set)

    for segment in correlated_segments:
        filename = segment["data_file"]
        view = views_by_filename.get(filename)
        if view is None:
            raise WarningPipelineError(f"correlated segment refers to unloaded source {filename!r}")
        canonical_security = segment["canonical_security"]
        asset_id = canonical_security["asset_id"]
        cusip = canonical_security.get("cusip")
        if isinstance(cusip, str) and cusip:
            cusips_by_asset_id[asset_id].add(cusip)
        key = (asset_id, view.request_cohort_key(), view.adjustment)
        history = histories.setdefault(
            key,
            {
                "view": view,
                "files": set(),
                "bars": [],
                "positions": {},
                "coverage_segments": [],
            },
        )
        history["files"].add(filename)
        ownership_interval = segment.get("ticker_ownership_interval")
        if ownership_interval is not None and not isinstance(ownership_interval, dict):
            raise WarningPipelineError(f"correlated segment has invalid ownership interval in {filename!r}")
        history["coverage_segments"].append(
            _SecurityCoverageSegment(
                data_file=filename,
                row_start=segment["row_start"],
                row_end=segment["row_end"],
                historical_symbol=segment.get("historical_symbol"),
                ownership_start_date_inclusive=(
                    ownership_interval.get("start_date_inclusive") if ownership_interval else None
                ),
                ownership_end_date_exclusive=(
                    ownership_interval.get("end_date_exclusive") if ownership_interval else None
                ),
                observed_session_dates=tuple(
                    timestamp.date().isoformat()
                    for row_number, timestamp in view.timestamp_by_row_number.items()
                    if segment["row_start"] <= row_number <= segment["row_end"]
                ),
            )
        )
        for row_number in range(segment["row_start"], segment["row_end"] + 1):
            bar = view.bars_by_row_number.get(row_number)
            if bar is None:
                excluded_integrity_rows += 1
                continue
            existing_position = history["positions"].get(bar.timestamp)
            if existing_position is not None:
                raise WarningPipelineError(
                    f"Alpaca asset UUID {asset_id} has duplicate {view.adjustment} timestamp "
                    f"{_iso_timestamp(bar.timestamp)} in {existing_position[0]} and {filename}"
                )
            history["bars"].append(bar)
            history["positions"][bar.timestamp] = (filename, row_number)
            correlated_analyzable_rows += 1

    result = []
    for (asset_id, request_cohort, adjustment), history in sorted(histories.items()):
        view = history["view"]
        result.append(
            _SecurityAdjustmentHistory(
                asset_id=asset_id,
                cusips=tuple(sorted(cusips_by_asset_id[asset_id])),
                request_cohort=request_cohort,
                adjustment=adjustment,
                adjustment_actions=view.adjustment_actions,
                data_files=tuple(sorted(history["files"])),
                bars=tuple(sorted(history["bars"], key=lambda bar: bar.timestamp)),
                source_positions=history["positions"],
                coverage_segments=tuple(
                    sorted(
                        history["coverage_segments"],
                        key=lambda segment: (segment.data_file, segment.row_start),
                    )
                ),
            )
        )

    correlated_row_count = inputs.correlation.correlated["row_count"]
    uncorrelated_row_count = inputs.correlation.uncorrelated["row_count"]
    if correlated_analyzable_rows + excluded_integrity_rows != correlated_row_count:
        raise WarningPipelineError("analyzable and integrity-excluded UUID rows do not exhaust correlation")
    return result, {
        "correlated_source_row_count": correlated_row_count,
        "uncorrelated_source_row_count": uncorrelated_row_count,
        "correlated_analyzable_row_count": correlated_analyzable_rows,
        "correlated_integrity_excluded_row_count": excluded_integrity_rows,
        "canonical_security_count": len(cusips_by_asset_id),
        "security_adjustment_history_count": len(result),
    }


def _robust_log_score(
    value: float,
    history: list[float],
    floor: float,
) -> tuple[float, float, float]:
    logarithms = [math.log(item) for item in history if item > 0]
    center = statistics.median(logarithms)
    median_absolute_deviation = statistics.median(abs(item - center) for item in logarithms)
    scale = max(1.4826 * median_absolute_deviation, floor)
    return (math.log(value) - center) / scale, center, scale


def _choose_primary_histories(
    histories: list[_SecurityAdjustmentHistory],
) -> tuple[_SecurityAdjustmentHistory, _SecurityAdjustmentHistory]:
    usable = [history for history in histories if history.bars]
    activity = min(
        usable,
        key=lambda history: (
            history.adjustment_actions != frozenset(),
            -len(history.bars),
            history.data_files,
        ),
    )
    activity_timestamps = set(activity.bars_by_timestamp)
    price_candidates = [history for history in usable if activity_timestamps.issubset(history.bars_by_timestamp)]
    prices = max(
        price_candidates,
        key=lambda history: (len(history.adjustment_actions), history.adjustment, history.data_files),
    )
    return activity, prices


def _validated_optional_date(value: str | None, description: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise WarningPipelineError(f"{description} is not an ISO date: {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise WarningPipelineError(f"{description} is not an ISO date: {value!r}") from error


def _evaluate_uuid_segment_session_coverage(
    primary: _SecurityAdjustmentHistory,
    collector: FindingCollector,
) -> None:
    request_start = date.fromisoformat(primary.request_cohort[4][:10])
    request_end = date.fromisoformat(primary.request_cohort[5][:10])
    for segment in primary.coverage_segments:
        observed_session_dates = segment.observed_session_dates
        if not observed_session_dates:
            continue
        collector.evaluated("uuid_segment_session_coverage")
        if observed_session_dates != tuple(sorted(set(observed_session_dates))):
            continue

        first_observed = date.fromisoformat(observed_session_dates[0])
        last_observed = date.fromisoformat(observed_session_dates[-1])
        ownership_start = _validated_optional_date(
            segment.ownership_start_date_inclusive,
            "ownership interval start",
        )
        ownership_end = _validated_optional_date(
            segment.ownership_end_date_exclusive,
            "ownership interval end",
        )
        coverage_start = max(request_start, ownership_start) if ownership_start else first_observed
        coverage_end = min(request_end, ownership_end) if ownership_end else last_observed + timedelta(days=1)
        if coverage_start >= coverage_end:
            raise WarningPipelineError(
                f"correlated UUID segment {segment.data_file}:{segment.row_start}-{segment.row_end} "
                "has no valid ownership coverage interval"
            )
        try:
            _, missing_session_dates, _ = compare_observed_to_expected_sessions(
                observed_session_dates,
                coverage_start.isoformat(),
                coverage_end.isoformat(),
            )
        except MarketCalendarError as error:
            raise WarningPipelineError(
                f"could not evaluate UUID session coverage for {segment.data_file}: {error}"
            ) from error
        if not missing_session_dates:
            continue
        collector.add(
            "uuid_segment_session_coverage",
            WARNING,
            "Expected market sessions are absent inside one correlated Alpaca UUID ownership segment.",
            data_files=(segment.data_file,),
            evidence={
                "row_start": segment.row_start,
                "row_end": segment.row_end,
                "historical_symbol": segment.historical_symbol,
                "observed_first_session": observed_session_dates[0],
                "observed_last_session": observed_session_dates[-1],
                "evaluated_start_date_inclusive": coverage_start.isoformat(),
                "evaluated_end_date_exclusive": coverage_end.isoformat(),
                "ownership_start_date_inclusive": segment.ownership_start_date_inclusive,
                "ownership_end_date_exclusive": segment.ownership_end_date_exclusive,
                "missing_session_count": len(missing_session_dates),
                "missing_sessions": list(missing_session_dates),
            },
            confidence="observed",
            basis=(
                "calendar comparison is scoped to one UUID segment; intervals between different UUIDs "
                "are intentionally excluded"
            ),
            canonical_security=primary.canonical_security,
        )


def _same_count_in_files(
    histories: list[_SecurityAdjustmentHistory],
    timestamp: datetime,
    trade_count: Decimal,
) -> list[str]:
    files = []
    for history in histories:
        matching_bar = history.bars_by_timestamp.get(timestamp)
        if matching_bar is not None and matching_bar.values["trade_count"] == trade_count:
            files.extend(history.data_files)
    return sorted(set(files))


def _source_rows_are_adjacent(
    history: _SecurityAdjustmentHistory,
    first: datetime,
    second: datetime,
) -> bool:
    first_file, first_row = history.source_positions[first]
    second_file, second_row = history.source_positions[second]
    return first_file == second_file and second_row == first_row + 1


def _evaluate_activity(
    histories: list[_SecurityAdjustmentHistory],
    primary: _SecurityAdjustmentHistory,
    collector: FindingCollector,
) -> set[datetime]:
    configuration = collector.configuration
    bars = list(primary.bars)
    flagged_activity: set[datetime] = set()
    for index, bar in enumerate(bars):
        trade_count = bar.numeric_value("trade_count")
        volume = bar.numeric_value("volume")
        history = [
            prior
            for prior in bars[max(0, index - configuration.activity_lookback_sessions) : index]
            if prior.numeric_value("trade_count") > 0 and prior.numeric_value("volume") > 0
        ]
        if len(history) < configuration.activity_minimum_history_sessions:
            continue
        historical_trade_counts = [item.numeric_value("trade_count") for item in history]
        historical_volumes = [item.numeric_value("volume") for item in history]
        historical_mean_sizes = [
            item.numeric_value("volume") / item.numeric_value("trade_count") for item in history
        ]
        median_trade_count = statistics.median(historical_trade_counts)
        median_volume = statistics.median(historical_volumes)
        median_mean_size = statistics.median(historical_mean_sizes)
        robust_score, _, robust_scale = _robust_log_score(
            trade_count,
            historical_trade_counts,
            configuration.activity_log_mad_floor,
        )
        trade_count_ratio = trade_count / median_trade_count
        volume_ratio = volume / median_volume
        mean_size_ratio = (volume / trade_count) / median_mean_size
        collector.evaluated("corroborated_activity_collapse")
        if (
            median_trade_count >= configuration.activity_minimum_baseline_trades
            and trade_count_ratio <= configuration.activity_trade_count_collapse_ratio
            and robust_score <= -configuration.activity_robust_z_threshold
            and (
                volume_ratio <= configuration.activity_volume_collapse_ratio
                or mean_size_ratio >= configuration.activity_mean_trade_size_multiple
            )
        ):
            flagged_activity.add(bar.timestamp)
            same_count_files = _same_count_in_files(
                histories,
                bar.timestamp,
                bar.values["trade_count"],
            )
            evidence = {
                **bar.exact_evidence(),
                "primary_file": primary.data_files[0],
                "prior_sessions_used": len(history),
                "prior_median_trade_count": median_trade_count,
                "trade_count_fraction_of_median": trade_count_ratio,
                "prior_median_volume": median_volume,
                "volume_fraction_of_median": volume_ratio,
                "volume_per_counted_trade": volume / trade_count,
                "prior_median_volume_per_counted_trade": median_mean_size,
                "mean_trade_size_multiple": mean_size_ratio,
                "log_trade_count_robust_z": robust_score,
                "robust_log_scale": robust_scale,
                "same_count_in_files": same_count_files,
                "thresholds": {
                    "trade_count_fraction_at_most": configuration.activity_trade_count_collapse_ratio,
                    "robust_z_at_most": -configuration.activity_robust_z_threshold,
                    "volume_fraction_at_most_or_size_multiple_at_least": [
                        configuration.activity_volume_collapse_ratio,
                        configuration.activity_mean_trade_size_multiple,
                    ],
                },
            }
            if trade_count == 1 and bar.values["high"] != bar.values["low"]:
                evidence["single_trade_caveat"] = (
                    "A corrected consolidated close can change prices without increasing Alpaca trade_count."
                )
            collector.add(
                "corroborated_activity_collapse",
                WARNING,
                f"{bar.session_date}: trade count collapses relative to this UUID history.",
                data_files=same_count_files,
                evidence=evidence,
                confidence="high",
                basis="within-UUID anomaly corroborated by volume or mean trade size",
                canonical_security=primary.canonical_security,
            )
    return flagged_activity


def _evaluate_one_trade_and_repeated_bars(
    primary: _SecurityAdjustmentHistory,
    flagged_activity: set[datetime],
    collector: FindingCollector,
) -> None:
    configuration = collector.configuration
    signatures: dict[tuple[Decimal, ...], AnalyzableDailyBar] = {}
    for bar in primary.bars:
        if bar.timestamp not in flagged_activity:
            collector.evaluated("one_trade_nonflat")
            if bar.values["trade_count"] == 1 and bar.values["high"] != bar.values["low"]:
                collector.add(
                    "one_trade_nonflat",
                    WARNING,
                    "One counted trade accompanies a non-flat daily price range.",
                    data_files=primary.data_files,
                    evidence=bar.exact_evidence(),
                    confidence="contextual",
                    basis="review clue; Alpaca price-only corrections can change OHLC without changing count",
                    canonical_security=primary.canonical_security,
                )
        collector.evaluated("repeated_liquid_bar")
        signature = bar.full_signature()
        earlier = signatures.get(signature)
        if (
            earlier is not None
            and bar.numeric_value("trade_count") >= configuration.repeated_bar_minimum_trades
            and bar.numeric_value("volume") >= configuration.repeated_bar_minimum_volume
        ):
            collector.add(
                "repeated_liquid_bar",
                WARNING,
                "A high-activity complete OHLCV/trade-count/VWAP signature repeats on another session.",
                data_files=primary.data_files,
                evidence={"earlier": earlier.exact_evidence(), "later": bar.exact_evidence()},
                confidence="high",
                basis="possible copy/replay; distinct sessions can still legitimately match",
                canonical_security=primary.canonical_security,
            )
        signatures[signature] = bar


def _evaluate_prices(
    raw: _SecurityAdjustmentHistory,
    adjusted: _SecurityAdjustmentHistory,
    collector: FindingCollector,
) -> None:
    configuration = collector.configuration
    bars = list(adjusted.bars)
    for index in range(1, len(bars) - 1):
        previous, current, following = bars[index - 1 : index + 2]
        if not (
            _source_rows_are_adjacent(adjusted, previous.timestamp, current.timestamp)
            and _source_rows_are_adjacent(adjusted, current.timestamp, following.timestamp)
        ):
            continue
        collector.evaluated("isolated_price_level_reversal")
        logarithmic_jump = math.log(current.numeric_value("close") / previous.numeric_value("close"))
        neighbor_distance = abs(math.log(following.numeric_value("close") / previous.numeric_value("close")))
        full_bar_disjoint = current.numeric_value("low") > max(
            previous.numeric_value("high"), following.numeric_value("high")
        ) or current.numeric_value("high") < min(previous.numeric_value("low"), following.numeric_value("low"))
        if (
            abs(logarithmic_jump) >= configuration.isolated_price_log_jump
            and neighbor_distance <= configuration.neighboring_price_log_distance
            and full_bar_disjoint
        ):
            collector.add(
                "isolated_price_level_reversal",
                WARNING,
                "An entire price bar jumps far from both adjacent UUID rows and immediately reverses.",
                data_files=adjusted.data_files,
                evidence={
                    "previous": previous.exact_evidence(),
                    "suspect": current.exact_evidence(),
                    "next": following.exact_evidence(),
                    "log_jump": logarithmic_jump,
                    "neighbor_log_distance": neighbor_distance,
                },
                confidence="high",
                basis="possible scale, decimal, or stale-row error; an extreme market event remains possible",
                canonical_security=adjusted.canonical_security,
            )
        collector.evaluated("extreme_unsupported_wick")
        body_high = max(current.numeric_value("open"), current.numeric_value("close"))
        body_low = min(current.numeric_value("open"), current.numeric_value("close"))
        if current.numeric_value("high") > configuration.unsupported_wick_multiple * max(
            body_high,
            current.numeric_value("vwap"),
            previous.numeric_value("high"),
            following.numeric_value("high"),
        ) or current.numeric_value("low") * configuration.unsupported_wick_multiple < min(
            body_low,
            current.numeric_value("vwap"),
            previous.numeric_value("low"),
            following.numeric_value("low"),
        ):
            collector.add(
                "extreme_unsupported_wick",
                WARNING,
                "An extreme high/low is unsupported by the bar body, VWAP, and adjacent UUID rows.",
                data_files=adjusted.data_files,
                evidence=current.exact_evidence(),
                confidence="high",
                basis="isolated price-extreme anomaly; no fixed daily-return limit is assumed",
                canonical_security=adjusted.canonical_security,
            )

    if raw is adjusted:
        return
    raw_bars = raw.bars_by_timestamp
    adjusted_bars = adjusted.bars_by_timestamp
    shared = sorted(set(raw_bars) & set(adjusted_bars))
    for previous_timestamp, current_timestamp in zip(shared, shared[1:]):
        if not _source_rows_are_adjacent(raw, previous_timestamp, current_timestamp):
            continue
        raw_previous, raw_current = raw_bars[previous_timestamp], raw_bars[current_timestamp]
        adjusted_previous = adjusted_bars[previous_timestamp]
        adjusted_current = adjusted_bars[current_timestamp]
        raw_log_return = math.log(raw_current.numeric_value("close") / raw_previous.numeric_value("close"))
        adjusted_log_return = math.log(
            adjusted_current.numeric_value("close") / adjusted_previous.numeric_value("close")
        )
        collector.evaluated("raw_discontinuity_explanation")
        if (
            abs(raw_log_return) > 0.35
            and abs(adjusted_log_return) < 0.12
            and abs(raw_log_return - adjusted_log_return) > 0.25
        ):
            collector.add(
                "raw_discontinuity_explanation",
                INFORMATION,
                "A large raw close discontinuity is largely removed by an adjusted view.",
                data_files=(*raw.data_files, *adjusted.data_files),
                evidence={
                    "session": raw_current.session_date.isoformat(),
                    "raw_simple_return": math.expm1(raw_log_return),
                    "adjusted_simple_return": math.expm1(adjusted_log_return),
                    "interpretation": (
                        "Evidence of an adjustment boundary, not independent confirmation of a corporate action."
                    ),
                },
                confidence="observed",
                basis="cross-view explanation",
                canonical_security=raw.canonical_security,
            )


def validate_security_segment_statistics(
    inputs: WarningPipelineInputs,
    configuration: WarningPipelineConfiguration,
) -> dict[str, Any]:
    collector = FindingCollector(configuration)
    histories, coverage = _build_security_histories(inputs)
    grouped: dict[tuple[str, tuple[str, ...]], list[_SecurityAdjustmentHistory]] = defaultdict(list)
    for history in histories:
        grouped[(history.asset_id, history.request_cohort)].append(history)

    evaluated_histories = []
    for (asset_id, request_cohort), security_histories in sorted(grouped.items()):
        collector.evaluated("alpaca_uuid_partition")
        usable = [history for history in security_histories if history.bars]
        if not usable:
            continue
        collector.evaluated("primary_activity_view")
        primary, price_view = _choose_primary_histories(usable)
        _evaluate_uuid_segment_session_coverage(primary, collector)
        flagged_activity = _evaluate_activity(usable, primary, collector)
        _evaluate_one_trade_and_repeated_bars(primary, flagged_activity, collector)
        _evaluate_prices(primary, price_view, collector)
        evaluated_histories.append(
            {
                "canonical_security": primary.canonical_security,
                "request_identity": list(request_cohort),
                "adjustment_views": sorted(history.adjustment for history in usable),
                "primary_activity_adjustment": primary.adjustment,
                "primary_price_adjustment": price_view.adjustment,
                "analyzed_activity_rows": len(primary.bars),
            }
        )

    return {
        "rule_order": security_segment_statistical_rule_order(),
        "integrity_classification": (
            "Warnings and observations only; these contextual rules never convert an authenticated row "
            "into an integrity error."
        ),
        "coverage": coverage,
        "evaluated_security_histories": evaluated_histories,
        **collector.as_report_object(),
    }

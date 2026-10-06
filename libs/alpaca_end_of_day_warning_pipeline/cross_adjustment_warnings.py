import math
from collections import defaultdict
from datetime import datetime
from typing import Any

from libs.common.market_data_rule_evaluators import (
    NOT_EVALUATED,
    RuleEvaluation,
    evaluate_common_multiplicative_price_factor,
)
from libs.common.market_data_rule_evaluators import (
    WARNING as RULE_WARNING,
)

from .findings import INFORMATION, WARNING, FindingCollector
from .models import AnalyzableDailyBar, DownloadValidationView
from .settings import WarningPipelineConfiguration, adjustment_label

CROSS_ADJUSTMENT_RULES = (
    (1, "comparable_request_cohorts", "Partition views by exact request identity before comparison."),
    (2, "declared_reference_consistency", "Compare producer pipeline and session/calendar declarations."),
    (3, "unique_adjustment_views", "Select at most one file for each adjustment action set."),
    (4, "cross_view_session_grid", "Compare timestamp grids for views with identical request bounds."),
    (5, "adjusted_trade_count", "Require trade counts to remain invariant across adjustment views."),
    (6, "within_row_price_factor", "Require one feasible multiplicative factor across OHLC and VWAP."),
    (7, "price_only_volume_invariance", "Require dividend/spin-off changes not to alter volume."),
    (8, "split_price_volume_reciprocity", "Compare split price factors with inverse volume factors."),
    (9, "adjustment_composition", "Compare combined adjustment factors with disjoint constituent products."),
    (10, "factor_regime_observation", "Record factor boundaries and warn on isolated one-row reversals."),
)


def cross_adjustment_rule_order() -> list[dict[str, Any]]:
    return [
        {"order": order, "name": name, "purpose": purpose} for order, name, purpose in CROSS_ADJUSTMENT_RULES
    ]


def _factor_interval(
    raw: AnalyzableDailyBar,
    adjusted: AnalyzableDailyBar,
    configuration: WarningPipelineConfiguration,
) -> RuleEvaluation:
    price_fields = ("open", "high", "low", "close", "vwap")
    return evaluate_common_multiplicative_price_factor(
        {field_name: raw.numeric_value(field_name) for field_name in price_fields},
        {field_name: adjusted.numeric_value(field_name) for field_name in price_fields},
        raw_absolute_error=configuration.raw_price_error,
        adjusted_absolute_error=configuration.adjusted_price_error,
        violation_outcome=RULE_WARNING,
    )


def _intervals_intersect(first: tuple[float, float], second: tuple[float, float]) -> bool:
    scale = max(abs(first[0]), abs(first[1]), abs(second[0]), abs(second[1]), 1.0)
    return max(first[0], second[0]) <= min(first[1], second[1]) + 1e-12 * scale


def _interval_middle(interval: tuple[float, float]) -> float:
    return (interval[0] + interval[1]) / 2


def _iso_timestamp(timestamp: datetime) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")


def _infer_factor_regimes(
    raw: DownloadValidationView,
    adjusted: DownloadValidationView,
    series: dict[datetime, tuple[float, float]],
    collector: FindingCollector,
) -> dict[str, Any]:
    configuration = collector.configuration
    timestamps = sorted(series)
    source_positions = {
        timestamp: position for position, timestamp in enumerate(raw.timestamp_by_row_number.values())
    }
    boundaries = []
    for previous, current in zip(timestamps, timestamps[1:]):
        if source_positions.get(current, -2) != source_positions.get(previous, -1) + 1:
            continue
        previous_interval, current_interval = series[previous], series[current]
        if min(_interval_middle(previous_interval), _interval_middle(current_interval)) <= 0:
            continue
        collector.evaluated("factor_regime_observation")
        relative_change = _interval_middle(current_interval) / _interval_middle(previous_interval) - 1
        if (
            not _intervals_intersect(previous_interval, current_interval)
            and abs(relative_change) >= configuration.factor_step_min_relative
        ):
            boundaries.append(
                {
                    "first_session_new_factor": raw.bars_by_timestamp[current].session_date.isoformat(),
                    "previous_timestamp": _iso_timestamp(previous),
                    "timestamp": _iso_timestamp(current),
                    "previous_factor_interval": previous_interval,
                    "new_factor_interval": current_interval,
                    "factor_relative_change": relative_change,
                }
            )

    for index in range(1, len(timestamps) - 1):
        previous, current, following = timestamps[index - 1 : index + 2]
        if not (
            source_positions.get(current, -2) == source_positions.get(previous, -1) + 1
            and source_positions.get(following, -2) == source_positions.get(current, -1) + 1
        ):
            continue
        before, during, after = series[previous], series[current], series[following]
        collector.evaluated("isolated_factor_reversal")
        if (
            _intervals_intersect(before, after)
            and not _intervals_intersect(before, during)
            and not _intervals_intersect(during, after)
            and _interval_middle(before) > 0
            and abs(_interval_middle(during) / _interval_middle(before) - 1)
            >= configuration.factor_spike_min_relative
        ):
            collector.add(
                "isolated_factor_reversal",
                WARNING,
                "An adjustment factor changes for one row and immediately reverses.",
                data_files=(raw.data_file, adjusted.data_file),
                evidence={
                    "session": raw.bars_by_timestamp[current].session_date.isoformat(),
                    "before": before,
                    "during": during,
                    "after": after,
                },
                confidence="high",
                basis="local factor discontinuity; adjacent genuine actions remain possible",
            )
    return {
        "raw_file": raw.data_file,
        "adjusted_file": adjusted.data_file,
        "adjustment": adjusted.adjustment,
        "shared_rows_with_feasible_factor": len(timestamps),
        "initial_factor_interval": series[timestamps[0]] if timestamps else None,
        "last_factor_interval": series[timestamps[-1]] if timestamps else None,
        "minimum_relative_step_reported": configuration.factor_step_min_relative,
        "boundaries": boundaries,
        "interpretation": (
            "Observed in the authenticated files; not an independently confirmed corporate-action ledger."
        ),
    }


def _validate_cohort(
    views: list[DownloadValidationView],
    collector: FindingCollector,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    configuration = collector.configuration
    filenames = [view.data_file for view in views]
    factor_regimes = []
    by_sha256: dict[str, list[str]] = defaultdict(list)
    for view in views:
        by_sha256[view.download_sha256].append(view.data_file)
    for download_sha256, matching_files in sorted(by_sha256.items()):
        if len(matching_files) > 1:
            collector.evaluated("identical_adjustment_files")
            collector.add(
                "identical_adjustment_files",
                INFORMATION,
                "Different adjustment labels have identical compressed bytes; this is not inherently wrong.",
                data_files=matching_files,
                evidence={"download_sha256": download_sha256},
                confidence="observed",
                basis="byte equivalence without an independent corporate-action ledger",
            )

    pipeline_versions = sorted({str(view.manifest.get("pipeline_version")) for view in views})
    if len(pipeline_versions) > 1:
        collector.evaluated("mixed_pipeline_versions")
        collector.add(
            "mixed_pipeline_versions",
            INFORMATION,
            "Comparable views were produced by multiple integrity-pipeline versions.",
            data_files=filenames,
            evidence={"pipeline_versions": pipeline_versions},
            confidence="observed",
            basis="producer provenance observation",
        )
    for digest_field in ("session_sha256", "calendar_sha256"):
        declared = sorted(
            {
                str(view.manifest[digest_field])
                for view in views
                if isinstance(view.manifest.get(digest_field), str)
            }
        )
        request_bounds = {(view.manifest.get("request_from"), view.manifest.get("request_to")) for view in views}
        collector.evaluated("declared_reference_consistency")
        if len(declared) > 1 and len(request_bounds) == 1:
            collector.add(
                "declared_reference_consistency",
                WARNING,
                "Comparable adjustment views declare different session or calendar digests.",
                data_files=filenames,
                evidence={"field": digest_field, "declared_sha256": declared},
                confidence="high",
                basis="producer reference-metadata disagreement",
            )

    variants: dict[frozenset[str], DownloadValidationView] = {}
    for view in views:
        collector.evaluated("unique_adjustment_views")
        if view.adjustment_actions in variants:
            collector.add(
                "unique_adjustment_views",
                WARNING,
                "More than one source declares the same adjustment action set; only the first is modeled.",
                data_files=(variants[view.adjustment_actions].data_file, view.data_file),
                confidence="high",
                basis="ambiguous adjustment bundle",
            )
            continue
        variants[view.adjustment_actions] = view

    raw = variants.get(frozenset())
    if raw is None:
        return factor_regimes, {
            "data_files": filenames,
            "status": "not_evaluated",
            "reason": "no raw adjustment view",
        }

    factors: dict[frozenset[str], dict[datetime, tuple[float, float]]] = {}
    for actions, adjusted in sorted(
        variants.items(), key=lambda item: (len(item[0]), adjustment_label(item[0]))
    ):
        if not actions:
            continue
        raw_timestamps = set(raw.timestamp_by_row_number.values())
        adjusted_timestamps = set(adjusted.timestamp_by_row_number.values())
        same_bounds = all(
            raw.manifest.get(field_name) == adjusted.manifest.get(field_name)
            for field_name in ("request_from", "request_to")
        )
        if same_bounds:
            collector.evaluated("cross_view_session_grid")
            if raw_timestamps != adjusted_timestamps:
                collector.add(
                    "cross_view_session_grid",
                    WARNING,
                    "Comparable requested adjustment views have different timestamp grids.",
                    data_files=(raw.data_file, adjusted.data_file),
                    evidence={
                        "missing_from_adjusted": [
                            _iso_timestamp(value)
                            for value in sorted(raw_timestamps - adjusted_timestamps)[
                                : configuration.maximum_evidence_examples
                            ]
                        ],
                        "extra_in_adjusted": [
                            _iso_timestamp(value)
                            for value in sorted(adjusted_timestamps - raw_timestamps)[
                                : configuration.maximum_evidence_examples
                            ]
                        ],
                        "missing_count": len(raw_timestamps - adjusted_timestamps),
                        "extra_count": len(adjusted_timestamps - raw_timestamps),
                    },
                    confidence="high",
                    basis="same request window; provider snapshots may still contain legitimate revisions",
                )
        shared_timestamps = sorted(set(raw.bars_by_timestamp) & set(adjusted.bars_by_timestamp))
        factors[actions] = {}
        for timestamp in shared_timestamps:
            raw_bar = raw.bars_by_timestamp[timestamp]
            adjusted_bar = adjusted.bars_by_timestamp[timestamp]
            collector.evaluated("adjusted_trade_count")
            if raw_bar.values["trade_count"] != adjusted_bar.values["trade_count"]:
                collector.add(
                    "adjusted_trade_count",
                    WARNING,
                    "Trade count differs across raw and adjusted views.",
                    data_files=(raw.data_file, adjusted.data_file),
                    evidence={
                        "timestamp": _iso_timestamp(timestamp),
                        "raw": str(raw_bar.values["trade_count"]),
                        "adjusted": str(adjusted_bar.values["trade_count"]),
                    },
                    confidence="high",
                    basis="adjustment-model disagreement; snapshots may contain legitimate revisions",
                )
            collector.evaluated("within_row_price_factor")
            factor_evaluation = _factor_interval(raw_bar, adjusted_bar, configuration)
            if factor_evaluation.outcome == NOT_EVALUATED:
                continue
            interval = factor_evaluation.measurements["factor_interval_intersection"]
            if factor_evaluation.outcome == RULE_WARNING:
                collector.add(
                    "within_row_price_factor",
                    WARNING,
                    "No common multiplicative factor explains OHLC and VWAP within serialization bounds.",
                    data_files=(raw.data_file, adjusted.data_file),
                    evidence={
                        "timestamp": _iso_timestamp(timestamp),
                        "factor_interval_intersection": interval,
                        "raw_prices": {
                            field_name: str(raw_bar.values[field_name])
                            for field_name in ("open", "high", "low", "close", "vwap")
                        },
                        "adjusted_prices": {
                            field_name: str(adjusted_bar.values[field_name])
                            for field_name in ("open", "high", "low", "close", "vwap")
                        },
                    },
                    confidence="high",
                    basis="multiplicative adjustment model; snapshots may contain legitimate revisions",
                )
            else:
                factors[actions][timestamp] = interval

    for split_enabled in (False, True):
        members = sorted(
            (view for actions, view in variants.items() if ("split" in actions) == split_enabled),
            key=lambda view: (len(view.adjustment_actions), view.data_file),
        )
        if not members:
            continue
        reference = members[0]
        for other in members[1:]:
            for timestamp in sorted(set(reference.bars_by_timestamp) & set(other.bars_by_timestamp)):
                collector.evaluated("price_only_volume_invariance")
                first = reference.bars_by_timestamp[timestamp]
                second = other.bars_by_timestamp[timestamp]
                if first.values["volume"] != second.values["volume"]:
                    collector.add(
                        "price_only_volume_invariance",
                        WARNING,
                        "Adding only dividend/spin-off adjustment changes volume.",
                        data_files=(reference.data_file, other.data_file),
                        evidence={
                            "timestamp": _iso_timestamp(timestamp),
                            "reference_volume": str(first.values["volume"]),
                            "other_volume": str(second.values["volume"]),
                        },
                        confidence="high",
                        basis="adjustment-model disagreement; snapshots may contain legitimate revisions",
                    )

    split_actions = frozenset(("split",))
    split_view = variants.get(split_actions)
    if split_view is not None:
        for timestamp, price_interval in factors.get(split_actions, {}).items():
            raw_bar, adjusted_bar = raw.bars_by_timestamp[timestamp], split_view.bars_by_timestamp[timestamp]
            raw_volume = raw_bar.numeric_value("volume")
            adjusted_volume = adjusted_bar.numeric_value("volume")
            if adjusted_volume <= configuration.split_volume_error:
                continue
            volume_interval = (
                raw_volume / (adjusted_volume + configuration.split_volume_error),
                raw_volume / (adjusted_volume - configuration.split_volume_error),
            )
            collector.evaluated("split_price_volume_reciprocity")
            if not _intervals_intersect(price_interval, volume_interval):
                collector.add(
                    "split_price_volume_reciprocity",
                    WARNING,
                    "Split-only price and volume factors are not reciprocal within serialization bounds.",
                    data_files=(raw.data_file, split_view.data_file),
                    evidence={
                        "timestamp": _iso_timestamp(timestamp),
                        "price_factor_interval": price_interval,
                        "volume_implied_price_interval": volume_interval,
                        "raw_volume": str(raw_bar.values["volume"]),
                        "split_volume": str(adjusted_bar.values["volume"]),
                    },
                    confidence="high",
                    basis="adjustment-model disagreement; snapshots may contain legitimate revisions",
                )

    factor_keys = sorted(factors, key=lambda actions: (len(actions), adjustment_label(actions)))
    for index, first_actions in enumerate(factor_keys):
        for second_actions in factor_keys[index + 1 :]:
            combined_actions = first_actions | second_actions
            if first_actions & second_actions or combined_actions not in factors:
                continue
            shared = sorted(
                set(factors[first_actions]) & set(factors[second_actions]) & set(factors[combined_actions])
            )
            for timestamp in shared:
                first_interval = factors[first_actions][timestamp]
                second_interval = factors[second_actions][timestamp]
                observed_combined = factors[combined_actions][timestamp]
                expected_combined = (
                    first_interval[0] * second_interval[0],
                    first_interval[1] * second_interval[1],
                )
                if not all(math.isfinite(value) for value in expected_combined):
                    continue
                collector.evaluated("adjustment_composition")
                if not _intervals_intersect(observed_combined, expected_combined):
                    collector.add(
                        "adjustment_composition",
                        WARNING,
                        "A combined adjustment is incompatible with its disjoint constituent factors.",
                        data_files=(
                            raw.data_file,
                            variants[first_actions].data_file,
                            variants[second_actions].data_file,
                            variants[combined_actions].data_file,
                        ),
                        evidence={
                            "timestamp": _iso_timestamp(timestamp),
                            "parts": [adjustment_label(first_actions), adjustment_label(second_actions)],
                            "expected_factor_interval": expected_combined,
                            "actual_factor_interval": observed_combined,
                        },
                        confidence="high",
                        basis="adjustment-model disagreement; snapshots may contain legitimate revisions",
                    )

    for actions, series in sorted(factors.items(), key=lambda item: (len(item[0]), adjustment_label(item[0]))):
        factor_regimes.append(_infer_factor_regimes(raw, variants[actions], series, collector))
    return factor_regimes, {
        "data_files": filenames,
        "status": "evaluated",
        "raw_file": raw.data_file,
        "adjustment_view_count": len(variants),
    }


def build_cross_adjustment_warnings(
    download_views: tuple[DownloadValidationView, ...],
    configuration: WarningPipelineConfiguration,
) -> dict[str, Any]:
    collector = FindingCollector(configuration)
    cohorts: dict[tuple[str, ...], list[DownloadValidationView]] = defaultdict(list)
    for view in download_views:
        cohorts[view.request_cohort_key()].append(view)
    factor_regimes = []
    coverage = []
    for cohort_key in sorted(cohorts):
        collector.evaluated("comparable_request_cohorts")
        cohort_regimes, cohort_coverage = _validate_cohort(
            sorted(cohorts[cohort_key], key=lambda view: view.data_file),
            collector,
        )
        factor_regimes.extend(cohort_regimes)
        coverage.append({"request_identity": list(cohort_key), **cohort_coverage})
    return {
        "rule_order": cross_adjustment_rule_order(),
        "integrity_classification": (
            "Warnings and observations only; deterministic data errors remain owned by "
            "integrity_failure.manifest.json."
        ),
        "coverage": coverage,
        "factor_regime_observations": factor_regimes,
        **collector.as_report_object(),
    }

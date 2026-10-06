import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from libs.alpaca_end_of_day_warning_pipeline import (
    WARNING_PIPELINE_VERSION,
    cross_adjustment_warnings_path,
    publish_warning_pipeline,
    security_segment_statistical_warnings_path,
    validate_warning_pipeline_change_log,
    warning_pipeline_manifest_path,
)
from libs.alpaca_end_of_day_warning_pipeline.cross_adjustment_warnings import (
    build_cross_adjustment_warnings,
)
from libs.alpaca_end_of_day_warning_pipeline.models import (
    AnalyzableDailyBar,
    AuthenticatedCorrelationOutputs,
    DownloadValidationView,
    WarningPipelineInputs,
)
from libs.alpaca_end_of_day_warning_pipeline.security_segment_statistics import (
    validate_security_segment_statistics,
)
from libs.alpaca_end_of_day_warning_pipeline.settings import DEFAULT_CONFIGURATION
from libs.common.market_data_rule_evaluators import (
    ERROR,
    WARNING,
    evaluate_common_multiplicative_price_factor,
)


def daily_bar(
    row_number: int,
    *,
    price: str,
    volume: str,
    trade_count: str,
    vwap: str | None = None,
) -> AnalyzableDailyBar:
    timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=row_number)
    base_price = Decimal(price)
    return AnalyzableDailyBar(
        row_number=row_number,
        timestamp_text=timestamp.isoformat().replace("+00:00", "Z"),
        timestamp=timestamp,
        session_date=date(2024, 1, 1) + timedelta(days=row_number),
        values={
            "open": base_price,
            "high": base_price + Decimal("1"),
            "low": base_price - Decimal("1"),
            "close": base_price + Decimal("0.1"),
            "volume": Decimal(volume),
            "trade_count": Decimal(trade_count),
            "vwap": Decimal(vwap) if vwap is not None else base_price + Decimal("0.05"),
        },
    )


def dated_daily_bar(row_number: int, session_date: date) -> AnalyzableDailyBar:
    bar = daily_bar(
        row_number,
        price=str(10 + row_number / 100),
        volume="1000",
        trade_count="100",
    )
    timestamp = datetime.combine(session_date, datetime.min.time(), tzinfo=timezone.utc)
    return AnalyzableDailyBar(
        row_number=row_number,
        timestamp_text=timestamp.isoformat().replace("+00:00", "Z"),
        timestamp=timestamp,
        session_date=session_date,
        values=bar.values,
    )


def download_view(
    filename: str,
    adjustment: str,
    bars: list[AnalyzableDailyBar],
    download_sha256: str,
) -> DownloadValidationView:
    actions = frozenset() if adjustment == "raw" else frozenset(adjustment.split(","))
    return DownloadValidationView(
        data_file=filename,
        download_sha256=download_sha256,
        adjustment=adjustment,
        adjustment_actions=actions,
        manifest={
            "symbol": "TEST",
            "adjustment": adjustment,
            "feed": "sip",
            "timeframe": "1Day",
            "asof": "2024-04-01",
            "request_from": "2024-01-01T00:00:00Z",
            "request_to": "2024-04-01T00:00:00Z",
            "pipeline_version": 14,
            "session_sha256": "s" * 64,
            "calendar_sha256": "c" * 64,
        },
        source_row_count=len(bars),
        timestamp_by_row_number={bar.row_number: bar.timestamp for bar in bars},
        bars_by_row_number={bar.row_number: bar for bar in bars},
        excluded_rows=(),
    )


class WarningPipelineTests(unittest.TestCase):
    def test_version_changelog_matches_warning_overlay(self) -> None:
        self.assertEqual(2, WARNING_PIPELINE_VERSION)
        self.assertEqual(WARNING_PIPELINE_VERSION, len(validate_warning_pipeline_change_log()))

    def test_shared_formula_keeps_measurement_static_while_policy_selects_severity(self) -> None:
        raw_prices = {"open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0, "vwap": 10.5}
        adjusted_prices = {"open": 5.0, "high": 6.0, "low": 4.5, "close": 5.5, "vwap": 5.8}

        warning_result = evaluate_common_multiplicative_price_factor(
            raw_prices,
            adjusted_prices,
            raw_absolute_error=0.000001,
            adjusted_absolute_error=0.01,
            violation_outcome=WARNING,
        )
        error_result = evaluate_common_multiplicative_price_factor(
            raw_prices,
            adjusted_prices,
            raw_absolute_error=0.000001,
            adjusted_absolute_error=0.01,
            violation_outcome=ERROR,
        )

        self.assertEqual(WARNING, warning_result.outcome)
        self.assertEqual(ERROR, error_result.outcome)
        self.assertEqual(warning_result.measurements, error_result.measurements)

    def test_statistics_never_compare_rows_across_alpaca_asset_uuid_boundary(self) -> None:
        bars = []
        for row_number in range(1, 61):
            if row_number == 25:
                volume, trade_count = "100", "1"
            elif row_number <= 30:
                volume, trade_count = str(100000 + row_number), "5000"
            else:
                volume, trade_count = str(200 + row_number), "10"
            bars.append(
                daily_bar(
                    row_number,
                    price=str(10 + row_number / 100),
                    volume=volume,
                    trade_count=trade_count,
                )
            )
        view = download_view("raw.csv.gz", "raw", bars, "a" * 64)
        source = {
            "data_file": "raw.csv.gz",
            "download_manifest": "raw.csv.gz.manifest.json",
            "download_sha256": "a" * 64,
            "rows": 60,
            "adjustment": "raw",
            "pipeline_state": "cached",
            "validation_pipeline_version": 14,
            "download_sha256_authenticated": True,
        }
        correlated = {
            "row_count": 60,
            "segments": [
                {
                    **source,
                    "row_start": 1,
                    "row_end": 30,
                    "row_count": 30,
                    "canonical_security": {"asset_id": "old-uuid", "cusip": "OLD"},
                },
                {
                    **source,
                    "row_start": 31,
                    "row_end": 60,
                    "row_count": 30,
                    "canonical_security": {"asset_id": "new-uuid", "cusip": "NEW"},
                },
            ],
        }
        inputs = WarningPipelineInputs(
            symbol="TEST",
            source_downloads=(source,),
            download_views=(view,),
            correlation=AuthenticatedCorrelationOutputs(
                receipt={"partition": {"row_partition_complete": True}},
                correlated=correlated,
                uncorrelated={"row_count": 0, "segments": []},
                correlated_sha256="b" * 64,
                uncorrelated_sha256="c" * 64,
            ),
            pipeline_receipt_manifest_sha256="d" * 64,
            integrity_failure_manifest_sha256="e" * 64,
        )

        report = validate_security_segment_statistics(inputs, DEFAULT_CONFIGURATION)

        warnings = [finding for finding in report["findings"] if finding["severity"] == "warning"]
        self.assertEqual(1, len(warnings))
        self.assertEqual("corroborated_activity_collapse", warnings[0]["check_id"])
        self.assertEqual("old-uuid", warnings[0]["canonical_security"]["asset_id"])
        self.assertNotIn("new-uuid", {item["canonical_security"]["asset_id"] for item in warnings})

    def test_session_gap_between_asset_uuids_is_not_a_missing_session_warning(self) -> None:
        bars = [
            dated_daily_bar(1, date(2024, 1, 2)),
            dated_daily_bar(2, date(2024, 1, 3)),
            dated_daily_bar(3, date(2024, 1, 8)),
            dated_daily_bar(4, date(2024, 1, 9)),
        ]
        view = download_view("raw.csv.gz", "raw", bars, "a" * 64)
        source = {
            "data_file": "raw.csv.gz",
            "download_manifest": "raw.csv.gz.manifest.json",
            "download_sha256": "a" * 64,
            "rows": 4,
            "adjustment": "raw",
            "pipeline_state": "cached",
            "validation_pipeline_version": 16,
            "download_sha256_authenticated": True,
        }
        inputs = WarningPipelineInputs(
            symbol="TEST",
            source_downloads=(source,),
            download_views=(view,),
            correlation=AuthenticatedCorrelationOutputs(
                receipt={"partition": {"row_partition_complete": True}},
                correlated={
                    "row_count": 4,
                    "segments": [
                        {
                            **source,
                            "row_start": 1,
                            "row_end": 2,
                            "row_count": 2,
                            "historical_symbol": "TEST",
                            "canonical_security": {"asset_id": "old-uuid", "cusip": "OLD"},
                        },
                        {
                            **source,
                            "row_start": 3,
                            "row_end": 4,
                            "row_count": 2,
                            "historical_symbol": "TEST",
                            "canonical_security": {"asset_id": "new-uuid", "cusip": "NEW"},
                        },
                    ],
                },
                uncorrelated={"row_count": 0, "segments": []},
                correlated_sha256="b" * 64,
                uncorrelated_sha256="c" * 64,
            ),
            pipeline_receipt_manifest_sha256="d" * 64,
            integrity_failure_manifest_sha256="e" * 64,
        )

        report = validate_security_segment_statistics(inputs, DEFAULT_CONFIGURATION)

        self.assertNotIn(
            "uuid_segment_session_coverage",
            {finding["check_id"] for finding in report["findings"]},
        )

    def test_missing_session_inside_one_asset_uuid_segment_is_a_warning(self) -> None:
        bars = [
            dated_daily_bar(1, date(2024, 1, 2)),
            dated_daily_bar(2, date(2024, 1, 4)),
        ]
        view = download_view("raw.csv.gz", "raw", bars, "a" * 64)
        source = {
            "data_file": "raw.csv.gz",
            "download_manifest": "raw.csv.gz.manifest.json",
            "download_sha256": "a" * 64,
            "rows": 2,
            "adjustment": "raw",
            "pipeline_state": "cached",
            "validation_pipeline_version": 16,
            "download_sha256_authenticated": True,
        }
        inputs = WarningPipelineInputs(
            symbol="TEST",
            source_downloads=(source,),
            download_views=(view,),
            correlation=AuthenticatedCorrelationOutputs(
                receipt={"partition": {"row_partition_complete": True}},
                correlated={
                    "row_count": 2,
                    "segments": [
                        {
                            **source,
                            "row_start": 1,
                            "row_end": 2,
                            "row_count": 2,
                            "historical_symbol": "TEST",
                            "canonical_security": {"asset_id": "one-uuid", "cusip": "ONE"},
                        }
                    ],
                },
                uncorrelated={"row_count": 0, "segments": []},
                correlated_sha256="b" * 64,
                uncorrelated_sha256="c" * 64,
            ),
            pipeline_receipt_manifest_sha256="d" * 64,
            integrity_failure_manifest_sha256="e" * 64,
        )

        report = validate_security_segment_statistics(inputs, DEFAULT_CONFIGURATION)
        coverage_findings = [
            finding for finding in report["findings"] if finding["check_id"] == "uuid_segment_session_coverage"
        ]

        self.assertEqual(1, len(coverage_findings))
        self.assertEqual(["2024-01-03"], coverage_findings[0]["evidence"]["missing_sessions"])
        self.assertEqual("one-uuid", coverage_findings[0]["canonical_security"]["asset_id"])

    def test_cross_adjustment_model_disagreement_is_a_warning_not_an_integrity_error(self) -> None:
        raw_bar = daily_bar(1, price="10", volume="1000", trade_count="100")
        adjusted_bar = AnalyzableDailyBar(
            row_number=1,
            timestamp_text=raw_bar.timestamp_text,
            timestamp=raw_bar.timestamp,
            session_date=raw_bar.session_date,
            values={
                "open": Decimal("5"),
                "high": Decimal("6"),
                "low": Decimal("4.5"),
                "close": Decimal("5.05"),
                "volume": Decimal("1000"),
                "trade_count": Decimal("100"),
                "vwap": Decimal("5.8"),
            },
        )
        views = (
            download_view("raw.csv.gz", "raw", [raw_bar], "a" * 64),
            download_view("dividend.csv.gz", "dividend", [adjusted_bar], "b" * 64),
        )

        report = build_cross_adjustment_warnings(views, DEFAULT_CONFIGURATION)

        self.assertEqual(1, report["warning_count"])
        self.assertEqual("within_row_price_factor", report["findings"][0]["check_id"])
        self.assertEqual({"warning"}, {finding["severity"] for finding in report["findings"]})
        self.assertIn("integrity_failure.manifest.json", report["integrity_classification"])

    def test_publication_is_digest_bound_and_cached_in_symbol_directory(self) -> None:
        empty_warning_report = {
            "warning_count": 0,
            "information_count": 0,
            "total_finding_count": 0,
            "omitted_finding_count": 0,
            "checks": [],
            "findings": [],
            "rule_order": [],
        }
        report = {
            "symbol": "TEST",
            "boundary": {
                "owner": "alpaca_end_of_day_warning_pipeline",
                "severity_domain": ["warning", "information"],
            },
            "configuration": DEFAULT_CONFIGURATION.as_report_object(),
            "validation_pipeline_version": 14,
            "calendar_sha256": "a" * 64,
            "pipeline_receipt_manifest_sha256": "b" * 64,
            "integrity_failure_manifest_sha256": "c" * 64,
            "source_downloads": [],
            "correlation": {
                "correlation_version": 6,
                "correlation_generation_sha256": "d" * 64,
                "correlated_output_sha256": "e" * 64,
                "uncorrelated_output_sha256": "f" * 64,
                "row_partition_complete": True,
            },
            "cross_adjustment": empty_warning_report,
            "security_segment_statistics": empty_warning_report,
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            first_receipt = publish_warning_pipeline(repository_root, report)
            paths = (
                cross_adjustment_warnings_path(repository_root, "TEST"),
                security_segment_statistical_warnings_path(repository_root, "TEST"),
                warning_pipeline_manifest_path(repository_root, "TEST"),
            )
            first_stats = tuple(path.stat() for path in paths)
            second_receipt = publish_warning_pipeline(repository_root, report)
            second_stats = tuple(path.stat() for path in paths)

        self.assertEqual("published", first_receipt["publication_status"])
        self.assertEqual("cached", second_receipt["publication_status"])
        self.assertEqual(
            [(item.st_size, item.st_mtime_ns) for item in first_stats],
            [(item.st_size, item.st_mtime_ns) for item in second_stats],
        )


if __name__ == "__main__":
    unittest.main()

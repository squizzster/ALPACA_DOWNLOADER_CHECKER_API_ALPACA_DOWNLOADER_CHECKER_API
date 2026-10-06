import io
import tempfile
import unittest
from pathlib import Path

from libs.alpaca_end_of_day_ticker_pipeline import (
    EndOfDayTickerPipelineResult,
    parse_end_of_day_ticker_pipeline_arguments,
    run_end_of_day_ticker_pipeline,
    run_end_of_day_tickers_pipeline_command,
)
from libs.download_alpaca_end_of_day_bars import (
    EndOfDayDownloadPlan,
    EndOfDayDownloadStageResult,
)


def correlation_receipt() -> dict:
    return {
        "publication_status": "published",
        "correlation_generation_sha256": "c" * 64,
        "partition": {
            "source_row_count": 10,
            "correlated_row_count": 6,
            "uncorrelated_row_count": 4,
            "row_partition_complete": True,
        },
    }


def warning_pipeline_receipt() -> dict:
    return {
        "publication_status": "published",
        "warning_pipeline_generation_sha256": "w" * 64,
        "summary": {
            "warning_count": 2,
            "information_count": 1,
        },
    }


class EndOfDayTickerPipelineTests(unittest.TestCase):
    def test_recorded_integrity_failures_continue_into_security_correlation(self) -> None:
        pipeline_events = []
        download_plan = EndOfDayDownloadPlan(
            symbol="FB",
            adjustment_modes=("raw",),
            force_authenticated_replacement=False,
            repair_orphaned_download_pair=False,
        )

        def run_download_and_validation_stage(
            supplied_download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            request_rate_limiter,
        ):
            pipeline_events.append("download_and_validation")
            self.assertEqual(download_plan, supplied_download_plan)
            self.assertEqual("2026-10-06T00:00:00Z", request_end_timestamp)
            return EndOfDayDownloadStageResult(
                symbol="FB",
                requested_adjustment_modes=("raw",),
                pricing_data_integrity_failure_count=1,
            )

        def inspect_security_lineage(
            ticker,
            api_key_id,
            api_secret_key,
            *,
            repository_root,
            request_rate_limiter,
        ):
            pipeline_events.append("identity")
            self.assertEqual(("FB", "key", "secret"), (ticker, api_key_id, api_secret_key))
            return {"symbol": ticker}

        def build_security_correlation_report(lineage_report, repository_root):
            pipeline_events.append("correlate")
            return {"symbol": lineage_report["symbol"]}

        def publish_security_correlation(repository_root, security_correlation_report):
            pipeline_events.append("publish_correlation")
            return correlation_receipt()

        def build_warning_pipeline_report(repository_root, symbol):
            pipeline_events.append("validate_market_data")
            self.assertEqual("FB", symbol)
            return {"symbol": symbol}

        def publish_warning_pipeline(repository_root, report):
            pipeline_events.append("publish_warning_pipeline")
            self.assertEqual("FB", report["symbol"])
            return warning_pipeline_receipt()

        with tempfile.TemporaryDirectory() as temporary_directory:
            result = run_end_of_day_ticker_pipeline(
                download_plan,
                repository_root=Path(temporary_directory),
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                request_end_timestamp="2026-10-06T00:00:00Z",
                download_and_validation_stage_runner=run_download_and_validation_stage,
                security_lineage_inspector=inspect_security_lineage,
                security_correlation_report_builder=build_security_correlation_report,
                security_correlation_publisher=publish_security_correlation,
                warning_pipeline_report_builder=build_warning_pipeline_report,
                warning_pipeline_publisher=publish_warning_pipeline,
            )

        self.assertEqual(
            [
                "download_and_validation",
                "identity",
                "correlate",
                "publish_correlation",
                "validate_market_data",
                "publish_warning_pipeline",
            ],
            pipeline_events,
        )
        self.assertTrue(result.download_stage.has_pricing_data_integrity_failures)
        self.assertTrue(result.security_correlation_receipt["partition"]["row_partition_complete"])
        self.assertEqual(2, result.warning_pipeline_receipt["summary"]["warning_count"])

    def test_repeatable_symbols_share_rate_limiters_and_finish_before_failure_exit(self) -> None:
        processed_symbols = []
        bar_rate_limiters = []
        identity_rate_limiters = []

        def run_ticker_pipeline(
            download_plan,
            *,
            repository_root,
            environment,
            request_end_timestamp,
            bar_request_rate_limiter,
            identity_request_rate_limiter,
        ):
            processed_symbols.append(download_plan.symbol)
            bar_rate_limiters.append(bar_request_rate_limiter)
            identity_rate_limiters.append(identity_request_rate_limiter)
            return EndOfDayTickerPipelineResult(
                download_stage=EndOfDayDownloadStageResult(
                    symbol=download_plan.symbol,
                    requested_adjustment_modes=download_plan.adjustment_modes,
                    pricing_data_integrity_failure_count=2 if download_plan.symbol == "FB" else 0,
                ),
                security_lineage_report={"symbol": download_plan.symbol},
                security_correlation_receipt=correlation_receipt(),
                warning_pipeline_receipt=warning_pipeline_receipt(),
            )

        output_stream = io.StringIO()
        error_stream = io.StringIO()
        cli_arguments = parse_end_of_day_ticker_pipeline_arguments(
            [
                "--symbol",
                "DELL",
                "-s",
                "fb",
                "-s",
                "TWTR",
                "-s",
                "dell",
                "--adjustment",
                "raw",
            ]
        )

        with self.assertRaisesRegex(SystemExit, "1"):
            run_end_of_day_tickers_pipeline_command(
                cli_arguments,
                environment={
                    "ALPACA_API_KEY_ID": "key",
                    "ALPACA_API_SECRET_KEY": "secret",
                },
                output_stream=output_stream,
                error_stream=error_stream,
                request_end_timestamp="2026-10-06T00:00:00Z",
                ticker_pipeline_runner=run_ticker_pipeline,
            )

        self.assertEqual(["DELL", "FB", "TWTR"], processed_symbols)
        self.assertEqual(1, len({id(rate_limiter) for rate_limiter in bar_rate_limiters}))
        self.assertEqual(1, len({id(rate_limiter) for rate_limiter in identity_rate_limiters}))
        self.assertEqual(3, output_stream.getvalue().count("correlated=6, uncorrelated=4"))
        self.assertEqual(3, output_stream.getvalue().count("warnings=2, information=1"))
        self.assertIn(
            "2 artifact(s) across 1 ticker(s) failed pricing/data integrity",
            error_stream.getvalue(),
        )


if __name__ == "__main__":
    unittest.main()

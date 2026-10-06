import argparse
import sys
from pathlib import Path
from typing import Mapping, Protocol, TextIO

from libs.alpaca_end_of_day_security_correlation import CORRELATION_VERSION
from libs.alpaca_end_of_day_warning_pipeline import WARNING_PIPELINE_VERSION
from libs.alpaca_security_lineage import AlpacaIdentityRequestRateLimiter
from libs.download_alpaca_end_of_day_bars import (
    AlpacaRequestRateLimiter,
    EndOfDayDownloadPlan,
    create_end_of_day_download_argument_parser,
    end_of_day_download_plan_from_command_line,
    end_of_day_download_plans_from_command_line,
)

from .contracts import EndOfDayTickerPipelineResult
from .orchestration import run_end_of_day_ticker_pipeline


class EndOfDayTickerPipelineRunner(Protocol):
    def __call__(
        self,
        download_plan: EndOfDayDownloadPlan,
        *,
        repository_root: Path | None = None,
        environment: Mapping[str, str] | None = None,
        request_end_timestamp: str | None = None,
        bar_request_rate_limiter: AlpacaRequestRateLimiter | None = None,
        identity_request_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
    ) -> EndOfDayTickerPipelineResult: ...


def parse_end_of_day_ticker_pipeline_arguments(
    arguments: list[str] | None = None,
) -> argparse.Namespace:
    argument_parser = create_end_of_day_download_argument_parser(
        description=(
            "Run the Alpaca end-of-day ticker pipeline: download/cache validation, "
            "identity evidence, security correlation, and warning overlays."
        ),
        repeatable_symbols=True,
    )
    return argument_parser.parse_args(arguments)


def _write_ticker_pipeline_result(
    pipeline_result: EndOfDayTickerPipelineResult,
    *,
    output_stream: TextIO,
) -> None:
    correlation_receipt = pipeline_result.security_correlation_receipt
    correlation_partition = correlation_receipt["partition"]
    print(
        f"{pipeline_result.download_stage.symbol}: security correlation "
        f"{correlation_receipt['publication_status']}, "
        f"correlation=v{CORRELATION_VERSION}, "
        f"correlated={correlation_partition['correlated_row_count']}, "
        f"uncorrelated={correlation_partition['uncorrelated_row_count']}, "
        f"generation={correlation_receipt['correlation_generation_sha256']}",
        file=output_stream,
        flush=True,
    )
    warning_receipt = pipeline_result.warning_pipeline_receipt
    warning_summary = warning_receipt["summary"]
    print(
        f"{pipeline_result.download_stage.symbol}: market-data warnings "
        f"{warning_receipt['publication_status']}, "
        f"warning-pipeline=v{WARNING_PIPELINE_VERSION}, "
        f"warnings={warning_summary['warning_count']}, "
        f"information={warning_summary['information_count']}, "
        f"generation={warning_receipt['warning_pipeline_generation_sha256']}",
        file=output_stream,
        flush=True,
    )


def _run_end_of_day_download_plans(
    download_plans: tuple[EndOfDayDownloadPlan, ...],
    *,
    repository_root: Path | None,
    environment: Mapping[str, str] | None,
    output_stream: TextIO,
    error_stream: TextIO,
    request_end_timestamp: str | None,
    ticker_pipeline_runner: EndOfDayTickerPipelineRunner,
) -> tuple[EndOfDayTickerPipelineResult, ...]:
    bar_request_rate_limiter = AlpacaRequestRateLimiter()
    identity_request_rate_limiter = AlpacaIdentityRequestRateLimiter()
    pipeline_results = []
    failed_integrity_artifact_count = 0
    tickers_with_integrity_failures = 0

    for download_plan in download_plans:
        pipeline_result = ticker_pipeline_runner(
            download_plan,
            repository_root=repository_root,
            environment=environment,
            request_end_timestamp=request_end_timestamp,
            bar_request_rate_limiter=bar_request_rate_limiter,
            identity_request_rate_limiter=identity_request_rate_limiter,
        )
        pipeline_results.append(pipeline_result)
        _write_ticker_pipeline_result(pipeline_result, output_stream=output_stream)
        failed_artifacts_for_ticker = pipeline_result.download_stage.pricing_data_integrity_failure_count
        if failed_artifacts_for_ticker:
            failed_integrity_artifact_count += failed_artifacts_for_ticker
            tickers_with_integrity_failures += 1

    if failed_integrity_artifact_count:
        print(
            "error: "
            f"{failed_integrity_artifact_count} artifact(s) across "
            f"{tickers_with_integrity_failures} ticker(s) failed pricing/data integrity",
            file=error_stream,
        )
        raise SystemExit(1)
    return tuple(pipeline_results)


def run_end_of_day_tickers_pipeline_command(
    cli_arguments: argparse.Namespace,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    output_stream: TextIO | None = None,
    error_stream: TextIO | None = None,
    request_end_timestamp: str | None = None,
    ticker_pipeline_runner: EndOfDayTickerPipelineRunner = run_end_of_day_ticker_pipeline,
) -> tuple[EndOfDayTickerPipelineResult, ...]:
    return _run_end_of_day_download_plans(
        end_of_day_download_plans_from_command_line(cli_arguments),
        repository_root=repository_root,
        environment=environment,
        output_stream=sys.stdout if output_stream is None else output_stream,
        error_stream=sys.stderr if error_stream is None else error_stream,
        request_end_timestamp=request_end_timestamp,
        ticker_pipeline_runner=ticker_pipeline_runner,
    )


def run_end_of_day_ticker_pipeline_command(
    cli_arguments: argparse.Namespace,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    output_stream: TextIO | None = None,
    error_stream: TextIO | None = None,
    request_end_timestamp: str | None = None,
    ticker_pipeline_runner: EndOfDayTickerPipelineRunner = run_end_of_day_ticker_pipeline,
) -> EndOfDayTickerPipelineResult:
    pipeline_results = _run_end_of_day_download_plans(
        (end_of_day_download_plan_from_command_line(cli_arguments),),
        repository_root=repository_root,
        environment=environment,
        output_stream=sys.stdout if output_stream is None else output_stream,
        error_stream=sys.stderr if error_stream is None else error_stream,
        request_end_timestamp=request_end_timestamp,
        ticker_pipeline_runner=ticker_pipeline_runner,
    )
    return pipeline_results[0]

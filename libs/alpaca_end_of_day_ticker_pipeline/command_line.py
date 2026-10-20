import argparse
import json
import sys
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Protocol, TextIO

from libs.alpaca_end_of_day_pipeline import (
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    AlpacaDownloadIntegrityError,
    authenticate_frozen_download,
    download_manifest_path,
    inspect_cached_download,
)
from libs.alpaca_end_of_day_security_correlation import (
    CORRELATION_VERSION,
    SecurityCorrelationError,
    authenticate_security_correlation,
    security_correlation_manifest_path,
)
from libs.alpaca_end_of_day_warning_pipeline import WARNING_PIPELINE_VERSION
from libs.alpaca_security_lineage import (
    AlpacaIdentityRequestRateLimiter,
    recurring_pipeline_should_skip,
)
from libs.download_alpaca_end_of_day_bars import (
    AlpacaInvalidTickerCodeError,
    AlpacaRequestRateLimiter,
    EndOfDayDownloadPlan,
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


@dataclass(frozen=True)
class _TickerPipelineFailure:
    symbol: str
    diagnostic: str
    exit_status: int
    reason_code: str | None = None


def _result_symbol_disposition(
    pipeline_result: EndOfDayTickerPipelineResult,
) -> dict | None:
    lineage = pipeline_result.security_lineage_report.get("lineage")
    disposition = lineage.get("symbol_disposition") if isinstance(lineage, dict) else None
    return disposition if isinstance(disposition, dict) else None


def _authenticated_frozen_sources_match_correlation(
    repository_root: Path,
    symbol: str,
    authenticated_correlation,
) -> bool:
    observed_sha256_by_inode: dict[tuple[int, int, int, int], str] = {}
    symbol_directory = repository_root / "data" / "end_of_day" / symbol
    try:
        for filename, source in authenticated_correlation.sources_by_filename.items():
            download_path = symbol_directory / filename
            manifest_path = download_manifest_path(download_path)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict):
                return False
            if (
                manifest.get("sha256") != source.get("download_sha256")
                or manifest.get("rows") != source.get("rows")
                or manifest.get("adjustment") != source.get("adjustment")
            ):
                return False
            request_identity = {
                field_name: manifest.get(field_name) for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS
            }
            if any(not isinstance(value, str) or not value for value in request_identity.values()):
                return False
            cache_inspection = inspect_cached_download(download_path, request_identity)
            if (
                cache_inspection.required_action
                not in {
                    "cached",
                    RECORDED_PRICING_DATA_FAILURE_ACTION,
                }
                or cache_inspection.download_manifest != manifest
            ):
                return False
            download_stat = download_path.stat()
            inode_key = (
                download_stat.st_dev,
                download_stat.st_ino,
                download_stat.st_size,
                download_stat.st_mtime_ns,
            )
            observed_sha256 = observed_sha256_by_inode.get(inode_key)
            if observed_sha256 is None:
                observed_sha256 = authenticate_frozen_download(download_path, manifest)
                observed_sha256_by_inode[inode_key] = observed_sha256
            elif observed_sha256 != manifest.get("sha256"):
                return False
    except (
        AlpacaDownloadIntegrityError,
        json.JSONDecodeError,
        KeyError,
        OSError,
        TypeError,
        UnicodeError,
        ValueError,
    ):
        return False
    return True


def _authenticated_recurring_skip_disposition(
    repository_root: Path,
    symbol: str,
) -> dict | None:
    """Trust a skip only after its complete correlation partition authenticates."""
    receipt_path = security_correlation_manifest_path(repository_root, symbol)
    try:
        candidate_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(candidate_receipt, dict):
        return None
    candidate_disposition = candidate_receipt.get("symbol_disposition")
    if not recurring_pipeline_should_skip(candidate_disposition):
        return None
    try:
        authenticated = authenticate_security_correlation(repository_root, symbol)
    except SecurityCorrelationError:
        return None
    if not _authenticated_frozen_sources_match_correlation(
        repository_root,
        symbol,
        authenticated,
    ):
        return None
    disposition = authenticated.receipt.get("symbol_disposition")
    return disposition if recurring_pipeline_should_skip(disposition) else None


def _write_ticker_pipeline_result(
    pipeline_result: EndOfDayTickerPipelineResult,
    *,
    output_stream: TextIO,
) -> None:
    symbol_resolution = pipeline_result.security_lineage_report.get("symbol_resolution")
    if isinstance(symbol_resolution, dict) and symbol_resolution.get("changed") is True:
        print(
            f"{symbol_resolution.get('submitted_symbol')} -> "
            f"{symbol_resolution.get('authoritative_symbol')}: authoritative Alpaca asset symbol",
            file=output_stream,
            flush=True,
        )
    disposition = _result_symbol_disposition(pipeline_result)
    if disposition is not None:
        print(
            f"{pipeline_result.download_stage.symbol}: symbol disposition "
            f"{disposition.get('classification')}, "
            f"policy={disposition.get('data_policy')}, "
            f"recurring={disposition.get('recurring_pipeline_action')}",
            file=output_stream,
            flush=True,
        )
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
    worker_count: int,
    honor_recurring_dispositions: bool,
    ticker_pipeline_runner: EndOfDayTickerPipelineRunner,
) -> tuple[EndOfDayTickerPipelineResult, ...]:
    if worker_count not in (1, 2, 3):
        raise ValueError("worker count must be 1, 2, or 3")
    frozen_request_end_timestamp = request_end_timestamp or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT00:00:00Z"
    )
    resolved_repository_root = Path.cwd() if repository_root is None else repository_root
    recurring_skips: list[tuple[str, dict]] = []
    if honor_recurring_dispositions:
        retained_download_plans = []
        for download_plan in download_plans:
            disposition = _authenticated_recurring_skip_disposition(
                resolved_repository_root,
                download_plan.symbol,
            )
            if disposition is None:
                retained_download_plans.append(download_plan)
                continue
            recurring_skips.append((download_plan.symbol, disposition))
            print(
                f"{download_plan.symbol}: skipped authenticated recurring disposition "
                f"{disposition['classification']} (policy={disposition['data_policy']})",
                file=output_stream,
                flush=True,
            )
        download_plans = tuple(retained_download_plans)

    shared_request_rate_limiter = AlpacaRequestRateLimiter()
    pipeline_results: list[EndOfDayTickerPipelineResult | None] = [None] * len(download_plans)
    ticker_pipeline_failures: list[_TickerPipelineFailure] = []
    failed_integrity_artifact_count = 0
    tickers_with_integrity_failures = 0
    disposition_retained_integrity_artifact_count = 0
    tickers_with_disposition_retained_integrity_failures = 0

    def run_ticker_pipeline(
        download_plan: EndOfDayDownloadPlan,
    ) -> EndOfDayTickerPipelineResult | _TickerPipelineFailure:
        try:
            return ticker_pipeline_runner(
                download_plan,
                repository_root=repository_root,
                environment=environment,
                request_end_timestamp=frozen_request_end_timestamp,
                bar_request_rate_limiter=shared_request_rate_limiter,
                identity_request_rate_limiter=shared_request_rate_limiter,
            )
        except AlpacaInvalidTickerCodeError as error:
            return _TickerPipelineFailure(
                symbol=download_plan.symbol,
                diagnostic=str(error),
                exit_status=1,
                reason_code=error.reason_code,
            )
        except SystemExit as error:
            exit_status = error.code if isinstance(error.code, int) and error.code in (1, 2) else 1
            return _TickerPipelineFailure(
                symbol=download_plan.symbol,
                diagnostic=str(error) or f"pipeline exited with status {exit_status}",
                exit_status=exit_status,
            )
        except Exception as error:
            return _TickerPipelineFailure(
                symbol=download_plan.symbol,
                diagnostic=f"{type(error).__name__}: {error}",
                exit_status=2 if isinstance(error, AlpacaDownloadIntegrityError) else 1,
            )

    def record_pipeline_result(position: int, pipeline_result: EndOfDayTickerPipelineResult) -> None:
        nonlocal disposition_retained_integrity_artifact_count
        nonlocal failed_integrity_artifact_count
        nonlocal tickers_with_disposition_retained_integrity_failures
        nonlocal tickers_with_integrity_failures
        pipeline_results[position] = pipeline_result
        _write_ticker_pipeline_result(pipeline_result, output_stream=output_stream)
        failed_artifacts_for_ticker = pipeline_result.download_stage.pricing_data_integrity_failure_count
        if failed_artifacts_for_ticker:
            disposition = _result_symbol_disposition(pipeline_result)
            if honor_recurring_dispositions and recurring_pipeline_should_skip(disposition):
                disposition_retained_integrity_artifact_count += failed_artifacts_for_ticker
                tickers_with_disposition_retained_integrity_failures += 1
            else:
                failed_integrity_artifact_count += failed_artifacts_for_ticker
                tickers_with_integrity_failures += 1

    def record_pipeline_outcome(
        position: int,
        pipeline_outcome: EndOfDayTickerPipelineResult | _TickerPipelineFailure,
    ) -> None:
        if isinstance(pipeline_outcome, _TickerPipelineFailure):
            ticker_pipeline_failures.append(pipeline_outcome)
            if pipeline_outcome.reason_code is not None:
                print(
                    f"error: {pipeline_outcome.symbol}: {pipeline_outcome.reason_code}; "
                    f"continuing batch: {pipeline_outcome.diagnostic}",
                    file=error_stream,
                    flush=True,
                )
            else:
                print(
                    f"error: {pipeline_outcome.symbol}: ticker pipeline failed; "
                    f"continuing batch: {pipeline_outcome.diagnostic}",
                    file=error_stream,
                    flush=True,
                )
            return
        record_pipeline_result(position, pipeline_outcome)

    if worker_count == 1:
        for position, download_plan in enumerate(download_plans):
            record_pipeline_outcome(position, run_ticker_pipeline(download_plan))
    else:
        executor = ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="alpaca-ticker",
        )
        future_positions: dict[Future[EndOfDayTickerPipelineResult | _TickerPipelineFailure], int] = {}
        try:
            for position, download_plan in enumerate(download_plans):
                future_positions[executor.submit(run_ticker_pipeline, download_plan)] = position
            for completed_future in as_completed(future_positions):
                record_pipeline_outcome(
                    future_positions[completed_future],
                    completed_future.result(),
                )
        except BaseException:
            executor.shutdown(wait=True, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)

    invalid_ticker_code_failures = [
        failure
        for failure in ticker_pipeline_failures
        if failure.reason_code == AlpacaInvalidTickerCodeError.reason_code
    ]
    unclassified_ticker_pipeline_failures = [
        failure for failure in ticker_pipeline_failures if failure.reason_code is None
    ]
    if invalid_ticker_code_failures:
        print(
            f"error: {len(invalid_ticker_code_failures)} known invalid_ticker_code error(s) during the batch",
            file=error_stream,
        )
    if unclassified_ticker_pipeline_failures:
        print(
            f"error: {len(unclassified_ticker_pipeline_failures)} unclassified "
            "ticker pipeline failure(s) during the batch",
            file=error_stream,
        )
    if recurring_skips:
        print(
            f"{len(recurring_skips)} ticker(s) skipped by authenticated recurring disposition",
            file=output_stream,
        )
    if disposition_retained_integrity_artifact_count:
        print(
            f"{disposition_retained_integrity_artifact_count} recorded integrity-failure artifact(s) "
            f"across {tickers_with_disposition_retained_integrity_failures} disposition-skipped "
            "ticker(s) retained for audit and omitted from the recurring failure total",
            file=output_stream,
        )
    if failed_integrity_artifact_count:
        print(
            "error: "
            f"{failed_integrity_artifact_count} artifact(s) across "
            f"{tickers_with_integrity_failures} ticker(s) failed pricing/data integrity",
            file=error_stream,
        )
    if ticker_pipeline_failures or failed_integrity_artifact_count:
        raise SystemExit(2 if any(failure.exit_status == 2 for failure in ticker_pipeline_failures) else 1)
    if any(pipeline_result is None for pipeline_result in pipeline_results):
        raise RuntimeError("ticker worker completed without publishing a result")
    return tuple(pipeline_result for pipeline_result in pipeline_results if pipeline_result is not None)


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
    requested_end_timestamp = request_end_timestamp or getattr(cli_arguments, "request_end_timestamp", None)
    return _run_end_of_day_download_plans(
        end_of_day_download_plans_from_command_line(cli_arguments),
        repository_root=repository_root,
        environment=environment,
        output_stream=sys.stdout if output_stream is None else output_stream,
        error_stream=sys.stderr if error_stream is None else error_stream,
        request_end_timestamp=requested_end_timestamp,
        worker_count=getattr(cli_arguments, "workers", 1),
        honor_recurring_dispositions=(
            getattr(cli_arguments, "symbol_file", None) is not None
            and not getattr(cli_arguments, "include_skipped_symbols", False)
        ),
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
    requested_end_timestamp = request_end_timestamp or getattr(cli_arguments, "request_end_timestamp", None)
    pipeline_results = _run_end_of_day_download_plans(
        (end_of_day_download_plan_from_command_line(cli_arguments),),
        repository_root=repository_root,
        environment=environment,
        output_stream=sys.stdout if output_stream is None else output_stream,
        error_stream=sys.stderr if error_stream is None else error_stream,
        request_end_timestamp=requested_end_timestamp,
        worker_count=1,
        honor_recurring_dispositions=False,
        ticker_pipeline_runner=ticker_pipeline_runner,
    )
    return pipeline_results[0]

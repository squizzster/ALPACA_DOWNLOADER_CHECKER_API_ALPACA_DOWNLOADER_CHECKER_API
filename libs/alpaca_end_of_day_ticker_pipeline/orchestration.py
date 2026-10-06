import os
from pathlib import Path
from typing import Any, Mapping, Protocol

from libs.alpaca_end_of_day_security_correlation import (
    build_security_correlation_report,
    publish_security_correlation,
)
from libs.alpaca_end_of_day_warning_pipeline import (
    build_warning_pipeline_report,
    publish_warning_pipeline,
)
from libs.alpaca_security_lineage import (
    AlpacaIdentityRequestRateLimiter,
    inspect_alpaca_security_lineage,
)
from libs.download_alpaca_end_of_day_bars import (
    AlpacaRequestRateLimiter,
    EndOfDayDownloadPlan,
    EndOfDayDownloadStageResult,
    run_end_of_day_download_stage,
)

from .contracts import EndOfDayTickerPipelineResult


class DownloadAndValidationStageRunner(Protocol):
    def __call__(
        self,
        download_plan: EndOfDayDownloadPlan,
        *,
        repository_root: Path | None = None,
        environment: Mapping[str, str] | None = None,
        request_end_timestamp: str | None = None,
        request_rate_limiter: AlpacaRequestRateLimiter | None = None,
    ) -> EndOfDayDownloadStageResult: ...


class SecurityLineageInspector(Protocol):
    def __call__(
        self,
        ticker: str,
        api_key_id: str,
        api_secret_key: str,
        *,
        repository_root: Path | None = None,
        request_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
    ) -> dict[str, Any]: ...


class SecurityCorrelationReportBuilder(Protocol):
    def __call__(
        self,
        lineage_report: dict[str, Any],
        repository_root: Path,
    ) -> dict[str, Any]: ...


class SecurityCorrelationPublisher(Protocol):
    def __call__(
        self,
        repository_root: Path,
        correlation_report: dict[str, Any],
    ) -> dict[str, Any]: ...


class WarningPipelineReportBuilder(Protocol):
    def __call__(
        self,
        repository_root: Path,
        symbol: str,
    ) -> dict[str, Any]: ...


class WarningPipelinePublisher(Protocol):
    def __call__(
        self,
        repository_root: Path,
        report: dict[str, Any],
    ) -> dict[str, Any]: ...


def _identity_api_credentials(environment: Mapping[str, str]) -> tuple[str, str]:
    api_key_id = environment.get("ALPACA_API_KEY_ID")
    api_secret_key = environment.get("ALPACA_API_SECRET_KEY")
    if not api_key_id or not api_secret_key:
        raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")
    return api_key_id, api_secret_key


def run_end_of_day_ticker_pipeline(
    download_plan: EndOfDayDownloadPlan,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    request_end_timestamp: str | None = None,
    bar_request_rate_limiter: AlpacaRequestRateLimiter | None = None,
    identity_request_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
    download_and_validation_stage_runner: DownloadAndValidationStageRunner = run_end_of_day_download_stage,
    security_lineage_inspector: SecurityLineageInspector = inspect_alpaca_security_lineage,
    security_correlation_report_builder: SecurityCorrelationReportBuilder = (build_security_correlation_report),
    security_correlation_publisher: SecurityCorrelationPublisher = publish_security_correlation,
    warning_pipeline_report_builder: WarningPipelineReportBuilder = (build_warning_pipeline_report),
    warning_pipeline_publisher: WarningPipelinePublisher = publish_warning_pipeline,
) -> EndOfDayTickerPipelineResult:
    resolved_repository_root = Path.cwd() if repository_root is None else repository_root
    command_environment = os.environ if environment is None else environment

    download_stage_result = download_and_validation_stage_runner(
        download_plan,
        repository_root=resolved_repository_root,
        environment=command_environment,
        request_end_timestamp=request_end_timestamp,
        request_rate_limiter=bar_request_rate_limiter,
    )

    api_key_id, api_secret_key = _identity_api_credentials(command_environment)
    security_lineage_report = security_lineage_inspector(
        download_stage_result.symbol,
        api_key_id,
        api_secret_key,
        repository_root=resolved_repository_root,
        request_rate_limiter=identity_request_rate_limiter,
    )
    security_correlation_report = security_correlation_report_builder(
        security_lineage_report,
        resolved_repository_root,
    )
    security_correlation_receipt = security_correlation_publisher(
        resolved_repository_root,
        security_correlation_report,
    )
    warning_pipeline_report = warning_pipeline_report_builder(
        resolved_repository_root,
        download_stage_result.symbol,
    )
    warning_pipeline_receipt = warning_pipeline_publisher(
        resolved_repository_root,
        warning_pipeline_report,
    )
    return EndOfDayTickerPipelineResult(
        download_stage=download_stage_result,
        security_lineage_report=security_lineage_report,
        security_correlation_receipt=security_correlation_receipt,
        warning_pipeline_receipt=warning_pipeline_receipt,
    )

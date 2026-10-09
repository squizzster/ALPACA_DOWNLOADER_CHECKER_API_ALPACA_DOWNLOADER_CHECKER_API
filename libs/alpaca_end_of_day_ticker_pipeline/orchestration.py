import os
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Mapping, Protocol

from libs.alpaca_end_of_day_security_correlation import (
    SecurityCorrelationError,
    build_security_correlation_report,
    publish_security_correlation_artifacts,
    publish_security_master_online_status,
    publish_security_master_update,
)
from libs.alpaca_end_of_day_warning_pipeline import (
    build_warning_pipeline_report,
    publish_warning_pipeline,
)
from libs.alpaca_security_lineage import (
    AlpacaIdentityClient,
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
from .online_status import (
    SecurityOnlineEvaluation,
    correlated_master_ids,
    evaluate_security_online_status,
)


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


class AuthoritativeSymbolResolver(Protocol):
    def __call__(
        self,
        submitted_symbol: str,
        api_key_id: str,
        api_secret_key: str,
        *,
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


class SecurityOnlineStatusEvaluator(Protocol):
    def __call__(
        self,
        repository_root: Path,
        master_ids: list[str],
    ) -> SecurityOnlineEvaluation: ...


class SecurityMasterPublisher(Protocol):
    def __call__(
        self,
        repository_root: Path,
        correlation_report: dict[str, Any],
        correlation_receipt: dict[str, Any],
        *,
        online_by_master_id: Mapping[str, bool] | None = None,
    ) -> dict[str, Any]: ...


class SecurityMasterOnlineStatusPublisher(Protocol):
    def __call__(
        self,
        repository_root: Path,
        online_by_master_id: Mapping[str, bool],
        *,
        availability_status_by_master_id: Mapping[str, str] | None = None,
        expected_security_object_sha256_by_master_id: Mapping[str, str],
        expected_source_object_sha256_by_symbol: Mapping[str, str],
    ) -> dict[str, Any]: ...


def _security_master_summary(master_receipt: dict[str, Any]) -> dict[str, Any]:
    return {
        "publication_status": master_receipt["publication_status"],
        "security_master_version": master_receipt["security_master_version"],
        "security_master_generation_sha256": master_receipt["security_master_generation_sha256"],
        "master_id_count": master_receipt["master_id_count"],
        "output_sha256": master_receipt["output"]["sha256"],
    }


def _identity_api_credentials(environment: Mapping[str, str]) -> tuple[str, str]:
    api_key_id = environment.get("ALPACA_API_KEY_ID")
    api_secret_key = environment.get("ALPACA_API_SECRET_KEY")
    if not api_key_id or not api_secret_key:
        raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")
    return api_key_id, api_secret_key


def resolve_authoritative_alpaca_symbol(
    submitted_symbol: str,
    api_key_id: str,
    api_secret_key: str,
    *,
    request_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
) -> dict[str, Any]:
    """Resolve an accepted Alpaca alias to the exact asset symbol returned by Alpaca."""
    client = AlpacaIdentityClient(
        api_key_id,
        api_secret_key,
        rate_limiter=request_rate_limiter,
    )
    lookup = client.get_asset_lookup(submitted_symbol)
    asset = lookup.get("asset")
    if asset is None:
        return {
            "submitted_symbol": submitted_symbol,
            "authoritative_symbol": submitted_symbol,
            "changed": False,
            "basis": "alpaca_asset_lookup_not_found",
            "http_status": lookup.get("http_status"),
            "asset": None,
        }
    if not isinstance(asset, dict):
        raise RuntimeError("Alpaca asset lookup returned a non-object asset")
    authoritative_symbol = asset.get("symbol")
    asset_id = asset.get("id")
    asset_class = asset.get("class")
    if (
        not isinstance(authoritative_symbol, str)
        or re.fullmatch(r"[A-Z][A-Z0-9.-]*", authoritative_symbol) is None
        or not isinstance(asset_id, str)
        or not asset_id
        or asset_class != "us_equity"
    ):
        raise RuntimeError(
            f"Alpaca asset lookup for {submitted_symbol!r} returned an invalid US-equity identity"
        )
    compact_asset = {
        field_name: asset.get(field_name)
        for field_name in ("id", "class", "exchange", "symbol", "name", "status", "tradable")
    }
    return {
        "submitted_symbol": submitted_symbol,
        "authoritative_symbol": authoritative_symbol,
        "changed": authoritative_symbol != submitted_symbol,
        "basis": "alpaca_asset_lookup",
        "http_status": lookup.get("http_status"),
        "asset": compact_asset,
    }


def run_end_of_day_ticker_pipeline(
    download_plan: EndOfDayDownloadPlan,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    request_end_timestamp: str | None = None,
    bar_request_rate_limiter: AlpacaRequestRateLimiter | None = None,
    identity_request_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
    authoritative_symbol_resolver: AuthoritativeSymbolResolver = resolve_authoritative_alpaca_symbol,
    download_and_validation_stage_runner: DownloadAndValidationStageRunner = run_end_of_day_download_stage,
    security_lineage_inspector: SecurityLineageInspector = inspect_alpaca_security_lineage,
    security_correlation_report_builder: SecurityCorrelationReportBuilder = (build_security_correlation_report),
    security_correlation_publisher: SecurityCorrelationPublisher = (publish_security_correlation_artifacts),
    warning_pipeline_report_builder: WarningPipelineReportBuilder = (build_warning_pipeline_report),
    warning_pipeline_publisher: WarningPipelinePublisher = publish_warning_pipeline,
    security_online_status_evaluator: SecurityOnlineStatusEvaluator = evaluate_security_online_status,
    security_master_publisher: SecurityMasterPublisher = publish_security_master_update,
    security_master_online_status_publisher: SecurityMasterOnlineStatusPublisher = (
        publish_security_master_online_status
    ),
) -> EndOfDayTickerPipelineResult:
    resolved_repository_root = Path.cwd() if repository_root is None else repository_root
    command_environment = os.environ if environment is None else environment

    api_key_id, api_secret_key = _identity_api_credentials(command_environment)
    symbol_resolution = authoritative_symbol_resolver(
        download_plan.symbol,
        api_key_id,
        api_secret_key,
        request_rate_limiter=identity_request_rate_limiter,
    )
    authoritative_symbol = symbol_resolution.get("authoritative_symbol")
    if not isinstance(authoritative_symbol, str):
        raise RuntimeError("authoritative symbol resolver returned no authoritative symbol")
    authoritative_download_plan = replace(download_plan, symbol=authoritative_symbol)

    download_stage_result = download_and_validation_stage_runner(
        authoritative_download_plan,
        repository_root=resolved_repository_root,
        environment=command_environment,
        request_end_timestamp=request_end_timestamp,
        request_rate_limiter=bar_request_rate_limiter,
    )

    security_lineage_report = security_lineage_inspector(
        download_stage_result.symbol,
        api_key_id,
        api_secret_key,
        repository_root=resolved_repository_root,
        request_rate_limiter=identity_request_rate_limiter,
    )
    security_lineage_report = {
        **security_lineage_report,
        "symbol_resolution": symbol_resolution,
    }
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
    security_master_receipt = security_master_publisher(
        resolved_repository_root,
        security_correlation_report,
        security_correlation_receipt,
        online_by_master_id=None,
    )
    master_ids = correlated_master_ids(security_correlation_report)
    for attempt in range(3):
        online_evaluation = security_online_status_evaluator(
            resolved_repository_root,
            master_ids,
        )
        try:
            security_master_receipt = security_master_online_status_publisher(
                resolved_repository_root,
                online_evaluation.online_by_master_id,
                availability_status_by_master_id=(online_evaluation.availability_status_by_master_id),
                expected_security_object_sha256_by_master_id=(
                    online_evaluation.security_object_sha256_by_master_id
                ),
                expected_source_object_sha256_by_symbol=(online_evaluation.source_object_sha256_by_symbol),
            )
        except SecurityCorrelationError as error:
            if "changed during online evaluation" not in str(error) or attempt == 2:
                raise
        else:
            break
    security_correlation_receipt = {
        **security_correlation_receipt,
        "security_master": _security_master_summary(security_master_receipt),
    }
    return EndOfDayTickerPipelineResult(
        download_stage=download_stage_result,
        security_lineage_report=security_lineage_report,
        security_correlation_receipt=security_correlation_receipt,
        warning_pipeline_receipt=warning_pipeline_receipt,
    )

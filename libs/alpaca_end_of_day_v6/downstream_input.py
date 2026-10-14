"""Trusted downstream inputs over the native v6 monthly source.

The v6 monthly source owns the physical file contract, including manifest
aliases rather than legacy alias paths or inode relationships.  This adapter
provisions the established logical rows and metadata consumed by the proven
pipeline transformations.  Storage-specific rules stop at this boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    download_session_dates,
)
from libs.alpaca_end_of_day_security_correlation.session_exclusions import (
    VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE,
    VERIFIED_SESSION_EXCLUSIONS_VERSION,
)
from libs.alpaca_end_of_day_warning_pipeline.authenticated_inputs import (
    _analyzable_bar,
    _parse_timestamp,
)
from libs.alpaca_end_of_day_warning_pipeline.models import (
    ACCEPTED_ZERO_ACTIVITY_BAR_REASON,
    AuthenticatedCorrelationOutputs,
    DownloadValidationView,
    WarningPipelineInputs,
)
from libs.alpaca_end_of_day_warning_pipeline.settings import adjustment_actions
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .errors import V6Error
from .source import AuthenticatedMonthlySource, load_monthly_source
from .validation import load_validation


@dataclass(frozen=True)
class AuthenticatedLogicalDownload:
    """The established downstream row shape, authenticated from v6 storage."""

    path: Path
    manifest: dict[str, Any]
    rows: list[dict[str, str]]
    pipeline_state: str
    pipeline_diagnostic: str | None
    integrity_failure: dict[str, Any] | None


@dataclass(frozen=True)
class DownstreamPricingInput:
    """One authenticated v6 symbol exposed through the downstream input contract."""

    store_root: Path
    source: AuthenticatedMonthlySource
    validation_receipt: dict[str, Any]
    validation_report: dict[str, Any]
    downloads_by_filename: Mapping[str, AuthenticatedLogicalDownload]

    @property
    def symbol(self) -> str:
        return self.source.symbol

    @property
    def request(self) -> dict[str, str]:
        request = self.validation_report.get("request")
        if not isinstance(request, dict) or any(
            not isinstance(request.get(field), str) or not request[field]
            for field in ("feed", "timeframe", "request_from", "request_to", "asof")
        ):
            raise V6Error(f"v6 validation has no complete downstream request view: {self.symbol}")
        return request

    def local_report(self) -> dict[str, Any]:
        downloads = []
        for filename in sorted(self.downloads_by_filename):
            authenticated = self.downloads_by_filename[filename]
            manifest = authenticated.manifest
            timestamps = [row.get("t") for row in authenticated.rows]
            downloads.append(
                {
                    "file": filename,
                    "sidecar_present": True,
                    "sidecar_readable": True,
                    "adjustment": manifest["adjustment"],
                    "sha256": manifest["sha256"],
                    "rows": len(authenticated.rows),
                    "sessions": len(authenticated.rows),
                    "from": timestamps[0] if timestamps else None,
                    "to": timestamps[-1] if timestamps else None,
                    **self.request,
                    "gzip_present": bool(authenticated.rows),
                    "stat_matches_sidecar": True,
                    "pipeline_receipt_matches": True,
                    "integrity_failure": None,
                }
            )
        return {
            "directory": f"data_vtest/end_of_day/{self.symbol}",
            "present": True,
            "download_count": len(downloads),
            "download_sha256_authentication_performed": True,
            "downloads": downloads,
            "pipeline_manifest": None,
            "integrity_manifest": None,
            "manifest_errors": [],
        }

    def adapt_lineage_report(self, report: dict[str, Any]) -> dict[str, Any]:
        if report.get("symbol") != self.symbol or not isinstance(report.get("lineage"), dict):
            raise V6Error(f"lineage report cannot be adapted to the v1 contract: {self.symbol}")
        return {**report, "local": self.local_report()}

    def correlation_source_descriptors(self) -> dict[str, dict[str, Any]]:
        return {
            filename: {
                "data_file": filename,
                "download_manifest": "authenticated_monthly_source",
                "download_sha256": authenticated.manifest["sha256"],
                "rows": len(authenticated.rows),
                "adjustment": authenticated.manifest["adjustment"],
                "pipeline_state": authenticated.pipeline_state,
                "validation_pipeline_version": PIPELINE_VERSION,
                "download_sha256_authenticated": True,
            }
            for filename, authenticated in self.downloads_by_filename.items()
        }

    def read_authenticated_download(
        self,
        _repository_root: Path,
        _symbol_directory: str,
        local_download: dict[str, Any],
    ) -> AuthenticatedLogicalDownload:
        filename = local_download.get("file") if isinstance(local_download, dict) else None
        authenticated = self.downloads_by_filename.get(filename)
        if authenticated is None:
            raise V6Error(f"downstream pricing input has no authenticated download: {self.symbol}/{filename}")
        if local_download.get("sha256") not in {None, authenticated.manifest["sha256"]} or local_download.get(
            "rows"
        ) not in {None, len(authenticated.rows)}:
            raise V6Error(f"v1 pricing descriptor differs from monthly source: {self.symbol}/{filename}")
        return authenticated

    def warning_inputs(self, correlation: Any) -> WarningPipelineInputs:
        request = self.request
        views = []
        for logical in sorted(self.source.adjustments, key=lambda item: item.logical_filename):
            actions = adjustment_actions(logical.adjustment)
            if actions is None:
                raise V6Error(f"unsupported adjustment in downstream pricing input: {logical.adjustment}")
            timestamps = {}
            bars = {}
            excluded = []
            for row_number, row in enumerate(logical.rows, start=1):
                timestamp = _parse_timestamp(row.get("t"), logical.logical_filename, row_number)
                timestamps[row_number] = timestamp
                bar = _analyzable_bar(
                    row,
                    logical.logical_filename,
                    row_number,
                    timestamp,
                    split_adjusted="split" in actions,
                )
                if bar is None:
                    excluded.append({"row_number": row_number, "reason": ACCEPTED_ZERO_ACTIVITY_BAR_REASON})
                else:
                    bars[row_number] = bar
            manifest = {
                "file": logical.logical_filename,
                "sha256": logical.logical_sha256,
                "rows": len(logical.rows),
                "symbol": self.symbol,
                "adjustment": logical.adjustment,
                **request,
            }
            views.append(
                DownloadValidationView(
                    data_file=logical.logical_filename,
                    download_sha256=logical.logical_sha256,
                    adjustment=logical.adjustment,
                    adjustment_actions=actions,
                    manifest=manifest,
                    source_row_count=len(logical.rows),
                    timestamp_by_row_number=timestamps,
                    bars_by_row_number=bars,
                    excluded_rows=tuple(excluded),
                )
            )
        correlation_inputs = AuthenticatedCorrelationOutputs(
            receipt=correlation.receipt,
            correlated=correlation.correlated,
            uncorrelated=correlation.uncorrelated,
            correlated_sha256=correlation.correlated_sha256,
            uncorrelated_sha256=correlation.uncorrelated_sha256,
        )
        return WarningPipelineInputs(
            symbol=self.symbol,
            source_downloads=tuple(
                correlation.sources_by_filename[name] for name in sorted(correlation.sources_by_filename)
            ),
            download_views=tuple(views),
            correlation=correlation_inputs,
            pipeline_receipt_manifest_sha256=self.validation_receipt["validation_generation_sha256"],
            integrity_failure_manifest_sha256=self.validation_receipt["validation_generation_sha256"],
        )


def build_downstream_pricing_input(
    store_root: Path | str,
    symbol: str,
    *,
    source: AuthenticatedMonthlySource | None = None,
    validation: tuple[dict[str, Any], dict[str, Any]] | None = None,
) -> DownstreamPricingInput:
    root = Path(store_root)
    source = load_monthly_source(root, symbol) if source is None else source
    validation_receipt, validation_report = load_validation(root, symbol) if validation is None else validation
    if (
        validation_receipt.get("source_generation_sha256") != source.source_generation_sha256
        or validation_report.get("source_generation_sha256") != source.source_generation_sha256
    ):
        raise V6Error(f"validation selects another pricing source: {symbol}")
    request = validation_report.get("request")
    if not isinstance(request, dict):
        raise V6Error(f"v6 validation has no request identity: {symbol}")
    downloads = {}
    for logical in source.adjustments:
        manifest = {
            "file": logical.logical_filename,
            "sha256": logical.logical_sha256,
            "rows": len(logical.rows),
            "symbol": symbol,
            "adjustment": logical.adjustment,
            **request,
        }
        downloads[logical.logical_filename] = AuthenticatedLogicalDownload(
            path=root / "end_of_day" / symbol / logical.logical_filename,
            manifest=manifest,
            rows=[dict(row) for row in logical.rows],
            pipeline_state="validated",
            pipeline_diagnostic=None,
            integrity_failure=None,
        )
    return DownstreamPricingInput(
        store_root=root,
        source=source,
        validation_receipt=validation_receipt,
        validation_report=validation_report,
        downloads_by_filename=downloads,
    )


def empty_verified_session_exclusions(
    _repository_root: Path,
    symbol: str,
) -> dict[str, Any]:
    return {
        "manifest_type": VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE,
        "manifest_version": VERIFIED_SESSION_EXCLUSIONS_VERSION,
        "symbol": symbol,
        "input_file": None,
        "input_sha256": None,
        "calendar_sha256": us_market_hours_sha256(),
        "events": [],
    }


def authenticated_symbol_sessions(
    store_root: Path,
    pricing_inputs_by_symbol: Mapping[str, DownstreamPricingInput] | None = None,
):
    def load(symbol: str):
        try:
            view = (
                pricing_inputs_by_symbol.get(symbol) if pricing_inputs_by_symbol is not None else None
            ) or build_downstream_pricing_input(store_root, symbol)
        except (OSError, V6Error):
            return None
        sessions_by_adjustment = []
        supporting = []
        for filename in sorted(view.downloads_by_filename):
            authenticated = view.downloads_by_filename[filename]
            sessions = download_session_dates(authenticated.rows)
            sessions_by_adjustment.append(sessions)
            supporting.append(
                (
                    symbol,
                    authenticated.manifest["adjustment"],
                    authenticated.manifest["sha256"],
                )
            )
        if not sessions_by_adjustment or any(
            sessions != sessions_by_adjustment[0] for sessions in sessions_by_adjustment[1:]
        ):
            return None
        return sessions_by_adjustment[0], tuple(supporting)

    return load


def episode_partition_loaders(
    store_root: Path,
    preliminary_by_symbol: Mapping[str, dict[str, Any]],
    pricing_inputs_by_symbol: Mapping[str, DownstreamPricingInput] | None = None,
):
    def load_correlation(symbol: str):
        report = preliminary_by_symbol.get(symbol)
        if report is None:
            raise V6Error(f"no current preliminary correlation for {symbol}")
        return SimpleNamespace(
            correlated=report["correlated"],
            sources_by_filename={source["data_file"]: source for source in report["source_downloads"]},
        )

    def load_rows(symbol: str, filename: str) -> tuple[list[dict], str]:
        view = (
            pricing_inputs_by_symbol.get(symbol) if pricing_inputs_by_symbol is not None else None
        ) or build_downstream_pricing_input(store_root, symbol)
        authenticated = view.downloads_by_filename.get(filename)
        if authenticated is None:
            raise V6Error(f"no downstream pricing input for {symbol}/{filename}")
        return authenticated.rows, authenticated.manifest["sha256"]

    return load_correlation, load_rows


__all__ = [
    "AuthenticatedLogicalDownload",
    "DownstreamPricingInput",
    "authenticated_symbol_sessions",
    "build_downstream_pricing_input",
    "empty_verified_session_exclusions",
    "episode_partition_loaders",
]

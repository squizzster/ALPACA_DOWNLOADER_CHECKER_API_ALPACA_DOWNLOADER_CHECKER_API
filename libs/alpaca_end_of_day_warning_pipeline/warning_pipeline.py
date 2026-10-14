from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION
from libs.alpaca_end_of_day_security_correlation import CORRELATION_VERSION
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .authenticated_inputs import load_warning_pipeline_inputs
from .cross_adjustment_warnings import build_cross_adjustment_warnings
from .models import WarningPipelineInputs
from .security_segment_statistics import validate_security_segment_statistics
from .settings import DEFAULT_CONFIGURATION, WarningPipelineConfiguration
from .warning_pipeline_version import (
    WARNING_PIPELINE_VERSION,
    validate_warning_pipeline_change_log,
)


def build_warning_pipeline_report(
    repository_root: Path,
    symbol: str,
    *,
    configuration: WarningPipelineConfiguration = DEFAULT_CONFIGURATION,
    inputs: WarningPipelineInputs | None = None,
) -> dict[str, Any]:
    """Build warning overlays without altering any upstream artifact."""
    validate_warning_pipeline_change_log()
    inputs = load_warning_pipeline_inputs(repository_root, symbol) if inputs is None else inputs
    if inputs.symbol != symbol:
        raise ValueError("warning-pipeline inputs do not match the requested symbol")
    cross_adjustment = build_cross_adjustment_warnings(inputs.download_views, configuration)
    security_segment_statistics = validate_security_segment_statistics(inputs, configuration)
    correlation_receipt = inputs.correlation.receipt
    return {
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "symbol": symbol,
        "boundary": {
            "owner": "alpaca_end_of_day_warning_pipeline",
            "input_boundaries": [
                "authenticated_frozen_downloads",
                "current_integrity_pipeline_receipts_and_failures",
                "authenticated_exhaustive_security_correlation_partition",
            ],
            "output_boundaries": [
                "cross_adjustment_warning_overlay",
                "internal_security_statistical_warning_overlay",
            ],
            "severity_domain": ["warning", "information"],
            "integrity_error_owner": "alpaca_end_of_day_pipeline/integrity_failure.manifest.json",
            "input_authentication_failures_abort": True,
        },
        "configuration": configuration.as_report_object(),
        "validation_pipeline_version": PIPELINE_VERSION,
        "calendar_sha256": us_market_hours_sha256(),
        "pipeline_receipt_manifest_sha256": inputs.pipeline_receipt_manifest_sha256,
        "integrity_failure_manifest_sha256": inputs.integrity_failure_manifest_sha256,
        "source_downloads": list(inputs.source_downloads),
        "correlation": {
            "correlation_version": CORRELATION_VERSION,
            "correlation_generation_sha256": correlation_receipt["correlation_generation_sha256"],
            "correlated_output_sha256": inputs.correlation.correlated_sha256,
            "uncorrelated_output_sha256": inputs.correlation.uncorrelated_sha256,
            "row_partition_complete": correlation_receipt["partition"]["row_partition_complete"],
        },
        "cross_adjustment": cross_adjustment,
        "security_segment_statistics": security_segment_statistics,
    }

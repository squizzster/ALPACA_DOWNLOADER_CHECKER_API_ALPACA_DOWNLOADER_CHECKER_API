import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_pipeline import (
    PIPELINE_VERSION,
    integrity_failure_manifest_path,
    pipeline_receipt_manifest_path,
)
from libs.alpaca_end_of_day_security_correlation import AuthenticatedSecurityCorrelation
from libs.common.file_integrity import calculate_file_sha256
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .artifact_paths import (
    cross_adjustment_warnings_path,
    security_segment_statistical_warnings_path,
    warning_pipeline_manifest_path,
)
from .errors import WarningPipelineError
from .publication import warning_pipeline_generation_sha256
from .settings import MANIFEST_VERSION
from .warning_pipeline_version import (
    WARNING_PIPELINE_VERSION,
    validate_warning_pipeline_change_log,
)


@dataclass(frozen=True)
class AuthenticatedWarningPipelineOutputs:
    receipt: dict[str, Any]
    cross_adjustment: dict[str, Any]
    security_segment_statistics: dict[str, Any]
    cross_adjustment_sha256: str
    security_segment_statistics_sha256: str


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise WarningPipelineError(f"could not read {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise WarningPipelineError(f"{description} is not a JSON object: {path}")
    return document


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _authenticate_output(
    path: Path,
    output_receipt: object,
    *,
    symbol: str,
    generation_sha256: str,
    expected_manifest_type: str,
) -> tuple[dict[str, Any], str]:
    if not isinstance(output_receipt, dict) or output_receipt.get("file") != path.name:
        raise WarningPipelineError(f"warning-pipeline receipt does not bind {path}")
    try:
        output_stat = path.stat()
    except OSError as error:
        raise WarningPipelineError(f"warning-pipeline output is unavailable: {path}: {error}") from error
    if output_stat.st_size != output_receipt.get("bytes") or output_stat.st_mtime_ns != output_receipt.get(
        "mtime_ns"
    ):
        raise WarningPipelineError(f"warning-pipeline output stat does not match its receipt: {path}")
    try:
        observed_sha256 = calculate_file_sha256(path)
    except OSError as error:
        raise WarningPipelineError(f"could not authenticate warning-pipeline output {path}: {error}") from error
    if observed_sha256 != output_receipt.get("sha256"):
        raise WarningPipelineError(f"warning-pipeline output SHA-256 does not match its receipt: {path}")
    document = _read_json_object(path, "warning-pipeline output")
    try:
        verified_sha256 = calculate_file_sha256(path)
        final_stat = path.stat()
    except OSError as error:
        raise WarningPipelineError(f"could not verify warning-pipeline output {path}: {error}") from error
    if verified_sha256 != observed_sha256 or (
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) != (output_stat.st_size, output_stat.st_mtime_ns):
        raise WarningPipelineError(f"warning-pipeline output changed while it was read: {path}")
    expected_identity = {
        "manifest_type": expected_manifest_type,
        "manifest_version": MANIFEST_VERSION,
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "symbol": symbol,
        "warning_pipeline_generation_sha256": generation_sha256,
    }
    for field_name, expected_value in expected_identity.items():
        if document.get(field_name) != expected_value:
            raise WarningPipelineError(
                f"warning-pipeline output {path} has unexpected {field_name}: {document.get(field_name)!r}"
            )
    return document, observed_sha256


def _validate_report_counts(report: object, description: str) -> dict[str, Any]:
    if not isinstance(report, dict):
        raise WarningPipelineError(f"{description} warning report is not an object")
    findings = report.get("findings")
    if not isinstance(findings, list):
        raise WarningPipelineError(f"{description} warning report has no findings list")
    if any(
        not isinstance(finding, dict)
        or finding.get("warning_pipeline_version") != WARNING_PIPELINE_VERSION
        or finding.get("severity") not in {"warning", "information"}
        for finding in findings
    ):
        raise WarningPipelineError(f"{description} warning report has a malformed finding")
    warning_count = sum(
        1 for finding in findings if isinstance(finding, dict) and finding.get("severity") == "warning"
    )
    information_count = sum(
        1 for finding in findings if isinstance(finding, dict) and finding.get("severity") == "information"
    )
    recorded_warning_count = report.get("warning_count")
    recorded_information_count = report.get("information_count")
    if (
        type(recorded_warning_count) is not int
        or type(recorded_information_count) is not int
        or warning_count > recorded_warning_count
        or information_count > recorded_information_count
    ):
        raise WarningPipelineError(f"{description} warning report severity counts are inconsistent")
    omitted_count = report.get("omitted_finding_count")
    if type(omitted_count) is not int or omitted_count < 0:
        raise WarningPipelineError(f"{description} warning report has an invalid omitted count")
    if (
        report.get("total_finding_count") != len(findings) + omitted_count
        or report.get("total_finding_count") != recorded_warning_count + recorded_information_count
    ):
        raise WarningPipelineError(f"{description} warning report total count is inconsistent")
    return report


def _summary(report: dict[str, Any]) -> dict[str, int]:
    return {
        "warning_count": report["warning_count"],
        "information_count": report["information_count"],
        "total_finding_count": report["total_finding_count"],
        "omitted_finding_count": report["omitted_finding_count"],
    }


def authenticate_warning_pipeline_outputs(
    repository_root: Path | str,
    symbol: str,
    correlation: AuthenticatedSecurityCorrelation,
) -> AuthenticatedWarningPipelineOutputs:
    """Authenticate current warning overlays and their upstream bindings without changing them."""
    validate_warning_pipeline_change_log()
    repository_root = Path(repository_root)
    receipt_path = warning_pipeline_manifest_path(repository_root, symbol)
    receipt = _read_json_object(receipt_path, "warning-pipeline receipt")
    expected_receipt_identity = {
        "manifest_type": "end_of_day_market_data_warning_overlays",
        "manifest_version": MANIFEST_VERSION,
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "validation_pipeline_version": PIPELINE_VERSION,
        "symbol": symbol,
    }
    for field_name, expected_value in expected_receipt_identity.items():
        if receipt.get(field_name) != expected_value:
            raise WarningPipelineError(
                f"warning-pipeline receipt {receipt_path} has unexpected {field_name}: "
                f"{receipt.get(field_name)!r}"
            )
    generation_sha256 = receipt.get("warning_pipeline_generation_sha256")
    if not _is_sha256(generation_sha256):
        raise WarningPipelineError("warning-pipeline receipt has an invalid generation digest")
    if receipt.get("calendar_sha256") != us_market_hours_sha256():
        raise WarningPipelineError("warning-pipeline receipt does not bind the current market-session calendar")

    expected_correlation = {
        "correlation_version": correlation.receipt.get("correlation_version"),
        "correlation_generation_sha256": correlation.receipt.get("correlation_generation_sha256"),
        "correlated_output_sha256": correlation.correlated_sha256,
        "uncorrelated_output_sha256": correlation.uncorrelated_sha256,
        "row_partition_complete": True,
    }
    if receipt.get("correlation") != expected_correlation:
        raise WarningPipelineError("warning-pipeline receipt does not bind the current security correlation")
    if receipt.get("source_downloads") != correlation.receipt.get("source_downloads"):
        raise WarningPipelineError("warning-pipeline receipt source downloads do not match correlation")

    symbol_directory = repository_root / "data" / "end_of_day" / symbol
    placeholder_path = symbol_directory / "placeholder.csv.gz"
    upstream_manifests = (
        (
            pipeline_receipt_manifest_path(placeholder_path),
            "pipeline_receipt_manifest_sha256",
        ),
        (
            integrity_failure_manifest_path(placeholder_path),
            "integrity_failure_manifest_sha256",
        ),
    )
    for manifest_path, digest_field in upstream_manifests:
        try:
            observed_sha256 = calculate_file_sha256(manifest_path)
        except OSError as error:
            raise WarningPipelineError(
                f"warning-pipeline input is unavailable: {manifest_path}: {error}"
            ) from error
        if observed_sha256 != receipt.get(digest_field):
            raise WarningPipelineError(
                f"warning-pipeline receipt does not bind the current {manifest_path.name}"
            )

    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise WarningPipelineError("warning-pipeline receipt has no outputs object")
    cross_adjustment, cross_adjustment_sha256 = _authenticate_output(
        cross_adjustment_warnings_path(repository_root, symbol),
        outputs.get("cross_adjustment"),
        symbol=symbol,
        generation_sha256=generation_sha256,
        expected_manifest_type="end_of_day_cross_adjustment_warning_overlay",
    )
    statistics, statistics_sha256 = _authenticate_output(
        security_segment_statistical_warnings_path(repository_root, symbol),
        outputs.get("security_segment_statistics"),
        symbol=symbol,
        generation_sha256=generation_sha256,
        expected_manifest_type="end_of_day_internal_security_statistical_warning_overlay",
    )
    for description, document in (
        ("cross-adjustment", cross_adjustment),
        ("security-segment", statistics),
    ):
        if document.get("source_downloads") != receipt.get("source_downloads"):
            raise WarningPipelineError(f"{description} warning output has inconsistent source downloads")
        if document.get("configuration") != receipt.get("configuration"):
            raise WarningPipelineError(f"{description} warning output has inconsistent configuration")
        if (
            document.get("severity_domain") != ["warning", "information"]
            or document.get("contains_integrity_errors") is not False
            or document.get("integrity_error_manifest") != "integrity_failure.manifest.json"
        ):
            raise WarningPipelineError(f"{description} warning output has inconsistent severity ownership")
    if cross_adjustment.get("input_boundary") != "authenticated_frozen_download_adjustment_bundle":
        raise WarningPipelineError("cross-adjustment warning output has an inconsistent input boundary")
    if statistics.get("input_boundary") != "authenticated_security_correlation_partition":
        raise WarningPipelineError("security-segment warning output has an inconsistent input boundary")
    if statistics.get("correlation") != expected_correlation:
        raise WarningPipelineError("security-segment warning output has inconsistent correlation binding")

    cross_report = _validate_report_counts(cross_adjustment.get("report"), "cross-adjustment")
    statistics_report = _validate_report_counts(statistics.get("report"), "security-segment")
    cross_summary = _summary(cross_report)
    statistics_summary = _summary(statistics_report)
    expected_summary = {
        "warning_count": cross_summary["warning_count"] + statistics_summary["warning_count"],
        "information_count": (cross_summary["information_count"] + statistics_summary["information_count"]),
        "omitted_finding_count": (
            cross_summary["omitted_finding_count"] + statistics_summary["omitted_finding_count"]
        ),
        "cross_adjustment": cross_summary,
        "security_segment_statistics": statistics_summary,
    }
    if receipt.get("summary") != expected_summary:
        raise WarningPipelineError("warning-pipeline receipt summary is inconsistent")

    generation_report = {
        "warning_pipeline_version": receipt.get("warning_pipeline_version"),
        "symbol": symbol,
        "boundary": receipt.get("boundary"),
        "configuration": receipt.get("configuration"),
        "validation_pipeline_version": receipt.get("validation_pipeline_version"),
        "calendar_sha256": receipt.get("calendar_sha256"),
        "pipeline_receipt_manifest_sha256": receipt.get("pipeline_receipt_manifest_sha256"),
        "integrity_failure_manifest_sha256": receipt.get("integrity_failure_manifest_sha256"),
        "source_downloads": receipt.get("source_downloads"),
        "correlation": receipt.get("correlation"),
        "cross_adjustment": cross_report,
        "security_segment_statistics": statistics_report,
    }
    if warning_pipeline_generation_sha256(generation_report) != generation_sha256:
        raise WarningPipelineError("warning-pipeline generation digest is inconsistent")
    return AuthenticatedWarningPipelineOutputs(
        receipt=receipt,
        cross_adjustment=cross_adjustment,
        security_segment_statistics=statistics,
        cross_adjustment_sha256=cross_adjustment_sha256,
        security_segment_statistics_sha256=statistics_sha256,
    )

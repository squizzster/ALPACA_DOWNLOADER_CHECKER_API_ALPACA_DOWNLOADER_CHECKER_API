import json
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from libs.common.atomic_writes import write_json_document_atomically
from libs.common.file_integrity import calculate_file_sha256

from .artifact_paths import (
    cross_adjustment_warnings_path,
    security_segment_statistical_warnings_path,
    warning_pipeline_manifest_path,
)
from .errors import WarningPipelineError
from .settings import MANIFEST_VERSION
from .warning_pipeline_version import (
    WARNING_PIPELINE_VERSION,
    validate_warning_pipeline_change_log,
)


def canonical_json_sha256(document: object) -> str:
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def warning_pipeline_generation_sha256(report: dict[str, Any]) -> str:
    return canonical_json_sha256(
        {
            "warning_pipeline_version": WARNING_PIPELINE_VERSION,
            "symbol": report["symbol"],
            "boundary": report["boundary"],
            "configuration": report["configuration"],
            "validation_pipeline_version": report["validation_pipeline_version"],
            "calendar_sha256": report["calendar_sha256"],
            "pipeline_receipt_manifest_sha256": report["pipeline_receipt_manifest_sha256"],
            "integrity_failure_manifest_sha256": report["integrity_failure_manifest_sha256"],
            "source_downloads": report["source_downloads"],
            "correlation": report["correlation"],
            "cross_adjustment": report["cross_adjustment"],
            "security_segment_statistics": report["security_segment_statistics"],
        }
    )


def _read_optional_receipt(path: Path) -> dict[str, Any] | None:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise WarningPipelineError(f"could not read existing warning-overlay receipt {path}: {error}") from error
    if not isinstance(document, dict):
        raise WarningPipelineError(f"existing warning-overlay receipt is not an object: {path}")
    return document


def _authenticated_cached_receipt(
    receipt_path: Path,
    output_paths: dict[str, Path],
    generation_sha256: str,
) -> dict[str, Any] | None:
    receipt = _read_optional_receipt(receipt_path)
    if receipt is None or receipt.get("warning_pipeline_generation_sha256") != generation_sha256:
        return None
    if receipt.get("warning_pipeline_version") != WARNING_PIPELINE_VERSION:
        return None
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise WarningPipelineError(f"warning-overlay receipt has no outputs object: {receipt_path}")
    for output_name, output_path in output_paths.items():
        output = outputs.get(output_name)
        if not isinstance(output, dict) or output.get("file") != output_path.name:
            raise WarningPipelineError(f"warning-overlay receipt does not bind {output_path}")
        try:
            output_stat = output_path.stat()
        except OSError as error:
            raise WarningPipelineError(
                f"warning-overlay output is unavailable: {output_path}: {error}"
            ) from error
        if output_stat.st_size != output.get("bytes") or output_stat.st_mtime_ns != output.get("mtime_ns"):
            raise WarningPipelineError(f"warning-overlay output stat does not match receipt: {output_path}")
        if calculate_file_sha256(output_path) != output.get("sha256"):
            raise WarningPipelineError(f"warning-overlay output SHA-256 does not match receipt: {output_path}")
    return receipt


def _publish_output(path: Path, document: dict[str, Any]) -> dict[str, Any]:
    write_json_document_atomically(path, document, compact=False)
    published_sha256 = calculate_file_sha256(path)
    if calculate_file_sha256(path) != published_sha256:
        raise WarningPipelineError(f"published warning overlay failed SHA-256 verification: {path}")
    output_stat = path.stat()
    return {
        "file": path.name,
        "sha256": published_sha256,
        "bytes": output_stat.st_size,
        "mtime_ns": output_stat.st_mtime_ns,
        "publication_sha256_verified": True,
    }


def _warning_summary(report_section: dict[str, Any]) -> dict[str, int]:
    return {
        "warning_count": report_section["warning_count"],
        "information_count": report_section["information_count"],
        "total_finding_count": report_section["total_finding_count"],
        "omitted_finding_count": report_section["omitted_finding_count"],
    }


def publish_warning_pipeline(
    repository_root: Path,
    report: dict[str, Any],
) -> dict[str, Any]:
    """Publish warning documents first and their authenticating receipt last."""
    validate_warning_pipeline_change_log()
    symbol = report["symbol"]
    cross_path = cross_adjustment_warnings_path(repository_root, symbol)
    statistics_path = security_segment_statistical_warnings_path(repository_root, symbol)
    receipt_path = warning_pipeline_manifest_path(repository_root, symbol)
    output_paths = {
        "cross_adjustment": cross_path,
        "security_segment_statistics": statistics_path,
    }
    generation_sha256 = warning_pipeline_generation_sha256(report)
    cached_receipt = _authenticated_cached_receipt(receipt_path, output_paths, generation_sha256)
    if cached_receipt is not None:
        return {**cached_receipt, "publication_status": "cached"}

    publication_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    common_document = {
        "manifest_version": MANIFEST_VERSION,
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "symbol": symbol,
        "warning_pipeline_generation_sha256": generation_sha256,
        "created_at": publication_timestamp,
        "severity_domain": ["warning", "information"],
        "contains_integrity_errors": False,
        "integrity_error_manifest": "integrity_failure.manifest.json",
        "configuration": report["configuration"],
        "source_downloads": report["source_downloads"],
    }
    cross_document = {
        **common_document,
        "manifest_type": "end_of_day_cross_adjustment_warning_overlay",
        "input_boundary": "authenticated_frozen_download_adjustment_bundle",
        "report": report["cross_adjustment"],
    }
    statistics_document = {
        **common_document,
        "manifest_type": "end_of_day_security_uuid_statistical_warning_overlay",
        "input_boundary": "authenticated_security_correlation_partition",
        "correlation": report["correlation"],
        "report": report["security_segment_statistics"],
    }
    cross_path.parent.mkdir(parents=True, exist_ok=True)
    cross_output = _publish_output(cross_path, cross_document)
    statistics_output = _publish_output(statistics_path, statistics_document)
    cross_summary = _warning_summary(report["cross_adjustment"])
    statistics_summary = _warning_summary(report["security_segment_statistics"])
    receipt = {
        "manifest_type": "end_of_day_market_data_warning_overlays",
        "manifest_version": MANIFEST_VERSION,
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "symbol": symbol,
        "warning_pipeline_generation_sha256": generation_sha256,
        "created_at": publication_timestamp,
        "boundary": report["boundary"],
        "configuration": report["configuration"],
        "validation_pipeline_version": report["validation_pipeline_version"],
        "calendar_sha256": report["calendar_sha256"],
        "pipeline_receipt_manifest_sha256": report["pipeline_receipt_manifest_sha256"],
        "integrity_failure_manifest_sha256": report["integrity_failure_manifest_sha256"],
        "source_downloads": report["source_downloads"],
        "correlation": report["correlation"],
        "summary": {
            "warning_count": cross_summary["warning_count"] + statistics_summary["warning_count"],
            "information_count": (cross_summary["information_count"] + statistics_summary["information_count"]),
            "omitted_finding_count": (
                cross_summary["omitted_finding_count"] + statistics_summary["omitted_finding_count"]
            ),
            "cross_adjustment": cross_summary,
            "security_segment_statistics": statistics_summary,
        },
        "outputs": {
            "cross_adjustment": cross_output,
            "security_segment_statistics": statistics_output,
        },
    }
    write_json_document_atomically(receipt_path, receipt, compact=False)
    return {**receipt, "publication_status": "published"}

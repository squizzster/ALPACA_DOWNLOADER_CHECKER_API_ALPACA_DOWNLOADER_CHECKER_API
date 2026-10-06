import json
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from libs.common.atomic_writes import write_json_document_atomically
from libs.common.file_integrity import calculate_file_sha256

from .artifact_paths import (
    correlated_pipeline_path,
    security_correlation_manifest_path,
    uncorrelated_pipeline_path,
)
from .correlation_version import CORRELATION_VERSION, validate_correlation_change_log
from .errors import SecurityCorrelationError
from .settings import MANIFEST_VERSION


def canonical_json_sha256(document: object) -> str:
    serialized_document = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return sha256(serialized_document).hexdigest()


def lineage_evidence_sha256(lineage_report: dict[str, Any]) -> str:
    semantic_lineage_report = deepcopy(lineage_report)
    semantic_lineage_report.pop("generated_at", None)
    lineage = semantic_lineage_report.get("lineage")
    if isinstance(lineage, dict):
        lineage.pop("api_request_count", None)
    return canonical_json_sha256(semantic_lineage_report)


def correlation_generation_sha256(correlation_report: dict[str, Any]) -> str:
    generation_identity = {
        "correlation_version": CORRELATION_VERSION,
        "symbol": correlation_report["symbol"],
        "lineage_evidence_sha256": correlation_report["lineage_evidence_sha256"],
        "source_downloads": correlation_report["source_downloads"],
        "ownership_interval_rule_order": correlation_report["ownership_interval_rule_order"],
        "row_correlation_rule_order": correlation_report["row_correlation_rule_order"],
        "uuid_lineage_edges": correlation_report["uuid_lineage_edges"],
        "ticker_ownership_intervals": correlation_report["ticker_ownership_intervals"],
        "evidence_issues": correlation_report["evidence_issues"],
        "files": correlation_report["files"],
        "correlated": correlation_report["correlated"],
        "uncorrelated": correlation_report["uncorrelated"],
    }
    return canonical_json_sha256(generation_identity)


def _read_optional_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(
            f"could not read existing correlation manifest {path}: {error}"
        ) from error
    if not isinstance(document, dict):
        raise SecurityCorrelationError(f"existing correlation manifest is not an object: {path}")
    return document


def _authenticated_cached_receipt(
    receipt_path: Path,
    correlated_path: Path,
    uncorrelated_path: Path,
    generation_sha256: str,
) -> dict[str, Any] | None:
    receipt = _read_optional_manifest(receipt_path)
    if receipt is None or receipt.get("correlation_generation_sha256") != generation_sha256:
        return None
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise SecurityCorrelationError(f"existing correlation manifest has no outputs object: {receipt_path}")

    for branch_name, branch_path in (
        ("correlated", correlated_path),
        ("uncorrelated", uncorrelated_path),
    ):
        output = outputs.get(branch_name)
        if not isinstance(output, dict):
            raise SecurityCorrelationError(
                f"existing correlation manifest has no {branch_name!r} output: {receipt_path}"
            )
        try:
            branch_stat = branch_path.stat()
        except OSError as error:
            raise SecurityCorrelationError(
                f"existing correlation output is unavailable: {branch_path}: {error}"
            ) from error
        if branch_stat.st_size != output.get("bytes") or branch_stat.st_mtime_ns != output.get("mtime_ns"):
            raise SecurityCorrelationError(
                f"existing correlation output stat does not match its receipt: {branch_path}"
            )
        observed_sha256 = calculate_file_sha256(branch_path)
        if observed_sha256 != output.get("sha256"):
            raise SecurityCorrelationError(
                f"existing correlation output SHA-256 does not match its receipt: {branch_path}"
            )
    return receipt


def _publish_branch(path: Path, document: dict[str, Any]) -> dict[str, Any]:
    write_json_document_atomically(path, document, compact=False)
    published_sha256 = calculate_file_sha256(path)
    verified_sha256 = calculate_file_sha256(path)
    if verified_sha256 != published_sha256:
        raise SecurityCorrelationError(f"published correlation output failed SHA-256 verification: {path}")
    published_stat = path.stat()
    return {
        "file": path.name,
        "sha256": published_sha256,
        "bytes": published_stat.st_size,
        "mtime_ns": published_stat.st_mtime_ns,
        "publication_sha256_verified": True,
    }


def publish_security_correlation(
    repository_root: Path,
    correlation_report: dict[str, Any],
) -> dict[str, Any]:
    validate_correlation_change_log()
    symbol = correlation_report["symbol"]
    correlated_path = correlated_pipeline_path(repository_root, symbol)
    uncorrelated_path = uncorrelated_pipeline_path(repository_root, symbol)
    receipt_path = security_correlation_manifest_path(repository_root, symbol)
    generation_sha256 = correlation_generation_sha256(correlation_report)

    cached_receipt = _authenticated_cached_receipt(
        receipt_path,
        correlated_path,
        uncorrelated_path,
        generation_sha256,
    )
    if cached_receipt is not None:
        return {**cached_receipt, "publication_status": "cached"}

    publication_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    correlated_document = {
        "manifest_type": "correlated_end_of_day_security_segments",
        "manifest_version": MANIFEST_VERSION,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "correlation_generation_sha256": generation_sha256,
        "lineage_evidence_sha256": correlation_report["lineage_evidence_sha256"],
        "created_at": publication_timestamp,
        "source_download_count": len(correlation_report["source_downloads"]),
        "source_downloads": correlation_report["source_downloads"],
        "ownership_interval_rule_order": correlation_report["ownership_interval_rule_order"],
        "row_correlation_rule_order": correlation_report["row_correlation_rule_order"],
        "uuid_lineage_edges": correlation_report["uuid_lineage_edges"],
        **correlation_report["correlated"],
    }
    uncorrelated_document = {
        "manifest_type": "uncorrelated_end_of_day_security_segments",
        "manifest_version": MANIFEST_VERSION,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "correlation_generation_sha256": generation_sha256,
        "lineage_evidence_sha256": correlation_report["lineage_evidence_sha256"],
        "created_at": publication_timestamp,
        "source_download_count": len(correlation_report["source_downloads"]),
        "source_downloads": correlation_report["source_downloads"],
        "ownership_interval_rule_order": correlation_report["ownership_interval_rule_order"],
        "row_correlation_rule_order": correlation_report["row_correlation_rule_order"],
        "uuid_lineage_edges": correlation_report["uuid_lineage_edges"],
        **correlation_report["uncorrelated"],
    }

    correlated_path.parent.mkdir(parents=True, exist_ok=True)
    correlated_output = _publish_branch(correlated_path, correlated_document)
    uncorrelated_output = _publish_branch(uncorrelated_path, uncorrelated_document)
    receipt = {
        "manifest_type": "end_of_day_security_correlation",
        "manifest_version": MANIFEST_VERSION,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "correlation_generation_sha256": generation_sha256,
        "lineage_evidence_sha256": correlation_report["lineage_evidence_sha256"],
        "lineage_evidence_digest_excludes": ["generated_at", "lineage.api_request_count"],
        "created_at": publication_timestamp,
        "boundary": correlation_report["boundary"],
        "source_download_count": len(correlation_report["source_downloads"]),
        "source_downloads": correlation_report["source_downloads"],
        "ownership_interval_rule_order": correlation_report["ownership_interval_rule_order"],
        "row_correlation_rule_order": correlation_report["row_correlation_rule_order"],
        "uuid_lineage_edges": correlation_report["uuid_lineage_edges"],
        "ticker_ownership_intervals": correlation_report["ticker_ownership_intervals"],
        "evidence_issues": correlation_report["evidence_issues"],
        "files": correlation_report["files"],
        "partition": {
            "source_row_count": correlation_report["source_row_count"],
            "correlated_row_count": correlation_report["correlated"]["row_count"],
            "uncorrelated_row_count": correlation_report["uncorrelated"]["row_count"],
            "row_partition_complete": correlation_report["row_partition_complete"],
        },
        "lineage_report": correlation_report["lineage_report"],
        "warnings": correlation_report["warnings"],
        "outputs": {
            "correlated": correlated_output,
            "uncorrelated": uncorrelated_output,
        },
    }
    write_json_document_atomically(receipt_path, receipt, compact=False)
    return {**receipt, "publication_status": "published"}

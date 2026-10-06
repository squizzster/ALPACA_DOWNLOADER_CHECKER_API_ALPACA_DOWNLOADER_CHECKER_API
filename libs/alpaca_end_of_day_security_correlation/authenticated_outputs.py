import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libs.common.file_integrity import calculate_file_sha256

from .artifact_paths import (
    correlated_pipeline_path,
    security_correlation_manifest_path,
    uncorrelated_pipeline_path,
)
from .correlation_version import CORRELATION_VERSION, validate_correlation_change_log
from .errors import SecurityCorrelationError
from .publication import correlation_generation_sha256
from .security_identity import is_security_id, security_identity_report_is_valid
from .settings import MANIFEST_VERSION


@dataclass(frozen=True)
class AuthenticatedSecurityCorrelation:
    receipt: dict[str, Any]
    correlated: dict[str, Any]
    uncorrelated: dict[str, Any]
    correlated_sha256: str
    uncorrelated_sha256: str
    sources_by_filename: dict[str, dict[str, Any]]


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(f"could not read {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise SecurityCorrelationError(f"{description} is not a JSON object: {path}")
    return document


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _safe_source_filename(value: object, description: str) -> str:
    if not isinstance(value, str) or Path(value).name != value or not value.endswith(".csv.gz"):
        raise SecurityCorrelationError(f"unsafe or invalid {description}: {value!r}")
    return value


def _authenticate_branch(
    path: Path,
    output_receipt: object,
    *,
    symbol: str,
    generation_sha256: str,
    expected_manifest_type: str,
) -> tuple[dict[str, Any], str]:
    if not isinstance(output_receipt, dict) or output_receipt.get("file") != path.name:
        raise SecurityCorrelationError(f"security-correlation receipt does not bind {path}")
    try:
        output_stat = path.stat()
    except OSError as error:
        raise SecurityCorrelationError(f"security-correlation output is unavailable: {path}: {error}") from error
    if output_stat.st_size != output_receipt.get("bytes") or output_stat.st_mtime_ns != output_receipt.get(
        "mtime_ns"
    ):
        raise SecurityCorrelationError(f"security-correlation output stat does not match its receipt: {path}")
    try:
        observed_sha256 = calculate_file_sha256(path)
    except OSError as error:
        raise SecurityCorrelationError(
            f"could not authenticate security-correlation output {path}: {error}"
        ) from error
    if observed_sha256 != output_receipt.get("sha256"):
        raise SecurityCorrelationError(f"security-correlation output SHA-256 does not match its receipt: {path}")
    document = _read_json_object(path, "security-correlation output")
    try:
        verified_sha256 = calculate_file_sha256(path)
        final_stat = path.stat()
    except OSError as error:
        raise SecurityCorrelationError(
            f"could not verify security-correlation output {path}: {error}"
        ) from error
    if verified_sha256 != observed_sha256 or (
        final_stat.st_size,
        final_stat.st_mtime_ns,
    ) != (output_stat.st_size, output_stat.st_mtime_ns):
        raise SecurityCorrelationError(f"security-correlation output changed while it was read: {path}")
    expected_identity = {
        "manifest_type": expected_manifest_type,
        "manifest_version": MANIFEST_VERSION,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "correlation_generation_sha256": generation_sha256,
    }
    for field_name, expected_value in expected_identity.items():
        if document.get(field_name) != expected_value:
            raise SecurityCorrelationError(
                f"security-correlation output {path} has unexpected {field_name}: {document.get(field_name)!r}"
            )
    return document, observed_sha256


def _generation_branch(document: dict[str, Any], *, uncorrelated: bool) -> dict[str, Any]:
    branch = {
        "segment_count": document.get("segment_count"),
        "row_count": document.get("row_count"),
        "segments": document.get("segments"),
    }
    if uncorrelated:
        branch.update(
            {
                "work_item_count": document.get("work_item_count"),
                "work_items": document.get("work_items"),
            }
        )
    return branch


def _validate_generation(
    receipt: dict[str, Any],
    correlated: dict[str, Any],
    uncorrelated: dict[str, Any],
) -> None:
    generation_report = {
        "correlation_version": receipt.get("correlation_version"),
        "symbol": receipt.get("symbol"),
        "lineage_evidence_sha256": receipt.get("lineage_evidence_sha256"),
        "source_downloads": receipt.get("source_downloads"),
        "ownership_interval_rule_order": receipt.get("ownership_interval_rule_order"),
        "row_correlation_rule_order": receipt.get("row_correlation_rule_order"),
        "uuid_lineage_edges": receipt.get("uuid_lineage_edges"),
        "ticker_ownership_intervals": receipt.get("ticker_ownership_intervals"),
        "evidence_issues": receipt.get("evidence_issues"),
        "files": receipt.get("files"),
        "correlated": _generation_branch(correlated, uncorrelated=False),
        "uncorrelated": _generation_branch(uncorrelated, uncorrelated=True),
    }
    observed_generation = correlation_generation_sha256(generation_report)
    if observed_generation != receipt.get("correlation_generation_sha256"):
        raise SecurityCorrelationError("security-correlation generation digest is inconsistent")


def _validate_partition(
    receipt: dict[str, Any],
    correlated: dict[str, Any],
    uncorrelated: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    source_downloads = receipt.get("source_downloads")
    if not isinstance(source_downloads, list):
        raise SecurityCorrelationError("security-correlation receipt has no source downloads")
    if receipt.get("source_download_count") != len(source_downloads):
        raise SecurityCorrelationError("security-correlation source download count is inconsistent")

    sources_by_filename: dict[str, dict[str, Any]] = {}
    for source in source_downloads:
        if not isinstance(source, dict):
            raise SecurityCorrelationError("security-correlation source download is not an object")
        filename = _safe_source_filename(source.get("data_file"), "source filename")
        if filename in sources_by_filename:
            raise SecurityCorrelationError(f"duplicate security-correlation source: {filename}")
        if source.get("download_manifest") != filename + ".manifest.json":
            raise SecurityCorrelationError(f"invalid source manifest filename for {filename}")
        if not _is_sha256(source.get("download_sha256")):
            raise SecurityCorrelationError(f"invalid source SHA-256 for {filename}")
        if type(source.get("rows")) is not int or source["rows"] < 1:
            raise SecurityCorrelationError(f"invalid source row count for {filename}")
        if not isinstance(source.get("adjustment"), str):
            raise SecurityCorrelationError(f"invalid source adjustment for {filename}")
        if source.get("download_sha256_authenticated") is not True:
            raise SecurityCorrelationError(f"source does not record SHA-256 authentication for {filename}")
        sources_by_filename[filename] = source

    for branch_name, document in (("correlated", correlated), ("uncorrelated", uncorrelated)):
        for shared_field in (
            "lineage_evidence_sha256",
            "source_downloads",
            "ownership_interval_rule_order",
            "row_correlation_rule_order",
            "uuid_lineage_edges",
        ):
            if document.get(shared_field) != receipt.get(shared_field):
                raise SecurityCorrelationError(
                    f"{branch_name} security-correlation output has inconsistent {shared_field}"
                )
        if document.get("source_download_count") != len(source_downloads):
            raise SecurityCorrelationError(
                f"{branch_name} security-correlation output has inconsistent source download count"
            )

    ranges_by_filename: dict[str, list[tuple[int, int]]] = {filename: [] for filename in sources_by_filename}
    calculated_branch_counts: dict[str, int] = {}
    for branch_name, document in (("correlated", correlated), ("uncorrelated", uncorrelated)):
        segments = document.get("segments")
        if not isinstance(segments, list):
            raise SecurityCorrelationError(f"{branch_name} security-correlation output has no segment list")
        calculated_count = 0
        for segment in segments:
            if not isinstance(segment, dict):
                raise SecurityCorrelationError(f"{branch_name} security-correlation segment is not an object")
            filename = segment.get("data_file")
            source = sources_by_filename.get(filename) if isinstance(filename, str) else None
            if source is None:
                raise SecurityCorrelationError(
                    f"security-correlation segment refers to unknown source {filename!r}"
                )
            row_start = segment.get("row_start")
            row_end = segment.get("row_end")
            row_count = segment.get("row_count")
            if (
                type(row_start) is not int
                or type(row_end) is not int
                or type(row_count) is not int
                or row_start < 1
                or row_end < row_start
                or row_count != row_end - row_start + 1
                or row_end > source["rows"]
            ):
                raise SecurityCorrelationError(f"invalid row range in {branch_name} segment for {filename}")
            if segment.get("download_sha256") != source.get("download_sha256"):
                raise SecurityCorrelationError(f"segment/source SHA-256 mismatch for {filename}")
            if segment.get("adjustment") != source.get("adjustment"):
                raise SecurityCorrelationError(f"segment/source adjustment mismatch for {filename}")
            if segment.get("download_manifest") != source.get("download_manifest"):
                raise SecurityCorrelationError(f"segment/source manifest mismatch for {filename}")
            if branch_name == "correlated":
                canonical_security = segment.get("canonical_security")
                security_identity = segment.get("security_identity")
                if (
                    not isinstance(canonical_security, dict)
                    or not is_security_id(canonical_security.get("security_id"))
                    or not security_identity_report_is_valid(security_identity)
                    or security_identity.get("security_id") != canonical_security.get("security_id")
                ):
                    raise SecurityCorrelationError(
                        f"correlated segment has no valid internal security identity: {filename}"
                    )
                alpaca_asset_id = canonical_security.get("alpaca_asset_id")
                if alpaca_asset_id is not None and (
                    not isinstance(alpaca_asset_id, str)
                    or alpaca_asset_id not in security_identity["member_alpaca_asset_ids"]
                ):
                    raise SecurityCorrelationError(
                        f"correlated segment has inconsistent Alpaca identity provenance: {filename}"
                    )
            ranges_by_filename[filename].append((row_start, row_end))
            calculated_count += row_count
        if calculated_count != document.get("row_count"):
            raise SecurityCorrelationError(f"{branch_name} security-correlation row count is inconsistent")
        if len(segments) != document.get("segment_count"):
            raise SecurityCorrelationError(f"{branch_name} security-correlation segment count is inconsistent")
        calculated_branch_counts[branch_name] = calculated_count

    work_items = uncorrelated.get("work_items")
    if not isinstance(work_items, list) or uncorrelated.get("work_item_count") != len(work_items):
        raise SecurityCorrelationError("uncorrelated security-correlation work-item count is inconsistent")

    for filename, source in sources_by_filename.items():
        expected_start = 1
        for row_start, row_end in sorted(ranges_by_filename[filename]):
            if row_start != expected_start:
                raise SecurityCorrelationError(
                    f"security-correlation row partition has a gap or overlap in {filename}"
                )
            expected_start = row_end + 1
        if expected_start != source["rows"] + 1:
            raise SecurityCorrelationError(f"security-correlation row partition does not exhaust {filename}")

    partition = receipt.get("partition")
    source_row_count = sum(source["rows"] for source in sources_by_filename.values())
    expected_partition = {
        "source_row_count": source_row_count,
        "correlated_row_count": calculated_branch_counts["correlated"],
        "uncorrelated_row_count": calculated_branch_counts["uncorrelated"],
        "row_partition_complete": True,
    }
    if partition != expected_partition:
        raise SecurityCorrelationError("security-correlation receipt partition is inconsistent")
    if source_row_count != sum(calculated_branch_counts.values()):
        raise SecurityCorrelationError("security-correlation row partition is not exhaustive")
    return sources_by_filename


def authenticate_security_correlation(
    repository_root: Path | str,
    symbol: str,
) -> AuthenticatedSecurityCorrelation:
    """Authenticate a published exhaustive correlation partition without changing it."""
    validate_correlation_change_log()
    repository_root = Path(repository_root)
    receipt_path = security_correlation_manifest_path(repository_root, symbol)
    receipt = _read_json_object(receipt_path, "security-correlation receipt")
    expected_receipt_identity = {
        "manifest_type": "end_of_day_security_correlation",
        "manifest_version": MANIFEST_VERSION,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
    }
    for field_name, expected_value in expected_receipt_identity.items():
        if receipt.get(field_name) != expected_value:
            raise SecurityCorrelationError(
                f"security-correlation receipt {receipt_path} has unexpected {field_name}: "
                f"{receipt.get(field_name)!r}"
            )
    generation_sha256 = receipt.get("correlation_generation_sha256")
    if not _is_sha256(generation_sha256):
        raise SecurityCorrelationError("security-correlation receipt has an invalid generation digest")
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise SecurityCorrelationError("security-correlation receipt has no outputs object")
    correlated, correlated_sha256 = _authenticate_branch(
        correlated_pipeline_path(repository_root, symbol),
        outputs.get("correlated"),
        symbol=symbol,
        generation_sha256=generation_sha256,
        expected_manifest_type="correlated_end_of_day_security_segments",
    )
    uncorrelated, uncorrelated_sha256 = _authenticate_branch(
        uncorrelated_pipeline_path(repository_root, symbol),
        outputs.get("uncorrelated"),
        symbol=symbol,
        generation_sha256=generation_sha256,
        expected_manifest_type="uncorrelated_end_of_day_security_segments",
    )
    sources_by_filename = _validate_partition(receipt, correlated, uncorrelated)
    _validate_generation(receipt, correlated, uncorrelated)
    return AuthenticatedSecurityCorrelation(
        receipt=receipt,
        correlated=correlated,
        uncorrelated=uncorrelated,
        correlated_sha256=correlated_sha256,
        uncorrelated_sha256=uncorrelated_sha256,
        sources_by_filename=sources_by_filename,
    )

import json
from datetime import datetime, timezone
from pathlib import Path

from libs.common.atomic_writes import write_json_document_atomically

from .artifact_paths import (
    download_manifest_path,
    integrity_failure_manifest_path,
    pipeline_receipt_manifest_path,
)
from .daily_bar_validation import EndOfDayValidationResult
from .errors import EndOfDayPipelineError, PricingDataIntegrityError
from .pipeline_version import PIPELINE_VERSION


def read_symbol_manifest_entries(
    symbol_manifest_path: Path,
    expected_symbol: str,
    expected_manifest_type: str,
    entry_collection_key: str,
) -> tuple[dict, dict[str, dict]] | None:
    """Read a pipeline-owned symbol manifest as entries keyed by data filename."""
    try:
        with symbol_manifest_path.open(encoding="utf-8") as manifest_file:
            manifest_document = json.load(manifest_file)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as manifest_load_error:
        raise EndOfDayPipelineError(
            f"could not load {symbol_manifest_path}: {manifest_load_error}"
        ) from manifest_load_error

    if not isinstance(manifest_document, dict):
        raise EndOfDayPipelineError(f"{symbol_manifest_path} is not a JSON object")
    if manifest_document.get("manifest_type") != expected_manifest_type:
        raise EndOfDayPipelineError(f"{symbol_manifest_path} has an invalid manifest type")
    if manifest_document.get("manifest_version") != 1:
        raise EndOfDayPipelineError(f"{symbol_manifest_path} has an unsupported manifest version")
    if manifest_document.get("symbol") != expected_symbol:
        raise EndOfDayPipelineError(f"{symbol_manifest_path} has the wrong symbol")

    manifest_entries = manifest_document.get(entry_collection_key)
    if not isinstance(manifest_entries, list):
        raise EndOfDayPipelineError(f"{symbol_manifest_path} {entry_collection_key} must be an array")
    entries_by_data_filename = {}
    for manifest_entry in manifest_entries:
        if not isinstance(manifest_entry, dict) or not isinstance(manifest_entry.get("data_file"), str):
            raise EndOfDayPipelineError(f"{symbol_manifest_path} contains an invalid artifact entry")
        data_filename = manifest_entry["data_file"]
        if data_filename in entries_by_data_filename:
            raise EndOfDayPipelineError(f"{symbol_manifest_path} contains duplicate entry {data_filename!r}")
        entries_by_data_filename[data_filename] = manifest_entry
    return manifest_document, entries_by_data_filename


def read_pipeline_receipts(
    receipt_manifest_path: Path, expected_symbol: str
) -> tuple[int, dict[str, dict]] | None:
    parsed_manifest = read_symbol_manifest_entries(
        receipt_manifest_path,
        expected_symbol,
        "end_of_day_pipeline_receipts",
        "receipts",
    )
    if parsed_manifest is None:
        return None
    manifest_document, receipts_by_data_filename = parsed_manifest
    manifest_pipeline_version = manifest_document.get("pipeline_version")
    if manifest_pipeline_version is None:
        # Read the v9 layout once so it can migrate through the normal stale
        # pipeline path; v10 writes the version only at manifest scope.
        legacy_pipeline_versions = {
            receipt.get("pipeline_version") for receipt in receipts_by_data_filename.values()
        }
        if len(legacy_pipeline_versions) == 1:
            manifest_pipeline_version = legacy_pipeline_versions.pop()
    if type(manifest_pipeline_version) is not int or manifest_pipeline_version < 1:
        raise EndOfDayPipelineError(f"{receipt_manifest_path} has an invalid pipeline version")
    return manifest_pipeline_version, receipts_by_data_filename


def read_integrity_failures(
    failure_manifest_path: Path, expected_symbol: str
) -> tuple[int, dict[str, dict]] | None:
    parsed_manifest = read_symbol_manifest_entries(
        failure_manifest_path,
        expected_symbol,
        "end_of_day_integrity_failures",
        "failures",
    )
    if parsed_manifest is None:
        return None
    manifest_document, failures_by_data_filename = parsed_manifest
    manifest_pipeline_version = manifest_document.get("pipeline_version")
    if type(manifest_pipeline_version) is not int or manifest_pipeline_version < 1:
        raise EndOfDayPipelineError(f"{failure_manifest_path} has an invalid pipeline version")
    return manifest_pipeline_version, failures_by_data_filename


def update_integrity_failure_manifest(
    download_path: Path,
    request_identity: dict,
    artifact_failure: dict | None,
    validation_timestamp: str,
) -> Path:
    failure_manifest_path = integrity_failure_manifest_path(download_path)
    existing_failure_state = read_integrity_failures(failure_manifest_path, request_identity["symbol"])
    if existing_failure_state is None or existing_failure_state[0] != PIPELINE_VERSION:
        failures_by_data_filename = {}
    else:
        _, failures_by_data_filename = existing_failure_state
    data_filename = download_path.name
    if artifact_failure is None:
        failures_by_data_filename.pop(data_filename, None)
    else:
        failures_by_data_filename[data_filename] = artifact_failure

    ordered_failures = [failures_by_data_filename[filename] for filename in sorted(failures_by_data_filename)]
    failure_manifest_document = {
        "manifest_type": "end_of_day_integrity_failures",
        "manifest_version": 1,
        "symbol": request_identity["symbol"],
        "pipeline_version": PIPELINE_VERSION,
        "updated_at": validation_timestamp,
        "failure_count": len(ordered_failures),
        "failures": ordered_failures,
    }
    write_json_document_atomically(failure_manifest_path, failure_manifest_document, compact=False)
    return failure_manifest_path


def update_pipeline_receipt_manifest(
    download_path: Path,
    request_identity: dict,
    pipeline_receipt: dict,
    validation_timestamp: str,
) -> Path:
    receipt_manifest_path = pipeline_receipt_manifest_path(download_path)
    existing_receipt_state = read_pipeline_receipts(receipt_manifest_path, request_identity["symbol"])
    if existing_receipt_state is None or existing_receipt_state[0] != PIPELINE_VERSION:
        receipts_by_data_filename = {}
    else:
        _, receipts_by_data_filename = existing_receipt_state
    receipts_by_data_filename[download_path.name] = pipeline_receipt
    ordered_receipts = [receipts_by_data_filename[filename] for filename in sorted(receipts_by_data_filename)]
    receipt_manifest_document = {
        "manifest_type": "end_of_day_pipeline_receipts",
        "manifest_version": 1,
        "symbol": request_identity["symbol"],
        "pipeline_version": PIPELINE_VERSION,
        "updated_at": validation_timestamp,
        "receipt_count": len(ordered_receipts),
        "receipts": ordered_receipts,
    }
    write_json_document_atomically(receipt_manifest_path, receipt_manifest_document, compact=False)
    return receipt_manifest_path


def record_validation_result(
    download_path: Path,
    request_identity: dict,
    download_manifest: dict,
    validation_result: EndOfDayValidationResult,
    *,
    raise_after_recording_failures: bool = True,
) -> dict:
    validation_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    pipeline_receipt = {
        "data_file": download_path.name,
        "download_manifest": download_manifest_path(download_path).name,
        "adjustment": request_identity["adjustment"],
        "download_sha256": download_manifest["sha256"],
        "processed_at": validation_timestamp,
        "rows_seen": validation_result.rows_seen,
        "sessions_seen": len(validation_result.observed_session_dates),
        "from": validation_result.first_bar_timestamp,
        "to": validation_result.last_bar_timestamp,
    }

    if validation_result.has_failures:
        artifact_failure = {
            **pipeline_receipt,
            "pipeline_version": PIPELINE_VERSION,
            "row_failure_count": len(validation_result.row_failures),
            "dataset_failure_count": len(validation_result.dataset_failures),
            "row_failures": list(validation_result.row_failures),
            "dataset_failures": list(validation_result.dataset_failures),
        }
        failure_manifest_path = update_integrity_failure_manifest(
            download_path,
            request_identity,
            artifact_failure,
            validation_timestamp,
        )
    else:
        failure_manifest_path = update_integrity_failure_manifest(
            download_path,
            request_identity,
            None,
            validation_timestamp,
        )

    update_pipeline_receipt_manifest(
        download_path,
        request_identity,
        pipeline_receipt,
        validation_timestamp,
    )

    if validation_result.has_failures and raise_after_recording_failures:
        raise PricingDataIntegrityError(
            f"{download_path}: pricing/data integrity failed; the authenticated "
            f"download was not changed; details: {failure_manifest_path}"
        )
    return pipeline_receipt

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .settings import EARLIEST_BAR_DATE


def load_optional_json(path: Path, errors: list[str]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        errors.append(f"{path}: {error}")
        return None
    if not isinstance(document, dict):
        errors.append(f"{path}: expected a JSON object")
        return None
    return document


def summarize_dataset_failure(dataset_failure: object) -> dict[str, Any]:
    if not isinstance(dataset_failure, dict):
        return {"malformed_failure": repr(dataset_failure)}
    summary = {"validation_error": dataset_failure.get("validation_error")}
    for field_name in ("missing_sessions", "unexpected_sessions"):
        sessions = dataset_failure.get(field_name)
        if isinstance(sessions, list):
            summary[f"{field_name}_count"] = len(sessions)
            if sessions:
                summary[f"first_{field_name}"] = sessions[0]
                summary[f"last_{field_name}"] = sessions[-1]
    return summary


def summarize_integrity_failure(failure: dict[str, Any]) -> dict[str, Any]:
    dataset_failures = failure.get("dataset_failures")
    if not isinstance(dataset_failures, list):
        dataset_failures = []
    return {
        "pipeline_version": failure.get("pipeline_version"),
        "row_failure_count": failure.get("row_failure_count"),
        "dataset_failure_count": failure.get("dataset_failure_count"),
        "dataset_failures": [summarize_dataset_failure(item) for item in dataset_failures],
    }


def _stat_download(data_path: Path, errors: list[str]):
    try:
        return data_path.stat()
    except FileNotFoundError:
        return None
    except OSError as error:
        errors.append(f"{data_path}: {error}")
        return None


def _download_summary_without_manifest(data_filename: str, data_stat, sidecar_present: bool):
    return {
        "file": data_filename,
        "sidecar_present": sidecar_present,
        "sidecar_readable": False,
        "adjustment": None,
        "sha256": None,
        "rows": None,
        "sessions": None,
        "from": None,
        "to": None,
        "request_from": None,
        "request_to": None,
        "asof": None,
        "gzip_present": data_stat is not None,
        "stat_matches_sidecar": False,
        "pipeline_receipt_matches": False,
        "integrity_failure": None,
    }


def inspect_local_downloads(repository_root: Path, symbol: str) -> dict[str, Any]:
    symbol_relative_directory = Path("data") / "end_of_day" / symbol
    symbol_directory = repository_root / symbol_relative_directory
    errors: list[str] = []
    pipeline_manifest = load_optional_json(symbol_directory / "pipeline.manifest.json", errors)
    integrity_manifest = load_optional_json(
        symbol_directory / "integrity_failure.manifest.json",
        errors,
    )

    receipts_by_file = {}
    if pipeline_manifest is not None and isinstance(pipeline_manifest.get("receipts"), list):
        receipts_by_file = {
            receipt.get("data_file"): receipt
            for receipt in pipeline_manifest["receipts"]
            if isinstance(receipt, dict) and isinstance(receipt.get("data_file"), str)
        }

    failures_by_file = {}
    if integrity_manifest is not None and isinstance(integrity_manifest.get("failures"), list):
        failures_by_file = {
            failure.get("data_file"): failure
            for failure in integrity_manifest["failures"]
            if isinstance(failure, dict) and isinstance(failure.get("data_file"), str)
        }

    symbol_directory_present = symbol_directory.is_dir()
    data_filenames = set()
    if symbol_directory_present:
        data_filenames.update(path.name for path in symbol_directory.glob("*.csv.gz"))
        data_filenames.update(
            path.name.removesuffix(".manifest.json") for path in symbol_directory.glob("*.csv.gz.manifest.json")
        )

    downloads = []
    for data_filename in sorted(data_filenames):
        data_path = symbol_directory / data_filename
        data_stat = _stat_download(data_path, errors)
        manifest_path = symbol_directory / f"{data_filename}.manifest.json"
        sidecar_present = manifest_path.exists()
        manifest = load_optional_json(manifest_path, errors)
        if manifest is None:
            downloads.append(_download_summary_without_manifest(data_filename, data_stat, sidecar_present))
            continue

        receipt = receipts_by_file.get(data_filename)
        failure = failures_by_file.get(data_filename)
        downloads.append(
            {
                "file": data_filename,
                "sidecar_present": True,
                "sidecar_readable": True,
                "adjustment": manifest.get("adjustment"),
                "sha256": manifest.get("sha256"),
                "rows": manifest.get("rows"),
                "sessions": manifest.get("sessions"),
                "from": manifest.get("from"),
                "to": manifest.get("to"),
                "request_from": manifest.get("request_from"),
                "request_to": manifest.get("request_to"),
                "asof": manifest.get("asof"),
                "gzip_present": data_stat is not None,
                "stat_matches_sidecar": (
                    data_stat is not None
                    and data_stat.st_size == manifest.get("bytes")
                    and data_stat.st_mtime_ns == manifest.get("mtime_ns")
                ),
                "pipeline_receipt_matches": (
                    isinstance(receipt, dict) and receipt.get("download_sha256") == manifest.get("sha256")
                ),
                "integrity_failure": (
                    summarize_integrity_failure(failure) if isinstance(failure, dict) else None
                ),
            }
        )

    return {
        "directory": str(symbol_relative_directory),
        "present": symbol_directory_present,
        "download_count": len(downloads),
        "download_sha256_authentication_performed": False,
        "downloads": downloads,
        "pipeline_manifest": (
            {
                "pipeline_version": pipeline_manifest.get("pipeline_version"),
                "receipt_count": pipeline_manifest.get("receipt_count"),
            }
            if pipeline_manifest is not None
            else None
        ),
        "integrity_manifest": (
            {
                "pipeline_version": integrity_manifest.get("pipeline_version"),
                "failure_count": integrity_manifest.get("failure_count"),
            }
            if integrity_manifest is not None
            else None
        ),
        "manifest_errors": errors,
    }


def local_query_dates(local_report: dict[str, Any]) -> tuple[str, str]:
    request_start_dates = []
    asof_dates = []
    for download in local_report["downloads"]:
        request_from = download.get("request_from")
        asof = download.get("asof")
        if isinstance(request_from, str) and len(request_from) >= 10:
            request_start_dates.append(request_from[:10])
        if isinstance(asof, str) and len(asof) >= 10:
            asof_dates.append(asof[:10])
    start_date = min(request_start_dates, default=EARLIEST_BAR_DATE)
    end_date = max(asof_dates, default=datetime.now(timezone.utc).date().isoformat())
    return start_date, end_date

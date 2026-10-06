import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_pipeline import (
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    authenticate_frozen_download,
    download_manifest_path,
    inspect_cached_download,
    read_frozen_alpaca_daily_bars,
)

from .errors import SecurityCorrelationError


@dataclass(frozen=True)
class AuthenticatedFrozenDownload:
    path: Path
    manifest: dict[str, Any]
    rows: list[dict]
    pipeline_state: str
    pipeline_diagnostic: str | None
    integrity_failure: dict[str, Any] | None


def _read_download_manifest(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(f"could not read download manifest {path}: {error}") from error
    if not isinstance(document, dict):
        raise SecurityCorrelationError(f"download manifest is not an object: {path}")
    return document


def read_authenticated_frozen_download(
    repository_root: Path,
    symbol_directory: str,
    local_download: dict[str, Any],
) -> AuthenticatedFrozenDownload:
    data_filename = local_download.get("file")
    if (
        not isinstance(data_filename, str)
        or Path(data_filename).name != data_filename
        or not data_filename.endswith(".csv.gz")
    ):
        raise SecurityCorrelationError(f"unsafe or invalid local download filename: {data_filename!r}")

    download_path = repository_root / symbol_directory / data_filename
    sidecar_path = download_manifest_path(download_path)
    download_manifest = _read_download_manifest(sidecar_path)
    request_identity = {
        field_name: download_manifest.get(field_name) for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS
    }
    missing_identity_fields = [
        field_name
        for field_name, field_value in request_identity.items()
        if not isinstance(field_value, str) or not field_value
    ]
    if missing_identity_fields:
        raise SecurityCorrelationError(
            f"{sidecar_path}: missing request identity fields: {', '.join(missing_identity_fields)}"
        )

    cache_inspection = inspect_cached_download(download_path, request_identity)
    allowed_pipeline_states = {"cached", RECORDED_PRICING_DATA_FAILURE_ACTION}
    if cache_inspection.required_action not in allowed_pipeline_states:
        raise SecurityCorrelationError(
            f"{download_path}: cannot correlate frozen download in state "
            f"{cache_inspection.required_action!r}: {cache_inspection.diagnostic}"
        )
    if cache_inspection.download_manifest is None:
        raise SecurityCorrelationError(f"{download_path}: cache inspection returned no download manifest")

    authenticate_frozen_download(download_path, cache_inspection.download_manifest)
    rows = read_frozen_alpaca_daily_bars(download_path)
    if len(rows) != download_manifest.get("rows"):
        raise SecurityCorrelationError(
            f"{download_path}: decompressed row count {len(rows)} does not match sidecar "
            f"row count {download_manifest.get('rows')!r}"
        )

    integrity_failure = local_download.get("integrity_failure")
    return AuthenticatedFrozenDownload(
        path=download_path,
        manifest=download_manifest,
        rows=rows,
        pipeline_state=cache_inspection.required_action,
        pipeline_diagnostic=cache_inspection.diagnostic,
        integrity_failure=integrity_failure if isinstance(integrity_failure, dict) else None,
    )

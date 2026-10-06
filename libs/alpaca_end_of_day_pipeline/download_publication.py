import csv
import io
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from libs.common.atomic_writes import create_temporary_sibling_path
from libs.common.file_integrity import calculate_file_sha256
from libs.common.us_equity_market_session_calendar import (
    market_session_dates_sha256,
    us_market_hours_sha256,
)

from .artifact_paths import download_manifest_path, integrity_failure_manifest_path
from .contracts import (
    CSV_COLUMN_ALPACA_FIELD_PAIRS,
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    MANIFEST_SCHEMA_VERSION,
    PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY,
)
from .daily_bar_validation import (
    alpaca_bar_to_csv_row,
    validate_download_request_identity,
    validate_end_of_day_bars,
)
from .errors import EndOfDayPipelineError, PricingDataIntegrityError
from .pipeline_version import PIPELINE_VERSION, validate_pipeline_change_log
from .symbol_manifests import record_validation_result


def _terminate_pigz_process(pigz_process: subprocess.Popen | None) -> None:
    if pigz_process is None:
        return
    if pigz_process.stdin is not None and not pigz_process.stdin.closed:
        pigz_process.stdin.close()
    if pigz_process.poll() is None:
        pigz_process.terminate()
    pigz_process.wait()
    if pigz_process.stderr is not None and not pigz_process.stderr.closed:
        pigz_process.stderr.close()


def validate_and_publish_alpaca_download(
    download_path: Path | str,
    downloaded_alpaca_bars: list[object] | tuple[object, ...],
    request_identity: dict,
    *,
    replacement_authorized: bool = False,
) -> dict:
    """Validate in memory, then own and publish the Alpaca download boundary."""
    validate_pipeline_change_log()
    validate_download_request_identity(request_identity)
    download_path = Path(download_path)
    sidecar_manifest_path = download_manifest_path(download_path)
    download_exists = download_path.exists()
    sidecar_manifest_exists = sidecar_manifest_path.exists()
    if (download_exists or sidecar_manifest_exists) and not replacement_authorized:
        raise EndOfDayPipelineError(
            f"refusing to replace the Alpaca download boundary for {download_path} "
            "without an authorized refresh or repair"
        )

    # Nothing compressed reaches disk until the complete response has traversed
    # the same row and dataset integrity pipeline used for cached downloads.
    downloaded_alpaca_bars = tuple(downloaded_alpaca_bars)
    validation_result = validate_end_of_day_bars(downloaded_alpaca_bars, request_identity)

    download_path.parent.mkdir(parents=True, exist_ok=True)
    compressed_download_temporary_path = create_temporary_sibling_path(download_path)
    pigz_process = None
    try:
        with compressed_download_temporary_path.open("wb") as compressed_download_file:
            pigz_process = subprocess.Popen(
                ["pigz", "-5", "-c"],
                stdin=subprocess.PIPE,
                stdout=compressed_download_file,
                stderr=subprocess.PIPE,
            )
            if pigz_process.stdin is None or pigz_process.stderr is None:
                raise EndOfDayPipelineError("could not open pigz streams")

            pigz_csv_input = io.TextIOWrapper(
                pigz_process.stdin,
                encoding="utf-8",
                newline="",
                write_through=True,
            )
            try:
                csv_writer = csv.DictWriter(
                    pigz_csv_input,
                    fieldnames=[csv_column for csv_column, _ in CSV_COLUMN_ALPACA_FIELD_PAIRS],
                )
                csv_writer.writeheader()
                for alpaca_bar in downloaded_alpaca_bars:
                    csv_writer.writerow(alpaca_bar_to_csv_row(alpaca_bar))
            finally:
                pigz_csv_input.close()

            pigz_stderr = pigz_process.stderr.read().decode("utf-8", "replace").strip()
            pigz_process.stderr.close()
            pigz_exit_code = pigz_process.wait()
            if pigz_exit_code != 0:
                raise EndOfDayPipelineError(
                    f"pigz failed with status {pigz_exit_code}: {pigz_stderr or 'no details'}"
                )
            compressed_download_file.flush()
            os.fsync(compressed_download_file.fileno())
    except BaseException:
        _terminate_pigz_process(pigz_process)
        compressed_download_temporary_path.unlink(missing_ok=True)
        raise

    compressed_download_sha256 = calculate_file_sha256(compressed_download_temporary_path)
    verified_compressed_download_sha256 = calculate_file_sha256(compressed_download_temporary_path)
    if verified_compressed_download_sha256 != compressed_download_sha256:
        compressed_download_temporary_path.unlink(missing_ok=True)
        raise EndOfDayPipelineError("compressed file failed SHA-256 verification")

    compressed_download_stat = compressed_download_temporary_path.stat()
    download_creation_timestamp = (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace(
            "+00:00",
            "Z",
        )
    )
    download_manifest = {
        "manifest_version": MANIFEST_SCHEMA_VERSION,
        "boundary": "alpaca_download",
        "pipeline_version": PIPELINE_VERSION,
        PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY: True,
        "file": download_path.name,
        "sha256": compressed_download_sha256,
        "bytes": compressed_download_stat.st_size,
        "mtime_ns": compressed_download_stat.st_mtime_ns,
        "rows": validation_result.rows_seen,
        "sessions": len(validation_result.observed_session_dates),
        "session_sha256": market_session_dates_sha256(validation_result.observed_session_dates),
        "calendar_sha256": us_market_hours_sha256(),
        "from": validation_result.first_bar_timestamp,
        "to": validation_result.last_bar_timestamp,
        **{field_name: request_identity[field_name] for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS},
        "created_at": download_creation_timestamp,
    }

    # Failed data may be published only after its failure evidence and pipeline
    # receipt are durable. A successful run needs no integrity-failure entry.
    if validation_result.has_failures:
        try:
            record_validation_result(
                download_path,
                request_identity,
                download_manifest,
                validation_result,
                raise_after_recording_failures=False,
            )
        except BaseException:
            compressed_download_temporary_path.unlink(missing_ok=True)
            raise

    sidecar_manifest_temporary_path = create_temporary_sibling_path(sidecar_manifest_path)
    try:
        with sidecar_manifest_temporary_path.open("w", encoding="utf-8") as sidecar_manifest_file:
            json.dump(download_manifest, sidecar_manifest_file, separators=(",", ":"))
            sidecar_manifest_file.write("\n")
            sidecar_manifest_file.flush()
            os.fsync(sidecar_manifest_file.fileno())
        os.replace(compressed_download_temporary_path, download_path)
        os.replace(sidecar_manifest_temporary_path, sidecar_manifest_path)
    except BaseException:
        compressed_download_temporary_path.unlink(missing_ok=True)
        sidecar_manifest_temporary_path.unlink(missing_ok=True)
        raise

    if validation_result.has_failures:
        raise PricingDataIntegrityError(
            f"{download_path}: pricing/data integrity failed; the authenticated "
            f"download was preserved; details: "
            f"{integrity_failure_manifest_path(download_path)}"
        )

    record_validation_result(
        download_path,
        request_identity,
        download_manifest,
        validation_result,
    )
    return download_manifest

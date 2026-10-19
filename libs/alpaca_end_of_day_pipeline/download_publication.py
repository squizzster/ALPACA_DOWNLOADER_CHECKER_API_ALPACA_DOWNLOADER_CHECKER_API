import csv
import io
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from libs.common.atomic_writes import create_temporary_sibling_path
from libs.common.file_integrity import calculate_file_sha256
from libs.common.us_equity_market_session_calendar import (
    market_session_dates_sha256,
    us_market_hours_sha256,
)

from .artifact_paths import download_manifest_path, integrity_failure_manifest_path
from .contracts import (
    ALPACA_DOWNLOAD_BOUNDARY,
    CSV_COLUMN_ALPACA_FIELD_PAIRS,
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY,
    EMPIRICAL_ADJUSTMENT_PATTERNS,
    EMPIRICAL_ADJUSTMENT_POLICY,
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
    storage_deduplication_candidates: tuple[Path, ...] = (),
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
    except BaseException:
        _terminate_pigz_process(pigz_process)
        compressed_download_temporary_path.unlink(missing_ok=True)
        raise

    compressed_download_sha256 = calculate_file_sha256(compressed_download_temporary_path)
    verified_compressed_download_sha256 = calculate_file_sha256(compressed_download_temporary_path)
    if verified_compressed_download_sha256 != compressed_download_sha256:
        compressed_download_temporary_path.unlink(missing_ok=True)
        raise EndOfDayPipelineError("compressed file failed SHA-256 verification")

    storage = None
    for candidate_path in storage_deduplication_candidates:
        candidate_path = Path(candidate_path)
        if (
            candidate_path.parent != download_path.parent
            or candidate_path == download_path
            or not candidate_path.is_file()
        ):
            continue
        if calculate_file_sha256(candidate_path) != compressed_download_sha256:
            continue
        hard_link_temporary_path = create_temporary_sibling_path(download_path)
        try:
            hard_link_temporary_path.unlink()
            os.link(candidate_path, hard_link_temporary_path)
        except BaseException:
            hard_link_temporary_path.unlink(missing_ok=True)
            compressed_download_temporary_path.unlink(missing_ok=True)
            raise
        compressed_download_temporary_path.unlink()
        compressed_download_temporary_path = hard_link_temporary_path
        storage = {
            "kind": "hard_link",
            "canonical_file": candidate_path.name,
        }
        break

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
        "boundary": ALPACA_DOWNLOAD_BOUNDARY,
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
    if storage is not None:
        download_manifest["storage"] = storage

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


def deduplicate_authenticated_download_storage(
    downloads_by_adjustment: Mapping[str, Path],
) -> dict[str, int]:
    """Atomically replace equal direct-download payloads with same-directory hard links."""
    validate_pipeline_change_log()
    authenticated = []
    parent_directory = None
    for adjustment, download_path in downloads_by_adjustment.items():
        download_path = Path(download_path)
        if parent_directory is None:
            parent_directory = download_path.parent
        if download_path.parent != parent_directory:
            raise EndOfDayPipelineError("storage deduplication inputs must share one directory")
        manifest_path = download_manifest_path(download_path)
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            download_stat = download_path.stat()
        except (OSError, json.JSONDecodeError) as error:
            raise EndOfDayPipelineError(
                f"could not inspect download for storage deduplication: {error}"
            ) from error
        if (
            not isinstance(manifest, dict)
            or manifest.get("boundary") != ALPACA_DOWNLOAD_BOUNDARY
            or manifest.get("adjustment") != adjustment
            or manifest.get("file") != download_path.name
            or manifest.get("bytes") != download_stat.st_size
            or manifest.get("mtime_ns") != download_stat.st_mtime_ns
            or manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY) is not True
        ):
            raise EndOfDayPipelineError(
                f"download is not eligible for authenticated storage deduplication: {download_path}"
            )
        observed_sha256 = calculate_file_sha256(download_path)
        if observed_sha256 != manifest.get("sha256"):
            raise EndOfDayPipelineError(
                f"download failed SHA-256 authentication before storage deduplication: {download_path}"
            )
        authenticated.append((adjustment, download_path, manifest, observed_sha256))

    canonical_by_sha256 = {}
    linked_file_count = 0
    reclaimed_bytes = 0
    for _adjustment, download_path, manifest, observed_sha256 in authenticated:
        canonical_path = canonical_by_sha256.setdefault(observed_sha256, download_path)
        if canonical_path == download_path or os.path.samefile(canonical_path, download_path):
            continue
        hard_link_temporary_path = create_temporary_sibling_path(download_path)
        manifest_path = download_manifest_path(download_path)
        manifest_temporary_path = create_temporary_sibling_path(manifest_path)
        try:
            hard_link_temporary_path.unlink()
            os.link(canonical_path, hard_link_temporary_path)
            linked_stat = hard_link_temporary_path.stat()
            updated_manifest = {
                **manifest,
                "bytes": linked_stat.st_size,
                "mtime_ns": linked_stat.st_mtime_ns,
                "storage": {
                    "kind": "hard_link",
                    "canonical_file": canonical_path.name,
                },
            }
            with manifest_temporary_path.open("w", encoding="utf-8") as manifest_file:
                json.dump(updated_manifest, manifest_file, separators=(",", ":"))
                manifest_file.write("\n")
                manifest_file.flush()
            replaced_size = download_path.stat().st_size
            os.replace(hard_link_temporary_path, download_path)
            os.replace(manifest_temporary_path, manifest_path)
        except BaseException:
            hard_link_temporary_path.unlink(missing_ok=True)
            manifest_temporary_path.unlink(missing_ok=True)
            raise
        if not os.path.samefile(canonical_path, download_path):
            raise EndOfDayPipelineError("published storage hard link does not share its canonical inode")
        if calculate_file_sha256(download_path) != observed_sha256:
            raise EndOfDayPipelineError("published storage hard link failed SHA-256 verification")
        linked_file_count += 1
        reclaimed_bytes += replaced_size

    return {
        "linked_file_count": linked_file_count,
        "reclaimed_bytes": reclaimed_bytes,
    }


def publish_empirical_adjustment_alias(
    alias_path: Path | str,
    source_download_path: Path | str,
    source_download_manifest: dict,
    request_identity: dict,
    *,
    policy: str,
    pattern: str,
    observed_downloads: Mapping[str, tuple[Path, dict]],
) -> dict:
    """Publish one explicitly inferred logical view as a hard link to a direct download."""
    validate_pipeline_change_log()
    validate_download_request_identity(request_identity)
    if policy != EMPIRICAL_ADJUSTMENT_POLICY:
        raise EndOfDayPipelineError(f"unsupported empirical adjustment policy: {policy!r}")
    if pattern not in EMPIRICAL_ADJUSTMENT_PATTERNS:
        raise EndOfDayPipelineError(f"unsupported empirical adjustment pattern: {pattern!r}")
    alias_path = Path(alias_path)
    source_download_path = Path(source_download_path)
    alias_manifest_path = download_manifest_path(alias_path)
    source_manifest_path = download_manifest_path(source_download_path)
    if alias_path.parent != source_download_path.parent or alias_path == source_download_path:
        raise EndOfDayPipelineError("empirical alias and source must be distinct files in one directory")
    if alias_path.name != request_identity["adjustment"].replace(",", "_") + ".csv.gz":
        raise EndOfDayPipelineError("empirical alias filename does not match its adjustment")
    if alias_path.exists() or alias_manifest_path.exists():
        raise EndOfDayPipelineError(f"refusing to replace empirical alias boundary for {alias_path}")
    if source_download_manifest.get("boundary") != ALPACA_DOWNLOAD_BOUNDARY:
        raise EndOfDayPipelineError("empirical alias source is not a direct Alpaca download")
    if source_download_path.name != source_download_manifest.get("adjustment", "").replace(",", "_") + ".csv.gz":
        raise EndOfDayPipelineError("empirical alias source filename does not match its adjustment")
    try:
        stored_source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EndOfDayPipelineError(f"could not read alias source manifest: {error}") from error
    if stored_source_manifest != source_download_manifest:
        raise EndOfDayPipelineError("empirical alias source manifest changed before publication")
    try:
        source_stat = source_download_path.stat()
    except OSError as error:
        raise EndOfDayPipelineError(f"could not stat empirical alias source: {error}") from error
    if (
        source_download_manifest.get("file") != source_download_path.name
        or source_download_manifest.get("bytes") != source_stat.st_size
        or source_download_manifest.get("mtime_ns") != source_stat.st_mtime_ns
        or source_download_manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY) is not True
    ):
        raise EndOfDayPipelineError("empirical alias source manifest does not authenticate its file stat")
    source_sha256 = calculate_file_sha256(source_download_path)
    if source_sha256 != source_download_manifest.get("sha256"):
        raise EndOfDayPipelineError("empirical alias source failed SHA-256 authentication")
    for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS:
        if (
            field_name != "adjustment"
            and source_download_manifest.get(field_name) != request_identity[field_name]
        ):
            raise EndOfDayPipelineError(
                f"empirical alias source has inconsistent request identity field {field_name!r}"
            )

    if len(observed_downloads) < 2:
        raise EndOfDayPipelineError("empirical alias requires at least two direct observations")
    observation_receipts = []
    source_was_observed = False
    for adjustment, (observed_path, observed_manifest) in observed_downloads.items():
        observed_path = Path(observed_path)
        observed_manifest_path = download_manifest_path(observed_path)
        try:
            observed_stat = observed_path.stat()
        except OSError as error:
            raise EndOfDayPipelineError(f"could not stat empirical alias evidence: {error}") from error
        if (
            observed_path.parent != alias_path.parent
            or observed_manifest.get("boundary") != ALPACA_DOWNLOAD_BOUNDARY
            or observed_manifest.get("adjustment") != adjustment
            or observed_manifest.get("file") != observed_path.name
            or observed_path.name != adjustment.replace(",", "_") + ".csv.gz"
            or observed_manifest.get("bytes") != observed_stat.st_size
            or observed_manifest.get("mtime_ns") != observed_stat.st_mtime_ns
            or observed_manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY) is not True
        ):
            raise EndOfDayPipelineError("empirical alias evidence is not a direct matching download")
        for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS:
            if field_name != "adjustment" and observed_manifest.get(field_name) != request_identity[field_name]:
                raise EndOfDayPipelineError(
                    f"empirical alias evidence has inconsistent request identity field {field_name!r}"
                )
        if calculate_file_sha256(observed_path) != observed_manifest.get("sha256"):
            raise EndOfDayPipelineError("empirical alias evidence failed SHA-256 authentication")
        try:
            stored_observed_manifest = json.loads(observed_manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise EndOfDayPipelineError(f"could not read empirical alias evidence: {error}") from error
        if stored_observed_manifest != observed_manifest:
            raise EndOfDayPipelineError("empirical alias evidence manifest changed before publication")
        source_was_observed = source_was_observed or observed_path == source_download_path
        observation_receipts.append(
            {
                "adjustment": adjustment,
                "file": observed_path.name,
                "manifest": observed_manifest_path.name,
                "manifest_sha256": calculate_file_sha256(observed_manifest_path),
                "sha256": observed_manifest["sha256"],
            }
        )
    if not source_was_observed:
        raise EndOfDayPipelineError("empirical alias source is absent from its direct observations")

    alias_path.parent.mkdir(parents=True, exist_ok=True)
    alias_temporary_path = create_temporary_sibling_path(alias_path)
    alias_manifest_temporary_path = create_temporary_sibling_path(alias_manifest_path)
    try:
        alias_temporary_path.unlink()
        os.link(source_download_path, alias_temporary_path)
        alias_stat = alias_temporary_path.stat()
        if calculate_file_sha256(alias_temporary_path) != source_sha256:
            raise EndOfDayPipelineError("empirical alias hard link failed SHA-256 verification")
        publication_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        alias_manifest = {
            "manifest_version": MANIFEST_SCHEMA_VERSION,
            "boundary": EMPIRICAL_ADJUSTMENT_ALIAS_BOUNDARY,
            "pipeline_version": PIPELINE_VERSION,
            PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY: True,
            "file": alias_path.name,
            "sha256": source_sha256,
            "bytes": alias_stat.st_size,
            "mtime_ns": alias_stat.st_mtime_ns,
            "rows": source_download_manifest["rows"],
            "sessions": source_download_manifest["sessions"],
            "session_sha256": source_download_manifest["session_sha256"],
            "calendar_sha256": source_download_manifest["calendar_sha256"],
            "from": source_download_manifest["from"],
            "to": source_download_manifest["to"],
            **{field_name: request_identity[field_name] for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS},
            "created_at": publication_timestamp,
            "storage": {
                "kind": "hard_link",
                "canonical_file": source_download_path.name,
            },
            "empirical_equivalence": {
                "policy": policy,
                "pattern": pattern,
                "source_adjustment": source_download_manifest["adjustment"],
                "source_file": source_download_path.name,
                "source_manifest": source_manifest_path.name,
                "source_manifest_sha256": calculate_file_sha256(source_manifest_path),
                "observed_downloads": observation_receipts,
            },
        }
        with alias_manifest_temporary_path.open("w", encoding="utf-8") as alias_manifest_file:
            json.dump(alias_manifest, alias_manifest_file, separators=(",", ":"))
            alias_manifest_file.write("\n")
            alias_manifest_file.flush()
        os.replace(alias_temporary_path, alias_path)
        os.replace(alias_manifest_temporary_path, alias_manifest_path)
    except BaseException:
        alias_temporary_path.unlink(missing_ok=True)
        alias_manifest_temporary_path.unlink(missing_ok=True)
        raise
    return alias_manifest

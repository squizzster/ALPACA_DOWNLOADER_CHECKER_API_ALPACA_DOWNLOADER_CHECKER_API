# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.
import csv
import gzip
import hashlib
import io
import json
import math
import os
import subprocess
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from datetime import time as datetime_time
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from us_equity_market_session_calendar import (
    MarketCalendarError,
    compare_observed_to_expected_sessions,
    market_session_dates_sha256,
    us_market_hours_sha256,
)

MANIFEST_SCHEMA_VERSION = 1
# Bump whenever validation, transformation, or serialization behavior changes.
PIPELINE_VERSION = 13
PIPELINE_CHANGE_LOG_PATH = Path(__file__).with_name("alpaca_end_of_day_pipeline_change_log.json")
PIPELINE_RECEIPT_MANIFEST_FILENAME = "pipeline.manifest.json"
PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY = "verified"
INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION = "invalid_frozen_download_boundary"
RECORDED_PRICING_DATA_FAILURE_ACTION = "recorded_pricing_data_failure"
NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")
CSV_COLUMN_ALPACA_FIELD_PAIRS = (
    ("timestamp", "t"),
    ("open", "o"),
    ("high", "h"),
    ("low", "l"),
    ("close", "c"),
    ("volume", "v"),
    ("trade_count", "n"),
    ("vwap", "vw"),
)
DOWNLOAD_REQUEST_IDENTITY_FIELDS = (
    "symbol",
    "adjustment",
    "feed",
    "timeframe",
    "request_from",
    "request_to",
    "asof",
)


class EndOfDayPipelineError(RuntimeError):
    pass


class PricingDataIntegrityError(EndOfDayPipelineError):
    pass


class AlpacaDownloadIntegrityError(EndOfDayPipelineError):
    pass


@lru_cache(maxsize=1)
def validate_pipeline_change_log() -> list[dict]:
    try:
        with PIPELINE_CHANGE_LOG_PATH.open(encoding="utf-8") as change_log_file:
            change_log_entries = json.load(change_log_file)
    except (OSError, json.JSONDecodeError) as change_log_error:
        raise EndOfDayPipelineError(
            f"could not load {PIPELINE_CHANGE_LOG_PATH}: {change_log_error}"
        ) from change_log_error

    if not isinstance(change_log_entries, list):
        raise EndOfDayPipelineError("pipeline version changelog must be a JSON array")

    if len(change_log_entries) != PIPELINE_VERSION:
        raise EndOfDayPipelineError(
            "pipeline changelog must contain exactly "
            f"{PIPELINE_VERSION} entries; found {len(change_log_entries)}"
        )

    for implicit_pipeline_version, change_log_entry in enumerate(change_log_entries, start=1):
        if not isinstance(change_log_entry, dict):
            raise EndOfDayPipelineError(f"pipeline changelog entry {implicit_pipeline_version} is not an object")
        if "version" in change_log_entry:
            raise EndOfDayPipelineError(
                f"pipeline changelog entry {implicit_pipeline_version} must not contain a version key; "
                "its array position is the version"
            )
        modifications = change_log_entry.get("modifications")
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(modification, str) or not modification for modification in modifications)
        ):
            raise EndOfDayPipelineError(
                f"pipeline changelog version {implicit_pipeline_version} has no modifications"
            )
    return change_log_entries


@dataclass(frozen=True)
class CachedDownloadInspection:
    required_action: str
    download_manifest: dict | None = None
    diagnostic: str | None = None


def download_manifest_path(download_path: Path | str) -> Path:
    download_path = Path(download_path)
    return download_path.with_name(download_path.name + ".manifest.json")


def pipeline_receipt_manifest_path(download_path: Path | str) -> Path:
    """Return the processing-receipt manifest for a symbol directory."""
    return Path(download_path).parent / PIPELINE_RECEIPT_MANIFEST_FILENAME


def integrity_failure_manifest_path(download_path: Path | str) -> Path:
    """Return the failure-only integrity manifest for a symbol directory."""
    return Path(download_path).parent / "integrity_failure.manifest.json"


def _read_symbol_manifest_entries(
    symbol_manifest_path: Path,
    expected_symbol: str,
    expected_manifest_type: str,
    entry_collection_key: str,
) -> tuple[dict, dict[str, dict]] | None:
    """Read a validator-owned symbol manifest as entries keyed by data file."""
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


def _read_pipeline_receipts(
    receipt_manifest_path: Path, expected_symbol: str
) -> tuple[int, dict[str, dict]] | None:
    parsed_manifest = _read_symbol_manifest_entries(
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


def _read_integrity_failures(
    failure_manifest_path: Path, expected_symbol: str
) -> tuple[int, dict[str, dict]] | None:
    parsed_manifest = _read_symbol_manifest_entries(
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


def inspect_cached_download(download_path: Path | str, request_identity: dict) -> CachedDownloadInspection:
    """Respect download ownership, then choose cached use, local validation, or network work."""
    validate_pipeline_change_log()
    download_path = Path(download_path)
    sidecar_manifest_path = download_manifest_path(download_path)
    try:
        download_file_stat = download_path.stat()
    except FileNotFoundError:
        download_file_stat = None
    except OSError as download_stat_error:
        return CachedDownloadInspection(
            required_action="error",
            diagnostic=f"could not stat data file: {download_stat_error}",
        )
    try:
        sidecar_manifest_stat = sidecar_manifest_path.stat()
    except FileNotFoundError:
        sidecar_manifest_stat = None
    except OSError as manifest_stat_error:
        return CachedDownloadInspection(
            required_action="error",
            diagnostic=f"could not stat manifest: {manifest_stat_error}",
        )

    if download_file_stat is None and sidecar_manifest_stat is None:
        return CachedDownloadInspection(
            required_action="download",
            diagnostic="data and manifest are absent",
        )
    if download_file_stat is None:
        return CachedDownloadInspection(
            required_action="repair_required",
            diagnostic="data file is missing while its manifest exists",
        )
    if sidecar_manifest_stat is None:
        return CachedDownloadInspection(
            required_action="repair_required",
            diagnostic="manifest is missing while its data file exists",
        )

    try:
        with sidecar_manifest_path.open(encoding="utf-8") as sidecar_manifest_file:
            download_manifest = json.load(sidecar_manifest_file)
    except (OSError, json.JSONDecodeError) as manifest_load_error:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic=f"invalid manifest: {manifest_load_error}",
        )

    if not isinstance(download_manifest, dict):
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest is not an object",
        )
    manifest_schema_version = download_manifest.get("manifest_version", MANIFEST_SCHEMA_VERSION)
    if manifest_schema_version != MANIFEST_SCHEMA_VERSION:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest schema does not match",
        )
    publication_sha256_was_verified = download_manifest.get(PUBLICATION_SHA256_VERIFIED_MANIFEST_KEY)
    if publication_sha256_was_verified is not True:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest does not record publication-time SHA-256 verification",
        )
    if download_manifest.get("file") != download_path.name:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest filename does not match",
        )
    if any(
        download_manifest.get(field_name) != request_identity.get(field_name)
        for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS
    ):
        return CachedDownloadInspection(
            required_action="refresh_required",
            download_manifest=download_manifest,
            diagnostic="request identity differs from the authenticated download",
        )

    recorded_download_sha256 = download_manifest.get("sha256")
    if not isinstance(recorded_download_sha256, str) or len(recorded_download_sha256) != 64:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="invalid SHA-256 in manifest",
        )
    try:
        int(recorded_download_sha256, 16)
    except ValueError:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="invalid SHA-256 in manifest",
        )

    if not isinstance(download_manifest.get("rows"), int) or download_manifest["rows"] < 1:
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="invalid row count in manifest",
        )
    if not download_manifest.get("from") or not download_manifest.get("to"):
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            diagnostic="manifest has no date coverage",
        )

    if (
        download_manifest.get("bytes") != download_file_stat.st_size
        or download_manifest.get("mtime_ns") != download_file_stat.st_mtime_ns
    ):
        return CachedDownloadInspection(
            required_action=INVALID_FROZEN_DOWNLOAD_BOUNDARY_ACTION,
            download_manifest=download_manifest,
            diagnostic="download file stat no longer matches its authenticated manifest",
        )

    try:
        receipt_manifest_state = _read_pipeline_receipts(
            pipeline_receipt_manifest_path(download_path), request_identity["symbol"]
        )
    except EndOfDayPipelineError as receipt_manifest_error:
        return CachedDownloadInspection(
            required_action="error",
            download_manifest=download_manifest,
            diagnostic=str(receipt_manifest_error),
        )

    if receipt_manifest_state is None:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="pipeline manifest is absent",
        )
    receipt_pipeline_version, receipts_by_data_filename = receipt_manifest_state
    if receipt_pipeline_version != PIPELINE_VERSION:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic=(f"receipt manifest pipeline v{receipt_pipeline_version} != v{PIPELINE_VERSION}"),
        )
    matching_receipt = receipts_by_data_filename.get(download_path.name)
    if matching_receipt is None:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="no pipeline receipt exists for this download",
        )
    if matching_receipt.get("download_sha256") != recorded_download_sha256:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="the pipeline receipt belongs to a different download SHA-256",
        )
    try:
        failure_manifest_state = _read_integrity_failures(
            integrity_failure_manifest_path(download_path), request_identity["symbol"]
        )
    except EndOfDayPipelineError as failure_manifest_error:
        return CachedDownloadInspection(
            required_action="error",
            download_manifest=download_manifest,
            diagnostic=str(failure_manifest_error),
        )
    if failure_manifest_state is None:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic="integrity failure manifest is absent",
        )
    failure_pipeline_version, failures_by_data_filename = failure_manifest_state
    if failure_pipeline_version != PIPELINE_VERSION:
        return CachedDownloadInspection(
            required_action="repipeline",
            download_manifest=download_manifest,
            diagnostic=(f"failure manifest pipeline v{failure_pipeline_version} != v{PIPELINE_VERSION}"),
        )
    matching_failure = failures_by_data_filename.get(download_path.name)
    if matching_failure is None:
        return CachedDownloadInspection(required_action="cached", download_manifest=download_manifest)
    if (
        matching_failure.get("download_sha256") != recorded_download_sha256
        or matching_failure.get("pipeline_version") != PIPELINE_VERSION
    ):
        return CachedDownloadInspection(
            required_action="error",
            download_manifest=download_manifest,
            diagnostic=f"stale failure entry in {integrity_failure_manifest_path(download_path)}",
        )
    return CachedDownloadInspection(
        required_action=RECORDED_PRICING_DATA_FAILURE_ACTION,
        download_manifest=download_manifest,
        diagnostic=f"failures are recorded in {integrity_failure_manifest_path(download_path)}",
    )


def read_frozen_alpaca_daily_bars(download_path: Path | str) -> list[dict]:
    """Read a prior compressed CSV fully into memory for pipeline migration."""
    download_path = Path(download_path)
    expected_csv_columns = [csv_column for csv_column, _ in CSV_COLUMN_ALPACA_FIELD_PAIRS]
    alpaca_daily_bars = []
    try:
        with gzip.open(download_path, mode="rt", encoding="utf-8", newline="") as compressed_csv_file:
            csv_rows = csv.DictReader(compressed_csv_file)
            if csv_rows.fieldnames != expected_csv_columns:
                raise EndOfDayPipelineError(f"unexpected CSV fields in {download_path}: {csv_rows.fieldnames!r}")
            for row_number, csv_row in enumerate(csv_rows, start=1):
                if None in csv_row:
                    raise EndOfDayPipelineError(f"row {row_number}: unexpected extra CSV columns")
                alpaca_daily_bars.append(
                    {
                        alpaca_field: csv_row[csv_column]
                        for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS
                    }
                )
    except (OSError, UnicodeError, csv.Error) as compressed_csv_error:
        raise EndOfDayPipelineError(
            f"could not read {download_path}: {compressed_csv_error}"
        ) from compressed_csv_error

    return alpaca_daily_bars


def _parse_utc_timestamp(timestamp_text: object, field_description: str) -> datetime:
    if not isinstance(timestamp_text, str):
        raise EndOfDayPipelineError(f"{field_description} is not a timestamp string")
    normalized_timestamp_text = (
        timestamp_text[:-1] + "+00:00" if timestamp_text.endswith("Z") else timestamp_text
    )
    try:
        parsed_timestamp = datetime.fromisoformat(normalized_timestamp_text)
    except ValueError as invalid_timestamp_error:
        raise EndOfDayPipelineError(
            f"invalid {field_description}: {timestamp_text!r}"
        ) from invalid_timestamp_error
    if parsed_timestamp.tzinfo is None:
        raise EndOfDayPipelineError(f"{field_description} has no timezone: {timestamp_text!r}")
    return parsed_timestamp.astimezone(timezone.utc)


def _parse_finite_number(field_value: object, field_name: str, row_number: int) -> float:
    if isinstance(field_value, bool):
        raise EndOfDayPipelineError(f"row {row_number}: {field_name} is not numeric")
    try:
        numeric_value = float(field_value)
    except (TypeError, ValueError) as numeric_conversion_error:
        raise EndOfDayPipelineError(
            f"row {row_number}: {field_name} is not numeric"
        ) from numeric_conversion_error
    if not math.isfinite(numeric_value):
        raise EndOfDayPipelineError(f"row {row_number}: {field_name} is not finite")
    return numeric_value


def _validate_daily_bar(
    alpaca_bar: object,
    row_number: int,
    previous_bar_timestamp: datetime | None,
    request_start_timestamp: datetime,
    request_end_timestamp: datetime,
) -> tuple[dict, datetime, str]:
    if not isinstance(alpaca_bar, Mapping):
        raise EndOfDayPipelineError(f"row {row_number}: bar is not an object")

    missing_alpaca_fields = [
        alpaca_field for _, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS if alpaca_bar.get(alpaca_field) is None
    ]
    if missing_alpaca_fields:
        raise EndOfDayPipelineError(f"row {row_number}: missing fields: {', '.join(missing_alpaca_fields)}")

    bar_timestamp = _parse_utc_timestamp(alpaca_bar["t"], f"row {row_number} timestamp")
    if bar_timestamp < request_start_timestamp or bar_timestamp >= request_end_timestamp:
        raise EndOfDayPipelineError(f"row {row_number}: timestamp is outside the request range")
    if previous_bar_timestamp is not None and bar_timestamp <= previous_bar_timestamp:
        raise EndOfDayPipelineError(f"row {row_number}: timestamps are not strictly increasing")

    market_session_date = bar_timestamp.astimezone(NEW_YORK_TIME_ZONE).date()
    expected_bar_timestamp = datetime.combine(
        market_session_date, datetime_time.min, tzinfo=NEW_YORK_TIME_ZONE
    ).astimezone(timezone.utc)
    if bar_timestamp != expected_bar_timestamp:
        raise EndOfDayPipelineError(
            f"row {row_number}: daily timestamp {alpaca_bar['t']!r} is not New York session midnight"
        )

    open_price = _parse_finite_number(alpaca_bar["o"], "open", row_number)
    high_price = _parse_finite_number(alpaca_bar["h"], "high", row_number)
    low_price = _parse_finite_number(alpaca_bar["l"], "low", row_number)
    close_price = _parse_finite_number(alpaca_bar["c"], "close", row_number)
    volume_weighted_average_price = _parse_finite_number(alpaca_bar["vw"], "vwap", row_number)
    share_volume = _parse_finite_number(alpaca_bar["v"], "volume", row_number)
    trade_count = _parse_finite_number(alpaca_bar["n"], "trade_count", row_number)

    if min(open_price, high_price, low_price, close_price, volume_weighted_average_price) <= 0:
        raise EndOfDayPipelineError(f"row {row_number}: prices must be positive")
    if high_price < low_price:
        raise EndOfDayPipelineError(f"row {row_number}: high is below low")
    for price_name, price_value in (
        ("open", open_price),
        ("close", close_price),
        ("vwap", volume_weighted_average_price),
    ):
        if price_value < low_price or price_value > high_price:
            raise EndOfDayPipelineError(f"row {row_number}: {price_name} is outside the low/high range")
    if share_volume <= 0 or not share_volume.is_integer():
        raise EndOfDayPipelineError(f"row {row_number}: volume is not a positive integer")
    if trade_count <= 0 or not trade_count.is_integer():
        raise EndOfDayPipelineError(f"row {row_number}: trade_count is not a positive integer")
    if trade_count > share_volume:
        raise EndOfDayPipelineError(f"row {row_number}: trade_count exceeds volume")

    csv_row = {
        csv_column: alpaca_bar[alpaca_field] for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS
    }
    return csv_row, bar_timestamp, market_session_date.isoformat()


@dataclass(frozen=True)
class EndOfDayValidationResult:
    rows_seen: int
    first_bar_timestamp: object
    last_bar_timestamp: object
    observed_session_dates: tuple[str, ...]
    row_failures: tuple[dict, ...]
    dataset_failures: tuple[dict, ...]

    @property
    def has_failures(self):
        return bool(self.row_failures or self.dataset_failures)


def _file_sha256(file_path: Path | str) -> str:
    file_digest = hashlib.sha256()
    with Path(file_path).open("rb") as binary_file:
        for file_chunk in iter(lambda: binary_file.read(1024 * 1024), b""):
            file_digest.update(file_chunk)
    return file_digest.hexdigest()


def _create_temporary_sibling_path(destination_path: Path | str) -> Path:
    destination_path = Path(destination_path)
    file_descriptor, temporary_filename = tempfile.mkstemp(
        dir=destination_path.parent,
        prefix=f".{destination_path.name}.tmp.",
    )
    os.close(file_descriptor)
    return Path(temporary_filename)


def _write_json_atomically(destination_path: Path, document: dict, *, compact: bool) -> None:
    temporary_path = _create_temporary_sibling_path(destination_path)
    try:
        with temporary_path.open("w", encoding="utf-8") as json_file:
            if compact:
                json.dump(document, json_file, separators=(",", ":"))
            else:
                json.dump(document, json_file, indent=2)
            json_file.write("\n")
            json_file.flush()
            os.fsync(json_file.fileno())
        os.replace(temporary_path, destination_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def _terminate_subprocess(process: subprocess.Popen | None) -> None:
    if process is None:
        return
    if process.stdin is not None and not process.stdin.closed:
        process.stdin.close()
    if process.poll() is None:
        process.terminate()
    process.wait()
    if process.stderr is not None and not process.stderr.closed:
        process.stderr.close()


def _serializable_exact_bar(alpaca_bar: object) -> dict:
    if not isinstance(alpaca_bar, Mapping):
        return {"python_type": type(alpaca_bar).__name__, "repr": repr(alpaca_bar)}
    exact_bar_data = dict(alpaca_bar)
    try:
        json.dumps(exact_bar_data)
    except (TypeError, ValueError):
        return {
            str(field_name): field_value
            if isinstance(field_value, (str, int, float, bool)) or field_value is None
            else repr(field_value)
            for field_name, field_value in exact_bar_data.items()
        }
    return exact_bar_data


def _alpaca_bar_to_csv_row(alpaca_bar: object) -> dict:
    if not isinstance(alpaca_bar, Mapping):
        return {
            csv_column: repr(alpaca_bar) if csv_column == "timestamp" else ""
            for csv_column, _ in CSV_COLUMN_ALPACA_FIELD_PAIRS
        }
    return {
        csv_column: alpaca_bar.get(alpaca_field) for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS
    }


def _validate_download_request_identity(request_identity: dict) -> tuple[datetime, datetime]:
    missing_identity_fields = [
        field_name for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS if field_name not in request_identity
    ]
    if missing_identity_fields:
        raise EndOfDayPipelineError(f"missing identity fields: {', '.join(missing_identity_fields)}")
    request_start_timestamp = _parse_utc_timestamp(request_identity["request_from"], "request_from")
    request_end_timestamp = _parse_utc_timestamp(request_identity["request_to"], "request_to")
    if request_start_timestamp >= request_end_timestamp:
        raise EndOfDayPipelineError("request_from must be before request_to")
    return request_start_timestamp, request_end_timestamp


def _validate_end_of_day_bars(
    alpaca_daily_bars: tuple[object, ...] | list[object], request_identity: dict
) -> EndOfDayValidationResult:
    """Run the complete in-memory row and dataset validation pipeline."""
    request_start_timestamp, request_end_timestamp = _validate_download_request_identity(request_identity)
    rows_seen = 0
    first_bar_timestamp = None
    last_bar_timestamp = None
    previous_bar_timestamp = None
    observed_session_dates = []
    row_failures = []

    for rows_seen, alpaca_bar in enumerate(alpaca_daily_bars, start=1):
        if isinstance(alpaca_bar, Mapping):
            if first_bar_timestamp is None:
                first_bar_timestamp = alpaca_bar.get("t")
            last_bar_timestamp = alpaca_bar.get("t")

        try:
            _, bar_timestamp, observed_session_date = _validate_daily_bar(
                alpaca_bar,
                rows_seen,
                previous_bar_timestamp,
                request_start_timestamp,
                request_end_timestamp,
            )
        except EndOfDayPipelineError as row_validation_error:
            row_failures.append(
                {
                    "row_number": rows_seen,
                    "csv_line": rows_seen + 1,
                    "data": _serializable_exact_bar(alpaca_bar),
                    "validation_error": str(row_validation_error),
                }
            )
            if not isinstance(alpaca_bar, Mapping):
                continue
            try:
                bar_timestamp = _parse_utc_timestamp(alpaca_bar.get("t"), f"row {rows_seen} timestamp")
            except EndOfDayPipelineError:
                continue
            observed_session_date = bar_timestamp.astimezone(NEW_YORK_TIME_ZONE).date().isoformat()

        previous_bar_timestamp = bar_timestamp
        observed_session_dates.append(observed_session_date)

    dataset_failures = []
    if rows_seen == 0:
        dataset_failures.append({"validation_error": "response contained no bars"})
    elif not observed_session_dates:
        dataset_failures.append({"validation_error": "no row contained a usable timestamp for session coverage"})
    else:
        try:
            expected_session_coverage, missing_session_dates, unexpected_session_dates = (
                compare_observed_to_expected_sessions(
                    observed_session_dates,
                    observed_session_dates[0],
                    request_identity["request_to"],
                )
            )
        except MarketCalendarError as calendar_validation_error:
            dataset_failures.append({"validation_error": str(calendar_validation_error)})
        else:
            if tuple(observed_session_dates) != expected_session_coverage.session_dates:
                session_coverage_failure = {
                    "validation_error": "US market session coverage failed",
                    "missing_sessions": list(missing_session_dates),
                    "unexpected_sessions": list(unexpected_session_dates),
                }
                if not missing_session_dates and not unexpected_session_dates:
                    session_coverage_failure["ordering_error"] = "sessions are duplicated or out of order"
                dataset_failures.append(session_coverage_failure)

    return EndOfDayValidationResult(
        rows_seen=rows_seen,
        first_bar_timestamp=first_bar_timestamp,
        last_bar_timestamp=last_bar_timestamp,
        observed_session_dates=tuple(observed_session_dates),
        row_failures=tuple(row_failures),
        dataset_failures=tuple(dataset_failures),
    )


def _update_integrity_failure_manifest(
    download_path: Path,
    request_identity: dict,
    artifact_failure: dict | None,
    validation_timestamp: str,
) -> Path:
    failure_manifest_path = integrity_failure_manifest_path(download_path)
    existing_failure_state = _read_integrity_failures(failure_manifest_path, request_identity["symbol"])
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
    _write_json_atomically(failure_manifest_path, failure_manifest_document, compact=False)
    return failure_manifest_path


def _update_pipeline_receipt_manifest(
    download_path: Path,
    request_identity: dict,
    pipeline_receipt: dict,
    validation_timestamp: str,
) -> Path:
    receipt_manifest_path = pipeline_receipt_manifest_path(download_path)
    existing_receipt_state = _read_pipeline_receipts(receipt_manifest_path, request_identity["symbol"])
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
    _write_json_atomically(receipt_manifest_path, receipt_manifest_document, compact=False)
    return receipt_manifest_path


def _record_validation_result(
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
        failure_manifest_path = _update_integrity_failure_manifest(
            download_path,
            request_identity,
            artifact_failure,
            validation_timestamp,
        )
    else:
        failure_manifest_path = _update_integrity_failure_manifest(
            download_path,
            request_identity,
            None,
            validation_timestamp,
        )

    _update_pipeline_receipt_manifest(
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


def authenticate_frozen_download(download_path: Path | str, download_manifest: dict) -> str:
    """Authenticate a gzip against its download-boundary sidecar."""
    recorded_download_sha256 = download_manifest.get("sha256")
    observed_download_sha256 = _file_sha256(download_path)
    if observed_download_sha256 != recorded_download_sha256:
        raise AlpacaDownloadIntegrityError(
            f"{download_path}: Alpaca download SHA-256 mismatch "
            f"(manifest={recorded_download_sha256}, actual={observed_download_sha256}); no pricing/data "
            "integrity result was published and this data is not eligible for "
            "repair; aborting without replacing either boundary file"
        )
    return observed_download_sha256


def authenticate_and_revalidate_cached_download(
    download_path: Path | str, request_identity: dict, download_manifest: dict
) -> dict:
    """Authenticate, then validate a download without writing either download file."""
    validate_pipeline_change_log()
    download_path = Path(download_path)
    authenticate_frozen_download(download_path, download_manifest)
    frozen_alpaca_bars = read_frozen_alpaca_daily_bars(download_path)
    validation_result = _validate_end_of_day_bars(frozen_alpaca_bars, request_identity)
    return _record_validation_result(
        download_path,
        request_identity,
        download_manifest,
        validation_result,
    )


def validate_and_publish_alpaca_download(
    download_path: Path | str,
    downloaded_alpaca_bars: list[object] | tuple[object, ...],
    request_identity: dict,
    *,
    replacement_authorized: bool = False,
) -> dict:
    """Validate in memory, then own and publish the Alpaca download boundary."""
    validate_pipeline_change_log()
    _validate_download_request_identity(request_identity)
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
    validation_result = _validate_end_of_day_bars(downloaded_alpaca_bars, request_identity)

    download_path.parent.mkdir(parents=True, exist_ok=True)
    compressed_download_temporary_path = _create_temporary_sibling_path(download_path)
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
                    csv_writer.writerow(_alpaca_bar_to_csv_row(alpaca_bar))
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
        _terminate_subprocess(pigz_process)
        compressed_download_temporary_path.unlink(missing_ok=True)
        raise

    compressed_download_sha256 = _file_sha256(compressed_download_temporary_path)
    verified_compressed_download_sha256 = _file_sha256(compressed_download_temporary_path)
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
            _record_validation_result(
                download_path,
                request_identity,
                download_manifest,
                validation_result,
                raise_after_recording_failures=False,
            )
        except BaseException:
            compressed_download_temporary_path.unlink(missing_ok=True)
            raise

    sidecar_manifest_temporary_path = _create_temporary_sibling_path(sidecar_manifest_path)
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

    _record_validation_result(
        download_path,
        request_identity,
        download_manifest,
        validation_result,
    )
    return download_manifest


# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.

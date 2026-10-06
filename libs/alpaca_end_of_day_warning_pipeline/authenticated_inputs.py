import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from libs.alpaca_end_of_day_pipeline import (
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    PIPELINE_VERSION,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    download_manifest_path,
    inspect_cached_download,
    integrity_failure_manifest_path,
    pipeline_receipt_manifest_path,
    read_frozen_alpaca_daily_bars,
)
from libs.alpaca_end_of_day_security_correlation import (
    CORRELATION_VERSION,
    correlated_pipeline_path,
    security_correlation_manifest_path,
    uncorrelated_pipeline_path,
)
from libs.common.file_integrity import calculate_file_sha256

from .errors import WarningPipelineError
from .models import (
    AnalyzableDailyBar,
    AuthenticatedCorrelationOutputs,
    DownloadValidationView,
    WarningPipelineInputs,
)
from .settings import adjustment_actions

NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")
NUMERIC_FIELDS = {
    "open": "o",
    "high": "h",
    "low": "l",
    "close": "c",
    "volume": "v",
    "trade_count": "n",
    "vwap": "vw",
}


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise WarningPipelineError(f"could not read {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise WarningPipelineError(f"{description} is not a JSON object: {path}")
    return document


def _validate_safe_filename(filename: object, suffix: str, description: str) -> str:
    if not isinstance(filename, str) or Path(filename).name != filename or not filename.endswith(suffix):
        raise WarningPipelineError(f"unsafe or invalid {description}: {filename!r}")
    return filename


def _authenticate_correlation_branch(
    path: Path,
    output_receipt: object,
    *,
    symbol: str,
    generation_sha256: str,
    expected_manifest_type: str,
) -> tuple[dict[str, Any], str]:
    if not isinstance(output_receipt, dict) or output_receipt.get("file") != path.name:
        raise WarningPipelineError(f"correlation receipt does not bind {path.name}")
    try:
        output_stat = path.stat()
    except OSError as error:
        raise WarningPipelineError(f"correlation output is unavailable: {path}: {error}") from error
    if output_stat.st_size != output_receipt.get("bytes") or output_stat.st_mtime_ns != output_receipt.get(
        "mtime_ns"
    ):
        raise WarningPipelineError(f"correlation output stat does not match its receipt: {path}")
    observed_sha256 = calculate_file_sha256(path)
    if observed_sha256 != output_receipt.get("sha256"):
        raise WarningPipelineError(f"correlation output SHA-256 does not match its receipt: {path}")
    document = _read_json_object(path, "correlation output")
    expected_identity = {
        "manifest_type": expected_manifest_type,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "correlation_generation_sha256": generation_sha256,
    }
    for field_name, expected_value in expected_identity.items():
        if document.get(field_name) != expected_value:
            raise WarningPipelineError(
                f"correlation output {path} has unexpected {field_name}: {document.get(field_name)!r}"
            )
    return document, observed_sha256


def _authenticate_correlation_outputs(
    repository_root: Path,
    symbol: str,
) -> AuthenticatedCorrelationOutputs:
    receipt_path = security_correlation_manifest_path(repository_root, symbol)
    receipt = _read_json_object(receipt_path, "security-correlation receipt")
    if receipt.get("manifest_type") != "end_of_day_security_correlation":
        raise WarningPipelineError(f"invalid correlation receipt type: {receipt_path}")
    if receipt.get("correlation_version") != CORRELATION_VERSION:
        raise WarningPipelineError(
            f"correlation receipt v{receipt.get('correlation_version')!r} is not current v{CORRELATION_VERSION}"
        )
    if receipt.get("symbol") != symbol:
        raise WarningPipelineError(f"correlation receipt has wrong symbol: {receipt_path}")
    partition = receipt.get("partition")
    if not isinstance(partition, dict) or partition.get("row_partition_complete") is not True:
        raise WarningPipelineError("security correlation does not contain a complete row partition")
    generation_sha256 = receipt.get("correlation_generation_sha256")
    if not isinstance(generation_sha256, str) or len(generation_sha256) != 64:
        raise WarningPipelineError("security-correlation receipt has an invalid generation digest")
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise WarningPipelineError("security-correlation receipt has no outputs object")
    correlated, correlated_sha256 = _authenticate_correlation_branch(
        correlated_pipeline_path(repository_root, symbol),
        outputs.get("correlated"),
        symbol=symbol,
        generation_sha256=generation_sha256,
        expected_manifest_type="correlated_end_of_day_security_segments",
    )
    uncorrelated, uncorrelated_sha256 = _authenticate_correlation_branch(
        uncorrelated_pipeline_path(repository_root, symbol),
        outputs.get("uncorrelated"),
        symbol=symbol,
        generation_sha256=generation_sha256,
        expected_manifest_type="uncorrelated_end_of_day_security_segments",
    )
    return AuthenticatedCorrelationOutputs(
        receipt=receipt,
        correlated=correlated,
        uncorrelated=uncorrelated,
        correlated_sha256=correlated_sha256,
        uncorrelated_sha256=uncorrelated_sha256,
    )


def _validate_partition(correlation: AuthenticatedCorrelationOutputs) -> dict[str, dict[str, Any]]:
    source_downloads = correlation.receipt.get("source_downloads")
    if not isinstance(source_downloads, list):
        raise WarningPipelineError("security-correlation receipt has no source downloads")
    sources_by_filename: dict[str, dict[str, Any]] = {}
    for source in source_downloads:
        if not isinstance(source, dict):
            raise WarningPipelineError("security-correlation source download is not an object")
        filename = _validate_safe_filename(source.get("data_file"), ".csv.gz", "source filename")
        if filename in sources_by_filename:
            raise WarningPipelineError(f"duplicate security-correlation source: {filename}")
        if type(source.get("rows")) is not int or source["rows"] < 1:
            raise WarningPipelineError(f"invalid source row count for {filename}")
        sources_by_filename[filename] = source

    ranges_by_filename: dict[str, list[tuple[int, int]]] = {filename: [] for filename in sources_by_filename}
    branch_total = 0
    for branch_name, document in (
        ("correlated", correlation.correlated),
        ("uncorrelated", correlation.uncorrelated),
    ):
        segments = document.get("segments")
        if not isinstance(segments, list):
            raise WarningPipelineError(f"{branch_name} correlation output has no segment list")
        calculated_count = 0
        for segment in segments:
            if not isinstance(segment, dict):
                raise WarningPipelineError(f"{branch_name} correlation segment is not an object")
            filename = segment.get("data_file")
            source = sources_by_filename.get(filename)
            if source is None:
                raise WarningPipelineError(f"correlation segment refers to unknown source {filename!r}")
            row_start, row_end, row_count = (
                segment.get("row_start"),
                segment.get("row_end"),
                segment.get("row_count"),
            )
            if (
                type(row_start) is not int
                or type(row_end) is not int
                or type(row_count) is not int
                or row_start < 1
                or row_end < row_start
                or row_count != row_end - row_start + 1
                or row_end > source["rows"]
            ):
                raise WarningPipelineError(f"invalid row range in {branch_name} segment for {filename}")
            if segment.get("download_sha256") != source.get("download_sha256"):
                raise WarningPipelineError(f"segment/source SHA-256 mismatch for {filename}")
            if segment.get("adjustment") != source.get("adjustment"):
                raise WarningPipelineError(f"segment/source adjustment mismatch for {filename}")
            if branch_name == "correlated":
                canonical_security = segment.get("canonical_security")
                if not isinstance(canonical_security, dict) or not isinstance(
                    canonical_security.get("asset_id"), str
                ):
                    raise WarningPipelineError(f"correlated segment has no Alpaca asset UUID: {filename}")
            ranges_by_filename[filename].append((row_start, row_end))
            calculated_count += row_count
        if calculated_count != document.get("row_count"):
            raise WarningPipelineError(f"{branch_name} correlation row count is inconsistent")
        if len(segments) != document.get("segment_count"):
            raise WarningPipelineError(f"{branch_name} correlation segment count is inconsistent")
        branch_total += calculated_count

    for filename, source in sources_by_filename.items():
        expected_start = 1
        for row_start, row_end in sorted(ranges_by_filename[filename]):
            if row_start != expected_start:
                raise WarningPipelineError(f"correlation row partition has a gap or overlap in {filename}")
            expected_start = row_end + 1
        if expected_start != source["rows"] + 1:
            raise WarningPipelineError(f"correlation row partition does not exhaust {filename}")
    if branch_total != correlation.receipt["partition"].get("source_row_count"):
        raise WarningPipelineError("correlation branch totals do not match the receipt")
    return sources_by_filename


def _recorded_failed_rows(
    repository_root: Path,
    symbol: str,
) -> tuple[dict[str, set[int]], str]:
    symbol_path = repository_root / "data" / "end_of_day" / symbol
    failure_path = integrity_failure_manifest_path(symbol_path / "placeholder.csv.gz")
    document = _read_json_object(failure_path, "integrity-failure manifest")
    if (
        document.get("manifest_type") != "end_of_day_integrity_failures"
        or document.get("symbol") != symbol
        or document.get("pipeline_version") != PIPELINE_VERSION
    ):
        raise WarningPipelineError(f"integrity-failure manifest is not current: {failure_path}")
    failures = document.get("failures")
    if not isinstance(failures, list) or document.get("failure_count") != len(failures):
        raise WarningPipelineError(f"integrity-failure manifest is malformed: {failure_path}")
    failed_rows_by_filename: dict[str, set[int]] = {}
    for failure in failures:
        if not isinstance(failure, dict):
            raise WarningPipelineError(f"integrity-failure entry is malformed: {failure_path}")
        filename = _validate_safe_filename(failure.get("data_file"), ".csv.gz", "failed source filename")
        row_failures = failure.get("row_failures")
        if not isinstance(row_failures, list):
            raise WarningPipelineError(f"row failures are malformed for {filename}")
        failed_rows: set[int] = set()
        for row_failure in row_failures:
            row_number = row_failure.get("row_number") if isinstance(row_failure, dict) else None
            if type(row_number) is not int or row_number < 1:
                raise WarningPipelineError(f"invalid failed row number for {filename}")
            failed_rows.add(row_number)
        failed_rows_by_filename[filename] = failed_rows
    return failed_rows_by_filename, calculate_file_sha256(failure_path)


def _parse_timestamp(value: object, filename: str, row_number: int) -> datetime:
    if not isinstance(value, str):
        raise WarningPipelineError(f"{filename} row {row_number}: timestamp is not a string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise WarningPipelineError(f"{filename} row {row_number}: invalid timestamp {value!r}") from error
    if parsed.tzinfo is None:
        raise WarningPipelineError(f"{filename} row {row_number}: timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def _analyzable_bar(row: dict, filename: str, row_number: int, timestamp: datetime) -> AnalyzableDailyBar:
    values = {}
    for field_name, source_field in NUMERIC_FIELDS.items():
        try:
            value = Decimal(str(row[source_field]))
        except (KeyError, InvalidOperation) as error:
            raise WarningPipelineError(
                f"{filename} row {row_number}: invalid {field_name} in integrity-accepted row"
            ) from error
        if not value.is_finite() or value <= 0:
            raise WarningPipelineError(
                f"{filename} row {row_number}: nonpositive/nonfinite {field_name} in integrity-accepted row"
            )
        values[field_name] = value
    return AnalyzableDailyBar(
        row_number=row_number,
        timestamp_text=str(row["t"]),
        timestamp=timestamp,
        session_date=timestamp.astimezone(NEW_YORK_TIME_ZONE).date(),
        values=values,
    )


def _load_download_view(
    repository_root: Path,
    symbol: str,
    source: dict[str, Any],
    failed_rows: set[int],
) -> DownloadValidationView:
    symbol_directory = repository_root / "data" / "end_of_day" / symbol
    filename = source["data_file"]
    download_path = symbol_directory / filename
    manifest = _read_json_object(download_manifest_path(download_path), "download manifest")
    request_identity = {field_name: manifest.get(field_name) for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS}
    if any(not isinstance(value, str) or not value for value in request_identity.values()):
        raise WarningPipelineError(f"download manifest has incomplete request identity: {download_path}")
    inspection = inspect_cached_download(download_path, request_identity)
    if inspection.required_action not in {"cached", RECORDED_PRICING_DATA_FAILURE_ACTION}:
        raise WarningPipelineError(
            f"{download_path}: cannot validate market data in state {inspection.required_action!r}: "
            f"{inspection.diagnostic}"
        )
    if (
        manifest.get("sha256") != source.get("download_sha256")
        or manifest.get("rows") != source.get("rows")
        or manifest.get("adjustment") != source.get("adjustment")
    ):
        raise WarningPipelineError(f"download manifest does not match correlation source: {download_path}")
    actions = adjustment_actions(manifest.get("adjustment"))
    if actions is None:
        raise WarningPipelineError(f"download has unsupported adjustment label: {download_path}")
    before_stat = download_path.stat()
    if before_stat.st_size != manifest.get("bytes") or before_stat.st_mtime_ns != manifest.get("mtime_ns"):
        raise WarningPipelineError(f"download stat does not match its authenticated manifest: {download_path}")
    rows = read_frozen_alpaca_daily_bars(download_path)
    after_stat = download_path.stat()
    if (before_stat.st_size, before_stat.st_mtime_ns) != (after_stat.st_size, after_stat.st_mtime_ns):
        raise WarningPipelineError(f"download changed while it was being read: {download_path}")
    if len(rows) != source["rows"]:
        raise WarningPipelineError(f"decompressed row count does not match correlation source: {download_path}")

    timestamps = {}
    bars = {}
    excluded_rows = []
    previous_timestamp = None
    for row_number, row in enumerate(rows, start=1):
        timestamp = _parse_timestamp(row.get("t"), filename, row_number)
        if previous_timestamp is not None and timestamp <= previous_timestamp:
            raise WarningPipelineError(f"{filename}: timestamps are not strictly increasing")
        previous_timestamp = timestamp
        timestamps[row_number] = timestamp
        if row_number in failed_rows:
            excluded_rows.append({"row_number": row_number, "reason": "recorded_integrity_row_failure"})
            continue
        bars[row_number] = _analyzable_bar(row, filename, row_number, timestamp)

    return DownloadValidationView(
        data_file=filename,
        download_sha256=source["download_sha256"],
        adjustment=source["adjustment"],
        adjustment_actions=actions,
        manifest=manifest,
        source_row_count=source["rows"],
        timestamp_by_row_number=timestamps,
        bars_by_row_number=bars,
        excluded_rows=tuple(excluded_rows),
    )


def load_warning_pipeline_inputs(
    repository_root: Path,
    symbol: str,
) -> WarningPipelineInputs:
    correlation = _authenticate_correlation_outputs(repository_root, symbol)
    sources_by_filename = _validate_partition(correlation)
    failed_rows_by_filename, integrity_failure_manifest_sha256 = _recorded_failed_rows(repository_root, symbol)
    unexpected_failure_files = set(failed_rows_by_filename) - set(sources_by_filename)
    if unexpected_failure_files:
        raise WarningPipelineError(
            f"integrity failures refer to non-source files: {sorted(unexpected_failure_files)!r}"
        )
    download_views = tuple(
        _load_download_view(
            repository_root,
            symbol,
            source,
            failed_rows_by_filename.get(filename, set()),
        )
        for filename, source in sorted(sources_by_filename.items())
    )
    symbol_directory = repository_root / "data" / "end_of_day" / symbol
    pipeline_manifest_path = pipeline_receipt_manifest_path(symbol_directory / "placeholder.csv.gz")
    if not pipeline_manifest_path.is_file():
        raise WarningPipelineError(f"pipeline receipt manifest is unavailable: {pipeline_manifest_path}")
    return WarningPipelineInputs(
        symbol=symbol,
        source_downloads=tuple(sources_by_filename[name] for name in sorted(sources_by_filename)),
        download_views=download_views,
        correlation=correlation,
        pipeline_receipt_manifest_sha256=calculate_file_sha256(pipeline_manifest_path),
        integrity_failure_manifest_sha256=integrity_failure_manifest_sha256,
    )

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
from datetime import datetime, time as datetime_time, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

from us_market_calendar import (
    MarketCalendarError,
    calendar_sha256,
    compare_sessions,
    session_digest,
    session_coverage,
)


MANIFEST_SCHEMA_VERSION = 1
# Bump whenever validation, transformation, or serialization behavior changes.
PIPELINE_VERSION = 11
PIPELINE_CHANGE_LOG_PATH = Path(__file__).with_name(
    "pipeline_version_change_log.json"
)
NEW_YORK = ZoneInfo("America/New_York")
CSV_FIELDS = (
    ("timestamp", "t"),
    ("open", "o"),
    ("high", "h"),
    ("low", "l"),
    ("close", "c"),
    ("volume", "v"),
    ("trade_count", "n"),
    ("vwap", "vw"),
)
IDENTITY_FIELDS = (
    "symbol",
    "adjustment",
    "feed",
    "timeframe",
    "request_from",
    "request_to",
    "asof",
)


class PipelineError(RuntimeError):
    pass


class PipelineIntegrityError(PipelineError):
    pass


class DownloadIntegrityError(PipelineError):
    pass


@lru_cache(maxsize=1)
def validate_pipeline_change_log():
    try:
        with PIPELINE_CHANGE_LOG_PATH.open(encoding="utf-8") as source:
            entries = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        raise PipelineError(
            f"could not load {PIPELINE_CHANGE_LOG_PATH}: {error}"
        ) from error

    if not isinstance(entries, list):
        raise PipelineError("pipeline version changelog must be a JSON array")

    if len(entries) != PIPELINE_VERSION:
        raise PipelineError(
            "pipeline changelog must contain exactly "
            f"{PIPELINE_VERSION} entries; found {len(entries)}"
        )

    for version, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise PipelineError(f"pipeline changelog entry {version} is not an object")
        if "version" in entry:
            raise PipelineError(
                f"pipeline changelog entry {version} must not contain a version key; "
                "its array position is the version"
            )
        modifications = entry.get("modifications")
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(value, str) or not value for value in modifications)
        ):
            raise PipelineError(
                f"pipeline changelog version {version} has no modifications"
            )
    return entries


@dataclass(frozen=True)
class CacheInspection:
    action: str
    manifest: dict | None = None
    issue: str | None = None
    missing_sessions: tuple[str, ...] = ()
    unexpected_sessions: tuple[str, ...] = ()


def manifest_path(data_path):
    data_path = Path(data_path)
    return data_path.with_name(data_path.name + ".manifest.json")


def pipeline_manifest_path(data_path):
    """Return the processing-receipt manifest for a symbol directory."""
    return Path(data_path).parent / "pipeline.manifest.json"


def integrity_failure_manifest_path(data_path):
    """Return the failure-only integrity manifest for a symbol directory."""
    return Path(data_path).parent / "integrity_failure.manifest.json"


def manifest_pipeline_version(manifest):
    """Read the writer version retained in legacy download manifests."""
    return manifest.get("pipeline_version", manifest.get("version"))


def _read_symbol_artifacts(path, symbol, manifest_type, collection_name):
    """Read a validator-owned symbol manifest as entries keyed by data file."""
    try:
        with Path(path).open(encoding="utf-8") as source:
            report = json.load(source)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as error:
        raise PipelineError(f"could not load {path}: {error}") from error

    if not isinstance(report, dict):
        raise PipelineError(f"{path} is not a JSON object")
    if report.get("manifest_type") != manifest_type:
        raise PipelineError(f"{path} has an invalid manifest type")
    if report.get("manifest_version") != 1:
        raise PipelineError(f"{path} has an unsupported manifest version")
    if report.get("symbol") != symbol:
        raise PipelineError(f"{path} has the wrong symbol")

    artifacts = report.get(collection_name)
    if not isinstance(artifacts, list):
        raise PipelineError(f"{path} {collection_name} must be an array")
    by_name = {}
    for artifact in artifacts:
        if not isinstance(artifact, dict) or not isinstance(
            artifact.get("data_file"), str
        ):
            raise PipelineError(f"{path} contains an invalid artifact entry")
        name = artifact["data_file"]
        if name in by_name:
            raise PipelineError(f"{path} contains duplicate entry {name!r}")
        by_name[name] = artifact
    return report, by_name


def _read_pipeline_receipts(path, symbol):
    loaded = _read_symbol_artifacts(
        path,
        symbol,
        "end_of_day_pipeline_receipts",
        "receipts",
    )
    if loaded is None:
        return None
    report, receipts = loaded
    version = report.get("pipeline_version")
    if version is None:
        # Read the v9 layout once so it can migrate through the normal stale
        # pipeline path; v10 writes the version only at manifest scope.
        legacy_versions = {
            receipt.get("pipeline_version") for receipt in receipts.values()
        }
        if len(legacy_versions) == 1:
            version = legacy_versions.pop()
    if type(version) is not int or version < 1:
        raise PipelineError(f"{path} has an invalid pipeline version")
    return version, receipts


def _read_integrity_failures(path, symbol):
    loaded = _read_symbol_artifacts(
        path,
        symbol,
        "end_of_day_integrity_failures",
        "failures",
    )
    if loaded is None:
        return None
    report, failures = loaded
    version = report.get("pipeline_version")
    if type(version) is not int or version < 1:
        raise PipelineError(f"{path} has an invalid pipeline version")
    return version, failures


def inspect_cached_artifact(data_path, identity):
    """Respect download ownership, then choose cached or local validation."""
    validate_pipeline_change_log()
    data_path = Path(data_path)
    path = manifest_path(data_path)
    try:
        data_stat = data_path.stat()
    except FileNotFoundError:
        data_stat = None
    except OSError as error:
        return CacheInspection(action="error", issue=f"could not stat data file: {error}")
    try:
        manifest_stat = path.stat()
    except FileNotFoundError:
        manifest_stat = None
    except OSError as error:
        return CacheInspection(action="error", issue=f"could not stat manifest: {error}")

    if data_stat is None and manifest_stat is None:
        return CacheInspection(action="download", issue="data and manifest are absent")
    if data_stat is None:
        return CacheInspection(
            action="repair_required",
            issue="data file is missing while its manifest exists",
        )
    if manifest_stat is None:
        return CacheInspection(
            action="repair_required",
            issue="manifest is missing while its data file exists",
        )

    try:
        with path.open(encoding="utf-8") as source:
            manifest = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        return CacheInspection(
            action="download_integrity_failed", issue=f"invalid manifest: {error}"
        )

    if not isinstance(manifest, dict):
        return CacheInspection(
            action="download_integrity_failed", issue="manifest is not an object"
        )
    manifest_version = manifest.get("manifest_version", MANIFEST_SCHEMA_VERSION)
    if manifest_version != MANIFEST_SCHEMA_VERSION:
        return CacheInspection(
            action="download_integrity_failed",
            issue="manifest schema does not match",
        )
    if manifest.get("verified") is not True:
        return CacheInspection(
            action="download_integrity_failed", issue="manifest is not verified"
        )
    if manifest.get("file") != data_path.name:
        return CacheInspection(
            action="download_integrity_failed",
            issue="manifest filename does not match",
        )
    if any(manifest.get(field) != identity.get(field) for field in IDENTITY_FIELDS):
        return CacheInspection(
            action="refresh_required",
            manifest=manifest,
            issue="request identity differs from the authenticated download",
        )

    digest = manifest.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        return CacheInspection(
            action="download_integrity_failed", issue="invalid SHA-256 in manifest"
        )
    try:
        int(digest, 16)
    except ValueError:
        return CacheInspection(
            action="download_integrity_failed", issue="invalid SHA-256 in manifest"
        )

    if not isinstance(manifest.get("rows"), int) or manifest["rows"] < 1:
        return CacheInspection(
            action="download_integrity_failed", issue="invalid row count in manifest"
        )
    if not manifest.get("from") or not manifest.get("to"):
        return CacheInspection(
            action="download_integrity_failed", issue="manifest has no date coverage"
        )

    if (
        manifest.get("bytes") != data_stat.st_size
        or manifest.get("mtime_ns") != data_stat.st_mtime_ns
    ):
        return CacheInspection(
            action="download_integrity_failed",
            manifest=manifest,
            issue="download file stat no longer matches its authenticated manifest",
        )

    try:
        receipt_state = _read_pipeline_receipts(
            pipeline_manifest_path(data_path), identity["symbol"]
        )
    except PipelineError as error:
        return CacheInspection(action="error", manifest=manifest, issue=str(error))

    if receipt_state is None:
        return CacheInspection(
            action="repipeline",
            manifest=manifest,
            issue="pipeline manifest is absent",
        )
    receipt_version, receipts = receipt_state
    if receipt_version != PIPELINE_VERSION:
        return CacheInspection(
            action="repipeline",
            manifest=manifest,
            issue=(
                f"receipt manifest pipeline v{receipt_version} "
                f"!= v{PIPELINE_VERSION}"
            ),
        )
    receipt = receipts.get(data_path.name)
    if receipt is None:
        return CacheInspection(
            action="repipeline",
            manifest=manifest,
            issue="no pipeline receipt exists for this download",
        )
    if receipt.get("download_sha256") != digest:
        return CacheInspection(
            action="repipeline",
            manifest=manifest,
            issue="the pipeline receipt belongs to a different download SHA-256",
        )
    try:
        failure_state = _read_integrity_failures(
            integrity_failure_manifest_path(data_path), identity["symbol"]
        )
    except PipelineError as error:
        return CacheInspection(action="error", manifest=manifest, issue=str(error))
    if failure_state is None:
        return CacheInspection(
            action="repipeline",
            manifest=manifest,
            issue="integrity failure manifest is absent",
        )
    failure_version, failures = failure_state
    if failure_version != PIPELINE_VERSION:
        return CacheInspection(
            action="repipeline",
            manifest=manifest,
            issue=(
                f"failure manifest pipeline v{failure_version} "
                f"!= v{PIPELINE_VERSION}"
            ),
        )
    failure = failures.get(data_path.name)
    if failure is None:
        return CacheInspection(action="cached", manifest=manifest)
    if (
        failure.get("download_sha256") != digest
        or failure.get("pipeline_version") != PIPELINE_VERSION
    ):
        return CacheInspection(
            action="error",
            manifest=manifest,
            issue=(
                f"stale failure entry in {integrity_failure_manifest_path(data_path)}"
            ),
        )
    return CacheInspection(
        action="integrity_failed",
        manifest=manifest,
        issue=f"failures are recorded in {integrity_failure_manifest_path(data_path)}",
    )


def read_bars_csv_gz(data_path):
    """Read a prior compressed CSV fully into memory for pipeline migration."""
    data_path = Path(data_path)
    expected_fields = [name for name, _ in CSV_FIELDS]
    bars = []
    try:
        with gzip.open(data_path, mode="rt", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != expected_fields:
                raise PipelineError(
                    f"unexpected CSV fields in {data_path}: {reader.fieldnames!r}"
                )
            for row_number, row in enumerate(reader, start=1):
                if None in row:
                    raise PipelineError(
                        f"row {row_number}: unexpected extra CSV columns"
                    )
                bars.append(
                    {source_name: row[name] for name, source_name in CSV_FIELDS}
                )
    except (OSError, UnicodeError, csv.Error) as error:
        raise PipelineError(f"could not read {data_path}: {error}") from error

    return bars


def _parse_timestamp(value, label):
    if not isinstance(value, str):
        raise PipelineError(f"{label} is not a timestamp string")
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise PipelineError(f"invalid {label}: {value!r}") from error
    if parsed.tzinfo is None:
        raise PipelineError(f"{label} has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _finite_number(value, field, row_number):
    if isinstance(value, bool):
        raise PipelineError(f"row {row_number}: {field} is not numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise PipelineError(f"row {row_number}: {field} is not numeric") from error
    if not math.isfinite(number):
        raise PipelineError(f"row {row_number}: {field} is not finite")
    return number


def _validate_bar(bar, row_number, previous_timestamp, start, end):
    if not isinstance(bar, Mapping):
        raise PipelineError(f"row {row_number}: bar is not an object")

    missing = [source for _, source in CSV_FIELDS if bar.get(source) is None]
    if missing:
        raise PipelineError(f"row {row_number}: missing fields: {', '.join(missing)}")

    timestamp = _parse_timestamp(bar["t"], f"row {row_number} timestamp")
    if timestamp < start or timestamp >= end:
        raise PipelineError(f"row {row_number}: timestamp is outside the request range")
    if previous_timestamp is not None and timestamp <= previous_timestamp:
        raise PipelineError(f"row {row_number}: timestamps are not strictly increasing")

    session_date = timestamp.astimezone(NEW_YORK).date()
    expected_timestamp = datetime.combine(
        session_date, datetime_time.min, tzinfo=NEW_YORK
    ).astimezone(timezone.utc)
    if timestamp != expected_timestamp:
        raise PipelineError(
            f"row {row_number}: daily timestamp {bar['t']!r} is not "
            "New York session midnight"
        )

    open_price = _finite_number(bar["o"], "open", row_number)
    high = _finite_number(bar["h"], "high", row_number)
    low = _finite_number(bar["l"], "low", row_number)
    close = _finite_number(bar["c"], "close", row_number)
    vwap = _finite_number(bar["vw"], "vwap", row_number)
    volume = _finite_number(bar["v"], "volume", row_number)
    trade_count = _finite_number(bar["n"], "trade_count", row_number)

    if min(open_price, high, low, close, vwap) <= 0:
        raise PipelineError(f"row {row_number}: prices must be positive")
    if high < low:
        raise PipelineError(f"row {row_number}: high is below low")
    for field, value in (
        ("open", open_price),
        ("close", close),
        ("vwap", vwap),
    ):
        if value < low or value > high:
            raise PipelineError(
                f"row {row_number}: {field} is outside the low/high range"
            )
    if volume <= 0 or not volume.is_integer():
        raise PipelineError(f"row {row_number}: volume is not a positive integer")
    if trade_count <= 0 or not trade_count.is_integer():
        raise PipelineError(f"row {row_number}: trade_count is not a positive integer")
    if trade_count > volume:
        raise PipelineError(f"row {row_number}: trade_count exceeds volume")

    row = {name: bar[source] for name, source in CSV_FIELDS}
    return row, timestamp, session_date.isoformat()


@dataclass(frozen=True)
class PipelineResult:
    rows_seen: int
    first: object
    last: object
    session_dates: tuple[str, ...]
    row_failures: tuple[dict, ...]
    dataset_failures: tuple[dict, ...]

    @property
    def has_failures(self):
        return bool(self.row_failures or self.dataset_failures)


def _sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _temporary_path(destination):
    destination = Path(destination)
    descriptor, name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.tmp.",
    )
    os.close(descriptor)
    return Path(name)


def _write_json_atomic(path, value, *, compact):
    partial_path = _temporary_path(path)
    try:
        with partial_path.open("w", encoding="utf-8") as output:
            if compact:
                json.dump(value, output, separators=(",", ":"))
            else:
                json.dump(value, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial_path, path)
    except BaseException:
        partial_path.unlink(missing_ok=True)
        raise


def _stop_process(process):
    if process is None:
        return
    if process.stdin is not None and not process.stdin.closed:
        process.stdin.close()
    if process.poll() is None:
        process.terminate()
    process.wait()
    if process.stderr is not None and not process.stderr.closed:
        process.stderr.close()


def _exact_row_data(bar):
    if not isinstance(bar, Mapping):
        return {"python_type": type(bar).__name__, "repr": repr(bar)}
    row = dict(bar)
    try:
        json.dumps(row)
    except (TypeError, ValueError):
        return {
            str(key): value
            if isinstance(value, (str, int, float, bool)) or value is None
            else repr(value)
            for key, value in row.items()
        }
    return row


def _csv_row_as_found(bar):
    if not isinstance(bar, Mapping):
        return {
            name: repr(bar) if name == "timestamp" else ""
            for name, _ in CSV_FIELDS
        }
    return {name: bar.get(source) for name, source in CSV_FIELDS}


def _validate_identity(identity):
    missing = [field for field in IDENTITY_FIELDS if field not in identity]
    if missing:
        raise PipelineError(f"missing identity fields: {', '.join(missing)}")
    start = _parse_timestamp(identity["request_from"], "request_from")
    end = _parse_timestamp(identity["request_to"], "request_to")
    if start >= end:
        raise PipelineError("request_from must be before request_to")
    return start, end


def _run_pipeline(bars, identity, write_row=None):
    """Run the one validation pipeline, optionally emitting each row unchanged."""
    start, end = _validate_identity(identity)
    count = 0
    first = None
    last = None
    previous_timestamp = None
    session_dates = []
    row_failures = []

    for count, bar in enumerate(bars, start=1):
        if write_row is not None:
            write_row(_csv_row_as_found(bar))
        if isinstance(bar, Mapping):
            first = first if first is not None else bar.get("t")
            last = bar.get("t")

        try:
            _, timestamp, session_date = _validate_bar(
                bar, count, previous_timestamp, start, end
            )
        except PipelineError as error:
            row_failures.append(
                {
                    "row_number": count,
                    "csv_line": count + 1,
                    "data": _exact_row_data(bar),
                    "validation_error": str(error),
                }
            )
            if not isinstance(bar, Mapping):
                continue
            try:
                timestamp = _parse_timestamp(
                    bar.get("t"), f"row {count} timestamp"
                )
            except PipelineError:
                continue
            session_date = timestamp.astimezone(NEW_YORK).date().isoformat()

        previous_timestamp = timestamp
        session_dates.append(session_date)

    dataset_failures = []
    if count == 0:
        dataset_failures.append({"validation_error": "response contained no bars"})
    elif not session_dates:
        dataset_failures.append(
            {
                "validation_error": (
                    "no row contained a usable timestamp for session coverage"
                )
            }
        )
    else:
        try:
            expected, missing, unexpected = compare_sessions(
                session_dates, session_dates[0], identity["request_to"]
            )
        except MarketCalendarError as error:
            dataset_failures.append({"validation_error": str(error)})
        else:
            if tuple(session_dates) != expected.dates:
                failure = {
                    "validation_error": "US market session coverage failed",
                    "missing_sessions": list(missing),
                    "unexpected_sessions": list(unexpected),
                }
                if not missing and not unexpected:
                    failure["ordering_error"] = (
                        "sessions are duplicated or out of order"
                    )
                dataset_failures.append(failure)

    return PipelineResult(
        rows_seen=count,
        first=first,
        last=last,
        session_dates=tuple(session_dates),
        row_failures=tuple(row_failures),
        dataset_failures=tuple(dataset_failures),
    )


def _update_failure_manifest(data_path, identity, failure, updated_at):
    path = integrity_failure_manifest_path(data_path)
    failure_state = _read_integrity_failures(path, identity["symbol"])
    if failure_state is None or failure_state[0] != PIPELINE_VERSION:
        failures = {}
    else:
        _, failures = failure_state
    name = Path(data_path).name
    if failure is None:
        failures.pop(name, None)
    else:
        failures[name] = failure

    ordered = [failures[name] for name in sorted(failures)]
    report = {
        "manifest_type": "end_of_day_integrity_failures",
        "manifest_version": 1,
        "symbol": identity["symbol"],
        "pipeline_version": PIPELINE_VERSION,
        "updated_at": updated_at,
        "failure_count": len(ordered),
        "failures": ordered,
    }
    _write_json_atomic(path, report, compact=False)
    return path


def _update_pipeline_manifest(data_path, identity, receipt, updated_at):
    path = pipeline_manifest_path(data_path)
    receipt_state = _read_pipeline_receipts(path, identity["symbol"])
    if receipt_state is None or receipt_state[0] != PIPELINE_VERSION:
        receipts = {}
    else:
        _, receipts = receipt_state
    receipts[Path(data_path).name] = receipt
    ordered = [receipts[name] for name in sorted(receipts)]
    report = {
        "manifest_type": "end_of_day_pipeline_receipts",
        "manifest_version": 1,
        "symbol": identity["symbol"],
        "pipeline_version": PIPELINE_VERSION,
        "updated_at": updated_at,
        "receipt_count": len(ordered),
        "receipts": ordered,
    }
    _write_json_atomic(path, report, compact=False)
    return path


def _record_pipeline_result(
    data_path, identity, download_manifest, result, *, raise_on_failure=True
):
    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    receipt = {
        "data_file": Path(data_path).name,
        "download_manifest": manifest_path(data_path).name,
        "adjustment": identity["adjustment"],
        "download_sha256": download_manifest["sha256"],
        "processed_at": checked_at,
        "rows_seen": result.rows_seen,
        "sessions_seen": len(result.session_dates),
        "from": result.first,
        "to": result.last,
    }

    if result.has_failures:
        failure = {
            **receipt,
            "pipeline_version": PIPELINE_VERSION,
            "row_failure_count": len(result.row_failures),
            "dataset_failure_count": len(result.dataset_failures),
            "row_failures": list(result.row_failures),
            "dataset_failures": list(result.dataset_failures),
        }
        failure_path = _update_failure_manifest(
            data_path, identity, failure, checked_at
        )
    else:
        failure_path = _update_failure_manifest(
            data_path, identity, None, checked_at
        )

    _update_pipeline_manifest(data_path, identity, receipt, checked_at)

    if result.has_failures and raise_on_failure:
        raise PipelineIntegrityError(
            f"{data_path}: pricing/data integrity failed; the authenticated "
            f"download was not changed; details: {failure_path}"
        )
    return receipt


def verify_download_file(data_path, download_manifest):
    """Authenticate a gzip against its download-boundary sidecar."""
    expected_digest = download_manifest.get("sha256")
    actual_digest = _sha256(data_path)
    if actual_digest != expected_digest:
        raise DownloadIntegrityError(
            f"{data_path}: Alpaca download SHA-256 mismatch "
            f"(manifest={expected_digest}, actual={actual_digest}); no pricing/data "
            "integrity result was published and this data is not eligible for "
            "repair; aborting without replacing either boundary file"
        )
    return actual_digest


def validate_cached_download(data_path, identity, download_manifest):
    """Authenticate, then validate a download without writing either download file."""
    validate_pipeline_change_log()
    verify_download_file(data_path, download_manifest)
    bars = read_bars_csv_gz(data_path)
    result = _run_pipeline(bars, identity)
    return _record_pipeline_result(data_path, identity, download_manifest, result)


def write_bars_csv_gz(data_path, bars, identity, *, allow_replace=False):
    """Validate in memory, then own and publish the Alpaca download boundary."""
    validate_pipeline_change_log()
    _validate_identity(identity)
    data_path = Path(data_path)
    path = manifest_path(data_path)
    data_exists = data_path.exists()
    manifest_exists = path.exists()
    if (data_exists or manifest_exists) and not allow_replace:
        raise PipelineError(
            f"refusing to replace the Alpaca download boundary for {data_path} "
            "without an authorized refresh or repair"
        )

    # Nothing compressed reaches disk until the complete response has traversed
    # the same row and dataset integrity pipeline used for cached downloads.
    bars = tuple(bars)
    result = _run_pipeline(bars, identity)

    data_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = _temporary_path(data_path)
    process = None
    try:
        with partial_path.open("wb") as compressed_output:
            process = subprocess.Popen(
                ["pigz", "-5", "-c"],
                stdin=subprocess.PIPE,
                stdout=compressed_output,
                stderr=subprocess.PIPE,
            )
            if process.stdin is None or process.stderr is None:
                raise PipelineError("could not open pigz streams")

            csv_output = io.TextIOWrapper(
                process.stdin, encoding="utf-8", newline="", write_through=True
            )
            try:
                writer = csv.DictWriter(
                    csv_output, fieldnames=[name for name, _ in CSV_FIELDS]
                )
                writer.writeheader()
                for bar in bars:
                    writer.writerow(_csv_row_as_found(bar))
            finally:
                csv_output.close()

            stderr = process.stderr.read().decode("utf-8", "replace").strip()
            process.stderr.close()
            return_code = process.wait()
            if return_code != 0:
                raise PipelineError(
                    f"pigz failed with status {return_code}: {stderr or 'no details'}"
                )
            compressed_output.flush()
            os.fsync(compressed_output.fileno())
    except BaseException:
        _stop_process(process)
        partial_path.unlink(missing_ok=True)
        raise

    digest = _sha256(partial_path)
    if _sha256(partial_path) != digest:
        partial_path.unlink(missing_ok=True)
        raise PipelineError("compressed file failed SHA-256 verification")

    data_stat = partial_path.stat()
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    manifest = {
        "manifest_version": MANIFEST_SCHEMA_VERSION,
        "boundary": "alpaca_download",
        "pipeline_version": PIPELINE_VERSION,
        "verified": True,
        "file": data_path.name,
        "sha256": digest,
        "bytes": data_stat.st_size,
        "mtime_ns": data_stat.st_mtime_ns,
        "rows": result.rows_seen,
        "sessions": len(result.session_dates),
        "session_sha256": session_digest(result.session_dates),
        "calendar_sha256": calendar_sha256(),
        "from": result.first,
        "to": result.last,
        **{field: identity[field] for field in IDENTITY_FIELDS},
        "created_at": created_at,
    }

    # Failed data may be published only after its failure evidence and pipeline
    # receipt are durable. A successful run needs no integrity-failure entry.
    if result.has_failures:
        try:
            _record_pipeline_result(
                data_path,
                identity,
                manifest,
                result,
                raise_on_failure=False,
            )
        except BaseException:
            partial_path.unlink(missing_ok=True)
            raise

    manifest_partial_path = _temporary_path(path)
    try:
        with manifest_partial_path.open("w", encoding="utf-8") as output:
            json.dump(manifest, output, separators=(",", ":"))
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(partial_path, data_path)
        os.replace(manifest_partial_path, path)
    except BaseException:
        partial_path.unlink(missing_ok=True)
        manifest_partial_path.unlink(missing_ok=True)
        raise

    if result.has_failures:
        raise PipelineIntegrityError(
            f"{data_path}: pricing/data integrity failed; the authenticated "
            f"download was preserved; details: "
            f"{integrity_failure_manifest_path(data_path)}"
        )

    _record_pipeline_result(data_path, identity, manifest, result)
    return manifest

# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.

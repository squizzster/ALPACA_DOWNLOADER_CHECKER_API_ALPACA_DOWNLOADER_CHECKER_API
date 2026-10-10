import json
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from libs.common.us_equity_market_session_calendar import (
    MarketCalendarError,
    expected_market_session_coverage,
    us_market_hours_sha256,
)

from .artifact_paths import security_correlation_directory
from .errors import SecurityCorrelationError

VERIFIED_SESSION_EXCLUSIONS_FILENAME = "verified_session_exclusions.json"
VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE = "verified_market_session_exclusions"
VERIFIED_SESSION_EXCLUSIONS_VERSION = 1
FULL_SESSION_SUSPENSION = "full_session_suspension"
SESSION_EXCLUSIONS_CORRELATION_VERSION = 12


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _parse_date(value: object, field_name: str) -> date:
    if not isinstance(value, str):
        raise SecurityCorrelationError(f"verified session exclusion has invalid {field_name}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise SecurityCorrelationError(
            f"verified session exclusion has invalid {field_name}: {value!r}"
        ) from error


def _parse_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise SecurityCorrelationError(f"verified session exclusion has invalid {field_name}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise SecurityCorrelationError(
            f"verified session exclusion has invalid {field_name}: {value!r}"
        ) from error
    if parsed.tzinfo is None:
        raise SecurityCorrelationError(f"verified session exclusion {field_name} has no timezone")
    return parsed


def _validate_source_descriptor(source: object) -> dict[str, Any]:
    if not isinstance(source, dict):
        raise SecurityCorrelationError("verified session exclusion has no source descriptor")
    required_fields = {
        "authority",
        "url",
        "retrieved_at",
        "http_status",
        "content_type",
        "file",
        "sha256",
        "bytes",
    }
    if set(source) != required_fields:
        raise SecurityCorrelationError("verified session exclusion source fields are invalid")
    url = source.get("url")
    if (
        not isinstance(source.get("authority"), str)
        or not source["authority"]
        or not isinstance(url, str)
        or urlparse(url).scheme != "https"
        or not urlparse(url).netloc
        or not isinstance(source.get("content_type"), str)
        or not source["content_type"]
        or source.get("http_status") != 200
        or not isinstance(source.get("file"), str)
        or not _is_sha256(source.get("sha256"))
        or type(source.get("bytes")) is not int
        or source["bytes"] <= 0
    ):
        raise SecurityCorrelationError("verified session exclusion source descriptor is invalid")
    _parse_timestamp(source.get("retrieved_at"), "source retrieved_at")
    return source


def _validate_event_structure(event: object) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise SecurityCorrelationError("verified session exclusion event is not an object")
    required_fields = {
        "event_type",
        "alpaca_asset_id",
        "cusip",
        "exchange",
        "start_date_inclusive",
        "end_date_exclusive",
        "halt_timestamp",
        "resume_timestamp",
        "session_count",
        "session_dates_sha256",
        "source",
    }
    if set(event) != required_fields or event.get("event_type") != FULL_SESSION_SUSPENSION:
        raise SecurityCorrelationError("verified session exclusion event fields are invalid")
    for field_name in ("alpaca_asset_id", "cusip", "exchange"):
        if not isinstance(event.get(field_name), str) or not event[field_name]:
            raise SecurityCorrelationError(f"verified session exclusion has invalid {field_name}")
    start_date = _parse_date(event.get("start_date_inclusive"), "start_date_inclusive")
    end_date = _parse_date(event.get("end_date_exclusive"), "end_date_exclusive")
    halt_timestamp = _parse_timestamp(event.get("halt_timestamp"), "halt_timestamp")
    resume_timestamp = _parse_timestamp(event.get("resume_timestamp"), "resume_timestamp")
    if (
        start_date >= end_date
        or halt_timestamp.date() != start_date
        or resume_timestamp.date() != end_date
        or halt_timestamp >= resume_timestamp
        or type(event.get("session_count")) is not int
        or event["session_count"] <= 0
        or not _is_sha256(event.get("session_dates_sha256"))
    ):
        raise SecurityCorrelationError("verified session exclusion boundaries are invalid")
    _validate_source_descriptor(event.get("source"))
    return event


def validate_verified_session_exclusions(
    value: object,
    symbol: str,
) -> dict[str, Any]:
    """Validate the digest-bound representation stored in correlation and master artifacts."""
    if not isinstance(value, dict):
        raise SecurityCorrelationError("verified session exclusions are not an object")
    required_fields = {
        "manifest_type",
        "manifest_version",
        "symbol",
        "input_file",
        "input_sha256",
        "calendar_sha256",
        "events",
    }
    events = value.get("events")
    if (
        set(value) != required_fields
        or value.get("manifest_type") != VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE
        or value.get("manifest_version") != VERIFIED_SESSION_EXCLUSIONS_VERSION
        or value.get("symbol") != symbol
        or not _is_sha256(value.get("calendar_sha256"))
        or not isinstance(events, list)
    ):
        raise SecurityCorrelationError("verified session exclusions are invalid")
    if events:
        if value.get("input_file") != VERIFIED_SESSION_EXCLUSIONS_FILENAME or not _is_sha256(
            value.get("input_sha256")
        ):
            raise SecurityCorrelationError("verified session exclusion input is invalid")
    elif value.get("input_file") is not None or value.get("input_sha256") is not None:
        raise SecurityCorrelationError("empty verified session exclusions name an input")
    validated_events = [_validate_event_structure(event) for event in events]
    canonical_events = sorted(
        validated_events,
        key=lambda event: json.dumps(event, sort_keys=True, separators=(",", ":")),
    )
    if events != canonical_events or len(
        {json.dumps(event, sort_keys=True, separators=(",", ":")) for event in events}
    ) != len(events):
        raise SecurityCorrelationError("verified session exclusions are not uniquely ordered")
    return value


def _authenticated_source_file(
    symbol_directory: Path,
    source: dict[str, Any],
) -> None:
    relative_path = Path(source["file"])
    if (
        relative_path.is_absolute()
        or any(part in {"", ".", ".."} for part in relative_path.parts)
        or relative_path.suffix.lower() not in {".html", ".htm", ".json", ".pdf", ".txt"}
    ):
        raise SecurityCorrelationError(f"verified session exclusion source has unsafe path: {source['file']!r}")
    path = symbol_directory / relative_path
    try:
        path.resolve(strict=False).relative_to(symbol_directory.resolve())
        before_stat = path.stat()
        content = path.read_bytes()
        after_stat = path.stat()
    except (OSError, ValueError) as error:
        raise SecurityCorrelationError(
            f"could not authenticate verified session exclusion source {path}: {error}"
        ) from error
    if (before_stat.st_size, before_stat.st_mtime_ns) != (
        after_stat.st_size,
        after_stat.st_mtime_ns,
    ):
        raise SecurityCorrelationError(f"verified session exclusion source changed while read: {path}")
    if len(content) != source["bytes"] or sha256(content).hexdigest() != source["sha256"]:
        raise SecurityCorrelationError(
            f"verified session exclusion source failed SHA-256 authentication: {path}"
        )


def verified_exclusion_session_dates(event: dict[str, Any]) -> tuple[str, ...]:
    """Return the exact regular sessions covered by one authenticated exclusion."""
    _validate_event_structure(event)
    try:
        coverage = expected_market_session_coverage(
            event["start_date_inclusive"],
            event["end_date_exclusive"],
        )
    except MarketCalendarError as error:
        raise SecurityCorrelationError(
            f"verified session exclusion exceeds the market calendar: {error}"
        ) from error
    if (
        coverage.session_count != event["session_count"]
        or coverage.session_dates_sha256 != event["session_dates_sha256"]
    ):
        raise SecurityCorrelationError("verified session exclusion does not match its market-calendar sessions")
    return coverage.session_dates


def read_verified_session_exclusions(
    repository_root: Path | str,
    symbol: str,
) -> dict[str, Any]:
    """Read and authenticate optional venue-session evidence for one source symbol."""
    symbol_directory = security_correlation_directory(repository_root, symbol)
    manifest_path = symbol_directory / VERIFIED_SESSION_EXCLUSIONS_FILENAME
    if not manifest_path.exists():
        return {
            "manifest_type": VERIFIED_SESSION_EXCLUSIONS_MANIFEST_TYPE,
            "manifest_version": VERIFIED_SESSION_EXCLUSIONS_VERSION,
            "symbol": symbol,
            "input_file": None,
            "input_sha256": None,
            "calendar_sha256": us_market_hours_sha256(),
            "events": [],
        }
    try:
        serialized = manifest_path.read_bytes()
        document = json.loads(serialized)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(
            f"could not read verified session exclusions {manifest_path}: {error}"
        ) from error
    if not isinstance(document, dict):
        raise SecurityCorrelationError(f"verified session exclusions are not an object: {manifest_path}")
    normalized = {
        **document,
        "input_file": VERIFIED_SESSION_EXCLUSIONS_FILENAME,
        "input_sha256": sha256(serialized).hexdigest(),
    }
    validate_verified_session_exclusions(normalized, symbol)
    if normalized["calendar_sha256"] != us_market_hours_sha256():
        raise SecurityCorrelationError("verified session exclusions use a different market calendar")
    for event in normalized["events"]:
        verified_exclusion_session_dates(event)
        _authenticated_source_file(symbol_directory, event["source"])
    return normalized

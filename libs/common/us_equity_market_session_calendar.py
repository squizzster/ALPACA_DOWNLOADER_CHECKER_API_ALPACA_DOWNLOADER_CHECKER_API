import hashlib
import json
from bisect import bisect_left
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

US_MARKET_HOURS_FILE_PATH = Path(__file__).with_name("us_market_hours.json")


class MarketCalendarError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExpectedMarketSessionCoverage:
    session_dates: tuple[str, ...]
    session_dates_sha256: str

    @property
    def session_count(self) -> int:
        return len(self.session_dates)

    @property
    def last_session_date(self) -> str | None:
        # Pricing cutoff selection relies on this endpoint and its empty-range None result.
        return self.session_dates[-1] if self.session_dates else None


def _validated_iso_date_prefix(date_or_timestamp_text: object, field_description: str) -> str:
    if not isinstance(date_or_timestamp_text, str) or len(date_or_timestamp_text) < 10:
        raise MarketCalendarError(f"invalid {field_description}: {date_or_timestamp_text!r}")
    iso_date_text = date_or_timestamp_text[:10]
    try:
        date.fromisoformat(iso_date_text)
    except ValueError as invalid_date_error:
        raise MarketCalendarError(f"invalid {field_description}: {iso_date_text!r}") from invalid_date_error
    return iso_date_text


def market_session_dates_sha256(session_dates: Iterable[str]) -> str:
    session_dates_digest = hashlib.sha256()
    for session_date in session_dates:
        session_dates_digest.update(session_date.encode("ascii"))
        session_dates_digest.update(b"\n")
    return session_dates_digest.hexdigest()


@lru_cache(maxsize=1)
def _load_us_market_session_dates_and_sha256() -> tuple[tuple[str, ...], str]:
    try:
        calendar_file_bytes = US_MARKET_HOURS_FILE_PATH.read_bytes()
        market_session_records = json.loads(calendar_file_bytes)
    except (OSError, json.JSONDecodeError) as calendar_load_error:
        raise MarketCalendarError(
            f"could not load {US_MARKET_HOURS_FILE_PATH}: {calendar_load_error}"
        ) from calendar_load_error

    if not isinstance(market_session_records, list) or not market_session_records:
        raise MarketCalendarError(f"{US_MARKET_HOURS_FILE_PATH} contains no sessions")

    session_dates = []
    for row_number, market_session_record in enumerate(market_session_records, start=1):
        if not isinstance(market_session_record, dict):
            raise MarketCalendarError(f"calendar row {row_number} is not an object")
        session_dates.append(
            _validated_iso_date_prefix(market_session_record.get("date"), f"calendar row {row_number}")
        )

    if session_dates != sorted(set(session_dates)):
        raise MarketCalendarError("calendar dates are not strictly increasing and unique")

    return tuple(session_dates), hashlib.sha256(calendar_file_bytes).hexdigest()


def us_market_hours_sha256() -> str:
    return _load_us_market_session_dates_and_sha256()[1]


@lru_cache(maxsize=256)
def expected_market_session_coverage(
    start_date_or_timestamp: str, end_date_or_timestamp: str
) -> ExpectedMarketSessionCoverage:
    """Return authoritative US sessions from start through exclusive end."""
    start_date = _validated_iso_date_prefix(start_date_or_timestamp, "session range start")
    end_date = _validated_iso_date_prefix(end_date_or_timestamp, "session range end")
    if start_date >= end_date:
        raise MarketCalendarError("session range start must be before end")

    session_dates, _ = _load_us_market_session_dates_and_sha256()
    first_supported = date.fromisoformat(session_dates[0])
    last_supported = date.fromisoformat(session_dates[-1])
    parsed_start = date.fromisoformat(start_date)
    parsed_end = date.fromisoformat(end_date)
    if parsed_start < first_supported or parsed_end > last_supported + timedelta(days=1):
        raise MarketCalendarError(
            f"requested range {start_date}..{end_date} exceeds calendar coverage "
            f"{session_dates[0]}..{session_dates[-1]}"
        )

    left = bisect_left(session_dates, start_date)
    right = bisect_left(session_dates, end_date)
    expected_session_dates = session_dates[left:right]
    return ExpectedMarketSessionCoverage(
        session_dates=expected_session_dates,
        session_dates_sha256=market_session_dates_sha256(expected_session_dates),
    )


def compare_observed_to_expected_sessions(
    observed_session_dates: Sequence[str],
    start_date_or_timestamp: str,
    end_date_or_timestamp: str,
) -> tuple[ExpectedMarketSessionCoverage, tuple[str, ...], tuple[str, ...]]:
    expected_coverage = expected_market_session_coverage(start_date_or_timestamp, end_date_or_timestamp)
    observed_dates = tuple(observed_session_dates)
    observed_date_set = set(observed_dates)
    expected_date_set = set(expected_coverage.session_dates)
    missing_session_dates = tuple(
        session_date for session_date in expected_coverage.session_dates if session_date not in observed_date_set
    )
    unexpected_session_dates = tuple(
        session_date for session_date in observed_dates if session_date not in expected_date_set
    )
    return expected_coverage, missing_session_dates, unexpected_session_dates

import hashlib
import json
from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path


CALENDAR_PATH = Path(__file__).with_name("us_market_hours.json")


class MarketCalendarError(RuntimeError):
    pass


@dataclass(frozen=True)
class SessionCoverage:
    dates: tuple[str, ...]
    sha256: str

    @property
    def count(self):
        return len(self.dates)

    @property
    def first(self):
        return self.dates[0] if self.dates else None

    @property
    def last(self):
        return self.dates[-1] if self.dates else None


def _date_part(value, label):
    if not isinstance(value, str) or len(value) < 10:
        raise MarketCalendarError(f"invalid {label}: {value!r}")
    value = value[:10]
    try:
        date.fromisoformat(value)
    except ValueError as error:
        raise MarketCalendarError(f"invalid {label}: {value!r}") from error
    return value


def session_digest(session_dates):
    digest = hashlib.sha256()
    for session_date in session_dates:
        digest.update(session_date.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


@lru_cache(maxsize=1)
def _calendar_data():
    try:
        raw = CALENDAR_PATH.read_bytes()
        records = json.loads(raw)
    except (OSError, json.JSONDecodeError) as error:
        raise MarketCalendarError(f"could not load {CALENDAR_PATH}: {error}") from error

    if not isinstance(records, list) or not records:
        raise MarketCalendarError(f"{CALENDAR_PATH} contains no sessions")

    session_dates = []
    for row_number, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise MarketCalendarError(f"calendar row {row_number} is not an object")
        session_dates.append(_date_part(record.get("date"), f"calendar row {row_number}"))

    if session_dates != sorted(set(session_dates)):
        raise MarketCalendarError("calendar dates are not strictly increasing and unique")

    return tuple(session_dates), hashlib.sha256(raw).hexdigest()


def calendar_sha256():
    return _calendar_data()[1]


@lru_cache(maxsize=256)
def session_coverage(start, end):
    """Return authoritative US sessions from start through exclusive end."""
    start_date = _date_part(start, "session range start")
    end_date = _date_part(end, "session range end")
    if start_date >= end_date:
        raise MarketCalendarError("session range start must be before end")

    session_dates, _ = _calendar_data()
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
    dates = session_dates[left:right]
    return SessionCoverage(dates=dates, sha256=session_digest(dates))


def compare_sessions(actual_dates, start, end):
    expected = session_coverage(start, end)
    actual = tuple(actual_dates)
    actual_set = set(actual)
    expected_set = set(expected.dates)
    missing = tuple(value for value in expected.dates if value not in actual_set)
    unexpected = tuple(value for value in actual if value not in expected_set)
    return expected, missing, unexpected

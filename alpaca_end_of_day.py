"""Public, read-only Python library for online end-of-day market data.

Consumer code should import this module and no module below ``libs``. The
implementation authenticates the engine's stored artifacts, then exposes only
the published online universe, public identities, and daily histories.
"""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any, BinaryIO, TextIO
from zoneinfo import ZoneInfo

from libs.alpaca_end_of_day_reader import EndOfDayReaderError
from libs.alpaca_end_of_day_reader import (
    read_alpaca_end_of_day_security_history as _read_authenticated_history,
)
from libs.alpaca_end_of_day_security_correlation import (
    SecurityCorrelationError,
    authenticate_security_master,
    is_master_id,
)
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

API_VERSION = 1
API_CHANGE_LOG_PATH = Path(__file__).resolve().parent / "resources" / "alpaca_end_of_day_api_change_log.json"
ADJUSTMENTS = tuple(ALPACA_ADJUSTMENT_MODES)
FLAT_ONLINE_CSV_COLUMNS = (
    "date",
    "unique_id",
    "ticker",
    "name",
    "cusip",
    "exchange",
    "asset_class",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "raw_volume",
    "raw_trade_count",
    "raw_vwap",
    "all_adjusted_open",
    "all_adjusted_high",
    "all_adjusted_low",
    "all_adjusted_close",
    "all_adjusted_volume",
    "all_adjusted_trade_count",
    "all_adjusted_vwap",
)
_DEFAULT_LIBRARY_PATH = Path(__file__).resolve().parent
_NEW_YORK_TIME_ZONE = ZoneInfo("America/New_York")
_FLAT_CSV_SORT_CHUNK_ROWS = 100_000
_CATALOG_CHANGED_MESSAGE = (
    "the data catalog changed after this library snapshot was opened; create a new DataLibrary"
)


class DataLibraryError(RuntimeError):
    """Raised when the public library cannot authenticate or return data."""


class PublicIdUnavailableError(DataLibraryError):
    """Raised when a public ID is not available in the online snapshot."""


class CatalogChangedError(DataLibraryError):
    """Raised rather than combining results from different catalog generations."""


@dataclass(frozen=True, slots=True)
class ExternalIdentifier:
    """One typed external identifier attached to a security identity."""

    kind: str
    value: str

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "value": self.value}


@dataclass(frozen=True, slots=True)
class Security:
    """Public identity and coverage metadata for one online security."""

    public_id: str
    tickers: tuple[str, ...]
    names: tuple[str, ...]
    cusips: tuple[str, ...]
    external_identifiers: tuple[ExternalIdentifier, ...]
    exchanges: tuple[str, ...]
    asset_classes: tuple[str, ...]
    first_session: date
    last_session: date
    adjustments: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible public representation."""
        return {
            "public_id": self.public_id,
            "tickers": list(self.tickers),
            "names": list(self.names),
            "cusips": list(self.cusips),
            "external_identifiers": [identifier.to_dict() for identifier in self.external_identifiers],
            "exchanges": list(self.exchanges),
            "asset_classes": list(self.asset_classes),
            "first_session": self.first_session.isoformat(),
            "last_session": self.last_session.isoformat(),
            "adjustments": list(self.adjustments),
        }


@dataclass(frozen=True, slots=True)
class TickerResolution:
    """One observed date range linking an exact ticker to an online public ID."""

    ticker: str
    public_id: str
    names: tuple[str, ...]
    first_session: date
    last_session: date
    adjustments: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "public_id": self.public_id,
            "names": list(self.names),
            "first_session": self.first_session.isoformat(),
            "last_session": self.last_session.isoformat(),
            "adjustments": list(self.adjustments),
        }


@dataclass(frozen=True, slots=True)
class DailyBar:
    """One exact end-of-day market-session observation."""

    session: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    trade_count: int
    vwap: Decimal

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible exact values without float conversion."""
        return {
            "session": self.session.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": self.volume,
            "trade_count": self.trade_count,
            "vwap": str(self.vwap),
        }


@dataclass(frozen=True, slots=True)
class History:
    """One adjustment history for an online public security."""

    security: Security
    adjustment: str
    start: date | None
    end: date | None
    bars: tuple[DailyBar, ...]
    generation: str

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSON-compatible public result."""
        return {
            "api_version": API_VERSION,
            "generation": self.generation,
            "security": self.security.to_dict(),
            "adjustment": self.adjustment,
            "range": {
                "start": self.start.isoformat() if self.start is not None else None,
                "end": self.end.isoformat() if self.end is not None else None,
                "inclusive": True,
            },
            "bar_count": len(self.bars),
            "bars": [bar.to_dict() for bar in self.bars],
        }


@dataclass(frozen=True, slots=True)
class Catalog:
    """Identity and size of one authenticated public-universe snapshot."""

    generation: str
    published_at: datetime
    online_count: int
    adjustments: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "api_version": API_VERSION,
            "generation": self.generation,
            "published_at": self.published_at.isoformat().replace("+00:00", "Z"),
            "online_count": self.online_count,
            "adjustments": list(self.adjustments),
        }


@dataclass(frozen=True, slots=True)
class FlatCsvExport:
    """Receipt for one completed online-universe flat CSV export."""

    generation: str
    online_security_count: int
    exported_security_count: int
    row_count: int
    start: date | None
    end: date | None
    columns: tuple[str, ...] = FLAT_ONLINE_CSV_COLUMNS

    def to_dict(self) -> dict[str, Any]:
        return {
            "api_version": API_VERSION,
            "generation": self.generation,
            "online_security_count": self.online_security_count,
            "exported_security_count": self.exported_security_count,
            "row_count": self.row_count,
            "range": {
                "start": self.start.isoformat() if self.start is not None else None,
                "end": self.end.isoformat() if self.end is not None else None,
                "inclusive": True,
            },
            "columns": list(self.columns),
        }


@dataclass(frozen=True, slots=True)
class _SessionIdentity:
    ticker: str
    name: str
    cusip: str
    exchange: str
    asset_class: str


@lru_cache(maxsize=1)
def _validate_api_change_log() -> None:
    try:
        entries = json.loads(API_CHANGE_LOG_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DataLibraryError(f"could not load public API changelog: {error}") from error
    if not isinstance(entries, list) or len(entries) != API_VERSION:
        found = len(entries) if isinstance(entries, list) else "non-array"
        raise DataLibraryError(f"public API changelog must contain exactly {API_VERSION} entries; found {found}")
    for implicit_version, entry in enumerate(entries, start=1):
        modifications = entry.get("modifications") if isinstance(entry, dict) else None
        if isinstance(entry, dict) and "version" in entry:
            raise DataLibraryError(f"public API changelog entry {implicit_version} contains a version key")
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise DataLibraryError(f"public API changelog entry {implicit_version} has no modifications")


def _canonical_public_id(public_id: str) -> str:
    if not isinstance(public_id, str):
        raise ValueError("public ID must be a string")
    canonical_id = public_id.lower()
    if not is_master_id(canonical_id):
        raise ValueError(f"invalid public ID: {public_id!r}")
    return canonical_id


def _string_tuple(record: dict[str, Any], field_name: str) -> tuple[str, ...]:
    values = record[field_name]
    if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
        raise DataLibraryError(f"authenticated security has invalid {field_name}")
    return tuple(values)


def _external_identifiers(record: dict[str, Any]) -> tuple[ExternalIdentifier, ...]:
    identifiers = record["figi_identifiers"]
    if not isinstance(identifiers, list):
        raise DataLibraryError("authenticated security has invalid external identifiers")
    result = []
    for identifier in identifiers:
        if (
            not isinstance(identifier, dict)
            or not isinstance(identifier.get("type"), str)
            or not identifier["type"]
            or not isinstance(identifier.get("value"), str)
            or not identifier["value"]
        ):
            raise DataLibraryError("authenticated security has invalid external identifiers")
        result.append(ExternalIdentifier(kind=identifier["type"], value=identifier["value"]))
    return tuple(result)


def _security_from_record(record: dict[str, Any]) -> Security:
    try:
        if record["online"] is not True:
            raise PublicIdUnavailableError("public ID is not available")
        return Security(
            public_id=record["master_id"],
            tickers=_string_tuple(record, "tickers"),
            names=_string_tuple(record, "names"),
            cusips=_string_tuple(record, "cusips"),
            external_identifiers=_external_identifiers(record),
            exchanges=_string_tuple(record, "exchanges"),
            asset_classes=_string_tuple(record, "asset_classes"),
            first_session=date.fromisoformat(record["first_observed_session_date"]),
            last_session=date.fromisoformat(record["last_observed_session_date"]),
            adjustments=_string_tuple(record, "available_adjustments"),
        )
    except PublicIdUnavailableError:
        raise
    except (KeyError, TypeError, ValueError) as error:
        raise DataLibraryError("authenticated security metadata does not satisfy the public contract") from error


def _ticker_resolutions_from_record(
    record: dict[str, Any],
    security: Security,
) -> tuple[TickerResolution, ...]:
    ranges = record.get("observed_history_ranges")
    if not isinstance(ranges, list):
        raise DataLibraryError("authenticated security has invalid observed history ranges")
    resolutions = set()
    try:
        for observed_range in ranges:
            if not isinstance(observed_range, dict):
                raise TypeError("observed history range is not an object")
            ticker = observed_range["ticker"]
            if not isinstance(ticker, str) or not ticker:
                raise TypeError("observed history range has no ticker")
            resolutions.add(
                TickerResolution(
                    ticker=ticker,
                    public_id=security.public_id,
                    names=security.names,
                    first_session=date.fromisoformat(observed_range["first_session_date"]),
                    last_session=date.fromisoformat(observed_range["last_session_date"]),
                    adjustments=_string_tuple(observed_range, "adjustments"),
                )
            )
    except (KeyError, TypeError, ValueError) as error:
        raise DataLibraryError("authenticated security has invalid observed history ranges") from error
    return tuple(
        sorted(
            resolutions,
            key=lambda item: (item.ticker, item.first_session, item.last_session, item.public_id),
        )
    )


def _exact_decimal(bar: dict[str, Any], field_name: str) -> Decimal:
    try:
        return Decimal(bar[field_name])
    except (KeyError, TypeError, InvalidOperation) as error:
        raise DataLibraryError(f"authenticated bar has invalid {field_name}") from error


def _exact_integer(bar: dict[str, Any], field_name: str) -> int:
    value = _exact_decimal(bar, field_name)
    if value != value.to_integral_value():
        raise DataLibraryError(f"authenticated bar has non-integral {field_name}")
    return int(value)


def _daily_bar_from_record(record: dict[str, Any]) -> DailyBar:
    try:
        timestamp_text = record["timestamp"]
        if not isinstance(timestamp_text, str):
            raise TypeError("timestamp is not a string")
        normalized_timestamp = timestamp_text[:-1] + "+00:00" if timestamp_text.endswith("Z") else timestamp_text
        timestamp = datetime.fromisoformat(normalized_timestamp)
        if timestamp.tzinfo is None:
            raise ValueError("timestamp has no time zone")
        session = timestamp.astimezone(_NEW_YORK_TIME_ZONE).date()
    except (KeyError, TypeError, ValueError) as error:
        raise DataLibraryError("authenticated bar has an invalid timestamp") from error
    return DailyBar(
        session=session,
        open=_exact_decimal(record, "open"),
        high=_exact_decimal(record, "high"),
        low=_exact_decimal(record, "low"),
        close=_exact_decimal(record, "close"),
        volume=_exact_integer(record, "volume"),
        trade_count=_exact_integer(record, "trade_count"),
        vwap=_exact_decimal(record, "vwap"),
    )


def _optional_date(value: date | str | None, field_name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a date, not a datetime")
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ValueError(f"{field_name} must use YYYY-MM-DD format") from error
    raise ValueError(f"{field_name} must be a date, YYYY-MM-DD string, or None")


def _normalized_search_text(value: str) -> str:
    return " ".join(value.split()).casefold()


def _security_search_rank(security: Security, query: str) -> tuple[int, int, int, str, str] | None:
    searchable_values = (
        *((0, value) for value in security.tickers),
        *((1, value) for value in security.names),
        *((2, value) for value in security.cusips),
        *((3, identifier.value) for identifier in security.external_identifiers),
        (4, security.public_id),
    )
    ranks = []
    for field_rank, value in searchable_values:
        normalized_value = _normalized_search_text(value)
        position = normalized_value.find(query)
        if position < 0:
            continue
        match_rank = 0 if normalized_value == query else 1 if position == 0 else 2
        ranks.append((match_rank, field_rank, position))
    if not ranks:
        return None
    display_name = security.names[0] if security.names else security.tickers[0]
    return (*min(ranks), display_name.casefold(), security.public_id)


def _joined_identity_values(values: Iterator[str]) -> str:
    return " | ".join(sorted(set(values)))


def _identity_date(value: object, field_name: str) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise DataLibraryError(f"authenticated identity has invalid {field_name}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise DataLibraryError(f"authenticated identity has invalid {field_name}") from error


@dataclass(frozen=True, slots=True)
class _DatedIdentity:
    ticker: str
    start: date | None
    end: date | None
    names: tuple[str, ...]
    cusips: tuple[str, ...]
    exchanges: tuple[str, ...]
    asset_classes: tuple[str, ...]

    def contains(self, session: date) -> bool:
        return (self.start is None or session >= self.start) and (self.end is None or session < self.end)


@dataclass(frozen=True, slots=True)
class _ObservedTickerRange:
    ticker: str
    first_session: date
    last_session: date

    def contains(self, session: date) -> bool:
        return self.first_session <= session <= self.last_session


class _IdentityTimeline:
    def __init__(self, record: dict[str, Any], security: Security) -> None:
        self._security = security
        self._dated_identities: list[_DatedIdentity] = []
        self._observed_ranges: list[_ObservedTickerRange] = []
        self._identities_by_ticker: dict[str, list[_DatedIdentity]] = {}
        try:
            identities = record["alpaca_identities"]
            observed_ranges = record["observed_history_ranges"]
            if not isinstance(identities, list) or not isinstance(observed_ranges, list):
                raise TypeError("identity timelines are not arrays")
            for identity in identities:
                if not isinstance(identity, dict):
                    raise TypeError("identity is not an object")
                tickers = _string_tuple(identity, "tickers")
                names = _string_tuple(identity, "names")
                cusips = _string_tuple(identity, "cusips")
                exchanges = _string_tuple(identity, "exchanges")
                asset_classes = _string_tuple(identity, "asset_classes")
                undated = _DatedIdentity(
                    ticker="",
                    start=None,
                    end=None,
                    names=names,
                    cusips=cusips,
                    exchanges=exchanges,
                    asset_classes=asset_classes,
                )
                for ticker in tickers:
                    self._identities_by_ticker.setdefault(ticker, []).append(undated)
                periods = identity["ticker_ownership_periods"]
                if not isinstance(periods, list):
                    raise TypeError("ticker ownership periods are not an array")
                for period in periods:
                    if not isinstance(period, dict):
                        raise TypeError("ticker ownership period is not an object")
                    ticker = period["ticker"]
                    if not isinstance(ticker, str) or not ticker:
                        raise TypeError("ticker ownership period has no ticker")
                    period_cusip = period.get("cusip")
                    dated = _DatedIdentity(
                        ticker=ticker,
                        start=_identity_date(period.get("start_date_inclusive"), "ticker start date"),
                        end=_identity_date(period.get("end_date_exclusive"), "ticker end date"),
                        names=names,
                        cusips=(period_cusip,) if isinstance(period_cusip, str) and period_cusip else cusips,
                        exchanges=exchanges,
                        asset_classes=asset_classes,
                    )
                    self._dated_identities.append(dated)
            for observed_range in observed_ranges:
                if not isinstance(observed_range, dict):
                    raise TypeError("observed ticker range is not an object")
                ticker = observed_range["ticker"]
                if not isinstance(ticker, str) or not ticker:
                    raise TypeError("observed ticker range has no ticker")
                self._observed_ranges.append(
                    _ObservedTickerRange(
                        ticker=ticker,
                        first_session=date.fromisoformat(observed_range["first_session_date"]),
                        last_session=date.fromisoformat(observed_range["last_session_date"]),
                    )
                )
        except DataLibraryError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise DataLibraryError("authenticated identity does not contain a usable ticker timeline") from error

    def for_session(self, session: date) -> _SessionIdentity:
        active_identities = [identity for identity in self._dated_identities if identity.contains(session)]
        matching_observed_ranges = [
            observed_range for observed_range in self._observed_ranges if observed_range.contains(session)
        ]
        observed_tickers = {observed_range.ticker for observed_range in matching_observed_ranges}
        preferred_observed_ticker = None
        if len(observed_tickers) == 1:
            preferred_observed_ticker = next(iter(observed_tickers))
        elif observed_tickers:
            latest_session_by_ticker = {
                ticker: max(
                    observed_range.last_session
                    for observed_range in matching_observed_ranges
                    if observed_range.ticker == ticker
                )
                for ticker in observed_tickers
            }
            latest_session = max(latest_session_by_ticker.values())
            continuing_tickers = {
                ticker
                for ticker, last_session in latest_session_by_ticker.items()
                if last_session == latest_session
            }
            if len(continuing_tickers) == 1:
                preferred_observed_ticker = next(iter(continuing_tickers))
        active_tickers = {identity.ticker for identity in active_identities}
        if len(active_tickers) > 1 and preferred_observed_ticker is not None:
            narrowed = [
                identity for identity in active_identities if identity.ticker == preferred_observed_ticker
            ]
            narrowed_tickers = {identity.ticker for identity in narrowed}
            if len(narrowed_tickers) == 1:
                active_identities = narrowed
                active_tickers = narrowed_tickers
        if len(active_tickers) == 1:
            ticker = next(iter(active_tickers))
            metadata = [identity for identity in active_identities if identity.ticker == ticker]
        else:
            if len(active_tickers) > 1:
                raise DataLibraryError(
                    f"online security {self._security.public_id} has ambiguous ticker ownership on {session}"
                )
            if preferred_observed_ticker is not None:
                ticker = preferred_observed_ticker
            elif not observed_tickers and len(self._security.tickers) == 1:
                ticker = self._security.tickers[0]
            else:
                raise DataLibraryError(
                    f"online security {self._security.public_id} has no unique ticker on {session}"
                )
            metadata = self._identities_by_ticker.get(ticker, [])
        names = _joined_identity_values(value for identity in metadata for value in identity.names)
        cusips = _joined_identity_values(value for identity in metadata for value in identity.cusips)
        exchanges = _joined_identity_values(value for identity in metadata for value in identity.exchanges)
        asset_classes = _joined_identity_values(
            value for identity in metadata for value in identity.asset_classes
        )
        return _SessionIdentity(
            ticker=ticker,
            name=names or _joined_identity_values(iter(self._security.names)),
            cusip=cusips or _joined_identity_values(iter(self._security.cusips)),
            exchange=exchanges or _joined_identity_values(iter(self._security.exchanges)),
            asset_class=asset_classes or _joined_identity_values(iter(self._security.asset_classes)),
        )


def _bar_csv_values(bar: DailyBar | None) -> tuple[str, ...]:
    if bar is None:
        return ("",) * 7
    return (
        str(bar.open),
        str(bar.high),
        str(bar.low),
        str(bar.close),
        str(bar.volume),
        str(bar.trade_count),
        str(bar.vwap),
    )


def _flat_csv_sort_key(row: tuple[str, ...] | list[str]) -> tuple[str, str]:
    return row[0], row[1]


def _csv_rows_as_bytes(rows: list[tuple[str, ...]]) -> bytes:
    output = io.StringIO(newline="")
    csv.writer(output).writerows(rows)
    return output.getvalue().encode("utf-8")


def _append_flat_csv_block(
    spool: BinaryIO,
    rows: list[tuple[str, ...]],
    segments_by_date: dict[str, list[tuple[int, int]]],
    last_unique_id_by_date: dict[str, str],
) -> None:
    rows.sort(key=_flat_csv_sort_key)
    previous_key = None
    first_row_index = 0
    while first_row_index < len(rows):
        session = rows[first_row_index][0]
        last_row_index = first_row_index + 1
        while last_row_index < len(rows) and rows[last_row_index][0] == session:
            last_row_index += 1
        session_rows = rows[first_row_index:last_row_index]
        for row in session_rows:
            key = _flat_csv_sort_key(row)
            if key == previous_key:
                raise DataLibraryError(f"flat CSV contains duplicate date and unique ID: {key}")
            previous_key = key
        first_unique_id = session_rows[0][1]
        previous_unique_id = last_unique_id_by_date.get(session)
        if previous_unique_id is not None and first_unique_id <= previous_unique_id:
            raise DataLibraryError(f"flat CSV security order is inconsistent on {session}")
        last_unique_id_by_date[session] = session_rows[-1][1]
        encoded_rows = _csv_rows_as_bytes(session_rows)
        offset = spool.tell()
        spool.write(encoded_rows)
        segments_by_date.setdefault(session, []).append((offset, len(encoded_rows)))
        first_row_index = last_row_index


def _spool_segment_bytes(spool: BinaryIO, offset: int, length: int) -> Iterator[bytes]:
    spool.seek(offset)
    remaining = length
    while remaining:
        block = spool.read(min(1024 * 1024, remaining))
        if not block:
            raise DataLibraryError("flat CSV temporary spool ended unexpectedly")
        remaining -= len(block)
        yield block


def _flat_csv_header_bytes() -> bytes:
    return _csv_rows_as_bytes([FLAT_ONLINE_CSV_COLUMNS])


def _write_flat_csv_binary_document(
    output: BinaryIO,
    spool: BinaryIO,
    segments_by_date: dict[str, list[tuple[int, int]]],
) -> None:
    output.write(_flat_csv_header_bytes())
    for session in sorted(segments_by_date):
        for offset, length in segments_by_date[session]:
            for block in _spool_segment_bytes(spool, offset, length):
                output.write(block)


def _write_flat_csv_text_document(
    output: TextIO,
    spool: BinaryIO,
    segments_by_date: dict[str, list[tuple[int, int]]],
) -> None:
    output.write(_flat_csv_header_bytes().decode("utf-8"))
    for session in sorted(segments_by_date):
        for offset, length in segments_by_date[session]:
            encoded_segment = b"".join(_spool_segment_bytes(spool, offset, length))
            output.write(encoded_segment.decode("utf-8"))


class DataLibrary:
    """Read one consistent snapshot of the authenticated online universe.

    ``path`` is the data-engine repository root. It defaults to the directory
    containing this module, so applications importing this checkout need no
    path configuration. Create a new instance to observe a newer publication.
    """

    def __init__(self, path: Path | str | None = None) -> None:
        _validate_api_change_log()
        self._path = _DEFAULT_LIBRARY_PATH if path is None else Path(path)
        try:
            self._master = authenticate_security_master(self._path)
        except SecurityCorrelationError as error:
            raise DataLibraryError(f"cannot open the authenticated data catalog: {error}") from error
        generation = self._master.receipt.get("security_master_generation_sha256")
        published_at = self._master.receipt.get("created_at")
        if not isinstance(generation, str) or not generation:
            raise DataLibraryError("authenticated data catalog has no generation")
        if not isinstance(published_at, str):
            raise DataLibraryError("authenticated data catalog has no publication time")
        try:
            self._published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise DataLibraryError("authenticated data catalog has an invalid publication time") from error
        if self._published_at.tzinfo is None:
            raise DataLibraryError("authenticated data catalog publication time has no time zone")
        self._generation = generation
        self._securities: tuple[Security, ...] | None = None
        self._ticker_resolutions: dict[str, tuple[TickerResolution, ...]] | None = None

    def _assert_current_generation(self) -> None:
        try:
            current_master = authenticate_security_master(self._path)
        except SecurityCorrelationError as error:
            raise DataLibraryError(f"cannot authenticate the current data catalog: {error}") from error
        current_generation = current_master.receipt.get("security_master_generation_sha256")
        if current_generation != self._generation:
            raise CatalogChangedError(_CATALOG_CHANGED_MESSAGE)

    def catalog_generation(self) -> str:
        """Return the digest identifying the current authenticated catalog."""
        return self._generation

    def catalog(self) -> Catalog:
        """Return version, publication, size, and adjustment metadata."""
        return Catalog(
            generation=self._generation,
            published_at=self._published_at,
            online_count=self.online_count(),
            adjustments=ADJUSTMENTS,
        )

    def online_count(self) -> int:
        """Return the number of securities in the online public universe."""
        return len(self.list_securities())

    def list_securities(self) -> tuple[Security, ...]:
        """Return every online security, ordered by public ID."""
        if self._securities is not None:
            return self._securities
        securities = []
        ticker_resolutions: dict[str, list[TickerResolution]] = {}
        try:
            for public_id in sorted(self._master.identities_by_master_id):
                record = self._master.identities_by_master_id[public_id]
                if record["online"] is True:
                    security = _security_from_record(record)
                    securities.append(security)
                    for resolution in _ticker_resolutions_from_record(record, security):
                        ticker_resolutions.setdefault(resolution.ticker.upper(), []).append(resolution)
        except SecurityCorrelationError as error:
            raise DataLibraryError(f"cannot authenticate the online security catalog: {error}") from error
        self._ticker_resolutions = {
            ticker: tuple(
                sorted(
                    resolutions,
                    key=lambda item: (item.first_session, item.last_session, item.public_id),
                )
            )
            for ticker, resolutions in ticker_resolutions.items()
        }
        self._securities = tuple(securities)
        return self._securities

    def resolve_ticker(
        self,
        ticker: str,
        *,
        on: date | str | None = None,
    ) -> tuple[TickerResolution, ...]:
        """Resolve an exact ticker to every observed online public-ID date range."""
        if not isinstance(ticker, str) or not ticker.strip():
            raise ValueError("ticker must be a non-empty string")
        session = _optional_date(on, "on")
        if self._ticker_resolutions is None:
            self.list_securities()
        assert self._ticker_resolutions is not None
        resolutions = self._ticker_resolutions.get(ticker.strip().upper(), ())
        if session is None:
            return resolutions
        return tuple(
            resolution
            for resolution in resolutions
            if resolution.first_session <= session <= resolution.last_session
        )

    def search_securities(
        self,
        query: str,
        *,
        limit: int | None = None,
    ) -> tuple[Security, ...]:
        """Search online identities by name, ticker, CUSIP, external ID, or public ID."""
        if not isinstance(query, str) or not query.strip():
            raise ValueError("search query must be a non-empty string")
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError("search limit must be a positive integer or None")
        normalized_query = _normalized_search_text(query)
        ranked_matches = []
        for security in self.list_securities():
            rank = _security_search_rank(security, normalized_query)
            if rank is not None:
                ranked_matches.append((rank, security))
        ranked_matches.sort(key=lambda item: item[0])
        if limit is not None:
            ranked_matches = ranked_matches[:limit]
        return tuple(security for _, security in ranked_matches)

    def get_security(self, public_id: str) -> Security:
        """Return public identity metadata for one online security."""
        canonical_id = _canonical_public_id(public_id)
        try:
            record = self._master.identities_by_master_id.get(canonical_id)
        except SecurityCorrelationError as error:
            raise DataLibraryError(f"cannot authenticate security {canonical_id}: {error}") from error
        if record is None:
            raise PublicIdUnavailableError("public ID is not available")
        return _security_from_record(record)

    def write_flat_online_csv(
        self,
        destination: Path | str | TextIO,
        *,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> FlatCsvExport:
        """Write a date-ordered CSV containing raw and fully adjusted online bars."""
        start_date = _optional_date(start, "start")
        end_date = _optional_date(end, "end")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("start must be on or before end")
        destination_path = Path(destination) if isinstance(destination, (str, os.PathLike)) else None
        if destination_path is None and not hasattr(destination, "write"):
            raise TypeError("CSV destination must be a path or writable text stream")
        if destination_path is not None:
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_parent = destination_path.parent
        else:
            temporary_parent = None

        securities = self.list_securities()
        row_count = 0
        exported_security_count = 0
        with tempfile.TemporaryDirectory(prefix="alpaca-flat-csv-", dir=temporary_parent) as directory:
            temporary_directory = Path(directory)
            spool_path = temporary_directory / "rows.spool"
            segments_by_date: dict[str, list[tuple[int, int]]] = {}
            last_unique_id_by_date: dict[str, str] = {}
            pending_rows: list[tuple[str, ...]] = []
            with spool_path.open("w+b") as spool:
                for security in securities:
                    required_adjustments = {"raw", "all"}
                    if not required_adjustments.issubset(security.adjustments):
                        raise DataLibraryError(
                            f"online security {security.public_id} does not provide raw and all-adjusted histories"
                        )
                    try:
                        record = self._master.identities_by_master_id[security.public_id]
                    except (KeyError, SecurityCorrelationError) as error:
                        raise DataLibraryError(
                            f"cannot authenticate online security {security.public_id} for CSV export"
                        ) from error
                    timeline = _IdentityTimeline(record, security)
                    raw_history = self.get_history(
                        security.public_id,
                        "raw",
                        start=start_date,
                        end=end_date,
                    )
                    adjusted_history = self.get_history(
                        security.public_id,
                        "all",
                        start=start_date,
                        end=end_date,
                    )
                    raw_by_session = {bar.session: bar for bar in raw_history.bars}
                    adjusted_by_session = {bar.session: bar for bar in adjusted_history.bars}
                    if len(raw_by_session) != len(raw_history.bars) or len(adjusted_by_session) != len(
                        adjusted_history.bars
                    ):
                        raise DataLibraryError(
                            f"online security {security.public_id} has duplicate sessions in a CSV history"
                        )
                    if raw_by_session.keys() != adjusted_by_session.keys():
                        raise DataLibraryError(
                            f"online security {security.public_id} has different raw and all-adjusted sessions"
                        )
                    if raw_by_session:
                        exported_security_count += 1
                    for session in sorted(raw_by_session):
                        identity = timeline.for_session(session)
                        pending_rows.append(
                            (
                                session.isoformat(),
                                security.public_id,
                                identity.ticker,
                                identity.name,
                                identity.cusip,
                                identity.exchange,
                                identity.asset_class,
                                *_bar_csv_values(raw_by_session[session]),
                                *_bar_csv_values(adjusted_by_session[session]),
                            )
                        )
                        row_count += 1
                    if len(pending_rows) >= _FLAT_CSV_SORT_CHUNK_ROWS:
                        _append_flat_csv_block(
                            spool,
                            pending_rows,
                            segments_by_date,
                            last_unique_id_by_date,
                        )
                        pending_rows = []
                if pending_rows:
                    _append_flat_csv_block(
                        spool,
                        pending_rows,
                        segments_by_date,
                        last_unique_id_by_date,
                    )
                spool.flush()

                if destination_path is None:
                    _write_flat_csv_text_document(destination, spool, segments_by_date)
                else:
                    descriptor, temporary_name = tempfile.mkstemp(
                        prefix=f".{destination_path.name}.",
                        suffix=".tmp",
                        dir=destination_path.parent,
                    )
                    temporary_path = Path(temporary_name)
                    try:
                        with os.fdopen(descriptor, "wb") as output:
                            _write_flat_csv_binary_document(output, spool, segments_by_date)
                            output.flush()
                            os.fsync(output.fileno())
                        os.replace(temporary_path, destination_path)
                    except BaseException:
                        temporary_path.unlink(missing_ok=True)
                        raise

        return FlatCsvExport(
            generation=self._generation,
            online_security_count=len(securities),
            exported_security_count=exported_security_count,
            row_count=row_count,
            start=start_date,
            end=end_date,
        )

    def get_history(
        self,
        public_id: str,
        adjustment: str = "raw",
        *,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> History:
        """Return all sessions or an inclusive date range for one online security."""
        canonical_id = _canonical_public_id(public_id)
        if adjustment not in ADJUSTMENTS:
            supported = ", ".join(ADJUSTMENTS)
            raise ValueError(f"unsupported adjustment {adjustment!r}; expected one of: {supported}")
        start_date = _optional_date(start, "start")
        end_date = _optional_date(end, "end")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("start must be on or before end")

        security = self.get_security(canonical_id)

        try:
            result = _read_authenticated_history(
                canonical_id,
                adjustment,
                repository_root=self._path,
            )
        except EndOfDayReaderError as error:
            self._assert_current_generation()
            raise DataLibraryError(f"cannot read security history: {error}") from error

        try:
            generation = result["provenance"]["security_master"]["security_master_generation_sha256"]
            if not isinstance(generation, str) or not generation:
                raise DataLibraryError("authenticated history has no data generation")
            if generation != self._generation:
                raise CatalogChangedError(_CATALOG_CHANGED_MESSAGE)
            if result["availability"]["online"] is not True:
                raise DataLibraryError("an online public history became unavailable")
            if result["integrity"]["error_count"] != 0:
                raise DataLibraryError("an online history contains integrity errors")
            result_security = _security_from_record(result["security"])
            all_bars = tuple(_daily_bar_from_record(bar) for bar in result["bars"])
            if result["bar_count"] != len(all_bars):
                raise DataLibraryError("authenticated history has an inconsistent bar count")
        except DataLibraryError:
            raise
        except (KeyError, TypeError) as error:
            raise DataLibraryError("authenticated history does not satisfy the public contract") from error

        if result_security != security:
            raise DataLibraryError("authenticated history identity does not match the public catalog")

        bars = tuple(
            bar
            for bar in all_bars
            if (start_date is None or bar.session >= start_date)
            and (end_date is None or bar.session <= end_date)
        )
        return History(
            security=security,
            adjustment=adjustment,
            start=start_date,
            end=end_date,
            bars=bars,
            generation=generation,
        )


__all__ = [
    "ADJUSTMENTS",
    "API_VERSION",
    "Catalog",
    "CatalogChangedError",
    "DailyBar",
    "DataLibrary",
    "DataLibraryError",
    "ExternalIdentifier",
    "FLAT_ONLINE_CSV_COLUMNS",
    "FlatCsvExport",
    "History",
    "PublicIdUnavailableError",
    "Security",
    "TickerResolution",
]

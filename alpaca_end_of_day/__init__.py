"""Public, read-only Python library for published end-of-day market data.

Consumer code should import this package and no module below ``libs``. The
pipeline has already checked its published boundaries; this consumer reads the
resulting online universe, public identities, and daily histories. The separate
StandaloneReader consumes resolved pricing from the standalone twins without
claiming the serving catalogue's availability contract.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, BinaryIO, TextIO

from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_serving_catalogue import storage as catalogue_storage
from libs.alpaca_serving_catalogue.contract import CatalogueError

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

API_VERSION = 19
API_CHANGE_LOG_PATH = _REPOSITORY_ROOT / "resources" / "alpaca_end_of_day_api_change_log.json"
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
_DEFAULT_STORE = _REPOSITORY_ROOT / "data_pricing"
_FLAT_CSV_SORT_CHUNK_ROWS = 100_000


class DataLibraryError(RuntimeError):
    """Raised when the public library cannot select or return data."""


class PublicIdUnavailableError(DataLibraryError):
    """Raised when a public ID is not available in the online snapshot."""


class CatalogChangedError(DataLibraryError):
    """Raised rather than combining results from different catalog generations."""


@dataclass(frozen=True, slots=True)
class MetadataObservation:
    """Known metadata, with explicit unknown observation/effective dates."""

    field: str
    value: str
    observed_at: datetime | None
    effective_from: date | None = None
    effective_through: date | None = None

    def to_dict(self):
        return {
            "field": self.field,
            "value": self.value,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "effective_from": self.effective_from.isoformat() if self.effective_from else None,
            "effective_through": self.effective_through.isoformat() if self.effective_through else None,
        }


@dataclass(frozen=True, slots=True)
class TickerOwnershipInterval:
    """Published dated label evidence; unknown endpoints stay unknown."""

    ticker: str
    start_inclusive: date | None
    end_exclusive: date | None
    cusip: str | None

    def to_dict(self):
        return {
            "ticker": self.ticker,
            "start_inclusive": self.start_inclusive.isoformat() if self.start_inclusive else None,
            "end_exclusive": self.end_exclusive.isoformat() if self.end_exclusive else None,
            "cusip": self.cusip,
        }


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
    observations: tuple[MetadataObservation, ...]
    ticker_ownership_intervals: tuple[TickerOwnershipInterval, ...]
    stored_session_count: int
    explained_missing_sessions: tuple[date, ...]

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
            "observations": [item.to_dict() for item in self.observations],
            "ticker_ownership_intervals": [item.to_dict() for item in self.ticker_ownership_intervals],
            "coverage": {
                "meaning": "approved_stored_history",
                "session_count": self.stored_session_count,
                "explained_missing_sessions": [d.isoformat() for d in self.explained_missing_sessions],
            },
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
    volume: Decimal
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
            "volume": str(self.volume),
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
    """Identity and size of one published public-universe snapshot."""

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


def _normalized_search_text(value: str) -> str:
    return " ".join(value.split()).casefold()


def _ticker_label(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Z0-9][A-Z0-9.-]*", value.strip().upper()) is None:
        raise ValueError("ticker must be a ticker label")
    return value.strip().upper()


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


def _security(record):
    return Security(
        public_id=record["public_id"],
        tickers=tuple(record["tickers"]),
        names=tuple(record["names"]),
        cusips=tuple(record["cusips"]),
        exchanges=tuple(record["exchanges"]),
        asset_classes=tuple(record["asset_classes"]),
        external_identifiers=tuple(ExternalIdentifier(**i) for i in record["external_identifiers"]),
        first_session=date.fromisoformat(record["first_session"]),
        last_session=date.fromisoformat(record["last_session"]),
        adjustments=tuple(record["adjustments"]),
        observations=tuple(
            MetadataObservation(
                field=o["field"],
                value=o["value"],
                observed_at=datetime.fromisoformat(o["observed_at"]) if o["observed_at"] else None,
                effective_from=date.fromisoformat(o["effective_from"]) if o["effective_from"] else None,
                effective_through=date.fromisoformat(o["effective_through"]) if o["effective_through"] else None,
            )
            for o in record["observations"]
        ),
        ticker_ownership_intervals=tuple(
            TickerOwnershipInterval(
                ticker=interval["ticker"],
                start_inclusive=date.fromisoformat(interval["start_inclusive"])
                if interval["start_inclusive"]
                else None,
                end_exclusive=date.fromisoformat(interval["end_exclusive"])
                if interval["end_exclusive"]
                else None,
                cusip=interval["cusip"],
            )
            for interval in record["ticker_ownership_intervals"]
        ),
        stored_session_count=record["coverage"]["session_count"],
        explained_missing_sessions=tuple(
            date.fromisoformat(d) for d in record["coverage"]["explained_missing_sessions"]
        ),
    )


def _optional_date(value, name):
    from .standalone import _date

    return _date(value, name)


class DataLibrary:
    """One in-memory public catalogue snapshot over an explicit standalone store.

    Histories compare the selected decision and upstream generations under group
    locks. Unrelated catalogue updates leave a compatible group readable.
    """

    def __init__(self, *args, store=None, wait=False, cache_bytes=0, **legacy):
        if args or legacy:
            raise TypeError(
                "DataLibrary now accepts store=PATH; positional repository roots and path= are removed. Bootstrap the standalone serving catalogue and migrate CLI callers to --store."
            )
        _validate_api_change_log()
        self.store = Path(_DEFAULT_STORE if store is None else store).absolute()
        self._reader = StandaloneReader(self.store, wait=wait, cache_bytes=cache_bytes)
        try:
            selected = catalogue_storage.snapshot(self.store)
            self._generation = selected["state"]["generation"]
            self._published_at = datetime.fromisoformat(selected["state"]["published_at"])
            self._records, self._groups = selected["records"], selected["groups"]
            self._securities = {
                public_id: _security(record["public"]) for public_id, record in self._records.items()
            }
            self._resolutions = {}
            for public_id, record in self._records.items():
                security = self._securities[public_id]
                for interval in record["public"]["ticker_ranges"]:
                    match = TickerResolution(
                        interval["ticker"],
                        public_id,
                        security.names,
                        date.fromisoformat(interval["first_session"]),
                        date.fromisoformat(interval["last_session"]),
                        security.adjustments,
                    )
                    self._resolutions.setdefault(match.ticker, []).append(match)
            self._resolutions = {
                ticker: tuple(sorted(matches, key=lambda r: (r.first_session, r.last_session, r.public_id)))
                for ticker, matches in self._resolutions.items()
            }
        except (CatalogueError, OSError, ValueError, KeyError, TypeError) as error:
            raise DataLibraryError(f"cannot open serving catalogue: {error}") from error

    def catalog_generation(self):
        return self._generation

    def catalog(self):
        # Public API entry point for external consumers, even without internal call sites.
        return Catalog(self._generation, self._published_at, self.online_count(), ADJUSTMENTS)

    def online_count(self):
        return len(self._securities)

    def list_securities(self):
        return tuple(self._securities.values())

    def get_security(self, public_id):
        from .standalone import _public_id

        security = self._securities.get(_public_id(public_id))
        if security is None:
            raise PublicIdUnavailableError("public ID is not available")
        return security

    def resolve_ticker(self, ticker, *, on=None):
        ticker, session = _ticker_label(ticker), _optional_date(on, "on")
        return tuple(
            r
            for r in self._resolutions.get(ticker, ())
            if session is None or r.first_session <= session <= r.last_session
        )

    def search_securities(self, query, *, limit=None):
        if not isinstance(query, str) or not query.strip():
            raise ValueError("search query must be a non-empty string")
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError("search limit must be a positive integer or None")
        query = _normalized_search_text(query)
        matches = [
            (rank, security)
            for security in self._securities.values()
            if (rank := _security_search_rank(security, query)) is not None
        ]
        return tuple(s for _, s in sorted(matches, key=lambda pair: pair[0])[:limit])

    def _read_views(self, public_id, adjustments, start, end):
        # All public access paths resolve availability before touching storage.
        security = self.get_security(public_id)
        key = self._records[security.public_id]["group_key"]
        expected = self._groups[key]
        try:
            with self._reader._open_group(
                expected["locator"],
                expected=expected["references"],
                shared_cache=True,
                before_payloads=lambda: catalogue_storage.assert_group(
                    self.store, key, expected["decision_generation"]
                ),
            ) as group:
                return group.get_histories(
                    public_id=security.public_id, adjustments=adjustments, start=start, end=end
                )["views"]
        except ReaderBusyError:
            raise
        except (CatalogueError, StandaloneReaderError) as error:
            raise CatalogChangedError(
                "selected serving history is no longer compatible; finish maintenance and open a new DataLibrary"
            ) from error

    def _history(self, security, adjustment, view, start, end):
        bars = tuple(
            DailyBar(
                session=date.fromisoformat(b["session"]),
                **{n: Decimal(b[n]) for n in ("open", "high", "low", "close", "volume", "vwap")},
                trade_count=int(Decimal(b["trade_count"])),
            )
            for b in view["bars"]
        )
        return History(security, adjustment, start, end, bars, self._generation)

    def get_histories(self, public_id, adjustments=ADJUSTMENTS, *, start=None, end=None):
        security = self.get_security(public_id)
        if (
            not isinstance(adjustments, (tuple, list))
            or not adjustments
            or any(a not in ADJUSTMENTS for a in adjustments)
        ):
            raise ValueError("adjustments must be a nonempty list or tuple of supported views")
        adjustments = tuple(dict.fromkeys(adjustments))
        start, end = _optional_date(start, "start"), _optional_date(end, "end")
        if start is not None and end is not None and start > end:
            raise ValueError("start must be on or before end")
        views = self._read_views(security.public_id, adjustments, start, end)
        return {a: self._history(security, a, views[a], start, end) for a in adjustments}

    def get_history(self, public_id, adjustment="raw", *, start=None, end=None):
        return self.get_histories(public_id, (adjustment,), start=start, end=end)[adjustment]

    def plan_csv_export(self, *, public_ids=None, start=None, end=None, workers=2):
        """Describe a per-identity CSV export using only this catalogue snapshot."""
        from ._csv_export import plan_export

        return plan_export(self, public_ids=public_ids, start=start, end=end, workers=workers)

    def write_csv_export(self, destination, *, public_ids=None, start=None, end=None, progress=None, workers=2):
        """Publish per-ID pricing CSVs, alias digests and a run completion manifest."""
        from ._csv_export import write_export

        return write_export(
            self, destination, public_ids=public_ids, start=start, end=end, progress=progress, workers=workers
        )

    @staticmethod
    def _dated_metadata(security, session):
        values = []
        for field in ("name", "cusip", "exchange", "asset_class"):
            matching = {
                o.value
                for o in security.observations
                if o.field == field
                and o.effective_from is not None
                and o.effective_through is not None
                and o.effective_from <= session <= o.effective_through
            }
            if len(matching) > 1:
                raise DataLibraryError(f"ambiguous historical {field} on {session}")
            values.append(next(iter(matching), ""))
        return values

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
            if destination_path.resolve().is_relative_to(self.store.resolve()):
                raise ValueError("CSV destination must be outside the source store")
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
                    results = self._read_views(security.public_id, ("raw", "all"), start_date, end_date)
                    raw_history = self._history(security, "raw", results["raw"], start_date, end_date)
                    adjusted_history = self._history(security, "all", results["all"], start_date, end_date)
                    ranges = self._records[security.public_id]["public"]["ticker_ranges"]
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
                        tickers = sorted(
                            {
                                r["ticker"]
                                for r in ranges
                                if r["first_session"] <= session.isoformat() <= r["last_session"]
                            }
                        )
                        if len(tickers) != 1:
                            raise DataLibraryError(f"ambiguous historical ticker on {session}")
                        # Undated observations never become historical row metadata.
                        metadata = self._dated_metadata(security, session)
                        pending_rows.append(
                            (
                                session.isoformat(),
                                security.public_id,
                                tickers[0],
                                *metadata,
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


from .standalone import (  # noqa: E402
    AmbiguousTickerError,
    ReaderBusyError,
    StandaloneReader,
    StandaloneReaderError,
)

__all__ = [
    "ADJUSTMENTS",
    "API_VERSION",
    "AmbiguousTickerError",
    "Catalog",
    "CatalogChangedError",
    "DailyBar",
    "DataLibrary",
    "DataLibraryError",
    "ExternalIdentifier",
    "FLAT_ONLINE_CSV_COLUMNS",
    "FlatCsvExport",
    "History",
    "MetadataObservation",
    "PublicIdUnavailableError",
    "ReaderBusyError",
    "Security",
    "StandaloneReader",
    "StandaloneReaderError",
    "TickerResolution",
    "TickerOwnershipInterval",
]

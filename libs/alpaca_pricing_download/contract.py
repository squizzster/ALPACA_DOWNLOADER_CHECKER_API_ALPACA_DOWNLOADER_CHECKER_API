"""Versioned, value-preserving pricing representation."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

RUNTIME_VERSION = 33
VERSION = 31  # Persisted publication and completed-request contract, independent of runtime releases.
# Version 31 retains the pricing representation. Sources may still select older
# unchanged boundary receipts; accept those independently of the source version.
SUPPORTED_VERSIONS = tuple(range(1, VERSION + 1))
VALIDATOR_FILE = "validator_first_last_sessions.json"
ADJUSTMENTS = (
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)
COLUMNS = ("timestamp", "open", "high", "low", "close", "volume", "trade_count", "vwap")
FIELDS = ("t", "o", "h", "l", "c", "v", "n", "vw")
HISTORY_START = "2016-01-01T00:00:00Z"
NEW_YORK = ZoneInfo("America/New_York")
Row = tuple[str, ...]


class DownloadError(RuntimeError):
    pass


class BarsSymbolUnavailable(DownloadError):
    """An exact provider rejection, not an empty response or an identity verdict."""

    def __init__(self, symbol: str, http_status: int):
        self.symbol = symbol
        self.http_status = http_status
        self.provider_message = f"invalid symbol: {symbol}"
        super().__init__(f"Alpaca bars rejected {symbol}: HTTP {http_status}: {self.provider_message}")


class InitialPricingUnavailable(DownloadError):
    """No source existed and the very first pricing response rejected its label."""

    def __init__(self, request: dict, error: BarsSymbolUnavailable):
        self.symbol = error.symbol
        self.evidence = {
            "outcome": "invalid_symbol",
            "http_status": error.http_status,
            "provider_message": error.provider_message,
            "request": dict(request),
        }
        super().__init__(str(error))


def failure_outcome(error):
    """Keep access denials distinct from exhausted transport/provider failures."""
    from .failures import RetainedAcquisitionError

    cause = error
    status = None
    seen = set()
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        if isinstance(cause, RetainedAcquisitionError):
            return {**cause.failure.to_outcome(), "error": str(error)}
        if isinstance(cause, InitialPricingUnavailable):
            return {**cause.evidence, **({"error": str(error)} if cause is not error else {})}
        if isinstance(cause, BarsSymbolUnavailable):
            return {
                "outcome": "invalid_symbol",
                "http_status": cause.http_status,
                "symbol": cause.symbol,
                "provider_message": cause.provider_message,
                "error": str(error),
            }
        code = (
            getattr(cause, "http_status", None) or getattr(cause, "status", None) or getattr(cause, "code", None)
        )
        if status is None and isinstance(code, int):
            status = code
        cause = cause.__cause__
    return {
        "outcome": "access_denied" if status in (401, 403) else "request_failed",
        "http_status": status if isinstance(status, int) else None,
        "error": str(error),
    }


def validate_version() -> None:
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_pricing_download_change_log.json"
    entries = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or len(entries) != RUNTIME_VERSION:
        raise DownloadError("pricing-download changelog length differs from its version")
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(entry.get("modifications"), list)
            or not entry["modifications"]
            or any(not isinstance(item, str) or not item for item in entry["modifications"])
        ):
            raise DownloadError("invalid pricing-download changelog entry")


def symbol_label(value: str) -> str:
    symbol = value.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]*", symbol):
        raise DownloadError(f"invalid ticker label: {value!r}")
    return symbol


def json_bytes(value: object) -> bytes:
    # Receipt generations bind these exact bytes, including field order, escaping and newline.
    return (json.dumps(value, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n").encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def timestamp(value: str) -> datetime:
    if not isinstance(value, str):
        raise DownloadError("provider timestamp is not a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise DownloadError(f"invalid provider timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        raise DownloadError("provider timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def month_of(row: Row) -> str:
    return timestamp(row[0]).astimezone(NEW_YORK).strftime("%Y-%m")


def csv_bytes(rows: tuple[Row, ...]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(COLUMNS)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def decode_csv(data: bytes) -> tuple[Row, ...]:
    try:
        reader = csv.reader(io.StringIO(gzip.decompress(data).decode("utf-8"), newline=""))
        if tuple(next(reader, ())) != COLUMNS:
            raise DownloadError("pricing payload has incorrect columns")
        rows = tuple(tuple(row) for row in reader)
    except (OSError, EOFError, UnicodeError, csv.Error) as error:
        raise DownloadError("invalid compressed pricing payload") from error
    if any(len(row) != len(COLUMNS) for row in rows):
        raise DownloadError("pricing payload has an incorrect row width")
    return rows


def transform(bars: list[dict], start: datetime, end: datetime) -> tuple[Row, ...]:
    # Raw provider data becomes a completed exact-row handoff here. These checks
    # establish representation/order/scope, not identity or serving eligibility;
    # consumers must not replay them over already normalized/published rows.
    rows = []
    previous = None
    for bar in bars:
        if not isinstance(bar, dict) or any(field not in bar for field in FIELDS):
            raise DownloadError("provider bar is missing required fields")
        if not isinstance(bar["t"], str):
            raise DownloadError("provider timestamp is not a string")
        instant = timestamp(bar["t"])
        if not start <= instant < end or (previous is not None and instant <= previous):
            raise DownloadError("provider rows are out of range, duplicated, or not ascending")
        previous = instant
        values = [bar["t"]]
        for field in FIELDS[1:]:
            value = bar[field]
            if type(value) is not int and not isinstance(value, Decimal):
                raise DownloadError(f"{field}: expected exact JSON integer/decimal, not {type(value).__name__}")
            if isinstance(value, Decimal) and not value.is_finite():
                raise DownloadError(f"{field}: non-finite JSON number")
            values.append(str(value))
        rows.append(tuple(values))
    return tuple(rows)


def build_boundary(
    symbol: str, kind: str, anchor: str, views: dict, request: dict
) -> tuple[dict, dict[str, bytes]]:
    entries = []
    payloads = {}
    canonical = {}
    prefix = "completed_years." if kind == "completed_years" else ""
    for adjustment in ADJUSTMENTS:
        rows = views[adjustment]
        logical = csv_bytes(rows)
        entry = {"adjustment": adjustment, "rows": len(rows), "logical_sha256": digest(logical)}
        if not rows:
            entry["storage"] = "empty"
        else:
            packed = gzip.compress(logical, compresslevel=5, mtime=0)
            # This is the transform's publication check, not a later lineage rule.
            if decode_csv(packed) != rows:
                raise DownloadError("gzip/CSV transformation changed provider values")
            # Exact-copy aliases are established by the producer from new bytes.
            # Readers follow the selected alias; they do not compare files again.
            earlier = canonical.get(packed)
            filename = prefix + adjustment.replace(",", "_") + ".csv.gz"
            if earlier is None:
                canonical[packed] = (adjustment, filename)
                payloads[filename] = packed
                entry["storage"] = "file"
            else:
                original, filename = earlier
                entry.update(storage="exact_copy", equivalent_to=original)
            entry.update(
                file=filename, sha256=digest(packed), bytes=len(packed), first=rows[0][0], last=rows[-1][0]
            )
        entries.append(entry)
    return {
        "schema_version": VERSION,
        "boundary": "alpaca_pricing_" + kind,
        "symbol": symbol,
        "anchor_month": anchor,
        "request": request,
        "adjustments": entries,
    }, payloads

"""The native v6 monthly pricing-source boundary.

The source publisher verifies each month that it writes.  Downstream packages
load that published boundary as trusted input; explicit authentication remains
available to an operator audit and is never part of ordinary consumption.
"""

from __future__ import annotations

import calendar
import csv
import fcntl
import gzip
import io
import os
import re
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from libs.alpaca_end_of_day_pipeline.contracts import CSV_COLUMN_ALPACA_FIELD_PAIRS
from libs.download_alpaca_end_of_day_bars.settings import ALPACA_ADJUSTMENT_MODES

from .common import (
    canonical_sha256,
    document_bytes,
    durability_batch,
    ensure_publication_lock,
    fsync_directory,
    read_json_object,
    replace_bytes,
    replace_json,
    safe_child,
    sha256_bytes,
    sha256_file,
)
from .errors import V6Error
from .versions import SOURCE_VERSION, validate_boundary_change_log

STORE_MANIFEST = "store.manifest.json"
SOURCE_MANIFEST = "source.manifest.json"
MONTH_MANIFEST = "month.manifest.json"
STORE_BOUNDARY = "alpaca_v6_store"
SOURCE_INVENTORY_BOUNDARY = "alpaca_v6_monthly_source_inventory"
MONTH_BOUNDARY = "alpaca_v6_monthly_pricing_source"
STORE_LAYOUT = "data_vtest/end_of_day/<TICKER>/<YEAR>/<Mon>/"
BAR_COLUMNS = tuple(column for column, _field in CSV_COLUMN_ALPACA_FIELD_PAIRS)
ALPACA_FIELDS = tuple(field for _column, field in CSV_COLUMN_ALPACA_FIELD_PAIRS)
ADJUSTMENT_FILENAMES = {
    adjustment: adjustment.replace(",", "_") + ".csv.gz" for adjustment in ALPACA_ADJUSTMENT_MODES
}
NEW_YORK = ZoneInfo("America/New_York")
SYMBOL_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]*")


@dataclass(frozen=True)
class MonthlyLogicalSource:
    placement: str
    month_receipt_sha256: str
    rows: tuple[dict[str, str], ...]
    row_tuples: tuple[tuple[str, ...], ...]
    logical_sha256: str


@dataclass(frozen=True)
class LogicalAdjustmentSource:
    symbol: str
    adjustment: str
    rows: tuple[dict[str, str], ...]
    row_tuples: tuple[tuple[str, ...], ...]
    logical_sha256: str
    month_receipt_sha256: tuple[tuple[str, str], ...]
    months: tuple[MonthlyLogicalSource, ...]

    @property
    def logical_filename(self) -> str:
        return ADJUSTMENT_FILENAMES[self.adjustment]


@dataclass(frozen=True)
class AuthenticatedMonthlySource:
    symbol: str
    inventory: dict[str, Any]
    inventory_sha256: str
    source_generation_sha256: str
    adjustments: tuple[LogicalAdjustmentSource, ...]

    @property
    def by_adjustment(self) -> dict[str, LogicalAdjustmentSource]:
        return {source.adjustment: source for source in self.adjustments}


def _store_document() -> dict[str, Any]:
    return {
        "schema_version": SOURCE_VERSION,
        "boundary": STORE_BOUNDARY,
        "dataset": "alpaca_end_of_day_v6",
        "layout": STORE_LAYOUT,
        "partition_order": ["symbol", "year", "month"],
        "month_names": list(calendar.month_abbr[1:]),
        "columns": list(BAR_COLUMNS),
        "adjustments": list(ALPACA_ADJUSTMENT_MODES),
        "gzip": {"compresslevel": 5, "mtime": 0},
        "duplicate_payloads": "month_manifest_exact_copy_aliases",
        "source_inventory": "structural_membership_only",
        "domain_rules": "owned_by_separate_downstream_boundaries",
    }


def initialize_v6_store(store_root: Path | str) -> None:
    validate_boundary_change_log("source")
    root = Path(store_root)
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink() or not root.is_dir():
        raise V6Error(f"v6 store root is not a regular directory: {root}")
    expected = _store_document()
    manifest_path = root / STORE_MANIFEST
    if manifest_path.exists():
        if read_json_object(manifest_path, "v6 store manifest") != expected:
            raise V6Error(f"v6 store is owned by a different format: {manifest_path}")
    else:
        unexpected = [path for path in root.iterdir() if path.name != STORE_MANIFEST]
        if unexpected:
            raise V6Error("v6 store is nonempty and has no matching store manifest")
        replace_json(manifest_path, expected)
    end_of_day = root / "end_of_day"
    end_of_day.mkdir(exist_ok=True)
    if end_of_day.is_symlink() or not end_of_day.is_dir():
        raise V6Error(f"v6 source root is not a regular directory: {end_of_day}")
    ensure_publication_lock(root)
    fsync_directory(root)


def _session_month(timestamp: object) -> tuple[int, int] | None:
    if not isinstance(timestamp, str):
        return None
    try:
        normalized = timestamp[:-1] + "+00:00" if timestamp.endswith("Z") else timestamp
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return None
        local = parsed.astimezone(NEW_YORK)
    except (ValueError, OverflowError):
        return None
    return local.year, local.month


def _month_name(month: int) -> str:
    if not 1 <= month <= 12:
        raise V6Error(f"invalid month: {month}")
    return calendar.month_abbr[month]


def _row_tuple(bar: object, row_number: int) -> tuple[str, ...]:
    if not isinstance(bar, Mapping):
        raise V6Error(f"row {row_number}: Alpaca bar is not an object")
    missing = [field for field in ALPACA_FIELDS if bar.get(field) is None]
    if missing:
        raise V6Error(f"row {row_number}: Alpaca bar is missing fields: {', '.join(missing)}")
    return tuple(str(bar[field]) for field in ALPACA_FIELDS)


def _csv_bytes(rows: Sequence[tuple[str, ...]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(BAR_COLUMNS)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _logical_digest(rows: Sequence[tuple[str, ...]]) -> str:
    return canonical_sha256([list(row) for row in rows])


def _month_document(
    symbol: str,
    placement: str,
    rows_by_adjustment: Mapping[str, tuple[tuple[str, ...], ...]],
) -> tuple[dict[str, Any], dict[str, bytes]]:
    canonical_by_bytes: dict[bytes, str] = {}
    payloads: dict[str, bytes] = {}
    entries = []
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        rows = rows_by_adjustment.get(adjustment, ())
        entry: dict[str, Any] = {
            "adjustment": adjustment,
            "logical_file": ADJUSTMENT_FILENAMES[adjustment],
            "rows": len(rows),
            "logical_sha256": _logical_digest(rows),
        }
        if rows:
            compressed = gzip.compress(_csv_bytes(rows), compresslevel=5, mtime=0)
            canonical = canonical_by_bytes.get(compressed)
            if canonical is None:
                canonical = adjustment
                canonical_by_bytes[compressed] = canonical
                payloads[ADJUSTMENT_FILENAMES[canonical]] = compressed
            payload = {
                "storage": "file" if canonical == adjustment else "exact_copy",
                "file": ADJUSTMENT_FILENAMES[canonical],
                "sha256": sha256_bytes(compressed),
                "bytes": len(compressed),
            }
            if canonical != adjustment:
                payload["equivalent_to"] = canonical
            entry.update(
                {
                    "first_timestamp": rows[0][0],
                    "last_timestamp": rows[-1][0],
                    "payload": payload,
                }
            )
        else:
            entry["payload"] = {"storage": "empty"}
        entries.append(entry)
    return (
        {
            "schema_version": SOURCE_VERSION,
            "boundary": MONTH_BOUNDARY,
            "symbol": symbol,
            "placement": placement,
            "adjustments": entries,
        },
        payloads,
    )


def _source_inventory(
    symbol: str,
    placements: Sequence[tuple[int, int] | None],
    empty_adjustments: Sequence[str],
) -> dict[str, Any]:
    years: dict[int, list[int]] = defaultdict(list)
    unplaced = False
    for placement in placements:
        if placement is None:
            unplaced = True
        else:
            years[placement[0]].append(placement[1])
    return {
        "schema_version": SOURCE_VERSION,
        "boundary": SOURCE_INVENTORY_BOUNDARY,
        "symbol": symbol,
        "years": [
            {"year": year, "months": [_month_name(month) for month in sorted(set(months))]}
            for year, months in sorted(years.items())
        ],
        "unplaced": unplaced,
        "empty_adjustments": [
            adjustment for adjustment in ALPACA_ADJUSTMENT_MODES if adjustment in empty_adjustments
        ],
    }


def _placement_parts(placement: tuple[int, int] | None) -> tuple[str, Path]:
    if placement is None:
        return "unplaced", Path("unplaced")
    year, month = placement
    return f"{year:04d}-{month:02d}", Path(f"{year:04d}") / _month_name(month)


def _stage_month(directory: Path, document: dict[str, Any], payloads: Mapping[str, bytes]) -> Path:
    directory.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=directory.parent, prefix=f".{directory.name}.staging."))
    try:
        for filename, content in payloads.items():
            replace_bytes(staging / filename, content)
        replace_json(staging / MONTH_MANIFEST, document)
        fsync_directory(staging)
        _authenticate_month_directory(staging, document["symbol"], document["placement"])
        return staging
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def _install_month(directory: Path, staged: Path, expected_document: dict[str, Any]) -> bool:
    current_manifest = directory / MONTH_MANIFEST
    expected_bytes = document_bytes(expected_document)
    if current_manifest.is_file() and current_manifest.read_bytes() == expected_bytes:
        shutil.rmtree(staged)
        return False
    backup = directory.with_name(f".{directory.name}.previous.{os.getpid()}")
    if backup.exists():
        shutil.rmtree(backup)
    if directory.exists():
        os.replace(directory, backup)
    try:
        os.replace(staged, directory)
        fsync_directory(directory.parent)
    except BaseException:
        if backup.exists() and not directory.exists():
            os.replace(backup, directory)
            fsync_directory(directory.parent)
        raise
    shutil.rmtree(backup, ignore_errors=True)
    return True


def _month_is_current(directory: Path, expected_document: dict[str, Any]) -> bool:
    manifest_path = directory / MONTH_MANIFEST
    if not manifest_path.is_file() or manifest_path.read_bytes() != document_bytes(expected_document):
        return False
    try:
        _authenticate_month_directory(
            directory,
            expected_document["symbol"],
            expected_document["placement"],
        )
    except V6Error:
        return False
    return True


def _request_boundary(value: str, description: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise V6Error(f"invalid {description}: {value!r}") from error
    if parsed.tzinfo is None or not value.endswith("T00:00:00Z"):
        raise V6Error(f"{description} must be an exclusive UTC midnight")
    return parsed


def _request_months(
    request_start_timestamp: str,
    request_end_timestamp: str,
) -> tuple[tuple[int, int], ...]:
    start = _request_boundary(request_start_timestamp, "request start")
    end = _request_boundary(request_end_timestamp, "request end")
    if not request_start_timestamp.endswith("-01T00:00:00Z") or start >= end:
        raise V6Error("monthly update must start at the first UTC midnight of a month")
    last_date = (end - timedelta(days=1)).date()
    cursor = date(start.year, start.month, 1)
    months = []
    while cursor <= last_date:
        months.append((cursor.year, cursor.month))
        cursor = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)
    return tuple(months)


def _source_generation_from_inventory(
    symbol_root: Path,
    symbol: str,
    inventory: dict[str, Any],
) -> tuple[str, str]:
    month_receipts = []
    for placement, relative in _inventory_placements(inventory):
        document = read_json_object(
            symbol_root / relative / MONTH_MANIFEST,
            "monthly source receipt",
        )
        if document.get("symbol") != symbol or document.get("placement") != placement:
            raise V6Error(f"monthly source receipt belongs to another placement: {symbol}/{placement}")
        month_receipts.append(
            {
                "placement": placement,
                "sha256": sha256_bytes(document_bytes(document)),
            }
        )
    inventory_sha256 = sha256_bytes(document_bytes(inventory))
    generation = canonical_sha256(
        {
            "boundary": MONTH_BOUNDARY,
            "schema_version": SOURCE_VERSION,
            "symbol": symbol,
            "inventory_sha256": inventory_sha256,
            "months": month_receipts,
        }
    )
    return inventory_sha256, generation


def publish_monthly_source(
    store_root: Path | str,
    symbol: str,
    bars_by_adjustment: Mapping[str, Sequence[object]],
    request_end_timestamp: str,
) -> dict[str, Any]:
    """Publish one complete eight-view response directly into monthly v6 storage."""
    initialize_v6_store(store_root)
    if not isinstance(symbol, str) or SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise V6Error(f"invalid canonical symbol: {symbol!r}")
    if tuple(bars_by_adjustment) != ALPACA_ADJUSTMENT_MODES:
        raise V6Error("a v6 source publication must contain all eight adjustments in canonical order")
    _request_boundary(request_end_timestamp, "request end")

    placements: dict[tuple[int, int] | None, dict[str, list[tuple[str, ...]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    complete_rows: dict[str, tuple[tuple[str, ...], ...]] = {}
    empty_adjustments = []
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        rows = tuple(_row_tuple(bar, number) for number, bar in enumerate(bars_by_adjustment[adjustment], 1))
        complete_rows[adjustment] = rows
        if not rows:
            empty_adjustments.append(adjustment)
        for row in rows:
            placements[_session_month(row[0])][adjustment].append(row)

    root = Path(store_root)
    symbol_root = safe_child(root / "end_of_day", symbol, "symbol")
    symbol_root.mkdir(parents=True, exist_ok=True)
    lock_path = symbol_root / ".publish.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    changed = []
    month_receipts = []
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        expected_directories: set[Path] = set()
        with durability_batch():
            for placement in sorted(placements, key=lambda item: (-1, -1) if item is None else item):
                placement_name, relative = _placement_parts(placement)
                directory = symbol_root / relative
                expected_directories.add(relative)
                month_rows = {
                    adjustment: tuple(placements[placement].get(adjustment, ()))
                    for adjustment in ALPACA_ADJUSTMENT_MODES
                }
                document, payloads = _month_document(
                    symbol,
                    placement_name,
                    month_rows,
                )
                month_receipts.append(
                    {
                        "placement": placement_name,
                        "sha256": sha256_bytes(document_bytes(document)),
                    }
                )
                if _month_is_current(directory, document):
                    continue
                staged = _stage_month(directory, document, payloads)
                if _install_month(directory, staged, document):
                    changed.append(placement_name)
            fsync_directory(symbol_root)

        inventory = _source_inventory(symbol, tuple(placements), empty_adjustments)
        inventory_path = symbol_root / SOURCE_MANIFEST
        inventory_changed = replace_json(inventory_path, inventory)
        if inventory_changed and read_json_object(inventory_path, "published source inventory") != inventory:
            raise V6Error(f"published source inventory differs: {symbol}")
        fsync_directory(symbol_root)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    inventory_sha256 = sha256_bytes(document_bytes(inventory))
    source_generation_sha256 = canonical_sha256(
        {
            "boundary": MONTH_BOUNDARY,
            "schema_version": SOURCE_VERSION,
            "symbol": symbol,
            "inventory_sha256": inventory_sha256,
            "months": month_receipts,
        }
    )
    return {
        "symbol": symbol,
        "source_generation_sha256": source_generation_sha256,
        "request_end": request_end_timestamp,
        "changed_placements": changed,
        "inventory_changed": inventory_changed,
        "rows": {adjustment: len(complete_rows[adjustment]) for adjustment in ALPACA_ADJUSTMENT_MODES},
    }


def publish_monthly_source_update(
    store_root: Path | str,
    symbol: str,
    bars_by_adjustment: Mapping[str, Sequence[object]],
    request_start_timestamp: str,
    request_end_timestamp: str,
) -> dict[str, Any]:
    """Replace only the explicitly requested complete month views for one existing symbol."""
    initialize_v6_store(store_root)
    if not isinstance(symbol, str) or SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise V6Error(f"invalid canonical symbol: {symbol!r}")
    if tuple(bars_by_adjustment) != ALPACA_ADJUSTMENT_MODES:
        raise V6Error("a v6 source update must contain all eight adjustments in canonical order")
    target_months = _request_months(request_start_timestamp, request_end_timestamp)
    target_set = set(target_months)
    placements: dict[tuple[int, int], dict[str, list[tuple[str, ...]]]] = defaultdict(lambda: defaultdict(list))
    update_rows = {}
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        rows = tuple(_row_tuple(bar, number) for number, bar in enumerate(bars_by_adjustment[adjustment], 1))
        update_rows[adjustment] = rows
        for row in rows:
            placement = _session_month(row[0])
            if placement is None or placement not in target_set:
                raise V6Error(
                    f"monthly update returned a row outside its requested months: {symbol}/{adjustment}"
                )
            placements[placement][adjustment].append(row)

    root = Path(store_root)
    symbol_root = safe_child(root / "end_of_day", symbol, "symbol")
    inventory_path = symbol_root / SOURCE_MANIFEST
    if not inventory_path.is_file():
        raise V6Error(f"monthly update requires an existing v6 source: {symbol}")
    lock_path = symbol_root / ".publish.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    changed = []
    skipped_empty = []
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        inventory = read_json_object(inventory_path, "source inventory")
        if (
            inventory.get("schema_version") != SOURCE_VERSION
            or inventory.get("boundary") != SOURCE_INVENTORY_BOUNDARY
            or inventory.get("symbol") != symbol
        ):
            raise V6Error(f"source inventory has the wrong identity: {inventory_path}")
        existing_placements = {
            None if placement == "unplaced" else tuple(map(int, placement.split("-"))): relative
            for placement, relative in _inventory_placements(inventory)
        }
        published_months = []
        for placement in target_months:
            month_rows = {
                adjustment: tuple(placements[placement].get(adjustment, ()))
                for adjustment in ALPACA_ADJUSTMENT_MODES
            }
            if not any(month_rows.values()):
                skipped_empty.append(f"{placement[0]:04d}-{placement[1]:02d}")
                continue
            placement_name, relative = _placement_parts(placement)
            document, payloads = _month_document(symbol, placement_name, month_rows)
            directory = symbol_root / relative
            if not _month_is_current(directory, document):
                staged = _stage_month(directory, document, payloads)
                if _install_month(directory, staged, document):
                    changed.append(placement_name)
            existing_placements[placement] = relative
            published_months.append(placement)

        empty_adjustments = set(inventory.get("empty_adjustments", ()))
        for adjustment, rows in update_rows.items():
            if rows:
                empty_adjustments.discard(adjustment)
        revised_inventory = _source_inventory(
            symbol,
            tuple(existing_placements),
            tuple(empty_adjustments),
        )
        inventory_changed = replace_json(inventory_path, revised_inventory)
        if (
            inventory_changed
            and read_json_object(
                inventory_path,
                "published source inventory",
            )
            != revised_inventory
        ):
            raise V6Error(f"published source inventory differs: {symbol}")
        fsync_directory(symbol_root)
        _inventory_sha256, source_generation = _source_generation_from_inventory(
            symbol_root,
            symbol,
            revised_inventory,
        )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)

    return {
        "symbol": symbol,
        "source_generation_sha256": source_generation,
        "request_start": request_start_timestamp,
        "request_end": request_end_timestamp,
        "requested_placements": [f"{year:04d}-{month:02d}" for year, month in target_months],
        "published_placements": [f"{year:04d}-{month:02d}" for year, month in published_months],
        "skipped_empty_placements": skipped_empty,
        "changed_placements": changed,
        "inventory_changed": inventory_changed,
        "rows": {adjustment: len(update_rows[adjustment]) for adjustment in ALPACA_ADJUSTMENT_MODES},
    }


def _inventory_placements(inventory: dict[str, Any]) -> list[tuple[str, Path]]:
    placements: list[tuple[str, Path]] = []
    years = inventory.get("years")
    if not isinstance(years, list):
        raise V6Error("source inventory has no years array")
    previous_year = 0
    for year_entry in years:
        if not isinstance(year_entry, dict) or type(year_entry.get("year")) is not int:
            raise V6Error("source inventory has an invalid year entry")
        year = year_entry["year"]
        months = year_entry.get("months")
        if year <= previous_year or not isinstance(months, list) or len(months) != len(set(months)):
            raise V6Error("source inventory years or months are not canonical")
        previous_year = year
        month_numbers = []
        for month_name in months:
            if month_name not in calendar.month_abbr[1:]:
                raise V6Error(f"source inventory has invalid month: {month_name!r}")
            month_numbers.append(list(calendar.month_abbr).index(month_name))
        if month_numbers != sorted(month_numbers):
            raise V6Error("source inventory months are not ordered")
        placements.extend(
            (f"{year:04d}-{month:02d}", Path(f"{year:04d}") / _month_name(month)) for month in month_numbers
        )
    if inventory.get("unplaced") is True:
        placements.append(("unplaced", Path("unplaced")))
    elif inventory.get("unplaced") is not False:
        raise V6Error("source inventory has invalid unplaced state")
    return placements


def _read_payload_rows(path: Path) -> tuple[tuple[str, ...], ...]:
    try:
        with gzip.open(path, mode="rt", encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream)
            if tuple(next(reader, ())) != BAR_COLUMNS:
                raise V6Error(f"monthly payload has unexpected CSV fields: {path}")
            rows = []
            for row_number, row in enumerate(reader, start=1):
                if len(row) != len(BAR_COLUMNS):
                    raise V6Error(f"{path}: row {row_number} has an invalid field count")
                rows.append(tuple(row))
            return tuple(rows)
    except V6Error:
        raise
    except (OSError, EOFError, UnicodeError, csv.Error) as error:
        raise V6Error(f"could not decode monthly payload {path}: {error}") from error


def _load_month_directory(
    directory: Path,
    symbol: str,
    placement: str,
) -> tuple[
    dict[str, Any],
    dict[str, tuple[tuple[str, ...], ...]],
    dict[str, str],
    str,
]:
    """Load one already-published month without re-authenticating its output."""
    manifest_path = directory / MONTH_MANIFEST
    document = read_json_object(manifest_path, "monthly source receipt")
    if (
        document.get("schema_version") != SOURCE_VERSION
        or document.get("boundary") != MONTH_BOUNDARY
        or document.get("symbol") != symbol
        or document.get("placement") != placement
    ):
        raise V6Error(f"monthly source receipt has the wrong identity: {manifest_path}")
    entries = document.get("adjustments")
    if not isinstance(entries, list) or [
        entry.get("adjustment") for entry in entries if isinstance(entry, dict)
    ] != list(ALPACA_ADJUSTMENT_MODES):
        raise V6Error(f"monthly source receipt does not represent all adjustments: {manifest_path}")

    rows_by_adjustment = {}
    logical_sha256_by_adjustment = {}
    rows_by_payload: dict[str, tuple[tuple[str, ...], ...]] = {}
    for entry in entries:
        adjustment = entry["adjustment"]
        payload = entry.get("payload")
        logical_sha256 = entry.get("logical_sha256")
        if (
            entry.get("logical_file") != ADJUSTMENT_FILENAMES[adjustment]
            or not isinstance(logical_sha256, str)
            or not isinstance(payload, dict)
        ):
            raise V6Error(f"monthly source adjustment entry is malformed: {manifest_path}")
        storage = payload.get("storage")
        if storage == "empty":
            rows = ()
        elif storage in {"file", "exact_copy"}:
            filename = payload.get("file")
            if not isinstance(filename, str) or Path(filename).name != filename:
                raise V6Error(f"monthly source payload filename is unsafe: {manifest_path}")
            if filename not in rows_by_payload:
                rows_by_payload[filename] = _read_payload_rows(directory / filename)
            rows = rows_by_payload[filename]
        else:
            raise V6Error(f"monthly source has invalid storage kind: {manifest_path}")
        rows_by_adjustment[adjustment] = rows
        logical_sha256_by_adjustment[adjustment] = logical_sha256
    return (
        document,
        rows_by_adjustment,
        logical_sha256_by_adjustment,
        sha256_bytes(document_bytes(document)),
    )


def _adjustment_receipt_digest(
    symbol: str,
    adjustment: str,
    months: Sequence[MonthlyLogicalSource],
) -> str:
    """Identify one logical view from already-published monthly receipt facts."""
    return canonical_sha256(
        {
            "boundary": MONTH_BOUNDARY,
            "schema_version": SOURCE_VERSION,
            "symbol": symbol,
            "adjustment": adjustment,
            "months": [
                {
                    "placement": month.placement,
                    "month_receipt_sha256": month.month_receipt_sha256,
                    "logical_sha256": month.logical_sha256,
                }
                for month in months
            ],
        }
    )


def load_monthly_source(store_root: Path | str, symbol: str) -> AuthenticatedMonthlySource:
    """Consume the published monthly boundary without auditing it again."""
    validate_boundary_change_log("source")
    root = Path(store_root)
    symbol_root = safe_child(root / "end_of_day", symbol, "symbol")
    inventory_path = symbol_root / SOURCE_MANIFEST
    inventory = read_json_object(inventory_path, "source inventory")
    if (
        inventory.get("schema_version") != SOURCE_VERSION
        or inventory.get("boundary") != SOURCE_INVENTORY_BOUNDARY
        or inventory.get("symbol") != symbol
    ):
        raise V6Error(f"source inventory has the wrong identity: {inventory_path}")

    rows_by_adjustment: dict[str, list[tuple[str, ...]]] = {
        adjustment: [] for adjustment in ALPACA_ADJUSTMENT_MODES
    }
    receipts_by_adjustment: dict[str, list[tuple[str, str]]] = {
        adjustment: [] for adjustment in ALPACA_ADJUSTMENT_MODES
    }
    months_by_adjustment: dict[str, list[MonthlyLogicalSource]] = {
        adjustment: [] for adjustment in ALPACA_ADJUSTMENT_MODES
    }
    placement_receipts = []
    for placement, relative in _inventory_placements(inventory):
        _document, month_rows, logical_digests, receipt_sha = _load_month_directory(
            symbol_root / relative,
            symbol,
            placement,
        )
        placement_receipts.append({"placement": placement, "sha256": receipt_sha})
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            rows_by_adjustment[adjustment].extend(month_rows[adjustment])
            receipts_by_adjustment[adjustment].append((placement, receipt_sha))
            row_tuples = month_rows[adjustment]
            months_by_adjustment[adjustment].append(
                MonthlyLogicalSource(
                    placement=placement,
                    month_receipt_sha256=receipt_sha,
                    rows=tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in row_tuples),
                    row_tuples=row_tuples,
                    logical_sha256=logical_digests[adjustment],
                )
            )

    adjustments = []
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        rows = tuple(rows_by_adjustment[adjustment])
        months = tuple(months_by_adjustment[adjustment])
        adjustments.append(
            LogicalAdjustmentSource(
                symbol=symbol,
                adjustment=adjustment,
                rows=tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in rows),
                row_tuples=rows,
                logical_sha256=_adjustment_receipt_digest(symbol, adjustment, months),
                month_receipt_sha256=tuple(receipts_by_adjustment[adjustment]),
                months=months,
            )
        )
    inventory_sha256 = sha256_bytes(document_bytes(inventory))
    return AuthenticatedMonthlySource(
        symbol=symbol,
        inventory=inventory,
        inventory_sha256=inventory_sha256,
        source_generation_sha256=canonical_sha256(
            {
                "boundary": MONTH_BOUNDARY,
                "schema_version": SOURCE_VERSION,
                "symbol": symbol,
                "inventory_sha256": inventory_sha256,
                "months": placement_receipts,
            }
        ),
        adjustments=tuple(adjustments),
    )


def load_monthly_adjustment(
    store_root: Path | str,
    symbol: str,
    adjustment: str,
) -> LogicalAdjustmentSource:
    """Read one logical adjustment from the already-published monthly source."""
    if adjustment not in ALPACA_ADJUSTMENT_MODES:
        raise V6Error(f"unsupported adjustment: {adjustment!r}")
    root = Path(store_root)
    symbol_root = safe_child(root / "end_of_day", symbol, "symbol")
    inventory = read_json_object(symbol_root / SOURCE_MANIFEST, "source inventory")
    rows: list[tuple[str, ...]] = []
    month_receipts = []
    months = []
    for placement, relative in _inventory_placements(inventory):
        manifest_path = symbol_root / relative / MONTH_MANIFEST
        document = read_json_object(manifest_path, "monthly source receipt")
        entries = document.get("adjustments")
        matches = (
            [entry for entry in entries if isinstance(entry, dict) and entry.get("adjustment") == adjustment]
            if isinstance(entries, list)
            else []
        )
        if len(matches) != 1:
            raise V6Error(f"monthly source has no unique {adjustment!r} view: {manifest_path}")
        payload = matches[0].get("payload")
        logical_sha256 = matches[0].get("logical_sha256")
        if not isinstance(payload, dict) or not isinstance(logical_sha256, str):
            raise V6Error(f"monthly source has a malformed {adjustment!r} view: {manifest_path}")
        if payload.get("storage") == "empty":
            month_rows = ()
        else:
            filename = payload.get("file")
            if not isinstance(filename, str) or Path(filename).name != filename:
                raise V6Error(f"monthly source payload filename is unsafe: {manifest_path}")
            month_rows = _read_payload_rows(manifest_path.parent / filename)
        rows.extend(month_rows)
        receipt_sha256 = sha256_bytes(document_bytes(document))
        month_receipts.append((placement, receipt_sha256))
        months.append(
            MonthlyLogicalSource(
                placement=placement,
                month_receipt_sha256=receipt_sha256,
                rows=tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in month_rows),
                row_tuples=month_rows,
                logical_sha256=logical_sha256,
            )
        )
    row_tuples = tuple(rows)
    month_tuple = tuple(months)
    return LogicalAdjustmentSource(
        symbol=symbol,
        adjustment=adjustment,
        rows=tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in row_tuples),
        row_tuples=row_tuples,
        logical_sha256=_adjustment_receipt_digest(symbol, adjustment, month_tuple),
        month_receipt_sha256=tuple(month_receipts),
        months=month_tuple,
    )


def read_monthly_adjustment_rows(
    store_root: Path | str,
    symbol: str,
    adjustment: str,
    *,
    payload_cache: dict[Path, tuple[tuple[str, ...], ...]] | None = None,
) -> tuple[dict[str, str], ...]:
    """Read one published logical view without calculating any audit digest."""
    root = Path(store_root)
    symbol_root = safe_child(root / "end_of_day", symbol, "symbol")
    inventory = read_json_object(symbol_root / SOURCE_MANIFEST, "source inventory")
    payload_cache = {} if payload_cache is None else payload_cache
    rows: list[tuple[str, ...]] = []
    for _placement, relative in _inventory_placements(inventory):
        manifest_path = symbol_root / relative / MONTH_MANIFEST
        document = read_json_object(manifest_path, "monthly source receipt")
        entries = document.get("adjustments")
        matches = (
            [entry for entry in entries if isinstance(entry, dict) and entry.get("adjustment") == adjustment]
            if isinstance(entries, list)
            else []
        )
        if len(matches) != 1:
            raise V6Error(f"monthly source has no unique {adjustment!r} view: {manifest_path}")
        payload = matches[0].get("payload")
        if not isinstance(payload, dict):
            raise V6Error(f"monthly source has a malformed {adjustment!r} view: {manifest_path}")
        if payload.get("storage") == "empty":
            continue
        filename = payload.get("file")
        if not isinstance(filename, str) or Path(filename).name != filename:
            raise V6Error(f"monthly source payload filename is unsafe: {manifest_path}")
        payload_path = manifest_path.parent / filename
        if payload_path not in payload_cache:
            payload_cache[payload_path] = _read_payload_rows(payload_path)
        rows.extend(payload_cache[payload_path])
    return tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in rows)


def _authenticate_month_directory(
    directory: Path,
    symbol: str,
    placement: str,
) -> tuple[dict[str, Any], dict[str, tuple[tuple[str, ...], ...]], str]:
    manifest_path = directory / MONTH_MANIFEST
    if directory.is_symlink() or manifest_path.is_symlink():
        raise V6Error(f"monthly source boundary cannot contain symlinks: {directory}")
    document = read_json_object(manifest_path, "monthly source receipt")
    if (
        document.get("schema_version") != SOURCE_VERSION
        or document.get("boundary") != MONTH_BOUNDARY
        or document.get("symbol") != symbol
        or document.get("placement") != placement
    ):
        raise V6Error(f"monthly source receipt has the wrong identity: {manifest_path}")
    entries = document.get("adjustments")
    if not isinstance(entries, list) or [
        entry.get("adjustment") for entry in entries if isinstance(entry, dict)
    ] != list(ALPACA_ADJUSTMENT_MODES):
        raise V6Error(f"monthly source receipt does not represent all adjustments: {manifest_path}")
    rows_by_adjustment: dict[str, tuple[tuple[str, ...], ...]] = {}
    canonical_entries: dict[str, dict[str, Any]] = {}
    payload_metadata_by_filename: dict[str, tuple[int, str]] = {}
    rows_by_payload: dict[str, tuple[tuple[str, ...], ...]] = {}
    used_payloads = set()
    for entry in entries:
        adjustment = entry["adjustment"]
        if entry.get("logical_file") != ADJUSTMENT_FILENAMES[adjustment]:
            raise V6Error(f"monthly source logical filename differs: {manifest_path}")
        rows_count = entry.get("rows")
        logical_digest = entry.get("logical_sha256")
        payload = entry.get("payload")
        if (
            type(rows_count) is not int
            or rows_count < 0
            or not isinstance(logical_digest, str)
            or not isinstance(payload, dict)
        ):
            raise V6Error(f"monthly source adjustment entry is malformed: {manifest_path}")
        storage = payload.get("storage")
        if storage == "empty":
            if rows_count != 0 or set(payload) != {"storage"}:
                raise V6Error(f"monthly empty source entry is malformed: {manifest_path}")
            rows = ()
        else:
            filename = payload.get("file")
            if not isinstance(filename, str) or Path(filename).name != filename:
                raise V6Error(f"monthly source payload filename is unsafe: {manifest_path}")
            payload_path = directory / filename
            if filename not in payload_metadata_by_filename:
                if not payload_path.is_file() or payload_path.is_symlink():
                    raise V6Error(f"monthly source payload is unavailable: {payload_path}")
                stat = payload_path.stat()
                payload_metadata_by_filename[filename] = (
                    stat.st_size,
                    sha256_file(payload_path),
                )
            observed_size, observed_digest = payload_metadata_by_filename[filename]
            if payload.get("bytes") != observed_size or payload.get("sha256") != observed_digest:
                raise V6Error(f"monthly source payload authentication failed: {payload_path}")
            if filename not in rows_by_payload:
                rows_by_payload[filename] = _read_payload_rows(payload_path)
            rows = rows_by_payload[filename]
            used_payloads.add(filename)
            if storage == "file":
                if filename != ADJUSTMENT_FILENAMES[adjustment] or "equivalent_to" in payload:
                    raise V6Error(f"monthly canonical payload is malformed: {manifest_path}")
                canonical_entries[adjustment] = entry
            elif storage == "exact_copy":
                equivalent = payload.get("equivalent_to")
                canonical = canonical_entries.get(equivalent)
                if canonical is None or canonical["payload"].get("storage") != "file":
                    raise V6Error(f"monthly exact-copy alias is not direct: {manifest_path}")
                for field in ("file", "sha256", "bytes"):
                    if payload.get(field) != canonical["payload"].get(field):
                        raise V6Error(f"monthly exact-copy payload differs: {manifest_path}")
                for field in ("rows", "logical_sha256", "first_timestamp", "last_timestamp"):
                    if entry.get(field) != canonical.get(field):
                        raise V6Error(f"monthly exact-copy logical metadata differs: {manifest_path}")
            else:
                raise V6Error(f"monthly source has invalid storage kind: {manifest_path}")
            if (
                rows_count == 0
                or entry.get("first_timestamp") != rows[0][0]
                or entry.get("last_timestamp") != rows[-1][0]
            ):
                raise V6Error(f"monthly source timestamp bounds differ: {manifest_path}")
        if len(rows) != rows_count or _logical_digest(rows) != logical_digest:
            raise V6Error(f"monthly source logical digest differs: {manifest_path}")
        rows_by_adjustment[adjustment] = rows

    unexpected_payloads = {path.name for path in directory.glob("*.csv.gz") if path.name not in used_payloads}
    if unexpected_payloads:
        raise V6Error(f"monthly source contains unowned payloads: {sorted(unexpected_payloads)!r}")
    return document, rows_by_adjustment, sha256_file(manifest_path)


def authenticate_monthly_source(
    store_root: Path | str,
    symbol: str,
) -> AuthenticatedMonthlySource:
    validate_boundary_change_log("source")
    root = Path(store_root)
    if read_json_object(root / STORE_MANIFEST, "v6 store manifest") != _store_document():
        raise V6Error("v6 store manifest does not match the supported format")
    symbol_root = safe_child(root / "end_of_day", symbol, "symbol")
    inventory_path = symbol_root / SOURCE_MANIFEST
    if symbol_root.is_symlink() or inventory_path.is_symlink():
        raise V6Error(f"v6 source boundary cannot contain symlinks: {symbol_root}")
    inventory = read_json_object(inventory_path, "source inventory")
    if (
        inventory.get("schema_version") != SOURCE_VERSION
        or inventory.get("boundary") != SOURCE_INVENTORY_BOUNDARY
        or inventory.get("symbol") != symbol
    ):
        raise V6Error(f"source inventory has the wrong identity: {inventory_path}")
    empty_adjustments = inventory.get("empty_adjustments")
    if not isinstance(empty_adjustments, list) or empty_adjustments != [
        adjustment for adjustment in ALPACA_ADJUSTMENT_MODES if adjustment in empty_adjustments
    ]:
        raise V6Error(f"source inventory has invalid empty adjustments: {inventory_path}")

    rows_by_adjustment: dict[str, list[tuple[str, ...]]] = {
        adjustment: [] for adjustment in ALPACA_ADJUSTMENT_MODES
    }
    receipts_by_adjustment: dict[str, list[tuple[str, str]]] = {
        adjustment: [] for adjustment in ALPACA_ADJUSTMENT_MODES
    }
    months_by_adjustment: dict[str, list[MonthlyLogicalSource]] = {
        adjustment: [] for adjustment in ALPACA_ADJUSTMENT_MODES
    }
    placement_receipts = []
    for placement, relative in _inventory_placements(inventory):
        document, month_rows, receipt_sha = _authenticate_month_directory(
            symbol_root / relative,
            symbol,
            placement,
        )
        placement_receipts.append({"placement": placement, "sha256": receipt_sha})
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            rows_by_adjustment[adjustment].extend(month_rows[adjustment])
            receipts_by_adjustment[adjustment].append((placement, receipt_sha))
            row_tuples = month_rows[adjustment]
            months_by_adjustment[adjustment].append(
                MonthlyLogicalSource(
                    placement=placement,
                    month_receipt_sha256=receipt_sha,
                    rows=tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in row_tuples),
                    row_tuples=row_tuples,
                    logical_sha256=_logical_digest(row_tuples),
                )
            )

    adjustments = []
    for adjustment in ALPACA_ADJUSTMENT_MODES:
        rows = tuple(rows_by_adjustment[adjustment])
        if not rows and adjustment not in empty_adjustments:
            raise V6Error(f"source inventory does not declare empty adjustment {adjustment}")
        if rows and adjustment in empty_adjustments:
            raise V6Error(f"source inventory incorrectly declares nonempty adjustment {adjustment}")
        row_dicts = tuple(dict(zip(ALPACA_FIELDS, row, strict=True)) for row in rows)
        adjustments.append(
            LogicalAdjustmentSource(
                symbol=symbol,
                adjustment=adjustment,
                rows=row_dicts,
                row_tuples=rows,
                logical_sha256=_logical_digest(rows),
                month_receipt_sha256=tuple(receipts_by_adjustment[adjustment]),
                months=tuple(months_by_adjustment[adjustment]),
            )
        )
    inventory_sha256 = sha256_file(inventory_path)
    generation = canonical_sha256(
        {
            "boundary": MONTH_BOUNDARY,
            "schema_version": SOURCE_VERSION,
            "symbol": symbol,
            "inventory_sha256": inventory_sha256,
            "months": placement_receipts,
        }
    )
    return AuthenticatedMonthlySource(
        symbol=symbol,
        inventory=inventory,
        inventory_sha256=inventory_sha256,
        source_generation_sha256=generation,
        adjustments=tuple(adjustments),
    )


__all__ = [
    "ADJUSTMENT_FILENAMES",
    "AuthenticatedMonthlySource",
    "BAR_COLUMNS",
    "LogicalAdjustmentSource",
    "MonthlyLogicalSource",
    "MONTH_BOUNDARY",
    "SOURCE_MANIFEST",
    "authenticate_monthly_source",
    "initialize_v6_store",
    "load_monthly_adjustment",
    "load_monthly_source",
    "publish_monthly_source",
    "publish_monthly_source_update",
    "read_monthly_adjustment_rows",
]

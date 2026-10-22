"""Per-identity CSV production from one accepted online catalogue snapshot."""

import csv
import fcntl
import hashlib
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import monotonic

from libs.alpaca_serving_catalogue import storage as catalogue_storage
from libs.alpaca_serving_catalogue.contract import CatalogueError

from . import (
    ADJUSTMENTS,
    API_VERSION,
    CatalogChangedError,
    ReaderBusyError,
    StandaloneReaderError,
)
from .standalone import _date

SCHEMA_VERSION = 1
FIELDS = ("open", "high", "low", "close", "volume", "trade_count", "vwap")
PREFIXES = dict(
    zip(
        ADJUSTMENTS,
        ("raw", "split", "dividend", "spin_off", "split_dividend", "split_spin_off", "dividend_spin_off", "all"),
        strict=True,
    )
)
VIEW_COLUMNS = {
    adjustment: {field: f"{PREFIXES[adjustment]}_{field}" for field in FIELDS} for adjustment in ADJUSTMENTS
}
LOGICAL_COLUMNS = ("date", *(column for columns in VIEW_COLUMNS.values() for column in columns.values()))


def _selection(data, public_ids, start, end):
    start, end = _date(start, "start"), _date(end, "end")
    if start is not None and end is not None and start > end:
        raise ValueError("start must be on or before end")
    if public_ids is None:
        securities = data._securities
    else:
        if not isinstance(public_ids, (list, tuple)) or not public_ids:
            raise ValueError("public_ids must be a nonempty list or tuple, or None for all online IDs")
        securities = {security.public_id: security for security in map(data.get_security, public_ids)}
    selected, groups = {}, {}
    for public_id, security in sorted(securities.items()):
        if (start is not None and security.last_session < start) or (
            end is not None and security.first_session > end
        ):
            continue
        selected[public_id] = security
        groups.setdefault(data._records[public_id]["group_key"], []).append(public_id)
    return selected, groups, start, end


def _range(start, end):
    return {
        "start": start.isoformat() if start is not None else None,
        "end": end.isoformat() if end is not None else None,
        "inclusive": True,
    }


def _plan(data, selected, groups, start, end):
    sources = {symbol for key in groups for symbol in data._groups[key]["references"]["pricing"]}
    lineage = {
        symbol
        for key in groups
        for symbol, generation in data._groups[key]["references"]["lineage"].items()
        if generation is not None
    }
    return {
        "catalogue_generation": data.catalog_generation(),
        "identities": len(selected),
        "groups": len(groups),
        "sources": len(sources),
        "requested_range": _range(start, end),
        "metadata_read_bounds": {
            "source_manifests": len(sources),
            "lineage_receipts": len(lineage),
            "lineage_boundary_manifests": 3 * len(lineage),
            "pricing_boundary_manifests": 3 * len(sources),
        },
        "payload_file_read_upper_bound": 3 * len(ADJUSTMENTS) * len(sources),
        "published_output_file_upper_bound": 2 * len(selected) + 1,
        "output_lock_files": 1,
        "temporary_manifest_files": 1,
        "planning_payload_reads": 0,
    }


def plan_export(data, *, public_ids=None, start=None, end=None):
    return _plan(data, *_selection(data, public_ids, start, end))


def _columns(views):
    """Project exact numeric columns and their aliases from already needed rows.

    Source manifests govern physical reuse in the reader. Output equality is
    scoped to this identity and date range: a different source digest may differ
    only outside that slice. This projection never reopens or hashes a source.
    """
    columns, aliases, series = ["date"], {}, []
    text_columns, numeric_columns = {}, {}
    for adjustment in ADJUSTMENTS:
        bars = views[adjustment]["bars"]
        for field, logical_column in VIEW_COLUMNS[adjustment].items():
            values = tuple(bar[field] for bar in bars)
            canonical = text_columns.get(values)
            if canonical is None:
                numbers = tuple(map(Decimal, values))
                canonical = numeric_columns.get(numbers)
                if canonical is None:
                    canonical = logical_column
                    numeric_columns[numbers] = canonical
                    columns.append(canonical)
                    series.append(values)
                text_columns[values] = canonical
            aliases[logical_column] = canonical
    return columns, aliases, series


class _CsvWriter:
    """Hash new output bytes as they enter the buffered destination stream."""

    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()
        self.bytes = 0

    def write(self, text):
        encoded = text.encode("utf-8")
        self.stream.write(encoded)
        self.digest.update(encoded)
        self.bytes += len(encoded)
        return len(text)


def _write_json(path, document):
    encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(encoded)
        stream.write(b"\n")


@contextmanager
def _publication_lock(output, wait):
    # One stable output lock, never a source inventory. Existence is not state.
    with (output / ".export.lock").open("a+b") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError as error:
            raise ReaderBusyError("CSV export destination is busy; retry or use --wait") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _write_identity(data, directory, security, views, start, end):
    raw = views["raw"]["bars"]
    if not raw:
        return None
    columns, aliases, series = _columns(views)
    public_id = security.public_id
    identity_directory = directory / public_id
    identity_directory.mkdir()
    csv_name = f"{public_id}_eod.csv"
    digest_name = f"{public_id}_digest.json"
    with (identity_directory / csv_name).open("xb") as stream:
        output = _CsvWriter(stream)
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(columns)
        # Serving guarantees aligned sessions across the eight views. Only the
        # new CSV's rectangular shape is enforced by strict transposition.
        writer.writerows(zip((bar["session"] for bar in raw), *series, strict=True))
    view_columns = {
        adjustment: {"columns": {field: aliases[column] for field, column in logical.items()}}
        for adjustment, logical in VIEW_COLUMNS.items()
    }
    digest = {
        "schema_version": SCHEMA_VERSION,
        "api_version": API_VERSION,
        "public_id": public_id,
        "catalogue_generation": data.catalog_generation(),
        "columns": columns,
        "column_aliases": aliases,
        "views": view_columns,
        "range": {"start": raw[0]["session"], "end": raw[-1]["session"], "inclusive": True},
        "requested_range": _range(start, end),
        "rows": len(raw),
        "csv": {"file": csv_name, "sha256": output.digest.hexdigest(), "bytes": output.bytes},
        "security": security.to_dict(),
    }
    _write_json(identity_directory / digest_name, digest)
    return {
        "csv": f"{directory.name}/{public_id}/{csv_name}",
        "digest": f"{directory.name}/{public_id}/{digest_name}",
        "rows": len(raw),
        "start": raw[0]["session"],
        "end": raw[-1]["session"],
        "columns": columns,
        "csv_bytes": output.bytes,
    }


def write_export(data, destination, *, public_ids=None, start=None, end=None, progress=None):
    selected, groups, start, end = _selection(data, public_ids, start, end)
    plan = _plan(data, selected, groups, start, end)
    output = Path(destination).absolute()
    if output.resolve().is_relative_to(data.store.resolve()):
        raise ValueError("CSV export destination must be outside the source store")
    if progress:
        progress("csv_export_plan", plan)
    output.mkdir(parents=True, exist_ok=True)
    with _publication_lock(output, data._reader.wait):
        started_at = datetime.now(timezone.utc)
        directory = output / started_at.strftime("%Y%m%dT%H%M%S.%fZ")
        directory.mkdir()
        identities = {}
        counts = {"identities": 0, "groups": 0, "rows": 0, "csv_bytes": 0, "physical_columns": 0}
        done, began = 0, monotonic()
        for key in sorted(groups):
            expected = data._groups[key]
            try:
                with data._reader._open_group(
                    expected["locator"],
                    expected=expected["references"],
                    before_payloads=lambda: catalogue_storage.assert_group(
                        data.store, key, expected["decision_generation"]
                    ),
                ) as group:
                    for public_id, views in group._iter_export_histories(groups[key], start=start, end=end):
                        entry = _write_identity(data, directory, selected[public_id], views, start, end)
                        if entry is not None:
                            identities[public_id] = entry
                            counts["identities"] += 1
                            counts["rows"] += entry["rows"]
                            counts["csv_bytes"] += entry["csv_bytes"]
                            counts["physical_columns"] += len(entry["columns"]) - 1
                        done += 1
                        if progress:
                            elapsed = monotonic() - began
                            progress(
                                "csv_export",
                                {
                                    "done": done,
                                    "total": len(selected),
                                    **counts,
                                    "elapsed_seconds": round(elapsed, 2),
                                    "estimated_remaining_seconds": round(
                                        elapsed * (len(selected) - done) / done, 2
                                    ),
                                },
                            )
                        del views
            except ReaderBusyError:
                raise
            except (CatalogueError, StandaloneReaderError) as error:
                raise CatalogChangedError(
                    "selected serving history is no longer compatible; finish maintenance and open a new DataLibrary"
                ) from error
            counts["groups"] += 1
        counts["logical_columns"] = len(identities) * len(ADJUSTMENTS) * len(FIELDS)
        completed_at = datetime.now(timezone.utc)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "api_version": API_VERSION,
            "format": "alpaca_public_id_eod_csv",
            "export_directory": directory.name,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "catalogue": data.catalog().to_dict(),
            "requested_range": _range(start, end),
            "csv": {
                "encoding": "utf-8",
                "delimiter": ",",
                "line_terminator": "\n",
                "date_column": "date",
                "date_timezone": "America/New_York",
                "numeric_encoding": "exact decimal strings; trade_count is integral",
            },
            "columns": list(LOGICAL_COLUMNS),
            "adjustments": {
                adjustment: {"prefix": PREFIXES[adjustment], "columns": columns}
                for adjustment, columns in VIEW_COLUMNS.items()
            },
            "counts": counts,
            "identities": identities,
        }
        # The manifest is the sole completion selector. Its previous version
        # remains authoritative on any failure; incomplete run directories are
        # unselected. No directory crawl, reread, fsync or payload audit follows.
        temporary = output / f".final_manifest.{directory.name}.tmp"
        if progress:
            # Consumer callbacks finish before the commit. A logging failure
            # cannot report an unsuccessful export after replacing its selector.
            progress("csv_export_publish", {"done": done, "total": len(selected), **counts})
        try:
            _write_json(temporary, manifest)
            os.replace(temporary, output / "final_manifest.json")
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return {
            "output": str(directory),
            "manifest": str(output / "final_manifest.json"),
            "catalogue_generation": data.catalog_generation(),
            "elapsed_seconds": round(monotonic() - began, 2),
            **counts,
        }

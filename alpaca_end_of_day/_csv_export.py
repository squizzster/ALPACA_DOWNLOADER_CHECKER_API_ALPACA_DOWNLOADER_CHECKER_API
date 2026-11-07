"""Per-identity CSV production from one accepted online catalogue snapshot."""

import csv
import fcntl
import hashlib
import io
import json
import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from time import monotonic

from libs.alpaca_serving_catalogue import storage as catalogue_storage
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.store_lock import StoreBusyError, operation_lock

from . import (
    ADJUSTMENTS,
    API_VERSION,
    CatalogChangedError,
    ReaderBusyError,
    StandaloneReaderError,
)
from .standalone import _date

SCHEMA_VERSION = 2
GIB = 1024**3
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
    # The opened DataLibrary owns eligibility and group references. Planning is
    # an in-memory projection of that accepted catalogue, never a source census.
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


def _workers(value):
    if type(value) is not int or value not in (1, 2, 3, 4):
        raise ValueError("workers must be between 1 and 4")
    return value


def _available_memory():
    # Linux MemAvailable includes memory reclaimable without swapping. This is
    # one operating-system observation, not a source-store inventory or cache.
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("available memory is unknown; specify memory_bytes explicitly")


def _memory_budget(value):
    if value is None:
        return min(5 * GIB, max(GIB, _available_memory() // 2))
    if type(value) is not int or value < 1:
        raise ValueError("memory_bytes must be a positive integer or None")
    return value


def _plan(data, selected, groups, start, end, workers, memory_bytes):
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
        "workers": workers,
        "maximum_active_groups": min(1, len(groups)),
        "memory_bytes": memory_bytes,
        "io_strategy": "batched_read_then_write",
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


def plan_export(data, *, public_ids=None, start=None, end=None, workers=2, memory_bytes=None):
    workers = _workers(workers)
    return _plan(data, *_selection(data, public_ids, start, end), workers, _memory_budget(memory_bytes))


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
    """Encode and hash new CSV bytes as they enter the in-memory buffer."""

    def __init__(self, stream, maximum_bytes):
        self.stream = stream
        self.maximum_bytes = maximum_bytes
        self.digest = hashlib.sha256()
        self.bytes = 0

    def write(self, text):
        encoded = text.encode("utf-8")
        if self.bytes + len(encoded) > self.maximum_bytes:
            raise MemoryError("one identity CSV/digest exceeds memory_bytes; increase the export budget")
        self.stream.write(encoded)
        self.digest.update(encoded)
        self.bytes += len(encoded)
        return len(text)


def _json_bytes(document):
    return (
        json.dumps(document, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"
    )


@dataclass
class _PreparedIdentity:
    public_id: str
    csv: io.BytesIO
    digest: bytes
    entry: dict

    @property
    def size(self):
        return self.entry["csv_bytes"] + len(self.digest)

    def close(self):
        self.csv.close()


def _write_json(path, document):
    encoded = document if isinstance(document, bytes) else _json_bytes(document)
    with path.open("xb") as stream:
        stream.write(encoded)


@contextmanager
def _publication_lock(output):
    # One stable output lock, never a source inventory. Existence is not state.
    with (output / ".export.lock").open("a+b") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ReaderBusyError("CSV export destination is busy; retry later") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _prepare_identity(data, security, views, start, end, memory_bytes):
    first, last = security.first_session.isoformat(), security.last_session.isoformat()
    views = {
        adjustment: {**view, "bars": [bar for bar in view["bars"] if first <= bar["session"] <= last]}
        for adjustment, view in views.items()
    }
    raw = views["raw"]["bars"]
    if not raw:
        return None
    columns, aliases, series = _columns(views)
    public_id = security.public_id
    csv_name = f"{public_id}_eod.csv"
    digest_name = f"{public_id}_digest.json"
    # This worker consumes only in-memory histories. It never opens a file.
    buffer = io.BytesIO()
    output = _CsvWriter(buffer, memory_bytes)
    try:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(columns)
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
            "security": {
                **security.to_dict(),
                # The opened catalogue already selected the dated labels from
                # all approved views. Carry that handoff intact, including for
                # date-filtered exports; do not reconstruct it from price rows
                # or broader ownership evidence.
                "ticker_ranges": data._records[public_id]["public"]["ticker_ranges"],
            },
        }
        entry = {
            "csv": f"{public_id}/{csv_name}",
            "digest": f"{public_id}/{digest_name}",
            "rows": len(raw),
            "start": raw[0]["session"],
            "end": raw[-1]["session"],
            "columns": columns,
            "csv_bytes": output.bytes,
        }

        prepared = _PreparedIdentity(public_id, buffer, _json_bytes(digest), entry)
        if prepared.size > memory_bytes:
            raise MemoryError("one identity CSV/digest exceeds memory_bytes; increase the export budget")
        return prepared
    except BaseException:
        buffer.close()
        raise


def _write_identity(directory, prepared):
    # Only the coordinator calls this, while source iteration is suspended.
    identity_directory = directory / prepared.public_id
    identity_directory.mkdir()
    with prepared.csv.getbuffer() as content:
        with (directory / prepared.entry["csv"]).open("xb") as stream:
            stream.write(content)
    _write_json(directory / prepared.entry["digest"], prepared.digest)
    return prepared.entry


def _export_group(data, selected, groups, key, start, end):
    """The coordinator reads one selected group with its shared payload memo."""
    expected = data._groups[key]
    try:
        with data._reader._open_group(
            expected["locator"], expected=expected["references"], wait=False, export_selection=True
        ) as group:
            with closing(group._iter_export_histories(groups[key], start=start, end=end)) as histories:
                for public_id, views in histories:
                    yield public_id, views
    except ReaderBusyError as error:
        raise ReaderBusyError("CSV export source is busy; retry later") from error
    except (CatalogueError, StandaloneReaderError) as error:
        raise CatalogChangedError(
            "selected serving history is no longer compatible; finish maintenance and open a new DataLibrary"
        ) from error


def _group_events(data, directory, selected, groups, start, end, workers, memory_bytes, progress=None):
    """One I/O owner alternates source consumption and destination batches.

    Workers only encode already-read histories. Ready output is byte-accounted;
    at most `workers` additional identity jobs may be in flight. A single output
    larger than the batch budget fails rather than spilling or rereading sources.
    No read-ahead thread or destination write can run behind the coordinator.
    """
    pending, batch = deque(), deque()
    buffered, prepared_count, done, batches = 0, 0, 0, 0
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="alpaca-csv")

    def report(phase):
        if progress:
            progress(
                "csv_export_buffer",
                {
                    "done": done,
                    "total": len(selected),
                    "prepared": prepared_count,
                    "buffered_bytes": buffered,
                    "memory_bytes": memory_bytes,
                    "batches": batches,
                    "io_phase": phase,
                    "workers": workers,
                },
            )

    def flush():
        nonlocal buffered, done, batches
        if not batch:
            return
        batches += 1
        report("write")
        while batch:
            kind, key, prepared = batch.popleft()
            try:
                entry = _write_identity(directory, prepared) if prepared is not None else None
            finally:
                if prepared is not None:
                    buffered -= prepared.size
                    prepared.close()
            if kind == "identity":
                done += 1
            report("write")
            yield kind, key, entry
        report("read")

    def collect():
        nonlocal buffered, prepared_count
        kind, key, future = pending[0]
        prepared = future.result() if future is not None else None
        # Retain ownership in pending until transferred to batch. An exception
        # while flushing must still close this completed future's output.
        if prepared is not None:
            if prepared.size > memory_bytes:
                raise MemoryError("one identity CSV/digest exceeds memory_bytes; increase the export budget")
            if buffered + prepared.size > memory_bytes:
                yield from flush()
            buffered += prepared.size
        batch.append((kind, key, prepared))
        pending.popleft()
        if kind == "identity":
            prepared_count += 1
            report("read")
        if buffered >= memory_bytes:
            yield from flush()

    try:
        report("read")
        for key in sorted(groups):
            with closing(_export_group(data, selected, groups, key, start, end)) as histories:
                for public_id, views in histories:
                    future = pool.submit(
                        _prepare_identity, data, selected[public_id], views, start, end, memory_bytes
                    )
                    pending.append(("identity", public_id, future))
                    del views, future
                    # Group completion markers do not consume worker slots.
                    while sum(item[2] is not None for item in pending) >= workers:
                        yield from collect()
            pending.append(("group", key, None))
        while pending:
            yield from collect()
        yield from flush()
    finally:
        for _, _, future in pending:
            if future is not None:
                future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
        for _, _, future in pending:
            if future is not None and not future.cancelled():
                try:
                    prepared = future.result()
                except BaseException:
                    continue
                if prepared is not None:
                    prepared.close()
        for _, _, prepared in batch:
            if prepared is not None:
                prepared.close()
        pending.clear()
        batch.clear()


def write_export(
    data, destination, *, public_ids=None, start=None, end=None, progress=None, workers=2, memory_bytes=None
):
    workers = _workers(workers)
    memory_bytes = _memory_budget(memory_bytes)
    try:
        # The catalogue transaction was released at DataLibrary construction.
        # Compare its token once after acquiring the shared operation lock and
        # retain that lock through final-manifest publication. This pins CLI
        # maintenance. Selected groups consume this accepted view without any
        # further selector reads or upstream freshness/authentication checks.
        with operation_lock(data.store, exclusive=False):
            try:
                current = catalogue_storage.selector_state(data.store)["generation"]
            except (CatalogueError, OSError, ValueError, KeyError, TypeError) as error:
                raise CatalogChangedError("selected serving catalogue is no longer available") from error
            if current != data.catalog_generation():
                raise CatalogChangedError(
                    "selected serving catalogue changed; open a new DataLibrary before exporting"
                )
            return _write_export(
                data,
                destination,
                public_ids=public_ids,
                start=start,
                end=end,
                progress=progress,
                workers=workers,
                memory_bytes=memory_bytes,
            )
    except StoreBusyError as error:
        raise ReaderBusyError("CSV export source store is busy; retry later") from error


def _write_export(data, destination, *, public_ids, start, end, progress, workers, memory_bytes):
    selected, groups, start, end = _selection(data, public_ids, start, end)
    plan = _plan(data, selected, groups, start, end, workers, memory_bytes)
    output = Path(destination).absolute()
    if output.resolve().is_relative_to(data.store.resolve()):
        raise ValueError("CSV export destination must be outside the source store")
    if progress:
        progress("csv_export_plan", plan)
    output.mkdir(parents=True, exist_ok=True)
    with _publication_lock(output):
        started_at = datetime.now(timezone.utc)
        directory = output / started_at.strftime("%Y%m%dT%H%M%S.%fZ")
        directory.mkdir()
        identities = {}
        counts = {"identities": 0, "groups": 0, "rows": 0, "csv_bytes": 0, "physical_columns": 0}
        done, began = 0, monotonic()
        io_state = {}
        write_started, write_done = began, 0

        def buffer_progress(stage, values):
            nonlocal write_started, write_done
            if values["io_phase"] == "write" and io_state.get("io_phase") != "write":
                write_started, write_done = monotonic(), values["done"]
            io_state.update(values)
            if progress:
                progress(stage, {**counts, **values})

        with closing(
            _group_events(data, directory, selected, groups, start, end, workers, memory_bytes, buffer_progress)
        ) as events:
            for kind, public_id, entry in events:
                if kind == "group":
                    counts["groups"] += 1
                    continue
                if entry is not None:
                    identities[public_id] = entry
                    counts["identities"] += 1
                    counts["rows"] += entry["rows"]
                    counts["csv_bytes"] += entry["csv_bytes"]
                    counts["physical_columns"] += len(entry["columns"]) - 1
                done += 1
                if progress:
                    elapsed = monotonic() - began
                    estimate = {}
                    if io_state.get("prepared") == len(selected) and done > write_done:
                        # Preparation and publication have different rates. Only
                        # estimate the final write phase from its own throughput.
                        estimate["estimated_remaining_seconds"] = round(
                            (monotonic() - write_started) * (len(selected) - done) / (done - write_done), 2
                        )
                    progress(
                        "csv_export",
                        {
                            **io_state,
                            "done": done,
                            "total": len(selected),
                            **counts,
                            "workers": workers,
                            "elapsed_seconds": round(elapsed, 2),
                            **estimate,
                        },
                    )
        identities = {
            public_id: identities[public_id]
            for key in sorted(groups)
            for public_id in groups[key]
            if public_id in identities
        }
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
        # Each run's manifest is its sole completion selector. A failed run has
        # no final manifest. No directory crawl, reread, fsync or payload audit
        # follows publication.
        temporary = directory / ".final_manifest.tmp"
        final = directory / "final_manifest.json"
        if progress:
            # Consumer callbacks finish before the commit. A logging failure
            # cannot report an unsuccessful export after replacing its selector.
            progress("csv_export_publish", {"done": done, "total": len(selected), **counts})
        try:
            _write_json(temporary, manifest)
            os.replace(temporary, final)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        return {
            "output": str(directory),
            "manifest": str(final),
            "catalogue_generation": data.catalog_generation(),
            "elapsed_seconds": round(monotonic() - began, 2),
            "workers": workers,
            "memory_bytes": memory_bytes,
            **counts,
        }

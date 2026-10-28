"""Per-identity CSV production from one accepted online catalogue snapshot."""

import csv
import fcntl
import hashlib
import json
import os
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from queue import Empty, Full, Queue
from threading import Event, Lock, Thread
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


def _plan(data, selected, groups, start, end, workers):
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
        "maximum_active_groups": min(workers, len(groups)),
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


def plan_export(data, *, public_ids=None, start=None, end=None, workers=2):
    workers = _workers(workers)
    return _plan(data, *_selection(data, public_ids, start, end), workers)


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
        "csv": f"{public_id}/{csv_name}",
        "digest": f"{public_id}/{digest_name}",
        "rows": len(raw),
        "start": raw[0]["session"],
        "end": raw[-1]["session"],
        "columns": columns,
        "csv_bytes": output.bytes,
    }


def _export_group(data, directory, selected, groups, key, start, end, stopped):
    """Keep one group's locks, decoded payloads and identity outputs together."""
    if stopped.is_set():
        return
    expected = data._groups[key]
    try:
        # Keep selected group locks and decoded payloads through its identity
        # outputs. The worker returns compact completion records; the coordinator
        # must not reread CSVs or reopen source groups to certify those records.
        with data._reader._open_group(
            expected["locator"],
            expected=expected["references"],
            wait=False,
            before_payloads=lambda: catalogue_storage.assert_group(
                data.store, key, expected["decision_generation"]
            ),
        ) as group:
            with closing(group._iter_export_histories(groups[key], start=start, end=end)) as histories:
                while not stopped.is_set():
                    try:
                        public_id, views = next(histories)
                    except StopIteration:
                        break
                    if stopped.is_set():
                        return
                    entry = _write_identity(data, directory, selected[public_id], views, start, end)
                    del views
                    yield "identity", public_id, entry
    except ReaderBusyError as error:
        raise ReaderBusyError("CSV export source is busy; retry later") from error
    except (CatalogueError, StandaloneReaderError) as error:
        raise CatalogChangedError(
            "selected serving history is no longer compatible; finish maintenance and open a new DataLibrary"
        ) from error
    if not stopped.is_set():
        yield "group", key, None


def _group_events(data, directory, selected, groups, start, end, workers):
    """Bound outstanding groups; only compact completion records cross threads."""
    stopped = Event()
    keys = iter(sorted(groups))
    count = min(workers, len(groups))
    if count < 2:
        for key in keys:
            yield from _export_group(data, directory, selected, groups, key, start, end, stopped)
        return

    jobs, completed = Queue(maxsize=count), Queue(maxsize=2 * count)
    state_lock, failures = Lock(), []

    def send(record):
        while not stopped.is_set():
            try:
                completed.put(record, timeout=0.05)
                return
            except Full:
                continue

    def work():
        try:
            while not stopped.is_set():
                try:
                    key = jobs.get(timeout=0.05)
                except Empty:
                    continue
                with closing(
                    _export_group(data, directory, selected, groups, key, start, end, stopped)
                ) as events:
                    for record in events:
                        send(record)
                        if stopped.is_set():
                            return
        except BaseException as error:
            # Latch the first failure before another group can be dispatched.
            with state_lock:
                if not failures:
                    failures.append(error)
                stopped.set()

    def dispatch():
        with state_lock:
            if failures:
                raise failures[0]
            key = next(keys, None)
            if key is None:
                return False
            jobs.put_nowait(key)
            return True

    threads = [Thread(target=work, name=f"alpaca-csv-{index + 1}") for index in range(count)]
    try:
        for thread in threads:
            thread.start()
        pending = sum(dispatch() for _ in range(count))
        while pending:
            with state_lock:
                if failures:
                    raise failures[0]
            try:
                record = completed.get(timeout=0.05)
            except Empty:
                continue
            yield record
            if record[0] == "group":
                pending -= 1
                pending += dispatch()
    finally:
        stopped.set()
        # Drain while joining: a worker may still be delivering its last result.
        # No output lock or final publication can outlive an active worker.
        while any(thread.is_alive() for thread in threads):
            try:
                completed.get(timeout=0.05)
            except Empty:
                pass
        for thread in threads:
            if thread.ident is not None:
                thread.join()


def write_export(data, destination, *, public_ids=None, start=None, end=None, progress=None, workers=2):
    workers = _workers(workers)
    try:
        # The catalogue transaction was released at DataLibrary construction.
        # Compare its token once after acquiring the shared operation lock and
        # retain that lock through final-manifest publication. This pins CLI
        # maintenance; selected group reads retain their own generation checks.
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
            )
    except StoreBusyError as error:
        raise ReaderBusyError("CSV export source store is busy; retry later") from error


def _write_export(data, destination, *, public_ids, start, end, progress, workers):
    selected, groups, start, end = _selection(data, public_ids, start, end)
    plan = _plan(data, selected, groups, start, end, workers)
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
        with closing(_group_events(data, directory, selected, groups, start, end, workers)) as events:
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
                    progress(
                        "csv_export",
                        {
                            "done": done,
                            "total": len(selected),
                            **counts,
                            "workers": workers,
                            "elapsed_seconds": round(elapsed, 2),
                            "estimated_remaining_seconds": round(elapsed * (len(selected) - done) / done, 2),
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
            **counts,
        }

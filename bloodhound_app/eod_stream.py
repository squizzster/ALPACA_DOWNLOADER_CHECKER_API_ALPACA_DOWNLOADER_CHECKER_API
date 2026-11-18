"""Serialize the accepted online-catalogue Bloodhound projection as daily events."""

import gzip
import io
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager, suppress
from datetime import datetime, timezone
from itertools import groupby
from operator import itemgetter
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc

from .alpaca_to_bloodhound import (
    ConversionError,
    DatedMetadata,
    History,
    history_split_factors,
    load_history,
    make_table,
    require_source_consistency,
    strict_date,
    unique_histories,
)
from .catalogue_input import OutputTables, open_histories

BLOOD_ID = "stock_engine_test_v1.0"
SOURCE = "alpaca"
GATHER_ROWS = 65536
DECODE_ROWS = 8192
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parents[1] / "exports" / "eod_stream"


def _observations(table, batch_size):
    # Convert a bounded group of Arrow scalars at once. Keep the signed int64
    # series IDs and finite float64 values at their original precision.
    names = table.column_names
    for batch in table.to_batches(max_chunksize=batch_size):
        columns = []
        for name, column in zip(names, batch.columns):
            if name == "date":
                values = column.to_numpy().astype("datetime64[D]").tolist()
            elif not column.null_count and (
                pa.types.is_floating(column.type) or pa.types.is_integer(column.type)
            ):
                values = column.to_numpy().tolist()
            else:
                values = column.to_pylist()
            columns.append(values)
        for values in zip(*columns):
            yield dict(zip(names, values))


def _ordered_observations(tables, progress):
    if not tables:
        return
    # Transfer ownership, then pack one column at a time. This releases its
    # original chunks before the next column needs a contiguous buffer.
    joined = pa.concat_tables([table for _, table in tables])
    tables.clear()
    schema, columns = joined.schema, joined.columns
    del joined
    for index, column in enumerate(columns):
        # Table.combine_chunks respects Arrow's per-buffer limit for strings.
        columns[index] = pa.table([column], names=["value"]).combine_chunks().column(0)
    packed = pa.Table.from_arrays(columns, schema=schema)
    order = pc.sort_indices(packed, sort_keys=[("date", "ascending"), ("ohlcv_series_id", "ascending")])
    for offset in range(0, len(order), GATHER_ROWS):
        block = pc.take(packed, order.slice(offset, GATHER_ROWS))
        yield from _observations(block, DECODE_ROWS)
        completed = offset + len(block)
        if progress and (completed % 1048576 == 0 or completed == len(order)):
            progress(f"Encoded {completed:,}/{len(order):,} observations.")


def _write_events(stream, tables, created, progress):
    encode = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode
    rows = _ordered_observations(tables, progress)
    first_day = last_day = None
    days = 0
    for days, (session, observations) in enumerate(groupby(rows, key=itemgetter("date")), 1):
        header = {"blood_id": BLOOD_ID, "run_id": uuid4().hex}
        meta = {"source": SOURCE, "date_created": created} if days == 1 else {}
        stream.write(encode({**header, "event_type": "START_EOD_MARKER", **meta}) + "\n")
        day = last_day = session.isoformat()
        if days == 1:
            first_day = day
        prefix = {**header, "event_type": "EOD"}
        for row in observations:
            row["date"] = day
            stream.write(encode({**prefix, **row}) + "\n")
        stream.write(encode({**header, "event_type": "FINISH_EOD_MARKER"}) + "\n")
    return days, first_day, last_day


@contextmanager
def _gzip_stream(raw):
    executable = shutil.which("pigz")
    if executable is None:
        with (
            gzip.GzipFile(filename="", mode="wb", compresslevel=6, fileobj=raw) as compressed,
            io.TextIOWrapper(compressed, encoding="utf-8", newline="\n") as stream,
        ):
            yield stream
        return

    env = {key: value for key, value in os.environ.items() if key not in ("PIGZ", "GZIP")}
    with subprocess.Popen(
        [executable, "-n", "-6", "-p", "4", "-c"],
        stdin=subprocess.PIPE,
        stdout=raw,
        env=env,
        text=True,
        encoding="utf-8",
    ) as process:
        try:
            yield process.stdin
            process.stdin.close()
            status = process.wait()
            if status:
                raise ConversionError(f"pigz compression failed with exit status {status}")
        except BaseException:
            process.kill()
            with suppress(BrokenPipeError):
                process.stdin.close()
            raise


def _publish(output, tables, created, *, replace, progress=None, new_run=False):
    output.parent.mkdir(parents=True, exist_ok=not new_run)
    descriptor, name = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".partial", dir=output.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as raw:
            with _gzip_stream(raw) as stream:
                days, first_day, last_day = _write_events(stream, tables, created, progress)
            output_bytes = raw.tell()
        if replace:
            os.replace(temporary, output)
        else:
            os.link(temporary, output)
        return days, output_bytes, first_day, last_day
    finally:
        temporary.unlink(missing_ok=True)


def export(
    store,
    output=None,
    *,
    start=None,
    end=None,
    public_ids=None,
    symbols=None,
    cache_mib=2048,
    memory_mib=8192,
    replace=False,
    progress=None,
):
    """Read the selected catalogue histories once and publish a gzip JSON Lines file."""
    began = perf_counter()
    started = datetime.now(timezone.utc)
    created = started.isoformat().replace("+00:00", "Z")
    start = strict_date(start) if start is not None else None
    end = strict_date(end) if end is not None else None
    if start and end and start > end:
        raise ConversionError("--from must not be after --through")
    store = Path(store).expanduser().resolve()
    new_run = output is None
    if new_run:
        output = DEFAULT_OUTPUT_ROOT / started.strftime("%Y%m%dT%H%M%S.%fZ") / "eod_stream.jsonl.gz"
        replace = False
    output = Path(output).expanduser().absolute()
    if not output.name.endswith(".jsonl.gz"):
        raise ConversionError("--output must end in .jsonl.gz")
    if output.resolve().is_relative_to(store):
        raise ConversionError("EOD stream output must be outside the source store")
    if new_run:
        if output.parent.exists():
            raise ConversionError(f"EOD export run directory already exists: {output.parent}")
    elif output.is_symlink() or (output.exists() and (not replace or not output.is_file())):
        raise ConversionError("output exists or is unsafe; use --replace for an existing export file")

    args = SimpleNamespace(
        store=store,
        public_id=public_ids,
        symbol=[symbol.strip().upper() for symbol in symbols] if symbols else None,
        cache_bytes=cache_mib * 1024**2,
        start_date=start,
        end_date=end,
        price_basis="all",
        unknown_security_type="unknown",
        unknown_exchange="unknown",
    )
    metadata = DatedMetadata(None)
    collected = OutputTables(memory_mib * 1024**2)
    row_count = 0
    source_seconds = projection_seconds = 0.0
    with open_histories(args, History, unique_histories, select_window=True) as histories:
        selected = len(histories)
        scope = histories.exported
        groups = len(scope.groups)
        input_rows = sum(security.stored_session_count for security in scope.securities.values())
        if progress:
            progress(
                f"Selected {selected:,} online identities across {groups:,} groups and {input_rows:,} retained sessions."
            )
        setup_seconds = perf_counter() - began
        iterator = iter(histories)
        position = 0
        while True:
            source_start = perf_counter()
            history = next(iterator, None)
            projection_start = perf_counter()
            source_seconds += projection_start - source_start
            if history is None:
                break
            position += 1
            try:
                frames, _, issues = load_history(history, metadata, args=args)
                require_source_consistency(issues)
                factors, _ = history_split_factors(history, frames)
                table = make_table(history, frames, factors, metadata, args)
            except (ValueError, TypeError, KeyError, pa.ArrowException) as error:
                raise ConversionError(f"{history.public_id}: {error}") from error
            if len(table):
                collected.add(history.identifier, table)
                row_count += len(table)
            del frames, factors, table, history
            projection_seconds += perf_counter() - projection_start
            if progress and (position % 25 == 0 or position == len(histories)):
                progress(f"Projected {position}/{len(histories)} identities and {row_count:,} observations.")
        if progress:
            progress(f"Writing {row_count:,} observations to gzip JSON Lines.")
        write_start = perf_counter()
        identities = len(collected.tables)
        days, output_bytes, first_day, last_day = _publish(
            output, collected.tables, created, replace=replace, progress=progress, new_run=new_run
        )
        return {
            "output": str(output),
            "days": days,
            "output_bytes": output_bytes,
            "rows": row_count,
            "identities": identities,
            "first_session": first_day,
            "last_session": last_day,
            "catalogue_generation": histories.catalogue["generation"],
            "selected_identities": selected,
            "groups": groups,
            "retained_sessions": input_rows,
            "retained_table_bytes": collected.used_bytes,
            "peak_rss_mib": round(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                / (1024**2 if sys.platform == "darwin" else 1024),
                1,
            ),
            "seconds": {
                "setup": round(setup_seconds, 3),
                "read_and_assemble": round(source_seconds, 3),
                "project": round(projection_seconds, 3),
                "write_and_publish": round(perf_counter() - write_start, 3),
                "total": round(perf_counter() - began, 3),
            },
        }

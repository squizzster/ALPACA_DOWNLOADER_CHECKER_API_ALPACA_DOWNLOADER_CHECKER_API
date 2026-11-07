"""Bounded legacy tail assembly from producer-selected pricing partitions."""

import csv
import gzip
import io
from pathlib import PurePosixPath

from .contract import COLUMNS, DownloadError
from .endpoints import TAIL_SIZE, TAIL_VERSION, Endpoints, validate_tail
from .storage import boundary_document, read_artifact, regular_path


def readall_rsplit(packed, count, *, before_month=None):
    """Decode once and parse only the final selected data rows, newest first.

    This is the measured read-all/rsplit strategy. The numeric pricing CSV has
    one physical line per row. Current-year overlap is discarded before deciding
    whether enough older rows were obtained; the same gzip is never reopened.
    """
    with gzip.GzipFile(fileobj=io.BytesIO(packed), mode="rb") as stream:
        remaining = stream.read()
    rows = []
    while remaining and len(rows) < count:
        pieces = remaining.rsplit(b"\n", count - len(rows) + 1)
        if len(pieces) == count - len(rows) + 2:
            remaining, lines = pieces[0], pieces[1:]
        else:
            remaining, lines = b"", pieces
        for line in reversed(lines):
            if not line:
                continue
            row = tuple(next(csv.reader([line.decode("utf-8")])))
            if row == COLUMNS:
                continue
            if before_month is not None and row[0][:7] >= before_month:
                continue
            rows.append(row)
            if len(rows) == count:
                break
    return tuple(reversed(rows))


def assemble_legacy_tail(root, source, endpoints, *, documents=None):
    """Caller selected a legacy source under its lock; assemble its missing fact.

    Existing endpoints and payload membership remain authoritative. This reads
    only all-view payloads needed for ten effective rows. No other adjustments,
    upstream checksums, directory discovery or provider requests participate.
    The owner must publish the resulting handoff through its ordinary journal.
    """
    if getattr(endpoints, "tail_all", None) is not None:
        return endpoints
    if source["schema_version"] >= TAIL_VERSION:
        raise DownloadError("published pricing tail is missing; finish owning recovery")
    points = endpoints["all"]
    tail = ()
    documents = {} if documents is None else documents
    if points:
        for kind in ("current_month", "current_year", "completed_years"):
            reference = source["boundaries"][kind]
            relative = reference["manifest"]
            if relative not in documents:
                documents[relative] = boundary_document(root, reference, source["symbol"], kind)
            document = documents[relative]
            entry = next(item for item in document["adjustments"] if item["adjustment"] == "all")
            if entry["storage"] == "empty":
                continue
            path = regular_path(root, str(PurePosixPath(relative).parent / entry["file"]), published=True)
            packed = read_artifact(path, purpose="legacy_tail_assembly", selector_sha256=reference["sha256"])
            rows = readall_rsplit(
                packed,
                TAIL_SIZE - len(tail),
                before_month=source["anchor_month"] if kind == "current_year" else None,
            )
            tail = rows + tail
            if len(tail) == TAIL_SIZE:
                break
    return Endpoints(
        endpoints, tail_all=validate_tail(tail, points[0] if points else None, points[-1] if points else None)
    )


def publish_legacy_tail(root, source, endpoints, *, documents=None, source_sha256=None):
    """Complete a selected legacy transition under the caller's pricing lock.

    Publish before any provider work, so an unrelated acquisition failure cannot
    discard a completed extraction. Metadata-only ancestry preserves every
    accepted observation and mapping; callers carry the returned generation.
    """
    from libs.common.debug_trace import event

    from .contract import HISTORY_START, timestamp
    from .download import publish_acquired

    documents = {} if documents is None else documents
    event("legacy_tail_selection", symbol=source["symbol"], maximum_payload_reads=3, adjustment="all")
    endpoints = assemble_legacy_tail(root, source, endpoints, documents=documents)
    publication = publish_acquired(
        root,
        source,
        source["symbol"],
        source["anchor_month"],
        source["latest_request"],
        timestamp(source.get("history_start", HISTORY_START)),
        endpoints,
        {},
        None,
        old_documents=documents,
        previous_source_sha256=source_sha256,
    )
    return publication, endpoints


def upgrade_legacy_entries(store, entries, *, progress=None):
    """Publish only explicitly selected legacy baselines, without changing prices.

    Planning can identify these rows from the compact summary alone. The selected
    owner opens its source and validator once, then declares bounded assembly
    reads. Its existing journal installs the tail and refreshes the summary.
    """
    from pathlib import Path

    from .check_summary import price_entry, replace_entries
    from .contract import digest, json_bytes
    from .storage import load_source, read_validator, recover, ticker_lock

    completed = {}
    for entry in entries:
        if entry.get("tail_all") is not None:
            continue
        if entry.get("tail_version", 31) >= TAIL_VERSION:
            raise DownloadError("new comparison summary lacks its required tail; finish owning recovery")
        symbol = entry["symbol"]
        root = regular_path(Path(store).absolute(), f"end_of_day/pricing/{symbol}", published=True)
        if progress:
            progress("legacy_tail_upgrade", {"symbol": symbol, "maximum_payload_reads": 3})
        with ticker_lock(root, exclusive=True):
            recover(root)
            source = load_source(root, symbol)
            if source is None or any(
                source["latest_request"][key] != entry[key] for key in ("asof", "end_exclusive")
            ):
                raise DownloadError("legacy tail selection changed before upgrade")
            endpoints = read_validator(root, source, published=True)
            if endpoints is None:
                raise DownloadError("legacy tail upgrade requires its published endpoint validator")
            if endpoints.tail_all is not None:
                generation = digest(json_bytes(source))
                replace_entries(
                    store,
                    {symbol: price_entry(root, source, endpoints=endpoints, source_sha256=generation)},
                    pricing_only=True,
                )
                completed[symbol] = {
                    "source": source,
                    "source_sha256": generation,
                    "endpoints": endpoints,
                    "changed": False,
                    "files_written": 0,
                }
                continue
            completed[symbol], _ = publish_legacy_tail(root, source, endpoints)
    return completed

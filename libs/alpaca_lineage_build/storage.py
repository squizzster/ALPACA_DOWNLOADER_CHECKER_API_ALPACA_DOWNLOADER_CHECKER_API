"""One worker, sorted pricing locks, roll-forward JSON publication, then acknowledgement."""

import fcntl
import json
import os
import sqlite3
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path, PurePosixPath

from libs.alpaca_pricing_download.contract import json_bytes, symbol_label
from libs.alpaca_pricing_download.storage import (
    STORE_DOCUMENT,
    acknowledge_update,
    fsync_directory,
    load_source,
    read_document,
    read_update,
    regular_path,
    ticker_lock,
    write_file,
)
from libs.alpaca_pricing_download.update_checks import acknowledge_checks, read_checks

from .acquisition import unavailable_pricing
from .contract import (
    MAX_CLOSURE,
    SUPPORTED_VERSIONS,
    VERSION,
    LineageBusyError,
    LineageError,
    document_digest,
    selected_document,
)


def path_at(root: Path, relative: str) -> Path:
    parts = PurePosixPath(relative).parts
    if (
        not parts
        or len(parts) > 4
        or relative != "/".join(parts)
        or any(part in {".", ".."} for part in parts)
        or PurePosixPath(relative).is_absolute()
    ):
        raise LineageError(f"unsafe lineage path: {relative!r}")
    path = root
    for part in parts:
        path = path / part
        if path.is_symlink():
            raise LineageError(f"lineage cannot traverse a symlink: {path}")
    return path


def atomic_json(path: Path, value: dict) -> None:
    data = json_bytes(value)
    if path.exists() and path.read_bytes() == data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path_at(path.parent, "." + path.name + ".writing")
    if temporary.exists():
        temporary.unlink()  # This writer's reserved, unpublished scratch file.
    write_file(temporary, data)
    os.replace(temporary, path)
    fsync_directory(path.parent)
    fsync_directory(path.parent.parent)


def check_store(store: Path) -> None:
    for path in (store, *store.parents):
        if path.is_symlink():
            raise LineageError(f"store cannot traverse a symlink: {path}")
    if read_document(store / "store.manifest.json") != STORE_DOCUMENT:
        raise LineageError("lineage requires a standalone pricing store; no migration is performed")


@contextmanager
def publication_lock(root: Path, *, exclusive=False, wait=True):
    """Protect only receipt/index installation or a short selection handshake."""
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        try:
            fcntl.flock(
                descriptor, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | (0 if wait else fcntl.LOCK_NB)
            )
        except BlockingIOError as error:
            raise LineageBusyError("publication is busy; retry later or use --wait") from error
        yield
    finally:
        os.close(descriptor)


def _index_version(root: Path) -> int | None:
    path = path_at(root, "index.sqlite3")
    if not path.exists():
        return None
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
            return connection.execute("PRAGMA user_version").fetchone()[0]
    except sqlite3.Error as error:
        raise LineageError("cannot load lineage dependency index") from error


@contextmanager
def writer_lock(store: Path):
    check_store(store)
    root = regular_path(store, "end_of_day/lineage")
    root.mkdir(parents=True, exist_ok=True)
    fsync_directory(root.parent)
    # One permanent inode per store, not per ticker. Readers never take it.
    with path_at(root, ".worker.lock").open("a+b") as worker:
        fcntl.flock(worker.fileno(), fcntl.LOCK_EX)
        with publication_lock(root, exclusive=True):
            # Drain v1/v2 writers/readers before activating the new lock order.
            # Older executables then reject the current protocol before taking pricing locks.
            # Only the protocol header changes: no receipt or prices are rebuilt.
            existing = _index_version(root)
            if existing is not None and existing not in SUPPORTED_VERSIONS:
                raise LineageError("unsupported lineage index")
            if existing != VERSION:
                _update_index(root, {})
        yield root


def select_inputs(root: Path, symbol: str, *, selected_loader=None) -> dict:
    """Select the published input group without scanning the universe."""
    index = read_index(root, symbol)
    receipt = read_receipt(root, symbol, index, selected_loader=selected_loader)
    if receipt is None:
        raise LineageError(f"no published lineage for {symbol}")
    inputs = receipt["input_sources"]
    absent = unavailable_pricing(receipt)
    group = set(inputs) | absent.keys()
    if symbol not in inputs or set(inputs).intersection(absent) or not 1 <= len(group) <= MAX_CLOSURE:
        raise LineageError("invalid lineage input group")
    selected = {
        name: (index if name == symbol else read_index(root, name))["symbols"].get(name)
        for name in sorted(group)
    }
    if any(selected[name] is not None for name in absent):
        raise LineageError("previously unavailable lineage input changed; run lineage first")
    return selected


@contextmanager
def reader_snapshot(
    store: Path, select, *, wait=True, selected_loader=None, include_boundaries=True, include_receipts=True
):
    """Snapshot selection, release gate, lock prices, reselect; never invert locks.

    select(root) returns symbol -> index entry (or None for offline discovery).
    Selected metadata is loaded under the brief gate, then pricing SH locks alone
    protect it throughout consumption. Unrelated groups can publish during reads.
    """
    check_store(store)
    root = regular_path(store, "end_of_day/lineage")

    def selection():
        # New lock order must not overlap an executable using the old order.
        # The worker upgrades this header under the old shared/exclusive gate.
        if _index_version(root) not in {None, VERSION}:
            raise LineageError("lineage lock protocol needs upgrade; run lineage first")
        if path_at(root, ".pending.json").exists():
            raise LineageError("lineage publication incomplete; resume lineage first")
        return select(root)

    for _ in range(3):
        with publication_lock(root, wait=wait):
            selected = selection()
        with ExitStack() as locks:
            locked = set()
            for symbol in sorted(selected):
                symbol_label(symbol)
                pricing = regular_path(store, f"end_of_day/pricing/{symbol}")
                if not pricing.exists():
                    continue
                stream = locks.enter_context(regular_path(pricing, ".download.lock").open("rb"))
                try:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_SH | (0 if wait else fcntl.LOCK_NB))
                except BlockingIOError as error:
                    raise LineageBusyError("pricing group is busy; retry later or use --wait") from error
                locked.add(symbol)
            with publication_lock(root, wait=wait):
                if selection() != selected:
                    continue  # Release every price lock before retrying a changed closure.
                index = {"symbols": {name: entry for name, entry in selected.items() if entry is not None}}
                receipts = (
                    {name: read_receipt(root, name, index, selected_loader=selected_loader) for name in selected}
                    if include_receipts
                    else {}
                )
                boundaries = (
                    {
                        name: read_boundaries(root, receipt, selected_loader=selected_loader)
                        for name, receipt in receipts.items()
                        if receipt
                    }
                    if include_boundaries
                    else {}
                )
            yield {
                "receipts": receipts,
                "boundaries": boundaries,
                "entries": selected,
                "locked": locked,
            }
            return
    raise LineageBusyError("lineage group changed during selection; retry later")


@contextmanager
def pricing_locks(store: Path, symbols):
    # Never nest consume_pricing_update: acquire the complete known set in one order.
    with ExitStack() as stack:
        locked = set()
        for symbol in sorted(set(symbols)):
            symbol_label(symbol)
            root = regular_path(store, f"end_of_day/pricing/{symbol}")
            if root.exists():
                stack.enter_context(ticker_lock(root, exclusive=True))
                locked.add(symbol)
        yield locked


def empty_index() -> dict:
    return {"schema_version": VERSION, "boundary": "alpaca_standalone_lineage_index", "symbols": {}}


def read_index(root: Path, symbol: str | None = None) -> dict:
    path = path_at(root, "index.sqlite3")
    if not path.exists():
        return empty_index()
    index = empty_index()
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
            if connection.execute("PRAGMA user_version").fetchone()[0] not in SUPPORTED_VERSIONS:
                raise LineageError("unsupported lineage index")
            statement = "SELECT symbol, receipt FROM symbols"
            parameters = () if symbol is None else (symbol,)
            statement += " ORDER BY symbol" if symbol is None else " WHERE symbol = ?"
            index["symbols"] = {
                label: json.loads(value) for label, value in connection.execute(statement, parameters)
            }
    except (sqlite3.Error, ValueError) as error:
        raise LineageError("cannot load lineage dependency index") from error
    for symbol, entry in index["symbols"].items():
        if symbol_label(symbol) != symbol or not isinstance(entry, dict):
            raise LineageError("invalid lineage index entry")
        if not isinstance(entry.get("dependencies"), list) or not isinstance(entry.get("asset_ids"), list):
            raise LineageError("lineage dependency index is incomplete")
    return index


def _update_index(root: Path, entries: dict) -> None:
    # One tiny indexed row per ticker, not a full-universe JSON rewrite on each
    # update. The chronological JSON boundaries remain the authoritative outputs.
    path = path_at(root, "index.sqlite3")
    path_at(root, "index.sqlite3-journal")
    try:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA journal_mode=DELETE")
            with connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS symbols (symbol TEXT PRIMARY KEY, receipt TEXT NOT NULL)"
                )
                connection.execute(f"PRAGMA user_version={VERSION}")
                connection.executemany(
                    "INSERT OR REPLACE INTO symbols(symbol, receipt) VALUES (?, ?)",
                    [(symbol, json_bytes(entry).decode()) for symbol, entry in sorted(entries.items())],
                )
        fsync_directory(root)
    except sqlite3.Error as error:
        raise LineageError("cannot publish lineage dependency index") from error


def read_receipt(root: Path, symbol: str, index: dict, *, selected_loader=None) -> dict | None:
    entry = index["symbols"].get(symbol)
    if entry is None:
        return None
    value = (selected_loader or selected_document)(
        path_at(root, f"{symbol}/lineage.manifest.json"), entry["sha256"]
    )
    if value.get("symbol") != symbol or value.get("schema_version") not in SUPPORTED_VERSIONS:
        raise LineageError("invalid lineage receipt")
    return value


def read_boundaries(root: Path, receipt: dict, *, selected_loader=None) -> dict:
    return {
        kind: (selected_loader or selected_document)(
            path_at(root, f"{receipt['symbol']}/{entry['manifest']}"), entry["sha256"]
        )
        for kind, entry in receipt["boundaries"].items()
    }


def _install(root: Path, journal: dict) -> None:
    # The journal and brief publication gate make multi-symbol visibility atomic to
    # cooperating readers. Index is the selector and is always installed last.
    for relative, value in sorted(journal["documents"].items()):
        atomic_json(path_at(root, relative), value)
    for relative in journal["retire"]:
        path = path_at(root, relative)
        if path.exists():
            path.unlink()
            fsync_directory(path.parent)
        parent = path.parent
        while parent != root and parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
            fsync_directory(parent.parent)
            parent = parent.parent
    _update_index(root, journal["index_updates"])


def _acknowledge(store: Path, journal: dict) -> None:
    expected_checks = journal.get("checks")
    current_checks = read_checks(store, journal["sources"]) if expected_checks else None
    acknowledged = []
    for symbol in sorted(journal["sources"]):
        root = regular_path(store, f"end_of_day/pricing/{symbol}")
        if regular_path(root, ".pending.json").exists():
            continue  # Downloader recovery still owns that input.
        source = load_source(root, symbol)
        if current_checks and (
            current_checks["epoch"] != expected_checks["epoch"]
            or (
                symbol in current_checks["checks"]
                and current_checks["checks"][symbol] != expected_checks["checks"].get(symbol)
            )
        ):
            continue  # Lost coverage or a newer hint cannot acknowledge an older snapshot.
        # A crash releases pricing locks. Never consume a newer notice merely
        # because it has exactly the same from/to dates as the older one.
        if source is not None and document_digest(source) == journal["sources"][symbol]:
            expected = journal["notices"].get(symbol)
            if expected is not None and read_update(root) == expected:
                acknowledge_update(root, expected)
            if read_update(root) is None:
                acknowledged.append(symbol)
    if expected_checks:
        acknowledge_checks(store, expected_checks, acknowledged)


def finish_publication(store: Path, root: Path, journal: dict) -> None:
    _install(root, journal)
    _acknowledge(store, journal)
    path_at(root, ".pending.json").unlink()
    fsync_directory(root)


def recover(store: Path, root: Path) -> bool:
    pending = path_at(root, ".pending.json")
    if not pending.exists():
        return False
    journal = read_document(pending)
    if (
        journal.get("schema_version") not in SUPPORTED_VERSIONS
        or journal.get("boundary") != "alpaca_lineage_transaction"
    ):
        raise LineageError("unsupported lineage recovery transaction")
    # Same order as publication/readers: price locks BEFORE the publication gate.
    group = set(journal["sources"]) | set(journal.get("unavailable_pricing", ()))
    with pricing_locks(store, group), publication_lock(root, exclusive=True):
        finish_publication(store, root, journal)
    return True


def publish(
    store: Path, root: Path, index: dict, receipts: dict, boundaries: dict, notices: dict, *, checks=None
) -> None:
    documents, retire = {}, []
    updated = {}
    for symbol, receipt in receipts.items():
        old = read_receipt(root, symbol, index)
        old_paths = {entry["manifest"] for entry in old["boundaries"].values()} if old else set()
        new_paths = {entry["manifest"] for entry in receipt["boundaries"].values()}
        retire.extend(f"{symbol}/{path}" for path in sorted(old_paths - new_paths))
        for kind, value in boundaries[symbol].items():
            if old is None or old["boundaries"][kind] != receipt["boundaries"][kind]:
                documents[f"{symbol}/{receipt['boundaries'][kind]['manifest']}"] = value
        documents[f"{symbol}/lineage.manifest.json"] = receipt
        updated[symbol] = {
            "sha256": document_digest(receipt),
            "source_sha256": receipt["source_sha256"],
            "dependencies": receipt["dependencies"],
            "asset_ids": receipt["asset_ids"],
            "versions": receipt["versions"],
        }
    # Stage only changed JSON. Never stage or copy a pricing payload.
    documents = {
        relative: value
        for relative, value in documents.items()
        if not path_at(root, relative).exists() or path_at(root, relative).read_bytes() != json_bytes(value)
    }
    journal = {
        "schema_version": VERSION,
        "boundary": "alpaca_lineage_transaction",
        "documents": documents,
        "retire": retire,
        "index_updates": updated,
        "sources": {symbol: receipt["source_sha256"] for symbol, receipt in receipts.items()},
        "unavailable_pricing": sorted(
            {name for receipt in receipts.values() for name in unavailable_pricing(receipt)}
        ),
        "notices": notices,
    }
    if checks is not None:
        journal["checks"] = {
            "epoch": checks["epoch"],
            "checks": {symbol: revision for symbol, revision in checks["checks"].items() if symbol in receipts},
        }
    # Caller already owns the complete pricing group and the worker-only lock.
    with publication_lock(root, exclusive=True):
        atomic_json(path_at(root, ".pending.json"), journal)
        finish_publication(store, root, journal)
    index["symbols"].update(updated)

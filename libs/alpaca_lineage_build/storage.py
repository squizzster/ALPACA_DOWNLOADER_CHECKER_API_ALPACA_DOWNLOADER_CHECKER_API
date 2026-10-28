"""One worker, sorted pricing locks, roll-forward JSON publication, then acknowledgement."""

import fcntl
import json
import os
import sqlite3
from contextlib import ExitStack, closing, contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import get_ident

from libs.alpaca_pricing_download.contract import json_bytes, symbol_label
from libs.alpaca_pricing_download.storage import (
    STORE_DOCUMENT,
    acknowledge_update,
    load_source,
    read_document,
    read_update,
    regular_path,
    ticker_lock,
    write_file,
)
from libs.alpaca_pricing_download.update_checks import acknowledge_checks, read_checks
from libs.common.sqlite_connections import connect_sqlite

from .acquisition import unavailable_pricing
from .contract import (
    MAX_CLOSURE,
    SUPPORTED_VERSIONS,
    VERSION,
    LineageBusyError,
    LineageError,
    document_digest,
    reusable_decision,
    selected_document,
)
from .correlation_policy_contract import POLICY_PROVENANCE_LINEAGE_VERSION


def path_at(root: Path, relative: str, *, published=False) -> Path:
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
        if not published and path.is_symlink():
            raise LineageError(f"lineage cannot traverse a symlink: {path}")
    return path


def atomic_json(path: Path, value: dict, *, compare=True) -> None:
    data = json_bytes(value)
    if compare and path.exists() and path.read_bytes() == data:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path_at(path.parent, "." + path.name + ".writing")
    if temporary.exists():
        temporary.unlink()  # This writer's reserved, unpublished scratch file.
    write_file(temporary, data)
    os.replace(temporary, path)


def check_store(store: Path) -> None:
    for path in (store, *store.parents):
        if path.is_symlink():
            raise LineageError(f"store cannot traverse a symlink: {path}")
    if read_document(store / "store.manifest.json") != STORE_DOCUMENT:
        raise LineageError("lineage requires a standalone pricing store; no migration is performed")


@dataclass(frozen=True)
class AcceptedStore:
    store: Path
    root: Path


def accept_store(store):
    """Accept immutable store compatibility once for the owning operation."""
    store = Path(store).absolute()
    check_store(store)
    return AcceptedStore(store, regular_path(store, "end_of_day/lineage"))


class WriterIndexView:
    """A selected index whose writer lock remains held by this thread."""

    def __init__(self, accepted, context):
        self.store, self.root = accepted.store, accepted.root
        self._context, self._index = context, None
        self._process, self._thread, self._active = os.getpid(), get_ident(), True

    def require_active(self, store):
        if not self._active or self._process != os.getpid() or self._thread != get_ident():
            raise LineageError("accepted lineage view requires its active writer lock")
        if Path(store).absolute() != self.store:
            raise LineageError("accepted lineage view belongs to another store")

    def index(self):
        self.require_active(self.store)
        if self._index is None:
            with publication_lock(self.root):
                if (self.root / ".pending.json").exists():
                    raise LineageError("lineage publication incomplete; resume lineage first")
                self._index = (
                    read_index(self.root) if self._context is None else self._context.select_index(self.root)
                )
        return self._index

    def select(self, store, symbols):
        self.require_active(store)
        if self._index is None:
            raise LineageError("lineage index has not been accepted")
        return {name: self._index["symbols"].get(name) for name in symbols}


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


def _index_version(root: Path, *, readonly=True) -> int | None:
    path = path_at(root, "index.sqlite3")
    if not path.exists():
        return None
    try:
        with closing(connect_sqlite(path, readonly=readonly)) as connection:
            return connection.execute("PRAGMA user_version").fetchone()[0]
    except sqlite3.Error as error:
        raise LineageError("cannot load lineage dependency index") from error


@contextmanager
def writer_lock(store: Path, *, accepted_store=None):
    accepted = accept_store(store) if accepted_store is None else accepted_store
    if accepted.store != Path(store).absolute():
        raise LineageError("accepted lineage store belongs to another store")
    root = accepted.root
    root.mkdir(parents=True, exist_ok=True)
    # One permanent inode per store, not per ticker. Readers never take it.
    with path_at(root, ".worker.lock").open("a+b") as worker:
        fcntl.flock(worker.fileno(), fcntl.LOCK_EX)
        with publication_lock(root, exclusive=True):
            # Drain v1/v2 writers/readers before activating the new lock order.
            # Older executables then reject the current protocol before taking pricing locks.
            # Only the protocol header changes: no receipt or prices are rebuilt.
            existing = _index_version(root, readonly=False)
            if existing is not None and existing not in SUPPORTED_VERSIONS:
                raise LineageError("unsupported lineage index")
            if existing != VERSION:
                _update_index(root, {})
        yield root


@contextmanager
def writer_snapshot(store, *, accepted_store=None, context=None):
    """Own the writer lock and lend its coherent index only for that lifetime."""
    accepted = accept_store(store) if accepted_store is None else accepted_store
    if context is not None and context.store != accepted.store:
        raise LineageError("lineage context belongs to another store")
    with writer_lock(store, accepted_store=accepted):
        view = WriterIndexView(accepted, context)
        try:
            yield view
        finally:
            view._active = False
            view._index = None
            view._context = None


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
    store: Path,
    select,
    *,
    wait=True,
    selected_loader=None,
    include_boundaries=True,
    include_receipts=True,
    writer_view=None,
):
    """Snapshot selection, release gate, lock prices, reselect; never invert locks.

    select(root) returns symbol -> index entry (or None for offline discovery).
    Selected metadata is loaded under the brief gate, then pricing SH locks alone
    protect it throughout consumption. Unrelated groups can publish during reads.
    """
    if writer_view is None:
        check_store(store)
        root = regular_path(store, "end_of_day/lineage")
    else:
        writer_view.require_active(store)
        root = writer_view.root

    def gate():
        return publication_lock(root, wait=wait) if writer_view is None else nullcontext()

    def selection():
        if writer_view is not None:
            writer_view.require_active(store)
            return select(root)
        # New lock order must not overlap an executable using the old order.
        # The worker upgrades this header under the old shared/exclusive gate.
        if _index_version(root) not in {None, *range(6, VERSION + 1)}:
            raise LineageError("lineage lock protocol needs upgrade; run lineage first")
        if path_at(root, ".pending.json").exists():
            raise LineageError("lineage publication incomplete; resume lineage first")
        return select(root)

    for _ in range(3):
        with gate():
            selected = selection()
        with ExitStack() as locks:
            locked = set()
            for symbol in sorted(selected):
                symbol_label(symbol)
                pricing = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
                try:
                    stream = locks.enter_context((pricing / ".download.lock").open("rb"))
                except FileNotFoundError:
                    if selected[symbol] is not None:
                        raise
                    continue
                try:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_SH | (0 if wait else fcntl.LOCK_NB))
                except BlockingIOError as error:
                    raise LineageBusyError("pricing group is busy; retry later or use --wait") from error
                locked.add(symbol)
            with gate():
                if writer_view is not None:
                    writer_view.require_active(store)
                elif selection() != selected:
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
def pricing_locks(store: Path, symbols, *, known=()):
    # Never nest consume_pricing_update: acquire the complete known set in one order.
    with ExitStack() as stack:
        locked = set()
        for symbol in sorted(set(symbols)):
            symbol_label(symbol)
            root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
            try:
                stack.enter_context(ticker_lock(root, exclusive=True))
            except FileNotFoundError:
                if symbol in known:
                    raise
                continue
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
        with closing(connect_sqlite(path, readonly=True)) as connection:
            connection.execute("BEGIN")
            if connection.execute("PRAGMA user_version").fetchone()[0] not in SUPPORTED_VERSIONS:
                raise LineageError("unsupported lineage index")
            statement = "SELECT symbol, receipt FROM symbols"
            parameters = () if symbol is None else (symbol,)
            statement += " ORDER BY symbol" if symbol is None else " WHERE symbol = ?"
            index["symbols"] = {
                label: json.loads(value) for label, value in connection.execute(statement, parameters)
            }
            state = _projection_state(connection)
            if state is not None:
                index["projection_state"] = state
    except (sqlite3.Error, ValueError) as error:
        raise LineageError("cannot load lineage dependency index") from error
    for symbol, entry in index["symbols"].items():
        if symbol_label(symbol) != symbol or not isinstance(entry, dict):
            raise LineageError("invalid lineage index entry")
        if not isinstance(entry.get("dependencies"), list) or not isinstance(entry.get("asset_ids"), list):
            raise LineageError("lineage dependency index is incomplete")
    return index


def _projection_state(connection):
    if connection.execute("SELECT 1 FROM sqlite_master WHERE name='projection_state'").fetchone():
        row = connection.execute(
            "SELECT schema_version, revision, complete FROM projection_state WHERE id=1"
        ).fetchone()
        if row is not None:
            version, revision, complete = row
            return {"schema_version": version, "revision": revision, "complete": bool(complete)}
    return None


def index_state(root):
    """Read only the selector token; caller holds the publication gate."""
    path = path_at(root, "index.sqlite3")
    if not path.exists():
        return None
    try:
        with closing(connect_sqlite(path, readonly=True)) as connection:
            connection.execute("BEGIN")
            if connection.execute("PRAGMA user_version").fetchone()[0] not in SUPPORTED_VERSIONS:
                raise LineageError("unsupported lineage index")
            return _projection_state(connection)
    except sqlite3.Error as error:
        raise LineageError("cannot read lineage projection state") from error


def assert_index_current(root, index):
    if (root / ".pending.json").exists():
        raise LineageError("pending lineage publication; recover it before selection")
    state = index_state(root)
    if state is None or state != index.get("projection_state"):
        raise LineageError("lineage changed during discovery; repeat update selection")


def refresh_index(root, index=None):
    """Retain accepted membership until its compact selector changes."""
    if (root / ".pending.json").exists():
        raise LineageError("pending lineage publication; recover it before selection")
    if index is not None:
        state = index_state(root)
        if state is not None and state == index.get("projection_state"):
            return index
    return read_index(root)


def _update_index(root: Path, entries: dict, *, projection_complete=False) -> dict:
    # One tiny indexed row per ticker, not a full-universe JSON rewrite on each
    # update. The chronological JSON boundaries remain the authoritative outputs.
    path = path_at(root, "index.sqlite3")
    path_at(root, "index.sqlite3-journal")
    try:
        with closing(connect_sqlite(path, create=True)) as connection:
            connection.execute("PRAGMA journal_mode=DELETE")
            with connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS symbols (symbol TEXT PRIMARY KEY, receipt TEXT NOT NULL)"
                )
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS projection_state "
                    "(id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER, revision INTEGER, complete INTEGER)"
                )
                from .projection import PROJECTION_VERSION

                count = connection.execute("SELECT COUNT(*) FROM symbols").fetchone()[0]
                from libs.alpaca_serving_catalogue.work import record_publication

                # The serving obligation commits with the lineage selector, before
                # upstream acknowledgement. Recovery can replay this transaction.
                record_publication(connection, entries, empty=count == 0)
                connection.execute(
                    "INSERT OR IGNORE INTO projection_state VALUES (1, ?, 0, ?)",
                    (PROJECTION_VERSION, int(count == 0)),
                )
                connection.execute(f"PRAGMA user_version={VERSION}")
                connection.executemany(
                    "INSERT OR REPLACE INTO symbols(symbol, receipt) VALUES (?, ?)",
                    [(symbol, json_bytes(entry).decode()) for symbol, entry in sorted(entries.items())],
                )
                if any(
                    entry.get("projection", {}).get("schema_version") != PROJECTION_VERSION
                    for entry in entries.values()
                ):
                    connection.execute("UPDATE projection_state SET complete=0 WHERE id=1")
                if projection_complete:
                    missing = connection.execute(
                        "SELECT COUNT(*) FROM symbols WHERE "
                        "COALESCE(json_extract(receipt, '$.projection.schema_version'), 0) != ?",
                        (PROJECTION_VERSION,),
                    ).fetchone()[0]
                    if missing:
                        raise LineageError("lineage projection migration is incomplete")
                    connection.execute(
                        "UPDATE projection_state SET complete=1, schema_version=? WHERE id=1",
                        (PROJECTION_VERSION,),
                    )
                connection.execute("UPDATE projection_state SET revision=revision+1 WHERE id=1")
                state = _projection_state(connection)
    except sqlite3.Error as error:
        raise LineageError("cannot publish lineage dependency index") from error
    return state


def read_receipt(root: Path, symbol: str, index: dict, *, selected_loader=None) -> dict | None:
    entry = index["symbols"].get(symbol)
    if entry is None:
        return None
    value = (selected_loader or selected_document)(
        path_at(root, f"{symbol}/lineage.manifest.json", published=True), entry["sha256"]
    )
    if value.get("symbol") != symbol or value.get("schema_version") not in SUPPORTED_VERSIONS:
        raise LineageError("invalid lineage receipt")
    return value


class ReceiptCache:
    """One selected generation per symbol, scoped to a single writer-lock lifetime."""

    def __init__(self, root, index, *, capacity=None):
        if capacity is not None and (type(capacity) is not int or capacity < 1):
            raise ValueError("receipt cache capacity must be positive")
        self.root = root
        self.index = index
        self.capacity = capacity
        self.receipts = {}

    def _remember(self, symbol, value):
        self.receipts.pop(symbol, None)
        self.receipts[symbol] = value
        while self.capacity is not None and len(self.receipts) > self.capacity:
            self.receipts.pop(next(iter(self.receipts)))

    def read(self, symbol):
        entry = self.index["symbols"].get(symbol)
        if entry is None:
            self.receipts.pop(symbol, None)
            return None
        generation = entry["sha256"]
        cached = self.receipts.get(symbol)
        if cached is None or cached[0] != generation:
            cached = generation, read_receipt(self.root, symbol, self.index)
        self._remember(symbol, cached)
        return cached[1]

    def published(self, receipts):
        for symbol, receipt in receipts.items():
            self._remember(symbol, (self.index["symbols"][symbol]["sha256"], receipt))


def read_boundaries(root: Path, receipt: dict, *, selected_loader=None, kinds=None) -> dict:
    return {
        kind: (selected_loader or selected_document)(
            path_at(root, f"{receipt['symbol']}/{entry['manifest']}", published=True), entry["sha256"]
        )
        for kind, entry in receipt["boundaries"].items()
        if kinds is None or kind in kinds
    }


def _install(root: Path, journal: dict, *, compare=True) -> dict:
    # The journal and brief publication gate make multi-symbol visibility atomic to
    # cooperating readers. Index is the selector and is always installed last.
    _validate_journal_policy(journal)
    for relative, value in sorted(journal["documents"].items()):
        atomic_json(path_at(root, relative), value, compare=compare)
    for relative in journal["retire"]:
        path = path_at(root, relative, published=journal["schema_version"] >= 12)
        path.unlink(missing_ok=True)
        parent = path.parent
        while parent != root and parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
            parent = parent.parent
    return _update_index(root, journal["index_updates"])


def _validate_journal_policy(journal: dict) -> None:
    """Require one frozen policy provenance across a transaction and its outputs."""
    from .correlation_policy_contract import valid_policy_provenance

    policy = journal.get("correlation_policy")
    schema_version = journal.get("schema_version")
    if (
        type(schema_version) is int
        and schema_version in SUPPORTED_VERSIONS
        and schema_version < POLICY_PROVENANCE_LINEAGE_VERSION
    ):
        if (
            any(
                isinstance(entry, dict) and entry.get("correlation_policy") is not None
                for entry in journal.get("index_updates", {}).values()
            )
            or any(
                isinstance(document, dict) and document.get("correlation_policy") is not None
                for relative, document in journal.get("documents", {}).items()
                if relative.endswith("/lineage.manifest.json")
            )
            or policy is not None
        ):
            raise LineageError("legacy lineage transaction has inconsistent policy provenance")
        return
    if (
        type(schema_version) is not int
        or schema_version not in SUPPORTED_VERSIONS
        or schema_version < POLICY_PROVENANCE_LINEAGE_VERSION
        or not valid_policy_provenance(policy)
    ):
        raise LineageError("lineage transaction has no frozen policy provenance")
    if any(
        not isinstance(entry, dict) or entry.get("correlation_policy") != policy
        for entry in journal.get("index_updates", {}).values()
    ):
        raise LineageError("lineage transaction index policy provenance is inconsistent")
    receipt_documents = [
        document
        for relative, document in journal.get("documents", {}).items()
        if relative.endswith("/lineage.manifest.json")
    ]
    if any(
        not isinstance(receipt, dict) or receipt.get("correlation_policy") != policy
        for receipt in receipt_documents
    ):
        raise LineageError("lineage transaction receipt policy provenance is inconsistent")


def _acknowledge(store: Path, journal: dict, *, sources=None) -> None:
    expected_checks = journal.get("checks")
    if sources is not None:
        # Selection, publication and acknowledgement share the same pricing locks.
        # Only recovery can encounter a newer source or notice. Hint deletion still
        # compares epoch/revision in its transaction, since that database is disposable.
        for symbol, expected in journal["notices"].items():
            acknowledge_update(store / "end_of_day/pricing" / symbol, expected, selected=True)
        if expected_checks:
            acknowledge_checks(store, expected_checks, journal["sources"])
        return
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
            current = read_update(root)
            if expected is not None and current == expected:
                acknowledge_update(root, expected)
                current = None
            if current is None:
                acknowledged.append(symbol)
    if expected_checks:
        acknowledge_checks(store, expected_checks, acknowledged)


def finish_publication(store: Path, root: Path, journal: dict, *, receipts=None, sources=None) -> dict:
    """Normal publication retains locked inputs; recovery must select them afresh."""
    from libs.alpaca_pricing_download.check_summary import invalidate, summary_path

    from .check_summary import refresh

    if receipts is None:
        invalidate(store, journal["sources"], owner="lineage")
    # A fresh journal contains only known changes. Recovery may already have
    # installed some of them, so compare existing files only on that path.
    state = _install(root, journal, compare=receipts is None)
    if receipts is None and summary_path(store).exists():
        index = {"symbols": journal["index_updates"]}
        receipts = {symbol: read_receipt(root, symbol, index) for symbol in journal["sources"]}
    if receipts is not None:
        refresh(store, receipts, sources=sources)
    _acknowledge(store, journal, sources=sources)
    path_at(root, ".pending.json").unlink()
    return state


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
    store: Path,
    root: Path,
    index: dict,
    receipts: dict,
    boundaries: dict,
    notices: dict,
    *,
    checks=None,
    previous=None,
    sources=None,
) -> None:
    if not receipts:
        raise LineageError("lineage publication requires at least one receipt")
    if any(not reusable_decision(receipt) for receipt in receipts.values()):
        raise LineageError("lineage publication requires the authorized current policy")
    frozen_policy = next(iter(receipts.values())).get("correlation_policy")
    if not isinstance(frozen_policy, dict) or any(
        receipt.get("correlation_policy") != frozen_policy for receipt in receipts.values()
    ):
        raise LineageError("lineage publication receipts have inconsistent policy provenance")
    documents, retire = {}, []
    updated = {}
    from .projection import project_receipt

    for symbol, receipt in receipts.items():
        old = read_receipt(root, symbol, index) if previous is None else previous[symbol]
        old_paths = {entry["manifest"] for entry in old["boundaries"].values()} if old else set()
        new_paths = {entry["manifest"] for entry in receipt["boundaries"].values()}
        retire.extend(f"{symbol}/{path}" for path in sorted(old_paths - new_paths))
        for kind, value in boundaries[symbol].items():
            if old is None or old["boundaries"][kind] != receipt["boundaries"][kind]:
                documents[f"{symbol}/{receipt['boundaries'][kind]['manifest']}"] = value
        updated[symbol] = {
            "sha256": document_digest(receipt),
            "source_sha256": receipt["source_sha256"],
            "dependencies": receipt["dependencies"],
            "asset_ids": receipt["asset_ids"],
            "group_members": sorted(set(receipt["input_sources"]) | set(unavailable_pricing(receipt))),
            "versions": receipt["versions"],
            "correlation_policy": receipt["correlation_policy"],
            "projection": project_receipt(receipt),
        }
        if index["symbols"].get(symbol, {}).get("sha256") != updated[symbol]["sha256"]:
            documents[f"{symbol}/lineage.manifest.json"] = receipt
    # Published generation references already identify changed JSON; do not reopen
    # old payloads to repeat that comparison. Installation remains idempotent.
    journal = {
        "schema_version": VERSION,
        "boundary": "alpaca_lineage_transaction",
        "correlation_policy": frozen_policy,
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
    _validate_journal_policy(journal)
    # Caller already owns the complete pricing group and the worker-only lock.
    with publication_lock(root, exclusive=True):
        from libs.alpaca_pricing_download.check_summary import invalidate

        invalidate(store, receipts, owner="lineage")
        atomic_json(path_at(root, ".pending.json"), journal)
        state = finish_publication(store, root, journal, receipts=receipts, sources=sources)
        index["symbols"].update(updated)
        index["projection_state"] = state

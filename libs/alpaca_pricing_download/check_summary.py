"""Minimal pricing projection; producers invalidate before changing its inputs."""

import json
import sqlite3
import uuid
from contextlib import closing, contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from libs.common.sqlite_connections import connect_sqlite

from .contract import COLUMNS, NEW_YORK, DownloadError, digest, json_bytes, symbol_label, timestamp
from .endpoints import TAIL_VERSION, Endpoints, history_windows, validate_tail
from .storage import regular_path

SUMMARY_FILE = "pricing_check_summary.sqlite3"
FORMAT = 4


def summary_path(store):
    return regular_path(Path(store).absolute(), SUMMARY_FILE)


@contextmanager
def _connection(store, *, create=False, readonly=False, allow_legacy=False):
    path = summary_path(store)
    regular_path(path.parent, SUMMARY_FILE + "-journal")
    try:
        with closing(connect_sqlite(path, readonly=readonly, create=create)) as db:
            if create:
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    if db.execute("PRAGMA user_version").fetchone()[0] == 0:
                        db.execute(
                            "CREATE TABLE state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
                            "epoch TEXT NOT NULL, revision INTEGER NOT NULL, complete INTEGER NOT NULL)"
                        )
                        db.execute("INSERT INTO state VALUES (1, ?, 0, 0)", (uuid.uuid4().hex,))
                        db.execute(
                            "CREATE TABLE entries (symbol TEXT PRIMARY KEY, asof TEXT NOT NULL, "
                            "end_exclusive TEXT NOT NULL, last_all TEXT NOT NULL, first_all TEXT NOT NULL, "
                            "tail_all TEXT, tail_version INTEGER NOT NULL, source_sha256 TEXT)"
                        )
                        db.execute(
                            "CREATE TABLE pending (symbol TEXT NOT NULL, owner TEXT NOT NULL, "
                            "PRIMARY KEY(symbol, owner))"
                        )
                        db.execute(f"PRAGMA user_version={FORMAT}")
                    elif db.execute("PRAGMA user_version").fetchone()[0] in (1, 2):
                        columns = [row[1] for row in db.execute("PRAGMA table_info(entries)")]
                        if columns not in (
                            ["symbol", "asof", "last_all"],
                            ["symbol", "asof", "end_exclusive", "last_all"],
                        ):
                            raise DownloadError("unsupported pricing check summary")
                        # Only the explicit rebuild command opens with create=True.
                        # Its selected pricing receipts repopulate the new fact;
                        # an ordinary reader must never infer coverage from old rows.
                        db.execute("DROP TABLE entries")
                        db.execute(
                            "CREATE TABLE entries (symbol TEXT PRIMARY KEY, asof TEXT NOT NULL, "
                            "end_exclusive TEXT NOT NULL, last_all TEXT NOT NULL, first_all TEXT NOT NULL, "
                            "tail_all TEXT, tail_version INTEGER NOT NULL, source_sha256 TEXT)"
                        )
                        db.execute("UPDATE state SET complete=0, revision=revision+1 WHERE singleton=1")
                        db.execute(f"PRAGMA user_version={FORMAT}")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version == 3 and not readonly:
                # Version 3 already certifies membership and both endpoints.
                # Preserve it while explicitly selecting missing legacy tails.
                with db:
                    db.execute("BEGIN IMMEDIATE")
                    db.execute("ALTER TABLE entries ADD COLUMN tail_all TEXT")
                    db.execute("ALTER TABLE entries ADD COLUMN tail_version INTEGER NOT NULL DEFAULT 31")
                    db.execute("ALTER TABLE entries ADD COLUMN source_sha256 TEXT")
                    db.execute(f"PRAGMA user_version={FORMAT}")
                    db.execute("UPDATE state SET revision=revision+1 WHERE singleton=1")
                version = FORMAT
            if version not in (3, FORMAT) and not (allow_legacy and version == 2):
                raise DownloadError("unsupported pricing check summary; run alpaca-cli-eod summary rebuild")
            with db:
                db.execute("BEGIN" if readonly else "BEGIN IMMEDIATE")
                yield db
    except (sqlite3.Error, TypeError, ValueError) as error:
        raise DownloadError("cannot use pricing check summary; rebuild it from stored records") from error


def _token(db):
    return db.execute("SELECT epoch, revision FROM state WHERE singleton=1").fetchone()


def validate_entry(entry):
    if (
        set(entry)
        not in (
            {"symbol", "asof", "end_exclusive", "last_all", "first_all"},
            {"symbol", "asof", "end_exclusive", "last_all", "first_all", "tail_all", "tail_version"},
            {
                "symbol",
                "asof",
                "end_exclusive",
                "last_all",
                "first_all",
                "tail_all",
                "tail_version",
                "source_sha256",
            },
        )
        or symbol_label(entry["symbol"]) != entry["symbol"]
    ):
        raise DownloadError("invalid pricing check summary entry")
    if date.fromisoformat(entry["asof"]).isoformat() != entry["asof"]:
        raise DownloadError("invalid pricing check asof")
    end = timestamp(entry["end_exclusive"])
    for row in (entry["first_all"], entry["last_all"]):
        if (
            not isinstance(row, (list, tuple))
            or len(row) != len(COLUMNS)
            or any(type(v) is not str for v in row)
        ):
            raise DownloadError("invalid pricing check row")
        if timestamp(row[0]) >= end:
            raise DownloadError("pricing check row is outside completed coverage")
        try:
            if any(not Decimal(value).is_finite() for value in row[1:]):
                raise DownloadError("non-finite pricing check value")
        except InvalidOperation as error:
            raise DownloadError("invalid pricing check value") from error
    first, last = entry["first_all"], entry["last_all"]
    if timestamp(first[0]) > timestamp(last[0]) or (timestamp(first[0]) == timestamp(last[0]) and first != last):
        raise DownloadError("invalid pricing check endpoint ordering")
    if "tail_version" in entry:
        if entry["tail_version"] not in (31, TAIL_VERSION):
            raise DownloadError("unsupported pricing comparison tail version")
        if entry["tail_version"] == TAIL_VERSION or entry["tail_all"] is not None:
            validate_tail(entry["tail_all"], first, last)
    if "source_sha256" in entry:
        generation = entry["source_sha256"]
        if generation is not None or entry.get("tail_version") == TAIL_VERSION:
            if (
                not isinstance(generation, str)
                or len(generation) != 64
                or any(c not in "0123456789abcdef" for c in generation)
            ):
                raise DownloadError("invalid pricing comparison generation")
    return (
        entry["symbol"],
        entry["asof"],
        entry["end_exclusive"],
        json_bytes(list(last)).decode(),
        json_bytes(list(first)).decode(),
    )


class _AcceptedRows(list):
    """Keep JSON/list equality while preventing mutation of accepted rows."""

    def _immutable(self, *args, **kwargs):
        raise TypeError("accepted summary rows are immutable")

    __setitem__ = __delitem__ = append = clear = extend = insert = pop = remove = reverse = sort = __iadd__ = (
        __imul__
    ) = _immutable

    def __deepcopy__(self, memo):
        from copy import deepcopy

        return deepcopy(list(self), memo)


class AcceptedEntry(dict):
    """Immutable summary acceptance, including reusable per-target append plans.

    A changed summary publication creates new entries. Never lend these facts to
    a different entry or target; consumers need no second row/tail validation.
    """

    def __init__(self, entry, serialized):
        super().__init__(
            {
                **entry,
                "first_all": _AcceptedRows(entry["first_all"]),
                "last_all": _AcceptedRows(entry["last_all"]),
                **(
                    {"tail_all": _AcceptedRows(_AcceptedRows(row) for row in entry["tail_all"])}
                    if entry.get("tail_all") is not None
                    else {}
                ),
            }
        )
        self.serialized = serialized
        self.append_plans = {}

    def _immutable(self, *args, **kwargs):
        raise TypeError("accepted summary entries are immutable")

    __setitem__ = __delitem__ = update = clear = pop = popitem = setdefault = __ior__ = _immutable

    def __deepcopy__(self, memo):
        # Explicit copies are unaccepted inputs, with no inherited plan cache.
        from copy import deepcopy

        return deepcopy(dict(self), memo)


def _serialized(entry):
    return (
        *(entry.serialized if isinstance(entry, AcceptedEntry) else validate_entry(entry)),
        json_bytes(entry["tail_all"]).decode() if entry.get("tail_all") is not None else None,
        entry.get("tail_version", 31),
        entry.get("source_sha256"),
    )


def _entries(db, clause="ORDER BY symbol", params=()):
    columns = (
        "tail_all, tail_version, source_sha256"
        if db.execute("PRAGMA user_version").fetchone()[0] == FORMAT
        else "NULL, 31, NULL"
    )
    result = []
    for symbol, asof, end, last, first, tail, version, generation in db.execute(
        f"SELECT symbol, asof, end_exclusive, last_all, first_all, {columns} FROM entries {clause}", params
    ):
        entry = {
            "symbol": symbol,
            "asof": asof,
            "end_exclusive": end,
            "last_all": json.loads(last),
            "first_all": json.loads(first),
            "tail_all": json.loads(tail) if tail is not None else None,
            "tail_version": version,
            "source_sha256": generation,
        }
        result.append(AcceptedEntry(entry, validate_entry(entry)))
    return result


def invalidate(store, symbols, *, owner="pricing"):
    """Under pricing locks, before either producer installs any changed input."""
    if not summary_path(store).exists():
        return  # Absence already fails closed; only an explicit rebuild establishes coverage.
    with _connection(store, allow_legacy=True) as db:
        db.executemany("INSERT OR IGNORE INTO pending VALUES (?, ?)", [(symbol, owner) for symbol in symbols])
        db.execute("UPDATE state SET revision=revision+1 WHERE singleton=1")


def replace_entries(store, entries, *, pricing_only=False):
    """Lineage supplies eligibility; None removes an ineligible or empty label."""
    # Publication ownership is split: pricing owns comparison rows; lineage
    # owns membership. Only their completed handoff may clear pending inputs;
    # a newer pricing baseline cannot stand in for unfinished lineage work.
    if not summary_path(store).exists():
        return
    values = {symbol: None if entry is None else _serialized(entry) for symbol, entry in entries.items()}
    if any(value is not None and value[0] != symbol for symbol, value in values.items()):
        raise DownloadError("pricing check entry owner differs")
    with _connection(store, allow_legacy=True) as db:
        legacy = db.execute("PRAGMA user_version").fetchone()[0] == 2
        before = db.total_changes
        for symbol, value in values.items():
            if (
                pricing_only
                and db.execute("SELECT 1 FROM pending WHERE symbol=? AND owner='lineage'", (symbol,)).fetchone()
            ):
                continue  # A price publication cannot complete an interrupted status change.
            if value is None:
                db.execute("DELETE FROM entries WHERE symbol=?", (symbol,))
            else:
                # Frozen journals may complete their old summary bookkeeping.
                # Readers still reject v2; only explicit rebuild supplies every
                # first endpoint. Recovery must not get stuck behind migration.
                db.execute(
                    (
                        "INSERT INTO entries VALUES (?, ?, ?, ?"
                        + (")" if legacy else ", ?, ?, ?, ?)")
                        + " ON CONFLICT(symbol) DO UPDATE SET "
                        "asof=excluded.asof, end_exclusive=excluded.end_exclusive, last_all=excluded.last_all"
                        + (
                            ""
                            if legacy
                            else ", first_all=excluded.first_all, tail_all=excluded.tail_all, tail_version=excluded.tail_version, source_sha256=excluded.source_sha256"
                        )
                        + " WHERE asof!=excluded.asof OR end_exclusive!=excluded.end_exclusive "
                        "OR last_all!=excluded.last_all"
                        + (
                            ""
                            if legacy
                            else " OR first_all!=excluded.first_all OR tail_all IS NOT excluded.tail_all OR tail_version!=excluded.tail_version OR source_sha256 IS NOT excluded.source_sha256"
                        )
                    ),
                    value[:4] if legacy else value,
                )
            db.execute("DELETE FROM pending WHERE symbol=?", (symbol,))
        if db.total_changes != before:
            db.execute("UPDATE state SET revision=revision+1 WHERE singleton=1")


def selected_inputs(store, symbols):
    """Lend completed endpoints and pending ownership to a locked producer.

    A lineage invalidation changes membership, not pricing's accepted endpoint.
    A pricing invalidation prevents endpoint reuse until that owner completes.
    Only the named publication's inputs are selected; unrelated pending work
    neither expands this selection nor invalidates these accepted endpoints.
    """
    symbols = tuple(symbols)
    if not symbols or not summary_path(store).exists():
        return None
    placeholders = ",".join("?" for _ in symbols)
    with _connection(store, readonly=True, allow_legacy=True) as db:
        complete = bool(db.execute("SELECT complete FROM state WHERE singleton=1").fetchone()[0])
        legacy = db.execute("PRAGMA user_version").fetchone()[0] == 2
        entries = (
            {entry["symbol"]: entry for entry in _entries(db, f"WHERE symbol IN ({placeholders})", symbols)}
            if not legacy
            else {}
        )
        pending = {symbol: set() for symbol in symbols}
        for symbol, owner in db.execute(
            f"SELECT symbol, owner FROM pending WHERE symbol IN ({placeholders})", symbols
        ):
            pending[symbol].add(owner)
    return {"complete": complete and not legacy, "entries": entries, "pending": pending}


def price_entry(root, source, *, published=True, endpoints=None, source_sha256=None):
    from .storage import read_validator

    request = source["latest_request"]
    if (request["timeframe"], request["feed"], request["currency"]) != ("1Day", "sip", "USD"):
        raise DownloadError("pricing check requires 1Day SIP USD inputs")
    # A publisher can lend its accepted endpoints directly. DO NOT reload the
    # validator or derive endpoints from prices when that handoff is supplied.
    if endpoints is None:
        endpoints = read_validator(root, source, published=published)
    if endpoints is None:
        raise DownloadError("pricing check requires a published endpoint validator")
    if source["schema_version"] >= TAIL_VERSION and getattr(endpoints, "tail_all", None) is None:
        raise DownloadError("new pricing publication lacks its required comparison tail")
    rows = endpoints["all"]
    if not rows:
        return None
    return {
        "symbol": source["symbol"],
        "asof": request["asof"],
        "end_exclusive": request["end_exclusive"],
        "last_all": list(rows[-1]),
        "first_all": list(rows[0]),
        "tail_all": [list(row) for row in endpoints.tail_all]
        if getattr(endpoints, "tail_all", None) is not None
        else None,
        "tail_version": TAIL_VERSION if getattr(endpoints, "tail_all", None) is not None else 31,
        "source_sha256": source_sha256 or digest(json_bytes(source)),
    }


def refresh_prices(root, *, source=None, endpoints=None, source_sha256=None):
    """Update an already eligible label without importing or recalculating lineage."""
    from .storage import load_source

    store, symbol = root.parents[2], root.name
    if not summary_path(store).exists():
        return
    with _connection(store, readonly=True, allow_legacy=True) as db:
        present = db.execute("SELECT 1 FROM entries WHERE symbol=?", (symbol,)).fetchone()
    if present:
        if source is None:
            source = load_source(root, symbol)
        replace_entries(
            store,
            {symbol: price_entry(root, source, endpoints=endpoints, source_sha256=source_sha256)},
            pricing_only=True,
        )
    # New eligibility, including a formerly empty active source, belongs to lineage.


def begin_rebuild(store):
    with _connection(store, create=True) as db:
        db.execute("UPDATE state SET complete=0, revision=revision+1 WHERE singleton=1")
        return _token(db)


def finish_rebuild(store, token, entries):
    values = [_serialized(entry) for entry in entries]
    with _connection(store) as db:
        if _token(db) != token:
            raise DownloadError("pricing changed during summary rebuild; rerun the rebuild")
        db.execute("DELETE FROM entries")
        db.executemany("INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?, ?)", values)
        db.execute("DELETE FROM pending")
        db.execute("UPDATE state SET complete=1, revision=revision+1 WHERE singleton=1")


def _require_ready(db):
    if not db.execute("SELECT complete FROM state WHERE singleton=1").fetchone()[0]:
        raise DownloadError("pricing check summary needs a complete rebuild")
    if db.execute("SELECT 1 FROM pending LIMIT 1").fetchone():
        raise DownloadError("pricing check summary has pending inputs; finish lineage or rebuild it")


def assert_current(store, token):
    """A resolver must reject results if publication changed its selected baseline."""
    # Recheck this selector's revision across intervening work, not the ticker
    # files that produced it. The token detects changed baselines/membership.
    with _connection(store, readonly=True) as db:
        _require_ready(db)
        if _token(db) != tuple(token):
            raise DownloadError("pricing check baseline changed; repeat the check")


def read_snapshot(store):
    """Read one small database, never ticker directories, receipts or bar files."""
    # This completed selector is the live-check universe and last-row authority.
    # DO NOT rediscover eligibility or verify each source path after accepting it.
    # Missing/incomplete/pending selectors fail closed into their owning recovery.
    if not summary_path(store).exists():
        raise DownloadError("pricing check summary is missing; rebuild it from stored records")
    with _connection(store, readonly=True) as db:
        _require_ready(db)
        token = _token(db)
        entries = _entries(db)
    return {"entries": entries, "sha256": digest(json_bytes(entries)), "token": token}


def refresh_snapshot(store, snapshot=None):
    """Reconstruct membership only after a real summary publication."""
    if snapshot is not None:
        with _connection(store, readonly=True) as db:
            _require_ready(db)
            if _token(db) == tuple(snapshot["token"]):
                return snapshot
    return read_snapshot(store)


def read_status(store):
    """Inspect coverage and pending owners without requiring a ready handoff."""
    if not summary_path(store).exists():
        raise DownloadError("pricing check summary is missing; run alpaca-cli-eod summary rebuild")
    with _connection(store, readonly=True) as db:
        complete = bool(db.execute("SELECT complete FROM state WHERE singleton=1").fetchone()[0])
        token = _token(db)
        entries = _entries(db)
        pending = [
            {"symbol": symbol, "owner": owner}
            for symbol, owner in db.execute("SELECT symbol, owner FROM pending ORDER BY symbol, owner")
        ]
    return {
        **describe({"entries": entries, "sha256": digest(json_bytes(entries))}),
        "schema_version": FORMAT,
        "token": token,
        "coverage_complete": complete,
        "pending_inputs": pending,
        "coverage_end_exclusive": {
            "earliest": min((entry["end_exclusive"] for entry in entries), default=None),
            "latest": max((entry["end_exclusive"] for entry in entries), default=None),
        },
    }


def pending_symbols(store):
    """Select interrupted producer inputs from the existing handoff, without scans."""
    if not summary_path(store).exists():
        raise DownloadError("pricing check summary is missing; rebuild it from stored records")
    with _connection(store, allow_legacy=True) as db:
        if not db.execute("SELECT complete FROM state WHERE singleton=1").fetchone()[0]:
            raise DownloadError("pricing check summary needs a complete rebuild")
        return [symbol for (symbol,) in db.execute("SELECT DISTINCT symbol FROM pending ORDER BY symbol")]


def describe(snapshot):
    groups, last_sessions, legacy = set(), set(), 0
    for entry in snapshot["entries"]:
        last_sessions.add(timestamp(entry["last_all"][0]).astimezone(NEW_YORK).date())
        if entry.get("tail_all") is None:
            legacy += 1
            continue
        endpoints = Endpoints({"all": (entry["first_all"], entry["last_all"])}, tail_all=entry["tail_all"])
        for start, end in history_windows(
            endpoints, cutoff=timestamp(entry["end_exclusive"]), utc_session_end=True
        ):
            groups.add((start, end, entry["asof"]))
    return {
        "tickers": len(snapshot["entries"]),
        "last_session_dates": len(last_sessions),
        "query_groups": len(groups),
        "legacy_tail_upgrades": legacy,
        "sha256": snapshot["sha256"],
    }

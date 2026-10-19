"""Minimal last-session projection; producers invalidate before changing its inputs."""

import json
import sqlite3
import uuid
from contextlib import closing, contextmanager
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from libs.common.sqlite_connections import connect_sqlite

from .contract import COLUMNS, NEW_YORK, DownloadError, digest, json_bytes, symbol_label, timestamp
from .storage import regular_path

SUMMARY_FILE = "pricing_check_summary.sqlite3"
FORMAT = 1  # Introduced by pricing v9 / lineage v7.


def summary_path(store):
    return regular_path(Path(store).absolute(), SUMMARY_FILE)


@contextmanager
def _connection(store, *, create=False, readonly=False):
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
                            "last_all TEXT NOT NULL)"
                        )
                        db.execute(
                            "CREATE TABLE pending (symbol TEXT NOT NULL, owner TEXT NOT NULL, "
                            "PRIMARY KEY(symbol, owner))"
                        )
                        db.execute(f"PRAGMA user_version={FORMAT}")
            if db.execute("PRAGMA user_version").fetchone()[0] != FORMAT:
                raise DownloadError("unsupported pricing check summary")
            with db:
                db.execute("BEGIN" if readonly else "BEGIN IMMEDIATE")
                yield db
    except (sqlite3.Error, TypeError, ValueError) as error:
        raise DownloadError("cannot use pricing check summary; rebuild it from stored records") from error


def _token(db):
    return db.execute("SELECT epoch, revision FROM state WHERE singleton=1").fetchone()


def validate_entry(entry):
    if set(entry) != {"symbol", "asof", "last_all"} or symbol_label(entry["symbol"]) != entry["symbol"]:
        raise DownloadError("invalid pricing check summary entry")
    if date.fromisoformat(entry["asof"]).isoformat() != entry["asof"]:
        raise DownloadError("invalid pricing check asof")
    row = entry["last_all"]
    if not isinstance(row, (list, tuple)) or len(row) != len(COLUMNS) or any(type(v) is not str for v in row):
        raise DownloadError("invalid pricing check row")
    timestamp(row[0])
    try:
        if any(not Decimal(value).is_finite() for value in row[1:]):
            raise DownloadError("non-finite pricing check value")
    except InvalidOperation as error:
        raise DownloadError("invalid pricing check value") from error
    return entry["symbol"], entry["asof"], json_bytes(list(row)).decode()


def invalidate(store, symbols, *, owner="pricing"):
    """Under pricing locks, before either producer installs any changed input."""
    if not summary_path(store).exists():
        return  # Absence already fails closed; only an explicit rebuild establishes coverage.
    with _connection(store) as db:
        db.executemany("INSERT OR IGNORE INTO pending VALUES (?, ?)", [(symbol, owner) for symbol in symbols])
        db.execute("UPDATE state SET revision=revision+1 WHERE singleton=1")


def replace_entries(store, entries, *, pricing_only=False):
    """Lineage supplies eligibility; None removes an ineligible or empty label."""
    if not summary_path(store).exists():
        return
    values = {symbol: None if entry is None else validate_entry(entry) for symbol, entry in entries.items()}
    if any(value is not None and value[0] != symbol for symbol, value in values.items()):
        raise DownloadError("pricing check entry owner differs")
    with _connection(store) as db:
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
                db.execute(
                    "INSERT INTO entries VALUES (?, ?, ?) ON CONFLICT(symbol) DO UPDATE SET "
                    "asof=excluded.asof, last_all=excluded.last_all "
                    "WHERE asof!=excluded.asof OR last_all!=excluded.last_all",
                    value,
                )
            db.execute("DELETE FROM pending WHERE symbol=?", (symbol,))
        if db.total_changes != before:
            db.execute("UPDATE state SET revision=revision+1 WHERE singleton=1")


def price_entry(root, source):
    from .storage import read_validator

    request = source["latest_request"]
    if (request["timeframe"], request["feed"], request["currency"]) != ("1Day", "sip", "USD"):
        raise DownloadError("pricing check requires 1Day SIP USD inputs")
    endpoints = read_validator(root, source)
    if endpoints is None:
        raise DownloadError("pricing check requires a published endpoint validator")
    rows = endpoints["all"]
    if not rows:
        return None
    return {"symbol": source["symbol"], "asof": request["asof"], "last_all": list(rows[-1])}


def refresh_prices(root):
    """Update an already eligible label without importing or recalculating lineage."""
    from .storage import load_source

    store, symbol = root.parents[2], root.name
    if not summary_path(store).exists():
        return
    with _connection(store, readonly=True) as db:
        present = db.execute("SELECT 1 FROM entries WHERE symbol=?", (symbol,)).fetchone()
    if present:
        replace_entries(store, {symbol: price_entry(root, load_source(root, symbol))}, pricing_only=True)
    # New eligibility, including a formerly empty active source, belongs to lineage.


def begin_rebuild(store):
    with _connection(store, create=True) as db:
        db.execute("UPDATE state SET complete=0, revision=revision+1 WHERE singleton=1")
        return _token(db)


def finish_rebuild(store, token, entries):
    values = [validate_entry(entry) for entry in entries]
    with _connection(store) as db:
        if _token(db) != token:
            raise DownloadError("pricing changed during summary rebuild; rerun the rebuild")
        db.execute("DELETE FROM entries")
        db.executemany("INSERT INTO entries VALUES (?, ?, ?)", values)
        db.execute("DELETE FROM pending")
        db.execute("UPDATE state SET complete=1, revision=revision+1 WHERE singleton=1")


def _require_ready(db):
    if not db.execute("SELECT complete FROM state WHERE singleton=1").fetchone()[0]:
        raise DownloadError("pricing check summary needs a complete rebuild")
    if db.execute("SELECT 1 FROM pending LIMIT 1").fetchone():
        raise DownloadError("pricing check summary has pending inputs; finish lineage or rebuild it")


def assert_current(store, token):
    """A resolver must reject results if publication changed its selected baseline."""
    with _connection(store, readonly=True) as db:
        _require_ready(db)
        if _token(db) != tuple(token):
            raise DownloadError("pricing check baseline changed; repeat the check")


def read_snapshot(store):
    """Read one small database, never ticker directories, receipts or bar files."""
    if not summary_path(store).exists():
        raise DownloadError("pricing check summary is missing; rebuild it from stored records")
    with _connection(store, readonly=True) as db:
        _require_ready(db)
        token = _token(db)
        entries = [
            {"symbol": symbol, "asof": asof, "last_all": json.loads(row)}
            for symbol, asof, row in db.execute("SELECT symbol, asof, last_all FROM entries ORDER BY symbol")
        ]
        for entry in entries:
            validate_entry(entry)
    return {"entries": entries, "sha256": digest(json_bytes(entries)), "token": token}


def pending_symbols(store):
    """Select interrupted producer inputs from the existing handoff, without scans."""
    if not summary_path(store).exists():
        raise DownloadError("pricing check summary is missing; rebuild it from stored records")
    with _connection(store) as db:
        if not db.execute("SELECT complete FROM state WHERE singleton=1").fetchone()[0]:
            raise DownloadError("pricing check summary needs a complete rebuild")
        return [symbol for (symbol,) in db.execute("SELECT DISTINCT symbol FROM pending ORDER BY symbol")]


def describe(snapshot):
    groups = {}
    for entry in snapshot["entries"]:
        day = timestamp(entry["last_all"][0]).astimezone(NEW_YORK).date().isoformat()
        groups.setdefault((day, entry["asof"]), []).append(entry["symbol"])
    return {
        "tickers": len(snapshot["entries"]),
        "last_session_dates": len({day for day, _ in groups}),
        "query_groups": len(groups),
        "sha256": snapshot["sha256"],
    }

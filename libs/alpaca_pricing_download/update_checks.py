"""Disposable lineage work hints. Published receipts remain the only data authority."""

import fcntl
import os
import sqlite3
import uuid
from contextlib import closing
from pathlib import Path

from .contract import DownloadError
from .storage import fsync_directory, regular_path

CHECK_FILE = "lineage_update_check.sqlite3"


def _connect(store):
    path = regular_path(Path(store).absolute(), CHECK_FILE)
    if not path.exists():
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            if not path.exists():
                temporary = regular_path(path.parent, ".lineage_update_check." + uuid.uuid4().hex)
                try:
                    with closing(sqlite3.connect(temporary, timeout=60)) as connection, connection:
                        connection.execute(
                            "CREATE TABLE state "
                            "(singleton INTEGER PRIMARY KEY CHECK(singleton = 1), epoch TEXT NOT NULL, "
                            "complete INTEGER NOT NULL, revision INTEGER NOT NULL)"
                        )
                        connection.execute(
                            "CREATE TABLE checks (symbol TEXT PRIMARY KEY, revision INTEGER NOT NULL)"
                        )
                        connection.execute("INSERT INTO state VALUES (1, ?, 0, 0)", (uuid.uuid4().hex,))
                    os.replace(temporary, path)
                    fsync_directory(path.parent)
                finally:
                    if temporary.exists():
                        temporary.unlink()
        finally:
            os.close(descriptor)
    # Never create an empty database if somebody deletes the cache concurrently.
    return sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=60)


def read_checks(store, symbols=None):
    """A missing list is an incomplete cache, never evidence that no work remains."""
    path = regular_path(Path(store).absolute(), CHECK_FILE)
    if not path.exists():
        return {"epoch": None, "revision": 0, "complete": False, "checks": {}}
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=60)) as connection:
            with connection:
                connection.execute("BEGIN")
                epoch, complete, revision = connection.execute(
                    "SELECT epoch, complete, revision FROM state WHERE singleton = 1"
                ).fetchone()
                statement = "SELECT symbol, revision FROM checks"
                parameters = () if symbols is None else tuple(sorted(symbols))
                if symbols is not None:
                    statement += " WHERE symbol IN (" + ",".join("?" for _ in parameters) + ")"
                checks = dict(connection.execute(statement + " ORDER BY symbol", parameters))
        return {"epoch": epoch, "revision": revision, "complete": bool(complete), "checks": checks}
    except (sqlite3.Error, TypeError, ValueError) as error:
        raise DownloadError("cannot read disposable lineage update check file") from error


def begin_checks(store):
    """Establish an incomplete cache before a fallback scan can start."""
    try:
        with closing(_connect(store)):
            pass
        return read_checks(store)
    except sqlite3.Error as error:
        raise DownloadError("cannot initialize lineage update check file") from error


def queue_check(store, symbol):
    """Called under the ticker lock, before a pricing publication becomes visible."""
    try:
        with closing(_connect(store)) as connection, connection:
            connection.execute("UPDATE state SET revision = revision + 1 WHERE singleton = 1")
            connection.execute(
                "INSERT OR REPLACE INTO checks SELECT ?, revision FROM state WHERE singleton = 1", (symbol,)
            )
            event = connection.execute("SELECT epoch, revision FROM state WHERE singleton = 1").fetchone()
        return event
    except sqlite3.Error as error:
        raise DownloadError("cannot queue lineage input check") from error


def acknowledge_checks(store, expected, symbols):
    """Remove only inspected hints; deletion/recreation or a newer update wins."""
    path = regular_path(Path(store).absolute(), CHECK_FILE)
    if not expected["checks"] or not path.exists() or expected["epoch"] is None:
        return
    try:
        with (
            closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=60)) as connection,
            connection,
        ):
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT epoch FROM state WHERE singleton = 1").fetchone()
            if row is None or row[0] != expected["epoch"]:
                return
            connection.executemany(
                "DELETE FROM checks WHERE symbol = ? AND revision = ?",
                [(symbol, expected["checks"][symbol]) for symbol in symbols if symbol in expected["checks"]],
            )
    except sqlite3.Error as error:
        raise DownloadError("cannot acknowledge lineage input checks") from error


def complete_checks(store, epoch):
    """Only a successful fallback scan may make its still-existing cache complete."""
    path = regular_path(Path(store).absolute(), CHECK_FILE)
    if not path.exists() or epoch is None:
        return
    try:
        with (
            closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=60)) as connection,
            connection,
        ):
            connection.execute("UPDATE state SET complete = 1 WHERE singleton = 1 AND epoch = ?", (epoch,))
    except sqlite3.Error as error:
        raise DownloadError("cannot complete lineage update check file") from error

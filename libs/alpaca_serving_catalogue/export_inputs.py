"""Durable producer invalidation for mutable catalogue-selected export inputs.

This is part of the serving selector, not a scheduling hint. Producers record
their intended generation before replacing any selected bytes. Serving accepts
only matching publications. Export consumes this boundary once under the store
operation lock; it never authenticates upstream files to reconstruct validity.
"""

import json
import sqlite3
from contextlib import closing

from libs.common.debug_trace import traced
from libs.common.sqlite_connections import connect_sqlite

from .contract import CatalogueError

VERSION = 1


def initialize(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS export_input_state "
        "(id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, complete INTEGER NOT NULL)"
    )
    connection.execute("INSERT OR IGNORE INTO export_input_state VALUES(1, ?, 0)", (VERSION,))
    connection.execute(
        "CREATE TABLE IF NOT EXISTS export_inputs "
        "(symbol TEXT NOT NULL, kind TEXT NOT NULL, generation TEXT NOT NULL, PRIMARY KEY(symbol, kind))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS export_input_bindings "
        "(group_key TEXT PRIMARY KEY, decision_generation TEXT NOT NULL)"
    )


def complete(connection):
    try:
        row = connection.execute("SELECT version, complete FROM export_input_state WHERE id=1").fetchone()
    except sqlite3.OperationalError as error:
        if "no such table" in str(error):
            return False
        raise
    if row is None:
        return False
    if row[0] != VERSION:
        raise CatalogueError("incompatible export input applicability; run catalogue rebuild")
    return bool(row[1])


def ready(store):
    from .storage import path

    with closing(connect_sqlite(path(store), readonly=True)) as connection:
        return complete(connection)


def bound_groups(store):
    """Explicit legacy rebuild retains completed group bindings across retries."""
    from .storage import path

    with closing(connect_sqlite(path(store), readonly=True)) as connection:
        try:
            return dict(connection.execute("SELECT group_key, decision_generation FROM export_input_bindings"))
        except sqlite3.OperationalError as error:
            if "no such table" in str(error):
                return {}
            raise


@traced("serving_export_inputs_invalidate")
def invalidate(store, kind, generations):
    """Called by the owning publisher before its first mutable installation."""
    from .storage import WRITE_TIMEOUT_SECONDS, path

    if not generations or not path(store).exists():
        return
    if kind not in {"pricing", "lineage"}:
        raise CatalogueError("unknown export input publication owner")
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        rows = [(symbol, kind, generation) for symbol, generation in sorted(generations.items())]
        statement = (
            "INSERT INTO export_inputs VALUES(?, ?, ?) "
            "ON CONFLICT(symbol, kind) DO UPDATE SET generation=excluded.generation "
            "WHERE generation!=excluded.generation"
        )
        try:
            connection.executemany(statement, rows)
        except sqlite3.OperationalError as error:
            if "no such table" not in str(error):
                raise
            # Legacy coverage stays incomplete until its explicit owner rebuilds.
            initialize(connection)
            connection.executemany(statement, rows)


def accept(connection, references):
    """The serving producer accepts exactly its locked, selected generations."""
    rows = [
        (symbol, kind, generation)
        for kind, entries in references.items()
        for symbol, generation in entries.items()
        if generation is not None
    ]
    statement = "DELETE FROM export_inputs WHERE symbol=? AND kind=? AND generation=?"
    try:
        connection.executemany(statement, rows)
    except sqlite3.OperationalError as error:
        if "no such table" not in str(error):
            raise
        initialize(connection)
        connection.executemany(statement, rows)


def retire(connection, key):
    try:
        connection.execute("DELETE FROM export_input_bindings WHERE group_key=?", (key,))
    except sqlite3.OperationalError as error:
        if "no such table" not in str(error):
            raise
        initialize(connection)


def bind(connection, key, decision_generation):
    connection.execute("INSERT OR REPLACE INTO export_input_bindings VALUES(?, ?)", (key, decision_generation))


def complete_unchanged_pricing(store, symbol, generation):
    """The producer finished the same accepted source identity under its lock."""
    from .storage import WRITE_TIMEOUT_SECONDS, path

    if not path(store).exists():
        return
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        accept(connection, {"pricing": {symbol: generation}})


def pending_lineage(store, *, initializing=False):
    """Select compact intentions once; only the lineage owner can resolve them."""
    from .storage import path

    with closing(connect_sqlite(path(store), readonly=True)) as connection:
        if not initializing and not complete(connection):
            raise CatalogueError("export input applicability incomplete; run catalogue rebuild")
        return dict(connection.execute("SELECT symbol, generation FROM export_inputs WHERE kind='lineage'"))


def pending_counts(store, symbols=None):
    """Report outstanding applicability work for published serving components.

    The owner has already accepted this database's contract. Its work queue does
    not establish whether publication invalidations were acknowledged. Read that
    separate compact completion fact once, without reopening any upstream body.
    Sources outside published components cannot invalidate an existing export.
    A focused request includes all members of its selected published components.
    """
    from .storage import path

    counts = {"pricing": 0, "lineage": 0}
    if symbols is not None and not symbols:
        return counts
    predicate, parameters = "", ()
    if symbols is not None:
        predicate = (
            " WHERE m.group_key IN (SELECT group_key FROM members "
            "WHERE symbol IN (SELECT value FROM json_each(?)))"
        )
        parameters = (json.dumps(sorted(symbols)),)
    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            rows = connection.execute(
                "SELECT i.kind, COUNT(*) FROM export_inputs i "
                "JOIN members m ON m.symbol=i.symbol" + predicate + " GROUP BY i.kind",
                parameters,
            )
            for kind, count in rows:
                if kind not in counts:
                    raise CatalogueError("unknown export input publication owner")
                counts[kind] = count
    except sqlite3.Error as error:
        raise CatalogueError("export input applicability unavailable; finish owning recovery") from error
    return counts


def reconcile_lineage(store, selected, generation):
    """Finish abandoned/completed lineage intentions under its owning worker lock.

    Caller has accepted no pending lineage journal and matching compact current
    index/serving generations. These are owner completion facts, not permission
    to reopen receipts, calculate ownership or authenticate historical prices.
    Conditional deletion cannot consume a subsequently renewed intention.
    """
    from .storage import WRITE_TIMEOUT_SECONDS, _state, path

    if not selected:
        return
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        if _state(connection, incomplete=True)["generation"] != generation:
            raise CatalogueError("serving selector changed before lineage acknowledgement")
        connection.executemany(
            "DELETE FROM export_inputs WHERE symbol=? AND kind='lineage' AND generation=?",
            sorted(selected.items()),
        )


def finish(connection):
    initialize(connection)
    connection.execute("UPDATE export_input_state SET complete=1 WHERE id=1 AND version=?", (VERSION,))


@traced("serving_export_inputs_select")
def select(store, generation, symbols):
    """One coherent applicability/token read for the selected export scope."""
    from .storage import _state, path

    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            connection.execute("BEGIN")
            if _state(connection)["generation"] != generation:
                raise CatalogueError("selected serving catalogue changed")
            if not complete(connection):
                raise CatalogueError("export input applicability incomplete; run catalogue rebuild")
            invalid = connection.execute(
                "SELECT symbol FROM export_inputs WHERE symbol IN (SELECT value FROM json_each(?)) LIMIT 1",
                (json.dumps(sorted(symbols)),),
            ).fetchone()
            if invalid is not None:
                raise CatalogueError(
                    "selected export inputs changed; finish pricing, lineage and catalogue maintenance"
                )
    except sqlite3.Error as error:
        raise CatalogueError("export input applicability unavailable; finish owning recovery") from error

"""Durable serving obligations in the transaction selecting lineage entries."""

import json
import sqlite3
from contextlib import closing

from libs.common.sqlite_connections import connect_sqlite

from .contract import CatalogueError

WORK_VERSION = 2


def record_publication(connection, entries, *, empty, handoff=None):
    # Called inside the lineage index transaction: selecting a changed receipt
    # and recording its serving obligation become visible together. Queue state
    # selects work; the committed catalogue decides whether it is already done.
    connection.execute(
        "CREATE TABLE IF NOT EXISTS serving_work (symbol TEXT PRIMARY KEY, generation TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS serving_work_state (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, complete INTEGER NOT NULL)"
    )
    connection.execute("INSERT OR IGNORE INTO serving_work_state VALUES(1, ?, ?)", (WORK_VERSION, int(empty)))
    connection.execute(
        "CREATE TABLE IF NOT EXISTS serving_handoffs (symbol TEXT PRIMARY KEY, generation TEXT NOT NULL, body TEXT NOT NULL)"
    )
    if handoff is not None:
        state = connection.execute("SELECT version FROM serving_work_state WHERE id=1").fetchone()
        if state != (WORK_VERSION,):
            raise CatalogueError("serving work handoff version changed; run catalogue rebuild")
    if any("group_members" not in entry for entry in entries.values()):
        connection.execute("UPDATE serving_work_state SET complete=0 WHERE id=1")
    for symbol, entry in entries.items():
        prior = connection.execute("SELECT receipt FROM symbols WHERE symbol=?", (symbol,)).fetchone()
        if prior is None or json.loads(prior[0])["sha256"] != entry["sha256"]:
            connection.execute("INSERT OR REPLACE INTO serving_work VALUES(?, ?)", (symbol, entry["sha256"]))
            connection.execute("DELETE FROM serving_handoffs WHERE symbol=?", (symbol,))
        if handoff is not None:
            connection.execute(
                "INSERT OR REPLACE INTO serving_handoffs VALUES(?, ?, ?)",
                (symbol, entry["sha256"], json.dumps(handoff, sort_keys=True, separators=(",", ":"))),
            )


def upgrade(store):
    """Explicit maintenance converts the compact queue without changing obligations."""
    with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3")) as connection, connection:
        state = connection.execute("SELECT version FROM serving_work_state WHERE id=1").fetchone()
        if state is None or state[0] not in (1, WORK_VERSION):
            raise CatalogueError("serving work missing or incompatible; explicit bootstrap required")
        if state[0] == WORK_VERSION:
            return
        connection.execute(
            "CREATE TABLE IF NOT EXISTS serving_handoffs (symbol TEXT PRIMARY KEY, generation TEXT NOT NULL, body TEXT NOT NULL)"
        )
        connection.execute("UPDATE serving_work_state SET version=? WHERE id=1", (WORK_VERSION,))


def read_handoffs(store, targets):
    """Only exact selected obligations can carry an unchanged-decision guarantee."""
    result = {}
    with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3", readonly=True)) as connection:
        connection.execute("BEGIN")
        for symbol, generation in sorted(targets.items()):
            row = connection.execute(
                "SELECT body FROM serving_handoffs WHERE symbol=? AND generation=?", (symbol, generation)
            ).fetchone()
            if row is None:
                continue
            handoff = json.loads(row[0])
            if handoff.get("version") != 1:
                raise CatalogueError("incompatible unchanged-lineage handoff")
            result[tuple(handoff["lineage_after"])] = handoff
    return tuple(result.values())


def read(store):
    # Complete queue coverage is its own boundary guarantee. Missing coverage
    # requires explicit bootstrap; DO NOT infer an empty queue or inspect every
    # receipt to reconstruct ordinary maintenance work.
    try:
        with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3", readonly=True)) as connection:
            connection.execute("BEGIN")
            state = connection.execute("SELECT version, complete FROM serving_work_state WHERE id=1").fetchone()
            if state is None or state[0] != WORK_VERSION or not state[1]:
                raise CatalogueError(
                    "serving work coverage incomplete; run alpaca-cli-eod catalogue bootstrap --store PATH"
                )
            return dict(connection.execute("SELECT symbol, generation FROM serving_work ORDER BY symbol"))
    except sqlite3.Error as error:
        raise CatalogueError(
            "serving work missing or incompatible; run alpaca-cli-eod catalogue bootstrap --store PATH"
        ) from error


def seed(store, entries):
    """Explicit bootstrap only; caller owns the lineage worker and publication gate."""
    with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3")) as connection, connection:
        record_publication(connection, {}, empty=False)
        connection.executemany(
            "INSERT OR REPLACE INTO serving_work VALUES(?, ?)",
            [(name, entry["sha256"]) for name, entry in entries.items()],
        )
        connection.execute("UPDATE serving_work_state SET version=?, complete=1 WHERE id=1", (WORK_VERSION,))


def acknowledge(store, targets):
    """Only after catalogue commit; a newer lineage obligation cannot be consumed."""
    # Interruption before this transaction leaves replayable work. The next
    # maintainer trusts a matching committed catalogue and only acknowledges it;
    # DO NOT republish unchanged decisions to make bookkeeping look complete.
    with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3")) as connection, connection:
        connection.executemany(
            "DELETE FROM serving_work WHERE symbol=? AND generation=?", sorted(targets.items())
        )
        connection.executemany(
            "DELETE FROM serving_handoffs WHERE symbol=? AND generation=?", sorted(targets.items())
        )

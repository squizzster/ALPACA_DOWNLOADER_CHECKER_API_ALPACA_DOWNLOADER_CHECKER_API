"""Durable serving obligations in the transaction selecting lineage entries."""

import json
import sqlite3
from contextlib import closing

from libs.common.sqlite_connections import connect_sqlite

from .contract import CatalogueError

WORK_VERSION = 1


def record_publication(connection, entries, *, empty):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS serving_work (symbol TEXT PRIMARY KEY, generation TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS serving_work_state (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, complete INTEGER NOT NULL)"
    )
    connection.execute("INSERT OR IGNORE INTO serving_work_state VALUES(1, ?, ?)", (WORK_VERSION, int(empty)))
    if any("group_members" not in entry for entry in entries.values()):
        connection.execute("UPDATE serving_work_state SET complete=0 WHERE id=1")
    for symbol, entry in entries.items():
        prior = connection.execute("SELECT receipt FROM symbols WHERE symbol=?", (symbol,)).fetchone()
        if prior is None or json.loads(prior[0])["sha256"] != entry["sha256"]:
            connection.execute("INSERT OR REPLACE INTO serving_work VALUES(?, ?)", (symbol, entry["sha256"]))


def read(store):
    try:
        with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3", readonly=True)) as connection:
            connection.execute("BEGIN")
            state = connection.execute("SELECT version, complete FROM serving_work_state WHERE id=1").fetchone()
            if state is None or state[0] != WORK_VERSION or not state[1]:
                raise CatalogueError(
                    "serving work coverage incomplete; run alpaca-eod catalogue bootstrap --store PATH"
                )
            return dict(connection.execute("SELECT symbol, generation FROM serving_work ORDER BY symbol"))
    except sqlite3.Error as error:
        raise CatalogueError(
            "serving work missing or incompatible; run alpaca-eod catalogue bootstrap --store PATH"
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
    with closing(connect_sqlite(store / "end_of_day/lineage/index.sqlite3")) as connection, connection:
        connection.executemany(
            "DELETE FROM serving_work WHERE symbol=? AND generation=?", sorted(targets.items())
        )

"""Durable registration intentions and work, subordinate to producer selectors."""

import fcntl
import json
import os
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from libs.alpaca_recovery_registry import DEFAULT_REGISTRY, publish_union
from libs.common.debug_trace import traced, traced_context
from libs.common.sqlite_connections import connect_sqlite

from .contract import LineageError

STATE_VERSION = 1
STATE_FILE = "universe_updates.sqlite3"


def now_utc():
    return datetime.now(timezone.utc).isoformat()


@traced_context("universe_update_lock")
@contextmanager
def update_lock(store):
    with (Path(store) / ".universe-update.lock").open("a+b") as guard:
        fcntl.flock(guard, fcntl.LOCK_EX)
        yield


def _check(connection):
    if connection.execute("PRAGMA user_version").fetchone()[0] != STATE_VERSION:
        raise LineageError("incompatible universe work state; explicit universe rebuild is required")
    row = connection.execute("SELECT complete FROM state WHERE id=1").fetchone()
    if row != (1,):
        raise LineageError("incomplete universe work state; explicit universe rebuild is required")


@traced("universe_state_read")
def read_state(store):
    # TRUST BOUNDARY: this database owns intentions/retry outcomes, not market
    # data validity. A saved status cannot substitute for a producer selector.
    try:
        with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
            connection.execute("BEGIN")
            _check(connection)
            work = {
                symbol: json.loads(value)
                for symbol, value in connection.execute("SELECT symbol, body FROM work")
            }
            registration = [
                symbol
                for (symbol,) in connection.execute(
                    "SELECT symbol FROM registration WHERE complete=0 ORDER BY symbol"
                )
            ]
            last = connection.execute(
                "SELECT id, observed_at FROM observations ORDER BY id DESC LIMIT 1"
            ).fetchone()
            revision = connection.execute("SELECT revision FROM state WHERE id=1").fetchone()[0]
    except (sqlite3.Error, ValueError) as error:
        raise LineageError(
            "universe work state unavailable; run alpaca-cli-eod universe rebuild explicitly"
        ) from error
    return {
        "schema_version": STATE_VERSION,
        "revision": revision,
        "work": work,
        "registration_pending": registration,
        "last_catalogue_observation": None if last is None else {"id": last[0], "observed_at_utc": last[1]},
    }


def initialize(store):
    """Explicit reconstruction preserves existing intentions and retry outcomes."""
    path = Path(store) / STATE_FILE
    if path.exists():
        # Incompatible durable work must be examined, never silently discarded.
        return read_state(store)
    descriptor, temporary = tempfile.mkstemp(prefix=".universe-state-", dir=path.parent)
    os.close(descriptor)
    try:
        with closing(connect_sqlite(temporary)) as connection, connection:
            connection.execute("BEGIN")
            connection.execute("CREATE TABLE state (id INTEGER PRIMARY KEY, complete INTEGER, revision INTEGER)")
            connection.execute("CREATE TABLE observations (id INTEGER PRIMARY KEY, observed_at TEXT, body TEXT)")
            connection.execute("CREATE TABLE registration (symbol TEXT PRIMARY KEY, complete INTEGER NOT NULL)")
            connection.execute("CREATE TABLE work (symbol TEXT PRIMARY KEY, body TEXT NOT NULL)")
            connection.execute(f"PRAGMA user_version={STATE_VERSION}")
            connection.execute("INSERT INTO state VALUES (1, 1, 0)")
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return read_state(store)


@traced("work_accept")
def accept(store, observation, additions, work):
    # Persist the accepted obligation before delegated work can publish. A crash
    # may leave unfinished work, but must not lose the requirement or its group.
    """One transaction persists all intentions before registry or pricing writes."""
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        _check(connection)
        cursor = connection.execute(
            "INSERT INTO observations(observed_at, body) VALUES (?, ?)",
            (observation["catalogue_observed_at_utc"], json.dumps(observation)),
        )
        connection.executemany("INSERT OR IGNORE INTO registration VALUES (?, 0)", [(s,) for s in additions])
        for symbol, item in work.items():
            value = {**item, "observation_id": cursor.lastrowid}
            row = connection.execute("SELECT body FROM work WHERE symbol=?", (symbol,)).fetchone()
            previous = json.loads(row[0]) if row else {}
            value["obligation_observation_id"] = (
                previous.get("obligation_observation_id", previous.get("observation_id"))
                if previous.get("obligation_id") == item.get("obligation_id") and item.get("obligation_id")
                else cursor.lastrowid
            )
            connection.execute("INSERT OR REPLACE INTO work VALUES (?, ?)", (symbol, json.dumps(value)))
        connection.execute("UPDATE state SET revision=revision+1 WHERE id=1")
    return cursor.lastrowid


def put_work(store, symbol, item):
    """Persist work first discovered in maintenance or dependency processing."""
    return put_work_group(store, {symbol: item})[symbol]


def put_work_group(store, work):
    """Accept a complete group's changed requirements in one transaction."""
    accepted = {}
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        _check(connection)
        for symbol, item in work.items():
            row = connection.execute("SELECT body FROM work WHERE symbol=?", (symbol,)).fetchone()
            accepted[symbol] = {**(json.loads(row[0]) if row else {}), **item}
            connection.execute(
                "INSERT OR REPLACE INTO work VALUES (?, ?)", (symbol, json.dumps(accepted[symbol]))
            )
        connection.execute("UPDATE state SET revision=revision+1 WHERE id=1")
    return accepted


@traced("registry_register")
def register(store, labels=(), *, registry=DEFAULT_REGISTRY):
    # New dependency discovery must use exactly this interruption-safe route too.
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        _check(connection)
        connection.executemany("INSERT OR IGNORE INTO registration VALUES (?, 0)", [(s,) for s in labels])
        selected = [s for (s,) in connection.execute("SELECT symbol FROM registration WHERE complete=0")]
    result = publish_union(selected, registry)
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        connection.executemany("UPDATE registration SET complete=1 WHERE symbol=?", [(s,) for s in selected])
        connection.execute("UPDATE state SET revision=revision+1 WHERE id=1")
    return result


@traced("work_acknowledgement")
def record_outcome(store, symbol, *, status, outcome, attempts=0):
    # Workflow completion references already accepted publications. DO NOT mint
    # a new lineage/pricing generation just to make this acknowledgement durable;
    # its SQLite transaction owns that durability independently of data publication.
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        row = connection.execute("SELECT body FROM work WHERE symbol=?", (symbol,)).fetchone()
        if row is None:
            raise LineageError(f"unregistered update work: {symbol}")
        value = json.loads(row[0])
        value.update(
            status=status,
            outcome=outcome,
            attempts=value.get("attempts", 0) + attempts,
            finished_at_utc=now_utc(),
        )
        value["retry_after_utc"] = (
            (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat() if status == "retry" else None
        )
        connection.execute("UPDATE work SET body=? WHERE symbol=?", (json.dumps(value), symbol))
        connection.execute("UPDATE state SET revision=revision+1 WHERE id=1")
    return value


def pending_count(state):
    return sum(item["status"] != "complete" for item in state["work"].values())

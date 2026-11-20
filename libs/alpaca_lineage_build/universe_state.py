"""Durable registration intentions and work, subordinate to producer selectors."""

import fcntl
import json
import os
import sqlite3
import tempfile
import threading
from contextlib import closing, contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from pathlib import Path

from libs.alpaca_recovery_registry import DEFAULT_REGISTRY, publish_union
from libs.common.debug_trace import traced, traced_context
from libs.common.sqlite_connections import connect_sqlite

from .contract import LineageError

STATE_VERSION = 2
_COMPACTION_PENDING = -2
_CREATE_HEAD = "CREATE TABLE observation_head (id INTEGER PRIMARY KEY, observed_at TEXT NOT NULL)"
STATE_FILE = "universe_updates.sqlite3"


def _read_work(value):
    from libs.alpaca_pricing_download.correction import validate_durable_observations

    item = json.loads(value)
    if "dependency_suffixes" in item:
        from .dependency_suffixes import validate_dependency_suffixes

        validate_dependency_suffixes(item)
    # Completed requirements are a producer handoff, not new correction input.
    # Selecting their receipt bindings later must not replay numeric validation.
    contract = item.get("pricing_correction")
    completed = item.get("correction_completion") or {}
    accepted = completed and completed.get("correction_id") == item.get("correction_id")
    if item.get("status") != "complete" and not accepted and contract is not None:
        validate_durable_observations(contract)
    mapping = item.get("required_numerical_mapping")
    if mapping is not None:
        request = mapping.get("request") if isinstance(mapping, dict) else None
        need = request.get("numerical_mapping") if isinstance(request, dict) else None
        if (
            not isinstance(mapping, dict)
            or set(mapping) != {"version", "request"}
            or type(mapping.get("version")) is not int
            or mapping.get("version") != 1
            or not isinstance(need, dict)
            or set(need) != {"version", "candidate_id", "binding"}
            or type(need.get("version")) is not int
            or need.get("version") != 1
            or not isinstance(need.get("candidate_id"), str)
            or not isinstance(need.get("binding"), dict)
            or not isinstance(need["binding"].get("pricing_generations"), dict)
            or not isinstance(need["binding"].get("members"), list)
            or need["binding"].get("symbol") not in need["binding"]["members"]
            or request.get("mapping_asof") != need["binding"].get("asof")
        ):
            raise LineageError("invalid required numerical mapping; owning recovery required")
    return item


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
    _require_complete(connection)


def _require_complete(connection):
    row = connection.execute("SELECT complete FROM state WHERE id=1").fetchone()
    if row != (1,):
        raise LineageError("incomplete universe work state; explicit universe rebuild is required")


def require_initialized(store):
    """Check the operation gate without deserializing any accepted work bodies."""
    try:
        with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
            _check(connection)
    except sqlite3.Error as error:
        raise LineageError(
            "universe work state unavailable; run alpaca-cli-eod universe rebuild explicitly"
        ) from error


def count_pending(store):
    """Read current workflow status only, without revalidating correction evidence."""
    with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
        _check(connection)
        return _count_pending(connection)


def _count_pending(connection):
    return connection.execute(
        "SELECT count(*) FROM work WHERE json_extract(body, '$.status') IS NOT 'complete'"
    ).fetchone()[0]


def rebuild_summary(store):
    """Initialize or migrate the workflow and report outstanding counts under the caller's lock."""
    _initialize_schema(store)
    with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
        connection.execute("BEGIN")
        # The caller retains its lock across schema acceptance and these counts.
        return {
            "initialized": True,
            "schema_version": STATE_VERSION,
            "pending_work": _count_pending(connection),
            "registration_pending": connection.execute(
                "SELECT count(*) FROM registration WHERE complete=0"
            ).fetchone()[0],
        }


def pending_status(store):
    """Project workflow outcomes without reopening completed pricing evidence."""
    with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
        connection.execute("BEGIN")
        _check(connection)
        revision = connection.execute("SELECT revision FROM state WHERE id=1").fetchone()[0]
        work = {}
        for symbol, body in connection.execute(
            "SELECT symbol, body FROM work WHERE json_extract(body, '$.status') IS NOT 'complete'"
        ):
            value = json.loads(body)
            work[symbol] = {key: value.get(key) for key in ("status", "reasons", "outcome", "obligation_id")}
        return {"revision": revision, "work": work}


@traced("universe_state_read")
def read_state(store, *, symbols=None, pending_only=False):
    """Read global compact controls and only the explicitly selected work bodies.

    ``None`` retains the full-work boundary. A selected caller must supply its
    accepted complete group; the returned partial work map is not a universe
    census. SQL selects those primary keys before body decoding or ingress
    validation, without discovering dependencies from unselected work payloads.
    ``pending_only`` additionally consumes the stored completion status without
    reopening completed correction evidence at ingress.
    """
    # TRUST BOUNDARY: this database owns intentions/retry outcomes, not market
    # data validity. A saved status cannot substitute for a producer selector.
    selected = None
    if symbols is not None:
        if isinstance(symbols, (str, bytes)):
            raise ValueError("symbols must be a collection of symbol labels")
        selected = tuple(symbols)
        if any(not isinstance(symbol, str) or not symbol for symbol in selected):
            raise ValueError("symbols must contain nonempty symbol labels")
        selected = tuple(sorted(set(selected)))
    try:
        with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
            connection.execute("BEGIN")
            _check(connection)
            work = {}
            if selected is None:
                queries = [("SELECT symbol, body FROM work", ())]
            else:
                # Bounded parameter batches also support older SQLite builds;
                # every query remains an indexed selection of requested keys.
                queries = [
                    (
                        "SELECT symbol, body FROM work WHERE symbol IN ("
                        + ",".join("?" for _ in selected[offset : offset + 900])
                        + ")",
                        selected[offset : offset + 900],
                    )
                    for offset in range(0, len(selected), 900)
                ]
            for statement, parameters in queries:
                if pending_only:
                    statement += " WHERE " if selected is None else " AND "
                    statement += "json_extract(body, '$.status') IS NOT 'complete'"
                for symbol, value in connection.execute(statement, parameters):
                    work[symbol] = _read_work(value)
            registration = [
                symbol
                for (symbol,) in connection.execute(
                    "SELECT symbol FROM registration WHERE complete=0 ORDER BY symbol"
                )
            ]
            last = connection.execute("SELECT id, observed_at FROM observation_head").fetchone()
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


@traced("numerical_work_read")
def read_numerical_work(store):
    """Select pending numerical requirements before pricing can advance them.

    Other work bodies and their pricing observations are not numerical inputs.
    Leave them opaque; the normal planning boundary owns their later ingress.
    """
    try:
        with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
            connection.execute("BEGIN")
            _check(connection)
            revision = connection.execute("SELECT revision FROM state WHERE id=1").fetchone()[0]
            work = {
                symbol: json.loads(body)
                for symbol, body in connection.execute(
                    "SELECT symbol, body FROM work WHERE CASE WHEN json_valid(body) THEN "
                    "json_extract(body, '$.status') IS NOT 'complete' AND "
                    "json_type(body, '$.required_numerical_completion') NOT IN ('null') "
                    "ELSE 0 END"
                )
            }
            return {"revision": revision, "work": work}
    except (sqlite3.Error, ValueError, TypeError) as error:
        raise LineageError("cannot select pending numerical completion requirements") from error


@traced("mapping_work_read")
def read_mapping_work(store):
    """Select only pending producer-derived mapping requirements before pricing."""
    try:
        with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection, connection:
            connection.execute("BEGIN")
            _check(connection)
            revision = connection.execute("SELECT revision FROM state WHERE id=1").fetchone()[0]
            work = {
                symbol: _read_work(body)
                for symbol, body in connection.execute(
                    "SELECT symbol, body FROM work WHERE CASE WHEN json_valid(body) THEN "
                    "json_extract(body, '$.status') IS NOT 'complete' AND "
                    "json_type(body, '$.required_numerical_mapping') = 'object' ELSE 0 END"
                )
            }
            return {"revision": revision, "work": work}
    except (sqlite3.Error, ValueError, TypeError) as error:
        raise LineageError("cannot select pending numerical mapping requirements") from error


def _migrate(path):
    with closing(connect_sqlite(path)) as connection:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (1, _COMPACTION_PENDING, STATE_VERSION):
                raise LineageError("incompatible universe work state; existing state requires owning recovery")
            _require_complete(connection)
            if version == STATE_VERSION:
                return
            if version == 1:
                connection.execute(_CREATE_HEAD)
                connection.execute(
                    "INSERT INTO observation_head SELECT id, observed_at FROM observations "
                    "ORDER BY id DESC LIMIT 1"
                )
                # Freed pages are removed by VACUUM; avoid overwriting retired payloads first.
                connection.execute("PRAGMA secure_delete=OFF")
                connection.execute("DROP TABLE observations")
                connection.execute(f"PRAGMA user_version={_COMPACTION_PENDING}")
        # VACUUM owns its transaction. An interruption here leaves the migration
        # resumable without replaying the head selection or discarding any work.
        if connection.execute("PRAGMA freelist_count").fetchone()[0]:
            connection.execute("VACUUM")
        connection.execute(f"PRAGMA user_version={STATE_VERSION}")


def initialize(store):
    """Explicitly initialize or migrate, returning the caller's full workflow view."""
    _initialize_schema(store)
    return read_state(store)


def _initialize_schema(store):
    """Create an empty workflow or migrate its existing controls under the update lock."""
    path = Path(store) / STATE_FILE
    if path.exists():
        _migrate(path)
        return
    descriptor, temporary = tempfile.mkstemp(prefix=".universe-state-", dir=path.parent)
    os.close(descriptor)
    try:
        with closing(connect_sqlite(temporary)) as connection, connection:
            connection.execute("BEGIN")
            connection.execute("CREATE TABLE state (id INTEGER PRIMARY KEY, complete INTEGER, revision INTEGER)")
            connection.execute(_CREATE_HEAD)
            connection.execute("CREATE TABLE registration (symbol TEXT PRIMARY KEY, complete INTEGER NOT NULL)")
            connection.execute("CREATE TABLE work (symbol TEXT PRIMARY KEY, body TEXT NOT NULL)")
            connection.execute(f"PRAGMA user_version={STATE_VERSION}")
            connection.execute("INSERT INTO state VALUES (1, 1, 0)")
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


@traced("work_accept")
def accept(store, observation, additions, work, *, handoff=False):
    # Persist the accepted obligation before delegated work can publish. A crash
    # may leave unfinished work, but must not lose the requirement or its group.
    """One transaction persists all intentions before registry or pricing writes."""
    effective = {}
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        _check(connection)
        cursor = connection.execute(
            "INSERT INTO observation_head(observed_at) VALUES (?)", (observation["catalogue_observed_at_utc"],)
        )
        # Allocate the next rowid before retiring the previous head. Work keeps
        # its original observation IDs even when no work was selected on this run.
        connection.execute("DELETE FROM observation_head WHERE id < ?", (cursor.lastrowid,))
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
            effective[symbol] = value
            connection.execute("INSERT OR REPLACE INTO work VALUES (?, ?)", (symbol, json.dumps(value)))
        revision_value = _advance(connection, required=handoff)
    return (
        {"observation_id": cursor.lastrowid, "work": effective, "revision": revision_value}
        if handoff
        else cursor.lastrowid
    )


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
        _advance(connection)
    return accepted


def accept_dependency_suffixes(store, updates, *, batch=None):
    """Checkpoint the pricing owner's empty observations against existing unfinished work."""
    if batch is None:
        with outcome_batch(store) as batch:
            return accept_dependency_suffixes(store, updates, batch=batch)
    connection = batch.require_active(store)
    for owner, receipts in updates.items():
        obligation_id = next(iter(receipts.values()))["obligation_id"]
        changed = connection.execute(
            "UPDATE work SET body=json_patch(body, ?) WHERE symbol=? "
            "AND json_extract(body, '$.obligation_id')=? AND json_extract(body, '$.status')!='complete'",
            (json.dumps({"dependency_suffixes": receipts}), owner, obligation_id),
        ).rowcount
        if changed != 1:
            raise LineageError(f"{owner}: dependency suffix selection belongs to superseded work")


def accept_retired_uuid(store, symbol, obligation_id, observation):
    """Checkpoint the metadata owner's selected UUID before group discovery or publication."""
    with outcome_batch(store) as batch:
        connection = batch.require_active(store)
        changed = connection.execute(
            "UPDATE work SET body=json_set(body,'$.retired_uuid_observation',json(?)) WHERE symbol=? "
            "AND json_extract(body,'$.obligation_id')=? AND json_extract(body,'$.status')!='complete'",
            (json.dumps(observation), symbol, obligation_id),
        ).rowcount
        if changed != 1:
            raise LineageError(f"{symbol}: retired UUID observation belongs to superseded work")


@traced("correction_completion_accept")
def accept_correction_completion(store, symbol, expected, completed):
    """A producer outcome cannot acknowledge a concurrently renewed requirement."""
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        _check(connection)
        row = connection.execute("SELECT body FROM work WHERE symbol=?", (symbol,)).fetchone()
        current = json.loads(row[0]) if row else {}
        if (current.get("obligation_id"), current.get("correction_id")) != (
            expected.get("obligation_id"),
            completed["correction_id"],
        ):
            raise LineageError("correction completion does not match its current obligation")
        current["correction_completion"] = completed
        connection.execute("UPDATE work SET body=? WHERE symbol=?", (json.dumps(current), symbol))
        _advance(connection)
    return current


@traced("registry_register")
def register(store, labels=(), *, registry=DEFAULT_REGISTRY, handoff=False):
    # New dependency discovery must use exactly this interruption-safe route too.
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        _check(connection)
        connection.executemany("INSERT OR IGNORE INTO registration VALUES (?, 0)", [(s,) for s in labels])
        selected = [s for (s,) in connection.execute("SELECT symbol FROM registration WHERE complete=0")]
    result = publish_union(selected, registry)
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        connection.executemany("UPDATE registration SET complete=1 WHERE symbol=?", [(s,) for s in selected])
        revision_value = _advance(connection, required=handoff)
    return {**result, "revision": revision_value} if handoff else result


class _OutcomeBatch:
    def __init__(self, store, connection, work):
        self.store = Path(store).absolute()
        self.connection = connection
        self.owner = (os.getpid(), threading.get_ident())
        self.active = True
        self.work = work
        self.completed = set()
        self.released_suffixes = []

    def require_active(self, store):
        if (
            not self.active
            or self.store != Path(store).absolute()
            or self.owner != (os.getpid(), threading.get_ident())
        ):
            raise LineageError("work outcome batch belongs to another store/owner or is no longer active")
        return self.connection


@contextmanager
def outcome_batch(store, *, handoff=False, work=None):
    """Commit accepted outcomes together; interruption rolls back the whole batch."""
    transferred = {}
    with closing(connect_sqlite(Path(store) / STATE_FILE)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        _check(connection)
        batch = _OutcomeBatch(store, connection, work)
        before = connection.total_changes
        try:
            yield batch
            if batch.released_suffixes:
                from .dependency_suffixes import transfer_unfinished_suffixes

                if batch.work is None:
                    # Direct acknowledgements without the controller's held view
                    # select only the recorded group members of released receipts.
                    members = sorted(
                        {
                            member
                            for suffixes in batch.released_suffixes
                            for receipt in suffixes.values()
                            for member in receipt["group"]
                        }
                        - batch.completed
                    )
                    batch.work = {
                        member: _read_work(body)
                        for member, body in connection.execute(
                            "SELECT symbol, body FROM work WHERE symbol IN (SELECT value FROM json_each(?))",
                            (json.dumps(members),),
                        )
                    }
                transferred = transfer_unfinished_suffixes(store, batch)
            if connection.total_changes != before:
                batch.revision = _advance(connection, required=handoff)
        finally:
            batch.active = False
    for member, suffixes in transferred.items():
        item = batch.work[member]
        item["dependency_suffixes"] = {**item.get("dependency_suffixes", {}), **suffixes}


@traced("work_acknowledgement")
def record_outcome(store, symbol, *, status, outcome, attempts=0, batch=None, expected=None):
    # Workflow completion references already accepted publications. DO NOT mint
    # a new lineage/pricing generation just to make this acknowledgement durable;
    # its SQLite transaction owns that durability independently of data publication.
    if batch is None:
        with outcome_batch(store) as batch:
            return record_outcome(
                store,
                symbol,
                status=status,
                outcome=outcome,
                attempts=attempts,
                batch=batch,
                expected=expected,
            )
    connection = batch.require_active(store)
    row = connection.execute("SELECT body FROM work WHERE symbol=?", (symbol,)).fetchone()
    if row is None:
        raise LineageError(f"unregistered update work: {symbol}")
    value = json.loads(row[0])
    if expected is not None and value.get("obligation_id") != expected.get("obligation_id"):
        raise LineageError(f"work changed before acknowledgement: {symbol}")
    value.update(
        status=status,
        outcome=outcome,
        attempts=value.get("attempts", 0) + attempts,
        finished_at_utc=now_utc(),
    )
    if status == "complete":
        value.pop("retired_uuid_observation", None)
        released = value.pop("dependency_suffixes", {})
        batch.completed.add(symbol)
        if released:
            batch.released_suffixes.append(released)
    value["retry_after_utc"] = (
        (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat() if status == "retry" else None
    )
    connection.execute("UPDATE work SET body=? WHERE symbol=?", (json.dumps(value), symbol))
    return value


def pending_count(state):
    return sum(item["status"] != "complete" for item in state["work"].values())


def revision(store):
    """Read only the workflow selector token for applicability, never work bodies."""
    with closing(connect_sqlite(Path(store) / STATE_FILE, readonly=True)) as connection:
        return connection.execute("SELECT revision FROM state WHERE id=1").fetchone()[0]


_mutations = ContextVar("pipeline_workflow_mutations", default=None)


@contextmanager
def mutation_handoff():
    """Lend revisions returned by the active producer, without rereading state."""
    result = {}
    token = _mutations.set(result)
    try:
        yield result
    finally:
        _mutations.reset(token)


def _advance(connection, *, required=False):
    connection.execute("UPDATE state SET revision=revision+1 WHERE id=1")
    selected = _mutations.get()
    if selected is None and not required:
        return None
    # This is the producer's newly updated selector, never an accepted work-body
    # reread. Older supported SQLite versions do not implement UPDATE RETURNING.
    value = connection.execute("SELECT revision FROM state WHERE id=1").fetchone()[0]
    if selected is not None:
        selected["revision"] = value
    return value

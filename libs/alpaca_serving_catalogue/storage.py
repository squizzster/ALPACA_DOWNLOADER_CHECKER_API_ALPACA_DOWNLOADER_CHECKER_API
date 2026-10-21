"""Compact transactional selector, public projections and previous contributions."""

import json
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime, timezone
from uuid import uuid4

from libs.common.sqlite_connections import connect_sqlite

from .contract import SCHEMA_VERSION, CatalogueError, policy, validate_version


def path(store):
    return store / "end_of_day/catalogue/serving_catalogue.sqlite3"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _state(connection, *, incomplete=False, old_policy=False):
    if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
        raise CatalogueError("incompatible serving catalogue schema; explicit schema migration required")
    row = connection.execute("SELECT body FROM state WHERE id=1").fetchone()
    if row is None:
        raise CatalogueError("missing serving catalogue state; run alpaca-eod catalogue bootstrap --store PATH")
    state = json.loads(row[0])
    if (
        state.get("boundary") != "alpaca_standalone_serving_catalogue"
        or state.get("schema_version") != SCHEMA_VERSION
    ):
        raise CatalogueError("incompatible serving catalogue selector")
    if not isinstance(state.get("generation"), str) or not state["generation"] or not state.get("published_at"):
        raise CatalogueError("invalid serving catalogue selector")
    if not old_policy and state.get("policy") != policy():
        raise CatalogueError("serving policy changed; run alpaca-eod catalogue rebuild --store PATH")
    if not incomplete and state.get("complete") is not True:
        raise CatalogueError("serving catalogue incomplete; resume alpaca-eod catalogue bootstrap --store PATH")
    return state


def _write_state(connection, *, complete):
    state = {
        "boundary": "alpaca_standalone_serving_catalogue",
        "schema_version": SCHEMA_VERSION,
        "generation": uuid4().hex,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "policy": policy(),
        "complete": complete,
    }
    connection.execute("INSERT OR REPLACE INTO state VALUES(1, ?)", (encoded(state),))
    return state


def initialize(store, *, rebuild=False):
    """Explicit bootstrap/rebuild; group decisions survive interrupted attempts."""
    validate_version()
    destination = path(store)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with closing(connect_sqlite(destination, create=True)) as connection, connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise CatalogueError("unsupported catalogue schema; an explicit schema migration is required")
        connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS groups (group_key TEXT PRIMARY KEY, decision_generation TEXT NOT NULL, selection TEXT NOT NULL, body TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS members (symbol TEXT PRIMARY KEY, group_key TEXT NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS members_group ON members(group_key)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS securities (public_id TEXT PRIMARY KEY, group_key TEXT NOT NULL, body TEXT NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS securities_group ON securities(group_key)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS identities (public_id TEXT PRIMARY KEY, group_key TEXT NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS identities_group ON identities(group_key)")
        exists = connection.execute("SELECT 1 FROM state WHERE id=1").fetchone()
        if exists:
            state = _state(connection, incomplete=True, old_policy=rebuild)
            if not rebuild and state["complete"]:
                return state
        return _write_state(connection, complete=False)


def controls(store, *, incomplete=False, recover=False, old_policy=False):
    """One compact control-plane view; no ticker files or histories."""
    validate_version()
    try:
        # Only the explicit maintenance owner permits native SQLite rollback.
        # Public reads and previews retain strictly read-only connections.
        with closing(connect_sqlite(path(store), readonly=not recover)) as connection:
            connection.execute("BEGIN")
            state = _state(connection, incomplete=incomplete, old_policy=old_policy)
            groups = {
                key: json.loads(selection)
                for key, selection in connection.execute("SELECT group_key, selection FROM groups")
            }
            members = dict(connection.execute("SELECT symbol, group_key FROM members"))
            return {"state": state, "groups": groups, "members": members}
    except (sqlite3.Error, ValueError, TypeError, KeyError) as error:
        raise CatalogueError(
            "cannot read serving catalogue; run alpaca-eod catalogue bootstrap --store PATH"
        ) from error


def read_groups(store, keys, *, generation):
    """Read only selected decision bodies from an already accepted catalogue."""
    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            connection.execute("BEGIN")
            if _state(connection, incomplete=True)["generation"] != generation:
                raise CatalogueError("selected serving catalogue changed")
            groups = {}
            for key in sorted(keys):
                row = connection.execute("SELECT body FROM groups WHERE group_key=?", (key,)).fetchone()
                if row is None:
                    raise CatalogueError("selected serving group unavailable")
                groups[key] = json.loads(row[0])
            return groups
    except (sqlite3.Error, ValueError, TypeError, KeyError) as error:
        raise CatalogueError("cannot read selected serving decisions") from error


def selector_state(store, *, recover=False):
    """Compact publication token for a previously accepted in-memory view."""
    with closing(connect_sqlite(path(store), readonly=not recover)) as connection:
        return _state(connection)


def refresh_controls(store, selected=None, *, recover=False):
    """Retain an accepted catalogue view until its transaction token changes."""
    if (
        selected is not None
        and selector_state(store, recover=recover)["generation"] == selected["state"]["generation"]
    ):
        return selected
    return controls(store, recover=recover)


def snapshot(store):
    """Materialize public fields and selectors, then release the read transaction."""
    validate_version()
    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            connection.execute("BEGIN")
            state = _state(connection)
            records, groups = {}, {}
            for public_id, body, key in connection.execute(
                "SELECT public_id, body, group_key FROM securities ORDER BY public_id"
            ):
                records[public_id] = {"public": json.loads(body), "group_key": key}
            for key, token, references, locator in connection.execute(
                "SELECT group_key, decision_generation, json_extract(selection, '$.references'), "
                "json_extract(selection, '$.locator') FROM groups WHERE group_key IN (SELECT group_key FROM securities)"
            ):
                groups[key] = {
                    "decision_generation": token,
                    "references": json.loads(references),
                    "locator": locator,
                }
            return {"state": state, "records": records, "groups": groups}
    except (sqlite3.Error, ValueError, KeyError, TypeError) as error:
        raise CatalogueError(
            "cannot open serving catalogue; run alpaca-eod catalogue bootstrap --store PATH"
        ) from error


def assert_group(store, key, token):
    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            connection.execute("BEGIN")
            _state(connection)
            selected = connection.execute(
                "SELECT decision_generation FROM groups WHERE group_key=?", (key,)
            ).fetchone()
            if selected != (token,):
                raise CatalogueError("selected serving decision changed")
    except sqlite3.Error as error:
        raise CatalogueError("selected serving publication unavailable") from error


def commit(store, previous, groups):
    """Replace complete old contributions atomically. No public ID collision merges."""
    for group in groups:
        validate_group(group)
    with closing(connect_sqlite(path(store))) as connection, connection:
        state = _state(connection, incomplete=True)
        for key in sorted(previous):
            connection.execute("DELETE FROM securities WHERE group_key=?", (key,))
            connection.execute("DELETE FROM members WHERE group_key=?", (key,))
            connection.execute("DELETE FROM groups WHERE group_key=?", (key,))
            connection.execute("DELETE FROM identities WHERE group_key=?", (key,))
        try:
            for group in groups:
                key = group["locator"]
                selection = {
                    **{name: group[name] for name in ("locator", "policy", "references", "decision_generation")},
                    "online_identities": len(group["securities"]),
                    "ineligible_identities": sum(not d["eligible"] for d in group["decisions"]),
                }
                connection.execute(
                    "INSERT INTO groups VALUES(?, ?, ?, ?)",
                    (key, group["decision_generation"], encoded(selection), encoded(group)),
                )
                connection.executemany(
                    "INSERT INTO members VALUES(?, ?)", [(name, key) for name in group["references"]["lineage"]]
                )
                connection.executemany(
                    "INSERT INTO securities VALUES(?, ?, ?)",
                    [(record["public_id"], key, encoded(record)) for record in group["securities"]],
                )
                connection.executemany(
                    "INSERT INTO identities VALUES(?, ?)",
                    [(record["public_id"], key) for record in group["decisions"]],
                )
        except sqlite3.IntegrityError as error:
            raise CatalogueError(
                "conflicting public identity or group membership; no catalogue published"
            ) from error
        return _write_state(connection, complete=state["complete"])


def validate_group(group):
    """Validate only new producer projections, never selected upstream payloads."""
    from libs.alpaca_pricing_download.contract import ADJUSTMENTS

    from .reuse import validate_history_validation

    try:
        references = group["references"]
        if (
            group["policy"] != policy()
            or not group["decision_generation"]
            or group["locator"] != min(references["pricing"])
        ):
            raise ValueError("invalid decision header")
        if (
            set(references) != {"pricing", "lineage"}
            or not references["pricing"].keys() <= references["lineage"].keys()
        ):
            raise ValueError("incomplete group references")
        for kind, selected in references.items():
            for name, token in selected.items():
                if (
                    not name
                    or (token is None and kind != "lineage")
                    or (token is not None and re.fullmatch(r"[0-9a-f]{64}", token) is None)
                ):
                    raise ValueError("invalid generation reference")
        decisions = {d["public_id"]: d for d in group["decisions"]}
        securities = {s["public_id"]: s for s in group["securities"]}
        if len(decisions) != len(group["decisions"]) or len(securities) != len(group["securities"]):
            raise ValueError("duplicate identity")
        if set(securities) != {key for key, value in decisions.items() if value["eligible"]}:
            raise ValueError("public projection differs from serving decisions")
        for public_id, decision in decisions.items():
            if (
                re.fullmatch(r"[0-9a-f]{64}", public_id) is None
                or type(decision["eligible"]) is not bool
                or bool(decision["reasons"]) == decision["eligible"]
            ):
                raise ValueError("invalid eligibility decision")
        for record in securities.values():
            if (
                record["adjustments"] != list(ADJUSTMENTS)
                or not record["tickers"]
                or record["first_session"] > record["last_session"]
                or record["coverage"]["session_count"] < 1
            ):
                raise ValueError("invalid public coverage")
            first, last = date.fromisoformat(record["first_session"]), date.fromisoformat(record["last_session"])
            if record["coverage"]["meaning"] != "approved_stored_history":
                raise ValueError("unknown coverage meaning")
            if any(
                not first < date.fromisoformat(day) < last
                for day in record["coverage"]["explained_missing_sessions"]
            ):
                raise ValueError("explained session outside coverage")
            for interval in record["ticker_ranges"]:
                if (
                    interval["ticker"] not in record["tickers"]
                    or not record["first_session"]
                    <= interval["first_session"]
                    <= interval["last_session"]
                    <= record["last_session"]
                ):
                    raise ValueError("invalid dated ticker projection")
            for interval in record["ticker_ownership_intervals"]:
                start = date.fromisoformat(interval["start_inclusive"]) if interval["start_inclusive"] else None
                end = date.fromisoformat(interval["end_exclusive"]) if interval["end_exclusive"] else None
                if not interval["ticker"] or (start is not None and end is not None and start >= end):
                    raise ValueError("invalid published ownership interval projection")
            for observation in record["observations"]:
                if (
                    observation["field"] not in {"name", "cusip", "exchange", "asset_class"}
                    or not observation["value"]
                ):
                    raise ValueError("invalid metadata projection")
                if (
                    observation["observed_at"] is not None
                    and datetime.fromisoformat(observation["observed_at"]).tzinfo is None
                ):
                    raise ValueError("undated observation timestamp")
                start, end = observation["effective_from"], observation["effective_through"]
                if (start is None) != (end is None) or (
                    start is not None
                    and not first <= date.fromisoformat(start) <= date.fromisoformat(end) <= last
                ):
                    raise ValueError("invalid historical metadata interval")
        validate_history_validation(group)
        encoded(group)
    except (KeyError, TypeError, ValueError) as error:
        raise CatalogueError(f"invalid new serving decision: {error}") from error


def finish_bootstrap(store):
    with closing(connect_sqlite(path(store))) as connection, connection:
        state = _state(connection, incomplete=True)
        return state if state["complete"] else _write_state(connection, complete=True)

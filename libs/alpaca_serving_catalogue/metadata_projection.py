"""Identity-scoped metadata published by serving, alongside its decision in SQLite."""

import json
import sqlite3
from collections import defaultdict
from collections.abc import Mapping
from contextlib import closing, contextmanager, nullcontext
from pathlib import Path

from libs.alpaca_lineage_build.storage import selected_metadata_receipts
from libs.common.sqlite_connections import connect_sqlite
from libs.common.store_lock import exclusive_owned, operation_lock

from . import export_inputs, storage
from .contract import CatalogueError

VERSION = 2
OLDEST_READABLE_VERSION = 1


def initialize(connection):
    connection.execute(
        "CREATE TABLE IF NOT EXISTS metadata_evidence (group_key TEXT PRIMARY KEY, version INTEGER NOT NULL, "
        "decision_generation TEXT NOT NULL, lineage TEXT NOT NULL, body TEXT NOT NULL)"
    )


def install(connection, key, group, facts):
    """Commit attribution with the selected group, including a completed empty result."""
    row = (
        key,
        VERSION,
        group["decision_generation"],
        storage.encoded(group["references"]["lineage"]),
        storage.encoded(facts),
    )
    sql = "INSERT OR REPLACE INTO metadata_evidence VALUES(?,?,?,?,?)"
    try:
        connection.execute(sql, row)
    except sqlite3.OperationalError as error:
        if "no such table" not in str(error):
            raise
        initialize(connection)
        connection.execute(sql, row)


def _group_selectors(db, keys=None, *, all_online=False):
    # The securities table contains online public IDs only. Its covering group
    # index supplies the universe without reading or decoding security bodies.
    if all_online:
        source = "groups NOT INDEXED WHERE group_key IN (SELECT DISTINCT group_key FROM securities)"
        args = ()
    else:
        source = "groups WHERE group_key IN (SELECT value FROM json_each(?))"
        args = (json.dumps(list(keys)),)
    return {
        key: {"decision_generation": token, "references": {"lineage": json.loads(lineage)}}
        for key, token, lineage in db.execute(
            "SELECT group_key, decision_generation, json_extract(selection,'$.references.lineage') FROM "
            + source,
            args,
        )
    }


def _selected(db, groups, *, bodies, all_online, versions=None):
    if not groups:
        return {}
    if not isinstance(groups, Mapping):
        groups = _group_selectors(db, groups)
    # The opened catalogue already owns these exact generation bindings. Passing
    # the held selectors avoids extracting and decoding the same group row again.
    bindings = [
        [key, group["decision_generation"], group["references"]["lineage"]] for key, group in groups.items()
    ]
    column = "m.body" if bodies else "m.group_key"
    if all_online:
        # Visit table pages in row order on a universe export. A partial or obsolete
        # projection fails the binding predicate before SQLite returns its body.
        source = """metadata_evidence AS m NOT INDEXED WHERE m.version BETWEEN ? AND ? AND (m.group_key,m.decision_generation,m.lineage)
                    IN (SELECT json_extract(value,'$[0]'),json_extract(value,'$[1]'),json_extract(value,'$[2]') FROM json_each(?))"""
        args = (OLDEST_READABLE_VERSION, VERSION, storage.encoded(bindings))
    else:
        source = """json_each(?) AS t CROSS JOIN metadata_evidence m ON m.group_key=json_extract(t.value,'$[0]')
                    WHERE m.version BETWEEN ? AND ? AND m.decision_generation=json_extract(t.value,'$[1]') AND m.lineage=json_extract(t.value,'$[2]')"""
        args = (storage.encoded(bindings), OLDEST_READABLE_VERSION, VERSION)
    try:
        rows = db.execute(f"SELECT m.group_key,m.version,{column} FROM " + source, args)
        selected = {}
        for key, version, value in rows:
            selected[key] = json.loads(value) if bodies else True
            if versions is not None:
                versions.add(version)
        return selected
    except sqlite3.OperationalError as error:
        if "no such table: metadata_evidence" not in str(error):
            raise
        return {}


def selected(store, groups, *, bodies, all_online=False, versions=None):
    """Consume applicable prepared rows using the held catalogue selectors."""
    if not groups:
        return {}
    with closing(connect_sqlite(storage.path(store), readonly=True)) as db:
        return _selected(db, groups, bodies=bodies, all_online=all_online, versions=versions)


def require(store, groups, *, all_online=False, versions=None):
    results = selected(store, groups, bodies=True, all_online=all_online, versions=versions)
    if results.keys() != set(groups):
        raise CatalogueError(
            "selected metadata projection incomplete; run alpaca-cli-eod catalogue prepare-metadata for the selected groups"
        )
    return {pid: item for group in results.values() for pid, item in group.items()}


def _prepare_groups(db, public_ids, symbols):
    from alpaca_end_of_day import PublicIdUnavailableError, _ticker_label
    from alpaca_end_of_day.standalone import _public_id

    if public_ids is not None and symbols is not None:
        raise ValueError("choose only one metadata selector")
    if public_ids is None and symbols is None:
        return _group_selectors(db, all_online=True)
    if public_ids is not None:
        if not isinstance(public_ids, (list, tuple)) or not public_ids:
            raise ValueError("public_ids must be a nonempty sequence")
        requested = {_public_id(pid) for pid in public_ids}
        pairs = db.execute(
            "SELECT public_id, group_key FROM securities WHERE public_id IN (SELECT value FROM json_each(?))",
            (json.dumps(sorted(requested)),),
        )
    else:
        if not isinstance(symbols, (list, tuple)) or not symbols:
            raise ValueError("symbols must be a nonempty sequence")
        requested = {_ticker_label(label) for label in symbols}
        # Historical labels need not occur in the dependency roster. Select every
        # group with a matching approved trading range, including reused tickers.
        pairs = db.execute(
            """SELECT DISTINCT json_extract(r.value,'$.ticker'), s.group_key
               FROM securities s, json_each(s.body,'$.ticker_ranges') r
               WHERE json_extract(r.value,'$.ticker') IN (SELECT value FROM json_each(?))""",
            (json.dumps(sorted(requested)),),
        )
    found, keys = set(), set()
    for identifier, key in pairs:
        found.add(identifier)
        keys.add(key)
    if found != requested:
        if public_ids is not None:
            raise PublicIdUnavailableError("public ID is not available")
        raise ValueError("ticker is not available")
    return _group_selectors(db, keys)


def _online_assignment_fields(db, missing, *, all_online):
    # Attribution consumes just the two public assignment fields for members of
    # the missing groups. Do not deserialize descriptive and coverage metadata.
    source = (
        "securities NOT INDEXED"
        if all_online
        else "securities WHERE group_key IN (SELECT value FROM json_each(?))"
    )
    args = () if all_online else (json.dumps(missing),)
    records = defaultdict(dict)
    for key, pid, ranges, intervals in db.execute(
        """SELECT group_key, public_id, json_extract(body,'$.ticker_ranges'),
                  json_extract(body,'$.ticker_ownership_intervals') FROM """
        + source,
        args,
    ):
        records[key][pid] = {
            "ticker_ranges": json.loads(ranges),
            "ticker_ownership_intervals": json.loads(intervals),
        }
    return records


@contextmanager
def _catalogue_errors():
    try:
        yield
    except (sqlite3.Error, json.JSONDecodeError) as error:
        raise CatalogueError(
            "cannot access serving metadata selection; finish owning catalogue maintenance"
        ) from error


def prepare(store, *, public_ids=None, symbols=None, plan=False, progress=None):
    """Explicit legacy migration. Commit each missing group once and resume at its compact selector."""
    from .metadata_evidence import project_group_evidence

    store = Path(store).absolute()
    gate = nullcontext() if exclusive_owned(store) else operation_lock(store, exclusive=not plan)
    all_online = public_ids is None and symbols is None
    with gate, _catalogue_errors():
        storage.validate_version()
        with closing(connect_sqlite(storage.path(store), readonly=True)) as db:
            db.execute("BEGIN")
            state = storage._state(db)
            groups = _prepare_groups(db, public_ids, symbols)
            available = _selected(db, groups, bodies=False, all_online=all_online)
            missing = sorted(groups.keys() - available.keys())
            refs = {key: groups[key]["references"]["lineage"] for key in missing}
            report = {
                "catalogue_generation": state["generation"],
                "selected_groups": len(groups),
                "pending_groups": len(missing),
                "selected_receipts": sum(token is not None for ref in refs.values() for token in ref.values()),
                "prepared_groups": 0,
                "receipt_reads": 0,
                "pricing_reads": 0,
                "provider_requests": 0,
            }
            if plan or not missing:
                return {**report, "plan": plan, "complete": True}
            references = {symbol: token for ref in refs.values() for symbol, token in ref.items()}
            export_inputs.select_held(db, references)
            records = _online_assignment_fields(db, missing, all_online=all_online and not available)
        root = store / "end_of_day/lineage"
        with (
            selected_metadata_receipts(root, references) as read,
            closing(connect_sqlite(storage.path(store), timeout=storage.WRITE_TIMEOUT_SECONDS)) as db,
        ):
            for key in missing:
                group = groups[key]
                receipts = {symbol: read(symbol) for symbol, token in refs[key].items() if token is not None}
                facts = project_group_evidence(records[key], receipts)
                with db:
                    install(db, key, group, facts)
                report["prepared_groups"] += 1
                report["receipt_reads"] += len(receipts)
                if progress:
                    progress(
                        "serving_catalogue", {"processed": report["prepared_groups"], "selected": len(missing)}
                    )
        return {**report, "plan": False, "complete": True}

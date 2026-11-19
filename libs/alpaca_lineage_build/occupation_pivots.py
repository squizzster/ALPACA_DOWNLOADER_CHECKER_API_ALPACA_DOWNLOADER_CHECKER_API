"""Index only the declared earlier occupations selected by their producing lineage journal."""

import json
from contextlib import closing

from libs.common.sqlite_connections import connect_sqlite

from .contract import RecoveryRequired

TABLE = "prior_trading_occupations"


def record(connection, entries):
    changed = {s: e for s, e in entries.items() if "dated_occupation_pivots" in e}
    if not changed:
        return
    connection.execute("""CREATE TABLE IF NOT EXISTS prior_trading_occupations(
                    locator TEXT NOT NULL,owner TEXT NOT NULL,day TEXT NOT NULL,token TEXT NOT NULL,
                    PRIMARY KEY(locator,owner))""")
    connection.execute("CREATE INDEX IF NOT EXISTS prior_trading_date ON prior_trading_occupations(day)")
    for locator, entry in changed.items():
        desired = entry["dated_occupation_pivots"]
        if not desired:
            connection.execute("DELETE FROM prior_trading_occupations WHERE locator=?", (locator,))
            continue
        slots = ",".join("?" for _ in desired)
        connection.execute(
            f"DELETE FROM prior_trading_occupations WHERE locator=? AND owner NOT IN ({slots})",
            (locator, *desired),
        )
        connection.executemany(
            """INSERT INTO prior_trading_occupations VALUES(?,?,?,?)
               ON CONFLICT(locator,owner) DO UPDATE SET day=excluded.day,token=excluded.token
               WHERE day!=excluded.day OR token!=excluded.token""",
            [(locator, pid, day, entry["dated_occupation_tokens"][pid]) for pid, day in desired.items()],
        )


def select(store, day):
    """Return compact selected candidates only. The producer's first publication creates this index."""
    from . import storage

    root = store / "end_of_day/lineage"
    with (
        storage.publication_lock(root),
        closing(connect_sqlite(storage.path_at(root, "index.sqlite3"), readonly=True)) as db,
    ):
        db.execute("BEGIN")
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone():
            return {}
        rows = db.execute(
            """SELECT p.locator,p.owner,p.token,s.receipt FROM prior_trading_occupations p
            JOIN symbols s ON s.symbol=p.locator WHERE p.day=? ORDER BY p.locator,p.owner""",
            (day,),
        )
        return {token: (locator, pid, json.loads(entry)) for locator, pid, token, entry in rows}


def selected_descriptor(store, locator, pid, token, entry):
    from . import storage

    root = store / "end_of_day/lineage"
    with storage.publication_lock(root):
        if storage.path_at(root, ".pending.json").exists():
            raise RecoveryRequired("interrupted lineage publication requires owning recovery")
        receipt = storage.read_receipt(root, locator, {"symbols": {locator: entry}})
    item = receipt.get("dated_occupations", {}).get(pid)
    if not item or item["token"] != token:
        raise RecoveryRequired("selected occupation certificate is unavailable")
    return item

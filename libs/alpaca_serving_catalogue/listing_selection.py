"""Select published trading ranges, competing owners, and applicable listing facts in one snapshot."""

import json
from collections import defaultdict
from contextlib import closing

from libs.common.sqlite_connections import connect_sqlite

from . import export_inputs, metadata_projection, storage
from .contract import CatalogueError


def select(store, symbols, public_ids, all_published, *, counts=None):
    from alpaca_end_of_day import _ticker_label
    from alpaca_end_of_day.standalone import _public_id

    if sum(map(bool, (symbols, public_ids, all_published))) != 1:
        raise ValueError("select symbols, public IDs or all published histories")
    with closing(connect_sqlite(storage.path(store), readonly=True)) as db:
        db.execute("BEGIN")
        state = storage._state(db)
        if symbols:
            requested = {_ticker_label(symbol) for symbol in symbols}
            args = (json.dumps(sorted(requested)),)
            rows = db.execute(
                """WITH requested(label) AS (SELECT value FROM json_each(?))
                   SELECT symbol,group_key FROM members JOIN requested ON symbol=label
                   WHERE EXISTS (SELECT 1 FROM securities s WHERE s.group_key=members.group_key)
                   UNION SELECT r.value->>'ticker',s.group_key FROM securities s,
                   json_each(s.body,'$.ticker_ranges') r JOIN requested ON r.value->>'ticker'=label""",
                args,
            )
            found = list(rows)
            missing = requested.difference(label for label, _ in found)
            if missing:
                raise ValueError(f"selected labels with no online trading group: {sorted(missing)}")
            keys = {key for _, key in found}
        elif public_ids:
            requested = {_public_id(pid) for pid in public_ids}
            found = dict(
                db.execute(
                    "SELECT public_id,group_key FROM securities WHERE public_id IN (SELECT value FROM json_each(?))",
                    (json.dumps(sorted(requested)),),
                )
            )
            if set(found) != requested:
                raise ValueError("one or more selected public IDs have no online trading range")
            keys = set(found.values())
        else:
            keys = None
        scope = "SELECT DISTINCT group_key FROM securities" if keys is None else "SELECT value FROM json_each(?)"
        args = () if keys is None else (json.dumps(sorted(keys)),)
        groups = {
            key: {
                "group_key": key,
                "decision_generation": token,
                "references": json.loads(refs),
                "ticker_ranges": {},
            }
            for key, token, refs in db.execute(
                f"SELECT group_key,decision_generation,json_extract(selection,'$.references') FROM groups WHERE group_key IN ({scope})",
                args,
            )
        }
        owners = defaultdict(list)
        for key, pid, ranges in db.execute(
            f"SELECT group_key,public_id,json_extract(body,'$.ticker_ranges') FROM securities WHERE group_key IN ({scope})",
            args,
        ):
            parsed = json.loads(ranges)
            groups[key]["ticker_ranges"][pid] = parsed
            for r in parsed:
                owners[r["ticker"]].append((pid, r["first_session"], r["last_session"]))
        if keys and owners:
            for pid, symbol, first, last in db.execute(
                """SELECT s.public_id,r.value->>'ticker',r.value->>'first_session',r.value->>'last_session'
                   FROM securities s,json_each(s.body,'$.ticker_ranges') r
                   WHERE s.group_key NOT IN (SELECT value FROM json_each(?))
                   AND r.value->>'ticker' IN (SELECT value FROM json_each(?))""",
                (json.dumps(sorted(keys)), json.dumps(sorted(owners))),
            ):
                owners[symbol].append((pid, first, last))
        # Published metadata is already attributed to each ID. It supplies completed assessments and
        # reusable literal trade observations without reopening source receipts.
        facts = metadata_projection._selected(
            db, groups, bodies=True, all_online=all_published, listing_fields=True
        )
        if set(facts) != {key for key, g in groups.items() if g["ticker_ranges"]}:
            raise CatalogueError("incomplete selected metadata; run catalogue prepare-metadata for these groups")
        if groups:
            export_inputs.select_held(
                db, {s for group in groups.values() for s in group["references"]["lineage"]}
            )
        from libs.alpaca_lineage_build.historical_listing_publication import bind_catalogue

        if all_published and counts is not None:
            online = sum(len(group["ticker_ranges"]) for group in groups.values())
            counts["catalogue_ids_without_online_range"] = (
                db.execute("SELECT COUNT(*) FROM identities").fetchone()[0] - online
            )
        return bind_catalogue(store, state, groups, owners), groups, owners, facts

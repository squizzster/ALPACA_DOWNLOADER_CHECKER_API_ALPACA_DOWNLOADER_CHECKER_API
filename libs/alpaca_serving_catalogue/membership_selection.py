"""Return dated label overlap from selected published histories for legacy lineage scheduling."""

import json
from collections import defaultdict
from contextlib import closing

from libs.common.sqlite_connections import connect_sqlite

from .contract import CatalogueError
from .storage import path


def overlapping_labels(records):
    """Two public IDs shared a dated trading label. This selects an ownership assessment."""
    labels = defaultdict(list)
    for public_id, ranges in records:
        for item in ranges:
            labels[item["ticker"]].append((item["first_session"], item["last_session"], public_id))
    for intervals in labels.values():
        last, owner = "", None
        for start, end, public_id in sorted(intervals):
            if start <= last and public_id != owner:
                return True
            if end > last:
                last, owner = end, public_id
    return False


def read_legacy_overlaps(store, groups):
    """Select dated metadata only for the unassessed, compatible multi-identity groups.

    The public securities table holds online metadata. Offline validated histories can also
    carry accepted ranges in their serving checkpoints. A conflict with no completed history
    has no dated range to offer. An unrelated group commit leaves these selections applicable.
    """
    keys = json.dumps(sorted(groups))
    ranges = {key: {} for key in groups}
    with closing(connect_sqlite(path(store), readonly=True)) as db:
        db.execute("BEGIN")
        selected = dict(
            db.execute(
                "SELECT group_key, decision_generation FROM groups WHERE group_key IN (SELECT value FROM json_each(?))",
                (keys,),
            )
        )
        if selected != {key: item["decision_generation"] for key, item in groups.items()}:
            raise CatalogueError("legacy membership selection changed; repeat the affected selection")
        for key, public_id, encoded in db.execute(
            "SELECT group_key, public_id, json_extract(body, '$.ticker_ranges') FROM securities "
            "WHERE group_key IN (SELECT value FROM json_each(?))",
            (keys,),
        ):
            ranges[key][public_id] = json.loads(encoded)
        offline = [key for key in groups if groups[key]["ineligible_identities"]]
        if offline:
            for key, public_id, encoded in db.execute(
                "SELECT g.group_key, r.value->>'$.public_id', r.value->'$.ticker_ranges' "
                "FROM groups g, json_each(g.body, '$.history_validation.prefix_records') r "
                "WHERE g.group_key IN (SELECT value FROM json_each(?)) "
                "AND NOT EXISTS (SELECT 1 FROM securities s WHERE s.public_id=r.value->>'$.public_id')",
                (json.dumps(offline),),
            ):
                if encoded is not None:
                    ranges[key][public_id] = json.loads(encoded)
    return {key: overlapping_labels(items.items()) for key, items in ranges.items()}

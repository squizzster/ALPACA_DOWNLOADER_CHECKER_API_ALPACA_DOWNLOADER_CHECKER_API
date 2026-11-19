"""Select legacy negative serving decisions that already support a later dated range."""

import json
from contextlib import closing

from alpaca_end_of_day._history_coverage import coverage_sources
from libs.common.sqlite_connections import connect_sqlite

from . import gap_suffix, storage
from .contract import CatalogueError, policy


def select(store, controls, *, publish=False):
    """Read only selected checkpoint fields once. Persist outstanding starts with their decision."""
    state = controls["state"]
    if "gap_suffix_reviews" in state:
        return state["gap_suffix_reviews"]
    current_policy = policy()
    candidates = {}
    for key, group in controls["groups"].items():
        if not group["ineligible_identities"] or group["policy"] != current_policy:
            continue
        repairs = group.get("late_bar_repairs", {})
        if repairs.get("version") != 2:
            raise CatalogueError(
                "late-bar selector missing or incompatible; run alpaca-cli-eod catalogue rebuild --store PATH"
            )
        if repairs["selected_identities"]:
            candidates[key] = {source: set(item["sessions"]) for source, item in repairs["sources"].items()}
    if not candidates and not publish:
        return {}
    reviews = {}
    with closing(connect_sqlite(storage.path(store), readonly=not publish)) as db, db:
        db.execute("BEGIN IMMEDIATE" if publish else "BEGIN")
        if storage._state(db, incomplete=True)["generation"] != state["generation"]:
            raise CatalogueError("serving catalogue changed before selecting legacy missing-session ranges")
        if candidates:
            rows = db.execute(
                "SELECT g.group_key, r.value->>'$.public_id', d.value, "
                "json_object('last_session', r.value->>'$.last_session', 'missing_sessions', r.value->'$.missing_sessions', "
                "'gap_bars', r.value->'$.gap_bars', 'ticker_ranges', r.value->'$.ticker_ranges'), "
                "json_extract(g.body, '$.history_validation.identities.' || (r.value->>'$.public_id')) "
                "FROM groups g, json_each(g.body, '$.decisions') d, json_each(g.body, '$.history_validation.prefix_records') r "
                "WHERE g.group_key IN (SELECT value FROM json_each(?)) "
                "AND g.body->>'$.history_validation.gap_suffix_version' IS NULL "
                "AND d.value->'$.reasons'='[\"unexplained_missing_sessions\"]' "
                "AND r.value->>'$.public_id'=d.value->>'$.public_id' AND r.value->'$.price_reasons'='[]'",
                (json.dumps(list(candidates)),),
            )
            for key, public_id, decision, record, coverage in rows:
                sources = {name for view in json.loads(coverage).values() for name in coverage_sources(view)}
                selected = candidates[key]
                if not sources or not sources <= selected.keys():
                    continue
                absent = set.intersection(*(selected[name] for name in sources))
                start = gap_suffix.legacy_start(json.loads(record), json.loads(decision), absent_sessions=absent)
                if start is not None:
                    reviews.setdefault(key, {})[public_id] = start
        if publish:
            updated = storage._write_state(
                db, complete=state["complete"], previous={**state, "gap_suffix_reviews": reviews}
            )
    if publish:
        from libs.alpaca_lineage_build.recovery_handoffs import serving_published

        controls["state"] = updated
        serving_published(store, state["generation"], updated["generation"], set())
    return reviews

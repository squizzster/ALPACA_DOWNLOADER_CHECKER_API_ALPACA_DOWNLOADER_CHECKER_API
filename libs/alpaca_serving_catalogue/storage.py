"""Compact transactional selector, public projections and previous contributions."""

import json
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime, timezone
from uuid import uuid4

from libs.common.debug_trace import traced
from libs.common.sqlite_connections import connect_sqlite

from .contract import SCHEMA_VERSION, CatalogueError, policy, validate_version
from .gaps import PROJECTION_VERSION, identity_gaps

WRITE_TIMEOUT_SECONDS = 120


def path(store):
    return store / "end_of_day/catalogue/serving_catalogue.sqlite3"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _state(connection, *, incomplete=False, old_policy=False):
    # Validate this selector's own contract, not its producer's inputs. A missing
    # or incompatible boundary fails into explicit maintenance; DO NOT rebuild
    # it by crawling lineage receipts from a reader or a planning call.
    if connection.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
        raise CatalogueError("incompatible serving catalogue schema; explicit schema migration required")
    row = connection.execute("SELECT body FROM state WHERE id=1").fetchone()
    if row is None:
        raise CatalogueError(
            "missing serving catalogue state; run alpaca-cli-eod catalogue bootstrap --store PATH"
        )
    state = json.loads(row[0])
    if (
        state.get("boundary") != "alpaca_standalone_serving_catalogue"
        or state.get("schema_version") != SCHEMA_VERSION
    ):
        raise CatalogueError("incompatible serving catalogue selector")
    if not isinstance(state.get("generation"), str) or not state["generation"] or not state.get("published_at"):
        raise CatalogueError("invalid serving catalogue selector")
    if not old_policy and state.get("policy") != policy():
        raise CatalogueError("serving policy changed; run alpaca-cli-eod catalogue rebuild --store PATH")
    if not incomplete and state.get("complete") is not True:
        raise CatalogueError(
            "serving catalogue incomplete; resume alpaca-cli-eod catalogue bootstrap --store PATH"
        )
    return state


def _write_state(connection, *, complete, previous=None):
    state = {
        "boundary": "alpaca_standalone_serving_catalogue",
        "schema_version": SCHEMA_VERSION,
        "generation": uuid4().hex,
        "published_at": datetime.now(timezone.utc).isoformat(),
        "policy": policy(),
        "complete": complete,
    }
    if previous is None:
        state["gap_suffix_reviews"] = {}
    elif "gap_suffix_reviews" in previous:
        state["gap_suffix_reviews"] = previous["gap_suffix_reviews"]
    connection.execute("INSERT OR REPLACE INTO state VALUES(1, ?)", (encoded(state),))
    return state


def initialize(store, *, rebuild=False):
    """Explicit bootstrap/rebuild; group decisions survive interrupted attempts."""
    validate_version()
    destination = path(store)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with (
        closing(connect_sqlite(destination, create=True, timeout=WRITE_TIMEOUT_SECONDS)) as connection,
        connection,
    ):
        from . import export_inputs

        export_inputs.initialize(connection)
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
        state = None
        if exists:
            state = _state(connection, incomplete=True, old_policy=rebuild)
            if not rebuild and state["complete"]:
                return state
        return _write_state(connection, complete=False, previous=state)


@traced("serving_controls_read")
def controls(store, *, incomplete=False, recover=False, old_policy=False, symbols=None):
    """One compact control-plane view; no ticker files or histories."""
    validate_version()
    try:
        # Only the explicit maintenance owner permits native SQLite rollback.
        # Public reads and previews retain strictly read-only connections.
        with closing(connect_sqlite(path(store), readonly=not recover)) as connection:
            connection.execute("BEGIN")
            state = _state(connection, incomplete=incomplete, old_policy=old_policy)
            if symbols is not None:
                # Each selected catalogue group already carries its complete membership.
                # Read the requested members through their index, then keep that whole group.
                places = ",".join("?" for _ in symbols)
                groups = {
                    key: json.loads(selection)
                    for key, selection in connection.execute(
                        "SELECT group_key, selection FROM groups WHERE group_key IN "
                        f"(SELECT group_key FROM members WHERE symbol IN ({places}))",
                        tuple(symbols),
                    )
                }
                members = {s: key for key, group in groups.items() for s in group["references"]["lineage"]}
                if members.keys() - set(symbols):
                    raise CatalogueError("accepted-owner recovery no longer selects the complete serving group")
                return {"state": state, "groups": groups, "members": members}
            groups = {
                key: json.loads(selection)
                for key, selection in connection.execute("SELECT group_key, selection FROM groups")
            }
            members = dict(connection.execute("SELECT symbol, group_key FROM members"))
            return {"state": state, "groups": groups, "members": members}
    except (sqlite3.Error, ValueError, TypeError, KeyError) as error:
        raise CatalogueError(
            "cannot read serving catalogue; run alpaca-cli-eod catalogue bootstrap --store PATH"
        ) from error


def read_groups(store, keys, *, generation):
    """Read only selected decision bodies from an already accepted catalogue."""
    from libs.alpaca_lineage_build.recovery_handoffs import recall

    keys = set(keys)
    carried = {
        key: value for key in keys if (value := recall(store, "serving_group", (generation, key))) is not None
    }
    if carried.keys() == keys:
        return carried
    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            connection.execute("BEGIN")
            # controls() released its earlier transaction. Pin selected bodies to
            # the accepted catalogue token here; a mismatch invalidates that view
            # rather than authorizing mixed old/new decisions or upstream audits.
            if _state(connection, incomplete=True)["generation"] != generation:
                raise CatalogueError("selected serving catalogue changed")
            groups = dict(carried)
            for key in sorted(keys - carried.keys()):
                row = connection.execute("SELECT body FROM groups WHERE group_key=?", (key,)).fetchone()
                if row is None:
                    raise CatalogueError("selected serving group unavailable")
                groups[key] = json.loads(row[0])
            return groups
    except (sqlite3.Error, ValueError, TypeError, KeyError) as error:
        raise CatalogueError("cannot read selected serving decisions") from error


def _late_bar_projection(groups):
    """Publish bounded scheduling evidence while the owning bodies are in memory."""
    selected, identities, selected_groups = {}, 0, 0
    for group in groups:
        group_selected = False
        for sessions, sources in identity_gaps(group):
            identity_selected = False
            for source in sources:
                facts = group["history_validation"].get("raw_gap_presence", {}).get(source)
                if facts is None or not sessions <= set(facts["checked_sessions"]):
                    raise CatalogueError("raw gap provenance missing; run catalogue rebuild")
                absent = sessions - set(facts["present_sessions"])
                if not absent:
                    continue
                identity_selected = group_selected = True
                token = group["references"]["pricing"][source]
                item = selected.setdefault(source, {"source_sha256": token, "sessions": set()})
                if item["source_sha256"] != token:
                    raise CatalogueError("conflicting selected pricing generation for late-bar repair")
                item["sessions"].update(absent)
            identities += identity_selected
        selected_groups += group_selected
    return {
        "version": PROJECTION_VERSION,
        "selected_groups": selected_groups,
        "selected_identities": identities,
        "sources": {
            source: {**item, "sessions": sorted(item["sessions"])} for source, item in sorted(selected.items())
        },
    }


def publish_projection(store, key, selection, group, accepted_state, *, retain_body=False):
    """Retain completed decisions while atomically selecting new scheduling facts."""
    projection = (
        _late_bar_projection((group,))
        if group is not None
        else {
            "version": PROJECTION_VERSION,
            "selected_groups": 0,
            "selected_identities": 0,
            "sources": {},
        }
    )
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        state = _state(connection, incomplete=True)
        if state["generation"] != accepted_state["generation"]:
            raise CatalogueError("catalogue changed during projection migration")
        revised = {**selection, "late_bar_repairs": projection}
        if group is None or retain_body:
            connection.execute("UPDATE groups SET selection=? WHERE group_key=?", (encoded(revised), key))
        else:
            connection.execute(
                "UPDATE groups SET selection=?, body=? WHERE group_key=?",
                (encoded(revised), encoded(group), key),
            )
        published = _write_state(connection, complete=state["complete"], previous=state)
    from libs.alpaca_lineage_build.recovery_handoffs import serving_published

    serving_published(store, state["generation"], published["generation"], {key})
    return published


def publish_supported_gap_ranges(store, selection, group, records, state, symbols):
    """Install only newly derived public ranges while carrying their accepted history."""
    from . import export_inputs, metadata_projection

    key = group["locator"]
    token = group["decision_generation"]
    for record in records:
        _validate_public_record(record)
    revised = {
        **selection,
        "decision_generation": token,
        "online_identities": selection["online_identities"] + len(records),
        "ineligible_identities": selection["ineligible_identities"] - len(records),
        "partial_gap": True,
        "supported_gap_symbols": sorted(set(selection.get("supported_gap_symbols", ())) | symbols),
    }
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        if _state(connection)["generation"] != state["generation"]:
            raise CatalogueError("catalogue changed before supported range publication")
        changed = connection.execute(
            "UPDATE groups SET decision_generation=?, selection=?, body=? WHERE group_key=? AND decision_generation=?",
            (
                token,
                encoded(revised),
                encoded({k: v for k, v in group.items() if k != "metadata_evidence"}),
                key,
                selection["decision_generation"],
            ),
        )
        if changed.rowcount != 1:
            raise CatalogueError("serving decision changed before supported range publication")
        if "metadata_evidence" in group:
            metadata_projection.install(connection, key, group, group["metadata_evidence"])
        connection.executemany(
            "INSERT INTO securities VALUES(?, ?, ?)",
            [(record["public_id"], key, encoded(record)) for record in records],
        )
        export_inputs.accept(connection, group["references"])
        export_inputs.bind(connection, key, token)
        published = _write_state(connection, complete=True, previous=state)
    from libs.alpaca_lineage_build.recovery_handoffs import serving_published

    serving_published(store, state["generation"], published["generation"], {key})
    return published


def _retarget_group(connection, key, handoff):
    from . import export_inputs, metadata_projection

    update = (
        "UPDATE groups SET selection=json_set(selection, '$.references.lineage', json(?)), "
        "body=json_set(body, '$.references.lineage', json(?)) WHERE group_key=?"
    )
    new = encoded(handoff["lineage_after"])
    connection.execute(update, (new, new, key))
    metadata_projection.retarget(connection, key, handoff)
    export_inputs.accept(connection, {"lineage": handoff["lineage_after"], "pricing": handoff["pricing"]})


def _retarget_decisions(store, handoffs, accepted_state):
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        state = _state(connection, incomplete=True)
        if state["generation"] != accepted_state["generation"]:
            raise CatalogueError("catalogue changed before unchanged-decision publication")
        if not handoffs:
            return state
        # The selector already bound the requested decisions and skipped completed groups.
        for key, handoff in handoffs.items():
            _retarget_group(connection, key, handoff)
        published = _write_state(connection, complete=state["complete"], previous=state)
    from libs.alpaca_lineage_build.recovery_handoffs import serving_published

    serving_published(store, state["generation"], published["generation"], handoffs.keys())
    return published


def retarget_decision(store, key, handoff, accepted_state):
    """Trust one producer handoff; preserve every completed serving outcome."""
    return _retarget_decisions(store, {key: handoff}, accepted_state)


def retarget_historical_decisions(store, handoffs_by_group, accepted_state):
    """Publish selected listing metadata and its catalogue selector atomically for the batch."""
    if any("historical_listings" not in handoff for handoff in handoffs_by_group.values()):
        raise CatalogueError("historical listing publication requires a completed metadata handoff")
    return _retarget_decisions(store, handoffs_by_group, accepted_state)


@traced("serving_late_bar_selection")
def select_late_bar_repairs(store, accepted):
    """Consume the accepted compact selector; never reopen group decisions.

    The producer has already selected exact source generations and sessions.
    Legacy selectors need explicit catalogue rebuild, not an implicit body crawl.
    """
    selected, identities, selected_groups, inspected = {}, 0, 0, 0
    for selection in accepted["groups"].values():
        projection = selection.get("late_bar_repairs")
        if not isinstance(projection, dict) or projection.get("version") != PROJECTION_VERSION:
            # Before suffix publication, an all-online group could have no
            # repair work. Its legacy selector remains sufficient for Prepare.
            if not selection["ineligible_identities"] and not selection.get("partial_gap"):
                continue
            raise CatalogueError(
                "late-bar selector missing or incompatible; run alpaca-cli-eod catalogue rebuild --store PATH"
            )
        if not projection["selected_groups"] and not selection["ineligible_identities"]:
            continue
        inspected += 1
        identities += projection["selected_identities"]
        selected_groups += projection["selected_groups"]
        for symbol, item in projection["sources"].items():
            target = selected.setdefault(symbol, {"source_sha256": item["source_sha256"], "sessions": set()})
            if target["source_sha256"] != item["source_sha256"]:
                raise CatalogueError("conflicting selected pricing generation for late-bar repair")
            target["sessions"].update(item["sessions"])
    return {
        "catalogue_generation": accepted["state"]["generation"],
        "inspected_groups": inspected,
        "selected_groups": selected_groups,
        "selected_identities": identities,
        "sources": {
            symbol: {**item, "sessions": sorted(item["sessions"])} for symbol, item in sorted(selected.items())
        },
    }


def selector_state(store, *, recover=False, incomplete=False):
    """Compact publication token for a previously accepted in-memory view."""
    with closing(connect_sqlite(path(store), readonly=not recover)) as connection:
        return _state(connection, incomplete=incomplete)


def refresh_controls(store, selected=None, *, recover=False, symbols=None):
    """Retain an accepted catalogue view until its transaction token changes."""
    if (
        selected is not None
        and selector_state(store, recover=recover)["generation"] == selected["state"]["generation"]
    ):
        return selected
    return (
        controls(store, recover=recover)
        if symbols is None
        else controls(store, recover=recover, symbols=symbols)
    )


def price_review_group(store, public_id):
    """Select one internal decision even when its product quotes are offline."""
    validate_version()
    with closing(connect_sqlite(path(store), readonly=True)) as connection:
        connection.execute("BEGIN")
        state = _state(connection)
        row = connection.execute(
            "SELECT g.body FROM identities i JOIN groups g ON g.group_key=i.group_key WHERE i.public_id=?",
            (public_id,),
        ).fetchone()
        if row is None:
            raise CatalogueError("price review identity unavailable")
        return state["generation"], json.loads(row[0])


def snapshot(store, *, public_id=None):
    """Materialize public fields and selectors, then release the read transaction."""
    # The serving producer already selected online identities. Read public rows
    # and their references in one transaction; DO NOT inspect the private reasons
    # or upstream ticker state to reconstruct eligibility at this boundary.
    validate_version()
    try:
        with closing(connect_sqlite(path(store), readonly=True)) as connection:
            connection.execute("BEGIN")
            state = _state(connection)
            records, groups = {}, {}
            predicate, args = (" WHERE public_id=?", (public_id,)) if public_id else ("", ())
            for public_id, body, key in connection.execute(
                "SELECT public_id, body, group_key FROM securities" + predicate + " ORDER BY public_id", args
            ):
                records[public_id] = {"public": json.loads(body), "group_key": key}
            for key, token, references, locator in connection.execute(
                "SELECT group_key, decision_generation, json_extract(selection, '$.references'), "
                "json_extract(selection, '$.locator') FROM groups WHERE group_key IN (SELECT group_key FROM securities"
                + predicate
                + ")",
                args,
            ):
                groups[key] = {
                    "decision_generation": token,
                    "references": json.loads(references),
                    "locator": locator,
                }
            return {"state": state, "records": records, "groups": groups}
    except (sqlite3.Error, ValueError, KeyError, TypeError) as error:
        raise CatalogueError(
            "cannot open serving catalogue; run alpaca-cli-eod catalogue bootstrap --store PATH"
        ) from error


def assert_group(store, key, token):
    # A public reader can outlive its catalogue transaction. Only this selected
    # decision must still match; unrelated group publications do not invalidate
    # it. Checking current selection is not a replay of the accepted decision.
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


@traced("serving_publish")
def commit(store, previous, groups, *, held_lineage=None):
    """Replace complete old contributions atomically. No public ID collision merges."""
    # This producer validates newly computed serving outputs before one SQLite
    # transaction installs bodies, public projections and its new selector.
    # Consumers trust that commit; DO NOT move validation to every catalogue read
    # or authenticate the already accepted lineage/pricing inputs again here.
    from . import metadata_projection

    for group in groups:
        validate_group(group)
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        state = _state(connection, incomplete=True)
        metadata_projection.initialize(connection)
        for key in sorted(previous):
            connection.execute("DELETE FROM metadata_evidence WHERE group_key=?", (key,))
            connection.execute("DELETE FROM securities WHERE group_key=?", (key,))
            connection.execute("DELETE FROM members WHERE group_key=?", (key,))
            connection.execute("DELETE FROM groups WHERE group_key=?", (key,))
            connection.execute("DELETE FROM identities WHERE group_key=?", (key,))
            # A legacy explicit rebuild may be interrupted after a component.
            # Keep completed binding tokens with its committed decisions, while
            # replacing only the components selected by this publication.
            from . import export_inputs

            export_inputs.retire(connection, key)
        try:
            for group in groups:
                key = group["locator"]
                selection = {
                    **{name: group[name] for name in ("locator", "policy", "references", "decision_generation")},
                    "online_identities": len(group["securities"]),
                    "ineligible_identities": sum(not d["eligible"] for d in group["decisions"]),
                    "late_bar_repairs": _late_bar_projection((group,)),
                }
                if any(d.get("publication_start_after_missing_session") for d in group["decisions"]):
                    selection["partial_gap"] = True
                from .offline import projection as offline_projection

                offline = offline_projection(group)
                if offline is not None:
                    selection["offline_ownership"] = offline
                connection.execute(
                    "INSERT INTO groups VALUES(?, ?, ?, ?)",
                    (
                        key,
                        group["decision_generation"],
                        encoded(selection),
                        encoded({k: v for k, v in group.items() if k != "metadata_evidence"}),
                    ),
                )
                if "metadata_evidence" in group:
                    metadata_projection.install(connection, key, group, group["metadata_evidence"])
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
                from . import export_inputs

                export_inputs.accept(connection, group["references"])
                export_inputs.bind(connection, key, group["decision_generation"])
        except sqlite3.IntegrityError as error:
            raise CatalogueError(
                "conflicting public identity or group membership; no catalogue published"
            ) from error
        if held_lineage:
            # The worker selected these intentions before evaluation. This new
            # complete component supersedes them; exact deletion preserves any
            # renewal, and the surrounding transaction preserves them on rollback.
            selected = {name for group in groups for name in group["references"]["lineage"]}
            export_inputs.accept(
                connection, {"lineage": {s: g for s, g in held_lineage.items() if s in selected}}
            )
        if "gap_suffix_reviews" in state:
            for key in previous:
                state["gap_suffix_reviews"].pop(key, None)
        published = _write_state(connection, complete=state["complete"], previous=state)
    from libs.alpaca_lineage_build.recovery_handoffs import serving_published

    serving_published(
        store, state["generation"], published["generation"], set(previous) | {g["locator"] for g in groups}
    )
    return published


def _validate_public_record(record):
    from libs.alpaca_pricing_download.contract import ADJUSTMENTS

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
        not first < date.fromisoformat(day) < last for day in record["coverage"]["explained_missing_sessions"]
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
        if observation["field"] not in {"name", "cusip", "exchange", "asset_class"} or not observation["value"]:
            raise ValueError("invalid metadata projection")
        if (
            observation["observed_at"] is not None
            and datetime.fromisoformat(observation["observed_at"]).tzinfo is None
        ):
            raise ValueError("undated observation timestamp")
        start, end = observation["effective_from"], observation["effective_through"]
        if (start is None) != (end is None) or (
            start is not None and not first <= date.fromisoformat(start) <= date.fromisoformat(end) <= last
        ):
            raise ValueError("invalid historical metadata interval")


def validate_group(group):
    """Validate only new producer projections, never selected upstream payloads."""
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
            _validate_public_record(record)
        validate_history_validation(group)
        from .offline import validate as validate_offline

        validate_offline(group)
        encoded(group)
    except (KeyError, TypeError, ValueError) as error:
        raise CatalogueError(f"invalid new serving decision: {error}") from error


def finish_bootstrap(store):
    with closing(connect_sqlite(path(store), timeout=WRITE_TIMEOUT_SECONDS)) as connection, connection:
        state = _state(connection, incomplete=True)
        from . import export_inputs

        export_inputs.finish(connection)
        return state if state["complete"] else _write_state(connection, complete=True, previous=state)


def other_ticker_ranges(store, ticker, public_id, owning_group, peer):
    """Consume the selected owning/peer groups inside the exclusive review operation."""
    with closing(connect_sqlite(path(store), readonly=True)) as db:
        rows = db.execute(
            """SELECT json_extract(body,'$.ticker_ranges') FROM securities
               WHERE public_id != ? AND group_key IN (?,?)""",
            (public_id, owning_group, peer[0] if peer else owning_group),
        )
        return [r for (ranges,) in rows for r in json.loads(ranges) if r["ticker"] == ticker]


def ticker_group_token(store, generation, ticker, owning_group):
    """Bind an independent current ticker's selected group before using its dated ranges."""
    with closing(connect_sqlite(path(store), readonly=True)) as db:
        db.execute("BEGIN")
        if _state(db)["generation"] != generation:
            raise CatalogueError("selected catalogue changed before ticker occupation selection")
        row = db.execute(
            """SELECT g.group_key,g.decision_generation FROM members m JOIN groups g USING(group_key)
                WHERE m.symbol=? AND g.group_key != ?""",
            (ticker, owning_group),
        ).fetchone()
        return list(row) if row else None

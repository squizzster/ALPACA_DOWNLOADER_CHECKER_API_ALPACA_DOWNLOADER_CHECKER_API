"""Publish already assessed literal listing ranges without changing accepted prices or ownership."""

import json
from contextlib import closing
from pathlib import Path

from libs.alpaca_serving_catalogue import export_inputs, work
from libs.alpaca_serving_catalogue import storage as catalogue
from libs.common.sqlite_connections import connect_sqlite
from libs.common.store_lock import exclusive_binding, exclusive_owned
from libs.common.us_equity_market_session_calendar import next_market_session_after

from . import storage
from .contract import LineageError, RecoveryRequired, document_digest
from .correlation_policy_contract import POLICY_PROVENANCE_LINEAGE_VERSION, valid_policy_provenance
from .historical_listing import assess_label_boundaries
from .recovery_handoffs import recall, remember

FIELD = "historical_listings"
RANGES = "historical_listing_ranges"
PUBLICATION = "historical_listing_publication"
CHANGE_SECTIONS = ("label_changes", "range_changes")


def _scope(row):
    return row["symbol"], row["first_session"], row["last_session"]


def covers_assessment(assessment, ticker_ranges):
    """The same owner and label must cover the original span, including adjacent market sessions."""
    symbol, first, last = _scope(assessment)
    if last < first:
        return False
    spans = sorted(
        (r["first_session"], r["last_session"])
        for r in ticker_ranges
        if r["ticker"] == symbol and r["last_session"] >= first and r["first_session"] <= last
    )
    through = None
    for start, end in spans:
        if through is None:
            if start > first:
                return False
        elif start > through and start != next_market_session_after(through):
            return False
        through = max(through or end, end)
        if through >= last:
            return True
    return False


def covers_change(change, ticker_ranges):
    for side in ("from", "to"):
        session = change[side + "_session"]
        labels = {
            row["ticker"] for row in ticker_ranges if row["first_session"] <= session <= row["last_session"]
        }
        if labels != {change[side + "_symbol"]}:
            return False
    return True


def retain(previous, receipt, entry):
    """Carry literal observations with their original stable owner, label and dates.

    A newly assembled serving decision applies them only within the current public
    ticker range. Changes to prices alone cannot invalidate a literal trade witness.
    """
    envelope = (previous or {}).get(FIELD)
    if FIELD in receipt and receipt[FIELD] != envelope:
        raise LineageError("historical listing assessments require their owning publication")
    if envelope is None:
        return {}
    receipt[FIELD] = envelope
    return {key: entry[key] for key in (RANGES, PUBLICATION) if key in entry}


def validate_journal(journal):
    """Each changed receipt retains its own frozen policy, including pre-policy receipts."""
    entries, handoffs = journal["index_updates"], journal["serving_handoffs"]
    if (
        journal["schema_version"] not in storage.SUPPORTED_VERSIONS
        or set(handoffs) != set(entries)
        or set(journal["documents"]) != {f"{name}/lineage.manifest.json" for name in entries}
        or not journal.get("projection_only")
        or journal["retire"]
        or journal["notices"]
        or journal["export_input_changes"]
    ):
        raise LineageError("invalid historical listing publication journal")
    for name, entry in entries.items():
        receipt = journal["documents"][f"{name}/lineage.manifest.json"]
        policy = receipt.get("correlation_policy")
        handoff = handoffs[name]
        if (
            entry.get("correlation_policy") != policy
            or (
                not valid_policy_provenance(policy)
                if policy is not None
                else receipt["schema_version"] >= POLICY_PROVENANCE_LINEAGE_VERSION
            )
            or handoff.get("lineage_after", {}).get(name) != entry["sha256"]
            or handoff.get("lineage_before", {}).get(name) != entry[PUBLICATION]["before"]
            or name == min(handoff["pricing"])
            and handoff.get(FIELD) != receipt.get(FIELD)
        ):
            raise LineageError("historical listing journal lost its frozen producer binding")


class _CatalogueSelection(dict):
    def __init__(self, state, groups, binding, owners):
        super().__init__(state)
        self.groups = groups
        self.binding = binding
        self.owners = owners

    def applies(self, store, items):
        return self.binding is exclusive_binding(store) and all(
            self.groups.get(group["group_key"]) is group for group, _ in items
        )

    def advance(self, published, handoffs):
        self.update(published)
        for key, handoff in handoffs.items():
            group = self.groups[key]
            group["references"] = {**group["references"], "lineage": handoff["lineage_after"]}


def bind_catalogue(store, state, groups, owners=None):
    """Lend the selector and its accepted input applicability inside the current exclusive operation."""
    binding = exclusive_binding(store)
    return _CatalogueSelection(state, groups, binding, owners) if binding is not None else state


def _selected_catalogue(store, items, symbols, accepted_state=None, *, require_ranges=False):
    if isinstance(accepted_state, _CatalogueSelection) and accepted_state.applies(store, items):
        return accepted_state, {g["group_key"]: (g["decision_generation"], g["references"]) for g, _ in items}
    with closing(connect_sqlite(catalogue.path(store), readonly=True)) as db:
        db.execute("BEGIN")
        state = catalogue._state(db)
        export_inputs.select_held(db, symbols)
        args = (json.dumps([selection["group_key"] for selection, _ in items]),)
        rows = {
            key: (token, json.loads(references))
            for key, token, references in db.execute(
                "SELECT group_key, decision_generation,json_extract(selection,'$.references') "
                "FROM groups WHERE group_key IN (SELECT value FROM json_each(?))",
                args,
            )
        }
        if require_ranges:
            owned = {key: {} for key in rows}
            for key, pid, ranges in db.execute(
                "SELECT group_key,public_id,json_extract(body,'$.ticker_ranges') FROM securities "
                "WHERE group_key IN (SELECT value FROM json_each(?))",
                args,
            ):
                owned[key][pid] = json.loads(ranges)
            if any(owned.get(g["group_key"]) != g["ticker_ranges"] for g, _ in items):
                raise RecoveryRequired("historical listing public ticker selection changed")
    return state, rows


def _selected_entries(root, symbols):
    with closing(connect_sqlite(storage.path_at(root, "index.sqlite3"), readonly=True)) as db:
        return {
            symbol: (json.loads(entry), json.loads(handoff) if handoff else None)
            for symbol, entry, handoff in db.execute(
                "SELECT s.symbol,s.receipt,h.body FROM symbols s LEFT JOIN serving_handoffs h "
                "ON h.symbol=s.symbol AND h.generation=json_extract(s.receipt,'$.sha256') "
                "AND json_extract(s.receipt,'$.historical_listing_publication.after')=h.generation "
                "WHERE s.symbol IN (SELECT value FROM json_each(?))",
                (json.dumps(sorted(symbols)),),
            )
        }


def installed(root, journal):
    """The index commits last; a matching selection already guarantees every frozen write."""
    expected = {name: row["sha256"] for name, row in journal["index_updates"].items()}
    with closing(connect_sqlite(storage.path_at(root, "index.sqlite3"), readonly=True)) as db:
        current = dict(
            db.execute(
                "SELECT symbol,json_extract(receipt,'$.sha256') FROM symbols "
                "WHERE symbol IN (SELECT value FROM json_each(?))",
                (json.dumps(list(expected)),),
            )
        )
    return current == expected


def _recover_journal(store, root, keys, *, require_clear):
    path = storage.path_at(root, ".pending.json")
    if path.exists():
        pending = storage.read_document(path)
        listing = pending.get(PUBLICATION) == 1
        selected = listing and any(h["group_key"] in keys for h in pending["serving_handoffs"].values())
        if selected:
            # The index selects a batch atomically. Even a narrower retry finishes
            # the original frozen transaction, then serves only requested groups.
            storage.recover_journal(store, root, pending)
        elif require_clear:
            raise RecoveryRequired("another lineage publication requires owning recovery")
    return path


def recover_pending(store, groups, *, accepted_state=None):
    """Finish only selected pending metadata obligations, including post-commit acknowledgements."""
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise LineageError("historical listing recovery requires the exclusive store operation")
    result = {"retargeted": {}, "acknowledged": 0, "catalogue_generation": None}
    if not groups:
        return result
    root = store / "end_of_day/lineage"
    if (root / ".pending.json").exists():
        with storage.writer_lock(store) as root:
            _recover_journal(store, root, groups, require_clear=False)
    # Select from the durable queue, never from the lineage universe or its
    # receipt bodies. Its generation was selected atomically with the work item.
    with closing(connect_sqlite(root / "index.sqlite3", readonly=True)) as db:
        db.execute("BEGIN")
        if not db.execute("SELECT EXISTS(SELECT 1 FROM serving_handoffs)").fetchone()[0]:
            return result
        anchors = [min(g["references"]["pricing"]) for g in groups.values() if g["references"]["pricing"]]
        pending = [
            (name, generation, json.loads(body))
            for name, generation, body in db.execute(
                "SELECT symbol,generation,body FROM serving_handoffs "
                "WHERE symbol IN (SELECT value FROM json_each(?)) "
                "AND json_extract(body,'$.historical_listings.version')=1",
                (json.dumps(anchors),),
            )
        ]
    if not pending:
        return result
    selected = [(groups[h["group_key"]], {}) for _, _, h in pending if h["group_key"] in groups]
    if not selected:
        return result
    names = {name for g, _ in selected for name in g["references"]["lineage"]}
    state, controls = _selected_catalogue(store, selected, names, accepted_state)
    acknowledgements = {}
    for name, generation, handoff in pending:
        key = handoff["group_key"]
        if key not in groups:
            continue
        current = controls.get(key)
        if (
            not current
            or current[0] != handoff["decision_generation"]
            or current[1]["pricing"] != handoff["pricing"]
            or current[1]["lineage"] not in (handoff["lineage_before"], handoff["lineage_after"])
        ):
            raise RecoveryRequired("historical listing recovery no longer matches the serving decision")
        if current[1]["lineage"] == handoff["lineage_before"]:
            result["retargeted"][key] = handoff
        result.setdefault("affected_public_ids", {})[key] = handoff.get(
            "assessed_public_ids", sorted(handoff[FIELD]["assessments"])
        )
        acknowledgements.update(_changed_targets(handoff))
    if result["retargeted"]:
        published = catalogue.retarget_historical_decisions(store, result["retargeted"], state)
        result["catalogue_generation"] = published["generation"]
        if isinstance(state, _CatalogueSelection):
            state.advance(published, result["retargeted"])
    work.acknowledge(store, acknowledgements)
    result["acknowledged"] = len(acknowledgements)
    return result


def _receipt(store, root, symbol, entry):
    key = (symbol, entry["sha256"])
    held = recall(store, "receipt", key)
    if held is not None:
        return held
    held = storage.selected_document(
        storage.path_at(root, f"{symbol}/lineage.manifest.json", published=True), entry["sha256"]
    )
    remember(store, "receipt", key, held)
    return held


def _changed_targets(handoff):
    return {s: gen for s, gen in handoff["lineage_after"].items() if gen != handoff["lineage_before"][s]}


def _replacements(selection, incoming):
    superseded = selection.get("superseded_listing_scopes") or {}
    if not superseded:
        return {}
    retired = {pid: sorted([list(scope) for scope in scopes]) for pid, scopes in superseded.items()}
    work_scopes = sorted([list(item) for item in selection.get("listing_work_scopes", ())])
    offered = sorted(
        [pid, *_scope(row), "competing_published_owners" in row["limits"]]
        for pid, rows in incoming.items()
        for row in rows
    )
    if not work_scopes or offered != work_scopes:
        raise RecoveryRequired("historical listing replacements differ from the selected ownership assessment")
    return {"superseded": retired, "replacements": work_scopes}


def _bind(selection, assessments, catalogue_row, entries):
    key, refs = selection["group_key"], selection["references"]
    prices, before = refs["pricing"], refs["lineage"]
    if not prices or set(prices) != {s for s, gen in before.items() if gen is not None}:
        raise RecoveryRequired("historical listing requires a complete selected priced group")
    anchor = min(prices)
    actual = {s: entries[s][0]["sha256"] if s in entries else None for s in before}
    if (
        catalogue_row is None
        or catalogue_row[0] != selection["decision_generation"]
        or catalogue_row[1]["pricing"] != prices
        or any(s not in entries or entries[s][0]["source_sha256"] != gen for s, gen in prices.items())
        or any(set(entries[s][0]["group_members"]) != set(before) for s in prices)
    ):
        raise RecoveryRequired("historical listing group, serving decision or pricing selection changed")

    marker = entries[anchor][0].get(PUBLICATION, {})
    published_here = (
        marker.get("group_key") == key
        and marker.get("decision_generation") == catalogue_row[0]
        and marker.get("after") == actual[anchor]
        and marker.get("lineage_after", actual) == actual
    )
    prior = marker.get("lineage_before", {**actual, anchor: marker.get("before")})
    if actual != before and not (published_here and prior == before):
        raise RecoveryRequired("historical listing lineage selection changed")
    served = catalogue_row[1]["lineage"]
    handoff = entries[anchor][1]
    if served != actual and not (
        published_here
        and handoff
        and FIELD in handoff
        and handoff["lineage_after"] == actual
        and handoff["lineage_before"] == served
        and handoff["pricing"] == prices
    ):
        raise RecoveryRequired("historical listing serving references changed; finish catalogue maintenance")
    if any(
        pid not in selection["ticker_ranges"]
        or any(not covers_assessment(row, selection["ticker_ranges"][pid]) for row in rows)
        for pid, rows in assessments.items()
    ):
        raise LineageError("historical listing assessment outside the selected public ticker range")
    return anchor, actual, handoff


def _merge(store, root, selection, incoming, entries, anchor, claim):
    # Normally one anchor holds the entire group. After a membership change an
    # old anchor can become a peer: its compact scope roster selects just the
    # receipt(s) needed to assemble this group's complete descriptive handoff.
    ticker_ranges = selection["ticker_ranges"]
    combined, baselines, peers = {}, {}, {}
    superseded = {pid: set(map(tuple, rows)) for pid, rows in claim.get("superseded", {}).items()}
    anchor_receipt = _receipt(store, root, anchor, entries[anchor][0])
    for name in sorted(selection["references"]["pricing"]):
        entry = entries[name][0]
        if not entry.get(RANGES):
            continue
        if not any(pid in ticker_ranges for pid in entry[RANGES]):
            continue
        receipt = anchor_receipt if name == anchor else _receipt(store, root, name, entry)
        marker = entry.get(PUBLICATION, {})
        same_decision = (
            marker.get("group_key") == selection["group_key"]
            and marker.get("decision_generation") == selection["decision_generation"]
        )
        for pid, rows in receipt[FIELD]["assessments"].items():
            if pid in ticker_ranges:
                baselines.setdefault(
                    pid,
                    (
                        entry[RANGES][pid],
                        [
                            change
                            for section in CHANGE_SECTIONS
                            for change in receipt[FIELD].get(section, {}).get(pid, ())
                        ],
                        same_decision,
                    ),
                )
                accepted = combined.setdefault(pid, {})
                for row in rows:
                    scope = _scope(row)
                    if scope not in superseded.get(pid, ()) and (
                        same_decision or covers_assessment(row, ticker_ranges[pid])
                    ):
                        accepted.setdefault(scope, row)
        if name != anchor:
            peer = dict(receipt)
            peer.pop(FIELD)
            retained = {pid: scopes for pid, scopes in entry[RANGES].items() if pid not in ticker_ranges}
            if retained:
                peer[FIELD] = {
                    "version": 1,
                    "assessments": {pid: receipt[FIELD]["assessments"][pid] for pid in retained},
                }
                for section in CHANGE_SECTIONS:
                    retained_changes = {
                        pid: r for pid, r in receipt[FIELD].get(section, {}).items() if pid in retained
                    }
                    if retained_changes:
                        peer[FIELD][section] = retained_changes
            peers[name] = (peer, retained)
    for pid, rows in incoming.items():
        accepted = combined.setdefault(pid, {})
        for row in rows:
            accepted.setdefault(_scope(row), row)
    assessments = {pid: [rows[k] for k in sorted(rows)] for pid, rows in combined.items()}
    scopes = {pid: [list(scope) for scope in sorted(rows)] for pid, rows in combined.items()}
    envelope = {"version": 1, "assessments": assessments}
    for pid, rows in assessments.items():
        prior, old_changes, same_decision = baselines.get(pid, ([], [], False))
        dirty = (set(map(tuple, prior)) ^ combined[pid].keys()) | superseded.get(pid, set())
        windows = {(first, last) for _, first, last in dirty}
        carried = [
            c
            for c in old_changes
            if not any(first <= c["to_session"] and c["from_session"] <= last for first, last in windows)
            and (same_decision or covers_change(c, ticker_ranges[pid]))
        ]
        if windows:
            carried.extend(
                c
                for c in assess_label_boundaries(rows, windows=windows, include_same_symbol=True)
                if covers_change(c, ticker_ranges[pid])
            )
        for change in sorted(carried, key=lambda c: c["from_session"]):
            section = "range_changes" if change["from_symbol"] == change["to_symbol"] else "label_changes"
            envelope.setdefault(section, {}).setdefault(pid, []).append(change)
    return {anchor: ({**anchor_receipt, FIELD: envelope}, scopes), **peers}


def _known_scopes(selection, entries):
    known = {}
    for name in selection["references"]["pricing"]:
        for pid, scopes in entries[name][0].get(RANGES, {}).items():
            known.setdefault(pid, set()).update(map(tuple, scopes))
    return known


def _plan(store, root, items, catalogues, entries, state):
    journal = {
        "schema_version": storage.VERSION,
        "boundary": "alpaca_lineage_transaction",
        PUBLICATION: 1,
        "projection_only": True,
        "index_updates": {},
        "documents": {},
        "retire": [],
        "sources": {},
        "unavailable_pricing": [],
        "notices": {},
        "export_input_changes": {},
        "serving_handoffs": {},
    }
    handoffs, acknowledgements = {}, {}
    for selection, incoming in items:
        key, refs = selection["group_key"], selection["references"]
        anchor, actual, pending = _bind(selection, incoming, catalogues.get(key), entries)
        known = _known_scopes(selection, entries)
        claim = _replacements(selection, incoming)
        marker = entries[anchor][0].get(PUBLICATION, {})
        if (
            not claim or marker.get("ownership_rebinding") == claim and marker.get("after") == actual[anchor]
        ) and all(
            pid in known and all(_scope(row) in known[pid] for row in rows) for pid, rows in incoming.items()
        ):
            # After a process interruption either publication can already have
            # committed. Trust the installed selector and consume the exact queue.
            if pending and FIELD in pending:
                if catalogues[key][1]["lineage"] != actual:
                    handoffs[key] = pending
                acknowledgements.update(_changed_targets(pending))
            continue
        if actual != refs["lineage"] or actual != catalogues[key][1]["lineage"]:
            raise RecoveryRequired("historical listing changes require the current group selection")
        if claim:
            if (
                not isinstance(state, _CatalogueSelection)
                or state.owners is None
                or any(
                    pid not in known or not set(map(tuple, scopes)) <= known[pid]
                    for pid, scopes in claim["superseded"].items()
                )
            ):
                raise RecoveryRequired(
                    "historical listing ownership replacement needs its current selected scope"
                )
            for pid, scopes in claim["superseded"].items():
                replacements = [{"ticker": row["symbol"], **row} for row in incoming[pid]]
                if any(
                    not covers_assessment({"symbol": sym, "first_session": a, "last_session": b}, replacements)
                    for sym, a, b in scopes
                ):
                    raise RecoveryRequired("incomplete historical listing ownership replacement")
        outputs = _merge(store, root, selection, incoming, entries, anchor, claim)
        after = {**actual, **{s: document_digest(r) for s, (r, _) in outputs.items()}}
        handoff = {
            "version": 1,
            "group_key": key,
            "decision_generation": selection["decision_generation"],
            "lineage_before": actual,
            "lineage_after": after,
            "pricing": refs["pricing"],
            "assessed_public_ids": sorted(incoming),
            "unchanged_metadata_projection_version": 2,
            FIELD: outputs[anchor][0][FIELD],
            **({"ownership_rebinding": claim, "catalogue_at_assessment": state["generation"]} if claim else {}),
        }
        for name, (receipt, scopes) in outputs.items():
            marker = {
                "group_key": key,
                "decision_generation": selection["decision_generation"],
                "before": actual[name],
                "after": after[name],
                **({"lineage_before": actual, "lineage_after": after} if len(outputs) > 1 else {}),
                **({"ownership_rebinding": claim} if claim else {}),
            }
            journal["documents"][f"{name}/lineage.manifest.json"] = receipt
            journal["index_updates"][name] = {
                **entries[name][0],
                "sha256": after[name],
                RANGES: scopes,
                PUBLICATION: marker,
            }
            journal["serving_handoffs"][name] = handoff
        journal["sources"].update(refs["pricing"])
        journal["unavailable_pricing"].extend(s for s, gen in actual.items() if gen is None)
        handoffs[key] = handoff
        acknowledgements.update(_changed_targets(handoff))
    return journal, handoffs, acknowledgements


def publish_many(store, items, *, accepted_state=None):
    """Install one bounded batch of selected producer results and acknowledge only served generations."""
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise LineageError("historical listing publication requires the exclusive store operation")
    items = list(items)
    if not items:
        return {"published": 0, "already_published": 0}
    keys = {selection["group_key"] for selection, _ in items}
    symbols = [symbol for selection, _ in items for symbol in selection["references"]["lineage"]]
    if len(items) != len(keys) or len(symbols) != len(set(symbols)):
        raise LineageError("historical listing batch repeats a selected group or member")
    with storage.writer_lock(store) as root:
        pending_path = _recover_journal(store, root, keys, require_clear=True)
        state, catalogues = _selected_catalogue(store, items, symbols, accepted_state, require_ranges=True)
        entries = _selected_entries(root, symbols)
        journal, handoffs, acknowledgements = _plan(store, root, items, catalogues, entries, state)
        changed = journal["index_updates"]
        if changed:
            validate_journal(journal)
            with (
                storage.pricing_locks(store, changed, known=changed),
                storage.publication_lock(root, exclusive=True),
            ):
                storage.atomic_json(pending_path, journal, compare=False)
                storage.finish_publication(store, root, journal, receipts={})
            for name, entry in changed.items():
                remember(
                    store,
                    "receipt",
                    (name, entry["sha256"]),
                    journal["documents"][f"{name}/lineage.manifest.json"],
                )
        if handoffs:
            published = catalogue.retarget_historical_decisions(store, handoffs, state)
            if isinstance(state, _CatalogueSelection):
                state.advance(published, handoffs)
        if acknowledgements:
            work.acknowledge(store, acknowledgements)
        new_groups = len({h["group_key"] for h in journal["serving_handoffs"].values()})
        return {"published": new_groups, "already_published": len(items) - new_groups}


def publish_historical_listings(store, selection, assessments, *, accepted_state=None):
    return publish_many(store, [(selection, assessments)], accepted_state=accepted_state)

"""Reconcile two selected, already published histories within one locked group."""

from datetime import date
from decimal import Decimal
from itertools import combinations
from pathlib import Path

from alpaca_end_of_day import DailyBar
from alpaca_end_of_day.standalone import _assignments, _read_views
from libs.alpaca_pricing_download.contract import symbol_label
from libs.alpaca_pricing_download.storage import load_source, published_path
from libs.alpaca_security_lineage.inference_evidence import provider_evidence
from libs.alpaca_serving_catalogue import export_inputs
from libs.alpaca_serving_catalogue import storage as serving
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import exclusive_owned

from . import storage
from .contract import RecoveryRequired
from .duplicate_assignments import AssignmentProjection, UnsupportedOverlap, rewrite
from .duplicate_history import AcceptedHistory, _pair, _resolved_splits

FIELD = "duplicate_source_slices"
MARKER = "selected_owner_repair"


def _select_group(store, key):
    """The caller selects one named serving group, never a universe of IDs."""
    state = serving.selector_state(store)
    group = serving.read_groups(store, {key}, generation=state["generation"])[key]
    pending = export_inputs.select(
        store, state["generation"], set(group["references"]["lineage"]), recover_lineage=True
    )
    return group, pending


def _candidates(securities, memberships, edges):
    """Use completed public spans before deciding whether any trade data are needed."""
    for first, second in combinations(securities, 2):
        a, b = sorted((first, second), key=lambda v: v["last_session"])
        left, right = (memberships[v["public_id"]] for v in (a, b))
        if (
            a["first_session"] != b["first_session"]
            or a["last_session"] >= b["last_session"]
            or a["coverage"]["session_count"] >= b["coverage"]["session_count"]
        ):
            continue
        last = date.fromisoformat(a["last_session"])
        if left["member_alpaca_asset_ids"] and set(left["member_alpaca_asset_ids"]) < set(
            right["member_alpaca_asset_ids"]
        ):
            if not any(
                e.old_label == e.new_label and e.day > last and e.successor in right["member_alpaca_asset_ids"]
                for e in edges.values()
            ):
                continue
        elif set(left["member_alpaca_asset_ids"]).isdisjoint(right["member_alpaca_asset_ids"]):
            if not any(
                e.old_label != e.new_label
                and e.day > last
                and e.predecessor in left["member_alpaca_asset_ids"]
                and e.successor in right["member_alpaca_asset_ids"]
                for e in edges.values()
            ):
                continue
        else:
            continue
        yield a, b


def _decision(store, group, prior, index, root):
    members = {i["security_id"]: i for r in prior.values() for i in r["identity_memberships"]}
    evidence = [provider_evidence(r["evidence"]) for r in prior.values()]
    edges = _resolved_splits(evidence)
    candidates = list(_candidates(group["securities"], members, edges))
    if not candidates:
        return {
            "status": "unresolved",
            "reason": "No dated provider ownership route between the selected public histories.",
        }
    if any(
        r.get("price_basis_limits")
        or r.get("numerical_source_equivalence")
        or r.get("numerical_source_revision")
        for r in prior.values()
    ):
        return {
            "status": "unresolved",
            "reason": "A separate numerical transfer requires a composed source scope.",
        }
    documents = {name: storage.read_boundaries(root, r) for name, r in prior.items()}
    refs = group["references"]
    sources = {
        name: load_source(published_path(store, "end_of_day/pricing/" + name), name) for name in refs["pricing"]
    }
    assignments = {s: list(_assignments(documents[s], r)) for s, r in prior.items()}
    cache = {}
    headers = {rec["public_id"]: rec for pair in candidates for rec in pair}

    def rows(pid):
        return _read_views(
            assignments,
            sources,
            store,
            pid,
            ("raw",),
            None,
            None,
            io_cache=cache,
            dated_interval=headers[pid].get("dated_price_interval"),
        )["raw"]["bars"]

    tapes, histories = {}, {}
    for pid in headers:
        tape = tapes[pid] = {bar["session"]: bar for bar in rows(pid)}
        labels = {date.fromisoformat(d): tuple(sorted(b["tickers"])) for d, b in tape.items()}
        raw = {}
        for d, b in tape.items():
            day = date.fromisoformat(d)
            raw[day] = DailyBar(
                day,
                *(Decimal(str(b[k])) for k in ("open", "high", "low", "close", "volume")),
                int(b["trade_count"]),
                Decimal(str(b["vwap"])),
            )
        histories[pid] = AcceptedHistory(pid, frozenset(members[pid]["member_alpaca_asset_ids"]), labels, raw)

    options = [
        result
        for a, b in candidates
        if (result := _pair(histories[a["public_id"]], histories[b["public_id"]], edges))
    ]
    if len(options) != 1:
        return {
            "status": "unresolved",
            "reason": "Matching traded history and unique admissible owner do not select one correction.",
        }
    (choice,) = options
    pieces = [
        {
            "source_id": s.source_id,
            "first": s.first.isoformat(),
            "last": s.last.isoformat(),
            "sessions": sum(s.first <= day <= s.last for day in histories[s.source_id].raw),
        }
        for s in choice.selection
    ]
    model = {
        "reason": choice.reason,
        "sources": list(choice.sources),
        "selection": pieces,
        "supporting_action_ids": list(choice.supporting_action_ids),
        "version": 1,
        "source_decision": group["decision_generation"],
        "source_pricing": refs["pricing"],
    }
    selected = {pid: members[pid] for pid in choice.sources}
    transition = relation = None
    if len(pieces) == 2:
        (identifier,) = choice.supporting_action_ids
        edge = edges[identifier]
        transition = next(a for e in evidence for a in e["corporate_actions"] if a.get("id") == identifier)

        def endpoint(label, asset, cusip, public_id):
            return {
                "historical_symbol": label,
                "asset_id": asset,
                "cusip": cusip,
                "security_id": public_id,
                "resolution": "resolved",
            }

        relation = {
            "relation_id": "provider:" + identifier,
            "relation_kind": "split_and_ticker_change",
            "identity_union": False,
            "corporate_action_id": identifier,
            "effective_date": edge.day.isoformat(),
            "first_successor_trading": pieces[-1]["first"],
            "old_rate": str(transition["old_rate"]),
            "new_rate": str(transition["new_rate"]),
            "predecessor": endpoint(
                edge.old_label, edge.predecessor, transition["old_cusip"], pieces[0]["source_id"]
            ),
            "successor": endpoint(
                edge.new_label, edge.successor, transition["new_cusip"], pieces[1]["source_id"]
            ),
        }
    accepted = {record["public_id"]: record for record in group["history_validation"]["prefix_records"]}
    for piece in pieces:
        pid = piece["source_id"]
        old = accepted.get(pid)
        if not old or old["price_reasons"] or old["missing_sessions"] or old["last_session"] != piece["last"]:
            return {"status": "unresolved", "reason": "Retained coverage cannot supply the selected checkpoint."}
        if piece["first"] != old["first_session"] and (piece["sessions"] < 2 or old.get("dated_price_token")):
            return {
                "status": "unresolved",
                "reason": "An existing accepted fragment needs a composed source scope.",
            }
    try:
        projection = AssignmentProjection(tapes, selected, model, documents, prior, transition=transition)
        updates = {name: rewrite(name, r, documents[name], projection, relation) for name, r in prior.items()}
    except UnsupportedOverlap as err:
        return {"status": "unresolved", "reason": str(err)}
    receipts = {name: r for name, (r, _) in updates.items()}
    from .duplicate_assignments import bind_retained_labels

    try:
        bind_retained_labels(receipts, set(choice.sources))
    except UnsupportedOverlap as err:
        return {"status": "unresolved", "reason": str(err)}
    boundaries = {name: d for name, (_, d) in updates.items()}
    storage.publish(
        store, root, index, receipts, boundaries, {}, previous=prior, sources=sources, completed_overlap=model
    )
    return {
        "status": "published",
        "owners": [s["source_id"] for s in pieces],
        "former_owners": choice.sources,
        "selected": pieces,
    }


def reconcile(store, group_key):
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise RecoveryRequired("selected owner reconciliation needs the exclusive store operation")
    group_key = symbol_label(group_key)
    group, dirty = _select_group(store, group_key)
    return run_selected(store, group, dirty)


def run_selected(store, group, dirty, *, occupation_id=None, client_factory=None):
    from . import occupation_storage

    kind = occupation_storage.MARKER if occupation_id else MARKER
    expected = (
        {"public_id": occupation_id, "source_decision": group["decision_generation"]}
        if occupation_id
        else group["decision_generation"]
    )
    refs = group["references"]
    with storage.writer_lock(store) as root:
        pending = storage.path_at(root, ".pending.json")
        if pending.exists():
            journal = storage.read_document(pending)
            if journal.get(kind) != expected or any(
                journal["export_input_changes"].get(n) != v for n, v in dirty.items()
            ):
                raise RecoveryRequired("different lineage transaction needs owning recovery")
            storage.recover_journal(store, root, journal)
            result = {"status": "recovered"}
        else:
            with storage.pricing_locks(store, refs["lineage"], known=refs["pricing"]):
                idx = storage.read_index(root, group["locator"], group=True)
                entries = idx["symbols"]
                if entries.keys() != refs["pricing"].keys() or any(
                    set(e["group_members"]) != refs["lineage"].keys() or e["source_sha256"] != refs["pricing"][s]
                    for s, e in entries.items()
                ):
                    raise RecoveryRequired("selected ownership inputs changed")
                if dirty:
                    if any(entries.get(s, {}).get("sha256") != v for s, v in dirty.items()) or any(
                        e.get(kind) != expected for e in entries.values()
                    ):
                        raise RecoveryRequired("unrelated lineage change needs owning catalogue maintenance")
                    result = {"status": "recovered"}
                elif any(e["sha256"] != refs["lineage"][s] for s, e in entries.items()):
                    raise RecoveryRequired("serving and selected lineage no longer agree")
                elif occupation_id and all(
                    occupation_id in e.get(occupation_storage.TOKENS, {}) for e in entries.values()
                ):
                    tokens = {e[occupation_storage.TOKENS][occupation_id] for e in entries.values()}
                    if len(tokens) != 1:
                        raise RecoveryRequired("dated occupation group has conflicting completed tokens")
                    return {"status": "already_published", "token": tokens.pop(), "complete": True, "failed": 0}
                elif (
                    not occupation_id
                    and entries
                    and all(e.get(MARKER) for e in entries.values())
                    and len({e[MARKER] for e in entries.values()}) == 1
                ):
                    return {
                        "status": "already_published",
                        "online_ids": [s["public_id"] for s in group["securities"]],
                    }
                elif not occupation_id and len(group["securities"]) < 2:
                    return {
                        "status": "already_single",
                        "online_ids": [s["public_id"] for s in group["securities"]],
                    }
                else:
                    if occupation_id:
                        from .occupation_review import OccupationReview

                        review = OccupationReview(root, group, occupation_id)
                        if "outcome" in review.state:
                            return review.state["outcome"]
                    prior = {n: storage.read_receipt(root, n, idx) for n in entries}
                    if occupation_id:
                        from .occupation_repair import _produce

                        result = _produce(store, group, prior, idx, root, occupation_id, client_factory, review)
                        if result["status"] == "unresolved":
                            review.finish(result)
                    else:
                        result = _decision(store, group, prior, idx, root)
    if result["status"] in ("published", "recovered"):
        result["serving"] = maintain(store, symbols=[group["locator"]])
        result["complete"] = result["serving"]["complete"]
        result["failed"] = result["serving"]["failed"]
    return result

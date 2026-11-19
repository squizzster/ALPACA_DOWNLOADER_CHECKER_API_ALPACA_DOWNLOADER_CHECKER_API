"""Assess a selected public identity across a reused trading label."""

from datetime import date
from functools import cache
from pathlib import Path
from uuid import uuid4

from alpaca_end_of_day import DataLibrary
from alpaca_end_of_day.standalone import _assignments, _read_views
from libs.alpaca_pricing_download.dated_rename import provider_rows
from libs.alpaca_pricing_download.storage import load_source, published_path
from libs.alpaca_security_lineage.inference_evidence import provider_evidence
from libs.alpaca_serving_catalogue import export_inputs
from libs.alpaca_serving_catalogue import storage as serving
from libs.common.store_lock import exclusive_owned

from . import storage
from .contract import RecoveryRequired
from .duplicate_assignments import UnsupportedOverlap, bind_retained_labels, rewrite
from .occupation_assignments import OccupationProjection
from .occupation_boundary import select, traded


def unresolved(reason):
    return {"status": "unresolved", "reason": reason, "complete": True, "failed": 0}


def has_prior_anchor(receipts, owner, label, first, last, ownership=()):
    """A contemporary resolved CUSIP protects its issuer; later projections do not establish prior ownership."""
    ids = set(owner["member_alpaca_asset_ids"])
    for receipt in receipts.values():
        evidence = provider_evidence(receipt["evidence"])
        for occupation in evidence.get("symbol_occupancy_evidence", ()):
            if occupation.get("symbol") != label or occupation.get("asset_id") not in ids:
                continue
            cusip = occupation.get("cusip")
            if not cusip:
                continue
            for item in occupation.get("evidence", ()):
                day = item.get("process_date")
                if day and first <= day <= last:
                    return True
                if item.get("role") != "new" or not day or day > first:
                    continue
                if any(
                    span["ticker"] == label
                    and span["cusip"] == cusip
                    and span["start_inclusive"] is not None
                    and day <= span["start_inclusive"] <= first
                    and (span["end_exclusive"] is None or first < span["end_exclusive"])
                    for span in ownership
                ):
                    return True
    return False


def _produce(store, group, prior, index, root, pid, client_factory, review):
    security = next(s for s in group["securities"] if s["public_id"] == pid)
    ranges = sorted(security["ticker_ranges"], key=lambda span: span["first_session"])
    if not ranges or any(
        left["last_session"] >= right["first_session"] for left, right in zip(ranges, ranges[1:])
    ):
        return unresolved("No unambiguous earlier trading-label interval.")
    span = ranges[0]
    locator = group["locator"]
    first, through, label = span["first_session"], span["last_session"], span["ticker"]
    current = ranges[-1]
    if any(
        r.get("price_basis_limits")
        or r.get("numerical_source_revision")
        or r.get("numerical_source_equivalence")
        for r in prior.values()
    ):
        return unresolved("A separate numerical transfer requires a composed owner scope.")
    checkpoint = next(r for r in group["history_validation"]["prefix_records"] if r["public_id"] == pid)
    if checkpoint["price_reasons"] or checkpoint["missing_sessions"] or checkpoint.get("dated_price_token"):
        return unresolved("This owner needs a composed numerical or gap checkpoint.")

    @cache
    def inputs():
        documents = {n: storage.read_boundaries(root, r) for n, r in prior.items()}
        sources = {n: load_source(published_path(store, "end_of_day/pricing/" + n), n) for n in prior}
        return documents, sources

    def read_held():
        documents, sources = inputs()
        assignments = {n: list(_assignments(documents[n], r)) for n, r in prior.items()}
        rows = _read_views(
            assignments,
            sources,
            store,
            pid,
            ("raw",),
            date.fromisoformat(first),
            date.fromisoformat(through),
            io_cache={},
            minimum_session=first,
            maximum_session=through,
        )["raw"]["bars"]
        return {b["session"]: b for b in rows}

    client = cache(client_factory)

    def route(symbol, asof):
        key = "|".join(("raw", symbol, asof, first, through))
        return review.observe(key, lambda: provider_rows(client()[0], symbol, asof, "raw", first, through))

    owner = next(i for r in prior.values() for i in r["identity_memberships"] if i["security_id"] == pid)

    def anchored_before(last):
        return has_prior_anchor(prior, owner, label, first, last, security["ticker_ownership_intervals"])

    held = review.observe("held", read_held)

    def infer():
        if not any(not traded(b) for b in held.values()):
            return None
        literal = route(label, "-")

        def component(prev, start, anchor):
            from . import unit_occupations

            def events(symbol, begin, end):
                key = "|".join(("actions", symbol, begin, end))
                return review.observe(
                    key, lambda: {"events": client()[1].get_corporate_actions(symbol, begin, end)}
                )["events"]

            return unit_occupations.select(
                [provider_evidence(r["evidence"]) for r in prior.values()],
                label,
                owner,
                literal,
                prev,
                start,
                route,
                events,
                prior_anchor=anchor,
            )

        return select(
            held,
            literal,
            route(current["ticker"], current["last_session"]),
            lambda asof: route(label, asof),
            anchored_before=anchored_before,
            unit_occupation=component if len(ranges) == 1 and label.endswith(".U") else None,
        )

    decision = review.decide(infer)
    if decision is None:
        return unresolved("The provider routes do not distinguish two dated trading occupations.")
    cut = decision["first"]
    if not (first < cut <= through):
        return unresolved("The earlier label supplies no distinct historical trade scope.")
    literal = route(label, "-")
    documents, sources = inputs()
    record = {
        **decision,
        "version": 1,
        "token": uuid4().hex,
        "locator": locator,
        "public_id": pid,
        "symbol": label,
        "prior_first_trade": min(d for d, b in literal.items() if traded(b)),
        "prior_asof": decision["last_prior_trade"],
        "current_symbol": current["ticker"],
        "current_asof": current["last_session"],
        "source_decision": group["decision_generation"],
        "source_pricing": group["references"]["pricing"],
        "members": sorted(prior),
        "unavailable": sorted(set(group["references"]["lineage"]) - prior.keys()),
    }
    keep_prior = decision.get("retained_side") == "prior"
    if keep_prior:
        from .occupation_continuation import terms

        record["terminal_evidence"] = {name: terms(r["evidence"], record) for name, r in prior.items()}
    if keep_prior and (
        len(ranges) != 1
        or first != security["first_session"]
        or through != security["last_session"]
        or checkpoint["gap_bars"]
    ):
        return unresolved("The terminal unit scope intersects independent retained evidence.")
    chosen_first, chosen_last = (
        (first, decision["last_prior_trade"]) if keep_prior else (cut, security["last_session"])
    )
    removed = sum(not chosen_first <= d <= chosen_last for d in held)
    model = {
        "version": 1,
        "reason": decision["reason"],
        "sources": [pid],
        "selection": [
            {
                "source_id": pid,
                "first": chosen_first,
                "last": chosen_last,
                "sessions": security["coverage"]["session_count"] - removed,
            }
        ],
        "source_decision": group["decision_generation"],
        "source_pricing": record["source_pricing"],
    }
    if keep_prior:
        previous, last = sorted(d for d in held if d <= chosen_last)[-2:]
        model["selection"][0]["final_raw_sessions"] = [
            {"session": d, "tickers": held[d]["tickers"]} for d in (previous, last)
        ]
    projection = OccupationProjection(
        held,
        owner,
        model,
        label,
        span,
        ranges,
        security["ticker_ownership_intervals"],
        terminal=decision.get("terminal_action"),
    )
    receipts, boundaries = {}, {}
    try:
        for name, r in prior.items():
            receipt, changed = rewrite(name, r, documents[name], projection)
            receipts[name], boundaries[name] = receipt, changed
        bind_retained_labels(receipts, {pid})
    except UnsupportedOverlap as err:
        return unresolved(str(err))
    earlier_literal = (
        None if keep_prior else {d: b for d, b in literal.items() if d <= decision["last_prior_trade"]}
    )
    storage.publish(
        store,
        root,
        index,
        receipts,
        boundaries,
        {},
        previous=prior,
        sources=sources,
        completed_overlap=model,
        completed_occupation=(record, earlier_literal),
    )
    return {"status": "published", "occupation": record, "selected": model["selection"]}


def reconcile(store, public_id, client_factory):
    from .duplicate_repair import run_selected

    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise RecoveryRequired("dated occupation repair requires the exclusive store operation")
    data = DataLibrary._for_online_identity(store, public_id)
    pid = data.get_security(public_id).public_id
    key = data._records[pid]["group_key"]
    generation = data.catalog_generation()
    group = serving.read_groups(store, [key], generation=generation)[key]
    dirty = export_inputs.select(store, generation, set(group["references"]["lineage"]), recover_lineage=True)
    return run_selected(store, group, dirty, occupation_id=pid, client_factory=client_factory)

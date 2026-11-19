"""Publish an independently identified earlier ticker on a selected existing owner's tape."""

from contextlib import ExitStack
from datetime import date, timedelta
from uuid import uuid4

from libs.alpaca_pricing_download import displaced_labels as prices
from libs.alpaca_pricing_download.price_basis_review import review_steps
from libs.alpaca_serving_catalogue import export_inputs
from libs.alpaca_serving_catalogue import storage as catalogue
from libs.common.us_equity_market_session_calendar import surrounding_market_sessions

from . import historical_labels, occupation_pivots, price_basis_limits
from .contract import LineageError, RecoveryRequired
from .dated_price_publication import publish_selection
from .price_basis_review import selected_many


def _review(store, pid, pivot, data, client, _):
    security = data.get_security(pid)
    record = data._records[pid]
    public = record["public"]
    group = data._groups[record["group_key"]]
    refs = group["references"]

    def result(status, reason):
        return None, dict(public_id=pid, cut=pivot, status=status, reason=reason)

    pending = export_inputs.select(store, data.catalog_generation(), set(refs["lineage"]), recover_lineage=True)
    finished = public.get("historical_label_interval")
    if finished:
        if pending:
            raise RecoveryRequired("historical label selection awaits owning maintenance")
        return result(
            "already_projected" if finished["cut"] == pivot else "unsupported",
            "a dated label interval is already selected",
        )
    adjacent = surrounding_market_sessions(pivot)
    if not adjacent:
        return result("unsupported", "the selected date lacks adjacent exchange sessions")
    prior_day = adjacent[0]
    selected = {
        token: item for token, item in occupation_pivots.select(store, prior_day).items() if item[1] != pid
    }
    if not selected:
        if pending:
            raise RecoveryRequired("changed lineage requires owning maintenance")
        return result("unsupported", "no completed earlier literal occupation ends on the preceding session")
    labels = {r["ticker"] for r in public["ticker_ranges"] if r["first_session"] <= pivot <= r["last_session"]}
    if len(labels) != 1:
        return result("unsupported", "the selected successor session has ambiguous trading labels")
    ticker = next(iter(labels))
    bind = dict(
        kind="displaced_label",
        version=1,
        store=str(store),
        public_id=pid,
        cut=pivot,
        source_decision=group["decision_generation"],
        references=refs,
        occupation_tokens=sorted(selected),
    )
    borrowed = {}
    with ExitStack() as stack:
        steps = stack.enter_context(review_steps(store, bind, completed_only=bool(pending)))
        if not steps:
            raise RecoveryRequired("changed lineage requires owning label-publication recovery")
        get, put = steps
        decision = get("decision")
        if pending and not decision:
            raise RecoveryRequired("another publication changed the selected label inputs")
        if decision is None:
            snapshot = None
            io_cache = {}

            def load(lo, hi):
                nonlocal snapshot
                if snapshot is None:
                    snapshot = stack.enter_context(
                        data._reader._open_group(group["locator"], expected=refs, export_selection="acquisition")
                    )
                    borrowed.update(snapshot._selected_receipts)
                return snapshot.bounded_acquisition_views(pid, lo, hi, modes=("raw",), cache=io_cache)["raw"][
                    "bars"
                ]

            def source(token):
                value = get("occupation:" + token)
                if value is None:
                    locator, owner, entry = selected[token]
                    value = put(
                        "occupation:" + token,
                        occupation_pivots.selected_descriptor(store, locator, owner, token, entry),
                    )

                def read():
                    from .occupation_storage import earlier_rows

                    # Immutable provider operands come from the selected journal,
                    # including historical observations the old price owner released.
                    return earlier_rows(store, value)

                return value, read

            def occupied(symbol, lo, hi):
                peer = catalogue.ticker_group_token(
                    store, data.catalog_generation(), symbol, record["group_key"]
                )
                return any(
                    r["first_session"] <= hi and r["last_session"] >= lo
                    for r in catalogue.other_ticker_ranges(store, symbol, pid, record["group_key"], peer)
                )

            end = min(security.last_session, date.fromisoformat(pivot) + timedelta(days=14)).isoformat()
            decision = prices.assess(
                ticker,
                security.first_session.isoformat(),
                pivot,
                prior_day,
                end,
                selected,
                load,
                source,
                occupied,
                client,
                get,
                put,
            )
            decision = put("decision", {**decision, "token": uuid4().hex})
    if decision["outcome"] != "projected":
        if pending:
            raise RecoveryRequired("another lineage publication changed the selected label")
        return result("unsupported", decision["reason"])
    item = {
        **{k: decision[k] for k in ("token", "basis_token", "occupation", "change", "witness_end")},
        "schema_version": 1,
        "public_id": pid,
        "cut": pivot,
        "source_decision": group["decision_generation"],
        "numerical_token": price_basis_limits.numerical_binding(
            public.get("dated_price_interval"), decision["change"]["first"], decision["witness_end"]
        ),
        "members": sorted(refs["pricing"]),
        "unavailable": sorted(set(refs["lineage"]) - set(refs["pricing"])),
    }
    if not historical_labels._valid(pid, item):
        raise LineageError("invalid displaced occupation label")
    publish_selection(
        store,
        group,
        item,
        pid,
        historical_labels.FIELD,
        historical_labels.TOKENS,
        previous=borrowed or None,
        pending_lineage=pending,
    )
    return group["locator"], dict(public_id=pid, cut=pivot, status="projected", assessment=item)


def review_many(store, public_ids, sessions, client):
    return selected_many(store, public_ids, sessions, client, _review)

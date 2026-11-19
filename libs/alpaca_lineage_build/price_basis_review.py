"""Coordinate a native, date-selective adjusted-price eligibility decision."""

from datetime import date
from pathlib import Path

from alpaca_end_of_day import DataLibrary
from libs.alpaca_pricing_download import price_basis_review as prices
from libs.alpaca_serving_catalogue import export_inputs
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import exclusive_owned

from . import price_basis_limits
from .contract import RecoveryRequired
from .dated_price_publication import action_window


def _review(store, pid, cut, data, client, historical_identity=None):
    security = data.get_security(pid)
    record = data._records[pid]
    group = data._groups[record["group_key"]]
    refs = group["references"]
    limits = record["public"].get("price_basis_limits", {})
    previous = limits.get(cut)
    if previous and (
        previous["reason"] in (price_basis_limits.PROXY, price_basis_limits.UNPRICED)
        or previous.get("restored_review")
    ):
        return None, dict(
            public_id=pid,
            cut=cut,
            status="unsupported",
            reason="this date has an independent quote-coverage review",
        )
    numerical = record["public"].get("dated_price_interval")
    # A numerical selection that only changes other dates cannot invalidate the review.
    first = min(
        [
            security.first_session,
            *[date.fromisoformat(v.get("original_first_session", v["witness_start"])) for v in limits.values()],
        ]
    )
    dates = None
    if previous:
        start, end = previous["witness_start"], previous["witness_end"]
    else:
        dates = prices.window(cut, first, security.last_session)
        start, end = (dates[0], dates[-1]) if dates else (cut, cut)
    token = price_basis_limits.numerical_binding(numerical, start, end)
    pending = export_inputs.select(store, data.catalog_generation(), set(refs["lineage"]), recover_lineage=True)
    if previous and not previous.get("pending") and previous.get("numerical_token") == token:
        if pending:
            raise RecoveryRequired("another lineage publication changed the selected price coverage")
        return None, dict(public_id=pid, cut=cut, status="already_reviewed", reason=previous["reason"])
    recheck = bool(previous)
    binding = dict(
        **({"historical_ticker": True} if historical_identity is not None else {}),
        schema_version=1,
        store=str(store),
        public_id=pid,
        cut=cut,
        source_decision=group["decision_generation"],
        references=refs,
        numerical_token=token,
        recheck=recheck,
    )
    borrowed = {}

    def load():
        nonlocal dates
        if dates is None:
            dates = prices.window(cut, first, security.last_session)
        if not dates:
            return {"dates": []}
        with data._reader._open_group(
            group["locator"], expected=refs, export_selection="acquisition"
        ) as snapshot:
            borrowed.update(snapshot._selected_receipts)
            cache = {}
            views = snapshot.bounded_acquisition_views(pid, dates[0], dates[-1], modes=("raw",), cache=cache)
            if cut not in dates[1:-1]:
                return {"dates": []}
            previous_day = dates[dates.index(cut) - 1]
            views.update(
                snapshot.bounded_acquisition_views(pid, previous_day, cut, modes=prices.MODES[1:], cache=cache)
            )
            actions = {}
            for receipt in borrowed.values():
                for a in action_window(receipt["evidence"], dates[0], dates[-1]):
                    key = a.get("id")
                    if not key or (key in actions and a != actions[key]):
                        return {"dates": []}
                    actions[key] = a
        return dict(dates=dates, views=views, actions=list(actions.values()))

    verdict = prices.review(
        store,
        binding,
        load,
        client,
        completed_only=bool(pending),
        **({"historical_identity": historical_identity} if historical_identity is not None else {}),
    )
    if verdict is None:
        raise RecoveryRequired("changed lineage requires owning maintenance before price coverage review")
    if verdict["outcome"] not in ("restricted", "restored"):
        if pending:
            raise RecoveryRequired("another lineage publication changed the selected price coverage")
        return None, dict(public_id=pid, cut=cut, status=verdict["outcome"], reason=verdict["reason"])
    descriptor = {
        k: verdict[k]
        for k in (
            "public_id",
            "cut",
            "previous_session",
            "symbol",
            "witness_start",
            "witness_end",
            "reason",
            "token",
            "source_decision",
            "numerical_token",
        )
    }
    descriptor["original_first_session"] = first.isoformat()
    if "published_symbol" in verdict:
        descriptor["published_symbol"] = verdict["published_symbol"]
    price_basis_limits.publish(store, group, descriptor, previous=borrowed or None, pending_lineage=pending)
    return group["locator"], dict(public_id=pid, cut=cut, status=verdict["outcome"], assessment=verdict)


def review_many(store, public_ids, sessions, client, *, historical_identity=None):
    return selected_many(store, public_ids, sessions, client, _review, historical_identity)


def selected_many(store, public_ids, sessions, client, assess, historical_identity=None):
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise RecoveryRequired("price coverage review requires the exclusive store operation")
    if not public_ids or len(public_ids) != len(sessions):
        raise ValueError("each selected public ID requires one review session")
    selected = dict.fromkeys(
        (pid, date.fromisoformat(day).isoformat()) for pid, day in zip(public_ids, sessions, strict=True)
    )
    pending = set()
    outcomes = []
    batches = []

    def flush():
        batch = maintain(store, symbols=sorted(pending))
        if batch["failed"] or not batch["complete"]:
            raise RecoveryRequired(f"price coverage decisions await serving maintenance: {batch['failures']}")
        batches.append(batch)
        pending.clear()

    for pid, cut in selected:
        data = DataLibrary._for_online_identity(store, pid)
        if pending and any(group["locator"] in pending for group in data._groups.values()):
            flush()
            data = DataLibrary._for_online_identity(store, pid)
        key, outcome = assess(store, pid, cut, data, client, historical_identity)
        outcomes.append(outcome)
        if key:
            pending.add(key)
    if pending:
        flush()
    return {
        "complete": True,
        "outcomes": outcomes,
        "serving": batches[-1] if batches else None,
        **({"prior_serving_batches": batches[:-1]} if len(batches) > 1 else {}),
        "http_requests": client.request_count
        + (historical_identity.request_count if historical_identity is not None else 0),
    }

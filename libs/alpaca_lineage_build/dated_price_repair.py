"""Coordinate a selected numerical acquisition against an accepted online group."""

from datetime import date
from pathlib import Path

from alpaca_end_of_day import DataLibrary
from libs.alpaca_pricing_download.dated_publication import acquire_selected
from libs.alpaca_serving_catalogue import export_inputs
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import exclusive_owned

from .contract import RecoveryRequired
from .dated_price_publication import publish


def _select(store, public_id, cut, client, data, *, repair_kind=None, carry_window=None):
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise RecoveryRequired("dated price acquisition requires the exclusive store operation")
    security = data.get_security(public_id)
    pid = security.public_id
    record = data._records[pid]
    group = data._groups[record["group_key"]]
    refs = group["references"]
    prior = record["public"].get("dated_price_interval")
    if prior and (repair_kind or prior["cut"] >= cut or prior.get("repair_kind")):
        from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval

        export_inputs.select(store, data.catalog_generation(), set(refs["lineage"]))
        review_day = (
            prior["first"]
            if repair_kind in ("split_timing", "split_carry", "conversion_timing")
            else prior["cut"]
        )
        if repair_kind == "retired_unit":
            review_day = prior["witness_end"]
        if repair_kind == "duplicate_split":
            review_day = prior.get("duplicate_scope", {}).get("review")
        same_scope = (
            carry_window is None or (prior.get("witness_start"), prior.get("witness_end")) == carry_window
        )
        if prior.get("repair_kind") == repair_kind and review_day == cut and same_scope:
            return None, {"status": "already_published", "interval": prior}
        if repair_kind or prior.get("repair_kind"):
            raise UnsupportedInterval("existing dated price interval requires a combined correction scope")
        raise RecoveryRequired("earlier correction would truncate an existing dated price interval")
    binding = {
        **({"repair_kind": repair_kind} if repair_kind else {}),
        **({"carry_window": carry_window} if carry_window else {}),
        "store": str(store),
        "public_id": pid,
        "cut": cut,
        "decision_generation": group["decision_generation"],
        "references": refs,
    }
    pending = export_inputs.select(store, data.catalog_generation(), set(refs["lineage"]), recover_lineage=True)
    previous = {}

    def inputs():
        # The catalogue has already established group applicability. Hold its selected
        # pricing locks while assembling the complete public input once.
        with data._reader._open_group(
            group["locator"], expected=refs, export_selection="acquisition"
        ) as snapshot:
            previous.update(snapshot._selected_receipts)
            if repair_kind in ("split_timing", "split_carry"):
                return _timing_inputs(snapshot, security, cut, carry_window)
            ranges = record["public"]["ticker_ranges"]
            if repair_kind == "retired_unit":
                from .retired_unit import inputs as retired_inputs

                return retired_inputs(snapshot, security, cut, ranges)
            if repair_kind == "conversion_timing":
                from .conversion_timing import inputs as conversion_inputs

                return conversion_inputs(snapshot, security, cut, ranges)
            if repair_kind == "duplicate_split":
                from .duplicate_split import inputs as duplicate_inputs

                return duplicate_inputs(snapshot, security, cut, ranges)
            if repair_kind in ("rename_split_basis", "former_label_basis"):
                from .rename_split_basis import inputs as basis_inputs

                return basis_inputs(snapshot, security, cut, ranges)
            carried = repair_kind == "carried_rename"
            scope = {}
            if carried:
                scope["carried"] = True
            elif repair_kind == "same_label_basis":
                scope["same_label"] = True
            views = snapshot.dated_acquisition_inputs(pid, security.first_session, cut, ranges, **scope)
            if carried:
                from .carried_rename import inputs_header

                ranges = inputs_header(snapshot, pid, ranges)
        return {"views": views, "ranges": ranges}

    interval = acquire_selected(store, binding, inputs, client, completed_only=bool(pending))
    if interval is None:
        raise RecoveryRequired("changed lineage requires owning maintenance before dated price acquisition")
    publish(store, group, interval, previous=previous or None, pending_lineage=pending)
    return group["locator"], {"status": "selected", "interval": interval}


def _timing_inputs(snapshot, security, day, carry_window=None):
    from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
    from libs.alpaca_pricing_download.shifted_split import session_window

    from .dated_price_publication import action_window

    pivot = date.fromisoformat(day)
    if not security.first_session < pivot < security.last_session:
        raise UnsupportedInterval("split review needs published sessions on both sides")
    if carry_window:
        first, last = carry_window
        if not (
            security.first_session
            <= date.fromisoformat(first)
            < pivot
            < date.fromisoformat(last)
            <= security.last_session
        ):
            raise UnsupportedInterval("carry bounds fall outside published security sessions")
    else:
        first, _, last = session_window(day)
    views = snapshot.bounded_acquisition_views(security.public_id, first, last)
    actions = []
    for receipt in snapshot._selected_receipts.values():
        for action in action_window(receipt["evidence"], first, last):
            if action not in actions:
                actions.append(action)
    inputs = {"views": views, "actions": actions}
    if carry_window:
        from .split_carry import endpoints

        codes = {action.get(name) for action in actions for name in ("old_cusip", "new_cusip")} - {None, ""}
        inputs["endpoints"] = sorted(
            {
                item
                for receipt in snapshot._selected_receipts.values()
                for item in endpoints(receipt["evidence"], codes)
            }
        )
    return inputs


def repair_many(store, public_ids, sessions, client, *, repair_kind=None, carry_windows=None):
    from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval

    if repair_kind not in (
        None,
        "split_timing",
        "split_carry",
        "conversion_timing",
        "retired_unit",
        "duplicate_split",
        "carried_rename",
        "rename_split_basis",
        "former_label_basis",
        "same_label_basis",
    ):
        raise ValueError("unknown dated price repair kind")
    if not public_ids or len(public_ids) != len(sessions):
        raise ValueError("each selected public ID requires one review session")
    if (repair_kind == "split_carry") != (carry_windows is not None):
        raise ValueError("split carry reviews require bounding traded sessions")
    windows = carry_windows if carry_windows is not None else [None] * len(sessions)
    targets = dict.fromkeys(
        (
            pid,
            date.fromisoformat(day).isoformat(),
            tuple(date.fromisoformat(v).isoformat() for v in window) if window else None,
        )
        for pid, day, window in zip(public_ids, sessions, windows, strict=True)
    )
    pending, outcomes, batches = set(), [], []

    def finish_pending():
        result = maintain(store, symbols=sorted(pending))
        if result["failed"] or not result["complete"]:
            raise RecoveryRequired(f"dated price selection awaits serving maintenance: {result['failures']}")
        batches.append(result)
        pending.clear()

    for public_id, day, window in targets:
        data = DataLibrary._for_online_identity(store, public_id)
        if pending and any(group["locator"] in pending for group in data._groups.values()):
            finish_pending()
            data = DataLibrary._for_online_identity(store, public_id)
        try:
            key, outcome = _select(
                store,
                public_id,
                day,
                client,
                data,
                **({"repair_kind": repair_kind} if repair_kind else {}),
                **({"carry_window": window} if window else {}),
            )
        except UnsupportedInterval as error:
            outcomes.append({"public_id": public_id, "cut": day, "status": "unsupported", "reason": str(error)})
            continue
        outcomes.append(outcome)
        if key:
            pending.add(key)
    if pending:
        finish_pending()
    for outcome in outcomes:
        if outcome["status"] == "selected":
            outcome["status"] = "published"
    return {
        "complete": True,
        "outcomes": outcomes,
        "serving": batches[-1] if batches else None,
        **({"prior_serving_batches": batches[:-1]} if len(batches) > 1 else {}),
        "http_requests": client.request_count if client is not None else 0,
    }

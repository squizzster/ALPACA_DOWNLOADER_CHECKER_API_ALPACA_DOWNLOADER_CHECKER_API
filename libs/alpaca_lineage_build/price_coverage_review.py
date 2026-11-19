"""Review an identity-wide price convention while retaining the accepted diagnostic history."""

from datetime import date
from pathlib import Path

from alpaca_end_of_day import StandaloneReader
from alpaca_end_of_day.standalone import _public_id
from libs.alpaca_pricing_download import price_basis_review, product_quote, untraded_split
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_pricing_download.shifted_split import session_window
from libs.alpaca_serving_catalogue import export_inputs, storage
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import exclusive_owned

from . import price_basis_limits
from .contract import RecoveryRequired
from .dated_price_publication import action_window
from .metadata import action_symbols


def review(store, public_id, day, client, *, bounds=None):
    store = Path(store).absolute()
    if not exclusive_owned(store):
        raise RecoveryRequired("product quote review requires the exclusive store operation")
    pid, cut = _public_id(public_id), date.fromisoformat(day).isoformat()
    generation, group = storage.price_review_group(store, pid)
    decision = next(item for item in group["decisions"] if item["public_id"] == pid)
    prior = decision.get("price_basis_limits", {}).get(cut)
    reason = untraded_split.UNPRICED if bounds else product_quote.PROXY
    if not decision["eligible"] and not (prior and reason in (prior["reason"], prior.get("restored_review"))):
        return dict(status="unsupported", reason="identity unavailable for product quote assessment")
    if prior and prior["reason"] != reason and prior.get("restored_review") != reason:
        return dict(status="unsupported", reason="an independent coverage disposition already owns this date")
    refs = group["references"]
    pending = export_inputs.select(store, generation, set(refs["lineage"]), recover_lineage=True)
    if (
        prior
        and bounds
        and not prior.get("pending")
        and bounds != (prior["witness_start"], prior["witness_end"])
    ):
        return dict(status="unsupported", reason="existing price interval has different accepted trade bounds")
    if prior and not prior.get("pending"):
        if pending:
            raise RecoveryRequired("changed lineage requires owning serving maintenance")
        return dict(status="already_reviewed", reason=prior["reason"], public_id=pid, cut=cut)
    prefix = next(item for item in group["history_validation"]["prefix_records"] if item["public_id"] == pid)
    numerical = next((i.get("dated_price_interval") for i in group["securities"] if i["public_id"] == pid), None)
    binding = dict(
        **({"untraded_split": list(bounds)} if bounds else {"product_quote": True}),
        schema_version=1,
        store=str(store),
        public_id=pid,
        cut=cut,
        source_decision=group["decision_generation"],
        references=refs,
        numerical_token=price_basis_limits.numerical_binding(
            numerical,
            *bounds,
        )
        if bounds
        else prefix.get("dated_price_token"),
        recheck=bool(prior),
    )
    borrowed = {}

    def select():
        if (
            bounds
            and numerical
            and numerical.get("repair_kind") in ("split_timing", "split_carry")
            and (numerical.get("witness_start"), numerical["first"], numerical["cut"])
            == (bounds[0], cut, bounds[1])
        ):
            if prior:
                return {
                    **{
                        k: prior[k]
                        for k in ("cut", "symbol", "previous_session", "witness_start", "witness_end")
                    },
                    "outcome": "restored",
                    "reason": price_basis_limits.RESTORED,
                }
            return dict(outcome="unchanged", reason="accepted numerical correction covers this split carry")
        ranges = prefix["ticker_ranges"]
        if bounds:
            if not prefix["first_session"] <= bounds[0] < cut <= bounds[1] <= prefix["last_session"]:
                return dict(
                    outcome="unsupported", reason="requested carry bounds fall outside the selected history"
                )
            ranges = [r for r in ranges if r["first_session"] <= bounds[1] and r["last_session"] >= bounds[0]]
        if len(ranges) != 1:
            return dict(outcome="unsupported", reason="product quote history crosses ticker occupations")
        symbol = ranges[0]["ticker"]
        with StandaloneReader(store)._open_group(
            group["locator"], expected=refs, export_selection="acquisition"
        ) as snapshot:
            borrowed.update(snapshot._selected_receipts)
            actions = []
            for receipt in borrowed.values():
                for a in action_window(receipt["evidence"], cut, bounds[1] if bounds else cut):
                    if symbol in action_symbols(a) and a not in actions:
                        actions.append(a)
            try:
                if bounds:
                    cache = {}
                    raw = snapshot.bounded_acquisition_views(pid, *bounds, modes=("raw",), cache=cache)["raw"][
                        "bars"
                    ]

                    if prior and prior.get("excluded_sessions"):
                        recovered = untraded_split.restored_trades(prior, raw)
                        if recovered:
                            return recovered

                    def other_views(start, end):
                        return snapshot.bounded_acquisition_views(
                            pid, start, end, modes=tuple(m for m in ADJUSTMENTS if m != "raw"), cache=cache
                        )

                    return untraded_split.plan(
                        raw, other_views, actions, symbol, cut, *bounds, prefix["last_session"]
                    )
                observed = sorted(
                    {asset for r in borrowed.values() for asset in product_quote.assets(r, pid, symbol)}
                )
                factor = product_quote.candidate(observed, actions, symbol, cut, recheck=bool(prior))
                previous = session_window(cut)[0]
                cache = {}
                raw = snapshot.bounded_acquisition_views(
                    pid, prefix["first_session"], prefix["last_session"], modes=("raw",), cache=cache
                )["raw"]["bars"]
                split = snapshot.bounded_acquisition_views(pid, previous, cut, modes=("split",), cache=cache)[
                    "split"
                ]["bars"]
                return product_quote.plan(
                    raw,
                    split,
                    symbol,
                    cut,
                    factor,
                    recheck=bool(prior),
                    named_proxy=product_quote.names_proxy(observed),
                )
            except UnsupportedInterval as exc:
                return dict(outcome="unsupported", reason=str(exc))

    result = price_basis_review.review(store, binding, select, client, completed_only=bool(pending))
    if result is None or pending and result["outcome"] not in ("restricted", "restored"):
        raise RecoveryRequired("changed lineage requires owning maintenance before product review")
    if result["outcome"] not in ("restricted", "restored"):
        return dict(status=result["outcome"], reason=result["reason"], public_id=pid, cut=cut)
    descriptor = {
        k: result[k]
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
    if result["outcome"] == "restored":
        descriptor["restored_review"] = reason
        if bounds:
            descriptor.update(excluded_sessions=prior["excluded_sessions"], terminal=prior["terminal"])
    elif bounds:
        descriptor.update(excluded_sessions=result["excluded_sessions"], terminal=result["terminal"])
    price_basis_limits.publish(store, group, descriptor, previous=borrowed or None, pending_lineage=pending)
    refresh = maintain(store, symbols=[group["locator"]])
    if refresh["failed"] or not refresh["complete"]:
        raise RecoveryRequired("product price decision awaits serving maintenance")
    return dict(status=result["outcome"], assessment=result, serving=refresh, http_requests=client.request_count)

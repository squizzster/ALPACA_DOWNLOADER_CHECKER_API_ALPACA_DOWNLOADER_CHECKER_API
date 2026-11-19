"""Lend selected native share terms and published ticker occupancy to a dated correction."""

from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval
from libs.alpaca_security_lineage.inference_evidence import provider_actions, reject_assertions

from .contract import RecoveryRequired
from .metadata import action_symbols


def selected_actions(evidence, labels, first, last=None):
    if evidence.get("event_supplements") and "provider_corporate_actions" not in evidence:
        raise RecoveryRequired("original provider share terms require metadata recovery")
    result = []
    for action in provider_actions(evidence):
        day = action.get("ex_date") or action.get("effective_date") or action.get("process_date")
        if day and first <= day and (last is None or day <= last) and action_symbols(action) & labels:
            reject_assertions(action, where="dated share-basis provider observation")
            result.append(action)
    return result


def inputs(snapshot, security, cut, ranges):
    labels = {r["ticker"] for r in ranges}
    opening = security.first_session.isoformat()
    actions = {}
    for receipt in snapshot._selected_receipts.values():
        for a in selected_actions(receipt["evidence"], labels, opening):
            key = a.get("id")
            if not key or key in actions and actions[key] != a:
                raise UnsupportedInterval("conflicting provider corporate-action records")
            actions[key] = a
    for items in snapshot._assignments.values():
        for mode, _, segment in items:
            if mode != "raw" or segment["security_identity"]["security_id"] == security.public_id:
                continue
            if segment.get("observation_selection") == "superseded":
                continue
            names = segment.get("observation_tickers", [segment["historical_symbol"]])
            if any(
                r["ticker"] in names
                and r["first_session"] <= segment["to"][:10]
                and segment["from"][:10] <= min(cut, r["last_session"])
                for r in ranges
            ):
                raise UnsupportedInterval("another selected raw history claims the same dated ticker")
    views = snapshot.dated_acquisition_inputs(security.public_id, security.first_session, cut, ranges)
    return {"views": views, "ranges": ranges, "actions": list(actions.values())}


def relevant_actions(evidence, interval):
    scope = interval["basis_action_scope"]
    actions = selected_actions(evidence, set(scope["symbols"]), interval["first"], scope["through"])
    if "spans" in scope:
        old, new, prior = scope["rename"]

        def material(a):
            day = a.get("ex_date") or a.get("effective_date") or a.get("process_date")
            if prior <= day <= interval["cut"] and action_symbols(a) & {old, new}:
                return a.get("action_type") != "cash_dividend"
            if a.get("action_type") in {"forward_split", "reverse_split"}:
                return any(a.get("symbol") == ticker and lo <= day <= hi for ticker, lo, hi in scope["spans"])
            return (
                a.get("action_type") == "name_change"
                and a.get("old_symbol") == a.get("new_symbol") == new
                and interval["cut"] < day <= scope["through"]
            )

        actions = [a for a in actions if material(a)]
    return sorted(actions, key=lambda a: (a.get("id") or "", repr(sorted(a.items()))))


def valid_scope(interval):
    from datetime import date

    if interval.get("repair_kind") != "rename_split_basis":
        return "basis_action_scope" not in interval
    scope = interval["basis_action_scope"]
    symbols = scope["symbols"]
    return (
        (
            set(scope) == {"symbols", "through"}
            or (
                set(scope) == {"symbols", "through", "spans", "rename"}
                and len(scope["rename"]) == 3
                and all(label in symbols for label in scope["rename"][:2])
                and interval["first"] <= date.fromisoformat(scope["rename"][2]).isoformat() < interval["cut"]
                and scope["spans"]
                and all(
                    len(span) == 3
                    and span[0] in symbols
                    and all(date.fromisoformat(d).isoformat() == d for d in span[1:])
                    and interval["first"] <= span[1] <= span[2] <= scope["through"]
                    for span in scope["spans"]
                )
            )
        )
        and isinstance(symbols, list)
        and bool(symbols)
        and symbols == sorted(set(symbols))
        and set(symbols) <= set(interval["members"])
        and date.fromisoformat(scope["through"]).isoformat() == scope["through"]
        and interval["witness_end"] < scope["through"]
    )

"""Select native rename observations within an already assigned price group."""

from libs.alpaca_security_lineage.inference_evidence import provider_actions, reject_assertions

from .contract import RecoveryRequired


def inputs_header(snapshot, public_id, ranges):
    labels = {run["ticker"] for run in ranges}
    actions = set()
    for receipt in snapshot._selected_receipts.values():
        evidence = receipt["evidence"]
        if evidence.get("event_supplements") and "provider_corporate_actions" not in evidence:
            raise RecoveryRequired("original provider name changes require metadata recovery")
        for action in provider_actions(evidence):
            if action.get("action_type") != "name_change":
                continue
            old, new = action.get("old_symbol"), action.get("new_symbol")
            if old in labels or new in labels:
                reject_assertions(action, where="carried rename provider action")
                actions.add((old, new, action.get("process_date"), action.get("effective_date")))
    competitors = set()
    for items in snapshot._assignments.values():
        for mode, _, segment in items:
            if mode != "raw" or segment["security_identity"]["security_id"] == public_id:
                continue
            for ticker in segment.get("observation_tickers", [segment["historical_symbol"]]):
                if ticker in labels and segment.get("observation_selection") != "superseded":
                    competitors.add((ticker, segment["from"][:10], segment["to"][:10]))
    return {
        "ranges": ranges,
        "actions": sorted(actions, key=repr),
        "competitors": sorted(competitors),
    }


def valid_changes(interval):
    from datetime import date

    changes = interval.get("label_changes")
    if interval.get("repair_kind") != "carried_rename":
        return changes is None
    if not isinstance(changes, list) or not changes:
        return False
    last = ""
    for change in changes:
        if set(change) != {"old", "new", "first", "last", "prior_session", "next_session"}:
            return False
        old, new = change["old"], change["new"]
        if old == new or old not in interval["members"] or new not in interval["members"]:
            return False
        dates = [change[k] for k in ("prior_session", "first", "last", "next_session")]
        if not (
            all(date.fromisoformat(day).isoformat() == day for day in dates)
            and dates[0] < dates[1] <= dates[2] < dates[3] <= interval["cut"]
            and last < dates[1]
            and interval["first"] <= dates[1]
        ):
            return False
        last = dates[2]
    return True


def relevant_actions(evidence, interval):
    """Only a change to the witnessed label handover can revoke its accepted tape."""
    from .dated_price_publication import action_window

    selected = []
    for change in interval["label_changes"]:
        for action in action_window(evidence, change["first"], change["next_session"]):
            if (
                action.get("action_type") == "name_change"
                and (action.get("old_symbol") == change["old"] or action.get("new_symbol") == change["new"])
                and action not in selected
            ):
                selected.append(action)
    return sorted(
        selected, key=lambda a: (a.get("process_date") or "", a.get("id") or "", repr(sorted(a.items())))
    )

"""Select a dated provider label from accepted lineage for a missing observation."""


def historical_route(receipt, day, source_generation):
    """A renamed/reused label is a mapping choice, never a history replacement.

    Consume published UUID membership and exact positive CUSIP observations. Missing
    or ambiguous membership supplies no alternative request. No correlation, price
    comparison, assignment construction or fresh metadata lookup happens here.
    """
    if receipt is None or receipt.get("source_sha256") != source_generation:
        return None
    evidence = receipt.get("evidence", {})
    lookups = evidence.get("asset_lookups", ())
    current = receipt["symbol"]
    current_ids = {
        lookup["asset"]["id"]
        for lookup in lookups
        if lookup.get("lookup_kind") == "symbol"
        and lookup.get("lookup") == current
        and lookup.get("found")
        and lookup.get("asset")
    }
    if len(current_ids) != 1:
        return None
    asset_id = next(iter(current_ids))
    memberships = [
        member
        for member in receipt.get("identity_memberships", ())
        if asset_id in member["member_alpaca_asset_ids"] and current in member.get("member_tickers", ())
    ]
    if len(memberships) != 1:
        return None
    membership = memberships[0]
    owner = membership["security_id"]
    label, seen, before = current, {current}, None
    # Only completed same-CUSIP rename observations bound to this positive
    # owner select a route. Unlike exchanges and competing predecessors do not.
    while True:
        changes = [
            event
            for event in evidence.get("symbol_changes", ())
            if event["new_symbol"] == label
            and day < event["process_date"]
            and (before is None or event["process_date"] < before)
        ]
        if not changes:
            break
        if len(changes) != 1:
            return None
        event = changes[0]
        cusip = event.get("old_cusip")
        if not cusip or cusip != event.get("new_cusip"):
            return None
        ids = {
            lookup["asset"]["id"]
            for lookup in lookups
            if lookup.get("lookup_kind") == "cusip"
            and lookup.get("lookup") == cusip
            and lookup.get("found")
            and lookup.get("asset")
        }
        if (
            ids != {asset_id}
            or event["old_symbol"] in seen
            or event["old_symbol"] not in membership.get("member_tickers", ())
        ):
            return None
        label, before = event["old_symbol"], event["process_date"]
        seen.add(label)
    if label == current:
        return None
    return {"symbol": label, "asof": day, "owner": owner, "asset_id": asset_id}

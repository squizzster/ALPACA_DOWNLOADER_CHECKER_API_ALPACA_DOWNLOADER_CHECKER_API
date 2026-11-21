"""Select a dated provider label from accepted lineage for a missing observation."""


def dated_price_scopes(indexed):
    """Lend closed own-label scopes directly from the accepted lineage selector."""
    return tuple(
        {**route, "lineage_generation": indexed["sha256"]}
        for route in indexed.get("projection", {}).get("historical_price_routes", ())
    )


def dated_routes(index, entries):
    result = {}
    for entry in entries:
        symbol = entry["symbol"]
        selected = index["symbols"].get(symbol) or {}
        generation = entry.get("source_sha256")
        if (
            generation
            and selected.get("source_sha256") == generation
            and (scopes := dated_price_scopes(selected))
        ):
            result[symbol] = scopes
    return result


def dated_route_selector(index):
    def select(symbol, source_generation):
        entry = index["symbols"].get(symbol) or {}
        return (
            dated_price_scopes(entry)
            if source_generation and entry.get("source_sha256") == source_generation
            else ()
        )

    return select


def assert_current_dated_routes(root, index, generations):
    """Bind selected routes again only if the compact lineage selector advanced."""
    from . import storage
    from .contract import LineageError

    with storage.publication_lock(root):
        if (root / ".pending.json").exists():
            raise LineageError("pending lineage publication; recover it before dated pricing correction")
        if storage.index_state(root) == index.get("projection_state"):
            return
        for symbol, generation in generations.items():
            entry = storage.read_index(root, symbol)["symbols"].get(symbol, {})
            if entry.get("sha256") != generation:
                raise LineageError(f"{symbol}: dated pricing route changed before correction")


def _retired_source_asset_id(receipt, lookups, current):
    """Use the producer's last positive own-label handoff after an exact 404."""
    unavailable = [
        lookup for lookup in lookups if lookup.get("lookup_kind") == "symbol" and lookup.get("lookup") == current
    ]
    if len(unavailable) != 1 or (
        unavailable[0].get("found"),
        unavailable[0].get("outcome"),
        unavailable[0].get("http_status"),
        unavailable[0].get("asset"),
    ) != (False, "not_found", 404, None):
        return None
    held = receipt.get("last_observed_symbol_lookup")
    asset = held.get("asset") if isinstance(held, dict) else None
    if (
        not isinstance(held, dict)
        or held.get("lookup_kind") != "symbol"
        or held.get("lookup") != current
        or (held.get("found"), held.get("outcome"), held.get("http_status")) != (True, "found", 200)
        or not isinstance(asset, dict)
        or asset.get("symbol") != current
        or asset.get("class") != "us_equity"
        or not isinstance(asset.get("id"), str)
        or not asset["id"]
    ):
        return None
    return asset["id"]


def historical_route(receipt, day, source_generation):
    """A renamed/reused label is a mapping choice, never a history replacement.

    Consume published UUID membership, an exact current or held own-label lookup and
    exact positive CUSIP observations. Missing or ambiguous membership supplies no
    alternative request. No correlation, price comparison, assignment construction
    or fresh metadata lookup happens here.
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
    if len(current_ids) == 1:
        asset_id = next(iter(current_ids))
    else:
        asset_id = _retired_source_asset_id(receipt, lookups, current)
        if current_ids or asset_id is None:
            return None
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


def route_selector(root, index):
    """Lend selected receipt facts lazily and at most once per source."""
    from . import storage

    receipts = {}

    def select(symbol, day, source_generation):
        indexed = index["symbols"].get(symbol)
        if (
            indexed is None
            or indexed.get("source_sha256") != source_generation
            or not isinstance(indexed.get("sha256"), str)
            or not indexed["sha256"]
        ):
            return None
        if symbol not in receipts:
            receipts[symbol] = storage.read_receipt(root, symbol, index)
        route = historical_route(receipts[symbol], day, source_generation)
        if route is not None:
            route["lineage_generation"] = indexed["sha256"]
        return route

    return select

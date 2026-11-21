"""Select older lineage decisions affected by a missing historical comparison."""

from libs.alpaca_end_of_day_security_correlation.stock_merger_occupation import DEPARTURE_VERSION
from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp

from .contract import MAX_CLOSURE


def select_reviews(accepted, work, graph, excluded, end):
    """Use compact retry and publication boundaries to select receipt reads."""
    entries = accepted["index"]["symbols"]
    candidates = {}
    for symbol, item in work.items():
        if symbol in excluded or not item.get("required_history_check"):
            continue
        prior = accepted["state"]["work"].get(symbol, {})
        outcome = prior.get("outcome") or {}
        entry = entries.get(symbol, {})
        if (
            outcome.get("stage") == "history_check"
            and outcome.get("reason") == "missing live comparison bar"
            and not item.get("pricing_correction")
            and entry
            and not entry.get("projection", {}).get("historical_price_routes")
            and symbol not in accepted["changed_rules"]
            and entry["versions"]["correlation"] < DEPARTURE_VERSION
        ):
            candidates[symbol] = outcome
    if not candidates:
        return {}
    reviews = {}
    for baseline in accepted["summary"]["entries"]:
        symbol = baseline["symbol"]
        if symbol not in candidates:
            continue
        entry = entries[symbol]
        outcome = candidates[symbol]
        if (
            baseline.get("source_sha256") != entry["source_sha256"]
            or outcome.get("source_sha256", entry["source_sha256"]) != entry["source_sha256"]
            or baseline["end_exclusive"][:10] > end
            or baseline.get("tail_all") is None
        ):
            continue
        first, *tail = [
            timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
            for row in (baseline["first_all"], *baseline["tail_all"])
        ]
        latest = tail[-1] if tail else first
        endpoints = {first, *tail}
        missing = {
            endpoint.get("session")
            for endpoint in outcome.get("missing_endpoints", ())
            if endpoint.get("symbol") == symbol and endpoint.get("asof") == baseline["asof"]
        }
        if not missing or not missing <= endpoints or latest in missing:
            continue
        group = tuple(entry["group_members"])
        if (
            len(group) > MAX_CLOSURE
            or set(group) != graph.closure({symbol})
            or any(
                name in excluded
                or any(
                    work.get(name, {}).get(flag)
                    for flag in ("acquire", "refresh_metadata", "invalidate_pricing", "pricing_correction")
                )
                for name in group
            )
        ):
            continue
        reviews[symbol] = {"latest": latest, "missing": sorted(missing), "members": list(group)}
    return reviews


def _could_have_departed(receipt, indexed, review):
    """Screen for the obsolete open own-label assignment before reading prices."""
    symbol = receipt["symbol"]
    lookup = indexed["projection"].get("last_observed_symbol_lookup") or {}
    active = lookup.get("asset") or {}
    if (lookup.get("found"), lookup.get("http_status"), lookup.get("lookup")) != (True, 200, symbol) or (
        active.get("symbol"),
        active.get("status"),
        active.get("tradable"),
    ) != (symbol, "active", True):
        return False
    intervals = [
        interval
        for instant, interval in receipt.get("observation_routes", {}).values()
        if instant[:10] == review["latest"]
        and interval.get("historical_symbol") == symbol
        and interval.get("asset_id") != active.get("id")
        and interval.get("start_date_inclusive")
        and interval.get("end_date_exclusive") is None
    ]
    return any(
        action.get("action_type") == "stock_merger"
        and action.get("acquiree_symbol") == symbol
        and action.get("acquiree_cusip") == prior.get("cusip")
        and action.get("acquirer_symbol") in review["members"]
        and action.get("acquirer_symbol") != symbol
        and any(
            prior["start_date_inclusive"] <= day < (action.get("effective_date") or "") <= review["latest"]
            for day in review["missing"]
        )
        for prior in intervals
        for action in receipt["evidence"]["corporate_actions"]
    )


def review_routes(store, reviews, index, *, context, progress):
    """Give the lineage producer exact accepted groups and no provider client."""
    from . import storage
    from .contract import failure_outcome
    from .pipeline import run_lineage
    from .rebuild import RetainedOnlyClient
    from .retained_recovery import RECOVERY_BOUNDARY, RECOVERY_VERSION, accept_selection

    root = context.accept_store().root
    groups, failures, seen = [], [], set()
    with storage.publication_lock(root):
        storage.assert_index_current(root, index)
        for symbol, review in reviews.items():
            members = review["members"]
            if seen.intersection(members):
                continue
            entry = index["symbols"][symbol]
            try:
                receipt = storage.read_receipt(root, symbol, index)
                if not _could_have_departed(receipt, entry, review):
                    continue
            except (RuntimeError, OSError, ValueError) as error:
                failures.append(
                    {
                        "symbol": symbol,
                        "affected": members,
                        "status": "failed",
                        "error": str(error),
                        "outcome": failure_outcome(error),
                    }
                )
                seen.update(members)
                continue
            sources = {name: index["symbols"][name] for name in members if name in index["symbols"]}
            if any(item["group_members"] != members for item in sources.values()):
                continue
            groups.append(
                {
                    "members": members,
                    "pricing_generations": {name: item["source_sha256"] for name, item in sources.items()},
                    "receipt_generations": {name: item["sha256"] for name, item in sources.items()},
                }
            )
            seen.update(members)
    if not groups:
        return {"results": failures, "failed": len(failures), "recomputed_groups": 0}
    policy = context.accept_correlation_policy()
    selection = accept_selection(
        {
            "schema_version": RECOVERY_VERSION,
            "boundary": RECOVERY_BOUNDARY,
            "store": str(store),
            "correlation_policy_sha256": policy.semantic_sha256,
            "groups": groups,
        },
        store,
    )
    result = run_lineage(
        store,
        [group.seed for group in selection.groups],
        identity_client=RetainedOnlyClient(),
        context=context,
        retained_rebuild=True,
        retained_recovery=selection,
        progress=progress,
    )
    result["results"].extend(failures)
    result["failed"] += len(failures)
    return result

"""Completed pricing selections retained for the lifetime of unfinished group work."""

from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.download import acquisition_asof

from . import universe_state
from .contract import LineageError


def accept_empty_suffixes(policy, groups, results):
    """An existing group obligation owns the result even when the peer has no work row."""
    updates = {}
    for result in results:
        peer = result["symbol"]
        anchors = groups[peer].intersection(policy.work)
        if not anchors:
            continue
        owner = peer if peer in anchors else min(anchors)
        item = policy.work[owner]
        receipt = {
            "version": 1,
            "obligation_id": item["obligation_id"],
            "owner": owner,
            "group": sorted(groups[peer]),
            "source_sha256": result["source_sha256"],
            **result["append_work_selection"],
        }
        if item.get("dependency_suffixes", {}).get(peer) != receipt:
            updates.setdefault(owner, {})[peer] = receipt
    if updates:
        universe_state.accept_dependency_suffixes(policy.store, updates)
        for owner, completed in updates.items():
            item = policy.work[owner]
            policy.work[owner] = {
                **item,
                "dependency_suffixes": {**item.get("dependency_suffixes", {}), **completed},
            }


def transfer_unfinished_suffixes(store, batch):
    """Keep a selected peer's result when its first group owner finishes before a sibling."""
    updates = {}
    for suffixes in batch.released_suffixes:
        for peer, receipt in suffixes.items():
            for member in dict.fromkeys((peer, *receipt["group"])):
                item = batch.work.get(member)
                if not item or item.get("status") == "complete" or member in batch.completed:
                    continue
                held = item.get("dependency_suffixes", {}).get(peer)
                if held is None or held.get("obligation_id") != item["obligation_id"]:
                    updates.setdefault(member, {})[peer] = {
                        **receipt,
                        "owner": member,
                        "obligation_id": item["obligation_id"],
                    }
                break
    if updates:
        universe_state.accept_dependency_suffixes(store, updates, batch=batch)
    return updates


def validate_dependency_suffixes(item):
    """Check the selected workflow document's own schema at deserialization."""
    suffixes = item.get("dependency_suffixes", {})
    if not isinstance(suffixes, dict):
        raise LineageError("invalid retained dependency suffixes; owning recovery required")
    for peer, receipt in suffixes.items():
        if (
            not isinstance(receipt, dict)
            or type(receipt.get("version")) is not int
            or receipt["version"] != 1
            or any(
                not isinstance(receipt.get(key), str) or not receipt[key]
                for key in ("owner", "obligation_id", "source_sha256", "start", "end_exclusive", "asof")
            )
            or not isinstance(receipt.get("group"), list)
            or not all(isinstance(member, str) for member in receipt["group"])
            or peer not in receipt["group"]
            or receipt["owner"] not in receipt["group"]
        ):
            raise LineageError("unsupported retained dependency suffix; owning recovery required")


def group_empty_suffixes(group, work):
    """Return only receipts of obligations still selected for this group."""
    receipts = {}
    for owner in group.intersection(work):
        item = work[owner]
        for peer, receipt in item.get("dependency_suffixes", {}).items():
            if (
                peer not in group
                or receipt["owner"] != owner
                or receipt["obligation_id"] != item["obligation_id"]
            ):
                continue
            receipts.setdefault(peer, []).append(receipt)
    return receipts


def applicable_empty_suffix(receipts, generation, source, end):
    """Compare only the selected source and provider request binding."""
    stop = timestamp(end + "T00:00:00Z")
    start = timestamp(source["latest_request"]["end_exclusive"]).isoformat()
    asof = acquisition_asof(source, stop)
    return any(
        receipt["source_sha256"] == generation
        and receipt["start"] == start
        and receipt["end_exclusive"] == stop.isoformat()
        and receipt["asof"] == asof
        for receipt in receipts
    )

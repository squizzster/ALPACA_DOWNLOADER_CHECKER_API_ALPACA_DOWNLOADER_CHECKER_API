"""Construct an absent legacy date projection from accepted raw positions."""

from contextlib import ExitStack

from alpaca_end_of_day.standalone import _rows
from libs.alpaca_lineage_build.recovery_handoffs import _source_selector, recall
from libs.alpaca_pricing_download.contract import NEW_YORK, month_of, timestamp
from libs.alpaca_pricing_download.storage import ticker_lock

from .contract import CatalogueError
from .reuse import _identity_prefix


def legacy_dates(store, checkpoint, record, selections, partitions):
    """Own one selected timestamp assembly, never repeat historical acceptance.

    The caller retains the lineage writer and operation gates. Pricing selector
    identities were captured under the preceding pricing locks. Reacquisition
    checks only those selectors; all ownership and numerical outcomes stay closed.
    """
    public_id = record["public_id"]
    inputs = checkpoint["history_validation"]["inputs"]
    positions = {
        name: _identity_prefix(item["views"]["raw"]["ownership"], item["views"]["raw"]["rows"], public_id)
        for name, item in inputs.items()
    }
    positions = {name: runs for name, runs in positions.items() if runs}
    if not positions or any(
        name not in selections or selections[name]["generation"] != inputs[name]["pricing_generation"]
        for name in positions
    ):
        raise CatalogueError(
            "legacy date projection lacks its selected source handoff; owning recovery required"
        )
    dates, payloads = set(), {}
    with ExitStack() as locks:
        for name in sorted(positions):
            root = store / "end_of_day/pricing" / name
            locks.enter_context(ticker_lock(root, exclusive=False))
        for name in sorted(positions):
            root = store / "end_of_day/pricing" / name
            selected = selections[name]
            if (root / ".pending.json").exists() or _source_selector(store, name) != selected["selector"]:
                raise CatalogueError("legacy date projection pricing selector changed; reselect owning work")
            source, offset = selected["source"], 0
            runs = positions[name]
            through = max(end for _, end, _ in runs)
            for kind in ("completed_years", "current_year", "current_month"):
                if offset >= through:
                    break
                key = str(root), kind, source["boundaries"][kind]["sha256"]
                raw_key = ("selected_raw", *key)
                if key in partitions:
                    rows = partitions[key]["raw"]
                elif raw_key in partitions:
                    rows = partitions[raw_key][0]
                else:
                    rows = _rows(root, source, kind, "raw", payloads)
                    document_key = (
                        "selected_manifest",
                        root / source["boundaries"][kind]["manifest"],
                        name,
                        kind,
                        key[-1],
                    )
                    document = payloads.get(document_key)
                    if document is None:
                        document = recall(store, "pricing_partition_document", (name, kind, key[-1]))
                    if document is not None:
                        partitions[raw_key] = rows, document
                active = (
                    tuple(row for row in rows if month_of(row) < source["anchor_month"])
                    if kind == "current_year"
                    else rows
                )
                for lower, upper, _ in runs:
                    left, right = max(0, lower - offset), min(len(active), upper - offset)
                    if left < right:
                        dates.update(
                            timestamp(row[0]).astimezone(NEW_YORK).date().isoformat()
                            for row in active[left:right]
                        )
                offset += len(active)
    dates = tuple(sorted(dates))
    # Validate this newly constructed projection, not its accepted prices.
    if (
        not dates
        or len(dates) != record["coverage"]["session_count"]
        or dates[0] != record["first_session"]
        or dates[-1] != record["last_session"]
    ):
        raise CatalogueError("legacy date projection differs from accepted positions; owning recovery required")
    return dates

"""Compact published lookup evidence; migration consumes receipts, never prices."""

from pathlib import Path

from .contract import LineageError

PROJECTION_VERSION = 1


def last_observed_symbol_lookup(receipt):
    """Absence is not an inactive observation; keep the last positive own-label fact."""
    lookups = [
        item
        for item in receipt["evidence"]["asset_lookups"]
        if item.get("lookup_kind") == "symbol" and item.get("lookup") == receipt["symbol"]
    ]
    if len(lookups) == 1 and lookups[0].get("asset"):
        asset = lookups[0]["asset"]
        if asset.get("symbol") == receipt["symbol"] and asset.get("class") == "us_equity":
            return {
                **lookups[0],
                "catalogue_observed_at_utc": receipt["evidence"].get("catalogue_observed_at_utc"),
            }
    return receipt.get("last_observed_symbol_lookup")


def project_receipt(receipt):
    evidence = [receipt["evidence"]]
    evidence.extend(
        item["evidence"] for item in receipt.get("unavailable_pricing", {}).values() if "evidence" in item
    )
    return {
        "schema_version": PROJECTION_VERSION,
        "symbol_lookups": [
            dict(lookup)
            for item in evidence
            for lookup in item["asset_lookups"]
            if lookup.get("lookup_kind") == "symbol"
        ],
        "processed_through": receipt["processed_through"],
        "pricing_request": receipt.get("pricing_request"),
        "source_rows": receipt["source_rows"],
        "unresolved_rows": receipt["unresolved_rows"],
        "catalogue_observed_at_utc": receipt["evidence"].get("catalogue_observed_at_utc"),
        "update_obligation_id": receipt.get("update_obligation_id"),
        "unavailable_obligations": receipt.get("unavailable_obligations", {}),
        "acquisition_evidence_sha256": receipt.get("acquisition_evidence_sha256"),
        "history_correction_id": receipt.get("history_correction_id"),
        "last_observed_symbol_lookup": last_observed_symbol_lookup(receipt),
    }


def require_complete(index):
    state = index.get("projection_state")
    if (
        not state
        or state["schema_version"] != PROJECTION_VERSION
        or not state["complete"]
        or any(
            entry.get("projection", {}).get("schema_version") != PROJECTION_VERSION
            for entry in index["symbols"].values()
        )
    ):
        raise LineageError("lineage lookup projection incomplete; run alpaca-cli-eod lineage migrate-index")


def migrate_index(store, *, batch_size=128, progress=None):
    """Explicit resumable metadata-only pass over the accepted index membership."""
    from . import storage

    if type(batch_size) is not int or batch_size < 1:
        raise ValueError("migration batch size must be positive")
    store = Path(store).absolute()
    with storage.writer_lock(store) as root:
        if storage.path_at(root, ".pending.json").exists():
            raise LineageError("finish pending lineage publication before migrating the index")
        index = storage.read_index(root)
        selected = [
            symbol
            for symbol, entry in index["symbols"].items()
            if entry.get("projection", {}).get("schema_version") != PROJECTION_VERSION
            or "group_members" not in entry
        ]
        completed = 0
        for offset in range(0, len(selected), batch_size):
            updates = {}
            for symbol in selected[offset : offset + batch_size]:
                receipt = storage.read_receipt(root, symbol, index)
                updates[symbol] = {
                    **index["symbols"][symbol],
                    "projection": project_receipt(receipt),
                    "group_members": sorted(
                        set(receipt["input_sources"]) | set(receipt.get("unavailable_pricing", {}))
                    ),
                }
            with storage.publication_lock(root, exclusive=True):
                storage._update_index(root, updates)
            completed += len(updates)
            if progress:
                progress(completed, len(selected))
        with storage.publication_lock(root, exclusive=True):
            storage._update_index(root, {}, projection_complete=True)
        return {
            "indexed": len(index["symbols"]),
            "receipts_read": completed,
            "projection_complete": True,
            "pricing_payloads_read": 0,
        }

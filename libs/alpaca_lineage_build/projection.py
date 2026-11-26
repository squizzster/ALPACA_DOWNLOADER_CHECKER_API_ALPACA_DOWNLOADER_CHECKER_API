"""Compact published lookup evidence; migration consumes receipts, never prices."""

from pathlib import Path

from .contract import LineageError

PROJECTION_VERSION = 1


def assignment_handoffs_complete(receipt):
    """Consume the receipt's completed coverage without deriving its contents."""
    return bool(receipt.get("serving_ownership")) and set(receipt.get("assignment_handoffs", ())) == set(
        receipt["boundaries"]
    )


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
            stamp = lookups[0].get("observed_at_utc")
            if stamp is None and lookups[0].get("source") == "asset_catalogue":
                stamp = receipt["evidence"].get("catalogue_observed_at_utc")
            return {
                **lookups[0],
                "catalogue_observed_at_utc": stamp,
            }
    return receipt.get("last_observed_symbol_lookup")


def project_receipt(receipt):
    # Producer-owned compact facts for selection/status. Consumers must use this
    # projection instead of opening all lineage receipts to reconstruct it.
    # Observation time and last-positive status are not historical ownership proof.
    evidence = [receipt["evidence"]]
    evidence.extend(
        item["evidence"] for item in receipt.get("unavailable_pricing", {}).values() if "evidence" in item
    )
    from .exchange_changes import next_dating_time

    next_dating = min((due for item in evidence if (due := next_dating_time(item))), default=None)
    retired = next(
        (item for item in receipt["evidence"]["asset_lookups"] if item.get("lookup_kind") == "asset_id"), None
    )
    historical_price_routes = [
        {
            "owner": owner,
            "asset_id": interval["asset_id"],
            "symbol": interval["historical_symbol"],
            "start": interval["start_date_inclusive"],
            "end_exclusive": interval["end_date_exclusive"],
            "asof": instant[:10],
        }
        for owner, (instant, interval) in sorted(receipt.get("observation_routes", {}).items())
        if interval.get("historical_symbol") == receipt["symbol"]
        and interval.get("start_date_inclusive")
        and interval.get("end_date_exclusive")
        and interval["start_date_inclusive"] <= instant[:10] < interval["end_date_exclusive"]
        and any(
            rule["name"] == "stock_merger_acquiree_terminal_boundary"
            for rule in interval.get("ownership_interval_rules", ())
        )
    ]
    return {
        "schema_version": PROJECTION_VERSION,
        "computation_revision": receipt.get("computation_revision", 0),
        "symbol_lookups": [
            dict(lookup)
            for item in evidence
            for lookup in item["asset_lookups"]
            if lookup.get("lookup_kind") == "symbol"
        ],
        "processed_through": receipt["processed_through"],
        "pricing_request": receipt.get("pricing_request"),
        "source_rows": receipt["source_rows"],
        **({"listing_date_after_utc": next_dating} if next_dating else {}),
        **(
            {
                "numerical_mapping_completed": {
                    "version": 1,
                    "candidate_ids": [
                        item["candidate_id"] for item in receipt["numerical_mapping_assessments"]["decisions"]
                    ],
                }
            }
            if "numerical_mapping_assessments" in receipt
            else {}
        ),
        "unresolved_rows": receipt["unresolved_rows"],
        "catalogue_observed_at_utc": receipt["evidence"].get("catalogue_observed_at_utc"),
        "update_obligation_id": receipt.get("update_obligation_id"),
        "unavailable_obligations": receipt.get("unavailable_obligations", {}),
        "acquisition_evidence_sha256": receipt.get("acquisition_evidence_sha256"),
        "history_correction_id": receipt.get("history_correction_id"),
        "assignment_handoffs_complete": assignment_handoffs_complete(receipt),
        "last_observed_symbol_lookup": last_observed_symbol_lookup(receipt),
        **({"historical_price_routes": historical_price_routes} if historical_price_routes else {}),
        **(
            {
                "retired_uuid_assessed": True,
                "retired_uuid_lookup": {
                    key: retired[key]
                    for key in ("lookup", "outcome", "http_status", "asset", "catalogue_conflict")
                    if key in retired
                },
            }
            if retired is not None
            else {}
        ),
        "sourced_fact_applications": receipt["evidence"].get("sourced_fact_applications", {}),
        "membership_transfers_complete": receipt.get("membership_transfer_handoff_version") == 1,
        "has_membership_rebind": bool(receipt.get("membership_only_rebind")),
        "source_ownership_envelopes_complete": {
            "current_source_ownership",
            "sourced_rename_ownership",
        }
        <= receipt.keys(),
        **(
            {"closed_source_handoff_version": receipt["closed_source_handoff_version"]}
            if "closed_source_handoff_version" in receipt
            else {}
        ),
        **({"retained_recovery": receipt["retained_recovery"]} if "retained_recovery" in receipt else {}),
    }


def require_complete(index):
    # Missing coverage is an explicit migration boundary, never permission for a
    # hidden receipt crawl during planning, status or an ordinary daily update.
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

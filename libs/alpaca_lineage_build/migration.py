"""Producer-owned promotion of one semantically identical policy transition."""

from pathlib import Path
from typing import Any

from .contract import MAX_CLOSURE, LineageError, document_digest, reusable_decision, versions
from .correlation_policy_contract import (
    PRODUCTION_CORRELATION_VERSION,
    PRODUCTION_LINEAGE_VERSION,
    PRODUCTION_POLICY_SEMANTIC_SHA256,
)

MIGRATION_FROM_LINEAGE_VERSION = 33
MIGRATION_FROM_CORRELATION_VERSION = 59
MIGRATION_TO_LINEAGE_VERSION = 34
MIGRATION_TO_CORRELATION_VERSION = 60


def can_promote_observation_decision(receipt, policy):
    # This narrowly authorized transition reuses a completed decision, not a
    # general license to relabel old policies. DO NOT extend it to other versions
    # or open prices to re-prove the exact handoff it was designed to preserve.
    """Prove only the completed lineage-33/correlation-59 to 34/60 transition.

    The caller already owns a coherent selected complete group with unchanged
    pricing generations and evidence. This pure proof performs no I/O. Every
    selected group member must pass before the existing journal publishes the
    current receipt and policy provenance.
    """
    current = versions()
    if (PRODUCTION_LINEAGE_VERSION, PRODUCTION_CORRELATION_VERSION) != (
        MIGRATION_TO_LINEAGE_VERSION,
        MIGRATION_TO_CORRELATION_VERSION,
    ) or (current["lineage"], current["correlation"]) != (
        MIGRATION_TO_LINEAGE_VERSION,
        MIGRATION_TO_CORRELATION_VERSION,
    ):
        return False
    # This proof runs only after OperationContext has recovered frozen journals.
    # Import the executable authorizer here so importing pipeline/recovery stays
    # independent of PyYAML, while direct callers still cannot forge behavior by
    # replacing fields on an otherwise canonical policy object.
    from .correlation_policy import authorize_production_policy

    try:
        authorize_production_policy(policy)
    except RuntimeError:
        return False
    if policy.semantic_sha256 != PRODUCTION_POLICY_SEMANTIC_SHA256:
        return False

    prior_versions = {
        **current,
        "lineage": MIGRATION_FROM_LINEAGE_VERSION,
        "correlation": MIGRATION_FROM_CORRELATION_VERSION,
    }
    prior_versions.pop("correlation_policy_sha256", None)
    return (
        isinstance(receipt, dict)
        and receipt.get("schema_version") == MIGRATION_FROM_LINEAGE_VERSION
        and receipt.get("versions") == prior_versions
        and "correlation_policy" not in receipt
    )


def _legacy_versions() -> dict[str, Any]:
    prior = {
        **versions(),
        "lineage": MIGRATION_FROM_LINEAGE_VERSION,
        "correlation": MIGRATION_FROM_CORRELATION_VERSION,
    }
    prior.pop("correlation_policy_sha256", None)
    return prior


def _entry_state(entry: Any) -> str | None:
    if reusable_decision(entry):
        return "current"
    if (
        isinstance(entry, dict)
        and entry.get("versions") == _legacy_versions()
        and "correlation_policy" not in entry
    ):
        return "legacy"
    return None


def _selector_groups(index: dict[str, Any]) -> list[tuple[str, ...]]:
    entries = index.get("symbols")
    if not isinstance(entries, dict) or not entries:
        raise LineageError("policy migration requires a nonempty completed lineage selector")
    groups: set[tuple[str, ...]] = set()
    for symbol, entry in sorted(entries.items()):
        members = entry.get("group_members") if isinstance(entry, dict) else None
        if (
            not isinstance(members, list)
            or not 1 <= len(members) <= MAX_CLOSURE
            or members != sorted(set(members))
            or any(not isinstance(item, str) or not item for item in members)
            or symbol not in members
        ):
            raise LineageError(f"{symbol}: policy migration requires exact complete-group membership")
        groups.add(tuple(members))
    for group in groups:
        available = [symbol for symbol in group if symbol in entries]
        if not available or any(
            tuple(entries[symbol].get("group_members", ())) != group for symbol in available
        ):
            raise LineageError(f"{group[0]}: policy migration selector group members disagree")
    return sorted(groups)


def _legacy_group_receipts(root, index, group, policy):
    """Read and prove one exact legacy group without opening prices or boundaries."""
    from . import storage
    from .acquisition import unavailable_pricing

    available = tuple(symbol for symbol in group if symbol in index["symbols"])
    receipts = {symbol: storage.read_receipt(root, symbol, index) for symbol in available}
    if any(receipt is None for receipt in receipts.values()):
        raise LineageError(f"{group[0]}: selected legacy receipt is missing")
    first = receipts[available[0]]
    input_sources = first.get("input_sources")
    unavailable = unavailable_pricing(first)
    if (
        not isinstance(input_sources, dict)
        or set(input_sources) != set(available)
        or any(not isinstance(value, str) or len(value) != 64 for value in input_sources.values())
        or not isinstance(unavailable, dict)
        or set(unavailable) != set(group) - set(available)
    ):
        raise LineageError(f"{group[0]}: legacy receipt does not cover its exact selected group")
    for symbol, receipt in receipts.items():
        entry = index["symbols"][symbol]
        if (
            not can_promote_observation_decision(receipt, policy)
            or receipt.get("symbol") != symbol
            or receipt.get("input_sources") != input_sources
            or unavailable_pricing(receipt) != unavailable
            or receipt.get("source_sha256") != input_sources[symbol]
            or entry.get("source_sha256") != input_sources[symbol]
        ):
            raise LineageError(f"{symbol}: exact lineage-33/correlation-59 promotion is unavailable")
    return receipts, input_sources, unavailable


def _locked_sources_for_migration(store, group, input_sources, unavailable, locked):
    """Validate selected source generations and absences without decoding payloads."""
    from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path

    from .acquisition import check_unavailable_pricing

    if set(locked) != set(group):
        raise LineageError(f"{group[0]}: policy migration could not lock the complete selected group")
    sources = {}
    for symbol in group:
        pricing_root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        if regular_path(pricing_root, ".pending.json", published=True).exists():
            raise LineageError(f"{symbol}: pricing publication is pending")
        if read_update(pricing_root, published=True) is not None:
            raise LineageError(f"{symbol}: pricing update notice is pending")
        source = load_source(pricing_root, symbol)
        if source is not None:
            sources[symbol] = source
    if set(sources) != set(input_sources):
        raise LineageError(f"{group[0]}: selected pricing availability changed")
    for symbol, expected in input_sources.items():
        if document_digest(sources[symbol]) != expected:
            raise LineageError(f"{symbol}: selected pricing generation changed")
    check_unavailable_pricing(store, unavailable, locked, sources=sources)
    return sources


def migrate_correlation_policy(store: str | Path, *, progress=None) -> dict[str, Any]:
    # Explicit offline migration owns this broad selector pass. Ordinary update
    # and reader paths must fail closed or use supported compatibility, not invoke
    # a hidden universe migration when a boundary is missing/incompatible.
    """Offline, group-atomic promotion of exact 33/59 decisions to 34/60.

    This dedicated path never imports the live lineage pipeline, constructs a
    provider client, opens a pricing payload or assignment boundary, or executes
    correlation/observation rules.
    """
    from libs.alpaca_pricing_download.check_summary import read_snapshot, summary_path
    from libs.alpaca_pricing_download.contract import DownloadError
    from libs.alpaca_pricing_download.update_checks import read_checks

    from . import storage
    from .projection import require_complete

    store = Path(store).absolute()
    failures: list[dict[str, Any]] = []
    promoted = 0
    promoted_receipts = 0
    already_current = 0
    already_current_receipts = 0
    with storage.writer_lock(store) as root:
        recovered = storage.recover(store, root)
        from .correlation_policy import load_authorized_policy

        policy = load_authorized_policy()
        checks = read_checks(store, readonly=True)
        if checks.get("complete") is not True or checks.get("epoch") is None or checks.get("checks") != {}:
            raise LineageError("policy migration requires a completed empty lineage update-check queue")
        if summary_path(store).exists():
            read_snapshot(store)
        with storage.publication_lock(root):
            index = storage.read_index(root)
            require_complete(index)
        groups = _selector_groups(index)

        def report_progress(completed):
            if progress is not None and (completed % 100 == 0 or completed == len(groups)):
                progress(completed, len(groups))

        # Classify the entire compact selector before the first publication.
        planned: list[tuple[tuple[str, ...], str | None]] = []
        for group in groups:
            states = {_entry_state(index["symbols"][symbol]) for symbol in group if symbol in index["symbols"]}
            planned.append((group, next(iter(states)) if len(states) == 1 else None))

        for completed, (group, state) in enumerate(planned, start=1):
            available_count = sum(symbol in index["symbols"] for symbol in group)
            if state == "current":
                already_current += 1
                already_current_receipts += available_count
                report_progress(completed)
                continue
            if state != "legacy":
                failures.append(
                    {
                        "group": list(group),
                        "reason": "group is neither wholly current nor exact policy-less lineage-33/correlation-59",
                    }
                )
                report_progress(completed)
                continue
            publication_started = False
            try:
                receipts, input_sources, unavailable = _legacy_group_receipts(root, index, group, policy)
                with storage.pricing_locks(store, group, known=input_sources) as locked:
                    sources = _locked_sources_for_migration(store, group, input_sources, unavailable, locked)
                    current_receipts = {
                        symbol: {
                            **receipt,
                            "schema_version": PRODUCTION_LINEAGE_VERSION,
                            "versions": versions(),
                            "correlation_policy": policy.provenance(),
                        }
                        for symbol, receipt in receipts.items()
                    }
                    publication_started = True
                    storage.publish(
                        store,
                        root,
                        index,
                        current_receipts,
                        {symbol: {} for symbol in current_receipts},
                        {},
                        checks=checks,
                        previous=receipts,
                        sources=sources,
                    )
            except (DownloadError, LineageError, OSError, ValueError) as error:
                if publication_started:
                    raise
                failures.append({"group": list(group), "reason": str(error)})
                report_progress(completed)
                continue
            promoted += 1
            promoted_receipts += len(receipts)
            report_progress(completed)

        from libs.alpaca_serving_catalogue.contract import CatalogueError
        from libs.alpaca_serving_catalogue.work import read as read_serving_work

        try:
            pending_serving = read_serving_work(store)
        except CatalogueError:
            pending_serving = None
            serving_action = "bootstrap_required"
        else:
            serving_action = "pending_update" if pending_serving else "none"

    return {
        "schema_version": 1,
        "boundary": "alpaca_lineage_policy_migration",
        "complete": not failures,
        "selected": len(groups),
        "promoted": promoted,
        "already_current": already_current,
        "failed": len(failures),
        "selected_groups": len(groups),
        "selected_receipts": len(index["symbols"]),
        "unavailable_group_count": sum(
            any(symbol not in index["symbols"] for symbol in group) for group in groups
        ),
        "promoted_groups": promoted,
        "promoted_receipts": promoted_receipts,
        "already_current_groups": already_current,
        "already_current_receipts": already_current_receipts,
        "failed_groups": len(failures),
        "failure_examples": failures[:20],
        "failures_truncated": len(failures) > 20,
        "recovered_publication": recovered,
        "policy": policy.provenance(),
        "serving_catalogue_action": serving_action,
        "serving_pending_symbols": None if pending_serving is None else len(pending_serving),
        "provider_requests": 0,
        "identity_rule_executions": 0,
        "pricing_payload_reads": 0,
        "pricing_writes": 0,
    }

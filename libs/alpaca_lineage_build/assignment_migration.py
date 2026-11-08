"""Explicit bounded assembly of legacy assignments into their compact handoffs."""

from copy import deepcopy
from pathlib import Path

from libs.common.debug_trace import traced

from . import storage
from .acquisition import unavailable_pricing
from .assignment_handoff import selected
from .contract import LineageError, reusable_decision
from .migration import _locked_sources_for_migration
from .projection import require_complete


@traced("assignment_handoff_migration")
def migrate(store, symbols=None, *, all_published=False, limit=128, progress=None):
    """Read only missing selected bodies, publish once through the owning journal.

    No pricing payloads, provider requests, correlation rules or numerical serving
    decisions are inputs. Complete groups retain their decisions and input tokens.
    The compact index records completion so retries skip published groups entirely.
    """
    if bool(symbols) == bool(all_published):
        raise LineageError("select symbols or explicitly select all published lineage")
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise LineageError("assignment migration limit must be between 1 and 10000 groups")
    store = Path(store).absolute()
    completed = current = reads = remaining = 0
    queue_ready = False
    with storage.writer_snapshot(store) as view:
        recovered = storage.recover(store, view.root)
        index = view.index()
        require_complete(index)
        if symbols and set(symbols) - index["symbols"].keys():
            raise LineageError("assignment migration requires published lineage symbols")
        entries = index["symbols"]
        groups = sorted({tuple(entries[s]["group_members"]) for s in (entries if all_published else symbols)})
        for number, group in enumerate(groups, 1):
            members = set(group) & entries.keys()
            if all(entries[s]["projection"].get("assignment_handoffs_complete") for s in members):
                current += 1
            elif completed >= limit:
                remaining += 1
            else:
                if any(not reusable_decision(entries[s]) for s in members):
                    raise LineageError("assignment migration requires the authorized current policy")
                receipts = {s: storage.read_receipt(view.root, s, index) for s in sorted(members)}
                needed = {
                    symbol: {kind for kind in receipt["boundaries"] if selected(receipt, kind) is None}
                    for symbol, receipt in receipts.items()
                }
                # A marker-only index transition changes no receipt generation
                # and produces no serving handoff, so it needs no queue upgrade.
                if any(needed.values()) and not queue_ready:
                    from libs.alpaca_serving_catalogue.work import upgrade

                    upgrade(store)
                    queue_ready = True
                first = next(iter(receipts.values()))
                with storage.pricing_locks(store, group, known=members) as locked:
                    sources = _locked_sources_for_migration(
                        store, group, first["input_sources"], unavailable_pricing(first), locked
                    )
                    boundaries = {}
                    for symbol, receipt in receipts.items():
                        kinds = needed[symbol]
                        boundaries[symbol] = (
                            storage.read_boundaries(view.root, receipt, kinds=kinds) if kinds else {}
                        )
                        reads += len(kinds)
                    storage.publish(
                        store,
                        view.root,
                        index,
                        deepcopy(receipts),
                        boundaries,
                        {},
                        previous=receipts,
                        sources=sources,
                        unchanged_decision=True,
                        projection_only=True,
                    )
                completed += 1
            if progress:
                progress(number, len(groups))
    return {
        "complete": True,
        "failed": 0,
        "selected_groups": len(groups),
        "migrated_groups": completed,
        "remaining_groups": remaining,
        "already_current_groups": current,
        "assignment_body_reads": reads,
        "pricing_payload_reads": 0,
        "provider_requests": 0,
        "recovered_publication": recovered,
    }

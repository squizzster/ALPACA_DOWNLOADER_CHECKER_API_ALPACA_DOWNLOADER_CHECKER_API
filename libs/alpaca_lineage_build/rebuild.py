"""Explicit retained-input lineage reconstruction after a computation repair."""

from contextlib import nullcontext
from pathlib import Path

from libs.common.debug_trace import event, traced

from . import storage, universe_state
from .context import accept_context, recover_and_accept_correlation_policy
from .contract import COMPUTATION_REVISION, LineageError, reusable_decision
from .projection import require_complete
from .selection import DependencyGraph


class RetainedOnlyClient:
    """Missing retained evidence is a blocker, never a provider request."""

    request_count = 0

    def get_asset_lookup(self, identifier):
        raise LineageError(f"retained metadata missing for {identifier}; explicit metadata recovery required")

    def get_corporate_actions(self, symbol, start, end):
        raise LineageError(f"{symbol}: retained action coverage missing for {start}..{end}")


@traced("lineage_rebuild")
def rebuild(store, *, all_published=False, plan=False, progress=None):
    """Recompute selected obsolete decisions, preserving upstream acceptance.

    A completed current revision is the durable retry handoff. No new work-state
    obligation, metadata observation, pricing check or universe census is needed.
    """
    if all_published is not True:
        raise LineageError("lineage rebuild requires explicit --all-published scope")
    store = Path(store).absolute()
    context = accept_context(store)
    universe_state.require_initialized(store)
    with universe_state.update_lock(store) if not plan else nullcontext():
        if not plan:
            recover_and_accept_correlation_policy(store, context)
        else:
            context.accept_correlation_policy()
        root = context.accept_store().root
        with storage.publication_lock(root):
            if storage.path_at(root, ".pending.json").exists():
                raise LineageError("lineage publication pending; resume owning recovery")
            index = context.select_index(root)
            require_complete(index)
        graph = DependencyGraph(index["symbols"])
        selected = sorted(name for name, entry in index["symbols"].items() if not reusable_decision(entry))
        visited = set()
        seeds = []
        for symbol in selected:
            if symbol not in visited:
                visited.update(graph.closure({symbol}))
                seeds.append(symbol)
        scope = {
            "computation_revision": COMPUTATION_REVISION,
            "published_symbols": len(index["symbols"]),
            "selected_symbols": len(selected),
            "selected_groups": len(seeds),
            "already_current_symbols": len(index["symbols"]) - len(selected),
            "provider_requests": 0,
            "pricing_writes": 0,
            "input_scope": "retained published prices and metadata at their accepted individual ranges",
        }
        event("lineage_rebuild_scope", **scope)
        if progress:
            progress("lineage_rebuild_scope", {"state": "started", **scope})
        if plan:
            return {"mode": "plan", **scope}
        from .pipeline import run_lineage

        completed = set()

        def observed(symbols):
            completed.update(set(symbols) & index["symbols"].keys())

        def relay(stage, values):
            if progress is None:
                return
            if stage in {"lineage_group", "lineage_complete"}:
                values = {**values, "completed": len(completed), "selected": len(selected)}
            progress(stage, values)

        result = run_lineage(
            store,
            seeds,
            identity_client=RetainedOnlyClient(),
            pricing_client=None,
            context=context,
            retained_rebuild=True,
            progress=relay,
            on_processed=observed,
        )
        # The publisher updated this coherent index. Its revision is the
        # completed handoff; do not reopen receipts to authenticate completion.
        index = context.index
        remaining = sorted(name for name, entry in index["symbols"].items() if not reusable_decision(entry))
        result.update(scope)
        result["remaining_symbols"] = remaining
        result["complete"] = not result["failed"] and not remaining
        result["recomputed_symbols"] = sorted(
            {symbol for item in result["results"] for symbol in item.get("processed", ())}
        )
        result["assignment_projection_complete_symbols"] = sum(
            bool(entry.get("projection", {}).get("assignment_handoffs_complete"))
            for entry in index["symbols"].values()
        )
        from libs.alpaca_serving_catalogue import storage as serving_storage
        from libs.alpaca_serving_catalogue.producer import maintain

        if result["complete"] and serving_storage.path(store).exists():
            serving = maintain(store, rebuild=True, context=context, progress=progress)
            result["serving_catalogue"] = serving
            result["failed"] += serving["failed"]
            result["complete"] = result["complete"] and serving["complete"]
        if progress:
            progress(
                "lineage_rebuild_complete",
                {
                    "complete": result["complete"],
                    "processed": len(result["recomputed_symbols"]),
                    "selected": len(selected),
                    "failed": result["failed"],
                    "provider_requests": 0,
                },
            )
        return result

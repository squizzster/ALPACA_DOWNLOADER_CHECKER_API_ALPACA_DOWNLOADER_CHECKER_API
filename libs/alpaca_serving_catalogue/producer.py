"""Offline bootstrap and bounded recovery of complete affected serving groups."""

from contextlib import ExitStack
from pathlib import Path

from libs.alpaca_lineage_build import storage as lineage

from . import storage, work
from .contract import CatalogueError, policy
from .policy import evaluate


def _index(store):
    lineage.check_store(store)
    root = store / "end_of_day/lineage"
    if not (root / "index.sqlite3").exists():
        raise CatalogueError("lineage index missing; publish lineage before catalogue bootstrap")
    with lineage.publication_lock(root):
        if (root / ".pending.json").exists():
            raise CatalogueError("lineage publication pending; resume its owner first")
        return lineage.read_index(root)


def _components(entries, pending, controls):
    """Use published current membership and previous contributions only."""
    done = set()
    members, groups = controls["members"], controls["groups"]
    for seed in sorted(pending):
        if seed in done:
            continue
        selected = {seed}
        while True:
            expanded = set(selected)
            for symbol in tuple(expanded):
                expanded.update(entries.get(symbol, {}).get("group_members", ()))
                key = members.get(symbol)
                if key is not None:
                    expanded.update(groups[key]["references"]["lineage"])
            if expanded == selected:
                break
            selected = expanded
        done.update(selected)
        yield selected


def _matches(group, entries):
    return group["policy"] == policy() and all(
        (entries[name]["sha256"] if name in entries else None) == expected
        for name, expected in group["references"]["lineage"].items()
    )


def preview(store, *, bootstrap=False, rebuild=False):
    """No writes/recovery, no receipts or payloads, and no provider access."""
    store = Path(store).absolute()
    index = _index(store)
    blockers = []
    try:
        controls = storage.controls(store, incomplete=bootstrap or rebuild, old_policy=rebuild)
    except CatalogueError as error:
        if not (bootstrap or rebuild) or storage.path(store).exists():
            blockers.append(str(error))
        controls = {"groups": {}, "members": {}}
    if bootstrap or rebuild:
        pending = {s: e["sha256"] for s, e in index["symbols"].items()}
    else:
        try:
            pending = work.read(store)
        except CatalogueError as error:
            blockers.append(str(error))
            pending = {}
    components = list(_components(index["symbols"], pending, controls))
    selected = set().union(*components) if components else set()
    indexed = selected & index["symbols"].keys()
    membership_reads = sum("group_members" not in index["symbols"][symbol] for symbol in indexed)
    return {
        "mode": "plan",
        "operation": "rebuild" if rebuild else "bootstrap" if bootstrap else "update",
        "selected_symbols": sorted(selected),
        "affected_components": None if membership_reads else len(components),
        "membership_projection_receipts": membership_reads,
        "indexed_sources": len(indexed),
        "receipt_reads_upper_bound": len(indexed) + membership_reads,
        "source_manifest_reads_upper_bound": len(indexed),
        "assignment_documents_upper_bound": 3 * len(indexed),
        "distinct_pricing_boundary_manifests_upper_bound": 3 * len(indexed),
        "distinct_pricing_payloads_upper_bound": 24 * len(indexed),
        "price_read_scope": "published append scopes reuse completed history checks and read only suffix-containing payloads; replacement, changed ownership or changed historical source selection reevaluates affected histories; matching decisions require no payload reads",
        "derived_publications_upper_bound": len(components) + int(bootstrap or rebuild),
        "provider_requests": 0,
        "identity_rule_executions": 0,
        "pricing_writes": 0,
        "blockers": blockers,
    }


def _project_group_members(root, index):
    """Explicit bounded bootstrap migration; receipt bodies stay batch scoped."""
    from libs.alpaca_lineage_build.acquisition import unavailable_pricing

    missing = [name for name, entry in index["symbols"].items() if "group_members" not in entry]
    for offset in range(0, len(missing), 128):
        updates = {}
        for name in missing[offset : offset + 128]:
            receipt = lineage.read_receipt(root, name, index)
            updates[name] = {
                **index["symbols"][name],
                "group_members": sorted(set(receipt["input_sources"]) | set(unavailable_pricing(receipt))),
            }
        with lineage.publication_lock(root, exclusive=True):
            index["projection_state"] = lineage._update_index(root, updates)
        index["symbols"].update(updates)
    return len(missing)


def maintain(store, *, symbols=None, bootstrap=False, rebuild=False, progress=None, context=None):
    from alpaca_end_of_day import StandaloneReader

    store = Path(store).absolute()
    scope = None if symbols is None else set(symbols)
    if scope is not None and (bootstrap or rebuild):
        raise ValueError("catalogue bootstrap and rebuild select all indexed groups")
    # This is an offline lineage consumer. Holding the worker serializes selection,
    # work seeding and acknowledgement with lineage without a universe file crawl.
    # Shared pricing locks still protect each newly evaluated group through commit.
    accepted_store = lineage.accept_store(store)
    result = {
        "complete": False,
        "failed": 0,
        "evaluated_groups": 0,
        "reused_groups": 0,
        "reused_history_prefixes": 0,
        "full_history_evaluations": 0,
        "checked_bars": 0,
        "published_components": 0,
        "failures": [],
        "provider_requests": 0,
        "identity_rule_executions": 0,
        "pricing_writes": 0,
    }
    controls = None
    if not (bootstrap or rebuild):
        # Native rollback belongs to this owner. Missing/incompatible boundaries
        # still fail before creating anything or upgrading the lineage protocol.
        controls = (
            storage.controls(store, recover=True) if context is None else context.select_serving(recover=True)
        )
    elif not (store / "end_of_day/lineage/index.sqlite3").exists():
        raise CatalogueError("lineage index missing; publish lineage before catalogue bootstrap")
    with lineage.writer_snapshot(store, accepted_store=accepted_store, context=context) as writer_view:
        root = writer_view.root
        if (root / ".pending.json").exists():
            raise CatalogueError("lineage publication pending; resume its owner first")
        if controls is not None:
            if controls["state"]["generation"] != storage.selector_state(store)["generation"]:
                controls = storage.controls(store)
            # The worker opened the index for native transaction recovery before
            # this read. An empty queue needs no membership reload or payloads.
            pending = work.read(store)
            if not pending or scope == set():
                return {
                    **result,
                    "complete": True,
                    "pending_symbols": [],
                    "generation": controls["state"]["generation"],
                }
        index = writer_view.index()
        entries = index["symbols"]
        if bootstrap or rebuild:
            storage.initialize(store, rebuild=rebuild)
            result["membership_projection_receipts"] = _project_group_members(root, index)
            with lineage.publication_lock(root, exclusive=True):
                work.seed(store, entries)
        if controls is None:
            controls = storage.controls(store, incomplete=True)
            pending = work.read(store)
        reader = StandaloneReader(store, wait=True, cache_bytes=64 * 1024 * 1024)
        selected = set()
        for component in _components(entries, pending if scope is None else scope, controls):
            if not component & pending.keys():
                continue
            selected.update(component)
            previous = {controls["members"][n] for n in component if n in controls["members"]}
            groups, covered = [], set()
            try:
                with ExitStack() as locks:
                    retained = set()
                    for key in sorted(previous):
                        prior = controls["groups"][key]
                        # This completed selector already proves the published
                        # decision. Matching references need no receipt/payload
                        # audit, and a policy rebuild never reacquires pricing.
                        if _matches(prior, entries):
                            retained.add(key)
                            covered.update(prior["references"]["lineage"])
                            result["reused_groups"] += 1
                    replaced = previous - retained
                    prior_groups = (
                        storage.read_groups(store, replaced, generation=controls["state"]["generation"])
                        if replaced
                        else {}
                    )
                    for symbol in sorted(component & entries.keys()):
                        if symbol in covered:
                            continue
                        if "group_members" not in entries[symbol]:
                            raise CatalogueError("group membership projection missing; run catalogue bootstrap")
                        members = entries[symbol]["group_members"]
                        references = {
                            "lineage": {
                                name: entries[name]["sha256"] if name in entries else None for name in members
                            },
                            "pricing": {
                                name: entries[name]["source_sha256"] for name in members if name in entries
                            },
                        }
                        group = locks.enter_context(
                            reader._open_group(
                                symbol,
                                expected=references,
                                include_unresolved=True,
                                shared_cache=True,
                                writer_view=writer_view,
                            )
                        )
                        if not set(group.references["lineage"]) <= component:
                            raise CatalogueError("published input group exceeds selected closure")
                        evaluated = evaluate(group, tuple(prior_groups.values()))
                        for counter, count in evaluated["validation_work"].items():
                            result[counter] += count
                        groups.append(evaluated)
                        covered.update(group.references["lineage"])
                        result["evaluated_groups"] += 1
                    if component & entries.keys() - covered:
                        raise CatalogueError("selected catalogue component is incomplete")
                    if replaced or groups:
                        controls["state"] = storage.commit(store, replaced, groups)
                        if context is not None:
                            context.serving = None
                        result["published_components"] += 1
                    # Commit is authoritative after interruption. Next invocation
                    # matches these input references and skips every calculation.
                    targets = {s: pending[s] for s in component if s in pending}
                    work.acknowledge(store, targets)
                if progress:
                    progress("serving_catalogue", {"state": "completed", "symbols": sorted(component)})
            except (RuntimeError, OSError, ValueError) as error:
                result["failed"] += 1
                result["failures"].append({"symbols": sorted(component), "error": str(error)})
        remaining = work.read(store)
        result["pending_symbols"] = sorted(remaining.keys() & selected if scope is not None else remaining)
        if not result["pending_symbols"] and not result["failed"]:
            state = storage.finish_bootstrap(store) if bootstrap or rebuild else controls["state"]
            result["complete"] = True
            result["generation"] = state["generation"]
    return result


def status(store, *, controls=None):
    store = Path(store).absolute()
    controls = storage.controls(store, incomplete=True) if controls is None else controls
    pending = work.read(store)
    groups = controls["groups"]
    return {
        "generation": controls["state"]["generation"],
        "coverage_complete": controls["state"]["complete"],
        "groups": len(groups),
        "online_identities": sum(g["online_identities"] for g in groups.values()),
        "ineligible_identities": sum(g["ineligible_identities"] for g in groups.values()),
        "pending_symbols": sorted(pending),
    }

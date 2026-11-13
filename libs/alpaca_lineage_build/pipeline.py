"""Single-worker dependency closure and the downloader/lineage handoff."""

from pathlib import Path

from libs.alpaca_pricing_download.acquire import MeasuredRequests, acquire_selected
from libs.alpaca_pricing_download.contract import (
    HISTORY_START,
    failure_outcome,
    symbol_label,
)
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path
from libs.alpaca_pricing_download.update_checks import (
    begin_checks,
    complete_checks,
    read_checks,
)
from libs.common.debug_trace import event, traced

from . import group_engine as _group_engine
from . import storage
from .acquisition import unavailable_pricing
from .context import accept_context
from .continuation import scoped_identity_memberships
from .contract import (
    LineageError,
    reusable_decision,
    reusable_metadata_versions,
)
from .correction_prefix import correction_from
from .metadata import (
    EvidenceClient,
    acquire,
    acquire_unpriced,
    action_requests,
    full_metadata,
)
from .migration import can_promote_observation_decision
from .observations import select_observations
from .pricing import load_pricing
from .selection import DependencyGraph
from .source_work import prepare_group_evidence_and_source_work
from .transform import correlate
from .work_contract import auxiliary_work


def _group_services():
    # Resolve the supported controller execution hooks at invocation, preserving
    # instrumentation and forbidden-work guards without carrying domain state.
    return _group_engine.GroupServices(
        current_sources=_current_sources,
        reuse_metadata=_reuse_metadata,
        acquire=acquire,
        acquire_unpriced=acquire_unpriced,
        can_promote_observation_decision=can_promote_observation_decision,
        closure=closure,
        correction_from=correction_from,
        correlate=correlate,
        download_pricing=download_pricing,
        event=event,
        finish_correlated_group=finish_correlated_group,
        load_pricing=load_pricing,
        prepare_group_evidence_and_source_work=prepare_group_evidence_and_source_work,
        read_checks=read_checks,
        read_update=read_update,
        scoped_identity_memberships=scoped_identity_memberships,
        select_observations=select_observations,
    )


PREFETCH_MEMBERS = 1000


def closure(index, seeds, asset_ids=(), *, graph=None):
    return (graph or DependencyGraph(index["symbols"])).closure(seeds, asset_ids)


@traced("pricing_selectors")
def _current_sources(store, locked, *, known=()):
    result = {}
    for symbol in sorted(locked):
        root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        if regular_path(root, ".pending.json").exists():
            raise LineageError(f"{symbol}: pricing publication incomplete; resume downloader first")
        source = load_source(root, symbol)
        if source is None and symbol in known:
            raise LineageError(f"{symbol}: published pricing receipt is missing")
        if source is not None:
            result[symbol] = source
    return result


def _reuse_metadata(seed, group, selected, previous, notices, refresh):
    # This is the complete-group reuse boundary: changed inputs, recorded
    # absence, pending notices or explicit refresh defeat reuse. On success,
    # DO NOT reacquire evidence merely because a later stage wants reassurance.
    unavailable = unavailable_pricing(previous.get(seed))
    return (
        not refresh
        and seed in selected
        and not notices
        and set(group) - selected.keys() == unavailable.keys()
        and all(
            prior is not None
            and prior["source_sha256"] == selected[symbol]
            and prior["input_sources"] == selected
            and unavailable_pricing(prior) == unavailable
            and reusable_metadata_versions(prior["versions"])
            for symbol, prior in previous.items()
        )
    )


def _published_pricing_ranges(index, group, pricing_handoff):
    """Plan from completed publications and the accepted lineage projection."""
    # Producer-returned requests and indexed coverage are already authoritative
    # for planning. DO NOT reopen every group's manifests to derive the same API
    # need; the execution owner still binds selected work under its pricing locks.
    if pricing_handoff is None:
        return None
    sources = {}
    for symbol in sorted(group):
        entry = index["symbols"].get(symbol)
        published = pricing_handoff.get(symbol)
        if entry is None and published is None:
            continue
        prior = (entry or {}).get("projection", {}).get("pricing_request")
        request = (published or {}).get("request") or prior
        if request is None:
            return None
        history_start = (published or {}).get("history_start") or (prior or {}).get("history_start")
        if published and published.get("mode") in {"initial", "full_refresh"}:
            history_start = request["start"]
        if history_start is None:
            return None
        sources[symbol] = {
            "history_start": history_start,
            "latest_request": request,
            **{
                key: published[key]
                for key in ("latest_session_correction", "history_changes")
                if published and key in published
            },
        }
    return sources


def _handoff_added_rows(publication):
    added = (publication or {}).get("added_rows")
    if not isinstance(added, dict) or not added:
        return None
    if any(type(value) is not int or value < 0 for value in added.values()):
        return None
    return any(added.values())


def _select_pending_pricing(store, members, index, pending, pricing_handoff):
    """Recover compact planning facts only for selected unfinished publications.

    There is no in-memory producer result after interruption. Select that work's
    current receipt once for planning; execution reselects under its writer locks.
    """
    selected = (set(members) & set(pending)) - pricing_handoff.keys()
    if not selected:
        return
    with storage.pricing_locks(store, selected, known=index["symbols"]) as locked:
        for symbol, source in _current_sources(store, locked, known=index["symbols"]).items():
            notice = read_update(store / "end_of_day/pricing" / symbol, published=True)
            pricing_handoff[symbol] = {
                **{
                    key: source[key] for key in ("latest_session_correction", "history_changes") if key in source
                },
                "source_sha256": storage.document_digest(source),
                "history_start": source.get("history_start", HISTORY_START),
                "request": source["latest_request"],
                "changed": notice is not None,
                "changed_from": notice["from"] if notice is not None else None,
                "pending_update": notice,
            }


@traced("metadata_prefetch")
def _prefetch_metadata(
    store,
    selected,
    index,
    graph,
    client,
    refresh,
    receipt_cache,
    end_date=None,
    work_policy=None,
    pricing_handoff=None,
):
    """Collect selected receipt ranges without opening prices or scanning directories.

    Publication still reselects pricing under its normal locks. Requests are
    cached by exact symbol/date bounds, so changed ranges cannot use old results.
    """
    if not client.supports_plans:
        return
    visited, requests, lookup_symbols = set(), set(), set()
    for label in selected:
        try:
            seed = symbol_label(label)
            if seed in visited:
                continue
            group = closure(index, {seed}, graph=graph)
            visited.update(group)
            sources = _published_pricing_ranges(index, group, pricing_handoff)
            if sources is None:
                with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
                    sources = _current_sources(store, locked, known=index["symbols"])
                    notices = {
                        s: notice
                        for s in sources
                        if (notice := read_update(store / "end_of_day/pricing" / s, published=True)) is not None
                    }
                    generations = {s: storage.document_digest(source) for s, source in sources.items()}
            else:
                notices = {}
                for symbol in sources:
                    publication = pricing_handoff.get(symbol, {})
                    notice = publication.get("pending_update")
                    if "pending_update" not in publication and publication.get("changed"):
                        notice = {
                            "from": publication.get("changed_from") or publication["request"]["start"][:10]
                        }
                    if notice is not None:
                        notices[symbol] = notice
                generations = {
                    symbol: pricing_handoff[symbol]["source_sha256"]
                    if symbol in pricing_handoff
                    else index["symbols"][symbol]["source_sha256"]
                    for symbol in sources
                }
            if end_date and any(
                source["latest_request"]["end_exclusive"][:10] > end_date for source in sources.values()
            ):
                continue
            previous = {s: receipt_cache.read(s) for s in sources}
            metadata_refresh = refresh or (
                work_policy is not None and work_policy.needs_refresh(group, previous)
            )
            history_refresh = refresh or (
                work_policy is not None and work_policy.needs_full_metadata(group, previous)
            )
            if _reuse_metadata(seed, group, generations, previous, notices, metadata_refresh):
                continue
            for symbol, source in sources.items():
                full = full_metadata(
                    previous[symbol],
                    None if correction_from(source, previous[symbol]) else notices.get(symbol),
                    history_refresh,
                )
                selected_requests = action_requests(
                    symbol,
                    source.get("history_start", HISTORY_START)[:10],
                    source["latest_request"]["end_exclusive"][:10],
                    previous[symbol],
                    refresh=full,
                )
                requests.update(selected_requests)
                added = _handoff_added_rows((pricing_handoff or {}).get(symbol))
                if full or previous[symbol] is None:
                    lookup_symbols.update(request[0] for request in selected_requests)
                elif added is not False and previous[symbol]["source_sha256"] != generations[symbol]:
                    # A source with new rows needs a current own-label observation.
                    # Historical queried labels remain covered by their completed
                    # report unless a newly returned action selects them on demand.
                    lookup_symbols.add(symbol)
                event(
                    "metadata_lookup_selection",
                    symbol=symbol,
                    selected=symbol in lookup_symbols,
                    added_rows=added,
                    source_changed=previous[symbol] is None
                    or previous[symbol]["source_sha256"] != generations[symbol],
                    historical_refresh=full,
                )
        except (RuntimeError, OSError, ValueError):
            continue  # The normal group path reports this failure without stopping unrelated work.
    client.prefetch_corporate_actions(requests)
    client.prefetch_symbols(lookup_symbols)


@traced("dependency_catchup")
def _catch_up_published_dependencies(
    store,
    selected,
    index,
    graph,
    client,
    progress=None,
    end_date=None,
    work_policy=None,
    register_discoveries=None,
    context=None,
    pricing_handoff=None,
    empty_suffixes=None,
):
    """The pricing owner validates retained history before advancing dependencies."""
    result = {
        "requested": 0,
        "succeeded": 0,
        "failed": 0,
        "results": [],
        "requests": {},
        "deferred_full_range": 0,
        "skipped": 0,
    }
    if client is None or not callable(getattr(client, "execute_plan", None)):
        return result
    visited, targets = set(), {}
    for label in selected:
        group = graph.closure({label})
        if group <= visited or (work_policy is not None and not work_policy.allowed(group)):
            continue
        visited.update(group)
        sources = _published_pricing_ranges(index, group, pricing_handoff)
        if sources is None:
            with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
                sources = _current_sources(store, locked, known=index["symbols"])
        if not sources:
            continue
        start = min(source.get("history_start", HISTORY_START)[:10] for source in sources.values())
        end = max(source["latest_request"]["end_exclusive"][:10] for source in sources.values())
        if end_date and end > end_date:
            continue
        for symbol, source in sources.items():
            if source.get("history_start", HISTORY_START)[:10] > start:
                result["deferred_full_range"] += 1
            elif source["latest_request"]["end_exclusive"][:10] < end:
                targets.setdefault(end, []).append(symbol)
    for end, symbols in targets.items():
        allowed = [s for s in symbols if work_policy is None or work_policy.allowed({s})]
        if not allowed:
            continue
        if register_discoveries is not None:
            # Group membership is already selected by the accepted lineage index.
            # Register it before any provider request; suffix absence can suppress
            # acquisition work, but it cannot erase the known dependency label.
            register_discoveries({member for symbol in allowed for member in graph.closure({symbol})})

        def persist_selected(selected_work):
            if work_policy is not None:
                for symbol in sorted(selected_work):
                    work_policy.require_dependencies(graph.closure({symbol}), end, {symbol})

        value = acquire_selected(
            store,
            allowed,
            end,
            client=client,
            progress=progress,
            context=context.pricing() if context is not None else None,
            select_append_work=True,
            on_append_work_selected=persist_selected,
        )
        result["results"].extend(value["results"])
        not_selected = {item["symbol"] for item in value["results"] if item.get("status") == "not_selected"}
        if empty_suffixes is not None:
            empty_suffixes.update(
                (
                    item["symbol"],
                    item["source_sha256"],
                    item["append_work_selection"]["end_exclusive"][:10],
                )
                for item in value["results"]
                if item.get("status") == "not_selected"
            )
        result["skipped"] += len(not_selected)
        if pricing_handoff is not None:
            pricing_handoff.update(
                (item["symbol"], item)
                for item in value["results"]
                if item.get("status") != "not_selected" and "source_sha256" in item and "request" in item
            )
        for key, count in value["requests"].items():
            if isinstance(count, dict):
                for metric, total in count.items():
                    result["requests"][metric] = result["requests"].get(metric, 0) + total
            else:
                result["requests"][key] = result["requests"].get(key, 0) + count
        result["requested"] += len(allowed)
        result["failed"] += value["failed"]
        if work_policy is not None:
            work_policy.record_results(value, stage="dependency_acquisition")
    result["succeeded"] = result["requested"] - result["failed"]
    return result


def finish_correlated_group(
    *,
    reports,
    acquisition_waves,
    affected,
    assignment_boundaries,
    checks,
    choice_prefix_sources,
    components,
    deps,
    evidence,
    graph,
    index,
    inputs,
    no_new_rows,
    notices,
    observer,
    old_boundaries,
    original_choice_inputs,
    previous,
    pricing_replacements,
    progress,
    read_boundaries,
    receipt_cache,
    required,
    resume,
    retained_recovery,
    root,
    seed,
    sources,
    store,
    unavailable,
    work_policy,
    completion=None,
):
    """Compatibility entrypoint for the complete-group engine."""
    return _group_engine.finish_correlated_group(
        reports=reports,
        acquisition_waves=acquisition_waves,
        affected=affected,
        assignment_boundaries=assignment_boundaries,
        checks=checks,
        choice_prefix_sources=choice_prefix_sources,
        components=components,
        deps=deps,
        evidence=evidence,
        graph=graph,
        index=index,
        inputs=inputs,
        no_new_rows=no_new_rows,
        notices=notices,
        observer=observer,
        old_boundaries=old_boundaries,
        original_choice_inputs=original_choice_inputs,
        previous=previous,
        pricing_replacements=pricing_replacements,
        progress=progress,
        read_boundaries=read_boundaries,
        receipt_cache=receipt_cache,
        required=required,
        resume=resume,
        retained_recovery=retained_recovery,
        root=root,
        seed=seed,
        sources=sources,
        store=store,
        unavailable=unavailable,
        work_policy=work_policy,
        services=_group_services(),
        completion=completion,
    )


def process_group(
    store,
    root,
    seed,
    *,
    identity_client,
    pricing_client=None,
    refresh=False,
    index=None,
    observer=None,
    graph=None,
    evidence_client=None,
    receipt_cache=None,
    end_date=None,
    register_discoveries=None,
    work_policy=None,
    context=None,
    progress=None,
    empty_suffixes=(),
    retained_rebuild=False,
    retained_recovery=None,
):
    """Compatibility entrypoint for the complete-group engine."""
    return _group_engine.process_group(
        store=store,
        root=root,
        seed=seed,
        identity_client=identity_client,
        pricing_client=pricing_client,
        refresh=refresh,
        index=index,
        observer=observer,
        graph=graph,
        evidence_client=evidence_client,
        receipt_cache=receipt_cache,
        end_date=end_date,
        register_discoveries=register_discoveries,
        work_policy=work_policy,
        context=context,
        progress=progress,
        empty_suffixes=empty_suffixes,
        retained_rebuild=retained_rebuild,
        retained_recovery=retained_recovery,
        services=_group_services(),
    )


def local_symbols(store: Path, *, pending: bool, known=()) -> list[str]:
    # Explicit discovery/reconstruction only. Normal selected updates use compact
    # membership/hints; DO NOT insert this census before useful steady-state work.
    from libs.common.work_scope import forbid_inventory

    forbid_inventory(store)
    directory = regular_path(store, "end_of_day/manifests" if pending else "end_of_day/pricing")
    if not directory.exists():
        return []
    result = []
    for path in sorted(directory.iterdir()):
        if path.name in known:
            result.append(path.name)
            continue  # The index already establishes this publication; no path inventory.
        if path.is_symlink() or not path.is_dir() or symbol_label(path.name) != path.name:
            raise LineageError(f"invalid ticker directory: {path}")
        filename = "update.json" if pending else "source.manifest.json"
        if regular_path(path, filename).exists():
            result.append(path.name)
    return result


def _source_generation_changes(store, index, *, skip=(), on_scanned=None):
    """Restore lost hint coverage from atomic source receipts, without lineage reads.

    A pricing publisher queues work before replacing its source receipt and repeats
    that queue if the hint database is recreated. A replacement racing this census
    therefore remains queued even when this pass observes the older generation.
    """
    changed = []
    skip = set(skip)
    candidates = [(symbol, entry) for symbol, entry in sorted(index["symbols"].items()) if symbol not in skip]
    for count, (symbol, entry) in enumerate(candidates, 1):
        root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        source = load_source(root, symbol)
        if source is None:
            raise LineageError(f"{symbol}: published pricing receipt is missing")
        if storage.document_digest(source) != entry.get("source_sha256"):
            changed.append(symbol)
        if on_scanned is not None and (count % 1000 == 0 or count == len(candidates)):
            on_scanned(count, len(candidates))
    return changed


def _cohorts(selected, graph, limit=PREFETCH_MEMBERS):
    """Keep each prefetch cache within one provider-sized dependency cohort."""
    cohort, members = [], set()
    for label in selected:
        try:
            group = graph.closure({symbol_label(label)})
        except (RuntimeError, ValueError):
            group = {label}  # The normal group path will report the invalid label.
        if cohort and len(members | group) > limit:
            yield cohort
            cohort, members = [], set()
        cohort.append(label)
        members.update(group)
    if cohort:
        yield cohort


@traced("lineage")
def run_lineage(
    store,
    symbols=None,
    *,
    identity_client,
    pricing_client=None,
    refresh=False,
    observer=None,
    on_processed=None,
    on_scanned=None,
    on_pricing=None,
    known_pending=(),
    evidence_client=None,
    end_date=None,
    register_discoveries=None,
    work_policy=None,
    context=None,
    progress=None,
    pricing_handoff=None,
    event_supplements=(),
    retained_rebuild=False,
    retained_recovery=None,
):
    if retained_recovery is not None and (
        not retained_rebuild
        or retained_recovery.store != Path(store).absolute()
        or symbols != [group.seed for group in retained_recovery.groups]
    ):
        raise LineageError("retained recovery requires its exact selected store and groups")
    if retained_rebuild and (symbols is None or refresh or pricing_client is not None or event_supplements):
        raise LineageError(
            "retained lineage rebuild requires explicit symbols and no provider acquisition or refresh"
        )
    if event_supplements:
        raise LineageError("research event supplements are inadmissible in lineage inference")
    context = accept_context(store, context, work_policy)
    metadata_before = identity_client.request_count
    pricing_metrics = {}
    if pricing_client is not None and callable(getattr(pricing_client, "execute_plan", None)):
        pricing_client = MeasuredRequests(pricing_client, pricing_metrics)
    store = Path(store).absolute()
    results, processed = [], set()
    with storage.writer_lock(store, accepted_store=context.accept_store()) as root:
        recovered = context.recover_lineage(root)
        context.accept_correlation_policy()
        with storage.publication_lock(root):
            index = context.select_index(root)
        recovery_groups = {}
        recovered_groups = []
        if retained_recovery is not None:
            if context.accept_correlation_policy().semantic_sha256 != retained_recovery.policy_sha256:
                raise LineageError("retained recovery policy changed from accepted comparison")
            pending_groups, recovered_groups = retained_recovery.select(index)
            recovery_groups = {group.seed: group for group in pending_groups}
            symbols = list(recovery_groups)
        checks = read_checks(store)
        if checks["epoch"] is None:
            checks = begin_checks(store)
        full_scan = symbols is None and not checks["complete"]
        stale = (
            {symbol for symbol, entry in index["symbols"].items() if not reusable_decision(entry)}
            if symbols is None
            else set()
        )
        stale_metadata = {
            symbol
            for symbol in stale
            if not reusable_metadata_versions(index["symbols"][symbol].get("versions"))
        }
        prefetch = set()
        if symbols is not None:
            selected = list(dict.fromkeys(symbols))
            prefetch.update(selected)
        else:
            # The list is disposable: only a completed full scan establishes its
            # coverage. Thereafter all/pending use hints and the existing index,
            # not thousands of individual source/lineage receipts.
            selected = list(checks["checks"])
            prefetch.update(checks["checks"])
            if full_scan:
                local = set(local_symbols(store, pending=False, known=index["symbols"]))
                notices = set(local_symbols(store, pending=True, known=index["symbols"]))
                discovered = local - index["symbols"].keys()
                selected.extend(sorted(discovered | notices | stale))
                prefetch.update(discovered | notices)
            elif refresh:
                selected.extend(index["symbols"])
                prefetch.update(index["symbols"])
        # A changed rule/calendar version invalidates processed inputs even without
        # a new pricing notice. Default pending mode picks up that explicit work.
        if symbols is None:
            selected.extend(sorted(stale))
            prefetch.update(stale_metadata)
        selected = list(dict.fromkeys([*selected, *known_pending]))
        if work_policy is not None and symbols is None:
            # Default standalone work includes every accepted obligation. Bounded
            # recovery callers must pass their explicit pending symbols; otherwise
            # ordinary next-session obligations become premature lineage work.
            selected = list(dict.fromkeys([*selected, *work_policy.work]))
            prefetch.update(work_policy.work)
        if work_policy is not None:
            # Start a durable dependency group from a published member. A missing
            # auxiliary selected first would otherwise be mistaken for an
            # independently requested, unpriceable seed.
            selected.sort(
                key=lambda label: (
                    label not in index["symbols"],
                    auxiliary_work(work_policy.work.get(label, {})),
                    label,
                )
            )
        prefetch.update(known_pending)
        if work_policy is not None:
            work_policy.index = index
            work_policy.context = context
            work_policy.update_index(index["symbols"])
        graph = work_policy.graph if work_policy is not None else DependencyGraph(index["symbols"])
        if progress:
            visited, groups = set(), 0
            for symbol in selected:
                if symbol not in visited:
                    visited.update(graph.closure({symbol}))
                    groups += 1
            progress("lineage_selection", {"symbols": len(selected), "groups": groups})
        if evidence_client is None:
            evidence_client = EvidenceClient(identity_client)
        if work_policy is not None:
            evidence_client.work_policy = work_policy
            for label in selected:
                if refresh and work_policy.allowed({label}):
                    for member in graph.closure({label}):
                        projection = index["symbols"].get(member, {}).get("projection") or {}
                        target = projection.get("processed_through", end_date)[:10]
                        work_policy.require(member, target, reason="explicit_lineage_refresh", refresh=True)
        stop = False

        catchups = []
        empty_suffixes = set()

        def catch_up(labels, handoff):
            nonlocal stop
            if retained_rebuild:
                return
            value = _catch_up_published_dependencies(
                store,
                labels,
                index,
                graph,
                pricing_client,
                progress=on_pricing,
                end_date=end_date,
                work_policy=work_policy,
                register_discoveries=register_discoveries,
                context=context,
                pricing_handoff=handoff,
                empty_suffixes=empty_suffixes,
            )
            catchups.append(value)
            if value["failed"]:
                results.extend(
                    {
                        "symbol": item["symbol"],
                        "status": "failed",
                        "error": item.get("error", "dependency pricing catch-up failed"),
                        "outcome": item.get("outcome", {}),
                    }
                    for item in value["results"]
                    if item["status"] in {"failed", "blocked"}
                )
                # Independent groups can still finish; the policy blocks failed closures.
                stop = work_policy is None

        def process_selected(labels, planned):
            nonlocal stop
            for cohort in _cohorts(labels, graph):
                cohort = [label for label in cohort if label not in processed]
                if not cohort:
                    continue
                members = set().union(
                    *(
                        graph.closure({label})
                        for label in cohort
                        if work_policy is None or work_policy.allowed(graph.closure({label}))
                    )
                )
                handoff = {
                    symbol: pricing_handoff[symbol] for symbol in members if symbol in (pricing_handoff or {})
                }
                if not retained_rebuild and (
                    evidence_client.supports_plans or callable(getattr(pricing_client, "execute_plan", None))
                ):
                    _select_pending_pricing(
                        store, members, index, members if full_scan else checks["checks"], handoff
                    )
                catch_up(cohort, handoff)
                if stop:
                    break
                receipt_cache = storage.ReceiptCache(root, index, capacity=PREFETCH_MEMBERS)
                requests = [
                    label
                    for label in cohort
                    if label in planned
                    and label not in processed
                    and (work_policy is None or work_policy.allowed(graph.closure({label})))
                ]
                if requests and not retained_rebuild:
                    prefetch_before = identity_client.request_count
                    if progress:
                        progress("metadata_prefetch", {"state": "started", "selected": len(requests)})
                    _prefetch_metadata(
                        store,
                        requests,
                        index,
                        graph,
                        evidence_client,
                        refresh,
                        receipt_cache,
                        end_date,
                        work_policy=work_policy,
                        pricing_handoff=handoff,
                    )
                    if progress:
                        progress(
                            "metadata_prefetch",
                            {"state": "completed", "requests": identity_client.request_count - prefetch_before},
                        )
                for label in cohort:
                    try:
                        symbol = symbol_label(label)
                        if symbol in processed:
                            continue
                        result = process_group(
                            store,
                            root,
                            symbol,
                            identity_client=identity_client,
                            pricing_client=pricing_client,
                            refresh=refresh,
                            index=index,
                            observer=observer,
                            graph=graph,
                            evidence_client=evidence_client,
                            receipt_cache=receipt_cache,
                            end_date=end_date,
                            register_discoveries=register_discoveries,
                            work_policy=work_policy,
                            context=context,
                            progress=progress,
                            empty_suffixes=empty_suffixes,
                            retained_rebuild=retained_rebuild,
                            retained_recovery=recovery_groups.get(symbol),
                        )
                        processed.update(result["processed"])
                        processed.update(result.get("unavailable_pricing", ()))
                        if on_processed is not None and result["processed"]:
                            on_processed(result["processed"])
                    except (RuntimeError, OSError, ValueError) as error:
                        affected = sorted(
                            work_policy.current_group if work_policy is not None else graph.closure({label})
                        )
                        if work_policy is not None:
                            work_policy.fail(affected, error, stage="lineage")
                        result = {
                            "affected": affected,
                            "symbol": label,
                            "status": "failed",
                            "error": str(error),
                            "outcome": failure_outcome(error),
                        }
                    results.append(result)
                    if progress:
                        progress(
                            "lineage_group",
                            {
                                "completed": len(processed),
                                "selected": len(selected),
                                **result,
                            },
                        )
                    if result["status"] == "failed" and storage.path_at(root, ".pending.json").exists():
                        stop = True
                        break  # Recovery must finish before another group can publish.
                if stop:
                    break

        if not stop:
            process_selected(selected, prefetch)

        failed = any(item["status"] in {"failed", "waiting_for_pricing"} for item in results)
        if full_scan and not stop and not failed:
            changed = _source_generation_changes(
                store,
                index,
                skip=processed,
                on_scanned=on_scanned,
            )
            if not stop:
                process_selected(changed, set(changed))
        if full_scan and not any(item["status"] in {"failed", "waiting_for_pricing"} for item in results):
            complete_checks(store, checks["epoch"])
    request_metrics = {}
    for value in catchups:
        for name, count in value["requests"].items():
            request_metrics[name] = request_metrics.get(name, 0) + count
    dependency_pricing = {
        name: sum(value.get(name, 0) for value in catchups)
        for name in ("requested", "succeeded", "failed", "skipped", "deferred_full_range")
    }
    dependency_pricing["requests"] = request_metrics
    if pricing_metrics:
        dependency_pricing["requests"] = pricing_metrics
    if progress:
        progress(
            "lineage_complete",
            {
                "processed": len(processed),
                "groups": len(results),
                "reused_decisions": sum(bool(item.get("reused_decision")) for item in results),
                "metadata_requests": identity_client.request_count - metadata_before,
                "pricing_requests": dependency_pricing["requests"],
            },
        )
    result = {
        "workers": 1,
        "recovered": recovered,
        "dependency_pricing": dependency_pricing,
        "results": results,
        "failed": sum(item["status"] in {"failed", "waiting_for_pricing"} for item in results),
    }
    if retained_recovery is not None:
        result["previously_completed_groups"] = len(recovered_groups)
        result["recomputed_groups"] = sum(item["status"] == "processed" for item in results)
    return result

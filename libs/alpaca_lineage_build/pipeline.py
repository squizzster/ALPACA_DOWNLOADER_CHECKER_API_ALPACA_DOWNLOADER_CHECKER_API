"""Single-worker dependency closure and the downloader/lineage handoff."""

from pathlib import Path

from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.contract import (
    HISTORY_START,
    InitialPricingUnavailable,
    failure_outcome,
    symbol_label,
)
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path
from libs.alpaca_pricing_download.update_checks import (
    acknowledge_checks,
    begin_checks,
    complete_checks,
    read_checks,
)

from . import storage
from .acquisition import check_unavailable_pricing, unavailable_pricing
from .contract import (
    MAX_ACQUISITION_WAVES,
    MAX_CLOSURE,
    VERSION,
    LineageError,
    reusable_metadata_versions,
    reusable_versions,
    validate_version,
)
from .metadata import EvidenceClient, acquire, acquire_unpriced, action_requests, dependencies, full_metadata
from .pricing import load_pricing
from .resume import can_resume, eligible_source, unique_assignment
from .selection import DependencyGraph
from .transform import correlate, make_publication
from .work_contract import acquisition_fingerprint, auxiliary_work, pricing_target

PREFETCH_MEMBERS = 1000


def closure(index, seeds, asset_ids=(), *, graph=None):
    return (graph or DependencyGraph(index["symbols"])).closure(seeds, asset_ids)


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


def _prefetch_metadata(
    store, root, selected, index, graph, client, refresh, receipt_cache, end_date=None, work_policy=None
):
    """Collect selected receipt ranges without opening prices or scanning directories.

    Publication still reselects pricing under its normal locks. Requests are
    cached by exact symbol/date bounds, so changed ranges cannot use old results.
    """
    if not client.supports_plans:
        return
    visited, requests = set(), set()
    for label in selected:
        try:
            seed = symbol_label(label)
            if seed in visited:
                continue
            group = closure(index, {seed}, graph=graph)
            visited.update(group)
            with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
                sources = _current_sources(store, locked, known=index["symbols"])
                if end_date and any(
                    source["latest_request"]["end_exclusive"][:10] > end_date for source in sources.values()
                ):
                    continue
                previous = {s: receipt_cache.read(s) for s in sources}
                group_refresh = refresh or (
                    work_policy is not None and work_policy.needs_refresh(group, previous)
                )
                notices = {
                    s: notice
                    for s in sources
                    if (notice := read_update(store / "end_of_day/pricing" / s, published=True)) is not None
                }
                generations = {s: storage.document_digest(source) for s, source in sources.items()}
                if _reuse_metadata(seed, group, generations, previous, notices, group_refresh):
                    continue
                for symbol, source in sources.items():
                    requests.update(
                        action_requests(
                            symbol,
                            source.get("history_start", HISTORY_START)[:10],
                            source["latest_request"]["asof"],
                            previous[symbol],
                            refresh=full_metadata(previous[symbol], notices.get(symbol), group_refresh),
                        )
                    )
        except (RuntimeError, OSError, ValueError):
            continue  # The normal group path reports this failure without stopping unrelated work.
    client.prefetch_corporate_actions(requests)
    client.prefetch_symbols({symbol for symbol, _, _ in requests})


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
):
    """The pricing owner validates retained history before advancing dependencies."""
    result = {
        "requested": 0,
        "succeeded": 0,
        "failed": 0,
        "results": [],
        "requests": {},
        "deferred_full_range": 0,
    }
    if client is None or not callable(getattr(client, "execute_plan", None)):
        return result
    visited, targets = set(), {}
    for label in selected:
        group = graph.closure({label})
        if group <= visited or (work_policy is not None and not work_policy.allowed(group)):
            continue
        visited.update(group)
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
        if work_policy is not None:
            for symbol in allowed:
                work_policy.require_dependencies(graph.closure({symbol}), end, {symbol})
        if register_discoveries is not None:
            register_discoveries({member for symbol in allowed for member in graph.closure({symbol})})
        value = acquire_selected(store, allowed, end, client=client, progress=progress)
        result["results"].extend(value["results"])
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
):
    index = storage.read_index(root) if index is None else index
    receipt_cache = receipt_cache or storage.ReceiptCache(root, index)
    graph = work_policy.graph if work_policy is not None else graph or DependencyGraph(index["symbols"])
    group = closure(index, {seed}, graph=graph)
    acquisition_waves = 0
    pricing_replacements = {}
    metadata_cache = {}
    unavailable = {}
    evidence_client = evidence_client or EvidenceClient(identity_client)
    for _ in range(MAX_CLOSURE + MAX_ACQUISITION_WAVES + 1):
        required = None
        if work_policy is not None:
            work_policy.current_group = set(group)
            work_policy.check(group)
        with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
            checks = read_checks(store, locked)
            sources = _current_sources(store, locked, known=index["symbols"])
            # A missing seed is missing pricing, just like a related label. With
            # no published range, None delegates defaults to the downloader.
            starts = [source.get("history_start", HISTORY_START)[:10] for source in sources.values()]
            if work_policy is not None:
                starts.extend(
                    work_policy.work[symbol]["pricing_start"]
                    for symbol in group
                    if work_policy.work.get(symbol, {}).get("pricing_start")
                )
            start = min(
                starts,
                default=HISTORY_START[:10] if end_date else None,
            )
            end = max(
                (source["latest_request"]["end_exclusive"][:10] for source in sources.values()), default=end_date
            )
            # Reload after delegation: concurrent publication outranks a remembered
            # rejection. Never turn a held source into an unavailable auxiliary.
            unavailable = {
                name: value
                for name, value in unavailable.items()
                if name not in sources
                and value["request"]["start"][:10] == start
                and value["request"]["end_exclusive"][:10] == end
            }
            previous = {symbol: receipt_cache.read(symbol) for symbol in sorted(sources)}
            if work_policy is not None:
                refresh = refresh or work_policy.needs_refresh(group, previous)
            notices = {
                symbol: notice
                for symbol in sorted(sources)
                if (notice := read_update(store / "end_of_day/pricing" / symbol, published=True)) is not None
            }
            selected = {name: storage.document_digest(source) for name, source in sources.items()}
            prior_unavailable = unavailable_pricing(previous.get(seed))
            reuse_metadata = _reuse_metadata(seed, group, selected, previous, notices, refresh)
            if reuse_metadata:
                check_unavailable_pricing(store, prior_unavailable, locked, sources=sources)
                unavailable = prior_unavailable
            missing = group - sources.keys() - unavailable.keys()
            if work_policy is not None and missing:
                work_policy.require_dependencies(group, end, missing, start=start)
                work_policy.check(group)
            if register_discoveries is not None:
                register_discoveries(group)
            if (
                reuse_metadata
                and all(reusable_versions(prior["versions"]) for prior in previous.values())
                and (work_policy is None or not work_policy.requires_publication(previous))
            ):
                if observer is not None:
                    observer(
                        store=store,
                        receipts=previous,
                        boundaries={
                            name: storage.read_boundaries(root, item) for name, item in previous.items()
                        },
                        inputs=None,
                        reports=None,
                        notices={},
                        resumed=False,
                    )
                from .check_summary import refresh as refresh_check_summary

                refresh_check_summary(store, previous, sources=sources)
                acknowledge_checks(store, checks, sources)
                return {
                    "symbol": seed,
                    "status": "unchanged",
                    "processed": sorted(sources),
                    "unavailable_pricing": sorted(unavailable),
                    "pricing_replacements": pricing_replacements,
                }
            if end_date and end and end > end_date:
                raise LineageError(
                    f"{seed}: required group reconciliation extends beyond requested cutoff {end_date}; "
                    "retained pricing and lineage remain unchanged"
                )
            required = {
                symbol: {"from": start, "end_exclusive": end}
                for symbol in sorted(group)
                if symbol not in sources and symbol not in unavailable
            }
            if not required:
                resume = len(group) == 1 and eligible_source(
                    sources[seed], previous[seed], notices.get(seed), refresh=refresh
                )
                old_boundaries = {
                    symbol: (
                        storage.read_boundaries(root, prior)
                        if prior and (resume or prior.get("schema_version") == VERSION)
                        else None
                    )
                    for symbol, prior in previous.items()
                }
                inputs = {
                    symbol: load_pricing(
                        store,
                        symbol,
                        source=sources[symbol],
                        active_only=resume,
                        previous_boundaries=old_boundaries[symbol],
                    )
                    for symbol in sorted(sources)
                }
                evidence, deps, assets = {}, {}, set()
                for symbol, view in inputs.items():
                    prior = previous[symbol]
                    if reuse_metadata:
                        # Only the downstream rules changed. Consume the approved
                        # evidence directly, then recalculate the complete group.
                        evidence[symbol] = prior["evidence"]
                    else:
                        full = full_metadata(prior, notices.get(symbol), refresh)
                        key = (symbol, view.source_sha256, full)
                        if key not in metadata_cache:
                            metadata_cache[key] = acquire(view, evidence_client, prior, refresh=full)
                        evidence[symbol] = metadata_cache[key]
                    deps[symbol] = dependencies(evidence[symbol], symbol)
                    assets.update(item["id"] for item in evidence[symbol]["identities"] if item.get("id"))
                for symbol, missing in sorted(unavailable.items()):
                    # Prices and metadata have different provider endpoints. Keep
                    # discovering real relationships even when bars reject a label.
                    if "evidence" not in missing:
                        missing["evidence"] = acquire_unpriced(symbol, missing["request"], evidence_client)
                    auxiliary = missing["evidence"]
                    deps[symbol] = dependencies(auxiliary, symbol)
                    assets.update(item["id"] for item in auxiliary["identities"] if item.get("id"))
                expanded = closure(
                    index, group.union(*(set(values) for values in deps.values())), assets, graph=graph
                )
                if expanded != group:
                    if work_policy is not None:
                        work_policy.require_dependencies(
                            expanded, end, expanded - sources.keys() - unavailable.keys(), start=start
                        )
                        work_policy.reconcile_evidence(
                            evidence, sources, previous, group=expanded, target=end, partial=True
                        )
                    group = expanded
                    continue  # Release every pricing lock before taking an enlarged, sorted set.
                replacements = (
                    work_policy.reconcile_evidence(
                        evidence | {s: value["evidence"] for s, value in unavailable.items()},
                        sources,
                        previous,
                        group=group,
                        target=end,
                    )
                    if work_policy is not None
                    else set()
                )
                for symbol in replacements:
                    required[symbol] = {
                        "from": start,
                        "end_exclusive": max(end, pricing_target(work_policy.work[symbol])),
                        "refresh_all": True,
                        "selected_source_sha256": selected[symbol],
                        "acquisition_evidence_sha256": acquisition_fingerprint(work_policy.work[symbol]),
                    }
                # A related input may exist but not cover the selected acquisition range.
                # Empty published responses count as acquired, not as missing history.
                for symbol, source in sources.items():
                    if (
                        source.get("history_start", HISTORY_START)[:10] > start
                        or source["latest_request"]["end_exclusive"][:10] < end
                    ):
                        required.setdefault(symbol, {"from": start, "end_exclusive": end})
                if not required:
                    check_unavailable_pricing(store, unavailable, locked, sources=sources)
                    if resume and not can_resume(
                        inputs[seed], previous[seed], old_boundaries[seed], evidence[seed]
                    ):
                        resume = False
                        inputs[seed] = load_pricing(store, seed, source=sources[seed])
                    reports = correlate(store, inputs, evidence)
                    if resume and (
                        reports[seed]["uncorrelated"]["row_count"]
                        or not unique_assignment(
                            reports[seed]["correlated"]["segments"], previous[seed]["asset_ids"][0]
                        )
                    ):
                        resume = False
                        inputs[seed] = load_pricing(store, seed, source=sources[seed])
                        reports = correlate(store, inputs, evidence)
                    receipts, boundaries = {}, {}
                    for symbol, view in inputs.items():
                        prior = previous[symbol]
                        old = old_boundaries[symbol]
                        receipts[symbol], boundaries[symbol] = make_publication(
                            view,
                            evidence[symbol],
                            reports[symbol],
                            sorted(set(deps[symbol]) | unavailable.keys()),
                            prior,
                            old,
                            resume_closed=resume,
                        )
                        receipts[symbol]["input_sources"] = {
                            name: item.source_sha256 for name, item in inputs.items()
                        }
                        if unavailable:
                            receipts[symbol]["unavailable_pricing"] = unavailable
                    if work_policy is not None:
                        work_policy.stamp(receipts, sources)
                    storage.publish(
                        store,
                        root,
                        index,
                        receipts,
                        boundaries,
                        notices,
                        checks=checks,
                        previous=previous,
                        sources=sources,
                    )
                    receipt_cache.published(receipts)
                    if work_policy is not None:
                        work_policy.update_index({symbol: index["symbols"][symbol] for symbol in receipts})
                    else:
                        graph.update({symbol: index["symbols"][symbol] for symbol in receipts})
                    # Inspection observes the actual publication while the same
                    # locks still protect its inputs. It never runs another resolver.
                    if observer is not None:
                        observer(
                            store=store,
                            receipts=receipts,
                            boundaries=boundaries,
                            inputs=inputs,
                            reports=reports,
                            notices=notices,
                            resumed=resume,
                        )
                    return {
                        "symbol": seed,
                        "status": "processed",
                        "processed": sorted(sources),
                        "acknowledged": sorted(notices),
                        "unresolved_rows": sum(item["unresolved_rows"] for item in receipts.values()),
                        "pricing_acquisition_waves": acquisition_waves,
                        "reused_closed_boundaries": resume,
                        "unavailable_pricing": sorted(unavailable),
                        "pricing_replacements": pricing_replacements,
                    }
        # Deliberately outside ALL pricing locks: the downloader owns acquisition,
        # endpoint checks, replacement, and the resulting pending notice.
        if required:
            if work_policy is not None:
                work_policy.require_dependencies(group, end, required, start=start)
                work_policy.check(group)
            if pricing_client is None:
                return {"symbol": seed, "status": "waiting_for_pricing", "required": required, "processed": []}
            if acquisition_waves >= MAX_ACQUISITION_WAVES:
                raise LineageError("pricing dependencies did not settle within three acquisition waves")
            acquisition_waves += 1
            for symbol, request in required.items():
                if work_policy is not None:
                    work_policy.check(group)
                    fingerprint = acquisition_fingerprint(work_policy.work[symbol])
                    if symbol not in sources and fingerprint:
                        request.update(selected_source_sha256=None, acquisition_evidence_sha256=fingerprint)
                try:
                    acquired = download_pricing(
                        store,
                        symbol,
                        request["end_exclusive"],
                        client=pricing_client,
                        from_date=request["from"],
                        **{k: v for k, v in request.items() if k not in {"from", "end_exclusive"}},
                    )
                    if request.get("refresh_all"):
                        pricing_replacements[symbol] = acquired["source_sha256"]
                        if work_policy is not None:
                            work_policy.history_approved[symbol] = acquired["source_sha256"]
                except InitialPricingUnavailable as error:
                    auxiliary = (
                        symbol != seed
                        and bool(sources)
                        and symbol not in sources
                        and symbol not in index["symbols"]
                        and (work_policy is None or auxiliary_work(work_policy.work.get(symbol, {})))
                    )
                    if not auxiliary:
                        if work_policy is not None:
                            work_policy.fail({symbol}, error, stage="dependency_acquisition")
                        raise
                    # This is a provider requestability fact, not a successful empty
                    # price response or permission to discard an identity candidate.
                    unavailable[symbol] = error.evidence
    raise LineageError("lineage dependency discovery did not settle")


def local_symbols(store: Path, *, pending: bool, known=()) -> list[str]:
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


def run_lineage(
    store,
    symbols=None,
    *,
    identity_client,
    pricing_client=None,
    all_symbols=False,
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
):
    validate_version()
    store = Path(store).absolute()
    results, processed = [], set()
    with storage.writer_lock(store) as root:
        recovered = storage.recover(store, root)
        index = storage.read_index(root)
        checks = read_checks(store)
        if checks["epoch"] is None:
            checks = begin_checks(store)
        full_scan = symbols is None and not checks["complete"]
        stale = (
            {
                symbol
                for symbol, entry in index["symbols"].items()
                if not reusable_versions(entry.get("versions"))
            }
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
            work_policy.update_index(index["symbols"])
        graph = work_policy.graph if work_policy is not None else DependencyGraph(index["symbols"])
        evidence_client = evidence_client or EvidenceClient(identity_client)
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

        def catch_up(labels):
            nonlocal stop
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

        catch_up(selected)

        def process_selected(labels, planned):
            nonlocal stop
            for cohort in _cohorts(labels, graph):
                receipt_cache = storage.ReceiptCache(root, index, capacity=PREFETCH_MEMBERS)
                requests = [
                    label
                    for label in cohort
                    if label in planned
                    and label not in processed
                    and (work_policy is None or work_policy.allowed(graph.closure({label})))
                ]
                if requests:
                    _prefetch_metadata(
                        store,
                        root,
                        requests,
                        index,
                        graph,
                        evidence_client,
                        refresh,
                        receipt_cache,
                        end_date,
                        work_policy=work_policy,
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
            catch_up(changed)
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
        for name in ("requested", "succeeded", "failed", "deferred_full_range")
    }
    dependency_pricing["requests"] = request_metrics
    return {
        "workers": 1,
        "recovered": recovered,
        "dependency_pricing": dependency_pricing,
        "results": results,
        "failed": sum(item["status"] in {"failed", "waiting_for_pricing"} for item in results),
    }

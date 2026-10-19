"""Single-worker dependency closure and the downloader/lineage handoff."""

from pathlib import Path

from libs.alpaca_pricing_download.contract import HISTORY_START, InitialPricingUnavailable, symbol_label
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


def closure(index, seeds, asset_ids=(), *, graph=None):
    return (graph or DependencyGraph(index["symbols"])).closure(seeds, asset_ids)


def _current_sources(store, locked):
    result = {}
    for symbol in sorted(locked):
        root = regular_path(store, f"end_of_day/pricing/{symbol}")
        if regular_path(root, ".pending.json").exists():
            raise LineageError(f"{symbol}: pricing publication incomplete; resume downloader first")
        source = load_source(root, symbol)
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


def _prefetch_metadata(store, root, selected, index, graph, client, refresh):
    """Collect selected receipt ranges without opening prices or scanning directories.

    Publication still reloads each group under its normal locks. Requests are
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
            with storage.pricing_locks(store, group) as locked:
                sources = _current_sources(store, locked)
                previous = {s: storage.read_receipt(root, s, index) for s in sources}
                notices = {
                    s: notice
                    for s in sources
                    if (notice := read_update(regular_path(store, f"end_of_day/pricing/{s}"))) is not None
                }
                generations = {s: storage.document_digest(source) for s, source in sources.items()}
                if _reuse_metadata(seed, group, generations, previous, notices, refresh):
                    continue
                for symbol, source in sources.items():
                    requests.update(
                        action_requests(
                            symbol,
                            source.get("history_start", HISTORY_START)[:10],
                            source["latest_request"]["asof"],
                            previous[symbol],
                            refresh=full_metadata(previous[symbol], notices.get(symbol), refresh),
                        )
                    )
        except (RuntimeError, OSError, ValueError):
            continue  # The normal group path reports this failure without stopping unrelated work.
    client.prefetch_corporate_actions(requests)
    client.prefetch_symbols({symbol for symbol, _, _ in requests})


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
):
    index = storage.read_index(root) if index is None else index
    graph = graph or DependencyGraph(index["symbols"])
    group = closure(index, {seed}, graph=graph)
    acquisition_waves = 0
    metadata_cache = {}
    unavailable = {}
    evidence_client = evidence_client or EvidenceClient(identity_client)
    for _ in range(MAX_CLOSURE + MAX_ACQUISITION_WAVES + 1):
        required = None
        with storage.pricing_locks(store, group) as locked:
            checks = read_checks(store, locked)
            sources = _current_sources(store, locked)
            # A missing seed is missing pricing, just like a related label. With
            # no published range, None delegates defaults to the downloader.
            start = min(
                (source.get("history_start", HISTORY_START)[:10] for source in sources.values()), default=None
            )
            end = max(
                (source["latest_request"]["end_exclusive"][:10] for source in sources.values()), default=None
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
            previous = {symbol: storage.read_receipt(root, symbol, index) for symbol in sorted(sources)}
            notices = {
                symbol: notice
                for symbol in sorted(sources)
                if (notice := read_update(regular_path(store, f"end_of_day/pricing/{symbol}"))) is not None
            }
            selected = {name: storage.document_digest(source) for name, source in sources.items()}
            prior_unavailable = unavailable_pricing(previous.get(seed))
            reuse_metadata = _reuse_metadata(seed, group, selected, previous, notices, refresh)
            if reuse_metadata:
                check_unavailable_pricing(store, prior_unavailable, locked)
                unavailable = prior_unavailable
            if reuse_metadata and all(reusable_versions(prior["versions"]) for prior in previous.values()):
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

                refresh_check_summary(store, previous)
                acknowledge_checks(store, checks, sources)
                return {"symbol": seed, "status": "unchanged", "processed": sorted(sources)}
            required = {
                symbol: {"from": start, "end_exclusive": end}
                for symbol in sorted(group)
                if symbol not in sources and symbol not in unavailable
            }
            if not required:
                old_boundaries = {
                    symbol: storage.read_boundaries(root, prior) if prior else None
                    for symbol, prior in previous.items()
                }
                resume = len(group) == 1 and eligible_source(
                    sources[seed], previous[seed], notices.get(seed), refresh=refresh
                )
                inputs = {
                    symbol: load_pricing(
                        store, symbol, active_only=resume, previous_boundaries=old_boundaries[symbol]
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
                    group = expanded
                    continue  # Release every pricing lock before taking an enlarged, sorted set.
                # A related input may exist but not cover the selected acquisition range.
                # Empty published responses count as acquired, not as missing history.
                for symbol, source in sources.items():
                    if (
                        source.get("history_start", HISTORY_START)[:10] > start
                        or source["latest_request"]["end_exclusive"][:10] < end
                    ):
                        required[symbol] = {"from": start, "end_exclusive": end}
                if not required:
                    check_unavailable_pricing(store, unavailable, locked)
                    if resume and not can_resume(
                        inputs[seed], previous[seed], old_boundaries[seed], evidence[seed]
                    ):
                        resume = False
                        inputs[seed] = load_pricing(store, seed)
                    reports = correlate(store, inputs, evidence)
                    if resume and (
                        reports[seed]["uncorrelated"]["row_count"]
                        or not unique_assignment(
                            reports[seed]["correlated"]["segments"], previous[seed]["asset_ids"][0]
                        )
                    ):
                        resume = False
                        inputs[seed] = load_pricing(store, seed)
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
                    storage.publish(store, root, index, receipts, boundaries, notices, checks=checks)
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
                    }
        # Deliberately outside ALL pricing locks: the downloader owns acquisition,
        # endpoint checks, replacement, and the resulting pending notice.
        if required:
            if pricing_client is None:
                return {"symbol": seed, "status": "waiting_for_pricing", "required": required, "processed": []}
            if acquisition_waves >= MAX_ACQUISITION_WAVES:
                raise LineageError("pricing dependencies did not settle within three acquisition waves")
            acquisition_waves += 1
            for symbol, request in required.items():
                try:
                    download_pricing(
                        store, symbol, request["end_exclusive"], client=pricing_client, from_date=request["from"]
                    )
                except InitialPricingUnavailable as error:
                    if symbol == seed or symbol in sources or symbol in index["symbols"]:
                        raise
                    # This is a provider requestability fact, not a successful empty
                    # price response or permission to discard an identity candidate.
                    unavailable[symbol] = error.evidence
    raise LineageError("lineage dependency discovery did not settle")


def local_symbols(store: Path, *, pending: bool) -> list[str]:
    directory = regular_path(store, "end_of_day/manifests" if pending else "end_of_day/pricing")
    if not directory.exists():
        return []
    result = []
    for path in sorted(directory.iterdir()):
        if path.is_symlink() or not path.is_dir() or symbol_label(path.name) != path.name:
            raise LineageError(f"invalid ticker directory: {path}")
        filename = "update.json" if pending else "source.manifest.json"
        if regular_path(path, filename).exists():
            result.append(path.name)
    return result


def run_lineage(
    store,
    symbols=None,
    *,
    identity_client,
    pricing_client=None,
    all_symbols=False,
    refresh=False,
    observer=None,
    known_pending=(),
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
        if symbols is not None:
            selected = list(dict.fromkeys(symbols))
        else:
            # The list is disposable: only a completed full scan establishes its
            # coverage. Thereafter all/pending use hints and the existing index,
            # not thousands of individual source/lineage receipts.
            selected = list(checks["checks"])
            if full_scan:
                selected.extend(local_symbols(store, pending=False))
                selected.extend(index["symbols"])
            elif refresh:
                selected.extend(index["symbols"])
        # A changed rule/calendar version invalidates processed inputs even without
        # a new pricing notice. Default pending mode picks up that explicit work.
        if symbols is None:
            selected.extend(
                symbol
                for symbol, entry in index["symbols"].items()
                if not reusable_versions(entry.get("versions"))
            )
        selected = list(dict.fromkeys([*selected, *known_pending]))
        graph = DependencyGraph(index["symbols"])
        evidence_client = EvidenceClient(identity_client)
        _prefetch_metadata(store, root, selected, index, graph, evidence_client, refresh)
        for label in selected:
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
                )
                processed.update(result["processed"])
            except (RuntimeError, OSError, ValueError) as error:
                result = {"symbol": label, "status": "failed", "error": str(error)}
            results.append(result)
            if storage.path_at(root, ".pending.json").exists():
                break  # Recovery must finish before another group can publish.
        if full_scan and not any(item["status"] in {"failed", "waiting_for_pricing"} for item in results):
            complete_checks(store, checks["epoch"])
    return {
        "workers": 1,
        "recovered": recovered,
        "results": results,
        "failed": sum(item["status"] in {"failed", "waiting_for_pricing"} for item in results),
    }

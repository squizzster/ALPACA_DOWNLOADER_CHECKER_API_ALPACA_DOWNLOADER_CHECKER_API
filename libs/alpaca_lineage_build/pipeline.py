"""Single-worker dependency closure and the downloader/lineage handoff."""

from pathlib import Path

from libs.alpaca_pricing_download.contract import HISTORY_START, symbol_label
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path

from . import storage
from .contract import MAX_ACQUISITION_WAVES, MAX_CLOSURE, LineageError, reusable_versions, validate_version
from .metadata import EvidenceClient, acquire, dependencies
from .pricing import load_pricing
from .resume import can_resume, eligible_source, unique_assignment
from .transform import correlate, make_publication


def closure(index, seeds, asset_ids=()):
    symbols, assets = set(seeds), set(asset_ids)
    while True:
        previous = (set(symbols), set(assets))
        for symbol, entry in index["symbols"].items():
            if (
                symbol in symbols
                or symbols.intersection(entry["dependencies"])
                or assets.intersection(entry["asset_ids"])
            ):
                symbols.add(symbol)
                symbols.update(entry["dependencies"])
                assets.update(entry["asset_ids"])
        if len(symbols) > MAX_CLOSURE:
            raise LineageError(f"lineage closure exceeds {MAX_CLOSURE} labels; no notices consumed")
        if (symbols, assets) == previous:
            return symbols


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


def process_group(
    store, root, seed, *, identity_client, pricing_client=None, refresh=False, index=None, observer=None
):
    index = storage.read_index(root) if index is None else index
    group = closure(index, {seed})
    acquisition_waves = 0
    metadata_cache = {}
    evidence_client = EvidenceClient(identity_client)
    for _ in range(MAX_CLOSURE + MAX_ACQUISITION_WAVES + 1):
        required = None
        with storage.pricing_locks(store, group) as locked:
            sources = _current_sources(store, locked)
            # A missing seed is missing pricing, just like a related label. With
            # no published range, None delegates defaults to the downloader.
            start = min(
                (source.get("history_start", HISTORY_START)[:10] for source in sources.values()), default=None
            )
            end = max(
                (source["latest_request"]["end_exclusive"][:10] for source in sources.values()), default=None
            )
            required = {
                symbol: {"from": start, "end_exclusive": end}
                for symbol in sorted(group)
                if symbol not in sources
            }
            if not required:
                previous = {symbol: storage.read_receipt(root, symbol, index) for symbol in sorted(group)}
                notices = {
                    symbol: notice
                    for symbol in sorted(group)
                    if (notice := read_update(regular_path(store, f"end_of_day/pricing/{symbol}"))) is not None
                }
                if (
                    not refresh
                    and not notices
                    and all(
                        previous[symbol] is not None
                        and previous[symbol]["source_sha256"] == storage.document_digest(sources[symbol])
                        and reusable_versions(previous[symbol]["versions"])
                        for symbol in group
                    )
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
                    return {"symbol": seed, "status": "unchanged", "processed": sorted(group)}
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
                    for symbol in sorted(group)
                }
                evidence, deps, assets = {}, {}, set()
                for symbol, view in inputs.items():
                    prior = previous[symbol]
                    full = refresh or (
                        prior is not None
                        and (
                            not reusable_versions(prior["versions"])
                            or notices.get(symbol, {}).get("from", prior["anchor_month"] + "-01")
                            < prior["anchor_month"] + "-01"
                        )
                    )
                    key = (symbol, view.source_sha256, full)
                    if key not in metadata_cache:
                        metadata_cache[key] = acquire(view, evidence_client, prior, refresh=full)
                    evidence[symbol] = metadata_cache[key]
                    deps[symbol] = dependencies(evidence[symbol], symbol)
                    assets.update(item["id"] for item in evidence[symbol]["identities"] if item.get("id"))
                expanded = closure(index, group.union(*(set(values) for values in deps.values())), assets)
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
                            deps[symbol],
                            prior,
                            old,
                            resume_closed=resume,
                        )
                        receipts[symbol]["input_sources"] = {
                            name: item.source_sha256 for name, item in inputs.items()
                        }
                    storage.publish(store, root, index, receipts, boundaries, notices)
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
                        "processed": sorted(group),
                        "acknowledged": sorted(notices),
                        "unresolved_rows": sum(item["unresolved_rows"] for item in receipts.values()),
                        "pricing_acquisition_waves": acquisition_waves,
                        "reused_closed_boundaries": resume,
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
                download_pricing(
                    store, symbol, request["end_exclusive"], client=pricing_client, from_date=request["from"]
                )
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
    store, symbols=None, *, identity_client, pricing_client=None, all_symbols=False, refresh=False, observer=None
):
    validate_version()
    store = Path(store).absolute()
    results, processed = [], set()
    with storage.writer_lock(store) as root:
        recovered = storage.recover(store, root)
        index = storage.read_index(root)
        selected = (
            list(dict.fromkeys(symbols))
            if symbols is not None
            else local_symbols(store, pending=not all_symbols)
        )
        # A changed rule/calendar version invalidates processed inputs even without
        # a new pricing notice. Default pending mode picks up that explicit work.
        if symbols is None:
            selected.extend(
                symbol
                for symbol, entry in index["symbols"].items()
                if not reusable_versions(entry.get("versions")) and symbol not in selected
            )
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
                )
                processed.update(result["processed"])
            except (RuntimeError, OSError, ValueError) as error:
                result = {"symbol": label, "status": "failed", "error": str(error)}
            results.append(result)
            if storage.path_at(root, ".pending.json").exists():
                break  # Recovery must finish before another group can publish.
    return {
        "workers": 1,
        "recovered": recovered,
        "results": results,
        "failed": sum(item["status"] in {"failed", "waiting_for_pricing"} for item in results),
    }

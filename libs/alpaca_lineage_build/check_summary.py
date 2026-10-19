"""Project trusted stored status and pricing endpoints into the minimal check summary."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from libs.alpaca_pricing_download import check_summary as summary
from libs.alpaca_pricing_download.storage import load_source, regular_path, ticker_lock

from . import storage
from .contract import LineageError


def stored_active(receipt):
    symbol = receipt["symbol"]
    lookups = [
        item
        for item in receipt["evidence"]["asset_lookups"]
        if item.get("lookup") == symbol and item.get("lookup_kind") == "symbol"
    ]
    if len(lookups) != 1:
        return False
    asset = lookups[0].get("asset") or {}
    return (asset.get("status"), asset.get("symbol"), asset.get("class")) == ("active", symbol, "us_equity")


def project(store, receipt, *, source=None):
    if not stored_active(receipt):
        return None
    symbol = receipt["symbol"]
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    source = load_source(root, symbol) if source is None else source
    if source is None:
        raise LineageError(f"missing published pricing for summary: {symbol}")
    return summary.price_entry(root, source)


def refresh(store, receipts, *, sources=None):
    """Caller owns the selected pricing locks and has installed the lineage receipt."""
    if not summary.summary_path(store).exists():
        return
    entries = {}
    for symbol, receipt in receipts.items():
        if sources is None:
            root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
            if regular_path(root, ".pending.json").exists():
                continue  # Recovery must select again after the interruption released locks.
        entries[symbol] = project(store, receipt, source=None if sources is None else sources[symbol])
    summary.replace_entries(store, entries)


def rebuild(store, *, progress=None):
    """One explicit offline pass over the existing index; no asset queries or history reads."""
    store = Path(store).absolute()
    # Do not recover or alter authoritative publications during this derived-only operation.
    storage.check_store(store)
    root = regular_path(store, "end_of_day/lineage")
    if not root.is_dir():
        raise LineageError("summary rebuild requires published lineage")
    import fcntl

    with storage.path_at(root, ".worker.lock").open("a+b") as guard:
        fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
        with storage.publication_lock(root):
            if storage.path_at(root, ".pending.json").exists():
                raise LineageError("finish interrupted lineage publication before rebuilding the summary")
            index = storage.read_index(root)
        token = summary.begin_rebuild(store)

        def selected(symbol):
            pricing = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
            with ticker_lock(pricing, exclusive=False):
                if regular_path(pricing, ".pending.json").exists():
                    raise LineageError(f"finish interrupted pricing publication for {symbol}")
                receipt = storage.read_receipt(root, symbol, index)
                return project(store, receipt)

        entries = []
        # Bounded I/O parallelism; the normal publication path touches only affected tickers.
        with ThreadPoolExecutor(max_workers=16) as pool:
            for count, entry in enumerate(pool.map(selected, sorted(index["symbols"])), 1):
                if entry is not None:
                    entries.append(entry)
                if progress is not None and count % 1000 == 0:
                    progress(count, len(index["symbols"]))
        summary.finish_rebuild(store, token, entries)
    return {"indexed_tickers": len(index["symbols"]), **summary.describe(summary.read_snapshot(store))}

"""Research comparison helpers; the supported live check is alpaca-eod pricing check."""

import json
import os
import time
import uuid
from collections import Counter
from pathlib import Path

from .contract import (
    ADJUSTMENTS,
    COLUMNS,
    HISTORY_START,
    NEW_YORK,
    DownloadError,
    json_bytes,
    symbol_label,
    timestamp,
)
from .download import download_pricing
from .handoff import load_published_views
from .live_check import DEFAULT_BATCH_SIZE as DEFAULT_BATCH_SIZE
from .live_check import VERSION as VERSION
from .live_check import changed_fields as changed_fields
from .live_check import plan as plan
from .live_check import verify as verify
from .refresh import acquire_full
from .storage import load_source, ticker_lock


def validate_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_live_check_experiment_change_log.json"
    entries = json.loads(path.read_bytes())
    if len(entries) != VERSION or any("version" in entry or not entry.get("modifications") for entry in entries):
        raise DownloadError("live-check experiment changelog/version mismatch")


def compare_history(stored, live):
    """Basic full-history comparison, keeping exact numeric values."""
    before = {timestamp(row[0]): row for row in stored}
    after = {timestamp(row[0]): row for row in live}
    changed, ohlc, fields = [], 0, Counter()
    common = before.keys() & after.keys()
    for instant in sorted(common):
        differences = changed_fields(before[instant], after[instant])
        if differences:
            changed.append(instant.astimezone(NEW_YORK).date().isoformat())
            fields.update(differences.keys())
            ohlc += bool(set(differences).intersection(COLUMNS[1:5]))
    return {
        "stored_rows": len(before),
        "downloaded_rows": len(after),
        "matched": len(common) - len(changed),
        "changed": len(changed),
        "ohlc_changed": ohlc,
        "other_fields_only": len(changed) - ohlc,
        "added": len(after.keys() - before.keys()),
        "removed": len(before.keys() - after.keys()),
        "first_changed": changed[0] if changed else None,
        "last_changed": changed[-1] if changed else None,
        "fields": dict(fields),
    }


def full_download_sample(handoff, store, sample_store, symbol, *, client):
    """Use a completed live-check result; acquire one fresh isolated test ticker."""
    symbol = symbol_label(symbol)
    if symbol not in {item["symbol"] for item in handoff["differences"]}:
        raise DownloadError("sample ticker is not in the supplied live-check differences")
    store, sample_store = Path(store).resolve(), Path(sample_store).resolve()
    if store == sample_store or store in sample_store.parents or sample_store in store.parents:
        raise DownloadError("sample store must be separate from the original store")
    if (sample_store / "end_of_day/pricing" / symbol).exists():
        raise DownloadError("sample ticker already exists; use a fresh experiment destination")
    started = time.monotonic()
    root = store / "end_of_day/pricing" / symbol
    with ticker_lock(root, exclusive=False):
        source = load_source(root, symbol)
        if source is None:
            raise DownloadError("no stored sample history")
        before = load_published_views(root, source)
    end = timestamp(source["latest_request"]["end_exclusive"]).date().isoformat()
    start = timestamp(source.get("history_start", HISTORY_START)).date().isoformat()
    if source["latest_request"].get("asof", end) != end:
        raise DownloadError("sample requires the original saved identity-mapping date")
    download_pricing(sample_store, symbol, end, client=client, refresh_all=True, from_date=start)
    sample_root = sample_store / "end_of_day/pricing" / symbol
    with ticker_lock(sample_root, exclusive=False):
        after = load_published_views(sample_root, load_source(sample_root, symbol))
    return {
        "symbol": symbol,
        "from": start,
        "end_exclusive": end,
        "seconds": round(time.monotonic() - started, 3),
        "views": {name: compare_history(before[name], after[name]) for name in ADJUSTMENTS},
    }


def full_download_samples(handoff, store, sample_store, symbols, *, client):
    """Resolve one selected batch, check it, then use normal full publication."""
    started = time.monotonic()
    symbols = list(dict.fromkeys(symbol_label(symbol) for symbol in symbols))
    eligible = {item["symbol"] for item in handoff["differences"]}
    store, sample_store = Path(store).resolve(), Path(sample_store).resolve()
    if store == sample_store or store in sample_store.parents or sample_store in store.parents:
        raise DownloadError("sample store must be separate from the original store")
    needs = []
    for symbol in symbols:
        if symbol not in eligible:
            raise DownloadError("sample ticker is not in the supplied live-check differences")
        if (sample_store / "end_of_day/pricing" / symbol).exists():
            raise DownloadError("sample ticker already exists; use a fresh experiment destination")
        root = store / "end_of_day/pricing" / symbol
        with ticker_lock(root, exclusive=False):
            source = load_source(root, symbol)
        if source is None:
            raise DownloadError("no stored sample history")
        end = source["latest_request"]["end_exclusive"]
        asof = source["latest_request"]["asof"]
        if timestamp(end).date().isoformat() != asof:
            raise DownloadError("sample requires the original saved identity-mapping date")
        needs.append(
            {
                "kind": "bars",
                "why": "changed_data",
                "symbols": [symbol],
                "asof": asof,
                "adjustments": list(ADJUSTMENTS),
                "ranges": [{"start": source.get("history_start", HISTORY_START), "end_exclusive": end}],
            }
        )
    metrics = {}
    consumer, action_plan = acquire_full(needs, client, metrics=metrics)
    results = [full_download_sample(handoff, store, sample_store, symbol, client=consumer) for symbol in symbols]
    return {
        "plan": action_plan,
        "requests": metrics,
        "seconds": round(time.monotonic() - started, 3),
        "results": results,
    }


def write_handoff(path, document):
    """Publish a complete JSON file atomically, preserving prior experiment output."""
    path = Path(path).absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    try:
        with temporary.open("xb") as stream:
            stream.write(json_bytes(document))
            stream.flush()
        os.link(temporary, path)  # Atomic; refuses to overwrite an existing result.
    finally:
        if temporary.exists():
            temporary.unlink()


if __name__ == "__main__":
    raise SystemExit("The experiment CLI is retired; use alpaca-eod pricing check.")

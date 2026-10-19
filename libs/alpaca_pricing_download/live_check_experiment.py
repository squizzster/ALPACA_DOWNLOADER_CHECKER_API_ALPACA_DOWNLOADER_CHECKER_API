"""Isolated step 2: trusted summary -> batched live comparisons -> JSON handoff.

Run with python3.11 -B -m libs.alpaca_pricing_download.live_check_experiment.
verify(snapshot, get_page) is shared with the integrated same-span refresh path:
an already loaded handoff and an injected HTTP page function.
The live-check CLI loads the summary once without reading ticker files. The
optional full-download sample consumes that result and compares selected histories.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from pathlib import Path

from .check_summary import read_snapshot
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
from .http import BarsClient
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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args(argv)
    try:
        validate_version()
        if args.output.exists():
            raise ValueError("output already exists; choose a new experiment output path")
        started = time.monotonic()
        snapshot = read_snapshot(args.store)
        load_seconds = time.monotonic() - started
        attempts = Counter()

        def opener(request, timeout):
            try:
                response = urllib.request.urlopen(request, timeout=timeout)
                attempts[str(response.status)] += 1
                return response
            except urllib.error.HTTPError as error:
                attempts[str(error.code)] += 1
                raise
            except Exception:
                attempts["transport_error"] += 1
                raise

        client = BarsClient(
            os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", ""), opener=opener
        )
        last_progress = 0.0

        def progress(done, total, requests):
            nonlocal last_progress
            now = time.monotonic()
            if done == total or now - last_progress >= 10:
                print(
                    f"live check: {done}/{total} tickers; {requests} page requests", file=sys.stderr, flush=True
                )
                last_progress = now

        handoff = verify(snapshot, client._get, batch_size=args.batch_size, progress=progress)
        handoff["summary_load_seconds"] = round(load_seconds, 6)
        handoff["requests"]["http_attempts"] = sum(attempts.values())
        handoff["requests"]["http_statuses"] = dict(sorted(attempts.items()))
        write_handoff(args.output, handoff)
        print(
            json.dumps(
                {
                    key: value
                    for key, value in handoff.items()
                    if key not in ("differences", "missing", "failures")
                }
            )
        )
        return 0 if handoff["complete"] else 2
    except (RuntimeError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

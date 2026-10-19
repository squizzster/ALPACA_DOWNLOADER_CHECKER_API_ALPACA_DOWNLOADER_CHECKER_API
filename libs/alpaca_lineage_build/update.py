"""Complete existing-universe update through the established producer boundaries."""

import time
from pathlib import Path

from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.append import append_ready
from libs.alpaca_pricing_download.check_summary import describe, pending_symbols, read_snapshot
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.download import request_end
from libs.alpaca_pricing_download.update_checks import read_checks

from .contract import validate_version
from .pipeline import run_lineage
from .refresh import refresh_existing


def finish_pending(store, *, pricing_client, identity_client, progress=None):
    """Use known work; let lineage restore missing disposable hint coverage."""
    pending = pending_symbols(store)
    checks = read_checks(store)
    selected = sorted(set(pending) | checks["checks"].keys())
    recovered = []
    for symbol in selected:
        root = pricing_storage.published_path(store, f"end_of_day/pricing/{symbol}")
        if pricing_storage.regular_path(root, ".pending.json").exists():
            with pricing_storage.ticker_lock(root, exclusive=True):
                if pricing_storage.recover(root):
                    recovered.append(symbol)
    if progress:
        progress(
            "recovery",
            {
                "known_pending": len(selected),
                "pricing_recovered": len(recovered),
                "restore_hint_coverage": not checks["complete"],
            },
        )
    processed = set()

    def observed(symbols):
        processed.update(symbols)
        if progress:
            progress("recovery", {"processed": len(processed)})

    def scanned(done, total):
        if progress:
            progress(
                "recovery",
                {"processed": len(processed), "source_generations_checked": done, "source_generations": total},
            )

    # The normal selector owns both pending work and version invalidation. Empty
    # hints alone cannot establish that existing lineage uses the current rules.
    lineage = run_lineage(
        store,
        pricing_client=pricing_client,
        identity_client=identity_client,
        known_pending=selected,
        on_processed=observed,
        on_scanned=scanned,
    )
    return {"pricing_recovered": recovered, "lineage": lineage, "failed": lineage["failed"]}


def update_everything(store, end_date=None, *, pricing_client, identity_client, workers=3, progress=None):
    """Fresh check -> same-span replacement/lineage -> append -> lineage -> summary.

    Freeze the completed-session target before any recovery or requests. Stage
    handoffs stay in memory; producer journals and pending notices own retries.
    """
    validate_version()
    if workers not in (1, 2, 3):
        raise DownloadError("update workers must be 1, 2 or 3")
    store = Path(store).absolute()
    end = request_end(end_date).date().isoformat()
    started = time.monotonic()
    result = {"complete": False, "failed": 0, "end_exclusive": end, "stages": {}}
    current_stage = "recovery"

    def stage(name, function, *args, **kwargs):
        nonlocal current_stage
        current_stage = name
        if progress:
            progress(name, {"state": "started", "end_exclusive": end})
        before = time.monotonic()
        value = function(*args, **kwargs)
        result["stages"][name] = {**value, "seconds": round(time.monotonic() - before, 3)}
        result["failed"] += value["failed"]
        return value

    try:
        recovered = stage(
            "recovery",
            finish_pending,
            store,
            pricing_client=pricing_client,
            identity_client=identity_client,
            progress=progress,
        )
        if recovered["failed"]:
            return result
        ready = []
        checked = stage(
            "refresh",
            refresh_existing,
            store,
            pricing_client=pricing_client,
            identity_client=identity_client,
            progress=progress,
            on_ready=ready.append,
        )
        if not checked["complete"]:
            return result
        # Refresh has completed its own lineage/publication boundary. Consume it directly.
        current_stage = "append"
        if len(ready) != 1:
            raise DownloadError("history refresh did not produce a ready append handoff")
        snapshot = ready.pop()
        appended = stage(
            "append",
            append_ready,
            store,
            snapshot,
            end,
            client=pricing_client,
            workers=workers,
            progress=progress,
        )
        selected = [
            row["symbol"]
            for row in appended["results"]
            if row["status"] != "failed" and row.get("mode") == "append"
        ]
        if selected:
            processed = set()

            def observed(symbols):
                processed.update(symbols)
                if progress:
                    progress("lineage", {"processed": len(processed), "selected": len(selected)})

            stage(
                "lineage",
                run_lineage,
                store,
                selected,
                pricing_client=pricing_client,
                identity_client=identity_client,
                on_processed=observed,
            )
        current_stage = "summary"
        result["summary"] = describe(read_snapshot(store))
        result["complete"] = result["failed"] == 0
        return result
    except (RuntimeError, OSError, ValueError) as error:
        result["failed"] += 1
        result["error"] = str(error)
        result["failed_stage"] = current_stage
        return result
    finally:
        result["seconds"] = round(time.monotonic() - started, 3)

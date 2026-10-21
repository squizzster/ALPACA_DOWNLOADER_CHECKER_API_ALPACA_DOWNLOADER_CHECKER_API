"""Complete existing-universe update through the established producer boundaries."""

import time
from pathlib import Path

from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.append import append_ready
from libs.alpaca_pricing_download.check_summary import describe, pending_symbols
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.download import request_end
from libs.alpaca_pricing_download.update_checks import read_checks

from .context import accept_context
from .pipeline import run_lineage
from .refresh import refresh_existing


def finish_pending(
    store,
    *,
    pricing_client,
    identity_client,
    progress=None,
    end_date=None,
    evidence_client=None,
    register_discoveries=None,
    work_policy=None,
    context=None,
):
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
        end_date=end_date,
        evidence_client=evidence_client,
        register_discoveries=register_discoveries,
        work_policy=work_policy,
        context=context,
        progress=progress,
        on_processed=observed,
        on_scanned=scanned,
        on_pricing=(
            (lambda stage, values: progress("recovery_dependency_" + stage, values)) if progress else None
        ),
    )
    return {"pricing_recovered": recovered, "lineage": lineage, "failed": lineage["failed"]}


def update_everything(
    store,
    end_date=None,
    *,
    pricing_client,
    identity_client,
    workers=3,
    progress=None,
    evidence_client=None,
    register_discoveries=None,
    work_policy=None,
    recover_pending=True,
    context=None,
):
    """Fresh check -> same-span replacement/lineage -> append -> lineage -> summary.

    Freeze the completed-session target before any recovery or requests. Stage
    handoffs stay in memory; producer journals and pending notices own retries.
    """
    context = accept_context(store, context, work_policy)
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
        if progress:
            progress(
                name,
                {"state": "completed", "failed": value["failed"], "seconds": result["stages"][name]["seconds"]},
            )
        return value

    try:
        recovered = (
            stage(
                "recovery",
                finish_pending,
                store,
                pricing_client=pricing_client,
                identity_client=identity_client,
                progress=progress,
                end_date=end,
                evidence_client=evidence_client,
                register_discoveries=register_discoveries,
                work_policy=work_policy,
                context=context,
            )
            if recover_pending
            else {"failed": 0}
        )
        if recovered["failed"]:
            return result
        remaining, maintained_symbols = None, set()
        for round_number in range(100):
            ready = []
            checked = stage(
                "refresh" if round_number == 0 else f"refresh_new_members_{round_number}",
                refresh_existing,
                store,
                pricing_client=pricing_client,
                identity_client=identity_client,
                progress=progress,
                on_ready=ready.append,
                end_date=end,
                evidence_client=evidence_client,
                register_discoveries=register_discoveries,
                work_policy=work_policy,
                symbols=remaining,
                context=context,
            )
            if not checked["complete"]:
                return result
            # Refresh has completed its own lineage/publication boundary. Consume it directly.
            current_stage = "append"
            if len(ready) != 1:
                raise DownloadError("history refresh did not produce a ready append handoff")
            snapshot = ready.pop()
            maintained_symbols.update(e["symbol"] for e in snapshot["entries"])
            appended = stage(
                "append" if round_number == 0 else f"append_new_members_{round_number}",
                append_ready,
                store,
                snapshot,
                end,
                client=pricing_client,
                workers=workers,
                progress=progress,
                context=context.pricing(),
            )
            if work_policy is not None:
                work_policy.record_results(appended, stage="append")
                work_policy.history_approved.update(
                    {
                        item["symbol"]: item["source_sha256"]
                        for item in appended["results"]
                        if "source_sha256" in item
                    }
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
                    "lineage" if round_number == 0 else f"lineage_new_members_{round_number}",
                    run_lineage,
                    store,
                    selected,
                    pricing_client=pricing_client,
                    identity_client=identity_client,
                    on_processed=observed,
                    end_date=end,
                    evidence_client=evidence_client,
                    register_discoveries=register_discoveries,
                    work_policy=work_policy,
                    context=context,
                    progress=progress,
                    pricing_handoff={
                        item["symbol"]: item for item in appended["results"] if "source_sha256" in item
                    },
                    on_pricing=(
                        (lambda pricing_stage, values: progress("lineage_dependency_" + pricing_stage, values))
                        if progress
                        else None
                    ),
                )
            latest = context.select_summary()
            remaining = {
                e["symbol"]
                for e in latest["entries"]
                if e["symbol"] not in maintained_symbols
                and e["asof"] <= end
                and (work_policy is None or work_policy.allowed({e["symbol"]}))
            }
            if not remaining or result["failed"]:
                break
        else:
            raise DownloadError("maintenance membership did not settle")
        current_stage = "summary"
        result["summary"] = describe(latest)
        result["complete"] = result["failed"] == 0 and not (work_policy and work_policy.blocked)
        return result
    except (RuntimeError, OSError, ValueError) as error:
        result["failed"] += 1
        result["error"] = str(error)
        result["failed_stage"] = current_stage
        return result
    finally:
        result["seconds"] = round(time.monotonic() - started, 3)

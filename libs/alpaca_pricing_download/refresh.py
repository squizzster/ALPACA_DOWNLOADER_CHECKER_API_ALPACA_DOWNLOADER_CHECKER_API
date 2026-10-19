"""Batched acquisition and guarded replacement of existing pricing spans."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from libs.alpaca_data_api_solver import build_plan

from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    NEW_YORK,
    DownloadError,
    digest,
    json_bytes,
    symbol_label,
    timestamp,
)
from .download import download_pricing
from .storage import load_source, regular_path, ticker_lock


def prepare_refresh(store, symbols):
    """Read only selected receipts; retain the complete original request coverage."""
    store = Path(store).absolute()
    prepared = []
    for symbol in sorted({symbol_label(value) for value in symbols}):
        root = regular_path(store, f"end_of_day/pricing/{symbol}")
        with ticker_lock(root, exclusive=False):
            if regular_path(root, ".pending.json").exists():
                raise DownloadError(f"{symbol}: finish interrupted pricing publication before checking")
            source = load_source(root, symbol)
        if source is None:
            raise DownloadError(f"{symbol}: no existing pricing to replace")
        start = timestamp(source.get("history_start", HISTORY_START))
        end = timestamp(source["latest_request"]["end_exclusive"])
        asof = source["latest_request"]["asof"]
        if (
            start != datetime.combine(start.date(), datetime.min.time(), tzinfo=timezone.utc)
            or end != datetime.combine(end.date(), datetime.min.time(), tzinfo=timezone.utc)
            or asof != end.date().isoformat()
        ):
            raise DownloadError(f"{symbol}: stored range/asof is incompatible with exact-span replacement")
        prepared.append(
            {
                "symbol": symbol,
                "source_sha256": digest(json_bytes(source)),
                "start": start.isoformat().replace("+00:00", "Z"),
                "end_exclusive": end.isoformat().replace("+00:00", "Z"),
                "asof": asof,
            }
        )
    return prepared


def acquire_full(needs, client, *, metrics=None):
    """Acquire shared full histories, then batch their dependent first-session checks.

    The returned client feeds the normal downloader; its existing bounded retry
    requests a fresh plan if the provider changed during acquisition.
    """
    acquisition = build_plan({"steps": [{"needs": needs}]})
    acquired = client.execute_plan(acquisition, metrics=metrics)
    cache, probes = {}, []
    for index, need in enumerate(needs):
        symbol, window = need["symbols"][0], need["ranges"][0]
        for adjustment in ADJUSTMENTS:
            rows = acquired[(0, index, 0, symbol, adjustment)]
            cache[
                (
                    False,
                    symbol,
                    adjustment,
                    timestamp(window["start"]),
                    timestamp(window["end_exclusive"]),
                    need["asof"],
                )
            ] = rows
            if rows:
                day = timestamp(rows[0]["t"]).astimezone(NEW_YORK).date()
                probes.append(
                    {
                        "kind": "bars",
                        "why": "acquisition_check",
                        "symbols": [symbol],
                        "asof": need["asof"],
                        "adjustments": [adjustment],
                        "ranges": [
                            {"start": day.isoformat(), "end_exclusive": (day + timedelta(days=1)).isoformat()}
                        ],
                    }
                )
    checks = build_plan({"steps": [{"needs": probes}]})
    checked = client.execute_plan(checks, metrics=metrics)
    for index, need in enumerate(probes):
        symbol, adjustment, window = need["symbols"][0], need["adjustments"][0], need["ranges"][0]
        start = datetime.fromisoformat(window["start"]).replace(tzinfo=NEW_YORK).astimezone(timezone.utc)
        end = datetime.fromisoformat(window["end_exclusive"]).replace(tzinfo=NEW_YORK).astimezone(timezone.utc)
        cache[(True, symbol, adjustment, start, end, need["asof"])] = checked[(0, index, 0, symbol, adjustment)]

    class PlannedClient:
        def fetch(self, symbol, adjustment, start, end, *, asof=None):
            key = (asof is not None, symbol, adjustment, start, end, asof or end.date().isoformat())
            if key in cache:
                return cache.pop(key)
            return client.fetch(symbol, adjustment, start, end, asof=asof)

    return PlannedClient(), {
        "schema_version": acquisition["schema_version"],
        "steps": acquisition["steps"] + checks["steps"],
    }


def replace_prepared(store, prepared, *, client, cohort_size=32, progress=None, before_publish=None):
    """Bound memory by cohort; the solver alone chooses concrete API requests."""
    if type(cohort_size) is not int or cohort_size < 1:
        raise DownloadError("refresh cohort size must be a positive integer")
    results, metrics = [], {}
    checked_baseline = False
    for offset in range(0, len(prepared), cohort_size):
        cohort = prepared[offset : offset + cohort_size]
        if progress:
            progress("acquiring", {"done": offset, "total": len(prepared), "cohort": len(cohort)})
        needs = [
            {
                "kind": "bars",
                "why": "changed_data",
                "symbols": [item["symbol"]],
                "asof": item["asof"],
                "adjustments": list(ADJUSTMENTS),
                "ranges": [{"start": item["start"], "end_exclusive": item["end_exclusive"]}],
            }
            for item in cohort
        ]
        try:
            consumer, _ = acquire_full(needs, client, metrics=metrics)
        except (RuntimeError, OSError, ValueError) as error:
            results.extend(
                {"symbol": item["symbol"], "status": "failed", "error": str(error)} for item in cohort
            )
            if progress:
                progress("pricing", {"done": len(results), "total": len(prepared), "failed": len(cohort)})
            continue
        if not checked_baseline:
            if before_publish:
                before_publish()
            checked_baseline = True
        for item in cohort:
            try:
                result = download_pricing(
                    store,
                    item["symbol"],
                    item["end_exclusive"][:10],
                    client=consumer,
                    refresh_all=True,
                    from_date=item["start"][:10],
                    expected_source_sha256=item["source_sha256"],
                )
                result["status"] = "replaced"
            except (RuntimeError, OSError, ValueError) as error:
                result = {"symbol": item["symbol"], "status": "failed", "error": str(error)}
            results.append(result)
            if progress:
                progress(
                    "pricing",
                    {
                        "done": len(results),
                        "total": len(prepared),
                        "symbol": item["symbol"],
                        "status": result["status"],
                    },
                )
        del consumer  # Release this cohort before acquiring another full history set.
    return {
        "results": results,
        "requests": metrics,
        "failed": sum(item["status"] == "failed" for item in results),
    }

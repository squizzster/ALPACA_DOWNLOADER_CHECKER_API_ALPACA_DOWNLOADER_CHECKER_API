"""Batched acquisition and guarded replacement of existing pricing spans."""

from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from libs.alpaca_data_api_solver import build_plan

from .context import accept_context
from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    BarsSymbolUnavailable,
    DownloadError,
    digest,
    failure_outcome,
    json_bytes,
    symbol_label,
    timestamp,
)
from .download import (
    _endpoint_window,
    _endpoints_match,
    _incremental_endpoints,
    acquisition_asof,
    download_pricing,
    established_pricing_rejection,
)
from .endpoints import endpoints_of
from .failures import AcquisitionFailure
from .responses import completed_bars
from .storage import load_source, read_validator, regular_path, ticker_lock


def check_retained_source(store, symbol, end_date, *, client):
    """Check a selected source independently of active-summary eligibility."""
    root = regular_path(Path(store), f"end_of_day/pricing/{symbol}", published=True)
    with ticker_lock(root, exclusive=False):
        if regular_path(root, ".pending.json").exists():
            raise DownloadError(f"{symbol}: finish interrupted pricing publication before checking")
        source = load_source(root, symbol)
        if source is None:
            raise DownloadError(f"{symbol}: no existing pricing to check")
        request = _refresh_request(symbol, source)
        if request["end_exclusive"][:10] > end_date:
            raise DownloadError(f"{symbol}: required history check exceeds requested cutoff")
        endpoints = read_validator(root, source, published=True)
        if endpoints is None:
            raise DownloadError(f"{symbol}: retained-history checking requires published endpoints")
        matched = not any(endpoints.values()) or _endpoints_match(
            client, symbol, endpoints, request["asof"], cutoff=timestamp(request["end_exclusive"])
        )
        return {**request, "matched": matched}


def _refresh_request(symbol, source):
    start = timestamp(source.get("history_start", HISTORY_START))
    end = timestamp(source["latest_request"]["end_exclusive"])
    asof = source["latest_request"]["asof"]
    if start != datetime.combine(
        start.date(), datetime.min.time(), tzinfo=timezone.utc
    ) or end != datetime.combine(end.date(), datetime.min.time(), tzinfo=timezone.utc):
        raise DownloadError(f"{symbol}: stored range/asof is incompatible with exact-span replacement")
    return {
        "symbol": symbol,
        "source_sha256": digest(json_bytes(source)),
        "history_correction_id": source.get("history_correction_id"),
        "start": start.isoformat().replace("+00:00", "Z"),
        "end_exclusive": end.isoformat().replace("+00:00", "Z"),
        "asof": asof,
    }


def prepare_refresh(store, symbols):
    """Read only selected receipts; retain the complete original request coverage."""
    # Selection is already owned by the live-check/correction caller. Read only
    # those ticker receipts and carry their generation/scope into replacement;
    # DO NOT widen an explicit correction into a retained-store inventory.
    store = Path(store).absolute()
    prepared = []
    for symbol in sorted({symbol_label(value) for value in symbols}):
        root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        with ticker_lock(root, exclusive=False):
            if regular_path(root, ".pending.json").exists():
                raise DownloadError(f"{symbol}: finish interrupted pricing publication before checking")
            source = load_source(root, symbol)
        if source is None:
            raise DownloadError(f"{symbol}: no existing pricing to replace")
        prepared.append(_refresh_request(symbol, source))
    return prepared


class PlannedClient:
    """Replay each buffered request once; retries use fresh provider responses."""

    def __init__(self, client, cache):
        self.client, self.cache = client, cache

    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        key = (asof is not None, symbol, adjustment, start, end, asof or end.date().isoformat())
        if key in self.cache:
            return self.cache.pop(key)
        return self.client.fetch(symbol, adjustment, start, end, asof=asof)

    def fetch_acquired(self, symbol, adjustment, start, end, *, asof=None):
        # DO NOT REMOVE WITHOUT USER AUTH!#
        # Consume the completed acquisition exactly once. Endpoint probes retain
        # their separate cache entry even for a one-session acquisition.
        key = (False, symbol, adjustment, start, end, asof or end.date().isoformat())
        if key in self.cache:
            return self.cache.pop(key)
        fetch = getattr(self.client, "fetch_acquired", self.client.fetch)
        return fetch(symbol, adjustment, start, end, asof=asof)


def _endpoint_needs(symbol, endpoints, asof, why, *, cutoff=None):
    return [
        {
            "kind": "bars",
            "why": why,
            "symbols": [symbol],
            "asof": asof,
            "adjustments": [adjustment],
            "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
        }
        for adjustment, rows in endpoints.items()
        for row in rows
        for start, end in [_endpoint_window(row, asof, cutoff=cutoff)]
    ]


def _prefetch(needs, client, *, check, metrics, progress=None, on_failure=None):
    started = monotonic()
    pages_before = (metrics or {}).get("page_requests", 0)
    plan = build_plan({"steps": [{"needs": needs}]})
    stage = needs[0]["why"] if needs else "acquisition_check"
    actions = plan["steps"][0]["actions"]
    if progress:
        progress(
            stage,
            {
                "state": "started",
                "symbols": len({need["symbols"][0] for need in needs}),
                "actions": len(actions),
                "minimum_pages": len(actions),
            },
        )
    options = {"on_failure": on_failure} if on_failure is not None else {}
    try:
        acquired = client.execute_plan(plan, metrics=metrics, **options)
    except (RuntimeError, OSError, ValueError):
        if progress:
            progress(
                stage,
                {
                    "state": "failed",
                    "seconds": monotonic() - started,
                    "page_requests": (metrics or {}).get("page_requests", 0) - pages_before,
                },
            )
        raise
    if progress:
        progress(
            stage,
            {
                "state": "completed",
                "seconds": monotonic() - started,
                "page_requests": (metrics or {}).get("page_requests", 0) - pages_before,
            },
        )
    cache = {}
    for index, need in enumerate(needs):
        symbol = need["symbols"][0]
        for adjustment in need["adjustments"]:
            for offset, window in enumerate(need["ranges"]):
                key = (0, index, offset, symbol, adjustment)
                if on_failure is not None and key not in acquired:
                    continue  # The caller retains the failure and excludes this ticker.
                cache[
                    (
                        check,
                        symbol,
                        adjustment,
                        timestamp(window["start"]),
                        timestamp(window["end_exclusive"]),
                        need["asof"],
                    )
                ] = completed_bars(
                    acquired[key],
                    symbol,
                    adjustment,
                    timestamp(window["start"]),
                    timestamp(window["end_exclusive"]),
                    need["asof"],
                )
    return cache, plan


def acquire_full(needs, client, *, metrics=None, retained_endpoints=None, progress=None, on_failure=None):
    """Acquire shared ranges, then batch the resulting history's first-session checks.

    The returned client feeds the normal downloader; its existing bounded retry
    requests a fresh plan if the provider changed during acquisition.
    """
    cache, acquisition = _prefetch(
        needs, client, check=False, metrics=metrics, progress=progress, on_failure=on_failure
    )
    probes = []
    for need in needs:
        symbol, window = need["symbols"][0], need["ranges"][0]
        start, end = timestamp(window["start"]), timestamp(window["end_exclusive"])
        keys = {name: (False, symbol, name, start, end, need["asof"]) for name in ADJUSTMENTS}
        if any(key not in cache for key in keys.values()):
            continue
        views = {name: cache[key].rows for name, key in keys.items()}
        prior = (retained_endpoints or {}).get(symbol)
        endpoints = _incremental_endpoints(prior, views, start) if prior is not None else endpoints_of(views)
        # These are fresh producer checks for newly acquired output, not a
        # revalidation of completed stored history. Their independent responses
        # must not be replaced with the acquisition body being checked.
        probes.extend(
            _endpoint_needs(
                symbol,
                {name: rows[:1] for name, rows in endpoints.items()},
                need["asof"],
                "acquisition_check",
                cutoff=end,
            )
        )
    checked, checks = _prefetch(
        probes, client, check=True, metrics=metrics, progress=progress, on_failure=on_failure
    )
    cache.update(checked)
    return PlannedClient(client, cache), {
        "schema_version": acquisition["schema_version"],
        "steps": acquisition["steps"] + checks["steps"],
    }


def acquire_retained(selected, end, *, client, metrics, progress=None):
    """Batch the ordinary downloader's three request stages for selected sources."""
    # These first/last checks and possible history replacements are for explicitly
    # selected retained dependencies. They are not a substitute for the daily
    # summary's single last-all check or permission to revalidate its other views.
    failures, probes = {}, []

    def failed(_step, action, error):
        outcome = failure_outcome(error)
        exact_fallback = (
            len(action["request"]["params"]["symbols"].split(",")) > 1 and outcome["http_status"] == 400
        )
        for target in action["deliver"]:
            for symbol in target["symbols"]:
                prior = failures.get(symbol)
                if prior is None or prior[1] and not exact_fallback or outcome["outcome"] == "access_denied":
                    failure = (
                        established_pricing_rejection(symbol, error)
                        if action["why"] == "retained_history_acquisition"
                        and isinstance(error, BarsSymbolUnavailable)
                        else error
                    )
                    failures[symbol] = (AcquisitionFailure.from_error(failure), exact_fallback)

    for symbol, (source, endpoints) in selected.items():
        asof = acquisition_asof(source, end)
        probes.extend(_endpoint_needs(symbol, endpoints, asof, "boundary_check", cutoff=end))
    cache, _ = _prefetch(probes, client, check=True, metrics=metrics, progress=progress, on_failure=failed)
    comparison = PlannedClient(client, dict(cache))
    needs, retained = [], {}
    for symbol, (source, endpoints) in selected.items():
        if symbol in failures:
            continue
        start = timestamp(source.get("history_start", HISTORY_START))
        asof = acquisition_asof(source, end)
        if _endpoints_match(comparison, symbol, endpoints, asof, cutoff=end):
            start = max(start, timestamp(source["anchor_month"] + "-01T00:00:00Z"))
            retained[symbol] = endpoints
        needs.append(
            {
                "kind": "bars",
                "why": "retained_history_acquisition",
                "symbols": [symbol],
                "adjustments": list(ADJUSTMENTS),
                "asof": asof,
                "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
            }
        )

    def batches():
        from .append import _cohorts

        if failures:
            yield list(failures), client, failures
        # Endpoint results select the actual ranges. Bound those acquisitions,
        # releasing their buffered histories before acquiring the next cohort.
        for cohort in _cohorts(needs):
            symbols = {need["symbols"][0] for need in cohort}
            consumer, _ = acquire_full(
                cohort,
                client,
                metrics=metrics,
                retained_endpoints=retained,
                progress=progress,
                on_failure=failed,
            )
            yield (
                sorted(symbols),
                PlannedClient(consumer, {key: value for key, value in cache.items() if key[1] in symbols}),
                failures,
            )
            del consumer

    return batches()


def replace_prepared(
    store, prepared, *, client, cohort_size=32, progress=None, before_publish=None, context=None
):
    """Bound memory by cohort; the solver alone chooses concrete API requests."""
    if type(cohort_size) is not int or cohort_size < 1:
        raise DownloadError("refresh cohort size must be a positive integer")
    context = accept_context(store, context)
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
            consumer, _ = acquire_full(needs, client, metrics=metrics, progress=progress)
        except (RuntimeError, OSError, ValueError) as error:
            results.extend(
                {
                    "symbol": item["symbol"],
                    "status": "failed",
                    "error": str(error),
                    "outcome": failure_outcome(error),
                }
                for item in cohort
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
                    # Preserve the selected generation across network I/O.
                    expected_source_sha256=item["source_sha256"],
                    history_correction_id=item.get("required_correction_id"),
                    context=context,
                )
                result["status"] = "replaced"
            except (RuntimeError, OSError, ValueError) as error:
                result = {
                    "symbol": item["symbol"],
                    "status": "failed",
                    "error": str(error),
                    "outcome": failure_outcome(error),
                }
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

"""Batched acquisition and guarded replacement of existing pricing spans."""

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from time import monotonic

from libs.alpaca_data_api_solver import build_plan
from libs.common.debug_trace import traced
from libs.common.provider_http import balanced_batches

from .adaptive import AdjustmentAcquisition, acquire_adjustments
from .context import accept_context
from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    NEW_YORK,
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
    acquisition_asof,
    download_pricing,
    established_pricing_rejection,
)
from .endpoints import endpoints_of, history_check_endpoints
from .failures import AcquisitionFailure
from .responses import completed_bars
from .storage import load_source, read_validator, regular_path, ticker_lock, trace_source_selection


@traced("retained_history_check")
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
        matched = _endpoints_match(
            client,
            symbol,
            history_check_endpoints(endpoints),
            request["asof"],
            cutoff=timestamp(request["end_exclusive"]),
            require_present=True,
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
        request = _refresh_request(symbol, source)
        trace_source_selection(source, request["source_sha256"], symbol=symbol, reason="replacement_selection")
        prepared.append(request)
    return prepared


@traced("provider_late_bar_check")
def probe_late_bar_repairs(store, selection, *, client, end_date=None, progress=None):
    """Probe only exact sessions named by a completed serving handoff.

    A positive raw-bar response selects the ordinary generation-bound full-span
    replacement. Empty responses leave the completed stored decision untouched.
    """
    sources = selection["sources"]
    report = {
        "catalogue_generation": selection["catalogue_generation"],
        "inspected_groups": selection["inspected_groups"],
        "selected_groups": selection["selected_groups"],
        "selected_identities": selection["selected_identities"],
        "selected_sources": len(sources),
        "requested_sources": 0,
        "requested_sessions": 0,
        "returned_sources": 0,
        "returned_sessions": 0,
        "stale_sources": [],
        "beyond_target": [],
        "outside_retained_span": 0,
        "requests": {},
    }
    if not sources:
        return [], report
    prepared = {item["symbol"]: item for item in prepare_refresh(store, sources)}
    needs = []
    for symbol, selected in sorted(sources.items()):
        request = prepared[symbol]
        if request["source_sha256"] != selected["source_sha256"]:
            report["stale_sources"].append(symbol)
            continue
        if end_date is not None and request["end_exclusive"][:10] > end_date:
            report["beyond_target"].append(symbol)
            continue
        ranges = []
        for value in selected["sessions"]:
            session = date.fromisoformat(value)
            start = datetime.combine(session, time.min, tzinfo=NEW_YORK).astimezone(timezone.utc)
            end = datetime.combine(session + timedelta(days=1), time.min, tzinfo=NEW_YORK).astimezone(
                timezone.utc
            )
            if value < request["start"][:10] or value >= request["end_exclusive"][:10]:
                report["outside_retained_span"] += 1
                continue
            ranges.append(
                {
                    "start": start.isoformat(),
                    "end_exclusive": end.isoformat(),
                }
            )
        if ranges:
            needs.append(
                {
                    "kind": "bars",
                    "why": "provider_late_bar_check",
                    "symbols": [symbol],
                    "asof": request["asof"],
                    "adjustments": ["raw"],
                    "ranges": ranges,
                }
            )
    report["requested_sources"] = len(needs)
    report["requested_sessions"] = sum(len(need["ranges"]) for need in needs)
    if not needs:
        return [], report
    cache, _ = _prefetch(
        needs,
        client,
        check=False,
        metrics=report["requests"],
        progress=progress,
        merge_ranges=False,
    )
    returned = {}
    for need in needs:
        symbol = need["symbols"][0]
        for window in need["ranges"]:
            key = (
                False,
                symbol,
                "raw",
                timestamp(window["start"]),
                timestamp(window["end_exclusive"]),
                need["asof"],
            )
            if cache[key].rows:
                session = timestamp(window["start"]).astimezone(NEW_YORK).date().isoformat()
                returned.setdefault(symbol, []).append(session)
    report["returned_sources"] = len(returned)
    report["returned_sessions"] = sum(len(values) for values in returned.values())
    return [
        {**prepared[symbol], "provider_late_sessions": sessions} for symbol, sessions in sorted(returned.items())
    ], report


class PlannedClient:
    """Replay each buffered request once; retries use fresh provider responses."""

    def __init__(self, client, cache, adjustments=None):
        self.client, self.cache = client, cache
        self.adjustments = adjustments or {}

    def take_adjustments(self, symbol, start, end, asof):
        key = symbol, start, end, asof
        if key in self.adjustments:
            return self.adjustments.pop(key)
        take = getattr(self.client, "take_adjustments", None)
        return take(symbol, start, end, asof) if take else AdjustmentAcquisition(*key)

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


def _prefetch(needs, client, *, check, metrics, progress=None, on_failure=None, merge_ranges=True):
    started = monotonic()
    pages_before = (metrics or {}).get("page_requests", 0)
    # Endpoint checks select exact sessions. Saving a page must not expand two
    # points into years of data that the executor normalizes and then discards.
    # Compatible identical windows still batch symbols and share pagination.
    plan = build_plan({"steps": [{"needs": needs}]}, merge_ranges=merge_ranges and not check)
    stage = needs[0]["why"] if needs else "acquisition_check"
    actions = plan["steps"][0]["actions"]
    if progress:
        progress(
            stage,
            {
                "state": "started",
                "symbols": len({need["symbols"][0] for need in needs}),
                "actions": len(actions),
                "minimum_pages": sum(
                    len(balanced_batches(action["request"]["params"]["symbols"].split(",")))
                    for action in actions
                ),
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


@traced("pricing_full_history")
def acquire_full(needs, client, *, metrics=None, retained_endpoints=None, progress=None, on_failure=None):
    """Acquire shared ranges; only new/replaced histories need producer confirmation.

    The returned client feeds the normal downloader; its existing bounded retry
    requests a fresh plan if the provider changed during acquisition.
    """
    states, acquisition = acquire_adjustments(
        needs, client, metrics=metrics, progress=progress, on_failure=on_failure
    )
    probes = []
    for need, state in zip(needs, states, strict=True):
        symbol, end = state.symbol, state.end
        if state.completed is None:
            continue
        views = state.completed.views
        if symbol in (retained_endpoints or {}):
            # A completed first/latest all comparison already approved this
            # generation. These are new-session rows only. No historical probes.
            continue
        endpoints = endpoints_of(views)
        # These are fresh producer checks for newly acquired output, not a
        # revalidation of completed stored history. Their independent responses
        # must not be replaced with the acquisition body being checked.
        probes.extend(
            _endpoint_needs(
                symbol,
                {name: endpoints[name][:1] for name in state.observed},
                need["asof"],
                "acquisition_check",
                cutoff=end,
            )
        )
    checked, checks = (
        _prefetch(probes, client, check=True, metrics=metrics, progress=progress, on_failure=on_failure)
        if probes
        else ({}, {"steps": []})
    )
    return PlannedClient(client, checked, {state.key: state for state in states}), {
        "schema_version": acquisition["schema_version"],
        "steps": acquisition["steps"] + checks["steps"],
    }


def acquire_retained(selected, end, *, client, metrics, progress=None, on_checked=None, approved=None):
    """First/latest all approval -> missing sessions, or selected full replacement."""
    failures, probes = {}, []
    approved = approved or {}

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
        if symbol in approved:
            continue
        asof = acquisition_asof(source, end)
        try:
            points = history_check_endpoints(endpoints)
        except DownloadError as error:
            failures[symbol] = (AcquisitionFailure.from_error(error), False)
            continue
        probes.extend(_endpoint_needs(symbol, points, asof, "boundary_check", cutoff=end))
    cache, _ = (
        _prefetch(probes, client, check=True, metrics=metrics, progress=progress, on_failure=failed)
        if probes
        else ({}, None)
    )
    # A completed batch omission cannot erase a known retained observation.
    # Confirm only omitted endpoints, once singly. Physical singleton responses
    # (including 414 splits) already establish their result and must not repeat.
    for key, value in list(cache.items()):
        _, symbol, adjustment, start, stop, asof = key
        if symbol in failures or value.rows or value.singleton:
            continue
        try:
            metrics["omission_probes"] = metrics.get("omission_probes", 0) + 1
            cache[key] = completed_bars(
                client.fetch(symbol, adjustment, start, stop, asof=asof),
                symbol,
                adjustment,
                start,
                stop,
                asof,
            )
            if cache[key].rows:
                metrics["omission_recovered"] = metrics.get("omission_recovered", 0) + 1
        except (RuntimeError, OSError, ValueError) as error:
            failures[symbol] = (AcquisitionFailure.from_error(error), False)
    comparison = PlannedClient(client, dict(cache))
    needs, retained = [], {}
    for symbol, (source, endpoints) in selected.items():
        if symbol in failures:
            continue
        start = timestamp(source.get("history_start", HISTORY_START))
        asof = acquisition_asof(source, end)
        try:
            matched = (
                approved[symbol]
                if symbol in approved
                else _endpoints_match(
                    comparison,
                    symbol,
                    history_check_endpoints(endpoints),
                    asof,
                    cutoff=end,
                    require_present=True,
                )
            )
        except (RuntimeError, OSError, ValueError) as error:
            failures[symbol] = (AcquisitionFailure.from_error(error), False)
            continue
        if on_checked is not None:
            on_checked(symbol, matched)
        if matched:
            start = timestamp(source["latest_request"]["end_exclusive"])
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
                consumer
                if on_checked is not None
                else PlannedClient(consumer, {key: value for key, value in cache.items() if key[1] in symbols}),
                failures,
            )
            del consumer

    return batches()


@traced("pricing_correction")
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

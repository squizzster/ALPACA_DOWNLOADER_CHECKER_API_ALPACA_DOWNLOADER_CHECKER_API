"""Bounded initial acquisition using the ordinary downloader's producer checks."""

from itertools import chain
from pathlib import Path

from . import append
from .append import _cohorts, append_entries
from .context import accept_context
from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    BarsSymbolUnavailable,
    DownloadError,
    InitialPricingUnavailable,
    digest,
    failure_outcome,
    json_bytes,
    timestamp,
)
from .download import acquisition_asof, download_pricing, request_end, request_start
from .http import BarsClient
from .refresh import acquire_full, acquire_retained
from .storage import load_source, read_update, read_validator, recover, regular_path, ticker_lock


class MeasuredRequests:
    """Use the ordinary fetch planner while retaining its page accounting."""

    fetch = BarsClient.fetch

    def __init__(self, client, metrics):
        self.client, self.metrics = client, metrics

    def execute_plan(self, plan, *, metrics=None, on_failure=None):
        options = {"on_failure": on_failure} if on_failure is not None else {}
        measured = self.metrics if metrics is None else metrics
        before = dict(measured)
        try:
            return self.client.execute_plan(plan, metrics=measured, **options)
        finally:
            if measured is not self.metrics:
                for key, value in measured.items():
                    if isinstance(value, (int, float)):
                        self.metrics[key] = self.metrics.get(key, 0) + value - before.get(key, 0)


def _endpoint_cohorts(retained):
    """Bound endpoint rows, independently of hypothetical replacement histories."""
    cohort, rows = {}, 0
    for symbol, value in retained.items():
        cost = sum(len(points) for points in (value[1] or {}).values())
        if cohort and (len(cohort) >= append.MAX_SYMBOLS or rows + cost > append.MAX_ROWS):
            yield cohort
            cohort, rows = {}, 0
        cohort[symbol] = value
        rows += cost
    if cohort:
        yield cohort


def acquire_selected(
    store,
    symbols,
    end_date,
    *,
    client,
    invalidate=(),
    progress=None,
    workers=3,
    evidence_fingerprints=None,
    from_dates=None,
    context=None,
):
    """Only selected labels open manifests; completed requests win over acknowledgements."""
    # This route checks historical endpoints in all views before acquisition.
    # A daily summary member belongs to approved append instead. Callers must not
    # select the whole store here because a compact boundary is temporarily pending.
    store = Path(store).absolute()
    evidence_fingerprints = evidence_fingerprints or {}
    from_dates = from_dates or {}
    end = request_end(end_date)
    symbols = sorted(set(symbols))
    for symbol in symbols:
        request_start(from_dates.get(symbol), end)
    context = accept_context(store, context)
    end_text = end.isoformat().replace("+00:00", "Z")
    needs, selected_generations, results, retained, empty = [], {}, [], {}, []
    for symbol in symbols:
        root = regular_path(store, f"end_of_day/pricing/{symbol}")
        # A selected acquisition may create its own paths. No universe inventory.
        root.mkdir(parents=True, exist_ok=True)
        with ticker_lock(root, exclusive=True, create=True):
            recover(root)
            source = load_source(root, symbol)
            selected_generations[symbol] = digest(json_bytes(source)) if source is not None else None
            covered = timestamp(source["latest_request"]["end_exclusive"]) if source else None
            stored_start = source.get("history_start", HISTORY_START)[:10] if source else None
            requested_start = from_dates.get(symbol)
            accepted_evidence = (
                symbol in evidence_fingerprints
                and source is not None
                and source.get("acquisition_evidence_sha256") == evidence_fingerprints[symbol]
            )
            if (
                source
                and (requested_start is None or requested_start >= stored_start)
                and (covered > end or covered == end and (symbol not in invalidate or accepted_evidence))
            ):
                results.append(
                    {
                        "symbol": symbol,
                        "status": "covered",
                        "already_beyond_target": covered > end,
                        "source_sha256": selected_generations[symbol],
                        "history_start": source.get("history_start", HISTORY_START),
                        "request": source["latest_request"],
                        "pending_update": read_update(root, published=True),
                    }
                )
                continue
            endpoints = read_validator(root, source, published=True) if source else None
        start = (
            min(stored_start, requested_start)
            if stored_start and requested_start
            else requested_start or stored_start or HISTORY_START[:10]
        )
        extending = source is not None and start < stored_start
        if extending and covered > end:
            results.append(
                {
                    "symbol": symbol,
                    "status": "failed",
                    "outcome": failure_outcome(
                        DownloadError(
                            "required history extension exceeds requested cutoff; retained data unchanged"
                        )
                    ),
                }
            )
            continue
        if source and not extending and (symbol not in invalidate or accepted_evidence):
            # Retain this coherent selection for batched ordinary endpoint checks.
            # Empty ranges use the existing incremental covered-range path.
            # DO NOT REMOVE WITHOUT USER AUTH!#
            # Empty fast append carries coverage and entity mapping separately.
            if endpoints is not None and not any(endpoints.values()):
                empty.append(
                    {
                        "symbol": symbol,
                        "asof": source["latest_request"]["asof"],
                        "end_exclusive": source["latest_request"]["end_exclusive"],
                        "last_all": None,
                    }
                )
            else:
                retained[symbol] = (source, endpoints)
            continue
        needs.append(
            {
                "kind": "bars",
                "why": "initial_or_invalidated_history",
                "symbols": [symbol],
                "adjustments": list(ADJUSTMENTS),
                "asof": acquisition_asof(source, end),
                "ranges": [
                    {
                        "start": start + "T00:00:00Z",
                        "end_exclusive": end_text,
                    }
                ],
            }
        )
    metrics = {}
    if progress:
        progress(
            "acquisition_selection",
            {
                "selected": len(symbols),
                "reused": sum(r["status"] == "covered" for r in results),
                "initial_or_invalidated": len(needs),
                "retained": len(retained),
                "empty": len(empty),
            },
        )
    ordinary_client = (
        MeasuredRequests(client, metrics) if callable(getattr(client, "execute_plan", None)) else client
    )
    for cohort in _cohorts(needs):
        if progress:
            progress(
                "acquiring",
                {"symbols": len(cohort), "completed": len(results), "selected": len(selected_generations)},
            )
        try:
            consumer, _ = acquire_full(cohort, client, metrics=metrics, progress=progress)
        except (RuntimeError, OSError, ValueError) as error:
            # A label-specific rejection must not fail every other member of a batch.
            # Exact fallback runs only for the selected failed cohort and remains bounded.
            outcome = failure_outcome(error)
            if not isinstance(error, BarsSymbolUnavailable) and outcome["http_status"] != 400:
                results.extend(
                    {
                        "symbol": need["symbols"][0],
                        "status": "blocked" if outcome["outcome"] == "access_denied" else "failed",
                        "outcome": outcome,
                    }
                    for need in cohort
                )
                continue
            consumer = ordinary_client
        for need in cohort:
            symbol = need["symbols"][0]
            try:
                value = download_pricing(
                    store,
                    symbol,
                    end_date,
                    client=consumer,
                    refresh_all=symbol in invalidate,
                    from_date=need["ranges"][0]["start"][:10],
                    selected_source_sha256=selected_generations[symbol],
                    acquisition_evidence_sha256=evidence_fingerprints.get(symbol),
                    context=context,
                )
                results.append({**value, "status": "published"})
            except InitialPricingUnavailable as error:
                results.append({"symbol": symbol, "status": "blocked", "outcome": error.evidence})
            except (RuntimeError, OSError, ValueError) as error:
                outcome = failure_outcome(error)
                results.append(
                    {
                        "symbol": symbol,
                        "status": "blocked" if outcome["outcome"] == "access_denied" else "failed",
                        "outcome": outcome,
                    }
                )
            if progress:
                progress(
                    "pricing",
                    {"completed": len(results), "selected": len(selected_generations), **results[-1]},
                )
        del consumer
    if empty:
        extended = append_entries(
            store,
            empty,
            end_date,
            client=client,
            workers=workers,
            progress=progress,
            approved_generations=selected_generations,
            context=context,
        )
        for item in extended["results"]:
            if item.get("outcome", {}).get("outcome") == "access_denied":
                item["status"] = "blocked"
            results.append(item)
        metrics["empty_coverage_append"] = extended["requests"]
    for selected in _endpoint_cohorts(retained):
        buffered = {symbol: value for symbol, value in selected.items() if value[1] is not None}
        completed = set()
        try:
            batches = (
                acquire_retained(buffered, end, client=ordinary_client, metrics=metrics, progress=progress)
                if buffered and callable(getattr(client, "execute_plan", None))
                else [(list(buffered), ordinary_client, {})]
            )
            for members, consumer, failures in chain(
                batches, [(selected.keys() - buffered.keys(), ordinary_client, {})]
            ):
                for symbol in members:
                    try:
                        failure, exact_fallback = failures.get(symbol, (None, False))
                        if failure is not None and not exact_fallback:
                            raise failure.exception()
                        value = download_pricing(
                            store,
                            symbol,
                            end_date,
                            client=ordinary_client if failure is not None else consumer,
                            selected_source_sha256=selected_generations[symbol],
                            context=context,
                        )
                        results.append({**value, "status": "published"})
                    except (RuntimeError, OSError, ValueError) as error:
                        outcome = failure_outcome(error)
                        results.append(
                            {
                                "symbol": symbol,
                                "status": "blocked" if outcome["outcome"] == "access_denied" else "failed",
                                "outcome": outcome,
                            }
                        )
                    completed.add(symbol)
                    if progress:
                        progress(
                            "pricing",
                            {"completed": len(results), "selected": len(selected_generations), **results[-1]},
                        )
                del consumer
        except (RuntimeError, OSError, ValueError) as error:
            outcome = failure_outcome(error)
            results.extend(
                {
                    "symbol": symbol,
                    "status": "blocked" if outcome["outcome"] == "access_denied" else "failed",
                    "outcome": outcome,
                }
                for symbol in selected.keys() - completed
            )
    return {
        "results": results,
        "requests": metrics,
        "failed": sum(item["status"] in {"failed", "blocked"} for item in results),
    }

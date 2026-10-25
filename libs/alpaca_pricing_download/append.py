"""Approved history -> only missing sessions -> ordinary pricing publication.

The caller owns the completed history-check/replacement boundary. This stage
consumes its in-memory summary; it does not repeat provider history checks.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path

from libs.alpaca_data_api_solver import build_plan
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .check_summary import assert_current, validate_entry
from .context import accept_context
from .contract import (
    ADJUSTMENTS,
    HISTORY_START,
    DownloadError,
    digest,
    failure_outcome,
    json_bytes,
    symbol_label,
    timestamp,
)
from .download import (
    _incremental_endpoints,
    _join,
    acquisition_asof,
    publish_acquired,
    request_end,
    updated_boundaries,
)
from .endpoints import endpoints_of, same_row, session_extent
from .handoff import load_published_boundary
from .responses import completed_bars
from .storage import load_source, read_validator, recover, regular_path, ticker_lock

# Bound provider work, not just URL length. A valid but oversized request can
# exhaust gateway retries before publishing anything; fewer requests can take
# much longer. Adjustment views have different costs: verify all eight before
# increasing this limit, not just the all-adjusted view or a small response limit.
MAX_SYMBOLS = 2000
MAX_ROWS = 200000  # Bound acquired rows across eight views per publication cohort.


def _validate_append_entry(entry):
    """Accept the active-summary row or an internal empty-source baseline."""
    if entry.get("last_all") is not None:
        symbol, asof, end, row = validate_entry(entry)
        return symbol, asof, timestamp(end), row
    if set(entry) != {"symbol", "asof", "end_exclusive", "last_all"}:
        raise DownloadError("invalid empty append entry")
    symbol = symbol_label(entry["symbol"])
    try:
        asof = date.fromisoformat(entry["asof"]).isoformat()
    except (TypeError, ValueError) as error:
        raise DownloadError("invalid empty append asof") from error
    end = timestamp(entry["end_exclusive"])
    return symbol, asof, end, None


def append_needs(entries, end, *, end_by_symbol=None):
    """Pure needs compiler: saved covered end/asof -> frozen missing interval."""
    needs, unchanged, seen = [], [], set()
    for entry in entries:
        symbol, asof, covered, _ = _validate_append_entry(entry)
        if symbol in seen:
            raise DownloadError(f"duplicate append ticker: {symbol}")
        seen.add(symbol)
        target = end if end_by_symbol is None else end_by_symbol[symbol]
        start = covered
        if start >= target:
            unchanged.append({"symbol": symbol, "status": "unchanged", "mode": "unchanged", "changed": False})
            continue
        needs.append(
            {
                "kind": "bars",
                "why": "append_completed_sessions",
                "symbols": [symbol],
                "adjustments": list(ADJUSTMENTS),
                "asof": acquisition_asof(
                    {"latest_request": {"asof": asof, "end_exclusive": entry["end_exclusive"]}}, target
                ),
                "ranges": [{"start": start.isoformat(), "end_exclusive": target.isoformat()}],
            }
        )
    return needs, unchanged


def _cohorts(needs):
    cohort, rows = [], 0
    for need in needs:
        window = need["ranges"][0]
        sessions = expected_market_session_coverage(window["start"], window["end_exclusive"]).session_count
        cost = max(1, sessions) * len(ADJUSTMENTS)
        if cohort and (len(cohort) >= MAX_SYMBOLS or rows + cost > MAX_ROWS):
            yield cohort
            cohort, rows = [], 0
        cohort.append(need)
        rows += cost
    if cohort:
        yield cohort


def select_append_sources(store, entries, end_date, *, required_symbols=()):
    """Select generations for the append subset before its history check begins."""
    needs, _ = append_needs(entries, request_end(end_date))
    result = {}
    selected = {need["symbols"][0] for need in needs} | set(required_symbols)
    for symbol in sorted(selected):
        root = regular_path(Path(store), f"end_of_day/pricing/{symbol}", published=True)
        with ticker_lock(root, exclusive=False):
            source = load_source(root, symbol)
            if source is None:
                raise DownloadError(f"{symbol}: selected append source missing")
            result[symbol] = digest(json_bytes(source))
    return result


def _publish_append(store, entry, need, bars, *, approved_source_sha256=None):
    symbol = entry["symbol"]
    root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
    window = need["ranges"][0]
    start, end = timestamp(window["start"]), timestamp(window["end_exclusive"])
    views = {
        name: completed_bars(bars[name], symbol, name, start, end, need["asof"]).rows for name in ADJUSTMENTS
    }
    with ticker_lock(root, exclusive=True):
        if regular_path(root, ".pending.json").exists():
            raise DownloadError("finish interrupted pricing publication before appending")
        recover(root)  # Discard only unjournaled staging, under the existing ticker lock.
        previous = load_source(root, symbol)
        if entry["last_all"] is not None and approved_source_sha256 is None:
            raise DownloadError(f"{symbol}: nonempty append requires a completed history-check approval")
        if approved_source_sha256 is not None and digest(json_bytes(previous)) != approved_source_sha256:
            raise DownloadError(f"{symbol}: source generation changed after the approved handoff")
        if (
            previous is None
            or timestamp(previous["latest_request"]["end_exclusive"]) != start
            or previous["latest_request"]["asof"] != entry["asof"]
        ):
            raise DownloadError(f"{symbol}: source coverage changed after the approved handoff")
        endpoints = read_validator(root, previous, published=True)
        expected, actual = entry["last_all"], endpoints["all"] if endpoints else None
        if not endpoints or (expected is None and actual) or (expected is not None and not actual):
            raise DownloadError(f"{symbol}: source baseline changed after the approved handoff")
        if expected is not None and not same_row(actual[-1], expected):
            raise DownloadError(f"{symbol}: source baseline changed after the approved handoff")
        if expected is None and any(endpoints.values()):
            raise DownloadError(f"{symbol}: partial adjustment history requires retained-history checking")
        for name, rows in views.items():
            if rows and endpoints[name] and timestamp(rows[0][0]) <= timestamp(endpoints[name][-1][0]):
                raise DownloadError(f"{symbol}: append would overlap approved {name} history")
        # Read only the hot month. Approved history is retained, never requested again.
        old_documents = {}
        read_selected = partial(load_published_boundary, selected=True, documents=old_documents)
        month = read_selected(root, previous, "current_month")
        combined = _join(month, views)
        anchor = (end - timedelta(days=1)).strftime("%Y-%m")
        changed_views = updated_boundaries(root, previous, anchor, combined, read=read_selected)
        selected = _incremental_endpoints(endpoints, views, start)
        update = session_extent(endpoints_of(views))
        request = {
            "symbol": symbol,
            "start": start.isoformat().replace("+00:00", "Z"),
            "end_exclusive": end.isoformat().replace("+00:00", "Z"),
            "asof": need["asof"],
            "feed": "sip",
            "currency": "USD",
            "timeframe": "1Day",
        }
        written = publish_acquired(
            root,
            previous,
            symbol,
            anchor,
            request,
            timestamp(previous.get("history_start", HISTORY_START)),
            selected,
            changed_views,
            update,
            old_documents=old_documents,
        )
        return {
            "symbol": symbol,
            "status": "updated" if update else "unchanged",
            "mode": "append",
            "request": request,
            "changed": update is not None,
            "added_rows": {name: len(rows) for name, rows in views.items()},
            "files_written": written["files_written"],
            "source_sha256": written["source_sha256"],
            "history_start": written["source"]["history_start"],
            "pending_update": written["pending_update"],
        }


def append_entries(
    store,
    entries,
    end_date=None,
    *,
    client,
    workers=3,
    progress=None,
    end_by_symbol=None,
    before_publish=None,
    approved_generations=None,
    context=None,
):
    """Batch explicit trusted baselines through the ordinary append publisher."""
    if workers not in (1, 2, 3):
        raise DownloadError("append workers must be 1, 2 or 3")
    store = Path(store).absolute()
    context = accept_context(store, context)
    entries = list(entries)
    if end_by_symbol is not None and set(end_by_symbol) != {entry.get("symbol") for entry in entries}:
        raise DownloadError("append target map does not match its entries")
    if end_by_symbol is None:
        end = request_end(end_date)
    else:
        end_by_symbol = {
            symbol: request_end((value if isinstance(value, datetime) else timestamp(value)).date().isoformat())
            for symbol, value in end_by_symbol.items()
        }
        end = max(end_by_symbol.values()) if end_by_symbol else request_end(end_date)
    needs, results = append_needs(entries, end, end_by_symbol=end_by_symbol)
    by_symbol = {entry["symbol"]: entry for entry in entries}
    approved_generations = approved_generations or {}
    for need in needs:
        symbol = need["symbols"][0]
        if by_symbol[symbol]["last_all"] is not None and symbol not in approved_generations:
            raise DownloadError(f"{symbol}: nonempty append requires a completed history-check approval")
    metrics, publishing = {}, False
    for cohort in _cohorts(needs):
        pages_before = metrics.get("page_requests", 0)
        # Different starts remain separate: never overfetch another ticker's approved dates.
        plan = build_plan({"steps": [{"needs": cohort}]}, max_symbols=MAX_SYMBOLS, merge_ranges=False)
        if progress:
            actions = len(plan["steps"][0]["actions"])
            progress("acquiring", {"tickers": len(cohort), "actions": actions, "minimum_pages": actions})
        try:
            acquired = client.execute_plan(plan, metrics=metrics)
        except (RuntimeError, OSError, ValueError) as error:
            results.extend(
                {
                    "symbol": need["symbols"][0],
                    "status": "failed",
                    "error": str(error)[:300],
                    "outcome": failure_outcome(error),
                }
                for need in cohort
            )
            if progress:
                progress(
                    "append_acquisition",
                    {"state": "failed", "page_requests": metrics.get("page_requests", 0) - pages_before},
                )
            continue
        if progress:
            progress(
                "append_acquisition",
                {"state": "completed", "page_requests": metrics.get("page_requests", 0) - pages_before},
            )
        if not publishing:
            if before_publish is not None:
                before_publish()
            publishing = True

        def publish_one(item, responses=acquired):
            index, need = item
            symbol = need["symbols"][0]
            try:
                bars = {name: responses[(0, index, 0, symbol, name)] for name in ADJUSTMENTS}
                return _publish_append(
                    store, by_symbol[symbol], need, bars, approved_source_sha256=approved_generations.get(symbol)
                )
            except (RuntimeError, OSError, ValueError, KeyError) as error:
                return {
                    "symbol": symbol,
                    "status": "failed",
                    "error": str(error)[:300],
                    "outcome": failure_outcome(error),
                }

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pricing-append") as pool:
            for result in pool.map(publish_one, enumerate(cohort)):
                results.append(result)
                if progress:
                    progress("pricing", {"done": len(results), "total": len(entries), **result})
        del publish_one, acquired
    order = {entry["symbol"]: index for index, entry in enumerate(entries)}
    results.sort(key=lambda result: order[result["symbol"]])
    failed = sum(result["status"] == "failed" for result in results)
    return {
        "requested": len(entries),
        "succeeded": len(entries) - failed,
        "failed": failed,
        "results": results,
        "requests": metrics,
    }


def append_ready(store, snapshot, end_date=None, *, client, workers=3, progress=None, context=None):
    """Consume the approved handoff directly; no disk discovery or history recheck.

    Input is the existing minimal summary snapshot, optionally selection-filtered.
    Publication checks its covered end and last row under the normal ticker lock.
    Any new lineage/CUSIP work is still owned by the unchanged lineage consumer.
    """
    store = Path(store).absolute()
    assert_current(store, snapshot["token"])
    return append_entries(
        store,
        snapshot["entries"],
        end_date,
        client=client,
        workers=workers,
        progress=progress,
        before_publish=lambda: assert_current(store, snapshot["token"]),
        approved_generations=snapshot.get("approved_generations"),
        context=context,
    )

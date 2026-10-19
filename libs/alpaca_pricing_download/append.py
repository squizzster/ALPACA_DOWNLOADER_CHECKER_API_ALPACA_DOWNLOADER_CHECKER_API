"""Approved history -> only missing sessions -> ordinary pricing publication.

The caller owns the completed history-check/replacement boundary. This stage
consumes its in-memory summary; it does not repeat provider history checks.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

from libs.alpaca_data_api_solver import build_plan
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from .check_summary import assert_current, validate_entry
from .contract import ADJUSTMENTS, HISTORY_START, DownloadError, timestamp, transform, validate_version
from .download import _incremental_endpoints, _join, publish_acquired, request_end, updated_boundaries
from .endpoints import endpoints_of, same_row, session_extent
from .handoff import load_published_boundary
from .storage import load_source, read_validator, recover, regular_path, ticker_lock

MAX_SYMBOLS = 8000  # Verified live with all eight views and full pagination.
MAX_ROWS = 200000  # Bound acquired rows across eight views per publication cohort.


def append_needs(entries, end):
    """Pure needs compiler: saved covered end/asof -> frozen missing interval."""
    needs, unchanged, seen = [], [], set()
    for entry in entries:
        symbol, asof, _ = validate_entry(entry)
        if symbol in seen:
            raise DownloadError(f"duplicate append ticker: {symbol}")
        seen.add(symbol)
        start = timestamp(asof + "T00:00:00Z")
        if start >= end:
            unchanged.append({"symbol": symbol, "status": "unchanged", "mode": "unchanged", "changed": False})
            continue
        needs.append(
            {
                "kind": "bars",
                "why": "append_completed_sessions",
                "symbols": [symbol],
                "adjustments": list(ADJUSTMENTS),
                "asof": end.date().isoformat(),
                "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
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


def _publish_append(store, entry, need, bars):
    symbol = entry["symbol"]
    root = regular_path(store, f"end_of_day/pricing/{symbol}")
    if not root.is_dir():
        raise DownloadError(f"{symbol}: approved pricing source is missing")
    window = need["ranges"][0]
    start, end = timestamp(window["start"]), timestamp(window["end_exclusive"])
    views = {name: transform(bars[name], start, end) for name in ADJUSTMENTS}
    with ticker_lock(root, exclusive=True):
        if regular_path(root, ".pending.json").exists():
            raise DownloadError("finish interrupted pricing publication before appending")
        recover(root)  # Discard only unjournaled staging, under the existing ticker lock.
        previous = load_source(root, symbol)
        if (
            previous is None
            or timestamp(previous["latest_request"]["end_exclusive"]) != start
            or previous["latest_request"]["asof"] != entry["asof"]
        ):
            raise DownloadError(f"{symbol}: source coverage changed after the approved handoff")
        endpoints = read_validator(root, previous)
        if not endpoints or not endpoints["all"] or not same_row(endpoints["all"][-1], entry["last_all"]):
            raise DownloadError(f"{symbol}: source baseline changed after the approved handoff")
        for name, rows in views.items():
            if rows and endpoints[name] and timestamp(rows[0][0]) <= timestamp(endpoints[name][-1][0]):
                raise DownloadError(f"{symbol}: append would overlap approved {name} history")
        # Read only the hot month. Approved history is retained, never requested again.
        month = load_published_boundary(root, previous, "current_month")
        combined = _join(month, views)
        anchor = (end - timedelta(days=1)).strftime("%Y-%m")
        changed_views = updated_boundaries(root, previous, anchor, combined, read=load_published_boundary)
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
        )
        return {
            "symbol": symbol,
            "status": "updated" if update else "unchanged",
            "mode": "append",
            "request": request,
            "changed": update is not None,
            "added_rows": {name: len(rows) for name, rows in views.items()},
            "files_written": written,
        }


def append_ready(store, snapshot, end_date=None, *, client, workers=3, progress=None):
    """Consume the approved handoff directly; no disk discovery or history recheck.

    Input is the existing minimal summary snapshot, optionally selection-filtered.
    Publication checks its covered end and last row under the normal ticker lock.
    Any new lineage/CUSIP work is still owned by the unchanged lineage consumer.
    """
    validate_version()
    if workers not in (1, 2, 3):
        raise DownloadError("append workers must be 1, 2 or 3")
    store = Path(store).absolute()
    end = request_end(end_date)
    entries = snapshot["entries"]
    needs, results = append_needs(entries, end)
    by_symbol = {entry["symbol"]: entry for entry in entries}
    assert_current(store, snapshot["token"])
    metrics, publishing = {}, False
    for cohort in _cohorts(needs):
        # Different starts remain separate: never overfetch another ticker's approved dates.
        plan = build_plan({"steps": [{"needs": cohort}]}, max_symbols=MAX_SYMBOLS, merge_ranges=False)
        if progress:
            progress("acquiring", {"tickers": len(cohort), "actions": len(plan["steps"][0]["actions"])})
        try:
            acquired = client.execute_plan(plan, metrics=metrics)
        except (RuntimeError, OSError, ValueError) as error:
            results.extend(
                {"symbol": need["symbols"][0], "status": "failed", "error": str(error)[:300]} for need in cohort
            )
            continue
        if not publishing:
            assert_current(store, snapshot["token"])
            publishing = True

        def publish_one(item, responses=acquired):
            index, need = item
            symbol = need["symbols"][0]
            try:
                bars = {name: responses[(0, index, 0, symbol, name)] for name in ADJUSTMENTS}
                return _publish_append(store, by_symbol[symbol], need, bars)
            except (RuntimeError, OSError, ValueError, KeyError) as error:
                return {"symbol": symbol, "status": "failed", "error": str(error)[:300]}

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

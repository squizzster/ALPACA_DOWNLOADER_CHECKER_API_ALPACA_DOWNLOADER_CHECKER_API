"""Production live check: trusted summary -> exact first/last adjusted comparisons.

The versioned JSON result retains its established experiment boundary name.
No ticker-file discovery, publication or saved report occurs here.
"""

import json
import time
import urllib.error
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from libs.alpaca_data_api_solver import build_plan
from libs.common.debug_trace import event
from libs.common.provider_http import balanced_batches

from .contract import COLUMNS, NEW_YORK, DownloadError, failure_outcome, timestamp
from .http import execute_bar_plan

VERSION = 8
DEFAULT_BATCH_SIZE = 4000


def validate_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_live_check_experiment_change_log.json"
    entries = json.loads(path.read_bytes())
    if len(entries) != VERSION or any("version" in entry or not entry.get("modifications") for entry in entries):
        raise DownloadError("live-check experiment changelog/version mismatch")


def _endpoints(entry):
    first, last = entry["first_all"], entry["last_all"]
    return (("first_and_last", first),) if first == last else (("first", first), ("last", last))


def plan(snapshot, batch_size=DEFAULT_BATCH_SIZE):
    groups = defaultdict(set)
    for entry in snapshot["entries"]:
        for _, row in _endpoints(entry):
            session = timestamp(row[0]).astimezone(NEW_YORK).date()
            groups[session, entry["asof"]].add(entry["symbol"])
    needs = [
        {
            "kind": "bars",
            "why": "boundary_check",
            "symbols": sorted(symbols),
            "adjustments": ["all"],
            "asof": asof,
            "ranges": [
                {
                    "start": datetime.combine(day, datetime.min.time(), tzinfo=NEW_YORK).isoformat(),
                    # The saved asof selects the entity. The exact retained row
                    # selects the observation window independently. UTC midnight
                    # preserves the established request bound without reaching
                    # the next New York daily bar.
                    "end_exclusive": datetime.combine(
                        day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc
                    ).isoformat(),
                }
            ],
        }
        for (day, asof), symbols in sorted(groups.items())
    ]
    # A first/last pair must never become an intervening full-history request.
    return build_plan({"steps": [{"needs": needs}]}, max_symbols=batch_size, merge_ranges=False)


def changed_fields(stored, live):
    result = {}
    for index, name in enumerate(COLUMNS):
        equal = (
            timestamp(stored[index]) == timestamp(live[index])
            if index == 0
            else Decimal(stored[index]) == Decimal(live[index])
        )
        if not equal:
            result[name] = {"stored": stored[index], "live": live[index]}
    return result


def verify(snapshot, get_page, *, batch_size=DEFAULT_BATCH_SIZE, progress=None):
    """Consume the trusted snapshot through the API solver and bar executor."""
    # The summary producer owns membership and both stored all-view endpoints. This owner
    # compares fresh provider observations only; it must not reopen local sources
    # or imply that a matching endpoint validates every interior historical row.
    started = time.monotonic()
    action_plan = plan(snapshot, batch_size)
    metrics = {
        "planned_batches": sum(
            len(balanced_batches(action["request"]["params"]["symbols"].split(",")))
            for step in action_plan["steps"]
            for action in step["actions"]
        ),
        "page_requests": 0,
        "oversize_splits": 0,
    }
    stats = dict.fromkeys(
        (
            "tickers",
            "last_session_dates",
            "checked",
            "matched",
            "differed",
            "ohlc_changed",
            "other_fields_only",
            "missing",
            "failed",
            "request_failures",
        ),
        0,
    )
    stats["tickers"] = len(snapshot["entries"])
    stats["last_session_dates"] = len(
        {timestamp(entry["last_all"][0]).astimezone(NEW_YORK).date() for entry in snapshot["entries"]}
    )
    differences, missing, missing_endpoints, matched, failures, failed = [], [], [], [], [], set()
    singleton_requests = set()

    def request_key(params, symbol):
        return symbol, params["start"], params["end"], params["asof"], params["adjustment"]

    def request(params):
        # Physical 414 subdivisions may already have checked a singleton. Keep
        # that request fact in memory so an empty result is not checked twice.
        if "," not in params["symbols"]:
            singleton_requests.add(request_key(params, params["symbols"]))
        return get_page(params)

    def on_failure(step, action, error):
        symbols = sorted({symbol for target in action["deliver"] for symbol in target["symbols"]})
        failed.update(symbols)
        cause = error.__cause__ or error
        code = cause.code if isinstance(cause, urllib.error.HTTPError) else None
        failures.append(
            {
                "symbols": symbols,
                "reason": f"HTTP {code}" if code else str(error)[:300],
                "outcome": failure_outcome(error),
            }
        )
        stats["request_failures"] += 1

    acquired = execute_bar_plan(action_plan, request, metrics=metrics, on_failure=on_failure)
    probe_steps = []
    for step_index, step in enumerate(action_plan["steps"]):
        probes = []
        for action in step["actions"]:
            params = action["request"]["params"]
            omitted = set()
            for target in action["deliver"]:
                for symbol in target["symbols"]:
                    value = acquired.get((step_index, target["need"], target["range"], symbol, "all"))
                    if (
                        value is not None
                        and not value.rows
                        and symbol not in failed
                        and request_key(params, symbol) not in singleton_requests
                    ):
                        omitted.add(symbol)
            for symbol in sorted(omitted):
                probes.append(
                    {
                        **action,
                        "request": {**action["request"], "params": {**params, "symbols": symbol}},
                        "deliver": [
                            {**target, "symbols": [symbol]}
                            for target in action["deliver"]
                            if symbol in target["symbols"]
                        ],
                    }
                )
        probe_steps.append({**step, "actions": probes})
    if any(step["actions"] for step in probe_steps):
        # A completed multi-symbol response can suppress a known retained bar
        # through its companions. Only this comparison owner knows a row is
        # expected. Probe only omitted endpoints, singly, with identical scope;
        # normal empty history acquisition remains the transport's authority.
        metrics["omission_probes"] = sum(len(step["actions"]) for step in probe_steps)
        confirmed = execute_bar_plan(
            {**action_plan, "steps": probe_steps}, request, metrics=metrics, on_failure=on_failure
        )
        metrics["omission_recovered"] = sum(len(value.rows) == 1 for value in confirmed.values())
        acquired.update(confirmed)
    actual = {
        (values.symbol, values.start.astimezone(NEW_YORK).date(), values.asof): values.rows
        for values in acquired.values()
    }
    for entry in snapshot["entries"]:
        symbol = entry["symbol"]
        if symbol in failed:
            continue
        changes, absent = [], False
        for endpoint, stored in _endpoints(entry):
            day = timestamp(stored[0]).astimezone(NEW_YORK).date()
            rows = actual.get((symbol, day, entry["asof"]))
            if rows is None or len(rows) > 1:
                failures.append(
                    {
                        "symbols": [symbol],
                        "reason": "missing plan delivery"
                        if rows is None
                        else "multiple bars for one daily session",
                    }
                )
                failed.add(symbol)
                break
            if not rows:
                absent = True
                detail = {
                    "symbol": symbol,
                    "endpoint": endpoint,
                    "session": day.isoformat(),
                    "asof": entry["asof"],
                }
                missing_endpoints.append(detail)
                event("missing_history_endpoint", **detail)
                continue
            fields = changed_fields(stored, rows[0])
            if fields:
                category = "ohlc_changed" if set(fields).intersection(COLUMNS[1:5]) else "other_fields_only"
                changes.append(
                    {
                        "symbol": symbol,
                        "session": day.isoformat(),
                        "endpoint": endpoint,
                        "kind": category,
                        "fields": fields,
                    }
                )
        if symbol in failed:
            continue
        stats["checked"] += 1
        if absent:
            missing.append(symbol)
            stats["missing"] += 1
            continue
        if not changes:
            stats["matched"] += 1
            matched.append(symbol)
            continue
        category = (
            "ohlc_changed" if any(item["kind"] == "ohlc_changed" for item in changes) else "other_fields_only"
        )
        stats[category] += 1
        stats["differed"] += 1
        differences.extend(changes)
    stats["failed"] = len(failed)
    if progress is not None:
        progress(stats["checked"] + stats["failed"], stats["tickers"], metrics["page_requests"])
    if stats["checked"] + stats["failed"] != stats["tickers"]:
        raise DownloadError("live comparison did not account for every input ticker")
    return {
        "schema_version": VERSION,
        "boundary": "alpaca_last_session_live_check_experiment",
        "baseline_sha256": snapshot["sha256"],
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "complete": not (stats["missing"] or stats["failed"]),
        "stats": stats,
        "requests": metrics,
        "live_seconds": round(time.monotonic() - started, 6),
        "differences": sorted(differences, key=lambda item: item["symbol"]),
        "matched": sorted(matched),
        "missing": sorted(missing),
        "missing_endpoints": sorted(missing_endpoints, key=lambda item: (item["symbol"], item["session"])),
        "failures": failures,
    }

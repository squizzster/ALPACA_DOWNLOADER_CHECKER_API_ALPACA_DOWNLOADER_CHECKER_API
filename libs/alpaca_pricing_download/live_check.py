"""Trusted first/tail summary -> exact, completed three-way pricing decisions."""

import json
import time
import urllib.error
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import datetime, timezone
from pathlib import Path

from libs.alpaca_data_api_solver import build_plan
from libs.common.debug_trace import event
from libs.common.provider_http import balanced_batches

from .contract import COLUMNS, NEW_YORK, DownloadError, failure_outcome, timestamp
from .endpoints import Endpoints, compare_history, comparison_rows, different_fields, history_windows
from .http import execute_bar_plan

VERSION = 12
DEFAULT_BATCH_SIZE = 4000


def validate_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_live_check_experiment_change_log.json"
    entries = json.loads(path.read_bytes())
    if len(entries) != VERSION or any("version" in entry or not entry.get("modifications") for entry in entries):
        raise DownloadError("live-check experiment changelog/version mismatch")


def _points(entry):
    if entry.get("tail_all") is None:
        raise DownloadError("legacy pricing comparison tail requires its selected producer upgrade")
    return Endpoints({"all": (entry["first_all"], entry["last_all"])}, tail_all=entry["tail_all"])


def _endpoints(entry):
    points = _points(entry)
    return tuple(
        (
            "first_and_last"
            if entry["first_all"] == entry["last_all"]
            else "first"
            if tuple(row) == tuple(entry["first_all"])
            else "last"
            if tuple(row) == tuple(entry["last_all"])
            else "tail",
            row,
        )
        for row in comparison_rows(entry["first_all"], points.tail_all)
    )


def _day(row):
    return timestamp(row[0]).astimezone(NEW_YORK).date()


def plan(snapshot, batch_size=DEFAULT_BATCH_SIZE):
    groups = defaultdict(set)
    for entry in snapshot["entries"]:
        for start, end in history_windows(_points(entry), cutoff=timestamp(entry["end_exclusive"])):
            groups[start, end, entry["asof"]].add(entry["symbol"])
    needs = [
        {
            "kind": "bars",
            "why": "boundary_check",
            "symbols": sorted(symbols),
            "adjustments": ["all"],
            "asof": asof,
            "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
        }
        for (start, end, asof), symbols in sorted(groups.items())
    ]
    return build_plan({"steps": [{"needs": needs}]}, max_symbols=batch_size, merge_ranges=False)


def changed_fields(stored, live):
    return {
        name: {"stored": stored[COLUMNS.index(name)], "live": live[COLUMNS.index(name)]}
        for name in different_fields(stored, live)
    }


def verify(snapshot, get_page, *, batch_size=DEFAULT_BATCH_SIZE, progress=None, missing_route=None):
    """Compare the producer's baseline once and carry usable observations forward."""
    started = time.monotonic()
    action_plan = plan(snapshot, batch_size)
    entries = {entry["symbol"]: entry for entry in snapshot["entries"]}
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
            "latest_session",
            "full_history",
        ),
        0,
    )
    stats["tickers"] = len(entries)
    stats["last_session_dates"] = len({_day(entry["last_all"]) for entry in entries.values()})
    differences, missing, missing_endpoints, matched, failures, failed = [], [], [], [], [], set()
    decisions = {}

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

    acquired = execute_bar_plan(action_plan, get_page, metrics=metrics, on_failure=on_failure)
    # Multi-symbol responses can omit one date while returning other tail rows.
    # Probe only missing dates singly; physical singletons already own absence.
    for key, value in list(acquired.items()):
        if value.symbol in failed or value.singleton:
            continue
        actual_days = {_day(row) for row in value.rows}
        expected = [
            row
            for _, row in _endpoints(entries[value.symbol])
            if value.start <= timestamp(row[0]) < value.end and _day(row) not in actual_days
        ]
        if not expected:
            continue
        needs = []
        for row in expected:
            points = Endpoints({"all": (row,)}, tail_all=(row,))
            start, end = history_windows(points, cutoff=value.end)[0]
            needs.append(
                {
                    "kind": "bars",
                    "why": "boundary_check",
                    "symbols": [value.symbol],
                    "adjustments": ["all"],
                    "asof": value.asof,
                    "ranges": [{"start": start.isoformat(), "end_exclusive": end.isoformat()}],
                }
            )
        probes = build_plan({"steps": [{"needs": needs}]}, merge_ranges=False)
        metrics["omission_probes"] = metrics.get("omission_probes", 0) + len(needs)
        confirmed = execute_bar_plan(probes, get_page, metrics=metrics, on_failure=on_failure)
        recovered = tuple(row for result in confirmed.values() for row in result.rows)
        metrics["omission_recovered"] = metrics.get("omission_recovered", 0) + len(recovered)
        acquired[key] = replace(
            value, rows=tuple(sorted((*value.rows, *recovered), key=lambda row: timestamp(row[0])))
        )
    actual = defaultdict(list)
    for value in acquired.values():
        actual[value.symbol].extend(value.rows)
    mapped_endpoints = []
    for symbol, entry in entries.items():
        if symbol in failed:
            continue
        by_day = {}
        for row in actual[symbol]:
            if _day(row) in by_day:
                failed.add(symbol)
                failures.append({"symbols": [symbol], "reason": "multiple bars for one daily session"})
                break
            by_day[_day(row)] = row
        if symbol in failed:
            continue
        if missing_route is not None:
            for endpoint, stored in _endpoints(entry):
                day = _day(stored)
                if day in by_day:
                    continue
                try:
                    route = missing_route(entry, day.isoformat())
                except (RuntimeError, OSError, ValueError) as error:
                    failures.append({"symbols": [symbol], "reason": str(error)[:300]})
                    failed.add(symbol)
                    break
                if route is None:
                    continue
                start, end = history_windows(
                    Endpoints({"all": (stored,)}, tail_all=(stored,)),
                    cutoff=datetime.fromisoformat(entry["end_exclusive"].replace("Z", "+00:00")),
                )[0]
                alternate = build_plan(
                    {
                        "steps": [
                            {
                                "needs": [
                                    {
                                        "kind": "bars",
                                        "why": "boundary_check",
                                        "symbols": [route["symbol"]],
                                        "adjustments": ["all"],
                                        "asof": route["asof"],
                                        "ranges": [
                                            {"start": start.isoformat(), "end_exclusive": end.isoformat()}
                                        ],
                                    }
                                ]
                            }
                        ]
                    },
                    merge_ranges=False,
                )
                metrics["mapped_endpoint_probes"] = metrics.get("mapped_endpoint_probes", 0) + 1
                route_failures = []
                observed = execute_bar_plan(
                    alternate,
                    get_page,
                    metrics=metrics,
                    on_failure=lambda step, action, error: route_failures.append(error),
                )
                if route_failures:
                    failures.append({"symbols": [symbol], "reason": str(route_failures[0])[:300]})
                    failed.add(symbol)
                    break
                rows = [row for value in observed.values() for row in value.rows]
                if len(rows) == 1 and _day(rows[0]) == day:
                    by_day[day] = rows[0]
                    actual[symbol].append(rows[0])
                    detail = {"symbol": symbol, "endpoint": endpoint, "session": day.isoformat(), **route}
                    detail["symbol"] = symbol
                    detail["mapped_symbol"] = route["symbol"]
                    mapped_endpoints.append(detail)
                    event("mapped_history_endpoint", **detail)
            if symbol in failed:
                continue
        stats["checked"] += 1
        absent = False
        for endpoint, stored in _endpoints(entry):
            if _day(stored) not in by_day:
                absent = True
                detail = {
                    "symbol": symbol,
                    "endpoint": endpoint,
                    "session": _day(stored).isoformat(),
                    "asof": entry["asof"],
                }
                missing_endpoints.append(detail)
                event("missing_history_endpoint", **detail)
        if absent:
            missing.append(symbol)
            stats["missing"] += 1
            continue
        decision = compare_history(entry["first_all"], entry["tail_all"], actual[symbol])
        decisions[symbol] = asdict(decision)
        if decision.matched:
            stats["matched"] += 1
            matched.append(symbol)
            continue
        changes = []
        fields_by_session = dict(decision.field_changes)
        for endpoint, stored in _endpoints(entry):
            names = fields_by_session.get(stored[0], ())
            if not names:
                continue
            live = by_day[_day(stored)]
            fields = {
                name: {"stored": stored[COLUMNS.index(name)], "live": live[COLUMNS.index(name)]}
                for name in names
            }
            changes.append(
                {
                    "symbol": symbol,
                    "session": _day(stored).isoformat(),
                    "endpoint": endpoint,
                    "kind": "ohlc_changed" if set(names).intersection(COLUMNS[1:5]) else "other_fields_only",
                    "fields": fields,
                }
            )
        if not changes:
            changes.append(
                {
                    "symbol": symbol,
                    "session": entry["last_all"][0][:10],
                    "endpoint": "tail",
                    "kind": "other_fields_only",
                    "fields": {},
                    "cause": "retained_session_membership",
                }
            )
        stats[
            "ohlc_changed" if any(item["kind"] == "ohlc_changed" for item in changes) else "other_fields_only"
        ] += 1
        stats["differed"] += 1
        stats[decision.decision] += 1
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
        "comparisons": decisions,
        "missing": sorted(missing),
        "mapped_endpoints": mapped_endpoints,
        "missing_endpoints": sorted(missing_endpoints, key=lambda item: (item["symbol"], item["session"])),
        "failures": failures,
    }

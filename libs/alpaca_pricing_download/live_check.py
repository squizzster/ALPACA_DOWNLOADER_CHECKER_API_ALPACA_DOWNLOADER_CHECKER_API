"""Production live check: trusted summary -> solver -> exact last-session comparison.

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

from .contract import COLUMNS, NEW_YORK, DownloadError, failure_outcome, timestamp
from .http import execute_bar_plan

VERSION = 5
DEFAULT_BATCH_SIZE = 4000


def validate_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_live_check_experiment_change_log.json"
    entries = json.loads(path.read_bytes())
    if len(entries) != VERSION or any("version" in entry or not entry.get("modifications") for entry in entries):
        raise DownloadError("live-check experiment changelog/version mismatch")


def plan(snapshot, batch_size=DEFAULT_BATCH_SIZE):
    groups = defaultdict(list)
    for entry in snapshot["entries"]:
        session = timestamp(entry["last_all"][0]).astimezone(NEW_YORK).date()
        groups[session, entry["asof"]].append(entry["symbol"])
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
    return build_plan({"steps": [{"needs": needs}]}, max_symbols=batch_size)


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
    started = time.monotonic()
    action_plan = plan(snapshot, batch_size)
    metrics = {
        "planned_batches": sum(len(step["actions"]) for step in action_plan["steps"]),
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
    differences, missing, failures, failed = [], [], [], set()

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
    actual = {key[3]: values.rows for key, values in acquired.items()}
    for entry in snapshot["entries"]:
        symbol = entry["symbol"]
        if symbol in failed:
            continue
        rows = actual.get(symbol)
        if rows is None or len(rows) > 1:
            failures.append(
                {
                    "symbols": [symbol],
                    "reason": "missing plan delivery" if rows is None else "multiple bars for one daily session",
                }
            )
            failed.add(symbol)
            continue
        stats["checked"] += 1
        if not rows:
            missing.append(symbol)
            stats["missing"] += 1
            continue
        fields = changed_fields(entry["last_all"], rows[0])
        if not fields:
            stats["matched"] += 1
            continue
        category = "ohlc_changed" if set(fields).intersection(COLUMNS[1:5]) else "other_fields_only"
        stats[category] += 1
        stats["differed"] += 1
        session = timestamp(entry["last_all"][0]).astimezone(NEW_YORK).date().isoformat()
        differences.append({"symbol": symbol, "session": session, "kind": category, "fields": fields})
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
        "missing": sorted(missing),
        "failures": failures,
    }

from __future__ import annotations

import csv
import json
import os
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from .completion import completed_batch, remember_batch, request_key
from .contract import DownloadError, failure_outcome, symbol_label, validate_version
from .download import download_pricing, parse_date
from .http import BarsClient
from .storage import initialize_store
from .update_checks import begin_checks, read_checks

MAX_WORKERS = 3
DEFAULT_UPDATE_STORE = Path(__file__).resolve().parents[2] / "data_pricing"


def selected_symbols(arguments) -> list[str]:
    if arguments.symbol is not None:
        values = arguments.symbol
    else:
        path = (
            Path(__file__).resolve().parents[2] / "resources/ticker.csv"
            if arguments.all_symbols
            else arguments.file
        )
        with path.open(encoding="utf-8-sig", newline="") as stream:
            values = [value for row in csv.reader(stream, strict=True) for value in row]
    # Validate individual labels inside their transaction so one bad ticker does not abort a batch.
    symbols = list(dict.fromkeys(value.strip().upper() for value in values if value.strip()))
    if not symbols:
        raise DownloadError("ticker selection is empty")
    return symbols


def run_arguments(arguments, *, registration_context=None):
    if registration_context is None:
        return _run_arguments(arguments)
    try:
        validate_version()
        selected = selected_symbols(arguments)
        if arguments.from_date is not None:
            parse_date(arguments.from_date)
        with registration_context() as (register, policy):
            allowed = []
            for label in selected:
                try:
                    symbol = symbol_label(label)
                except (DownloadError, ValueError):
                    continue  # The ticker result reports malformed input independently.
                if policy.allowed({symbol}):
                    policy.require(
                        symbol, arguments.request_end, reason="explicit_pricing_acquisition", acquire=True
                    )
                    allowed.append(symbol)
            register(allowed)
            return _run_arguments(arguments, work_policy=policy)
    except (RuntimeError, OSError, ValueError, csv.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


def _run_arguments(arguments, *, work_policy=None):
    try:
        validate_version()
        end = arguments.request_end
        symbols = selected_symbols(arguments)
        if arguments.from_date is not None:
            parse_date(arguments.from_date)
        client = BarsClient(os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", ""))
        request = request_key(symbols, end, arguments.from_date)
        reused = not arguments.refresh_all and completed_batch(arguments.store, request)
        if reused:
            initialize_store(arguments.store.absolute())
        before = read_checks(arguments.store.absolute(), ())
        if before["epoch"] is None and arguments.store.is_dir():
            initialize_store(arguments.store.absolute())
            before = begin_checks(arguments.store.absolute())
    except (RuntimeError, OSError, ValueError, OverflowError, csv.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    events = []

    def update_ticker(symbol):
        # One worker owns the entire ticker boundary, including its lock, probes,
        # all adjustment views, bounded restart, and final publication.
        if work_policy is not None:
            try:
                work_policy.check({symbol})
            except RuntimeError as error:
                return {"symbol": symbol, "status": "failed", "error": str(error), "deferred": True}
        try:
            if reused:
                return {
                    "symbol": symbol,
                    "status": "unchanged",
                    "mode": "unchanged",
                    "requests": 0,
                    "files_written": 0,
                    "changed": False,
                }
            result = download_pricing(
                arguments.store,
                symbol,
                end,
                client=client,
                refresh_all=arguments.refresh_all,
                from_date=arguments.from_date,
                on_check=events.append,
            )
            result["status"] = (
                "recovered"
                if result.get("recovered") and result.get("requests") == 0
                else "unchanged"
                if result.get("mode") == "unchanged"
                else "empty"
                if not result["has_data"]
                else "updated"
                if result["changed"]
                else "unchanged"
            )
        except (RuntimeError, OSError, ValueError) as error:
            result = {
                "symbol": symbol,
                "status": "failed",
                "error": str(error),
                "outcome": failure_outcome(error),
            }
        return result

    results = [None] * len(symbols)
    worker_count = min(arguments.workers, len(symbols))
    remaining = iter(enumerate(symbols))
    completed = 0
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="pricing-ticker") as pool:
        pending = {
            pool.submit(update_ticker, symbol): index
            for index, symbol in (next(remaining) for _ in range(worker_count))
        }
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                result = future.result()
                if work_policy is not None and result["status"] == "failed" and not result.get("deferred"):
                    try:
                        symbol_label(symbols[index])
                    except (DownloadError, ValueError):
                        pass
                    else:
                        work_policy.fail({symbols[index]}, result, stage="acquisition")
                results[index] = result  # Keep the final summary in the deduplicated input order.
                completed += 1
                print(
                    f"[{completed}/{len(symbols)}] {symbols[index]}: {result['status']}"
                    + (f" — {result['error']}" if "error" in result else ""),
                    file=sys.stderr,
                )
                following = next(remaining, None)
                if following is not None:
                    index, symbol = following
                    pending[pool.submit(update_ticker, symbol)] = index
    failed = sum(result["status"] == "failed" for result in results)
    if not failed and not reused:
        try:
            remember_batch(arguments.store, request, before, events)
        except (DownloadError, OSError, ValueError) as error:
            # This optional memo never changes the completed per-ticker outcomes.
            print(f"completed-batch memo unavailable: {error}", file=sys.stderr)
    print(
        json.dumps(
            {
                "requested": len(symbols),
                "succeeded": len(symbols) - failed,
                "failed": failed,
                "results": results,
            },
            separators=(",", ":"),
        )
    )
    return 1 if failed else 0

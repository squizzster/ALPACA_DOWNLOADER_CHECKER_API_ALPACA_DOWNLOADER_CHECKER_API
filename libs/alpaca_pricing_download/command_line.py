from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import timedelta
from pathlib import Path

from .completion import completed_batch, remember_batch, request_key
from .contract import DownloadError, validate_version
from .download import download_pricing, parse_date, request_end
from .http import BarsClient
from .storage import initialize_store
from .update_checks import begin_checks, read_checks

MAX_WORKERS = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download ticker pricing only; reuse an identical completed request boundary, "
            "check the first and last stored sessions before advancing it, "
            "and replace all history on a mismatch. No identity, lineage, or catalogue work."
        )
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--symbol", "-s", action="append", help="exact ticker; repeat for multiple tickers")
    selection.add_argument("--file", type=Path, help="headerless CSV or one ticker per line")
    selection.add_argument(
        "--all", dest="all_symbols", action="store_true", help="all tickers in resources/ticker.csv"
    )
    parser.add_argument(
        "--store", type=Path, required=True, help="explicit standalone pricing-store destination"
    )
    parser.add_argument(
        "--workers",
        type=int,
        choices=range(1, MAX_WORKERS + 1),
        default=MAX_WORKERS,
        help="concurrent ticker workers (default: 3); all share one 199-request/minute budget",
    )
    end = parser.add_mutually_exclusive_group()
    end.add_argument("--request-end", metavar="YYYY-MM-DD", help="exclusive UTC midnight (default: today UTC)")
    end.add_argument("--to", dest="to_date", metavar="YYYY-MM-DD", help="inclusive completed date")
    parser.add_argument(
        "--from",
        dest="from_date",
        metavar="YYYY-MM-DD",
        help="history start, on/after 2016-01-01; cannot exclude stored observations",
    )
    parser.add_argument(
        "--force-download",
        "--refresh-all",
        dest="refresh_all",
        action="store_true",
        help="reacquire the full stored history range, even when endpoints match",
    )
    return parser


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


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        validate_version()
        symbols = selected_symbols(arguments)
        end_text = arguments.request_end
        if arguments.to_date is not None:
            end_text = (parse_date(arguments.to_date) + timedelta(days=1)).isoformat()
        end = request_end(end_text).date().isoformat()
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
    except (DownloadError, OSError, ValueError, OverflowError, csv.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    events = []

    def update_ticker(symbol):
        # One worker owns the entire ticker boundary, including its lock, probes,
        # all adjustment views, bounded restart, and final publication.
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
        except (DownloadError, OSError, ValueError) as error:
            result = {"symbol": symbol, "status": "failed", "error": str(error)}
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

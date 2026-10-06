#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from eod_pipeline import (
    PIPELINE_VERSION,
    PipelineIntegrityError,
    inspect_cached_artifact,
    validate_cached_download,
    validate_pipeline_change_log,
    verify_download_file,
    write_bars_csv_gz,
)


REQUESTS_PER_MINUTE = 180
MIN_REQUEST_INTERVAL = 60 / REQUESTS_PER_MINUTE
REQUEST_START = "2016-01-01T00:00:00Z"
FEED = "sip"
TIMEFRAME = "1Day"

ADJUSTMENTS = (
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)

class RateLimiter:
    def __init__(self):
        self.next_request = 0.0

    def wait(self):
        delay = self.next_request - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        self.next_request = time.monotonic() + MIN_REQUEST_INTERVAL

    def defer(self, seconds):
        self.next_request = max(self.next_request, time.monotonic() + seconds)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download daily Alpaca bars as compressed CSV."
    )
    parser.add_argument("--symbol", required=True, help="Stock ticker, for example DELL")
    parser.add_argument(
        "--adjustment",
        action="append",
        choices=ADJUSTMENTS,
        metavar="MODE",
        help=(
            "raw, split, dividend, spin-off, split,dividend, split,spin-off, "
            "dividend,spin-off, or all; repeat for multiple modes "
            "(default: every mode)"
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--force",
        action="store_true",
        help="replace a coherent, authenticated Alpaca download pair",
    )
    mode.add_argument(
        "--repair",
        action="store_true",
        help="repair a download pair when exactly one file is missing",
    )
    return parser.parse_args()


def reset_delay(headers, default=60.0):
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0) + 0.25
        except ValueError:
            pass

    reset_at = headers.get("X-RateLimit-Reset")
    if reset_at:
        try:
            return max(float(reset_at) - time.time(), 0.0) + 0.25
        except ValueError:
            pass
    return default


def request_bars(symbol, adjustment, api_key, api_secret, end, limiter):
    params = urllib.parse.urlencode(
        {
            "symbols": symbol,
            "timeframe": TIMEFRAME,
            "start": REQUEST_START,
            "end": end,
            "feed": FEED,
            "adjustment": adjustment,
            "asof": end[:10],
            "limit": 10000,
            "sort": "asc",
        }
    )
    request = urllib.request.Request(
        f"https://data.alpaca.markets/v2/stocks/bars?{params}",
        headers={
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": api_secret,
        },
    )

    for attempt in range(6):
        limiter.wait()
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.load(response)
                if response.headers.get("X-RateLimit-Remaining") == "0":
                    limiter.defer(reset_delay(response.headers))
                break
        except urllib.error.HTTPError as error:
            if error.code != 429 or attempt == 5:
                raise
            delay = reset_delay(error.headers)
            error.close()
            print(
                f"{adjustment}: rate limited; retrying in {delay:.2f}s",
                file=sys.stderr,
            )
            limiter.defer(delay)

    if payload.get("next_page_token"):
        raise RuntimeError("response exceeded the single-request 10,000-bar limit")

    bars_by_symbol = payload.get("bars", {})
    if symbol in bars_by_symbol:
        return bars_by_symbol[symbol]
    if len(bars_by_symbol) == 1:
        return next(iter(bars_by_symbol.values()))
    return []


def main():
    args = parse_args()
    validate_pipeline_change_log()
    symbol = args.symbol.upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]*", symbol):
        raise SystemExit(f"invalid symbol: {args.symbol!r}")

    adjustments = tuple(dict.fromkeys(args.adjustment or ADJUSTMENTS))
    end = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
    asof = end[:10]
    output_dir = Path("data") / "end_of_day" / symbol
    output_dir.mkdir(parents=True, exist_ok=True)
    limiter = RateLimiter()
    api_key = None
    api_secret = None
    integrity_failures = 0

    for adjustment in adjustments:
        filename = adjustment.replace(",", "_") + ".csv.gz"
        path = output_dir / filename
        identity = {
            "symbol": symbol,
            "adjustment": adjustment,
            "feed": FEED,
            "timeframe": TIMEFRAME,
            "request_from": REQUEST_START,
            "request_to": end,
            "asof": asof,
        }
        inspection = inspect_cached_artifact(path, identity)
        repairing = inspection.action == "repair_required"

        if inspection.action == "error":
            print(f"error: {path}: {inspection.issue}", file=sys.stderr)
            raise SystemExit(2)
        if inspection.action == "download_integrity_failed":
            print(
                f"error: {path}: Alpaca download boundary failed: "
                f"{inspection.issue}; aborting without repair or replacement",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if repairing and not args.repair:
            print(
                f"error: {path}: cache corruption: {inspection.issue}; "
                "use --repair to repair",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if args.repair and not repairing:
            if inspection.manifest is not None:
                verify_download_file(path, inspection.manifest)
            print(
                f"error: {path}: --repair applies only when exactly one download "
                "boundary file is missing",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if inspection.action == "refresh_required" and not args.force:
            print(
                f"error: {path}: {inspection.issue}; use --force to replace "
                "the Alpaca download boundary",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if inspection.action == "integrity_failed" and not args.force:
            print(
                f"error: {path}: pricing/data integrity failed: "
                f"{inspection.issue}; use --force to fetch a new Alpaca download",
                file=sys.stderr,
            )
            integrity_failures += 1
            continue

        if args.force and inspection.manifest is not None:
            verify_download_file(path, inspection.manifest)

        bypass_cache = args.force or repairing

        if not bypass_cache and inspection.action == "cached":
            existing_manifest = inspection.manifest
            if existing_manifest is None:
                raise RuntimeError("cache inspection returned no manifest")
            print(
                f"{path}: cached {existing_manifest['rows']} rows "
                f"({existing_manifest['from']} to {existing_manifest['to']}), "
                f"pipeline=v{PIPELINE_VERSION}"
            )
            continue

        if not bypass_cache and inspection.action == "repipeline":
            existing_manifest = inspection.manifest
            if existing_manifest is None:
                raise RuntimeError("re-pipeline inspection returned no manifest")
            try:
                integrity = validate_cached_download(
                    path, identity, existing_manifest
                )
            except PipelineIntegrityError as error:
                print(f"error: {error}", file=sys.stderr)
                integrity_failures += 1
                continue
            print(
                f"{path}: validated {integrity['rows_seen']} frozen rows, "
                f"pipeline=v{PIPELINE_VERSION}, "
                f"sha256={existing_manifest['sha256']}"
            )
            continue
        else:
            if repairing:
                print(f"{path}: repairing cache ({inspection.issue}); downloading")
            elif (
                inspection.issue
                and inspection.issue != "data and manifest are absent"
            ):
                print(f"{path}: cache unusable ({inspection.issue}); downloading")

            if api_key is None or api_secret is None:
                api_key = os.environ.get("ALPACA_API_KEY_ID")
                api_secret = os.environ.get("ALPACA_API_SECRET_KEY")
                if not api_key or not api_secret:
                    raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")

            try:
                bars = request_bars(
                    symbol, adjustment, api_key, api_secret, end, limiter
                )
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", "replace")
                raise SystemExit(
                    f"{adjustment}: Alpaca returned HTTP {error.code}: {detail}"
                )
            except urllib.error.URLError as error:
                raise SystemExit(f"{adjustment}: request failed: {error.reason}")

        try:
            manifest = write_bars_csv_gz(
                path, bars, identity, allow_replace=bypass_cache
            )
        except PipelineIntegrityError as error:
            print(f"error: {error}", file=sys.stderr)
            integrity_failures += 1
            continue
        print(
            f"{path}: {manifest['rows']} rows "
            f"({manifest['from']} to {manifest['to']}), "
            f"sha256={manifest['sha256']}"
        )

    if integrity_failures:
        print(
            f"error: {integrity_failures} artifact(s) failed pricing/data integrity",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)

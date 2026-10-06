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

from alpaca_end_of_day_pipeline import (
    PIPELINE_VERSION,
    AlpacaDownloadIntegrityError,
    PricingDataIntegrityError,
    authenticate_and_revalidate_cached_download,
    authenticate_frozen_download,
    inspect_cached_download,
    validate_and_publish_alpaca_download,
    validate_pipeline_change_log,
)

REQUESTS_PER_MINUTE = 180
MIN_REQUEST_INTERVAL = 60 / REQUESTS_PER_MINUTE
EARLIEST_REQUEST_TIMESTAMP = "2016-01-01T00:00:00Z"
ALPACA_DATA_FEED = "sip"
ALPACA_BAR_TIMEFRAME = "1Day"

ALPACA_ADJUSTMENT_MODES = (
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)


class AlpacaRequestRateLimiter:
    def __init__(self) -> None:
        self.next_request_monotonic = 0.0

    def wait_until_request_allowed(self) -> None:
        delay_seconds = self.next_request_monotonic - time.monotonic()
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        self.next_request_monotonic = time.monotonic() + MIN_REQUEST_INTERVAL

    def defer_requests_for_seconds(self, delay_seconds: float) -> None:
        self.next_request_monotonic = max(
            self.next_request_monotonic,
            time.monotonic() + delay_seconds,
        )


def parse_command_line_arguments() -> argparse.Namespace:
    argument_parser = argparse.ArgumentParser(description="Download daily Alpaca bars as compressed CSV.")
    argument_parser.add_argument("--symbol", required=True, help="Stock ticker, for example DELL")
    argument_parser.add_argument(
        "--adjustment",
        action="append",
        choices=ALPACA_ADJUSTMENT_MODES,
        metavar="MODE",
        help=(
            "raw, split, dividend, spin-off, split,dividend, split,spin-off, "
            "dividend,spin-off, or all; repeat for multiple modes "
            "(default: every mode)"
        ),
    )
    replacement_mode_arguments = argument_parser.add_mutually_exclusive_group()
    replacement_mode_arguments.add_argument(
        "--force",
        action="store_true",
        help="replace a coherent, authenticated Alpaca download pair",
    )
    replacement_mode_arguments.add_argument(
        "--repair",
        action="store_true",
        help="repair a download pair when exactly one file is missing",
    )
    return argument_parser.parse_args()


def rate_limit_delay_seconds(response_headers, default_seconds: float = 60.0) -> float:
    retry_after = response_headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), 0.0) + 0.25
        except ValueError:
            pass

    reset_timestamp = response_headers.get("X-RateLimit-Reset")
    if reset_timestamp:
        try:
            return max(float(reset_timestamp) - time.time(), 0.0) + 0.25
        except ValueError:
            pass
    return default_seconds


def request_alpaca_daily_bars(
    symbol: str,
    adjustment_mode: str,
    api_key_id: str,
    api_secret_key: str,
    request_end_timestamp: str,
    rate_limiter: AlpacaRequestRateLimiter,
) -> list[dict]:
    query_parameters = urllib.parse.urlencode(
        {
            "symbols": symbol,
            "timeframe": ALPACA_BAR_TIMEFRAME,
            "start": EARLIEST_REQUEST_TIMESTAMP,
            "end": request_end_timestamp,
            "feed": ALPACA_DATA_FEED,
            "adjustment": adjustment_mode,
            "asof": request_end_timestamp[:10],
            "limit": 10000,
            "sort": "asc",
        }
    )
    http_request = urllib.request.Request(
        f"https://data.alpaca.markets/v2/stocks/bars?{query_parameters}",
        headers={
            "APCA-API-KEY-ID": api_key_id,
            "APCA-API-SECRET-KEY": api_secret_key,
        },
    )

    for retry_attempt in range(6):
        rate_limiter.wait_until_request_allowed()
        try:
            with urllib.request.urlopen(http_request, timeout=60) as http_response:
                response_document = json.load(http_response)
                if http_response.headers.get("X-RateLimit-Remaining") == "0":
                    rate_limiter.defer_requests_for_seconds(rate_limit_delay_seconds(http_response.headers))
                break
        except urllib.error.HTTPError as http_error:
            if http_error.code != 429 or retry_attempt == 5:
                raise
            delay_seconds = rate_limit_delay_seconds(http_error.headers)
            http_error.close()
            print(
                f"{adjustment_mode}: rate limited; retrying in {delay_seconds:.2f}s",
                file=sys.stderr,
            )
            rate_limiter.defer_requests_for_seconds(delay_seconds)

    if response_document.get("next_page_token"):
        raise RuntimeError("response exceeded the single-request 10,000-bar limit")

    daily_bars_by_symbol = response_document.get("bars", {})
    if symbol in daily_bars_by_symbol:
        return daily_bars_by_symbol[symbol]
    if len(daily_bars_by_symbol) == 1:
        return next(iter(daily_bars_by_symbol.values()))
    return []


def main() -> None:
    cli_arguments = parse_command_line_arguments()
    validate_pipeline_change_log()
    normalized_symbol = cli_arguments.symbol.upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]*", normalized_symbol):
        raise SystemExit(f"invalid symbol: {cli_arguments.symbol!r}")

    requested_adjustment_modes = tuple(dict.fromkeys(cli_arguments.adjustment or ALPACA_ADJUSTMENT_MODES))
    request_end_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
    request_asof_date = request_end_timestamp[:10]
    symbol_download_directory = Path("data") / "end_of_day" / normalized_symbol
    symbol_download_directory.mkdir(parents=True, exist_ok=True)
    rate_limiter = AlpacaRequestRateLimiter()
    api_key_id = None
    api_secret_key = None
    failed_integrity_artifact_count = 0

    for adjustment_mode in requested_adjustment_modes:
        download_filename = adjustment_mode.replace(",", "_") + ".csv.gz"
        download_path = symbol_download_directory / download_filename
        request_identity = {
            "symbol": normalized_symbol,
            "adjustment": adjustment_mode,
            "feed": ALPACA_DATA_FEED,
            "timeframe": ALPACA_BAR_TIMEFRAME,
            "request_from": EARLIEST_REQUEST_TIMESTAMP,
            "request_to": request_end_timestamp,
            "asof": request_asof_date,
        }
        cache_inspection = inspect_cached_download(download_path, request_identity)
        download_pair_is_orphaned = cache_inspection.required_action == "repair_required"

        if cache_inspection.required_action == "error":
            print(f"error: {download_path}: {cache_inspection.diagnostic}", file=sys.stderr)
            raise SystemExit(2)
        if cache_inspection.required_action == "download_integrity_failed":
            print(
                f"error: {download_path}: Alpaca download boundary failed: "
                f"{cache_inspection.diagnostic}; aborting without repair or replacement",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if download_pair_is_orphaned and not cli_arguments.repair:
            print(
                f"error: {download_path}: cache corruption: {cache_inspection.diagnostic}; "
                "use --repair to repair",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if cli_arguments.repair and not download_pair_is_orphaned:
            if cache_inspection.download_manifest is not None:
                authenticate_frozen_download(download_path, cache_inspection.download_manifest)
            print(
                f"error: {download_path}: --repair applies only when exactly one download "
                "boundary file is missing",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if cache_inspection.required_action == "refresh_required" and not cli_arguments.force:
            print(
                f"error: {download_path}: {cache_inspection.diagnostic}; use --force to replace "
                "the Alpaca download boundary",
                file=sys.stderr,
            )
            raise SystemExit(2)
        if cache_inspection.required_action == "integrity_failed" and not cli_arguments.force:
            print(
                f"error: {download_path}: pricing/data integrity failed: "
                f"{cache_inspection.diagnostic}; use --force to fetch a new Alpaca download",
                file=sys.stderr,
            )
            failed_integrity_artifact_count += 1
            continue

        if cli_arguments.force and cache_inspection.download_manifest is not None:
            authenticate_frozen_download(download_path, cache_inspection.download_manifest)

        replacement_authorized = cli_arguments.force or download_pair_is_orphaned

        if not replacement_authorized and cache_inspection.required_action == "cached":
            cached_download_manifest = cache_inspection.download_manifest
            if cached_download_manifest is None:
                raise RuntimeError("cache inspection returned no manifest")
            print(
                f"{download_path}: cached {cached_download_manifest['rows']} rows "
                f"({cached_download_manifest['from']} to {cached_download_manifest['to']}), "
                f"pipeline=v{PIPELINE_VERSION}"
            )
            continue

        if not replacement_authorized and cache_inspection.required_action == "repipeline":
            cached_download_manifest = cache_inspection.download_manifest
            if cached_download_manifest is None:
                raise RuntimeError("re-pipeline inspection returned no manifest")
            try:
                pipeline_receipt = authenticate_and_revalidate_cached_download(
                    download_path,
                    request_identity,
                    cached_download_manifest,
                )
            except PricingDataIntegrityError as integrity_error:
                print(f"error: {integrity_error}", file=sys.stderr)
                failed_integrity_artifact_count += 1
                continue
            print(
                f"{download_path}: validated {pipeline_receipt['rows_seen']} frozen rows, "
                f"pipeline=v{PIPELINE_VERSION}, "
                f"sha256={cached_download_manifest['sha256']}"
            )
            continue
        else:
            if download_pair_is_orphaned:
                print(f"{download_path}: repairing cache ({cache_inspection.diagnostic}); downloading")
            elif cache_inspection.diagnostic and cache_inspection.diagnostic != "data and manifest are absent":
                print(f"{download_path}: cache unusable ({cache_inspection.diagnostic}); downloading")

            if api_key_id is None or api_secret_key is None:
                api_key_id = os.environ.get("ALPACA_API_KEY_ID")
                api_secret_key = os.environ.get("ALPACA_API_SECRET_KEY")
                if not api_key_id or not api_secret_key:
                    raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")

            try:
                downloaded_alpaca_bars = request_alpaca_daily_bars(
                    normalized_symbol,
                    adjustment_mode,
                    api_key_id,
                    api_secret_key,
                    request_end_timestamp,
                    rate_limiter,
                )
            except urllib.error.HTTPError as http_error:
                response_body = http_error.read().decode("utf-8", "replace")
                raise SystemExit(
                    f"{adjustment_mode}: Alpaca returned HTTP {http_error.code}: {response_body}"
                ) from http_error
            except urllib.error.URLError as request_error:
                raise SystemExit(f"{adjustment_mode}: request failed: {request_error.reason}") from request_error

        try:
            published_download_manifest = validate_and_publish_alpaca_download(
                download_path,
                downloaded_alpaca_bars,
                request_identity,
                replacement_authorized=replacement_authorized,
            )
        except PricingDataIntegrityError as integrity_error:
            print(f"error: {integrity_error}", file=sys.stderr)
            failed_integrity_artifact_count += 1
            continue
        print(
            f"{download_path}: {published_download_manifest['rows']} rows "
            f"({published_download_manifest['from']} to {published_download_manifest['to']}), "
            f"sha256={published_download_manifest['sha256']}"
        )

    if failed_integrity_artifact_count:
        print(
            f"error: {failed_integrity_artifact_count} artifact(s) failed pricing/data integrity",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except AlpacaDownloadIntegrityError as download_integrity_error:
        print(f"error: {download_integrity_error}", file=sys.stderr)
        raise SystemExit(2)
    except (OSError, RuntimeError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

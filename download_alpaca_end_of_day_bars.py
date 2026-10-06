#!/usr/bin/env python3
"""Stable executable entry point for downloading Alpaca end-of-day bars."""

import sys

from libs.alpaca_end_of_day_pipeline import AlpacaDownloadIntegrityError
from libs.download_alpaca_end_of_day_bars import (
    ALPACA_ADJUSTMENT_MODES,
    ALPACA_BAR_TIMEFRAME,
    ALPACA_DATA_FEED,
    EARLIEST_REQUEST_TIMESTAMP,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUESTS_PER_MINUTE,
    AlpacaRequestRateLimiter,
    parse_command_line_arguments,
    rate_limit_delay_seconds,
    request_alpaca_daily_bars,
    run_end_of_day_download_command,
)

# Backward-compatible spelling from the former single-file implementation.
MIN_REQUEST_INTERVAL = MIN_REQUEST_INTERVAL_SECONDS

__all__ = [
    "ALPACA_ADJUSTMENT_MODES",
    "ALPACA_BAR_TIMEFRAME",
    "ALPACA_DATA_FEED",
    "AlpacaRequestRateLimiter",
    "EARLIEST_REQUEST_TIMESTAMP",
    "MIN_REQUEST_INTERVAL",
    "REQUESTS_PER_MINUTE",
    "main",
    "parse_command_line_arguments",
    "rate_limit_delay_seconds",
    "request_alpaca_daily_bars",
]


def main() -> None:
    run_end_of_day_download_command(parse_command_line_arguments())


if __name__ == "__main__":
    try:
        main()
    except AlpacaDownloadIntegrityError as download_integrity_error:
        print(f"error: {download_integrity_error}", file=sys.stderr)
        raise SystemExit(2)
    except (OSError, RuntimeError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

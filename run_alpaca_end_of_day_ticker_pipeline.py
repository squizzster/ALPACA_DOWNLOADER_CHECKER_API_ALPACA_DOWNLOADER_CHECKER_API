#!/usr/bin/env python3
"""Stable entry point for the complete Alpaca end-of-day ticker pipeline."""

import sys

sys.dont_write_bytecode = True

from libs.alpaca_end_of_day_pipeline import AlpacaDownloadIntegrityError  # noqa: E402
from libs.alpaca_end_of_day_ticker_pipeline import (  # noqa: E402
    EndOfDayTickerPipelineResult,
    parse_end_of_day_ticker_pipeline_arguments,
    run_end_of_day_ticker_pipeline,
    run_end_of_day_ticker_pipeline_command,
    run_end_of_day_tickers_pipeline_command,
)

__all__ = [
    "EndOfDayTickerPipelineResult",
    "main",
    "parse_end_of_day_ticker_pipeline_arguments",
    "run_end_of_day_ticker_pipeline",
    "run_end_of_day_ticker_pipeline_command",
    "run_end_of_day_tickers_pipeline_command",
]


def main() -> None:
    run_end_of_day_tickers_pipeline_command(parse_end_of_day_ticker_pipeline_arguments())


if __name__ == "__main__":
    try:
        main()
    except AlpacaDownloadIntegrityError as download_integrity_error:
        print(f"error: {download_integrity_error}", file=sys.stderr)
        raise SystemExit(2)
    except (OSError, RuntimeError, ValueError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

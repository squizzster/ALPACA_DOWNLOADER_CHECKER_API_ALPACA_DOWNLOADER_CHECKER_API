#!/usr/bin/env python3
"""Stable entry point for the complete Alpaca end-of-day ticker pipeline."""

import signal
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


class GracefulShutdownRequested(BaseException):
    def __init__(self, signal_number: int) -> None:
        super().__init__(signal_number)
        self.signal_number = signal_number


def _request_graceful_shutdown(signal_number, _frame) -> None:
    raise GracefulShutdownRequested(signal_number)


def main() -> None:
    previous_sighup_handler = signal.signal(signal.SIGHUP, _request_graceful_shutdown)
    try:
        run_end_of_day_tickers_pipeline_command(parse_end_of_day_ticker_pipeline_arguments())
    finally:
        signal.signal(signal.SIGHUP, previous_sighup_handler)


if __name__ == "__main__":
    try:
        main()
    except AlpacaDownloadIntegrityError as download_integrity_error:
        print(f"error: {download_integrity_error}", file=sys.stderr)
        raise SystemExit(2)
    except GracefulShutdownRequested as shutdown_request:
        print("shutdown requested; active ticker workers drained", file=sys.stderr)
        raise SystemExit(128 + shutdown_request.signal_number)
    except KeyboardInterrupt:
        print("shutdown requested; active ticker workers drained", file=sys.stderr)
        raise SystemExit(130)
    except (OSError, RuntimeError, ValueError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

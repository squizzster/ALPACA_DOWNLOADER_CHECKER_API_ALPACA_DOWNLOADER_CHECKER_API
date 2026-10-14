#!/usr/bin/env python3.11
"""Stable entry point for the native monthly v6 pipeline."""

import signal
import sys

sys.dont_write_bytecode = True

from libs.alpaca_end_of_day_v6.command_line import (  # noqa: E402
    main as _v6_main,
)
from libs.alpaca_end_of_day_v6.command_line import (  # noqa: E402
    parse_command_line_arguments,
    run_v6_pipeline_command,
)
from libs.alpaca_end_of_day_v6.errors import V6Error  # noqa: E402

__all__ = [
    "main",
    "parse_command_line_arguments",
    "run_v6_pipeline_command",
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
        _v6_main()
    finally:
        signal.signal(signal.SIGHUP, previous_sighup_handler)


if __name__ == "__main__":
    try:
        main()
    except GracefulShutdownRequested as shutdown_request:
        print("shutdown requested; active ticker workers drained", file=sys.stderr)
        raise SystemExit(128 + shutdown_request.signal_number)
    except KeyboardInterrupt:
        print("shutdown requested; active ticker workers drained", file=sys.stderr)
        raise SystemExit(130)
    except (OSError, RuntimeError, ValueError, V6Error) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

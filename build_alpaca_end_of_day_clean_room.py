#!/usr/bin/env python3.11
"""Stable entry point for a provider-only clean-room generation."""

import signal
import sys

sys.dont_write_bytecode = True

from libs.alpaca_end_of_day_clean_room.command_line import main as _main  # noqa: E402
from libs.alpaca_end_of_day_clean_room.generation import CleanRoomError  # noqa: E402
from libs.alpaca_end_of_day_v6.errors import V6Error  # noqa: E402


class GracefulShutdownRequested(BaseException):
    def __init__(self, signal_number: int) -> None:
        super().__init__(signal_number)
        self.signal_number = signal_number


def _request_graceful_shutdown(signal_number, _frame) -> None:
    raise GracefulShutdownRequested(signal_number)


def main() -> None:
    previous_sighup_handler = signal.signal(signal.SIGHUP, _request_graceful_shutdown)
    try:
        _main()
    finally:
        signal.signal(signal.SIGHUP, previous_sighup_handler)


if __name__ == "__main__":
    try:
        main()
    except GracefulShutdownRequested as shutdown_request:
        print("shutdown requested; current boundary checkpoint retained", file=sys.stderr)
        raise SystemExit(128 + shutdown_request.signal_number)
    except KeyboardInterrupt:
        print("shutdown requested; current boundary checkpoint retained", file=sys.stderr)
        raise SystemExit(130)
    except (CleanRoomError, OSError, RuntimeError, ValueError, V6Error) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

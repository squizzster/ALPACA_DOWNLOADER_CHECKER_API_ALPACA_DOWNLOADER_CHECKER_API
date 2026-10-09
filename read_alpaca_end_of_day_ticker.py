#!/usr/bin/env python3
"""Stable read-only entry point for an authenticated master-ID security history."""

import sys

# Keep this read-only command from creating Python bytecode caches.
sys.dont_write_bytecode = True

from libs.alpaca_end_of_day_reader import (  # noqa: E402
    READER_VERSION,
    EndOfDayReaderError,
    get_alpaca_end_of_day_security_availability,
    is_alpaca_end_of_day_security_online,
    list_alpaca_end_of_day_master_ids_by_availability_status,
    list_online_alpaca_end_of_day_master_ids,
    parse_command_line_arguments,
    read_alpaca_end_of_day_security_history,
    read_alpaca_end_of_day_ticker,
    run_end_of_day_reader_command,
)

__all__ = [
    "EndOfDayReaderError",
    "READER_VERSION",
    "get_alpaca_end_of_day_security_availability",
    "is_alpaca_end_of_day_security_online",
    "list_alpaca_end_of_day_master_ids_by_availability_status",
    "list_online_alpaca_end_of_day_master_ids",
    "main",
    "parse_command_line_arguments",
    "read_alpaca_end_of_day_security_history",
    "read_alpaca_end_of_day_ticker",
    "run_end_of_day_reader_command",
]


def main() -> None:
    run_end_of_day_reader_command(parse_command_line_arguments())


if __name__ == "__main__":
    try:
        main()
    except (EndOfDayReaderError, OSError, RuntimeError, ValueError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

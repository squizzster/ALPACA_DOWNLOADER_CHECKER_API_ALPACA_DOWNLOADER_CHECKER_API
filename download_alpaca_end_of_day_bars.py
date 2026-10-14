#!/usr/bin/env python3.11
"""Stable executable entry point for the native monthly v6 pipeline."""

import sys

from libs.alpaca_end_of_day_v6.command_line import (
    main,
    parse_command_line_arguments,
    run_v6_pipeline_command,
)
from libs.alpaca_end_of_day_v6.errors import V6Error

__all__ = [
    "main",
    "parse_command_line_arguments",
    "run_v6_pipeline_command",
]


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, V6Error) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)

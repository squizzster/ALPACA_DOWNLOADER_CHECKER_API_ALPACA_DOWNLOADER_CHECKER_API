#!/usr/bin/env python3.11
"""Command-line client for the public Alpaca end-of-day data library."""

import argparse
import json
import sys
from pathlib import Path
from typing import TextIO

# Keep this read-only command from creating Python bytecode caches.
sys.dont_write_bytecode = True

from alpaca_end_of_day import (  # noqa: E402
    ADJUSTMENTS,
    DataLibrary,
    DataLibraryError,
    FlatCsvExport,
    History,
)


def parse_command_line_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    argument_parser = argparse.ArgumentParser(
        description="Read or export online end-of-day data through the public data library.",
        epilog="Dates are inclusive US market-session dates.",
    )
    output_selection = argument_parser.add_mutually_exclusive_group(required=True)
    output_selection.add_argument(
        "--public-id",
        "--master-id",
        dest="public_id",
        help="stable 64-hex public security ID",
    )
    output_selection.add_argument(
        "--dump-flat-online-csv",
        nargs="?",
        const="-",
        metavar="PATH",
        help="write every online security as date-ordered raw and all-adjusted CSV; '-' means stdout",
    )
    argument_parser.add_argument(
        "--type",
        dest="adjustment",
        choices=ADJUSTMENTS,
        metavar="TYPE",
        help="single-history adjustment view: " + ", ".join(repr(adjustment) for adjustment in ADJUSTMENTS),
    )
    argument_parser.add_argument("--start", help="first session date to return, inclusive (YYYY-MM-DD)")
    argument_parser.add_argument("--end", help="last session date to return, inclusive (YYYY-MM-DD)")
    parsed_arguments = argument_parser.parse_args(arguments)
    if parsed_arguments.public_id is not None and parsed_arguments.adjustment is None:
        argument_parser.error("--type is required with --public-id")
    if parsed_arguments.dump_flat_online_csv is not None and parsed_arguments.adjustment is not None:
        argument_parser.error("--type cannot be used with --dump-flat-online-csv; the CSV contains both views")
    return parsed_arguments


def read_history(
    public_id: str,
    adjustment: str = "raw",
    *,
    start: str | None = None,
    end: str | None = None,
    repository_root: Path | str | None = None,
) -> History:
    """Read one history through the same public library used by applications."""
    return DataLibrary(repository_root).get_history(
        public_id,
        adjustment,
        start=start,
        end=end,
    )


def run_end_of_day_reader_command(
    cli_arguments: argparse.Namespace,
    *,
    repository_root: Path | str | None = None,
    output_stream: TextIO | None = None,
) -> History | FlatCsvExport:
    if cli_arguments.dump_flat_online_csv is not None:
        data = DataLibrary(repository_root)
        destination = (
            (sys.stdout if output_stream is None else output_stream)
            if cli_arguments.dump_flat_online_csv == "-"
            else Path(cli_arguments.dump_flat_online_csv)
        )
        return data.write_flat_online_csv(
            destination,
            start=cli_arguments.start,
            end=cli_arguments.end,
        )
    history = read_history(
        cli_arguments.public_id,
        cli_arguments.adjustment,
        start=cli_arguments.start,
        end=cli_arguments.end,
        repository_root=repository_root,
    )
    command_output = sys.stdout if output_stream is None else output_stream
    json.dump(history.to_dict(), command_output, indent=2, allow_nan=False)
    command_output.write("\n")
    return history


def main() -> None:
    run_end_of_day_reader_command(parse_command_line_arguments())


if __name__ == "__main__":
    try:
        main()
    except (DataLibraryError, OSError, ValueError) as runtime_error:
        print(f"error: {runtime_error}", file=sys.stderr)
        raise SystemExit(1)


__all__ = [
    "main",
    "parse_command_line_arguments",
    "read_history",
    "run_end_of_day_reader_command",
]

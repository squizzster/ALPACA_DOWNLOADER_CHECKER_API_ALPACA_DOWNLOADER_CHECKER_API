import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, TextIO

from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

from .history import read_alpaca_end_of_day_security_history


def parse_command_line_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    argument_parser = argparse.ArgumentParser(
        description=(
            "Read authenticated Alpaca end-of-day bars for one published master ID without updating data."
        ),
        epilog=(
            "The type is an Alpaca adjustment view; 'all' means the all-corporate-actions-adjusted view. "
            "The result is one JSON document on stdout."
        ),
    )
    argument_parser.add_argument(
        "--type",
        dest="adjustment_type",
        required=True,
        choices=ALPACA_ADJUSTMENT_MODES,
        metavar="TYPE",
        help=(
            "adjustment view to read: " + ", ".join(repr(adjustment) for adjustment in ALPACA_ADJUSTMENT_MODES)
        ),
    )
    argument_parser.add_argument(
        "--master-id",
        dest="master_id",
        required=True,
        help="repository-defined SHA-256 historical-pricing master ID",
    )
    return argument_parser.parse_args(arguments)


def run_end_of_day_reader_command(
    cli_arguments: argparse.Namespace,
    *,
    repository_root: Path | str | None = None,
    output_stream: TextIO | None = None,
    reader: Callable[..., dict[str, Any]] = read_alpaca_end_of_day_security_history,
) -> dict[str, Any]:
    result = reader(
        cli_arguments.master_id,
        cli_arguments.adjustment_type,
        repository_root=repository_root,
    )
    command_output = sys.stdout if output_stream is None else output_stream
    json.dump(result, command_output, indent=2, allow_nan=False)
    command_output.write("\n")
    return result

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
    DataLibrary,
    FlatCsvExport,
    History,
)


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
    if cli_arguments.operation == "export":
        data = DataLibrary(repository_root)
        destination = (
            (sys.stdout if output_stream is None else output_stream)
            if cli_arguments.output == "-"
            else Path(cli_arguments.output)
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

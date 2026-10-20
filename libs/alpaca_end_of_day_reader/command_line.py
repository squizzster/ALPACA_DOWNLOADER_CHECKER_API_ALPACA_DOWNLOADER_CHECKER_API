import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, TextIO

from .history import read_alpaca_end_of_day_security_history


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

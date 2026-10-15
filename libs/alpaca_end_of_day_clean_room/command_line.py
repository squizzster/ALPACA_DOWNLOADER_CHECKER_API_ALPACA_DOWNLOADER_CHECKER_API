"""Command-line contract for provider-only clean-room construction."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Mapping, TextIO

from .generation import activate_clean_room_generation, run_clean_room_generation


def _exclusive_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD") from error
    return f"{parsed.isoformat()}T00:00:00Z"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build and fully audit a restartable provider-only end-of-day generation "
            "without consulting another runtime store."
        )
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("resources/ticker.csv"),
        help="one-record sorted CSV universe (default: resources/ticker.csv)",
    )
    parser.add_argument(
        "--activate",
        action="store_true",
        help="after the full audit passes, atomically select this generation as data_vtest",
    )
    parser.add_argument(
        "--store",
        type=Path,
        default=Path("data_clean_room"),
        help="new or matching resumable clean-room store (default: data_clean_room)",
    )
    parser.add_argument(
        "--request-end",
        type=_exclusive_date,
        required=True,
        metavar="YYYY-MM-DD",
        help="exclusive UTC request boundary shared by the complete universe",
    )
    return parser


def parse_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(arguments)


def run_command(
    arguments: argparse.Namespace,
    *,
    environment: Mapping[str, str] | None = None,
    output: TextIO | None = None,
) -> dict:
    completion = run_clean_room_generation(
        arguments.universe,
        arguments.store,
        request_end_timestamp=arguments.request_end,
        environment=environment,
        output=sys.stderr if output is None else output,
    )
    if not arguments.activate:
        return completion
    activation = activate_clean_room_generation(
        Path.cwd(),
        arguments.store,
        output=sys.stderr if output is None else output,
    )
    return {"completion": completion, "activation": activation}


def main(argv: list[str] | None = None) -> int:
    result = run_command(parse_arguments(argv))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


__all__ = ["build_parser", "main", "parse_arguments", "run_command"]

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Mapping, TextIO

from .pipeline import EVIDENCE_COHORT, PARITY_REQUEST_END, run_v6_pipeline


def _date_end(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD") from error
    return f"{parsed.isoformat()}T00:00:00Z"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Acquire and publish the native monthly Alpaca end-of-day v6 store."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--symbol", "-s", action="append", dest="symbols")
    selection.add_argument("--file", type=Path, dest="symbol_file")
    selection.add_argument(
        "--evidence-cohort",
        action="store_true",
        help="use the locked ten-source v6 admission cohort",
    )
    parser.add_argument(
        "--request-start",
        type=_date_end,
        metavar="YYYY-MM-01",
        help="first month to replace for already-published symbols (defaults to request-end month)",
    )
    parser.add_argument("--request-end", type=_date_end, metavar="YYYY-MM-DD")
    return parser


def parse_command_line_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(arguments)


def _symbols(arguments: argparse.Namespace) -> list[str]:
    if arguments.evidence_cohort:
        return list(EVIDENCE_COHORT)
    if arguments.symbol_file is not None:
        values = [line.strip() for line in arguments.symbol_file.read_text(encoding="utf-8").splitlines()]
        values = [value for value in values if value]
    else:
        values = arguments.symbols
    normalized = list(dict.fromkeys(value.upper() for value in values))
    invalid = [value for value in normalized if re.fullmatch(r"[A-Z][A-Z0-9.-]*", value) is None]
    if invalid:
        raise SystemExit(f"invalid symbol: {invalid[0]!r}")
    return normalized


def run_v6_pipeline_command(
    arguments: argparse.Namespace,
    *,
    repository_root: Path | str | None = None,
    environment: Mapping[str, str] | None = None,
    progress_stream: TextIO | None = None,
) -> dict:
    request_end = arguments.request_end
    if arguments.evidence_cohort and request_end is None:
        request_end = PARITY_REQUEST_END
    return run_v6_pipeline(
        _symbols(arguments),
        repository_root=repository_root,
        environment=environment,
        request_start_timestamp=arguments.request_start,
        request_end_timestamp=request_end,
        output=sys.stderr if progress_stream is None else progress_stream,
    )


def main(argv: list[str] | None = None) -> int:
    result = run_v6_pipeline_command(parse_command_line_arguments(argv))
    print(json.dumps(result, separators=(",", ":")))
    return 0


__all__ = [
    "build_parser",
    "main",
    "parse_command_line_arguments",
    "run_v6_pipeline_command",
]

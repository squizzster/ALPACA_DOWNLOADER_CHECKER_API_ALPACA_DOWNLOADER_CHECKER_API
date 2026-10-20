import argparse
import json
import os
import sys
from pathlib import Path
from typing import Callable, Mapping, TextIO

from .correlation import correlate_and_publish_alpaca_end_of_day_security_segments
from .settings import SYMBOL_PATTERN


def run_security_correlation_command(
    cli_arguments: argparse.Namespace,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    output_stream: TextIO | None = None,
    correlator: Callable[..., dict] = correlate_and_publish_alpaca_end_of_day_security_segments,
) -> dict:
    symbol = cli_arguments.ticker.upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise SystemExit(f"invalid ticker: {cli_arguments.ticker!r}")

    command_environment = os.environ if environment is None else environment
    api_key_id = command_environment.get("ALPACA_API_KEY_ID")
    api_secret_key = command_environment.get("ALPACA_API_SECRET_KEY")
    if not api_key_id or not api_secret_key:
        raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")

    receipt = correlator(
        symbol,
        api_key_id,
        api_secret_key,
        repository_root=repository_root,
    )
    command_output = output_stream or sys.stdout
    json.dump(receipt, command_output, indent=2)
    command_output.write("\n")
    return receipt

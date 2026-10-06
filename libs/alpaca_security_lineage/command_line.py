import argparse
import json
import os
import sys
from pathlib import Path
from typing import Callable, Mapping, TextIO

from .alpaca_http import AlpacaIdentityClient
from .inspection import inspect_alpaca_security_lineage
from .settings import SYMBOL_PATTERN


def parse_command_line_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    argument_parser = argparse.ArgumentParser(
        description="Inspect local downloads and direct Alpaca identity evidence for one ticker."
    )
    argument_parser.add_argument("ticker", help="US-equity ticker, for example FB")
    return argument_parser.parse_args(arguments)


def run_security_lineage_command(
    cli_arguments: argparse.Namespace,
    *,
    repository_root: Path | None = None,
    environment: Mapping[str, str] | None = None,
    output_stream: TextIO | None = None,
    client_factory: Callable[[str, str], AlpacaIdentityClient] = AlpacaIdentityClient,
) -> dict:
    symbol = cli_arguments.ticker.upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise SystemExit(f"invalid ticker: {cli_arguments.ticker!r}")

    command_environment = os.environ if environment is None else environment
    api_key_id = command_environment.get("ALPACA_API_KEY_ID")
    api_secret_key = command_environment.get("ALPACA_API_SECRET_KEY")
    if not api_key_id or not api_secret_key:
        raise SystemExit("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")

    report = inspect_alpaca_security_lineage(
        symbol,
        api_key_id,
        api_secret_key,
        repository_root=repository_root,
        client_factory=client_factory,
    )

    command_output = output_stream or sys.stdout
    json.dump(report, command_output, indent=2)
    command_output.write("\n")
    return report

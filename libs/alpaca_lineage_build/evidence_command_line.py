"""JSON evidence CLI using the same acquisition and publication workflow."""

import argparse
import json
import os
import sys
from pathlib import Path

from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient

from .evidence import output_evidence


def build_parser():
    parser = argparse.ArgumentParser(
        description="Run standalone lineage and output its decision evidence as JSON."
    )
    parser.add_argument("ticker", help="ticker label; dependencies are handled by the real lineage pipeline")
    parser.add_argument("--store", type=Path, default=Path(__file__).resolve().parents[2] / "data_pricing")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--offline",
        action="store_true",
        help="inspect published evidence only; no downloads, writes or rule replay",
    )
    mode.add_argument(
        "--refresh-metadata", action="store_true", help="use the normal pipeline's full metadata refresh"
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        identity_client = pricing_client = None
        if not args.offline:
            key = os.environ.get("ALPACA_API_KEY_ID", "")
            secret = os.environ.get("ALPACA_API_SECRET_KEY", "")
            if not key or not secret:
                raise ValueError("ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY are required (or use --offline)")
            identity_client, pricing_client = AlpacaIdentityClient(key, secret), BarsClient(key, secret)
        result = output_evidence(
            args.store,
            args.ticker,
            offline=args.offline,
            identity_client=identity_client,
            pricing_client=pricing_client,
            refresh=args.refresh_metadata,
        )
    except (RuntimeError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 1 if result["workflow"]["failed"] else 0

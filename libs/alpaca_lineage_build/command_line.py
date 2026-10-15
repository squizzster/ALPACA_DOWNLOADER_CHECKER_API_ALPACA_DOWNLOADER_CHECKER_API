"""One lineage worker, consuming local pricing; missing bars use its sibling."""

import argparse
import csv
import json
import os
import sys
from pathlib import Path

from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient

from .pipeline import run_lineage


def build_parser():
    parser = argparse.ArgumentParser(
        description="Build dated security lineage from standalone pricing; one worker."
    )
    parser.add_argument("--store", type=Path, required=True, help="existing standalone pricing store")
    choice = parser.add_mutually_exclusive_group()
    choice.add_argument(
        "-s", "--symbol", action="append", help="ticker; missing pricing uses downloader defaults; repeatable"
    )
    choice.add_argument("--file", type=Path, help="headerless CSV or one ticker per line")
    choice.add_argument(
        "--all", action="store_true", dest="all_symbols", help="all locally downloaded tickers, not the universe"
    )
    choice.add_argument("--pending", action="store_true", help="consume pending pricing notices (default)")
    parser.add_argument("--workers", type=int, choices=(1,), default=1, help="exactly one lineage worker")
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="reacquire complete identity/action evidence, including older corrections",
    )
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="report needed pricing and leave notices pending instead of acquiring it",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        symbols = args.symbol
        if args.file is not None:
            with args.file.open(encoding="utf-8-sig", newline="") as stream:
                symbols = [
                    value.strip().upper()
                    for row in csv.reader(stream, strict=True)
                    for value in row
                    if value.strip()
                ]
            if not symbols:
                raise ValueError("ticker file is empty")
        key = os.environ.get("ALPACA_API_KEY_ID", "")
        secret = os.environ.get("ALPACA_API_SECRET_KEY", "")
        if not key or not secret:
            raise ValueError("ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY are required")
        result = run_lineage(
            args.store,
            symbols,
            identity_client=AlpacaIdentityClient(key, secret),
            pricing_client=None if args.no_download else BarsClient(key, secret),
            all_symbols=args.all_symbols,
            refresh=args.refresh_metadata,
        )
    except (RuntimeError, OSError, ValueError, csv.Error) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 1 if result["failed"] else 0

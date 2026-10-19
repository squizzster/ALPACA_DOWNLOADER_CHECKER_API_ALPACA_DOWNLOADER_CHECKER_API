"""One lineage worker, consuming local pricing; missing bars use its sibling."""

import argparse
import csv
import json
import os
import sys
import time
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
        "--all",
        action="store_true",
        dest="all_symbols",
        help="update local lineage using changed-input hints; scan receipts if hints are missing",
    )
    choice.add_argument(
        "--pending", action="store_true", help="update changed inputs and outdated lineage versions (default)"
    )
    choice.add_argument(
        "--rebuild-check-summary",
        action="store_true",
        help="offline one-off rebuild of the minimal active last-session summary from stored records",
    )
    choice.add_argument(
        "--check-summary",
        action="store_true",
        help="read summary counts and digest only; no ticker scan, API requests or writes",
    )
    choice.add_argument(
        "--refresh-existing",
        action="store_true",
        help="live-check summary entries, replace changed histories over their stored spans, then run lineage",
    )
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
        if args.rebuild_check_summary or args.check_summary:
            if args.refresh_metadata or args.no_download:
                raise ValueError("summary operations cannot be combined with lineage update options")
            from libs.alpaca_pricing_download.check_summary import describe, read_snapshot

            from .check_summary import rebuild

            def progress(done, total):
                print(f"summary: {done}/{total} stored records", file=sys.stderr, flush=True)

            result = (
                rebuild(args.store, progress=progress)
                if args.rebuild_check_summary
                else describe(read_snapshot(args.store))
            )
            print(json.dumps(result, separators=(",", ":")))
            return 0
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
        if args.refresh_existing:
            if args.refresh_metadata or args.no_download:
                raise ValueError("same-span refresh cannot be combined with lineage override options")
            from .refresh import refresh_existing

            last_progress, last_stage = 0.0, None

            def progress(stage, values):
                nonlocal last_progress, last_stage
                now = time.monotonic()
                if (
                    stage != last_stage
                    or now - last_progress >= 10
                    or values.get("done", -1) == values.get("total")
                ):
                    print(stage + ": " + json.dumps(values, separators=(",", ":")), file=sys.stderr, flush=True)
                    last_progress, last_stage = now, stage

            result = refresh_existing(
                args.store,
                pricing_client=BarsClient(key, secret),
                identity_client=AlpacaIdentityClient(key, secret),
                progress=progress,
            )
        else:
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

"""CLI adapter for the standalone resolved-pricing reader."""

import argparse
import csv
import json
import sys
from pathlib import Path

from . import ADJUSTMENTS, ReaderBusyError, StandaloneReader, StandaloneReaderError


def build_parser():
    parser = argparse.ArgumentParser(
        description="Read published standalone pricing through its resolved lineage; no downloads or writes.",
        epilog="Dates are inclusive market-session dates. This is not the legacy online catalogue.",
    )
    parser.add_argument("--store", type=Path, required=True, help="standalone downloader/lineage store")
    parser.add_argument("-s", "--symbol", required=True, help="ticker locating the published lineage group")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--resolve", action="store_true", help="list matching IDs without reading prices")
    selection.add_argument("--public-id", help="explicit ID when the ticker has been reused")
    types = parser.add_mutually_exclusive_group()
    types.add_argument(
        "--type",
        dest="adjustments",
        choices=ADJUSTMENTS,
        nargs="+",
        action="extend",
        metavar="ADJUSTMENT",
        help="one or more exact adjustment names; repeatable; default raw (all means fully adjusted)",
    )
    types.add_argument("--all-types", action="store_true", help="read all eight adjustment views together")
    parser.add_argument("--from", "--start", dest="start", help="first session, inclusive (YYYY-MM-DD)")
    parser.add_argument("--to", "--end", dest="end", help="last session, inclusive (YYYY-MM-DD)")
    parser.add_argument("--format", choices=("json", "csv"), default="json", help="stdout format")
    parser.add_argument("--wait", action="store_true", help="wait for producer locks instead of reporting busy")
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    adjustments = list(dict.fromkeys(ADJUSTMENTS if args.all_types else (args.adjustments or ["raw"])))
    if args.resolve and (args.start or args.end or args.format != "json" or adjustments != ["raw"]):
        parser.error("--resolve returns all dated identities as JSON; omit history options")
    try:
        reader = StandaloneReader(args.store, wait=args.wait)
        multiple = len(adjustments) > 1
        if args.resolve:
            result = reader.resolve_ticker(args.symbol)
        else:
            read = reader.get_histories if multiple else reader.get_history
            result = read(
                args.symbol,
                adjustments if multiple else adjustments[0],
                public_id=args.public_id,
                start=args.start,
                end=args.end,
            )
        if args.format == "csv":
            columns = (
                "session",
                "public_id",
                "tickers",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "trade_count",
                "vwap",
            )
            if multiple:
                columns = ("adjustment", *columns)
            writer = csv.DictWriter(sys.stdout, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            views = result["views"] if multiple else {result["adjustment"]: result}
            for adjustment, history in views.items():
                for bar in history["bars"]:
                    row = {
                        **bar,
                        "public_id": result["security"]["public_id"],
                        "tickers": "|".join(bar["tickers"]),
                    }
                    if multiple:
                        row["adjustment"] = adjustment
                    writer.writerow(row)
        else:
            print(json.dumps(result, indent=2, allow_nan=False))
    except ReaderBusyError as error:
        print(f"busy: {error}", file=sys.stderr)
        return 3
    except (StandaloneReaderError, RuntimeError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0

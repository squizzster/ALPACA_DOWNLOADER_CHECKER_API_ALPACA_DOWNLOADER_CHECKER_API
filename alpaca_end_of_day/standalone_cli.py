"""CLI adapter for the standalone resolved-pricing reader."""

import csv
import json
import sys

from . import ADJUSTMENTS, ReaderBusyError, StandaloneReader, StandaloneReaderError


def run_arguments(args):
    adjustments = list(dict.fromkeys(ADJUSTMENTS if args.all_types else (args.adjustments or ["raw"])))
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

"""Standalone command line for the business-equity metadata baseline."""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from alpaca_end_of_day import ReaderBusyError, StandaloneReader, StandaloneReaderError

# Consumer boundary: use the public reader, never libs/ storage or policy internals.
# The evidence export's resolved IDs do not assert DataLibrary online eligibility.
from . import ROOT, VERSION, validate_version
from .baseline import BaselineError, plan_baseline, write_baseline


def build_parser():
    parser = argparse.ArgumentParser(
        description="Export existing public-ID metadata and lineage without pricing."
    )
    parser.add_argument("--version", action="version", version=str(VERSION))
    commands = parser.add_subparsers(dest="command", required=True)
    baseline = commands.add_parser("baseline", help="Export all resolved identities in indexed lineage groups.")
    baseline.add_argument("--store", type=Path, default=ROOT.parent / "data_pricing")
    baseline.add_argument(
        "--output", type=Path, help="New run directory; defaults to baselines/<UTC timestamp>."
    )
    baseline.add_argument(
        "--plan", action="store_true", help="Read only the compact source roster; write nothing."
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        validate_version()
        reader = StandaloneReader(args.store)
        last = time.monotonic()

        def progress(done, total, groups, identities):
            nonlocal last
            current = time.monotonic()
            if done == total or current - last >= 5:
                print(
                    f"baseline: sources {done}/{total}, groups {groups}, public_ids {identities}",
                    file=sys.stderr,
                    flush=True,
                )
                last = current

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        output = args.output if args.output is not None else ROOT / "baselines" / stamp
        # Keep this accepted public snapshot through the entire export. DO NOT
        # select again per group or probe prices to reconfirm its roster.
        with reader.open_published_evidence() as snapshot:
            plan = plan_baseline(reader, snapshot.roster)
            if args.plan:
                print(json.dumps(plan, indent=2))
                return 0
            print(json.dumps({"plan": plan}), file=sys.stderr, flush=True)
            result = write_baseline(reader, snapshot, output, progress=progress)
        print(
            json.dumps(
                {key: result[key] for key in ("manifest", "source_count", "group_count", "public_id_count")},
                indent=2,
            )
        )
        return 0
    except ReaderBusyError as error:
        print(f"baseline busy: {error}", file=sys.stderr)
        return 3
    except (BaselineError, StandaloneReaderError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"baseline failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("baseline interrupted", file=sys.stderr)
        return 130

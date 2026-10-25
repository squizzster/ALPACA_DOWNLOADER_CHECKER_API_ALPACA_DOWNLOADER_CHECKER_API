"""Thin executable adapter for read-only lineage policy comparisons and audits."""

import argparse
import json
import sys
from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import DownloadError

from .audit import MAX_AUDIT_SECONDS, audit
from .comparison import compare
from .contract import PolicyLabError
from .policy import load_policy

REPOSITORY = Path(__file__).resolve().parents[2]


class Parser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, allow_abbrev=False)


def _bounded_differences(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if not 0 <= parsed <= 1000:
        raise argparse.ArgumentTypeError("must be between 0 and 1000")
    return parsed


def _bounded_timeout(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if not 1 <= parsed <= MAX_AUDIT_SECONDS:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_AUDIT_SECONDS}")
    return parsed


def build_parser():
    parser = Parser(
        prog="alpaca-lineage-lab",
        description="Compare or audit a YAML policy against trusted published lineage.",
    )
    commands = parser.add_subparsers(dest="command", required=True, parser_class=Parser)
    command = commands.add_parser("compare")
    command.add_argument("--store", type=Path, default=REPOSITORY / "data_pricing")
    command.add_argument("--symbol", required=True)
    command.add_argument("--policy", required=True, type=Path)
    command.add_argument("--max-differences", type=_bounded_differences, default=20)
    command = commands.add_parser("audit")
    command.add_argument("--store", type=Path, default=REPOSITORY / "data_pricing")
    command.add_argument("--policy", required=True, type=Path)
    command.add_argument(
        "--timeout-seconds",
        type=_bounded_timeout,
        default=MAX_AUDIT_SECONDS,
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        policy = load_policy(args.policy)
        if args.command == "compare":
            report = compare(
                args.store,
                args.symbol,
                policy,
                max_differences=args.max_differences,
            )
        else:
            report = audit(
                args.store,
                policy,
                timeout_seconds=args.timeout_seconds,
            )
    except (PolicyLabError, LineageError, DownloadError, SecurityCorrelationError, OSError, ValueError) as error:
        print(f"alpaca-lineage-lab: {error}", file=sys.stderr)
        return 2
    json.dump(report, sys.stdout, sort_keys=True, indent=2)
    sys.stdout.write("\n")
    if report.get("status") == "timed_out":
        return 124
    parity = report["comparison"]["parity"] if args.command == "compare" else report["summary"]["parity"]
    return 0 if parity else 1

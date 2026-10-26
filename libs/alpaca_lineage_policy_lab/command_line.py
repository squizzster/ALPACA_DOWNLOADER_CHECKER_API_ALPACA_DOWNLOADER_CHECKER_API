"""Thin executable adapter for read-only lineage policy comparisons and audits."""

import argparse
import json
import sys
from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_heartbeat import DEFAULT_HEARTBEAT_INTERVALS, LifecycleReporter, Phase, add_heartbeat_argument
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import DownloadError

from .audit import MAX_AUDIT_SECONDS, audit
from .comparison import compare
from .contract import PolicyLabError
from .policy import load_policy

REPOSITORY = Path(__file__).resolve().parents[2]
PHASES = {
    name: Phase(message, {"estimated_remaining_seconds": (int, str)})
    for name, message in {
        "POLICY": "Loading the selected correlation policy.",
        "COMPARE": "Comparing policy decisions for the selected identity group.",
        "AUDIT": "Auditing the selected policy against completed lineage.",
    }.items()
}


class Parser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, allow_abbrev=False)
        add_heartbeat_argument(self)


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
    fixed_interval = getattr(args, "heartbeat_seconds", None)
    intervals = (fixed_interval,) if fixed_interval else DEFAULT_HEARTBEAT_INTERVALS
    reporter = LifecycleReporter(
        PHASES,
        heartbeat_intervals=intervals,
        initial_message=f"Starting policy {args.command}.",
        first_heartbeat_context={"estimated_remaining_seconds": "TBD"} if intervals[0] == 10 else None,
    )
    try:
        reporter.start()
        reporter.update("POLICY", context={"estimated_remaining_seconds": "TBD"})
        return _run(args, reporter)
    except KeyboardInterrupt:
        reporter.finish("aborted", 130)
        raise
    except SystemExit as error:
        code = error.code if type(error.code) is int else 0 if error.code is None else 1
        if error.code is not None and type(error.code) is not int:
            reporter.diagnostic(str(error.code), level="error")
        reporter.finish("success" if code == 0 else "aborted", code)
        raise
    except Exception as error:
        reporter.finish("error", 1, error=error)
        raise


def _run(args, reporter):
    try:
        policy = load_policy(args.policy)
        reporter.update(args.command.upper(), context={"estimated_remaining_seconds": "TBD"})
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
        reporter.finish("error", 2, error=error)
        return 2
    json.dump(report, sys.stdout, sort_keys=True, indent=2)
    sys.stdout.write("\n")
    if report.get("status") == "timed_out":
        reporter.finish("error", 124, error=RuntimeError("Policy audit timed out; no conclusion."))
        return 124
    parity = report["comparison"]["parity"] if args.command == "compare" else report["summary"]["parity"]
    code = 0 if parity else 1
    reporter.finish(
        "success" if parity else "error",
        code,
        error=None if parity else RuntimeError("Policy comparison found differences."),
    )
    return code

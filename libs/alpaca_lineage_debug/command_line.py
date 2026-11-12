"""Operator adapter; every diagnostic is a call through the public JSON API."""

import argparse
import json
import sys
from pathlib import Path

from alpaca_end_of_day import LineageDebugError, debug_lineage
from libs.alpaca_heartbeat import LifecycleReporter, Phase

from .api import REQUEST_SCHEMA
from .policy import DEFAULT_POLICY

REPOSITORY = Path(__file__).resolve().parents[2]


def build_parser():
    parser = argparse.ArgumentParser(
        prog="alpaca-lineage-debug",
        description="Explain recorded lineage or explicitly research one YAML policy; production remains read-only.",
        allow_abbrev=False,
    )
    commands = parser.add_subparsers(dest="operation", required=True)
    for name, help_text in (
        ("init", "Create an editable research YAML policy."),
        ("rules", "List ordered rules, phases and current code locations."),
        ("explain", "Explain one complete group's recorded decisions without reading prices."),
        ("run", "Explicitly replay a policy for selected tickers or every indexed group."),
        ("screen", "Plan the membership experiment using only completed metadata."),
        ("export", "Report dated ticker instances in a completed CSV export."),
        ("handoff", "Inspect a saved stage document without running the pipeline."),
    ):
        sub = commands.add_parser(name, help=help_text, description=help_text, allow_abbrev=False)
        sub.add_argument("--format", choices=("text", "json"), default="text")
        if name == "init":
            sub.add_argument("--output", type=Path, help="New YAML file; existing files are never overwritten.")
        if name in ("rules", "run", "screen"):
            sub.add_argument("--policy", default=str(DEFAULT_POLICY))
        if name in ("run", "explain", "screen"):
            sub.add_argument("--store", default=str(REPOSITORY / "data_pricing"))
        if name == "run":
            sub.add_argument(
                "--screen-report", help="Consume a metadata screen without rereading its index or receipts."
            )
            sub.add_argument("--reuse-report", help="Carry a completed paired result under matching bindings.")
            sub.add_argument(
                "--control-policy", help="Evaluate unchanged control and candidate on the same held inputs."
            )
            scope = sub.add_mutually_exclusive_group(required=True)
            scope.add_argument("--tickers", "--symbols", dest="symbols", nargs="+")
            scope.add_argument("--all", action="store_true")
            sub.add_argument("--max-differences", type=int, default=20)
            sub.add_argument(
                "--timeout-seconds", type=float, help="Optional deadline checked between complete groups."
            )
        if name == "screen":
            sub.add_argument("--control-policy", required=True)
        if name == "explain":
            sub.add_argument("--ticker", "--symbol", dest="symbol", required=True)
            sub.add_argument("--session")
            sub.add_argument("--adjustment", default="raw")
            sub.add_argument("--max-segments", type=int, default=100)
        if name == "export":
            sub.add_argument("--manifest", required=True)
            sub.add_argument("--tickers", dest="symbols", nargs="+", default=[])
            sub.add_argument("--include-single", action="store_true")
        if name == "handoff":
            sub.add_argument("--input", required=True)
            sub.add_argument("--tickers", dest="symbols", nargs="+", default=[])
    return parser


def render(report):
    operation = report["operation"]
    lines = [f"Lineage {operation}: {report['status']} (production read-only)"]
    if operation == "rules":
        lines.append(f"Policy: {report['policy']['source']} [{report['policy']['policy_role']}]")
        for category in ("ownership_rules", "row_rules"):
            lines.append(category.replace("_", " ") + ":")
            for rule in report[category]:
                code = rule["implementation"]
                lines.append(f"  {rule['order']:2}. {rule['name']}  {code['file']}:{code['line']}")
        lines.append(f"Terminal: {report['terminal_rule']}")
        code = report["numerical_source_selection"]["implementation"]
        lines.append(f"Numerical choice: {code['file']}:{code['line']} (fixed Python postprocessor)")
        for step in report["fixed_steps"]:
            code = step["implementation"]
            lines.append(f"Fixed step: {step['name']}  {code['file']}:{code['line']}")
        lines.append("Research YAML can reorder/remove registered established ownership and row rules.")
        lines.append("New predicates and the fixed steps require an implementation change; see --format json.")
    elif operation == "run":
        lines.append(json.dumps(report["summary"], sort_keys=True))
        for group in report["groups"]:
            comparison = group["comparison"]
            changed = {name: item["count"] for name, item in comparison["differences"].items() if item["count"]}
            lines.append(
                f"  {', '.join(group['group'])}: {'parity' if comparison['parity'] else 'differences'} {changed}"
            )
            if "published_to_control" in group:
                lines.append(
                    f"    stored/control drift: {not group['published_to_control']['parity']}; candidate-only decisions changed: {not comparison['decision_parity']}"
                )
        for item in report["failures"]:
            lines.append(f"  {', '.join(item['group'])}: ERROR {item['error']}")
    elif operation == "explain":
        lines.append("Complete group: " + ", ".join(report["group"]))
        for name, source in report["sources"].items():
            lines.append(f"{name}: {source['status']}")
            if source["status"] != "published":
                continue
            lines.append(f"  Rules: {source['rule_counts']}; omitted segments: {source['omitted_segments']}")
            lines.append(f"  Numerical-choice handoffs: {source['observation_choice_capabilities']}")
            for segment in source["segments"]:
                identity = segment.get("security_identity", {}).get("security_id", "unresolved")
                lines.append(
                    f"  {segment['from']}..{segment['to']} owner={identity} "
                    f"rule={segment.get('correlation_rule', {}).get('name', 'unrecorded')} "
                    f"observation={segment.get('observation_selection', 'unrecorded')}"
                )
                if segment.get("reason_codes") or segment.get("warnings"):
                    lines.append(
                        f"    reasons={segment.get('reason_codes', [])} warnings={segment.get('warnings', [])}"
                    )
        lines.append(f"Overlapping selected bounds: {len(report['selected_segment_overlaps'])}")
        lines.append(
            "Use --format json for recorded evidence, generation bindings and complete first-match paths."
        )
    elif operation == "export":
        lines.append(json.dumps(report["summary"], sort_keys=True))
        for ticker in report["tickers"]:
            lines.append(
                f"{ticker['ticker']}: {ticker['instance_count']} identities; {ticker['classification']}"
            )
            for item in ticker["instances"]:
                lines.append(f"  {item['public_id']} {item['ranges']} {item['csv']}")
    elif operation == "screen":
        lines.append(json.dumps(report["summary"], sort_keys=True))
        lines.append(f"Expected evaluation I/O: {report['expected_io']}")
        for group in report["candidate_groups"]:
            lines.append("  Possible candidate effect: " + ", ".join(group["group"]))
        lines.append("Use --format json to carry the accepted index and selected receipt values into run.")
    else:
        lines.append(json.dumps(report, sort_keys=True, indent=2))
    for limit in report.get("limits", []):
        lines.append(limit)
    return "\n".join(lines) + "\n"


def main(argv=None):
    args = vars(build_parser().parse_args(argv))
    output_format = args.pop("format")
    output = args.pop("output", None)
    if args["operation"] == "export":
        args["multiple_only"] = not args.pop("include_single")
    args = {key: value for key, value in args.items() if value is not None}
    reporter = LifecycleReporter(
        {"DIAGNOSE": Phase("Reading selected diagnostic inputs.", {})},
        initial_message=f"Starting lineage {args['operation']} (production read-only).",
    )
    try:
        reporter.start()
        reporter.update("DIAGNOSE")
        report = json.loads(
            debug_lineage(json.dumps({"schema": REQUEST_SCHEMA, "schema_version": 1, **args}).encode())
        )
        if args["operation"] == "init":
            if output is not None:
                with output.open("x", encoding="utf-8") as stream:
                    stream.write(report["yaml"])
                text = str(output) + "\n"
            else:
                text = report["yaml"] if output_format == "text" else json.dumps(report, indent=2) + "\n"
        else:
            text = (
                json.dumps(report, sort_keys=True, indent=2) + "\n"
                if output_format == "json"
                else render(report)
            )
        sys.stdout.write(text)
        sys.stdout.flush()
        code = (
            124
            if report["status"] == "timed_out"
            else 2
            if not report["execution_complete"]
            else 1
            if args["operation"] == "run" and not report["summary"]["parity"]
            else 0
        )
        reporter.finish("success" if code == 0 else "error", code)
        return code
    except (LineageDebugError, OSError, ValueError) as error:
        reporter.finish("error", 2, error=error)
        return 2
    except KeyboardInterrupt:
        reporter.finish("aborted", 130)
        return 130

"""One explicit CLI; domain operations remain in their owning packages."""

import argparse
import csv
import json
import os
from contextlib import nullcontext
from datetime import timedelta
from pathlib import Path

from alpaca_end_of_day import ADJUSTMENTS, ReaderBusyError
from libs.alpaca_heartbeat import (
    LifecycleReporter,
    diagnostic,
)
from libs.alpaca_pricing_download.download import parse_date, pricing_session_end
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_recovery_registry import DEFAULT_REGISTRY, load_labels
from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient
from libs.common.store_lock import StoreBusyError, operation_lock

from .registry_commit import commit_registry

VERSION = 24
REPOSITORY = Path(__file__).resolve().parents[2]
# CLI defaults live here; acquisition consumes the parsed store and worker count.
DEFAULT_STORE = REPOSITORY / "data_pricing"


class Parser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, allow_abbrev=False)


def _store(parser, default=DEFAULT_STORE, *, required=False):
    parser.add_argument("--store", type=Path, default=default, required=required)


def bounded_integer(lower, upper):
    def parse(value):
        try:
            parsed = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError("must be an integer") from error
        if not lower <= parsed <= upper:
            raise argparse.ArgumentTypeError(f"must be between {lower} and {upper}")
        return parsed

    return parse


def _dates(parser, *, start=True, pricing=False):
    if start:
        parser.add_argument("--from", dest="start", help="inclusive first session, YYYY-MM-DD")
    parser.add_argument(
        "--through",
        dest="end",
        help="inclusive cutoff date, rounded back to a completed trading session, YYYY-MM-DD"
        if pricing
        else "inclusive last date, YYYY-MM-DD",
    )


def _selection(parser, *, required=False, recovery=False):
    group = parser.add_mutually_exclusive_group(required=required)
    group.add_argument("--symbol", action="append", help="exact ticker label; repeatable")
    group.add_argument("--symbols-file", type=Path, help="headerless CSV or one label per line")
    if recovery:
        group.add_argument("--recovery-universe", action="store_true", help="all additive recovery labels")


def _workers(parser):
    parser.add_argument("--workers", type=int, choices=(1, 2, 3), default=3)


def build_parser():
    parser = Parser(
        prog="alpaca-cli-eod", description="Maintain and read Alpaca EOD pricing and dated security identities."
    )
    commands = parser.add_subparsers(dest="command", required=True, parser_class=Parser)
    update = commands.add_parser(
        "update", help="discover, register, reconcile and update the standalone universe"
    )
    _store(update)
    _dates(update, start=False, pricing=True)
    _workers(update)
    update.add_argument("--plan", action="store_true", help="read-only preview; may fetch current metadata")
    update.add_argument(
        "--retry-blocked", action="store_true", help="retry explicit rejections and access denials"
    )
    status = commands.add_parser("status", help="inspect compact local coverage and pending work")
    _store(status)
    for name in ("resolve", "read"):
        p = commands.add_parser(
            name,
            help="read published standalone identities"
            if name == "resolve"
            else "read resolved standalone pricing",
        )
        _store(p)
        p.add_argument("--symbol", required=True)
        p.add_argument("--wait", action="store_true")
        p.set_defaults(
            resolve=name == "resolve",
            public_id=None,
            adjustments=None,
            all_types=False,
            start=None,
            end=None,
            format="json",
        )
        if name == "read":
            p.add_argument("--public-id")
            g = p.add_mutually_exclusive_group()
            g.add_argument("--adjustment", dest="adjustments", choices=ADJUSTMENTS, nargs="+", action="extend")
            g.add_argument("--all-adjustments", dest="all_types", action="store_true")
            _dates(p)
            p.add_argument("--format", choices=("json", "csv"), default="json")
    export = commands.add_parser("export").add_subparsers(dest="operation", required=True, parser_class=Parser)
    p = export.add_parser("csv", help="export online public identities with distinct adjustment columns")
    _store(p)
    _dates(p)
    p.add_argument("--output", type=Path, default=REPOSITORY / "exports" / "csv")
    p.add_argument("--public-id", action="append", help="online public identity; repeatable; default all")
    p.add_argument(
        "--workers", type=int, choices=(1, 2, 3, 4), default=2, help="concurrent complete groups; default 2"
    )
    p.add_argument(
        "--plan", action="store_true", help="offline catalogue scope preview; no payload reads or writes"
    )
    p = export.add_parser("bloodhound", help="offline eight-view Parquet conversion")
    _store(p)
    _dates(p)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--symbol", action="append")
    p.add_argument("--public-id", action="append")
    p.add_argument("--wait", action="store_true")
    p.add_argument("--memory-mib", type=bounded_integer(256, 8192), default=8192)
    p.add_argument("--cache-mib", type=bounded_integer(0, 6144), default=2048)
    p.add_argument("--include-all-passed", action="store_true")
    p.add_argument("--replace", action="store_true")
    pricing = commands.add_parser("pricing").add_subparsers(dest="operation", required=True, parser_class=Parser)
    for name in ("acquire", "check", "refresh"):
        p = pricing.add_parser(name)
        _store(p, required=name == "acquire")
        _selection(p, required=name == "acquire", recovery=name == "acquire")
        if name == "acquire":
            _dates(p, pricing=True)
            _workers(p)
        if name == "check":
            p.add_argument("--batch-size", type=int, default=4000)
    lineage = commands.add_parser("lineage").add_subparsers(dest="operation", required=True, parser_class=Parser)
    for name in ("update", "refresh", "inspect", "migrate-index", "migrate-policy"):
        p = lineage.add_parser(name)
        _store(p)
        if name in {"update", "refresh"}:
            _selection(p)
            _dates(p, start=False, pricing=True)
            p.add_argument("--no-pricing-acquisition", action="store_true")
        elif name == "inspect":
            p.add_argument("--symbol", required=True)
        elif name == "migrate-index":
            p.add_argument("--batch-size", type=int, default=128)
    universe = commands.add_parser("universe").add_subparsers(
        dest="operation", required=True, parser_class=Parser
    )
    for name in ("reconcile", "rebuild"):
        p = universe.add_parser(name)
        _store(p)
        if name == "reconcile":
            _dates(p, start=False, pricing=True)
    summary = commands.add_parser("summary").add_subparsers(dest="operation", required=True, parser_class=Parser)
    _store(summary.add_parser("rebuild"))
    catalogue = commands.add_parser(
        "catalogue", help="published serving catalogue over the standalone store"
    ).add_subparsers(dest="operation", required=True, parser_class=Parser)
    for name in ("bootstrap", "rebuild", "update", "status", "list", "search", "resolve", "read", "export"):
        p = catalogue.add_parser(name)
        _store(p)
        if name in {"bootstrap", "rebuild", "update"}:
            p.add_argument("--plan", action="store_true", help="offline scope preview; no writes or recovery")
        if name == "search":
            p.add_argument("query")
            p.add_argument("--limit", type=bounded_integer(1, 100000))
        if name == "resolve":
            p.add_argument("--symbol", required=True)
            p.add_argument("--on")
        if name in {"read", "export"}:
            _dates(p)
            p.add_argument("--wait", action="store_true")
            if name == "read":
                p.add_argument("--public-id", required=True)
                p.add_argument("--adjustment", choices=ADJUSTMENTS, default="raw")
            else:
                p.add_argument("--output", required=True, help="CSV destination or '-' for stdout")
    return parser


def symbols(args):
    from libs.alpaca_pricing_download.contract import symbol_label

    if getattr(args, "recovery_universe", False):
        return list(load_labels())
    if getattr(args, "symbols_file", None):
        with args.symbols_file.open(encoding="utf-8-sig", newline="") as stream:
            values = [v for row in csv.reader(stream, strict=True) for v in row if v.strip()]
    else:
        values = getattr(args, "symbol", None)
    if values is None:
        return None
    if not values:
        raise ValueError("symbol selection is empty")
    return list(dict.fromkeys(symbol_label(value) for value in values))


def end_exclusive(value):
    return pricing_session_end(value).date().isoformat()


def _pricing_end(args):
    end = end_exclusive(args.end)
    args.pricing_cutoff = {
        "requested_through": args.end,
        "pricing_through_session": (parse_date(end) - timedelta(days=1)).isoformat(),
        "end_exclusive": end,
    }
    diagnostic(
        getattr(args, "_lifecycle", None),
        "pricing cutoff: " + json.dumps(args.pricing_cutoff, separators=(",", ":")),
    )
    return end


def clients():
    key, secret = os.environ.get("ALPACA_API_KEY_ID", ""), os.environ.get("ALPACA_API_SECRET_KEY", "")
    if not key or not secret:
        raise ValueError("ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY are required")
    return BarsClient(key, secret), AlpacaIdentityClient(key, secret)


def _store_operation_mode(args):
    """Classify the complete command before any store or provider work begins."""
    # Keep the outer store gate ahead of owner locks and provider side effects.
    # Read-only selection/status must not acquire write authority by silently
    # repairing missing controls or bootstrapping a store.
    command, operation = args.command, getattr(args, "operation", None)
    if command == "export" and operation == "csv" and not args.plan:
        return "shared"
    writes = (
        (command == "update" and not args.plan)
        or (command == "pricing" and operation in {"acquire", "refresh"})
        or (command == "lineage" and operation in {"update", "refresh", "migrate-index", "migrate-policy"})
        or (command == "universe" and operation in {"reconcile", "rebuild"})
        or (command == "summary" and operation == "rebuild")
        or (command == "catalogue" and operation in {"bootstrap", "rebuild", "update"} and not args.plan)
    )
    if not writes:
        return None
    return "exclusive"


def _store_operation(args):
    """Take the one outer store gate for commands that export or write."""
    mode = _store_operation_mode(args)
    if mode is None:
        return nullcontext()
    return operation_lock(args.store, exclusive=mode == "exclusive")


def dispatch(args):
    # CLI dispatch delegates to domain owners. DO NOT add receipt/payload audits
    # or independent work selection here; accepted handoffs belong to those owners.
    from .command_progress import count_callback, progress_callback

    reporter = getattr(args, "_lifecycle", None)
    progress = progress_callback(reporter)
    command, operation = args.command, getattr(args, "operation", None)
    if command in {"read", "resolve"}:
        from alpaca_end_of_day.standalone_cli import run_arguments

        return run_arguments(args)
    if command == "export":
        if operation == "csv":
            from alpaca_end_of_day import DataLibrary

            from .csv_progress import hold_plan, progress_callback

            reporter = getattr(args, "_lifecycle", None)
            if reporter is not None:
                reporter.update("CATALOGUE")
            data = DataLibrary(store=args.store)
            options = {
                "public_ids": args.public_id,
                "start": args.start,
                "end": args.end,
                "workers": args.workers,
            }
            if reporter is not None:
                reporter.update("PLAN")
            if args.plan:
                result = data.plan_csv_export(**options)
                hold_plan(reporter, result)
                return result
            return data.write_csv_export(args.output, **options, progress=progress_callback(reporter))
        from libs.alpaca_bloodhound_export.command_line import run_arguments

        return run_arguments(args)
    if command == "status":
        from libs.alpaca_lineage_build.status import inspect_status

        return inspect_status(args.store)
    if command == "summary":
        from libs.alpaca_lineage_build.check_summary import rebuild

        return rebuild(
            args.store,
            progress=count_callback(reporter, "summary", "symbols"),
        )
    if command == "universe":
        from libs.alpaca_lineage_build.universe_update import rebuild_universe, reconcile_universe

        if operation == "rebuild":
            return rebuild_universe(args.store)
        end = _pricing_end(args)
        _, identity = clients()
        return reconcile_universe(args.store, end, identity_client=identity)
    if command == "lineage":
        if operation == "migrate-policy":
            from libs.alpaca_lineage_build.migration import migrate_correlation_policy

            return migrate_correlation_policy(
                args.store,
                progress=count_callback(reporter, "policy_migration", "groups"),
            )
        if operation == "migrate-index":
            from libs.alpaca_lineage_build.projection import migrate_index

            return migrate_index(
                args.store,
                batch_size=args.batch_size,
                progress=count_callback(reporter, "index_migration", "receipts"),
            )
        if operation == "inspect":
            from libs.alpaca_lineage_build.evidence import output_evidence

            return output_evidence(args.store, args.symbol, offline=True)
        end = _pricing_end(args)
        pricing, identity = clients()
        from libs.alpaca_lineage_build.pipeline import run_lineage
        from libs.alpaca_lineage_build.registration import standalone_registration

        with standalone_registration(args.store, end, registry=DEFAULT_REGISTRY) as (register, policy):
            result = run_lineage(
                args.store,
                symbols(args),
                identity_client=identity,
                pricing_client=None if args.no_pricing_acquisition else pricing,
                refresh=operation == "refresh",
                end_date=end,
                register_discoveries=register,
                work_policy=policy,
                progress=progress,
                on_pricing=progress,
            )
        selected = {
            symbol
            for item in result["results"]
            for symbol in [item["symbol"], *item.get("processed", []), *item.get("affected", [])]
        }
        pending = sorted(s for s in policy.pending_work if policy.graph.closure({s}) & selected)
        complete = not result["failed"] and not pending
        from libs.alpaca_serving_catalogue.producer import maintain
        from libs.alpaca_serving_catalogue.storage import path as catalogue_path

        if catalogue_path(Path(args.store)).exists():
            serving = maintain(args.store, symbols=selected, progress=progress)
            return {
                **result,
                "serving_catalogue": serving,
                "pending_work": pending,
                "failed": result["failed"] + serving["failed"],
                "complete": complete and serving["complete"],
            }
        return {**result, "pending_work": pending, "complete": complete}
    if command == "update":
        end = _pricing_end(args)
        pricing, identity = clients()
        from libs.alpaca_lineage_build.universe_update import plan_update, run_update

        if args.plan:
            return plan_update(args.store, end, identity_client=identity, retry_blocked=args.retry_blocked)
        return run_update(
            args.store,
            end,
            pricing_client=pricing,
            identity_client=identity,
            workers=args.workers,
            retry_blocked=args.retry_blocked,
            progress=progress,
        )
    if command == "pricing":
        if operation == "acquire":
            from libs.alpaca_lineage_build.registration import standalone_registration
            from libs.alpaca_pricing_download.command_line import run_arguments

            args.request_end = _pricing_end(args)
            args.from_date, args.refresh_all = args.start, False
            args.all_symbols, args.file = args.recovery_universe, args.symbols_file
            return run_arguments(
                args,
                progress=progress,
                registration_context=lambda: standalone_registration(
                    args.store, args.request_end, registry=DEFAULT_REGISTRY
                ),
            )
        pricing, identity = clients()
        selected = symbols(args)
        if operation == "check":
            from libs.alpaca_pricing_download.check_summary import assert_current, read_snapshot
            from libs.alpaca_pricing_download.live_check import verify

            snapshot = read_snapshot(args.store)
            if selected is not None:
                entries = {e["symbol"]: e for e in snapshot["entries"]}
                if set(selected) - entries.keys():
                    raise ValueError("selected symbols are absent from the eligible summary")
                snapshot = {**snapshot, "entries": [entries[s] for s in selected]}
            from libs.alpaca_lineage_build.registration import command_policy

            policy = command_policy(args.store, end_exclusive(None))
            policy.check(e["symbol"] for e in snapshot["entries"])
            result = verify(
                snapshot,
                pricing._get,
                batch_size=args.batch_size,
                progress=count_callback(reporter, "pricing_check", "symbols"),
            )
            assert_current(args.store, snapshot["token"])
            return result
        from libs.alpaca_lineage_build.registration import standalone_registration

        with standalone_registration(args.store, end_exclusive(None), registry=DEFAULT_REGISTRY) as (
            register,
            policy,
        ):
            if selected is None:
                from libs.alpaca_lineage_build.refresh import refresh_existing

                result = refresh_existing(
                    args.store,
                    pricing_client=pricing,
                    identity_client=identity,
                    progress=progress,
                    register_discoveries=register,
                    work_policy=policy,
                )
                if result["deferred_symbols"]:
                    result["complete"] = False
                return result
            from libs.alpaca_pricing_download.refresh import prepare_refresh, replace_prepared

            policy.check(selected)
            prepared = prepare_refresh(args.store, selected)
            policy.require_correction(prepared, require_check=False)
            register(selected)
            result = replace_prepared(args.store, prepared, client=pricing, progress=progress)
            policy.record_results(result, stage="history_correction")
            return result
    if command == "catalogue":
        return catalogue_command(args)
    raise AssertionError("unhandled command")


def catalogue_command(args):
    from .command_progress import progress_callback

    if args.operation in {"read", "export"}:
        from alpaca_end_of_day.catalogue_cli import run_end_of_day_reader_command

        run_end_of_day_reader_command(args, store=args.store)
        return 0
    if args.operation in {"bootstrap", "rebuild", "update", "status"}:
        from libs.alpaca_serving_catalogue.producer import maintain, preview, status

        if args.operation == "status":
            result = status(args.store)
            return {**result, "complete": result["coverage_complete"] and not result["pending_symbols"]}
        options = {"bootstrap": args.operation == "bootstrap", "rebuild": args.operation == "rebuild"}
        if args.plan:
            return preview(args.store, **options)
        return maintain(args.store, **options, progress=progress_callback(getattr(args, "_lifecycle", None)))
    from alpaca_end_of_day import DataLibrary

    data = DataLibrary(store=args.store)
    if args.operation == "search":
        values = data.search_securities(args.query, limit=args.limit)
    elif args.operation == "resolve":
        values = data.resolve_ticker(args.symbol, on=args.on)
    else:
        values = data.list_securities()
    return {"generation": data.catalog_generation(), "matches": [value.to_dict() for value in values]}


def main(argv=None):
    args = build_parser().parse_args(argv)
    from .command_progress import PHASES, hold_operation
    from .csv_progress import INITIAL_MESSAGE
    from .csv_progress import PHASES as CSV_PHASES

    csv_export = args.command == "export" and args.operation == "csv"
    command_name = " ".join(filter(None, (args.command, getattr(args, "operation", None))))
    reporter = LifecycleReporter(
        CSV_PHASES if csv_export else PHASES,
        initial_message=INITIAL_MESSAGE if csv_export else f"Starting {command_name}.",
        first_heartbeat_context={"estimated_remaining_seconds": "TBD"},
    )
    args._lifecycle = reporter
    try:
        reporter.start()
        if not csv_export:
            hold_operation(reporter, args)
        changelog = json.loads((REPOSITORY / "resources/alpaca_cli_change_log.json").read_bytes())
        if len(changelog) != VERSION or any(
            "version" in item or not item.get("modifications") for item in changelog
        ):
            raise ValueError("CLI changelog/version mismatch")
        with _store_operation(args):
            result = dispatch(args)
        if isinstance(result, int):
            code = result
        else:
            if hasattr(args, "pricing_cutoff"):
                result = {**result, "pricing_cutoff": args.pricing_cutoff}
            print(json.dumps(result, separators=(",", ":"), allow_nan=False))
            code = (
                int(not result["processing_complete"])
                if args.command == "status"
                else int(bool(result.get("failed") or result.get("blockers") or result.get("complete") is False))
            )
        if args.command == "update" and not args.plan and code == 0:
            commit_registry(REPOSITORY, DEFAULT_REGISTRY, reporter)
        reporter.finish(
            "success" if code == 0 else "error",
            code,
            error=RuntimeError(
                "CSV export did not complete" if csv_export else f"{command_name} did not complete"
            )
            if code
            else None,
        )
        return code
    except (ReaderBusyError, StoreBusyError) as error:
        reporter.finish("error", 3, error=error)
        return 3
    except (RuntimeError, OSError, ValueError, csv.Error) as error:
        reporter.finish("error", 1, error=error)
        return 1
    except KeyboardInterrupt:
        reporter.finish("aborted", 130)
        if not csv_export:
            raise
        return 130
    except SystemExit as error:
        code = error.code if type(error.code) is int else 0 if error.code is None else 1
        if error.code is not None and type(error.code) is not int:
            reporter.diagnostic(str(error.code), level="error")
        reporter.finish("success" if code == 0 else "aborted", code)
        raise
    except Exception as error:
        reporter.finish("error", 1, error=error)
        if not csv_export:
            raise
        return 1

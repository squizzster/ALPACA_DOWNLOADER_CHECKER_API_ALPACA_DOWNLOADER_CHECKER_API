"""Translate existing command observations into the shared stderr protocol."""

import json
import threading

from libs.alpaca_heartbeat import Phase

# Progress is a projection of producer callbacks, not a new observation pass.
# DO NOT read stores or repeat work to improve counters/ETA. A sampled component's
# "completed" state is not authoritative completion of the whole command.

FIELDS = {
    name: int
    for name in (
        "selected",
        "processed",
        "completed",
        "pending_work",
        "requests",
        "groups",
        "symbols",
        "tickers",
        "checked",
        "failed",
        "records",
        "work",
        "registry_additions",
        "registered",
        "actions",
        "minimum_pages",
        "page_requests",
        "http_requests",
        "metadata_requests",
        "metadata_http_requests",
        "pricing_http_requests",
        "workers",
        "wave",
        "required",
        "cohort",
        "reused_decisions",
        "rows",
        "identities",
        "computation_revision",
        "published_symbols",
        "selected_symbols",
        "selected_groups",
        "already_current_symbols",
        "assignment_projection_complete_symbols",
    )
}
FIELDS.update(
    {name: str for name in ("stage", "state", "symbol", "status", "unit", "end_exclusive", "path", "commit")}
)
FIELDS.update(seconds=(int, float), complete=bool, estimated_remaining_seconds=(int, str))
FIELDS.update(
    estimated_remaining_lower_seconds=int,
    estimated_remaining_upper_seconds=int,
    eta_pricing_pages_completed=int,
    eta_scope=str,
    eta_status=str,
    estimated_phase_remaining_seconds=int,
    eta_phase=str,
)
EXPECTED_FIELDS = (
    "boundary_reads",
    "selected_pricing_manifests",
    "late_bar_probe_manifest_reads",
    "late_bar_probe_sources",
    "late_bar_probe_sessions",
    "late_bar_probe_actions_upper_bound",
    "append_approval_manifest_reads",
    "selected_history_obligation_manifest_reads",
    "selected_lineage_labels",
    "existing_lineage_receipts_selected",
    "catalogue_actions",
    "selection_metadata_http_requests_observed",
    "history_check_actions",
    "initial_or_invalidated_actions_estimate",
    "empty_coverage_actions_estimate",
    "acquisition_actions_estimate",
    "append_actions_before_reconciliation",
    "http_pages_minimum",
    "http_pages_dense_history_estimate",
    "pricing_publications_estimate",
    "serving_pending_symbols",
)
FIELDS.update({"expected_" + name: int for name in EXPECTED_FIELDS})
PHASES = {
    name: Phase(message, FIELDS)
    for name, message in {
        "WAIT": "Waiting for the store lock; press Ctrl-C to cancel.",
        "UPDATE": "Updating pricing, lineage and the serving catalogue.",
        "RECOVERY": "Recovering interrupted publications.",
        "CATALOGUE": "Reading or maintaining the serving catalogue.",
        "PLAN": "Selecting and planning work.",
        "PRICING": "Acquiring, checking or refreshing pricing.",
        "LINEAGE": "Processing selected lineage groups.",
        "MIGRATE": "Migrating the selected lineage boundary.",
        "SUMMARY": "Rebuilding the pricing check summary.",
        "UNIVERSE": "Maintaining universe registration and work state.",
        "STATUS": "Inspecting published status boundaries.",
        "READ": "Reading selected published histories.",
        "RESOLVE": "Resolving dated security identities.",
        "EXPORT": "Converting and publishing selected histories.",
        "COMMIT": "Committing the completed update's recovery registry changes.",
        "PUSH": "Pushing the recovery registry commit to its upstream branch.",
    }.items()
}


def operation_phase(args):
    command, operation = args.command, getattr(args, "operation", None)
    if getattr(args, "plan", False):
        return "PLAN"
    if command == "lineage" and operation.startswith("migrate-"):
        return "MIGRATE"
    return command.upper()


def hold_operation(reporter, args):
    if reporter is not None:
        reporter.update(
            operation_phase(args),
            scope=" ".join(filter(None, (args.command, getattr(args, "operation", None)))),
            context={"estimated_remaining_seconds": "TBD"},
        )


def progress_callback(reporter, *, estimate=None):
    if reporter is None:
        return None

    expected_context = {}
    latest = None
    lock = threading.RLock()

    def observe(stage, values):
        nonlocal latest
        try:
            if stage == "eta_request":
                if estimate is None:
                    return False
                estimate.request(values)
                if latest is None:
                    return False
                phase, message, context, counts = latest
                return reporter.update(
                    phase, message=message, context={**context, **estimate.context()}, **counts
                )
            if estimate is not None:
                estimate.observe(stage, values)
            phase = "UPDATE"
            for fragment, candidate in (
                ("recovery", "RECOVERY"),
                ("serving", "CATALOGUE"),
                ("catalogue", "CATALOGUE"),
                ("lineage", "LINEAGE"),
                ("metadata", "LINEAGE"),
                ("migration", "MIGRATE"),
                ("summary", "SUMMARY"),
                ("selected", "PLAN"),
                ("registration", "UNIVERSE"),
                ("check", "PRICING"),
                ("pricing", "PRICING"),
                ("acquir", "PRICING"),
                ("history", "PRICING"),
            ):
                if fragment in stage:
                    phase = candidate
                    break
            observed = dict(values)
            for name in ("symbols", "processed", "pending_work", "required"):
                if isinstance(observed.get(name), (list, tuple, set, dict)):
                    observed[name] = len(observed[name])
            for name in ("requests", "pricing_requests"):
                if isinstance(values.get(name), dict):
                    for metric, value in values[name].items():
                        if metric in FIELDS and type(value) is int:
                            observed[metric] = value
            expected = values.get("expected_io")
            if isinstance(expected, dict):
                expected_context.clear()
                for name in EXPECTED_FIELDS:
                    value = expected.get(name)
                    if name == "boundary_reads" and isinstance(value, list):
                        value = len(value)
                    if type(value) is int:
                        expected_context["expected_" + name] = value
            context = {
                name: value
                for name, value in observed.items()
                if name in FIELDS
                and type(value) in (FIELDS[name] if isinstance(FIELDS[name], tuple) else (FIELDS[name],))
            }
            context.update(expected_context, stage=stage, estimated_remaining_seconds="TBD")
            done = values.get("done", values.get("completed"))
            total = values.get("total", values.get("selected"))
            counts = (
                {"current": done, "total": total}
                if type(done) is int and type(total) is int and 0 <= done <= total
                else {}
            )
            message = stage.replace("_", " ").capitalize()
            if values.get("state") == "completed" or stage.endswith("_complete"):
                message += " component completed"
            latest = phase, message + ".", context, counts
            displayed = {**context, **estimate.context()} if estimate is not None else context
            accepted = reporter.update(phase, message=message + ".", context=displayed, **counts)
            if isinstance(expected, dict):
                # Selection emits this before acquisition/publication, even if the
                # next timed heartbeat would otherwise miss the planning phase.
                reporter.diagnostic(json.dumps({"expected_io": expected}, separators=(",", ":")))
            return accepted
        except Exception:
            return False

    def progress(stage, values):
        # Request workers may report concurrently with a producer phase change.
        # Serialize only in-memory reporting; never hold this lock over HTTP.
        with lock:
            return observe(stage, values)

    return progress


def count_callback(reporter, stage, unit):
    progress = progress_callback(reporter)
    if progress is None:
        return None
    return lambda done, total, *pages: progress(
        stage, {"done": done, "total": total, "unit": unit, **({"page_requests": pages[0]} if pages else {})}
    )

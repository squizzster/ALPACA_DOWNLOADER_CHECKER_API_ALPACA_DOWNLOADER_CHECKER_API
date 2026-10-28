"""Translate existing command observations into the shared stderr protocol."""

from libs.alpaca_heartbeat import Phase

# Progress is a projection of producer callbacks, not a new observation pass.
# DO NOT read stores or repeat work to improve counters/ETA. A sampled component's
# "completed" state is not authoritative completion of the whole command.

FIELDS = {
    name: int
    for name in (
        "selected",
        "processed",
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
    )
}
FIELDS.update(
    {name: str for name in ("stage", "state", "symbol", "status", "unit", "end_exclusive", "path", "commit")}
)
FIELDS.update(seconds=(int, float), complete=bool, estimated_remaining_seconds=(int, str))
PHASES = {
    name: Phase(message, FIELDS)
    for name, message in {
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


def progress_callback(reporter):
    if reporter is None:
        return None

    def progress(stage, values):
        try:
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
            context = {
                name: value
                for name, value in values.items()
                if name in FIELDS
                and type(value) in (FIELDS[name] if isinstance(FIELDS[name], tuple) else (FIELDS[name],))
            }
            context.update(stage=stage, estimated_remaining_seconds="TBD")
            done, total = values.get("done"), values.get("total")
            counts = (
                {"current": done, "total": total}
                if type(done) is int and type(total) is int and 0 <= done <= total
                else {}
            )
            return reporter.update(
                phase, message=stage.replace("_", " ").capitalize() + ".", context=context, **counts
            )
        except Exception:
            return False

    return progress


def count_callback(reporter, stage, unit):
    progress = progress_callback(reporter)
    if progress is None:
        return None
    return lambda done, total, *pages: progress(
        stage, {"done": done, "total": total, "unit": unit, **({"page_requests": pages[0]} if pages else {})}
    )

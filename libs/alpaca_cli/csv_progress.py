"""CSV operation phases and the translation of producer progress into status."""

from libs.alpaca_heartbeat import Phase

INITIAL_MESSAGE = (
    "The CSV Exporter system has successfully started, typically report this back to the user "
    "where relevant to do so."
)

_COUNTS = {
    name: int for name in ("identities", "groups", "rows", "csv_bytes", "physical_columns", "logical_columns")
}
_PLAN = {
    name: int
    for name in (
        "identities",
        "groups",
        "sources",
        "workers",
        "maximum_active_groups",
        "source_manifests",
        "lineage_receipts",
        "lineage_boundary_manifests",
        "pricing_boundary_manifests",
        "payload_file_read_upper_bound",
        "published_output_file_upper_bound",
        "output_lock_files",
        "temporary_manifest_files",
    )
}
_PLAN["catalogue_generation"] = str
PHASES = {
    "CATALOGUE": Phase("Opening the published online catalogue."),
    "PLAN": Phase("Selecting CSV export work from the published catalogue.", _PLAN),
    "EXPORT": Phase(
        "Writing identity CSV files and their digests.",
        {
            **_PLAN,
            **_COUNTS,
            "estimated_remaining_seconds": (int, str),
        },
    ),
    "PUBLISH": Phase("Publishing the completed export manifest.", _COUNTS),
}


def hold_plan(reporter, values, *, phase="PLAN"):
    if reporter is None:
        return False
    try:
        context = {name: value for name, value in values.items() if name in _PLAN}
        context.update(
            {name: value for name, value in values.get("metadata_read_bounds", {}).items() if name in _PLAN}
        )
        total = values["identities"]
        if phase == "EXPORT":
            context["estimated_remaining_seconds"] = "TBD"
        return reporter.update(phase, current=total if phase == "PLAN" else 0, total=total, context=context)
    except Exception:
        return False


def progress_callback(reporter):
    if reporter is None:
        return None

    def progress(stage, values):
        try:
            if stage == "csv_export_plan":
                return hold_plan(reporter, values, phase="EXPORT")
            if stage not in {"csv_export", "csv_export_publish"}:
                return False
            context = {name: value for name, value in values.items() if name in _COUNTS}
            if stage == "csv_export" and "workers" in values:
                context["workers"] = values["workers"]
            phase = "PUBLISH" if stage == "csv_export_publish" else "EXPORT"
            if phase == "EXPORT":
                context["estimated_remaining_seconds"] = "TBD"
            if phase == "EXPORT" and "estimated_remaining_seconds" in values:
                context["estimated_remaining_seconds"] = int(values["estimated_remaining_seconds"])
            return reporter.update(phase, current=values["done"], total=values["total"], context=context)
        except Exception:
            # Formatting or reporting must never change the exporter outcome.
            return False

    return progress

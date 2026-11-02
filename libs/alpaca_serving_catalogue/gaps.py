"""Producer-owned raw presence for exact scheduled source/session gaps."""

from .contract import CatalogueError

PROJECTION_VERSION = 2


def gap_queries(group):
    records = {record["public_id"]: record for record in group["history_validation"]["prefix_records"]}
    coverage = group["history_validation"]["identities"]
    queries = {}
    for decision in group["decisions"]:
        if "unexplained_missing_sessions" not in decision["reasons"]:
            continue
        public_id = decision["public_id"]
        days = set(records[public_id]["missing_sessions"]) - set(decision["explained_missing_sessions"])
        for order in coverage[public_id].values():
            for source in order["subsets"]:
                queries.setdefault(source, set()).update(days)
    return {source: days for source, days in queries.items() if days}


def complete_presence(snapshot, completed, previous):
    """Reuse accepted raw facts under producer ancestry; read only new gap scope."""
    queries = gap_queries(completed)
    result, pending = {}, {}
    for source, days in queries.items():
        checked, present = set(), set()
        for prior in previous:
            facts = prior["history_validation"].get("raw_gap_presence", {}).get(source)
            if source not in prior["history_validation"]["inputs"]:
                continue
            # A completed all-view union cannot attribute presence to a source,
            # but absence from that union proves raw absence for every member.
            # Preserve that already established fact when a newly unexplained
            # gap appears; do not reopen unchanged sources merely to repeat it.
            absent = set().union(
                *(
                    set(record["missing_sessions"]) - set(record["source_sessions_in_gaps"])
                    for record in prior["history_validation"]["prefix_records"]
                )
            )
            common = days & (set(facts["checked_sessions"]) | absent if facts else absent)
            if common and snapshot.preserves_pricing_prefix(
                source, prior["history_validation"]["inputs"][source], through=max(common)
            ):
                checked.update(common)
                if facts:
                    present.update(common & set(facts["present_sessions"]))
        result[source] = {
            "pricing_generation": completed["references"]["pricing"][source],
            "checked_sessions": sorted(days),
            "present_sessions": sorted(present),
        }
        if days - checked:
            pending[source] = days - checked
    if pending:
        for source, facts in snapshot.source_raw_presence(pending).items():
            result[source]["present_sessions"] = sorted(
                set(result[source]["present_sessions"]) | set(facts["present_sessions"])
            )
    completed["history_validation"]["raw_gap_presence"] = result
    validate_presence(completed)


def validate_presence(group):
    """Validate newly produced provenance, never an accepted consumer checkpoint."""
    facts = group["history_validation"].get("raw_gap_presence", {})
    queries = gap_queries(group)
    if facts.keys() != queries.keys():
        raise CatalogueError("raw gap provenance does not cover selected sources")
    for source, days in queries.items():
        item = facts[source]
        if (
            item["pricing_generation"] != group["references"]["pricing"][source]
            or item["checked_sessions"] != sorted(days)
            or item["present_sessions"] != sorted(set(item["present_sessions"]))
            or not set(item["present_sessions"]) <= days
        ):
            raise CatalogueError("invalid source-specific raw gap provenance")


def migrate_projections(store, accepted, *, progress=None):
    """Explicit conversion of selected v1 bodies; no lineage or numeric replay."""
    from alpaca_end_of_day._standalone_group import raw_source_presence
    from libs.alpaca_lineage_build import storage as lineage
    from libs.alpaca_pricing_download.storage import load_source, regular_path

    from . import storage

    converted, source_reads = 0, 0
    state = accepted["state"]
    for key, selection in sorted(accepted["groups"].items()):
        if selection.get("late_bar_repairs", {}).get("version") == PROJECTION_VERSION:
            continue
        group = None
        if selection["ineligible_identities"]:
            group = storage.read_groups(store, [key], generation=state["generation"])[key]
            queries = gap_queries(group)
            sources = {}
            with lineage.pricing_locks(store, queries, known=queries):
                for name in queries:
                    root = store / "end_of_day/pricing" / name
                    if regular_path(root, ".pending.json", published=True).exists():
                        raise CatalogueError(f"{name}: pricing publication pending; resume its owner")
                    source = load_source(root, name)
                    if source is None or lineage.document_digest(source) != group["references"]["pricing"][name]:
                        raise CatalogueError(f"{name}: raw projection input changed; finish its owning update")
                    sources[name] = source
                source_reads += len(sources)
                group["history_validation"]["raw_gap_presence"] = raw_source_presence(
                    store, sources, group["references"]["pricing"], queries
                )
                validate_presence(group)
                state = storage.publish_projection(store, key, selection, group, state)
        else:
            state = storage.publish_projection(store, key, selection, None, state)
        converted += 1
        if progress:
            progress(
                "serving_projection_migration", {"completed_groups": converted, "source_reads": source_reads}
            )
    return {
        "generation": state["generation"],
        "catalogue_complete": state["complete"],
        "projection_groups": converted,
        "projection_source_reads": source_reads,
    }

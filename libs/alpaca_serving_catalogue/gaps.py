"""Producer-owned raw presence for exact scheduled source/session gaps."""

from alpaca_end_of_day._history_coverage import coverage_sources

from .contract import CatalogueError
from .source_equivalence import applicable_dates

PROJECTION_VERSION = 2


def identity_gaps(group):
    """Yield the unexplained dates and contributors from each assessed full history."""
    records = {record["public_id"]: record for record in group["history_validation"]["prefix_records"]}
    coverage = group["history_validation"]["identities"]
    for decision in group["decisions"]:
        if not (
            "unexplained_missing_sessions" in decision["reasons"]
            or decision["eligible"]
            and decision.get("publication_start_after_missing_session")
        ):
            continue
        public_id = decision["public_id"]
        days = set(records[public_id]["missing_sessions"]) - set(decision["explained_missing_sessions"])
        if days:
            yield days, {source for order in coverage[public_id].values() for source in coverage_sources(order)}


def gap_queries(group):
    queries = {}
    for days, sources in identity_gaps(group):
        for source in sources:
            queries.setdefault(source, set()).update(days)
    return queries


def refresh_presence(snapshot, previous, inputs, days, observed_raw):
    """Refresh all-view gap evidence for the dates and sources still unanswered."""
    unknown = {name: set(days) for name in inputs}
    present = set()
    for group in previous:
        checkpoint = group["history_validation"]
        old = checkpoint["inputs"]
        carried = {}
        for name in inputs.keys() & old.keys():
            if inputs[name]["pricing_generation"] == old[name]["pricing_generation"]:
                carried[name] = days
            else:
                inserted = set()
                carried[name] = applicable_dates(snapshot, name, old[name], days, inserted_sessions=inserted)
                present.update(inserted)
                observed_raw.setdefault(name, {}).update(dict.fromkeys(inserted, True))
        whole = days.intersection(*carried.values()) if carried.keys() == old.keys() else set()
        for record in checkpoint["prefix_records"]:
            common = days.intersection(record["missing_sessions"])
            union_present = common.intersection(record["source_sessions_in_gaps"])
            # A retained all-view union proves group absence for every surviving
            # source. Its positives survive when every possible contributor does.
            present.update(union_present & whole)
            absent = common - union_present
            for name, applicable in carried.items():
                known_absent = absent & applicable
                unknown[name].difference_update(known_absent)
                observed_raw.setdefault(name, {}).update(dict.fromkeys(known_absent, False))
        for name, facts in checkpoint.get("raw_gap_presence", {}).items():
            if name in carried:
                positives = carried[name].intersection(facts["present_sessions"])
                present.update(positives)
                accepted = observed_raw.setdefault(name, {})
                accepted.update(
                    (day, day in positives) for day in carried[name].intersection(facts["checked_sessions"])
                )
    pending = {name: required - present for name, required in unknown.items() if required - present}
    if pending:
        present.update(snapshot._selected_gap_presence(pending, observed_raw))
    return present


def _absent_from_all_views(checkpoint):
    # Presence in the all-view union cannot identify a source; absence proves
    # every source in this completed group lacks a raw row on that date.
    return set().union(
        *(
            set(record["missing_sessions"]) - set(record["source_sessions_in_gaps"])
            for record in checkpoint["prefix_records"]
        )
    )


def resolve_raw_presence(snapshot, queries, previous, generations, observed_raw):
    """Resolve selected source/dates from current observations or applicable published facts."""
    result, pending = {}, {}
    for source, days in queries.items():
        held = (observed_raw or {}).get(source, {})
        checked = days.intersection(held)
        present = {day for day in checked if held[day]}
        unassessed = days - checked
        for prior in previous:
            if not unassessed:
                break
            facts = prior["history_validation"].get("raw_gap_presence", {}).get(source)
            if source not in prior["history_validation"]["inputs"]:
                continue
            absent = _absent_from_all_views(prior["history_validation"])
            common = unassessed & (set(facts["checked_sessions"]) | absent if facts else absent)
            old = prior["history_validation"]["inputs"][source]
            if old["pricing_generation"] != generations[source]:
                inserted = set()
                applicable = applicable_dates(snapshot, source, old, unassessed, inserted_sessions=inserted)
                common.intersection_update(applicable)
                checked.update(inserted)
                present.update(inserted)
                unassessed.difference_update(inserted)
            if common:
                checked.update(common)
                unassessed.difference_update(common)
                if facts:
                    present.update(common & set(facts["present_sessions"]))
        result[source] = {
            "pricing_generation": generations[source],
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
    return result


def complete_presence(snapshot, completed, previous, *, observed_raw=None):
    """Reuse accepted raw facts under producer ancestry; read only new gap scope."""
    queries = gap_queries(completed)
    completed["history_validation"]["raw_gap_presence"] = resolve_raw_presence(
        snapshot, queries, previous, completed["references"]["pricing"], observed_raw
    )
    validate_presence(completed, queries)


def validate_presence(group, queries):
    """Validate newly produced provenance, never an accepted consumer checkpoint."""
    facts = group["history_validation"].get("raw_gap_presence", {})
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
        if selection.get("partial_gap") or selection["ineligible_identities"]:
            group = storage.read_groups(store, [key], generation=state["generation"])[key]
        if group is not None and "raw_gap_presence" in group["history_validation"]:
            # A completed raw-presence handoff is sufficient to reinstall the
            # scheduling projection for either an online or an offline identity.
            state = storage.publish_projection(store, key, selection, group, state, retain_body=True)
        elif group is not None:
            queries = gap_queries(group)
            generations = group["references"]["pricing"]
            absent = _absent_from_all_views(group["history_validation"])
            pending = {name: days - absent for name, days in queries.items() if not days <= absent}
            sources = {}
            with lineage.pricing_locks(store, pending, known=pending):
                for name in pending:
                    root = store / "end_of_day/pricing" / name
                    if regular_path(root, ".pending.json", published=True).exists():
                        raise CatalogueError(f"{name}: pricing publication pending; resume its owner")
                    source = load_source(root, name)
                    if source is None or lineage.document_digest(source) != generations[name]:
                        raise CatalogueError(f"{name}: raw projection input changed; finish its owning update")
                    sources[name] = source
                source_reads += len(sources)
                found = raw_source_presence(store, sources, generations, pending) if pending else {}
                group["history_validation"]["raw_gap_presence"] = {
                    name: {
                        "pricing_generation": generations[name],
                        "checked_sessions": sorted(days),
                        "present_sessions": found[name]["present_sessions"] if name in pending else [],
                    }
                    for name, days in queries.items()
                }
                validate_presence(group, queries)
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

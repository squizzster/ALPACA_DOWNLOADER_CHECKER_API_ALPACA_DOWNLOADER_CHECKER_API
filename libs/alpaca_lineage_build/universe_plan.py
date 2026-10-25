"""Select update work from compact accepted boundaries, without ticker discovery."""

from datetime import timedelta
from math import ceil
from pathlib import Path

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_pricing_download import check_summary
from libs.alpaca_pricing_download.append import MAX_SYMBOLS, _cohorts, append_needs
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, HISTORY_START, timestamp
from libs.alpaca_pricing_download.live_check import plan as check_plan
from libs.alpaca_pricing_download.update_checks import read_checks
from libs.alpaca_recovery_registry import load_labels
from libs.alpaca_security_lineage.lineage import compact_asset
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage

from . import storage, universe_state
from .catalogue import DISCOVERY_EXCHANGES, by_symbol, discovery_labels
from .contract import LineageError, document_digest, reusable_decision
from .projection import require_complete
from .work_contract import (
    acquisition_fingerprint,
    covers_pricing_start,
    lookup_fact,
    obligation,
    pricing_target,
    replacement_fingerprint,
    restriction,
    satisfying_entry,
    work_graph,
)


def boundaries(store, registry, *, context=None):
    from libs.alpaca_serving_catalogue.producer import status as serving_status
    from libs.alpaca_serving_catalogue.storage import controls as serving_controls

    store = Path(store)
    storage.check_store(store)
    root = store / "end_of_day/lineage"
    with storage.publication_lock(root):
        if (root / ".pending.json").exists():
            raise LineageError("pending lineage publication; recover it before planning")
        index = storage.read_index(root) if context is None else context.select_index(root)
        require_complete(index)
        # An unavailable summary is not an empty universe. Its owner must finish
        # the named pending inputs before ordinary selection can use this boundary.
        # Substituting None would send every active source through historical
        # endpoint checks, current-month acquisition and producer checks again.
        summary = check_summary.read_snapshot(store) if context is None else context.select_summary()
    checks = read_checks(store, readonly=True)
    if not checks["complete"]:
        raise LineageError("lineage scheduling coverage missing; explicit lineage update is required")
    return {
        "index": index,
        "summary": summary,
        "checks": checks,
        "registry": set(load_labels(registry)),
        "state": universe_state.read_state(store),
        "serving_catalogue": serving_status(
            store, controls=serving_controls(store) if context is None else context.select_serving()
        ),
    }


def changed_evidence(index, observation, evidence_client, *, excluded=(), work=None):
    """Positive evidence selects groups; failed exact lookups remain explicit work."""
    catalogue = by_symbol(observation)
    changed, mapping, failures = set(), set(), {}
    previous_lookups = {
        symbol: list(entry["projection"]["symbol_lookups"]) for symbol, entry in index["symbols"].items()
    }
    for symbol, item in (work or {}).items():
        if item["status"] != "complete":
            accepted = item.get("required_lookups", {})
            # Accepted unfinished evidence is newer than the indexed decision.
            # Comparing both for the same lookup would continually rediscover an
            # already accepted mapping and invalidate its completed replacement.
            previous_lookups[symbol] = [
                lookup for lookup in previous_lookups.get(symbol, []) if lookup["lookup"] not in accepted
            ] + [{**lookup, "lookup": label} for label, lookup in accepted.items()]
    for symbol, lookups in previous_lookups.items():
        if symbol in excluded:
            continue
        for previous in lookups:
            label = previous["lookup"]
            if label not in catalogue or label in excluded:
                continue
            try:
                current = evidence_client.get_symbol_lookup(label)
            except (RuntimeError, OSError, ValueError) as error:
                from libs.alpaca_pricing_download.contract import failure_outcome

                failures[symbol] = {
                    "error": str(error),
                    "outcome": failure_outcome(error),
                    "request_identifiers": getattr(error, "request_identifiers", []),
                }
                continue
            asset = compact_asset(current["asset"]) if current.get("asset") else None
            if lookup_fact(previous) != lookup_fact(current):
                changed.add(symbol)
                old = previous.get("asset") or {}
                if (old.get("id"), old.get("symbol")) != ((asset or {}).get("id"), (asset or {}).get("symbol")):
                    mapping.add(symbol)
    return changed, mapping, failures


def select_work(accepted, observation, evidence_client, end_date, *, retry_blocked=False):
    if accepted.get("summary") is None:
        raise LineageError("complete pricing check summary required before selecting update work")
    index = accepted["index"]
    entries, state = index["symbols"], accepted["state"]
    catalogue = by_symbol(observation)
    discovered = discovery_labels(observation)
    graph = work_graph(entries, state["work"])
    satisfied_labels = {
        s
        for s, item in state["work"].items()
        if item["status"] != "complete" and satisfying_entry(s, item, entries, accepted["checks"]["checks"])
    }
    restrictions = {
        s: reason
        for s, item in state["work"].items()
        if s not in satisfied_labels and (reason := restriction(item, end_date, retry_blocked=retry_blocked))
    }
    excluded = set().union(*(graph.closure({s}) for s in restrictions)) if restrictions else set()
    evidence_client.restricted_identifiers.update(
        identifier
        for s in restrictions
        for identifier in state["work"][s].get("outcome", {}).get("request_identifiers", [])
    )
    changed, mapping, failures = changed_evidence(
        index, observation, evidence_client, excluded=excluded, work=state["work"]
    )
    reconciled = set().union(*(graph.closure({s}) for s in changed)) if changed else set()
    mapping_groups = set().union(*(graph.closure({s}) for s in mapping)) if mapping else set()
    reasons = {}

    def add(symbol, why):
        reasons.setdefault(symbol, set()).add(why)

    for symbol in accepted["checks"]["checks"]:
        add(symbol, "pending_lineage")
    for symbol, entry in entries.items():
        if not reusable_decision(entry):
            add(symbol, "changed_rules")

    # Stored active identity remains maintenance authority when the catalogue omits
    # a label. Explicit current inactivity is a reconciliation event, not absence.
    stored_active = {
        s
        for s, entry in entries.items()
        if any(
            item.get("asset", {}).get("status") == "active" and item.get("asset", {}).get("symbol") == s
            for item in (
                [entry["projection"]["last_observed_symbol_lookup"]]
                if entry["projection"].get("last_observed_symbol_lookup")
                else [lookup for lookup in entry["projection"]["symbol_lookups"] if lookup.get("lookup") == s]
            )
            if item.get("asset")
        )
    }
    current_active = {s for s, rows in catalogue.items() if any(r["status"] == "active" for r in rows)}
    maintained = (
        discovered | (stored_active - (catalogue.keys() - current_active)) | (current_active & entries.keys())
    )
    summarized = {e["symbol"] for e in accepted["summary"]["entries"]}
    for symbol in maintained:
        entry = entries.get(symbol)
        if entry is None:
            add(symbol, "initial_acquisition")
        elif symbol not in summarized and entry["projection"]["processed_through"][:10] < end_date:
            add(
                symbol,
                "advance_empty_coverage"
                if entry["projection"]["source_rows"] == 0
                else "advance_retained_coverage",
            )
        elif symbol in summarized and entry["projection"]["processed_through"][:10] < end_date:
            add(symbol, "advance_summary_coverage")
    for symbol in reconciled:
        add(symbol, "changed_metadata")
    for symbol in mapping_groups:
        add(symbol, "changed_identity_mapping")
    for symbol in failures:
        add(symbol, "selection_metadata_failed")
    for symbol, prior in state["work"].items():
        if prior["status"] != "complete" and symbol not in satisfied_labels:
            add(symbol, "unfinished_work")
    deferred = [
        {"symbol": s, "reason": restrictions.get(s, "affected_group_restricted")} for s in sorted(excluded)
    ]
    work = {}
    for symbol, why in sorted(reasons.items()):
        if symbol in excluded:
            continue
        prior = state["work"].get(symbol, {})
        unfinished = prior.get("status") not in (None, "complete") and symbol not in satisfied_labels
        entry = entries.get(symbol)
        required_group = set(prior.get("required_group", [])) if unfinished else set()
        if entry is None and not (why | set(prior.get("reasons", []))).intersection(
            {"initial_acquisition", "explicit_pricing_acquisition"}
        ):
            group = graph.closure({symbol})
            if group.intersection(entries):
                # A missing member of a published group belongs to lineage;
                # metadata reconciliation must not turn it into a new seed.
                why.add("dependency_reconciliation")
                required_group.update(group)
        labels = {symbol}
        if entry:
            labels.update(item["lookup"] for item in entry["projection"]["symbol_lookups"])
        lookups = dict(prior.get("required_lookups", {})) if unfinished else {}
        labels.update(lookups)
        for label in sorted(labels):
            if label not in catalogue:
                continue
            try:
                lookups[label] = evidence_client.get_symbol_lookup(label)
            except (RuntimeError, OSError, ValueError) as error:
                from libs.alpaca_pricing_download.contract import failure_outcome

                failures[symbol] = {
                    "error": str(error),
                    "outcome": failure_outcome(error),
                    "request_identifiers": getattr(error, "request_identifiers", []),
                }
        fingerprint = document_digest({"lookups": lookups}) if lookups else prior.get("evidence_fingerprint")
        if (
            unfinished
            and prior.get("evidence_fingerprint")
            and {label: lookup_fact(value) for label, value in lookups.items()}
            == {label: lookup_fact(value) for label, value in prior.get("required_lookups", {}).items()}
        ):
            # Catalogue/exact provenance does not create a new evidence requirement.
            fingerprint = prior["evidence_fingerprint"]
        # Metadata requirements describe the current lookups. The separate pricing
        # requirement survives status/exchange changes, but never a mapping change.
        acquisition_evidence = (
            acquisition_fingerprint(prior) if unfinished and symbol not in mapping_groups else fingerprint
        )
        covered = entry["projection"]["processed_through"][:10] if entry else None
        required_start = prior.get("pricing_start") if unfinished else None
        extend_history = bool(entry) and not covers_pricing_start(
            {"pricing_start": required_start}, entry["projection"].get("pricing_request")
        )
        invalidate = symbol in mapping_groups or (unfinished and prior.get("invalidate_pricing", False))
        required_end = end_date if symbol in maintained or covered is None else covered
        if unfinished:
            required_end = max(required_end, pricing_target(prior))
        if invalidate and covered and covered > end_date:
            deferred.append({"symbol": symbol, "reason": "required_replacement_exceeds_requested_cutoff"})
        if extend_history and covered > end_date:
            deferred.append({"symbol": symbol, "reason": "required_history_extension_exceeds_requested_cutoff"})
        # Preserve the required coverage, mapping and correction, not yesterday's
        # route to them. A summarized maintained source reaches its accepted target
        # through live check -> approved append. Carrying prior.acquire forward
        # would make a temporary recovery detour survive every later restart.
        item = {
            "reasons": sorted(why | (set(prior.get("reasons", [])) if unfinished else set())),
            "end_exclusive": end_date,
            "pricing_start": required_start,
            "pricing_end_exclusive": required_end,
            "evidence_fingerprint": fingerprint,
            "acquisition_evidence_fingerprint": acquisition_evidence,
            "required_lookups": lookups,
            "status": "pending",
            "attempts": prior.get("attempts", 0),
            "required_history_check": unfinished and prior.get("required_history_check", False),
            "correction_id": prior.get("correction_id") if unfinished else None,
            "required_group": sorted(required_group),
            "acquire": entry is None
            or invalidate
            or extend_history
            or bool(why & {"advance_empty_coverage", "advance_retained_coverage"})
            or (
                covered is not None
                and covered < required_end
                and (symbol not in summarized or symbol not in maintained)
            ),
            "invalidate_pricing": invalidate,
            "refresh_metadata": symbol in reconciled
            or symbol in failures
            or (unfinished and prior.get("refresh_metadata", False)),
        }
        work[symbol] = item
        # A missing fresh record cannot erase previously accepted positive evidence.
        for label, lookup in lookups.items():
            if label not in catalogue:
                evidence_client.assets.setdefault(label, lookup)
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # The cause belongs to the complete affected group, even when a member's
    # own lookup is unchanged or absent from the catalogue. Use only accepted
    # in-memory evidence; retries retain their already bound requirements.
    replacements = {
        symbol
        for symbol, item in work.items()
        if symbol in mapping_groups or item["invalidate_pricing"] and acquisition_fingerprint(item) is None
    }
    while replacements:
        group = graph.closure({min(replacements)})
        lookups = {
            lookup["lookup"]: lookup
            for symbol in sorted(group)
            for lookup in entries.get(symbol, {}).get("projection", {}).get("symbol_lookups", [])
        }
        for symbol in sorted(group):
            prior = state["work"].get(symbol, {})
            if prior.get("status") not in (None, "complete"):
                lookups.update(prior.get("required_lookups", {}))
        for symbol in sorted(group):
            lookups.update(work.get(symbol, {}).get("required_lookups", {}))
        fingerprint = replacement_fingerprint(lookups)
        for symbol in group & replacements:
            work[symbol]["acquisition_evidence_fingerprint"] = fingerprint
            work[symbol]["required_group"] = sorted(group)
        replacements.difference_update(group)
    for symbol, item in work.items():
        prior = state["work"].get(symbol, {})
        unfinished = prior.get("status") not in (None, "complete") and symbol not in satisfied_labels
        work[symbol] = obligation(item, prior if unfinished else None)
    registered = set(entries)
    deferred_labels = {item["symbol"] for item in deferred}
    restricted_groups = set().union(*(graph.closure({s}) for s in deferred_labels)) if deferred_labels else set()
    deferred.extend(
        {"symbol": s, "reason": "affected_group_restricted"} for s in sorted(restricted_groups - deferred_labels)
    )
    registered.update(dep for entry in entries.values() for dep in entry["dependencies"])
    additions = sorted((registered | discovered | set(state["registration_pending"])) - accepted["registry"])
    return {
        "work": work,
        "registration_additions": additions,
        "already_published": sorted(satisfied_labels - work.keys()),
        "deferred": deferred,
        "selection_failures": failures,
        "discovered_active_labels": len(discovered),
    }


def preview(accepted, selection, observation, end_date, *, metadata_requests=1):
    summary = accepted["summary"]
    deferred = {item["symbol"] for item in selection["deferred"]} | set(selection.get("selection_failures", {}))
    eligible = [
        entry
        for entry in summary["entries"]
        if entry["end_exclusive"][:10] <= end_date and entry["symbol"] not in deferred
    ]
    history = check_plan({**summary, "entries": eligible})
    append, _ = append_needs(eligible, timestamp(end_date + "T00:00:00Z"))
    initial, empty = [], []
    for symbol, item in selection["work"].items():
        if not item["acquire"] or symbol in deferred:
            continue
        projection = accepted["index"]["symbols"].get(symbol, {}).get("projection", {})
        request = projection.get("pricing_request") or {}
        target = pricing_target(item)
        start = request.get("history_start") or HISTORY_START
        if item.get("pricing_start"):
            start = (min(start[:10], item["pricing_start"]) if request else item["pricing_start"]) + "T00:00:00Z"
        extending_empty = (
            "advance_empty_coverage" in item["reasons"]
            and not item["invalidate_pricing"]
            and covers_pricing_start(item, request)
        )
        if extending_empty:
            start = projection["processed_through"]
        (empty if extending_empty else initial).append(
            {
                "kind": "bars",
                "why": "append_completed_sessions" if extending_empty else "initial_or_invalidated_history",
                "symbols": [symbol],
                "adjustments": list(ADJUSTMENTS),
                "asof": target,
                "ranges": [{"start": start, "end_exclusive": target + "T00:00:00Z"}],
            }
        )
    plans = [build_plan({"steps": [{"needs": cohort}]}) for cohort in _cohorts(initial)]
    empty_plans = [
        build_plan({"steps": [{"needs": cohort}]}, max_symbols=MAX_SYMBOLS, merge_ranges=False)
        for cohort in _cohorts(empty)
    ]
    append_plans = [
        build_plan({"steps": [{"needs": cohort}]}, max_symbols=MAX_SYMBOLS, merge_ranges=False)
        for cohort in _cohorts(append)
    ]

    def count(plans):
        return sum(len(step["actions"]) for plan in plans for step in plan["steps"])

    def pages(plans):
        total = 0
        for plan in plans:
            for step in plan["steps"]:
                for action in step["actions"]:
                    params = action["request"]["params"]
                    start = timestamp(params["start"])
                    end = timestamp(params["end"]) + timedelta(microseconds=1)
                    sessions = max(
                        1,
                        expected_market_session_coverage(
                            start.date().isoformat(),
                            max(end.date(), start.date() + timedelta(days=1)).isoformat(),
                        ).session_count,
                    )
                    total += max(1, ceil(sessions * len(params["symbols"].split(",")) / params["limit"]))
        return total

    return {
        "mode": "plan",
        "end_exclusive": end_date,
        "feed": "sip",
        "discovery_exchanges": DISCOVERY_EXCHANGES,
        "catalogue_observed_at_utc": observation["catalogue_observed_at_utc"],
        "catalogue_records": len(observation["records"]),
        "summary_symbols": len(summary["entries"]),
        "already_beyond_target": [e["symbol"] for e in summary["entries"] if e["end_exclusive"][:10] > end_date],
        **selection,
        "expected_io": {
            "boundary_reads": [
                "recovery_registry",
                "lineage_index",
                "pricing_check_summary",
                "universe_work_state",
                "lineage_hints",
                "serving_catalogue_and_obligations",
            ],
            "selected_pricing_manifests": len(initial) + len(empty),
            "append_approval_manifest_reads": len(append),
            "selected_history_obligation_manifest_reads": sum(
                bool(item.get("required_history_check"))
                for symbol, item in selection["work"].items()
                if symbol not in deferred
            ),
            "selected_lineage_labels": len(selection["work"]),
            "existing_lineage_receipts_selected": len(
                set(selection["work"]) & accepted["index"]["symbols"].keys()
            ),
            "catalogue_actions": 1,
            "selection_metadata_http_requests_observed": metadata_requests,
            "history_check_actions": count([history]),
            "initial_or_invalidated_actions_estimate": count(plans),
            "empty_coverage_actions_estimate": count(empty_plans),
            "acquisition_actions_estimate": count(plans) + count(empty_plans),
            "append_actions_before_reconciliation": count(append_plans),
            "http_pages_minimum": metadata_requests
            + count([history])
            + count(plans)
            + count(empty_plans)
            + count(append_plans),
            "http_pages_dense_history_estimate": metadata_requests
            + pages([history])
            + pages(plans)
            + pages(empty_plans)
            + pages(append_plans),
            "http_page_assumptions": "known work only, not an ETA: one daily row per selected exchange session and full pages; sparse histories reduce rows, underfilled pages and retries increase requests; exhaust every page token",
            "conditional_work": "retained dependency endpoint checks and their selected incremental/replacement acquisitions, same-span corrections, first-session checks, additional exact/CUSIP lookups, corporate actions, dependency expansion and publications for every resulting complete group; these may dominate the known minimum",
            "pricing_publications_estimate": len(initial) + len(empty) + len(append),
            "lineage_publications": "complete affected groups; dependency expansion may add work",
            "serving_pending_symbols": accepted["serving_catalogue"]["pending_symbols"],
            "serving_history_reads": "published append scopes reuse completed history checks; suffix payloads supply all eight views; replacement or changed historical selection reevaluates affected histories",
            "serving_publications": "one transaction per affected complete component, including previous contributions; zero extra provider requests or pricing writes",
        },
        "blockers": [],
    }

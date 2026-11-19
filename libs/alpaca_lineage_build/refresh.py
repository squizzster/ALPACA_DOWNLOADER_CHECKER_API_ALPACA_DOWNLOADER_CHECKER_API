"""Live check and correction produce an explicit, generation-bound append approval."""

from libs.alpaca_pricing_download.acquire import MeasuredRequests
from libs.alpaca_pricing_download.append import select_append_sources
from libs.alpaca_pricing_download.check_summary import assert_current, describe
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.correction import applicable_contract
from libs.alpaca_pricing_download.endpoints import EndpointSelection, HistoryComparison
from libs.alpaca_pricing_download.history_tail import upgrade_legacy_entries
from libs.alpaca_pricing_download.live_check import verify
from libs.alpaca_pricing_download.refresh import (
    check_retained_source,
    prepare_refresh,
    probe_late_bar_repairs,
    recover_selected_correction,
    replace_prepared,
)
from libs.alpaca_pricing_download.storage import regular_path
from libs.common.debug_trace import event, span, traced

from . import storage
from .completed_corrections import covers_selected_advance
from .context import HistoryScope, accept_context, recover_and_accept_correlation_policy
from .pipeline import run_lineage
from .work_contract import acquisition_fingerprint


@traced("required_history")
def refresh_required_history(store, *, pricing_client, work_policy, end_date, context=None, progress=None):
    """Accepted checks/corrections stay executable after summary removal."""
    # This owner handles explicitly owed checks outside active summary membership.
    # Absence from that summary does not erase a correction or authorize checking
    # every other inactive ticker. Consume only the selected durable obligations.
    empty = {"results": [], "requests": {}, "failed": 0, "checked": []}
    if not any(item.get("required_history_check") for item in work_policy.work.values()):
        return empty
    context = accept_context(store, context, work_policy)
    recover_and_accept_correlation_policy(store, context)
    metrics = {}
    if callable(getattr(pricing_client, "execute_plan", None)):
        pricing_client = MeasuredRequests(pricing_client, metrics)
    summarized = {entry["symbol"] for entry in context.select_summary()["entries"]}
    prepared, recovered, checked = [], [], {}
    for symbol, item in list(work_policy.work.items()):
        if symbol in summarized or not item.get("required_history_check") or not work_policy.allowed({symbol}):
            continue
        try:
            if progress:
                progress("required_history", {"symbol": symbol, "state": "checking"})
            selected = check_retained_source(
                store,
                symbol,
                end_date,
                client=pricing_client,
                pending=item,
                indexed_generation=work_policy.index["symbols"].get(symbol, {}).get("source_sha256"),
            )
        except (RuntimeError, OSError, ValueError) as error:
            work_policy.fail({symbol}, error, stage="history_check")
            continue
        if selected.get("publication"):
            recovered.append(selected["publication"])
        elif not selected["matched"] or (
            item.get("correction_id") and item["correction_id"] != selected["history_correction_id"]
        ):
            prepared.append(selected)
        else:
            checked[symbol] = selected["source_sha256"]
    prepared = [item for item in prepared if work_policy.allowed({item["symbol"]})]
    work_policy.require_correction([item for item in prepared if not item.get("completed_correction")])
    replaced = (
        replace_prepared(
            store,
            prepared,
            client=pricing_client,
            context=context.pricing(),
            progress=progress,
        )
        if prepared
        else empty
    )
    replaced["results"].extend(recovered)
    work_policy.record_results(replaced, stage="history_correction")
    work_policy.accept_correction_completions(replaced["results"])
    checked.update(
        (item["symbol"], item["source_sha256"]) for item in replaced["results"] if item["status"] == "replaced"
    )
    work_policy.history_approved.update(checked)
    if progress:
        progress("required_history", {"state": "completed", "checked": len(checked), "requests": metrics})
    return {**replaced, "checked": sorted(checked), "requests": metrics or replaced["requests"]}


@traced("live_check_and_correction")
def refresh_existing(
    store,
    *,
    pricing_client,
    identity_client,
    progress=None,
    cohort_size=32,
    on_ready=None,
    end_date=None,
    evidence_client=None,
    register_discoveries=None,
    work_policy=None,
    symbols=None,
    excluded_symbols=(),
    late_bar_repairs=None,
    context=None,
):
    """Check newly eligible members too; approval never expands with the summary."""
    context = accept_context(store, context, work_policy)
    excluded_symbols = frozenset(excluded_symbols)
    recover_and_accept_correlation_policy(store, context)
    pricing_context = context.pricing()
    verified, generations, checks, completed_replacements = {}, {}, [], {}
    accepted_insertions = {}
    verified_scopes, compared_scopes, replacement_scopes = {}, {}, {}
    pricing = {"results": [], "failed": 0, "requests": {}}
    lineage = {"results": [], "failed": 0}
    beyond, restricted = set(), set()
    snapshot = context.select_summary()
    with storage.publication_lock(context.accept_store().root):
        selected_index = context.select_index(context.accept_store().root)
    accepted_generations = {s: entry["source_sha256"] for s, entry in selected_index["symbols"].items()}
    accepted_generations.update(
        (entry["symbol"], entry["source_sha256"]) for entry in snapshot["entries"] if entry.get("source_sha256")
    )

    def in_scope(entry):
        return (
            (symbols is None or entry["symbol"] in symbols)
            and entry["symbol"] not in excluded_symbols
            and (not end_date or entry["end_exclusive"][:10] <= end_date)
            and (work_policy is None or work_policy.allowed({entry["symbol"]}))
        )

    def requested_scope(symbol, *, producer=None):
        if producer is not None:
            return HistoryScope(
                producer.end_exclusive[:10],
                producer.acquisition_fingerprint,
                producer.correction_id,
                producer,
            )
        requirement = work_policy.work.get(symbol, {}) if work_policy else {}
        fingerprint = acquisition_fingerprint(requirement) if requirement.get("invalidate_pricing") else None
        if requirement.get("invalidate_pricing") and fingerprint is None:
            raise DownloadError(
                f"{symbol}: missing accepted mapping-evidence handoff; owning metadata/lineage recovery required"
            )
        return HistoryScope(
            end_date,
            fingerprint,
            requirement.get("correction_id"),
            producer,
        )

    def scope_applies(scope, symbol):
        requirement = work_policy.work.get(symbol, {}) if work_policy else {}
        fingerprint = acquisition_fingerprint(requirement) if requirement.get("invalidate_pricing") else None
        if requirement.get("invalidate_pricing") and fingerprint is None:
            return False
        return scope.applies(
            end_date,
            fingerprint,
            requirement.get("correction_id"),
        )

    upgraded = {}
    legacy = [entry for entry in snapshot["entries"] if in_scope(entry) and entry.get("tail_all") is None]
    if legacy:
        assert_current(store, snapshot["token"])
        upgraded = upgrade_legacy_entries(store, legacy, progress=progress)
        snapshot = context.select_summary()
        # Metadata-only generations preserve all pricing observations. Carry
        # their newly published generation into selection; do not recalculate
        # historical lineage merely to install a comparison tail.
        accepted_generations.update((symbol, value["source_sha256"]) for symbol, value in upgraded.items())

    def accept_completed_publications(ready, index):
        for entry in ready["entries"]:
            symbol = entry["symbol"]
            if not in_scope(entry) or verified.get(symbol) == entry:
                continue
            requirement = work_policy.work.get(symbol, {}) if work_policy else {}
            for origin, publications in (
                ("full_history", pricing_context.full_publications),
                ("approved_append", pricing_context.approved_appends),
            ):
                accepted = publications.get(symbol)
                if accepted is None or not accepted.applies(
                    context.store,
                    entry,
                    index["symbols"].get(symbol),
                    end_date,
                    invalidate_mapping=requirement.get("invalidate_pricing", False),
                    acquisition_fingerprint=acquisition_fingerprint(requirement),
                    correction_id=requirement.get("correction_id"),
                ):
                    continue
                verified[symbol] = dict(entry)
                verified_scopes[symbol] = requested_scope(symbol, producer=accepted)
                generations[symbol] = accepted.generation
                event(
                    "history_completed_publication_reused",
                    origin=origin,
                    symbol=symbol,
                    source_generation=accepted.generation,
                    asof=accepted.asof,
                    end_exclusive=accepted.end_exclusive,
                )
                break

    accept_completed_publications(snapshot, selected_index)
    compared = {}
    if context.history_checks or context.history_comparisons:
        for entry in snapshot["entries"]:
            symbol = entry["symbol"]
            if not in_scope(entry):
                continue
            prior = context.history_checks.get(symbol)
            generation = accepted_generations.get(symbol)
            correction = work_policy.work.get(symbol, {}).get("correction_id") if work_policy else None
            comparison = context.history_comparisons.get(symbol)
            if prior is not None and prior == (entry, generation, correction):
                scope = context.history_scope("check", symbol, prior)
                if scope_applies(scope, symbol) and (
                    scope.producer is None
                    or selected_index["symbols"].get(symbol, {}).get("source_sha256") == generation
                ):
                    verified[symbol] = dict(entry)
                    verified_scopes[symbol] = scope
                    generations[symbol] = generation
            if comparison is not None and comparison[:2] == (entry, generation):
                scope = context.history_scope("comparison", symbol, comparison)
                route_generation = comparison[3] if len(comparison) > 3 else None
                if scope_applies(scope, symbol) and (
                    route_generation is None
                    or selected_index["symbols"].get(symbol, {}).get("sha256") == route_generation
                ):
                    compared[symbol] = comparison[2]
                    compared_scopes[symbol] = scope

    def reconcile_publications(publications):
        value = run_lineage(
            store,
            sorted(publications),
            identity_client=identity_client,
            pricing_client=pricing_client,
            end_date=end_date,
            evidence_client=evidence_client,
            register_discoveries=register_discoveries,
            work_policy=work_policy,
            context=context,
            progress=progress,
            on_pricing=progress,
            pricing_handoff=publications,
        )
        lineage["results"].extend(value["results"])
        lineage["failed"] += value["failed"]
        return {
            symbol: generation
            for item in value["results"]
            for symbol, generation in item.get("pricing_replacements", {}).items()
        }

    def replace_and_reconcile(prepared, baseline_token, *, recovered=(), serving_generation=None):
        def before_publish():
            assert_current(store, baseline_token)
            if serving_generation is not None:
                from libs.alpaca_serving_catalogue.storage import selector_state

                if selector_state(store)["generation"] != serving_generation:
                    raise DownloadError("selected serving catalogue changed before late-bar repair")

        selected_scopes = {
            request["symbol"]: requested_scope(request["symbol"]) for request in (*prepared, *recovered)
        }
        replaced = replace_prepared(
            store,
            prepared,
            client=pricing_client,
            cohort_size=cohort_size,
            progress=progress,
            before_publish=before_publish,
            context=pricing_context,
        )
        replaced["results"].extend(recovered)
        pricing["results"].extend(replaced["results"])
        pricing["failed"] += replaced["failed"]
        for key, value in replaced["requests"].items():
            pricing["requests"][key] = pricing["requests"].get(key, 0) + value
        if work_policy is not None:
            work_policy.record_results(replaced, stage="history_correction")
            work_policy.accept_correction_completions(replaced["results"])
        selected = [item["symbol"] for item in replaced["results"] if item["status"] == "replaced"]
        lineage_replacements = {}
        publications = {
            item["symbol"]: item
            for item in replaced["results"]
            if "source_sha256" in item
            and (
                (
                    not item.get("correction_completion")
                    and item.get("consistency_check") != "completed_correction"
                )
                or selected_index["symbols"].get(item["symbol"], {}).get("source_sha256")
                != item["source_sha256"]
            )
        }
        if publications:
            lineage_replacements = reconcile_publications(publications)
        else:
            assert_current(store, baseline_token)
        for item in replaced["results"]:
            if item["status"] == "replaced":
                symbol = item["symbol"]
                selected_scope = selected_scopes[symbol]
                if item.get("completed_scope") == "selected_sessions":
                    # The probe may have just created a new correction ID. Compose
                    # its publication with the approval accepted before the probe
                    # for the exact retained source, mapping and target.
                    prior = verified.get(symbol)
                    comparison = context.history_comparisons.get(symbol, ())
                    # A mapped endpoint depends on the old lineage receipt. Its
                    # successor selecting the new pricing generation expires it.
                    routed = (
                        len(comparison) > 3
                        and comparison[3] is not None
                        and prior is not None
                        and comparison[1] == item.get("previous_source_sha256")
                        and verified_scopes[symbol].producer is None
                    )
                    if (
                        item.get("retained_history_unchanged")
                        and prior is not None
                        and generations[symbol] == item["previous_source_sha256"]
                        and prior["asof"] == item["request"]["asof"]
                        and not routed
                        and verified_scopes[symbol].applies(
                            selected_scope.target, selected_scope.acquisition_fingerprint
                        )
                    ):
                        accepted_insertions[symbol] = item["source_sha256"]
                        replacement_scopes[symbol] = selected_scope
                else:
                    completed_replacements[symbol] = item["source_sha256"]
                    replacement_scopes[symbol] = selected_scope
                generations[symbol] = item["source_sha256"]
        # Reconciliation may replace pricing again after observing a changed
        # identity. Only its completed producer handoff approves that generation.
        generations.update(lineage_replacements)
        completed_replacements.update(lineage_replacements)
        for symbol in set(selected) | lineage_replacements.keys():
            verified.pop(symbol, None)
            compared.pop(symbol, None)
            context.history_comparisons.pop(symbol, None)
        return selected

    def accept_publication_snapshot(ready, index):
        current = {entry["symbol"]: entry for entry in ready["entries"]}
        current_generations = {s: item["source_sha256"] for s, item in index["symbols"].items()}
        current_generations.update(
            (symbol, entry["source_sha256"]) for symbol, entry in current.items() if entry.get("source_sha256")
        )
        # Reconciliation may advance dependencies without reporting them as
        # replacements. Invalidate against the refreshed compact selectors,
        # not just the producer's list of explicitly replaced members.
        for symbol in list(compared):
            prior = context.history_comparisons.get(symbol)
            if (
                prior is None
                or prior[:2] != (current.get(symbol), current_generations.get(symbol))
                or not scope_applies(compared_scopes[symbol], symbol)
                or (
                    len(prior) > 3
                    and prior[3] is not None
                    and index["symbols"].get(symbol, {}).get("sha256") != prior[3]
                )
            ):
                compared.pop(symbol)
                compared_scopes.pop(symbol, None)
        # A fresh comparison may bind a recovered pricing source ahead of the
        # lineage index. Only an index change invalidates that generation here;
        # append still checks the selected source under its reacquired lock.
        for symbol, entry in list(verified.items()):
            if (
                not in_scope(entry)
                or not scope_applies(verified_scopes[symbol], symbol)
                or current.get(symbol) != entry
                or (
                    verified_scopes[symbol].producer is not None
                    and index["symbols"].get(symbol, {}).get("source_sha256") != generations.get(symbol)
                )
                or (
                    accepted_generations.get(symbol) != current_generations.get(symbol)
                    and generations.get(symbol, accepted_generations.get(symbol))
                    != current_generations.get(symbol)
                )
            ):
                verified.pop(symbol)
                verified_scopes.pop(symbol, None)
        accepted_generations.clear()
        accepted_generations.update(current_generations)
        # A successful producer correction survives an unrelated failure only
        # after lineage accepted that exact generation. This also replaces any
        # older comparison before late-bar repair rejoins the daily sequence.
        for entry in ready["entries"]:
            symbol = entry["symbol"]
            generation = completed_replacements.get(symbol) or accepted_insertions.get(symbol)
            if (
                in_scope(entry)
                and generation is not None
                and index["symbols"].get(symbol, {}).get("source_sha256") == generation
            ):
                scope = replacement_scopes.get(symbol)
                if scope is not None and scope_applies(scope, symbol):
                    verified[symbol] = dict(entry)
                    verified_scopes[symbol] = scope
                    generations[symbol] = generation
        accept_completed_publications(ready, index)

    late_bar_check = {
        "catalogue_generation": None,
        "inspected_groups": 0,
        "selected_groups": 0,
        "selected_identities": 0,
        "selected_sources": 0,
        "requested_sources": 0,
        "requested_sessions": 0,
        "returned_sources": 0,
        "returned_sessions": 0,
        "stale_sources": [],
        "beyond_target": [],
        "outside_retained_span": 0,
        "requests": {},
    }
    if late_bar_repairs is not None:
        repair_sources = {}
        for symbol, item in late_bar_repairs["sources"].items():
            if symbols is not None and symbol not in symbols:
                continue
            if symbol in excluded_symbols:
                continue
            if work_policy is not None and not work_policy.allowed({symbol}):
                restricted.add(symbol)
                continue
            repair_sources[symbol] = item
        repair_selection = {**late_bar_repairs, "sources": repair_sources}
        prepared, late_bar_check = probe_late_bar_repairs(
            store,
            repair_selection,
            client=pricing_client,
            end_date=end_date,
            progress=progress,
            context=pricing_context,
            on_positive=work_policy.require_correction if work_policy is not None else None,
            correction_requirements=work_policy.work if work_policy is not None else None,
        )
        if prepared:
            replace_and_reconcile(
                prepared,
                snapshot["token"],
                serving_generation=late_bar_repairs["catalogue_generation"],
            )
            if not pricing["failed"] and not lineage["failed"]:
                snapshot = context.select_summary()
                with storage.publication_lock(context.accept_store().root):
                    accept_publication_snapshot(snapshot, context.select_index(context.accept_store().root))
        for failure in late_bar_check["failures"]:
            pricing["failed"] += len(failure["symbols"])
            pricing["results"].extend(
                {**failure, "symbol": symbol, "status": "failed"} for symbol in failure["symbols"]
            )
            if work_policy is not None:
                work_policy.fail(failure["symbols"], failure, stage="late_bar_check")
    for _ in range(100):
        if pricing["failed"] or lineage["failed"]:
            break
        candidates = []
        for entry in snapshot["entries"]:
            symbol = entry["symbol"]
            if symbol in verified and not scope_applies(verified_scopes[symbol], symbol):
                verified.pop(symbol)
                verified_scopes.pop(symbol, None)
            if symbol in compared and not scope_applies(compared_scopes[symbol], symbol):
                compared.pop(symbol)
                compared_scopes.pop(symbol, None)
            if symbols is not None and symbol not in symbols:
                continue
            if symbol in excluded_symbols:
                continue
            if end_date and entry["end_exclusive"][:10] > end_date:
                beyond.add(symbol)
            elif work_policy is not None and not work_policy.allowed({symbol}):
                restricted.add(symbol)
            elif verified.get(symbol) != entry:
                candidates.append(entry)
        if not candidates:
            break
        if work_policy is not None:
            work_policy.require_maintenance(candidates, end_date)
        if register_discoveries is not None:
            register_discoveries(entry["symbol"] for entry in candidates)
        pending = work_policy.work if work_policy else {}
        required = {
            entry["symbol"]
            for entry in candidates
            if (item := pending.get(entry["symbol"]))
            and (
                item.get("required_history_check")
                or item.get("correction_id")
                and not item.get("correction_completion")
            )
        }
        source_selections, recovered = {}, {}

        def accept_selected(root, source, generation):
            symbol = source["symbol"]
            item = pending.get(symbol, {})
            correction_id = item.get("correction_id")
            if correction_id and symbol in required:
                if regular_path(root, ".pending.json").exists():
                    raise DownloadError(f"{symbol}: finish interrupted pricing publication before checking")
                result = recover_selected_correction(
                    root,
                    source,
                    generation,
                    correction_id,
                    completion=work_policy.correction_completion_for(symbol, generation, source),
                    needs_notice=selected_index["symbols"].get(symbol, {}).get("source_sha256") != generation,
                )
                if result is not None:
                    recovered[symbol] = result

        selected_sources = (
            select_append_sources(
                store,
                candidates,
                end_date,
                required_symbols=required,
                selections=source_selections,
                on_selected=accept_selected,
            )
            if on_ready or required
            else {}
        )
        durable_selections, durable_full = {}, {}
        full_obligations, completed_corrections, session_completed = set(), set(), set()
        carried_completions = set()
        for entry in candidates:
            symbol = entry["symbol"]
            work = work_policy.work.get(symbol, {}) if work_policy else {}
            contract = work.get("pricing_correction")
            correction_id = work.get("correction_id")
            publication = recovered.get(symbol)
            if work.get("correction_completion") and not work.get("required_history_check"):
                carried_completions.add(symbol)
                continue
            if contract is not None and (
                contract.get("schema_version") not in (1, 2, 3)
                or contract.get("scope") not in ("latest_session", "full_history")
            ):
                raise DownloadError("unsupported durable correction; owning recovery required")
            if publication:
                if (
                    end_date
                    and entry["end_exclusive"][:10] < end_date
                    and not covers_selected_advance(
                        contract,
                        store=context.store,
                        entry=entry,
                        publication=publication,
                        target=end_date,
                        invalidated=work.get("invalidate_pricing", False),
                    )
                ):
                    # An old completion closes its own correction. A new advance
                    # needs a fresh comparison unless its producer is still held.
                    receipt = publication["correction_completion"]
                    if receipt:
                        same_operation = (
                            work.get("correction_completion") == receipt
                            and contract is not None
                            and work_policy.end == end_date
                            and work_policy.completed_corrections.get(symbol, {}).get("correction_id")
                            != correction_id
                            and not work.get("invalidate_pricing")
                        )
                    else:
                        accepted = pricing_context.full_publications.get(symbol)
                        same_operation = (
                            work_policy.end == end_date
                            and accepted is not None
                            and accepted.applies(
                                context.store,
                                entry,
                                selected_index["symbols"].get(symbol),
                                entry["end_exclusive"],
                                correction_id=correction_id,
                                invalidate_mapping=work.get("invalidate_pricing", False),
                                acquisition_fingerprint=acquisition_fingerprint(work),
                            )
                        )
                    if not same_operation:
                        if receipt and work.get("correction_completion") != receipt:
                            work_policy.accept_correction_completions([publication])
                            work_policy.completed_corrections[symbol] = work_policy.work[symbol]
                        carried_completions.add(symbol)
                        continue
                completed_corrections.add(symbol)
                if publication["completed_scope"] == "selected_sessions":
                    session_completed.add(symbol)
                continue
            generation = selected_sources.get(symbol, accepted_generations.get(symbol))
            source = source_selections.get(symbol, {})
            applicable = (
                applicable_contract(
                    contract,
                    store=store,
                    symbol=symbol,
                    generation=generation,
                    start=source.get("history_start"),
                    end=entry["end_exclusive"],
                    asof=entry["asof"],
                )
                if contract is not None
                else False
            )
            if correction_id and (contract is None or (applicable and contract["scope"] == "full_history")):
                full_obligations.add(symbol)
                if contract is not None:
                    durable_full[symbol] = contract
                continue
            if not applicable or contract.get("scope") != "latest_session" or work.get("invalidate_pricing"):
                continue
            if not contract.get("selection"):
                raise DownloadError("invalid durable latest-session correction; owning recovery required")
            selection = EndpointSelection.from_document(contract["selection"])
            if (
                entry["end_exclusive"] == contract.get("comparison_end_exclusive", contract["end_exclusive"])
                and entry["asof"] == (selection.comparison_asof or selection.asof)
                and selection.applies(
                    store,
                    symbol,
                    generation,
                    contract["end_exclusive"],
                    contract["asof"],
                    lineage_generation=(selected_index["symbols"].get(symbol) or {}).get("sha256"),
                )
            ):
                durable_selections[symbol] = selection
                compared[symbol] = selection.comparison
                scope = requested_scope(symbol)
                compared_scopes[symbol] = scope
                comparison = (dict(entry), generation, selection.comparison)
                if selection.lineage_generation is not None:
                    comparison += (selection.lineage_generation,)
                context.cache_history("comparison", symbol, comparison, scope)
        selected_snapshot = {
            **snapshot,
            "entries": [
                e
                for e in candidates
                if e["symbol"] not in compared
                and e["symbol"] not in full_obligations | (completed_corrections - session_completed)
            ],
        }
        assert_current(store, snapshot["token"])
        if progress:
            progress("checking", {"tickers": len(candidates)})
        # Live check owns remote comparison of this accepted summary snapshot.
        # DO NOT reopen ticker histories to re-prove the stored baseline here.
        selected_scopes = {entry["symbol"]: requested_scope(entry["symbol"]) for entry in candidates}
        with span("live_history_check", symbols=len(selected_snapshot["entries"])) as check_trace:
            from .comparison_routes import route_selector

            select_route = route_selector(context.accept_store().root, selected_index)

            def missing_route(entry, day):
                return select_route(entry["symbol"], day, entry["source_sha256"])

            handoff = verify(selected_snapshot, pricing_client._get, missing_route=missing_route)
            check_trace.update(complete=handoff["complete"], **handoff["stats"], requests=handoff["requests"])
        routed_generations = {}
        for route in handoff["mapped_endpoints"]:
            generation = route.get("lineage_generation")
            prior = routed_generations.setdefault(route["symbol"], generation)
            if generation is None or prior != generation:
                raise DownloadError("mapped endpoint routes changed lineage generation")
        routed_differences = {
            symbol
            for symbol in routed_generations
            if symbol in handoff["comparisons"] and handoff["comparisons"][symbol]["decision"] != "unchanged"
        }
        if routed_differences:
            for symbol in sorted(routed_differences):
                event(
                    "mapped_history_difference_unhandled",
                    symbol=symbol,
                    decision=handoff["comparisons"][symbol]["decision"],
                    lineage_generation=routed_generations[symbol],
                )
                if work_policy is not None:
                    work_policy.fail(
                        {symbol},
                        DownloadError(
                            f"{symbol}: mapped retained-history difference requires route-aware correction"
                        ),
                        stage="history_check",
                    )
            checks.append(handoff)
            error = DownloadError(
                "mapped retained-history difference requires route-aware correction; no replacements started"
            )
            error.work_failure_recorded = work_policy is not None
            raise error
        assert_current(store, snapshot["token"])
        # Live-check completion is per symbol. A peer's missing endpoint or
        # failed request cannot revoke this producer's completed comparison.
        # Retain only the accepted summary and selected generation, without
        # opening validators/payloads or granting unfinished corrections.
        matched = set(handoff["matched"])
        resolved = set()
        if work_policy is not None:
            from .completed_corrections import applicable as completed_comparison_applies

            for symbol, comparison in handoff["comparisons"].items():
                if comparison["decision"] != "full_history":
                    continue
                entry = next(e for e in candidates if e["symbol"] == symbol)
                generation = selected_sources.get(symbol, accepted_generations.get(symbol))
                if completed_comparison_applies(
                    work_policy.completed_corrections.get(symbol),
                    store=context.store,
                    entry=entry,
                    generation=generation,
                    live_all=comparison["live_all"],
                    changed_sessions=comparison["changed_sessions"],
                    added_sessions=comparison["added_sessions"],
                    invalidated=work_policy.work.get(symbol, {}).get("invalidate_pricing", False),
                ):
                    resolved.add(symbol)
                    matched.add(symbol)
                    event(
                        "history_completed_correction_reused",
                        symbol=symbol,
                        source_generation=generation,
                        correction_id=work_policy.completed_corrections[symbol]["correction_id"],
                    )
        differing = {item["symbol"] for item in handoff["differences"]}
        completed = matched | differing
        for difference in handoff["differences"]:
            event(
                "history_endpoint_difference",
                symbol=difference["symbol"],
                endpoint=difference["endpoint"],
                session=difference["session"],
                kind=difference["kind"],
                fields=tuple(difference["fields"]),
                field_values=difference["fields"],
            )
        for entry in candidates:
            symbol = entry["symbol"]
            generation = selected_sources.get(symbol, accepted_generations.get(symbol))
            if symbol in completed:
                event(
                    "history_comparison",
                    symbol=symbol,
                    store=str(store),
                    source_generation=generation,
                    matched=symbol in handoff["matched"],
                    completed_correction_reused=symbol in resolved,
                    summary_token=snapshot["token"],
                    asof=entry["asof"],
                    end_exclusive=entry["end_exclusive"],
                    first_session=entry["first_all"][0],
                    last_session=entry["last_all"][0],
                )
            if symbol in completed and generation is not None:
                decision = HistoryComparison(**handoff["comparisons"][symbol])
                if symbol in resolved:
                    decision = HistoryComparison("unchanged", (), decision.live_all)
                scope = selected_scopes[symbol]
                comparison = (dict(entry), generation, decision)
                if symbol in routed_generations:
                    comparison += (routed_generations[symbol],)
                context.cache_history("comparison", symbol, comparison, scope)
                compared[symbol] = decision
                compared_scopes[symbol] = scope
                if symbol in matched:
                    generations[symbol] = generation
            if (
                symbol in matched
                and generation is not None
                and (
                    symbol in session_completed
                    or symbol in carried_completions
                    or not (work_policy is not None and work_policy.work.get(symbol, {}).get("correction_id"))
                )
            ):
                verified[symbol] = entry
                correction = work_policy.work.get(symbol, {}).get("correction_id") if work_policy else None
                verified_scopes[symbol] = selected_scopes[symbol]
                context.cache_history(
                    "check", symbol, (dict(entry), generation, correction), verified_scopes[symbol]
                )
        checks.append(handoff)
        if progress:
            progress(
                "checked",
                {**handoff["stats"], "requests": handoff["requests"], "seconds": handoff["live_seconds"]},
            )
        if not handoff["complete"]:
            if work_policy is not None:
                for failure in handoff["failures"]:
                    work_policy.fail(failure["symbols"], failure, stage="history_check")
                for symbol in handoff["missing"]:
                    work_policy.fail(
                        [symbol],
                        {
                            "reason": "missing live comparison bar",
                            "missing_endpoints": [
                                item for item in handoff["missing_endpoints"] if item["symbol"] == symbol
                            ],
                        },
                        stage="history_check",
                    )
            error = DownloadError(
                f"live check incomplete: {handoff['stats']['missing']} missing, "
                f"{handoff['stats']['failed']} failed; no replacements started"
            )
            error.work_failure_recorded = work_policy is not None
            raise error
        differing = {
            entry["symbol"]
            for entry in candidates
            if entry["symbol"] in compared and not compared[entry["symbol"]].matched
        }
        required_corrections = {
            entry["symbol"]
            for entry in candidates
            if work_policy is not None
            and work_policy.work.get(entry["symbol"], {}).get("correction_id")
            and entry["symbol"] not in completed_corrections - differing
            and entry["symbol"] not in carried_completions
        }
        prepared = prepare_refresh(
            store,
            differing | required_corrections,
            comparisons={
                symbol: decision
                for symbol, decision in compared.items()
                if symbol in differing | required_corrections
                and symbol not in full_obligations | completed_corrections
            },
            approved_generations={symbol: context.history_comparisons[symbol][1] for symbol in compared},
            selections=durable_selections,
            end_date=end_date if on_ready is not None else None,
            lineage_generations={
                symbol: comparison[3]
                for symbol, comparison in context.history_comparisons.items()
                if len(comparison) > 3
            },
        )
        for item in prepared:
            contract = durable_full.get(item["symbol"])
            if contract is not None:
                if item["source_sha256"] != contract["source_sha256"]:
                    raise DownloadError("pricing changed after durable correction selection")
                item["correction_observations"] = contract.get("observations", {})
                item["correction_cause"] = contract["cause"]
                if "advance_end_exclusive" in contract:
                    item["advance_end_exclusive"] = contract["advance_end_exclusive"]
            elif (
                on_ready is not None
                and end_date
                and item["correction_scope"] == "full_history"
                and item["correction_cause"] == "selected_all_rows_changed"
                and item["end_exclusive"][:10] < end_date
                and item["symbol"] in compared_scopes
                and compared_scopes[item["symbol"]].target == end_date
            ):
                # A full correction replaces only retained sessions. Keep the
                # separately selected append target across a stop between them.
                item["advance_end_exclusive"] = end_date
        if work_policy is not None:
            prepared = [
                item
                for item in prepared
                if item["symbol"] in differing
                or item.get("completed_correction")
                or item["history_correction_id"] != work_policy.work[item["symbol"]]["correction_id"]
            ]
            work_policy.require_correction([item for item in prepared if not item.get("completed_correction")])
        assert_current(store, snapshot["token"])
        recovered_results = [
            recovered[symbol] for symbol in sorted(completed_corrections - session_completed - differing)
        ]
        selected = replace_and_reconcile(prepared, snapshot["token"], recovered=recovered_results)
        if pricing["failed"] or lineage["failed"]:
            break
        ready = context.select_summary()
        current = {item["symbol"]: item for item in ready["entries"]}
        for entry in candidates:
            symbol = entry["symbol"]
            scope = (
                replacement_scopes.get(symbol)
                if symbol in selected
                else verified_scopes.get(symbol) or compared_scopes.get(symbol)
            )
            compared.pop(symbol, None)
            if current.get(symbol) == entry or symbol in selected:
                if symbol in current:
                    if scope is None:
                        raise DownloadError("completed history publication lacks its request-scope handoff")
                    verified[symbol] = dict(current[symbol])
                    verified_scopes[symbol] = scope
                    if symbol in selected_sources and symbol not in selected:
                        generations[symbol] = selected_sources[symbol]
        if prepared or recovered_results:
            with storage.publication_lock(context.accept_store().root):
                accept_publication_snapshot(ready, context.select_index(context.accept_store().root))
        snapshot = ready
        # Refresh membership only after publication. New members get a fresh comparison;
        # changing membership cannot promote an unchecked source into approval.
    else:
        raise DownloadError("history-check membership did not settle")
    try:
        if pricing["failed"] or lineage["failed"]:
            ready = context.select_summary()
        else:
            assert_current(store, snapshot["token"])
            ready = snapshot
        summary = describe(ready)
    except DownloadError as error:
        summary = {"error": str(error)}
        if work_policy is not None:
            work_policy.command_failures.append({"stage": "summary", "error": str(error)})
    if "error" not in summary:
        with storage.publication_lock(context.accept_store().root):
            index = context.select_index(context.accept_store().root)
        # A legacy tail publication changes the pricing selector, not its rows.
        # Finish that selected lineage handoff even if an independent correction
        # failed. Carry the publication; never reacquire or reconstruct its tail.
        pending_upgrades = {
            entry["symbol"]: upgraded[entry["symbol"]]
            for entry in ready["entries"]
            if entry["symbol"] in upgraded
            and verified.get(entry["symbol"]) == entry
            and generations.get(entry["symbol"]) == upgraded[entry["symbol"]]["source_sha256"]
            and index["symbols"].get(entry["symbol"], {}).get("source_sha256")
            != upgraded[entry["symbol"]]["source_sha256"]
            and in_scope(entry)
        }
        if pending_upgrades:
            replacements = reconcile_publications(pending_upgrades)
            generations.update(replacements)
            completed_replacements.update(replacements)
            ready = context.select_summary()
            summary = describe(ready)
            with storage.publication_lock(context.accept_store().root):
                index = context.select_index(context.accept_store().root)
        accept_publication_snapshot(ready, index)
        for entry in ready["entries"]:
            symbol = entry["symbol"]
            generation = index["symbols"].get(symbol, {}).get("source_sha256")
            if in_scope(entry) and verified.get(symbol) == entry and generation is not None:
                # A covered source needs no append manifest read. Its completed
                # index generation and checked summary are sufficient here.
                if generations.get(symbol, accepted_generations.get(symbol)) == generation:
                    correction = work_policy.work.get(symbol, {}).get("correction_id") if work_policy else None
                    context.cache_history(
                        "check", symbol, (dict(entry), generation, correction), verified_scopes[symbol]
                    )
    failed = pricing["failed"] + lineage["failed"] + int("error" in summary)
    approved_entries = (
        [e for e in ready["entries"] if in_scope(e) and verified.get(e["symbol"]) == e]
        if "error" not in summary
        else []
    )
    if "error" not in summary and on_ready is not None and (not failed or approved_entries):
        on_ready(
            {
                **ready,
                "entries": approved_entries,
                "approved_generations": generations,
                "approved_scopes": {
                    entry["symbol"]: verified_scopes[entry["symbol"]] for entry in approved_entries
                },
            }
        )
    if "error" not in summary and work_policy is not None:
        # A completed pricing replacement approves its generation even when
        # lineage removes the now-inactive symbol from the active summary.
        approved_symbols = {entry["symbol"] for entry in approved_entries}
        approved = {
            symbol: generation
            for symbol, generation in generations.items()
            if work_policy.allowed({symbol})
            and index["symbols"].get(symbol, {}).get("source_sha256") == generation
            and (
                symbol in approved_symbols
                or completed_replacements.get(symbol) == generation
                or accepted_insertions.get(symbol) == generation
            )
        }
        work_policy.history_approved.update(approved)
        # Standalone refresh owns its final lineage handoff too. Daily maintenance
        # supplies on_ready and stamps the checked generation after its append.
        finish = [
            s
            for s in approved
            if s in work_policy.work
            and work_policy.work[s].get("required_history_check")
            and work_policy.allowed({s})
            and s not in work_policy.completed_decisions
        ]
        if on_ready is None and finish:
            from libs.alpaca_pricing_download.update_checks import read_checks

            pending = read_checks(store)["checks"]
            finish = [
                symbol
                for symbol in finish
                if work_policy.satisfying_entry(symbol, index["symbols"], pending) is None
            ]
        if on_ready is None and finish:
            value = run_lineage(
                store,
                finish,
                identity_client=identity_client,
                pricing_client=pricing_client,
                end_date=end_date,
                evidence_client=evidence_client,
                register_discoveries=register_discoveries,
                work_policy=work_policy,
                context=context,
                progress=progress,
                on_pricing=progress,
            )
            lineage["results"].extend(value["results"])
            lineage["failed"] += value["failed"]
            failed += value["failed"]
    stats, requests = {}, {}
    for check in checks:
        for key, value in check["stats"].items():
            stats[key] = stats.get(key, 0) + value
        for key, value in check["requests"].items():
            requests[key] = requests.get(key, 0) + value
    return {
        "complete": failed == 0,
        "failed": failed,
        "check": {"stats": stats, "requests": requests, "seconds": sum(c["live_seconds"] for c in checks)},
        "late_bar_check": late_bar_check,
        "pricing": pricing,
        "lineage": lineage,
        "summary": summary,
        "already_beyond_target": sorted(beyond),
        "deferred_symbols": sorted(restricted),
    }

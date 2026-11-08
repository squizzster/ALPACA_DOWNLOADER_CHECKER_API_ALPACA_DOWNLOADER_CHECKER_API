"""Single-worker dependency closure and the downloader/lineage handoff."""

from pathlib import Path

from libs.alpaca_pricing_download.acquire import MeasuredRequests, acquire_selected
from libs.alpaca_pricing_download.contract import (
    HISTORY_START,
    InitialPricingUnavailable,
    failure_outcome,
    symbol_label,
)
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source, read_update, regular_path
from libs.alpaca_pricing_download.update_checks import (
    acknowledge_checks,
    begin_checks,
    complete_checks,
    read_checks,
)
from libs.common.debug_trace import event, traced

from . import storage
from .acquisition import check_unavailable_pricing, unavailable_mapping_current, unavailable_pricing
from .assignment_handoff import selected as assignment_handoff
from .certificate_migration import can_promote_group
from .context import accept_context
from .continuation import identity_intervals, identity_memberships, retained_report
from .contract import (
    MAX_ACQUISITION_WAVES,
    MAX_CLOSURE,
    VERSION,
    LineageError,
    reusable_computation,
    reusable_decision,
    reusable_metadata_versions,
    versions,
)
from .correction_prefix import complete_prefix_boundaries, correction_from
from .metadata import (
    EvidenceClient,
    RetainedReportClient,
    acquire,
    acquire_unpriced,
    action_requests,
    dependencies,
    full_metadata,
)
from .migration import can_promote_observation_decision
from .observations import select_observations
from .pricing import load_pricing
from .resume import (
    append_history_invalidation,
    complete_append,
    requires_lineage,
    reusable_prefix,
    unchanged_source,
)
from .selection import DependencyGraph
from .transform import correlate, make_publication, retained_decision_report
from .work_contract import acquisition_fingerprint, auxiliary_work, pricing_target

PREFETCH_MEMBERS = 1000


def closure(index, seeds, asset_ids=(), *, graph=None):
    return (graph or DependencyGraph(index["symbols"])).closure(seeds, asset_ids)


@traced("pricing_selectors")
def _current_sources(store, locked, *, known=()):
    result = {}
    for symbol in sorted(locked):
        root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        if regular_path(root, ".pending.json").exists():
            raise LineageError(f"{symbol}: pricing publication incomplete; resume downloader first")
        source = load_source(root, symbol)
        if source is None and symbol in known:
            raise LineageError(f"{symbol}: published pricing receipt is missing")
        if source is not None:
            result[symbol] = source
    return result


def _reuse_metadata(seed, group, selected, previous, notices, refresh):
    # This is the complete-group reuse boundary: changed inputs, recorded
    # absence, pending notices or explicit refresh defeat reuse. On success,
    # DO NOT reacquire evidence merely because a later stage wants reassurance.
    unavailable = unavailable_pricing(previous.get(seed))
    return (
        not refresh
        and seed in selected
        and not notices
        and set(group) - selected.keys() == unavailable.keys()
        and all(
            prior is not None
            and prior["source_sha256"] == selected[symbol]
            and prior["input_sources"] == selected
            and unavailable_pricing(prior) == unavailable
            and reusable_metadata_versions(prior["versions"])
            for symbol, prior in previous.items()
        )
    )


def _published_pricing_ranges(index, group, pricing_handoff):
    """Plan from completed publications and the accepted lineage projection."""
    # Producer-returned requests and indexed coverage are already authoritative
    # for planning. DO NOT reopen every group's manifests to derive the same API
    # need; the execution owner still binds selected work under its pricing locks.
    if pricing_handoff is None:
        return None
    sources = {}
    for symbol in sorted(group):
        entry = index["symbols"].get(symbol)
        published = pricing_handoff.get(symbol)
        if entry is None and published is None:
            continue
        prior = (entry or {}).get("projection", {}).get("pricing_request")
        request = (published or {}).get("request") or prior
        if request is None:
            return None
        history_start = (published or {}).get("history_start") or (prior or {}).get("history_start")
        if published and published.get("mode") in {"initial", "full_refresh"}:
            history_start = request["start"]
        if history_start is None:
            return None
        sources[symbol] = {
            "history_start": history_start,
            "latest_request": request,
            **{
                key: published[key]
                for key in ("latest_session_correction", "history_changes")
                if published and key in published
            },
        }
    return sources


def _handoff_added_rows(publication):
    added = (publication or {}).get("added_rows")
    if not isinstance(added, dict) or not added:
        return None
    if any(type(value) is not int or value < 0 for value in added.values()):
        return None
    return any(added.values())


def _select_pending_pricing(store, members, index, pending, pricing_handoff):
    """Recover compact planning facts only for selected unfinished publications.

    There is no in-memory producer result after interruption. Select that work's
    current receipt once for planning; execution reselects under its writer locks.
    """
    selected = (set(members) & set(pending)) - pricing_handoff.keys()
    if not selected:
        return
    with storage.pricing_locks(store, selected, known=index["symbols"]) as locked:
        for symbol, source in _current_sources(store, locked, known=index["symbols"]).items():
            notice = read_update(store / "end_of_day/pricing" / symbol, published=True)
            pricing_handoff[symbol] = {
                **{
                    key: source[key] for key in ("latest_session_correction", "history_changes") if key in source
                },
                "source_sha256": storage.document_digest(source),
                "history_start": source.get("history_start", HISTORY_START),
                "request": source["latest_request"],
                "changed": notice is not None,
                "changed_from": notice["from"] if notice is not None else None,
                "pending_update": notice,
            }


@traced("metadata_prefetch")
def _prefetch_metadata(
    store,
    selected,
    index,
    graph,
    client,
    refresh,
    receipt_cache,
    end_date=None,
    work_policy=None,
    pricing_handoff=None,
):
    """Collect selected receipt ranges without opening prices or scanning directories.

    Publication still reselects pricing under its normal locks. Requests are
    cached by exact symbol/date bounds, so changed ranges cannot use old results.
    """
    if not client.supports_plans:
        return
    visited, requests, lookup_symbols = set(), set(), set()
    for label in selected:
        try:
            seed = symbol_label(label)
            if seed in visited:
                continue
            group = closure(index, {seed}, graph=graph)
            visited.update(group)
            sources = _published_pricing_ranges(index, group, pricing_handoff)
            if sources is None:
                with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
                    sources = _current_sources(store, locked, known=index["symbols"])
                    notices = {
                        s: notice
                        for s in sources
                        if (notice := read_update(store / "end_of_day/pricing" / s, published=True)) is not None
                    }
                    generations = {s: storage.document_digest(source) for s, source in sources.items()}
            else:
                notices = {}
                for symbol in sources:
                    publication = pricing_handoff.get(symbol, {})
                    notice = publication.get("pending_update")
                    if "pending_update" not in publication and publication.get("changed"):
                        notice = {
                            "from": publication.get("changed_from") or publication["request"]["start"][:10]
                        }
                    if notice is not None:
                        notices[symbol] = notice
                generations = {
                    symbol: pricing_handoff[symbol]["source_sha256"]
                    if symbol in pricing_handoff
                    else index["symbols"][symbol]["source_sha256"]
                    for symbol in sources
                }
            if end_date and any(
                source["latest_request"]["end_exclusive"][:10] > end_date for source in sources.values()
            ):
                continue
            previous = {s: receipt_cache.read(s) for s in sources}
            metadata_refresh = refresh or (
                work_policy is not None and work_policy.needs_refresh(group, previous)
            )
            history_refresh = refresh or (
                work_policy is not None and work_policy.needs_full_metadata(group, previous)
            )
            if _reuse_metadata(seed, group, generations, previous, notices, metadata_refresh):
                continue
            for symbol, source in sources.items():
                full = full_metadata(
                    previous[symbol],
                    None if correction_from(source, previous[symbol]) else notices.get(symbol),
                    history_refresh,
                )
                selected_requests = action_requests(
                    symbol,
                    source.get("history_start", HISTORY_START)[:10],
                    source["latest_request"]["end_exclusive"][:10],
                    previous[symbol],
                    refresh=full,
                )
                requests.update(selected_requests)
                added = _handoff_added_rows((pricing_handoff or {}).get(symbol))
                if full or previous[symbol] is None:
                    lookup_symbols.update(request[0] for request in selected_requests)
                elif added is not False and previous[symbol]["source_sha256"] != generations[symbol]:
                    # A source with new rows needs a current own-label observation.
                    # Historical queried labels remain covered by their completed
                    # report unless a newly returned action selects them on demand.
                    lookup_symbols.add(symbol)
                event(
                    "metadata_lookup_selection",
                    symbol=symbol,
                    selected=symbol in lookup_symbols,
                    added_rows=added,
                    source_changed=previous[symbol] is None
                    or previous[symbol]["source_sha256"] != generations[symbol],
                    historical_refresh=full,
                )
        except (RuntimeError, OSError, ValueError):
            continue  # The normal group path reports this failure without stopping unrelated work.
    client.prefetch_corporate_actions(requests)
    client.prefetch_symbols(lookup_symbols)


@traced("dependency_catchup")
def _catch_up_published_dependencies(
    store,
    selected,
    index,
    graph,
    client,
    progress=None,
    end_date=None,
    work_policy=None,
    register_discoveries=None,
    context=None,
    pricing_handoff=None,
    empty_suffixes=None,
):
    """The pricing owner validates retained history before advancing dependencies."""
    result = {
        "requested": 0,
        "succeeded": 0,
        "failed": 0,
        "results": [],
        "requests": {},
        "deferred_full_range": 0,
        "skipped": 0,
    }
    if client is None or not callable(getattr(client, "execute_plan", None)):
        return result
    visited, targets = set(), {}
    for label in selected:
        group = graph.closure({label})
        if group <= visited or (work_policy is not None and not work_policy.allowed(group)):
            continue
        visited.update(group)
        sources = _published_pricing_ranges(index, group, pricing_handoff)
        if sources is None:
            with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
                sources = _current_sources(store, locked, known=index["symbols"])
        if not sources:
            continue
        start = min(source.get("history_start", HISTORY_START)[:10] for source in sources.values())
        end = max(source["latest_request"]["end_exclusive"][:10] for source in sources.values())
        if end_date and end > end_date:
            continue
        for symbol, source in sources.items():
            if source.get("history_start", HISTORY_START)[:10] > start:
                result["deferred_full_range"] += 1
            elif source["latest_request"]["end_exclusive"][:10] < end:
                targets.setdefault(end, []).append(symbol)
    for end, symbols in targets.items():
        allowed = [s for s in symbols if work_policy is None or work_policy.allowed({s})]
        if not allowed:
            continue
        if register_discoveries is not None:
            # Group membership is already selected by the accepted lineage index.
            # Register it before any provider request; suffix absence can suppress
            # acquisition work, but it cannot erase the known dependency label.
            register_discoveries({member for symbol in allowed for member in graph.closure({symbol})})

        def persist_selected(selected_work):
            if work_policy is not None:
                for symbol in sorted(selected_work):
                    work_policy.require_dependencies(graph.closure({symbol}), end, {symbol})

        value = acquire_selected(
            store,
            allowed,
            end,
            client=client,
            progress=progress,
            context=context.pricing() if context is not None else None,
            select_append_work=True,
            on_append_work_selected=persist_selected,
        )
        result["results"].extend(value["results"])
        not_selected = {item["symbol"] for item in value["results"] if item.get("status") == "not_selected"}
        if empty_suffixes is not None:
            empty_suffixes.update(
                (
                    item["symbol"],
                    item["source_sha256"],
                    item["append_work_selection"]["end_exclusive"][:10],
                )
                for item in value["results"]
                if item.get("status") == "not_selected"
            )
        result["skipped"] += len(not_selected)
        if pricing_handoff is not None:
            pricing_handoff.update(
                (item["symbol"], item)
                for item in value["results"]
                if item.get("status") != "not_selected" and "source_sha256" in item and "request" in item
            )
        for key, count in value["requests"].items():
            if isinstance(count, dict):
                for metric, total in count.items():
                    result["requests"][metric] = result["requests"].get(metric, 0) + total
            else:
                result["requests"][key] = result["requests"].get(key, 0) + count
        result["requested"] += len(allowed)
        result["failed"] += value["failed"]
        if work_policy is not None:
            work_policy.record_results(value, stage="dependency_acquisition")
    result["succeeded"] = result["requested"] - result["failed"]
    return result


@traced("lineage_group")
def process_group(
    store,
    root,
    seed,
    *,
    identity_client,
    pricing_client=None,
    refresh=False,
    index=None,
    observer=None,
    graph=None,
    evidence_client=None,
    receipt_cache=None,
    end_date=None,
    register_discoveries=None,
    work_policy=None,
    context=None,
    progress=None,
    empty_suffixes=(),
    retained_rebuild=False,
):
    context = accept_context(store, context, work_policy)
    correlation_policy = context.accept_correlation_policy()
    index = storage.read_index(root) if index is None else index
    receipt_cache = receipt_cache or storage.ReceiptCache(root, index)
    graph = work_policy.graph if work_policy is not None else graph or DependencyGraph(index["symbols"])
    group = closure(index, {seed}, graph=graph)
    if (
        seed not in index["symbols"]
        and work_policy is not None
        and auxiliary_work(work_policy.work.get(seed, {}))
    ):
        seed = min(group.intersection(index["symbols"]), default=seed)
    acquisition_waves = 0
    pricing_replacements = {}
    metadata_cache = {}
    pricing_inputs = {}
    partitions = {}
    assignment_boundaries = {}

    def read_boundaries(prior, kinds=None):
        selected_kinds = prior["boundaries"] if kinds is None else kinds
        missing = {
            kind
            for kind in selected_kinds
            if (prior["symbol"], kind, prior["boundaries"][kind]["sha256"]) not in assignment_boundaries
        }
        if missing:
            for kind, document in storage.read_boundaries(root, prior, kinds=missing).items():
                assignment_boundaries[prior["symbol"], kind, prior["boundaries"][kind]["sha256"]] = document
        return {
            kind: assignment_boundaries[prior["symbol"], kind, prior["boundaries"][kind]["sha256"]]
            for kind in selected_kinds
        }

    unavailable = {}
    evidence_client = evidence_client or EvidenceClient(identity_client)
    for _ in range(MAX_CLOSURE + MAX_ACQUISITION_WAVES + 1):
        required = None
        if work_policy is not None:
            work_policy.current_group = set(group)
            work_policy.check(group)
        with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
            checks = read_checks(store, locked)
            sources = _current_sources(store, locked, known=index["symbols"])
            # A missing seed is missing pricing, just like a related label. With
            # no published range, None delegates defaults to the downloader.
            starts = [source.get("history_start", HISTORY_START)[:10] for source in sources.values()]
            if work_policy is not None:
                starts.extend(
                    work_policy.work[symbol]["pricing_start"]
                    for symbol in group
                    if work_policy.work.get(symbol, {}).get("pricing_start")
                )
            start = min(
                starts,
                default=HISTORY_START[:10] if end_date else None,
            )
            end = max(
                (source["latest_request"]["end_exclusive"][:10] for source in sources.values()), default=end_date
            )
            # Reload after delegation: concurrent publication outranks a remembered
            # rejection. Never turn a held source into an unavailable auxiliary.
            unavailable = {
                name: value
                for name, value in unavailable.items()
                if name not in sources
                and value["request"]["start"][:10] == start
                and value["request"]["end_exclusive"][:10] == end
                and unavailable_mapping_current(value, evidence_client)
            }
            previous = {symbol: receipt_cache.read(symbol) for symbol in sorted(sources)}
            metadata_refresh = refresh or (
                work_policy is not None and work_policy.needs_refresh(group, previous)
            )
            history_refresh = refresh or (
                work_policy is not None and work_policy.needs_full_metadata(group, previous)
            )
            notices = {
                symbol: notice
                for symbol in sorted(sources)
                if (notice := read_update(store / "end_of_day/pricing" / symbol, published=True)) is not None
            }
            selected = {name: storage.document_digest(source) for name, source in sources.items()}
            prior_unavailable = unavailable_pricing(previous.get(seed))
            if retained_rebuild:
                if (
                    notices
                    or not previous
                    or any(
                        prior is None
                        or not reusable_metadata_versions(prior["versions"])
                        or prior["source_sha256"] != selected[name]
                        or prior["input_sources"] != selected
                        or unavailable_pricing(prior) != prior_unavailable
                        for name, prior in previous.items()
                    )
                    or set(group) != set(sources) | set(prior_unavailable)
                    or any(
                        "evidence" not in item or "request" not in item for item in prior_unavailable.values()
                    )
                ):
                    raise LineageError(
                        "retained lineage rebuild requires coherent completed inputs; finish owning recovery first"
                    )
                unavailable = dict(prior_unavailable)
            retained_evidence = [prior["evidence"] for prior in previous.values() if prior] + [
                item["evidence"] for item in prior_unavailable.values() if "evidence" in item
            ]
            evidence_client.retain_event_supplements(retained_evidence)
            changed_event_supplements = not retained_rebuild and any(
                evidence_client.supplements_for(facts) != facts.get("event_supplements", [])
                for facts in retained_evidence
            )
            from .metadata import DEPENDENCY_HANDOFF_CORRELATION_VERSION

            if not retained_rebuild and any(
                prior and prior["versions"]["correlation"] < DEPENDENCY_HANDOFF_CORRELATION_VERSION
                for prior in previous.values()
            ):
                evidence_client.prepare_retained(
                    [prior["evidence"] for prior in previous.values() if prior]
                    + [item["evidence"] for item in prior_unavailable.values() if "evidence" in item]
                )
            reuse_metadata = _reuse_metadata(seed, group, selected, previous, notices, metadata_refresh)
            metadata_current = reuse_metadata and (
                retained_rebuild
                or all(evidence_client.evidence_current(prior["evidence"]) for prior in previous.values())
            )
            computation_current = all(reusable_computation(prior) for prior in previous.values())
            # Absence belongs to its own request/mapping, not its siblings' price
            # envelopes. The locked source selection has already established that
            # no source or pending publication supersedes it.
            if not refresh and not retained_rebuild:
                unavailable.update(
                    {
                        name: value
                        for name, value in prior_unavailable.items()
                        if name in group
                        and name not in sources
                        and value["request"]["start"][:10] == start
                        and value["request"]["end_exclusive"][:10] == end
                        and value["request"]["asof"] == end
                        and unavailable_mapping_current(value, evidence_client)
                        and not (work_policy is not None and work_policy.needs_full_metadata({name}, previous))
                    }
                )
            if prior_unavailable.keys() != unavailable.keys():
                reuse_metadata = False
            missing = group - sources.keys() - unavailable.keys()
            if work_policy is not None and missing:
                work_policy.require_dependencies(group, end, missing, start=start)
                work_policy.check(group)
            if register_discoveries is not None:
                register_discoveries(group)
            promoted_decision = (
                reuse_metadata
                and metadata_current
                and computation_current
                and not changed_event_supplements
                and can_promote_group(previous, group)
                and all(
                    evidence_client.evidence_current(item["evidence"])
                    for item in prior_unavailable.values()
                    if "evidence" in item
                )
            )
            if (
                reuse_metadata
                and metadata_current
                and computation_current
                and not changed_event_supplements
                and (
                    promoted_decision
                    or all(
                        reusable_decision(prior) or can_promote_observation_decision(prior, correlation_policy)
                        for prior in previous.values()
                    )
                )
                and (work_policy is None or work_policy.decision_inputs_satisfied(sources, selected))
            ):
                event("lineage_required", symbol=seed, required=False, reason="accepted_unchanged_inputs")
                receipts = {
                    symbol: {
                        **prior,
                        "schema_version": VERSION,
                        "versions": versions(),
                        "correlation_policy": correlation_policy.provenance(),
                    }
                    for symbol, prior in previous.items()
                }
                if work_policy is not None:
                    completed = work_policy.stamp(receipts, sources)
                # DO NOT restore publication for workflow acknowledgement alone.
                # stamp() has checked the selected inputs under their owning locks;
                # its approval belongs in durable work state, not a new data
                # generation that forces serving to reopen unchanged assignments.
                # Only these acknowledgement fields are excluded: evidence,
                # pricing, ownership, versions and policy still require publication.
                acknowledgements = {"update_obligation_id", "evidence_requirements", "unavailable_obligations"}
                published = any(
                    {k: v for k, v in receipt.items() if k not in acknowledgements}
                    != {k: v for k, v in previous[symbol].items() if k not in acknowledgements}
                    for symbol, receipt in receipts.items()
                )
                if not published:
                    receipts = previous
                if published:
                    if progress:
                        progress("lineage_publication", {"symbols": sorted(receipts), "reused_decision": True})
                    storage.publish(
                        store,
                        root,
                        index,
                        receipts,
                        {s: {} for s in receipts},
                        {},
                        checks=checks,
                        previous=previous,
                        sources=sources,
                        unchanged_decision=promoted_decision,
                    )
                    receipt_cache.published(receipts)
                    if work_policy is not None:
                        work_policy.update_index({s: index["symbols"][s] for s in receipts})
                if work_policy is not None:
                    work_policy.accept_completion(receipts, index["symbols"], completed)
                if observer is not None:
                    observer(
                        store=store,
                        receipts=receipts,
                        boundaries={
                            name: storage.read_boundaries(root, item) for name, item in previous.items()
                        },
                        inputs=None,
                        reports=None,
                        notices={},
                        resumed=False,
                    )
                from .check_summary import refresh as refresh_check_summary

                if not published:
                    refresh_check_summary(store, previous, sources=sources, unchanged=True)
                    acknowledge_checks(store, checks, sources)
                return {
                    "symbol": seed,
                    "status": "processed" if published else "unchanged",
                    "reused_decision": True,
                    "pricing_rows_processed": 0,
                    "historical_rows_reused": sum(item["source_rows"] for item in previous.values()),
                    "processed": sorted(sources),
                    "unavailable_pricing": sorted(unavailable),
                    "pricing_replacements": pricing_replacements,
                }
            if end_date and end and end > end_date:
                raise LineageError(
                    f"{seed}: required group reconciliation extends beyond requested cutoff {end_date}; "
                    "retained pricing and lineage remain unchanged"
                )
            required = {
                symbol: {"from": start, "end_exclusive": end}
                for symbol in sorted(group)
                if symbol not in sources and symbol not in unavailable
            }
            if not required:
                # One decision for this selected source/receipt under these locks.
                # None is a completed negative result too; consumers retain it.
                corrections = {
                    symbol: correction_from(source, previous[symbol]) for symbol, source in sources.items()
                }
                unchanged = (
                    not refresh
                    and not changed_event_supplements
                    and group == sources.keys() | unavailable.keys()
                    and all(
                        unchanged_source(source, previous[symbol], selected=selected[symbol])
                        for symbol, source in sources.items()
                    )
                )
                resume = unchanged or (
                    not changed_event_supplements
                    and group == sources.keys() | unavailable.keys()
                    and all(
                        reusable_prefix(
                            source,
                            previous[symbol],
                            selected=selected[symbol],
                            refresh=refresh,
                            selected_correction=corrections[symbol],
                        )
                        for symbol, source in sources.items()
                    )
                )
                event(
                    "lineage_reuse_gate",
                    symbol=seed,
                    members=len(group),
                    mode="unchanged" if unchanged else "prefix" if resume else "full",
                    reason="accepted_unchanged_inputs"
                    if unchanged
                    else "accepted_pricing_prefix"
                    if resume
                    else "changed_or_unproven_inputs",
                )
                retained_kinds = {}
                if not resume:
                    corrections = dict.fromkeys(sources)
                projections = {}
                if resume and any(corrections.values()):

                    def identities_at(symbol, day):
                        prior = previous[symbol]
                        summary = prior.get("assignment_summary", {}).get("raw", {})
                        if not summary.get("last_session") or summary["last_session"] < day:
                            return set()
                        documents = {}
                        if "identity_intervals" not in prior:
                            anchor = prior["anchor_month"]
                            kind = (
                                "current_month"
                                if day[:7] == anchor
                                else "current_year"
                                if day[:4] == anchor[:4]
                                else "completed_years"
                            )
                            documents = read_boundaries(prior, kinds={kind})
                        return {
                            item["security_id"]
                            for item in identity_intervals(prior, documents)
                            if item["from"] <= day <= item["to"]
                        }

                    affected = {}
                    for name, day in corrections.items():
                        if day:
                            affected.setdefault(day, set()).update(identities_at(name, day))
                    for name in sources:
                        days = [
                            day for day, identities in affected.items() if identities_at(name, day) & identities
                        ]
                        if days:
                            projections[name] = min(days)
                for symbol, prior in previous.items():
                    retained_kinds[symbol] = None
                    if not resume and prior:
                        retained_kinds[symbol] = {
                            kind for kind in prior["boundaries"] if assignment_handoff(prior, kind) is None
                        }
                    if resume and "assignment_summary" in prior:
                        old_anchor, anchor = prior["anchor_month"], sources[symbol]["anchor_month"]
                        if sources[symbol]["latest_request"]["end_exclusive"] <= prior["processed_through"]:
                            retained_kinds[symbol] = set()
                        elif old_anchor == anchor:
                            retained_kinds[symbol] = set() if unchanged else {"current_month"}
                        elif old_anchor[:4] == anchor[:4]:
                            retained_kinds[symbol] = {"current_year", "current_month"}
                        if corrections[symbol] and retained_kinds[symbol] is not None:
                            day = corrections[symbol]
                            retained_kinds[symbol].add(
                                "current_month"
                                if day[:7] == old_anchor
                                else "current_year"
                                if day[:4] == old_anchor[:4]
                                else "completed_years"
                            )
                        if symbol in projections and retained_kinds[symbol] is not None:
                            day = projections[symbol]
                            retained_kinds[symbol].add("current_month")
                            if day[:7] < old_anchor:
                                retained_kinds[symbol].add("current_year")
                            if day[:4] < old_anchor[:4]:
                                retained_kinds[symbol].add("completed_years")
                old_boundaries = {
                    symbol: (
                        read_boundaries(prior, kinds=retained_kinds[symbol])
                        if prior and (resume or prior.get("schema_version") == VERSION)
                        else None
                    )
                    for symbol, prior in previous.items()
                }
                for symbol, day in corrections.items():
                    if day or symbol in projections:
                        complete_prefix_boundaries(
                            root, previous[symbol], old_boundaries[symbol], projections.get(symbol) or day
                        )
                inputs = {}
                for symbol in sources:
                    partitions.setdefault(symbol, {})
                if progress:
                    progress(
                        "lineage_inputs",
                        {
                            "symbols": sorted(sources),
                            "active_only": resume,
                            "from": previous[seed]["processed_through"][:10] if resume else start,
                            "scope": "added_sessions" if resume else "complete_group",
                        },
                    )
                for symbol in sorted(sources):
                    scope = (
                        selected[symbol],
                        resume,
                        previous[symbol]["boundaries"] if resume else None,
                        projections.get(symbol),
                    )
                    cached = pricing_inputs.get(symbol)
                    if cached is None or cached[0] != scope:
                        cached = (
                            scope,
                            load_pricing(
                                store,
                                symbol,
                                source=sources[symbol],
                                source_sha256=selected[symbol],
                                selected_correction=corrections[symbol],
                                previous_boundaries=old_boundaries[symbol],
                                resume_from=(
                                    projections.get(symbol)
                                    or corrections[symbol]
                                    or previous[symbol]["processed_through"][:10]
                                )
                                if resume
                                else None,
                                previous=previous[symbol],
                                partitions=partitions[symbol],
                                projection_from=projections.get(symbol),
                            ),
                        )
                        pricing_inputs[symbol] = cached
                    inputs[symbol] = cached[1]
                if progress:
                    progress("lineage_evidence", {"symbols": sorted(group), "reused": reuse_metadata})
                for _metadata_round in range(MAX_CLOSURE + 1):
                    evidence, deps, assets = {}, {}, set()
                    for symbol, view in inputs.items():
                        prior = previous[symbol]
                        if retained_rebuild:
                            evidence[symbol] = prior["evidence"]
                        elif reuse_metadata and evidence_client.evidence_current(prior["evidence"]):
                            # Only the downstream rules changed. Consume the approved
                            # evidence directly, then recalculate the complete group.
                            evidence[symbol] = prior["evidence"]
                        else:
                            full = full_metadata(
                                prior, None if corrections[symbol] else notices.get(symbol), history_refresh
                            )
                            key = (symbol, view.source_sha256, full)
                            if key not in metadata_cache or not evidence_client.evidence_current(
                                metadata_cache[key]
                            ):
                                can_continue = (
                                    not full
                                    and prior is not None
                                    and evidence_client.catalogue_attempted
                                    and evidence_client.catalogue is not None
                                )
                                metadata_cache[key] = acquire(
                                    view,
                                    RetainedReportClient(evidence_client, prior["evidence"])
                                    if reuse_metadata or can_continue
                                    else evidence_client,
                                    prior,
                                    refresh=full,
                                )
                            evidence[symbol] = metadata_cache[key]
                        deps[symbol] = (
                            prior["dependencies"] if retained_rebuild else dependencies(evidence[symbol], symbol)
                        )
                        assets.update(item["id"] for item in evidence[symbol]["identities"] if item.get("id"))
                    for symbol, missing in sorted(unavailable.items()):
                        # Prices and metadata have different provider endpoints. Keep
                        # discovering real relationships even when bars reject a label.
                        if not retained_rebuild and (
                            "evidence" not in missing
                            or not evidence_client.evidence_current(missing["evidence"])
                        ):
                            missing["evidence"] = acquire_unpriced(
                                symbol,
                                missing["request"],
                                evidence_client,
                                previous=missing.get("evidence") if reuse_metadata else None,
                            )
                        auxiliary = missing["evidence"]
                        deps[symbol] = dependencies(auxiliary, symbol)
                        assets.update(item["id"] for item in auxiliary["identities"] if item.get("id"))
                    # A later member may acquire an exact mapping that supersedes
                    # an earlier retained observation. Reassemble only stale new
                    # reports from the in-memory producer cache, under these locks.
                    if retained_rebuild or all(
                        evidence_client.evidence_current(facts)
                        for facts in [*evidence.values(), *(item["evidence"] for item in unavailable.values())]
                    ):
                        break
                else:
                    raise LineageError("selected metadata observations did not become coherent")
                expanded = closure(
                    index, group.union(*(set(values) for values in deps.values())), assets, graph=graph
                )
                if expanded != group:
                    if retained_rebuild:
                        raise LineageError(
                            "retained lineage rebuild evidence exceeds its accepted complete group"
                        )
                    from libs.common.work_scope import accept_expansion

                    accept_expansion(store, expanded, anchors=group, authority="metadata_dependencies")
                    if progress:
                        progress(
                            "dependency_expansion",
                            {"symbols": sorted(expanded), "added": sorted(expanded - group)},
                        )
                    if work_policy is not None:
                        work_policy.require_dependencies(
                            expanded, end, expanded - sources.keys() - unavailable.keys(), start=start
                        )
                        work_policy.reconcile_evidence(
                            evidence, sources, previous, group=expanded, target=end, partial=True
                        )
                    group = expanded
                    continue  # Release every pricing lock before taking an enlarged, sorted set.
                replacements = (
                    work_policy.reconcile_evidence(
                        evidence | {s: value["evidence"] for s, value in unavailable.items()},
                        sources,
                        previous,
                        group=group,
                        target=end,
                    )
                    if work_policy is not None
                    else set()
                )
                for symbol in replacements:
                    required[symbol] = {
                        "from": start,
                        "end_exclusive": max(end, pricing_target(work_policy.work[symbol])),
                        "refresh_all": True,
                        "selected_source_sha256": selected[symbol],
                        "acquisition_evidence_sha256": acquisition_fingerprint(work_policy.work[symbol]),
                    }
                # A related input may exist but not cover the selected acquisition range.
                # Empty published responses count as acquired, not as missing history.
                for symbol, source in () if retained_rebuild else sources.items():
                    if source.get("history_start", HISTORY_START)[:10] > start:
                        required.setdefault(symbol, {"from": start, "end_exclusive": end})
                    elif source["latest_request"]["end_exclusive"][:10] < end:
                        accepted_empty = (symbol, selected[symbol], end) in empty_suffixes
                        event(
                            "dependency_suffix_selection",
                            symbol=symbol,
                            required=not accepted_empty,
                            reason="completed_empty_suffix" if accepted_empty else "selection_unavailable",
                            end_exclusive=end,
                        )
                        if not accepted_empty:
                            required.setdefault(symbol, {"from": start, "end_exclusive": end})
                if not required:
                    check_unavailable_pricing(store, unavailable, locked, sources=sources)
                    historical_lineage = requires_lineage(
                        previous,
                        evidence,
                        pricing_unchanged=resume
                        and (unchanged or all(complete_append(view) for view in inputs.values())),
                        refresh=refresh,
                        append=resume,
                    )
                    append_invalidation = (
                        append_history_invalidation(previous, inputs)
                        if resume and not historical_lineage
                        else None
                    )
                    if append_invalidation is not None:
                        event(
                            "lineage_append_invalidation",
                            symbol=seed,
                            scope="complete_group",
                            **append_invalidation,
                        )
                        historical_lineage = True
                    no_new_rows = not historical_lineage and not any(
                        rows for view in inputs.values() for rows in view.views.values()
                    )
                    event(
                        "lineage_required",
                        symbol=seed,
                        required=not no_new_rows,
                        retained_history_required=historical_lineage,
                        scope="complete_group"
                        if historical_lineage
                        else "none"
                        if no_new_rows
                        else "added_sessions",
                        retained_rows=sum(item["source_rows"] for item in previous.values() if item),
                        reason="changed_or_unproven_inputs"
                        if historical_lineage
                        else "accepted_unchanged_inputs"
                        if no_new_rows
                        else "accepted_prefix_new_sessions",
                    )
                    if historical_lineage and resume:
                        # Evidence changed after pricing alone proved reusable.
                        # Announce selected owner work before reading its bodies.
                        event(
                            "lineage_work_selected", symbol=seed, symbols=sorted(inputs), scope="complete_group"
                        )
                        for name, prior in previous.items():
                            missing = {
                                kind
                                for kind in prior["boundaries"].keys() - old_boundaries[name].keys()
                                if assignment_handoff(prior, kind) is None
                            }
                            old_boundaries[name].update(read_boundaries(prior, kinds=missing))
                        inputs = {
                            name: load_pricing(
                                store,
                                name,
                                source=source,
                                partitions=partitions[name],
                                source_sha256=selected[name],
                                selected_correction=None,
                            )
                            for name, source in sources.items()
                        }
                    resume = not historical_lineage
                    if resume and not no_new_rows:
                        for name, prior in previous.items():
                            last = prior.get("assignment_summary", {}).get("raw", {}).get("last_session")
                            if "identity_memberships" not in prior and last:
                                anchor = prior["anchor_month"]
                                kind = (
                                    "current_month"
                                    if last[:7] == anchor
                                    else "current_year"
                                    if last[:4] == anchor[:4]
                                    else "completed_years"
                                )
                                if kind not in old_boundaries[name]:
                                    old_boundaries[name].update(read_boundaries(prior, kinds={kind}))
                        membership = identity_memberships(previous, old_boundaries)
                        for name, view in inputs.items():
                            view.accepted_identity_memberships = {
                                **membership,
                                **identity_memberships({name: previous[name]}, {name: old_boundaries[name]}),
                            }
                            if name in projections:
                                view.accepted_report = retained_report(
                                    previous[name],
                                    old_boundaries[name],
                                    view,
                                    unchanged_before=corrections[name] or previous[name]["processed_through"],
                                )
                    if progress:
                        progress(
                            "lineage_decision_reuse" if no_new_rows else "lineage_correlation",
                            {"symbols": sorted(inputs)},
                        )
                    # Retiring publications determine the locked work scope;
                    # current evidence determines the replacement components.
                    # Correlate each replacement with exactly the inputs its
                    # receipt will select, then publish all of them together.
                    current_graph = DependencyGraph(
                        {
                            symbol: {
                                "dependencies": set(deps[symbol]) | unavailable.keys(),
                                "asset_ids": [item["id"] for item in facts["identities"] if item.get("id")],
                            }
                            for symbol, facts in evidence.items()
                        }
                    )

                    def correlate_components(selected_inputs, *, retain=False):
                        selected_components, selected_reports = {}, {}
                        for name in sorted(selected_inputs):
                            if name in selected_components:
                                continue
                            component = current_graph.closure({name})
                            members = component & selected_inputs.keys()
                            selected_components.update(dict.fromkeys(members, component))
                            selected_reports.update(
                                {
                                    member: retained_decision_report(previous[member], policy=correlation_policy)
                                    for member in members
                                }
                                if retain
                                else correlate(
                                    store,
                                    {member: selected_inputs[member] for member in members},
                                    {member: evidence[member] for member in members},
                                    policy=correlation_policy,
                                    selected_group=sorted(component),
                                    unavailable_pricing=sorted(component & unavailable.keys()),
                                    evidence_generations={
                                        member: storage.document_digest(evidence[member]) for member in members
                                    },
                                    pricing_generations={
                                        member: selected_inputs[member].source_sha256 for member in members
                                    },
                                )
                            )
                        return selected_components, selected_reports

                    components, reports = correlate_components(inputs, retain=no_new_rows)
                    # DO NOT REMOVE WITHOUT USER AUTH!#
                    # The complete group establishes one numerical source choice
                    # for all views. A required mapping correction must publish
                    # through pricing first; never stamp the old input as done.
                    if not no_new_rows:
                        reports, mapping_corrections = select_observations(inputs, reports)
                        required.update(mapping_corrections)
                    if not required:
                        receipts, boundaries = {}, {}
                        for symbol, view in inputs.items():
                            prior = previous[symbol]
                            old = old_boundaries[symbol]
                            receipts[symbol], boundaries[symbol] = make_publication(
                                view,
                                evidence[symbol],
                                reports[symbol],
                                sorted(set(deps[symbol]) | unavailable.keys()),
                                prior,
                                old,
                                resume_closed=resume,
                            )
                            receipts[symbol]["input_sources"] = {
                                name: inputs[name].source_sha256
                                for name in sorted(components[symbol] & inputs.keys())
                            }
                            if unavailable:
                                receipts[symbol]["unavailable_pricing"] = unavailable
                        if work_policy is not None:
                            completed = work_policy.stamp(receipts, sources)
                        if progress:
                            progress(
                                "lineage_publication",
                                {"symbols": sorted(receipts), "reused_decision": no_new_rows},
                            )
                        storage.publish(
                            store,
                            root,
                            index,
                            receipts,
                            boundaries,
                            notices,
                            checks=checks,
                            previous=previous,
                            sources=sources,
                        )
                        receipt_cache.published(receipts)
                        if work_policy is not None:
                            work_policy.update_index({symbol: index["symbols"][symbol] for symbol in receipts})
                            work_policy.accept_completion(receipts, index["symbols"], completed)
                        else:
                            graph.update({symbol: index["symbols"][symbol] for symbol in receipts})
                        # Inspection observes the actual publication while the same
                        # locks still protect its inputs. It never runs another resolver.
                        if observer is not None:
                            observed_boundaries = {
                                symbol: {
                                    **storage.read_boundaries(
                                        root,
                                        receipt,
                                        kinds=receipt["boundaries"].keys() - boundaries[symbol].keys(),
                                    ),
                                    **boundaries[symbol],
                                }
                                for symbol, receipt in receipts.items()
                            }
                            observer(
                                store=store,
                                receipts=receipts,
                                boundaries=observed_boundaries,
                                inputs=inputs,
                                reports=None if no_new_rows else reports,
                                notices=notices,
                                resumed=resume,
                            )
                        return {
                            "symbol": seed,
                            "status": "processed",
                            "processed": sorted(sources),
                            "acknowledged": sorted(notices),
                            "unresolved_rows": sum(item["unresolved_rows"] for item in receipts.values()),
                            "pricing_acquisition_waves": acquisition_waves,
                            "reused_decision": no_new_rows,
                            "reused_closed_boundaries": resume,
                            "historical_rows_reused": sum(
                                item["rows"]
                                for view in inputs.values()
                                for item in view.previous_assignment_summary.values()
                            )
                            if resume
                            else 0,
                            "pricing_rows_processed": sum(
                                len(rows) for view in inputs.values() for rows in view.views.values()
                            ),
                            "unavailable_pricing": sorted(unavailable),
                            "pricing_replacements": pricing_replacements,
                        }
        # Deliberately outside ALL pricing locks: the downloader owns acquisition,
        # endpoint checks, replacement, and the resulting pending notice.
        if required:
            if work_policy is not None:
                work_policy.require_dependencies(group, end, required, start=start)
                work_policy.check(group)
            if pricing_client is None:
                return {"symbol": seed, "status": "waiting_for_pricing", "required": required, "processed": []}
            if acquisition_waves >= MAX_ACQUISITION_WAVES:
                raise LineageError("pricing dependencies did not settle within three acquisition waves")
            acquisition_waves += 1
            if progress:
                progress("dependency_acquisition", {"wave": acquisition_waves, "required": required})
            for symbol, request in required.items():
                if work_policy is not None:
                    work_policy.check(group)
                    fingerprint = acquisition_fingerprint(work_policy.work[symbol])
                    if symbol not in sources and fingerprint:
                        request.update(selected_source_sha256=None, acquisition_evidence_sha256=fingerprint)
                try:
                    acquired = download_pricing(
                        store,
                        symbol,
                        request["end_exclusive"],
                        client=pricing_client,
                        from_date=request["from"],
                        context=context.pricing() if context is not None else None,
                        **{k: v for k, v in request.items() if k not in {"from", "end_exclusive"}},
                    )
                    if request.get("refresh_all"):
                        pricing_replacements[symbol] = acquired["source_sha256"]
                        if work_policy is not None:
                            work_policy.history_approved[symbol] = acquired["source_sha256"]
                    if progress:
                        progress("dependency_pricing", {"symbol": symbol, "state": "completed", **acquired})
                except InitialPricingUnavailable as error:
                    auxiliary = (
                        symbol != seed
                        and bool(sources)
                        and symbol not in sources
                        and symbol not in index["symbols"]
                        and (work_policy is None or auxiliary_work(work_policy.work.get(symbol, {})))
                    )
                    if not auxiliary:
                        if work_policy is not None:
                            work_policy.fail({symbol}, error, stage="dependency_acquisition")
                        raise
                    # This is a provider requestability fact, not a successful empty
                    # price response or permission to discard an identity candidate.
                    unavailable[symbol] = error.evidence
                    if progress:
                        progress("dependency_pricing", {"symbol": symbol, "state": "unavailable"})
    raise LineageError("lineage dependency discovery did not settle")


def local_symbols(store: Path, *, pending: bool, known=()) -> list[str]:
    # Explicit discovery/reconstruction only. Normal selected updates use compact
    # membership/hints; DO NOT insert this census before useful steady-state work.
    from libs.common.work_scope import forbid_inventory

    forbid_inventory(store)
    directory = regular_path(store, "end_of_day/manifests" if pending else "end_of_day/pricing")
    if not directory.exists():
        return []
    result = []
    for path in sorted(directory.iterdir()):
        if path.name in known:
            result.append(path.name)
            continue  # The index already establishes this publication; no path inventory.
        if path.is_symlink() or not path.is_dir() or symbol_label(path.name) != path.name:
            raise LineageError(f"invalid ticker directory: {path}")
        filename = "update.json" if pending else "source.manifest.json"
        if regular_path(path, filename).exists():
            result.append(path.name)
    return result


def _source_generation_changes(store, index, *, skip=(), on_scanned=None):
    """Restore lost hint coverage from atomic source receipts, without lineage reads.

    A pricing publisher queues work before replacing its source receipt and repeats
    that queue if the hint database is recreated. A replacement racing this census
    therefore remains queued even when this pass observes the older generation.
    """
    changed = []
    skip = set(skip)
    candidates = [(symbol, entry) for symbol, entry in sorted(index["symbols"].items()) if symbol not in skip]
    for count, (symbol, entry) in enumerate(candidates, 1):
        root = regular_path(store, f"end_of_day/pricing/{symbol}", published=True)
        source = load_source(root, symbol)
        if source is None:
            raise LineageError(f"{symbol}: published pricing receipt is missing")
        if storage.document_digest(source) != entry.get("source_sha256"):
            changed.append(symbol)
        if on_scanned is not None and (count % 1000 == 0 or count == len(candidates)):
            on_scanned(count, len(candidates))
    return changed


def _cohorts(selected, graph, limit=PREFETCH_MEMBERS):
    """Keep each prefetch cache within one provider-sized dependency cohort."""
    cohort, members = [], set()
    for label in selected:
        try:
            group = graph.closure({symbol_label(label)})
        except (RuntimeError, ValueError):
            group = {label}  # The normal group path will report the invalid label.
        if cohort and len(members | group) > limit:
            yield cohort
            cohort, members = [], set()
        cohort.append(label)
        members.update(group)
    if cohort:
        yield cohort


@traced("lineage")
def run_lineage(
    store,
    symbols=None,
    *,
    identity_client,
    pricing_client=None,
    refresh=False,
    observer=None,
    on_processed=None,
    on_scanned=None,
    on_pricing=None,
    known_pending=(),
    evidence_client=None,
    end_date=None,
    register_discoveries=None,
    work_policy=None,
    context=None,
    progress=None,
    pricing_handoff=None,
    event_supplements=(),
    retained_rebuild=False,
):
    if retained_rebuild and (symbols is None or refresh or pricing_client is not None or event_supplements):
        raise LineageError(
            "retained lineage rebuild requires explicit symbols and no provider acquisition or refresh"
        )
    if event_supplements and symbols is None:
        raise LineageError("event supplements require explicit selected symbols")
    if event_supplements:
        from libs.alpaca_security_lineage.event_supplements import validate_supplements

        event_supplements = validate_supplements(list(event_supplements))
    context = accept_context(store, context, work_policy)
    metadata_before = identity_client.request_count
    pricing_metrics = {}
    if pricing_client is not None and callable(getattr(pricing_client, "execute_plan", None)):
        pricing_client = MeasuredRequests(pricing_client, pricing_metrics)
    store = Path(store).absolute()
    results, processed = [], set()
    with storage.writer_lock(store, accepted_store=context.accept_store()) as root:
        recovered = context.recover_lineage(root)
        context.accept_correlation_policy()
        with storage.publication_lock(root):
            index = context.select_index(root)
        checks = read_checks(store)
        if checks["epoch"] is None:
            checks = begin_checks(store)
        full_scan = symbols is None and not checks["complete"]
        stale = (
            {symbol for symbol, entry in index["symbols"].items() if not reusable_decision(entry)}
            if symbols is None
            else set()
        )
        stale_metadata = {
            symbol
            for symbol in stale
            if not reusable_metadata_versions(index["symbols"][symbol].get("versions"))
        }
        prefetch = set()
        if symbols is not None:
            selected = list(dict.fromkeys(symbols))
            prefetch.update(selected)
        else:
            # The list is disposable: only a completed full scan establishes its
            # coverage. Thereafter all/pending use hints and the existing index,
            # not thousands of individual source/lineage receipts.
            selected = list(checks["checks"])
            prefetch.update(checks["checks"])
            if full_scan:
                local = set(local_symbols(store, pending=False, known=index["symbols"]))
                notices = set(local_symbols(store, pending=True, known=index["symbols"]))
                discovered = local - index["symbols"].keys()
                selected.extend(sorted(discovered | notices | stale))
                prefetch.update(discovered | notices)
            elif refresh:
                selected.extend(index["symbols"])
                prefetch.update(index["symbols"])
        # A changed rule/calendar version invalidates processed inputs even without
        # a new pricing notice. Default pending mode picks up that explicit work.
        if symbols is None:
            selected.extend(sorted(stale))
            prefetch.update(stale_metadata)
        selected = list(dict.fromkeys([*selected, *known_pending]))
        if work_policy is not None and symbols is None:
            # Default standalone work includes every accepted obligation. Bounded
            # recovery callers must pass their explicit pending symbols; otherwise
            # ordinary next-session obligations become premature lineage work.
            selected = list(dict.fromkeys([*selected, *work_policy.work]))
            prefetch.update(work_policy.work)
        if work_policy is not None:
            # Start a durable dependency group from a published member. A missing
            # auxiliary selected first would otherwise be mistaken for an
            # independently requested, unpriceable seed.
            selected.sort(
                key=lambda label: (
                    label not in index["symbols"],
                    auxiliary_work(work_policy.work.get(label, {})),
                    label,
                )
            )
        prefetch.update(known_pending)
        if work_policy is not None:
            work_policy.index = index
            work_policy.context = context
            work_policy.update_index(index["symbols"])
        graph = work_policy.graph if work_policy is not None else DependencyGraph(index["symbols"])
        if progress:
            visited, groups = set(), 0
            for symbol in selected:
                if symbol not in visited:
                    visited.update(graph.closure({symbol}))
                    groups += 1
            progress("lineage_selection", {"symbols": len(selected), "groups": groups})
        if evidence_client is not None and event_supplements:
            raise LineageError(
                "supply event supplements when constructing the evidence client or the run, not both"
            )
        evidence_client = evidence_client or EvidenceClient(identity_client, event_supplements=event_supplements)
        if work_policy is not None:
            evidence_client.work_policy = work_policy
            for label in selected:
                if refresh and work_policy.allowed({label}):
                    for member in graph.closure({label}):
                        projection = index["symbols"].get(member, {}).get("projection") or {}
                        target = projection.get("processed_through", end_date)[:10]
                        work_policy.require(member, target, reason="explicit_lineage_refresh", refresh=True)
        stop = False

        catchups = []
        empty_suffixes = set()

        def catch_up(labels, handoff):
            nonlocal stop
            if retained_rebuild:
                return
            value = _catch_up_published_dependencies(
                store,
                labels,
                index,
                graph,
                pricing_client,
                progress=on_pricing,
                end_date=end_date,
                work_policy=work_policy,
                register_discoveries=register_discoveries,
                context=context,
                pricing_handoff=handoff,
                empty_suffixes=empty_suffixes,
            )
            catchups.append(value)
            if value["failed"]:
                results.extend(
                    {
                        "symbol": item["symbol"],
                        "status": "failed",
                        "error": item.get("error", "dependency pricing catch-up failed"),
                        "outcome": item.get("outcome", {}),
                    }
                    for item in value["results"]
                    if item["status"] in {"failed", "blocked"}
                )
                # Independent groups can still finish; the policy blocks failed closures.
                stop = work_policy is None

        def process_selected(labels, planned):
            nonlocal stop
            for cohort in _cohorts(labels, graph):
                cohort = [label for label in cohort if label not in processed]
                if not cohort:
                    continue
                members = set().union(
                    *(
                        graph.closure({label})
                        for label in cohort
                        if work_policy is None or work_policy.allowed(graph.closure({label}))
                    )
                )
                handoff = {
                    symbol: pricing_handoff[symbol] for symbol in members if symbol in (pricing_handoff or {})
                }
                if not retained_rebuild and (
                    evidence_client.supports_plans or callable(getattr(pricing_client, "execute_plan", None))
                ):
                    _select_pending_pricing(
                        store, members, index, members if full_scan else checks["checks"], handoff
                    )
                catch_up(cohort, handoff)
                if stop:
                    break
                receipt_cache = storage.ReceiptCache(root, index, capacity=PREFETCH_MEMBERS)
                requests = [
                    label
                    for label in cohort
                    if label in planned
                    and label not in processed
                    and (work_policy is None or work_policy.allowed(graph.closure({label})))
                ]
                if requests and not retained_rebuild:
                    prefetch_before = identity_client.request_count
                    if progress:
                        progress("metadata_prefetch", {"state": "started", "selected": len(requests)})
                    _prefetch_metadata(
                        store,
                        requests,
                        index,
                        graph,
                        evidence_client,
                        refresh,
                        receipt_cache,
                        end_date,
                        work_policy=work_policy,
                        pricing_handoff=handoff,
                    )
                    if progress:
                        progress(
                            "metadata_prefetch",
                            {"state": "completed", "requests": identity_client.request_count - prefetch_before},
                        )
                for label in cohort:
                    try:
                        symbol = symbol_label(label)
                        if symbol in processed:
                            continue
                        result = process_group(
                            store,
                            root,
                            symbol,
                            identity_client=identity_client,
                            pricing_client=pricing_client,
                            refresh=refresh,
                            index=index,
                            observer=observer,
                            graph=graph,
                            evidence_client=evidence_client,
                            receipt_cache=receipt_cache,
                            end_date=end_date,
                            register_discoveries=register_discoveries,
                            work_policy=work_policy,
                            context=context,
                            progress=progress,
                            empty_suffixes=empty_suffixes,
                            retained_rebuild=retained_rebuild,
                        )
                        processed.update(result["processed"])
                        processed.update(result.get("unavailable_pricing", ()))
                        if on_processed is not None and result["processed"]:
                            on_processed(result["processed"])
                    except (RuntimeError, OSError, ValueError) as error:
                        affected = sorted(
                            work_policy.current_group if work_policy is not None else graph.closure({label})
                        )
                        if work_policy is not None:
                            work_policy.fail(affected, error, stage="lineage")
                        result = {
                            "affected": affected,
                            "symbol": label,
                            "status": "failed",
                            "error": str(error),
                            "outcome": failure_outcome(error),
                        }
                    results.append(result)
                    if progress:
                        progress(
                            "lineage_group",
                            {
                                "completed": len(processed),
                                "selected": len(selected),
                                **result,
                            },
                        )
                    if result["status"] == "failed" and storage.path_at(root, ".pending.json").exists():
                        stop = True
                        break  # Recovery must finish before another group can publish.
                if stop:
                    break

        if not stop:
            process_selected(selected, prefetch)

        failed = any(item["status"] in {"failed", "waiting_for_pricing"} for item in results)
        if full_scan and not stop and not failed:
            changed = _source_generation_changes(
                store,
                index,
                skip=processed,
                on_scanned=on_scanned,
            )
            if not stop:
                process_selected(changed, set(changed))
        if full_scan and not any(item["status"] in {"failed", "waiting_for_pricing"} for item in results):
            complete_checks(store, checks["epoch"])
    request_metrics = {}
    for value in catchups:
        for name, count in value["requests"].items():
            request_metrics[name] = request_metrics.get(name, 0) + count
    dependency_pricing = {
        name: sum(value.get(name, 0) for value in catchups)
        for name in ("requested", "succeeded", "failed", "skipped", "deferred_full_range")
    }
    dependency_pricing["requests"] = request_metrics
    if pricing_metrics:
        dependency_pricing["requests"] = pricing_metrics
    if progress:
        progress(
            "lineage_complete",
            {
                "processed": len(processed),
                "groups": len(results),
                "reused_decisions": sum(bool(item.get("reused_decision")) for item in results),
                "metadata_requests": identity_client.request_count - metadata_before,
                "pricing_requests": dependency_pricing["requests"],
            },
        )
    result = {
        "workers": 1,
        "recovered": recovered,
        "dependency_pricing": dependency_pricing,
        "results": results,
        "failed": sum(item["status"] in {"failed", "waiting_for_pricing"} for item in results),
    }
    if event_supplements:
        unmatched = sorted(evidence_client.event_supplements.keys() - evidence_client.applied_event_supplements)
        result["unmatched_event_supplements"] = unmatched
        result["failed"] += bool(unmatched)
        if unmatched and work_policy is not None:
            work_policy.command_failures.append({"stage": "lineage", "unmatched_event_supplements": unmatched})
    return result

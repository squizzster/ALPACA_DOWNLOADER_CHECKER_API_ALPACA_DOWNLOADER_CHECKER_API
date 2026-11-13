"""Complete-group lineage execution and its single publication consumer.

The engine owns dependency closure, selective computation and forward acquisition
waves. Execution collaborators are supplied by the controller; they carry no
accepted domain state. All such state remains in the explicit operation context,
selected inputs and completed reports.
"""

from dataclasses import dataclass
from typing import Callable

from libs.alpaca_pricing_download.contract import (
    HISTORY_START,
    InitialPricingUnavailable,
)
from libs.alpaca_pricing_download.update_checks import (
    acknowledge_checks,
)
from libs.common.debug_trace import traced

from . import storage
from .acquisition import check_unavailable_pricing, unavailable_mapping_current, unavailable_pricing
from .assignment_handoff import selected as assignment_handoff
from .certificate_migration import can_promote_group
from .context import accept_context
from .continuation import identity_intervals, retained_report
from .contract import (
    MAX_ACQUISITION_WAVES,
    MAX_CLOSURE,
    VERSION,
    LineageError,
    current_decision,
    retained_v78_decision,
    retained_v79_decision,
    retained_v80_decision,
    retained_v81_decision,
    retained_v82_decision,
    retained_v83_decision,
    retained_v84_decision,
    retained_v85_decision,
    retained_v86_decision,
    retained_v87_decision,
    retained_v88_decision,
    retained_v89_decision,
    retained_v90_decision,
    reusable_computation,
    reusable_metadata_versions,
    versions,
)
from .correction_prefix import complete_prefix_boundaries
from .metadata import (
    EvidenceClient,
    RetainedReportClient,
    dependencies,
    full_metadata,
)
from .observation_carry import project_choices
from .pricing import metadata_source_control
from .resume import (
    append_history_invalidation,
    complete_append,
    requires_lineage,
    source_reuse,
)
from .selection import DependencyGraph
from .source_work import execute_source_work, retained_ownership_report
from .transform import make_publication, retained_decision_report
from .work_contract import acquisition_fingerprint, auxiliary_work, pricing_target


@dataclass(frozen=True)
class GroupServices:
    """Execution call sites, kept injectable at the existing controller boundary."""

    current_sources: Callable
    reuse_metadata: Callable
    acquire: Callable
    acquire_unpriced: Callable
    can_promote_observation_decision: Callable
    closure: Callable
    correction_from: Callable
    correlate: Callable
    download_pricing: Callable
    event: Callable
    finish_correlated_group: Callable
    load_pricing: Callable
    prepare_group_evidence_and_source_work: Callable
    read_checks: Callable
    read_update: Callable
    scoped_identity_memberships: Callable
    select_observations: Callable


@dataclass(frozen=True)
class CompletedGroupPublication:
    """Newly constructed publication and producer-completed work measurements.

    This record lends completed decisions to the one publication consumer. It
    does not reopen inputs or repeat correlation, numerical choice or ownership.
    """

    receipts: dict
    boundaries: dict
    reused_decision: bool
    reused_closed_boundaries: bool
    historical_rows_reused: int
    pricing_rows_processed: int
    membership_refinement: dict | None = None


def consume_membership_plan(context, completion):
    """Acknowledge only the exact transient plan this successful group consumed."""
    selected = completion.get("accepted_membership_plan")
    if selected is not None:
        key, accepted = selected
        if context.completed_membership_plans.get(key) is accepted:
            del context.completed_membership_plans[key]


def release_completed_assemblies(context, store, members):
    """Release superseded assembly roots after the entire selected group commits.

    Enlarged groups lend the same generation-keyed buffers forward. Once their
    final consumer succeeds, subset roots have no remaining assembly consumer.
    Unrelated failures and completed-publication records remain untouched.
    """
    members = set(members)
    for key in tuple(context.group_completions):
        if (
            isinstance(key, tuple)
            and len(key) >= 3
            and key[0] == "assembly"
            and key[1] == str(store)
            and set(key[2]) <= members
        ):
            del context.group_completions[key]


def publish_completed_group(
    publication,
    *,
    acquisition_waves,
    checks,
    graph,
    index,
    notices,
    previous,
    pricing_replacements,
    receipt_cache,
    root,
    seed,
    sources,
    store,
    unavailable,
    work_policy,
    inputs=None,
    reports=None,
    observer=None,
    progress=None,
    completion=None,
):
    """Publish once, then consume its exact index and acknowledgement handoffs."""
    receipts, boundaries = publication.receipts, publication.boundaries
    effects = {} if completion is None else completion.setdefault("effects", {})
    if work_policy is not None and "stamp" not in effects:
        effects["stamp"] = work_policy.stamp(receipts, sources)
    if progress and not effects.get("progress"):
        progress(
            "lineage_publication",
            {"symbols": sorted(receipts), "reused_decision": publication.reused_decision},
        )
        effects["progress"] = True
    if "published" not in effects:
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
        effects["published"] = {symbol: index["symbols"][symbol] for symbol in receipts}
    if not effects.get("receipt_cache"):
        receipt_cache.published(receipts)
        effects["receipt_cache"] = True
    if work_policy is not None:
        if not effects.get("index"):
            work_policy.update_index(effects["published"])
            effects["index"] = True
        if not effects.get("acknowledged"):
            work_policy.accept_completion(receipts, index["symbols"], effects["stamp"])
            effects["acknowledged"] = True
    elif not effects.get("index"):
        graph.update(effects["published"])
        effects["index"] = True
    # Inspection observes the actual publication while the same
    # locks still protect its inputs. It never runs another resolver.
    if observer is not None and not effects.get("observed"):
        if "observed_boundaries" not in effects:
            effects["observed_boundaries"] = {
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
            boundaries=effects["observed_boundaries"],
            inputs=inputs,
            reports=None if publication.reused_decision else reports,
            notices=notices,
            resumed=publication.reused_closed_boundaries,
        )
        effects["observed"] = True
    result = {
        "symbol": seed,
        "status": "processed",
        "processed": sorted(sources),
        "acknowledged": sorted(notices),
        "unresolved_rows": sum(item["unresolved_rows"] for item in receipts.values()),
        "pricing_acquisition_waves": acquisition_waves,
        "reused_decision": publication.reused_decision,
        "reused_closed_boundaries": publication.reused_closed_boundaries,
        "historical_rows_reused": publication.historical_rows_reused,
        "pricing_rows_processed": publication.pricing_rows_processed,
        "unavailable_pricing": sorted(unavailable),
        "pricing_replacements": pricing_replacements,
    }
    if publication.membership_refinement is not None:
        result["membership_refinement"] = {
            name: value
            for name, value in publication.membership_refinement.items()
            if name not in {"input_evidence", "input_receipts"}
        }
    return result


@traced("lineage_finish_correlated_group")
def finish_correlated_group(
    *,
    services,
    reports,
    acquisition_waves,
    affected,
    assignment_boundaries,
    checks,
    choice_prefix_sources,
    components,
    deps,
    evidence,
    graph,
    index,
    inputs,
    no_new_rows,
    notices,
    observer,
    old_boundaries,
    original_choice_inputs,
    previous,
    pricing_replacements,
    progress,
    read_boundaries,
    receipt_cache,
    required,
    resume,
    retained_recovery,
    root,
    seed,
    sources,
    store,
    unavailable,
    work_policy,
    completion=None,
):
    """Consume completed correlation and retained choices under the owning locks.

    Callers retain the exact selected inputs and metadata, and establish current
    selector applicability under the normal writer/pricing locks. No upstream
    acquisition, materialization or correlation belongs in this consumer.
    """
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # The complete group establishes one numerical source choice
    # for all views. A required mapping correction must publish
    # through pricing first; never stamp the old input as done.
    completion = {} if completion is None else completion
    if "numerical" in completion:
        reports, mapping_corrections = completion["numerical"]
        required.update(mapping_corrections)
    elif not no_new_rows:
        retained_choices = {}
        for name, prior in choice_prefix_sources.items():

            def choice_boundary(kind, *, prior=prior):
                key = prior["symbol"], kind, prior["boundaries"][kind]["sha256"]
                if key not in assignment_boundaries:
                    handoff = prior.get("assignment_handoffs", {}).get(kind)
                    if handoff is None:
                        read_boundaries(prior, kinds={kind})
                    else:
                        assignment_boundaries[key] = storage.read_assignment_projection(
                            root, prior, kind, handoff
                        )
                return assignment_boundaries[key]

            retained_choices[name] = project_choices(
                prior,
                inputs[name],
                receipt_generation=index["symbols"][name]["sha256"],
                legacy_boundary=choice_boundary,
                invalidated=affected,
                original_view=original_choice_inputs[name],
            )
            reports[name]["retained_observation_routes"] = prior.get("observation_routes", {})
        reports, mapping_corrections = services.select_observations(
            inputs,
            reports,
            retained_choices=retained_choices,
            invalidated_scopes=affected,
            instant_keys=completion.setdefault("instant_keys", {}),
        )
        completion["numerical"] = reports, dict(mapping_corrections)
        required.update(mapping_corrections)
    if not required:
        receipts, boundaries = {}, {}
        parts = completion.setdefault("parts", {})
        for symbol, view in inputs.items():
            prior = previous[symbol]
            old = old_boundaries[symbol]
            if symbol not in parts:
                parts[symbol] = make_publication(
                    view,
                    evidence[symbol],
                    reports[symbol],
                    sorted(set(deps[symbol]) | unavailable.keys()),
                    prior,
                    old,
                    resume_closed=resume,
                )
            receipts[symbol], boundaries[symbol] = parts[symbol]
            receipts[symbol]["input_sources"] = {
                name: inputs[name].source_sha256 for name in sorted(components[symbol] & inputs.keys())
            }
            if unavailable:
                receipts[symbol]["unavailable_pricing"] = unavailable
            if retained_recovery is not None:
                if (
                    receipts[symbol]["input_sources"] != retained_recovery.pricing
                    or tuple(sorted(components[symbol])) != retained_recovery.members
                ):
                    raise LineageError("retained recovery changed its selected group")
                receipts[symbol]["retained_recovery"] = retained_recovery.completion()
        if "publication" not in completion:
            publication = CompletedGroupPublication(
                receipts,
                boundaries,
                no_new_rows,
                resume,
                sum(
                    item["rows"]
                    for view in inputs.values()
                    if resume
                    for item in view.previous_assignment_summary.values()
                ),
                sum(len(rows) for view in inputs.values() for rows in view.views.values()),
                membership_refinement=completion.get("membership_refinement"),
            )
            completion["publication"] = publication.__dict__
        return publish_completed_group(
            CompletedGroupPublication(**completion["publication"]),
            acquisition_waves=acquisition_waves,
            checks=checks,
            graph=graph,
            index=index,
            inputs=inputs,
            notices=notices,
            observer=observer,
            previous=previous,
            pricing_replacements=pricing_replacements,
            progress=progress,
            receipt_cache=receipt_cache,
            reports=reports,
            root=root,
            seed=seed,
            sources=sources,
            store=store,
            unavailable=unavailable,
            work_policy=work_policy,
            completion=completion,
        )


def correlate_components(
    store, selected_inputs, evidence, previous, deps, unavailable, *, policy, retain, services, completion=None
):
    """Evaluate each current component with exactly its selected receipt inputs.

    Retiring groups determine lock scope; current evidence determines replacement
    components. Accepted reports bypass correlation completely.
    """
    completion = {} if completion is None else completion
    completed = completion.setdefault("components", {})
    if "component_members" not in completion:
        current_graph = DependencyGraph(
            {
                symbol: {
                    "dependencies": set(deps[symbol]) | unavailable.keys(),
                    "asset_ids": [item["id"] for item in facts["identities"] if item.get("id")],
                }
                for symbol, facts in evidence.items()
            }
        )
        selected_components = {}
        for name in sorted(selected_inputs):
            if name not in selected_components:
                component = current_graph.closure({name})
                selected_components.update(dict.fromkeys(component & selected_inputs.keys(), component))
        completion["component_members"] = selected_components
    selected_components = completion["component_members"]
    selected_reports, visited = {}, set()
    for name in sorted(selected_inputs):
        component = selected_components[name]
        members = component & selected_inputs.keys()
        component_key = tuple(sorted(members))
        if component_key in visited:
            continue
        visited.add(component_key)
        if component_key in completed:
            selected_reports.update(completed[component_key])
            continue
        if not retain:
            for member in sorted(members):
                view = selected_inputs[member]
                services.event(
                    "lineage_correlation_inputs",
                    symbol=member,
                    selected_generation=view.source_sha256,
                    materialization_scope=view.materialization_scope,
                )
        selected_reports.update(
            {member: retained_decision_report(previous[member], policy=policy) for member in members}
            if retain
            else services.correlate(
                store,
                {member: selected_inputs[member] for member in members},
                {member: evidence[member] for member in members},
                policy=policy,
                selected_group=sorted(component),
                unavailable_pricing=sorted(component & unavailable.keys()),
                auxiliary_evidence={
                    name: unavailable[name]["evidence"] for name in component & unavailable.keys()
                },
                evidence_generations={member: storage.document_digest(evidence[member]) for member in members},
                pricing_generations={member: selected_inputs[member].source_sha256 for member in members},
            )
        )
        completed[component_key] = {member: selected_reports[member] for member in members}
    return selected_components, selected_reports


def complete_numerical_group(store, root, seed, group, index, *, context, work_policy, pending, progress=None):
    """Drain an accepted numerical requirement before any ordinary group work."""
    if work_policy is None:
        return None
    required = [
        name
        for name in sorted(group)
        if work_policy.work.get(name, {}).get("required_numerical_completion") is not None
    ]
    if not required:
        return None
    work_policy.current_group = set(group)
    work_policy.check(group)
    outcomes = []
    for name in required:
        selection = work_policy.numerical_selection(name)
        if selection is not None:
            from .numerical_recovery import resume

            outcomes.append(
                resume(
                    selection,
                    context=context,
                    work_policy=work_policy,
                    progress=progress,
                    writer_view=(root, index),
                )
            )
    if any(
        work_policy.satisfying_entry(name, index["symbols"], pending) is None
        for name in group
        if name in work_policy.work
    ):
        return None  # A later pricing/metadata target remains ordinary selected work.
    return {
        "symbol": seed,
        "status": "processed" if outcomes else "unchanged",
        "processed": sorted(set(group) & index["symbols"].keys()),
        "numerical_completion": outcomes,
        "reused_decision": True,
        "provider_requests": 0,
        "pricing_writes": 0,
        "identity_rule_executions": 0,
    }


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
    retained_recovery=None,
    pending_checks=None,
    services,
):
    context = accept_context(store, context, work_policy)
    correlation_policy = context.accept_correlation_policy()
    index = storage.read_index(root) if index is None else index
    receipt_cache = receipt_cache or storage.ReceiptCache(root, index)
    graph = work_policy.graph if work_policy is not None else graph or DependencyGraph(index["symbols"])
    group = services.closure(index, {seed}, graph=graph)
    if (
        seed not in index["symbols"]
        and work_policy is not None
        and auxiliary_work(work_policy.work.get(seed, {}))
    ):
        seed = min(group.intersection(index["symbols"]), default=seed)
    if work_policy is not None and any(
        work_policy.work.get(name, {}).get("required_numerical_completion") is not None for name in group
    ):
        pending = services.read_checks(store, group)["checks"] if pending_checks is None else pending_checks
        completed = complete_numerical_group(
            store,
            root,
            seed,
            group,
            index,
            context=context,
            work_policy=work_policy,
            pending=pending,
            progress=progress,
        )
        if completed is not None:
            return completed
    acquisition_waves = 0
    pricing_replacements = {}
    evidence_client = evidence_client or EvidenceClient(identity_client)
    sourced_operation = context.sourced_operations.setdefault(evidence_client, {})
    metadata_cache = sourced_operation.setdefault("metadata", {})
    pricing_inputs = {}
    partitions = {}
    assignment_boundaries = {}
    source_work_cache = sourced_operation.setdefault("source_work", {})
    prefix_reuse_cache = sourced_operation.setdefault("pricing_prefix", {})

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
    for _ in range(MAX_CLOSURE + MAX_ACQUISITION_WAVES + 1):
        required = None
        if work_policy is not None:
            work_policy.current_group = set(group)
            work_policy.check(group)
        with storage.pricing_locks(store, group, known=index["symbols"]) as locked:
            checks = services.read_checks(store, locked)
            sources = services.current_sources(store, locked, known=index["symbols"])
            selected = {name: storage.document_digest(source) for name, source in sources.items()}
            if retained_recovery is not None:
                retained_recovery.accept_sources(group, selected)
            # A returned publication is authoritative. A later acknowledgement
            # failure cannot send its completed calculations through production again.
            for cached_key, held in tuple(context.group_completions.items()):
                effects = held.get("effects", {})
                published = effects.get("published")
                if (
                    published
                    and held.get("store") == str(store)
                    and held.get("group") == tuple(sorted(group))
                    and held.get("pricing") == selected
                    and held.get("policy") == correlation_policy.semantic_sha256
                    and held.get("execution") == context.accept_ruleset().execution_sha256
                    and held.get("refresh") == refresh
                    and held.get("retained_rebuild") == retained_rebuild
                    and held.get("end_date") == end_date
                    and all(
                        index["symbols"].get(name, {}).get("sha256") == entry["sha256"]
                        for name, entry in published.items()
                    )
                ):
                    if "publish_args" in held:
                        value = publish_completed_group(
                            CompletedGroupPublication(**held["publication"]),
                            **held["publish_args"],
                            graph=graph,
                            index=index,
                            receipt_cache=receipt_cache,
                            root=root,
                            store=store,
                            work_policy=work_policy,
                            completion=held,
                        )
                    else:
                        finish = held["finish_args"]
                        assignment_boundaries = finish["assignment_boundaries"]
                        value = services.finish_correlated_group(
                            **finish,
                            graph=graph,
                            index=index,
                            observer=observer,
                            progress=progress,
                            read_boundaries=read_boundaries,
                            receipt_cache=receipt_cache,
                            root=root,
                            store=store,
                            work_policy=work_policy,
                            completion=held,
                        )
                    if value is not None:
                        consume_membership_plan(context, held)
                        context.group_completions.pop(cached_key, None)
                        release_completed_assemblies(context, store, group)
                        return value
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
            assembly_key = (
                "assembly",
                str(store),
                tuple(sorted(group)),
                tuple(sorted(selected.items())),
                tuple((name, index["symbols"].get(name, {}).get("sha256")) for name in sorted(sources)),
                correlation_policy.semantic_sha256,
                context.accept_ruleset().execution_sha256,
                refresh,
                retained_rebuild,
                end_date,
            )
            for obsolete_key, held in tuple(context.group_completions.items()):
                if (
                    held.get("store") == str(store)
                    and held.get("group") == tuple(sorted(group))
                    and held.get("pricing") != selected
                ):
                    # These exact input generations actually changed. Keeping
                    # their inactive payload copies cannot lend acceptance.
                    context.group_completions.pop(obsolete_key, None)
                    context.group_completions.pop(held["assembly_key"], None)
            held_previous = context.group_completions.get(assembly_key, {}).get("previous", {})
            previous = {
                symbol: held_previous[symbol] if symbol in held_previous else receipt_cache.read(symbol)
                for symbol in sorted(sources)
            }
            metadata_refresh = refresh or (
                work_policy is not None and work_policy.needs_refresh(group, previous)
            )
            history_refresh = refresh or (
                work_policy is not None and work_policy.needs_full_metadata(group, previous)
            )
            notices = {
                symbol: notice
                for symbol in sorted(sources)
                if (notice := services.read_update(store / "end_of_day/pricing" / symbol, published=True))
                is not None
            }
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
            explicit_membership = context.completed_membership_plans.get(tuple(sorted(group)))
            if explicit_membership is not None and (
                context._ruleset is None or context._ruleset.membership_refinement is None
            ):
                raise LineageError("completed membership input requires its enabled registered producer")
            membership_projection_requested = explicit_membership is not None
            from .metadata import DEPENDENCY_HANDOFF_CORRELATION_VERSION

            if not retained_rebuild and any(
                prior and prior["versions"]["correlation"] < DEPENDENCY_HANDOFF_CORRELATION_VERSION
                for prior in previous.values()
            ):
                evidence_client.prepare_retained(
                    [evidence_client.provider_view(prior["evidence"]) for prior in previous.values() if prior]
                    + [
                        evidence_client.provider_view(item["evidence"])
                        for item in prior_unavailable.values()
                        if "evidence" in item
                    ]
                )
            reuse_metadata = services.reuse_metadata(seed, group, selected, previous, notices, metadata_refresh)
            metadata_current = reuse_metadata and (
                retained_rebuild
                or all(evidence_client.evidence_current(prior["evidence"]) for prior in previous.values())
            )
            computation_current = retained_recovery is None and all(
                reusable_computation(prior) for prior in previous.values()
            )
            # Select completed-decision compatibility once for this coherent
            # receipt view; acknowledgement consumes the selected mode below.
            decision_modes = {
                name: "v78"
                if retained_v78_decision(prior)
                else "v79"
                if retained_v79_decision(prior)
                else "v80"
                if retained_v80_decision(prior)
                else "v81"
                if retained_v81_decision(prior)
                else "v82"
                if retained_v82_decision(prior)
                else "v83"
                if retained_v83_decision(prior)
                else "v84"
                if retained_v84_decision(prior)
                else "v85"
                if retained_v85_decision(prior)
                else "v86"
                if retained_v86_decision(prior)
                else "v87"
                if retained_v87_decision(prior)
                else "v88"
                if retained_v88_decision(prior)
                else "v89"
                if retained_v89_decision(prior)
                else "v90"
                if retained_v90_decision(prior)
                else "current"
                if current_decision(prior)
                else None
                for name, prior in previous.items()
            }
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
                and not membership_projection_requested
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
                and not membership_projection_requested
                and (
                    promoted_decision
                    or all(
                        decision_modes[name]
                        or services.can_promote_observation_decision(prior, correlation_policy)
                        for name, prior in previous.items()
                    )
                )
                and (work_policy is None or work_policy.decision_inputs_satisfied(sources, selected))
            ):
                services.event(
                    "lineage_required", symbol=seed, required=False, reason="accepted_unchanged_inputs"
                )
                receipts = {
                    symbol: {
                        **prior,
                        # Carry an applicable frozen decision's exact provenance
                        # through acknowledgement;
                        # it is not a new calculation or header migration.
                        **(
                            {}
                            if decision_modes[symbol]
                            in {
                                "v78",
                                "v79",
                                "v80",
                                "v81",
                                "v82",
                                "v83",
                                "v84",
                                "v85",
                                "v86",
                                "v87",
                                "v88",
                                "v89",
                                "v90",
                            }
                            else {
                                "schema_version": VERSION,
                                "versions": versions(),
                                "correlation_policy": correlation_policy.provenance(),
                            }
                        ),
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
            # The unchanged publication above stays closed. Only selected new
            # work consumes provider-only views of legacy auxiliary metadata;
            # replacing that view must not mutate the prior receipt's body.
            unavailable = {
                name: {
                    **value,
                    **(
                        {"evidence": evidence_client.provider_view(value["evidence"])}
                        if "evidence" in value
                        and value["evidence"] is prior_unavailable.get(name, {}).get("evidence")
                        else {}
                    ),
                }
                for name, value in unavailable.items()
            }
            required = {
                symbol: {"from": start, "end_exclusive": end}
                for symbol in sorted(group)
                if symbol not in sources and symbol not in unavailable
            }
            if not required:
                # One decision for this selected source/receipt under these locks.
                # None is a completed negative result too; consumers retain it.
                prefix_decisions = {}
                for symbol, source in sources.items():
                    if retained_rebuild:
                        prefix_decisions[symbol] = None, (False, False)
                        continue
                    key = selected[symbol], index["symbols"].get(symbol, {}).get("sha256")
                    if key not in prefix_reuse_cache:
                        correction = services.correction_from(source, previous[symbol])
                        prefix_reuse_cache[key] = (
                            correction,
                            source_reuse(
                                source,
                                previous[symbol],
                                selected=selected[symbol],
                                selected_correction=correction,
                            ),
                        )
                    prefix_decisions[symbol] = prefix_reuse_cache[key]
                corrections = {name: decision[0] for name, decision in prefix_decisions.items()}
                choice_corrections = dict(corrections)
                prefix_scope = (
                    retained_recovery is None
                    and not membership_projection_requested
                    and group == sources.keys() | unavailable.keys()
                )
                unchanged = (
                    prefix_scope
                    and not refresh
                    and all(decision[1][0] for decision in prefix_decisions.values())
                )
                resume = (
                    prefix_scope
                    and not refresh
                    and all(decision[1][1] for decision in prefix_decisions.values())
                )
                services.event(
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
                source_work_requested = not retained_rebuild and membership_projection_requested
                if source_work_requested:
                    controls = {
                        name: metadata_source_control(
                            store,
                            name,
                            source,
                            selected[name],
                            previous[name],
                            accepted_reuse=prefix_decisions[name][1],
                        )
                        for name, source in sources.items()
                    }
                    for name, control in controls.items():
                        control.previous_receipt_generation = index["symbols"].get(name, {}).get("sha256")
                        if control.previous is not None:
                            control.accepted_provider_evidence = evidence_client.provider_view(
                                control.previous["evidence"], local_report=control.local
                            )
                    if progress:
                        progress("lineage_evidence", {"symbols": sorted(group), "reused": reuse_metadata})
                    for _metadata_round in range(MAX_CLOSURE + 1):
                        evidence, deps, assets = {}, {}, set()
                        for symbol, view in controls.items():
                            prior = previous[symbol]
                            full = full_metadata(
                                prior, None if corrections[symbol] else notices.get(symbol), history_refresh
                            )
                            key = (symbol, view.source_sha256, full)
                            if retained_rebuild:
                                evidence[symbol] = view.accepted_provider_evidence
                            elif key in metadata_cache and evidence_client.evidence_current(metadata_cache[key]):
                                # A receipt reader can return another accepted copy.
                                # Keep the completed metadata producer object and
                                # its downstream operation binding instead.
                                evidence[symbol] = metadata_cache[key]
                            elif (
                                prior is not None
                                and (reuse_metadata or not full and view.source_sha256 == prior["source_sha256"])
                                and evidence_client.evidence_current(prior["evidence"])
                            ):
                                # Applicable peer metadata remains the same completed
                                # object even when another source selects new work.
                                # Rebuilding an equal copy loses its producer binding
                                # and repeats downstream work after interruption.
                                metadata_cache[key] = evidence[symbol] = view.accepted_provider_evidence
                            else:
                                if key not in metadata_cache or not evidence_client.evidence_current(
                                    metadata_cache[key]
                                ):
                                    can_continue = (
                                        not full
                                        and prior is not None
                                        and evidence_client.catalogue_attempted
                                        and evidence_client.catalogue is not None
                                    )
                                    metadata_cache[key] = services.acquire(
                                        view,
                                        RetainedReportClient(evidence_client, prior["evidence"])
                                        if reuse_metadata or can_continue
                                        else evidence_client,
                                        prior,
                                        refresh=full,
                                    )
                                evidence[symbol] = metadata_cache[key]
                            deps[symbol] = (
                                prior["dependencies"]
                                if retained_rebuild
                                else dependencies(evidence[symbol], symbol)
                            )
                            assets.update(
                                item["id"] for item in evidence[symbol]["identities"] if item.get("id")
                            )
                        for symbol, missing in sorted(unavailable.items()):
                            # Prices and metadata have different provider endpoints. Keep
                            # discovering real relationships even when bars reject a label.
                            if not retained_rebuild and (
                                "evidence" not in missing
                                or not evidence_client.evidence_current(missing["evidence"])
                            ):
                                missing["evidence"] = services.acquire_unpriced(
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
                            for facts in [
                                *evidence.values(),
                                *(item["evidence"] for item in unavailable.values()),
                            ]
                        ):
                            break
                    else:
                        raise LineageError("selected metadata observations did not become coherent")
                    expanded = services.closure(
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
                            services.event(
                                "dependency_suffix_selection",
                                symbol=symbol,
                                required=not accepted_empty,
                                reason="completed_empty_suffix" if accepted_empty else "selection_unavailable",
                                end_exclusive=end,
                            )
                            if not accepted_empty:
                                required.setdefault(symbol, {"from": start, "end_exclusive": end})
                    if required:
                        # Metadata closure selected acquisition, never historical
                        # payload assembly, before releasing the owning locks.
                        pass
                    else:
                        auxiliary = {name: value["evidence"] for name, value in unavailable.items()}
                        membership_plan_key = tuple(sorted(controls))
                        accepted_membership_plan = context.completed_membership_plans.get(membership_plan_key)
                        source_plan = services.prepare_group_evidence_and_source_work(
                            controls,
                            evidence,
                            auxiliary,
                            cache=source_work_cache,
                            ruleset=context._ruleset,
                            accepted_membership_plan=accepted_membership_plan,
                        )
                        if any(
                            source_plan.memberships.get(member, identity)["security_id"]
                            != identity["security_id"]
                            for control in controls.values()
                            if control.previous is not None
                            for identity in control.previous.get("identity_memberships", ())
                            for member in identity["member_alpaca_asset_ids"]
                        ):
                            from libs.alpaca_serving_catalogue import storage as serving_storage

                            from .recovery_handoffs import remember

                            if serving_storage.path(store).exists():
                                catalogue = context.select_serving()
                                donors = source_work_cache.setdefault("donor_checkpoints", {})
                                keys = {
                                    catalogue["members"][name]
                                    for name in controls
                                    if name in catalogue["members"]
                                }
                                missing = {
                                    key
                                    for key in keys
                                    if key not in donors
                                    or donors[key]["decision_generation"]
                                    != catalogue["groups"][key]["decision_generation"]
                                }
                                if missing:
                                    selected_donors = serving_storage.read_groups(
                                        store, missing, generation=catalogue["state"]["generation"]
                                    )
                                    donors.update(selected_donors)
                                    for key, donor in selected_donors.items():
                                        remember(
                                            store,
                                            "serving_group",
                                            (catalogue["state"]["generation"], key),
                                            donor,
                                        )
                        services.event(
                            "lineage_source_work_plan",
                            symbol=seed,
                            dispositions=source_plan.dispositions,
                            rows_to_evaluate=sorted(
                                name
                                for name, disposition in source_plan.dispositions.items()
                                if disposition in {"B", "C"}
                            ),
                            membership_projection_required=source_plan.projection_required,
                        )
                        receipts, boundaries, planned_inputs, reports = execute_source_work(
                            store,
                            root,
                            controls,
                            evidence,
                            auxiliary,
                            source_plan,
                            correlation_policy,
                            source_work_cache,
                            loader=services.load_pricing,
                        )
                        for name, receipt in receipts.items():
                            receipt["dependencies"] = sorted(set(deps[name]) | unavailable.keys())
                            receipt["input_sources"] = dict(selected)
                            if unavailable:
                                receipt["unavailable_pricing"] = unavailable
                        completion_key = ("source_completed", *assembly_key[1:])
                        completion = context.group_completions.get(completion_key)
                        if completion is None or not (
                            completion["evidence"].keys() == evidence.keys()
                            and all(completion["evidence"][name] is facts for name, facts in evidence.items())
                        ):
                            completion = {
                                "store": str(store),
                                "group": tuple(sorted(group)),
                                "pricing": dict(selected),
                                "policy": correlation_policy.semantic_sha256,
                                "execution": context.accept_ruleset().execution_sha256,
                                "refresh": refresh,
                                "retained_rebuild": retained_rebuild,
                                "end_date": end_date,
                                "evidence": dict(evidence),
                                "assembly_key": assembly_key,
                            }
                            if accepted_membership_plan is not None:
                                completion["accepted_membership_plan"] = (
                                    membership_plan_key,
                                    accepted_membership_plan,
                                )
                            context.group_completions[completion_key] = completion
                        if "publication" not in completion:
                            publication = CompletedGroupPublication(
                                receipts,
                                boundaries,
                                not reports,
                                not reports,
                                sum(
                                    (
                                        previous[name]["source_rows"]
                                        for name, view in planned_inputs.items()
                                        if not getattr(view, "materialized", True)
                                    )
                                )
                                if not reports
                                else sum(
                                    (
                                        previous[name]["source_rows"]
                                        for name in sources
                                        if source_plan.dispositions[name] in {"A", "retained"}
                                    )
                                ),
                                sum(
                                    (
                                        len(rows)
                                        for view in planned_inputs.values()
                                        if getattr(view, "materialized", True)
                                        for rows in view.views.values()
                                    )
                                ),
                                membership_refinement=source_plan.membership_refinement,
                            )
                            completion["publication"] = publication.__dict__
                            completion["publish_args"] = {
                                "acquisition_waves": acquisition_waves,
                                "checks": checks,
                                "notices": notices,
                                "previous": previous,
                                "pricing_replacements": pricing_replacements,
                                "seed": seed,
                                "sources": sources,
                                "unavailable": unavailable,
                            }
                        result = publish_completed_group(
                            CompletedGroupPublication(**completion["publication"]),
                            **completion["publish_args"],
                            graph=graph,
                            index=index,
                            receipt_cache=receipt_cache,
                            root=root,
                            store=store,
                            work_policy=work_policy,
                            completion=completion,
                        )
                        consume_membership_plan(context, completion)
                        context.group_completions.pop(completion_key, None)
                        release_completed_assemblies(context, store, group)
                        return result
                    raise LineageError(
                        "metadata closure requires explicitly selected missing pricing before evaluation"
                    )
                # Assembly has the same source/receipt scope as its producer.
                # It survives a peer or publication failure, never a new generation.
                assembly = context.group_completions.setdefault(assembly_key, {})
                assembly.setdefault("previous", previous)
                # Dependency closure can enlarge this scope after assembly.
                # Lend the existing generation-keyed values into the enlarged
                # owner; a new scope does not invalidate unchanged source data.
                pricing_inputs = assembly.setdefault("pricing_inputs", pricing_inputs)
                partitions = assembly.setdefault("partitions", partitions)
                assignment_boundaries = assembly.setdefault("assignment_boundaries", assignment_boundaries)
                retained_kinds = {}
                if not resume:
                    corrections = dict.fromkeys(sources)
                projections = {}
                affected = {}
                if any(choice_corrections.values()):
                    affected_identities = {}

                    def identities_at(symbol, day):
                        prior = previous[symbol]
                        if prior is None:
                            return set()
                        key = symbol, day
                        if key in affected_identities:
                            return affected_identities[key]
                        summary = prior.get("assignment_summary", {}).get("raw", {})
                        if not summary.get("last_session") or summary["last_session"] < day:
                            affected_identities[key] = set()
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
                        affected_identities[key] = {
                            item["security_id"]
                            for item in identity_intervals(prior, documents)
                            if item["from"] <= day <= item["to"]
                        }
                        return affected_identities[key]

                    for name, day in choice_corrections.items():
                        if day:
                            affected.setdefault(day, set()).update(identities_at(name, day))
                    for name in sources if resume else ():
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
                            services.load_pricing(
                                store,
                                symbol,
                                source=sources[symbol],
                                source_sha256=selected[symbol],
                                selected_correction=corrections[symbol],
                                selected_change_from=(notices.get(symbol) or {}).get("from"),
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
                            evidence[symbol] = evidence_client.provider_view(
                                prior["evidence"], local_report=view.local
                            )
                        elif reuse_metadata and evidence_client.evidence_current(prior["evidence"]):
                            # Only the downstream rules changed. Consume the approved
                            # evidence directly, then recalculate the complete group.
                            evidence[symbol] = evidence_client.provider_view(
                                prior["evidence"], local_report=view.local
                            )
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
                                metadata_cache[key] = services.acquire(
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
                            missing["evidence"] = services.acquire_unpriced(
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
                expanded = services.closure(
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
                        services.event(
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
                    membership_deltas = (
                        {} if resume or any(decision[1][1] for decision in prefix_decisions.values()) else None
                    )
                    # Compare the metadata owner's completed provider views.
                    # Legacy research provenance stays in the original receipts;
                    # its omission from fresh inference is not a history change.
                    provider_previous = previous
                    if membership_deltas is not None:
                        provider_previous = dict(previous)
                        for name, prior in previous.items():
                            if prior is None or not (resume or prefix_decisions[name][1][1]):
                                continue
                            before = evidence_client.provider_view(
                                prior["evidence"], local_report=inputs[name].local
                            )
                            if before is not prior["evidence"]:
                                provider_previous[name] = {**prior, "evidence": before}
                    historical_lineage = requires_lineage(
                        provider_previous,
                        evidence,
                        pricing_unchanged=resume
                        and (unchanged or all(complete_append(view) for view in inputs.values())),
                        refresh=refresh,
                        append=resume,
                        completed_deltas=membership_deltas,
                        auxiliary_previous={
                            name: evidence_client.provider_view(item["evidence"])
                            for name, item in unavailable_pricing(previous.get(seed)).items()
                        }
                        if membership_deltas is not None
                        else None,
                        auxiliary_current={name: item["evidence"] for name, item in unavailable.items()}
                        if membership_deltas is not None
                        else None,
                        classification_sources={
                            name for name, decision in prefix_decisions.items() if decision[1][1]
                        }
                        if not resume
                        else None,
                    )
                    append_invalidation = (
                        append_history_invalidation(provider_previous, inputs)
                        if resume and not historical_lineage
                        else None
                    )
                    if append_invalidation is not None:
                        services.event(
                            "lineage_append_invalidation",
                            symbol=seed,
                            scope="complete_group",
                            **append_invalidation,
                        )
                        historical_lineage = True
                    # Prefix-only certificates are applicable only after the
                    # final work scope stays incremental. Complete-group work
                    # materializes every selected history and establishes new
                    # authority through that producer boundary.
                    accepted_prefix_sources = {
                        name: prior
                        for name, prior in previous.items()
                        if not historical_lineage
                        and resume
                        and prior is not None
                        and prior.get("row_partition_complete") is True
                        and prior.get("unresolved_rows") == 0
                        and membership_deltas.get(name, {}).get("scope") in {"none", "added_sessions"}
                    }
                    choice_prefix_sources = {
                        name: prior
                        for name, prior in previous.items()
                        if not retained_rebuild
                        and retained_recovery is None
                        and prior is not None
                        and prefix_decisions[name][1][1]
                        and membership_deltas is not None
                        and membership_deltas.get(name, {}).get("scope") in {"none", "added_sessions"}
                    }
                    original_choice_inputs = dict(inputs)
                    no_new_rows = not historical_lineage and not any(
                        rows for view in inputs.values() for rows in view.views.values()
                    )
                    services.event(
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
                    if historical_lineage:
                        partial_inputs = {
                            name
                            for name, view in inputs.items()
                            if view.materialization_scope != "complete_history"
                        }
                    else:
                        partial_inputs = set()
                    if partial_inputs:
                        # Evidence changed after pricing alone proved reusable.
                        # Announce selected owner work before reading its bodies.
                        services.event(
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
                            name: services.load_pricing(
                                store,
                                name,
                                source=source,
                                partitions=partitions[name],
                                source_sha256=selected[name],
                                selected_correction=None,
                            )
                            if name in partial_inputs
                            else inputs[name]
                            for name, source in sources.items()
                        }
                    if historical_lineage:
                        incomplete = sorted(
                            name
                            for name, view in inputs.items()
                            if view.materialization_scope != "complete_history"
                        )
                        if incomplete:
                            raise LineageError(
                                "complete-group lineage requires complete pricing history for "
                                + ", ".join(incomplete)
                            )
                    resume = not historical_lineage
                    for name, certificate in accepted_prefix_sources.items():
                        inputs[name].accepted_current_source_ownership = certificate.get(
                            "current_source_ownership"
                        )
                    scoped_memberships = services.scoped_identity_memberships(
                        choice_prefix_sources, old_boundaries
                    )
                    for name, memberships in scoped_memberships.items():
                        view = inputs[name]
                        view.accepted_identity_memberships = memberships
                        if historical_lineage and corrections[name] is None and name not in projections:
                            # A peer selected complete-group work, but this
                            # source's pricing prefix and owner metadata remain
                            # accepted. Carry its old row decisions in their
                            # actual full-source positions. New and unresolved
                            # rows alone enter the owning cascade.
                            view.accepted_report = {
                                **retained_decision_report(previous[name], policy=correlation_policy),
                                **retained_ownership_report(
                                    previous[name], view.local, memberships, root, assignment_boundaries
                                ),
                            }
                            view.accepted_report["source_row_count"] = sum(
                                len(rows) for rows in view.views.values()
                            )
                    if resume and not no_new_rows:
                        resumed_previous = dict(previous)
                        for name, prior in resumed_previous.items():
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
                        memberships = services.scoped_identity_memberships(resumed_previous, old_boundaries)
                        for name, view in inputs.items():
                            if name not in resumed_previous:
                                continue
                            view.accepted_identity_memberships = memberships[name]
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
                    completion_key = ("completed", *assembly_key[1:], no_new_rows, resume)
                    completion = context.group_completions.get(completion_key)
                    if completion is None or not (
                        completion["evidence"].keys() == evidence.keys()
                        and all(completion["evidence"][name] is facts for name, facts in evidence.items())
                    ):
                        completion = {
                            "store": str(store),
                            "group": tuple(sorted(group)),
                            "pricing": dict(selected),
                            "policy": correlation_policy.semantic_sha256,
                            "execution": context.accept_ruleset().execution_sha256,
                            "refresh": refresh,
                            "retained_rebuild": retained_rebuild,
                            "end_date": end_date,
                            "evidence": dict(evidence),
                            "assembly_key": assembly_key,
                        }
                        context.group_completions[completion_key] = completion
                    if "correlation" not in completion:
                        components, reports = correlate_components(
                            store,
                            inputs,
                            evidence,
                            previous,
                            deps,
                            unavailable,
                            policy=correlation_policy,
                            retain=no_new_rows,
                            services=services,
                            completion=completion,
                        )
                        if not no_new_rows and all(previous.values()):
                            from .membership_updates import invalidations, update_reports

                            reports, trace = update_reports(
                                reports,
                                evidence,
                                selected={
                                    "store": str(store),
                                    "selected_group": sorted(group),
                                    "receipts": previous,
                                    "pricing_generations": selected,
                                    "receipt_generations": {
                                        name: index["symbols"][name]["sha256"] for name in previous
                                    },
                                },
                                ruleset=context._ruleset,
                            )
                            affected = dict(affected)
                            for day, identifiers in invalidations(
                                inputs, reports, trace, instants=completion.setdefault("instant_keys", {})
                            ).items():
                                affected[day] = set(affected.get(day, ())) | identifiers
                            completion["membership_refinement"] = trace
                        completion["correlation"] = components, reports, affected
                    components, reports, affected = completion["correlation"]
                    completion["finish_args"] = {
                        "reports": reports,
                        "acquisition_waves": acquisition_waves,
                        "affected": affected,
                        "assignment_boundaries": assignment_boundaries,
                        "checks": checks,
                        "choice_prefix_sources": choice_prefix_sources,
                        "components": components,
                        "deps": deps,
                        "evidence": evidence,
                        "inputs": inputs,
                        "no_new_rows": no_new_rows,
                        "notices": notices,
                        "old_boundaries": old_boundaries,
                        "original_choice_inputs": original_choice_inputs,
                        "previous": previous,
                        "pricing_replacements": pricing_replacements,
                        "required": required,
                        "resume": resume,
                        "retained_recovery": retained_recovery,
                        "seed": seed,
                        "sources": sources,
                        "unavailable": unavailable,
                    }
                    completed_group = services.finish_correlated_group(
                        reports=reports,
                        acquisition_waves=acquisition_waves,
                        affected=affected,
                        assignment_boundaries=assignment_boundaries,
                        checks=checks,
                        choice_prefix_sources=choice_prefix_sources,
                        components=components,
                        deps=deps,
                        evidence=evidence,
                        graph=graph,
                        index=index,
                        inputs=inputs,
                        no_new_rows=no_new_rows,
                        notices=notices,
                        observer=observer,
                        old_boundaries=old_boundaries,
                        original_choice_inputs=original_choice_inputs,
                        previous=previous,
                        pricing_replacements=pricing_replacements,
                        progress=progress,
                        read_boundaries=read_boundaries,
                        receipt_cache=receipt_cache,
                        required=required,
                        resume=resume,
                        retained_recovery=retained_recovery,
                        root=root,
                        seed=seed,
                        sources=sources,
                        store=store,
                        unavailable=unavailable,
                        work_policy=work_policy,
                        completion=completion,
                    )
                    if completed_group is not None:
                        context.group_completions.pop(completion_key, None)
                        release_completed_assemblies(context, store, group)
                        return completed_group
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
                    acquired = services.download_pricing(
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

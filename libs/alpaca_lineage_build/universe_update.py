"""Discovery, registration and selective reconciliation around pricing/lineage owners."""

from contextlib import ExitStack
from pathlib import Path

from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.check_summary import assert_current, pending_symbols
from libs.alpaca_pricing_download.download import request_end
from libs.alpaca_pricing_download.update_checks import read_checks
from libs.alpaca_recovery_registry import DEFAULT_REGISTRY
from libs.common.debug_trace import traced
from libs.common.work_scope import selected_work

from . import storage, universe_plan, universe_state
from .catalogue import observe_catalogue
from .context import accept_context, recover_and_accept_correlation_policy
from .contract import LineageError
from .metadata import EvidenceClient
from .pipeline import run_lineage
from .refresh import refresh_required_history
from .registration import command_policy
from .sourced_rules import load_applications
from .update import update_everything
from .work_contract import WorkPolicy, acquisition_fingerprint, auxiliary_work, pricing_target


@traced("recovery")
def recover_publications(store, *, context=None):
    """Roll forward known journals only; retry selection owns subsequent network work."""
    # Named owner controls bound recovery. DO NOT replace missing controls with
    # an implicit filesystem census or use recovery to acquire fresh evidence.
    accepted_store = storage.accept_store(store) if context is None else context.accept_store()
    selected = set(pending_symbols(store)) | set(read_checks(store)["checks"])
    recovered = []
    for symbol in sorted(selected):
        root = pricing_storage.published_path(store, f"end_of_day/pricing/{symbol}")
        if (root / ".pending.json").exists():
            with pricing_storage.ticker_lock(root, exclusive=True):
                if pricing_storage.recover(root):
                    recovered.append(symbol)
    with storage.writer_lock(store, accepted_store=accepted_store) as root:
        lineage = context.recover_lineage(root) if context is not None else storage.recover(store, root)
    return {"pricing_recovered": recovered, "lineage_recovered": lineage, "failed": 0}


@traced("plan")
def plan_update(
    store, end_date, *, identity_client, registry=DEFAULT_REGISTRY, retry_blocked=False, coalesce_late_bars=True
):
    end = request_end(end_date).date().isoformat()
    # Planning is read-only and does not recover journals, but it still uses
    # policy-bearing lineage projections. Reject an invalid live policy before
    # the catalogue client can issue a request.
    context = accept_context(store)
    context.accept_correlation_policy()
    # Accept every local selector needed to derive work before observing the
    # provider. A missing boundary is a blocker, not permission to perform an
    # otherwise unbounded catalogue request and then discover that no plan can
    # be built from it.
    try:
        accepted = universe_plan.boundaries(
            store, registry, context=context, coalesce_late_bars=coalesce_late_bars
        )
    except (RuntimeError, OSError, ValueError) as error:
        return {
            "mode": "plan",
            "end_exclusive": end,
            "blockers": [str(error)],
            "catalogue_observed_at_utc": None,
            "catalogue_records": 0,
            "selection_metadata_http_requests_observed": 0,
        }
    applications, supplements = load_applications()
    metadata_before = identity_client.request_count
    observation = observe_catalogue(identity_client)
    evidence = EvidenceClient(identity_client, observation=observation, event_supplements=supplements)
    evidence.rule_applications = applications
    selection = universe_plan.select_work(accepted, observation, evidence, end, retry_blocked=retry_blocked)
    report = universe_plan.preview(
        accepted,
        selection,
        observation,
        end,
        metadata_requests=identity_client.request_count - metadata_before,
    )
    pending = accepted["checks"]["checks"]
    if pending:
        report["pending_lineage_symbols"] = sorted(pending)
        report["blockers"].append("pending producer work must finish before the final execution scope is known")
    return report


@traced("update")
def run_update(
    store,
    end_date=None,
    *,
    pricing_client,
    identity_client,
    registry=DEFAULT_REGISTRY,
    workers=3,
    progress=None,
    retry_blocked=False,
    coalesce_late_bars=True,
):
    context = accept_context(store)
    store = Path(store).absolute()
    end = request_end(end_date).date().isoformat()
    result = {"complete": False, "end_exclusive": end, "failed": 0, "stages": {}}
    metadata_started = identity_client.request_count
    pricing_started = getattr(pricing_client, "request_count", None)
    with universe_state.update_lock(store), ExitStack() as scopes:
        from .recovery_handoffs import operation as recovery_handoffs

        scopes.enter_context(recovery_handoffs(store))
        # Explicit universe initialization is the outer operation gate. Fail
        # before recovering any owner publication when it is absent; recovery
        # must not silently initialize or otherwise mutate an uninitialized
        # store.
        universe_state.require_initialized(store)

        prepared = prepare_locked(
            store,
            end,
            pricing_client=pricing_client,
            identity_client=identity_client,
            registry=registry,
            workers=workers,
            progress=progress,
            retry_blocked=retry_blocked,
            coalesce_late_bars=coalesce_late_bars,
            context=context,
            result=result,
        )
        if "policy" not in prepared:
            return prepared
        scopes.enter_context(selected_work(store, prepared["storage_symbols"]))
        return continue_locked(
            store,
            end,
            prepared,
            pricing_client=pricing_client,
            identity_client=identity_client,
            registry=registry,
            workers=workers,
            progress=progress,
            metadata_started=metadata_started,
            pricing_started=pricing_started,
        )


def prepare_locked(
    store,
    end,
    *,
    pricing_client,
    identity_client,
    registry,
    workers,
    progress,
    retry_blocked,
    coalesce_late_bars,
    context,
    result,
):
    """Complete baseline restoration and initial selection under the operation lock."""

    with universe_state.mutation_handoff() as mutations:
        prepared = _prepare_locked(
            store,
            end,
            pricing_client=pricing_client,
            identity_client=identity_client,
            registry=registry,
            workers=workers,
            progress=progress,
            retry_blocked=retry_blocked,
            coalesce_late_bars=coalesce_late_bars,
            context=context,
            result=result,
        )
        if "policy" in prepared:
            prepared["workflow_revision"] = mutations["revision"]
        return prepared


def _prepare_locked(
    store,
    end,
    *,
    pricing_client,
    identity_client,
    registry,
    workers,
    progress,
    retry_blocked,
    coalesce_late_bars,
    context,
    result,
):
    """Complete baseline restoration and initial selection under the operation lock."""
    from libs.alpaca_serving_catalogue import work as serving_work

    registered = set()

    @traced("registration")
    def register(symbols):
        additions = set(symbols) - registered
        if additions:
            universe_state.register(store, additions, registry=registry)
            registered.update(additions)

    # Finish already-journaled publication with its original inputs. New
    # dependency acquisitions are explicitly capped by this invocation.
    if progress:
        progress("recovery", {"state": "started"})
    # Finish every owner journal under its frozen provenance before the new
    # runtime policy is loaded. A broken new YAML file cannot strand an
    # already-recorded valid transaction.
    context.select_serving(recover=True)
    recovered = recover_publications(store, context=context)
    if progress:
        progress("recovery", {"state": "completed", **recovered})
    result["stages"]["recovery"] = recovered
    if recovered["failed"]:
        result["failed"] = recovered["failed"]
        return result
    context.accept_correlation_policy()
    applications, supplements = load_applications()

    from .projection import require_complete

    root = store / "end_of_day/lineage"
    # Recovery may have replaced the selector; accept its current boundary
    # only after authorizing the policy for all new work.
    context.index = None
    with storage.writer_lock(store, accepted_store=context.accept_store()), storage.publication_lock(root):
        pending_serving = serving_work.read(store)
        context.index = storage.read_index(root)
        require_complete(context.index)
    from .closed_source_recovery import recover as recover_closed_sources

    with storage.writer_lock(store, accepted_store=context.accept_store()):
        repaired = recover_closed_sources(store, root, context.index, pending_serving, applications)
    if repaired:
        result["stages"]["closed_source_recovery"] = {"symbols": repaired, "failed": 0}
    from .membership_transfer_recovery import recover as recover_membership_transfers

    with storage.writer_lock(store, accepted_store=context.accept_store()):
        repaired_transfers = recover_membership_transfers(
            store, root, context.index, pending_serving, applications, context.select_serving()
        )
    if repaired_transfers:
        result["stages"]["membership_transfer_recovery"] = {
            "symbols": repaired_transfers,
            "failed": 0,
        }
    if not read_checks(store)["complete"]:
        raise LineageError("lineage scheduling coverage missing; explicit lineage update is required")
    pending = pending_symbols(store)
    recovery_evidence = None
    if pending:
        # Recovery selects the summary's named inputs and their complete groups.
        # Pass an explicit selection: the default lineage command also selects
        # every durable obligation, including ordinary next-session appends.
        # Never plan a universe against a missing/temporarily pending summary.
        recovery_policy = command_policy(store, end, context=context, retry_blocked=retry_blocked)
        recovery_policy.rule_applications = applications
        recovery_evidence = EvidenceClient(identity_client, event_supplements=supplements)
        from .selection import DependencyGraph
        from .sourced_rules import configure

        configure(
            recovery_evidence,
            applications,
            DependencyGraph(context.index["symbols"]),
            context.index["symbols"],
        )
        callback = (lambda stage, values: progress("recovery_" + stage, values)) if progress else None
        recoverable = [symbol for symbol in pending if recovery_policy.allowed({symbol})]
        if recoverable:
            # The full update needs one catalogue. Accept it before selected
            # recovery so every phase shares the same identity observation.
            recovery_evidence.observe_catalogue()
        value = (
            run_lineage(
                store,
                recoverable,
                identity_client=identity_client,
                pricing_client=pricing_client,
                end_date=end,
                work_policy=recovery_policy,
                evidence_client=recovery_evidence,
                register_discoveries=register,
                context=context,
                progress=callback,
                on_pricing=callback,
            )
            if recoverable
            else {"failed": 0, "results": []}
        )
        recovery_policy.acknowledge()
        result["stages"]["recovery_lineage"] = value
        remaining = pending_symbols(store)
        if remaining:
            return {
                **result,
                "failed": value["failed"],
                "pending_work": universe_state.pending_count(universe_state.read_state(store)),
                "blockers": ["pending lineage must finish before selecting daily update work"],
                "pending_lineage_symbols": remaining,
                "deferred": [
                    {"symbol": symbol, "reason": "affected_group_restricted"}
                    for symbol in remaining
                    if not recovery_policy.allowed({symbol})
                ],
            }
    accepted = universe_plan.boundaries(store, registry, context=context, coalesce_late_bars=coalesce_late_bars)
    metadata_before = identity_client.request_count
    if progress:
        progress("catalogue", {"state": "started"})
    evidence = recovery_evidence or EvidenceClient(identity_client, event_supplements=supplements)
    evidence.rule_applications = applications
    # Retain every accepted observation and lookup across recovery. Only the
    # scheduling policy is replaced by the newly accepted work selection.
    evidence.work_policy = None
    evidence.restricted_identifiers.clear()
    observation = evidence.observe_catalogue()
    if progress:
        progress(
            "catalogue",
            {
                "state": "completed",
                "records": len(observation["records"]),
                "http_requests": identity_client.request_count - metadata_before,
            },
        )
    registered.update(accepted["registry"])
    selection = universe_plan.select_work(accepted, observation, evidence, end, retry_blocked=retry_blocked)
    result["plan"] = universe_plan.preview(
        accepted,
        selection,
        observation,
        end,
        metadata_requests=identity_client.request_count - metadata_before,
    )
    # The accepted compact selectors own this scope. New metadata may add a
    # complete dependency group only at its explicit expansion handoff.
    seeds = (
        set(selection["work"])
        | {entry["symbol"] for entry in accepted["summary"]["entries"] if entry["end_exclusive"][:10] <= end}
        | set(accepted["late_bar_repairs"]["sources"])
        | set(accepted["serving_catalogue"]["pending_symbols"])
    )
    storage_symbols = set()
    if progress:
        progress(
            "selected",
            {
                "work": len(selection["work"]),
                "registry_additions": len(selection["registration_additions"]),
                "expected_io": result["plan"]["expected_io"],
            },
        )
    if accepted["summary"] is not None:
        assert_current(store, accepted["summary"]["token"])
    with storage.publication_lock(root):
        storage.assert_index_current(root, accepted["index"])
    committed = universe_state.accept(
        store, observation, selection["registration_additions"], selection["work"], handoff=True
    )
    selection["work"] = committed["work"]
    result["stages"]["registration"] = universe_state.register(store, registry=registry, handoff=True)
    if progress:
        progress("registration", {"state": "completed", **result["stages"]["registration"]})
    registered.update(selection["registration_additions"])
    with universe_state.outcome_batch(store) as batch:
        for symbol in selection["already_published"]:
            universe_state.record_outcome(
                store,
                symbol,
                status="complete",
                outcome={"published_before_acknowledgement": True},
                batch=batch,
                expected=accepted["state"]["work"][symbol],
            )
    work = selection["work"]
    # Selection has already accepted catalogue facts, restrictions and group
    # membership. Delegate those exact requirements; later stages must not
    # reconstruct them by reading the whole store again.
    policy = WorkPolicy(
        store,
        accepted["index"],
        {
            **{
                s: item
                for s, item in accepted["state"]["work"].items()
                if s not in selection["already_published"]
            },
            **work,
        },
        selection["deferred"],
        end,
        context=context,
    )
    policy.rule_applications = applications
    # Reuse the accepted work policy's graph instead of reconstructing the
    # entire membership graph just to enforce its selected storage scope.
    for symbol in seeds:
        if symbol not in storage_symbols:
            storage_symbols.update(policy.graph.closure({symbol}))
    evidence.work_policy = policy
    for symbol, failure in selection["selection_failures"].items():
        policy.fail({symbol}, failure, stage="selection")

    # Existing summary members belong to live check -> correction -> append
    # BEFORE discovery can advance one of their dependencies. Otherwise an
    # empty/new sibling could select them through another acquisition path.
    # The completed maintenance handoff is retained by this operation;
    # later phases check only new members or genuinely changed generations.
    def keeps_summary_membership(symbol):
        item = work.get(symbol, {})
        if item.get("invalidate_pricing"):
            return False
        if not item.get("refresh_metadata"):
            return True
        # Selection already acquired this lookup. Name/exchange changes do
        # not postpone an otherwise eligible source's history approval;
        # inactivity, absence or changed identity must reconcile first.
        current = (item.get("required_lookups", {}).get(symbol) or {}).get("asset") or {}
        return (
            current.get("symbol") == symbol
            and current.get("status") == "active"
            and current.get("class") == "us_equity"
        )

    maintenance_symbols = {
        entry["symbol"] for entry in accepted["summary"]["entries"] if keeps_summary_membership(entry["symbol"])
    }
    return {
        "accepted": accepted,
        "selection": {**selection, "work": policy.work},
        "policy": policy,
        "context": context,
        "evidence": evidence,
        "registered": registered,
        "storage_symbols": storage_symbols,
        "maintenance_symbols": maintenance_symbols,
        "result": result,
    }


def continue_locked(
    store,
    end,
    prepared,
    *,
    pricing_client,
    identity_client,
    registry,
    workers,
    progress,
    metadata_started,
    pricing_started,
):
    """Compose history production and serving for the existing update command."""
    histories = histories_locked(
        store,
        end,
        prepared,
        pricing_client=pricing_client,
        identity_client=identity_client,
        registry=registry,
        workers=workers,
        progress=progress,
        metadata_started=metadata_started,
        pricing_started=pricing_started,
    )
    if histories is None:
        return prepared["result"]
    return serve_locked(store, histories, context=prepared["context"], progress=progress)


def histories_locked(
    store,
    end,
    prepared,
    *,
    pricing_client,
    identity_client,
    registry,
    workers,
    progress,
    metadata_started,
    pricing_started,
):
    """Finish pricing, dependency closure, lineage and history acknowledgements.

    This owner does not evaluate or publish serving. A missing completion
    boundary returns no handoff; completed independent failures remain explicit.
    """
    from .projection import require_complete

    accepted, selection = prepared["accepted"], prepared["selection"]
    policy, context, evidence = prepared["policy"], prepared["context"], prepared["evidence"]
    registered, maintenance_symbols = prepared["registered"], prepared["maintenance_symbols"]
    result, work = prepared["result"], policy.work
    root = store / "end_of_day/lineage"

    @traced("registration")
    def register(symbols):
        additions = set(symbols) - registered
        if additions:
            universe_state.register(store, additions, registry=registry)
            registered.update(additions)

    initial_maintenance = update_everything(
        store,
        end,
        pricing_client=pricing_client,
        identity_client=identity_client,
        workers=workers,
        progress=progress,
        evidence_client=evidence,
        register_discoveries=register,
        recover_pending=False,
        work_policy=policy,
        context=context,
        symbols=maintenance_symbols,
    )
    result["stages"]["maintenance"] = initial_maintenance
    acquisition_groups = {}
    for symbol, item in work.items():
        # Missing members accepted only through dependency expansion need
        # group context. There the lineage owner can distinguish an exact
        # auxiliary rejection from a rejected seed or retained history.
        lineage_owned_auxiliary = (
            symbol not in accepted["index"]["symbols"]
            and bool(set(item.get("required_group", ())) - {symbol})
            and auxiliary_work(item)
        )
        if item["acquire"] and not lineage_owned_auxiliary and policy.allowed({symbol}):
            acquisition_groups.setdefault(pricing_target(item), []).append(symbol)
    pricing = {"results": [], "requests": {}, "failed": 0}
    for target, symbols in sorted(acquisition_groups.items()):
        value = acquire_selected(
            store,
            symbols,
            target,
            client=pricing_client,
            workers=workers,
            progress=progress,
            invalidate={s for s in symbols if work[s]["invalidate_pricing"]},
            evidence_fingerprints={s: acquisition_fingerprint(work[s]) for s in symbols},
            from_dates={s: work[s].get("pricing_start") for s in symbols},
            context=context.pricing(),
        )
        pricing["results"].extend(value["results"])
        pricing["requests"][target] = value["requests"]
        pricing["failed"] += value["failed"]
    result["stages"]["acquisition"] = pricing
    policy.record_results(pricing, stage="acquisition")
    failed_symbols = {s for s in work if not policy.allowed({s})}
    summarized = {entry["symbol"] for entry in accepted["summary"]["entries"]}
    changed_sources = {
        item["symbol"]: item["source_sha256"]
        for item in pricing["results"]
        if "source_sha256" in item
        and item["source_sha256"] != policy.index["symbols"].get(item["symbol"], {}).get("source_sha256")
    }
    # Ordinary summary advancement owns its check -> append -> lineage
    # handoff below. Do not republish its old span before appending it.
    selected = {
        symbol
        for symbol, item in work.items()
        if symbol not in failed_symbols
        and not (
            symbol in policy.completed_decisions
            and policy.satisfying_entry(symbol, policy.index["symbols"], changed_sources) is not None
        )
        and (
            symbol not in summarized
            or item["acquire"]
            or item["invalidate_pricing"]
            or item["refresh_metadata"]
            or symbol in accepted["checks"]["checks"]
            or set(item.get("reasons", ())) & {"changed_rules", "sourced_facts"}
        )
    }
    lineage_results = []

    def lineage_phase(name, labels, publications=()):
        callback = (lambda stage, values: progress(name + "_" + stage, values)) if progress else None
        if progress:
            progress(name, {"state": "started", "selected": len(labels)})
        value = run_lineage(
            store,
            sorted(labels),
            identity_client=identity_client,
            pricing_client=pricing_client,
            evidence_client=evidence,
            end_date=end,
            register_discoveries=register,
            work_policy=policy,
            context=context,
            progress=callback,
            on_pricing=callback,
            pricing_handoff={item["symbol"]: item for item in publications if "source_sha256" in item},
        )
        if progress:
            progress(name, {"state": "completed", "failed": value["failed"]})
        lineage_results.append(value)
        result["stages"].setdefault(name, []).append(value)
        return value

    if selected:
        # The policy distinguishes unfinished evidence from an outstanding
        # approval. An unconditional refresh would discard a completed decision.
        lineage_phase("discovery_lineage", selected, pricing["results"])
    result["stages"].setdefault("discovery_lineage", [])
    if not pending_symbols(store):
        checked = refresh_required_history(
            store,
            pricing_client=pricing_client,
            work_policy=policy,
            end_date=end,
            context=context,
            progress=progress,
        )
        result["stages"]["required_history"] = checked
        if checked["checked"]:
            lineage_phase(
                "required_history_lineage",
                [s for s in checked["checked"] if policy.allowed({s})],
                checked["results"],
            )
    # Preserve the established check -> correction -> append sequence.
    maintained = (
        {"complete": False, "failed": 0, "deferred": "pending lineage prevents live history selection"}
        if pending_symbols(store)
        else update_everything(
            store,
            end,
            pricing_client=pricing_client,
            identity_client=identity_client,
            workers=workers,
            progress=progress,
            evidence_client=evidence,
            register_discoveries=register,
            recover_pending=False,
            work_policy=policy,
            late_bar_repairs=accepted["late_bar_repairs"],
            context=context,
        )
    )
    result["stages"]["maintenance_after_discovery"] = maintained

    @traced("completion_boundary")
    def completion_boundary():
        with storage.publication_lock(root):
            if (root / ".pending.json").exists():
                result["failed"] += 1
                result["error"] = "lineage publication remains pending; completed work is not acknowledged"
                return None
            index = context.select_index(root)
            require_complete(index)
            return index, read_checks(store)["checks"]

    boundary = completion_boundary()
    if boundary is None:
        return None
    index, pending = boundary
    # Fresh source-bound approval can complete unchanged published work here.
    # DO NOT route a retry through lineage merely to stamp its workflow ID:
    # that changes every downstream generation despite unchanged data.
    completed = {s: policy.satisfying_entry(s, index["symbols"], pending) for s in policy.work}
    # Keep this coherent result until a publication changes the boundary.
    # DO NOT rerun completion checks once per acknowledgement for reassurance.
    finish = [
        s
        for s, item in policy.work.items()
        if item.get("required_history_check")
        and s in policy.history_approved
        and policy.allowed({s})
        and not completed[s]
    ]
    if finish:
        lineage_phase("approval", finish)
        boundary = completion_boundary()
        if boundary is None:
            return None
        index, pending = boundary
        completed = {s: policy.satisfying_entry(s, index["symbols"], pending) for s in policy.work}
    unfinished = []
    with universe_state.outcome_batch(store) as batch:
        for symbol, item in policy.work.items():
            if symbol in policy.blocked:
                continue
            entry = completed[symbol]
            if entry:
                universe_state.record_outcome(
                    store,
                    symbol,
                    batch=batch,
                    expected=item,
                    status="complete",
                    outcome={
                        "lineage_generation": entry["sha256"],
                        "source_generation": entry["source_sha256"],
                        "source_rows": entry["projection"]["source_rows"],
                        "unresolved_rows": entry["projection"]["unresolved_rows"],
                    }
                    if symbol in index["symbols"]
                    else {"lineage_generation": entry["sha256"], "outcome": "unavailable_auxiliary_pricing"},
                    attempts=1,
                )
            else:
                unfinished.append(symbol)
    if unfinished:
        policy.fail(unfinished, {"reason": "required pricing or lineage not complete"}, stage="completion")
    result["failures"] = [*policy.failures, *policy.command_failures]
    result["pending_work"] = universe_state.count_pending(store)
    # Workflow owns failed symbol obligations; command owners record failures
    # without a symbol separately. Stage totals describe these same outcomes.
    result["failed"] = len(result["failures"])
    result["requests"] = {"metadata_http_requests": identity_client.request_count - metadata_started}
    if pricing_started is not None:
        result["requests"]["pricing_http_requests"] = pricing_client.request_count - pricing_started
    return {
        "result": result,
        "history_complete": bool(
            initial_maintenance["complete"]
            and maintained["complete"]
            and not result["failed"]
            and not result["pending_work"]
            and not selection["deferred"]
        ),
    }


def serve_locked(store, histories, *, context, progress=None):
    """Consume completed history results without invoking their producers."""
    from libs.alpaca_serving_catalogue.producer import maintain

    result = histories["result"]
    try:
        serving = maintain(store, progress=progress, context=context)
    except (RuntimeError, OSError, ValueError) as error:
        serving = {"complete": False, "failed": 1, "error": str(error)}
    result["stages"]["serving_catalogue"] = serving
    result["failed"] += serving["failed"]
    result["failures"].extend(
        {"stage": "serving_catalogue", **failure}
        for failure in serving.get("failures", [{"error": serving["error"]}] if "error" in serving else [])
    )
    result["complete"] = histories["history_complete"] and serving["complete"] and not result["failed"]
    if progress:
        progress(
            "completion",
            {
                "complete": result["complete"],
                "pending_work": result["pending_work"],
                "requests": result["requests"],
            },
        )
    return result


def rebuild_universe(store):
    """Initialize intentions explicitly, independently of later lineage construction."""
    store = Path(store).absolute()
    pricing_storage.initialize_store(store)
    with universe_state.update_lock(store):
        state = universe_state.initialize(store)
        return {
            "initialized": True,
            "schema_version": state["schema_version"],
            "pending_work": universe_state.pending_count(state),
            "registration_pending": len(state["registration_pending"]),
        }


def reconcile_universe(store, end_date, *, identity_client, registry=DEFAULT_REGISTRY):
    """Accept one observation and register its selected work without acquisition."""
    context = accept_context(store)
    with universe_state.update_lock(store):
        recover_and_accept_correlation_policy(store, context)
        accepted = universe_plan.boundaries(store, registry, context=context)
        observation = observe_catalogue(identity_client)
        evidence = EvidenceClient(identity_client, observation=observation)
        selection = universe_plan.select_work(accepted, observation, evidence, end_date)
        assert_current(store, accepted["summary"]["token"])
        universe_state.accept(store, observation, selection["registration_additions"], selection["work"])
        registration = universe_state.register(store, registry=registry)
        policy = WorkPolicy(
            store,
            accepted["index"],
            selection["work"],
            selection["deferred"],
            end_date,
            context=context,
        )
        for symbol, failure in selection["selection_failures"].items():
            policy.fail({symbol}, failure, stage="selection")
        return {
            "registration": registration,
            "pending_work": len(selection["work"]),
            "catalogue_observed_at_utc": observation["catalogue_observed_at_utc"],
        }

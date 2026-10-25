"""Discovery, registration and selective reconciliation around pricing/lineage owners."""

from pathlib import Path

from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.check_summary import assert_current, pending_symbols
from libs.alpaca_pricing_download.download import request_end
from libs.alpaca_pricing_download.update_checks import read_checks
from libs.alpaca_recovery_registry import DEFAULT_REGISTRY

from . import storage, universe_plan, universe_state
from .catalogue import observe_catalogue
from .context import accept_context
from .contract import LineageError
from .metadata import EvidenceClient
from .pipeline import run_lineage
from .refresh import refresh_required_history
from .registration import command_policy
from .update import update_everything
from .work_contract import WorkPolicy, acquisition_fingerprint, auxiliary_work, pricing_target, satisfying_entry


def recover_publications(store):
    """Roll forward known journals only; retry selection owns subsequent network work."""
    selected = set(pending_symbols(store)) | set(read_checks(store)["checks"])
    recovered = []
    for symbol in sorted(selected):
        root = pricing_storage.published_path(store, f"end_of_day/pricing/{symbol}")
        if (root / ".pending.json").exists():
            with pricing_storage.ticker_lock(root, exclusive=True):
                if pricing_storage.recover(root):
                    recovered.append(symbol)
    with storage.writer_lock(store) as root:
        lineage = storage.recover(store, root)
    return {"pricing_recovered": recovered, "lineage_recovered": lineage, "failed": 0}


def plan_update(store, end_date, *, identity_client, registry=DEFAULT_REGISTRY, retry_blocked=False):
    end = request_end(end_date).date().isoformat()
    metadata_before = identity_client.request_count
    observation = observe_catalogue(identity_client)
    try:
        accepted = universe_plan.boundaries(store, registry)
    except (RuntimeError, OSError, ValueError) as error:
        return {
            "mode": "plan",
            "end_exclusive": end,
            "blockers": [str(error)],
            "catalogue_observed_at_utc": observation["catalogue_observed_at_utc"],
            "catalogue_records": len(observation["records"]),
            "selection_metadata_http_requests_observed": identity_client.request_count - metadata_before,
        }
    evidence = EvidenceClient(identity_client, observation=observation)
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
):
    context = accept_context(store)
    store = Path(store).absolute()
    end = request_end(end_date).date().isoformat()
    result = {"complete": False, "end_exclusive": end, "failed": 0, "stages": {}}
    metadata_started = identity_client.request_count
    pricing_started = getattr(pricing_client, "request_count", None)
    with universe_state.update_lock(store):
        # Fail before network or any recovery if durable state was never initialized.
        universe_state.read_state(store)
        from libs.alpaca_serving_catalogue import work as serving_work

        # Daily completion includes serving publication. Adoption is explicit and
        # offline; never silently bootstrap from an ordinary network update.
        context.select_serving(recover=True)
        from .projection import require_complete

        root = store / "end_of_day/lineage"
        # Recover SQLite's own interrupted transaction before accepting compact
        # selectors. No payload repair or catalogue bootstrap occurs here.
        with storage.writer_lock(store), storage.publication_lock(root):
            serving_work.read(store)
            context.index = storage.read_index(root)
            require_complete(context.index)
        if not read_checks(store)["complete"]:
            raise LineageError("lineage scheduling coverage missing; explicit lineage update is required")
        registered = set()

        def register(symbols):
            additions = set(symbols) - registered
            if additions:
                universe_state.register(store, additions, registry=registry)
                registered.update(additions)

        # Finish already-journaled publication with its original inputs. New
        # dependency acquisitions are explicitly capped by this invocation.
        if progress:
            progress("recovery", {"state": "started"})
        recovered = recover_publications(store)
        if progress:
            progress("recovery", {"state": "completed", **recovered})
        result["stages"]["recovery"] = recovered
        if recovered["failed"]:
            result["failed"] = recovered["failed"]
            return result
        pending = pending_symbols(store)
        if pending:
            # Recovery selects the summary's named inputs and their complete groups.
            # Pass an explicit selection: the default lineage command also selects
            # every durable obligation, including ordinary next-session appends.
            # Never plan a universe against a missing/temporarily pending summary.
            recovery_policy = command_policy(store, end, context=context, retry_blocked=retry_blocked)
            callback = (lambda stage, values: progress("recovery_" + stage, values)) if progress else None
            recoverable = [symbol for symbol in pending if recovery_policy.allowed({symbol})]
            value = (
                run_lineage(
                    store,
                    recoverable,
                    identity_client=identity_client,
                    pricing_client=pricing_client,
                    end_date=end,
                    work_policy=recovery_policy,
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
        accepted = universe_plan.boundaries(store, registry, context=context)
        metadata_before = identity_client.request_count
        if progress:
            progress("catalogue", {"state": "started"})
        observation = observe_catalogue(identity_client)
        if progress:
            progress(
                "catalogue",
                {
                    "state": "completed",
                    "records": len(observation["records"]),
                    "http_requests": identity_client.request_count - metadata_before,
                },
            )
        evidence = EvidenceClient(identity_client, observation=observation)
        registered.update(accepted["registry"])
        selection = universe_plan.select_work(accepted, observation, evidence, end, retry_blocked=retry_blocked)
        result["plan"] = universe_plan.preview(
            accepted,
            selection,
            observation,
            end,
            metadata_requests=identity_client.request_count - metadata_before,
        )
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
        universe_state.accept(store, observation, selection["registration_additions"], selection["work"])
        result["stages"]["registration"] = universe_state.register(store, registry=registry)
        if progress:
            progress("registration", {"state": "completed", **result["stages"]["registration"]})
        registered.update(selection["registration_additions"])
        for symbol in selection["already_published"]:
            universe_state.record_outcome(
                store, symbol, status="complete", outcome={"published_before_acknowledgement": True}
            )
        work = selection["work"]
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
        evidence.work_policy = policy
        for symbol, failure in selection["selection_failures"].items():
            policy.fail({symbol}, failure, stage="selection")
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
        # Ordinary summary advancement owns its check -> append -> lineage
        # handoff below. Do not republish its old span before appending it.
        selected = {
            symbol
            for symbol, item in work.items()
            if symbol not in failed_symbols
            and (
                symbol not in summarized
                or item["acquire"]
                or item["invalidate_pricing"]
                or item["refresh_metadata"]
                or symbol in accepted["checks"]["checks"]
                or "changed_rules" in item.get("reasons", ())
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
                context=context,
            )
        )
        result["stages"]["maintenance"] = maintained

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
            return result
        index, pending = boundary
        # A recovered maintenance failure cannot be acknowledged by a receipt
        # published before its new history check. Already stamped append results
        # satisfy this from the projection without reopening their complete groups.
        finish = [
            s
            for s, item in policy.work.items()
            if item.get("required_history_check")
            and s in policy.history_approved
            and policy.allowed({s})
            and not satisfying_entry(s, item, index["symbols"], pending)
        ]
        if finish:
            lineage_phase("approval", finish)
            boundary = completion_boundary()
            if boundary is None:
                return result
            index, pending = boundary
        for symbol, item in policy.work.items():
            if symbol in policy.blocked:
                continue
            entry = satisfying_entry(symbol, item, index["symbols"], pending)
            if entry:
                universe_state.record_outcome(
                    store,
                    symbol,
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
                policy.fail({symbol}, {"reason": "required pricing or lineage not complete"}, stage="completion")
        result["failures"] = policy.failures
        state = universe_state.read_state(store)
        result["pending_work"] = universe_state.pending_count(state)
        result["failed"] = (
            len(policy.failures) + sum(group["failed"] for group in lineage_results) + maintained["failed"]
        )
        from libs.alpaca_serving_catalogue.producer import maintain

        try:
            serving = maintain(store, progress=progress, context=context)
        except (RuntimeError, OSError, ValueError) as error:
            serving = {"complete": False, "failed": 1, "error": str(error)}
        result["stages"]["serving_catalogue"] = serving
        result["failed"] += serving["failed"]
        result["complete"] = (
            maintained["complete"]
            and serving["complete"]
            and not result["failed"]
            and not result["pending_work"]
            and not selection["deferred"]
        )
        result["requests"] = {"metadata_http_requests": identity_client.request_count - metadata_started}
        if pricing_started is not None:
            result["requests"]["pricing_http_requests"] = pricing_client.request_count - pricing_started
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
    with universe_state.update_lock(store):
        accepted = universe_plan.boundaries(store, registry)
        observation = observe_catalogue(identity_client)
        evidence = EvidenceClient(identity_client, observation=observation)
        selection = universe_plan.select_work(accepted, observation, evidence, end_date)
        assert_current(store, accepted["summary"]["token"])
        universe_state.accept(store, observation, selection["registration_additions"], selection["work"])
        registration = universe_state.register(store, registry=registry)
        policy = WorkPolicy(store, accepted["index"], selection["work"], selection["deferred"], end_date)
        for symbol, failure in selection["selection_failures"].items():
            policy.fail({symbol}, failure, stage="selection")
        return {
            "registration": registration,
            "pending_work": len(selection["work"]),
            "catalogue_observed_at_utc": observation["catalogue_observed_at_utc"],
        }

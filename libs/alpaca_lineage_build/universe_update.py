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
from .contract import LineageError, validate_version
from .metadata import EvidenceClient
from .pipeline import run_lineage
from .refresh import refresh_required_history
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
    validate_version()
    store = Path(store).absolute()
    end = request_end(end_date).date().isoformat()
    result = {"complete": False, "end_exclusive": end, "failed": 0, "stages": {}}
    with universe_state.update_lock(store):
        # Fail before network or any recovery if durable state was never initialized.
        universe_state.read_state(store)
        from .projection import require_complete

        require_complete(storage.read_index(store / "end_of_day/lineage"))
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
        recovered = recover_publications(store)
        result["stages"]["recovery"] = recovered
        if recovered["failed"]:
            result["failed"] = recovered["failed"]
            return result
        accepted = universe_plan.boundaries(store, registry, allow_pending=True)
        metadata_before = identity_client.request_count
        observation = observe_catalogue(identity_client)
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
        current = storage.read_index(store / "end_of_day/lineage")
        if current.get("projection_state") != accepted["index"].get("projection_state"):
            raise LineageError("lineage changed during discovery; repeat update selection")
        universe_state.accept(store, observation, selection["registration_additions"], selection["work"])
        result["stages"]["registration"] = universe_state.register(store, registry=registry)
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
            )
            pricing["results"].extend(value["results"])
            pricing["requests"][target] = value["requests"]
            pricing["failed"] += value["failed"]
        result["stages"]["acquisition"] = pricing
        policy.record_results(pricing, stage="acquisition")
        failed_symbols = {s for s in work if not policy.allowed({s})}
        selected = set(work) - failed_symbols
        refreshed = {s for s in selected if work[s]["refresh_metadata"]}
        lineage_results = []
        for symbols, refresh in ((refreshed, True), (selected - refreshed, False)):
            if symbols:
                lineage_results.append(
                    run_lineage(
                        store,
                        sorted(symbols),
                        identity_client=identity_client,
                        pricing_client=pricing_client,
                        evidence_client=evidence,
                        end_date=end,
                        refresh=refresh,
                        register_discoveries=register,
                        work_policy=policy,
                    )
                )
        result["stages"]["discovery_lineage"] = lineage_results
        if not pending_symbols(store):
            checked = refresh_required_history(
                store, pricing_client=pricing_client, work_policy=policy, end_date=end
            )
            result["stages"]["required_history"] = checked
            if checked["checked"]:
                lineage_results.append(
                    run_lineage(
                        store,
                        [s for s in checked["checked"] if policy.allowed({s})],
                        identity_client=identity_client,
                        pricing_client=pricing_client,
                        evidence_client=evidence,
                        end_date=end,
                        register_discoveries=register,
                        work_policy=policy,
                    )
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
            )
        )
        result["stages"]["maintenance"] = maintained
        # A recovered maintenance failure cannot be acknowledged by a receipt
        # published before its new history check. Stamp only after that handoff.
        finish = [
            s
            for s, item in policy.work.items()
            if item.get("required_history_check") and s in policy.history_approved and policy.allowed({s})
        ]
        if finish:
            lineage_results.append(
                run_lineage(
                    store,
                    finish,
                    identity_client=identity_client,
                    pricing_client=pricing_client,
                    evidence_client=evidence,
                    end_date=end,
                    register_discoveries=register,
                    work_policy=policy,
                )
            )
        root = store / "end_of_day/lineage"
        with storage.publication_lock(root):
            if (root / ".pending.json").exists():
                result["failed"] += 1
                result["error"] = "lineage publication remains pending; completed work is not acknowledged"
                return result
            index = storage.read_index(root)
            pending = read_checks(store)["checks"]
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
        result["complete"] = (
            maintained["complete"]
            and not result["failed"]
            and not result["pending_work"]
            and not selection["deferred"]
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

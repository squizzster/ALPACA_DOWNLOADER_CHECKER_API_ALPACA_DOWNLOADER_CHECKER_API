"""Project accepted component application inventory without lineage evaluation.

The caller owns the operation, writer and complete pricing-component locks. Its
selected receipts/index and pricing generations are already accepted handoffs.
Only current peer inventory markers are authority here; this owner cannot bind
facts, resolve ownership, select prices or acknowledge pricing/correction work.
An explicit metadata-only completion lends its proof to normal workflow acknowledgement.
"""

from pathlib import Path

from .contract import LineageError


def prepare(store, index, receipts, receipt_generations, pricing_generations, required_applications):
    """Produce complete new headers from the exact held accepted component."""
    original, updated, selected_receipts, selected_pricing, authorities = {}, {}, {}, {}, {}
    changed, unavailable = [], set()
    for symbol, requirements in required_applications.items():
        if symbol not in receipts:
            raise LineageError(f"{symbol}: missing selected application-inventory receipt")
        receipt = receipts[symbol]
        missing = {
            name: fingerprint
            for name, fingerprint in requirements.items()
            if receipt["evidence"].get("sourced_fact_applications", {}).get(name) != fingerprint
        }
        component = receipt["input_sources"]
        auxiliaries = receipt.get("unavailable_pricing", {})
        unavailable.update(auxiliaries)
        for name, expected in component.items():
            entry = index["symbols"].get(name)
            if (
                name not in receipts
                or entry is None
                or receipt_generations.get(name) != entry["sha256"]
                or receipts[name]["source_sha256"] != expected
                or entry["source_sha256"] != expected
                or pricing_generations.get(name) != expected
                or receipts[name]["input_sources"] != component
            ):
                raise LineageError(f"{symbol}: changed selected component; finish owning lineage work")
            selected_receipts[name] = receipt_generations[name]
            selected_pricing[name] = expected
            original[name] = receipts[name]
            updated.setdefault(name, receipts[name])
        if set(auxiliaries).intersection(pricing_generations):
            raise LineageError(f"{symbol}: formerly unavailable component now has pricing")
        if not missing:
            continue
        available = {name: receipts[name]["evidence"] for name in component}
        available.update((name, item["evidence"]) for name, item in auxiliaries.items())
        supplied = {}
        for application, fingerprint in missing.items():
            donor = next(
                (
                    name
                    for name, evidence in available.items()
                    if evidence.get("sourced_fact_applications", {}).get(application) == fingerprint
                ),
                None,
            )
            if donor is None:
                raise LineageError(f"{symbol}: no accepted component marker for {application}")
            supplied[application] = donor
        previous = updated[symbol]
        updated[symbol] = {
            **previous,
            "evidence": {
                **previous["evidence"],
                "sourced_fact_applications": {
                    **previous["evidence"].get("sourced_fact_applications", {}),
                    **missing,
                },
            },
        }
        authorities[symbol] = supplied
        changed.append(symbol)
    return {
        "store": str(Path(store).absolute()),
        "receipts": updated,
        "previous": original,
        "receipt_generations": selected_receipts,
        "pricing_generations": selected_pricing,
        "unavailable": sorted(unavailable),
        "required_applications": {name: dict(value) for name, value in required_applications.items()},
        "updated": sorted(changed),
        "authorities": authorities,
    }


def publish_locked(store, root, index, plan, pricing_generations, required_applications):
    """Publish only inventory, preserving all upstream and correction obligations."""
    if (
        str(Path(store).absolute()) != plan["store"]
        or Path(root).absolute() != Path(plan["store"]) / "end_of_day/lineage"
    ):
        raise LineageError("application-inventory plan belongs to another store")
    if set(plan["unavailable"]).intersection(pricing_generations):
        raise LineageError("application-inventory unavailable source acquired pricing")
    if any(required_applications.get(name) != value for name, value in plan["required_applications"].items()):
        raise LineageError("application-inventory requirement changed")
    if any(
        pricing_generations.get(name) != generation for name, generation in plan["pricing_generations"].items()
    ):
        raise LineageError("application-inventory pricing generation changed")
    expected = plan.get("result", {}).get("lineage_generations", plan["receipt_generations"])
    if any(index["symbols"].get(name, {}).get("sha256") != generation for name, generation in expected.items()):
        raise LineageError("application-inventory lineage generation changed")
    if "result" in plan:
        return plan["result"]
    if plan["updated"]:
        from . import storage

        storage.publish(
            Path(store).absolute(),
            root,
            index,
            plan["receipts"],
            {name: {} for name in plan["receipts"]},
            {},
            previous=plan["previous"],
            unchanged_decision=True,
            application_inventory_recovery=True,
        )
    from .recovery_handoffs import remember

    for name, receipt in plan["receipts"].items():
        remember(store, "receipt", (name, index["symbols"][name]["sha256"]), receipt)
    plan["result"] = {
        "status": "published" if plan["updated"] else "unchanged",
        "updated": plan["updated"],
        "authorities": plan["authorities"],
        "lineage_generations": {name: index["symbols"][name]["sha256"] for name in plan["receipts"]},
        "acknowledged_work": [],
    }
    return plan["result"]


def accept_metadata_completion(policy, index, plan, expected_work, pricing_generations):
    """Complete only the exact metadata-application job; never assert price coverage.

    The published inventory plan is the producer result. Current workflow and
    generation bindings establish applicability; held lookup values establish
    this separate job's admitted metadata scope once. Normal workflow
    acknowledgement consumes the resulting completed-decision handoff.
    """
    from .work_contract import WorkDeferred, lookup_fact, symbol_lookups

    result = plan.get("result")
    if result is None:
        raise WorkDeferred("application inventory has not completed publication")
    if str(Path(policy.store).absolute()) != plan["store"]:
        raise WorkDeferred("metadata completion belongs to another store")
    if any(
        index["symbols"].get(name, {}).get("sha256") != generation
        for name, generation in result["lineage_generations"].items()
    ) or any(
        pricing_generations.get(name) != generation for name, generation in plan["pricing_generations"].items()
    ):
        raise WorkDeferred("metadata completion selected generation changed")
    if not policy.end:
        raise WorkDeferred("metadata completion requires a frozen operation target")
    admitted = {"sourced_facts", "lineage_evidence"}
    fields = (
        "obligation_id",
        "end_exclusive",
        "required_group",
        "required_lookups",
        "required_sourced_facts",
        "evidence_fingerprint",
        "acquisition_evidence_fingerprint",
    )
    bindings = {}
    members = set(plan["receipts"]) | set(plan["unavailable"])
    for symbol, expected in expected_work.items():
        item = policy.work.get(symbol)
        required = plan["required_applications"].get(symbol)
        if item is None or any(item.get(name) != expected.get(name) for name in fields):
            raise WorkDeferred(f"{symbol}: metadata obligation changed")
        if (
            not required
            or item.get("required_sourced_facts") != required
            or not item.get("reasons")
            or set(item["reasons"]) - admitted
            or item.get("acquire")
            or item.get("invalidate_pricing")
            or item.get("required_history_check")
            or item.get("pricing_start")
            or item.get("correction_id")
            or item.get("pricing_correction")
            or item["end_exclusive"] > policy.end
            or (item.get("pricing_end_exclusive") or item["end_exclusive"]) > policy.end
            or set(item.get("required_group", ())) - members
            or symbol not in plan["receipts"]
            or not policy.allowed({symbol})
        ):
            raise WorkDeferred(f"{symbol}: obligation is not an admitted metadata-only application job")
        bindings[symbol] = {name: item.get(name) for name in fields}
    completed = plan.get("metadata_completion")
    if completed is not None:
        if completed["bindings"] != bindings:
            raise WorkDeferred("completed metadata obligation changed")
        # A restored consumer receives the producer's actual completed records,
        # rather than deriving their lookup scope or completion bindings again.
        policy.completed_decisions.update(completed["decisions"])
        return completed["result"]
    evidence = {name: receipt["evidence"] for name, receipt in plan["receipts"].items()}
    for receipt in plan["receipts"].values():
        evidence.update(
            (name, value["evidence"]) for name, value in receipt.get("unavailable_pricing", {}).items()
        )
    observed = symbol_lookups(evidence)
    for symbol in bindings:
        if any(
            label not in observed or lookup_fact(expected) != lookup_fact(observed[label])
            for label, expected in policy.work[symbol].get("required_lookups", {}).items()
        ):
            raise WorkDeferred(f"{symbol}: held metadata differs from the admitted obligation")
    accepted = {symbol: symbol for symbol in bindings}
    policy.accept_completion(plan["receipts"], index["symbols"], accepted)
    response = {"accepted_metadata_work": sorted(accepted), "price_coverage_asserted": False}
    plan["metadata_completion"] = {
        "bindings": bindings,
        "decisions": {name: policy.completed_decisions[name] for name in accepted},
        "result": response,
    }
    return response

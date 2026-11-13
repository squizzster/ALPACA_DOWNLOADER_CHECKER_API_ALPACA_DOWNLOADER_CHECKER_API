"""Shared documented-update decisions for production and explicit research.

Accepted owner plans and numerical decisions are inputs, not work to repeat.
This module performs no storage/provider I/O and never publishes. Callers own
selected complete groups and carry the completed result into their publisher.
"""

from libs.alpaca_end_of_day_security_correlation import documented_markets, documented_refinement
from libs.alpaca_end_of_day_security_correlation.documented_markets import refinement_ids as refinement_ids


def merge_plan(previous, fresh):
    if not previous:
        return fresh
    owners = {owner["source_fact_id"]: owner for owner in previous["owners"]}
    for owner in fresh["owners"]:
        old = owners.get(owner["source_fact_id"])
        if old is not None and old != owner:
            raise ValueError("changed accepted documented owner requires owning reconciliation")
        owners[owner["source_fact_id"]] = old if old is not None else owner
    routes = dict(previous["source_routes"])
    for name, route in fresh["source_routes"].items():
        if name in routes and routes[name] != route:
            raise ValueError("changed accepted documented source context requires owning reconciliation")
        routes[name] = route
    return {**fresh, "owners": list(owners.values()), "source_routes": routes}


def numeric_changes(before, after):
    """Only new owners/components reopen a contest; metadata does not prove prices."""
    changes = []
    for name, report in after.items():
        for segment in report["correlated"]["segments"]:
            first, last = segment["row_start"], segment["row_end"]
            covered = 0
            for old in before[name]["correlated"]["segments"]:
                if old["adjustment"] != segment["adjustment"]:
                    continue
                left, right = max(first, old["row_start"]), min(last, old["row_end"])
                if left > right:
                    continue
                covered += right - left + 1
                if old["security_identity"]["security_id"] != segment["security_identity"]["security_id"]:
                    changes.append(
                        {
                            "source": name,
                            "adjustment": segment["adjustment"],
                            "row_start": left,
                            "row_end": right,
                        }
                    )
            if not covered:
                changes.append(
                    {"source": name, "adjustment": segment["adjustment"], "row_start": first, "row_end": last}
                )
            elif covered != last - first + 1:
                # Effective accepted boundaries partition source positions. A new
                # piece must not partly overlap an accepted positive interval.
                raise ValueError("documented candidate has an unpartitioned numerical change")
    return changes


def project_plan(inputs, reports, evidence, plan, *, refine_fact_ids=(), selected_routes_only=False):
    """Apply one completed owner plan with explicit positive-refinement scope.

    None selects every owner; an empty set preserves every accepted positive.
    Allocation of unresolved rows remains bound to the plan's source routes.
    """
    if plan:
        raise ValueError("researched ownership plans are inadmissible for new inference")
    owners = (
        plan["owners"]
        if refine_fact_ids is None
        else [owner for owner in plan["owners"] if owner["source_fact_id"] in refine_fact_ids]
    )
    if owners:
        reports = documented_refinement.refine(inputs, reports, {**plan, "owners": owners})
    selected = reports
    if selected_routes_only:
        members = {uid for owner in plan["owners"] for uid in owner["identity"]["member_alpaca_asset_ids"]}
        selected = {
            name: report
            for name, report in reports.items()
            if members.intersection(plan["source_routes"].get(name, ()))
        }
    return {
        **reports,
        **documented_markets.project(
            {name: inputs[name] for name in selected},
            selected,
            evidence,
            plan=plan,
        ),
    }


def apply_updates(inputs, reports, evidence, receipts, entries):
    """Bind only newly asserted chronology and retain completed old owner plans."""
    if entries:
        raise ValueError("researched factual updates are inadmissible for new inference")
    evidence = dict(evidence)
    plans = []
    affected = set()
    fresh = {}
    for entry in entries:
        for name in entry["symbols"]:
            prior = {
                fact["match"]["source_id"]: fact
                for fact in evidence[name].get("sourced_market_occupancy", {}).get("facts", ())
            }
            for fact in entry["supplements"]:
                old = prior.get(fact["match"]["source_id"])
                if old is not None:
                    if old != fact:
                        raise ValueError("changed accepted fact requires owning documented reconciliation")
                    previous_intent = (
                        evidence[name].get("sourced_fact_intents", {}).get(fact["match"]["source_id"], True)
                    )
                    if previous_intent == entry.get("refine_accepted_dates", True):
                        continue
                fresh.setdefault(name, []).append(fact)
    if fresh:
        refine_ids = {
            fact["match"]["source_id"]
            for entry in entries
            if entry.get("refine_accepted_dates", True)
            for fact in entry["supplements"]
        }
        augmented = {
            name: {**evidence[name], "sourced_market_occupancy": {"schema_version": 1, "facts": values}}
            for name, values in fresh.items()
        }
        accepted = {
            name: {
                uid: identity
                for identity in receipt["identity_memberships"]
                for uid in identity["member_alpaca_asset_ids"]
            }
            for name, receipt in receipts.items()
        }
        plan = bind_updates(
            augmented,
            receipts,
            accepted_memberships=accepted,
            allow_containing_membership=True,
            allow_reclassification=True,
            allow_holding_exchange=True,
        )
        plans.append(plan)
        affected.update(
            name
            for name, report in reports.items()
            if any(
                set(segment["security_identity"]["member_alpaca_asset_ids"])
                <= set(owner["identity"]["member_alpaca_asset_ids"])
                for segment in report["correlated"]["segments"]
                for owner in plan["owners"]
            )
            or any(
                set(plan["source_routes"].get(name, ())) & set(owner["identity"]["member_alpaca_asset_ids"])
                for owner in plan["owners"]
            )
        )
        # Existing positives can be refined even where today's symbol lookup
        # identifies a later unrelated occupant. That does not authorize filling
        # unresolved observations under that foreign current source context.
        reports = project_plan(
            inputs,
            reports,
            augmented,
            plan,
            refine_fact_ids=refine_ids,
            selected_routes_only=True,
        )
        for name, value in augmented.items():
            prior = evidence[name].get("sourced_market_occupancy", {}).get("facts", ())
            evidence[name] = {
                **value,
                "sourced_market_occupancy": {
                    "schema_version": 1,
                    "facts": list(
                        {fact["match"]["source_id"]: fact for fact in (*prior, *fresh[name])}.values()
                    ),
                },
                "sourced_fact_intents": {
                    **evidence[name].get("sourced_fact_intents", {}),
                    **{
                        fact["match"]["source_id"]: entry.get("refine_accepted_dates", True)
                        for entry in entries
                        if name in entry["symbols"]
                        for fact in entry["supplements"]
                    },
                },
            }
            reports[name] = {
                **reports[name],
                "documented_identity_membership": merge_plan(
                    receipts[name].get("documented_identity_membership"), plan
                ),
            }
            affected.add(name)
    return reports, evidence, plans, affected


def pending_intent(previous, evidence):
    """A new scoped instruction is work even when its factual body is unchanged."""
    return any(
        intent != (previous or {}).get("evidence", {}).get("sourced_fact_intents", {}).get(identifier, True)
        for identifier, intent in evidence.get("sourced_fact_intents", {}).items()
    )


def bind_updates(evidence, previous, *, accepted_memberships, **options):
    """Bind new assertions once and consume already completed compatible owners."""
    completed = None
    accepted_facts = {}
    for receipt in previous.values():
        if not receipt:
            continue
        existing = receipt.get("documented_identity_membership")
        if existing is not None:
            completed = merge_plan(completed, existing)
        for fact in documented_markets.facts_for(receipt.get("evidence", {})):
            accepted_facts[fact["match"]["source_id"]] = fact
    bound = {owner["source_fact_id"] for owner in (completed or {}).get("owners", ())}
    selected = {}
    for name, report in evidence.items():
        fresh = []
        for fact in documented_markets.facts_for(report):
            identifier = fact["match"]["source_id"]
            if identifier in bound:
                if accepted_facts.get(identifier) != fact:
                    raise ValueError("changed accepted fact requires owning documented reconciliation")
            else:
                fresh.append(fact)
        selected[name] = {
            **report,
            "sourced_market_occupancy": {"schema_version": 1, "facts": fresh},
        }
    if any(documented_markets.facts_for(report) for report in selected.values()):
        fresh = documented_markets.bind(selected, accepted_memberships=accepted_memberships, **options)
        return merge_plan(completed, fresh)
    if completed is None:
        return documented_markets.bind(selected, accepted_memberships=accepted_memberships, **options)
    return completed

"""Plan new documented ownership before opening selected historical rows.

The metadata owner binds exact provider observations once. Accepted positives
and explicit numerical choices remain closed; only unresolved positions and a
genuinely appended suffix enter the new producer. Changed mappings or raw owner
evidence select the ordinary owning reconciliation instead.
"""

from pathlib import Path

from libs.alpaca_end_of_day_security_correlation import documented_markets
from libs.alpaca_security_lineage.sourced_facts import EXTENSIONS, MARKET_KIND, PUBLIC_COMMON_KIND, items

from .contract import CORRELATION_VERSION, LineageError
from .documented_updates import bind_updates, pending_intent, refinement_ids
from .resume import continuing_request
from .source_work_contract import SourceWorkPlan
from .source_work_contract import binding_applies as _binding_applies


def binding(controls, evidence):
    return tuple(
        (
            name,
            control.source_sha256,
            getattr(control, "previous_receipt_generation", None) or id(control.previous),
            id(evidence[name]),
        )
        for name, control in sorted(controls.items())
    )


def prepare(controls, evidence, auxiliary, cache, unchanged_owners):
    from libs.alpaca_security_lineage.inference_evidence import reject_assertions

    reject_assertions(evidence, where="retired researched plan")
    reject_assertions(auxiliary, where="retired auxiliary assertion evidence")
    new_facts = any(
        fact not in items((controls[name].previous or {}).get("evidence", {}), MARKET_KIND)
        for name, report in evidence.items()
        for fact in documented_markets.facts_for(report)
    )
    if auxiliary or not (
        new_facts or any(pending_intent(controls[name].previous, value) for name, value in evidence.items())
    ):
        return None
    for name, control in controls.items():
        prior = control.previous
        prefix = (
            control.accepted_reuse is not None
            and control.accepted_reuse[1]
            or prior is not None
            and any(
                change["source_sha256"] == prior["source_sha256"]
                and change["changed_from"] is not None
                and change["changed_from"] >= prior["processed_through"][:10]
                for change in control.source.get("history_changes", ())
            )
            and continuing_request(control.source, prior)
        )
        if not prior or not (_binding_applies(control) or prefix) or not unchanged_owners(name):
            return None
        old, new = prior["evidence"], evidence[name]
        old_markets, new_markets = items(old, MARKET_KIND), items(new, MARKET_KIND)
        if any(fact not in new_markets for fact in old_markets):
            raise LineageError("changed documented ownership requires owning dated-market reconciliation")
        if any(
            fact["facts"]["schema_version"] not in (3, 4, 5) for fact in new_markets if fact not in old_markets
        ) or any(
            items(old, kind) != items(new, kind)
            for kind in EXTENSIONS
            if kind != MARKET_KIND and not (CORRELATION_VERSION >= 91 and kind == PUBLIC_COMMON_KIND)
        ):
            return None
    common = None
    accepted_memberships = {
        name: {
            identifier: identity
            for identity in control.previous["identity_memberships"]
            for identifier in identity["member_alpaca_asset_ids"]
        }
        for name, control in controls.items()
    }
    if CORRELATION_VERSION >= 91:
        from .documented_continuity import bind as continuing_public

        common, completed_members = continuing_public(controls, evidence, cache)
        if common["certificates"]:
            accepted_memberships = dict.fromkeys(controls, completed_members)
    key = binding(controls, evidence)
    completed = cache.setdefault("documented_memberships", {})
    if key not in completed:
        completed[key] = bind_updates(
            evidence,
            {name: control.previous for name, control in controls.items()},
            accepted_memberships=accepted_memberships,
            allow_containing_membership=CORRELATION_VERSION >= 88,
            allow_reclassification=CORRELATION_VERSION >= 89,
            allow_holding_exchange=CORRELATION_VERSION >= 90,
        )
    documented = completed[key]
    members = {
        identifier: identity
        for control in controls.values()
        for identity in control.previous["identity_memberships"]
        for identifier in identity["member_alpaca_asset_ids"]
    }
    members.update(
        (identifier, owner["identity"])
        for owner in documented["owners"]
        for identifier in owner["identity"]["member_alpaca_asset_ids"]
    )
    labels = {episode["symbol"] for owner in documented["owners"] for episode in owner["episodes"]}
    owned_ids = {
        identifier
        for owner in documented["owners"]
        for identifier in owner["identity"]["member_alpaca_asset_ids"]
    }
    refine_dates = (
        CORRELATION_VERSION >= 91
        or CORRELATION_VERSION >= 90
        and any(
            fact["facts"]["schema_version"] == 5
            for report in evidence.values()
            for fact in documented_markets.facts_for(report)
        )
    )
    refine_ids = refinement_ids(evidence, documented, default=refine_dates)
    refine_dates = bool(refine_ids)
    stable_facts = {
        fact["match"]["source_id"]
        for report in evidence.values()
        for fact in documented_markets.facts_for(report)
        if fact["facts"]["schema_version"] == 5
    }
    dispositions = {}
    prepared_reports = {}
    for name, control in controls.items():
        applies = name in labels or owned_ids.intersection(documented["source_routes"].get(name, ()))
        needs_refinement = any(
            owner["source_fact_id"] in refine_ids
            and (
                name in {episode["symbol"] for episode in owner["episodes"]}
                or set(owner["identity"]["member_alpaca_asset_ids"]).intersection(
                    documented["source_routes"].get(name, ())
                )
            )
            for owner in documented["owners"]
        )
        # Unresolved rows are completed outcomes too. New common chronology
        # selects those rows only on a source to which its bound owner applies;
        # an unchanged unrelated peer keeps its accepted unresolved partition.
        # A changed pricing binding still requires its own reconciliation.
        selected = (
            applies and control.previous["unresolved_rows"] or not _binding_applies(control) or needs_refinement
        )
        changed_membership = any(
            members[identity["member_alpaca_asset_ids"][0]]["security_id"] != identity["security_id"]
            for identity in control.previous["identity_memberships"]
        )
        if selected and refine_dates and not control.previous["unresolved_rows"] and _binding_applies(control):
            from .source_work import accepted_choices

            choices = accepted_choices(control, Path(control.store) / "end_of_day/lineage", cache)
            if all(runs and all(run.state == "superseded" for run in runs) for runs in choices.runs.values()):
                boundaries = {
                    day
                    for owner in documented["owners"]
                    for episode in owner["episodes"]
                    for day in (episode["start_inclusive"], episode["end_exclusive"])
                    if day is not None
                }
                selected = any(
                    run.first is None
                    or run.last is None
                    or any(run.first < day <= run.last for day in boundaries)
                    for runs in choices.runs.values()
                    for run in runs
                )
        dispositions[name] = "B" if selected else "A" if changed_membership or needs_refinement else "retained"
        if dispositions[name] == "A" and needs_refinement:
            from .source_work import retained_ownership_report

            existing = getattr(control, "accepted_report", None)
            if existing is None:
                report_key = name, control.previous_receipt_generation, control.source_sha256
                accepted = cache.setdefault("accepted_ownership_reports", {})
                if report_key not in accepted:
                    accepted[report_key] = retained_ownership_report(
                        control.previous,
                        control.local,
                        {},
                        Path(control.store) / "end_of_day/lineage",
                        cache.setdefault("assignments", {}),
                    )
                existing = accepted[report_key]
            prepared_reports[name] = existing
        if selected and not applies:
            return None
    prior = next(iter(controls.values())).previous
    empty = {"version": 1, "certificates": [], "issues": []}
    return SourceWorkPlan(
        dispositions,
        members,
        prior.get("sourced_rename_membership") or empty,
        common or prior.get("public_common_continuation_membership") or empty,
        {},
        prior.get("security_relations"),
        {},
        "A" in dispositions.values(),
        documented_membership=documented,
        refine_accepted_dates=refine_dates,
        refinement_fact_ids=refine_ids,
        refinement_policy_version=91 if CORRELATION_VERSION >= 91 else 90,
        prepared_reports=prepared_reports,
        stable_numerical_identities=frozenset(
            owner["identity"]["security_id"]
            for owner in documented["owners"]
            if owner["source_fact_id"] in stable_facts
        ),
    )


def project(view, control, evidence, plan, policy, cache):
    """Consume a completed projection or produce only the selected new scope."""
    key = (
        view.symbol,
        control.source_sha256,
        getattr(control, "previous_receipt_generation", None) or id(control.previous),
        id(evidence),
    )
    completed = cache.setdefault("documented_reports", {})
    if key not in completed:
        from .recapitalization_work import append_unresolved

        report = append_unresolved(view, control.previous, view.accepted_report)
        from .documented_updates import project_plan

        completed[key] = project_plan(
            {view.symbol: view},
            {view.symbol: report},
            {view.symbol: evidence},
            plan.documented_membership,
            refine_fact_ids=plan.refinement_fact_ids if plan.refine_accepted_dates else (),
        )[view.symbol]
    report = completed[key]
    report["correlation_policy"] = policy.provenance()
    report["sourced_rename_membership"] = plan.rename_membership
    report["public_common_continuation_membership"] = plan.common_membership
    report["security_relations"] = plan.relation_projection
    return report

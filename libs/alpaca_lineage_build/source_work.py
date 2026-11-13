"""Metadata closure's completed source-work selection and membership assembly.

A closed source is a typed accepted control, never an empty PricingInput. The
producer plans source work before a pricing loader can run. Legacy assignment
assembly is explicit output construction and cannot open historical prices.
"""

from copy import deepcopy
from pathlib import Path

from libs.alpaca_end_of_day_security_correlation import security_relations
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.inference_evidence import provider_evidence, reject_assertions

from . import source_observations, storage
from .assignment_handoff import produce, selected
from .contract import COMPUTATION_REVISION, VERSION, LineageError, document_digest, versions
from .observation_carry import membership_choices
from .pricing import ClosedAcceptedSource
from .resume import _evidence_facts, continuing_request, unchanged_rows
from .source_work_contract import SourceWorkPlan, require_generic_plan
from .source_work_contract import binding_applies as _binding_applies


def _provider_facts(evidence):
    return _evidence_facts(evidence, append=True, historical=True)


def _accepted_owner_scope(previous):
    """Consume source-bound ownership, independently of its public component."""
    evidence, symbol = previous["evidence"], previous["symbol"]
    members = {
        identifier for item in previous["identity_memberships"] for identifier in item["member_alpaca_asset_ids"]
    }
    labels = {symbol}
    owners = {
        (item.get("asset") or {}).get("id")
        for item in evidence.get("asset_lookups", ())
        if item["lookup_kind"] == "symbol" and item["lookup"] == symbol
    } | {
        item.get("asset_id")
        for item in evidence.get("symbol_occupancy_evidence", ())
        if item.get("symbol") == symbol
    }
    for _, interval in previous.get("observation_routes", {}).values():
        owners.add(interval.get("asset_id"))
        if interval.get("historical_symbol"):
            labels.add(interval["historical_symbol"])
    current = previous.get("current_source_ownership")
    if current:
        owners.add(current["asset_id"])
    rename = previous.get("sourced_rename_ownership")
    if rename:
        for certificate in previous["sourced_rename_membership"]["certificates"]:
            if certificate["action_id"] == rename["action_id"]:
                owners.update((certificate["old_id"], certificate["new_id"]))
                labels.update((certificate["old_symbol"], certificate["new_symbol"]))
                labels.update(item[0] for item in certificate["labels"])
    common = previous.get("public_common_continuation_ownership")
    if common:
        for certificate in previous["public_common_continuation_membership"]["certificates"]:
            if certificate["source_fact_id"] == common["source_fact_id"]:
                for role in ("predecessor", "successor"):
                    owners.add(certificate[role]["asset_id"])
                    labels.add(certificate[role]["historical_symbol"])
        for action in evidence.get("corporate_actions", ()):
            if action.get("id") in common["route"]:
                labels.update((action["old_symbol"], action["new_symbol"]))
    for view in (previous.get("serving_ownership") or {}).get("views", {}).values():
        for run in view["ownership"]:
            labels.update(run["owner"][1] or ())
    return owners & members, labels


def _accepted_owners_apply(previous, after, *, scope=None, prior_evidence=None):
    """Compare prior observations and new evidence relevant to accepted owners.

    New dependency observations do not reopen accepted owners. Preserve every
    prior observation for comparison, and admit new contradictions only within
    the prior source/owner scope, never the newly joined membership component.
    Pricing generation and effective mapping are checked by the caller.
    """
    left = _provider_facts(previous["evidence"] if prior_evidence is None else prior_evidence)
    right = _provider_facts(after)
    owner_ids, labels = _accepted_owner_scope(previous) if scope is None else scope
    cusips = {
        item["lookup"]
        for evidence in (left, right)
        for item in evidence.get("asset_lookups", ())
        if item["lookup_kind"] == "cusip" and (item.get("asset") or {}).get("id") in owner_ids
    }

    def relevant(value):
        if isinstance(value, list):
            return any(relevant(item) for item in value)
        if not isinstance(value, dict):
            return False
        return any(
            relevant(item)
            or isinstance(item, str)
            and (
                (key == "id" or key.endswith("asset_id"))
                and item in owner_ids
                or key.endswith("symbol")
                and item in labels
                or key.endswith("cusip")
                and item in cusips
            )
            for key, item in value.items()
        )

    if owner_ids:
        for field in ("corporate_actions", "symbol_changes", "symbol_occupancy_evidence"):
            prior_records = left.get(field, ())
            prior_ids = {item["id"] for item in prior_records if item.get("id") is not None}
            right[field] = [
                item
                for item in right.get(field, ())
                if item in prior_records or item.get("id") in prior_ids or relevant(item)
            ]
        # Counts and unresolved endpoints describe the selected action union;
        # they cannot independently expand historical ownership work.
        right["corporate_action_count"] = len(right.get("corporate_actions", ()))
        right["unresolved_cusips"] = [
            value
            for value in right.get("unresolved_cusips", ())
            if value in left.get("unresolved_cusips", ()) or value in cusips
        ]
    old_lookups = {(item["lookup_kind"], item["lookup"]) for item in left.get("asset_lookups", ())}
    right["asset_lookups"] = [
        item
        for item in right.get("asset_lookups", ())
        if (item["lookup_kind"], item["lookup"]) in old_lookups
        or (
            item["lookup"] in (labels if item["lookup_kind"] == "symbol" else cusips)
            and (item.get("asset") or {}).get("id") not in owner_ids
        )
    ]
    old_ids = {item["id"] for item in left.get("identities", ())}
    right["identities"] = [item for item in right.get("identities", ()) if item["id"] in old_ids]
    disposition = right.get("symbol_disposition")
    if isinstance(disposition, dict) and "resolved_alpaca_asset_ids" in disposition:
        disposition["resolved_alpaca_asset_ids"] = [
            identifier for identifier in disposition["resolved_alpaca_asset_ids"] if identifier in old_ids
        ]
    for evidence in (left, right):
        disposition = evidence.get("symbol_disposition")
        if isinstance(disposition, dict):
            # A metadata-only control cannot know the new total row count of
            # an approved append. That technical capability does not invalidate
            # accepted historical owners; pricing already owns the prefix.
            disposition.pop("local_market_data", None)
        for identity in evidence.get("identities", ()):
            # Lookup provenance does not alter an already accepted provider ID.
            identity.pop("lookup_cusips", None)
            identity.pop("lookup_symbols", None)
        evidence["asset_lookups"] = sorted(
            evidence.get("asset_lookups", ()), key=lambda item: (item["lookup_kind"], item["lookup"])
        )
        evidence["identities"] = sorted(evidence.get("identities", ()), key=lambda item: item["id"])
        # Group closure is newly established above; existing per-source owners
        # are not invalidated solely by additional metadata query membership.
        evidence.pop("queried_symbols", None)
    return left == right


def prepare_group_evidence_and_source_work(
    controls, evidence, auxiliary, *, cache, ruleset=None, accepted_membership_plan=None
):
    """Select ordinary source work and any explicitly enabled scoped refinement."""
    plan = _prepare_group_evidence_and_source_work(controls, evidence, auxiliary, cache=cache)
    if ruleset is None or ruleset.membership_refinement is None:
        return plan
    from .membership_updates import prepare
    from .transform import retained_decision_report

    def report_loader(name):
        control = controls[name]
        report = (plan.prepared_reports or {}).get(name)
        if report is None:
            report = {
                **control.previous,
                **retained_decision_report(control.previous, policy=ruleset.policy),
                **retained_ownership_report(
                    control.previous,
                    control.local,
                    {},
                    Path(control.store) / "end_of_day/lineage",
                    cache.setdefault("assignments", {}),
                ),
            }
        return report

    return prepare(
        controls, evidence, auxiliary, plan, ruleset, cache, report_loader, completed=accepted_membership_plan
    )


def _prepare_group_evidence_and_source_work(controls, evidence, auxiliary, *, cache):
    """Select closed accepted owners or generic correlation from provider inputs."""
    binding = tuple((name, id(item)) for name, item in sorted({**evidence, **auxiliary}.items()))
    plan_key = (
        binding,
        tuple(
            (
                name,
                control.store,
                control.source_sha256,
                getattr(control, "previous_receipt_generation", None) or id(control.previous),
            )
            for name, control in sorted(controls.items())
        ),
    )
    plans = cache.setdefault("plans", {})
    if plan_key in plans:
        return plans[plan_key]
    reject_assertions(evidence, where="source-work provider evidence")
    reject_assertions(auxiliary, where="source-work auxiliary evidence")
    applicability = cache.setdefault("owner_applicability", {})
    candidates = {}
    for control in controls.values():
        for identity in (control.previous or {}).get("identity_memberships", ()):
            for identifier in identity["member_alpaca_asset_ids"]:
                candidates.setdefault(identifier, {})[identity["security_id"]] = identity
    # Each source keeps its own accepted component. Incomparable peer scopes do
    # not manufacture a new union merely because they contain a common UUID.
    memberships = {
        identifier: next(iter(owners.values())) for identifier, owners in candidates.items() if len(owners) == 1
    }
    dispositions = {}
    for name, control in controls.items():
        prior = control.previous
        if not prior or not _binding_applies(control):
            dispositions[name] = "C"
            continue
        key = name, getattr(control, "previous_receipt_generation", None) or id(prior), id(evidence[name])
        if key not in applicability:
            held = getattr(control, "accepted_provider_evidence", None)
            if held is None:
                held = provider_evidence(prior["evidence"])
                control.accepted_provider_evidence = held
            applicability[key] = _accepted_owners_apply(prior, evidence[name], prior_evidence=held)
        if applicability[key]:
            if not control.local["counts_known"]:
                raise LineageError(
                    "accepted source lacks compact counts; explicit assignment projection required"
                )
            dispositions[name] = "retained"
        else:
            dispositions[name] = "C"
    empty = {"version": 1, "certificates": [], "issues": []}
    relations = {"version": 1, "relations": [], "issues": []}
    plan = SourceWorkPlan(dispositions, memberships, empty, empty, relations, relations, {}, False)
    plans[plan_key] = plan
    return plan


def closed_source(control, plan):
    scoped_membership = bool(plan.membership_refinement and plan.membership_refinement["changes"])
    memberships = (
        {
            identifier: identity
            for identity in control.previous["identity_memberships"]
            for identifier in identity["member_alpaca_asset_ids"]
        }
        if scoped_membership
        else plan.memberships
        if plan.dispositions[control.symbol] == "A"
        else {
            **plan.memberships,
            **{
                identifier: identity
                for identity in control.previous["identity_memberships"]
                for identifier in identity["member_alpaca_asset_ids"]
            },
        }
    )
    report = None if scoped_membership else getattr(control, "accepted_report", None)
    if report is None:
        report = (plan.prepared_reports or {}).get(control.symbol)
    if report is not None and not scoped_membership:
        projections = plan.closed_report_projections
        refine_metadata = plan.refine_accepted_dates and plan.dispositions[control.symbol] == "A"
        key = (
            control.store,
            control.symbol,
            control.previous_receipt_generation,
            control.source_sha256,
            id(plan.memberships),
            id(plan.documented_membership),
            refine_metadata,
        )
        if key not in projections:
            projected = rebind_retained_report(report, control.previous, memberships)
            projections[key] = projected
        report = projections[key]
    return ClosedAcceptedSource(
        control.store,
        control.symbol,
        control.source,
        control.source_sha256,
        control.local,
        control.previous,
        previous_receipt_generation=control.previous_receipt_generation,
        accepted_reuse=control.accepted_reuse,
        accepted_report=report,
        accepted_identity_memberships=memberships,
        accepted_public_common_membership=control.previous.get("public_common_continuation_membership"),
        accepted_security_relations=control.previous.get("security_relations"),
        accepted_sourced_rename_membership=control.previous.get("sourced_rename_membership"),
        accepted_documented_membership=control.previous.get("documented_identity_membership"),
        accepted_common_bounds=plan.common_bounds,
    )


def rebind_retained_report(report, previous, memberships):
    """Project a completed owner report into an actually changed component.

    Its dated asset owners, labels, positions and numerical states remain
    accepted. Only public-component references change; no rows are opened and
    no assignment or numerical decision is reconstructed.
    """
    changes = {
        identity["security_id"]: current
        for identity in previous["identity_memberships"]
        if (current := memberships.get(identity["member_alpaca_asset_ids"][0], identity))["security_id"]
        != identity["security_id"]
    }
    if not changes:
        return report
    segments = []
    for segment in report["correlated"]["segments"]:
        identity = changes.get(segment["security_identity"]["security_id"])
        segments.append(
            {
                **segment,
                "security_identity": identity,
                "canonical_security": {**segment["canonical_security"], "security_id": identity["security_id"]},
            }
            if identity is not None
            else segment
        )
    routes = {}
    for identifier, route in report.get("retained_observation_routes", {}).items():
        current = changes.get(identifier, {}).get("security_id", identifier)
        if current not in routes or routes[current][0] < route[0]:
            routes[current] = route
    return {
        **report,
        "correlated": {**report["correlated"], "segments": segments},
        "retained_observation_routes": routes,
    }


def assignment_documents(root, previous, cache, *, kinds=None):
    result = {}
    for kind, reference in previous["boundaries"].items():
        if kinds is not None and kind not in kinds:
            continue
        key = previous["symbol"], kind, reference["sha256"]
        if key not in cache:
            handoff = selected(previous, kind)
            cache[key] = (
                storage.read_assignment_projection(root, previous, kind, handoff)
                if handoff
                else storage.read_boundaries(root, previous, kinds={kind})[kind]
            )
        result[kind] = cache[key]
    return result


def project_closed_source(view, evidence, plan, policy, root, cache):
    """Assemble changed identity references, preserving every dated owner/value."""
    prior = view.previous
    receipt = {
        **prior,
        "schema_version": VERSION,
        "versions": versions(),
        "computation_revision": COMPUTATION_REVISION,
        "correlation_policy": policy.provenance(),
        "source_sha256": view.source_sha256,
        "pricing_boundaries": view.source["boundaries"],
        "pricing_request": {**view.source["latest_request"], "history_start": view.source.get("history_start")},
        "processed_through": view.source["latest_request"]["end_exclusive"],
        "evidence": evidence,
        "closed_source_handoff_version": 1,
        "current_source_ownership": prior.get("current_source_ownership"),
        "sourced_rename_ownership": prior.get("sourced_rename_ownership"),
        "sourced_rename_membership": prior.get("sourced_rename_membership"),
        "public_common_continuation_membership": prior.get("public_common_continuation_membership"),
        "public_common_continuation_ownership": prior.get("public_common_continuation_ownership"),
        "documented_identity_membership": prior.get("documented_identity_membership"),
        "security_relations": prior.get("security_relations", plan.relation_projection),
        "membership_only_rebind": None,
    }
    memberships = view.accepted_identity_memberships
    id_map = {
        identity["security_id"]: memberships[identity["member_alpaca_asset_ids"][0]]
        for identity in prior.get("identity_memberships", ())
    }
    changes = {old: identity for old, identity in id_map.items() if old != identity["security_id"]}
    receipt["identity_memberships"] = list(
        {identity["security_id"]: identity for identity in id_map.values()}.values()
    )
    documents = {}
    dated_metadata = (
        view.accepted_report if plan.refine_accepted_dates and plan.dispositions[view.symbol] == "A" else None
    )
    if changes or dated_metadata is not None:
        from .transform import ASSIGNMENT_FIELDS

        projected = {
            (segment["adjustment"], segment["row_start"], segment["row_end"]): segment
            for segment in (dated_metadata or {}).get("correlated", {}).get("segments", ())
        }
        old_documents = assignment_documents(root, prior, cache)
        receipt["boundaries"] = deepcopy(prior["boundaries"])
        receipt["assignment_handoffs"] = dict(prior.get("assignment_handoffs", {}))
        for kind, old in old_documents.items():
            changed = deepcopy(old)
            dirty = False
            for entry, original_entry in zip(changed["adjustments"], old["adjustments"]):
                for segment, original_segment in zip(entry["segments"], original_entry["segments"]):
                    if kind == "current_year" and segment["from"][:7] >= prior["anchor_month"]:
                        continue
                    identity = original_segment.get("security_identity")
                    if identity and identity["security_id"] in changes:
                        segment["security_identity"] = changes[identity["security_id"]]
                        segment["canonical_security"] = {
                            **segment["canonical_security"],
                            "security_id": segment["security_identity"]["security_id"],
                        }
                        dirty = True
                    if dated_metadata is not None and identity is not None:
                        offset = prior["serving_ownership"]["offsets"][entry["adjustment"]][kind]
                        value = projected[
                            entry["adjustment"], offset + segment["row_start"], offset + segment["row_end"]
                        ]
                        fields = {key: value[key] for key in ASSIGNMENT_FIELDS if key in value}
                        dirty |= any(segment.get(key) != val for key, val in fields.items())
                        segment.update(fields)
            if dirty:
                changed["schema_version"] = VERSION
                generation = document_digest(changed)
                receipt["boundaries"][kind] = {**prior["boundaries"][kind], "sha256": generation}
                receipt["assignment_handoffs"][kind] = produce(changed, generation)
                documents[kind] = changed
        receipt["assignment_handoff_kinds"] = sorted(receipt["assignment_handoffs"])
        receipt["identity_intervals"] = [
            {**interval, "security_id": id_map[interval["security_id"]]["security_id"]}
            for interval in prior["identity_intervals"]
        ]
        receipt["observation_routes"] = {}
        for old, route in prior["observation_routes"].items():
            new = id_map[old]["security_id"]
            if new not in receipt["observation_routes"] or receipt["observation_routes"][new][0] < route[0]:
                receipt["observation_routes"][new] = route
        if dated_metadata is not None:
            receipt["observation_routes"] = {}
            for segment in dated_metadata["correlated"]["segments"]:
                if segment["adjustment"] == "raw" and segment["historical_symbol"] == view.symbol:
                    receipt["observation_routes"][segment["security_identity"]["security_id"]] = [
                        segment["last_timestamp"],
                        segment.get("ticker_ownership_interval") or {},
                    ]
    return receipt, documents


def accepted_report(view, plan, root, cache):
    """Selected peer report assembly, only when the evaluator consults it."""
    if view.accepted_report is not None:
        return view.accepted_report
    view.accepted_report = retained_ownership_report(view.previous, view.local, plan.memberships, root, cache)
    return view.accepted_report


def retained_ownership_report(prior, local, memberships, root, cache, *, kinds=None):
    """Assemble accepted row decisions in complete-source positions, without prices."""
    documents = assignment_documents(root, prior, cache, kinds=kinds)
    # These logical input identities already belong to the selected pricing
    # handoff. Carry the same provenance on both descriptors and segments: peer
    # rules consume it alongside held rows, without hashing or reopening prices.
    # Closed metadata controls have no logical download digest or row reader;
    # projecting their accepted ownership must not invent either capability.
    sources = {
        item["adjustment"]: {
            "data_file": item["file"],
            **{
                key: item[key]
                for key in ("adjustment", "asof", "request_from", "request_to", "feed", "timeframe")
                if key in item
            },
            **(
                {
                    "download_manifest": item["file"] + ".manifest.json",
                    "download_sha256": item["sha256"],
                }
                if "sha256" in item
                else {}
            ),
        }
        for item in local["downloads"]
    }
    branches = {name: {"row_count": 0, "segments": []} for name in ("correlated", "uncorrelated")}
    for adjustment in ADJUSTMENTS:
        offset = 0
        for kind in ("completed_years", "current_year", "current_month"):
            if kind not in documents:
                continue
            if kinds is not None:
                # The accepted compact handoff already gives full-source
                # positions. A selected boundary does not need earlier bodies
                # reopened merely to count their rows again.
                offset = prior["serving_ownership"]["offsets"][adjustment][kind]
            entry = next(item for item in documents[kind]["adjustments"] if item["adjustment"] == adjustment)
            for old in entry["segments"]:
                if kind == "current_year" and old["from"][:7] >= prior["anchor_month"]:
                    continue
                segment = {
                    **old,
                    **sources[adjustment],
                    "row_start": offset + 1,
                    "row_end": offset + old["row_count"],
                    "first_session_date": old["from"],
                    "last_session_date": old["to"],
                }
                identity = segment.get("security_identity")
                if identity:
                    segment["accepted_row_ownership"] = True
                    segment["security_identity"] = memberships.get(
                        identity["member_alpaca_asset_ids"][0], identity
                    )
                    if segment.get("canonical_security"):
                        segment["canonical_security"] = {
                            **segment["canonical_security"],
                            "security_id": segment["security_identity"]["security_id"],
                        }
                branch = branches[segment.pop("branch")]
                branch["segments"].append(segment)
                branch["row_count"] += segment["row_count"]
                offset += segment["row_count"]
    identities = prior.get("identity_memberships")
    if identities is None:
        identities = {
            segment["security_identity"]["security_id"]: segment["security_identity"]
            for segment in branches["correlated"]["segments"]
        }.values()
    return {
        "source_row_count": prior["source_rows"],
        "source_downloads": [{**item, **sources[item["adjustment"]]} for item in local["downloads"]],
        **branches,
        "evidence_issues": prior["evidence_issues"],
        "row_partition_complete": prior["row_partition_complete"],
        "ownership_interval_rule_order": prior["ownership_interval_rule_order"],
        "row_correlation_rule_order": prior["row_correlation_rule_order"],
        "retained_observation_routes": {
            memberships.get(identity["member_alpaca_asset_ids"][0], identity)["security_id"]: prior[
                "observation_routes"
            ][identity["security_id"]]
            for identity in identities
            if identity["security_id"] in prior.get("observation_routes", {})
        },
        "ticker_ownership_intervals": list(
            {
                str(segment.get("ticker_ownership_interval")): segment["ticker_ownership_interval"]
                for segment in branches["correlated"]["segments"]
                if segment.get("ticker_ownership_interval")
            }.values()
        ),
    }


def selected_common_source_prerequisites(root, controls, plan, cache):
    """Assemble accepted dated owners/routes only for a selected new claim."""
    if not {"B", "C"}.intersection(plan.dispositions.values()):
        return {}, ()
    endpoints = {
        item[role]["asset_id"]
        for item in plan.common_membership["certificates"]
        for role in ("predecessor", "successor")
    }
    bounds, actions = {}, {}
    for control in controls.values():
        prior = control.previous
        if prior is None or not endpoints.intersection(
            identifier
            for identity in prior.get("identity_memberships", ())
            for identifier in identity["member_alpaca_asset_ids"]
        ):
            continue
        documents = assignment_documents(root, prior, cache)
        supported = set()
        for kind, document in documents.items():
            raw = next(item for item in document["adjustments"] if item["adjustment"] == "raw")
            for segment in raw["segments"]:
                if (
                    segment["branch"] != "correlated"
                    or kind == "current_year"
                    and segment["from"][:7] >= prior["anchor_month"]
                ):
                    continue
                identity = segment.get("canonical_security") or {}
                asset_id = identity.get("alpaca_asset_id") or identity.get("asset_id")
                if asset_id in endpoints:
                    old = bounds.get(asset_id)
                    bounds[asset_id] = (
                        (min(old[0], segment["from"]), max(old[1], segment["to"]))
                        if old
                        else (segment["from"], segment["to"])
                    )
                    supported.update(
                        (segment.get("ticker_ownership_interval") or {}).get(
                            "supporting_corporate_action_ids", ()
                        )
                    )
        for action in prior["evidence"].get("corporate_actions", ()):
            if (
                action.get("id") in supported
                and action.get("action_type") == "name_change"
                and action.get("old_cusip") == action.get("new_cusip")
            ):
                actions[action["id"]] = action
    return bounds, tuple(actions[key] for key in sorted(actions))


def accepted_donor_grids(control, cache):
    """Retain embedded offline checkpoints under their selecting body token."""
    from libs.alpaca_serving_catalogue.offline import previous_checkpoints

    selected = cache.get("donor_checkpoints", {})
    binding = tuple(sorted((key, value["decision_generation"]) for key, value in selected.items()))
    views = cache.setdefault("donor_checkpoint_views", {})
    if binding not in views:
        views[binding] = previous_checkpoints(selected.values())
    key = control.symbol, control.source_sha256, id(control.previous), binding
    complete = cache.setdefault("donor_grid_ids", {})
    if key not in complete:
        complete[key] = set().union(
            *(
                source_observations.complete_donor_ids(
                    control.symbol, control.previous["serving_ownership"], control.source_sha256, checkpoint
                )
                for checkpoint in views[binding]
            )
        )
    return complete[key]


def accepted_choices(control, root, cache):
    """Borrow the first accepted explicit-choice projection for this binding."""
    key = control.symbol, control.previous_receipt_generation, control.source_sha256
    choices = cache.setdefault("observation_choices", {})
    if key not in choices:
        choices[key] = membership_choices(
            control.previous,
            source_generation=control.source_sha256,
            receipt_generation=control.previous_receipt_generation,
            legacy_boundary=lambda kind: assignment_documents(
                root, control.previous, cache.setdefault("assignments", {})
            )[kind],
        )
    return choices[key]


def _source_work_scope(controls, evidence, auxiliary, policy, plan=None):
    return (
        policy.semantic_sha256
        if plan is None or not plan.membership_refinement
        else policy.semantic_sha256 + ":" + plan.membership_refinement["execution_sha256"],
        tuple(
            (
                name,
                control.store,
                control.source_sha256,
                getattr(control, "previous_receipt_generation", None) or id(control.previous),
                id(evidence[name]),
            )
            for name, control in sorted(controls.items())
        ),
        tuple((name, id(facts)) for name, facts in sorted(auxiliary.items())),
    )


def execute_source_work(store, root, controls, evidence, auxiliary, plan, policy, cache, *, loader):
    """Carry completed stages through publication retries under exact bindings."""
    completed = cache.setdefault("completed_source_work", {})
    scope = _source_work_scope(controls, evidence, auxiliary, policy, plan)
    existing = completed.get(scope)
    if existing is not None and "outputs" in existing:
        return existing["outputs"]
    if existing is None or "decisions" not in existing:
        require_generic_plan(plan)
        reject_assertions(evidence, where="source-work provider evidence")
        reject_assertions(auxiliary, where="source-work auxiliary evidence")
    handoff = completed.setdefault(scope, {"controls": tuple(controls.values()), "evidence": {}})
    handoff["evidence"].update((id(value), value) for value in (*evidence.values(), *auxiliary.values()))
    try:
        if "outputs" not in handoff:
            handoff["outputs"] = _execute_source_work(
                store, root, controls, evidence, auxiliary, plan, policy, cache, loader=loader, handoff=handoff
            )
        return handoff["outputs"]
    finally:
        # A selected pricing loader can complete previously unknown technical
        # row counts. This producer-owned metadata projection is the same scope;
        # carry its resulting binding rather than repeating completed decisions.
        completed[_source_work_scope(controls, evidence, auxiliary, policy, plan)] = handoff
        handoff["evidence"].update((id(value), value) for value in evidence.values())


def _execute_source_work(store, root, controls, evidence, auxiliary, plan, policy, cache, *, loader, handoff):
    """Materialize named work only; accepted peers have no pricing-reader API."""
    from .transform import make_publication

    if "decisions" not in handoff:
        handoff["decisions"] = _decide_source_work(
            store, root, controls, evidence, auxiliary, plan, policy, cache, loader=loader
        )
    closed, materialized, reports, observation_changes, source_equivalence = handoff["decisions"]
    receipts, documents = {}, {}
    parts = handoff.setdefault("publications", {})
    for name, view in closed.items():
        if name in parts:
            receipts[name], documents[name] = parts[name]
            continue
        receipts[name], documents[name] = project_closed_source(
            view, evidence[name], plan, policy, root, cache.setdefault("assignments", {})
        )
        if name in observation_changes:
            receipt = receipts[name]
            prior_documents = assignment_documents(root, view.previous, cache.setdefault("assignments", {}))
            for kind in receipt["boundaries"]:
                document = documents[name].get(kind, prior_documents[kind])
                changed = source_observations.apply_selection(
                    document,
                    observation_changes[name],
                    {
                        adjustment: view.previous["serving_ownership"]["offsets"][adjustment][kind]
                        for adjustment in ADJUSTMENTS
                    },
                )
                if changed != document:
                    generation = document_digest(changed)
                    documents[name][kind] = changed
                    receipt["boundaries"][kind] = {**receipt["boundaries"][kind], "sha256": generation}
                    receipt["assignment_handoffs"][kind] = produce(changed, generation)
        parts[name] = receipts[name], documents[name]
    for name, view in materialized.items():
        if name not in parts:
            parts[name] = make_publication(
                view, evidence[name], reports[name], [], previous=controls[name].previous
            )
        receipts[name], documents[name] = parts[name]
    # Final endpoint projection consumes completed membership after source work.
    if "relations" not in handoff:
        final_membership = dict(plan.memberships)
        for receipt in receipts.values():
            for identity in receipt["identity_memberships"]:
                for identifier in identity["member_alpaca_asset_ids"]:
                    final_membership[identifier] = identity
        handoff["relations"] = (
            plan.relation_projection
            if final_membership == plan.memberships or not plan.relation_certificate.get("relations")
            else security_relations.project(plan.relation_certificate, final_membership)
        )
    relation_projection = handoff["relations"]
    from .continuation import validate_handoff
    from .membership_transfer import build as build_membership_transfer
    from .membership_transfer import carry_prefix as carry_membership_prefix

    finished = handoff.setdefault("finished", set())
    for name, receipt in receipts.items():
        if name in finished:
            continue
        if source_equivalence is not None:
            receipt["numerical_source_equivalence_version"] = 1
            receipt["numerical_source_equivalence"] = source_equivalence
        else:
            receipt.pop("numerical_source_equivalence_version", None)
            receipt.pop("numerical_source_equivalence", None)
        if not receipt.get("security_relations"):
            receipt["security_relations"] = relation_projection
        receipt["membership_transfer_handoff_version"] = 1
        control = controls[name]
        prior = control.previous
        if prior is not None:

            def current_documents():
                missing = receipt["boundaries"].keys() - documents[name].keys()
                retained = (
                    assignment_documents(root, prior, cache.setdefault("assignments", {})) if missing else {}
                )
                return {**retained, **documents[name]}

            preserves_pricing = (
                control.accepted_reuse[1]
                if control.accepted_reuse is not None
                else unchanged_rows(control.source, prior, selected=control.source_sha256)
                and continuing_request(control.source, prior)
            )
            if prior.get("membership_only_rebind") is not None:
                # Its original donor can still be waiting for serving. Carry
                # that completed decision rather than rebinding to an unserved
                # intermediate receipt merely because publication advanced.
                if not preserves_pricing:
                    raise LineageError("changed membership-transfer pricing requires owning transfer recovery")
                transfer = carry_membership_prefix(prior, receipt, prior["assignment_summary"])
            else:
                transfer = build_membership_transfer(
                    name,
                    old_identity_memberships=prior["identity_memberships"],
                    old_serving_ownership=prior.get("serving_ownership"),
                    old_receipt_generation=control.previous_receipt_generation,
                    old_pricing_generation=prior["source_sha256"],
                    old_boundaries=prior["boundaries"],
                    current_receipt=receipt,
                    current_documents=current_documents,
                    preserves_pricing=preserves_pricing,
                    accepted_identity_changes={
                        identity["security_id"]: plan.memberships.get(
                            identity["member_alpaca_asset_ids"][0], identity
                        )
                        for identity in prior["identity_memberships"]
                    }
                    if plan.membership_refinement and plan.membership_refinement["changes"]
                    else None,
                    allow_label_changes=bool(
                        name in materialized and getattr(materialized[name], "completed_source_metadata", False)
                    ),
                )
            receipt["membership_only_rebind"] = transfer
        validate_handoff(receipt)
        finished.add(name)
    return receipts, documents, {**closed, **materialized}, reports


def append_unresolved(view, prior, report):
    """Assemble only genuinely new positions beside the accepted old report."""
    report = {
        **report,
        "uncorrelated": {
            "row_count": report["uncorrelated"]["row_count"],
            "segments": list(report["uncorrelated"]["segments"]),
        },
    }
    report["source_row_count"] = sum(len(rows) for rows in view.views.values())
    for adjustment in ADJUSTMENTS:
        first = prior["assignment_summary"][adjustment]["rows"] + 1
        rows = view.views[adjustment]
        if first <= len(rows):
            report["uncorrelated"]["segments"].append(
                {
                    "adjustment": adjustment,
                    "row_start": first,
                    "row_end": len(rows),
                    "row_count": len(rows) - first + 1,
                    "first_timestamp": rows[first - 1][0],
                    "last_timestamp": rows[-1][0],
                }
            )
            report["uncorrelated"]["row_count"] += len(rows) - first + 1
    return report


def _decide_source_work(store, root, controls, evidence, auxiliary, plan, policy, cache, *, loader):
    """Complete the owning decisions once, before constructing publications."""
    from .transform import correlate

    closed = {
        name: closed_source(control, plan)
        for name, control in controls.items()
        if plan.dispositions[name] in {"A", "retained"}
    }
    materialized = {}
    bounds, routes = selected_common_source_prerequisites(
        root, controls, plan, cache.setdefault("assignments", {})
    )
    for name, control in controls.items():
        if name in closed:
            continue
        key = name, control.source_sha256
        values = cache.setdefault("pricing", {})
        if key not in values:
            values[key] = loader(
                store,
                name,
                source=control.source,
                source_sha256=control.source_sha256,
                partitions=cache.setdefault("partitions", {}).setdefault(name, {}),
            )
        view = values[key]
        if not control.local["counts_known"]:
            from libs.alpaca_security_lineage.symbol_disposition import classify_symbol_disposition

            evidence[name] = {
                **evidence[name],
                "symbol_disposition": classify_symbol_disposition(
                    name,
                    view.local,
                    evidence[name]["asset_lookups"],
                    evidence[name]["identities"],
                    evidence[name].get("symbol_occupancy_evidence", ()),
                ),
            }
        view.accepted_public_common_membership = plan.common_membership
        view.accepted_security_relations = plan.relation_certificate
        view.accepted_sourced_rename_membership = plan.rename_membership
        view.accepted_identity_memberships = plan.memberships
        view.accepted_common_bounds = bounds
        view.accepted_common_routes = routes
        view.accepted_documented_membership = plan.documented_membership
        if plan.dispositions[name] == "B":
            retained = closed_source(control, plan)
            retained.local = view.local
            view.accepted_report = accepted_report(retained, plan, root, cache.setdefault("assignments", {}))
            if plan.membership_refinement and plan.membership_refinement["changes"]:
                view.accepted_report = append_unresolved(view, control.previous, view.accepted_report)
        materialized[name] = view
    reports = {}
    completed_reports = {}
    for name, view in materialized.items():
        prepared = (plan.prepared_reports or {}).get(name)
        complete_prepared_scope = prepared is not None and prepared.get("source_row_count") == sum(
            item["rows"] for item in view.local["downloads"]
        )
        if complete_prepared_scope:
            view.completed_source_metadata = True
        if complete_prepared_scope or (
            plan.membership_refinement
            and plan.membership_refinement["changes"]
            and view.accepted_report["uncorrelated"]["row_count"] == 0
        ):
            completed_reports[name] = view.accepted_report
    pending = {name: view for name, view in materialized.items() if name not in completed_reports}
    if pending:
        reports = correlate(
            store,
            pending,
            {name: evidence[name] for name in pending},
            policy=policy,
            selected_group=sorted(controls.keys() | auxiliary.keys()),
            unavailable_pricing=sorted(auxiliary),
            auxiliary_evidence=auxiliary,
            closed_sources=closed,
            closed_evidence={name: evidence[name] for name in closed},
            closed_report_loader=lambda name: accepted_report(
                closed[name], plan, root, cache.setdefault("assignments", {})
            ),
        )
    reports.update(completed_reports)
    intervals = {}
    for name, control in closed.items():
        prior = control.previous
        if plan.membership_refinement and plan.membership_refinement["changes"]:
            report = accepted_report(control, plan, root, cache.setdefault("assignments", {}))
            intervals[name] = [
                (
                    segment["security_identity"]["security_id"],
                    segment["first_timestamp"][:10],
                    segment["last_timestamp"][:10],
                )
                for segment in report["correlated"]["segments"]
            ]
            continue
        ids = {
            identity["security_id"]: plan.memberships.get(identity["member_alpaca_asset_ids"][0], identity)[
                "security_id"
            ]
            for identity in prior["identity_memberships"]
        }
        intervals[name] = [
            (ids[item["security_id"]], item["from"], item["to"]) for item in prior["identity_intervals"]
        ]
    intervals.update(
        {
            name: [
                (
                    segment["security_identity"]["security_id"],
                    segment["first_timestamp"][:10],
                    segment["last_timestamp"][:10],
                )
                for segment in report["correlated"]["segments"]
            ]
            for name, report in reports.items()
        }
    )
    counterpart_names = {
        name
        for name in closed
        if any(
            pid == other_pid and max(first, other_first) <= min(last, other_last)
            for pid, first, last in intervals[name]
            for other, spans in intervals.items()
            if other != name
            for other_pid, other_first, other_last in spans
        )
    }
    closed_reports = {
        name: accepted_report(closed[name], plan, root, cache.setdefault("assignments", {}))
        for name in counterpart_names
    }
    identity_changes = {}
    for control in controls.values():
        for identity in (control.previous or {}).get("identity_memberships", ()):
            current = plan.memberships.get(identity["member_alpaca_asset_ids"][0], identity)
            identity_changes[identity["security_id"]] = current

    retained_selections = {
        name: (
            control.previous["serving_ownership"],
            {identity["security_id"]: identity for identity in control.previous["identity_memberships"]}
            if plan.membership_refinement and plan.membership_refinement["changes"]
            else identity_changes,
            accepted_donor_grids(control, cache),
            accepted_choices(control, root, cache),
        )
        for name, control in controls.items()
        if plan.dispositions[name] in {"A", "B", "retained"}
        and control.previous is not None
        and control.previous.get("serving_ownership")
        and (
            control.accepted_reuse[1]
            if control.accepted_reuse is not None
            else unchanged_rows(control.source, control.previous, selected=control.source_sha256)
            and continuing_request(control.source, control.previous)
        )
    }
    selection_output = {
        "track_opened": any(
            (control.previous or {}).get("numerical_source_equivalence") for control in controls.values()
        )
    }
    reports, observation_changes, _ = source_observations.reconcile(
        store,
        materialized,
        reports,
        closed,
        closed_reports,
        cache.setdefault("counterpart_partitions", {}),
        retained=retained_selections,
        selection_output=selection_output,
        stable_identities=plan.stable_numerical_identities,
    )
    from .source_equivalence import build as build_source_equivalence

    equivalence = build_source_equivalence(controls, selection_output, identity_changes)
    return closed, materialized, reports, observation_changes, equivalence

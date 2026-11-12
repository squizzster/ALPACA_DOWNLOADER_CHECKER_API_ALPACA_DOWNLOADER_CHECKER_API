"""Explicitly review completed research output for selected production publication.

Preparation establishes metadata equivalence once without storage/provider I/O.
Approval consumes that held review scope; publication remains with the existing
generation-bound prepared publisher and its journal. Research stays research:
the admitted output records its original identity and the separate approval.
"""

from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path

from libs.alpaca_security_lineage.event_supplements import _freeze, supplement_key
from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND, items

from .assignment_handoff import AcceptedObservationChoices
from .contract import COMPUTATION_REVISION, LineageError, document_digest
from .prepared_recovery import PreparedCandidate
from .retained_recovery import RECOVERY_BOUNDARY, accept_selection

APPROVAL_SCHEMA = "alpaca.lineage.research_admission_approval"
RESEARCH_BOUNDARY = "alpaca_lineage_research_candidate"
_DESCRIPTIVE = {
    "policy_id",
    "policy_version",
    "policy_role",
    "semantic_sha256",
    "file_sha256",
    "source",
    "research",
}


@dataclass(frozen=True)
class ResearchAdmission:
    scope: dict
    selection: object
    proposed: PreparedCandidate


def _require(condition, message):
    if not condition:
        raise LineageError("research admission: " + message)


def _identity(policy):
    return {
        key: policy[key]
        for key in (
            "policy_id",
            "policy_version",
            "policy_role",
            "execution_profile",
            "implementation",
            "semantic_sha256",
            "file_sha256",
        )
    }


def _fact_records(entries, group, *, canonical=False):
    records = {}
    for entry in entries:
        scope = set(entry["symbols"])
        if not scope.intersection(group):
            continue
        _require(scope <= group, "factual application crosses the selected complete group")
        facts = entry["facts"] if canonical else entry["supplements"]
        for fact in facts:
            _require(
                fact["kind"] == MARKET_KIND and fact["facts"]["schema_version"] in {3, 4, 5},
                "unsupported research evidence producer in selected scope",
            )
            key = supplement_key(fact), tuple(sorted(scope))
            _require(key not in records, "duplicate selected research fact scope")
            records[key] = fact, entry.get("refine_accepted_dates", True)
    return records


def _restore_choices(receipt):
    """Restore completed values, not their old run validation or derivation."""
    for kind, handoff in receipt.get("assignment_handoffs", {}).items():
        if "observation_choices_version" not in handoff and "observation_choices" not in handoff:
            continue
        choices = handoff.get("observation_choices")
        generation = receipt["boundaries"][kind]["sha256"]
        _require(
            handoff.get("observation_choices_version") == 1
            and handoff.get("generation") == generation
            and isinstance(choices, dict)
            and isinstance(choices.get("views"), dict)
            and isinstance(choices.get("partitions"), (list, tuple)),
            "incomplete completed numerical-choice transport",
        )
        if isinstance(choices, AcceptedObservationChoices):
            _require(choices.generation == generation, "changed numerical-choice generation")
        else:
            handoff["observation_choices"] = AcceptedObservationChoices(
                choices["views"], tuple(_freeze(part) for part in choices["partitions"]), generation
            )


def prepare_admission(report, ruleset, store, baseline_receipts, *, group_index=0):
    """Prepare one exact review packet; this does not authorize publication.

    Baseline bodies must be separately supplied by their selected receipt owner.
    No baseline body, omitted equality result or missing assignment is rebuilt
    from a candidate. All assignment selectors belong to its completed producer.
    """
    store = Path(store).absolute()
    _require(
        isinstance(report, dict)
        and report.get("schema") == "alpaca.lineage.debug.report"
        and type(report.get("schema_version")) is int
        and report.get("schema_version") == 1
        and report.get("operation") == "run"
        and report.get("store") == str(store)
        and isinstance(report.get("implementation_binding"), dict)
        and type(group_index) is int
        and isinstance(report.get("groups"), list)
        and 0 <= group_index < len(report["groups"]),
        "missing completed research report or selected group",
    )
    group = report["groups"][group_index]
    _require(isinstance(group, dict), "missing completed group result")
    handoff = group.get("candidate_handoff", {})
    members = group.get("group")
    _require(
        isinstance(members, list)
        and all(isinstance(name, str) for name in members)
        and members == sorted(set(members))
        and members
        and isinstance(handoff, dict)
        and handoff.get("boundary") == RESEARCH_BOUNDARY
        and type(handoff.get("schema_version")) is int
        and handoff.get("schema_version") == 1
        and handoff.get("publication_authorized") is False
        and handoff.get("store") == str(store)
        and group.get("control_kind") == "accepted_publication_no_replay"
        and group.get("control_correlation_reused") is True,
        "requires an unmodified completed accepted-publication research handoff",
    )
    candidate, control = group.get("candidate_policy", {}), group.get("control_policy", {})
    _require(
        isinstance(candidate, dict) and isinstance(control, dict), "missing candidate/control policy identity"
    )
    production = ruleset.policy.as_report_object()
    core = {key: value for key, value in production.items() if key not in _DESCRIPTIVE}
    _require(
        candidate.get("policy_role") == "research_candidate"
        and control.get("policy_role") == "production"
        and control.get("semantic_sha256") == production["semantic_sha256"]
        and candidate == handoff.get("policy")
        and all(
            {key: value for key, value in policy.items() if key not in _DESCRIPTIVE} == core
            for policy in (candidate, control)
        ),
        "candidate/control cascade is not equivalent to the accepted production policy",
    )
    options = candidate.get("research", {})
    _require(
        isinstance(options, dict)
        and options.get("membership_refinement") == ruleset.membership_refinement
        and options.get("control_mode") in {"accepted_publication", "documented_scopes"},
        "candidate producer selection differs from canonical production",
    )
    applications = [app for app in ruleset.applications if set(app["symbols"]).intersection(members)]
    _require(
        _fact_records(options.get("documented_facts", ()), set(members))
        == _fact_records(applications, set(members), canonical=True),
        "candidate facts or date intent differ from canonical scoped applications",
    )
    comparison = group.get("comparison", {})
    _require(
        comparison.get("candidate_partition_complete") is True
        and comparison.get("mapping_corrections") == {"control": {}, "candidate": {}},
        "incomplete candidate assignments or outstanding pricing correction",
    )
    pricing, baseline = handoff.get("pricing_generations"), handoff.get("baseline_receipt_generations")
    selection = accept_selection(
        {
            "schema_version": 1,
            "boundary": RECOVERY_BOUNDARY,
            "store": str(store),
            "correlation_policy_sha256": production["semantic_sha256"],
            "groups": [{"members": members, "pricing_generations": pricing, "receipt_generations": baseline}],
        },
        store,
    )
    source_group = selection.groups[0]
    receipts, bodies = handoff.get("receipts"), handoff.get("boundaries")
    _require(
        all(
            isinstance(value, dict) and value.keys() == source_group.receipts.keys()
            for value in (receipts, bodies, baseline_receipts)
        ),
        "separate selected baseline receipts and complete candidate outputs are required",
    )
    proposed, changed, assignments = deepcopy(receipts), {}, {}
    provenance = ruleset.policy.provenance()
    for name, receipt in proposed.items():
        old = baseline_receipts[name]
        _require(isinstance(receipt, dict) and isinstance(old, dict), name + ": missing selected receipt body")
        original_policy = receipt.get("correlation_policy", {})
        expected_policy = candidate if original_policy.get("policy_role") == "research_candidate" else control
        _require(
            receipt.get("symbol") == old.get("symbol") == name
            and receipt.get("source_sha256") == old.get("source_sha256") == pricing[name]
            and receipt.get("computation_revision") == COMPUTATION_REVISION
            and isinstance(receipt.get("boundaries"), dict)
            and isinstance(bodies[name], dict)
            and bodies[name].keys() == receipt["boundaries"].keys()
            and isinstance(receipt.get("evidence"), dict)
            and all(
                original_policy.get(key) == expected_policy.get(key)
                for key in (
                    "policy_id",
                    "policy_version",
                    "policy_role",
                    "execution_profile",
                    "implementation",
                    "semantic_sha256",
                )
            ),
            name + ": incomplete completed output or selected baseline binding",
        )
        documented = receipt.get("documented_identity_membership")
        needs_documented = any(
            fact["kind"] == MARKET_KIND and fact["facts"]["schema_version"] in {3, 4, 5}
            for fact in items(receipt["evidence"])
        )
        if needs_documented or documented is not None:
            _require(
                isinstance(documented, dict)
                and set(documented) == {"version", "owners", "source_routes"}
                and type(documented["version"]) is int
                and documented["version"] == 1
                and isinstance(documented["owners"], list)
                and isinstance(documented["source_routes"], dict),
                name + ": missing completed documented ownership handoff",
            )
        assignments[name] = bodies[name]
        changed[name] = {
            kind: body
            for kind, body in bodies[name].items()
            if receipt["boundaries"][kind] != old.get("boundaries", {}).get(kind)
        }
        for kind in changed[name]:
            handoff_value = receipt.get("assignment_handoffs", {}).get(kind, {})
            _require(
                isinstance(handoff_value.get("_projection"), dict)
                and handoff_value.get("generation") == receipt["boundaries"][kind]["sha256"],
                name + ": missing completed assignment projection",
            )
        _restore_choices(receipt)
        # This is new production bookkeeping for the reviewed canonical fact,
        # not another ownership/numerical evaluation of its completed result.
        observed = {supplement_key(fact): fact for fact in items(receipt["evidence"])}
        fingerprints = dict(receipt["evidence"].get("sourced_fact_applications", {}))
        intents = dict(receipt["evidence"].get("sourced_fact_intents", {}))
        for app in applications:
            if all(observed.get(supplement_key(fact)) == fact for fact in app["facts"]):
                fingerprints[app["id"]] = app["fingerprint"]
                for fact in app["facts"]:
                    intents[fact["match"]["source_id"]] = app.get("refine_accepted_dates", True)
        receipt["evidence"] = {**receipt["evidence"], "sourced_fact_applications": fingerprints}
        if intents:
            receipt["evidence"]["sourced_fact_intents"] = intents
        receipt["correlation_policy"] = deepcopy(provenance)
        receipt["input_sources"] = dict(pricing)
    scope = _freeze(
        {
            "admission_version": 1,
            "store": str(store),
            "members": members,
            "research_implementation": report["implementation_binding"],
            "candidate_policy": _identity(candidate),
            "control_policy": _identity(control),
            "production_policy": {
                key: value for key, value in _identity(production).items() if key != "file_sha256"
            },
            "production_execution_sha256": ruleset.execution_sha256,
            "canonical_applications": {app["id"]: app["fingerprint"] for app in applications},
            "pricing_generations": pricing,
            "baseline_receipt_generations": baseline,
            "candidate_boundaries": {name: receipt["boundaries"] for name, receipt in receipts.items()},
            "equivalence": "unchanged_rule_cascade_and_exact_canonical_scoped_facts_and_intents",
            "numerical_choices": "completed_result_consumed_without_replay",
            "optional_equality_capture": handoff.get("equality_capture"),
        }
    )
    operation_id = document_digest(scope)
    selection = replace(selection, groups=(replace(source_group, operation_id=operation_id),))
    prepared = PreparedCandidate(proposed, changed, baseline_receipts, provenance, assignments)
    return ResearchAdmission(scope, selection, prepared)


def approve_admission(admission, approval):
    """Consume the explicit exact-scope review without repeating preparation."""
    _require(
        isinstance(admission, ResearchAdmission)
        and isinstance(approval, dict)
        and approval.get("schema") == APPROVAL_SCHEMA
        and type(approval.get("schema_version")) is int
        and approval.get("schema_version") == 1
        and approval.get("status") == "approved"
        and isinstance(approval.get("approval_id"), str)
        and approval["approval_id"]
        and isinstance(approval.get("conclusion"), str)
        and approval["conclusion"]
        and approval.get("scope") == admission.scope,
        "publication requires an explicit approval of this exact review scope",
    )
    receipts = deepcopy(admission.proposed.receipts)
    origin = {
        "version": 1,
        "origin": RESEARCH_BOUNDARY,
        "original_publication_authorized": False,
        "approval_id": approval["approval_id"],
        "conclusion": approval["conclusion"],
        "approved_scope": admission.scope,
    }
    for receipt in receipts.values():
        receipt["research_admission"] = origin
    return replace(admission.selection, prepared=replace(admission.proposed, receipts=receipts))


def require_canonical_applicability(prepared, ruleset):
    """Check only current named recipe applicability, never the admitted work."""
    origins = [receipt.get("research_admission") for receipt in prepared.receipts.values()]
    origin = next((value for value in origins if value is not None), None)
    if origin is None:
        return
    _require(
        isinstance(origin, dict)
        and isinstance(origin.get("approved_scope"), dict)
        and all(value == origin for value in origins),
        "incomplete approved group provenance",
    )
    scope = origin["approved_scope"]
    members = set(scope["members"])
    current = {
        app["id"]: app["fingerprint"] for app in ruleset.applications if members.intersection(app["symbols"])
    }
    _require(
        current == scope["canonical_applications"]
        and ruleset.execution_sha256 == scope["production_execution_sha256"],
        "approved canonical applications or producer selection changed",
    )

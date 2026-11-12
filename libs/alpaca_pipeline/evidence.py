"""Semantic transport of completed metadata observations for one operation.

The shared value codec preserves aliases with carried receipts and facts. Import
rebuilds only process-local identity indexes: no accepted action projection,
supplement validation, catalogue assembly or provider observation is repeated.
"""

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
from libs.alpaca_security_lineage.event_supplements import EventProjection

EVIDENCE_ERROR_TYPES = (RuntimeError, OSError, ValueError, LineageError, DownloadError, IdentityHttpError)
SCHEMA = "alpaca.pipeline.evidence"
SCHEMA_VERSION = 1

# Semantic field ownership is explicit. Provider and policy have their own owners.
_FIELDS = (
    "assets",
    "actions",
    "action_intervals",
    "by_id",
    "catalogue",
    "catalogue_assets",
    "exact_lookup_reasons",
    "catalogue_attempted",
    "catalogue_error",
    "catalogue_records",
    "catalogue_plan",
    "action_conflicts",
    "observation",
    "restricted_identifiers",
    "retained_symbols",
    "event_supplements",
    "retained_event_supplements",
    "applied_event_supplements",
    "rule_applications",
    "rule_fact_scopes",
)


def export_evidence(evidence, encode):
    """Carry accepted values and completed projections through an alias codec."""
    state = {field: getattr(evidence, field) for field in _FIELDS}
    state["accepted_application_projections"] = list(
        {id(value): value for value in evidence.application_projections.values()}.values()
    )
    state["accepted_event_projections"] = list(evidence.event_projection.accepted.values())
    state["accepted_supplement_evidence"] = list(evidence._retained_supplement_evidence.values())
    return {"schema": SCHEMA, "schema_version": SCHEMA_VERSION, "state": encode(state)}


def import_evidence(document, client, decode, *, work_policy=None):
    """Consume the completed producer handoff without invoking its producers."""
    if (
        not isinstance(document, dict)
        or set(document) != {"schema", "schema_version", "state"}
        or document["schema"] != SCHEMA
        or type(document["schema_version"]) is not int
        or document["schema_version"] != SCHEMA_VERSION
    ):
        raise LineageError("unsupported pipeline evidence handoff")
    state = decode(document["state"])
    if not isinstance(state, dict) or set(state) != {
        *_FIELDS,
        "accepted_event_projections",
        "accepted_supplement_evidence",
        "accepted_application_projections",
    }:
        raise LineageError("incomplete pipeline evidence handoff")
    # Constructor observation/fact parameters would repeat producer acceptance.
    evidence = object.__new__(EvidenceClient)
    evidence.client = client
    evidence.work_policy = work_policy
    for field in _FIELDS:
        setattr(evidence, field, state[field])
    evidence.application_projections = {}
    for held in state["accepted_application_projections"]:
        evidence.application_projections[id(held[0])] = held
        evidence.application_projections[id(held[2])] = held
    evidence.event_projection = EventProjection()
    for action, supplement, projected in state["accepted_event_projections"]:
        evidence.event_projection.accepted[id(action), id(supplement)] = (action, supplement, projected)
    evidence._retained_supplement_evidence = {
        id(value): value for value in state["accepted_supplement_evidence"]
    }
    return evidence

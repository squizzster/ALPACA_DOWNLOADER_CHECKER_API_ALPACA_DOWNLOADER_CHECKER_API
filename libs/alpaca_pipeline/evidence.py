"""Transport completed provider observations without restoring research inputs.

Only process-local indexes are restored. Catalogue assembly, action acceptance
and provider observations have already completed and are never repeated here.
"""

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError
from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence, reject_assertions

EVIDENCE_ERROR_TYPES = (RuntimeError, OSError, ValueError, LineageError, DownloadError, IdentityHttpError)
SCHEMA = "alpaca.pipeline.evidence"
SCHEMA_VERSION = 1

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
)
_RETIRED_FIELDS = (
    "event_supplements",
    "retained_event_supplements",
    "applied_event_supplements",
    "rule_applications",
    "rule_fact_scopes",
    "application_projections",
    "_retained_supplement_evidence",
    "accepted_event_projections",
    "accepted_supplement_evidence",
    "accepted_application_projections",
)


def _require_provider_state(state):
    # These are observations used for fresh work, not closed publication bodies.
    if isinstance(state, ProviderEvidence):
        return
    try:
        reject_assertions(state, where="pipeline provider evidence")
    except ValueError as error:
        raise LineageError(str(error)) from error


def export_evidence(evidence, encode):
    """Carry observed values; authored-fact caches are never a stage capability."""
    for name in _RETIRED_FIELDS:
        if getattr(evidence, name, None):
            raise LineageError("pipeline evidence cannot carry research assertions: " + name)
    projection = getattr(evidence, "event_projection", None)
    if projection is not None and getattr(projection, "accepted", None):
        raise LineageError("pipeline evidence cannot carry event projections")
    state = {field: getattr(evidence, field) for field in _FIELDS}
    _require_provider_state(state)
    # The original anchors a completed projection's identity. Its legacy
    # provenance is never reinstated as fresh evidence or validated again.
    views = list(evidence._provider_evidence_views.values())
    for _, projected in views:
        _require_provider_state(projected)
    # This producer has accepted the entire selected cache, including the
    # projection pairs below. Preserve that acceptance without copying their
    # aliased bodies or asking the next process to traverse them again.
    state = ProviderEvidence(state)
    return {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "state": encode(state),
        "provider_evidence_views": encode(views),
    }


def import_evidence(document, client, decode, *, work_policy=None):
    """Restore provider caches without constructors or accepted-work replay."""
    if (
        not isinstance(document, dict)
        or set(document) != {"schema", "schema_version", "state", "provider_evidence_views"}
        or document["schema"] != SCHEMA
        or type(document["schema_version"]) is not int
        or document["schema_version"] != SCHEMA_VERSION
    ):
        raise LineageError("unsupported pipeline evidence handoff")
    state = decode(document["state"])
    if not isinstance(state, dict) or set(state) != set(_FIELDS):
        raise LineageError("incomplete pipeline evidence handoff; research assertion state is not supported")
    accepted_provider_state = isinstance(state, ProviderEvidence)
    if not accepted_provider_state:
        _require_provider_state(state)
    views = decode(document["provider_evidence_views"])
    if not isinstance(views, list) or any(
        not isinstance(pair, (list, tuple)) or len(pair) != 2 for pair in views
    ):
        raise LineageError("incomplete provider-evidence projection handoff")
    if not accepted_provider_state:
        for _, projected in views:
            _require_provider_state(projected)
    evidence = object.__new__(EvidenceClient)
    evidence.client = client
    evidence.work_policy = work_policy
    evidence._provider_evidence_views = {id(original): (original, projected) for original, projected in views}
    for field in _FIELDS:
        setattr(evidence, field, state[field])
    return evidence

"""Keep authored research assertions outside fresh lineage inference.

Completed publication bodies are not fresh evidence. Their owners carry those
results without calling this guard or reopening their historical provenance.
"""

from collections.abc import Mapping

ASSERTION_FIELDS = frozenset(
    {
        "event_supplements",
        "sourced_successions",
        "sourced_public_common_continuations",
        "sourced_security_relations",
        "sourced_market_occupancy",
        "sourced_fact_intents",
        "sourced_fact_applications",
        "documented_facts",
        "research_admission",
        "identity_interpretation",
        "provider_endpoint_discrepancy",
        "sourced_facts",
        "applications",
        "supplements",
        "assertions",
        "assertion_overlays",
        "research_certificates",
        "test_symbol_evidence",
        "rule_applications",
        "rule_fact_scopes",
    }
)


class ProviderEvidence(dict):
    """Completed provider-only producer result; consumers trust its acceptance."""


def accept_provider_evidence(value, *, where="provider evidence"):
    if isinstance(value, ProviderEvidence):
        return value
    reject_assertions(value, where=where)
    return ProviderEvidence(value)


def reject_assertions(value, *, where="inference evidence"):
    """Reject active assertion values in explicitly selected fresh input only.

    Empty legacy placeholders grant no capability. Generic rule options and
    provider observation values remain admissible. This function performs no I/O.
    """
    seen = set()

    def visit(item):
        if isinstance(item, ProviderEvidence):
            return
        if not isinstance(item, (Mapping, list, tuple)) or id(item) in seen:
            return
        seen.add(id(item))
        if isinstance(item, Mapping):
            if item.get("classification") == "exchange_test_symbol":
                raise ValueError(f"{where}: researched test-symbol classification is inadmissible")
            if {"kind", "match", "facts", "sources"}.issubset(item):
                raise ValueError(f"{where}: authored factual assertions are inadmissible")
            for key, child in item.items():
                if key in ASSERTION_FIELDS and child:
                    raise ValueError(f"{where}: research fact injection is inadmissible ({key})")
                visit(child)
        else:
            for child in item:
                visit(child)

    visit(value)


def provider_evidence(evidence, *, local_report=None):
    """Select retained raw observations for NEW inference, preserving closed output.

    The metadata owner caches this projection. No receipts, prices or provider
    endpoints are opened. Historical decisions and journals are not inputs here.
    """
    if isinstance(evidence, ProviderEvidence):
        return evidence
    disposition = evidence.get("symbol_disposition") or {}
    authored = (
        any(evidence.get(key) for key in ASSERTION_FIELDS)
        or disposition.get("classification") == "exchange_test_symbol"
        or bool(disposition.get("test_symbol_evidence"))
    )
    if not authored and "provider_corporate_actions" not in evidence:
        return accept_provider_evidence(evidence)
    from .lineage import report_from_observations

    if evidence.get("event_supplements") and "provider_corporate_actions" not in evidence:
        raise ValueError(
            "retained assertion projection lacks its raw provider observations; metadata recovery required"
        )
    actions = evidence.get("provider_corporate_actions", evidence.get("corporate_actions", ()))
    reject_assertions(actions, where="retained provider observations")
    query = evidence["query"]
    local = local_report or {"present": True, "downloads": [], "counts_known": False}
    report = report_from_observations(
        query["input_symbol"],
        local,
        actions,
        evidence["asset_lookups"],
        evidence["queried_symbols"],
        start_date=query["corporate_actions_start"],
        end_date=query["corporate_actions_end"],
        local_evidence=evidence.get("symbol_disposition", {}).get("local_market_data"),
    )
    report["query"] = query
    report.pop("api_request_count", None)
    for key in ("catalogue_observed_at_utc", "mapping_endpoint_observations"):
        if key in evidence:
            report[key] = evidence[key]
    return accept_provider_evidence(report)


def provider_actions(evidence):
    """Consume the stored raw provider union without applying authored overlays."""
    return evidence.get("provider_corporate_actions", evidence.get("corporate_actions", ()))

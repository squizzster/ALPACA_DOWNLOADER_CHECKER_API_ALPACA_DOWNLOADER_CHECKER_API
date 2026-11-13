"""Read-only legacy application provenance; authored inference is retired.

These compatibility names never authorize factual application, publication or
workflow completion. Existing journal bodies remain with their publication owner.
"""

from libs.alpaca_security_lineage.inference_evidence import reject_assertions
from libs.alpaca_security_lineage.sourced_facts import items as items

from .contract import LineageError, document_digest


def _reject():
    raise LineageError("authored fact applications are inadmissible; use explicit generic lineage recovery")


def _generic_client(client):
    if any(getattr(client, name, ()) for name in ("rule_applications", "rule_fact_scopes", "event_supplements")):
        _reject()


def application_fingerprint(entry):
    """Describe a retained legacy entry without accepting it for execution."""
    value = {key: item for key, item in entry.items() if key != "refine_accepted_dates"}
    if entry.get("refine_accepted_dates", True) is False:
        value["refine_accepted_dates"] = False
    return document_digest(value)


def accept_applications(entries):
    if type(entries) is not list or entries:
        _reject()
    return (), ()


def load_applications(path=None):
    if path is not None:
        _reject()
    return (), ()


def configure(client, applications, graph, entries):
    if applications:
        _reject()
    _generic_client(client)
    return {}


def incorporated(client, evidence, *, applications=None):
    if applications:
        _reject()
    _generic_client(client)
    reject_assertions(evidence, where="retired factual incorporation")
    return {}


def incorporated_group(client, evidence, *, applications):
    if applications:
        _reject()
    _generic_client(client)
    reject_assertions(evidence, where="retired factual incorporation")
    return {}


def project_application_intents(client, evidence):
    _generic_client(client)
    reject_assertions(evidence, where="retired factual projection")
    return evidence


def install_ruleset_facts(client, ruleset):
    if getattr(ruleset, "applications", ()) or getattr(ruleset, "supplements", ()):
        _reject()
    _generic_client(client)

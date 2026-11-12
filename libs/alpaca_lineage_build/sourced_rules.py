"""Accepted, explicitly scoped rule facts consumed by ordinary daily updates."""

import json
from pathlib import Path

from libs.alpaca_pricing_download.contract import symbol_label
from libs.alpaca_security_lineage.event_supplements import accept_supplements, supplement_key
from libs.alpaca_security_lineage.sourced_facts import items

from .contract import LineageError, document_digest

DEFAULT_FACTS = Path(__file__).resolve().parents[2] / "resources/alpaca_sourced_event_facts.json"


def load_applications(path=DEFAULT_FACTS):
    document = json.loads(Path(path).read_text())
    if set(document) != {"schema_version", "applications"} or document["schema_version"] != 1:
        raise LineageError("unsupported sourced rule application registry")
    applications, raw, identifiers = [], [], set()
    for entry in document["applications"]:
        if set(entry) != {"id", "symbols", "supplements"} or not entry["id"] or entry["id"] in identifiers:
            raise LineageError("sourced rule applications require unique IDs and explicit symbol scopes")
        symbols = tuple(sorted({symbol_label(s) for s in entry["symbols"]}))
        if not symbols or not entry["supplements"]:
            raise LineageError("sourced rule applications require symbols and facts")
        identifiers.add(entry["id"])
        applications.append(
            {
                "id": entry["id"],
                "symbols": symbols,
                "fingerprint": document_digest(entry),
                "keys": frozenset(supplement_key(item) for item in entry["supplements"]),
            }
        )
        raw.extend(entry["supplements"])
    accepted = accept_supplements(raw)
    by_key = {supplement_key(item): item for item in accepted}
    for application in applications:
        application["facts"] = tuple(by_key[key] for key in sorted(application["keys"]))
    return applications, accepted


def configure(client, applications, graph, entries):
    """Select only named existing anchors; no historical-label discovery or I/O."""
    requirements, scopes = {}, {}
    for application in applications:
        anchors = set(application["symbols"]) & entries.keys()
        group = set().union(*(graph.closure({s}) for s in anchors)) if anchors else set()
        for key in application["keys"]:
            scopes.setdefault(key, set()).update(group)
        for symbol in anchors:
            accepted = entries[symbol]["projection"].get("sourced_fact_applications", {})
            if accepted.get(application["id"]) != application["fingerprint"]:
                requirements.setdefault(symbol, {})[application["id"]] = application["fingerprint"]
    client.rule_applications = applications
    client.rule_fact_scopes = scopes
    return requirements


def incorporated(client, evidence):
    """Record incorporation at metadata publication, independently of certification."""
    observed = {
        supplement_key(item): item for item in (*evidence.get("event_supplements", ()), *items(evidence))
    }
    return {
        application["id"]: application["fingerprint"]
        for application in client.rule_applications
        if all(observed.get(supplement_key(fact)) == fact for fact in application["facts"])
    }

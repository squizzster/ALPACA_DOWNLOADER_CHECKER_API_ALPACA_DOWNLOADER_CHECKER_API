"""Accepted, explicitly scoped rule facts consumed by ordinary daily updates."""

import json
from pathlib import Path

from libs.alpaca_pricing_download.contract import symbol_label
from libs.alpaca_security_lineage.event_supplements import (
    _AcceptedSupplements,
    _FrozenDict,
    accept_supplements,
    supplement_key,
)
from libs.alpaca_security_lineage.sourced_facts import items

from .contract import LineageError, document_digest


def accept_applications(entries):
    """Accept new scoped facts once, retaining legacy application identities."""
    if type(entries) is not list:
        raise LineageError("sourced rule applications must be a list")
    pending, raw, identifiers = [], [], set()
    for entry in entries:
        if (
            type(entry) is not dict
            or set(entry) - {"id", "symbols", "supplements", "refine_accepted_dates"}
            or not {"id", "symbols", "supplements"} <= set(entry)
            or type(entry["id"]) is not str
            or not entry["id"]
            or entry["id"] in identifiers
            or type(entry["symbols"]) is not list
            or type(entry["supplements"]) is not list
        ):
            raise LineageError("sourced rule applications require unique IDs and explicit symbol scopes")
        refine_dates = entry.get("refine_accepted_dates", True)
        if type(refine_dates) is not bool:
            raise LineageError("sourced rule refine_accepted_dates must be boolean")
        symbols = tuple(sorted({symbol_label(s) for s in entry["symbols"]}))
        if not symbols or not entry["supplements"]:
            raise LineageError("sourced rule applications require symbols and facts")
        identifiers.add(entry["id"])
        pending.append((entry, symbols, refine_dates))
        raw.extend(entry["supplements"])
    accepted = accept_supplements(raw) if raw else _AcceptedSupplements()
    by_key = {supplement_key(item): item for item in accepted}
    applications = []
    for entry, symbols, refine_dates in pending:
        # An omitted intent historically means date refinement. Spelling that
        # default explicitly does not select new work; false does, in its scope.
        fingerprint_entry = {key: value for key, value in entry.items() if key != "refine_accepted_dates"}
        if not refine_dates:
            fingerprint_entry["refine_accepted_dates"] = False
        application = {
            "id": entry["id"],
            "symbols": symbols,
            "fingerprint": document_digest(fingerprint_entry),
            "keys": frozenset(supplement_key(item) for item in entry["supplements"]),
        }
        if "refine_accepted_dates" in entry:
            application["refine_accepted_dates"] = refine_dates
        applications.append(
            _FrozenDict({**application, "facts": tuple(by_key[key] for key in sorted(application["keys"]))})
        )
    return applications, accepted


def load_applications(path=None):
    """Compatibility adapter; ordinary configuration lives only in the YAML.

    Explicit JSON paths remain available for existing selected CLI/test imports.
    They are never a fallback or a second live registry.
    """
    if path is None or Path(path).suffix.lower() != ".json":
        from .ruleset import load_ruleset

        ruleset = load_ruleset() if path is None else load_ruleset(path)
        return ruleset.applications, ruleset.supplements
    with Path(path).open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise LineageError("sourced rule application registry exceeds 1048576 bytes")
    document = json.loads(raw)
    if (
        type(document) is not dict
        or set(document) != {"schema_version", "applications"}
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
    ):
        raise LineageError("unsupported sourced rule application registry")
    return accept_applications(document["applications"])


def configure(client, applications, graph, entries):
    """Select only named existing anchors; no historical-label discovery or I/O."""
    requirements, scopes = {}, {}
    for application in applications:
        anchors = set(application["symbols"]) & entries.keys()
        # Declared anchors are permitted fact scope even before first lineage
        # publication. This selects no work or lookup: a caller must actually
        # select that named source before all_event_supplements can lend facts.
        group = set(application["symbols"])
        if anchors:
            group.update(set().union(*(graph.closure({s}) for s in anchors)))
        for key in application["keys"]:
            scopes.setdefault(key, set()).update(group)
        for symbol in anchors:
            accepted = entries[symbol]["projection"].get("sourced_fact_applications", {})
            if accepted.get(application["id"]) != application["fingerprint"]:
                requirements.setdefault(symbol, {})[application["id"]] = application["fingerprint"]
    client.rule_applications = applications
    client.rule_fact_scopes = scopes
    return requirements


def incorporated(client, evidence, *, applications=None):
    """Record incorporation at metadata publication, independently of certification."""
    observed = {
        supplement_key(item): item for item in (*evidence.get("event_supplements", ()), *items(evidence))
    }
    return {
        application["id"]: application["fingerprint"]
        for application in (client.rule_applications if applications is None else applications)
        if all(observed.get(supplement_key(fact)) == fact for fact in application["facts"])
    }


def project_application_intents(client, evidence):
    """Project newly accepted instruction metadata without reacquiring evidence.

    A current fingerprint is consumed directly. Only a changed application
    selects its factual-incorporation projection; this is not rule execution.
    """
    cache = client.application_projections
    held = cache.get(id(evidence))
    if held is not None and held[1] is client.rule_applications:
        return held[2]
    applications = [
        app
        for app in client.rule_applications
        if evidence.get("sourced_fact_applications", {}).get(app["id"]) != app["fingerprint"]
    ]
    if not applications:
        cache[id(evidence)] = (evidence, client.rule_applications, evidence)
        return evidence
    current = incorporated(client, evidence, applications=applications)
    selected = [app for app in applications if current.get(app["id"]) == app["fingerprint"]]
    if not selected:
        cache[id(evidence)] = (evidence, client.rule_applications, evidence)
        return evidence
    fingerprints = dict(evidence.get("sourced_fact_applications", {}))
    intents = dict(evidence.get("sourced_fact_intents", {}))
    for application in selected:
        fingerprints[application["id"]] = application["fingerprint"]
        for fact in application["facts"]:
            if fact["kind"] == "dated_market_occupancy":
                intents[fact["match"]["source_id"]] = application.get("refine_accepted_dates", True)
    result = {**evidence, "sourced_fact_applications": fingerprints}
    if intents:
        result["sourced_fact_intents"] = intents
    held = evidence, client.rule_applications, result
    cache[id(evidence)] = cache[id(result)] = held
    return result


def install_ruleset_facts(client, ruleset):
    """Lend accepted canonical facts to a newly created standalone client.

    Existing stage clients already carry their producer's explicit fact scope.
    This helper is only for constructing a new owner, never restoring a handoff.
    Operation overlays cannot silently replace conflicting canonical facts.
    """
    for fact in ruleset.supplements:
        key = supplement_key(fact)
        overlay = client.event_supplements.get(key)
        if overlay is not None and overlay != fact:
            raise LineageError("operation supplement conflicts with its canonical sourced application")
        client.event_supplements[key] = fact

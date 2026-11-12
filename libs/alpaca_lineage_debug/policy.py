"""One YAML policy, whitelisted rules, and the existing shared evaluator.

Research policies can change the established rule lists. They cannot authorize
production, replace implementations with imports, or change the frozen phases.
"""

import copy
import hashlib
import inspect
import json
from dataclasses import dataclass, replace
from pathlib import Path

import yaml

from libs.alpaca_end_of_day_security_correlation import (
    documented_markets,
    market_occupancy,
    recapitalizations,
    security_identity,
    terminal_fallback,
)
from libs.alpaca_end_of_day_security_correlation.component_refinement import NAME as REFINEMENT_NAME
from libs.alpaca_lineage_build import correlation_policy as compiler
from libs.alpaca_lineage_build import observations, transform
from libs.alpaca_security_lineage.event_supplements import accept_supplements
from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND

REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = compiler.CANONICAL_POLICY_PATH
RESEARCH_ROLE = "research_candidate"


def policy_applies(recorded, current):
    """Formatting and file location are provenance, not executable applicability."""
    actual = current.as_report_object()
    return all(
        recorded.get(name) == actual.get(name)
        for name in (
            "semantic_sha256",
            "implementation",
            "execution_profile",
            "policy_role",
            "research",
        )
    )


def policy_applies_to_group(recorded, current, group):
    """New facts in another explicit scope do not invalidate completed work."""
    actual = current.as_report_object()
    descriptive = {"policy_id", "policy_version", "file_sha256", "semantic_sha256", "source", "research"}
    if {k: v for k, v in recorded.items() if k not in descriptive} != {
        k: v for k, v in actual.items() if k not in descriptive
    }:
        return False

    def selected(value):
        options = value.get("research", {})
        facts = [
            entry for entry in options.get("documented_facts", ()) if set(entry["symbols"]).intersection(group)
        ]
        facts = [{**entry, "refine_accepted_dates": entry.get("refine_accepted_dates", True)} for entry in facts]
        mode = options.get("control_mode", "replay")
        if mode == "documented_scopes":
            mode = "accepted_publication" if facts else "replay"
        return options.get("membership_refinement"), mode, facts

    # Accepted immutable tuples and their JSON array spelling have one meaning.
    return json.dumps(selected(recorded), sort_keys=True) == json.dumps(selected(actual), sort_keys=True)


@dataclass(frozen=True)
class ResearchPolicy(compiler.CompiledPolicy):
    membership_refinement: str | None = None
    control_mode: str = "replay"
    documented_facts: tuple = ()

    def research_options(self):
        result = {"membership_refinement": self.membership_refinement}
        if self.control_mode != "replay" or self.documented_facts:
            result.update(control_mode=self.control_mode, documented_facts=list(self.documented_facts))
        return result

    def provenance(self):
        return {**super().provenance(), "research": self.research_options()}

    def as_report_object(self):
        return {**super().as_report_object(), "research": self.research_options()}


def load_policy(path):
    path = Path(path)
    with path.open("rb") as stream:
        raw = stream.read(compiler.MAX_POLICY_BYTES + 1)
    if len(raw) > compiler.MAX_POLICY_BYTES:
        raise ValueError("policy exceeds 65536 bytes")
    document = yaml.load(raw.decode("utf-8"), Loader=compiler._UniqueSafeLoader)
    if not isinstance(document, dict) or type(document.get("schema_version")) is not int:
        raise ValueError("policy schema_version must be an integer")
    if not isinstance(document, dict) or document.get("policy_role") != RESEARCH_ROLE:
        # Compile the bytes already read. The production compiler owns schema-4
        # meaning; older policy profiles remain available through the policy lab.
        if not isinstance(document, dict) or document.get("schema_version") != 4:
            raise ValueError("lineage debug requires a schema-4 YAML policy")
        return compiler._compile_schema_four(document, raw=raw, path=path)
    if document.get("schema_version") != 4:
        raise ValueError("research policies require schema_version 4")
    execution = document.get("execution")
    if not isinstance(execution, dict) or not isinstance(execution.get("established_fixed_point"), dict):
        raise ValueError("research policy requires an established_fixed_point mapping")
    established = execution["established_fixed_point"]
    ownership = compiler._rule_names(established.get("ownership_rules"), "ownership_rules")
    rows = compiler._rule_names(established.get("row_rules"), "row_rules")
    normalized = copy.deepcopy(document)
    options = normalized.pop("research", {"membership_refinement": None})
    if not isinstance(options, dict) or set(options) - {
        "membership_refinement",
        "control_mode",
        "documented_facts",
    }:
        raise ValueError("unknown research options")
    options.setdefault("membership_refinement", None)
    refinement = options["membership_refinement"]
    if refinement is not None and refinement != REFINEMENT_NAME:
        raise ValueError("unknown research membership refinement")
    control_mode = options.get("control_mode", "replay")
    facts = options.get("documented_facts", [])
    if control_mode not in {"replay", "accepted_publication", "documented_scopes"} or not isinstance(
        facts, list
    ):
        raise ValueError("invalid research control mode or documented facts")
    if facts and control_mode not in {"accepted_publication", "documented_scopes"}:
        raise ValueError("documented research facts require accepted_publication control")
    accepted_facts, fact_ids = [], set()
    for entry in facts:
        if (
            not isinstance(entry, dict)
            or set(entry) - {"symbols", "supplements", "refine_accepted_dates"}
            or not {"symbols", "supplements"} <= set(entry)
        ):
            raise ValueError("documented fact scope requires symbols and supplements")
        refine_dates = entry.get("refine_accepted_dates", True)
        if type(refine_dates) is not bool:
            raise ValueError("refine_accepted_dates must be boolean")
        scope = entry["symbols"]
        if (
            not isinstance(scope, list)
            or not scope
            or any(not isinstance(name, str) or not name or name != name.upper() for name in scope)
            or len(set(scope)) != len(scope)
        ):
            raise ValueError("documented facts need explicit unique symbol scope")
        supplements = accept_supplements(entry["supplements"])
        if any(
            item["kind"] != MARKET_KIND or item["facts"]["schema_version"] not in {3, 4, 5}
            for item in supplements
        ):
            raise ValueError("research chronology requires documented market facts")
        identifiers = {item["match"]["source_id"] for item in supplements}
        if fact_ids.intersection(identifiers):
            raise ValueError("duplicate documented fact across research scopes")
        fact_ids.update(identifiers)
        accepted_facts.append(
            {"symbols": tuple(scope), "supplements": supplements, "refine_accepted_dates": refine_dates}
        )
    normalized["policy_role"] = "production"
    target = normalized["execution"]["established_fixed_point"]
    target["ownership_rules"] = [rule.name for rule in compiler.OWNERSHIP_INTERVAL_RULES[:3]]
    target["row_rules"] = [
        rule.name for rule in compiler.ROW_CORRELATION_RULES if rule.name != compiler.MANDATORY_ROW_DEFAULT
    ]
    base = compiler._compile_schema_four(normalized, raw=raw, path=path)
    if (base.implementation_correlation_version, base.implementation_lineage_version) != (
        compiler.CORRELATION_VERSION,
        compiler.LINEAGE_VERSION,
    ):
        raise ValueError("research rule editing requires the current implementation")
    ownership_rules = compiler._resolve_rules(
        ownership, {rule.name: rule for rule in base.established_ownership_rules}, "ownership_rules"
    )
    row_rules = compiler._resolve_rules(
        rows,
        {rule.name: rule for rule in base.row_rules if rule.name != compiler.MANDATORY_ROW_DEFAULT},
        "row_rules",
    )
    semantic = {
        "research_contract": 1,
        "base_semantic_sha256": base.semantic_sha256,
        "ownership_rules": ownership,
        "row_rules": rows,
        "research": options,
    }
    research = ResearchPolicy(
        **base.__dict__,
        membership_refinement=refinement,
        control_mode=control_mode,
        documented_facts=tuple(accepted_facts),
    )
    return replace(
        research,
        policy_role=RESEARCH_ROLE,
        established_ownership_rules=ownership_rules,
        ownership_rules=(*ownership_rules, base.ownership_rules[-1]),
        row_rules=(*row_rules, base.row_rules[-1]),
        semantic_sha256=hashlib.sha256(
            json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


def template():
    document = yaml.load(DEFAULT_POLICY.read_bytes(), Loader=compiler._UniqueSafeLoader)
    document["policy_id"] = "lineage_research"
    document["policy_role"] = RESEARCH_ROLE
    document["research"] = {"membership_refinement": None}
    return (
        "# Research only: reorder or remove registered rules in the lists below.\n"
        "# The unresolved default, terminal rule and numerical postprocessor remain explicit.\n"
        + yaml.safe_dump(document, sort_keys=False)
    )


def location(function):
    source = inspect.getsourcefile(function)
    return {
        "file": str(Path(source).relative_to(REPOSITORY)),
        "line": inspect.getsourcelines(function)[1],
        "function": function.__name__,
        "description": inspect.getdoc(function),
        "basis": "current_executable_code",
    }


def rule_index(policy):
    def describe(rules):
        return [
            {"order": position, "name": rule.name, "implementation": location(rule.apply)}
            for position, rule in enumerate(rules, 1)
        ]

    return {
        "policy": policy.as_report_object(),
        "ownership_rules": describe(policy.ownership_rules),
        "established_ownership_rules": [rule.name for rule in policy.established_ownership_rules],
        "row_rules": describe(policy.row_rules),
        "terminal_rule": policy.terminal_rule,
        "terminal_implementation": location(terminal_fallback.apply_terminal_continuous_ticker_fallback),
        "fixed_steps": [
            {"name": name, "implementation": location(function), "yaml_selectable": False}
            for name, function in (
                ("shared_correlation_and_projection_adapter", transform.correlate),
                ("public_identity_membership", security_identity.security_identity_for_alpaca_assets),
                ("dated_market_occupancy", market_occupancy.project),
                ("recapitalizations", recapitalizations.project),
                ("documented_public_security_projection", documented_markets.project),
            )
        ],
        "numerical_source_selection": {
            "implementation": location(observations.select_observations),
            "yaml_selectable": False,
            "meaning": "Dated row ownership and the numerical observation supplying its prices are separate decisions.",
        },
        "phases": [
            "established fixed point",
            "freeze baseline",
            "unresolved complete-group reconciliation",
            "optional ownership fallback",
            "second fixed point",
            "implementation-bound market, recapitalization and documented-public projections",
            "numerical observation selection",
        ],
        "editing": {
            "role": RESEARCH_ROLE,
            "editable": ["established ownership rule order/subset", "row rule order/subset"],
            "new_predicates": "Implement and register generic Python rules; YAML cannot execute arbitrary code.",
            "production_publication": False,
        },
    }

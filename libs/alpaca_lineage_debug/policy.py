"""One YAML policy, whitelisted rules, and the existing shared evaluator.

Research policies can change the established rule lists. They cannot authorize
production, replace implementations with imports, or change the frozen phases.
"""

import copy
import hashlib
import inspect
import json
from dataclasses import dataclass, fields, replace
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
from libs.alpaca_lineage_build.ruleset import accept_components
from libs.alpaca_lineage_build.ruleset_contract import AcceptedRuleset
from libs.alpaca_security_lineage.event_supplements import _AcceptedSupplements, accept_supplements
from libs.alpaca_security_lineage.sourced_facts import MARKET_KIND

REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = compiler.CANONICAL_POLICY_PATH
RESEARCH_ROLE = "research_candidate"


def control_view(policy):
    """Lend the accepted rule cascade without injecting candidate facts/producers."""
    values = {field.name: getattr(policy, field.name) for field in fields(compiler.CompiledPolicy)}
    bundle = getattr(policy, "ruleset", None)
    return ControlPolicy(**values, ruleset_applications=bundle.applications if bundle is not None else ())


def require_research_application_support(policy, control, group):
    """Reject new unsupported factual intent before materializing any prices."""
    bundle = getattr(policy, "ruleset", None)
    if bundle is None:
        return
    group = set(group)

    def scoped(applications):
        return {entry["id"]: entry for entry in applications if group.intersection(entry["symbols"])}

    current = scoped(bundle.applications)
    control_bundle = getattr(control, "ruleset", None)
    prior = scoped(
        getattr(
            control,
            "ruleset_applications",
            control_bundle.applications if control_bundle is not None else (),
        )
    )
    for identifier in current.keys() | prior.keys():
        entry, old = current.get(identifier), prior.get(identifier)
        if entry is not None and old is not None and entry["fingerprint"] == old["fingerprint"]:
            continue
        if entry is None:
            raise ValueError(
                f"removing configured fact application {identifier} requires owning evidence reconciliation"
            )
        if any(
            fact["kind"] != MARKET_KIND or fact["facts"]["schema_version"] not in {3, 4, 5}
            for fact in entry["facts"]
        ):
            raise ValueError(
                f"unsupported research-fact producer for application {identifier}; "
                "use its owning evidence producer before comparison"
            )


@dataclass(frozen=True)
class ControlPolicy(compiler.CompiledPolicy):
    # Inputs identifying unchanged configured facts, never instructions to apply
    # them to the accepted publication used as control.
    ruleset_applications: tuple = ()


def policy_applies(recorded, current):
    """Formatting and file location are provenance, not executable applicability."""
    actual = current.as_report_object()

    def research(value):
        options = value.get("research")
        if (
            type(options) is dict
            and not set(options) - {"membership_refinement", "control_mode", "documented_facts"}
            and options.get("membership_refinement") is None
            and options.get("control_mode", "replay") == "replay"
            and options.get("documented_facts", ()) in ((), [])
        ):
            return None
        return options

    return all(
        recorded.get(name) == actual.get(name)
        for name in ("semantic_sha256", "implementation", "execution_profile", "policy_role")
    ) and research(recorded) == research(actual)


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
    ruleset: AcceptedRuleset | None = None

    def research_options(self):
        result = {"membership_refinement": self.membership_refinement}
        if self.control_mode != "replay" or self.documented_facts:
            result.update(control_mode=self.control_mode, documented_facts=list(self.documented_facts))
        if self.ruleset is not None:
            result.update(
                execution_sha256=self.ruleset.execution_sha256,
                rule_applications=[
                    {key: entry[key] for key in ("id", "symbols", "fingerprint")}
                    for entry in self.ruleset.applications
                ],
            )
        return result

    def provenance(self):
        return {**super().provenance(), "research": self.research_options()}

    def as_report_object(self):
        return {**super().as_report_object(), "research": self.research_options()}


def load_policy(path):
    # Lexical normalization performs no filesystem authentication and lets a
    # relative canonical argument lend its completed configuration to control.
    path = Path(path).absolute()
    raw, outer = compiler.read_policy_document(path)
    document = compiler.policy_document(outer)
    if "ruleset_schema_version" not in outer:
        return _compile_research_document(path, raw, document)
    applications, supplements, refinement, execution_sha256 = accept_components(outer)
    documented = []
    for entry in applications:
        facts = _AcceptedSupplements(
            fact
            for fact in entry["facts"]
            if fact["kind"] == MARKET_KIND and fact["facts"]["schema_version"] in {3, 4, 5}
        )
        if facts:
            documented.append(
                {
                    "symbols": list(entry["symbols"]),
                    "supplements": facts,
                    "refine_accepted_dates": entry.get("refine_accepted_dates", True),
                }
            )
    role = document.get("policy_role")
    if role == RESEARCH_ROLE:
        settings = document.get("research", {})
        if type(settings) is not dict or set(settings) - {"control_mode"}:
            raise ValueError("combined YAML keeps facts in evidence and membership selection in producers")
        candidate = copy.deepcopy(document)
        candidate["research"] = {
            "membership_refinement": refinement,
            "control_mode": settings.get("control_mode", "documented_scopes"),
            "documented_facts": documented,
        }
        policy = _compile_research_document(path, raw, candidate)
    else:
        base = compiler.compile_policy_document(document, raw=raw, path=path)
        policy = ResearchPolicy(
            **base.__dict__,
            membership_refinement=refinement,
            control_mode="documented_scopes",
            documented_facts=tuple(documented),
        )
    accepted = AcceptedRuleset(policy, applications, supplements, refinement, execution_sha256)
    return replace(policy, ruleset=accepted)


def _compile_research_document(path, raw, document):
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


def template(*, combined=False):
    _, document = compiler.read_policy_document(DEFAULT_POLICY)
    policy = compiler.policy_document(document)
    policy["policy_id"] = "lineage_research"
    policy["policy_role"] = RESEARCH_ROLE
    policy["research"] = {"control_mode": "documented_scopes"} if combined else {"membership_refinement": None}
    if not combined:
        document = policy
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

    result = {
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
    bundle = getattr(policy, "ruleset", None)
    if bundle is not None:
        result["ruleset"] = {
            "schema_version": 1,
            "execution_sha256": bundle.execution_sha256,
            "membership_refinement": bundle.membership_refinement,
            "applications": [
                {
                    "id": entry["id"],
                    "symbols": entry["symbols"],
                    "fingerprint": entry["fingerprint"],
                    "refine_accepted_dates": entry.get("refine_accepted_dates", True),
                    "facts": [
                        {
                            "kind": fact["kind"],
                            "schema_version": fact["facts"]["schema_version"],
                            "diagnostic_chronology_projection": fact["kind"] == MARKET_KIND
                            and fact["facts"]["schema_version"] in {3, 4, 5},
                        }
                        for fact in entry["facts"]
                    ],
                }
                for entry in bundle.applications
            ],
            "meaning": "Bounded documented-market updates are explicit research work; other facts remain with their existing producer.",
        }
    return result

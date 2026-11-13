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
    security_identity,
    terminal_fallback,
)
from libs.alpaca_end_of_day_security_correlation.component_refinement import NAME as REFINEMENT_NAME
from libs.alpaca_lineage_build import correlation_policy as compiler
from libs.alpaca_lineage_build import observations, transform
from libs.alpaca_lineage_build.ruleset import accept_components
from libs.alpaca_lineage_build.ruleset_contract import AcceptedRuleset
from libs.alpaca_security_lineage.inference_evidence import reject_assertions

REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = compiler.CANONICAL_POLICY_PATH
RESEARCH_ROLE = "research_candidate"


def require_generic_policy(policy):
    """Reject removed fact candidates before any selected storage is opened."""
    if policy is None:
        return
    if hasattr(policy, "documented_facts") or getattr(policy, "control_mode", "replay") != "replay":
        raise ValueError("fact-backed research policies are not supported; use registered generic rules")
    compiler.reject_injected_evidence(vars(policy))
    reject_assertions(vars(policy), where="debug candidate policy")
    bundle = getattr(policy, "ruleset", None)
    if bundle is not None and (getattr(bundle, "applications", ()) or getattr(bundle, "supplements", ())):
        raise ValueError("configured fact applications are not supported")


def control_view(policy):
    """Lend the accepted generic cascade with the candidate producer disabled."""
    require_generic_policy(policy)
    values = {field.name: getattr(policy, field.name) for field in fields(compiler.CompiledPolicy)}
    return compiler.CompiledPolicy(**values)


def _generic_options(value):
    options = value.get("research", {})
    if type(options) is not dict or set(options) - {
        "membership_refinement",
        "control_mode",
        "execution_sha256",
        "rule_applications",
    }:
        raise ValueError("saved policy contains unsupported research inputs")
    if options.get("control_mode", "replay") != "replay" or options.get("rule_applications", ()):
        raise ValueError("saved policy contains fact-backed research inputs")
    return options.get("membership_refinement"), options.get("execution_sha256")


def policy_applies(recorded, current):
    """Formatting and file location are provenance, not executable applicability."""
    require_generic_policy(current)
    actual = current.as_report_object()
    return all(
        recorded.get(name) == actual.get(name)
        for name in ("semantic_sha256", "implementation", "execution_profile", "policy_role")
    ) and _generic_options(recorded) == _generic_options(actual)


def policy_applies_to_group(recorded, current, group):
    """Generic rule/producer bindings apply equally to every selected identity."""
    return policy_applies(recorded, current)


@dataclass(frozen=True)
class ResearchPolicy(compiler.CompiledPolicy):
    membership_refinement: str | None = None
    ruleset: AcceptedRuleset | None = None

    def research_options(self):
        result = {"membership_refinement": self.membership_refinement}
        if self.ruleset is not None:
            result["execution_sha256"] = self.ruleset.execution_sha256
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
    refinement, execution_sha256 = accept_components(outer)
    if document.get("policy_role") == RESEARCH_ROLE:
        settings = document.get("research", {})
        if type(settings) is not dict or set(settings) - {"control_mode"}:
            raise ValueError("combined YAML keeps generic producer selection in producers")
        candidate = copy.deepcopy(document)
        candidate["research"] = {"membership_refinement": refinement, **settings}
        policy = _compile_research_document(path, raw, candidate)
    else:
        base = compiler.compile_policy_document(document, raw=raw, path=path)
        policy = ResearchPolicy(**base.__dict__, membership_refinement=refinement)
    accepted = AcceptedRuleset(policy, refinement, execution_sha256)
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
    if not isinstance(options, dict) or set(options) - {"membership_refinement", "control_mode"}:
        raise ValueError("research accepts generic producer selection only")
    if options.get("control_mode", "replay") != "replay":
        raise ValueError("fact-backed research control modes are not supported")
    options.pop("control_mode", None)
    options.setdefault("membership_refinement", None)
    refinement = options["membership_refinement"]
    if refinement is not None and refinement != REFINEMENT_NAME:
        raise ValueError("unknown research membership refinement")
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
    policy["research"] = {} if combined else {"membership_refinement": None}
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
            "schema_version": compiler.RULESET_SCHEMA_VERSION,
            "execution_sha256": bundle.execution_sha256,
            "membership_refinement": bundle.membership_refinement,
            "meaning": "Registered generic policy and producer selection only.",
        }
    return result

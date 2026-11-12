"""One YAML policy, whitelisted rules, and the existing shared evaluator.

Research policies can change the established rule lists. They cannot authorize
production, replace implementations with imports, or change the frozen phases.
"""

import copy
import hashlib
import inspect
import json
from dataclasses import replace
from pathlib import Path

import yaml

from libs.alpaca_end_of_day_security_correlation import (
    documented_markets,
    market_occupancy,
    recapitalizations,
    security_identity,
    terminal_fallback,
)
from libs.alpaca_lineage_build import correlation_policy as compiler
from libs.alpaca_lineage_build import observations, transform

REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_POLICY = compiler.CANONICAL_POLICY_PATH
RESEARCH_ROLE = "research_candidate"


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
    }
    return replace(
        base,
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

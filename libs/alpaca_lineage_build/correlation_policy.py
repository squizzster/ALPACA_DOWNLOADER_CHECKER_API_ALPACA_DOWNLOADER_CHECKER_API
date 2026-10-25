"""Compile strict YAML into the shared, whitelisted correlation policy engine."""

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from libs.alpaca_end_of_day_security_correlation.correlation_version import (
    CORRELATION_VERSION,
    validate_correlation_change_log,
)
from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    OWNERSHIP_INTERVAL_RULES,
    OwnershipIntervalRule,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    ROW_CORRELATION_RULES,
    RowCorrelationRule,
)
from libs.alpaca_end_of_day_security_correlation.terminal_fallback import (
    TERMINAL_FALLBACK_RULE_NAME,
)
from libs.alpaca_lineage_build.contract import VERSION as LINEAGE_VERSION

from .contract import SUPPORTED_POLICY_SCHEMA_VERSIONS, CorrelationPolicyError
from .correlation_policy_contract import (
    PRODUCTION_EXECUTION_PROFILE,
    PRODUCTION_POLICY_ROLE,
    reusable_policy,
)
from .correlation_policy_contract import (
    PRODUCTION_POLICY_ID as PRODUCTION_POLICY_ID,
)
from .correlation_policy_contract import (
    PRODUCTION_POLICY_SEMANTIC_SHA256 as PRODUCTION_POLICY_SEMANTIC_SHA256,
)
from .correlation_policy_contract import (
    PRODUCTION_POLICY_VERSION as PRODUCTION_POLICY_VERSION,
)
from .correlation_policy_contract import (
    expected_policy_identity as expected_policy_identity,
)
from .policy_active_suffix import (
    EFFECT as ACTIVE_SUFFIX_EFFECT,
)
from .policy_active_suffix import (
    FACT_PRODUCER as ACTIVE_SUFFIX_FACT_PRODUCER,
)
from .policy_active_suffix import (
    FACT_TYPES as ACTIVE_SUFFIX_FACT_TYPES,
)
from .policy_active_suffix import (
    ActiveSuffixRule,
)
from .policy_engine import (
    CORRECTED_PROFILE,
    CORRECTED_RECONCILIATION,
    CORRECTED_SUFFIX,
    INDEPENDENT_DEPENDENCY,
    LEGACY_DEPENDENCY,
    PUBLISHED_PROFILE,
    PUBLISHED_RECONCILIATION,
    PUBLISHED_SUFFIX,
)
from .policy_evidence_quality import (
    EFFECT as EVIDENCE_EFFECT,
)
from .policy_evidence_quality import (
    FACT_PRODUCER as EVIDENCE_FACT_PRODUCER,
)
from .policy_evidence_quality import (
    FACT_TYPES as EVIDENCE_FACT_TYPES,
)
from .policy_evidence_quality import (
    EvidenceQualityRule,
)

MAX_POLICY_BYTES = 64 * 1024
MANDATORY_ROW_DEFAULT = "unresolved_uuid_candidates"
CANONICAL_POLICY_PATH = (
    Path(__file__).resolve().parents[2] / "resources" / "alpaca_security_correlation_policy.yaml"
)
_ID = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_RULE_ID = re.compile(r"[A-Z][A-Z0-9_]{0,95}\Z")
_VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\Z")


class _UniqueSafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as error:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from error
        if duplicate:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class CompiledPolicy:
    schema_version: int
    policy_id: str
    policy_version: str
    policy_role: str
    execution_profile: str
    implementation_correlation_version: int
    implementation_lineage_version: int | None
    max_passes: int
    established_ownership_rules: tuple[OwnershipIntervalRule, ...]
    ownership_rules: tuple[OwnershipIntervalRule, ...]
    row_rules: tuple[RowCorrelationRule, ...]
    terminal_rule: str
    evidence_fact_producers: tuple[str, ...]
    evidence_fact_types: tuple[tuple[str, str], ...]
    evidence_rules: tuple[EvidenceQualityRule, ...]
    active_suffix_fact_producers: tuple[str, ...]
    active_suffix_fact_types: tuple[tuple[str, str], ...]
    active_suffix_rules: tuple[ActiveSuffixRule, ...]
    fallback_profile: str | None
    reconciliation_producer: str | None
    ownership_fallback_producer: str | None
    producer_dependency: str | None
    file_sha256: str
    semantic_sha256: str
    source: str

    def provenance(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_role": self.policy_role,
            "execution_profile": self.execution_profile,
            "implementation": {
                "correlation_version": self.implementation_correlation_version,
                "lineage_version": self.implementation_lineage_version,
            },
            "semantic_sha256": self.semantic_sha256,
            "file_sha256": self.file_sha256,
        }

    def as_report_object(self) -> dict[str, Any]:
        return {
            **self.provenance(),
            "implementation_correlation_version": self.implementation_correlation_version,
            "implementation_lineage_version": self.implementation_lineage_version,
            "max_passes": self.max_passes,
            "established_ownership_rules": [rule.name for rule in self.established_ownership_rules],
            "ownership_rules": [rule.name for rule in self.ownership_rules],
            "row_rules": [rule.name for rule in self.row_rules],
            "terminal_rule": self.terminal_rule,
            "evidence_fact_producers": list(self.evidence_fact_producers),
            "evidence_fact_types": dict(self.evidence_fact_types),
            "evidence_rules": [rule.as_report_object() for rule in self.evidence_rules],
            "active_suffix_fact_producers": list(self.active_suffix_fact_producers),
            "active_suffix_fact_types": dict(self.active_suffix_fact_types),
            "active_suffix_rules": [rule.as_report_object() for rule in self.active_suffix_rules],
            "fallback_profile": self.fallback_profile,
            "reconciliation_producer": self.reconciliation_producer,
            "ownership_fallback_producer": self.ownership_fallback_producer,
            "producer_dependency": self.producer_dependency,
            "file_sha256": self.file_sha256,
            "semantic_sha256": self.semantic_sha256,
            "source": self.source,
        }


def _mapping(value, where, keys):
    if type(value) is not dict:
        raise CorrelationPolicyError(f"{where} must be a mapping")
    actual = set(value)
    expected = set(keys)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(str(key) for key in actual - expected)
        detail = []
        if missing:
            detail.append(f"missing {missing}")
        if unknown:
            detail.append(f"unknown {unknown}")
        raise CorrelationPolicyError(f"{where} has invalid keys: {', '.join(detail)}")
    return value


def _string(value, where, pattern=None):
    if type(value) is not str or not value or (pattern is not None and pattern.fullmatch(value) is None):
        raise CorrelationPolicyError(f"{where} is invalid")
    return value


def _integer(value, where, *, lower=None, upper=None):
    if type(value) is not int:
        raise CorrelationPolicyError(f"{where} must be an integer")
    if lower is not None and value < lower or upper is not None and value > upper:
        raise CorrelationPolicyError(f"{where} must be between {lower} and {upper}")
    return value


def _rule_names(value, where):
    if type(value) is not list:
        raise CorrelationPolicyError(f"{where} must be a list")
    if any(type(item) is not str or not item for item in value):
        raise CorrelationPolicyError(f"{where} must contain nonempty rule names")
    if len(set(value)) != len(value):
        raise CorrelationPolicyError(f"{where} contains duplicate rule names")
    return value


def _resolve_rules(names, registry, where):
    unknown = sorted(set(names) - registry.keys())
    if unknown:
        raise CorrelationPolicyError(f"{where} contains unknown rules: {unknown}")
    return tuple(registry[name] for name in names)


def _compile_evidence_policy(root, stages, *, exact_facts):
    facts = root["facts"]
    if type(facts) is not dict or (
        set(facts) != set(EVIDENCE_FACT_TYPES) if exact_facts else not set(EVIDENCE_FACT_TYPES).issubset(facts)
    ):
        raise CorrelationPolicyError("facts must declare exactly the supported evidence-quality facts")
    for name, expected_type in EVIDENCE_FACT_TYPES.items():
        declaration = _mapping(facts[name], f"facts.{name}", ("type",))
        if declaration["type"] != expected_type:
            raise CorrelationPolicyError(f"facts.{name}.type must be {expected_type!r}")

    stage = _mapping(stages["evidence_quality"], "stages.evidence_quality", ("type", "rules"))
    if stage["type"] != "per_candidate_action":
        raise CorrelationPolicyError("stages.evidence_quality.type must be 'per_candidate_action'")
    rule_values = stage["rules"]
    if type(rule_values) is not list or not rule_values:
        raise CorrelationPolicyError("stages.evidence_quality.rules must be a nonempty list")
    rules = []
    semantic_rules = []
    seen_ids = set()
    for index, value in enumerate(rule_values):
        where = f"stages.evidence_quality.rules[{index}]"
        rule = _mapping(value, where, ("id", "description", "scope", "when", "effect"))
        rule_id = _string(rule["id"], where + ".id", _RULE_ID)
        if rule_id in seen_ids:
            raise CorrelationPolicyError("stages.evidence_quality.rules contains duplicate rule IDs")
        seen_ids.add(rule_id)
        description = _string(rule["description"], where + ".description")
        if rule["scope"] != "cross_receipt_action_pair":
            raise CorrelationPolicyError(where + ".scope must be 'cross_receipt_action_pair'")
        when = _mapping(rule["when"], where + ".when", ("all",))
        conditions = when["all"]
        if type(conditions) is not list or not conditions:
            raise CorrelationPolicyError(where + ".when.all must be a nonempty list")
        compiled_conditions = []
        seen_facts = set()
        semantic_conditions = []
        for condition_index, condition_value in enumerate(conditions):
            condition_where = f"{where}.when.all[{condition_index}]"
            condition = _mapping(condition_value, condition_where, ("fact", "eq"))
            fact = _string(condition["fact"], condition_where + ".fact")
            if fact not in EVIDENCE_FACT_TYPES:
                raise CorrelationPolicyError(f"{condition_where}.fact is unknown: {fact!r}")
            if fact in seen_facts:
                raise CorrelationPolicyError(f"{where}.when.all contains duplicate fact {fact!r}")
            seen_facts.add(fact)
            expected = condition["eq"]
            if type(expected) is not bool:
                raise CorrelationPolicyError(condition_where + ".eq must be a boolean")
            compiled_conditions.append((fact, expected))
            semantic_conditions.append({"fact": fact, "eq": expected})
        if seen_facts != set(EVIDENCE_FACT_TYPES):
            raise CorrelationPolicyError(f"{where}.when.all must gate every supported fact exactly once")
        effect = _mapping(rule["effect"], where + ".effect", tuple(EVIDENCE_EFFECT))
        if effect != EVIDENCE_EFFECT:
            raise CorrelationPolicyError(f"{where}.effect must be the fixed lab quarantine effect")
        compiled = EvidenceQualityRule(
            rule_id=rule_id,
            description=description,
            scope="cross_receipt_action_pair",
            conditions=tuple(compiled_conditions),
            effect=tuple(EVIDENCE_EFFECT.items()),
        )
        rules.append(compiled)
        semantic_rules.append(
            {
                "id": rule_id,
                "description": description,
                "scope": "cross_receipt_action_pair",
                "when": {"all": semantic_conditions},
                "effect": dict(EVIDENCE_EFFECT),
            }
        )
    return (
        (EVIDENCE_FACT_PRODUCER,),
        tuple(sorted(EVIDENCE_FACT_TYPES.items())),
        tuple(rules),
        semantic_rules,
    )


def _compile_active_suffix_policy(root, stages):
    facts = root["facts"]
    if type(facts) is not dict or not set(ACTIVE_SUFFIX_FACT_TYPES).issubset(facts):
        raise CorrelationPolicyError("facts must declare every supported active-suffix fact")
    for name, expected_type in ACTIVE_SUFFIX_FACT_TYPES.items():
        declaration = _mapping(facts[name], f"facts.{name}", ("type",))
        if declaration["type"] != expected_type:
            raise CorrelationPolicyError(f"facts.{name}.type must be {expected_type!r}")
    stage = _mapping(
        stages["active_suffix_assignment"],
        "stages.active_suffix_assignment",
        ("type", "position", "rules"),
    )
    if stage["type"] != "per_symbol_certificate":
        raise CorrelationPolicyError("stages.active_suffix_assignment.type must be 'per_symbol_certificate'")
    if stage["position"] != "immediately_before_unresolved_fallback":
        raise CorrelationPolicyError(
            "stages.active_suffix_assignment.position must be 'immediately_before_unresolved_fallback'"
        )
    values = stage["rules"]
    if type(values) is not list or not values:
        raise CorrelationPolicyError("stages.active_suffix_assignment.rules must be a nonempty list")
    compiled_rules = []
    semantic_rules = []
    seen_ids = set()
    for index, value in enumerate(values):
        where = f"stages.active_suffix_assignment.rules[{index}]"
        rule = _mapping(value, where, ("id", "description", "scope", "when", "effect"))
        rule_id = _string(rule["id"], where + ".id", _RULE_ID)
        if rule_id in seen_ids:
            raise CorrelationPolicyError("active-suffix rules contain duplicate rule IDs")
        seen_ids.add(rule_id)
        description = _string(rule["description"], where + ".description")
        if rule["scope"] != "single_symbol_observed_suffix":
            raise CorrelationPolicyError(where + ".scope must be 'single_symbol_observed_suffix'")
        when = _mapping(rule["when"], where + ".when", ("all",))
        conditions = when["all"]
        if type(conditions) is not list or not conditions:
            raise CorrelationPolicyError(where + ".when.all must be a nonempty list")
        compiled_conditions = []
        semantic_conditions = []
        seen_facts = set()
        for condition_index, value in enumerate(conditions):
            condition_where = f"{where}.when.all[{condition_index}]"
            condition = _mapping(value, condition_where, ("fact", "eq"))
            fact = _string(condition["fact"], condition_where + ".fact")
            if fact not in ACTIVE_SUFFIX_FACT_TYPES:
                raise CorrelationPolicyError(f"{condition_where}.fact is unknown: {fact!r}")
            if fact in seen_facts:
                raise CorrelationPolicyError(f"{where}.when.all contains duplicate fact {fact!r}")
            seen_facts.add(fact)
            expected = condition["eq"]
            if type(expected) is not bool:
                raise CorrelationPolicyError(condition_where + ".eq must be a boolean")
            compiled_conditions.append((fact, expected))
            semantic_conditions.append({"fact": fact, "eq": expected})
        if seen_facts != set(ACTIVE_SUFFIX_FACT_TYPES):
            raise CorrelationPolicyError(
                f"{where}.when.all must gate every supported active-suffix fact exactly once"
            )
        effect = _mapping(rule["effect"], where + ".effect", tuple(ACTIVE_SUFFIX_EFFECT))
        if effect != ACTIVE_SUFFIX_EFFECT:
            raise CorrelationPolicyError(f"{where}.effect must be the fixed active-suffix effect")
        compiled_rules.append(
            ActiveSuffixRule(
                rule_id=rule_id,
                description=description,
                scope="single_symbol_observed_suffix",
                conditions=tuple(compiled_conditions),
                effect=tuple(ACTIVE_SUFFIX_EFFECT.items()),
            )
        )
        semantic_rules.append(
            {
                "id": rule_id,
                "description": description,
                "scope": "single_symbol_observed_suffix",
                "when": {"all": semantic_conditions},
                "effect": dict(ACTIVE_SUFFIX_EFFECT),
            }
        )
    return (
        (ACTIVE_SUFFIX_FACT_PRODUCER,),
        tuple(sorted(ACTIVE_SUFFIX_FACT_TYPES.items())),
        tuple(compiled_rules),
        semantic_rules,
    )


def _compile_schema_four(
    document: dict[str, Any],
    *,
    raw: bytes,
    path: Path,
) -> CompiledPolicy:
    """Compile the explicit frozen-baseline/two-phase correlation-v59 cascade."""
    policy_role_value = document.get("policy_role")
    root_keys = (
        "schema_version",
        "policy_id",
        "policy_version",
        "policy_role",
        "implementation",
        "execution",
    )
    if policy_role_value == "corrected_candidate":
        root_keys = (*root_keys, "fact_producers", "facts", "stages")
    root = _mapping(
        document,
        "policy",
        root_keys,
    )
    policy_id = _string(root["policy_id"], "policy_id", _ID)
    policy_version = _string(root["policy_version"], "policy_version", _VERSION)
    policy_role = _string(root["policy_role"], "policy_role")
    implementation = _mapping(
        root["implementation"],
        "implementation",
        ("correlation_version", "lineage_version"),
    )
    correlation_version = _integer(
        implementation["correlation_version"],
        "implementation.correlation_version",
    )
    lineage_version = _integer(
        implementation["lineage_version"],
        "implementation.lineage_version",
    )
    implementation_tuple = (correlation_version, lineage_version)
    if implementation_tuple not in {(59, 33), (CORRELATION_VERSION, LINEAGE_VERSION)}:
        raise CorrelationPolicyError(
            "schema-v4 implementation must be the preserved 59/33 profile or the running "
            f"{CORRELATION_VERSION}/{LINEAGE_VERSION} profile"
        )

    execution = _mapping(
        root["execution"],
        "execution",
        (
            "established_fixed_point",
            "frozen_baseline",
            "reconciliation",
            "ownership_fallback",
            "second_fixed_point",
            "producer_evaluation",
            "observation_selection",
        ),
    )
    established = _mapping(
        execution["established_fixed_point"],
        "execution.established_fixed_point",
        (
            "max_passes",
            "ownership_rules",
            "row_rules",
            "row_default",
            "terminal_rule",
        ),
    )
    max_passes = _integer(
        established["max_passes"],
        "execution.established_fixed_point.max_passes",
        lower=2,
        upper=10,
    )
    if max_passes != 3:
        raise CorrelationPolicyError("schema-v4 established fixed point must use exactly three passes")
    ownership_names = _rule_names(
        established["ownership_rules"],
        "execution.established_fixed_point.ownership_rules",
    )
    expected_ownership = [rule.name for rule in OWNERSHIP_INTERVAL_RULES[:3]]
    if ownership_names != expected_ownership:
        raise CorrelationPolicyError(
            "schema-v4 established ownership rules must be the ordered first three v59 rules"
        )
    row_names = _rule_names(
        established["row_rules"],
        "execution.established_fixed_point.row_rules",
    )
    expected_rows = [rule.name for rule in ROW_CORRELATION_RULES if rule.name != MANDATORY_ROW_DEFAULT]
    if row_names != expected_rows:
        raise CorrelationPolicyError("schema-v4 row rules must be the unchanged ordered v59 rules")
    if established["row_default"] != MANDATORY_ROW_DEFAULT:
        raise CorrelationPolicyError(
            f"execution.established_fixed_point.row_default must be {MANDATORY_ROW_DEFAULT!r}"
        )
    if established["terminal_rule"] != TERMINAL_FALLBACK_RULE_NAME:
        raise CorrelationPolicyError(
            f"execution.established_fixed_point.terminal_rule must be {TERMINAL_FALLBACK_RULE_NAME!r}"
        )
    if execution["frozen_baseline"] is not True:
        raise CorrelationPolicyError("execution.frozen_baseline must be true")

    reconciliation = _mapping(
        execution["reconciliation"],
        "execution.reconciliation",
        ("scope", "implementation"),
    )
    if reconciliation["scope"] != "unresolved_only_complete_group":
        raise CorrelationPolicyError("execution.reconciliation.scope must be 'unresolved_only_complete_group'")
    reconciliation_producer = _string(
        reconciliation["implementation"],
        "execution.reconciliation.implementation",
    )
    fallback = _mapping(
        execution["ownership_fallback"],
        "execution.ownership_fallback",
        ("optional", "position", "implementation"),
    )
    if fallback["optional"] is not True or fallback["position"] != 4:
        raise CorrelationPolicyError("schema-v4 ownership fallback must be optional rule position 4")
    fallback_producer = _string(
        fallback["implementation"],
        "execution.ownership_fallback.implementation",
    )
    second = _mapping(
        execution["second_fixed_point"],
        "execution.second_fixed_point",
        ("seed", "max_passes"),
    )
    if second["seed"] != "frozen_baseline" or second["max_passes"] != max_passes:
        raise CorrelationPolicyError(
            "schema-v4 second fixed point must use the frozen baseline and the established pass limit"
        )
    producers = _mapping(
        execution["producer_evaluation"],
        "execution.producer_evaluation",
        ("timing", "dependency"),
    )
    if producers["timing"] != "once_from_frozen_baseline":
        raise CorrelationPolicyError("execution.producer_evaluation.timing must be 'once_from_frozen_baseline'")
    dependency = _string(
        producers["dependency"],
        "execution.producer_evaluation.dependency",
    )
    profile_values = (
        reconciliation_producer,
        fallback_producer,
        dependency,
    )
    if profile_values == (PUBLISHED_RECONCILIATION, PUBLISHED_SUFFIX, LEGACY_DEPENDENCY):
        fallback_profile = PUBLISHED_PROFILE
        expected_role = (
            PRODUCTION_POLICY_ROLE
            if implementation_tuple == (CORRELATION_VERSION, LINEAGE_VERSION)
            else "published_reference"
        )
    elif profile_values == (CORRECTED_RECONCILIATION, CORRECTED_SUFFIX, INDEPENDENT_DEPENDENCY):
        fallback_profile = CORRECTED_PROFILE
        expected_role = "corrected_candidate"
    else:
        raise CorrelationPolicyError("schema-v4 fallback implementations do not form a whitelisted profile")
    if policy_role != expected_role:
        raise CorrelationPolicyError(f"schema-v4 fallback profile requires policy_role {expected_role!r}")
    observation = _mapping(
        execution["observation_selection"],
        "execution.observation_selection",
        ("type", "implementation"),
    )
    expected_observation = f"lineage_v{lineage_version}"
    if observation != {
        "type": "fixed_python_postprocessor",
        "implementation": expected_observation,
    }:
        raise CorrelationPolicyError(
            f"schema-v4 observation selection must be the fixed {expected_observation} postprocessor"
        )

    if fallback_profile == CORRECTED_PROFILE:
        expected_producers = [EVIDENCE_FACT_PRODUCER, ACTIVE_SUFFIX_FACT_PRODUCER]
        if root["fact_producers"] != expected_producers:
            raise CorrelationPolicyError(f"fact_producers must be {expected_producers!r}")
        expected_facts = {*EVIDENCE_FACT_TYPES, *ACTIVE_SUFFIX_FACT_TYPES}
        if type(root["facts"]) is not dict or set(root["facts"]) != expected_facts:
            raise CorrelationPolicyError("facts must declare exactly the corrected schema-v4 facts")
        typed_stages = _mapping(
            root["stages"],
            "stages",
            ("evidence_quality", "active_suffix_assignment"),
        )
        (
            evidence_fact_producers,
            evidence_fact_types,
            evidence_rules,
            semantic_evidence_rules,
        ) = _compile_evidence_policy(root, typed_stages, exact_facts=False)
        (
            active_suffix_fact_producers,
            active_suffix_fact_types,
            active_suffix_rules,
            semantic_active_suffix_rules,
        ) = _compile_active_suffix_policy(root, typed_stages)
        if len(evidence_rules) != 1 or len(active_suffix_rules) != 1:
            raise CorrelationPolicyError("corrected schema-v4 requires exactly one rule for each typed producer")
    else:
        evidence_fact_producers = ()
        evidence_fact_types = ()
        evidence_rules = ()
        semantic_evidence_rules = None
        active_suffix_fact_producers = ()
        active_suffix_fact_types = ()
        active_suffix_rules = ()
        semantic_active_suffix_rules = None

    established_rules = tuple(OWNERSHIP_INTERVAL_RULES[:3])
    ownership_rules = (*established_rules, OWNERSHIP_INTERVAL_RULES[3])
    row_rules = tuple(ROW_CORRELATION_RULES)
    semantic = {
        "schema_version": 4,
        "execution": {
            "established_fixed_point": {
                "max_passes": max_passes,
                "ownership_rules": ownership_names,
                "row_rules": row_names,
                "row_default": MANDATORY_ROW_DEFAULT,
                "terminal_rule": TERMINAL_FALLBACK_RULE_NAME,
            },
            "frozen_baseline": True,
            "reconciliation": {
                "scope": "unresolved_only_complete_group",
                "implementation": reconciliation_producer,
            },
            "ownership_fallback": {
                "optional": True,
                "position": 4,
                "implementation": fallback_producer,
            },
            "second_fixed_point": {
                "seed": "frozen_baseline",
                "max_passes": max_passes,
            },
            "producer_evaluation": {
                "timing": "once_from_frozen_baseline",
                "dependency": dependency,
            },
            "observation_selection": {
                "type": "fixed_python_postprocessor",
                "implementation": "lineage_observation_selection",
            },
        },
    }
    if fallback_profile == CORRECTED_PROFILE:
        semantic.update(
            fact_producers=[*evidence_fact_producers, *active_suffix_fact_producers],
            facts={name: {"type": kind} for name, kind in (*evidence_fact_types, *active_suffix_fact_types)},
            stages={
                "evidence_quality": {
                    "type": "per_candidate_action",
                    "rules": semantic_evidence_rules,
                },
                "active_suffix_assignment": {
                    "type": "per_symbol_certificate",
                    "position": "immediately_before_unresolved_fallback",
                    "rules": semantic_active_suffix_rules,
                },
            },
        )
    canonical = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    return CompiledPolicy(
        schema_version=4,
        policy_id=policy_id,
        policy_version=policy_version,
        policy_role=policy_role,
        execution_profile=(
            PRODUCTION_EXECUTION_PROFILE
            if policy_role == PRODUCTION_POLICY_ROLE
            else "correlation_v59_two_phase"
        ),
        implementation_correlation_version=correlation_version,
        implementation_lineage_version=lineage_version,
        max_passes=max_passes,
        established_ownership_rules=established_rules,
        ownership_rules=ownership_rules,
        row_rules=row_rules,
        terminal_rule=TERMINAL_FALLBACK_RULE_NAME,
        evidence_fact_producers=evidence_fact_producers,
        evidence_fact_types=evidence_fact_types,
        evidence_rules=evidence_rules,
        active_suffix_fact_producers=active_suffix_fact_producers,
        active_suffix_fact_types=active_suffix_fact_types,
        active_suffix_rules=active_suffix_rules,
        fallback_profile=fallback_profile,
        reconciliation_producer=reconciliation_producer,
        ownership_fallback_producer=fallback_producer,
        producer_dependency=dependency,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=hashlib.sha256(canonical).hexdigest(),
        source=str(path),
    )


def load_policy(path: str | Path) -> CompiledPolicy:
    """Load one bounded YAML document and resolve only in-process rule registries."""
    path = Path(path)
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_POLICY_BYTES + 1)
    except OSError as error:
        raise CorrelationPolicyError(f"could not read policy {path}: {error}") from error
    if len(raw) > MAX_POLICY_BYTES:
        raise CorrelationPolicyError(f"policy exceeds {MAX_POLICY_BYTES} bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise CorrelationPolicyError("policy must be UTF-8") from error
    try:
        document = yaml.load(text, Loader=_UniqueSafeLoader)
    except yaml.YAMLError as error:
        raise CorrelationPolicyError(f"invalid YAML policy: {error}") from error

    if type(document) is not dict:
        raise CorrelationPolicyError("policy must be a mapping")
    schema_version = _integer(document.get("schema_version"), "schema_version")
    if schema_version not in SUPPORTED_POLICY_SCHEMA_VERSIONS:
        raise CorrelationPolicyError(f"unsupported policy schema_version {schema_version}")
    if schema_version == 4:
        return _compile_schema_four(document, raw=raw, path=path)
    root_keys = (
        ("schema_version", "policy_id", "policy_version", "implementation", "fixed_point", "stages")
        if schema_version == 1
        else (
            "schema_version",
            "policy_id",
            "policy_version",
            "implementation",
            "fixed_point",
            "fact_producers",
            "facts",
            "stages",
        )
    )
    root = _mapping(document, "policy", root_keys)
    policy_id = _string(root["policy_id"], "policy_id", _ID)
    policy_version = _string(root["policy_version"], "policy_version", _VERSION)

    implementation = _mapping(root["implementation"], "implementation", ("correlation_version",))
    implementation_version = _integer(
        implementation["correlation_version"], "implementation.correlation_version"
    )
    if implementation_version not in {58, CORRELATION_VERSION}:
        raise CorrelationPolicyError(
            "legacy policy implementation correlation_version must be 58 or "
            f"the running whitelist version {CORRELATION_VERSION}"
        )
    fixed_point = _mapping(root["fixed_point"], "fixed_point", ("max_passes",))
    max_passes = _integer(fixed_point["max_passes"], "fixed_point.max_passes", lower=2, upper=10)

    if schema_version == 1:
        stage_keys = ("ownership_intervals", "row_assignment", "terminal_promotion")
    elif schema_version == 2:
        stage_keys = ("evidence_quality", "ownership_intervals", "row_assignment", "terminal_promotion")
    else:
        stage_keys = (
            "evidence_quality",
            "active_suffix_assignment",
            "ownership_intervals",
            "row_assignment",
            "terminal_promotion",
        )
    stages = _mapping(root["stages"], "stages", stage_keys)
    if schema_version == 1:
        evidence_fact_producers = ()
        evidence_fact_types = ()
        evidence_rules = ()
        semantic_evidence_rules = None
        active_suffix_fact_producers = ()
        active_suffix_fact_types = ()
        active_suffix_rules = ()
        semantic_active_suffix_rules = None
    else:
        expected_producers = (
            [EVIDENCE_FACT_PRODUCER]
            if schema_version == 2
            else [EVIDENCE_FACT_PRODUCER, ACTIVE_SUFFIX_FACT_PRODUCER]
        )
        if root["fact_producers"] != expected_producers:
            raise CorrelationPolicyError(f"fact_producers must be {expected_producers!r}")
        expected_facts = set(EVIDENCE_FACT_TYPES)
        if schema_version == 3:
            expected_facts.update(ACTIVE_SUFFIX_FACT_TYPES)
        if type(root["facts"]) is not dict or set(root["facts"]) != expected_facts:
            raise CorrelationPolicyError("facts must declare exactly the supported policy facts")
        (
            evidence_fact_producers,
            evidence_fact_types,
            evidence_rules,
            semantic_evidence_rules,
        ) = _compile_evidence_policy(root, stages, exact_facts=schema_version == 2)
        if schema_version == 3:
            (
                active_suffix_fact_producers,
                active_suffix_fact_types,
                active_suffix_rules,
                semantic_active_suffix_rules,
            ) = _compile_active_suffix_policy(root, stages)
        else:
            active_suffix_fact_producers = ()
            active_suffix_fact_types = ()
            active_suffix_rules = ()
            semantic_active_suffix_rules = None
    ownership = _mapping(stages["ownership_intervals"], "stages.ownership_intervals", ("type", "rules"))
    if ownership["type"] != "sequential":
        raise CorrelationPolicyError("stages.ownership_intervals.type must be 'sequential'")
    ownership_names = _rule_names(ownership["rules"], "stages.ownership_intervals.rules")
    ownership_registry = {rule.name: rule for rule in OWNERSHIP_INTERVAL_RULES}
    ownership_rules = _resolve_rules(ownership_names, ownership_registry, "stages.ownership_intervals.rules")

    row = _mapping(stages["row_assignment"], "stages.row_assignment", ("type", "rules", "default"))
    if row["type"] != "first_match":
        raise CorrelationPolicyError("stages.row_assignment.type must be 'first_match'")
    if row["default"] != MANDATORY_ROW_DEFAULT:
        raise CorrelationPolicyError(f"stages.row_assignment.default must be {MANDATORY_ROW_DEFAULT!r}")
    row_names = _rule_names(row["rules"], "stages.row_assignment.rules")
    if MANDATORY_ROW_DEFAULT in row_names:
        raise CorrelationPolicyError("mandatory row default must not also appear in row rules")
    row_registry = {rule.name: rule for rule in ROW_CORRELATION_RULES}
    row_rules = _resolve_rules(row_names, row_registry, "stages.row_assignment.rules")
    row_rules += (row_registry[MANDATORY_ROW_DEFAULT],)

    terminal = _mapping(stages["terminal_promotion"], "stages.terminal_promotion", ("type", "rule"))
    if terminal["type"] != "whole_series":
        raise CorrelationPolicyError("stages.terminal_promotion.type must be 'whole_series'")
    if terminal["rule"] != TERMINAL_FALLBACK_RULE_NAME:
        raise CorrelationPolicyError(f"stages.terminal_promotion.rule must be {TERMINAL_FALLBACK_RULE_NAME!r}")
    if implementation_version == 58:
        if ownership_names != [rule.name for rule in OWNERSHIP_INTERVAL_RULES[:3]]:
            raise CorrelationPolicyError("versioned v58 execution requires the frozen three ownership rules")
        if row_names != [rule.name for rule in ROW_CORRELATION_RULES[:-1]]:
            raise CorrelationPolicyError("versioned v58 execution requires the frozen row-rule cascade")

    legacy_policy_role = "historical_reference" if implementation_version == 58 else "legacy_candidate"
    legacy_execution_profile = (
        "correlation_v58_single_phase"
        if implementation_version == 58
        else "correlation_v59_single_phase_candidate"
    )
    semantic = {
        "schema_version": schema_version,
        "policy_id": policy_id,
        "policy_version": policy_version,
        "policy_role": legacy_policy_role,
        "execution_profile": legacy_execution_profile,
        "implementation": {"correlation_version": implementation_version},
        "fixed_point": {"max_passes": max_passes},
        "stages": {
            "ownership_intervals": {"type": "sequential", "rules": ownership_names},
            "row_assignment": {
                "type": "first_match",
                "rules": row_names,
                "default": MANDATORY_ROW_DEFAULT,
            },
            "terminal_promotion": {
                "type": "whole_series",
                "rule": TERMINAL_FALLBACK_RULE_NAME,
            },
        },
    }
    if schema_version >= 2:
        semantic["fact_producers"] = [
            *evidence_fact_producers,
            *active_suffix_fact_producers,
        ]
        semantic["facts"] = {
            name: {"type": kind} for name, kind in (*evidence_fact_types, *active_suffix_fact_types)
        }
        semantic["stages"] = {
            "evidence_quality": {
                "type": "per_candidate_action",
                "rules": semantic_evidence_rules,
            },
            **semantic["stages"],
        }
    if schema_version == 3:
        semantic["stages"] = {
            "evidence_quality": semantic["stages"]["evidence_quality"],
            "active_suffix_assignment": {
                "type": "per_symbol_certificate",
                "position": "immediately_before_unresolved_fallback",
                "rules": semantic_active_suffix_rules,
            },
            **{name: value for name, value in semantic["stages"].items() if name != "evidence_quality"},
        }
    canonical = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
    return CompiledPolicy(
        schema_version=schema_version,
        policy_id=policy_id,
        policy_version=policy_version,
        policy_role=legacy_policy_role,
        execution_profile=legacy_execution_profile,
        implementation_correlation_version=implementation_version,
        implementation_lineage_version=None,
        max_passes=max_passes,
        established_ownership_rules=ownership_rules,
        ownership_rules=ownership_rules,
        row_rules=row_rules,
        terminal_rule=TERMINAL_FALLBACK_RULE_NAME,
        evidence_fact_producers=evidence_fact_producers,
        evidence_fact_types=evidence_fact_types,
        evidence_rules=evidence_rules,
        active_suffix_fact_producers=active_suffix_fact_producers,
        active_suffix_fact_types=active_suffix_fact_types,
        active_suffix_rules=active_suffix_rules,
        fallback_profile=None,
        reconciliation_producer=None,
        ownership_fallback_producer=None,
        producer_dependency=None,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        semantic_sha256=hashlib.sha256(canonical).hexdigest(),
        source=str(path),
    )


def _production_executable_profile(policy: CompiledPolicy) -> bool:
    """Bind authorization to the immutable whitelisted behavior, without I/O."""
    return (
        policy.max_passes == 3
        and policy.established_ownership_rules == OWNERSHIP_INTERVAL_RULES[:3]
        and policy.ownership_rules == OWNERSHIP_INTERVAL_RULES
        and policy.row_rules == ROW_CORRELATION_RULES
        and policy.terminal_rule == TERMINAL_FALLBACK_RULE_NAME
        and policy.evidence_fact_producers == ()
        and policy.evidence_fact_types == ()
        and policy.evidence_rules == ()
        and policy.active_suffix_fact_producers == ()
        and policy.active_suffix_fact_types == ()
        and policy.active_suffix_rules == ()
        and policy.fallback_profile == PUBLISHED_PROFILE
        and policy.reconciliation_producer == PUBLISHED_RECONCILIATION
        and policy.ownership_fallback_producer == PUBLISHED_SUFFIX
        and policy.producer_dependency == LEGACY_DEPENDENCY
    )


def authorize_production_policy(policy: CompiledPolicy) -> CompiledPolicy:
    if (
        not isinstance(policy, CompiledPolicy)
        or not reusable_policy(policy.provenance())
        or not _production_executable_profile(policy)
    ):
        raise CorrelationPolicyError("canonical production policy identity is not authorized")
    # The canonical path is resolved once when this module is imported. Keep
    # subsequent acceptance pure: callers assert the cached immutable object
    # for every group without repeating filesystem metadata I/O.
    if policy.source != str(CANONICAL_POLICY_PATH):
        raise CorrelationPolicyError("production policy must use the canonical repository resource")
    return policy


def load_authorized_policy() -> CompiledPolicy:
    """Read and authorize the sole live policy before any store or network work."""
    validate_correlation_change_log()
    return authorize_production_policy(load_policy(CANONICAL_POLICY_PATH))

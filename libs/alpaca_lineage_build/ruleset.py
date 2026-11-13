"""Accept the canonical YAML's generic policy and producer selection."""

from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.component_refinement import NAME as REFINEMENT_NAME
from libs.alpaca_end_of_day_security_correlation.correlation_version import validate_correlation_change_log

from . import correlation_policy as compiler
from .contract import CorrelationPolicyError, document_digest
from .correlation_policy import (
    CANONICAL_POLICY_PATH,
    compile_policy_document,
    policy_document,
    read_policy_document,
)
from .ruleset_contract import AcceptedRuleset


def load_ruleset(path=CANONICAL_POLICY_PATH, *, authorize=False) -> AcceptedRuleset:
    """Accept each input once; consumers carry this record rather than reload it.

    Policy semantics and producer selection have independent identities. The
    container format never revokes an applicable completed lineage decision.
    """
    if authorize:
        validate_correlation_change_log()
    raw, document = read_policy_document(path)
    policy = compile_policy_document(policy_document(document), raw=raw, path=Path(path))
    refinement, execution_sha256 = accept_components(document)
    if authorize:
        compiler.authorize_production_policy(policy)
    return AcceptedRuleset(
        policy=policy,
        membership_refinement=refinement,
        execution_sha256=execution_sha256,
    )


def accept_components(document):
    """Accept parsed component inputs once for production or diagnostic compilation."""
    policy_document(document)
    if "ruleset_schema_version" in document:
        producers = document["producers"]
        if type(producers) is not dict or set(producers) != {"membership_refinement"}:
            raise CorrelationPolicyError("ruleset producers require explicit membership_refinement")
        refinement = producers["membership_refinement"]
        if refinement is not None and refinement != REFINEMENT_NAME:
            raise CorrelationPolicyError("unknown ruleset membership refinement producer")
    else:
        refinement = None
    return (
        refinement,
        document_digest({"membership_refinement": refinement}),
    )

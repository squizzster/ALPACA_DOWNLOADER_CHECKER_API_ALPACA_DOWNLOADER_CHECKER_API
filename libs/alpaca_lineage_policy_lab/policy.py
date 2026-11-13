"""Read-only lab adapter for the production-owned policy compiler."""

from libs.alpaca_lineage_build.correlation_policy import *  # noqa: F403
from libs.alpaca_lineage_build.correlation_policy import load_policy as _load_policy
from libs.alpaca_lineage_build.correlation_policy import reject_injected_evidence
from libs.alpaca_security_lineage.inference_evidence import reject_assertions

from .contract import PolicyLabError, validate_change_log


def require_generic_candidate(policy):
    """Reject direct factual candidates before selecting any retained input."""
    if hasattr(policy, "documented_facts") or getattr(policy, "control_mode", "replay") != "replay":
        raise PolicyLabError("fact-backed research policies are not supported")
    reject_injected_evidence(vars(policy))
    reject_assertions(vars(policy), where="lab candidate policy")
    bundle = getattr(policy, "ruleset", None)
    if bundle is not None and (getattr(bundle, "applications", ()) or getattr(bundle, "supplements", ())):
        raise PolicyLabError("configured fact applications are not supported")


def load_policy(path):
    # Compilation supplies executable policy meaning, not production authority.
    # The lab may compare a candidate; only the production owner's authorization
    # path can select policy for new stored lineage publications.
    validate_change_log()
    return _load_policy(path)

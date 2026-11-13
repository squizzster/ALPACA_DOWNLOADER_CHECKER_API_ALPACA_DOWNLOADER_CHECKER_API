"""Retired researched continuity recipe; completed publications remain separate."""

from libs.alpaca_security_lineage.inference_evidence import reject_assertions

from .contract import LineageError


def bind(controls, evidence, cache):
    """Reject authored recipes instead of extending legacy public membership."""
    reject_assertions(evidence, where="documented continuity")
    for control in controls.values():
        prior = control.previous or {}
        reject_assertions(prior.get("evidence", {}), where="retained documented continuity recipe")
        if (prior.get("public_common_continuation_membership") or {}).get("certificates"):
            raise LineageError("researched continuity recipe is inadmissible for new inference")
    return {"version": 1, "certificates": [], "issues": []}, None

"""Producer-owned promotion of narrowly proved, unchanged rule decisions."""

from .contract import versions
from .occupancy import may_have_action_anchored_current_suffix, may_have_duplicate_foreign_occupancy


def _correlation_59_decision_may_change(receipt):
    """Recognize receipts touched by the two correlation-59 rules."""
    asset_ids_value = receipt.get("asset_ids")
    if not isinstance(asset_ids_value, list) or any(
        not isinstance(asset_id, str) or not asset_id for asset_id in asset_ids_value
    ):
        return True
    asset_ids = set(asset_ids_value)
    if len(asset_ids) != len(asset_ids_value):
        return True
    if receipt.get("unresolved_rows") == 0:
        return False
    evidence = receipt.get("evidence")
    if not isinstance(evidence, dict):
        return bool(asset_ids) or receipt.get("unresolved_rows") != 0

    if may_have_action_anchored_current_suffix(evidence):
        return True
    symbol = receipt.get("symbol")
    return isinstance(symbol, str) and may_have_duplicate_foreign_occupancy(evidence, symbol)


def can_promote_observation_decision(receipt):
    """Prove only the completed 31-or-32/58 transition to 33/59.

    The caller already owns a coherent selected group with unchanged inputs.
    This function performs no I/O and never makes an old handoff reader-compatible;
    the producer must publish a current receipt through its existing journal.
    """
    current = versions()
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # These are exact migration proofs, not a rolling compatibility exemption.
    if (current["lineage"], current["correlation"]) != (33, 59):
        return False
    published = receipt.get("versions")
    prior = any(
        receipt.get("schema_version") == lineage_version
        and published
        == {
            **current,
            "lineage": lineage_version,
            "correlation": 58,
        }
        for lineage_version in (31, 32)
    )
    if not prior:
        return False

    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Inspect only this selected receipt's already loaded metadata, once during
    # migration. A possibly changed decision sends the COMPLETE group through
    # correlation; an unchanged decision needs no pricing decode, history scan or
    # new API work. Subsequent runs consume the current receipt and skip this proof.
    return not _correlation_59_decision_may_change(receipt)

"""Retired authored holding-company succession certification."""

from libs.alpaca_security_lineage.inference_evidence import reject_assertions


def _related(value, identifiers):
    if isinstance(value, dict):
        return any(_related(item, identifiers) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_related(item, identifiers) for item in value)
    return isinstance(value, str) and value in identifiers


def certify_sourced_successions(lineage):
    """Reject authored assertions; accepted old outputs remain inert provenance."""
    reject_assertions(lineage, where="holding-company succession certification")
    return (), ()

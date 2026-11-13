"""Completed configuration acceptance, safe to restore without executing a loader.

This record carries producer results across stage JSON. Construction deliberately
does not parse YAML, compile rules or inspect retained storage. Only registered
generic policy and producer selection cross this boundary.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AcceptedRuleset:
    policy: Any
    membership_refinement: str | None
    execution_sha256: str

    @property
    def applications(self):
        """Empty compatibility view; authored applications are never accepted."""
        return ()

    @property
    def supplements(self):
        """Empty compatibility view, excluded from serialized handoffs."""
        return ()

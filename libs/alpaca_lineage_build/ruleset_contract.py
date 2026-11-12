"""Completed configuration acceptance, safe to restore without executing a loader.

This record carries producer results across stage JSON. Construction deliberately
does not parse YAML, compile rules, validate facts or inspect retained storage.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AcceptedRuleset:
    policy: Any
    applications: tuple
    supplements: Any
    membership_refinement: str | None
    execution_sha256: str

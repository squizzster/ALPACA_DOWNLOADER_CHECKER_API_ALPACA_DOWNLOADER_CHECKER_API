"""Lightweight serialized identity for the mandatory production policy.

Trusted receipt/index readers and frozen-journal recovery import this module,
not the YAML compiler or evaluator. It validates compact provenance without
reading policy bytes and intentionally depends only on the standard library.
"""

import re
from typing import Any

SUPPORTED_POLICY_SCHEMA_VERSIONS = frozenset((1, 2, 3, 4))
PRODUCTION_POLICY_SCHEMA_VERSION = 4
PRODUCTION_POLICY_ID = "alpaca_security_correlation"
PRODUCTION_POLICY_VERSION = "1.0.0"
PRODUCTION_POLICY_ROLE = "production"
PRODUCTION_EXECUTION_PROFILE = "correlation_v60_policy_engine"
PRODUCTION_CORRELATION_VERSION = 60
PRODUCTION_LINEAGE_VERSION = 34
POLICY_PROVENANCE_LINEAGE_VERSION = 34
PRODUCTION_POLICY_SEMANTIC_SHA256 = "ebe0d4a6c14f32ee8e9e23ab842d4f01f1ef80ea264230cd87260c0187d4f307"

_ID = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def valid_sha256(value: Any) -> bool:
    """Return whether *value* is a canonical serialized SHA-256."""
    return isinstance(value, str) and _SHA256.fullmatch(value) is not None


def expected_policy_identity() -> dict[str, Any]:
    return {
        "schema_version": PRODUCTION_POLICY_SCHEMA_VERSION,
        "policy_id": PRODUCTION_POLICY_ID,
        "policy_version": PRODUCTION_POLICY_VERSION,
        "policy_role": PRODUCTION_POLICY_ROLE,
        "execution_profile": PRODUCTION_EXECUTION_PROFILE,
        "implementation": {
            "correlation_version": PRODUCTION_CORRELATION_VERSION,
            "lineage_version": PRODUCTION_LINEAGE_VERSION,
        },
        "semantic_sha256": PRODUCTION_POLICY_SEMANTIC_SHA256,
    }


def valid_policy_provenance(provenance: Any) -> bool:
    """Validate frozen policy provenance without loading today's YAML."""
    if not isinstance(provenance, dict) or set(provenance) != {
        "schema_version",
        "policy_id",
        "policy_version",
        "policy_role",
        "execution_profile",
        "implementation",
        "semantic_sha256",
        "file_sha256",
    }:
        return False
    implementation = provenance.get("implementation")
    return (
        type(provenance.get("schema_version")) is int
        and provenance["schema_version"] in SUPPORTED_POLICY_SCHEMA_VERSIONS
        and isinstance(provenance.get("policy_id"), str)
        and _ID.fullmatch(provenance["policy_id"]) is not None
        and isinstance(provenance.get("policy_version"), str)
        and _VERSION.fullmatch(provenance["policy_version"]) is not None
        and isinstance(provenance.get("policy_role"), str)
        and _ID.fullmatch(provenance["policy_role"]) is not None
        and isinstance(provenance.get("execution_profile"), str)
        and _ID.fullmatch(provenance["execution_profile"]) is not None
        and isinstance(implementation, dict)
        and set(implementation) == {"correlation_version", "lineage_version"}
        and type(implementation.get("correlation_version")) is int
        and implementation["correlation_version"] > 0
        and type(implementation.get("lineage_version")) is int
        and implementation["lineage_version"] > 0
        and valid_sha256(provenance.get("semantic_sha256"))
        and valid_sha256(provenance.get("file_sha256"))
    )


def reusable_policy(provenance: Any) -> bool:
    """Use semantic identity for compatibility; file bytes are provenance only."""
    return valid_policy_provenance(provenance) and all(
        provenance.get(key) == value for key, value in expected_policy_identity().items()
    )

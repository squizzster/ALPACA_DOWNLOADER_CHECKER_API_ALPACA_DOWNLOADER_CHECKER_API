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
PRODUCTION_EXECUTION_PROFILE = "correlation_v91_policy_engine"
PRODUCTION_CORRELATION_VERSION = 91
PRODUCTION_LINEAGE_VERSION = 37
POLICY_PROVENANCE_LINEAGE_VERSION = 34
PRODUCTION_POLICY_SEMANTIC_SHA256 = "cd121b16a5bef02cb62ea60c6e12e8614f5d6289981c4289b8499b078b021401"
RETAINED_V90_SEMANTIC_SHA256 = "282c29e5684433d3ecc50d253c4a2946a70c7a5a3bccd439a2d313034f708fa6"
RETAINED_V89_SEMANTIC_SHA256 = "417e96f3229302e60af32b18e3da4ebe277f4b4a447c0c0147f7051102792b1a"
RETAINED_V88_SEMANTIC_SHA256 = "55d2d011073c634924ee53bd139c1f48482a9eaa8de2016cb83d4527c21852c5"
RETAINED_V87_SEMANTIC_SHA256 = "5a468e0dd0d2bf57837abd8bd27b054f4927807b4e44a62db52ff58e5bc2d047"
RETAINED_V86_SEMANTIC_SHA256 = "8ce3e4d331049213f31cd268a6a00420b1434a0fb8ba6997590c032325071dbf"
RETAINED_V85_SEMANTIC_SHA256 = "0f402f18c6e37eeb0f9354150ec5914ea6eb19c1ef4aeb464c93d0265173c9b2"
RETAINED_V84_SEMANTIC_SHA256 = "6a704e19e0ffcbb8b3998fe211023a6b1e3a7559e8481580cda3f462873c11a9"
RETAINED_V83_SEMANTIC_SHA256 = "3dc7e55965ae377d406db4916ae10d4563fd31a5cda42c4cb6b15b3d6fdadc83"
RETAINED_V82_SEMANTIC_SHA256 = "ebe0d4a6c14f32ee8e9e23ab842d4f01f1ef80ea264230cd87260c0187d4f307"

RETAINED_V76_SEMANTIC_SHA256 = "ebe0d4a6c14f32ee8e9e23ab842d4f01f1ef80ea264230cd87260c0187d4f307"
RETAINED_V78_SEMANTIC_SHA256 = "ebe0d4a6c14f32ee8e9e23ab842d4f01f1ef80ea264230cd87260c0187d4f307"
RETAINED_V79_SEMANTIC_SHA256 = "ebe0d4a6c14f32ee8e9e23ab842d4f01f1ef80ea264230cd87260c0187d4f307"

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


def retained_v78_policy(provenance: Any) -> bool:
    """Recognize completed v78 provenance without authorizing new execution.

    The v79 rule is gated solely by the new sourced-successions extension,
    which the v78 producer could not publish. Completed v78 decisions therefore
    retain their meaning and provenance until their actual inputs change. This
    exception must not follow future production versions automatically.
    """
    frozen = {
        "schema_version": 4,
        "policy_id": "alpaca_security_correlation",
        "policy_version": "1.0.0",
        "policy_role": "production",
        "execution_profile": "correlation_v78_policy_engine",
        "implementation": {"correlation_version": 78, "lineage_version": 34},
        "semantic_sha256": RETAINED_V78_SEMANTIC_SHA256,
    }
    return (
        PRODUCTION_CORRELATION_VERSION == 79
        and PRODUCTION_LINEAGE_VERSION == 34
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v79_policy(provenance: Any) -> bool:
    """Recognize the exact prior policy, without authorizing its execution.

    Decision compatibility additionally requires compact producer evidence that
    the new closed-departure exception cannot apply. This policy identity alone
    never approves reuse or promotion of an affected unresolved decision.
    """
    frozen = {
        "schema_version": 4,
        "policy_id": "alpaca_security_correlation",
        "policy_version": "1.0.0",
        "policy_role": "production",
        "execution_profile": "correlation_v79_policy_engine",
        "implementation": {"correlation_version": 79, "lineage_version": 34},
        "semantic_sha256": RETAINED_V79_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {
            (80, 35),
            (81, 36),
            (82, 37),
            (83, 37),
            (84, 37),
            (85, 37),
            (86, 37),
            (87, 37),
            (88, 37),
            (89, 37),
            (90, 37),
            (91, 37),
        }
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v80_policy(provenance):
    """Only the exact completed prior producer, never new policy execution."""
    frozen = {
        "schema_version": 4,
        "policy_id": "alpaca_security_correlation",
        "policy_version": "1.0.0",
        "policy_role": "production",
        "execution_profile": "correlation_v80_policy_engine",
        "implementation": {"correlation_version": 80, "lineage_version": 35},
        "semantic_sha256": RETAINED_V82_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {
            (81, 36),
            (82, 37),
            (83, 37),
            (84, 37),
            (85, 37),
            (86, 37),
            (87, 37),
            (88, 37),
            (89, 37),
            (90, 37),
            (91, 37),
        }
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v81_policy(provenance):
    """New sourced kinds cannot alter old inputs; retain exact frozen provenance."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v81_policy_engine",
        "implementation": {"correlation_version": 81, "lineage_version": 36},
        "semantic_sha256": RETAINED_V82_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {(82, 37), (83, 37), (84, 37), (85, 37), (86, 37), (87, 37), (88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v82_policy(provenance):
    """The completed v82 producer cannot contain the new dated-market stage.

    Named sourced applications select their affected groups through metadata;
    unrelated completed decisions retain their exact original provenance.
    """
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v82_policy_engine",
        "implementation": {"correlation_version": 82, "lineage_version": 37},
        "semantic_sha256": RETAINED_V82_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {(83, 37), (84, 37), (85, 37), (86, 37), (87, 37), (88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v83_policy(provenance):
    """Retain exact completed v83 decisions until new adjustment facts select work."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v83_policy_engine",
        "implementation": {"correlation_version": 83, "lineage_version": 37},
        "semantic_sha256": RETAINED_V83_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {(84, 37), (85, 37), (86, 37), (87, 37), (88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v84_policy(provenance):
    """New parent facts cannot occur in completed v84 producer inputs."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v84_policy_engine",
        "implementation": {"correlation_version": 84, "lineage_version": 37},
        "semantic_sha256": RETAINED_V84_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {(85, 37), (86, 37), (87, 37), (88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v85_policy(provenance):
    """Preserve completed decisions outside newly interpreted recapitalizations."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v85_policy_engine",
        "implementation": {"correlation_version": 85, "lineage_version": 37},
        "semantic_sha256": RETAINED_V85_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {(86, 37), (87, 37), (88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v86_policy(provenance):
    """New documented first-owner facts cannot occur in completed v86 inputs."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v86_policy_engine",
        "implementation": {"correlation_version": 86, "lineage_version": 37},
        "semantic_sha256": RETAINED_V86_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION)
        in {(87, 37), (88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v87_policy(provenance):
    """Completed v87 bindings had no unresolved containing-component choice."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v87_policy_engine",
        "implementation": {"correlation_version": 87, "lineage_version": 37},
        "semantic_sha256": RETAINED_V87_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION) in {(88, 37), (89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v88_policy(provenance):
    """Completed v88 inputs contain no public-reclassification assertion."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v88_policy_engine",
        "implementation": {"correlation_version": 88, "lineage_version": 37},
        "semantic_sha256": RETAINED_V88_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION) in {(89, 37), (90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v89_policy(provenance):
    """Completed v89 inputs contain no holding-exchange refinement assertion."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v89_policy_engine",
        "implementation": {"correlation_version": 89, "lineage_version": 37},
        "semantic_sha256": RETAINED_V89_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION) in {(90, 37), (91, 37)}
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )


def retained_v90_policy(provenance):
    """Completed v90 publications predate composed public-continuity refinement."""
    frozen = {
        **expected_policy_identity(),
        "execution_profile": "correlation_v90_policy_engine",
        "implementation": {"correlation_version": 90, "lineage_version": 37},
        "semantic_sha256": RETAINED_V90_SEMANTIC_SHA256,
    }
    return (
        (PRODUCTION_CORRELATION_VERSION, PRODUCTION_LINEAGE_VERSION) == (91, 37)
        and valid_policy_provenance(provenance)
        and all(provenance.get(key) == value for key, value in frozen.items())
    )

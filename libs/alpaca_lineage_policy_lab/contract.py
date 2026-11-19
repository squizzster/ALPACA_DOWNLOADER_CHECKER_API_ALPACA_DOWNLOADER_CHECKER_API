"""Versioned contracts for the read-only correlation policy laboratory."""

import json
from functools import lru_cache
from pathlib import Path

from libs.alpaca_lineage_build import correlation_policy_contract as policies
from libs.alpaca_lineage_build.contract import CorrelationPolicyError as PolicyLabError

LAB_VERSION = 27
LAB_SCHEMA_VERSION = 8
SUPPORTED_POLICY_SCHEMA_VERSIONS = frozenset((1, 2, 3, 4))
LAB_REPORT_BOUNDARY = "alpaca_lineage_policy_lab_comparison"
LAB_AUDIT_BOUNDARY = "alpaca_lineage_policy_lab_audit"
LAB_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[2] / "resources" / "alpaca_lineage_policy_lab_change_log.json"
)

FROZEN_BASELINE_POLICIES = {
    (33, 59): None,  # Published before policy provenance.
    (34, 76): policies.RETAINED_V76_SEMANTIC_SHA256,
    (34, 77): policies.RETAINED_V76_SEMANTIC_SHA256,
    (34, 79): policies.RETAINED_V79_SEMANTIC_SHA256,
    (37, 82): policies.RETAINED_V82_SEMANTIC_SHA256,
    (37, 83): policies.RETAINED_V83_SEMANTIC_SHA256,
    (37, 84): policies.RETAINED_V84_SEMANTIC_SHA256,
    (37, 85): policies.RETAINED_V85_SEMANTIC_SHA256,
    (37, 86): policies.RETAINED_V86_SEMANTIC_SHA256,
    (37, 87): policies.RETAINED_V87_SEMANTIC_SHA256,
    (37, 88): policies.RETAINED_V88_SEMANTIC_SHA256,
    (37, 89): policies.RETAINED_V89_SEMANTIC_SHA256,
    (37, 90): policies.RETAINED_V90_SEMANTIC_SHA256,
    (37, 91): policies.RETAINED_V91_SEMANTIC_SHA256,
    (37, 92): policies.RETAINED_V92_SEMANTIC_SHA256,
    (37, 93): policies.RETAINED_V93_SEMANTIC_SHA256,
}


def replayable_lineage_versions(published):
    # Replay readability is deliberately separate from production reuse approval.
    # Accepting the historical baseline here does not migrate/relabel its receipts
    # or permit a candidate policy to publish over them.
    from libs.alpaca_lineage_build.contract import versions

    current = versions()
    if not isinstance(published, dict) or any(
        type(published.get(key)) is not int for key in ("lineage", "correlation", "identity_report")
    ):
        return False
    if published == current:
        return True
    pair = published["lineage"], published["correlation"]
    if current["identity_report"] != 4 or pair not in FROZEN_BASELINE_POLICIES:
        return False
    expected = {**current, "lineage": pair[0], "correlation": pair[1]}
    digest = FROZEN_BASELINE_POLICIES[pair]
    if digest is None:
        del expected["correlation_policy_sha256"]
    else:
        expected["correlation_policy_sha256"] = digest
    return published == expected


@lru_cache(maxsize=1)
def validate_change_log() -> list[dict]:
    try:
        entries = json.loads(LAB_CHANGE_LOG_PATH.read_bytes())
    except (OSError, json.JSONDecodeError) as error:
        raise PolicyLabError(f"could not load {LAB_CHANGE_LOG_PATH}: {error}") from error
    if not isinstance(entries, list) or len(entries) != LAB_VERSION:
        raise PolicyLabError(f"policy lab changelog must contain exactly {LAB_VERSION} entries")
    for implicit_version, entry in enumerate(entries, start=1):
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(entry.get("modifications"), list)
            or not entry["modifications"]
            or any(not isinstance(item, str) or not item for item in entry["modifications"])
        ):
            raise PolicyLabError(f"invalid policy lab changelog entry {implicit_version}")
    return entries

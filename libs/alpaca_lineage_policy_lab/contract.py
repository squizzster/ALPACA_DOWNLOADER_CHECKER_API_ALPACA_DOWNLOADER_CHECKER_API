"""Versioned contracts for the read-only correlation policy laboratory."""

import json
from functools import lru_cache
from pathlib import Path

from libs.alpaca_lineage_build.contract import CorrelationPolicyError as PolicyLabError

LAB_VERSION = 9
LAB_SCHEMA_VERSION = 8
SUPPORTED_POLICY_SCHEMA_VERSIONS = frozenset((1, 2, 3, 4))
LAB_REPORT_BOUNDARY = "alpaca_lineage_policy_lab_comparison"
LAB_AUDIT_BOUNDARY = "alpaca_lineage_policy_lab_audit"
LAB_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[2] / "resources" / "alpaca_lineage_policy_lab_change_log.json"
)


def replayable_lineage_versions(published):
    # Replay readability is deliberately separate from production reuse approval.
    # Accepting the historical baseline here does not migrate/relabel its receipts
    # or permit a candidate policy to publish over them.
    from libs.alpaca_lineage_build.contract import versions

    current = versions()
    legacy = {**current, "lineage": 33, "correlation": 59}
    legacy.pop("correlation_policy_sha256", None)
    return published in (current, legacy)


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

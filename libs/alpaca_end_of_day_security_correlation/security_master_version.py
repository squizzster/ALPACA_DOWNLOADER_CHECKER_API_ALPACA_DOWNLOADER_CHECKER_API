# SECURITY MASTER VERSION RULE: EVERY CHANGE MUST BUMP THE VERSION AND APPEND ONE CHANGELOG ENTRY.
import json
from functools import lru_cache
from pathlib import Path

from .errors import SecurityCorrelationError

SECURITY_MASTER_VERSION = 5
SECURITY_MASTER_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[2] / "resources" / "alpaca_security_master_change_log.json"
)


@lru_cache(maxsize=1)
def validate_security_master_change_log() -> list[dict]:
    try:
        entries = json.loads(SECURITY_MASTER_CHANGE_LOG_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(f"could not load {SECURITY_MASTER_CHANGE_LOG_PATH}: {error}") from error
    if not isinstance(entries, list) or len(entries) != SECURITY_MASTER_VERSION:
        found = len(entries) if isinstance(entries, list) else "non-array"
        raise SecurityCorrelationError(
            f"security-master changelog must contain exactly {SECURITY_MASTER_VERSION} entries; found {found}"
        )
    for implicit_version, entry in enumerate(entries, start=1):
        modifications = entry.get("modifications") if isinstance(entry, dict) else None
        if isinstance(entry, dict) and "version" in entry:
            raise SecurityCorrelationError(
                f"security-master changelog entry {implicit_version} must not contain a version key"
            )
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise SecurityCorrelationError(
                f"security-master changelog entry {implicit_version} has no modifications"
            )
    return entries


# SECURITY MASTER VERSION RULE: EVERY CHANGE MUST BUMP THE VERSION AND APPEND ONE CHANGELOG ENTRY.

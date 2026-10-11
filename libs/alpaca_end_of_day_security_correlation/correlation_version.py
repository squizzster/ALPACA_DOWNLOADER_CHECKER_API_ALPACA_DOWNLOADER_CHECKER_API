# CORRELATION VERSION RULE: EVERY CORRELATION CHANGE MUST BUMP CORRELATION_VERSION AND APPEND ONE CHANGELOG ENTRY.
import json
from functools import lru_cache
from pathlib import Path

from .errors import SecurityCorrelationError

CORRELATION_VERSION = 20
SUPPORTED_CORRELATION_VERSIONS = frozenset((8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, CORRELATION_VERSION))
CORRELATION_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[2] / "alpaca_end_of_day_security_correlation_change_log.json"
)


@lru_cache(maxsize=1)
def validate_correlation_change_log() -> list[dict]:
    try:
        with CORRELATION_CHANGE_LOG_PATH.open(encoding="utf-8") as change_log_file:
            change_log_entries = json.load(change_log_file)
    except (OSError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(f"could not load {CORRELATION_CHANGE_LOG_PATH}: {error}") from error

    if not isinstance(change_log_entries, list):
        raise SecurityCorrelationError("correlation version changelog must be a JSON array")
    if len(change_log_entries) != CORRELATION_VERSION:
        raise SecurityCorrelationError(
            "correlation changelog must contain exactly "
            f"{CORRELATION_VERSION} entries; found {len(change_log_entries)}"
        )

    for implicit_version, entry in enumerate(change_log_entries, start=1):
        if not isinstance(entry, dict):
            raise SecurityCorrelationError(f"correlation changelog entry {implicit_version} is not an object")
        if "version" in entry:
            raise SecurityCorrelationError(
                f"correlation changelog entry {implicit_version} must not contain a version key"
            )
        modifications = entry.get("modifications")
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise SecurityCorrelationError(
                f"correlation changelog version {implicit_version} has no modifications"
            )
    return change_log_entries


# CORRELATION VERSION RULE: EVERY CORRELATION CHANGE MUST BUMP CORRELATION_VERSION AND APPEND ONE CHANGELOG ENTRY.

# WARNING PIPELINE VERSION RULE: EVERY CHANGE MUST BUMP THE VERSION AND APPEND ONE CHANGELOG ENTRY.
import json
from functools import lru_cache
from pathlib import Path

from .errors import WarningPipelineError

WARNING_PIPELINE_VERSION = 6
WARNING_PIPELINE_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[2] / "alpaca_end_of_day_warning_pipeline_change_log.json"
)


@lru_cache(maxsize=1)
def validate_warning_pipeline_change_log() -> list[dict]:
    try:
        with WARNING_PIPELINE_CHANGE_LOG_PATH.open(encoding="utf-8") as change_log_file:
            entries = json.load(change_log_file)
    except (OSError, json.JSONDecodeError) as error:
        raise WarningPipelineError(f"could not load {WARNING_PIPELINE_CHANGE_LOG_PATH}: {error}") from error
    if not isinstance(entries, list) or len(entries) != WARNING_PIPELINE_VERSION:
        found = len(entries) if isinstance(entries, list) else "non-array"
        raise WarningPipelineError(
            f"warning-pipeline changelog must contain exactly {WARNING_PIPELINE_VERSION} entries; found {found}"
        )
    for implicit_version, entry in enumerate(entries, start=1):
        modifications = entry.get("modifications") if isinstance(entry, dict) else None
        if isinstance(entry, dict) and "version" in entry:
            raise WarningPipelineError(
                f"warning-pipeline changelog entry {implicit_version} must not contain a version key"
            )
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise WarningPipelineError(
                f"warning-pipeline changelog entry {implicit_version} has no modifications"
            )
    return entries


# WARNING PIPELINE VERSION RULE: EVERY CHANGE MUST BUMP THE VERSION AND APPEND ONE CHANGELOG ENTRY.

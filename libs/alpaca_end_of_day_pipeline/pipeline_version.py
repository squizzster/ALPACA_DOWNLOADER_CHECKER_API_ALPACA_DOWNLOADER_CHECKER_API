# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.
import json
from functools import lru_cache
from pathlib import Path

from .errors import EndOfDayPipelineError

# Bump whenever validation, transformation, serialization, or pipeline ownership changes.
PIPELINE_VERSION = 16
PIPELINE_CHANGE_LOG_PATH = Path(__file__).resolve().parents[2] / "alpaca_end_of_day_pipeline_change_log.json"


@lru_cache(maxsize=1)
def validate_pipeline_change_log() -> list[dict]:
    try:
        with PIPELINE_CHANGE_LOG_PATH.open(encoding="utf-8") as change_log_file:
            change_log_entries = json.load(change_log_file)
    except (OSError, json.JSONDecodeError) as change_log_error:
        raise EndOfDayPipelineError(
            f"could not load {PIPELINE_CHANGE_LOG_PATH}: {change_log_error}"
        ) from change_log_error

    if not isinstance(change_log_entries, list):
        raise EndOfDayPipelineError("pipeline version changelog must be a JSON array")

    if len(change_log_entries) != PIPELINE_VERSION:
        raise EndOfDayPipelineError(
            "pipeline changelog must contain exactly "
            f"{PIPELINE_VERSION} entries; found {len(change_log_entries)}"
        )

    for implicit_pipeline_version, change_log_entry in enumerate(change_log_entries, start=1):
        if not isinstance(change_log_entry, dict):
            raise EndOfDayPipelineError(f"pipeline changelog entry {implicit_pipeline_version} is not an object")
        if "version" in change_log_entry:
            raise EndOfDayPipelineError(
                f"pipeline changelog entry {implicit_pipeline_version} must not contain a version key; "
                "its array position is the version"
            )
        modifications = change_log_entry.get("modifications")
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(modification, str) or not modification for modification in modifications)
        ):
            raise EndOfDayPipelineError(
                f"pipeline changelog version {implicit_pipeline_version} has no modifications"
            )
    return change_log_entries


# PIPELINE VERSION RULE: EVERY PIPELINE CHANGE MUST BUMP PIPELINE_VERSION AND APPEND ONE CHANGELOG ENTRY; ARRAY LENGTH MUST EQUAL PIPELINE_VERSION.

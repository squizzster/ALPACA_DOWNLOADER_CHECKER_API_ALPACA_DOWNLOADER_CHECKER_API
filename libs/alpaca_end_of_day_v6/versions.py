"""Independently version the serialized boundaries introduced by v6."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from .errors import V6Error

SOURCE_VERSION = 2
LINEAGE_SOURCE_VERSION = 2
VALIDATION_VERSION = 3
CORRELATION_ENVELOPE_VERSION = 2
WARNING_ENVELOPE_VERSION = 2
IDENTITY_CANDIDATE_VERSION = 2
AVAILABILITY_VERSION = 1
CATALOGUE_PUBLICATION_VERSION = 2
UPDATE_EXECUTION_VERSION = 1

_RESOURCE_ROOT = Path(__file__).resolve().parents[2] / "resources"
_CHANGE_LOGS = {
    "update_execution": (
        UPDATE_EXECUTION_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_update_execution_change_log.json",
    ),
    "source": (SOURCE_VERSION, _RESOURCE_ROOT / "alpaca_v6_source_change_log.json"),
    "lineage_source": (
        LINEAGE_SOURCE_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_lineage_source_change_log.json",
    ),
    "validation": (VALIDATION_VERSION, _RESOURCE_ROOT / "alpaca_v6_validation_change_log.json"),
    "correlation_envelope": (
        CORRELATION_ENVELOPE_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_correlation_envelope_change_log.json",
    ),
    "warning_envelope": (
        WARNING_ENVELOPE_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_warning_envelope_change_log.json",
    ),
    "identity_candidate": (
        IDENTITY_CANDIDATE_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_identity_candidate_change_log.json",
    ),
    "availability": (
        AVAILABILITY_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_availability_change_log.json",
    ),
    "catalogue_publication": (
        CATALOGUE_PUBLICATION_VERSION,
        _RESOURCE_ROOT / "alpaca_v6_catalogue_publication_change_log.json",
    ),
}


@lru_cache(maxsize=None)
def validate_boundary_change_log(boundary: str) -> tuple[dict, ...]:
    try:
        version, path = _CHANGE_LOGS[boundary]
    except KeyError as error:
        raise V6Error(f"unknown v6 boundary: {boundary}") from error
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise V6Error(f"could not load {path}: {error}") from error
    if not isinstance(entries, list) or len(entries) != version:
        raise V6Error(f"{boundary} changelog length must equal its owning version")
    for implicit_version, entry in enumerate(entries, start=1):
        modifications = entry.get("modifications") if isinstance(entry, dict) else None
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise V6Error(f"invalid {boundary} changelog entry {implicit_version}")
    return tuple(entries)

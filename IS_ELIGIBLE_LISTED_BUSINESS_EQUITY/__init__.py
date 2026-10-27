"""Independent, read-only baseline exports for future business-equity assessments."""

import json
from pathlib import Path

VERSION = 1
SCHEMA_VERSION = 1
ROOT = Path(__file__).resolve().parent


def validate_version():
    entries = json.loads((ROOT / "change_log.json").read_text())
    if (
        not isinstance(entries, list)
        or len(entries) != VERSION
        or any(
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(entry.get("modifications"), list)
            or not entry["modifications"]
            or any(not isinstance(item, str) or not item for item in entry["modifications"])
            for entry in entries
        )
    ):
        raise ValueError("business-equity export changelog/version mismatch")

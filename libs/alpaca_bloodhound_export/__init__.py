"""Deterministic, offline Bloodhound-shaped export of standalone Alpaca pricing."""

import json
from pathlib import Path

VERSION = 8


def validate_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_bloodhound_export_change_log.json"
    entries = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(entries, list) or len(entries) != VERSION:
        raise RuntimeError("Bloodhound export changelog differs from its version")
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(entry.get("modifications"), list)
            or not entry["modifications"]
            or any(not isinstance(value, str) or not value for value in entry["modifications"])
        ):
            raise RuntimeError("invalid Bloodhound export changelog entry")

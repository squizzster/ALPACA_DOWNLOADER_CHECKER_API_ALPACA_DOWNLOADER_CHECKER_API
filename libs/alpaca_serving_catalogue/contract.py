"""The serving owner adds eligibility; it never repeats ownership decisions."""

import json
from functools import lru_cache
from pathlib import Path

from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

VERSION = 4
SCHEMA_VERSION = 2


class CatalogueError(RuntimeError):
    pass


def policy():
    return {"version": VERSION, "calendar_sha256": us_market_hours_sha256()}


@lru_cache(maxsize=1)
def validate_version():
    path = Path(__file__).resolve().parents[2] / "resources/alpaca_serving_catalogue_change_log.json"
    entries = json.loads(path.read_bytes())
    if (
        not isinstance(entries, list)
        or len(entries) != VERSION
        or any(
            not isinstance(e, dict)
            or "version" in e
            or not isinstance(e.get("modifications"), list)
            or not e["modifications"]
            or any(not isinstance(m, str) or not m for m in e["modifications"])
            for e in entries
        )
    ):
        raise CatalogueError("serving catalogue changelog/version mismatch")

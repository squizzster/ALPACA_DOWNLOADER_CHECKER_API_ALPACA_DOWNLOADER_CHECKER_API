# READER VERSION RULE: EVERY PUBLIC RESULT OR CLI CONTRACT CHANGE MUST BUMP READER_VERSION AND APPEND ONE CHANGELOG ENTRY.
import json
from functools import lru_cache
from pathlib import Path

from .errors import EndOfDayReaderError

READER_VERSION = 7
READER_CHANGE_LOG_PATH = (
    Path(__file__).resolve().parents[2] / "artifacts" / "alpaca_end_of_day_reader_change_log.json"
)


@lru_cache(maxsize=1)
def validate_reader_change_log() -> list[dict]:
    try:
        entries = json.loads(READER_CHANGE_LOG_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise EndOfDayReaderError(f"could not load {READER_CHANGE_LOG_PATH}: {error}") from error
    if not isinstance(entries, list) or len(entries) != READER_VERSION:
        found = len(entries) if isinstance(entries, list) else "non-array"
        raise EndOfDayReaderError(
            f"reader changelog must contain exactly {READER_VERSION} entries; found {found}"
        )
    for implicit_version, entry in enumerate(entries, start=1):
        modifications = entry.get("modifications") if isinstance(entry, dict) else None
        if isinstance(entry, dict) and "version" in entry:
            raise EndOfDayReaderError(
                f"reader changelog entry {implicit_version} must not contain a version key"
            )
        if (
            not isinstance(modifications, list)
            or not modifications
            or any(not isinstance(item, str) or not item for item in modifications)
        ):
            raise EndOfDayReaderError(f"reader changelog entry {implicit_version} has no modifications")
    return entries


# READER VERSION RULE: EVERY PUBLIC RESULT OR CLI CONTRACT CHANGE MUST BUMP READER_VERSION AND APPEND ONE CHANGELOG ENTRY.

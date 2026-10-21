"""Producer-owned changed spans between selected pricing generations."""

import re
from datetime import date

from .contract import DownloadError, digest, json_bytes

MAX_ANCESTORS = 64


def change_history(previous, update):
    """Carry the existing publication's change decision, without another audit.

    Each entry bounds every change since its ancestor, including removed rows.
    None means identical observations. Full replacement retains its full span.
    The bounded chain also covers several updates before serving maintenance.
    """
    if previous is None:
        return []
    changed = update["from"] if update else None
    result = [{"source_sha256": digest(json_bytes(previous)), "changed_from": changed}]
    for prior in previous.get("history_changes", ())[: MAX_ANCESTORS - 1]:
        starts = [day for day in (changed, prior["changed_from"]) if day is not None]
        result.append({"source_sha256": prior["source_sha256"], "changed_from": min(starts) if starts else None})
    return result


def validate_history_changes(value):
    if not isinstance(value, list) or len(value) > MAX_ANCESTORS:
        raise DownloadError("invalid pricing change history")
    seen = set()
    for entry in value:
        try:
            token, start = entry["source_sha256"], entry["changed_from"]
            if (
                set(entry) != {"source_sha256", "changed_from"}
                or not isinstance(token, str)
                or re.fullmatch(r"[0-9a-f]{64}", token) is None
                or token in seen
                or (start is not None and date.fromisoformat(start).isoformat() != start)
            ):
                raise ValueError
            seen.add(token)
        except (KeyError, TypeError, ValueError) as error:
            raise DownloadError("invalid pricing change ancestor") from error

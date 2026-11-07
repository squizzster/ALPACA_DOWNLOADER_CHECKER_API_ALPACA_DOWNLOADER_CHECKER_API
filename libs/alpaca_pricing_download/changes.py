"""Producer-owned changed spans between selected pricing generations."""

import re
from datetime import date

from .contract import ADJUSTMENTS, DownloadError, csv_bytes, digest, json_bytes, month_of, timestamp

MAX_ANCESTORS = 64


def equal_effective_replacement(previous, anchor, views, documents):
    """Compare new values with accepted logical digests, never old payload bytes.

    The active month overrides the year snapshot. The year may therefore stop
    inside that month after an ordinary append. Its complete recorded bounds
    still prove its nonoverridden prefix when the new rows inside those bounds
    match exactly and no new nonoverridden rows appear outside them.
    """
    if previous is None or previous["anchor_month"] != anchor or set(views) != set(documents):
        return False
    if set(views) != {"completed_years", "current_year", "current_month"}:
        return False
    for kind, document in documents.items():
        entries = {entry["adjustment"]: entry for entry in document["adjustments"]}
        for adjustment in ADJUSTMENTS:
            entry, rows = entries[adjustment], views[kind][adjustment]
            if kind == "current_year":
                if entry["rows"]:
                    first, last = timestamp(entry["first"]), timestamp(entry["last"])
                    selected = tuple(row for row in rows if first <= timestamp(row[0]) <= last)
                    if any(month_of(row) != anchor and not first <= timestamp(row[0]) <= last for row in rows):
                        return False
                else:
                    selected = ()
                    if any(month_of(row) != anchor for row in rows):
                        return False
            else:
                selected = rows
            if len(selected) != entry["rows"] or digest(csv_bytes(selected)) != entry["logical_sha256"]:
                return False
    return True


def change_history(previous, update, *, previous_source_sha256=None):
    """Carry the existing publication's change decision, without another audit.

    Each entry bounds every change since its ancestor, including removed rows.
    None means identical observations. Full replacement retains its full span.
    The bounded chain also covers several updates before serving maintenance.
    """
    if previous is None:
        return []
    # The pricing owner already determined this changed span. Consumers use
    # these ancestor tokens to retain unaffected prefixes; DO NOT rescan bars
    # to reconstruct ancestry. A missing/expired ancestor grants no reuse proof.
    changed = update["from"] if update else None
    generation = previous_source_sha256 or digest(json_bytes(previous))
    result = [{"source_sha256": generation, "changed_from": changed}]
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

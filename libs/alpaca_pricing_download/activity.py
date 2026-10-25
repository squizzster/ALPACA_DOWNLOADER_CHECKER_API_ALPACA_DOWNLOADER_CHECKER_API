"""Relationships guaranteed by one newly acquired eight-view observation bundle."""

from decimal import Decimal

from .contract import ADJUSTMENTS, DownloadError


# DO NOT REMOVE WITHOUT USER AUTH!#
# This guarantee is established when producing a bundle. Downstream readers
# trust the receipt; they must never add the same validation at each boundary.
def validate_activity(views):
    """Validate new producer output, never reopen or authenticate retained input.

    Adjustments change prices and, for splits, share units. They cannot change
    the number of executions. Dividend and spin-off adjustments retain the raw
    share units too. Compare exact decimals: spelling and binary-float rounding
    must not turn equal provider observations into a disagreement.

    Session completeness is a separate owning contract. An absent observation
    is not a zero-volume bar, so only compare observations present in both views.
    """
    raw = {row[0]: row for row in views["raw"]}
    checked = set()
    for adjustment in ADJUSTMENTS[1:]:
        rows = views[adjustment]
        unsplit = adjustment in {"dividend", "spin-off", "dividend,spin-off"}
        key = id(rows), unsplit
        if rows is views["raw"] or key in checked:
            continue
        checked.add(key)
        for row in rows:
            base = raw.get(row[0])
            if base is None:
                continue
            if Decimal(row[6]) != Decimal(base[6]):
                raise DownloadError(f"adjustment views disagree on trade count at {row[0]}")
            if unsplit and Decimal(row[5]) != Decimal(base[5]):
                raise DownloadError(f"adjustment views disagree on raw share volume at {row[0]}")

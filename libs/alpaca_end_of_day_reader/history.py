"""Consume the lineage producer's completed numerical source choice."""

from .errors import EndOfDayReaderError


def require_unique_observation(timestamp: str, count: int) -> None:
    """A malformed selected history needs its owner's bounded recovery.

    Called only while assembling requested data. It neither compares values nor
    recalculates a producer choice from coverage, labels or other adjustments.
    """
    if count != 1:
        raise EndOfDayReaderError(
            f"competing producer-selected observations at {timestamp}; "
            "finish generation-bound lineage recover for the selected complete group"
        )

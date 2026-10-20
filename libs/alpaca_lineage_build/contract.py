"""The lineage twin owns assignments, never another copy of pricing."""

from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.correlation_version import CORRELATION_VERSION
from libs.alpaca_pricing_download.contract import digest, json_bytes
from libs.alpaca_pricing_download.storage import read_document
from libs.alpaca_security_lineage.settings import REPORT_VERSION
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

VERSION = 16
SUPPORTED_VERSIONS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, VERSION)
MAX_CLOSURE = 100
MAX_ACQUISITION_WAVES = 3


class LineageError(RuntimeError):
    pass


class LineageBusyError(LineageError):
    pass


def versions() -> dict:
    return {
        "lineage": VERSION,
        "identity_report": REPORT_VERSION,
        "correlation": CORRELATION_VERSION,
        "calendar_sha256": us_market_hours_sha256(),
    }


def reusable_versions(published: dict) -> bool:
    """Execution and trusted-handoff changes preserve v2-v15 decisions.

    Do not generalize this exception to other execution or rule versions. Older
    readable schemas are not necessarily reusable, and all other version fields
    must still match exactly.
    """
    current = versions()
    return any(
        published == {**current, "lineage": version}
        for version in (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, VERSION)
    )


def reusable_metadata_versions(published: dict) -> bool:
    """A correlation rule change does not invalidate approved upstream evidence."""
    return (
        isinstance(published, dict)
        and type(published.get("correlation")) is int
        and published["correlation"] > 0
        and reusable_versions({**published, "correlation": CORRELATION_VERSION})
    )


def validate_version() -> None:
    import json

    path = Path(__file__).resolve().parents[2] / "resources/alpaca_lineage_build_change_log.json"
    entries = json.loads(path.read_bytes())
    if not isinstance(entries, list) or len(entries) != VERSION:
        raise LineageError("standalone lineage changelog/version mismatch")
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or "version" in entry
            or not isinstance(entry.get("modifications"), list)
            or not entry["modifications"]
            or any(not isinstance(item, str) or not item for item in entry["modifications"])
        ):
            raise LineageError("invalid standalone lineage changelog")


def document_digest(value: dict) -> str:
    return digest(json_bytes(value))


def selected_document(path: Path, expected: str) -> dict:
    # The producer checks outputs at publication. Consumers select its receipt;
    # they do not repeat that audit. expected identifies the selected generation.
    if not isinstance(expected, str) or len(expected) != 64:
        raise LineageError("invalid selected lineage generation")
    return read_document(path, published=True)

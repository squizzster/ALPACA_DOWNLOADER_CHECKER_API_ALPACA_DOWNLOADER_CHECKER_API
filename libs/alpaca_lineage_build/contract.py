"""The lineage twin owns assignments, never another copy of pricing."""

from pathlib import Path

from libs.alpaca_end_of_day_security_correlation.correlation_version import CORRELATION_VERSION
from libs.alpaca_pricing_download.contract import digest, json_bytes
from libs.alpaca_pricing_download.storage import read_document
from libs.alpaca_security_lineage.settings import REPORT_VERSION
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .correlation_policy_contract import (
    POLICY_PROVENANCE_LINEAGE_VERSION,
    PRODUCTION_CORRELATION_VERSION,
    PRODUCTION_LINEAGE_VERSION,
    PRODUCTION_POLICY_SEMANTIC_SHA256,
    reusable_policy,
    valid_sha256,
)
from .correlation_policy_contract import (
    SUPPORTED_POLICY_SCHEMA_VERSIONS as SUPPORTED_POLICY_SCHEMA_VERSIONS,
)

VERSION = 34
SUPPORTED_VERSIONS = tuple(range(1, VERSION + 1))
MAX_CLOSURE = 100
MAX_ACQUISITION_WAVES = 3


class LineageError(RuntimeError):
    pass


class LineageBusyError(LineageError):
    pass


class CorrelationPolicyError(LineageError):
    """The correlation policy is malformed, unsupported, or unauthorized."""


def versions() -> dict:
    if (CORRELATION_VERSION, VERSION) != (
        PRODUCTION_CORRELATION_VERSION,
        PRODUCTION_LINEAGE_VERSION,
    ):
        raise LineageError("lineage and correlation versions do not match the production policy contract")
    return {
        "lineage": VERSION,
        "identity_report": REPORT_VERSION,
        "correlation": CORRELATION_VERSION,
        "correlation_policy_sha256": PRODUCTION_POLICY_SEMANTIC_SHA256,
        "calendar_sha256": us_market_hours_sha256(),
    }


def reusable_versions(published: dict) -> bool:
    """Reuse only a completed observation decision under the current contract."""
    current = versions()
    # DO NOT REMOVE WITHOUT USER AUTH!#
    # Older assignments have no producer-owned numerical source choice. They
    # need one explicit migration; subsequent compatible decisions remain reusable.
    return published == current


def reusable_decision(document: dict) -> bool:
    """Require both the runtime tuple and the authorized policy meaning."""
    return (
        isinstance(document, dict)
        and reusable_versions(document.get("versions"))
        and reusable_policy(document.get("correlation_policy"))
    )


def reusable_metadata_versions(published: dict) -> bool:
    """A correlation rule change does not invalidate approved upstream evidence."""
    legacy_keys = {"lineage", "identity_report", "correlation", "calendar_sha256"}
    policy_bound_keys = legacy_keys | {"correlation_policy_sha256"}
    if not isinstance(published, dict) or type(published.get("lineage")) is not int:
        return False
    expected_keys = (
        legacy_keys if published["lineage"] < POLICY_PROVENANCE_LINEAGE_VERSION else policy_bound_keys
    )
    if set(published) != expected_keys:
        return False
    current = versions()
    return (
        2 <= published["lineage"] <= VERSION
        and type(published.get("correlation")) is int
        and published["correlation"] > 0
        and published.get("identity_report") == current["identity_report"]
        and published.get("calendar_sha256") == current["calendar_sha256"]
        and (
            published["lineage"] < POLICY_PROVENANCE_LINEAGE_VERSION
            or valid_sha256(published["correlation_policy_sha256"])
        )
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

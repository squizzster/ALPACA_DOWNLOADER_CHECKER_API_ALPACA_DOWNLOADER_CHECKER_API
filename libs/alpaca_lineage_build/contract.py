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
    RETAINED_V78_SEMANTIC_SHA256,
    RETAINED_V79_SEMANTIC_SHA256,
    RETAINED_V82_SEMANTIC_SHA256,
    RETAINED_V83_SEMANTIC_SHA256,
    RETAINED_V84_SEMANTIC_SHA256,
    RETAINED_V85_SEMANTIC_SHA256,
    RETAINED_V86_SEMANTIC_SHA256,
    RETAINED_V87_SEMANTIC_SHA256,
    RETAINED_V88_SEMANTIC_SHA256,
    RETAINED_V89_SEMANTIC_SHA256,
    RETAINED_V90_SEMANTIC_SHA256,
    retained_v78_policy,
    retained_v79_policy,
    retained_v80_policy,
    retained_v81_policy,
    retained_v82_policy,
    retained_v83_policy,
    retained_v84_policy,
    retained_v85_policy,
    retained_v86_policy,
    retained_v87_policy,
    retained_v88_policy,
    retained_v89_policy,
    retained_v90_policy,
    reusable_policy,
    valid_sha256,
)
from .correlation_policy_contract import (
    SUPPORTED_POLICY_SCHEMA_VERSIONS as SUPPORTED_POLICY_SCHEMA_VERSIONS,
)

RUNTIME_VERSION = 102
COMPUTATION_REVISION = 2
# A runtime repair is not a new data policy. DO NOT force completed stores through
# reconstruction merely to align runtime and persisted publication version numbers.
VERSION = 37  # Persisted publication and policy contract, independent of runtime releases.
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


def reusable_computation(document: dict) -> bool:
    """An older computation cannot preserve a known defective assignment result.

    This revision selects owning lineage reconstruction from accepted prices and
    metadata. It neither revokes those upstream handoffs nor changes rule policy.
    """
    return (
        isinstance(document, dict)
        and type(document.get("computation_revision")) is int
        and document["computation_revision"] == COMPUTATION_REVISION
    )


def current_decision(document: dict) -> bool:
    """Recognize output computed under the current authorized policy."""
    return (
        isinstance(document, dict)
        and reusable_computation(document)
        and reusable_versions(document.get("versions"))
        and reusable_policy(document.get("correlation_policy"))
    )


def retained_v78_decision(document: dict) -> bool:
    """Recognize the frozen revision-1 compatibility boundary only.

    Receipt/index producers accepted these inputs before sourced successions
    existed. Consumers must not open evidence or prices to prove that absence
    again. Newly submitted facts invalidate the selected group's evidence at its
    existing owning reuse gate, before this completed result can be consumed.
    Revision 2 deliberately retires this reuse path and selects reconstruction.
    """
    return (
        isinstance(document, dict)
        and VERSION == 34
        and REPORT_VERSION == 4
        and COMPUTATION_REVISION == 1
        and isinstance(document.get("versions"), dict)
        and document["versions"].get("correlation") == 78
        and reusable_computation(document)
        and document.get("versions")
        == {
            "lineage": 34,
            "identity_report": 4,
            "correlation": 78,
            "correlation_policy_sha256": RETAINED_V78_SEMANTIC_SHA256,
            "calendar_sha256": us_market_hours_sha256(),
        }
        and retained_v78_policy(document.get("correlation_policy"))
    )


def retained_v79_decision(document: dict) -> bool:
    """Keep provably unaffected completed decisions under their original policy.

    The v80 exception can change only unresolved rows in a source with distinct
    departing and current UUIDs. The accepted receipt/index already supplies both
    facts. Consume that compact handoff; never reopen evidence, assignments or
    prices to prove applicability again. Changed inputs retain their ordinary
    owning reconciliation gates. Missing compact evidence fails closed.
    """
    if not (
        isinstance(document, dict)
        and (VERSION, PRODUCTION_CORRELATION_VERSION)
        in {
            (35, 80),
            (36, 81),
            (37, 82),
            (37, 83),
            (37, 84),
            (37, 85),
            (37, 86),
            (37, 87),
            (37, 88),
            (37, 89),
            (37, 90),
            (37, 91),
        }
        and COMPUTATION_REVISION == 2
        and reusable_computation(document)
        and document.get("versions")
        == {
            "lineage": 34,
            "identity_report": REPORT_VERSION,
            "correlation": 79,
            "correlation_policy_sha256": RETAINED_V79_SEMANTIC_SHA256,
            "calendar_sha256": us_market_hours_sha256(),
        }
        and REPORT_VERSION == 4
        and retained_v79_policy(document.get("correlation_policy"))
    ):
        return False
    counts = document if "unresolved_rows" in document else document.get("projection", {})
    unresolved = counts.get("unresolved_rows") if isinstance(counts, dict) else None
    assets = document.get("asset_ids")
    members = document.get("group_members")
    if (
        members is None
        and isinstance(document.get("input_sources"), dict)
        and isinstance(document.get("unavailable_pricing", {}), dict)
    ):
        members = (*document["input_sources"], *document.get("unavailable_pricing", {}))
    return (
        (type(unresolved) is int and unresolved == 0)
        or (isinstance(assets, list) and len(assets) != 2)
        or (isinstance(members, (list, tuple)) and len(members) != 2)
    )


def retained_v80_decision(document):
    """The v80 producer cannot publish the new sourced-rename fact kind.

    New facts invalidate their selected group at metadata ingress. Unchanged
    completed v80 calculations retain their provenance without body reads.
    """
    return (
        isinstance(document, dict)
        and VERSION in (36, 37)
        and COMPUTATION_REVISION == 2
        and REPORT_VERSION == 4
        and reusable_computation(document)
        and document.get("versions")
        == {
            "lineage": 35,
            "identity_report": 4,
            "correlation": 80,
            "correlation_policy_sha256": RETAINED_V82_SEMANTIC_SHA256,
            "calendar_sha256": us_market_hours_sha256(),
        }
        and retained_v80_policy(document.get("correlation_policy"))
    )


def retained_v81_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {
            **versions(),
            "lineage": 36,
            "correlation": 81,
            "correlation_policy_sha256": RETAINED_V82_SEMANTIC_SHA256,
        }
        and retained_v81_policy(document.get("correlation_policy"))
    )


def retained_v82_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 82, "correlation_policy_sha256": RETAINED_V82_SEMANTIC_SHA256}
        and retained_v82_policy(document.get("correlation_policy"))
    )


def retained_v83_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 83, "correlation_policy_sha256": RETAINED_V83_SEMANTIC_SHA256}
        and retained_v83_policy(document.get("correlation_policy"))
    )


def retained_v84_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 84, "correlation_policy_sha256": RETAINED_V84_SEMANTIC_SHA256}
        and retained_v84_policy(document.get("correlation_policy"))
    )


def retained_v85_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 85, "correlation_policy_sha256": RETAINED_V85_SEMANTIC_SHA256}
        and retained_v85_policy(document.get("correlation_policy"))
    )


def retained_v86_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 86, "correlation_policy_sha256": RETAINED_V86_SEMANTIC_SHA256}
        and retained_v86_policy(document.get("correlation_policy"))
    )


def retained_v87_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 87, "correlation_policy_sha256": RETAINED_V87_SEMANTIC_SHA256}
        and retained_v87_policy(document.get("correlation_policy"))
    )


def retained_v88_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 88, "correlation_policy_sha256": RETAINED_V88_SEMANTIC_SHA256}
        and retained_v88_policy(document.get("correlation_policy"))
    )


def retained_v89_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 89, "correlation_policy_sha256": RETAINED_V89_SEMANTIC_SHA256}
        and retained_v89_policy(document.get("correlation_policy"))
    )


def retained_v90_decision(document):
    return (
        isinstance(document, dict)
        and VERSION == 37
        and reusable_computation(document)
        and document.get("versions")
        == {**versions(), "correlation": 90, "correlation_policy_sha256": RETAINED_V90_SEMANTIC_SHA256}
        and retained_v90_policy(document.get("correlation_policy"))
    )


def reusable_decision(document: dict) -> bool:
    """Accept current outputs and exactly applicable retained-policy handoffs."""
    if isinstance(document, dict) and isinstance(document.get("versions"), dict):
        if document["versions"].get("correlation") == 90 and PRODUCTION_CORRELATION_VERSION == 91:
            return retained_v90_decision(document)
        if document["versions"].get("correlation") == 89 and PRODUCTION_CORRELATION_VERSION in (90, 91):
            return retained_v89_decision(document)
        if document["versions"].get("correlation") == 88 and PRODUCTION_CORRELATION_VERSION in (89, 90, 91):
            return retained_v88_decision(document)
        if document["versions"].get("correlation") == 87 and PRODUCTION_CORRELATION_VERSION in (88, 89, 90, 91):
            return retained_v87_decision(document)
        if document["versions"].get("correlation") == 86 and PRODUCTION_CORRELATION_VERSION in (
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v86_decision(document)
        if document["versions"].get("correlation") == 85 and PRODUCTION_CORRELATION_VERSION in (
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v85_decision(document)
        if document["versions"].get("correlation") == 84 and PRODUCTION_CORRELATION_VERSION in (
            85,
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v84_decision(document)
        if document["versions"].get("correlation") == 83 and PRODUCTION_CORRELATION_VERSION in (
            84,
            85,
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v83_decision(document)
        if document["versions"].get("correlation") == 82 and PRODUCTION_CORRELATION_VERSION in (
            83,
            84,
            85,
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v82_decision(document)
        if document["versions"].get("correlation") == 81 and PRODUCTION_CORRELATION_VERSION in (
            82,
            83,
            84,
            85,
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v81_decision(document)
        if document["versions"].get("correlation") == 78:
            return retained_v78_decision(document)
        if document["versions"].get("correlation") == 80 and PRODUCTION_CORRELATION_VERSION in (
            81,
            82,
            83,
            84,
            85,
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v80_decision(document)
        if document["versions"].get("correlation") == 79 and PRODUCTION_CORRELATION_VERSION in (
            80,
            81,
            82,
            83,
            84,
            85,
            86,
            87,
            88,
            89,
            90,
            91,
        ):
            return retained_v79_decision(document)
    return current_decision(document)


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
    if not isinstance(entries, list) or len(entries) != RUNTIME_VERSION:
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
    # DO NOT replace this selector read with hash/stat/ancestor-path verification;
    # physical authentication belongs to an explicitly requested owner audit.
    if not isinstance(expected, str) or len(expected) != 64:
        raise LineageError("invalid selected lineage generation")
    return read_document(path, published=True)

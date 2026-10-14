"""Correlation over authenticated v6 logical adjustment streams."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from libs.alpaca_end_of_day_security_correlation.correlation import (
    build_security_correlation_report,
)
from libs.alpaca_end_of_day_security_correlation.correlation_version import (
    CORRELATION_VERSION,
    validate_correlation_change_log,
)
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.security_identity import is_security_id
from libs.alpaca_end_of_day_security_correlation.session_exclusions import (
    validate_verified_session_exclusions,
)
from libs.alpaca_end_of_day_security_correlation.settings import (
    CORRELATED_BRANCH,
    UNCORRELATED_BRANCH,
)

from .common import canonical_sha256, read_json_object, replace_json, safe_child, sha256_file
from .downstream_input import (
    DownstreamPricingInput,
    authenticated_symbol_sessions,
    build_downstream_pricing_input,
    empty_verified_session_exclusions,
    episode_partition_loaders,
)
from .errors import V6Error
from .lineage import load_lineage
from .paths import correlation_directory
from .versions import CORRELATION_ENVELOPE_VERSION, validate_boundary_change_log

CORRELATION_BOUNDARY = "alpaca_v6_security_correlation"
CORRELATED_OUTPUT = "correlated.json"
UNCORRELATED_OUTPUT = "uncorrelated.json"
CORRELATION_MANIFEST = "correlation.manifest.json"
CORRELATION_EVIDENCE_FIELDS = (
    "verified_session_exclusions",
    "uuid_lineage_edges",
    "ticker_ownership_intervals",
    "evidence_issues",
)


@dataclass(frozen=True)
class AuthenticatedV6Correlation:
    receipt: dict[str, Any]
    correlated: dict[str, Any]
    uncorrelated: dict[str, Any]
    correlated_sha256: str
    uncorrelated_sha256: str
    sources_by_filename: dict[str, dict[str, Any]]


def _adapt_v1_correlation_output(report: dict[str, Any]) -> dict[str, Any]:
    """Translate the established correlation report into the v6 disk contract."""
    for source in report["source_downloads"]:
        source["download_manifest"] = "authenticated_monthly_source"
    for source in report["files"]:
        source["download_manifest"] = "authenticated_monthly_source"
    for branch in (CORRELATED_BRANCH, UNCORRELATED_BRANCH):
        for segment in report[branch]["segments"]:
            segment["download_manifest"] = "authenticated_monthly_source"
    report["boundary"] = {
        "owner": CORRELATION_BOUNDARY,
        "immediate_inputs": ["monthly_pricing", "validation", "lineage"],
        "outputs": [CORRELATED_BRANCH, UNCORRELATED_BRANCH],
        "row_partition_is_exhaustive": True,
        "transformation_contract": "alpaca_end_of_day_security_correlation",
    }
    return report


def build_correlation_report(
    store_root: Path | str,
    symbol: str,
    *,
    preliminary_by_symbol: Mapping[str, dict[str, Any]] | None = None,
    pricing_input: DownstreamPricingInput | None = None,
    pricing_inputs_by_symbol: Mapping[str, DownstreamPricingInput] | None = None,
    lineage: tuple[dict[str, Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run the established correlation transform over a v1-shaped monthly view."""
    validate_boundary_change_log("correlation_envelope")
    validate_correlation_change_log()
    root = Path(store_root)
    view = pricing_input or build_downstream_pricing_input(root, symbol)
    validation_receipt = view.validation_receipt
    lineage_receipt, lineage_report = load_lineage(root, symbol) if lineage is None else lineage
    if lineage_receipt.get(
        "source_generation_sha256"
    ) != view.source.source_generation_sha256 or lineage_receipt.get(
        "validation_generation_sha256"
    ) != validation_receipt.get("validation_generation_sha256"):
        raise V6Error(f"lineage selects another pricing boundary: {symbol}")
    correlation_loader = source_rows_loader = None
    if preliminary_by_symbol is not None:
        correlation_loader, source_rows_loader = episode_partition_loaders(
            root,
            preliminary_by_symbol,
            pricing_inputs_by_symbol,
        )
    report = build_security_correlation_report(
        view.adapt_lineage_report(lineage_report),
        root,
        authenticated_download_reader=view.read_authenticated_download,
        verified_session_exclusions_reader=empty_verified_session_exclusions,
        authenticated_symbol_sessions=authenticated_symbol_sessions(
            root,
            pricing_inputs_by_symbol,
        ),
        correlation_loader=correlation_loader,
        source_rows_loader=source_rows_loader,
        assess_episode_partitions=preliminary_by_symbol is not None,
    )
    report = _adapt_v1_correlation_output(report)
    report.update(
        {
            "source_generation_sha256": view.source.source_generation_sha256,
            "validation_generation_sha256": validation_receipt["validation_generation_sha256"],
            "lineage_generation_sha256": lineage_receipt["lineage_generation_sha256"],
        }
    )
    return report


def _output_document(report: dict[str, Any], branch: str) -> dict[str, Any]:
    branch_report = report[branch]
    document = {
        "schema_version": CORRELATION_ENVELOPE_VERSION,
        "boundary": CORRELATION_BOUNDARY,
        "correlation_version": CORRELATION_VERSION,
        "symbol": report["symbol"],
        "branch": branch,
        "segment_count": branch_report["segment_count"],
        "row_count": branch_report["row_count"],
        "segments": branch_report["segments"],
    }
    if branch == UNCORRELATED_BRANCH:
        document.update(
            {
                "work_item_count": branch_report["work_item_count"],
                "work_items": branch_report["work_items"],
            }
        )
    return document


def publish_correlation(store_root: Path | str, report: dict[str, Any]) -> dict[str, Any]:
    validate_boundary_change_log("correlation_envelope")
    root = Path(store_root)
    symbol = report["symbol"]
    directory = safe_child(root / "correlation", symbol, "symbol")
    directory.mkdir(parents=True, exist_ok=True)
    output_documents = {
        CORRELATED_BRANCH: _output_document(report, CORRELATED_BRANCH),
        UNCORRELATED_BRANCH: _output_document(report, UNCORRELATED_BRANCH),
    }
    output_paths = {
        CORRELATED_BRANCH: directory / CORRELATED_OUTPUT,
        UNCORRELATED_BRANCH: directory / UNCORRELATED_OUTPUT,
    }
    descriptors = {}
    for branch in (CORRELATED_BRANCH, UNCORRELATED_BRANCH):
        path = output_paths[branch]
        replace_json(path, output_documents[branch])
        descriptors[branch] = {
            "file": path.name,
            "sha256": sha256_file(path),
            "bytes": path.stat().st_size,
        }
    generation = canonical_sha256(
        {
            "schema_version": CORRELATION_ENVELOPE_VERSION,
            "correlation_version": CORRELATION_VERSION,
            "symbol": symbol,
            "source_generation_sha256": report["source_generation_sha256"],
            "validation_generation_sha256": report["validation_generation_sha256"],
            "lineage_generation_sha256": report["lineage_generation_sha256"],
            **{field: report[field] for field in CORRELATION_EVIDENCE_FIELDS},
            "outputs": descriptors,
        }
    )
    receipt = {
        "schema_version": CORRELATION_ENVELOPE_VERSION,
        "boundary": CORRELATION_BOUNDARY,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "correlation_generation_sha256": generation,
        "source_generation_sha256": report["source_generation_sha256"],
        "validation_generation_sha256": report["validation_generation_sha256"],
        "lineage_generation_sha256": report["lineage_generation_sha256"],
        **{field: report[field] for field in CORRELATION_EVIDENCE_FIELDS},
        "partition": {
            "source_row_count": report["source_row_count"],
            "correlated_row_count": report["correlated"]["row_count"],
            "uncorrelated_row_count": report["uncorrelated"]["row_count"],
            "row_partition_complete": True,
        },
        "outputs": descriptors,
    }
    published = _validate_correlation_outputs(directory, symbol, receipt)
    if published.receipt != receipt:
        raise V6Error(f"published correlation receipt differs: {symbol}")
    manifest_path = directory / CORRELATION_MANIFEST
    replace_json(manifest_path, receipt)
    if read_json_object(manifest_path, "published correlation receipt") != receipt:
        raise V6Error(f"published correlation receipt differs: {symbol}")
    return receipt


def authenticate_correlation(
    store_root: Path | str,
    symbol: str,
) -> AuthenticatedV6Correlation:
    validate_boundary_change_log("correlation_envelope")
    validate_correlation_change_log()
    root = Path(store_root)
    directory = correlation_directory(root, symbol)
    receipt = read_json_object(directory / CORRELATION_MANIFEST, "v6 correlation receipt")
    return _validate_correlation_outputs(directory, symbol, receipt)


def _validate_correlation_outputs(
    directory: Path,
    symbol: str,
    receipt: dict[str, Any],
) -> AuthenticatedV6Correlation:
    if (
        receipt.get("schema_version") != CORRELATION_ENVELOPE_VERSION
        or receipt.get("boundary") != CORRELATION_BOUNDARY
        or receipt.get("correlation_version") != CORRELATION_VERSION
        or receipt.get("symbol") != symbol
    ):
        raise V6Error(f"v6 correlation receipt has the wrong identity: {symbol}")
    expected_inputs = {
        field: receipt.get(field)
        for field in (
            "source_generation_sha256",
            "validation_generation_sha256",
            "lineage_generation_sha256",
        )
    }
    if any(not isinstance(value, str) or not value for value in expected_inputs.values()):
        raise V6Error(f"v6 correlation has an invalid immediate-input identity: {symbol}")
    if any(
        not isinstance(receipt.get(field), list)
        for field in ("uuid_lineage_edges", "ticker_ownership_intervals", "evidence_issues")
    ):
        raise V6Error(f"v6 correlation has malformed identity evidence: {symbol}")
    try:
        validate_verified_session_exclusions(
            receipt.get("verified_session_exclusions"),
            symbol,
        )
    except SecurityCorrelationError as error:
        raise V6Error(f"v6 correlation has invalid session exclusions: {symbol}") from error
    documents = {}
    digests = {}
    expected_files = {CORRELATED_BRANCH: CORRELATED_OUTPUT, UNCORRELATED_BRANCH: UNCORRELATED_OUTPUT}
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise V6Error(f"v6 correlation receipt has no outputs: {symbol}")
    for branch, filename in expected_files.items():
        descriptor = outputs.get(branch)
        path = directory / filename
        if (
            not isinstance(descriptor, dict)
            or descriptor.get("file") != filename
            or not path.is_file()
            or path.is_symlink()
            or descriptor.get("bytes") != path.stat().st_size
            or descriptor.get("sha256") != sha256_file(path)
        ):
            raise V6Error(f"v6 correlation output authentication failed: {symbol}/{branch}")
        document = read_json_object(path, f"v6 {branch} correlation output")
        if (
            document.get("schema_version") != CORRELATION_ENVELOPE_VERSION
            or document.get("boundary") != CORRELATION_BOUNDARY
            or document.get("correlation_version") != CORRELATION_VERSION
            or document.get("symbol") != symbol
            or document.get("branch") != branch
        ):
            raise V6Error(f"v6 correlation output has the wrong identity: {symbol}/{branch}")
        documents[branch] = document
        digests[branch] = descriptor["sha256"]
    expected_generation = canonical_sha256(
        {
            "schema_version": CORRELATION_ENVELOPE_VERSION,
            "correlation_version": CORRELATION_VERSION,
            "symbol": symbol,
            **expected_inputs,
            **{field: receipt[field] for field in CORRELATION_EVIDENCE_FIELDS},
            "outputs": outputs,
        }
    )
    if receipt.get("correlation_generation_sha256") != expected_generation:
        raise V6Error(f"v6 correlation generation differs: {symbol}")

    sources = _published_sources(documents)
    ranges_by_filename = {filename: [] for filename in sources}
    counts = {}
    for branch, document in documents.items():
        segments = document.get("segments")
        if not isinstance(segments, list) or document.get("segment_count") != len(segments):
            raise V6Error(f"v6 correlation segment count differs: {symbol}/{branch}")
        count = 0
        for segment in segments:
            filename = segment.get("data_file") if isinstance(segment, dict) else None
            source_descriptor = sources.get(filename)
            if source_descriptor is None:
                raise V6Error(f"v6 correlation refers to an unknown logical source: {filename!r}")
            start, end, row_count = segment.get("row_start"), segment.get("row_end"), segment.get("row_count")
            if (
                type(start) is not int
                or type(end) is not int
                or type(row_count) is not int
                or start < 1
                or end < start
                or row_count != end - start + 1
                or end > source_descriptor["rows"]
                or segment.get("download_sha256") != source_descriptor["download_sha256"]
                or segment.get("adjustment") != source_descriptor["adjustment"]
            ):
                raise V6Error(f"v6 correlation has an invalid segment: {symbol}/{filename}")
            if branch == CORRELATED_BRANCH:
                security = segment.get("canonical_security")
                if not isinstance(security, dict) or not is_security_id(security.get("security_id")):
                    raise V6Error(f"v6 correlated segment has no valid identity: {symbol}/{filename}")
            ranges_by_filename[filename].append((start, end))
            count += row_count
        if document.get("row_count") != count:
            raise V6Error(f"v6 correlation row count differs: {symbol}/{branch}")
        counts[branch] = count
    for filename, source_descriptor in sources.items():
        expected = 1
        for start, end in sorted(ranges_by_filename[filename]):
            if start != expected:
                raise V6Error(f"v6 correlation partition has a gap or overlap: {symbol}/{filename}")
            expected = end + 1
        if expected != source_descriptor["rows"] + 1:
            raise V6Error(f"v6 correlation does not exhaust logical source: {symbol}/{filename}")
    expected_partition = {
        "source_row_count": sum(item["rows"] for item in sources.values()),
        "correlated_row_count": counts[CORRELATED_BRANCH],
        "uncorrelated_row_count": counts[UNCORRELATED_BRANCH],
        "row_partition_complete": True,
    }
    if receipt.get("partition") != expected_partition:
        raise V6Error(f"v6 correlation partition receipt differs: {symbol}")
    return AuthenticatedV6Correlation(
        receipt=receipt,
        correlated=documents[CORRELATED_BRANCH],
        uncorrelated=documents[UNCORRELATED_BRANCH],
        correlated_sha256=digests[CORRELATED_BRANCH],
        uncorrelated_sha256=digests[UNCORRELATED_BRANCH],
        sources_by_filename=sources,
    )


def _published_sources(documents: Mapping[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
    for document in documents.values():
        for segment in document.get("segments", ()):
            if not isinstance(segment, dict):
                raise V6Error("v6 published correlation contains a malformed segment")
            filename = segment.get("data_file")
            if not isinstance(filename, str) or Path(filename).name != filename:
                raise V6Error("v6 published correlation contains an unsafe source filename")
            descriptor = sources.setdefault(
                filename,
                {
                    "data_file": filename,
                    "download_manifest": "authenticated_monthly_source",
                    "download_sha256": segment.get("download_sha256"),
                    "rows": 0,
                    "adjustment": segment.get("adjustment"),
                    "pipeline_state": segment.get("pipeline_state"),
                    "validation_pipeline_version": segment.get("validation_pipeline_version"),
                    "download_sha256_authenticated": True,
                    "symbol": segment.get("requested_symbol"),
                    "asof": segment.get("asof"),
                    "request_from": segment.get("request_from"),
                    "request_to": segment.get("request_to"),
                    "feed": segment.get("feed"),
                    "timeframe": segment.get("timeframe"),
                },
            )
            row_end = segment.get("row_end")
            if type(row_end) is not int:
                raise V6Error(f"v6 published correlation has no row bound: {filename}")
            descriptor["rows"] = max(descriptor["rows"], row_end)
    return sources


def load_correlation(store_root: Path | str, symbol: str) -> AuthenticatedV6Correlation:
    """Consume a published correlation boundary without auditing it again."""
    directory = correlation_directory(store_root, symbol)
    receipt = read_json_object(directory / CORRELATION_MANIFEST, "v6 correlation receipt")
    correlated = read_json_object(directory / CORRELATED_OUTPUT, "v6 correlated output")
    uncorrelated = read_json_object(directory / UNCORRELATED_OUTPUT, "v6 uncorrelated output")
    if any(document.get("symbol") != symbol for document in (receipt, correlated, uncorrelated)):
        raise V6Error(f"v6 correlation belongs to another symbol: {symbol}")
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise V6Error(f"v6 correlation receipt has no outputs: {symbol}")
    try:
        correlated_sha256 = outputs[CORRELATED_BRANCH]["sha256"]
        uncorrelated_sha256 = outputs[UNCORRELATED_BRANCH]["sha256"]
    except (KeyError, TypeError) as error:
        raise V6Error(f"v6 correlation receipt has incomplete outputs: {symbol}") from error
    documents = {CORRELATED_BRANCH: correlated, UNCORRELATED_BRANCH: uncorrelated}
    return AuthenticatedV6Correlation(
        receipt=receipt,
        correlated=correlated,
        uncorrelated=uncorrelated,
        correlated_sha256=correlated_sha256,
        uncorrelated_sha256=uncorrelated_sha256,
        sources_by_filename=_published_sources(documents),
    )


def load_correlation_receipt(store_root: Path | str, symbol: str) -> dict[str, Any]:
    """Select the current correlation receipt without reading or auditing its outputs."""
    receipt = read_json_object(
        correlation_directory(store_root, symbol) / CORRELATION_MANIFEST,
        "v6 correlation receipt",
    )
    if (
        receipt.get("schema_version") != CORRELATION_ENVELOPE_VERSION
        or receipt.get("boundary") != CORRELATION_BOUNDARY
        or receipt.get("correlation_version") != CORRELATION_VERSION
        or receipt.get("symbol") != symbol
    ):
        raise V6Error(f"v6 correlation receipt has the wrong identity: {symbol}")
    return receipt


__all__ = [
    "AuthenticatedV6Correlation",
    "build_correlation_report",
    "authenticate_correlation",
    "load_correlation",
    "load_correlation_receipt",
    "publish_correlation",
]

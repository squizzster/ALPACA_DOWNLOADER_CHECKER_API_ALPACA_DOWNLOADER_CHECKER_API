"""Advisory warning boundary for authenticated v6 inputs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION
from libs.alpaca_end_of_day_security_correlation import CORRELATION_VERSION
from libs.alpaca_end_of_day_warning_pipeline.authenticated_outputs import (
    AuthenticatedWarningPipelineOutputs,
)
from libs.alpaca_end_of_day_warning_pipeline.warning_pipeline import (
    build_warning_pipeline_report,
)
from libs.alpaca_end_of_day_warning_pipeline.warning_pipeline_version import (
    WARNING_PIPELINE_VERSION,
    validate_warning_pipeline_change_log,
)
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256

from .common import canonical_sha256, read_json_object, replace_json, safe_child, sha256_file
from .correlation import AuthenticatedV6Correlation, load_correlation
from .downstream_input import DownstreamPricingInput, build_downstream_pricing_input
from .errors import V6Error
from .paths import warning_directory
from .versions import WARNING_ENVELOPE_VERSION, validate_boundary_change_log

WARNING_BOUNDARY = "alpaca_v6_market_data_warnings"
CROSS_OUTPUT = "cross_adjustment.json"
STATISTICS_OUTPUT = "security_segment_statistics.json"
WARNING_MANIFEST = "warnings.manifest.json"


def build_warning_report(
    store_root: Path | str,
    symbol: str,
    *,
    pricing_input: DownstreamPricingInput | None = None,
    correlation: AuthenticatedV6Correlation | None = None,
) -> dict[str, Any]:
    """Run the established warning transform over the downstream pricing input."""
    validate_boundary_change_log("warning_envelope")
    validate_warning_pipeline_change_log()
    root = Path(store_root)
    view = pricing_input or build_downstream_pricing_input(root, symbol)
    correlation = correlation or load_correlation(root, symbol)
    if correlation.receipt.get(
        "source_generation_sha256"
    ) != view.source.source_generation_sha256 or correlation.receipt.get(
        "validation_generation_sha256"
    ) != view.validation_receipt.get("validation_generation_sha256"):
        raise V6Error(f"correlation selects another pricing boundary: {symbol}")
    report = build_warning_pipeline_report(
        root,
        symbol,
        inputs=view.warning_inputs(correlation),
    )
    report["source_generation_sha256"] = view.source.source_generation_sha256
    report["validation_generation_sha256"] = view.validation_receipt["validation_generation_sha256"]
    report["correlation_generation_sha256"] = correlation.receipt["correlation_generation_sha256"]
    report["boundary"] = {
        "owner": WARNING_BOUNDARY,
        "immediate_inputs": ["monthly_pricing", "validation", "correlation"],
        "transformation_contract": "alpaca_end_of_day_warning_pipeline",
    }
    return report


def _output_document(report: dict[str, Any], output_name: str) -> dict[str, Any]:
    return {
        "schema_version": WARNING_ENVELOPE_VERSION,
        "boundary": WARNING_BOUNDARY,
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "symbol": report["symbol"],
        "output": output_name,
        "report": report[output_name],
    }


def publish_warnings(store_root: Path | str, report: dict[str, Any]) -> dict[str, Any]:
    validate_boundary_change_log("warning_envelope")
    root = Path(store_root)
    symbol = report["symbol"]
    directory = safe_child(root / "warnings", symbol, "symbol")
    directory.mkdir(parents=True, exist_ok=True)
    outputs = {}
    for output_name, filename in (
        ("cross_adjustment", CROSS_OUTPUT),
        ("security_segment_statistics", STATISTICS_OUTPUT),
    ):
        path = directory / filename
        replace_json(path, _output_document(report, output_name))
        outputs[output_name] = {
            "file": filename,
            "sha256": sha256_file(path),
            "bytes": path.stat().st_size,
        }
    generation = canonical_sha256(
        {
            "schema_version": WARNING_ENVELOPE_VERSION,
            "warning_pipeline_version": WARNING_PIPELINE_VERSION,
            "symbol": symbol,
            "source_generation_sha256": report["source_generation_sha256"],
            "validation_generation_sha256": report["validation_generation_sha256"],
            "correlation_generation_sha256": report["correlation_generation_sha256"],
            "configuration": report["configuration"],
            "calendar_sha256": report["calendar_sha256"],
            "outputs": outputs,
        }
    )
    cross = report["cross_adjustment"]
    statistics = report["security_segment_statistics"]
    receipt = {
        "schema_version": WARNING_ENVELOPE_VERSION,
        "boundary": WARNING_BOUNDARY,
        "warning_pipeline_version": WARNING_PIPELINE_VERSION,
        "validation_pipeline_version": PIPELINE_VERSION,
        "correlation_version": CORRELATION_VERSION,
        "symbol": symbol,
        "warning_pipeline_generation_sha256": generation,
        "source_generation_sha256": report["source_generation_sha256"],
        "validation_generation_sha256": report["validation_generation_sha256"],
        "correlation_generation_sha256": report["correlation_generation_sha256"],
        "configuration": report["configuration"],
        "calendar_sha256": report["calendar_sha256"],
        "summary": {
            "warning_count": cross["warning_count"] + statistics["warning_count"],
            "information_count": cross["information_count"] + statistics["information_count"],
            "omitted_finding_count": (cross["omitted_finding_count"] + statistics["omitted_finding_count"]),
        },
        "outputs": outputs,
    }
    published = _validate_warning_outputs(directory, symbol, receipt)
    if published.receipt != receipt:
        raise V6Error(f"published warning receipt differs: {symbol}")
    manifest_path = directory / WARNING_MANIFEST
    replace_json(manifest_path, receipt)
    if read_json_object(manifest_path, "published warning receipt") != receipt:
        raise V6Error(f"published warning receipt differs: {symbol}")
    return receipt


def _validate_report_counts(report: object, description: str) -> dict[str, Any]:
    if not isinstance(report, dict) or not isinstance(report.get("findings"), list):
        raise V6Error(f"v6 {description} warning report is malformed")
    findings = report["findings"]
    warning_count = sum(item.get("severity") == "warning" for item in findings if isinstance(item, dict))
    information_count = sum(item.get("severity") == "information" for item in findings if isinstance(item, dict))
    if (
        warning_count > report.get("warning_count", -1)
        or information_count > report.get("information_count", -1)
        or report.get("total_finding_count") != len(findings) + report.get("omitted_finding_count", -1)
        or report.get("total_finding_count")
        != report.get("warning_count", -1) + report.get("information_count", -1)
    ):
        raise V6Error(f"v6 {description} warning counts differ")
    return report


def authenticate_warnings(
    store_root: Path | str,
    symbol: str,
) -> AuthenticatedWarningPipelineOutputs:
    validate_boundary_change_log("warning_envelope")
    validate_warning_pipeline_change_log()
    root = Path(store_root)
    directory = warning_directory(root, symbol)
    receipt = read_json_object(directory / WARNING_MANIFEST, "v6 warning receipt")
    return _validate_warning_outputs(directory, symbol, receipt)


def _validate_warning_outputs(
    directory: Path,
    symbol: str,
    receipt: dict[str, Any],
) -> AuthenticatedWarningPipelineOutputs:
    if (
        receipt.get("schema_version") != WARNING_ENVELOPE_VERSION
        or receipt.get("boundary") != WARNING_BOUNDARY
        or receipt.get("warning_pipeline_version") != WARNING_PIPELINE_VERSION
        or receipt.get("validation_pipeline_version") != PIPELINE_VERSION
        or receipt.get("correlation_version") != CORRELATION_VERSION
        or receipt.get("symbol") != symbol
        or receipt.get("calendar_sha256") != us_market_hours_sha256()
        or not isinstance(receipt.get("source_generation_sha256"), str)
        or not isinstance(receipt.get("validation_generation_sha256"), str)
        or not isinstance(receipt.get("correlation_generation_sha256"), str)
    ):
        raise V6Error(f"v6 warning receipt has stale or invalid inputs: {symbol}")
    documents = {}
    digests = {}
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise V6Error(f"v6 warning receipt has no outputs: {symbol}")
    for output_name, filename in (
        ("cross_adjustment", CROSS_OUTPUT),
        ("security_segment_statistics", STATISTICS_OUTPUT),
    ):
        descriptor = outputs.get(output_name)
        path = directory / filename
        if (
            not isinstance(descriptor, dict)
            or descriptor.get("file") != filename
            or not path.is_file()
            or path.is_symlink()
            or descriptor.get("bytes") != path.stat().st_size
            or descriptor.get("sha256") != sha256_file(path)
        ):
            raise V6Error(f"v6 warning output authentication failed: {symbol}/{output_name}")
        document = read_json_object(path, f"v6 {output_name} warning output")
        if (
            document.get("schema_version") != WARNING_ENVELOPE_VERSION
            or document.get("boundary") != WARNING_BOUNDARY
            or document.get("warning_pipeline_version") != WARNING_PIPELINE_VERSION
            or document.get("symbol") != symbol
            or document.get("output") != output_name
        ):
            raise V6Error(f"v6 warning output has the wrong identity: {symbol}/{output_name}")
        _validate_report_counts(document.get("report"), output_name)
        documents[output_name] = document
        digests[output_name] = descriptor["sha256"]
    expected_summary = {
        "warning_count": sum(
            documents[output_name]["report"]["warning_count"]
            for output_name in ("cross_adjustment", "security_segment_statistics")
        ),
        "information_count": sum(
            documents[output_name]["report"]["information_count"]
            for output_name in ("cross_adjustment", "security_segment_statistics")
        ),
        "omitted_finding_count": sum(
            documents[output_name]["report"]["omitted_finding_count"]
            for output_name in ("cross_adjustment", "security_segment_statistics")
        ),
    }
    if receipt.get("summary") != expected_summary:
        raise V6Error(f"v6 warning receipt summary differs: {symbol}")
    generation = canonical_sha256(
        {
            "schema_version": WARNING_ENVELOPE_VERSION,
            "warning_pipeline_version": WARNING_PIPELINE_VERSION,
            "symbol": symbol,
            "source_generation_sha256": receipt["source_generation_sha256"],
            "validation_generation_sha256": receipt["validation_generation_sha256"],
            "correlation_generation_sha256": receipt["correlation_generation_sha256"],
            "configuration": receipt["configuration"],
            "calendar_sha256": receipt["calendar_sha256"],
            "outputs": outputs,
        }
    )
    if receipt.get("warning_pipeline_generation_sha256") != generation:
        raise V6Error(f"v6 warning generation differs: {symbol}")
    return AuthenticatedWarningPipelineOutputs(
        receipt=receipt,
        cross_adjustment=documents["cross_adjustment"],
        security_segment_statistics=documents["security_segment_statistics"],
        cross_adjustment_sha256=digests["cross_adjustment"],
        security_segment_statistics_sha256=digests["security_segment_statistics"],
    )


def load_warnings(store_root: Path | str, symbol: str) -> AuthenticatedWarningPipelineOutputs:
    """Consume a published warning boundary without auditing it again."""
    directory = warning_directory(store_root, symbol)
    receipt = read_json_object(directory / WARNING_MANIFEST, "v6 warning receipt")
    cross = read_json_object(directory / CROSS_OUTPUT, "v6 cross-adjustment warning output")
    statistics = read_json_object(
        directory / STATISTICS_OUTPUT,
        "v6 security-segment warning output",
    )
    if any(document.get("symbol") != symbol for document in (receipt, cross, statistics)):
        raise V6Error(f"v6 warnings belong to another symbol: {symbol}")
    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise V6Error(f"v6 warning receipt has no outputs: {symbol}")
    try:
        cross_sha256 = outputs["cross_adjustment"]["sha256"]
        statistics_sha256 = outputs["security_segment_statistics"]["sha256"]
    except (KeyError, TypeError) as error:
        raise V6Error(f"v6 warning receipt has incomplete outputs: {symbol}") from error
    return AuthenticatedWarningPipelineOutputs(
        receipt=receipt,
        cross_adjustment=cross,
        security_segment_statistics=statistics,
        cross_adjustment_sha256=cross_sha256,
        security_segment_statistics_sha256=statistics_sha256,
    )


def load_warning_receipt(store_root: Path | str, symbol: str) -> dict[str, Any]:
    """Select the current warning receipt without reading or auditing its outputs."""
    receipt = read_json_object(
        warning_directory(store_root, symbol) / WARNING_MANIFEST,
        "v6 warning receipt",
    )
    if (
        receipt.get("schema_version") != WARNING_ENVELOPE_VERSION
        or receipt.get("boundary") != WARNING_BOUNDARY
        or receipt.get("warning_pipeline_version") != WARNING_PIPELINE_VERSION
        or receipt.get("validation_pipeline_version") != PIPELINE_VERSION
        or receipt.get("correlation_version") != CORRELATION_VERSION
        or receipt.get("symbol") != symbol
        or receipt.get("calendar_sha256") != us_market_hours_sha256()
    ):
        raise V6Error(f"v6 warning receipt has the wrong identity: {symbol}")
    return receipt


__all__ = [
    "authenticate_warnings",
    "build_warning_report",
    "load_warning_receipt",
    "load_warnings",
    "publish_warnings",
]

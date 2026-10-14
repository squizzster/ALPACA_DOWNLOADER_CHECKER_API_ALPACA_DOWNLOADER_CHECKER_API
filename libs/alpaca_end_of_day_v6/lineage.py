"""Current normalized Alpaca identity evidence as the v6 lineage source."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_security_correlation.publication import lineage_evidence_sha256
from libs.alpaca_security_lineage import AlpacaIdentityClient, build_identity_report
from libs.alpaca_security_lineage.settings import REPORT_VERSION

from .common import canonical_sha256, read_json_object, replace_json, safe_child, sha256_file
from .downstream_input import DownstreamPricingInput, build_downstream_pricing_input
from .errors import V6Error
from .paths import lineage_directory
from .versions import LINEAGE_SOURCE_VERSION, validate_boundary_change_log

LINEAGE_BOUNDARY = "alpaca_v6_lineage_source"
LINEAGE_OUTPUT = "lineage.json"
LINEAGE_MANIFEST = "lineage.manifest.json"


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _pricing_view(
    store_root: Path,
    symbol: str,
    request_end_timestamp: str,
    pricing_input: DownstreamPricingInput | None,
) -> DownstreamPricingInput:
    view = pricing_input or build_downstream_pricing_input(store_root, symbol)
    if view.request["request_to"] != request_end_timestamp:
        raise V6Error(f"validation request boundary differs during lineage acquisition: {symbol}")
    return view


def acquire_and_publish_lineage(
    store_root: Path | str,
    symbol: str,
    request_end_timestamp: str,
    api_key_id: str,
    api_secret_key: str,
    *,
    rate_limiter=None,
    symbol_resolution: dict[str, Any] | None = None,
    pricing_input: DownstreamPricingInput | None = None,
    client_factory=AlpacaIdentityClient,
) -> dict[str, Any]:
    validate_boundary_change_log("lineage_source")
    root = Path(store_root)
    pricing_view = _pricing_view(root, symbol, request_end_timestamp, pricing_input)
    local = pricing_view.local_report()
    client = client_factory(api_key_id, api_secret_key, rate_limiter=rate_limiter)
    lineage = build_identity_report(symbol, local, client)
    report = {
        "report_version": REPORT_VERSION,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "symbol": symbol,
        "lineage": lineage,
    }
    if symbol_resolution is not None:
        report["symbol_resolution"] = symbol_resolution
    evidence_sha256 = lineage_evidence_sha256(report)
    source_generation = pricing_view.source.source_generation_sha256
    validation_generation = pricing_view.validation_receipt["validation_generation_sha256"]
    generation = canonical_sha256(
        {
            "schema_version": LINEAGE_SOURCE_VERSION,
            "report_version": REPORT_VERSION,
            "symbol": symbol,
            "source_generation_sha256": source_generation,
            "validation_generation_sha256": validation_generation,
            "lineage_evidence_sha256": evidence_sha256,
        }
    )
    directory = safe_child(root / "lineage", symbol, "symbol")
    manifest_path = directory / LINEAGE_MANIFEST
    output_path = directory / LINEAGE_OUTPUT
    if manifest_path.is_file() and output_path.is_file():
        existing_receipt, existing_report = load_lineage(root, symbol)
        if (
            existing_receipt.get("schema_version") == LINEAGE_SOURCE_VERSION
            and existing_receipt.get("boundary") == LINEAGE_BOUNDARY
            and existing_report.get("report_version") == REPORT_VERSION
            and existing_receipt.get("source_generation_sha256") == source_generation
            and existing_receipt.get("validation_generation_sha256") == validation_generation
            and existing_receipt.get("lineage_evidence_sha256") == evidence_sha256
            and existing_receipt.get("lineage_generation_sha256") == generation
        ):
            return existing_receipt

    directory.mkdir(parents=True, exist_ok=True)
    replace_json(output_path, report)
    receipt = {
        "schema_version": LINEAGE_SOURCE_VERSION,
        "boundary": LINEAGE_BOUNDARY,
        "symbol": symbol,
        "lineage_generation_sha256": generation,
        "lineage_evidence_sha256": evidence_sha256,
        "source_generation_sha256": source_generation,
        "validation_generation_sha256": validation_generation,
        "normalization": "current_asset_and_corporate_action_evidence",
        "raw_http_retained": False,
        "output": {
            "file": LINEAGE_OUTPUT,
            "sha256": sha256_file(output_path),
            "bytes": output_path.stat().st_size,
        },
    }
    _validate_lineage_output(symbol, receipt, report, output_path)
    replace_json(manifest_path, receipt)
    if read_json_object(manifest_path, "published lineage receipt") != receipt:
        raise V6Error(f"published lineage output differs: {symbol}")
    return receipt


def load_lineage(store_root: Path | str, symbol: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Consume a published lineage boundary without auditing it again."""
    directory = lineage_directory(store_root, symbol)
    receipt = read_json_object(directory / LINEAGE_MANIFEST, "v6 lineage receipt")
    report = read_json_object(directory / LINEAGE_OUTPUT, "v6 lineage output")
    if receipt.get("symbol") != symbol or report.get("symbol") != symbol:
        raise V6Error(f"v6 lineage belongs to another symbol: {symbol}")
    return receipt, report


def authenticate_lineage(store_root: Path | str, symbol: str) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_boundary_change_log("lineage_source")
    directory = lineage_directory(store_root, symbol)
    receipt = read_json_object(directory / LINEAGE_MANIFEST, "v6 lineage receipt")
    output_path = directory / LINEAGE_OUTPUT
    report = read_json_object(output_path, "v6 lineage output")
    _validate_lineage_output(symbol, receipt, report, output_path)
    return receipt, report


def _validate_lineage_output(
    symbol: str,
    receipt: dict[str, Any],
    report: dict[str, Any],
    output_path: Path,
) -> None:
    output = receipt.get("output")
    if (
        receipt.get("schema_version") != LINEAGE_SOURCE_VERSION
        or receipt.get("boundary") != LINEAGE_BOUNDARY
        or receipt.get("symbol") != symbol
        or report.get("report_version") != REPORT_VERSION
        or receipt.get("raw_http_retained") is not False
        or not all(
            _is_sha256(receipt.get(field))
            for field in (
                "source_generation_sha256",
                "validation_generation_sha256",
                "lineage_evidence_sha256",
            )
        )
        or not isinstance(output, dict)
        or output.get("file") != LINEAGE_OUTPUT
        or not output_path.is_file()
        or output_path.is_symlink()
        or output.get("bytes") != output_path.stat().st_size
        or output.get("sha256") != sha256_file(output_path)
    ):
        raise V6Error(f"v6 lineage output authentication failed: {symbol}")
    evidence_sha256 = lineage_evidence_sha256(report)
    expected_generation = canonical_sha256(
        {
            "schema_version": LINEAGE_SOURCE_VERSION,
            "report_version": REPORT_VERSION,
            "symbol": symbol,
            "source_generation_sha256": receipt["source_generation_sha256"],
            "validation_generation_sha256": receipt["validation_generation_sha256"],
            "lineage_evidence_sha256": evidence_sha256,
        }
    )
    if (
        report.get("symbol") != symbol
        or evidence_sha256 != receipt.get("lineage_evidence_sha256")
        or expected_generation != receipt.get("lineage_generation_sha256")
    ):
        raise V6Error(f"v6 lineage generation differs: {symbol}")


__all__ = [
    "LINEAGE_BOUNDARY",
    "acquire_and_publish_lineage",
    "authenticate_lineage",
    "load_lineage",
]

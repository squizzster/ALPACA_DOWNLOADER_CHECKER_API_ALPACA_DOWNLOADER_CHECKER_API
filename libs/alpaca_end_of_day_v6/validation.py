"""Monthly pricing validation as a compact, incrementally published boundary.

The producer validates newly written month artifacts.  Ordinary consumers read
the published ticker receipt and summary without authenticating prior outputs;
the explicit audit entry point traverses and authenticates the complete current
boundary.
"""

from __future__ import annotations

import calendar
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from libs.alpaca_end_of_day_pipeline import PIPELINE_VERSION
from libs.alpaca_end_of_day_pipeline.daily_bar_validation import validate_end_of_day_bars
from libs.common.us_equity_market_session_calendar import (
    market_session_dates_sha256,
    us_market_hours_sha256,
)
from libs.download_alpaca_end_of_day_bars.settings import (
    ALPACA_ADJUSTMENT_MODES,
    ALPACA_BAR_TIMEFRAME,
    ALPACA_DATA_FEED,
    EARLIEST_REQUEST_TIMESTAMP,
)

from .common import (
    canonical_sha256,
    durability_batch,
    read_json_object,
    replace_json,
    safe_child,
    sha256_file,
)
from .errors import V6Error
from .paths import validation_directory
from .source import AuthenticatedMonthlySource, MonthlyLogicalSource, load_monthly_source
from .versions import VALIDATION_VERSION, validate_boundary_change_log

VALIDATION_BOUNDARY = "alpaca_v6_pricing_validation"
MONTH_VALIDATION_BOUNDARY = "alpaca_v6_pricing_month_validation"
VALIDATION_OUTPUT = "validation.json"
VALIDATION_MANIFEST = "validation.manifest.json"
MONTH_VALIDATION_OUTPUT = "month.validation.json"
VALIDATION_STRATEGY = "mirrored_month_results_with_predecessor_seams"


def request_identity(
    symbol: str,
    adjustment: str,
    request_end_timestamp: str,
) -> dict[str, str]:
    return {
        "symbol": symbol,
        "adjustment": adjustment,
        "feed": ALPACA_DATA_FEED,
        "timeframe": ALPACA_BAR_TIMEFRAME,
        "request_from": EARLIEST_REQUEST_TIMESTAMP,
        "request_to": request_end_timestamp,
        "asof": request_end_timestamp[:10],
    }


def _month_request_identity(
    symbol: str,
    adjustment: str,
    placement: str,
) -> dict[str, str]:
    if placement == "unplaced":
        return request_identity(symbol, adjustment, "2100-01-01T00:00:00Z")
    try:
        year, month = (int(part) for part in placement.split("-"))
        datetime(year, month, 1, tzinfo=timezone.utc)
    except (TypeError, ValueError) as error:
        raise V6Error(f"invalid monthly validation placement: {placement!r}") from error
    next_year = year + (month == 12)
    next_month = month % 12 + 1
    request = request_identity(
        symbol,
        adjustment,
        f"{next_year:04d}-{next_month:02d}-01T00:00:00Z",
    )
    request["request_from"] = f"{year:04d}-{month:02d}-01T00:00:00Z"
    return request


def _empty_month_result(part: MonthlyLogicalSource, adjustment: str) -> dict[str, Any]:
    return {
        "adjustment": adjustment,
        "source_logical_sha256": part.logical_sha256,
        "rows_seen": 0,
        "first_timestamp": None,
        "last_timestamp": None,
        "observed_session_dates": [],
        "session_count": 0,
        "session_sha256": market_session_dates_sha256(()),
        "row_failure_count": 0,
        "dataset_failure_count": 0,
        "row_failures": [],
        "dataset_failures": [],
    }


def _validate_month_part(
    part: MonthlyLogicalSource,
    symbol: str,
    adjustment: str,
) -> dict[str, Any]:
    if not part.rows:
        return _empty_month_result(part, adjustment)
    result = validate_end_of_day_bars(
        list(part.rows),
        _month_request_identity(symbol, adjustment, part.placement),
    )
    sessions = list(result.observed_session_dates)
    return {
        "adjustment": adjustment,
        "source_logical_sha256": part.logical_sha256,
        "rows_seen": result.rows_seen,
        "first_timestamp": result.first_bar_timestamp,
        "last_timestamp": result.last_bar_timestamp,
        "observed_session_dates": sessions,
        "session_count": len(sessions),
        "session_sha256": market_session_dates_sha256(sessions),
        "row_failure_count": len(result.row_failures),
        "dataset_failure_count": len(result.dataset_failures),
        "row_failures": list(result.row_failures),
        "dataset_failures": list(result.dataset_failures),
    }


def _source_months(
    source: AuthenticatedMonthlySource,
) -> list[tuple[str, dict[str, MonthlyLogicalSource], str]]:
    by_placement: dict[str, dict[str, MonthlyLogicalSource]] = {}
    placement_order: list[str] = []
    for logical in source.adjustments:
        for part in logical.months:
            if part.placement not in by_placement:
                by_placement[part.placement] = {}
                placement_order.append(part.placement)
            by_placement[part.placement][logical.adjustment] = part

    result = []
    for placement in placement_order:
        parts = by_placement[placement]
        receipt_digests = {part.month_receipt_sha256 for part in parts.values()}
        if set(parts) != set(ALPACA_ADJUSTMENT_MODES) or len(receipt_digests) != 1:
            raise V6Error(f"monthly validation input is incomplete: {source.symbol}/{placement}")
        result.append((placement, parts, next(iter(receipt_digests))))
    return result


def _month_result(
    source: AuthenticatedMonthlySource,
    placement: str,
    parts: Mapping[str, MonthlyLogicalSource],
    source_month_receipt_sha256: str,
) -> tuple[dict[str, Any], str]:
    adjustments = [
        _validate_month_part(parts[adjustment], source.symbol, adjustment)
        for adjustment in ALPACA_ADJUSTMENT_MODES
    ]
    month = {
        "adjustments": adjustments,
        "passed": not any(item["row_failure_count"] or item["dataset_failure_count"] for item in adjustments),
    }
    digest = canonical_sha256(
        {
            "schema_version": VALIDATION_VERSION,
            "validation_pipeline_version": PIPELINE_VERSION,
            "calendar_sha256": us_market_hours_sha256(),
            "symbol": source.symbol,
            "placement": placement,
            "source_month_receipt_sha256": source_month_receipt_sha256,
            "month": month,
        }
    )
    return month, digest


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _predecessor_seam(
    left_placement: str,
    left_month: Mapping[str, Any],
    right_placement: str,
    right_month: Mapping[str, Any],
) -> dict[str, Any]:
    left_adjustments = left_month.get("adjustments")
    right_adjustments = right_month.get("adjustments")
    if not isinstance(left_adjustments, list) or not isinstance(right_adjustments, list):
        raise V6Error(f"validation month has no adjustment evidence: {left_placement}/{right_placement}")
    if len(left_adjustments) != len(right_adjustments):
        raise V6Error(f"validation month adjustment counts differ: {left_placement}/{right_placement}")
    comparisons = []
    for left, right in zip(left_adjustments, right_adjustments, strict=True):
        if left.get("adjustment") != right.get("adjustment"):
            raise V6Error(f"validation month adjustment order differs: {left_placement}/{right_placement}")
        left_timestamp = left.get("last_timestamp")
        right_timestamp = right.get("first_timestamp")
        parsed_left = _timestamp(left_timestamp)
        parsed_right = _timestamp(right_timestamp)
        checked = parsed_left is not None and parsed_right is not None
        comparisons.append(
            {
                "adjustment": left["adjustment"],
                "left_last_timestamp": left_timestamp,
                "right_first_timestamp": right_timestamp,
                "checked": checked,
                "strictly_increasing": not checked or parsed_left < parsed_right,
            }
        )
    return {
        "left_placement": left_placement,
        "right_placement": right_placement,
        "adjustments": comparisons,
        "passed": all(item["strictly_increasing"] for item in comparisons),
    }


def _month_relative_path(placement: str) -> Path:
    if placement == "unplaced":
        return Path("unplaced") / MONTH_VALIDATION_OUTPUT
    try:
        year_text, month_text = placement.split("-")
        year = int(year_text)
        month = int(month_text)
        if year_text != f"{year:04d}" or month_text != f"{month:02d}" or not 1 <= month <= 12:
            raise ValueError
    except (TypeError, ValueError) as error:
        raise V6Error(f"invalid validation placement: {placement!r}") from error
    return Path(year_text) / calendar.month_abbr[month] / MONTH_VALIDATION_OUTPUT


def _month_generation(document: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {key: value for key, value in document.items() if key != "month_validation_generation_sha256"}
    )


def _month_document(
    *,
    symbol: str,
    placement: str,
    source_month_receipt_sha256: str,
    month: dict[str, Any],
    month_result_sha256: str,
    predecessor: dict[str, Any] | None,
    predecessor_seam: dict[str, Any] | None,
) -> dict[str, Any]:
    document = {
        "schema_version": VALIDATION_VERSION,
        "boundary": MONTH_VALIDATION_BOUNDARY,
        "validation_pipeline_version": PIPELINE_VERSION,
        "calendar_sha256": us_market_hours_sha256(),
        "symbol": symbol,
        "placement": placement,
        "source_month_receipt_sha256": source_month_receipt_sha256,
        "month_result_sha256": month_result_sha256,
        "predecessor": predecessor,
        "month": month,
        "predecessor_seam": predecessor_seam,
        "passed": month.get("passed") is True
        and (predecessor_seam is None or predecessor_seam.get("passed") is True),
    }
    document["month_validation_generation_sha256"] = _month_generation(document)
    return document


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _month_output_descriptor(relative: Path, path: Path) -> dict[str, Any]:
    return {
        "file": relative.as_posix(),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }


def _month_descriptor(
    document: Mapping[str, Any],
    relative: Path,
    output_path: Path,
) -> dict[str, Any]:
    predecessor = document.get("predecessor")
    return {
        "placement": document["placement"],
        "source_month_receipt_sha256": document["source_month_receipt_sha256"],
        "month_result_sha256": document["month_result_sha256"],
        "predecessor_month_result_sha256": (
            predecessor.get("month_result_sha256") if isinstance(predecessor, dict) else None
        ),
        "month_validation_generation_sha256": document["month_validation_generation_sha256"],
        "passed": document["passed"],
        "output": _month_output_descriptor(relative, output_path),
    }


def _validate_month_output(
    symbol: str,
    descriptor: Mapping[str, Any],
    document: Mapping[str, Any],
    output_path: Path,
) -> None:
    output = descriptor.get("output")
    predecessor = document.get("predecessor")
    predecessor_digest = predecessor.get("month_result_sha256") if isinstance(predecessor, dict) else None
    month = document.get("month")
    adjustments = month.get("adjustments") if isinstance(month, dict) else None
    malformed = (
        document.get("schema_version") != VALIDATION_VERSION
        or document.get("boundary") != MONTH_VALIDATION_BOUNDARY
        or document.get("validation_pipeline_version") != PIPELINE_VERSION
        or document.get("calendar_sha256") != us_market_hours_sha256()
        or document.get("symbol") != symbol
        or descriptor.get("placement") != document.get("placement")
        or descriptor.get("source_month_receipt_sha256") != document.get("source_month_receipt_sha256")
        or descriptor.get("month_result_sha256") != document.get("month_result_sha256")
        or descriptor.get("predecessor_month_result_sha256") != predecessor_digest
        or descriptor.get("month_validation_generation_sha256")
        != document.get("month_validation_generation_sha256")
        or document.get("month_validation_generation_sha256") != _month_generation(document)
        or not _is_sha256(document.get("source_month_receipt_sha256"))
        or not _is_sha256(document.get("month_result_sha256"))
        or not isinstance(adjustments, list)
        or [entry.get("adjustment") for entry in adjustments if isinstance(entry, dict)]
        != list(ALPACA_ADJUSTMENT_MODES)
        or descriptor.get("passed") is not document.get("passed")
        or not isinstance(output, dict)
        or not output_path.is_file()
        or output_path.is_symlink()
        or output.get("bytes") != output_path.stat().st_size
        or output.get("sha256") != sha256_file(output_path)
    )
    if malformed:
        raise V6Error(
            f"v6 monthly validation output authentication failed: {symbol}/{descriptor.get('placement')}"
        )


def _prior_receipt(root: Path, symbol: str) -> dict[str, Any] | None:
    path = validation_directory(root, symbol) / VALIDATION_MANIFEST
    if not path.is_file():
        return None
    receipt = read_json_object(path, "prior v6 validation receipt")
    if (
        receipt.get("schema_version") != VALIDATION_VERSION
        or receipt.get("boundary") != VALIDATION_BOUNDARY
        or receipt.get("symbol") != symbol
        or receipt.get("validation_pipeline_version") != PIPELINE_VERSION
        or receipt.get("calendar_sha256") != us_market_hours_sha256()
        or not isinstance(receipt.get("months"), list)
    ):
        return None
    return receipt


def _trusted_month_document(
    directory: Path,
    descriptor: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any]:
    output = descriptor.get("output")
    filename = output.get("file") if isinstance(output, dict) else None
    if not isinstance(filename, str):
        raise V6Error(f"prior validation month descriptor is malformed: {symbol}")
    expected = _month_relative_path(descriptor.get("placement", ""))
    if filename != expected.as_posix():
        raise V6Error(f"prior validation month path differs: {symbol}/{filename}")
    document = read_json_object(directory / expected, "trusted v6 monthly validation")
    if (
        document.get("symbol") != symbol
        or document.get("placement") != descriptor.get("placement")
        or document.get("month_result_sha256") != descriptor.get("month_result_sha256")
    ):
        raise V6Error(f"prior validation month identity differs: {symbol}/{descriptor.get('placement')}")
    return document


def _validation_generation(receipt: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {key: value for key, value in receipt.items() if key != "validation_generation_sha256"}
    )


def _validate_validation_output(
    symbol: str,
    receipt: dict[str, Any],
    report: dict[str, Any],
    output_path: Path,
    *,
    require_passed: bool = True,
    authenticate_months: bool = False,
) -> None:
    output = receipt.get("output")
    months = receipt.get("months")
    placements = (
        [entry.get("placement") for entry in months if isinstance(entry, dict)]
        if isinstance(months, list)
        else []
    )
    malformed = (
        receipt.get("schema_version") != VALIDATION_VERSION
        or receipt.get("boundary") != VALIDATION_BOUNDARY
        or receipt.get("symbol") != symbol
        or receipt.get("validation_pipeline_version") != PIPELINE_VERSION
        or receipt.get("calendar_sha256") != us_market_hours_sha256()
        or report.get("schema_version") != VALIDATION_VERSION
        or report.get("boundary") != VALIDATION_BOUNDARY
        or report.get("strategy") != VALIDATION_STRATEGY
        or report.get("symbol") != symbol
        or report.get("validation_pipeline_version") != PIPELINE_VERSION
        or report.get("calendar_sha256") != us_market_hours_sha256()
        or report.get("source_generation_sha256") != receipt.get("source_generation_sha256")
        or report.get("request") != receipt.get("request")
        or report.get("month_count") != len(placements)
        or receipt.get("month_count") != len(placements)
        or len(placements) != len(set(placements))
        or not isinstance(report.get("failed_placements"), list)
        or not isinstance(output, dict)
        or output.get("file") != VALIDATION_OUTPUT
        or not output_path.is_file()
        or output_path.is_symlink()
        or output.get("bytes") != output_path.stat().st_size
        or output.get("sha256") != sha256_file(output_path)
        or receipt.get("validation_generation_sha256") != _validation_generation(receipt)
        or receipt.get("passed") is not report.get("passed")
    )
    if malformed:
        raise V6Error(f"v6 validation output authentication failed: {symbol}")
    if authenticate_months:
        directory = output_path.parent
        for descriptor in months:
            relative = _month_relative_path(descriptor["placement"])
            month_path = directory / relative
            month = read_json_object(month_path, "v6 monthly validation output")
            if descriptor.get("output", {}).get("file") != relative.as_posix():
                raise V6Error(f"v6 monthly validation output path differs: {symbol}/{descriptor['placement']}")
            _validate_month_output(symbol, descriptor, month, month_path)
    if require_passed and (receipt.get("passed") is not True or report.get("passed") is not True):
        raise V6Error(f"v6 pricing validation failed: {symbol}")


def publish_validation(
    store_root: Path | str,
    symbol: str,
    request_end_timestamp: str,
    *,
    source: AuthenticatedMonthlySource | None = None,
) -> dict[str, Any]:
    """Validate changed source months and publish the compact ticker receipt last."""
    validate_boundary_change_log("validation")
    root = Path(store_root)
    source = load_monthly_source(root, symbol) if source is None else source
    if source.symbol != symbol:
        raise V6Error(f"validation source belongs to another symbol: {symbol}")

    directory = safe_child(root / "validation", symbol, "symbol")
    directory.mkdir(parents=True, exist_ok=True)
    prior = _prior_receipt(root, symbol)
    prior_by_placement = {
        entry["placement"]: entry
        for entry in (prior.get("months", []) if prior is not None else [])
        if isinstance(entry, dict) and isinstance(entry.get("placement"), str)
    }

    descriptors = []
    rebuilt_months: dict[str, dict[str, Any]] = {}
    previous_descriptor: dict[str, Any] | None = None
    previous_placement: str | None = None
    durability = durability_batch() if prior is None else nullcontext()
    with durability:
        for placement, parts, source_receipt in _source_months(source):
            predecessor_digest = (
                previous_descriptor["month_result_sha256"] if previous_descriptor is not None else None
            )
            old_descriptor = prior_by_placement.get(placement)
            if (
                old_descriptor is not None
                and old_descriptor.get("source_month_receipt_sha256") == source_receipt
                and old_descriptor.get("predecessor_month_result_sha256") == predecessor_digest
            ):
                descriptor = old_descriptor
            else:
                if (
                    old_descriptor is not None
                    and old_descriptor.get("source_month_receipt_sha256") == source_receipt
                ):
                    trusted = _trusted_month_document(directory, old_descriptor, symbol)
                    month = trusted.get("month")
                    month_result_sha256 = trusted.get("month_result_sha256")
                    if not isinstance(month, dict) or not _is_sha256(month_result_sha256):
                        raise V6Error(f"prior validation month result is malformed: {symbol}/{placement}")
                else:
                    month, month_result_sha256 = _month_result(
                        source,
                        placement,
                        parts,
                        source_receipt,
                    )

                predecessor = None
                seam = None
                if previous_descriptor is not None and previous_placement is not None:
                    previous_month = rebuilt_months.get(previous_placement)
                    if previous_month is None:
                        previous_document = _trusted_month_document(
                            directory,
                            previous_descriptor,
                            symbol,
                        )
                        previous_month = previous_document.get("month")
                    if not isinstance(previous_month, dict):
                        raise V6Error(
                            f"prior validation month result is malformed: {symbol}/{previous_placement}"
                        )
                    predecessor = {
                        "placement": previous_placement,
                        "source_month_receipt_sha256": previous_descriptor["source_month_receipt_sha256"],
                        "month_result_sha256": predecessor_digest,
                    }
                    seam = _predecessor_seam(
                        previous_placement,
                        previous_month,
                        placement,
                        month,
                    )
                document = _month_document(
                    symbol=symbol,
                    placement=placement,
                    source_month_receipt_sha256=source_receipt,
                    month=month,
                    month_result_sha256=month_result_sha256,
                    predecessor=predecessor,
                    predecessor_seam=seam,
                )
                relative = _month_relative_path(placement)
                month_path = directory / relative
                replace_json(month_path, document)
                descriptor = _month_descriptor(document, relative, month_path)
                _validate_month_output(symbol, descriptor, document, month_path)
                rebuilt_months[placement] = month

            descriptors.append(descriptor)
            previous_descriptor = descriptor
            previous_placement = placement

    request = request_identity(symbol, ALPACA_ADJUSTMENT_MODES[0], request_end_timestamp)
    request.pop("adjustment")
    failed_placements = [
        descriptor["placement"] for descriptor in descriptors if descriptor.get("passed") is not True
    ]
    passed = bool(descriptors) and not failed_placements
    report = {
        "schema_version": VALIDATION_VERSION,
        "boundary": VALIDATION_BOUNDARY,
        "strategy": VALIDATION_STRATEGY,
        "validation_pipeline_version": PIPELINE_VERSION,
        "symbol": symbol,
        "source_generation_sha256": source.source_generation_sha256,
        "request": request,
        "calendar_sha256": us_market_hours_sha256(),
        "month_count": len(descriptors),
        "failed_placements": failed_placements,
        "passed": passed,
    }
    output_path = directory / VALIDATION_OUTPUT
    replace_json(output_path, report)
    receipt = {
        "schema_version": VALIDATION_VERSION,
        "boundary": VALIDATION_BOUNDARY,
        "symbol": symbol,
        "source_generation_sha256": source.source_generation_sha256,
        "validation_pipeline_version": PIPELINE_VERSION,
        "calendar_sha256": us_market_hours_sha256(),
        "request": request,
        "month_count": len(descriptors),
        "months": descriptors,
        "output": {
            "file": VALIDATION_OUTPUT,
            "sha256": sha256_file(output_path),
            "bytes": output_path.stat().st_size,
        },
        "passed": passed,
    }
    receipt["validation_generation_sha256"] = _validation_generation(receipt)
    published_report = read_json_object(output_path, "published validation output")
    _validate_validation_output(
        symbol,
        receipt,
        published_report,
        output_path,
        require_passed=False,
        authenticate_months=False,
    )
    if published_report != report:
        raise V6Error(f"published validation output differs: {symbol}")

    manifest_path = directory / VALIDATION_MANIFEST
    replace_json(manifest_path, receipt)
    if read_json_object(manifest_path, "published validation receipt") != receipt:
        raise V6Error(f"published validation receipt differs: {symbol}")
    return receipt


def load_validation(
    store_root: Path | str,
    symbol: str,
    *,
    require_passed: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Consume the published summary without re-authenticating the boundary."""
    directory = validation_directory(store_root, symbol)
    receipt = read_json_object(directory / VALIDATION_MANIFEST, "v6 validation receipt")
    report = read_json_object(directory / VALIDATION_OUTPUT, "v6 validation output")
    if receipt.get("symbol") != symbol or report.get("symbol") != symbol:
        raise V6Error(f"v6 validation belongs to another symbol: {symbol}")
    if require_passed and (receipt.get("passed") is not True or report.get("passed") is not True):
        raise V6Error(f"v6 pricing validation failed: {symbol}")
    return receipt, report


def authenticate_validation(
    store_root: Path | str,
    symbol: str,
    *,
    require_passed: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Explicitly authenticate the complete current validation boundary."""
    validate_boundary_change_log("validation")
    directory = validation_directory(store_root, symbol)
    receipt = read_json_object(directory / VALIDATION_MANIFEST, "v6 validation receipt")
    output_path = directory / VALIDATION_OUTPUT
    report = read_json_object(output_path, "v6 validation output")
    _validate_validation_output(
        symbol,
        receipt,
        report,
        output_path,
        require_passed=require_passed,
        authenticate_months=True,
    )
    return receipt, report


__all__ = [
    "MONTH_VALIDATION_BOUNDARY",
    "VALIDATION_BOUNDARY",
    "authenticate_validation",
    "load_validation",
    "publish_validation",
    "request_identity",
]

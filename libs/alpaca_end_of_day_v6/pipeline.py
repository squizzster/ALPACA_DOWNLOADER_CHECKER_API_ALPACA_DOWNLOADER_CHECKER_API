"""The single native v6 acquisition-to-catalogue pipeline."""

from __future__ import annotations

import os
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from libs.alpaca_end_of_day_ticker_pipeline.orchestration import (
    resolve_authoritative_alpaca_symbol,
)
from libs.alpaca_security_lineage import AlpacaIdentityRequestRateLimiter
from libs.download_alpaca_end_of_day_bars import (
    ALPACA_ADJUSTMENT_MODES,
    AlpacaRequestRateLimiter,
    request_alpaca_daily_bars,
    request_alpaca_daily_bars_batch,
)

from .catalogue import (
    build_affected_identity_candidate,
    build_identity_candidate_from_boundaries,
    current_source_closure,
    evaluate_availability,
    load_identity_candidate,
    load_reusable_publication,
    publish_availability,
    publish_catalogue,
    publish_identity_candidate,
)
from .common import PUBLICATION_PENDING, publication_transaction, replace_json
from .correlation import (
    build_correlation_report,
    load_correlation,
    load_correlation_receipt,
    publish_correlation,
)
from .downstream_input import build_downstream_pricing_input
from .errors import V6Error
from .lineage import acquire_and_publish_lineage, authenticate_lineage, load_lineage
from .paths import current_store_root
from .source import (
    SOURCE_MANIFEST,
    authenticate_monthly_source,
    initialize_v6_store,
    load_monthly_source,
    publish_monthly_source,
    publish_monthly_source_update,
)
from .validation import authenticate_validation, load_validation, publish_validation
from .warnings import build_warning_report, load_warning_receipt, publish_warnings

EVIDENCE_COHORT = (
    "GOOG",
    "ONMD",
    "DKDCA",
    "META",
    "ACY",
    "MTMT",
    "MPU",
    "BFA",
    "SNTI",
    "DYNS",
)
PARITY_REQUEST_END = "2026-10-12T00:00:00Z"
LAST_RUN_RECORD = "last_run.json"


def _request_source_views(
    symbol: str,
    api_key_id: str,
    api_secret_key: str,
    request_end_timestamp: str,
    bar_rate_limiter: AlpacaRequestRateLimiter,
    *,
    request_start_timestamp: str | None = None,
) -> OrderedDict[str, list[dict[str, Any]]]:
    """Acquire independent adjustment views concurrently and return contract order."""

    def request(adjustment: str) -> list[dict[str, Any]]:
        optional_start = (
            {"request_start_timestamp": request_start_timestamp} if request_start_timestamp is not None else {}
        )
        return request_alpaca_daily_bars(
            symbol,
            adjustment,
            api_key_id,
            api_secret_key,
            request_end_timestamp,
            bar_rate_limiter,
            **optional_start,
        )

    with ThreadPoolExecutor(max_workers=len(ALPACA_ADJUSTMENT_MODES)) as executor:
        requests = {adjustment: executor.submit(request, adjustment) for adjustment in ALPACA_ADJUSTMENT_MODES}
        return OrderedDict((adjustment, requests[adjustment].result()) for adjustment in ALPACA_ADJUSTMENT_MODES)


def _request_source_view_batch(
    symbols: Sequence[str],
    api_key_id: str,
    api_secret_key: str,
    request_end_timestamp: str,
    bar_rate_limiter: AlpacaRequestRateLimiter,
) -> dict[str, OrderedDict[str, list[dict[str, Any]]]]:
    """Acquire eight ordered adjustment views for a bounded symbol batch."""
    requested = tuple(symbols)
    with ThreadPoolExecutor(max_workers=len(ALPACA_ADJUSTMENT_MODES)) as executor:
        requests = {
            adjustment: executor.submit(
                request_alpaca_daily_bars_batch,
                requested,
                adjustment,
                api_key_id,
                api_secret_key,
                request_end_timestamp,
                bar_rate_limiter,
            )
            for adjustment in ALPACA_ADJUSTMENT_MODES
        }
        by_adjustment = {adjustment: requests[adjustment].result() for adjustment in ALPACA_ADJUSTMENT_MODES}
    return {
        symbol: OrderedDict(
            (adjustment, by_adjustment[adjustment][symbol]) for adjustment in ALPACA_ADJUSTMENT_MODES
        )
        for symbol in requested
    }


def _credentials(environment: Mapping[str, str]) -> tuple[str, str]:
    key = environment.get("ALPACA_API_KEY_ID")
    secret = environment.get("ALPACA_API_SECRET_KEY")
    if not key or not secret:
        raise V6Error("set ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY")
    return key, secret


def _request_end(value: str | None) -> str:
    if value is None:
        from libs.alpaca_pricing_download.download import request_end

        return request_end(None).strftime("%Y-%m-%dT00:00:00Z")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise V6Error(f"invalid request end: {value!r}") from error
    if parsed.tzinfo is None or not value.endswith("T00:00:00Z"):
        raise V6Error("request end must be an exclusive UTC midnight")
    return value


def _request_start(value: str | None, request_end_timestamp: str) -> str:
    if value is None:
        from datetime import timedelta

        last_day = datetime.fromisoformat(request_end_timestamp.replace("Z", "+00:00")) - timedelta(days=1)
        return last_day.strftime("%Y-%m-01T00:00:00Z")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        end = datetime.fromisoformat(request_end_timestamp.replace("Z", "+00:00"))
    except ValueError as error:
        raise V6Error(f"invalid request start: {value!r}") from error
    if parsed.tzinfo is None or not value.endswith("-01T00:00:00Z") or parsed >= end:
        raise V6Error("request start must be a month-opening UTC midnight before request end")
    return value


def _load_reusable_boundaries(
    store: Path,
    processing_symbols: Sequence[str],
    pricing_inputs: Mapping[str, Any],
    lineages: Mapping[str, tuple[dict[str, Any], dict[str, Any]]],
) -> dict[str, Any] | None:
    """Select a complete current publication without auditing prior outputs."""
    correlation_receipts = {}
    warning_receipts = {}
    try:
        for symbol in processing_symbols:
            pricing = pricing_inputs[symbol]
            lineage_receipt, _lineage_report = lineages[symbol]
            correlation = load_correlation_receipt(store, symbol)
            if (
                correlation.get("source_generation_sha256") != pricing.source.source_generation_sha256
                or correlation.get("validation_generation_sha256")
                != pricing.validation_receipt["validation_generation_sha256"]
                or correlation.get("lineage_generation_sha256") != lineage_receipt["lineage_generation_sha256"]
            ):
                return None
            correlation_receipts[symbol] = correlation

            warning = load_warning_receipt(store, symbol)
            if (
                warning.get("source_generation_sha256") != pricing.source.source_generation_sha256
                or warning.get("validation_generation_sha256")
                != pricing.validation_receipt["validation_generation_sha256"]
                or warning.get("correlation_generation_sha256") != correlation["correlation_generation_sha256"]
            ):
                return None
            warning_receipts[symbol] = warning

        publication = load_reusable_publication(
            store,
            {
                symbol: receipt["correlation_generation_sha256"]
                for symbol, receipt in correlation_receipts.items()
            },
        )
    except V6Error:
        return None
    if publication is None:
        return None
    return {
        "correlation": correlation_receipts,
        "warnings": warning_receipts,
        **publication,
    }


def _last_run_document(result: Mapping[str, Any]) -> dict[str, Any]:
    source_results = result["source"]
    changed_months = sum(len(source_result["changed_placements"]) for source_result in source_results.values())
    return {
        "kind": "alpaca_v6_last_completed_run",
        "completed_at": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "request": {
            "from": result["request_start"],
            "to": result["request_end"],
            "submitted_symbols": result["submitted_symbols"],
            "canonical_symbols": result["canonical_symbols"],
        },
        "stats": {
            "processing_symbol_count": len(result["processing_symbols"]),
            "changed_month_count": changed_months,
            "inventory_changed_symbol_count": sum(
                bool(source_result["inventory_changed"]) for source_result in source_results.values()
            ),
            "downstream_publication_reused": result["publication_reused"],
        },
    }


def run_v6_pipeline(
    symbols: Sequence[str],
    *,
    repository_root: Path | str | None = None,
    store_root: Path | str | None = None,
    environment: Mapping[str, str] | None = None,
    request_start_timestamp: str | None = None,
    request_end_timestamp: str | None = None,
    bar_rate_limiter: AlpacaRequestRateLimiter | None = None,
    identity_rate_limiter: AlpacaIdentityRequestRateLimiter | None = None,
    symbol_resolutions: Sequence[Mapping[str, Any]] | None = None,
    source_views_by_symbol: Mapping[str, Mapping[str, Sequence[object]]] | None = None,
    accept_failed_validation: bool = False,
    reuse_acquired_boundaries: bool = False,
    output=None,
) -> dict[str, Any]:
    """Acquire a coherent cohort and publish the reader-visible catalogue last."""
    if store_root is not None and repository_root is not None:
        raise V6Error("choose either a repository root or an explicit v6 store root")
    repository = Path.cwd() if repository_root is None else Path(repository_root)
    store = Path(store_root) if store_root is not None else current_store_root(repository)
    environment = os.environ if environment is None else environment
    api_key_id, api_secret_key = _credentials(environment)
    request_end_timestamp = _request_end(request_end_timestamp)
    request_start_timestamp = _request_start(request_start_timestamp, request_end_timestamp)
    submitted = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
    if not submitted:
        raise V6Error("v6 pipeline requires at least one symbol")

    bar_rate_limiter = bar_rate_limiter or AlpacaRequestRateLimiter()
    identity_rate_limiter = identity_rate_limiter or AlpacaIdentityRequestRateLimiter()
    resolutions: list[dict[str, Any]] = []
    canonical_symbols = []
    if symbol_resolutions is not None and len(symbol_resolutions) != len(submitted):
        raise V6Error("supplied symbol-resolution count differs from submitted symbols")
    for position, symbol in enumerate(submitted):
        resolution = (
            dict(symbol_resolutions[position])
            if symbol_resolutions is not None
            else resolve_authoritative_alpaca_symbol(
                symbol,
                api_key_id,
                api_secret_key,
                request_rate_limiter=identity_rate_limiter,
            )
        )
        if resolution.get("submitted_symbol") != symbol:
            raise V6Error(f"supplied symbol resolution belongs to another label: {symbol}")
        canonical = resolution.get("authoritative_symbol")
        if not isinstance(canonical, str) or not canonical:
            raise V6Error(f"Alpaca returned no authoritative symbol for {symbol}")
        resolutions.append(resolution)
        canonical_symbols.append(canonical)
    canonical_symbols = list(dict.fromkeys(canonical_symbols))
    initialize_v6_store(store)
    recovery_required = (store / PUBLICATION_PENDING).exists()

    publication_marker = {
        "boundary": "alpaca_v6_publication_transaction",
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "request_end": request_end_timestamp,
        "request_start": request_start_timestamp,
        "submitted_symbols": list(submitted),
        "canonical_symbols": canonical_symbols,
        "pid": os.getpid(),
    }

    from .update_contract import validate_update_request

    with publication_transaction(
        store, publication_marker, before_publish=lambda: validate_update_request(store, publication_marker)
    ):
        result = _publish_v6_boundaries(
            store=store,
            submitted=submitted,
            canonical_symbols=canonical_symbols,
            resolutions=resolutions,
            request_start_timestamp=request_start_timestamp,
            request_end_timestamp=request_end_timestamp,
            api_key_id=api_key_id,
            api_secret_key=api_secret_key,
            bar_rate_limiter=bar_rate_limiter,
            identity_rate_limiter=identity_rate_limiter,
            source_views_by_symbol=source_views_by_symbol,
            allow_reuse=not recovery_required or reuse_acquired_boundaries,
            accept_failed_validation=accept_failed_validation,
            reuse_acquired_boundaries=reuse_acquired_boundaries,
            output=output,
        )
        try:
            replace_json(store / LAST_RUN_RECORD, _last_run_document(result))
        except (OSError, V6Error) as error:
            result["last_run_recorded"] = False
            if output is not None:
                print(f"last-run record was not written: {error}", file=output, flush=True)
        else:
            result["last_run_recorded"] = True
        return result


def _publish_v6_boundaries(
    *,
    store: Path,
    submitted: tuple[str, ...],
    canonical_symbols: list[str],
    resolutions: list[dict[str, Any]],
    request_start_timestamp: str,
    request_end_timestamp: str,
    api_key_id: str,
    api_secret_key: str,
    bar_rate_limiter: AlpacaRequestRateLimiter,
    identity_rate_limiter: AlpacaIdentityRequestRateLimiter,
    source_views_by_symbol: Mapping[str, Mapping[str, Sequence[object]]] | None,
    allow_reuse: bool,
    accept_failed_validation: bool,
    reuse_acquired_boundaries: bool,
    output,
) -> dict[str, Any]:
    """Publish every boundary while the store-wide transaction lock is held."""
    has_catalogue = (store / "catalogue" / "catalogue.manifest.json").is_file()
    source_views_by_symbol = {} if source_views_by_symbol is None else source_views_by_symbol
    unexpected_source_views = set(source_views_by_symbol) - set(canonical_symbols)
    if unexpected_source_views:
        raise V6Error(
            f"supplied source views belong to unrequested symbols: {sorted(unexpected_source_views)!r}"
        )
    processing_symbols = (
        list(current_source_closure(store, canonical_symbols)) if has_catalogue else list(canonical_symbols)
    )
    source_results = {}
    authenticated_sources = {}
    for symbol in canonical_symbols:
        existing_source = (store / "end_of_day" / symbol / SOURCE_MANIFEST).is_file()
        if reuse_acquired_boundaries and existing_source:
            authenticated_source = authenticate_monthly_source(store, symbol)
            authenticated_sources[symbol] = authenticated_source
            source_results[symbol] = {
                "symbol": symbol,
                "source_generation_sha256": authenticated_source.source_generation_sha256,
                "request_end": request_end_timestamp,
                "changed_placements": [],
                "inventory_changed": False,
                "rows": {
                    adjustment.adjustment: len(adjustment.rows)
                    for adjustment in authenticated_source.adjustments
                },
                "reused_authenticated_boundary": True,
            }
            if output is not None:
                print(
                    f"{symbol}: authenticated pricing source retained "
                    f"{authenticated_source.source_generation_sha256}",
                    file=output,
                    flush=True,
                )
            continue
        supplied_views = source_views_by_symbol.get(symbol)
        if supplied_views is None:
            views = _request_source_views(
                symbol,
                api_key_id,
                api_secret_key,
                request_end_timestamp,
                bar_rate_limiter,
                **({"request_start_timestamp": request_start_timestamp} if existing_source else {}),
            )
        else:
            if tuple(supplied_views) != ALPACA_ADJUSTMENT_MODES:
                raise V6Error(f"supplied source views are not in canonical adjustment order: {symbol}")
            views = supplied_views
        source_results[symbol] = (
            publish_monthly_source_update(
                store,
                symbol,
                views,
                request_start_timestamp,
                request_end_timestamp,
            )
            if existing_source
            else publish_monthly_source(
                store,
                symbol,
                views,
                request_end_timestamp,
            )
        )
        if output is not None:
            print(
                f"{symbol}: pricing source {source_results[symbol]['source_generation_sha256']}",
                file=output,
                flush=True,
            )

    monthly_sources = {
        symbol: authenticated_sources.get(symbol) or load_monthly_source(store, symbol)
        for symbol in processing_symbols
    }
    validation_receipts = {}
    validations = {}
    for symbol in processing_symbols:
        if symbol in canonical_symbols:
            if symbol in authenticated_sources:
                validation_manifest = store / "validation" / symbol / "validation.manifest.json"
                validation_output = store / "validation" / symbol / "validation.json"
                if validation_manifest.is_file() and validation_output.is_file():
                    validation_receipts[symbol], validation_report = authenticate_validation(
                        store,
                        symbol,
                        require_passed=False,
                    )
                else:
                    validation_receipts[symbol] = publish_validation(
                        store,
                        symbol,
                        request_end_timestamp,
                        source=monthly_sources[symbol],
                    )
                    validation_report = load_validation(
                        store,
                        symbol,
                        require_passed=False,
                    )[1]
                if (
                    validation_receipts[symbol].get("source_generation_sha256")
                    != monthly_sources[symbol].source_generation_sha256
                    or validation_receipts[symbol].get("request", {}).get("request_to") != request_end_timestamp
                ):
                    raise V6Error(f"reusable validation boundary differs from this request: {symbol}")
                validations[symbol] = (validation_receipts[symbol], validation_report)
            else:
                validation_receipts[symbol] = publish_validation(
                    store,
                    symbol,
                    request_end_timestamp,
                    source=monthly_sources[symbol],
                )
        if symbol not in validations:
            validations[symbol] = load_validation(store, symbol, require_passed=False)
    failed_validation = [symbol for symbol, (receipt, _report) in validations.items() if not receipt["passed"]]
    if failed_validation:
        if accept_failed_validation and set(failed_validation).issubset(canonical_symbols):
            return {
                "request_end": request_end_timestamp,
                "request_start": request_start_timestamp,
                "submitted_symbols": list(submitted),
                "canonical_symbols": canonical_symbols,
                "processing_symbols": processing_symbols,
                "symbol_resolutions": resolutions,
                "source": source_results,
                "validation": validation_receipts,
                "lineage": {},
                "correlation": {},
                "warnings": {},
                "identity_candidate": None,
                "availability": None,
                "catalogue": None,
                "publication_reused": False,
                "admitted": False,
                "rejection": {
                    "reason": "pricing_validation_failed",
                    "symbols": failed_validation,
                },
            }
        raise V6Error(f"v6 pricing validation failed for: {', '.join(failed_validation)}")
    pricing_inputs = {
        symbol: build_downstream_pricing_input(
            store,
            symbol,
            source=monthly_sources[symbol],
            validation=validations[symbol],
        )
        for symbol in processing_symbols
    }

    resolution_by_canonical = {resolution["authoritative_symbol"]: resolution for resolution in resolutions}
    lineage_receipts = {}
    lineages = {}
    for symbol in processing_symbols:
        if symbol in canonical_symbols:
            lineage_manifest = store / "lineage" / symbol / "lineage.manifest.json"
            lineage_output = store / "lineage" / symbol / "lineage.json"
            if reuse_acquired_boundaries and lineage_manifest.is_file() and lineage_output.is_file():
                lineage_receipt, lineage_report = authenticate_lineage(store, symbol)
                if (
                    lineage_receipt.get("source_generation_sha256")
                    != pricing_inputs[symbol].source.source_generation_sha256
                    or lineage_receipt.get("validation_generation_sha256")
                    != pricing_inputs[symbol].validation_receipt["validation_generation_sha256"]
                ):
                    raise V6Error(f"reusable lineage boundary differs from pricing inputs: {symbol}")
                lineage_receipts[symbol] = lineage_receipt
                lineages[symbol] = (lineage_receipt, lineage_report)
            else:
                lineage_receipts[symbol] = acquire_and_publish_lineage(
                    store,
                    symbol,
                    request_end_timestamp,
                    api_key_id,
                    api_secret_key,
                    rate_limiter=identity_rate_limiter,
                    symbol_resolution=resolution_by_canonical[symbol],
                    pricing_input=pricing_inputs[symbol],
                )
            if output is not None:
                print(
                    f"{symbol}: lineage source {lineage_receipts[symbol]['lineage_generation_sha256']}",
                    file=output,
                    flush=True,
                )
        if symbol not in lineages:
            lineages[symbol] = load_lineage(store, symbol)

    reusable = (
        _load_reusable_boundaries(
            store,
            processing_symbols,
            pricing_inputs,
            lineages,
        )
        if allow_reuse and has_catalogue
        else None
    )
    if reusable is not None:
        if output is not None:
            print("current downstream publication retained", file=output, flush=True)
        return {
            "request_end": request_end_timestamp,
            "request_start": request_start_timestamp,
            "submitted_symbols": list(submitted),
            "canonical_symbols": canonical_symbols,
            "processing_symbols": processing_symbols,
            "symbol_resolutions": resolutions,
            "source": source_results,
            "validation": validation_receipts,
            "lineage": lineage_receipts,
            "correlation": reusable["correlation"],
            "warnings": reusable["warnings"],
            "identity_candidate": reusable["identity_candidate"],
            "availability": reusable["availability"],
            "catalogue": reusable["catalogue"],
            "publication_reused": True,
            "admitted": True,
            "rejection": None,
        }

    first_pass = {
        symbol: build_correlation_report(
            store,
            symbol,
            pricing_input=pricing_inputs[symbol],
            pricing_inputs_by_symbol=pricing_inputs,
            lineage=lineages[symbol],
        )
        for symbol in processing_symbols
    }
    second_pass = {
        symbol: build_correlation_report(
            store,
            symbol,
            preliminary_by_symbol=first_pass,
            pricing_input=pricing_inputs[symbol],
            pricing_inputs_by_symbol=pricing_inputs,
            lineage=lineages[symbol],
        )
        for symbol in processing_symbols
    }
    final_reports = {
        symbol: build_correlation_report(
            store,
            symbol,
            preliminary_by_symbol=second_pass,
            pricing_input=pricing_inputs[symbol],
            pricing_inputs_by_symbol=pricing_inputs,
            lineage=lineages[symbol],
        )
        for symbol in processing_symbols
    }
    for symbol in processing_symbols:
        if second_pass[symbol] != final_reports[symbol]:
            raise V6Error(f"v6 cross-symbol correlation did not reach a fixed point: {symbol}")
    correlation_receipts = {
        symbol: publish_correlation(store, final_reports[symbol]) for symbol in processing_symbols
    }
    correlations = {symbol: load_correlation(store, symbol) for symbol in processing_symbols}

    warning_receipts = {}
    for symbol in processing_symbols:
        warning_receipts[symbol] = publish_warnings(
            store,
            build_warning_report(
                store,
                symbol,
                pricing_input=pricing_inputs[symbol],
                correlation=correlations[symbol],
            ),
        )

    candidate = (
        build_affected_identity_candidate(store, correlations, lineages)
        if has_catalogue
        else build_identity_candidate_from_boundaries(correlations, lineages)
    )
    candidate_receipt = publish_identity_candidate(store, candidate)
    candidate = load_identity_candidate(store)
    availability_correlations = dict(correlations)
    availability_lineages = dict(lineages)
    for symbol in candidate.supporting_symbols:
        availability_correlations.setdefault(symbol, load_correlation(store, symbol))
        availability_lineages.setdefault(symbol, load_lineage(store, symbol))
    availability_report = evaluate_availability(
        store,
        candidate,
        availability_correlations,
        availability_lineages,
        pricing_inputs_by_symbol=pricing_inputs,
    )
    availability_receipt = publish_availability(store, availability_report, candidate)
    catalogue_receipt = publish_catalogue(store, candidate)
    return {
        "request_end": request_end_timestamp,
        "request_start": request_start_timestamp,
        "submitted_symbols": list(submitted),
        "canonical_symbols": canonical_symbols,
        "processing_symbols": processing_symbols,
        "symbol_resolutions": resolutions,
        "source": source_results,
        "validation": validation_receipts,
        "lineage": lineage_receipts,
        "correlation": correlation_receipts,
        "warnings": warning_receipts,
        "identity_candidate": candidate_receipt,
        "availability": availability_receipt,
        "catalogue": catalogue_receipt,
        "publication_reused": False,
        "admitted": True,
        "rejection": None,
    }


__all__ = ["EVIDENCE_COHORT", "PARITY_REQUEST_END", "run_v6_pipeline"]

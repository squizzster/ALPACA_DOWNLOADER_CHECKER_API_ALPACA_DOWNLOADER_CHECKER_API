"""Private identity candidate, availability, and final v6 catalogue publication."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from libs.alpaca_end_of_day_reader.errors import EndOfDayReaderError
from libs.alpaca_end_of_day_security_correlation import (
    LINEAGE_GAP_AVAILABILITY_STATUS,
    OFFLINE_AVAILABILITY_STATUS,
    ONLINE_AVAILABILITY_STATUS,
    AuthenticatedSecurityMaster,
    SecurityCorrelationError,
    authenticate_security_master,
    is_master_id,
)
from libs.alpaca_end_of_day_security_correlation.artifact_paths import (
    security_master_directory,
    security_master_manifest_path,
    security_master_path,
)
from libs.alpaca_end_of_day_security_correlation.security_master import (
    SECURITY_MASTER_VERSION,
    _aggregate_security_entries,
    _publish_security_master_root,
    _publish_security_master_snapshot,
    _publish_security_object,
    _publish_source_object,
    _replace_inventory_entries,
    _source_contribution,
)
from libs.alpaca_end_of_day_ticker_pipeline.online_status import (
    evaluate_security_online_status_view,
)

from .common import (
    assert_publication_complete,
    canonical_sha256,
    publication_lock,
    read_json_object,
    replace_json,
    sha256_file,
)
from .errors import V6Error
from .paths import catalogue_directory, current_store_root, legacy_engine_root
from .reader import PublishedHistoryInputs, read_history_from_master
from .source import read_monthly_adjustment_rows
from .versions import (
    AVAILABILITY_VERSION,
    CATALOGUE_PUBLICATION_VERSION,
    IDENTITY_CANDIDATE_VERSION,
    validate_boundary_change_log,
)
from .warnings import load_warnings

IDENTITY_CANDIDATE_BOUNDARY = "alpaca_v6_identity_candidate"
IDENTITY_CANDIDATE_OUTPUT = "identity_candidate.json"
IDENTITY_CANDIDATE_MANIFEST = "identity_candidate.manifest.json"
AVAILABILITY_BOUNDARY = "alpaca_v6_availability"
AVAILABILITY_OUTPUT = "availability.json"
AVAILABILITY_MANIFEST = "availability.manifest.json"
CATALOGUE_BOUNDARY = "alpaca_v6_public_catalogue"
CATALOGUE_MANIFEST = "catalogue.manifest.json"
RETENTION_STATE = "retention.json"
NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class IdentityCandidate:
    generation_sha256: str
    correlation_set_sha256: str
    master: AuthenticatedSecurityMaster
    sources: tuple[dict[str, Any], ...]
    mode: str
    base_catalogue_generation_sha256: str | None
    correlation_generations: dict[str, str]
    replacement_symbols: tuple[str, ...]
    supporting_symbols: tuple[str, ...]
    retired_identity_ids: tuple[str, ...]


def _candidate_source(
    report: dict[str, Any],
    receipt: dict[str, Any],
) -> dict[str, Any]:
    # Match the established v1 candidate view: None asks the security-master
    # builder to provision provisional offline state for exactly the identities
    # contributed by this correlation report.  Availability replaces that
    # provisional state after exercising the authenticated reader.
    return _source_contribution(report, receipt, None)


def _offline_candidate_state(
    sources: Sequence[dict[str, Any]],
) -> tuple[tuple[dict[str, Any], ...], list[dict[str, Any]]]:
    ordered = sorted((deepcopy(source) for source in sources), key=lambda item: item["symbol"])
    for source in ordered:
        for identity in source["identities"]:
            identity["online"] = False
            identity["availability_status"] = OFFLINE_AVAILABILITY_STATUS
    identities = _aggregate_security_entries(ordered)
    return tuple(ordered), identities


def _candidate_content(
    *,
    mode: str,
    base_catalogue_generation_sha256: str | None,
    correlation_generations: Mapping[str, str],
    replacement_symbols: Sequence[str],
    supporting_symbols: Sequence[str],
    retired_identity_ids: Sequence[str],
    sources: Sequence[dict[str, Any]],
    identities: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "mode": mode,
        "base_catalogue_generation_sha256": base_catalogue_generation_sha256,
        "correlation_generations": {
            symbol: correlation_generations[symbol] for symbol in sorted(correlation_generations)
        },
        "replacement_symbols": list(replacement_symbols),
        "supporting_symbols": list(supporting_symbols),
        "retired_identity_ids": list(retired_identity_ids),
        "sources": [deepcopy(source) for source in sources],
        "identities": [deepcopy(identity) for identity in identities],
    }


def _candidate_generation(content: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {
            "schema_version": IDENTITY_CANDIDATE_VERSION,
            "boundary": IDENTITY_CANDIDATE_BOUNDARY,
            **content,
        }
    )


def _candidate_master(
    generation: str,
    sources: Sequence[dict[str, Any]],
    identities: Sequence[dict[str, Any]],
) -> AuthenticatedSecurityMaster:
    master_document = {
        "manifest_type": "v6_private_identity_candidate",
        "security_master_version": SECURITY_MASTER_VERSION,
        "security_master_generation_sha256": generation,
        "created_at": None,
        "source_count": len(sources),
        "master_id_count": len(identities),
    }
    return AuthenticatedSecurityMaster(
        receipt={
            "security_master_version": SECURITY_MASTER_VERSION,
            "security_master_generation_sha256": generation,
            "created_at": None,
            "source_count": len(sources),
            "master_id_count": len(identities),
        },
        master=master_document,
        master_sha256=generation,
        sources_by_symbol={source["symbol"]: source for source in sources},
        identities_by_master_id={identity["master_id"]: identity for identity in identities},
    )


def _identity_candidate_from_sources(
    sources: Sequence[dict[str, Any]],
    *,
    mode: str = "snapshot",
    base_catalogue_generation_sha256: str | None = None,
    replacement_symbols: Sequence[str] | None = None,
    supporting_symbols: Sequence[str] = (),
    retired_identity_ids: Sequence[str] = (),
) -> IdentityCandidate:
    ordered_sources, identities = _offline_candidate_state(sources)
    source_symbols = tuple(source["symbol"] for source in ordered_sources)
    replacements = source_symbols if replacement_symbols is None else tuple(sorted(replacement_symbols))
    supporting = tuple(sorted(supporting_symbols))
    retired = tuple(sorted(retired_identity_ids))
    correlation_generations = {
        source["symbol"]: source["correlation_generation_sha256"] for source in ordered_sources
    }
    correlation_set = canonical_sha256(correlation_generations)
    content = _candidate_content(
        mode=mode,
        base_catalogue_generation_sha256=base_catalogue_generation_sha256,
        correlation_generations=correlation_generations,
        replacement_symbols=replacements,
        supporting_symbols=supporting,
        retired_identity_ids=retired,
        sources=ordered_sources,
        identities=identities,
    )
    generation = _candidate_generation(content)
    return IdentityCandidate(
        generation_sha256=generation,
        correlation_set_sha256=correlation_set,
        master=_candidate_master(generation, ordered_sources, identities),
        sources=ordered_sources,
        mode=mode,
        base_catalogue_generation_sha256=base_catalogue_generation_sha256,
        correlation_generations=correlation_generations,
        replacement_symbols=replacements,
        supporting_symbols=supporting,
        retired_identity_ids=retired,
    )


def build_identity_candidate(
    reports_by_symbol: Mapping[str, dict[str, Any]],
    receipts_by_symbol: Mapping[str, dict[str, Any]],
) -> IdentityCandidate:
    if set(reports_by_symbol) != set(receipts_by_symbol) or not reports_by_symbol:
        raise V6Error("identity candidate requires matching nonempty correlation reports and receipts")
    sources = tuple(
        _candidate_source(reports_by_symbol[symbol], receipts_by_symbol[symbol])
        for symbol in sorted(reports_by_symbol)
    )
    return _identity_candidate_from_sources(sources)


def _correlation_report_from_boundaries(
    symbol: str,
    correlation: Any,
    lineage: tuple[dict[str, Any], dict[str, Any]],
) -> dict[str, Any]:
    receipt = correlation.receipt
    lineage_receipt, lineage_report = lineage
    if (
        receipt.get("symbol") != symbol
        or correlation.correlated.get("symbol") != symbol
        or lineage_receipt.get("symbol") != symbol
        or lineage_report.get("symbol") != symbol
        or receipt.get("lineage_generation_sha256") != lineage_receipt.get("lineage_generation_sha256")
    ):
        raise V6Error(f"candidate inputs belong to different boundaries: {symbol}")
    required_evidence = (
        "verified_session_exclusions",
        "uuid_lineage_edges",
        "ticker_ownership_intervals",
        "evidence_issues",
    )
    if any(field not in receipt for field in required_evidence):
        raise V6Error(f"correlation boundary has incomplete identity evidence: {symbol}")
    return {
        "symbol": symbol,
        "lineage_report": lineage_report,
        "correlated": {
            "segments": correlation.correlated.get("segments"),
        },
        **{field: receipt[field] for field in required_evidence},
    }


def build_identity_candidate_from_boundaries(
    correlations_by_symbol: Mapping[str, Any],
    lineages_by_symbol: Mapping[str, tuple[dict[str, Any], dict[str, Any]]],
) -> IdentityCandidate:
    """Transform trusted published correlation and lineage boundaries."""
    if set(correlations_by_symbol) != set(lineages_by_symbol) or not correlations_by_symbol:
        raise V6Error("identity candidate requires matching correlation and lineage boundaries")
    reports = {
        symbol: _correlation_report_from_boundaries(
            symbol,
            correlations_by_symbol[symbol],
            lineages_by_symbol[symbol],
        )
        for symbol in sorted(correlations_by_symbol)
    }
    receipts = {symbol: correlations_by_symbol[symbol].receipt for symbol in sorted(correlations_by_symbol)}
    return build_identity_candidate(reports, receipts)


def build_affected_identity_candidate(
    store_root: Path | str,
    correlations_by_symbol: Mapping[str, Any],
    lineages_by_symbol: Mapping[str, tuple[dict[str, Any], dict[str, Any]]],
) -> IdentityCandidate:
    """Build the closed identity change set over one trusted published catalogue."""
    root = Path(store_root)
    base_receipt = read_json_object(
        catalogue_directory(root) / CATALOGUE_MANIFEST,
        "v6 catalogue receipt",
    )
    base_generation = base_receipt.get("catalogue_generation_sha256")
    if not is_master_id(base_generation):
        raise V6Error("affected identity candidate has no trusted base catalogue")
    base = _load_catalogue_store(root)
    replacements = build_identity_candidate_from_boundaries(
        correlations_by_symbol,
        lineages_by_symbol,
    )
    replacement_by_symbol = {source["symbol"]: source for source in replacements.sources}
    replacement_symbols = set(replacement_by_symbol)
    closure_symbols = set(replacement_symbols)
    affected_ids = {
        identity["master_id"] for source in replacement_by_symbol.values() for identity in source["identities"]
    }
    for symbol in replacement_symbols:
        old_source = base.sources_by_symbol.get(symbol)
        if old_source is not None:
            affected_ids.update(identity["master_id"] for identity in old_source["identities"])

    while True:
        previous = (frozenset(closure_symbols), frozenset(affected_ids))
        for master_id in tuple(affected_ids):
            old_identity = base.identities_by_master_id.get(master_id)
            if old_identity is not None:
                closure_symbols.update(old_identity["source_symbols"])
        for symbol in tuple(closure_symbols - replacement_symbols):
            try:
                old_source = base.sources_by_symbol[symbol]
            except KeyError as error:
                raise V6Error(f"catalogue identity names an unavailable source: {symbol}") from error
            affected_ids.update(identity["master_id"] for identity in old_source["identities"])
        if previous == (frozenset(closure_symbols), frozenset(affected_ids)):
            break

    supporting_symbols = tuple(sorted(closure_symbols - replacement_symbols))
    sources = tuple(
        replacement_by_symbol[symbol] if symbol in replacement_by_symbol else base.sources_by_symbol[symbol]
        for symbol in sorted(closure_symbols)
    )
    _normalized_sources, identities = _offline_candidate_state(sources)
    current_ids = {identity["master_id"] for identity in identities}
    retired_ids = tuple(sorted(affected_ids - current_ids))
    return _identity_candidate_from_sources(
        sources,
        mode="affected_closure",
        base_catalogue_generation_sha256=base_generation,
        replacement_symbols=tuple(sorted(replacement_symbols)),
        supporting_symbols=supporting_symbols,
        retired_identity_ids=retired_ids,
    )


def current_source_closure(
    store_root: Path | str,
    seed_symbols: Sequence[str],
) -> tuple[str, ...]:
    """Return the trusted current identity closure containing the named sources."""
    seeds = set(seed_symbols)
    if not seeds:
        raise V6Error("source closure requires at least one seed symbol")
    base = _load_catalogue_store(Path(store_root))
    closure = set(seeds)
    identities = set()
    while True:
        previous = (frozenset(closure), frozenset(identities))
        for symbol in tuple(closure):
            source = base.sources_by_symbol.get(symbol)
            if source is not None:
                identities.update(identity["master_id"] for identity in source["identities"])
        for master_id in tuple(identities):
            identity = base.identities_by_master_id.get(master_id)
            if identity is not None:
                closure.update(identity["source_symbols"])
        if previous == (frozenset(closure), frozenset(identities)):
            return tuple(sorted(closure))


def _identity_candidate_document(candidate: IdentityCandidate) -> dict[str, Any]:
    identities = [
        candidate.master.identities_by_master_id[master_id]
        for master_id in sorted(candidate.master.identities_by_master_id)
    ]
    content = _candidate_content(
        mode=candidate.mode,
        base_catalogue_generation_sha256=candidate.base_catalogue_generation_sha256,
        correlation_generations=candidate.correlation_generations,
        replacement_symbols=candidate.replacement_symbols,
        supporting_symbols=candidate.supporting_symbols,
        retired_identity_ids=candidate.retired_identity_ids,
        sources=candidate.sources,
        identities=identities,
    )
    return {
        "schema_version": IDENTITY_CANDIDATE_VERSION,
        "boundary": IDENTITY_CANDIDATE_BOUNDARY,
        "candidate_generation_sha256": candidate.generation_sha256,
        "correlation_set_sha256": candidate.correlation_set_sha256,
        **content,
    }


def _candidate_from_document(
    document: dict[str, Any],
    *,
    validate_output: bool,
) -> IdentityCandidate:
    if (
        document.get("schema_version") != IDENTITY_CANDIDATE_VERSION
        or document.get("boundary") != IDENTITY_CANDIDATE_BOUNDARY
        or document.get("mode") not in {"snapshot", "affected_closure"}
    ):
        raise V6Error("v6 identity candidate has the wrong identity")
    sources = document.get("sources")
    identities = document.get("identities")
    correlation_generations = document.get("correlation_generations")
    replacement_symbols = document.get("replacement_symbols")
    supporting_symbols = document.get("supporting_symbols")
    retired_identity_ids = document.get("retired_identity_ids")
    if (
        not isinstance(sources, list)
        or not sources
        or any(not isinstance(source, dict) for source in sources)
        or not isinstance(identities, list)
        or any(not isinstance(identity, dict) for identity in identities)
        or not isinstance(correlation_generations, dict)
        or not isinstance(replacement_symbols, list)
        or not isinstance(supporting_symbols, list)
        or not isinstance(retired_identity_ids, list)
    ):
        raise V6Error("v6 identity candidate is malformed")
    source_symbols = [source.get("symbol") for source in sources]
    identity_ids = [identity.get("master_id") for identity in identities]
    if (
        any(not isinstance(symbol, str) or not symbol for symbol in source_symbols)
        or source_symbols != sorted(set(source_symbols))
        or any(not is_master_id(master_id) for master_id in identity_ids)
        or identity_ids != sorted(set(identity_ids))
        or replacement_symbols != sorted(set(replacement_symbols))
        or supporting_symbols != sorted(set(supporting_symbols))
        or set(replacement_symbols) & set(supporting_symbols)
        or set(replacement_symbols) | set(supporting_symbols) != set(source_symbols)
        or retired_identity_ids != sorted(set(retired_identity_ids))
        or any(not is_master_id(master_id) for master_id in retired_identity_ids)
        or set(retired_identity_ids) & set(identity_ids)
        or set(correlation_generations) != set(source_symbols)
        or any(not is_master_id(value) for value in correlation_generations.values())
    ):
        raise V6Error("v6 identity candidate membership is malformed")
    mode = document["mode"]
    base_generation = document.get("base_catalogue_generation_sha256")
    if mode == "snapshot":
        if (
            base_generation is not None
            or replacement_symbols != source_symbols
            or supporting_symbols
            or retired_identity_ids
        ):
            raise V6Error("v6 snapshot identity candidate has incremental state")
    elif not is_master_id(base_generation) or not replacement_symbols:
        raise V6Error("v6 affected identity candidate has no base or replacements")

    content = _candidate_content(
        mode=mode,
        base_catalogue_generation_sha256=base_generation,
        correlation_generations=correlation_generations,
        replacement_symbols=replacement_symbols,
        supporting_symbols=supporting_symbols,
        retired_identity_ids=retired_identity_ids,
        sources=sources,
        identities=identities,
    )
    if validate_output:
        try:
            expected_sources, expected_identities = _offline_candidate_state(sources)
        except (KeyError, SecurityCorrelationError) as error:
            raise V6Error("v6 identity candidate output is invalid") from error
        if list(expected_sources) != sources or expected_identities != identities:
            raise V6Error("v6 identity candidate aggregation differs")
        if any(
            correlation_generations[source["symbol"]] != source.get("correlation_generation_sha256")
            for source in sources
        ):
            raise V6Error("v6 identity candidate correlation bindings differ")
        if document.get("correlation_set_sha256") != canonical_sha256(correlation_generations):
            raise V6Error("v6 identity candidate correlation set differs")
        if document.get("candidate_generation_sha256") != _candidate_generation(content):
            raise V6Error("v6 identity candidate generation differs")

    generation = document.get("candidate_generation_sha256")
    correlation_set = document.get("correlation_set_sha256")
    if not is_master_id(generation) or not is_master_id(correlation_set):
        raise V6Error("v6 identity candidate has invalid generations")
    source_tuple = tuple(sources)
    return IdentityCandidate(
        generation_sha256=generation,
        correlation_set_sha256=correlation_set,
        master=_candidate_master(generation, source_tuple, identities),
        sources=source_tuple,
        mode=mode,
        base_catalogue_generation_sha256=base_generation,
        correlation_generations=dict(correlation_generations),
        replacement_symbols=tuple(replacement_symbols),
        supporting_symbols=tuple(supporting_symbols),
        retired_identity_ids=tuple(retired_identity_ids),
    )


def publish_identity_candidate(store_root: Path | str, candidate: IdentityCandidate) -> dict[str, Any]:
    validate_boundary_change_log("identity_candidate")
    root = Path(store_root)
    output_path = root / IDENTITY_CANDIDATE_OUTPUT
    replace_json(output_path, _identity_candidate_document(candidate))
    output = {
        "file": IDENTITY_CANDIDATE_OUTPUT,
        "sha256": sha256_file(output_path),
        "bytes": output_path.stat().st_size,
    }
    receipt = {
        "schema_version": IDENTITY_CANDIDATE_VERSION,
        "boundary": IDENTITY_CANDIDATE_BOUNDARY,
        "candidate_generation_sha256": candidate.generation_sha256,
        "correlation_set_sha256": candidate.correlation_set_sha256,
        "mode": candidate.mode,
        "source_count": candidate.master.master["source_count"],
        "identity_count": candidate.master.master["master_id_count"],
        "output": output,
    }
    published = _validate_identity_candidate_output(root, receipt)
    if _identity_candidate_document(published) != _identity_candidate_document(candidate):
        raise V6Error("published identity candidate differs")
    path = root / IDENTITY_CANDIDATE_MANIFEST
    replace_json(path, receipt)
    if read_json_object(path, "published identity candidate receipt") != receipt:
        raise V6Error("published identity candidate receipt differs")
    return receipt


def _validate_identity_candidate_output(
    store_root: Path,
    receipt: dict[str, Any],
) -> IdentityCandidate:
    output = receipt.get("output")
    output_path = store_root / IDENTITY_CANDIDATE_OUTPUT
    if (
        receipt.get("schema_version") != IDENTITY_CANDIDATE_VERSION
        or receipt.get("boundary") != IDENTITY_CANDIDATE_BOUNDARY
        or not isinstance(output, dict)
        or output.get("file") != IDENTITY_CANDIDATE_OUTPUT
        or not output_path.is_file()
        or output_path.is_symlink()
        or output.get("bytes") != output_path.stat().st_size
        or output.get("sha256") != sha256_file(output_path)
    ):
        raise V6Error("v6 identity candidate output authentication failed")
    candidate = _candidate_from_document(
        read_json_object(output_path, "v6 identity candidate output"),
        validate_output=True,
    )
    if (
        receipt.get("candidate_generation_sha256") != candidate.generation_sha256
        or receipt.get("correlation_set_sha256") != candidate.correlation_set_sha256
        or receipt.get("mode") != candidate.mode
        or receipt.get("source_count") != len(candidate.sources)
        or receipt.get("identity_count") != len(candidate.master.identities_by_master_id)
    ):
        raise V6Error("v6 identity candidate receipt differs from its output")
    return candidate


def authenticate_identity_candidate(store_root: Path | str) -> IdentityCandidate:
    """Explicitly authenticate the published candidate boundary."""
    validate_boundary_change_log("identity_candidate")
    root = Path(store_root)
    receipt = read_json_object(root / IDENTITY_CANDIDATE_MANIFEST, "v6 identity candidate receipt")
    return _validate_identity_candidate_output(root, receipt)


def load_identity_candidate(store_root: Path | str) -> IdentityCandidate:
    """Consume the published candidate boundary without auditing it again."""
    root = Path(store_root)
    receipt = read_json_object(root / IDENTITY_CANDIDATE_MANIFEST, "v6 identity candidate receipt")
    output = receipt.get("output")
    if (
        receipt.get("schema_version") != IDENTITY_CANDIDATE_VERSION
        or receipt.get("boundary") != IDENTITY_CANDIDATE_BOUNDARY
        or not isinstance(output, dict)
        or output.get("file") != IDENTITY_CANDIDATE_OUTPUT
    ):
        raise V6Error("v6 identity candidate receipt has the wrong identity")
    candidate = _candidate_from_document(
        read_json_object(root / IDENTITY_CANDIDATE_OUTPUT, "v6 identity candidate output"),
        validate_output=False,
    )
    if (
        receipt.get("candidate_generation_sha256") != candidate.generation_sha256
        or receipt.get("correlation_set_sha256") != candidate.correlation_set_sha256
        or receipt.get("mode") != candidate.mode
        or receipt.get("source_count") != len(candidate.sources)
        or receipt.get("identity_count") != len(candidate.master.identities_by_master_id)
    ):
        raise V6Error("v6 identity candidate receipt points to another candidate")
    return candidate


def _warning_set_generation(store_root: Path, symbols: Sequence[str]) -> str:
    generations = {}
    for symbol in sorted(symbols):
        warning = load_warnings(store_root, symbol)
        generations[symbol] = warning.receipt["warning_pipeline_generation_sha256"]
    return canonical_sha256(generations)


def _lineage_inputs_for_identity(
    store_root: Path,
    candidate: IdentityCandidate,
    correlations_by_symbol: Mapping[str, Any],
    lineages_by_symbol: Mapping[str, tuple[dict[str, Any], dict[str, Any]]],
    identity: dict[str, Any],
    raw_sources_by_symbol: dict[str, Any],
    payload_cache: dict[Path, tuple[tuple[str, ...], ...]],
) -> tuple[tuple[dict[str, Any], ...], frozenset[str]]:
    receipts = []
    raw_sessions = set()
    for symbol in identity["source_symbols"]:
        try:
            source = candidate.master.sources_by_symbol[symbol]
            correlation = correlations_by_symbol[symbol]
            lineage_receipt, lineage_report = lineages_by_symbol[symbol]
        except KeyError as error:
            raise V6Error(f"availability has no published evidence for: {symbol}") from error
        correlation_receipt = correlation.receipt
        if correlation_receipt.get("correlation_generation_sha256") != source.get(
            "correlation_generation_sha256"
        ) or correlation_receipt.get("lineage_generation_sha256") != lineage_receipt.get(
            "lineage_generation_sha256"
        ):
            raise V6Error(f"availability evidence belongs to another boundary: {symbol}")
        receipts.append(
            {
                "evidence_issues": correlation_receipt["evidence_issues"],
                "lineage_report": lineage_report,
            }
        )
        raw = raw_sources_by_symbol.get(symbol)
        if raw is None:
            raw = read_monthly_adjustment_rows(
                store_root,
                symbol,
                "raw",
                payload_cache=payload_cache,
            )
            raw_sources_by_symbol[symbol] = raw
        for row in raw:
            timestamp = datetime.fromisoformat(row["t"].replace("Z", "+00:00"))
            raw_sessions.add(timestamp.astimezone(NEW_YORK).date().isoformat())
    return tuple(receipts), frozenset(raw_sessions)


def evaluate_availability(
    store_root: Path | str,
    candidate: IdentityCandidate,
    correlations_by_symbol: Mapping[str, Any],
    lineages_by_symbol: Mapping[str, tuple[dict[str, Any], dict[str, Any]]],
    *,
    pricing_inputs_by_symbol: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(store_root)
    history_inputs = PublishedHistoryInputs()
    raw_sources_by_symbol: dict[str, Any] = {}
    if pricing_inputs_by_symbol is not None:
        for symbol, pricing_input in pricing_inputs_by_symbol.items():
            history_inputs.seed_pricing_input(pricing_input)
            raw = next(
                (
                    download.rows
                    for download in pricing_input.downloads_by_filename.values()
                    if download.manifest.get("adjustment") == "raw"
                ),
                None,
            )
            if raw is not None:
                raw_sources_by_symbol[symbol] = raw

    def history_reader(master_id: str, adjustment: str) -> dict[str, Any]:
        try:
            return read_history_from_master(
                root,
                candidate.master,
                master_id,
                adjustment,
                inputs=history_inputs,
            )
        except V6Error as error:
            raise EndOfDayReaderError(str(error)) from error

    def lineage_reader(identity: dict[str, Any]):
        try:
            return _lineage_inputs_for_identity(
                root,
                candidate,
                correlations_by_symbol,
                lineages_by_symbol,
                identity,
                raw_sources_by_symbol,
                history_inputs.payloads,
            )
        except V6Error as error:
            raise SecurityCorrelationError(str(error)) from error

    evaluation = evaluate_security_online_status_view(
        candidate.master.identities_by_master_id,
        master=candidate.master,
        history_reader=history_reader,
        lineage_evidence_reader=lineage_reader,
        security_object_sha256=lambda master_id: canonical_sha256(
            candidate.master.identities_by_master_id[master_id]
        ),
        source_object_sha256=lambda symbol: canonical_sha256(candidate.master.sources_by_symbol[symbol]),
    )
    decisions = [
        {
            "master_id": master_id,
            "online": evaluation.online_by_master_id[master_id],
            "availability_status": evaluation.availability_status_by_master_id[master_id],
        }
        for master_id in sorted(evaluation.online_by_master_id)
    ]
    return {
        "schema_version": AVAILABILITY_VERSION,
        "boundary": AVAILABILITY_BOUNDARY,
        "candidate_generation_sha256": candidate.generation_sha256,
        "warning_set_sha256": _warning_set_generation(root, tuple(candidate.master.sources_by_symbol)),
        "decisions": decisions,
    }


def publish_availability(
    store_root: Path | str,
    report: dict[str, Any],
    candidate: IdentityCandidate,
) -> dict[str, Any]:
    validate_boundary_change_log("availability")
    root = Path(store_root)
    directory = root / "availability"
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / AVAILABILITY_OUTPUT
    replace_json(output_path, report)
    output = {
        "file": AVAILABILITY_OUTPUT,
        "sha256": sha256_file(output_path),
        "bytes": output_path.stat().st_size,
    }
    generation = canonical_sha256(
        {
            "schema_version": AVAILABILITY_VERSION,
            "candidate_generation_sha256": report["candidate_generation_sha256"],
            "warning_set_sha256": report["warning_set_sha256"],
            "output": output,
        }
    )
    receipt = {
        "schema_version": AVAILABILITY_VERSION,
        "boundary": AVAILABILITY_BOUNDARY,
        "availability_generation_sha256": generation,
        "candidate_generation_sha256": report["candidate_generation_sha256"],
        "warning_set_sha256": report["warning_set_sha256"],
        "identity_count": len(report["decisions"]),
        "output": output,
    }
    published_decisions = _validate_availability_output(root, receipt, candidate)
    expected_decisions = {item["master_id"]: item for item in report["decisions"]}
    if published_decisions != expected_decisions:
        raise V6Error("published availability output differs")
    manifest_path = directory / AVAILABILITY_MANIFEST
    replace_json(manifest_path, receipt)
    if read_json_object(manifest_path, "published availability receipt") != receipt:
        raise V6Error("published availability receipt differs")
    return receipt


def _validate_availability_output(
    store_root: Path,
    receipt: dict[str, Any],
    candidate: IdentityCandidate,
) -> dict[str, dict[str, Any]]:
    directory = store_root / "availability"
    output_path = directory / AVAILABILITY_OUTPUT
    output = receipt.get("output")
    if (
        receipt.get("schema_version") != AVAILABILITY_VERSION
        or receipt.get("boundary") != AVAILABILITY_BOUNDARY
        or receipt.get("candidate_generation_sha256") != candidate.generation_sha256
        or not isinstance(output, dict)
        or output.get("file") != AVAILABILITY_OUTPUT
        or not output_path.is_file()
        or output.get("bytes") != output_path.stat().st_size
        or output.get("sha256") != sha256_file(output_path)
    ):
        raise V6Error("v6 availability receipt is stale or unauthenticated")
    report = read_json_object(output_path, "v6 availability output")
    if (
        report.get("schema_version") != AVAILABILITY_VERSION
        or report.get("boundary") != AVAILABILITY_BOUNDARY
        or report.get("candidate_generation_sha256") != candidate.generation_sha256
        or report.get("warning_set_sha256") != receipt.get("warning_set_sha256")
    ):
        raise V6Error("v6 availability immediate inputs differ")
    generation = canonical_sha256(
        {
            "schema_version": AVAILABILITY_VERSION,
            "candidate_generation_sha256": candidate.generation_sha256,
            "warning_set_sha256": receipt["warning_set_sha256"],
            "output": output,
        }
    )
    if receipt.get("availability_generation_sha256") != generation:
        raise V6Error("v6 availability generation differs")
    decisions = report.get("decisions")
    if not isinstance(decisions, list) or receipt.get("identity_count") != len(decisions):
        raise V6Error("v6 availability decisions are malformed")
    by_id = {}
    for decision in decisions:
        master_id = decision.get("master_id") if isinstance(decision, dict) else None
        status = decision.get("availability_status") if isinstance(decision, dict) else None
        online = decision.get("online") if isinstance(decision, dict) else None
        if (
            not is_master_id(master_id)
            or master_id in by_id
            or status
            not in {
                ONLINE_AVAILABILITY_STATUS,
                OFFLINE_AVAILABILITY_STATUS,
                LINEAGE_GAP_AVAILABILITY_STATUS,
            }
            or type(online) is not bool
            or online != (status == ONLINE_AVAILABILITY_STATUS)
        ):
            raise V6Error("v6 availability contains an invalid decision")
        by_id[master_id] = decision
    if set(by_id) != set(candidate.master.identities_by_master_id):
        raise V6Error("v6 availability does not cover the identity candidate")
    return by_id


def load_availability(store_root: Path, candidate: IdentityCandidate) -> tuple[dict, dict]:
    """Consume the published availability boundary without auditing it again."""
    directory = store_root / "availability"
    receipt = read_json_object(directory / AVAILABILITY_MANIFEST, "v6 availability receipt")
    report = read_json_object(directory / AVAILABILITY_OUTPUT, "v6 availability output")
    if (
        receipt.get("candidate_generation_sha256") != candidate.generation_sha256
        or report.get("candidate_generation_sha256") != candidate.generation_sha256
    ):
        raise V6Error("v6 availability belongs to another identity candidate")
    decisions = report.get("decisions")
    if not isinstance(decisions, list):
        raise V6Error("v6 availability has no decisions")
    by_id = {
        decision["master_id"]: decision
        for decision in decisions
        if isinstance(decision, dict) and isinstance(decision.get("master_id"), str)
    }
    return receipt, by_id


def _descriptor_file(descriptor: object) -> str | None:
    filename = descriptor.get("file") if isinstance(descriptor, dict) else None
    return filename if isinstance(filename, str) else None


def _load_pending_reclamation(root: Path) -> set[str]:
    path = catalogue_directory(root) / RETENTION_STATE
    if not path.is_file():
        return set()
    document = read_json_object(path, "v6 catalogue retention state")
    pending = document.get("reclaim_after_next_publication")
    if (
        document.get("schema_version") != CATALOGUE_PUBLICATION_VERSION
        or not isinstance(pending, list)
        or pending != sorted(set(pending))
        or any(not isinstance(item, str) or not item for item in pending)
    ):
        raise V6Error("v6 catalogue retention state is malformed")
    return set(pending)


def _reclaim_superseded_files(
    master_directory: Path,
    pending: set[str],
    current_changed_files: set[str],
) -> dict[str, int]:
    removed_files = removed_bytes = 0
    touched_directories = set()
    for filename in sorted(pending - current_changed_files):
        path = _published_descriptor_path(
            master_directory,
            {"file": filename},
            Path(filename).parent,
            "superseded catalogue object",
        )
        if not path.is_file():
            continue
        removed_bytes += path.stat().st_size
        path.unlink()
        removed_files += 1
        touched_directories.add(path.parent)
    for directory in sorted(touched_directories, key=lambda item: len(item.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            pass
    return {"removed_files": removed_files, "removed_bytes": removed_bytes}


def _candidate_sources_with_availability(
    candidate: IdentityCandidate,
    decisions: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    sources = []
    for candidate_source in candidate.sources:
        source = deepcopy(candidate_source)
        for identity in source["identities"]:
            master_id = identity["master_id"]
            try:
                decision = decisions[master_id]
            except KeyError as error:
                raise V6Error(f"availability has no candidate identity: {master_id}") from error
            identity["online"] = decision["online"]
            identity["availability_status"] = decision["availability_status"]
        sources.append(source)
    return sources


def _record_descriptor_replacement(
    old: object,
    new: object,
    *,
    superseded: set[str],
    current_changed: set[str],
) -> None:
    old_file = _descriptor_file(old)
    new_file = _descriptor_file(new)
    if new_file is not None:
        current_changed.add(new_file)
    if old_file is not None and old_file != new_file:
        superseded.add(old_file)


def _publish_affected_catalogue_engine(
    root: Path,
    candidate: IdentityCandidate,
    sources: Sequence[dict[str, Any]],
) -> tuple[dict[str, Any], set[str], set[str]]:
    current_receipt = read_json_object(
        catalogue_directory(root) / CATALOGUE_MANIFEST,
        "v6 catalogue receipt",
    )
    if current_receipt.get("catalogue_generation_sha256") != candidate.base_catalogue_generation_sha256:
        raise V6Error("affected identity candidate belongs to another base catalogue")
    existing = _load_catalogue_store(root)
    engine_root = legacy_engine_root(root)
    master_directory = security_master_directory(engine_root)
    master_directory.mkdir(parents=True, exist_ok=True)
    superseded: set[str] = set()
    current_changed: set[str] = set()

    source_updates = {}
    for source in sources:
        symbol = source["symbol"]
        if existing.sources_by_symbol.get(symbol) != source:
            source_updates[symbol] = _publish_source_object(master_directory, source)
    source_inventory = (
        _replace_inventory_entries(
            master_directory,
            "source",
            existing.sources_by_symbol,
            source_updates,
        )
        if source_updates
        else existing.master["source_inventory"]
    )
    if source_updates:
        new_sources = _PublishedObjectMapping(
            master_directory,
            "sources",
            source_inventory,
        )
        for symbol, descriptor in source_updates.items():
            old_descriptor = (
                existing.sources_by_symbol.object_descriptor(symbol)
                if symbol in existing.sources_by_symbol
                else None
            )
            _record_descriptor_replacement(
                old_descriptor,
                descriptor,
                superseded=superseded,
                current_changed=current_changed,
            )
        for prefix in {hashlib.sha256(symbol.encode("utf-8")).hexdigest()[:2] for symbol in source_updates}:
            _record_descriptor_replacement(
                existing.sources_by_symbol.bucket_descriptors.get(prefix),
                new_sources.bucket_descriptors.get(prefix),
                superseded=superseded,
                current_changed=current_changed,
            )
        _record_descriptor_replacement(
            existing.master["source_inventory"],
            source_inventory,
            superseded=superseded,
            current_changed=current_changed,
        )

    securities = _aggregate_security_entries(list(sources))
    security_updates: dict[str, dict[str, Any] | None] = {}
    for security in securities:
        master_id = security["master_id"]
        if existing.identities_by_master_id.get(master_id) != security:
            security_updates[master_id] = _publish_security_object(master_directory, security)
    for master_id in candidate.retired_identity_ids:
        if master_id in existing.identities_by_master_id:
            security_updates[master_id] = None
    security_inventory = (
        _replace_inventory_entries(
            master_directory,
            "security",
            existing.identities_by_master_id,
            security_updates,
        )
        if security_updates
        else existing.master["security_inventory"]
    )
    if security_updates:
        new_securities = _PublishedObjectMapping(
            master_directory,
            "securities",
            security_inventory,
        )
        for master_id, descriptor in security_updates.items():
            old_descriptor = (
                existing.identities_by_master_id.object_descriptor(master_id)
                if master_id in existing.identities_by_master_id
                else None
            )
            _record_descriptor_replacement(
                old_descriptor,
                descriptor,
                superseded=superseded,
                current_changed=current_changed,
            )
        for prefix in {
            hashlib.sha256(master_id.encode("utf-8")).hexdigest()[:2] for master_id in security_updates
        }:
            _record_descriptor_replacement(
                existing.identities_by_master_id.bucket_descriptors.get(prefix),
                new_securities.bucket_descriptors.get(prefix),
                superseded=superseded,
                current_changed=current_changed,
            )
        _record_descriptor_replacement(
            existing.master["security_inventory"],
            security_inventory,
            superseded=superseded,
            current_changed=current_changed,
        )
    receipt = _publish_security_master_root(
        engine_root,
        source_inventory,
        security_inventory,
        existing=existing,
    )
    return receipt, superseded, current_changed


def publish_catalogue(
    store_root: Path | str,
    candidate: IdentityCandidate,
) -> dict[str, Any]:
    validate_boundary_change_log("catalogue_publication")
    root = Path(store_root)
    availability_receipt, decisions = load_availability(root, candidate)
    sources = _candidate_sources_with_availability(candidate, decisions)
    engine_root = legacy_engine_root(root)
    master_directory = security_master_directory(engine_root)
    catalogue_path = catalogue_directory(root) / CATALOGUE_MANIFEST
    if candidate.mode == "snapshot":
        if catalogue_path.exists():
            raise V6Error("snapshot catalogue publication requires a virgin catalogue boundary")
        engine_receipt = _publish_security_master_snapshot(engine_root, sources)
        superseded: set[str] = set()
        current_changed: set[str] = set()
    else:
        engine_receipt, superseded, current_changed = _publish_affected_catalogue_engine(
            root,
            candidate,
            sources,
        )
    master = _load_engine_store(root)
    if (
        master.receipt["security_master_generation_sha256"]
        != engine_receipt["security_master_generation_sha256"]
    ):
        raise V6Error("published catalogue engine generation could not be authenticated")
    generation_identity = {
        "schema_version": CATALOGUE_PUBLICATION_VERSION,
        "mode": candidate.mode,
        "base_catalogue_generation_sha256": candidate.base_catalogue_generation_sha256,
        "candidate_generation_sha256": candidate.generation_sha256,
        "availability_generation_sha256": availability_receipt["availability_generation_sha256"],
        "candidate_correlation_set_sha256": candidate.correlation_set_sha256,
        "security_master_generation_sha256": master.receipt["security_master_generation_sha256"],
        "security_master_output_sha256": master.master_sha256,
    }
    catalogue_generation = canonical_sha256(generation_identity)
    directory = catalogue_directory(root)
    directory.mkdir(parents=True, exist_ok=True)
    receipt = {
        "schema_version": CATALOGUE_PUBLICATION_VERSION,
        "boundary": CATALOGUE_BOUNDARY,
        "catalogue_generation_sha256": catalogue_generation,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "mode": candidate.mode,
        "base_catalogue_generation_sha256": candidate.base_catalogue_generation_sha256,
        "candidate_generation_sha256": candidate.generation_sha256,
        "availability_generation_sha256": availability_receipt["availability_generation_sha256"],
        "candidate_correlation_set_sha256": candidate.correlation_set_sha256,
        "source_count": len(master.sources_by_symbol),
        "identity_count": len(master.identities_by_master_id),
        "security_master": {
            "generation_sha256": master.receipt["security_master_generation_sha256"],
            "output_sha256": master.master_sha256,
        },
    }
    pending_reclamation = _load_pending_reclamation(root)
    reclamation = _reclaim_superseded_files(
        master_directory,
        pending_reclamation,
        current_changed,
    )
    retention = {
        "schema_version": CATALOGUE_PUBLICATION_VERSION,
        "current_generation_sha256": master.receipt["security_master_generation_sha256"],
        "policy": "current_plus_immediately_previous_superseded_objects",
        "reclaim_after_next_publication": sorted(superseded),
        "previous_files_retained": len(superseded),
        **reclamation,
    }
    retention_path = directory / RETENTION_STATE
    replace_json(retention_path, retention)
    if read_json_object(retention_path, "published catalogue retention state") != retention:
        raise V6Error("published catalogue retention state differs")
    # This is the reader-visible publication point and is deliberately written last.
    replace_json(directory / CATALOGUE_MANIFEST, receipt)
    if read_json_object(directory / CATALOGUE_MANIFEST, "published catalogue receipt") != receipt:
        raise V6Error("published catalogue receipt differs")
    return receipt


def _published_descriptor_path(
    master_directory: Path,
    descriptor: object,
    expected_prefix: Path,
    description: str,
) -> Path:
    filename = descriptor.get("file") if isinstance(descriptor, dict) else None
    if not isinstance(filename, str):
        raise V6Error(f"published {description} has no filename")
    relative = Path(filename)
    if (
        relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
        or relative.parts[: len(expected_prefix.parts)] != expected_prefix.parts
    ):
        raise V6Error(f"published {description} has an unsafe filename: {filename!r}")
    return master_directory / relative


class _PublishedObjectMapping(Mapping[str, dict[str, Any]]):
    """Lazy consumer view of a catalogue inventory; it does not audit digests."""

    def __init__(self, master_directory: Path, kind: str, descriptor: dict[str, Any]) -> None:
        self.master_directory = master_directory
        self.kind = kind
        self.key_field = "symbol" if kind == "sources" else "master_id"
        self.object_field = "source" if kind == "sources" else "security"
        self.inventory_directory = Path("inventories") / kind
        self.object_directory = Path("objects") / kind
        root_path = _published_descriptor_path(
            master_directory,
            descriptor,
            self.inventory_directory / "roots",
            f"{kind} inventory",
        )
        self.inventory = read_json_object(root_path, f"published {kind} inventory")
        buckets = self.inventory.get("buckets")
        if not isinstance(buckets, list):
            raise V6Error(f"published {kind} inventory has no buckets")
        self.bucket_descriptors = {
            bucket.get("prefix"): bucket for bucket in buckets if isinstance(bucket, dict)
        }
        self._bucket_cache: dict[str, dict[str, dict[str, Any]]] = {}
        self._object_cache: dict[str, dict[str, Any]] = {}

    def __len__(self) -> int:
        count = self.inventory.get("entry_count")
        if type(count) is not int:
            raise V6Error(f"published {self.kind} inventory has no entry count")
        return count

    def _entries(self, prefix: str) -> dict[str, dict[str, Any]]:
        cached = self._bucket_cache.get(prefix)
        if cached is not None:
            return cached
        descriptor = self.bucket_descriptors.get(prefix)
        if descriptor is None:
            self._bucket_cache[prefix] = {}
            return {}
        path = _published_descriptor_path(
            self.master_directory,
            descriptor,
            self.inventory_directory / "buckets" / prefix,
            f"{self.kind} bucket",
        )
        document = read_json_object(path, f"published {self.kind} bucket")
        raw_entries = document.get("entries")
        if not isinstance(raw_entries, list):
            raise V6Error(f"published {self.kind} bucket has no entries")
        entries = {}
        for entry in raw_entries:
            key = entry.get(self.key_field) if isinstance(entry, dict) else None
            object_descriptor = entry.get("object") if isinstance(entry, dict) else None
            if not isinstance(key, str) or not isinstance(object_descriptor, dict):
                raise V6Error(f"published {self.kind} bucket has a malformed entry")
            entries[key] = object_descriptor
        self._bucket_cache[prefix] = entries
        return entries

    def _bucket_entries(self, prefix: str) -> dict[str, dict[str, Any]]:
        """Provide the trusted inventory interface used by incremental publication."""
        return self._entries(prefix)

    def object_descriptor(self, key: str) -> dict[str, Any]:
        try:
            return self._entries(hashlib.sha256(key.encode("utf-8")).hexdigest()[:2])[key]
        except KeyError as error:
            raise KeyError(key) from error

    def __iter__(self) -> Iterator[str]:
        keys = [key for prefix in sorted(self.bucket_descriptors) for key in self._entries(prefix)]
        yield from sorted(keys)

    def __getitem__(self, key: str) -> dict[str, Any]:
        cached = self._object_cache.get(key)
        if cached is not None:
            return cached
        prefix = hashlib.sha256(key.encode("utf-8")).hexdigest()[:2]
        try:
            descriptor = self._entries(prefix)[key]
        except KeyError as error:
            raise KeyError(key) from error
        path = _published_descriptor_path(
            self.master_directory,
            descriptor,
            self.object_directory / prefix,
            f"{self.kind} object",
        )
        document = read_json_object(path, f"published {self.kind} object")
        value = document.get(self.object_field)
        if not isinstance(value, dict) or value.get(self.key_field) != key:
            raise V6Error(f"published {self.kind} object has the wrong identity: {key}")
        self._object_cache[key] = value
        return value


def _load_engine_store(root: Path) -> AuthenticatedSecurityMaster:
    """Load the published engine output as a trusted producer result."""
    engine_root = legacy_engine_root(root)
    engine_receipt = read_json_object(
        security_master_manifest_path(engine_root),
        "published security-master receipt",
    )
    index = read_json_object(
        security_master_path(engine_root),
        "published security-master index",
    )
    generation = index.get("security_master_generation_sha256")
    output = engine_receipt.get("output")
    if (
        engine_receipt.get("manifest_type") != "end_of_day_security_master_index_receipt"
        or index.get("manifest_type") != "end_of_day_security_master_index"
        or engine_receipt.get("security_master_version") != SECURITY_MASTER_VERSION
        or index.get("security_master_version") != SECURITY_MASTER_VERSION
        or engine_receipt.get("security_master_generation_sha256") != generation
        or not is_master_id(generation)
        or not isinstance(output, dict)
        or output.get("file") != security_master_path(engine_root).name
        or not is_master_id(output.get("sha256"))
    ):
        raise V6Error("published catalogue engine has an incoherent selection receipt")
    master_directory = security_master_directory(engine_root)
    source_inventory = index.get("source_inventory")
    security_inventory = index.get("security_inventory")
    if not isinstance(source_inventory, dict) or not isinstance(security_inventory, dict):
        raise V6Error("published catalogue has no inventories")
    return AuthenticatedSecurityMaster(
        receipt=engine_receipt,
        master=index,
        master_sha256=output["sha256"],
        sources_by_symbol=_PublishedObjectMapping(master_directory, "sources", source_inventory),
        identities_by_master_id=_PublishedObjectMapping(
            master_directory,
            "securities",
            security_inventory,
        ),
    )


def _load_catalogue_store(root: Path) -> AuthenticatedSecurityMaster:
    """Load the reader-visible catalogue boundary without authenticating it."""
    receipt = read_json_object(
        catalogue_directory(root) / CATALOGUE_MANIFEST,
        "v6 catalogue receipt",
    )
    if (
        receipt.get("schema_version") != CATALOGUE_PUBLICATION_VERSION
        or receipt.get("boundary") != CATALOGUE_BOUNDARY
    ):
        raise V6Error("v6 catalogue receipt has the wrong identity")
    master = _load_engine_store(root)
    published_engine = receipt.get("security_master")
    if (
        not isinstance(published_engine, dict)
        or published_engine.get("generation_sha256") != master.receipt.get("security_master_generation_sha256")
        or published_engine.get("output_sha256") != master.master_sha256
        or receipt.get("source_count") != len(master.sources_by_symbol)
        or receipt.get("identity_count") != len(master.identities_by_master_id)
    ):
        raise V6Error("published catalogue points to another catalogue generation")
    return master


def load_reusable_publication(
    store_root: Path | str,
    correlation_generations: Mapping[str, str],
) -> dict[str, dict[str, Any]] | None:
    """Select an already-published catalogue that contains the named correlations.

    This is a trusted consumer operation.  It compares publication identities and
    does not authenticate any previously published output bytes.
    """
    root = Path(store_root)
    catalogue_receipt = read_json_object(
        catalogue_directory(root) / CATALOGUE_MANIFEST,
        "v6 catalogue receipt",
    )
    master = _load_catalogue_store(root)
    for symbol, generation in correlation_generations.items():
        source = master.sources_by_symbol.get(symbol)
        if source is None or source.get("correlation_generation_sha256") != generation:
            return None

    candidate_receipt = read_json_object(
        root / IDENTITY_CANDIDATE_MANIFEST,
        "v6 identity candidate receipt",
    )
    availability_receipt = read_json_object(
        root / "availability" / AVAILABILITY_MANIFEST,
        "v6 availability receipt",
    )
    if (
        candidate_receipt.get("schema_version") != IDENTITY_CANDIDATE_VERSION
        or candidate_receipt.get("boundary") != IDENTITY_CANDIDATE_BOUNDARY
        or availability_receipt.get("schema_version") != AVAILABILITY_VERSION
        or availability_receipt.get("boundary") != AVAILABILITY_BOUNDARY
        or catalogue_receipt.get("schema_version") != CATALOGUE_PUBLICATION_VERSION
        or catalogue_receipt.get("boundary") != CATALOGUE_BOUNDARY
        or catalogue_receipt.get("candidate_generation_sha256")
        != candidate_receipt.get("candidate_generation_sha256")
        or catalogue_receipt.get("candidate_correlation_set_sha256")
        != candidate_receipt.get("correlation_set_sha256")
        or availability_receipt.get("candidate_generation_sha256")
        != candidate_receipt.get("candidate_generation_sha256")
        or catalogue_receipt.get("availability_generation_sha256")
        != availability_receipt.get("availability_generation_sha256")
    ):
        return None
    return {
        "identity_candidate": candidate_receipt,
        "availability": availability_receipt,
        "catalogue": catalogue_receipt,
    }


def _authenticate_catalogue_store(root: Path) -> AuthenticatedSecurityMaster:
    validate_boundary_change_log("catalogue_publication")
    receipt_path = catalogue_directory(root) / CATALOGUE_MANIFEST
    receipt = read_json_object(receipt_path, "v6 catalogue receipt")
    if (
        receipt.get("schema_version") != CATALOGUE_PUBLICATION_VERSION
        or receipt.get("boundary") != CATALOGUE_BOUNDARY
    ):
        raise V6Error("v6 catalogue receipt has the wrong identity")
    master = authenticate_security_master(legacy_engine_root(root))
    if (
        receipt.get("source_count") != len(master.sources_by_symbol)
        or receipt.get("identity_count") != len(master.identities_by_master_id)
        or receipt.get("security_master")
        != {
            "generation_sha256": master.receipt["security_master_generation_sha256"],
            "output_sha256": master.master_sha256,
        }
    ):
        raise V6Error("v6 catalogue receipt does not bind the current catalogue engine")
    candidate = authenticate_identity_candidate(root)
    if (
        receipt.get("candidate_generation_sha256") != candidate.generation_sha256
        or receipt.get("candidate_correlation_set_sha256") != candidate.correlation_set_sha256
        or receipt.get("mode") != candidate.mode
        or receipt.get("base_catalogue_generation_sha256") != candidate.base_catalogue_generation_sha256
    ):
        raise V6Error("v6 catalogue candidate binding differs")
    availability_receipt = read_json_object(
        root / "availability" / AVAILABILITY_MANIFEST,
        "v6 availability receipt",
    )
    decisions = _validate_availability_output(root, availability_receipt, candidate)
    if receipt.get("availability_generation_sha256") != availability_receipt.get(
        "availability_generation_sha256"
    ):
        raise V6Error("v6 catalogue has a stale availability binding")
    expected_sources = _candidate_sources_with_availability(candidate, decisions)
    expected_identities = _aggregate_security_entries(expected_sources)
    expected_sources_by_symbol = {source["symbol"]: source for source in expected_sources}
    expected_identities_by_id = {identity["master_id"]: identity for identity in expected_identities}
    if candidate.mode == "snapshot" and (
        set(master.sources_by_symbol) != set(expected_sources_by_symbol)
        or set(master.identities_by_master_id) != set(expected_identities_by_id)
    ):
        raise V6Error("v6 snapshot catalogue membership differs")
    for symbol, source in expected_sources_by_symbol.items():
        if master.sources_by_symbol.get(symbol) != source:
            raise V6Error(f"v6 catalogue candidate source differs: {symbol}")
    for master_id, identity in expected_identities_by_id.items():
        if master.identities_by_master_id.get(master_id) != identity:
            raise V6Error(f"v6 catalogue candidate identity differs: {master_id}")
    for master_id in candidate.retired_identity_ids:
        if master_id in master.identities_by_master_id:
            raise V6Error(f"v6 catalogue retained a retired identity: {master_id}")
    expected_generation = canonical_sha256(
        {
            "schema_version": CATALOGUE_PUBLICATION_VERSION,
            "mode": candidate.mode,
            "base_catalogue_generation_sha256": candidate.base_catalogue_generation_sha256,
            "candidate_generation_sha256": candidate.generation_sha256,
            "availability_generation_sha256": availability_receipt["availability_generation_sha256"],
            "candidate_correlation_set_sha256": candidate.correlation_set_sha256,
            "security_master_generation_sha256": master.receipt["security_master_generation_sha256"],
            "security_master_output_sha256": master.master_sha256,
        }
    )
    if receipt.get("catalogue_generation_sha256") != expected_generation:
        raise V6Error("v6 catalogue generation differs")
    return master


def authenticate_catalogue(repository_root: Path | str) -> AuthenticatedSecurityMaster:
    """Authenticate the sole supported public catalogue at data_vtest."""
    root = current_store_root(repository_root)
    with publication_lock(root, exclusive=False):
        assert_publication_complete(root)
        return _authenticate_catalogue_store(root)


def load_catalogue(repository_root: Path | str) -> AuthenticatedSecurityMaster:
    """Open the sole published v6 catalogue as a trusted consumer input."""
    root = current_store_root(repository_root)
    with publication_lock(root, exclusive=False):
        assert_publication_complete(root)
        return _load_catalogue_store(root)


__all__ = [
    "IdentityCandidate",
    "authenticate_identity_candidate",
    "authenticate_catalogue",
    "build_affected_identity_candidate",
    "build_identity_candidate",
    "build_identity_candidate_from_boundaries",
    "current_source_closure",
    "evaluate_availability",
    "load_availability",
    "load_identity_candidate",
    "load_reusable_publication",
    "publish_availability",
    "publish_catalogue",
    "publish_identity_candidate",
    "load_catalogue",
]

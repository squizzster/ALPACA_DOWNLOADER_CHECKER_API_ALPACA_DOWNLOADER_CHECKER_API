import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from libs.alpaca_end_of_day_pipeline import (
    CSV_COLUMN_ALPACA_FIELD_PAIRS,
    DOWNLOAD_REQUEST_IDENTITY_FIELDS,
    PIPELINE_VERSION,
    RECORDED_PRICING_DATA_FAILURE_ACTION,
    EndOfDayPipelineError,
    authenticate_frozen_download,
    download_manifest_path,
    inspect_cached_download,
    integrity_failure_manifest_path,
    read_frozen_alpaca_daily_bars,
)
from libs.alpaca_end_of_day_security_correlation import (
    AuthenticatedSecurityCorrelation,
    AuthenticatedSecurityMaster,
    SecurityCorrelationError,
    authenticate_security_correlation,
    authenticate_security_master,
    is_master_id,
)
from libs.alpaca_end_of_day_warning_pipeline import (
    AuthenticatedWarningPipelineOutputs,
    authenticate_warning_pipeline_outputs,
)
from libs.alpaca_end_of_day_warning_pipeline.errors import WarningPipelineError
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES

from .errors import EndOfDayReaderError
from .reader_version import READER_VERSION, validate_reader_change_log

ALLOWED_PIPELINE_STATES = frozenset(("cached", RECORDED_PRICING_DATA_FAILURE_ACTION))
BAR_FIELD_NAMES = tuple(csv_column for csv_column, _ in CSV_COLUMN_ALPACA_FIELD_PAIRS)


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise EndOfDayReaderError(f"could not read {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise EndOfDayReaderError(f"{description} is not a JSON object: {path}")
    return document


def _canonical_master_id(master_id: str) -> str:
    if not isinstance(master_id, str):
        raise ValueError("master ID must be a string")
    canonical_id = master_id.lower()
    if not is_master_id(canonical_id):
        raise ValueError(f"invalid master ID: {master_id!r}")
    return canonical_id


def _validated_adjustment_type(adjustment_type: str) -> str:
    if adjustment_type not in ALPACA_ADJUSTMENT_MODES:
        supported = ", ".join(ALPACA_ADJUSTMENT_MODES)
        raise ValueError(f"unsupported end-of-day type {adjustment_type!r}; expected one of: {supported}")
    return adjustment_type


def _master_id_matches(value: object, canonical_id: str) -> bool:
    return isinstance(value, str) and value.lower() == canonical_id and is_master_id(value.lower())


def _validate_correlation_master_binding(
    correlation: AuthenticatedSecurityCorrelation,
    source: dict[str, Any],
    master_id: str,
) -> None:
    expected = {
        "correlation_version": correlation.receipt["correlation_version"],
        "correlation_generation_sha256": correlation.receipt["correlation_generation_sha256"],
        "correlated_output_sha256": correlation.correlated_sha256,
        "uncorrelated_output_sha256": correlation.uncorrelated_sha256,
    }
    for field_name, observed_value in expected.items():
        if source.get(field_name) != observed_value:
            raise EndOfDayReaderError(
                f"security-master source {source.get('symbol')!r} has a stale {field_name} binding"
            )
    source_identities = source.get("identities")
    matching_identities = (
        [
            identity
            for identity in source_identities
            if isinstance(identity, dict) and identity.get("master_id") == master_id
        ]
        if isinstance(source_identities, list)
        else []
    )
    if len(matching_identities) != 1:
        raise EndOfDayReaderError(
            f"security-master source {source.get('symbol')!r} does not uniquely bind master ID {master_id}"
        )


def _authenticated_correlations(
    repository_root: Path,
    master: AuthenticatedSecurityMaster,
    master_identity: dict[str, Any],
) -> list[tuple[str, AuthenticatedSecurityCorrelation]]:
    correlations = []
    master_id = master_identity["master_id"]
    for symbol in master_identity["source_symbols"]:
        source = master.sources_by_symbol.get(symbol)
        if source is None:
            raise EndOfDayReaderError(f"security master has no source record for published symbol {symbol!r}")
        try:
            correlation = authenticate_security_correlation(repository_root, symbol)
        except SecurityCorrelationError as error:
            raise EndOfDayReaderError(
                f"cannot authenticate security correlation for {symbol}: {error}"
            ) from error
        _validate_correlation_master_binding(correlation, source, master_id)
        correlations.append((symbol, correlation))
    return correlations


def _matching_segments(
    correlations: list[tuple[str, AuthenticatedSecurityCorrelation]],
    master_id: str,
    adjustment_type: str,
) -> list[tuple[str, AuthenticatedSecurityCorrelation, dict[str, Any]]]:
    matches = []
    for symbol, correlation in correlations:
        for segment in correlation.correlated["segments"]:
            if (
                _master_id_matches(segment["canonical_security"]["security_id"], master_id)
                and segment["adjustment"] == adjustment_type
            ):
                matches.append((symbol, correlation, segment))
    return matches


def _authenticate_warning_outputs(
    repository_root: Path,
    correlations_by_symbol: dict[str, AuthenticatedSecurityCorrelation],
) -> dict[str, AuthenticatedWarningPipelineOutputs]:
    outputs = {}
    for symbol, correlation in sorted(correlations_by_symbol.items()):
        try:
            outputs[symbol] = authenticate_warning_pipeline_outputs(
                repository_root,
                symbol,
                correlation,
            )
        except WarningPipelineError as error:
            raise EndOfDayReaderError(f"cannot authenticate warning overlays for {symbol}: {error}") from error
    return outputs


def _source_ranges(segments: list[dict[str, Any]]) -> list[tuple[int, int]]:
    return sorted((segment["row_start"], segment["row_end"]) for segment in segments)


def _row_in_ranges(row_number: int, ranges: list[tuple[int, int]]) -> bool:
    return any(row_start <= row_number <= row_end for row_start, row_end in ranges)


def _current_integrity_failure(
    symbol_directory: Path,
    symbol: str,
    source: dict[str, Any],
) -> dict[str, Any] | None:
    failure_path = integrity_failure_manifest_path(symbol_directory / source["data_file"])
    document = _read_json_object(failure_path, "integrity-failure manifest")
    if (
        document.get("manifest_type") != "end_of_day_integrity_failures"
        or document.get("symbol") != symbol
        or document.get("pipeline_version") != PIPELINE_VERSION
    ):
        raise EndOfDayReaderError(f"integrity-failure manifest is not current: {failure_path}")
    failures = document.get("failures")
    if not isinstance(failures, list) or document.get("failure_count") != len(failures):
        raise EndOfDayReaderError(f"integrity-failure manifest is malformed: {failure_path}")
    matches = [
        failure
        for failure in failures
        if isinstance(failure, dict) and failure.get("data_file") == source["data_file"]
    ]
    if len(matches) > 1:
        raise EndOfDayReaderError(f"integrity-failure manifest has duplicate entries for {source['data_file']}")
    if not matches:
        return None
    failure = matches[0]
    if (
        failure.get("download_sha256") != source.get("download_sha256")
        or failure.get("adjustment") != source.get("adjustment")
        or failure.get("pipeline_version") != PIPELINE_VERSION
    ):
        raise EndOfDayReaderError(
            f"integrity-failure entry does not match the selected source: {source['data_file']}"
        )
    row_failures = failure.get("row_failures")
    dataset_failures = failure.get("dataset_failures")
    if (
        not isinstance(row_failures, list)
        or not isinstance(dataset_failures, list)
        or failure.get("row_failure_count") != len(row_failures)
        or failure.get("dataset_failure_count") != len(dataset_failures)
    ):
        raise EndOfDayReaderError(f"integrity-failure entry is malformed for {source['data_file']}")
    return failure


def _read_authenticated_source(
    repository_root: Path,
    symbol: str,
    source: dict[str, Any],
) -> tuple[list[dict[str, str]], dict[str, Any], str]:
    symbol_directory = repository_root / "data" / "end_of_day" / symbol
    download_path = symbol_directory / source["data_file"]
    manifest_path = download_manifest_path(download_path)
    manifest = _read_json_object(manifest_path, "download manifest")
    request_identity = {field_name: manifest.get(field_name) for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS}
    if any(not isinstance(value, str) or not value for value in request_identity.values()):
        raise EndOfDayReaderError(f"download manifest has incomplete request identity: {manifest_path}")
    try:
        inspection = inspect_cached_download(download_path, request_identity)
    except EndOfDayPipelineError as error:
        raise EndOfDayReaderError(f"could not inspect frozen download {download_path}: {error}") from error
    if inspection.required_action not in ALLOWED_PIPELINE_STATES:
        raise EndOfDayReaderError(
            f"{download_path}: cannot read frozen download in state {inspection.required_action!r}: "
            f"{inspection.diagnostic}"
        )
    if inspection.download_manifest != manifest:
        raise EndOfDayReaderError(f"cache inspection returned inconsistent metadata for {download_path}")
    if (
        manifest.get("sha256") != source.get("download_sha256")
        or manifest.get("rows") != source.get("rows")
        or manifest.get("adjustment") != source.get("adjustment")
        or manifest.get("file") != source.get("data_file")
    ):
        raise EndOfDayReaderError(f"download manifest does not match correlation source: {download_path}")

    try:
        before_stat = download_path.stat()
        observed_sha256 = authenticate_frozen_download(download_path, manifest)
        authenticated_stat = download_path.stat()
        rows = read_frozen_alpaca_daily_bars(download_path)
        verified_sha256 = authenticate_frozen_download(download_path, manifest)
        after_stat = download_path.stat()
    except (OSError, EndOfDayPipelineError) as error:
        raise EndOfDayReaderError(f"could not read frozen download {download_path}: {error}") from error
    expected_stat = (before_stat.st_size, before_stat.st_mtime_ns)
    if expected_stat != (authenticated_stat.st_size, authenticated_stat.st_mtime_ns) or expected_stat != (
        after_stat.st_size,
        after_stat.st_mtime_ns,
    ):
        raise EndOfDayReaderError(f"frozen download changed while it was being authenticated: {download_path}")
    if observed_sha256 != source["download_sha256"] or verified_sha256 != observed_sha256:
        raise EndOfDayReaderError(f"frozen download does not match correlation source: {download_path}")
    if len(rows) != source["rows"]:
        raise EndOfDayReaderError(f"decompressed row count does not match correlation source: {download_path}")
    return rows, manifest, inspection.required_action


def _validate_segments_against_authenticated_source(
    segments: list[dict[str, Any]],
    source: dict[str, Any],
    manifest: dict[str, Any],
    pipeline_state: str,
    rows: list[dict[str, str]],
) -> None:
    if source.get("validation_pipeline_version") != PIPELINE_VERSION:
        raise EndOfDayReaderError(
            f"correlation source {source['data_file']} does not use validation pipeline v{PIPELINE_VERSION}"
        )
    if source.get("pipeline_state") != pipeline_state:
        raise EndOfDayReaderError(
            f"correlation source {source['data_file']} does not match its current pipeline state"
        )
    segment_manifest_fields = {
        "requested_symbol": "symbol",
        "asof": "asof",
        "request_from": "request_from",
        "request_to": "request_to",
        "feed": "feed",
        "timeframe": "timeframe",
    }
    for segment in segments:
        for segment_field, manifest_field in segment_manifest_fields.items():
            if segment.get(segment_field) != manifest.get(manifest_field):
                raise EndOfDayReaderError(
                    f"correlation segment {segment.get('segment_id')!r} has inconsistent {segment_field}"
                )
        if (
            segment.get("pipeline_state") != pipeline_state
            or segment.get("validation_pipeline_version") != PIPELINE_VERSION
        ):
            raise EndOfDayReaderError(
                f"correlation segment {segment.get('segment_id')!r} has stale validation state"
            )
        if rows[segment["row_start"] - 1].get("t") != segment.get("first_timestamp") or rows[
            segment["row_end"] - 1
        ].get("t") != segment.get("last_timestamp"):
            raise EndOfDayReaderError(
                f"correlation segment {segment.get('segment_id')!r} timestamp bounds do not match its rows"
            )


def _integrity_errors_for_source(
    source_id: str,
    failure: dict[str, Any] | None,
    ranges: list[tuple[int, int]],
) -> tuple[list[dict[str, Any]], dict[int, list[str]]]:
    errors = []
    error_ids_by_row: dict[int, list[str]] = defaultdict(list)
    if failure is None:
        return errors, error_ids_by_row
    for failure_index, row_failure in enumerate(failure["row_failures"], start=1):
        if not isinstance(row_failure, dict) or type(row_failure.get("row_number")) is not int:
            raise EndOfDayReaderError(f"malformed row integrity failure for {source_id}")
        row_number = row_failure["row_number"]
        if not _row_in_ranges(row_number, ranges):
            continue
        error_id = f"{source_id}:row:{row_number}:error:{failure_index}"
        errors.append(
            {
                "error_id": error_id,
                "scope": "row",
                "source_id": source_id,
                **row_failure,
            }
        )
        error_ids_by_row[row_number].append(error_id)
    for failure_index, dataset_failure in enumerate(failure["dataset_failures"], start=1):
        if not isinstance(dataset_failure, dict):
            raise EndOfDayReaderError(f"malformed dataset integrity failure for {source_id}")
        errors.append(
            {
                "error_id": f"{source_id}:dataset:error:{failure_index}",
                "scope": "dataset",
                "source_id": source_id,
                **dataset_failure,
            }
        )
    return errors, error_ids_by_row


def _bar_values(row: dict[str, Any]) -> dict[str, str]:
    return {csv_column: row[alpaca_field] for csv_column, alpaca_field in CSV_COLUMN_ALPACA_FIELD_PAIRS}


def _source_provenance(
    source_id: str,
    symbol: str,
    row_number: int,
    segment: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "symbol": symbol,
        "historical_symbol": segment.get("historical_symbol"),
        "data_file": segment["data_file"],
        "row_number": row_number,
        "segment_id": segment["segment_id"],
    }


def _record_bar_candidate(
    candidates_by_timestamp: dict[str, list[dict[str, Any]]],
    values: dict[str, str],
    provenance: dict[str, Any],
    integrity_error_ids: list[str],
) -> None:
    candidates_by_timestamp[values["timestamp"]].append(
        {
            "values": values,
            "provenance": provenance,
            "integrity_error_ids": list(integrity_error_ids),
        }
    )


def _bar_value_key(candidate: dict[str, Any]) -> tuple[str, ...]:
    values = candidate["values"]
    return tuple(values[field_name] for field_name in BAR_FIELD_NAMES)


def _resolve_bar_candidates(
    candidates_by_timestamp: dict[str, list[dict[str, Any]]],
    selected_timestamps_by_source: dict[str, frozenset[str]],
    *,
    allow_strict_superset_selection: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    bars = []
    resolution_groups: dict[tuple[str, tuple[str, ...], tuple[str, ...]], dict[str, Any]] = {}
    resolved_differing_overlap_count = 0
    for timestamp in sorted(candidates_by_timestamp):
        candidates = candidates_by_timestamp[timestamp]
        keys = {_bar_value_key(candidate) for candidate in candidates}
        chosen_key = next(iter(keys)) if len(keys) == 1 else None
        if chosen_key is None:
            chosen_candidates = []
            selection_rule = None
            if allow_strict_superset_selection:
                maximal_candidates = []
                for candidate in candidates:
                    source_id = candidate["provenance"]["source_id"]
                    coverage = selected_timestamps_by_source[source_id]
                    if not any(
                        coverage < selected_timestamps_by_source[other["provenance"]["source_id"]]
                        for other in candidates
                    ):
                        maximal_candidates.append(candidate)
                maximal_keys = {_bar_value_key(candidate) for candidate in maximal_candidates}
                if len(maximal_keys) == 1:
                    chosen_candidates = maximal_candidates
                    selection_rule = "strict_superset_of_correlated_session_coverage"

            if not chosen_candidates:
                historical_ticker_candidates = [
                    candidate
                    for candidate in candidates
                    if candidate["provenance"].get("symbol") == candidate["provenance"].get("historical_symbol")
                ]
                historical_ticker_source_ids = {
                    candidate["provenance"]["source_id"] for candidate in historical_ticker_candidates
                }
                historical_ticker_keys = {
                    _bar_value_key(candidate) for candidate in historical_ticker_candidates
                }
                if len(historical_ticker_source_ids) == 1 and len(historical_ticker_keys) == 1:
                    chosen_candidates = historical_ticker_candidates
                    selection_rule = "requested_symbol_matches_historical_ticker"

            if not chosen_candidates or selection_rule is None:
                raise EndOfDayReaderError(
                    f"conflicting authenticated bars for security history at {timestamp}; "
                    "no source was preferred"
                )
            chosen_key = _bar_value_key(chosen_candidates[0])
            preferred_source_ids = tuple(
                sorted(
                    {
                        candidate["provenance"]["source_id"]
                        for candidate in chosen_candidates
                        if _bar_value_key(candidate) == chosen_key
                    }
                )
            )
            superseded_source_ids = tuple(
                sorted(
                    {
                        candidate["provenance"]["source_id"]
                        for candidate in candidates
                        if _bar_value_key(candidate) != chosen_key
                    }
                )
            )
            resolved_differing_overlap_count += 1
            resolution_key = (selection_rule, preferred_source_ids, superseded_source_ids)
            resolution = resolution_groups.setdefault(
                resolution_key,
                {
                    "selection_rule": selection_rule,
                    "preferred_source_ids": list(preferred_source_ids),
                    "superseded_source_ids": list(superseded_source_ids),
                    "first_timestamp": timestamp,
                    "last_timestamp": timestamp,
                    "timestamp_count": 0,
                },
            )
            resolution["last_timestamp"] = timestamp
            resolution["timestamp_count"] += 1

        chosen_candidates = [candidate for candidate in candidates if _bar_value_key(candidate) == chosen_key]
        values = chosen_candidates[0]["values"]
        bars.append(
            {
                **values,
                "integrity_error_ids": list(
                    dict.fromkeys(
                        error_id
                        for candidate in chosen_candidates
                        for error_id in candidate["integrity_error_ids"]
                    )
                ),
                "warning_ids": [],
                "provenance": [candidate["provenance"] for candidate in chosen_candidates],
            }
        )
    return bars, {
        "policy": (
            "strict_superset_then_historical_ticker" if allow_strict_superset_selection else "historical_ticker"
        ),
        "resolved_differing_overlap_count": resolved_differing_overlap_count,
        "resolutions": list(resolution_groups.values()),
    }


def _finding_applies_to_selected_rows(
    finding: dict[str, Any],
    selected_filename: str,
    selected_ranges: list[tuple[int, int]],
    selected_timestamps: set[str],
) -> bool:
    data_files = finding.get("data_files")
    if not isinstance(data_files, list) or selected_filename not in data_files:
        return False
    evidence = finding.get("evidence")
    if not isinstance(evidence, dict):
        return True
    evidence_timestamp = evidence.get("timestamp")
    if isinstance(evidence_timestamp, str) and evidence_timestamp not in selected_timestamps:
        return False
    evidence_row_number = evidence.get("row_number")
    if type(evidence_row_number) is int and not _row_in_ranges(evidence_row_number, selected_ranges):
        return False
    return True


def _applicable_warnings(
    master_id: str,
    warning_outputs_by_symbol: dict[str, AuthenticatedWarningPipelineOutputs],
    source_contexts: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    contexts_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for context in source_contexts:
        contexts_by_symbol[context["symbol"]].append(context)

    applicable = []
    total_omitted_findings = 0
    for symbol, warning_outputs in sorted(warning_outputs_by_symbol.items()):
        overlay_documents = (
            ("cross_adjustment", warning_outputs.cross_adjustment),
            ("security_segment_statistics", warning_outputs.security_segment_statistics),
        )
        for overlay_name, document in overlay_documents:
            report = document["report"]
            total_omitted_findings += report["omitted_finding_count"]
            for finding_index, finding in enumerate(report["findings"], start=1):
                if not isinstance(finding, dict):
                    raise EndOfDayReaderError(f"malformed {overlay_name} warning finding for {symbol}")
                if overlay_name == "security_segment_statistics":
                    canonical_security = finding.get("canonical_security")
                    if not isinstance(canonical_security, dict) or not _master_id_matches(
                        canonical_security.get("security_id"), master_id
                    ):
                        continue
                matching_source_ids = []
                for context in contexts_by_symbol[symbol]:
                    if _finding_applies_to_selected_rows(
                        finding,
                        context["data_file"],
                        context["ranges"],
                        context["timestamps"],
                    ):
                        matching_source_ids.append(context["source_id"])
                if not matching_source_ids:
                    continue
                warning_id = f"{symbol}:{overlay_name}:finding:{finding_index}"
                applicable.append(
                    {
                        "warning_id": warning_id,
                        "symbol": symbol,
                        "overlay": overlay_name,
                        "source_ids": sorted(set(matching_source_ids)),
                        **finding,
                    }
                )
    return applicable, total_omitted_findings


def _attach_warning_ids(bars: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> None:
    bars_by_timestamp = {bar["timestamp"]: bar for bar in bars}
    for warning in warnings:
        evidence = warning.get("evidence")
        timestamp = evidence.get("timestamp") if isinstance(evidence, dict) else None
        if isinstance(timestamp, str) and timestamp in bars_by_timestamp:
            bars_by_timestamp[timestamp]["warning_ids"].append(warning["warning_id"])


def read_alpaca_end_of_day_security_history(
    master_id: str,
    adjustment_type: str,
    *,
    repository_root: Path | str | None = None,
) -> dict[str, Any]:
    """Return one authenticated master-ID adjustment history without mutating local data."""
    validate_reader_change_log()
    canonical_id = _canonical_master_id(master_id)
    selected_type = _validated_adjustment_type(adjustment_type)
    root = Path.cwd() if repository_root is None else Path(repository_root)

    try:
        master = authenticate_security_master(root)
    except SecurityCorrelationError as error:
        raise EndOfDayReaderError(f"cannot authenticate security master: {error}") from error
    master_identity = master.identities_by_master_id.get(canonical_id)
    if master_identity is None:
        raise EndOfDayReaderError(f"master ID is not published: {canonical_id}")

    correlations = _authenticated_correlations(root, master, master_identity)
    matching_segments = _matching_segments(correlations, canonical_id, selected_type)
    if not matching_segments:
        raise EndOfDayReaderError(
            f"no correlated {selected_type!r} end-of-day rows found for master ID {canonical_id}"
        )

    correlations_by_symbol = {symbol: correlation for symbol, correlation, _ in matching_segments}
    warnings_by_symbol = _authenticate_warning_outputs(root, correlations_by_symbol)
    segments_by_source: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    correlation_by_source: dict[tuple[str, str], AuthenticatedSecurityCorrelation] = {}
    for symbol, correlation, segment in matching_segments:
        source_key = (symbol, segment["data_file"])
        segments_by_source[source_key].append(segment)
        correlation_by_source[source_key] = correlation

    candidates_by_timestamp: dict[str, list[dict[str, Any]]] = defaultdict(list)
    selected_timestamps_by_source: dict[str, frozenset[str]] = {}
    integrity_errors = []
    sources = []
    result_segments = []
    source_contexts = []
    selected_source_row_count = 0

    for (symbol, filename), segments in sorted(segments_by_source.items()):
        correlation = correlation_by_source[(symbol, filename)]
        source = correlation.sources_by_filename[filename]
        source_id = f"{symbol}/{filename}"
        ranges = _source_ranges(segments)
        symbol_directory = root / "data" / "end_of_day" / symbol
        rows, download_manifest, pipeline_state = _read_authenticated_source(root, symbol, source)
        _validate_segments_against_authenticated_source(
            segments,
            source,
            download_manifest,
            pipeline_state,
            rows,
        )
        failure = _current_integrity_failure(symbol_directory, symbol, source)
        source_errors, error_ids_by_row = _integrity_errors_for_source(source_id, failure, ranges)
        integrity_errors.extend(source_errors)
        selected_timestamps = set()

        for segment in sorted(segments, key=lambda item: (item["row_start"], item["row_end"])):
            selected_source_row_count += segment["row_count"]
            result_segments.append(
                {
                    "source_id": source_id,
                    **segment,
                }
            )
            for row_number in range(segment["row_start"], segment["row_end"] + 1):
                values = _bar_values(rows[row_number - 1])
                selected_timestamps.add(values["timestamp"])
                _record_bar_candidate(
                    candidates_by_timestamp,
                    values,
                    _source_provenance(source_id, symbol, row_number, segment),
                    error_ids_by_row[row_number],
                )

        selected_timestamps_by_source[source_id] = frozenset(selected_timestamps)

        warning_outputs = warnings_by_symbol[symbol]
        sources.append(
            {
                "source_id": source_id,
                "symbol": symbol,
                "data_file": filename,
                "adjustment": source["adjustment"],
                "rows_in_frozen_download": source["rows"],
                "selected_row_count": sum(end - start + 1 for start, end in ranges),
                "download_sha256": source["download_sha256"],
                "download_sha256_authenticated": True,
                "download_manifest": source["download_manifest"],
                "request_identity": {
                    field_name: download_manifest[field_name] for field_name in DOWNLOAD_REQUEST_IDENTITY_FIELDS
                },
                "pipeline_state": pipeline_state,
                "validation_pipeline_version": source["validation_pipeline_version"],
                "correlation_generation_sha256": correlation.receipt["correlation_generation_sha256"],
                "warning_pipeline_generation_sha256": warning_outputs.receipt[
                    "warning_pipeline_generation_sha256"
                ],
            }
        )
        source_contexts.append(
            {
                "source_id": source_id,
                "symbol": symbol,
                "data_file": filename,
                "ranges": ranges,
                "timestamps": selected_timestamps,
            }
        )

    bars, source_selection = _resolve_bar_candidates(
        candidates_by_timestamp,
        selected_timestamps_by_source,
        allow_strict_superset_selection=selected_type != "raw",
    )
    applicable_warnings, source_overlay_omitted_finding_count = _applicable_warnings(
        canonical_id,
        warnings_by_symbol,
        source_contexts,
    )
    _attach_warning_ids(bars, applicable_warnings)
    warning_count = sum(item.get("severity") == "warning" for item in applicable_warnings)
    information_count = sum(item.get("severity") == "information" for item in applicable_warnings)

    correlation_generations = [
        {
            "symbol": symbol,
            "correlation_version": correlation.receipt["correlation_version"],
            "correlation_generation_sha256": correlation.receipt["correlation_generation_sha256"],
            "correlated_output_sha256": correlation.correlated_sha256,
            "uncorrelated_output_sha256": correlation.uncorrelated_sha256,
            "row_partition_complete": True,
        }
        for symbol, correlation in correlations
    ]
    warning_generations = [
        {
            "symbol": symbol,
            "warning_pipeline_version": outputs.receipt["warning_pipeline_version"],
            "warning_pipeline_generation_sha256": outputs.receipt["warning_pipeline_generation_sha256"],
            "cross_adjustment_output_sha256": outputs.cross_adjustment_sha256,
            "security_segment_statistics_output_sha256": (outputs.security_segment_statistics_sha256),
        }
        for symbol, outputs in sorted(warnings_by_symbol.items())
    ]
    return {
        "reader_version": READER_VERSION,
        "query": {
            "master_id": canonical_id,
            "type": selected_type,
        },
        "availability": {
            "online": master_identity["online"],
            "availability_status": master_identity["availability_status"],
        },
        "value_policy": {
            "returned_value_source": "authenticated_frozen_download",
            "rows_with_recorded_integrity_errors_are_preserved": True,
        },
        "repair": {
            "overlay": None,
            "repaired_data": None,
        },
        "source_selection": source_selection,
        "security": master_identity,
        "bar_count": len(bars),
        "selected_source_row_count": selected_source_row_count,
        "bars": bars,
        "segment_count": len(result_segments),
        "segments": result_segments,
        "integrity": {
            "error_count": len(integrity_errors),
            "errors": integrity_errors,
        },
        "warnings": {
            "warning_count": warning_count,
            "information_count": information_count,
            "applicable_finding_count": len(applicable_warnings),
            "source_overlay_omitted_finding_count": source_overlay_omitted_finding_count,
            "findings": applicable_warnings,
        },
        "provenance": {
            "symbols": sorted(correlations_by_symbol),
            "security_master": {
                "security_master_version": master.receipt["security_master_version"],
                "security_master_generation_sha256": master.receipt["security_master_generation_sha256"],
                "index_sha256": master.master_sha256,
                "master_id_resolved": True,
            },
            "source_count": len(sources),
            "sources": sources,
            "correlation_generations": correlation_generations,
            "warning_generations": warning_generations,
        },
    }


read_alpaca_end_of_day_ticker = read_alpaca_end_of_day_security_history

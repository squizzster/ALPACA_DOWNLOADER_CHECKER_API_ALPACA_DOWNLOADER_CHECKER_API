import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

from libs.common.atomic_writes import write_json_document_atomically
from libs.common.file_integrity import calculate_file_sha256

from .artifact_paths import security_master_manifest_path, security_master_path
from .correlation_version import CORRELATION_VERSION
from .errors import SecurityCorrelationError
from .security_identity import (
    SECURITY_ID_SCHEME_VERSION,
    is_security_id,
    security_id_for_alpaca_asset_ids,
)
from .security_master_version import SECURITY_MASTER_VERSION, validate_security_master_change_log

IDENTITY_STRING_LIST_FIELDS = (
    "tickers",
    "names",
    "cusips",
    "exchanges",
    "asset_classes",
    "statuses",
)
IDENTITY_CHANGE_ACTION_TYPES = frozenset(
    {
        "cash_merger",
        "name_change",
        "reorganization",
        "spin_off",
        "stock_and_cash_merger",
        "stock_merger",
    }
)


@dataclass(frozen=True)
class AuthenticatedSecurityMaster:
    receipt: dict[str, Any]
    master: dict[str, Any]
    master_sha256: str
    sources_by_symbol: dict[str, dict[str, Any]]
    identities_by_master_id: dict[str, dict[str, Any]]


def _canonical_json(document: object) -> str:
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _canonical_json_sha256(document: object) -> str:
    return sha256(_canonical_json(document).encode("utf-8")).hexdigest()


def _read_json_object(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(f"could not read {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise SecurityCorrelationError(f"{description} is not a JSON object: {path}")
    return document


def _string_values(record: dict[str, Any], *field_names: str) -> set[str]:
    values = set()
    for field_name in field_names:
        value = record.get(field_name)
        if isinstance(value, str) and value:
            values.add(value)
        elif isinstance(value, list):
            values.update(item for item in value if isinstance(item, str) and item)
    return values


def _figi_identifiers(records: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    identifiers = set()
    for record in records:
        for field_name, value in record.items():
            if "figi" not in field_name.lower():
                continue
            if isinstance(value, str) and value:
                identifiers.add((field_name, value))
            elif isinstance(value, list):
                identifiers.update((field_name, item) for item in value if isinstance(item, str) and item)
    return [{"type": field_name, "value": value} for field_name, value in sorted(identifiers)]


def _candidate_records_by_asset_id(
    segments: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for segment in segments:
        candidates = segment.get("candidate_securities")
        if not isinstance(candidates, list):
            continue
        for candidate in candidates:
            asset_id = candidate.get("asset_id") if isinstance(candidate, dict) else None
            if isinstance(asset_id, str):
                result.setdefault(asset_id, []).append(candidate)
    return result


def _ticker_ownership_periods(
    asset_id: str,
    correlation_report: dict[str, Any],
) -> list[dict[str, Any]]:
    periods = []
    intervals = correlation_report.get("ticker_ownership_intervals")
    if not isinstance(intervals, list):
        return periods
    for interval in intervals:
        if not isinstance(interval, dict) or interval.get("asset_id") != asset_id:
            continue
        periods.append(
            {
                "ticker": interval.get("historical_symbol"),
                "cusip": interval.get("cusip"),
                "start_date_inclusive": interval.get("start_date_inclusive"),
                "end_date_exclusive": interval.get("end_date_exclusive"),
                "start_basis": interval.get("start_basis"),
                "end_basis": interval.get("end_basis"),
            }
        )
    return sorted(periods, key=_canonical_json)


def _alpaca_identity_record(
    asset_id: str,
    correlation_report: dict[str, Any],
    candidate_records: list[dict[str, Any]],
) -> dict[str, Any]:
    lineage = correlation_report["lineage_report"]["lineage"]
    records = list(candidate_records)
    identities = lineage.get("identities")
    if isinstance(identities, list):
        records.extend(
            identity for identity in identities if isinstance(identity, dict) and identity.get("id") == asset_id
        )
    asset_lookups = lineage.get("asset_lookups")
    if isinstance(asset_lookups, list):
        records.extend(
            asset
            for lookup in asset_lookups
            if isinstance(lookup, dict)
            and isinstance((asset := lookup.get("asset")), dict)
            and asset.get("id") == asset_id
        )
    occupancy_evidence = lineage.get("symbol_occupancy_evidence")
    occupancy_records = (
        [
            occupancy
            for occupancy in occupancy_evidence
            if isinstance(occupancy, dict) and occupancy.get("asset_id") == asset_id
        ]
        if isinstance(occupancy_evidence, list)
        else []
    )
    tickers = set()
    names = set()
    cusips = set()
    exchanges = set()
    asset_classes = set()
    statuses = set()
    for record in records:
        tickers.update(_string_values(record, "symbol", "asset_symbol", "lookup_symbols"))
        names.update(_string_values(record, "name", "asset_name"))
        cusips.update(_string_values(record, "cusip", "lookup_cusips"))
        exchanges.update(_string_values(record, "exchange"))
        asset_classes.update(_string_values(record, "class", "asset_class"))
        statuses.update(_string_values(record, "status", "asset_status"))
    for occupancy in occupancy_records:
        tickers.update(_string_values(occupancy, "symbol"))
        cusips.update(_string_values(occupancy, "cusip"))
    return {
        "alpaca_asset_id": asset_id,
        "tickers": sorted(tickers),
        "names": sorted(names),
        "cusips": sorted(cusips),
        "figi_identifiers": _figi_identifiers((*records, *occupancy_records)),
        "exchanges": sorted(exchanges),
        "asset_classes": sorted(asset_classes),
        "statuses": sorted(statuses),
        "ticker_ownership_periods": _ticker_ownership_periods(asset_id, correlation_report),
    }


def _observed_history_ranges(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    adjustments_by_range: dict[tuple[str, str, str], set[str]] = {}
    for segment in segments:
        ticker = segment.get("historical_symbol") or segment.get("requested_symbol")
        first_date = segment.get("first_session_date")
        last_date = segment.get("last_session_date")
        adjustment = segment.get("adjustment")
        if not all(isinstance(value, str) and value for value in (ticker, first_date, last_date)):
            raise SecurityCorrelationError("cannot index a correlated segment without dated ticker history")
        key = (ticker, first_date, last_date)
        if isinstance(adjustment, str) and adjustment:
            adjustments_by_range.setdefault(key, set()).add(adjustment)
    return [
        {
            "ticker": ticker,
            "first_session_date": first_date,
            "last_session_date": last_date,
            "adjustments": sorted(adjustments_by_range[(ticker, first_date, last_date)]),
        }
        for ticker, first_date, last_date in sorted(adjustments_by_range)
    ]


def _identity_events(
    correlation_report: dict[str, Any],
    member_asset_ids: tuple[str, ...],
) -> list[dict[str, Any]]:
    lineage = correlation_report["lineage_report"]["lineage"]
    member_set = set(member_asset_ids)
    links_by_action_id: dict[str, list[dict[str, Any]]] = {}
    occupancy_evidence = lineage.get("symbol_occupancy_evidence")
    if isinstance(occupancy_evidence, list):
        for occupancy in occupancy_evidence:
            if not isinstance(occupancy, dict) or occupancy.get("asset_id") not in member_set:
                continue
            evidence_items = occupancy.get("evidence")
            if not isinstance(evidence_items, list):
                continue
            for evidence in evidence_items:
                if (
                    not isinstance(evidence, dict)
                    or evidence.get("action_type") not in IDENTITY_CHANGE_ACTION_TYPES
                    or not isinstance(evidence.get("action_id"), str)
                ):
                    continue
                links_by_action_id.setdefault(evidence["action_id"], []).append(
                    {
                        "alpaca_asset_id": occupancy["asset_id"],
                        "ticker": evidence.get("symbol") or occupancy.get("symbol"),
                        "cusip": evidence.get("cusip") or occupancy.get("cusip"),
                        "role": evidence.get("role"),
                    }
                )

    edges = correlation_report.get("uuid_lineage_edges")
    if isinstance(edges, list):
        for edge in edges:
            if not isinstance(edge, dict) or not isinstance(edge.get("action_id"), str):
                continue
            for prefix in ("predecessor", "successor"):
                asset_id = edge.get(f"{prefix}_asset_id")
                if asset_id not in member_set:
                    continue
                links_by_action_id.setdefault(edge["action_id"], []).append(
                    {
                        "alpaca_asset_id": asset_id,
                        "ticker": edge.get(f"{prefix}_symbol"),
                        "cusip": edge.get(f"{prefix}_cusip"),
                        "role": prefix,
                    }
                )

    corporate_actions = lineage.get("corporate_actions")
    actions_by_id = (
        {
            action["id"]: action
            for action in corporate_actions
            if isinstance(action, dict) and isinstance(action.get("id"), str)
        }
        if isinstance(corporate_actions, list)
        else {}
    )
    events = []
    for action_id, raw_links in sorted(links_by_action_id.items()):
        action = actions_by_id.get(action_id, {})
        links = sorted({tuple(sorted(link.items())) for link in raw_links}, key=str)
        normalized_links = [dict(link) for link in links]
        action_dates = {
            field_name: value
            for field_name, value in sorted(action.items())
            if field_name.endswith("_date") and isinstance(value, str) and value
        }
        action_tickers = sorted(
            {
                value
                for field_name, value in action.items()
                if "symbol" in field_name and isinstance(value, str) and value
            }
        )
        action_cusips = sorted(
            {
                value
                for field_name, value in action.items()
                if "cusip" in field_name and isinstance(value, str) and value
            }
        )
        events.append(
            {
                "event_id": action_id,
                "action_type": action.get("action_type"),
                "dates": action_dates,
                "tickers": action_tickers,
                "cusips": action_cusips,
                "master_member_links": normalized_links,
            }
        )
    return events


def _source_master_entries(correlation_report: dict[str, Any]) -> list[dict[str, Any]]:
    lineage_report = correlation_report.get("lineage_report")
    lineage = lineage_report.get("lineage") if isinstance(lineage_report, dict) else None
    correlated = correlation_report.get("correlated")
    segments = correlated.get("segments") if isinstance(correlated, dict) else None
    if not isinstance(lineage, dict) or not isinstance(segments, list):
        raise SecurityCorrelationError("cannot build security master without correlated lineage evidence")

    segments_by_master_id: dict[str, list[dict[str, Any]]] = {}
    for segment in segments:
        canonical_security = segment.get("canonical_security") if isinstance(segment, dict) else None
        master_id = canonical_security.get("security_id") if isinstance(canonical_security, dict) else None
        if not is_security_id(master_id):
            raise SecurityCorrelationError("cannot build security master from an invalid master ID")
        segments_by_master_id.setdefault(master_id, []).append(segment)

    entries = []
    for master_id, security_segments in sorted(segments_by_master_id.items()):
        identity_reports = [segment.get("security_identity") for segment in security_segments]
        member_sets = {
            tuple(identity.get("member_alpaca_asset_ids", ()))
            for identity in identity_reports
            if isinstance(identity, dict)
        }
        if len(member_sets) != 1:
            raise SecurityCorrelationError(f"master ID {master_id} has inconsistent Alpaca-ID membership")
        member_asset_ids = next(iter(member_sets))
        if security_id_for_alpaca_asset_ids(member_asset_ids) != master_id:
            raise SecurityCorrelationError(f"master ID {master_id} does not match its Alpaca-ID membership")
        candidate_records = _candidate_records_by_asset_id(security_segments)
        alpaca_identities = [
            _alpaca_identity_record(
                asset_id,
                correlation_report,
                candidate_records.get(asset_id, []),
            )
            for asset_id in member_asset_ids
        ]
        tickers = {
            ticker
            for identity in identity_reports
            if isinstance(identity, dict)
            for ticker in identity.get("member_tickers", ())
            if isinstance(ticker, str) and ticker
        }
        tickers.update(ticker for identity in alpaca_identities for ticker in identity["tickers"])
        observed_ranges = _observed_history_ranges(security_segments)
        entries.append(
            {
                "master_id": master_id,
                "master_id_scheme": {
                    "kind": "sha256",
                    "version": SECURITY_ID_SCHEME_VERSION,
                    "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
                },
                "alpaca_asset_ids": list(member_asset_ids),
                "tickers": sorted(tickers),
                "names": sorted({name for identity in alpaca_identities for name in identity["names"]}),
                "cusips": sorted({cusip for identity in alpaca_identities for cusip in identity["cusips"]}),
                "figi_identifiers": [
                    {"type": identifier_type, "value": value}
                    for identifier_type, value in sorted(
                        {
                            (identifier["type"], identifier["value"])
                            for identity in alpaca_identities
                            for identifier in identity["figi_identifiers"]
                        }
                    )
                ],
                "exchanges": sorted(
                    {value for identity in alpaca_identities for value in identity["exchanges"]}
                ),
                "asset_classes": sorted(
                    {value for identity in alpaca_identities for value in identity["asset_classes"]}
                ),
                "statuses": sorted({value for identity in alpaca_identities for value in identity["statuses"]}),
                "membership_bases": sorted(
                    {
                        identity["basis"]
                        for identity in identity_reports
                        if isinstance(identity, dict) and isinstance(identity.get("basis"), str)
                    }
                ),
                "first_observed_session_date": min(item["first_session_date"] for item in observed_ranges),
                "last_observed_session_date": max(item["last_session_date"] for item in observed_ranges),
                "available_adjustments": sorted(
                    {adjustment for item in observed_ranges for adjustment in item["adjustments"]}
                ),
                "observed_history_ranges": observed_ranges,
                "identity_events": _identity_events(correlation_report, member_asset_ids),
                "alpaca_identities": alpaca_identities,
            }
        )
    return entries


def _source_contribution(
    correlation_report: dict[str, Any],
    correlation_receipt: dict[str, Any],
) -> dict[str, Any]:
    outputs = correlation_receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise SecurityCorrelationError("cannot bind security master to correlation outputs")
    correlated_output = outputs.get("correlated")
    uncorrelated_output = outputs.get("uncorrelated")
    if not isinstance(correlated_output, dict) or not isinstance(uncorrelated_output, dict):
        raise SecurityCorrelationError("cannot bind security master to incomplete correlation outputs")
    return {
        "symbol": correlation_report["symbol"],
        "correlation_version": CORRELATION_VERSION,
        "correlation_generation_sha256": correlation_receipt["correlation_generation_sha256"],
        "correlated_output_sha256": correlated_output["sha256"],
        "uncorrelated_output_sha256": uncorrelated_output["sha256"],
        "identities": _source_master_entries(correlation_report),
    }


def _unique_objects(objects: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_json = {_canonical_json(item): item for item in objects}
    return [by_json[key] for key in sorted(by_json)]


def _merge_alpaca_identity_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records_by_asset_id: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        records_by_asset_id.setdefault(record["alpaca_asset_id"], []).append(record)
    merged = []
    for asset_id, matching_records in sorted(records_by_asset_id.items()):
        item: dict[str, Any] = {"alpaca_asset_id": asset_id}
        for field_name in IDENTITY_STRING_LIST_FIELDS:
            item[field_name] = sorted({value for record in matching_records for value in record[field_name]})
        item["figi_identifiers"] = _unique_objects(
            identifier for record in matching_records for identifier in record["figi_identifiers"]
        )
        item["ticker_ownership_periods"] = _unique_objects(
            period for record in matching_records for period in record["ticker_ownership_periods"]
        )
        merged.append(item)
    return merged


def _merged_observed_ranges(
    matching: list[tuple[dict[str, Any], dict[str, Any]]],
) -> list[dict[str, Any]]:
    adjustments_by_range: dict[tuple[str, str, str, str], set[str]] = {}
    for source, entry in matching:
        for item in entry["observed_history_ranges"]:
            key = (
                source["symbol"],
                item["ticker"],
                item["first_session_date"],
                item["last_session_date"],
            )
            adjustments_by_range.setdefault(key, set()).update(item["adjustments"])
    return [
        {
            "source_symbol": source_symbol,
            "ticker": ticker,
            "first_session_date": first_date,
            "last_session_date": last_date,
            "adjustments": sorted(adjustments_by_range[key]),
        }
        for key in sorted(adjustments_by_range)
        for source_symbol, ticker, first_date, last_date in (key,)
    ]


def _aggregate_security_entries(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    occurrences: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for source in sources:
        for entry in source["identities"]:
            occurrences.setdefault(entry["master_id"], []).append((source, entry))

    securities = []
    for master_id, matching in sorted(occurrences.items()):
        member_sets = {tuple(entry["alpaca_asset_ids"]) for _, entry in matching}
        if len(member_sets) != 1:
            raise SecurityCorrelationError(f"master ID {master_id} has conflicting Alpaca-ID membership")
        alpaca_asset_ids = list(next(iter(member_sets)))
        observed_ranges = _merged_observed_ranges(matching)
        securities.append(
            {
                "master_id": master_id,
                "master_id_scheme": {
                    "kind": "sha256",
                    "version": SECURITY_ID_SCHEME_VERSION,
                    "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
                },
                "alpaca_asset_ids": alpaca_asset_ids,
                "tickers": sorted({value for _, entry in matching for value in entry["tickers"]}),
                "names": sorted({value for _, entry in matching for value in entry["names"]}),
                "cusips": sorted({value for _, entry in matching for value in entry["cusips"]}),
                "figi_identifiers": _unique_objects(
                    identifier for _, entry in matching for identifier in entry["figi_identifiers"]
                ),
                "exchanges": sorted({value for _, entry in matching for value in entry["exchanges"]}),
                "asset_classes": sorted({value for _, entry in matching for value in entry["asset_classes"]}),
                "statuses": sorted({value for _, entry in matching for value in entry["statuses"]}),
                "membership_bases": sorted(
                    {value for _, entry in matching for value in entry["membership_bases"]}
                ),
                "first_observed_session_date": min(item["first_session_date"] for item in observed_ranges),
                "last_observed_session_date": max(item["last_session_date"] for item in observed_ranges),
                "available_adjustments": sorted(
                    {adjustment for item in observed_ranges for adjustment in item["adjustments"]}
                ),
                "observed_history_ranges": observed_ranges,
                "identity_events": _unique_objects(
                    event for _, entry in matching for event in entry["identity_events"]
                ),
                "source_symbols": sorted({source["symbol"] for source, _ in matching}),
                "alpaca_identities": _merge_alpaca_identity_records(
                    [identity for _, entry in matching for identity in entry["alpaca_identities"]]
                ),
            }
        )
    return securities


def _generation_sha256(sources: list[dict[str, Any]], securities: list[dict[str, Any]]) -> str:
    return _canonical_json_sha256(
        {
            "security_master_version": SECURITY_MASTER_VERSION,
            "sources": sources,
            "securities": securities,
        }
    )


def _contains_diagnostic_payload(value: object) -> bool:
    if isinstance(value, dict):
        return any(
            "warning" in str(key).lower() or "error" in str(key).lower() or _contains_diagnostic_payload(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_diagnostic_payload(item) for item in value)
    return False


def _validate_source(source: object) -> None:
    if not isinstance(source, dict) or not isinstance(source.get("symbol"), str) or not source["symbol"]:
        raise SecurityCorrelationError("security master contains an invalid source")
    if source.get("correlation_version") != CORRELATION_VERSION:
        raise SecurityCorrelationError("security master source has an unexpected correlation version")
    for field_name in (
        "correlation_generation_sha256",
        "correlated_output_sha256",
        "uncorrelated_output_sha256",
    ):
        if not is_security_id(source.get(field_name)):
            raise SecurityCorrelationError(f"security master source has invalid {field_name}")
    identities = source.get("identities")
    if not isinstance(identities, list):
        raise SecurityCorrelationError("security master source has no identity list")
    master_ids = []
    for identity in identities:
        if not isinstance(identity, dict):
            raise SecurityCorrelationError("security master source identity is not an object")
        master_id = identity.get("master_id")
        member_ids = identity.get("alpaca_asset_ids")
        if (
            not is_security_id(master_id)
            or not isinstance(member_ids, list)
            or member_ids != sorted(set(member_ids))
            or security_id_for_alpaca_asset_ids(member_ids) != master_id
            or identity.get("master_id_scheme")
            != {
                "kind": "sha256",
                "version": SECURITY_ID_SCHEME_VERSION,
                "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
            }
        ):
            raise SecurityCorrelationError("security master source has an invalid master identity")
        master_ids.append(master_id)
    if master_ids != sorted(set(master_ids)):
        raise SecurityCorrelationError("security master source identities are not uniquely ordered")


def _validate_master_document(
    document: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if document.get("manifest_type") != "end_of_day_security_master_index":
        raise SecurityCorrelationError("security master has an unexpected manifest type")
    if document.get("security_master_version") != SECURITY_MASTER_VERSION:
        raise SecurityCorrelationError("security master has an unexpected version")
    sources = document.get("sources")
    securities = document.get("securities")
    if not isinstance(sources, list) or not isinstance(securities, list):
        raise SecurityCorrelationError("security master has no sources or securities list")
    if _contains_diagnostic_payload(sources) or _contains_diagnostic_payload(securities):
        raise SecurityCorrelationError("security master identity records contain diagnostics")
    try:
        for source in sources:
            _validate_source(source)
        if sources != sorted(sources, key=lambda item: item["symbol"]):
            raise SecurityCorrelationError("security master sources are not canonically ordered")
        symbols = [source["symbol"] for source in sources]
        if len(symbols) != len(set(symbols)):
            raise SecurityCorrelationError("security master contains duplicate source symbols")
        expected_securities = _aggregate_security_entries(sources)
    except (KeyError, TypeError, ValueError) as error:
        raise SecurityCorrelationError(f"security master structure is invalid: {error}") from error
    if securities != expected_securities:
        raise SecurityCorrelationError("security master aggregate does not match its sources")
    expected_generation = _generation_sha256(sources, securities)
    if document.get("security_master_generation_sha256") != expected_generation:
        raise SecurityCorrelationError("security master generation digest is inconsistent")
    if document.get("source_count") != len(sources) or document.get("master_id_count") != len(securities):
        raise SecurityCorrelationError("security master counts are inconsistent")
    sources_by_symbol = {source["symbol"]: source for source in sources}
    identities_by_master_id = {}
    for security in securities:
        master_id = security.get("master_id")
        member_ids = security.get("alpaca_asset_ids")
        if (
            not is_security_id(master_id)
            or not isinstance(member_ids, list)
            or security_id_for_alpaca_asset_ids(member_ids) != master_id
            or master_id in identities_by_master_id
        ):
            raise SecurityCorrelationError("security master contains an invalid master identity")
        identities_by_master_id[master_id] = security
    return sources_by_symbol, identities_by_master_id


def _authenticate_optional_security_master(
    repository_root: Path,
) -> AuthenticatedSecurityMaster | None:
    output_path = security_master_path(repository_root)
    receipt_path = security_master_manifest_path(repository_root)
    if not output_path.exists() and not receipt_path.exists():
        return None
    if not output_path.exists() or not receipt_path.exists():
        raise SecurityCorrelationError("security-master index and receipt are an orphaned pair")
    receipt = _read_json_object(receipt_path, "security-master receipt")
    if (
        receipt.get("manifest_type") != "end_of_day_security_master_index_receipt"
        or receipt.get("security_master_version") != SECURITY_MASTER_VERSION
    ):
        raise SecurityCorrelationError("security-master receipt has an unexpected identity")
    output_receipt = receipt.get("output")
    if not isinstance(output_receipt, dict) or output_receipt.get("file") != output_path.name:
        raise SecurityCorrelationError("security-master receipt does not bind its index")
    if (
        not is_security_id(output_receipt.get("sha256"))
        or output_receipt.get("publication_sha256_verified") is not True
    ):
        raise SecurityCorrelationError("security-master receipt has invalid output authentication")
    try:
        output_stat = output_path.stat()
    except OSError as error:
        raise SecurityCorrelationError(f"security-master index is unavailable: {error}") from error
    if (output_stat.st_size, output_stat.st_mtime_ns) != (
        output_receipt.get("bytes"),
        output_receipt.get("mtime_ns"),
    ):
        raise SecurityCorrelationError("security-master index stat does not match its receipt")
    observed_sha256 = calculate_file_sha256(output_path)
    if observed_sha256 != output_receipt.get("sha256"):
        raise SecurityCorrelationError("security-master index SHA-256 does not match its receipt")
    document = _read_json_object(output_path, "security-master index")
    sources_by_symbol, identities_by_master_id = _validate_master_document(document)
    verified_sha256 = calculate_file_sha256(output_path)
    final_stat = output_path.stat()
    if verified_sha256 != observed_sha256 or (final_stat.st_size, final_stat.st_mtime_ns) != (
        output_stat.st_size,
        output_stat.st_mtime_ns,
    ):
        raise SecurityCorrelationError("security-master index changed while it was read")
    if receipt.get("security_master_generation_sha256") != document.get("security_master_generation_sha256"):
        raise SecurityCorrelationError("security-master receipt has an inconsistent generation digest")
    if (
        receipt.get("source_count") != document["source_count"]
        or receipt.get("master_id_count") != document["master_id_count"]
    ):
        raise SecurityCorrelationError("security-master receipt has inconsistent counts")
    return AuthenticatedSecurityMaster(
        receipt=receipt,
        master=document,
        master_sha256=observed_sha256,
        sources_by_symbol=sources_by_symbol,
        identities_by_master_id=identities_by_master_id,
    )


def authenticate_security_master(
    repository_root: Path | str,
) -> AuthenticatedSecurityMaster:
    """Authenticate and return the published master identity index."""
    validate_security_master_change_log()
    authenticated = _authenticate_optional_security_master(Path(repository_root))
    if authenticated is None:
        raise SecurityCorrelationError("security master is not published")
    return authenticated


def lookup_security_master_identity(
    repository_root: Path | str,
    master_id: str,
) -> dict[str, Any]:
    """Return one authenticated identity record selected only by exact master ID."""
    canonical_id = master_id.lower() if isinstance(master_id, str) else master_id
    if not is_security_id(canonical_id):
        raise ValueError(f"invalid master ID: {master_id!r}")
    authenticated = authenticate_security_master(repository_root)
    identity = authenticated.identities_by_master_id.get(canonical_id)
    if identity is None:
        raise SecurityCorrelationError(f"master ID is not published: {canonical_id}")
    return deepcopy(identity)


def publish_security_master_update(
    repository_root: Path | str,
    correlation_report: dict[str, Any],
    correlation_receipt: dict[str, Any],
) -> dict[str, Any]:
    """Replace one symbol contribution and atomically publish the aggregate index."""
    validate_security_master_change_log()
    root = Path(repository_root)
    existing = _authenticate_optional_security_master(root)
    source = _source_contribution(correlation_report, correlation_receipt)
    sources_by_symbol = dict(existing.sources_by_symbol) if existing is not None else {}
    sources_by_symbol[source["symbol"]] = source
    sources = [sources_by_symbol[symbol] for symbol in sorted(sources_by_symbol)]
    securities = _aggregate_security_entries(sources)
    generation_sha256 = _generation_sha256(sources, securities)
    if existing is not None and existing.receipt.get("security_master_generation_sha256") == generation_sha256:
        return {**existing.receipt, "publication_status": "cached"}

    output_path = security_master_path(root)
    receipt_path = security_master_manifest_path(root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    publication_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    document = {
        "manifest_type": "end_of_day_security_master_index",
        "security_master_version": SECURITY_MASTER_VERSION,
        "security_master_generation_sha256": generation_sha256,
        "created_at": publication_timestamp,
        "source_count": len(sources),
        "master_id_count": len(securities),
        "sources": sources,
        "securities": securities,
    }
    if _contains_diagnostic_payload(document["securities"]):
        raise SecurityCorrelationError("refusing to publish diagnostics in the security master")
    write_json_document_atomically(output_path, document, compact=False)
    published_sha256 = calculate_file_sha256(output_path)
    if calculate_file_sha256(output_path) != published_sha256:
        raise SecurityCorrelationError("published security-master index failed SHA-256 verification")
    output_stat = output_path.stat()
    receipt = {
        "manifest_type": "end_of_day_security_master_index_receipt",
        "security_master_version": SECURITY_MASTER_VERSION,
        "security_master_generation_sha256": generation_sha256,
        "created_at": publication_timestamp,
        "source_count": len(sources),
        "master_id_count": len(securities),
        "output": {
            "file": output_path.name,
            "sha256": published_sha256,
            "bytes": output_stat.st_size,
            "mtime_ns": output_stat.st_mtime_ns,
            "publication_sha256_verified": True,
        },
    }
    write_json_document_atomically(receipt_path, receipt, compact=False)
    return {**receipt, "publication_status": "published"}

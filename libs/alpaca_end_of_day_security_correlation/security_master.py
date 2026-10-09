import json
import threading
from collections.abc import Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

from libs.common.atomic_writes import write_json_document_atomically
from libs.common.file_integrity import calculate_file_sha256

from .artifact_paths import (
    legacy_security_master_manifest_path,
    legacy_security_master_path,
    security_master_directory,
    security_master_manifest_path,
    security_master_path,
)
from .correlation_version import CORRELATION_VERSION
from .errors import SecurityCorrelationError
from .security_identity import (
    SECURITY_ID_SCHEME_VERSION,
    is_security_id,
    security_id_for_alpaca_asset_ids,
)
from .security_master_version import SECURITY_MASTER_VERSION, validate_security_master_change_log
from .settings import SYMBOL_PATTERN

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

_SECURITY_MASTER_PUBLICATION_LOCK = threading.Lock()

ONLINE_AVAILABILITY_STATUS = "online"
OFFLINE_AVAILABILITY_STATUS = "offline"
LINEAGE_GAP_AVAILABILITY_STATUS = "lineage_gap"
SECURITY_AVAILABILITY_STATUSES = frozenset(
    {
        ONLINE_AVAILABILITY_STATUS,
        OFFLINE_AVAILABILITY_STATUS,
        LINEAGE_GAP_AVAILABILITY_STATUS,
    }
)


def _availability_is_valid(online: object, availability_status: object) -> bool:
    return (
        type(online) is bool
        and availability_status in SECURITY_AVAILABILITY_STATUSES
        and online == (availability_status == ONLINE_AVAILABILITY_STATUS)
    )


@dataclass(frozen=True)
class AuthenticatedSecurityMaster:
    receipt: dict[str, Any]
    master: dict[str, Any]
    master_sha256: str
    sources_by_symbol: Mapping[str, dict[str, Any]]
    identities_by_master_id: Mapping[str, dict[str, Any]]


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


def _source_master_entries(
    correlation_report: dict[str, Any],
    online_by_master_id: Mapping[str, bool],
    availability_status_by_master_id: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
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
    resolved_availability_status = (
        {
            master_id: (ONLINE_AVAILABILITY_STATUS if online else OFFLINE_AVAILABILITY_STATUS)
            for master_id, online in online_by_master_id.items()
        }
        if availability_status_by_master_id is None
        else availability_status_by_master_id
    )
    if (
        set(online_by_master_id) != set(segments_by_master_id)
        or set(resolved_availability_status) != set(segments_by_master_id)
        or any(
            not _availability_is_valid(
                online_by_master_id[master_id],
                resolved_availability_status[master_id],
            )
            for master_id in segments_by_master_id
        )
    ):
        raise SecurityCorrelationError(
            "security-master availability must contain one consistent status for every correlated master ID"
        )
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
                "online": online_by_master_id[master_id],
                "availability_status": resolved_availability_status[master_id],
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
    online_by_master_id: Mapping[str, bool] | None,
    availability_status_by_master_id: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    outputs = correlation_receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise SecurityCorrelationError("cannot bind security master to correlation outputs")
    correlated_output = outputs.get("correlated")
    uncorrelated_output = outputs.get("uncorrelated")
    if not isinstance(correlated_output, dict) or not isinstance(uncorrelated_output, dict):
        raise SecurityCorrelationError("cannot bind security master to incomplete correlation outputs")
    segments = correlation_report.get("correlated", {}).get("segments", [])
    master_ids = {
        segment.get("canonical_security", {}).get("security_id")
        for segment in segments
        if isinstance(segment, dict) and isinstance(segment.get("canonical_security"), dict)
    }
    resolved_online = (
        {master_id: False for master_id in master_ids if is_security_id(master_id)}
        if online_by_master_id is None
        else online_by_master_id
    )
    return {
        "symbol": correlation_report["symbol"],
        "correlation_version": CORRELATION_VERSION,
        "correlation_generation_sha256": correlation_receipt["correlation_generation_sha256"],
        "correlated_output_sha256": correlated_output["sha256"],
        "uncorrelated_output_sha256": uncorrelated_output["sha256"],
        "identities": _source_master_entries(
            correlation_report,
            resolved_online,
            availability_status_by_master_id,
        ),
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


def _aggregate_security_entries(
    sources: list[dict[str, Any]],
    *,
    include_online: bool = True,
    include_availability_status: bool = True,
) -> list[dict[str, Any]]:
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
        security = {
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
            "membership_bases": sorted({value for _, entry in matching for value in entry["membership_bases"]}),
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
        if include_online:
            security["online"] = all(entry["online"] for _, entry in matching)
        if include_availability_status:
            statuses = {entry["availability_status"] for _, entry in matching}
            if OFFLINE_AVAILABILITY_STATUS in statuses:
                security["availability_status"] = OFFLINE_AVAILABILITY_STATUS
            elif LINEAGE_GAP_AVAILABILITY_STATUS in statuses:
                security["availability_status"] = LINEAGE_GAP_AVAILABILITY_STATUS
            else:
                security["availability_status"] = ONLINE_AVAILABILITY_STATUS
        securities.append(security)
    return securities


LEGACY_SECURITY_MASTER_VERSION = 1
SHARDED_SECURITY_MASTER_VERSION_WITHOUT_ONLINE = 2
SHARDED_SECURITY_MASTER_VERSION_WITHOUT_AVAILABILITY_STATUS = 3
SECURITY_MASTER_OBJECT_VERSION = 1
SECURITY_MASTER_INVENTORY_VERSION = 1
INVENTORY_BUCKET_PREFIX_HEX_CHARACTERS = 2

_INVENTORY_CONFIGURATION = {
    "source": {
        "key_field": "symbol",
        "object_field": "source",
        "object_manifest_type": "end_of_day_security_master_source",
        "bucket_manifest_type": "end_of_day_security_master_source_inventory_bucket",
        "inventory_manifest_type": "end_of_day_security_master_source_inventory",
        "object_directory": Path("objects") / "sources",
        "inventory_directory": Path("inventories") / "sources",
    },
    "security": {
        "key_field": "master_id",
        "object_field": "security",
        "object_manifest_type": "end_of_day_security_master_identity",
        "bucket_manifest_type": "end_of_day_security_master_identity_inventory_bucket",
        "inventory_manifest_type": "end_of_day_security_master_identity_inventory",
        "object_directory": Path("objects") / "securities",
        "inventory_directory": Path("inventories") / "securities",
    },
}


def _is_sha256(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _bucket_prefix(key: str) -> str:
    return sha256(key.encode("utf-8")).hexdigest()[:INVENTORY_BUCKET_PREFIX_HEX_CHARACTERS]


def _serialized_json_bytes(document: dict[str, Any], *, compact: bool) -> bytes:
    if compact:
        serialized = json.dumps(document, separators=(",", ":"))
    else:
        serialized = json.dumps(document, indent=2)
    return (serialized + "\n").encode("utf-8")


def _publish_immutable_document(
    master_directory: Path,
    relative_directory: Path,
    document: dict[str, Any],
    *,
    filename_prefix: str | None = None,
) -> dict[str, Any]:
    serialized = _serialized_json_bytes(document, compact=True)
    digest = sha256(serialized).hexdigest()
    filename = f"{digest}.json" if filename_prefix is None else f"{filename_prefix}.{digest}.json"
    relative_path = relative_directory / filename
    destination = master_directory / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        try:
            existing_bytes = destination.read_bytes()
        except OSError as error:
            raise SecurityCorrelationError(
                f"could not authenticate immutable security-master object {destination}: {error}"
            ) from error
        if existing_bytes != serialized:
            raise SecurityCorrelationError(
                f"immutable security-master object has unexpected content: {destination}"
            )
    else:
        write_json_document_atomically(destination, document, compact=True)
        try:
            published_bytes = destination.read_bytes()
        except OSError as error:
            raise SecurityCorrelationError(
                f"could not verify immutable security-master object {destination}: {error}"
            ) from error
        if published_bytes != serialized:
            raise SecurityCorrelationError(
                f"published immutable security-master object is inconsistent: {destination}"
            )
    return {
        "file": relative_path.as_posix(),
        "sha256": digest,
        "bytes": len(serialized),
    }


def _validated_relative_path(
    master_directory: Path,
    value: object,
    expected_directory: Path,
    description: str,
) -> Path:
    if not isinstance(value, str):
        raise SecurityCorrelationError(f"{description} has no filename")
    relative_path = Path(value)
    if (
        relative_path.is_absolute()
        or relative_path.suffix != ".json"
        or any(part in {"", ".", ".."} for part in relative_path.parts)
        or relative_path.parts[: len(expected_directory.parts)] != expected_directory.parts
    ):
        raise SecurityCorrelationError(f"{description} has an unsafe filename: {value!r}")
    destination = master_directory / relative_path
    try:
        destination.resolve(strict=False).relative_to(master_directory.resolve())
    except (OSError, ValueError) as error:
        raise SecurityCorrelationError(f"{description} escapes the security-master directory") from error
    return destination


def _validate_object_descriptor(
    master_directory: Path,
    descriptor: object,
    expected_directory: Path,
    description: str,
) -> Path:
    if not isinstance(descriptor, dict):
        raise SecurityCorrelationError(f"{description} descriptor is not an object")
    path = _validated_relative_path(
        master_directory,
        descriptor.get("file"),
        expected_directory,
        description,
    )
    digest = descriptor.get("sha256")
    byte_count = descriptor.get("bytes")
    if not _is_sha256(digest) or type(byte_count) is not int or byte_count <= 0:
        raise SecurityCorrelationError(f"{description} descriptor has invalid integrity metadata")
    if not path.name.endswith(f".{digest}.json") and path.name != f"{digest}.json":
        raise SecurityCorrelationError(f"{description} filename does not contain its SHA-256")
    return path


def _read_authenticated_immutable_document(
    master_directory: Path,
    descriptor: object,
    expected_directory: Path,
    description: str,
) -> dict[str, Any]:
    path = _validate_object_descriptor(
        master_directory,
        descriptor,
        expected_directory,
        description,
    )
    try:
        before_stat = path.stat()
        serialized = path.read_bytes()
        after_stat = path.stat()
    except OSError as error:
        raise SecurityCorrelationError(f"could not read {description} {path}: {error}") from error
    if (before_stat.st_size, before_stat.st_mtime_ns) != (
        after_stat.st_size,
        after_stat.st_mtime_ns,
    ):
        raise SecurityCorrelationError(f"{description} changed while it was read: {path}")
    if len(serialized) != descriptor.get("bytes") or sha256(serialized).hexdigest() != descriptor.get("sha256"):
        raise SecurityCorrelationError(f"{description} failed SHA-256 authentication: {path}")
    try:
        document = json.loads(serialized)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise SecurityCorrelationError(f"{description} is not valid JSON: {path}: {error}") from error
    if not isinstance(document, dict):
        raise SecurityCorrelationError(f"{description} is not a JSON object: {path}")
    return document


def _publish_source_object(master_directory: Path, source: dict[str, Any]) -> dict[str, Any]:
    _validate_source(source)
    symbol = source["symbol"]
    document = {
        "manifest_type": _INVENTORY_CONFIGURATION["source"]["object_manifest_type"],
        "manifest_version": SECURITY_MASTER_OBJECT_VERSION,
        "security_master_version": SECURITY_MASTER_VERSION,
        "symbol": symbol,
        "source": source,
    }
    return _publish_immutable_document(
        master_directory,
        _INVENTORY_CONFIGURATION["source"]["object_directory"] / _bucket_prefix(symbol),
        document,
        filename_prefix=symbol,
    )


def _validate_security_entry(
    security: object,
    *,
    require_online: bool = True,
    require_availability_status: bool = True,
) -> None:
    if not isinstance(security, dict) or _contains_diagnostic_payload(security):
        raise SecurityCorrelationError("security master contains an invalid security identity")
    master_id = security.get("master_id")
    member_ids = security.get("alpaca_asset_ids")
    source_symbols = security.get("source_symbols")
    if (
        not is_security_id(master_id)
        or not isinstance(member_ids, list)
        or member_ids != sorted(set(member_ids))
        or security_id_for_alpaca_asset_ids(member_ids) != master_id
        or security.get("master_id_scheme")
        != {
            "kind": "sha256",
            "version": SECURITY_ID_SCHEME_VERSION,
            "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
        }
        or not isinstance(source_symbols, list)
        or not source_symbols
        or source_symbols != sorted(set(source_symbols))
        or any(not isinstance(symbol, str) or not SYMBOL_PATTERN.fullmatch(symbol) for symbol in source_symbols)
        or (require_online and type(security.get("online")) is not bool)
        or (
            require_availability_status
            and not _availability_is_valid(
                security.get("online"),
                security.get("availability_status"),
            )
        )
    ):
        raise SecurityCorrelationError("security master contains an invalid master identity")


def _publish_security_object(master_directory: Path, security: dict[str, Any]) -> dict[str, Any]:
    _validate_security_entry(security)
    master_id = security["master_id"]
    document = {
        "manifest_type": _INVENTORY_CONFIGURATION["security"]["object_manifest_type"],
        "manifest_version": SECURITY_MASTER_OBJECT_VERSION,
        "security_master_version": SECURITY_MASTER_VERSION,
        "master_id": master_id,
        "security": security,
    }
    return _publish_immutable_document(
        master_directory,
        _INVENTORY_CONFIGURATION["security"]["object_directory"] / _bucket_prefix(master_id),
        document,
        filename_prefix=master_id,
    )


def _publish_inventory_bucket(
    master_directory: Path,
    kind: str,
    prefix: str,
    object_descriptors_by_key: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    configuration = _INVENTORY_CONFIGURATION[kind]
    key_field = configuration["key_field"]
    entries = [
        {key_field: key, "object": object_descriptors_by_key[key]} for key in sorted(object_descriptors_by_key)
    ]
    document = {
        "manifest_type": configuration["bucket_manifest_type"],
        "manifest_version": SECURITY_MASTER_INVENTORY_VERSION,
        "security_master_version": SECURITY_MASTER_VERSION,
        "key_kind": key_field,
        "bucket_prefix": prefix,
        "entry_count": len(entries),
        "entries": entries,
    }
    descriptor = _publish_immutable_document(
        master_directory,
        configuration["inventory_directory"] / "buckets" / prefix,
        document,
    )
    return {"prefix": prefix, "entry_count": len(entries), **descriptor}


def _publish_inventory_root(
    master_directory: Path,
    kind: str,
    bucket_descriptors_by_prefix: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    configuration = _INVENTORY_CONFIGURATION[kind]
    buckets = [bucket_descriptors_by_prefix[prefix] for prefix in sorted(bucket_descriptors_by_prefix)]
    entry_count = sum(bucket["entry_count"] for bucket in buckets)
    document = {
        "manifest_type": configuration["inventory_manifest_type"],
        "manifest_version": SECURITY_MASTER_INVENTORY_VERSION,
        "security_master_version": SECURITY_MASTER_VERSION,
        "key_kind": configuration["key_field"],
        "bucket_scheme": {
            "kind": "sha256_prefix",
            "prefix_hex_characters": INVENTORY_BUCKET_PREFIX_HEX_CHARACTERS,
        },
        "entry_count": entry_count,
        "bucket_count": len(buckets),
        "buckets": buckets,
    }
    descriptor = _publish_immutable_document(
        master_directory,
        configuration["inventory_directory"] / "roots",
        document,
    )
    return {"entry_count": entry_count, **descriptor}


def _publish_complete_inventory(
    master_directory: Path,
    kind: str,
    object_descriptors_by_key: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    entries_by_prefix: dict[str, dict[str, dict[str, Any]]] = {}
    for key, descriptor in object_descriptors_by_key.items():
        entries_by_prefix.setdefault(_bucket_prefix(key), {})[key] = descriptor
    bucket_descriptors = {
        prefix: _publish_inventory_bucket(master_directory, kind, prefix, entries)
        for prefix, entries in sorted(entries_by_prefix.items())
    }
    return _publish_inventory_root(master_directory, kind, bucket_descriptors)


def _generation_sha256(
    source_inventory: dict[str, Any],
    security_inventory: dict[str, Any],
    *,
    security_master_version: int = SECURITY_MASTER_VERSION,
) -> str:
    return _canonical_json_sha256(
        {
            "security_master_version": security_master_version,
            "source_inventory": source_inventory,
            "security_inventory": security_inventory,
        }
    )


def _legacy_generation_sha256(
    sources: list[dict[str, Any]],
    securities: list[dict[str, Any]],
) -> str:
    return _canonical_json_sha256(
        {
            "security_master_version": LEGACY_SECURITY_MASTER_VERSION,
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


def _validate_source(
    source: object,
    *,
    require_online: bool = True,
    require_availability_status: bool = True,
) -> None:
    if (
        not isinstance(source, dict)
        or not isinstance(source.get("symbol"), str)
        or not SYMBOL_PATTERN.fullmatch(source["symbol"])
    ):
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
            or (require_online and type(identity.get("online")) is not bool)
            or (
                require_availability_status
                and not _availability_is_valid(
                    identity.get("online"),
                    identity.get("availability_status"),
                )
            )
        ):
            raise SecurityCorrelationError("security master source has an invalid master identity")
        master_ids.append(master_id)
    if master_ids != sorted(set(master_ids)):
        raise SecurityCorrelationError("security master source identities are not uniquely ordered")


def _validate_legacy_master_document(
    document: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if document.get("manifest_type") != "end_of_day_security_master_index":
        raise SecurityCorrelationError("security master has an unexpected manifest type")
    if document.get("security_master_version") != LEGACY_SECURITY_MASTER_VERSION:
        raise SecurityCorrelationError("security master has an unexpected version")
    sources = document.get("sources")
    securities = document.get("securities")
    if not isinstance(sources, list) or not isinstance(securities, list):
        raise SecurityCorrelationError("security master has no sources or securities list")
    if _contains_diagnostic_payload(sources) or _contains_diagnostic_payload(securities):
        raise SecurityCorrelationError("security master identity records contain diagnostics")
    try:
        for source in sources:
            _validate_source(
                source,
                require_online=False,
                require_availability_status=False,
            )
        if sources != sorted(sources, key=lambda item: item["symbol"]):
            raise SecurityCorrelationError("security master sources are not canonically ordered")
        symbols = [source["symbol"] for source in sources]
        if len(symbols) != len(set(symbols)):
            raise SecurityCorrelationError("security master contains duplicate source symbols")
        expected_securities = _aggregate_security_entries(
            sources,
            include_online=False,
            include_availability_status=False,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise SecurityCorrelationError(f"security master structure is invalid: {error}") from error
    if securities != expected_securities:
        raise SecurityCorrelationError("security master aggregate does not match its sources")
    expected_generation = _legacy_generation_sha256(sources, securities)
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


def _validate_inventory_root(
    master_directory: Path,
    kind: str,
    descriptor: object,
    *,
    security_master_version: int = SECURITY_MASTER_VERSION,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    configuration = _INVENTORY_CONFIGURATION[kind]
    document = _read_authenticated_immutable_document(
        master_directory,
        descriptor,
        configuration["inventory_directory"] / "roots",
        f"security-master {kind} inventory",
    )
    expected_identity = {
        "manifest_type": configuration["inventory_manifest_type"],
        "manifest_version": SECURITY_MASTER_INVENTORY_VERSION,
        "security_master_version": security_master_version,
        "key_kind": configuration["key_field"],
        "bucket_scheme": {
            "kind": "sha256_prefix",
            "prefix_hex_characters": INVENTORY_BUCKET_PREFIX_HEX_CHARACTERS,
        },
    }
    for field_name, expected_value in expected_identity.items():
        if document.get(field_name) != expected_value:
            raise SecurityCorrelationError(f"security-master {kind} inventory has unexpected {field_name}")
    buckets = document.get("buckets")
    if not isinstance(buckets, list):
        raise SecurityCorrelationError(f"security-master {kind} inventory has no bucket list")
    prefixes = []
    bucket_descriptors = {}
    entry_count = 0
    for bucket in buckets:
        if not isinstance(bucket, dict):
            raise SecurityCorrelationError(f"security-master {kind} inventory bucket is not an object")
        prefix = bucket.get("prefix")
        if (
            not isinstance(prefix, str)
            or len(prefix) != INVENTORY_BUCKET_PREFIX_HEX_CHARACTERS
            or any(character not in "0123456789abcdef" for character in prefix)
            or type(bucket.get("entry_count")) is not int
            or bucket["entry_count"] <= 0
        ):
            raise SecurityCorrelationError(f"security-master {kind} inventory has an invalid bucket")
        _validate_object_descriptor(
            master_directory,
            bucket,
            configuration["inventory_directory"] / "buckets" / prefix,
            f"security-master {kind} inventory bucket",
        )
        prefixes.append(prefix)
        bucket_descriptors[prefix] = bucket
        entry_count += bucket["entry_count"]
    if prefixes != sorted(set(prefixes)):
        raise SecurityCorrelationError(f"security-master {kind} inventory buckets are not canonical")
    if (
        document.get("bucket_count") != len(buckets)
        or document.get("entry_count") != entry_count
        or not isinstance(descriptor, dict)
        or descriptor.get("entry_count") != entry_count
    ):
        raise SecurityCorrelationError(f"security-master {kind} inventory counts are inconsistent")
    return document, bucket_descriptors


class _InventoryObjectMapping(Mapping[str, dict[str, Any]]):
    def __init__(
        self,
        master_directory: Path,
        kind: str,
        inventory_descriptor: dict[str, Any],
        inventory_document: dict[str, Any],
        bucket_descriptors: dict[str, dict[str, Any]],
        *,
        source_mapping: "_InventoryObjectMapping | None" = None,
        security_master_version: int = SECURITY_MASTER_VERSION,
    ) -> None:
        self.master_directory = master_directory
        self.kind = kind
        self.configuration = _INVENTORY_CONFIGURATION[kind]
        self.inventory_descriptor = inventory_descriptor
        self.inventory_document = inventory_document
        self.bucket_descriptors = bucket_descriptors
        self.source_mapping = source_mapping
        self.security_master_version = security_master_version
        self._bucket_entries_cache: dict[str, dict[str, dict[str, Any]]] = {}
        self._object_cache: dict[str, dict[str, Any]] = {}

    def __len__(self) -> int:
        return self.inventory_document["entry_count"]

    def __iter__(self) -> Iterator[str]:
        keys = [key for prefix in sorted(self.bucket_descriptors) for key in self._bucket_entries(prefix)]
        yield from sorted(keys)

    def _bucket_entries(self, prefix: str) -> dict[str, dict[str, Any]]:
        cached = self._bucket_entries_cache.get(prefix)
        if cached is not None:
            return cached
        descriptor = self.bucket_descriptors.get(prefix)
        if descriptor is None:
            self._bucket_entries_cache[prefix] = {}
            return {}
        document = _read_authenticated_immutable_document(
            self.master_directory,
            descriptor,
            self.configuration["inventory_directory"] / "buckets" / prefix,
            f"security-master {self.kind} inventory bucket {prefix}",
        )
        expected_identity = {
            "manifest_type": self.configuration["bucket_manifest_type"],
            "manifest_version": SECURITY_MASTER_INVENTORY_VERSION,
            "security_master_version": self.security_master_version,
            "key_kind": self.configuration["key_field"],
            "bucket_prefix": prefix,
        }
        for field_name, expected_value in expected_identity.items():
            if document.get(field_name) != expected_value:
                raise SecurityCorrelationError(
                    f"security-master {self.kind} inventory bucket has unexpected {field_name}"
                )
        raw_entries = document.get("entries")
        if not isinstance(raw_entries, list) or document.get("entry_count") != len(raw_entries):
            raise SecurityCorrelationError(f"security-master {self.kind} inventory bucket count is inconsistent")
        key_field = self.configuration["key_field"]
        entries = {}
        keys = []
        for entry in raw_entries:
            key = entry.get(key_field) if isinstance(entry, dict) else None
            object_descriptor = entry.get("object") if isinstance(entry, dict) else None
            valid_key = isinstance(key, str) and (
                is_security_id(key) if self.kind == "security" else SYMBOL_PATTERN.fullmatch(key) is not None
            )
            if not valid_key or _bucket_prefix(key) != prefix:
                raise SecurityCorrelationError(
                    f"security-master {self.kind} inventory bucket contains an invalid key"
                )
            _validate_object_descriptor(
                self.master_directory,
                object_descriptor,
                self.configuration["object_directory"] / prefix,
                f"security-master {self.kind} object",
            )
            entries[key] = object_descriptor
            keys.append(key)
        if keys != sorted(set(keys)) or len(keys) != descriptor["entry_count"]:
            raise SecurityCorrelationError(
                f"security-master {self.kind} inventory bucket entries are not canonical"
            )
        self._bucket_entries_cache[prefix] = entries
        return entries

    def object_descriptor(self, key: str) -> dict[str, Any]:
        try:
            return self._bucket_entries(_bucket_prefix(key))[key]
        except KeyError as error:
            raise KeyError(key) from error

    def __getitem__(self, key: str) -> dict[str, Any]:
        if not isinstance(key, str):
            raise KeyError(key)
        cached = self._object_cache.get(key)
        if cached is not None:
            return cached
        descriptor = self.object_descriptor(key)
        prefix = _bucket_prefix(key)
        document = _read_authenticated_immutable_document(
            self.master_directory,
            descriptor,
            self.configuration["object_directory"] / prefix,
            f"security-master {self.kind} object {key}",
        )
        expected_identity = {
            "manifest_type": self.configuration["object_manifest_type"],
            "manifest_version": SECURITY_MASTER_OBJECT_VERSION,
            "security_master_version": self.security_master_version,
            self.configuration["key_field"]: key,
        }
        for field_name, expected_value in expected_identity.items():
            if document.get(field_name) != expected_value:
                raise SecurityCorrelationError(f"security-master {self.kind} object has unexpected {field_name}")
        value = document.get(self.configuration["object_field"])
        require_online = self.security_master_version >= 3
        require_availability_status = self.security_master_version >= 4
        if self.kind == "source":
            _validate_source(
                value,
                require_online=require_online,
                require_availability_status=require_availability_status,
            )
            if value["symbol"] != key:
                raise SecurityCorrelationError("security-master source object has inconsistent symbol")
        else:
            _validate_security_entry(
                value,
                require_online=require_online,
                require_availability_status=require_availability_status,
            )
            if value["master_id"] != key:
                raise SecurityCorrelationError("security-master identity object has inconsistent master ID")
            if self.source_mapping is None:
                raise SecurityCorrelationError("security-master identity has no source inventory")
            try:
                contributing_sources = [self.source_mapping[symbol] for symbol in value["source_symbols"]]
                matching = [
                    security
                    for security in _aggregate_security_entries(
                        contributing_sources,
                        include_online=require_online,
                        include_availability_status=require_availability_status,
                    )
                    if security["master_id"] == key
                ]
            except (KeyError, TypeError, ValueError) as error:
                raise SecurityCorrelationError(
                    f"security-master identity {key} cannot authenticate its source contributions"
                ) from error
            if matching != [value]:
                raise SecurityCorrelationError(
                    f"security-master identity {key} does not match its source contributions"
                )
        self._object_cache[key] = value
        return value


def _validate_master_document(
    master_directory: Path,
    document: dict[str, Any],
    *,
    security_master_version: int = SECURITY_MASTER_VERSION,
) -> tuple[_InventoryObjectMapping, _InventoryObjectMapping]:
    if document.get("manifest_type") != "end_of_day_security_master_index":
        raise SecurityCorrelationError("security master has an unexpected manifest type")
    if document.get("security_master_version") != security_master_version:
        raise SecurityCorrelationError("security master has an unexpected version")
    source_inventory = document.get("source_inventory")
    security_inventory = document.get("security_inventory")
    source_document, source_buckets = _validate_inventory_root(
        master_directory,
        "source",
        source_inventory,
        security_master_version=security_master_version,
    )
    security_document, security_buckets = _validate_inventory_root(
        master_directory,
        "security",
        security_inventory,
        security_master_version=security_master_version,
    )
    if (
        document.get("source_count") != source_document["entry_count"]
        or document.get("master_id_count") != security_document["entry_count"]
    ):
        raise SecurityCorrelationError("security master counts are inconsistent")
    expected_generation = _generation_sha256(
        source_inventory,
        security_inventory,
        security_master_version=security_master_version,
    )
    if document.get("security_master_generation_sha256") != expected_generation:
        raise SecurityCorrelationError("security master generation digest is inconsistent")
    source_mapping = _InventoryObjectMapping(
        master_directory,
        "source",
        source_inventory,
        source_document,
        source_buckets,
        security_master_version=security_master_version,
    )
    security_mapping = _InventoryObjectMapping(
        master_directory,
        "security",
        security_inventory,
        security_document,
        security_buckets,
        source_mapping=source_mapping,
        security_master_version=security_master_version,
    )
    return source_mapping, security_mapping


def _authenticate_optional_security_master(
    repository_root: Path,
    *,
    security_master_version: int = SECURITY_MASTER_VERSION,
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
        or receipt.get("security_master_version") != security_master_version
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
    sources_by_symbol, identities_by_master_id = _validate_master_document(
        output_path.parent,
        document,
        security_master_version=security_master_version,
    )
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


def _replace_inventory_entries(
    master_directory: Path,
    kind: str,
    existing_mapping: _InventoryObjectMapping | None,
    updates: dict[str, dict[str, Any] | None],
) -> dict[str, Any]:
    bucket_descriptors = dict(existing_mapping.bucket_descriptors) if existing_mapping is not None else {}
    updates_by_prefix: dict[str, dict[str, dict[str, Any] | None]] = {}
    for key, descriptor in updates.items():
        updates_by_prefix.setdefault(_bucket_prefix(key), {})[key] = descriptor
    for prefix, bucket_updates in sorted(updates_by_prefix.items()):
        current_entries = dict(existing_mapping._bucket_entries(prefix)) if existing_mapping is not None else {}
        for key, descriptor in bucket_updates.items():
            if descriptor is None:
                current_entries.pop(key, None)
            else:
                current_entries[key] = descriptor
        if current_entries:
            bucket_descriptors[prefix] = _publish_inventory_bucket(
                master_directory,
                kind,
                prefix,
                current_entries,
            )
        else:
            bucket_descriptors.pop(prefix, None)
    return _publish_inventory_root(master_directory, kind, bucket_descriptors)


def _publish_security_master_root(
    repository_root: Path,
    source_inventory: dict[str, Any],
    security_inventory: dict[str, Any],
    *,
    existing: AuthenticatedSecurityMaster | None,
) -> dict[str, Any]:
    generation_sha256 = _generation_sha256(source_inventory, security_inventory)
    if existing is not None and existing.receipt.get("security_master_generation_sha256") == generation_sha256:
        return {**existing.receipt, "publication_status": "cached"}
    output_path = security_master_path(repository_root)
    receipt_path = security_master_manifest_path(repository_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    publication_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    document = {
        "manifest_type": "end_of_day_security_master_index",
        "security_master_version": SECURITY_MASTER_VERSION,
        "security_master_generation_sha256": generation_sha256,
        "created_at": publication_timestamp,
        "source_count": source_inventory["entry_count"],
        "master_id_count": security_inventory["entry_count"],
        "source_inventory": source_inventory,
        "security_inventory": security_inventory,
    }
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
        "source_count": source_inventory["entry_count"],
        "master_id_count": security_inventory["entry_count"],
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


def _publish_security_master_snapshot(
    repository_root: Path,
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    symbols = [source.get("symbol") for source in sources]
    if symbols != sorted(set(symbols)):
        raise SecurityCorrelationError("security-master snapshot sources are not canonical")
    for source in sources:
        _validate_source(source)
    securities = _aggregate_security_entries(sources)
    master_directory = security_master_directory(repository_root)
    master_directory.mkdir(parents=True, exist_ok=True)
    source_objects = {source["symbol"]: _publish_source_object(master_directory, source) for source in sources}
    security_objects = {
        security["master_id"]: _publish_security_object(master_directory, security) for security in securities
    }
    source_inventory = _publish_complete_inventory(master_directory, "source", source_objects)
    security_inventory = _publish_complete_inventory(master_directory, "security", security_objects)
    return _publish_security_master_root(
        repository_root,
        source_inventory,
        security_inventory,
        existing=None,
    )


def _authenticate_legacy_security_master(
    repository_root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    output_path = legacy_security_master_path(repository_root)
    receipt_path = legacy_security_master_manifest_path(repository_root)
    if not output_path.is_file() or not receipt_path.is_file():
        raise SecurityCorrelationError("legacy security-master index and receipt are unavailable")
    receipt = _read_json_object(receipt_path, "legacy security-master receipt")
    if (
        receipt.get("manifest_type") != "end_of_day_security_master_index_receipt"
        or receipt.get("security_master_version") != LEGACY_SECURITY_MASTER_VERSION
    ):
        raise SecurityCorrelationError("legacy security-master receipt has an unexpected identity")
    output = receipt.get("output")
    if not isinstance(output, dict) or output.get("file") != output_path.name:
        raise SecurityCorrelationError("legacy security-master receipt does not bind its index")
    stat = output_path.stat()
    if (stat.st_size, stat.st_mtime_ns) != (output.get("bytes"), output.get("mtime_ns")):
        raise SecurityCorrelationError("legacy security-master index stat does not match its receipt")
    observed_sha256 = calculate_file_sha256(output_path)
    if observed_sha256 != output.get("sha256"):
        raise SecurityCorrelationError("legacy security-master index SHA-256 does not match its receipt")
    document = _read_json_object(output_path, "legacy security-master index")
    _validate_legacy_master_document(document)
    if (
        receipt.get("security_master_generation_sha256") != document.get("security_master_generation_sha256")
        or receipt.get("source_count") != document.get("source_count")
        or receipt.get("master_id_count") != document.get("master_id_count")
    ):
        raise SecurityCorrelationError("legacy security-master receipt is inconsistent")
    return document, receipt


def migrate_legacy_security_master(repository_root: Path | str) -> dict[str, Any]:
    """Publish a current sharded master from one authenticated version-1 snapshot."""
    validate_security_master_change_log()
    root = Path(repository_root)
    with _SECURITY_MASTER_PUBLICATION_LOCK:
        existing = _authenticate_optional_security_master(root)
        if existing is not None:
            return {**existing.receipt, "publication_status": "cached"}
        legacy_document, _ = _authenticate_legacy_security_master(root)
        migrated_sources = deepcopy(legacy_document["sources"])
        for source in migrated_sources:
            for identity in source["identities"]:
                identity["online"] = False
                identity["availability_status"] = OFFLINE_AVAILABILITY_STATUS
        receipt = _publish_security_master_snapshot(root, migrated_sources)
        authenticated = _authenticate_optional_security_master(root)
        if authenticated is None:
            raise SecurityCorrelationError("sharded security master was not published")
        if (
            len(authenticated.sources_by_symbol) != legacy_document["source_count"]
            or len(authenticated.identities_by_master_id) != legacy_document["master_id_count"]
        ):
            raise SecurityCorrelationError("sharded security-master migration changed its counts")
        if [
            {
                **source,
                "identities": [
                    {
                        **identity,
                        "online": False,
                        "availability_status": OFFLINE_AVAILABILITY_STATUS,
                    }
                    for identity in source["identities"]
                ],
            }
            for source in legacy_document["sources"]
        ] != list(authenticated.sources_by_symbol.values()):
            raise SecurityCorrelationError("sharded security-master migration changed its contents")
        return receipt


def migrate_sharded_security_master_online_status(
    repository_root: Path | str,
    online_by_symbol_and_master_id: Mapping[str, Mapping[str, bool]],
) -> dict[str, Any]:
    """Migrate an authenticated version-2 sharded master to persisted online booleans."""
    validate_security_master_change_log()
    root = Path(repository_root)
    with _SECURITY_MASTER_PUBLICATION_LOCK:
        output_path = security_master_path(root)
        document = _read_json_object(output_path, "security-master index")
        if document.get("security_master_version") == SECURITY_MASTER_VERSION:
            existing = _authenticate_optional_security_master(root)
            if existing is None:
                raise SecurityCorrelationError("security master is not published")
            return {**existing.receipt, "publication_status": "cached"}
        if document.get("security_master_version") != SHARDED_SECURITY_MASTER_VERSION_WITHOUT_ONLINE:
            raise SecurityCorrelationError(
                "online-status migration requires a version-2 sharded security master"
            )
        previous = _authenticate_optional_security_master(
            root,
            security_master_version=SHARDED_SECURITY_MASTER_VERSION_WITHOUT_ONLINE,
        )
        if previous is None:
            raise SecurityCorrelationError("version-2 sharded security master is not published")

        migrated_sources = []
        expected_status_symbols = set()
        for symbol in previous.sources_by_symbol:
            source = deepcopy(previous.sources_by_symbol[symbol])
            expected_master_ids = {identity["master_id"] for identity in source["identities"]}
            if expected_master_ids:
                expected_status_symbols.add(symbol)
                statuses = online_by_symbol_and_master_id.get(symbol)
                if (
                    not isinstance(statuses, Mapping)
                    or set(statuses) != expected_master_ids
                    or any(type(value) is not bool for value in statuses.values())
                ):
                    raise SecurityCorrelationError(
                        f"online-status migration has incomplete status for source {symbol}"
                    )
                for identity in source["identities"]:
                    identity["online"] = statuses[identity["master_id"]]
                    identity["availability_status"] = (
                        ONLINE_AVAILABILITY_STATUS if identity["online"] else OFFLINE_AVAILABILITY_STATUS
                    )
            migrated_sources.append(source)
        if set(online_by_symbol_and_master_id) != expected_status_symbols:
            raise SecurityCorrelationError("online-status migration contains unknown or empty source symbols")

        receipt = _publish_security_master_snapshot(root, migrated_sources)
        authenticated = _authenticate_optional_security_master(root)
        if authenticated is None:
            raise SecurityCorrelationError("current security master was not published")
        if len(authenticated.sources_by_symbol) != len(previous.sources_by_symbol) or len(
            authenticated.identities_by_master_id
        ) != len(previous.identities_by_master_id):
            raise SecurityCorrelationError("online-status migration changed security-master counts")
        return receipt


def migrate_sharded_security_master_availability_status(
    repository_root: Path | str,
    availability_status_by_master_id: Mapping[str, str],
) -> dict[str, Any]:
    """Migrate a version-3 master to explicit provider-neutral availability classifications."""
    validate_security_master_change_log()
    root = Path(repository_root)
    with _SECURITY_MASTER_PUBLICATION_LOCK:
        output_path = security_master_path(root)
        document = _read_json_object(output_path, "security-master index")
        if document.get("security_master_version") == SECURITY_MASTER_VERSION:
            existing = _authenticate_optional_security_master(root)
            if existing is None:
                raise SecurityCorrelationError("security master is not published")
            return {**existing.receipt, "publication_status": "cached"}
        if (
            document.get("security_master_version")
            != SHARDED_SECURITY_MASTER_VERSION_WITHOUT_AVAILABILITY_STATUS
        ):
            raise SecurityCorrelationError(
                "availability-status migration requires a version-3 sharded security master"
            )
        previous = _authenticate_optional_security_master(
            root,
            security_master_version=SHARDED_SECURITY_MASTER_VERSION_WITHOUT_AVAILABILITY_STATUS,
        )
        if previous is None:
            raise SecurityCorrelationError("version-3 sharded security master is not published")

        expected_master_ids = set(previous.identities_by_master_id)
        if set(availability_status_by_master_id) != expected_master_ids or any(
            status not in SECURITY_AVAILABILITY_STATUSES for status in availability_status_by_master_id.values()
        ):
            raise SecurityCorrelationError(
                "availability-status migration requires one valid status for every master ID"
            )

        migrated_sources = []
        for symbol in previous.sources_by_symbol:
            source = deepcopy(previous.sources_by_symbol[symbol])
            for identity in source["identities"]:
                master_id = identity["master_id"]
                status = availability_status_by_master_id[master_id]
                previously_online = identity["online"]
                if status == ONLINE_AVAILABILITY_STATUS and not previously_online:
                    raise SecurityCorrelationError(
                        f"availability-status migration cannot promote offline master ID {master_id}"
                    )
                if status == LINEAGE_GAP_AVAILABILITY_STATUS and not previously_online:
                    raise SecurityCorrelationError(
                        f"availability-status migration cannot classify previously offline master ID {master_id} "
                        "as a newly detected lineage gap"
                    )
                identity["online"] = status == ONLINE_AVAILABILITY_STATUS
                identity["availability_status"] = status
            migrated_sources.append(source)

        receipt = _publish_security_master_snapshot(root, migrated_sources)
        authenticated = _authenticate_optional_security_master(root)
        if authenticated is None:
            raise SecurityCorrelationError("current security master was not published")
        if len(authenticated.sources_by_symbol) != len(previous.sources_by_symbol) or len(
            authenticated.identities_by_master_id
        ) != len(previous.identities_by_master_id):
            raise SecurityCorrelationError("availability-status migration changed security-master counts")
        return receipt


def _publish_security_master_online_status(
    repository_root: Path | str,
    online_by_master_id: Mapping[str, bool],
    *,
    availability_status_by_master_id: Mapping[str, str] | None,
    expected_security_object_sha256_by_master_id: Mapping[str, str],
    expected_source_object_sha256_by_symbol: Mapping[str, str],
) -> dict[str, Any]:
    root = Path(repository_root)
    existing = _authenticate_optional_security_master(root)
    if existing is None:
        raise SecurityCorrelationError("security master is not published")
    resolved_availability_status = (
        {
            master_id: (ONLINE_AVAILABILITY_STATUS if online else OFFLINE_AVAILABILITY_STATUS)
            for master_id, online in online_by_master_id.items()
        }
        if availability_status_by_master_id is None
        else availability_status_by_master_id
    )
    if (
        set(online_by_master_id) != set(expected_security_object_sha256_by_master_id)
        or set(resolved_availability_status) != set(online_by_master_id)
        or any(not is_security_id(master_id) for master_id in online_by_master_id)
        or any(
            not _availability_is_valid(
                online_by_master_id[master_id],
                resolved_availability_status[master_id],
            )
            for master_id in online_by_master_id
        )
    ):
        raise SecurityCorrelationError("availability publication has an invalid master-ID status set")
    if not online_by_master_id:
        return {**existing.receipt, "publication_status": "cached"}

    securities = {}
    contributing_symbols = set()
    for master_id in sorted(online_by_master_id):
        try:
            security = existing.identities_by_master_id[master_id]
            descriptor = existing.identities_by_master_id.object_descriptor(master_id)
        except KeyError as error:
            raise SecurityCorrelationError(
                f"cannot publish online status for unpublished master ID {master_id}"
            ) from error
        if descriptor.get("sha256") != expected_security_object_sha256_by_master_id[master_id]:
            raise SecurityCorrelationError(
                f"security-master identity {master_id} changed during online evaluation"
            )
        securities[master_id] = security
        contributing_symbols.update(security["source_symbols"])

    if set(expected_source_object_sha256_by_symbol) != contributing_symbols:
        raise SecurityCorrelationError("online-status publication has an incomplete source snapshot")
    for symbol in sorted(contributing_symbols):
        try:
            descriptor = existing.sources_by_symbol.object_descriptor(symbol)
        except KeyError as error:
            raise SecurityCorrelationError(
                f"online-status publication refers to unknown source {symbol}"
            ) from error
        if descriptor.get("sha256") != expected_source_object_sha256_by_symbol[symbol]:
            raise SecurityCorrelationError(f"security-master source {symbol} changed during online evaluation")

    updated_sources = {}
    for symbol in sorted(contributing_symbols):
        source = deepcopy(existing.sources_by_symbol[symbol])
        matching_master_ids = {
            identity["master_id"]
            for identity in source["identities"]
            if identity["master_id"] in online_by_master_id
        }
        for identity in source["identities"]:
            master_id = identity["master_id"]
            if master_id in matching_master_ids:
                identity["online"] = online_by_master_id[master_id]
                identity["availability_status"] = resolved_availability_status[master_id]
        updated_sources[symbol] = source

    master_directory = security_master_directory(root)
    source_object_updates = {
        symbol: _publish_source_object(master_directory, source)
        for symbol, source in updated_sources.items()
        if source != existing.sources_by_symbol[symbol]
    }
    source_inventory = (
        _replace_inventory_entries(
            master_directory,
            "source",
            existing.sources_by_symbol,
            source_object_updates,
        )
        if source_object_updates
        else existing.master["source_inventory"]
    )

    security_object_updates = {}
    for master_id, old_security in securities.items():
        contributing_sources = [
            updated_sources.get(symbol, existing.sources_by_symbol[symbol])
            for symbol in old_security["source_symbols"]
        ]
        matching = [
            security
            for security in _aggregate_security_entries(contributing_sources)
            if security["master_id"] == master_id
        ]
        if (
            len(matching) != 1
            or matching[0]["online"] != online_by_master_id[master_id]
            or matching[0]["availability_status"] != resolved_availability_status[master_id]
        ):
            raise SecurityCorrelationError(
                f"could not rebuild availability for unique master identity {master_id}"
            )
        if matching[0] != old_security:
            security_object_updates[master_id] = _publish_security_object(
                master_directory,
                matching[0],
            )
    security_inventory = (
        _replace_inventory_entries(
            master_directory,
            "security",
            existing.identities_by_master_id,
            security_object_updates,
        )
        if security_object_updates
        else existing.master["security_inventory"]
    )
    return _publish_security_master_root(
        root,
        source_inventory,
        security_inventory,
        existing=existing,
    )


def publish_security_master_online_status(
    repository_root: Path | str,
    online_by_master_id: Mapping[str, bool],
    *,
    availability_status_by_master_id: Mapping[str, str] | None = None,
    expected_security_object_sha256_by_master_id: Mapping[str, str],
    expected_source_object_sha256_by_symbol: Mapping[str, str],
) -> dict[str, Any]:
    """Commit reader-tested availability if the evaluated shards are unchanged."""
    validate_security_master_change_log()
    with _SECURITY_MASTER_PUBLICATION_LOCK:
        return _publish_security_master_online_status(
            repository_root,
            online_by_master_id,
            availability_status_by_master_id=availability_status_by_master_id,
            expected_security_object_sha256_by_master_id=(expected_security_object_sha256_by_master_id),
            expected_source_object_sha256_by_symbol=expected_source_object_sha256_by_symbol,
        )


def _publish_security_master_update(
    repository_root: Path | str,
    correlation_report: dict[str, Any],
    correlation_receipt: dict[str, Any],
    online_by_master_id: Mapping[str, bool] | None,
) -> dict[str, Any]:
    root = Path(repository_root)
    existing = _authenticate_optional_security_master(root)
    source = _source_contribution(
        correlation_report,
        correlation_receipt,
        online_by_master_id,
    )
    symbol = source["symbol"]
    old_source = existing.sources_by_symbol.get(symbol) if existing is not None else None
    if old_source == source:
        return {**existing.receipt, "publication_status": "cached"}

    master_directory = security_master_directory(root)
    master_directory.mkdir(parents=True, exist_ok=True)
    source_descriptor = _publish_source_object(master_directory, source)
    source_inventory = _replace_inventory_entries(
        master_directory,
        "source",
        existing.sources_by_symbol if existing is not None else None,
        {symbol: source_descriptor},
    )

    old_identities = old_source.get("identities", []) if isinstance(old_source, dict) else []
    new_identities = source["identities"]
    security_inventory = (
        existing.master["security_inventory"]
        if existing is not None
        else _publish_complete_inventory(master_directory, "security", {})
    )
    if old_identities != new_identities:
        affected_master_ids = sorted({identity["master_id"] for identity in (*old_identities, *new_identities)})
        old_master_ids = {identity["master_id"] for identity in old_identities}
        new_master_ids = {identity["master_id"] for identity in new_identities}
        security_updates: dict[str, dict[str, Any] | None] = {}
        for master_id in affected_master_ids:
            old_security = existing.identities_by_master_id.get(master_id) if existing is not None else None
            source_symbols = set(old_security["source_symbols"] if old_security is not None else [])
            if master_id in old_master_ids:
                source_symbols.discard(symbol)
            if master_id in new_master_ids:
                source_symbols.add(symbol)
            if not source_symbols:
                security_updates[master_id] = None
                continue
            contributing_sources = [
                source if source_symbol == symbol else existing.sources_by_symbol[source_symbol]
                for source_symbol in sorted(source_symbols)
            ]
            matching = [
                security
                for security in _aggregate_security_entries(contributing_sources)
                if security["master_id"] == master_id
            ]
            if len(matching) != 1:
                raise SecurityCorrelationError(f"could not rebuild unique security-master identity {master_id}")
            security_updates[master_id] = _publish_security_object(master_directory, matching[0])
        security_inventory = _replace_inventory_entries(
            master_directory,
            "security",
            existing.identities_by_master_id if existing is not None else None,
            security_updates,
        )

    return _publish_security_master_root(
        root,
        source_inventory,
        security_inventory,
        existing=existing,
    )


def publish_security_master_update(
    repository_root: Path | str,
    correlation_report: dict[str, Any],
    correlation_receipt: dict[str, Any],
    *,
    online_by_master_id: Mapping[str, bool] | None = None,
) -> dict[str, Any]:
    """Replace one symbol contribution and its persisted online state atomically."""
    validate_security_master_change_log()
    with _SECURITY_MASTER_PUBLICATION_LOCK:
        return _publish_security_master_update(
            repository_root,
            correlation_report,
            correlation_receipt,
            online_by_master_id,
        )

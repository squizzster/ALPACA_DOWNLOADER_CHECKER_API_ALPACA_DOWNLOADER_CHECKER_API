"""Select and publish public security metadata from one accepted catalogue snapshot."""

import gzip
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from . import ADJUSTMENTS, CatalogChangedError
from .standalone import _date

EXTENSIONS = (
    "asset_snapshots",
    "corporate_actions",
    "cross_ticker_listing_changes",
    "historical_listings",
    "listing_exchange_changes",
    "trade_observations",
)
RECORDS_FILE = "securities.jsonl.gz"
MANIFEST_FILE = "manifest.json"
HEX64 = re.compile("[0-9a-f]{64}")
ORDER = {
    "ticker_ranges": ("first_session", "last_session", "ticker"),
    "ticker_ownership_intervals": ("start_inclusive", "end_exclusive", "ticker", "cusip"),
    "observations": ("field", "effective_from", "effective_through", "observed_at", "value"),
    "external_identifiers": ("kind", "value"),
}


@dataclass(frozen=True)
class Selection:
    public_ids: tuple[str, ...]
    series_ids: dict[str, str]
    selection: dict
    catalogue_generation: str


def _read_selector(path):
    with path.open(encoding="utf-8") as stream:
        document = json.load(stream)
    if not isinstance(document, dict):
        raise ValueError("pricing export selector must be a JSON object")
    return document


def _date_filter(value):
    if not isinstance(value, dict) or value.get("inclusive") is not True:
        raise ValueError("pricing export must declare its inclusive date filter")
    start = _date(value["start"], "price filter start")
    end = _date(value["end"], "price filter end")
    if start and end and start > end:
        raise ValueError("pricing export date filter is reversed")
    return {"start_inclusive": start and start.isoformat(), "end_inclusive": end and end.isoformat()}


def _csv_pair(path, document):
    if (
        path.name != "final_manifest.json"
        or document.get("format") != "alpaca_public_id_eod_csv"
        or type(document.get("schema_version")) is not int
        or document["schema_version"] not in (1, 2)
        or not document.get("completed_at")
        or not isinstance(document.get("identities"), dict)
    ):
        raise ValueError("expected a completed native CSV final_manifest.json")
    scope = {
        "kind": "csv",
        "manifest_ref": str(path),
        "status": "complete",
        "date_filter": _date_filter(document.get("requested_range")),
    }
    return document["identities"].keys(), {}, scope


def _bloodhound_pair(path, document):
    if (
        not path.name.lower().endswith(".parquet.report.json")
        or document.get("application") != "alpaca_to_bloodhound"
        or document.get("input_format") != "alpaca_online_catalogue"
        or document.get("status") not in ("complete", "partial")
        or document.get("output_written") is not True
        or not isinstance(document.get("histories"), list)
        or not isinstance(document.get("output_sha256"), str)
        or not HEX64.fullmatch(document["output_sha256"])
    ):
        raise ValueError("expected a completed native online Bloodhound conversion report")
    series_ids = {}
    for history in document["histories"]:
        if history["status"] == "outside_date_filter":
            continue
        if history["status"] != "exported":
            raise ValueError("unrecognized history status in Bloodhound report")
        public_id = history["public_id"]
        identifier = history["ohlcv_series_id"]
        if type(identifier) is not int or not 0 < identifier < 1 << 63:
            raise ValueError("Bloodhound ohlcv_series_id must be a positive signed int64")
        if public_id in series_ids:
            raise ValueError("duplicate exported public_id in Bloodhound report")
        series_ids[public_id] = str(identifier)
    scope = {
        "kind": "bloodhound",
        "report_ref": str(path),
        "status": document["status"],
        "output_sha256": document["output_sha256"],
        "date_filter": _date_filter(document.get("date_filter")),
    }
    return series_ids.keys(), series_ids, scope


def select(data, *, public_ids=None, symbols=None, csv_manifest=None, bloodhound_report=None):
    selectors = (public_ids, symbols, csv_manifest, bloodhound_report)
    if sum(value is not None for value in selectors) > 1:
        raise ValueError("choose only one metadata selector")
    series_ids = {}
    generation = data.catalog_generation()
    if csv_manifest is not None or bloodhound_report is not None:
        path = Path(csv_manifest if csv_manifest is not None else bloodhound_report).resolve()
        document = _read_selector(path)
        if (
            not isinstance(document.get("catalogue"), dict)
            or document["catalogue"].get("generation") != generation
        ):
            raise CatalogChangedError("paired price export requires its matching catalogue generation")
        selector = _csv_pair if csv_manifest is not None else _bloodhound_pair
        try:
            ids, series_ids, scope = selector(path, document)
        except (KeyError, TypeError) as error:
            raise ValueError("malformed pricing export selector") from error
        # Only the named selector owns membership. No CSV digest, price file, or omitted history is opened.
        for public_id in ids:
            if data.get_security(public_id).public_id != public_id:
                raise ValueError("noncanonical public_id in pricing export selector")
    elif public_ids is not None:
        if not isinstance(public_ids, (tuple, list)) or not public_ids:
            raise ValueError("public_ids must be a nonempty sequence")
        ids = {data.get_security(public_id).public_id for public_id in public_ids}
        scope = {"kind": "public_ids"}
    elif symbols is not None:
        if not isinstance(symbols, (tuple, list)) or not symbols:
            raise ValueError("symbols must be a nonempty sequence")
        ids = set()
        for symbol in symbols:
            matches = data.resolve_ticker(symbol)
            if not matches:
                raise ValueError("ticker is not available")
            ids.update(match.public_id for match in matches)
        scope = {"kind": "public_ids"}
    else:
        ids = data._securities.keys()
        scope = {"kind": "all_online"}
    return Selection(tuple(sorted(ids)), series_ids, scope, generation)


def available_extensions(versions):
    required = {"cross_ticker_listing_changes": 3, "historical_listings": 3, "listing_exchange_changes": 2}
    return tuple(key for key in EXTENSIONS if key not in required or required[key] in versions)


def plan(selected, group_count, *, catalogue_only=False, extensions=EXTENSIONS):
    return {
        "catalogue_generation": selected.catalogue_generation,
        "selection": dict(selected.selection),
        "identities": len(selected.public_ids),
        "groups": group_count,
        "extensions": [] if catalogue_only else list(extensions),
    }


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":"))


def _ordered(section, values):
    # Catalogue ranges and observations have a different publication order.
    # All fields of these public facts participate in their wire order.
    if len(values) < 2:
        return values

    fields = ORDER[section]
    return sorted(values, key=lambda item: tuple(item[field] or "" for field in fields))


def _record(data, public_id, selected, enrichments, extensions):
    # Project the catalogue's already-accepted public body; full coverage belongs to
    # the identity even when the parent price export selected only part of its history.
    public = data._records[public_id]["public"]
    coverage = {
        "first_session": public["first_session"],
        "last_session": public["last_session"],
        "session_count": public["coverage"]["session_count"],
    }
    if gaps := public["coverage"]["explained_missing_sessions"]:
        coverage["explained_missing_sessions"] = gaps
    if tuple(public["adjustments"]) != ADJUSTMENTS:
        coverage["adjustments"] = public["adjustments"]
    record = {"public_id": public_id, "coverage": coverage}
    for section in ("ticker_ranges", "ticker_ownership_intervals", "observations"):
        record[section] = _ordered(section, public[section])
    if identifiers := public["external_identifiers"]:
        record["external_identifiers"] = _ordered("external_identifiers", identifiers)
    if selected.series_ids:
        record["ohlcv_series_id"] = selected.series_ids[public_id]
    for extension in extensions:
        if values := enrichments.get(extension):
            record[extension] = values  # The enrichment publisher owns their canonical order.
    return record


def write(
    data, destination, selected, enrichments, *, catalogue_only=False, extensions=EXTENSIONS, progress=None
):
    output = Path(destination).resolve()
    if output.is_relative_to(data.store.resolve()):
        raise ValueError("metadata export destination must be outside the source store")
    output.mkdir(parents=True, exist_ok=False)
    scope = dict(selected.selection)
    for field in ("manifest_ref", "report_ref"):
        if field in scope:
            scope[field] = os.path.relpath(scope[field], output)
    manifest = {
        "schema": "alpaca.security_metadata",
        "version": 3,
        "catalogue_generation": selected.catalogue_generation,
        "records_file": RECORDS_FILE,
        "identity_count": len(selected.public_ids),
        "default_adjustments": list(ADJUSTMENTS),
        "extensions": [] if catalogue_only else list(extensions),
        "selection": scope,
    }
    records_path = output / RECORDS_FILE
    with (
        records_path.open("xb") as stream,
        gzip.GzipFile(fileobj=stream, mode="wb", filename="", mtime=0, compresslevel=6) as compressed,
    ):
        for position, public_id in enumerate(selected.public_ids, 1):
            sections = {} if catalogue_only else enrichments.get(public_id, {})
            record = _record(data, public_id, selected, sections, manifest["extensions"])
            compressed.write((_json(record) + "\n").encode("utf-8"))
            if progress and position % 250 == 0:
                progress("metadata_export_records", {"done": position, "total": manifest["identity_count"]})
    final = output / MANIFEST_FILE
    temporary = output / ".manifest.tmp"
    if progress:
        progress(
            "metadata_export_publish", {"done": manifest["identity_count"], "total": manifest["identity_count"]}
        )
    with temporary.open("xb") as stream:
        stream.write((_json(manifest) + "\n").encode("utf-8"))
    os.replace(temporary, final)
    return {
        "output": str(output),
        "manifest": str(final),
        "records_file": str(records_path),
        "identities": manifest["identity_count"],
        "catalogue_generation": selected.catalogue_generation,
        "selection": scope,
        "extensions": manifest["extensions"],
    }

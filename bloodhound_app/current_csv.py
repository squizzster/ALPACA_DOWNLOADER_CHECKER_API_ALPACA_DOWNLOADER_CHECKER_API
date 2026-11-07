"""Read completed schema-1/2 Alpaca per-identity CSV exports.

The final manifest selects membership; only selected digests and CSVs are opened.
Published checksums are provenance, not a request to authenticate upstream work.
No store, provider, directory census, or alias-value comparison is needed.
"""

import csv
import io
import json
from bisect import bisect_left
from pathlib import Path

import pandas as pd

FIELDS = ("open", "high", "low", "close", "volume", "trade_count", "vwap")
VIEWS = ("raw", "split", "dividend", "spin-off", "split,dividend", "split,spin-off", "dividend,spin-off", "all")
SUPPORTED_SCHEMAS = (1, 2)


def read_object(path):
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"{path}: duplicate JSON member {key}")
            result[key] = value
        return result

    result = json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=pairs)
    if not isinstance(result, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return result


def selected_path(root, name):
    if not isinstance(name, str) or not name or Path(name).is_absolute():
        raise ValueError("Export members must name relative paths")
    path = (root / name).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError(f"Export member escapes run directory: {name}")
    return path


def select(root, public_ids=None):
    """Accept a final selector once; defer digest/payload reads until consumption."""
    manifest_path = root / "final_manifest.json"
    if not manifest_path.exists():
        return None
    manifest = read_object(manifest_path)
    if (
        type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] not in SUPPORTED_SCHEMAS
        or manifest.get("format") != "alpaca_public_id_eod_csv"
    ):
        raise ValueError("Unsupported completed Alpaca CSV export schema/format")
    catalogue = manifest.get("catalogue")
    if (
        not isinstance(catalogue, dict)
        or not isinstance(catalogue.get("generation"), str)
        or not catalogue["generation"]
    ):
        raise ValueError("Completed CSV export must declare its catalogue generation")
    identities = manifest.get("identities")
    if not isinstance(identities, dict) or not manifest.get("completed_at"):
        raise ValueError("Incomplete CSV export selector")
    ids = list(identities) if public_ids is None else list(dict.fromkeys(public_ids))
    missing = set(ids) - identities.keys()
    if missing:
        raise ValueError(f"Public IDs absent from completed CSV export: {sorted(missing)}")
    for pid in ids:
        if not isinstance(pid, str) or not pid or not isinstance(identities[pid], dict):
            raise ValueError("Invalid identity entry in completed CSV export")
    return manifest, [(pid, identities[pid]) for pid in ids]


def _selected_ticker_intervals(intervals, tickers, exported_range, sessions):
    """Scope the completed digest once; retain the selected intervals for all rows/views.

    The producer's CSV range is authoritative. Historical/future labels outside
    that range cannot affect the new output and require no label validation.
    """
    first, last = exported_range["start"], exported_range["end"]
    selected = []
    for interval in intervals:
        if not isinstance(interval, dict):
            raise ValueError("Invalid published ticker ownership interval")
        start, end = interval.get("start_inclusive"), interval.get("end_exclusive")
        if (end is not None and end <= first) or (start is not None and start > last):
            continue
        # Completed exports already guarantee sorted sessions. Select actual
        # overlap without rescanning, sorting or authenticating that grid.
        index = 0 if start is None else bisect_left(sessions, start)
        if index == len(sessions) or (end is not None and sessions[index] >= end):
            continue
        ticker = interval.get("ticker")
        if ticker not in tickers:
            raise ValueError("Invalid published ticker ownership interval")
        selected.append((start, end, ticker))
    return selected


def _legacy_ticker_labels(security, exported_range, sessions):
    """Compatibility projection for schema-1 exports lacking the dated handoff."""
    tickers = security.get("tickers", [])
    intervals = security.get("ticker_ownership_intervals", [])
    if (
        not isinstance(tickers, list)
        or not all(isinstance(x, str) and x for x in tickers)
        or not isinstance(intervals, list)
    ):
        raise ValueError("Invalid published ticker metadata")
    selected_intervals = _selected_ticker_intervals(intervals, tickers, exported_range, sessions)
    single_ticker = tickers[0] if len(set(tickers)) == 1 else ""
    symbols = []
    for session in sessions:
        candidates = {
            ticker
            for start, end, ticker in selected_intervals
            if (start is None or start <= session) and (end is None or session < end)
        }
        if len(candidates) == 1:
            symbols.append(next(iter(candidates)))
        elif not candidates:
            symbols.append(single_ticker)
        else:
            symbols.append("")
    return symbols


def _published_ticker_labels(ranges, sessions):
    """Project the accepted catalogue's inclusive label ranges into output rows.

    These are completed observed-label decisions, not ownership evidence to
    reconcile. Preserve their distinct candidates without re-proving membership,
    endpoint coverage or compatibility with the descriptive ticker metadata.
    """
    selected = [(item["first_session"], item["last_session"], item["ticker"]) for item in ranges]
    symbols = []
    for session in sessions:
        candidates = {ticker for start, end, ticker in selected if start <= session <= end}
        symbols.append(next(iter(candidates)) if len(candidates) == 1 else "")
    return symbols


def load(root, public_id, selector, entry, validate_frame, metadata=None):
    digest_path = selected_path(root, entry.get("digest"))
    csv_path = selected_path(root, entry.get("csv"))
    digest = read_object(digest_path)
    if type(digest.get("schema_version")) is not int or digest["schema_version"] not in SUPPORTED_SCHEMAS:
        raise ValueError("Unsupported CSV digest schema")
    if digest["schema_version"] != selector["schema_version"]:
        raise ValueError("CSV digest schema differs from selected export")
    if (
        digest.get("public_id") != public_id
        or digest.get("catalogue_generation") != selector["catalogue"]["generation"]
    ):
        raise ValueError("CSV digest identity/catalogue generation differs from selected export")
    security = digest.get("security")
    if not isinstance(security, dict) or security.get("public_id") != public_id:
        raise ValueError("CSV digest security does not match selected public ID")
    ranges = security.get("ticker_ranges") if digest["schema_version"] == 2 else None
    if digest["schema_version"] == 2 and not isinstance(ranges, list):
        raise ValueError("Schema-2 CSV digest requires published ticker_ranges; regenerate the completed export")
    payload = csv_path.read_bytes()
    reader = csv.reader(io.StringIO(payload.decode("utf-8-sig")), strict=True)
    header = next(reader, [])
    if not header or len(header) != len(set(header)) or header != digest.get("columns") or "date" not in header:
        raise ValueError("CSV header differs from published digest columns")
    for row in reader:
        if row and len(row) != len(header):
            raise ValueError("CSV record width differs from its header")
    wide = pd.read_csv(
        io.BytesIO(payload), keep_default_na=False, float_precision="round_trip", dtype={"date": "string"}
    )
    if wide.empty:
        raise ValueError("Selected exported CSV is empty")
    sessions = wide["date"].astype(str).to_numpy()
    symbols = (
        _published_ticker_labels(ranges, sessions)
        if ranges is not None
        else _legacy_ticker_labels(security, digest["range"], sessions)
    )
    base = pd.DataFrame({"session": wide["date"], "symbol": symbols})
    if metadata is not None:
        base["symbol"] = metadata.columns(public_id, sessions, base, "", "")["symbol"]
    if base["symbol"].eq("").any():
        raise ValueError("Dated ticker is ambiguous or unavailable; provide --metadata with symbol intervals")
    views, aliases = digest.get("views"), digest.get("column_aliases")
    if not isinstance(views, dict) or not isinstance(aliases, dict):
        raise ValueError("CSV digest must provide views and column_aliases")
    frames = {}
    for view in VIEWS:
        spec = views.get(view)
        if not isinstance(spec, dict) or not isinstance(spec.get("columns"), dict):
            raise ValueError(f"Missing published view mapping: {view}")
        columns = spec["columns"]
        prefix = view.replace("spin-off", "spin_off").replace(",", "_")
        frame = base.copy()
        for field in FIELDS:
            physical = columns.get(field)
            if (
                not isinstance(physical, str)
                or physical not in header
                or aliases.get(f"{prefix}_{field}") != physical
            ):
                raise ValueError(f"Invalid direct column alias for {view}/{field}")
            frame[field] = wide[physical].to_numpy(copy=True)
        # All views share the completed export's ordered, unique session grid.
        # Numeric projection does not invalidate that producer-owned decision.
        frames[view] = validate_frame(frame, csv_path, public_id, accepted_session_grid=True)
    provenance = [
        {
            "file": entry["csv"],
            "bytes": digest.get("csv", {}).get("bytes"),
            "file_sha256": digest.get("csv", {}).get("sha256"),
            "checksum_authority": "completed_export_digest_not_recomputed",
            "digest": entry["digest"],
            "catalogue_generation": digest["catalogue_generation"],
            "ticker_label_authority": "completed_export_ticker_ranges"
            if ranges is not None
            else "schema_1_metadata",
        }
    ]
    return frames, provenance

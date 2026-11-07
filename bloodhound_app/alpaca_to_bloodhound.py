#!/usr/bin/env python3
"""Convert resolved Alpaca daily histories to the 18-column Bloodhound interface.

Run: python alpaca_to_bloodhound.py --data-in DIRECTORY --blood-out FILE.parquet
See README.md for the input, factor-evidence, and metadata contracts.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import zlib
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bloodhound_actions import load_action_evidence
from bloodhound_factors import infer_split_factors, validate_split_factors

VERSION = "1.2.0"
RUNTIME_VERSION = 4
ADJUSTMENTS = (
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
)
REQUIRED_VIEWS = {"raw", "split", "all"}
OHLC = ("open", "high", "low", "close")
OPTIONAL_METADATA = ("cik", "composite_figi", "share_class_figi", "company_name")
NUMERIC_COLUMNS = (
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "raw_close",
    "volume",
    "dollar_volume",
    "adjustment_ratio",
    "split_adjustment_ratio",
)
BLOODHOUND_SCHEMA = pa.schema(
    [
        pa.field("date", pa.timestamp("ns"), nullable=False),
        pa.field("ohlcv_series_id", pa.int64(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        *[pa.field(name, pa.string(), nullable=True) for name in OPTIONAL_METADATA],
        pa.field("security_type", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        *[pa.field(name, pa.float64(), nullable=False) for name in NUMERIC_COLUMNS],
    ]
)


class ConversionError(ValueError):
    """An input or output failed the declared conversion contract."""


def strict_date(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"Expected YYYY-MM-DD, got {value!r}")
    date.fromisoformat(value)
    return value


def sha256_file(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def series_id(public_id: str) -> int:
    """Deterministic positive signed int64, independent of discovery order/subsets."""
    return (int.from_bytes(hashlib.sha256(public_id.encode("utf-8")).digest()[:8], "big") & ((1 << 63) - 1)) or 1


def canonical_adjustment(value: str) -> str:
    value = str(value).strip().lower().replace(" ", "")
    if value in ("raw", "all"):
        return value
    parts = set(value.replace("+", ",").split(","))
    if not parts or not parts <= {"split", "dividend", "spin-off"}:
        raise ConversionError(f"Unsupported adjustment {value!r}")
    if len(parts) == 3:
        return "all"
    return ",".join(x for x in ("split", "dividend", "spin-off") if x in parts)


@dataclass
class History:
    directory: Path
    manifest: dict[str, Any]
    manifest_path: Path | None
    public_id: str
    read_symbol: str
    views: dict[str, dict[str, Any]]
    current_export: dict[str, Any] | None = None


def contained_payload(directory: Path, name: str) -> Path:
    if not isinstance(name, str) or not name:
        raise ConversionError("Every adjustment must name its CSV payload")
    relative = Path(name)
    path = (directory / relative).resolve()
    if relative.is_absolute() or not path.is_relative_to(directory.resolve()):
        raise ConversionError(f"Payload path escapes its security directory: {name!r}")
    if not path.is_file() or not (path.name.endswith(".csv") or path.name.endswith(".csv.gz")):
        raise ConversionError(f"CSV payload is missing or has an unsupported suffix: {path}")
    return path


def discover_histories(root: Path, public_ids=None) -> list[History]:
    from current_csv import select

    current = select(root, public_ids)
    if current is not None:
        selector, entries = current
        return unique_histories(
            [
                History(
                    root,
                    {},
                    None,
                    pid,
                    pid,
                    {name: {"file": entry.get("csv")} for name in ADJUSTMENTS},
                    {"selector": selector, "entry": entry},
                )
                for pid, entry in entries
            ]
        )
    if public_ids is not None:
        raise ConversionError("--public-id requires a completed current CSV run directory")
    manifests = sorted(root.rglob("manifest.json"))
    histories: list[History] = []
    for path in manifests:
        if not path.resolve().is_relative_to(root.resolve()):
            raise ConversionError(f"Manifest escapes input directory: {path}")
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(manifest, dict) or not isinstance(manifest.get("security", {}), dict):
                raise ConversionError("Manifest and its security member must be JSON objects")
            if not isinstance(manifest.get("adjustments"), list) or not all(
                isinstance(entry, dict) for entry in manifest["adjustments"]
            ):
                raise ConversionError("Manifest adjustments must be a list of objects")
            if manifest.get("lineage_generation") is not None and not isinstance(
                manifest["lineage_generation"], str
            ):
                raise ConversionError("lineage_generation must be a string when supplied")
            public_id = str(manifest.get("public_id") or manifest.get("security", {}).get("public_id") or "")
            if not public_id.strip():
                raise ConversionError("Missing public_id")
            if manifest.get("security", {}).get("public_id", public_id) != public_id:
                raise ConversionError("Manifest public_id disagrees with security.public_id")
            views = {}
            for entry in manifest["adjustments"]:
                name = canonical_adjustment(entry["adjustment"])
                if name in views:
                    raise ConversionError(f"Duplicate logical adjustment {name}")
                views[name] = entry
            missing = REQUIRED_VIEWS - views.keys()
            if missing:
                raise ConversionError(f"Missing logical views: {', '.join(sorted(missing))}")
            histories.append(
                History(
                    path.parent, manifest, path, public_id, str(manifest.get("read_symbol") or public_id), views
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ConversionError(f"Invalid manifest {path}: {exc}") from exc

    if not manifests:
        # A documented minimal adapter for ordinary paired/triplet daily CSV exports.
        parents = {p.parent for pattern in ("raw.csv", "raw.csv.gz") for p in root.rglob(pattern)}
        for directory in sorted(parents):
            if not directory.resolve().is_relative_to(root.resolve()):
                raise ConversionError(f"Input history escapes input directory: {directory}")
            views = {}
            for name in ADJUSTMENTS:
                stem = name.replace(",", "_")
                matches = [
                    directory / (stem + suffix)
                    for suffix in (".csv", ".csv.gz")
                    if (directory / (stem + suffix)).is_file()
                ]
                if len(matches) > 1:
                    raise ConversionError(f"Ambiguous CSV/gzip copies of {name} in {directory}")
                if matches:
                    views[name] = {"adjustment": name, "file": matches[0].name}
            missing = REQUIRED_VIEWS - views.keys()
            if missing:
                raise ConversionError(
                    f"{directory}: without a manifest, explicit raw/split/all files are required; missing {sorted(missing)}"
                )
            raw_path = contained_payload(directory, views["raw"]["file"])
            opener = gzip.open if raw_path.name.endswith(".gz") else open
            with opener(raw_path, "rt", encoding="utf-8-sig", newline="") as handle:
                first = next(csv.DictReader(handle), None)
            if first is None:
                raise ConversionError(f"Empty raw CSV: {raw_path}")
            public_id = first.get("public_id") or f"local:{directory.name}"
            label = first.get("symbol") or first.get("tickers") or directory.name
            histories.append(History(directory, {}, None, public_id, label, views))

    if not histories:
        raise ConversionError("No security manifests or raw.csv/raw.csv.gz histories found")
    return unique_histories(histories)


def unique_histories(histories):
    by_public: dict[str, Path] = {}
    by_numeric: dict[int, str] = {}
    for item in histories:
        if item.public_id in by_public:
            raise ConversionError(
                f"Repeated public_id {item.public_id}; found in {by_public[item.public_id]} and {item.directory}. Consolidate generations explicitly."
            )
        by_public[item.public_id] = item.directory
        number = series_id(item.public_id)
        if number in by_numeric and by_numeric[number] != item.public_id:
            raise ConversionError("Deterministic series-ID collision; use distinct source identities")
        by_numeric[number] = item.public_id
    return sorted(histories, key=lambda item: series_id(item.public_id))


def bad_rows(mask: np.ndarray, frame: pd.DataFrame, limit: int = 5) -> str:
    positions = np.flatnonzero(mask)
    examples = ", ".join(str(frame.iloc[int(i)]["session"]) for i in positions[:limit])
    return f"{len(positions)} row(s); first sessions: {examples}"


def read_frame(payload: bytes, path: Path, public_id: str) -> pd.DataFrame:
    records = csv.reader(io.StringIO(payload.decode("utf-8-sig")), strict=True)
    header = next(records, [])
    if len(header) != len(set(header)):
        raise ConversionError(f"Duplicate CSV column names in {path}")
    for row in records:
        if row and len(row) != len(header):
            raise ConversionError(
                f"{path}: CSV record ending on line {records.line_num} has {len(row)} fields; expected {len(header)}"
            )
    frame = pd.read_csv(
        io.BytesIO(payload),
        encoding="utf-8-sig",
        keep_default_na=False,
        float_precision="round_trip",
        dtype={key: "string" for key in ("session", "date", "symbol", "tickers", "public_id", "timestamp")},
    )
    return validate_frame(frame, path, public_id)


def validate_frame(
    frame: pd.DataFrame, path: Path, public_id: str, *, accepted_session_grid=False
) -> pd.DataFrame:
    if "session" not in frame and "date" in frame:
        frame = frame.rename(columns={"date": "session"})
    required = {"session", "open", "high", "low", "close", "volume"}
    if not required <= set(frame.columns) or frame.empty:
        raise ConversionError(f"{path}: nonempty daily CSV must contain {sorted(required)}")
    if not accepted_session_grid:
        for session in frame["session"]:
            strict_date(str(session))
        duplicate = frame["session"].duplicated(keep=False).to_numpy()
        if duplicate.any():
            raise ConversionError(f"Duplicate series/session keys: {bad_rows(duplicate, frame)}")
        frame = frame.sort_values("session", kind="stable").reset_index(drop=True)
    if "public_id" in frame and not (frame["public_id"] == public_id).all():
        raise ConversionError(f"CSV public_id disagrees with {public_id}")
    if "symbol" not in frame and "tickers" in frame:
        frame["symbol"] = frame["tickers"]
    if "symbol" not in frame:
        raise ConversionError(f"{path}: dated symbol or tickers column is required")
    symbols = frame["symbol"].astype(str)
    if symbols.str.strip().eq("").any() or symbols.str.contains(r"[,;|\s]", regex=True).any():
        raise ConversionError(f"{path}: symbols must be a single nonempty dated ticker per row")
    for field in (*OHLC, "volume", "vwap", "trade_count"):
        if field not in frame:
            continue
        values = pd.to_numeric(frame[field], errors="raise").to_numpy(dtype=np.float64)
        invalid = ~np.isfinite(values) | (values <= 0 if field in OHLC else values < 0)
        if invalid.any():
            raise ConversionError(f"Invalid {field}: {bad_rows(invalid, frame)}")
        frame[field] = values
    geometry = (
        (frame["low"] > frame["high"])
        | (frame["open"] < frame["low"])
        | (frame["open"] > frame["high"])
        | (frame["close"] < frame["low"])
        | (frame["close"] > frame["high"])
    )
    if geometry.any():
        raise ConversionError(f"Invalid OHLC geometry: {bad_rows(geometry.to_numpy(), frame)}")
    if "timestamp" in frame:
        parsed = pd.to_datetime(frame["timestamp"], utc=True, format="mixed", errors="raise")
        local = parsed.dt.tz_convert("America/New_York")
        invalid = (
            (local.dt.strftime("%Y-%m-%d") != frame["session"])
            | (local.dt.hour != 0)
            | (local.dt.minute != 0)
            | (local.dt.second != 0)
            | (local.dt.microsecond != 0)
        )
        if invalid.any():
            raise ConversionError(
                f"Expected a daily bar timestamp at New York midnight matching session: {bad_rows(invalid.to_numpy(), frame)}"
            )
    return frame


def load_history(
    history: History, metadata=None
) -> tuple[dict[str, pd.DataFrame], list[dict[str, Any]], list[dict[str, Any]]]:
    if history.current_export is not None:
        from current_csv import load

        current = history.current_export
        frames, payloads = load(
            history.directory, history.public_id, current["selector"], current["entry"], validate_frame, metadata
        )
        # The adapter projects every view from one completed session/label base,
        # including any dated-metadata override applied before projection. Carry
        # that alignment decision for this returned bundle, without rescanning it.
        alignment_complete = True
    else:
        cache: dict[Path, tuple[pd.DataFrame, dict[str, Any]]] = {}
        frames = {}
        for name, entry in history.views.items():
            path = contained_payload(history.directory, entry.get("file"))
            if path not in cache:
                encoded = path.read_bytes()
                decoded = gzip.decompress(encoded) if path.name.endswith(".gz") else encoded
                hashes = {
                    "file": path.relative_to(history.directory).as_posix(),
                    "bytes": len(encoded),
                    "file_sha256": hashlib.sha256(encoded).hexdigest(),
                    "logical_sha256": hashlib.sha256(decoded).hexdigest(),
                }
                cache[path] = (read_frame(decoded, path, history.public_id), hashes)
            frame, hashes = cache[path]
            for key, actual in (
                ("bytes", hashes["bytes"]),
                ("gzip_sha256", hashes["file_sha256"]),
                ("logical_sha256", hashes["logical_sha256"]),
                ("rows", len(frame)),
                ("first_session", str(frame.iloc[0]["session"])),
                ("last_session", str(frame.iloc[-1]["session"])),
            ):
                if key in entry and entry[key] != actual:
                    raise ConversionError(
                        f"{name}: manifest {key} mismatch for {path.name}; expected {entry[key]!r}, observed {actual!r}"
                    )
            frames[name] = frame
        payloads = [item[1] for item in cache.values()]
        alignment_complete = False

    raw = frames["raw"]
    issues = []
    aligned_frames = {id(raw)}
    for name, frame in frames.items():
        if not alignment_complete and id(frame) not in aligned_frames:
            # Legacy files are independent inputs. Accept each loaded frame once;
            # physical aliases retain that acceptance for this history lifetime.
            if len(frame) != len(raw) or not frame["session"].equals(raw["session"]):
                raise ConversionError(f"{name}: session keys do not exactly align with raw")
            if not np.array_equal(frame["symbol"].to_numpy(), raw["symbol"].to_numpy()):
                raise ConversionError(f"{name}: dated ticker labels differ from raw")
            if "timestamp" in raw and "timestamp" in frame and not raw["timestamp"].equals(frame["timestamp"]):
                raise ConversionError(f"{name}: timestamp values differ from raw")
            aligned_frames.add(id(frame))
        if name == "raw":
            continue
        if "trade_count" in raw and "trade_count" in frame:
            mismatch = (raw["trade_count"] != frame["trade_count"]).to_numpy()
            if mismatch.any():
                indices = np.flatnonzero(mismatch)
                issues.append(
                    {
                        "view": name,
                        "kind": "trade_count_mismatch",
                        "count": len(indices),
                        "first_session": str(raw.iloc[int(indices[0])]["session"]),
                        "last_session": str(raw.iloc[int(indices[-1])]["session"]),
                        "examples": [
                            {
                                "session": str(raw.iloc[int(i)]["session"]),
                                "raw": float(raw.iloc[int(i)]["trade_count"]),
                                "adjusted": float(frame.iloc[int(i)]["trade_count"]),
                            }
                            for i in indices[:5]
                        ],
                    }
                )
        split_view = name == "all" or "split" in name.split(",")
        if not split_view:
            mismatch = (raw["volume"] != frame["volume"]).to_numpy()
            if mismatch.any():
                issues.append(
                    {
                        "view": name,
                        "kind": "non_split_volume_changed",
                        "count": int(mismatch.sum()),
                        "detail": bad_rows(mismatch, raw),
                    }
                )
        else:
            mismatch = ((raw["volume"] == 0) & (frame["volume"] > 1)).to_numpy()
            if mismatch.any():
                indices = np.flatnonzero(mismatch)
                issues.append(
                    {
                        "view": name,
                        "kind": "raw_zero_adjusted_positive_volume",
                        "count": len(indices),
                        "first_session": str(raw.iloc[int(indices[0])]["session"]),
                        "last_session": str(raw.iloc[int(indices[-1])]["session"]),
                        "examples": [
                            {
                                "session": str(raw.iloc[int(i)]["session"]),
                                "raw_volume": 0.0,
                                "adjusted_volume": float(frame.iloc[int(i)]["volume"]),
                            }
                            for i in indices[:5]
                        ],
                    }
                )
    security = history.manifest.get("security", {})
    for key, actual in (
        ("first_session", str(raw.iloc[0]["session"])),
        ("last_session", str(raw.iloc[-1]["session"])),
    ):
        if key in security and security[key] != actual:
            raise ConversionError(f"security.{key} differs from held history")
    return frames, payloads, issues


class DatedMetadata:
    """Optional interval records. Empty optional metadata remains null."""

    def __init__(self, path: Path | None):
        self.rows: dict[str, list[dict[str, str]]] = {}
        if path is None:
            return
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, strict=True)
            if len(reader.fieldnames or []) != len(set(reader.fieldnames or [])):
                raise ConversionError("Metadata CSV has duplicate column names")
            if not {"public_id", "start_date", "end_date"} <= set(reader.fieldnames or []):
                raise ConversionError("Metadata CSV requires public_id,start_date,end_date")
            for row in reader:
                if None in row or any(value is None for value in row.values()):
                    raise ConversionError("Metadata CSV row width differs from its header")
                row = {key: value.strip() for key, value in row.items()}
                strict_date(row["start_date"])
                strict_date(row["end_date"])
                if not row["public_id"] or row["start_date"] > row["end_date"]:
                    raise ConversionError("Invalid metadata identity or interval")
                if row.get("symbol") and re.search(r"[,;|\s]", row["symbol"]):
                    raise ConversionError("Metadata symbol must be one ticker without separators or whitespace")
                self.rows.setdefault(row["public_id"], []).append(row)
        for public_id, rows in self.rows.items():
            rows.sort(key=lambda row: row["start_date"])
            for previous, following in zip(rows, rows[1:]):
                if previous["end_date"] >= following["start_date"]:
                    raise ConversionError(f"Overlapping metadata intervals for {public_id}")

    def columns(
        self, public_id: str, sessions: np.ndarray, raw: pd.DataFrame, unknown_type: str, unknown_exchange: str
    ) -> dict[str, list[Any]]:
        count = len(raw)
        result = {name: [None] * count for name in OPTIONAL_METADATA}
        result.update(
            symbol=raw["symbol"].astype(str).tolist(),
            security_type=[unknown_type] * count,
            exchange=[unknown_exchange] * count,
        )
        for row in self.rows.get(public_id, []):
            positions = np.flatnonzero((sessions >= row["start_date"]) & (sessions <= row["end_date"]))
            for name in result:
                value = row.get(name, "")
                if value:
                    for index in positions:
                        result[name][int(index)] = value
        return result


def make_table(
    history: History,
    frames: dict[str, pd.DataFrame],
    factors: np.ndarray,
    metadata: DatedMetadata,
    args: argparse.Namespace,
) -> pa.Table:
    raw = frames["raw"]
    supplied = frames["all"]
    sessions = raw["session"].to_numpy(dtype=str)
    values: dict[str, Any] = {
        "date": pd.to_datetime(sessions, format="%Y-%m-%d").to_numpy(dtype="datetime64[ns]"),
        "ohlcv_series_id": np.full(len(raw), series_id(history.public_id), dtype=np.int64),
        **metadata.columns(history.public_id, sessions, raw, args.unknown_security_type, args.unknown_exchange),
    }
    for name in OHLC:
        values["adjusted_" + name] = (
            supplied[name].to_numpy(dtype=np.float64, copy=True)
            if args.price_basis == "all"
            else raw[name].to_numpy(dtype=np.float64) * factors
        )
    values["raw_close"] = raw["close"].to_numpy(dtype=np.float64, copy=True)
    values["volume"] = raw["volume"].to_numpy(dtype=np.float64, copy=True)
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        values["dollar_volume"] = values["raw_close"] * values["volume"]
        values["adjustment_ratio"] = values["adjusted_close"] / values["raw_close"]
    values["split_adjustment_ratio"] = np.asarray(factors, dtype=np.float64)
    for name in NUMERIC_COLUMNS:
        array = values[name]
        if np.any(~np.isfinite(array)):
            raise ConversionError(f"Nonfinite output numeric values in {name}")
        if name not in ("volume", "dollar_volume") and np.any(array <= 0):
            raise ConversionError(f"Nonpositive output values in {name}")
    if any(not symbol.strip() or re.search(r"[,;|\s]", symbol) for symbol in values["symbol"]):
        raise ConversionError("Output symbol must be a single nonempty dated ticker")
    mask = np.ones(len(raw), dtype=bool)
    if args.start_date:
        mask &= sessions >= args.start_date
    if args.end_date:
        mask &= sessions <= args.end_date
    table = pa.Table.from_arrays(
        [pa.array(values[field.name], type=field.type) for field in BLOODHOUND_SCHEMA], schema=BLOODHOUND_SCHEMA
    )
    for field, column in zip(table.schema, table.columns):
        if not field.nullable and column.null_count:
            raise ConversionError(f"Null in required column {field.name}")
    return table.filter(pa.array(mask))


def write_json_atomic(path: Path, content: dict[str, Any]) -> None:
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(content, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def publish_pair(parquet_temporary: Path, output: Path, report_path: Path, report: dict[str, Any]) -> None:
    """Stage both artifacts and roll back ordinary commit errors.

    Each rename is atomic; no filesystem provides a two-file rename transaction.
    Backups protect the prior pair on caught failures. A process/power failure in
    the commit interval may leave backup files for manual recovery.
    """
    descriptor, staged_name = tempfile.mkstemp(
        prefix=f".{report_path.name}.", suffix=".staged", dir=report_path.parent
    )
    os.close(descriptor)
    staged_report = Path(staged_name)
    backups: dict[Path, Path] = {}
    installed: list[Path] = []
    try:
        # Serialization and disk space errors happen before publication.
        write_json_atomic(staged_report, report)
        for target in (output, report_path):
            if target.exists():
                descriptor, backup_name = tempfile.mkstemp(
                    prefix=f".{target.name}.", suffix=".backup", dir=target.parent
                )
                os.close(descriptor)
                backup = Path(backup_name)
                backup.unlink()
                try:
                    os.link(target, backup)
                except OSError:
                    shutil.copy2(target, backup)
                backups[target] = backup
        for source, target in ((parquet_temporary, output), (staged_report, report_path)):
            os.replace(source, target)
            installed.append(target)
    except BaseException:
        for target in reversed(installed):
            target.unlink(missing_ok=True)
        for target, backup in backups.items():
            os.replace(backup, target)
            # Replacing a target with another hardlink to the same inode can
            # be a no-op on POSIX; remove the remaining backup name as well.
            backup.unlink(missing_ok=True)
        raise
    else:
        for backup in backups.values():
            backup.unlink(missing_ok=True)
    finally:
        staged_report.unlink(missing_ok=True)


def publish_failure_report(output: Path, report_path: Path, report: dict[str, Any]) -> Path:
    # An unsuccessful --overwrite run must preserve the prior successful pair.
    destination = Path(str(output) + ".failure.json") if output.exists() else report_path
    write_json_atomic(destination, report)
    return destination


def validate_written_parquet(path: Path) -> dict[str, Any]:
    """Read the actual serialized artifact; validate derived equations and ordering."""
    parquet = pq.ParquetFile(path)
    if not parquet.schema_arrow.equals(BLOODHOUND_SCHEMA, check_metadata=False):
        raise ConversionError("Written Parquet schema differs from the required 18-column schema")
    rows = 0
    previous_key: tuple[int, int] | None = None
    counts: dict[int, int] = {}
    for batch in parquet.iter_batches(batch_size=65536):
        table = pa.Table.from_batches([batch])
        for field in BLOODHOUND_SCHEMA:
            if not field.nullable and table[field.name].null_count:
                raise ConversionError(f"Written file contains nulls in {field.name}")
        arrays = {name: table[name].to_numpy(zero_copy_only=False) for name in NUMERIC_COLUMNS}
        if any(not np.all(np.isfinite(x)) for x in arrays.values()):
            raise ConversionError("Written file contains nonfinite numeric values")
        if not np.array_equal(arrays["raw_close"] * arrays["volume"], arrays["dollar_volume"]):
            raise ConversionError("Written dollar_volume does not exactly equal raw_close * volume")
        if not np.array_equal(arrays["adjusted_close"] / arrays["raw_close"], arrays["adjustment_ratio"]):
            raise ConversionError("Written adjustment_ratio does not exactly equal adjusted_close / raw_close")
        times = table["date"].to_numpy(zero_copy_only=False).astype("datetime64[ns]").astype(np.int64)
        numbers = table["ohlcv_series_id"].to_numpy(zero_copy_only=False)
        if np.any(times % 86_400_000_000_000 != 0) or np.any(numbers <= 0):
            raise ConversionError("Written dates must be midnight, and series IDs must be positive")
        same = numbers[1:] == numbers[:-1]
        if np.any(numbers[1:] < numbers[:-1]) or np.any(same & (times[1:] <= times[:-1])):
            raise ConversionError("Written series/session keys are duplicated or unsorted")
        if len(numbers):
            first = (int(numbers[0]), int(times[0]))
            if previous_key is not None and first <= previous_key:
                raise ConversionError("Written ordering fails across record batches")
            previous_key = (int(numbers[-1]), int(times[-1]))
        unique, n = np.unique(numbers, return_counts=True)
        for number, count in zip(unique, n):
            counts[int(number)] = counts.get(int(number), 0) + int(count)
        rows += len(table)
    return {
        "rows": rows,
        "series": len(counts),
        "schema": "exact_18_columns",
        "derived_equations": "exact_float64",
        "keys": "unique_and_sorted",
        "dates": "timezone_free_midnight",
        "numeric_values": "finite",
        "calendar_validation": "session labels validated; exchange-calendar membership not asserted",
    }


def convert(args: argparse.Namespace) -> dict[str, Any]:
    root = args.data_in.expanduser().resolve()
    output = args.blood_out.expanduser().resolve()
    report_path = Path(str(output) + ".report.json")
    if not root.is_dir():
        raise ConversionError(f"--data-in must be an existing extracted directory: {root}")
    if output.suffix.lower() != ".parquet":
        raise ConversionError("--blood-out must end in .parquet")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise ConversionError("--start-date must be on or before --end-date")
    if not args.unknown_security_type.strip() or not args.unknown_exchange.strip():
        raise ConversionError("Unknown metadata sentinels must be nonempty")
    if args.split_source == "actions":
        if not (args.split_events and args.action_coverage and args.horizon):
            raise ConversionError(
                "--split-source actions requires --split-events, --action-coverage, and --horizon"
            )
    elif args.split_events or args.action_coverage or args.horizon:
        raise ConversionError("Action files and --horizon require --split-source actions")
    for path in (output, report_path):
        if path.exists() and not args.overwrite:
            raise ConversionError(f"Output already exists: {path}; use --overwrite to replace it")
        if path.is_dir():
            raise ConversionError(f"Output path is a directory: {path}")
    histories = discover_histories(root, getattr(args, "public_id", None))
    # Every possible publication destination, including failure diagnostics,
    # must be disjoint from selected inputs before any output can be written.
    inputs = {p.resolve() for p in (args.split_events, args.action_coverage, args.metadata) if p}
    for history in histories:
        if history.manifest_path:
            inputs.add(history.manifest_path.resolve())
        inputs.update(
            (history.directory / name).resolve()
            for name in {view.get("file") for view in history.views.values()}
            if isinstance(name, str)
        )
        if history.current_export:
            inputs.add(root / "final_manifest.json")
            name = history.current_export["entry"].get("digest")
            if isinstance(name, str):
                inputs.add((history.directory / name).resolve())
    for destination in (output, report_path, Path(str(output) + ".failure.json")):
        if destination.resolve() in inputs or (
            destination.exists()
            and any(input_path.exists() and destination.samefile(input_path) for input_path in inputs)
        ):
            raise ConversionError("Output must not replace an input file")
    metadata = DatedMetadata(args.metadata)
    actions = (
        load_action_evidence(args.split_events, args.action_coverage, args.horizon)
        if args.split_source == "actions"
        else None
    )
    evidence_name = (
        "action_records_with_declared_coverage" if actions else "inferred_from_rounded_provider_views"
    )
    report: dict[str, Any] = {
        "application": "alpaca_to_bloodhound",
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "status": "processing",
        "input_directory": str(root),
        "output_file": str(output),
        "price_basis": args.price_basis,
        "split_factor_evidence": evidence_name,
        "corporate_actions_independently_certified": False,
        "horizon": args.horizon if actions else None,
        "horizon_note": (
            "User-declared action horizon; checked against supplied views"
            if actions
            else "Provider adjustment basis; an absolute event horizon is not declared by these manifests. No final factor is forced to one."
        ),
        "date_filter": {
            "start": args.start_date,
            "end": args.end_date,
            "inclusive": True,
            "factors_computed_before_filter": True,
        },
        "formulas": {
            "adjusted_ohlc": "copy each all OHLC"
            if args.price_basis == "all"
            else "raw OHLC * split_adjustment_ratio",
            "volume": "raw.volume",
            "raw_close": "raw.close",
            "dollar_volume": "raw_close * volume",
            "adjustment_ratio": "adjusted_close / raw_close",
            "cutoff_volume_for_downstream_use_only": "volume(t) * S(c) / S(t)",
        },
        "metadata_policy": {
            "security_type_unknown": args.unknown_security_type,
            "exchange_unknown": args.unknown_exchange,
            "undated_company_names": "not copied into historical rows",
            "dated_metadata_file": str(args.metadata) if args.metadata else None,
            "unknown_enum_contract": "Exact downstream permitted sentinel values were not supplied; both defaults are configurable.",
        },
        "validation_scope": "The supplied 18-column contract and documented model; no external corporate-action or full downstream application audit.",
        "histories_discovered": len(histories),
        "histories": [],
        "omitted_histories": [],
        "input_evidence": {},
        "input_format": "alpaca_public_id_eod_csv"
        if any(h.current_export for h in histories)
        else "legacy_per_view_csv",
    }
    for name, path in (
        ("split_events", args.split_events),
        ("action_coverage", args.action_coverage),
        ("metadata", args.metadata),
    ):
        if path:
            report["input_evidence"][name] = {"file": str(path), "sha256": sha256_file(path)}
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".partial", dir=output.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    schema = BLOODHOUND_SCHEMA.with_metadata(
        {
            b"bloodhound_converter_version": VERSION.encode(),
            b"price_basis": args.price_basis.encode(),
            b"volume_basis": b"raw_as_traded",
            b"split_factor_evidence": evidence_name.encode(),
            b"split_horizon": (args.horizon or "provider_basis_not_declared").encode(),
            b"numeric_contract": b"dollar_volume=raw_close*volume;adjustment_ratio=adjusted_close/raw_close",
        }
    )
    total_rows = 0
    try:
        with pq.ParquetWriter(
            temporary, schema, compression=args.compression, version="2.6", use_dictionary=True
        ) as writer:
            for position, history in enumerate(histories, 1):
                item: dict[str, Any] = {
                    "public_id": history.public_id,
                    "ohlcv_series_id": series_id(history.public_id),
                    "read_symbol": history.read_symbol,
                    "input_subdirectory": str(history.directory.relative_to(root)),
                    "lineage_generation": history.manifest.get("lineage_generation"),
                }
                try:
                    if history.manifest_path:
                        item["manifest_sha256"] = sha256_file(history.manifest_path)
                    frames, payloads, issues = load_history(history, metadata)
                    raw = frames["raw"]
                    item.update(
                        input_rows=len(raw),
                        first_session=str(raw.iloc[0]["session"]),
                        last_session=str(raw.iloc[-1]["session"]),
                        payloads=payloads,
                        logical_views={key: entry["file"] for key, entry in history.views.items()},
                        dated_symbols=list(dict.fromkeys(raw["symbol"].astype(str))),
                    )
                    if issues:
                        item["source_issues"] = issues
                        first = issues[0]
                        raise ConversionError(
                            f"Source views conflict: {first['kind']} in {first['view']} ({first['count']} rows)"
                        )
                    numeric_views = {
                        key: {
                            field: frame[field].to_numpy(dtype=np.float64)
                            for field in (*OHLC, "volume", "vwap")
                            if field in frame
                        }
                        for key, frame in frames.items()
                    }
                    if actions:
                        factors, diagnostics = actions.factors_for(
                            history.public_id, raw["session"].to_numpy(dtype=str)
                        )
                        diagnostics["view_consistency"] = validate_split_factors(numeric_views, factors)
                    else:
                        factors, diagnostics = infer_split_factors(numeric_views)
                    for segment in diagnostics.get("segments", []):
                        segment["first_session"] = str(raw.iloc[segment["start_index"]]["session"])
                        segment["last_session"] = str(raw.iloc[segment["end_index"] - 1]["session"])
                    item["split_factor_details"] = diagnostics
                    table = make_table(history, frames, factors, metadata, args)
                    if len(table):
                        writer.write_table(
                            table.replace_schema_metadata(schema.metadata), row_group_size=len(table)
                        )
                        total_rows += len(table)
                        item["status"] = "exported"
                    else:
                        item["status"] = "outside_date_filter"
                    item["output_rows"] = len(table)
                    report["histories"].append(item)
                except (
                    ValueError,
                    TypeError,
                    KeyError,
                    OSError,
                    EOFError,
                    csv.Error,
                    zlib.error,
                    pa.ArrowException,
                ) as exc:
                    item.update(status="rejected", error=str(exc), error_type=type(exc).__name__)
                    if getattr(exc, "row_indices", None):
                        item["error_row_indices"] = list(exc.row_indices)
                        item["error_sessions"] = [str(raw.iloc[index]["session"]) for index in exc.row_indices]
                    if getattr(exc, "view", None):
                        item["error_view"] = exc.view
                    report["omitted_histories"].append(item)
                    if not args.quiet:
                        print(f"REJECT {history.read_symbol}: {exc}", file=sys.stderr)
                if not args.quiet and (position % 25 == 0 or position == len(histories)):
                    print(
                        f"Checked {position}/{len(histories)} histories; staged {total_rows:,} rows; rejected {len(report['omitted_histories'])}.",
                        file=sys.stderr,
                    )
        report["staged_rows"] = total_rows
        report["histories_exported"] = sum(item["status"] == "exported" for item in report["histories"])
        report["histories_rejected"] = len(report["omitted_histories"])
        if report["omitted_histories"] and args.on_error == "fail":
            report.update(
                status="failed",
                output_written=False,
                output_rows=0,
                failure="Source histories were rejected. No new Parquet was published. Use --on-error skip explicitly to omit entire rejected histories.",
            )
            failure_path = publish_failure_report(output, report_path, report)
            raise ConversionError(
                f"{len(report['omitted_histories'])} histories rejected. No new Parquet written. Report: {failure_path}"
            )
        if total_rows == 0:
            report.update(
                status="failed", output_written=False, output_rows=0, failure="No rows remain for export"
            )
            failure_path = publish_failure_report(output, report_path, report)
            raise ConversionError(f"No rows remain for export. Report: {failure_path}")
        report["serialized_validation"] = validate_written_parquet(temporary)
        if report["serialized_validation"]["rows"] != total_rows:
            raise ConversionError("Serialized row count differs from staged row count")
        report.update(
            status="partial" if report["omitted_histories"] else "complete",
            output_written=True,
            output_rows=total_rows,
            output_bytes=temporary.stat().st_size,
            output_sha256=sha256_file(temporary),
        )
        publish_pair(temporary, output, report_path, report)
        return report
    finally:
        temporary.unlink(missing_ok=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    result.add_argument(
        "--data-in",
        required=True,
        type=Path,
        help="Completed current CSV run directory, or legacy per-view history directory",
    )
    result.add_argument(
        "--blood-out", required=True, type=Path, help="Destination Bloodhound .parquet data file"
    )
    result.add_argument(
        "--public-id", action="append", help="Select a public ID from a completed current CSV run; repeatable"
    )
    result.add_argument(
        "--split-source",
        choices=("infer", "actions"),
        default="infer",
        help="infer = provider-view estimates (default); actions = exact products from supplied records",
    )
    result.add_argument("--split-events", type=Path, help="Action CSV, used only with --split-source actions")
    result.add_argument(
        "--action-coverage", type=Path, help="CSV declaring complete action coverage per public_id"
    )
    result.add_argument("--horizon", type=strict_date, help="Action normalization horizon, YYYY-MM-DD")
    result.add_argument(
        "--price-basis",
        choices=("all", "split"),
        default="all",
        help="all preserves supplied adjusted OHLC; split constructs raw OHLC * S",
    )
    result.add_argument("--metadata", type=Path, help="Optional dated identity/listing metadata CSV")
    result.add_argument(
        "--unknown-security-type", default="unknown", help="Required string for unavailable security type"
    )
    result.add_argument(
        "--unknown-exchange", default="unknown", help="Required string for unavailable listing MIC"
    )
    result.add_argument(
        "--start-date", type=strict_date, help="Inclusive session filter, applied after factor calculation"
    )
    result.add_argument(
        "--end-date", type=strict_date, help="Inclusive session filter, applied after factor calculation"
    )
    result.add_argument(
        "--on-error",
        choices=("fail", "skip"),
        default="fail",
        help="fail publishes no Parquet on a rejected history; skip explicitly omits entire rejected histories",
    )
    result.add_argument("--compression", choices=("zstd", "snappy", "gzip", "none"), default="zstd")
    result.add_argument(
        "--overwrite", action="store_true", help="Allow replacement of existing output/report files"
    )
    result.add_argument(
        "--quiet", action="store_true", help="Suppress progress; final summary and errors remain"
    )
    result.add_argument("--version", action="version", version=VERSION)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.compression == "none":
        args.compression = None
    try:
        report = convert(args)
    except (ValueError, TypeError, OSError, EOFError, csv.Error, zlib.error, pa.ArrowException) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        f"Wrote {report['output_rows']:,} rows from {report['histories_exported']} histories to {args.blood_out}"
    )
    print(f"Split factors: {report['split_factor_evidence']}")
    if report["histories_rejected"]:
        print(
            f"PARTIAL EXPORT: {report['histories_rejected']} entire histories omitted (--on-error skip).",
            file=sys.stderr,
        )
    print(f"Report: {args.blood_out}.report.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

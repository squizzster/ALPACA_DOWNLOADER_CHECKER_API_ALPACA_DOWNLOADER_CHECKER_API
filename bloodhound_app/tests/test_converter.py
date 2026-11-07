"""End-to-end tests using tiny, synthetic provider exports.

Run from the application directory with:
    python -m unittest discover -s tests -v

These fixtures deliberately include individually rounded prices, fractional raw
volume, dated ticker changes, manifest aliases, and an uninformative first bar.
No downloaded market data, provider credentials, or network calls are needed.
"""

from __future__ import annotations

import copy
import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

APP = Path(__file__).resolve().parents[1] / "alpaca_to_bloodhound.py"
SOURCE_ID = "a" * 64
SECOND_ID = "b" * 64
CSV_COLUMNS = [
    "session",
    "public_id",
    "tickers",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_count",
    "vwap",
]
OUTPUT_COLUMNS = [
    "date",
    "ohlcv_series_id",
    "symbol",
    "cik",
    "composite_figi",
    "share_class_figi",
    "company_name",
    "security_type",
    "exchange",
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "raw_close",
    "volume",
    "dollar_volume",
    "adjustment_ratio",
    "split_adjustment_ratio",
]
ADJUSTMENTS = [
    "raw",
    "split",
    "dividend",
    "spin-off",
    "split,dividend",
    "split,spin-off",
    "dividend,spin-off",
    "all",
]


def example_views(public_id: str = SOURCE_ID, symbol: str | None = None):
    """Two old-unit bars, followed by an effective-session bar in new units."""

    def row(session, ticker, prices, volume, vwap):
        return dict(
            zip(
                CSV_COLUMNS,
                [
                    session,
                    public_id,
                    ticker,
                    session + "T05:00:00Z",
                    *prices,
                    volume,
                    17,
                    vwap,
                ],
            )
        )

    old = symbol or "OLD"
    new = symbol or "NEW"
    raw = [
        row("2024-01-02", old, (100.0, 102.0, 99.0, 101.0), 3000000.25, 100.5),
        row("2024-01-03", old, (90.0, 93.0, 87.0, 91.0), 4000000.5, 90.9),
        row("2024-01-04", new, (35.0, 36.0, 34.0, 35.0), 125000.75, 35.0),
    ]
    split = [
        row("2024-01-02", old, (33.33, 34.0, 33.0, 33.67), 9000000.75, 33.5),
        row("2024-01-03", old, (30.0, 31.0, 29.0, 30.33), 12000001.5, 30.3),
        copy.deepcopy(raw[-1]),
    ]
    return {"raw": raw, "split": split, "all": copy.deepcopy(split)}


def csv_bytes(rows, symbol_column=False):
    data = io.StringIO(newline="")
    names = ["symbol" if name == "tickers" and symbol_column else name for name in CSV_COLUMNS]
    writer = csv.DictWriter(data, fieldnames=names, lineterminator="\n")
    writer.writeheader()
    for original in rows:
        row = dict(original)
        if symbol_column:
            row["symbol"] = row.pop("tickers")
        writer.writerow(row)
    return data.getvalue().encode("utf-8")


def write_simple(directory, views=None, *, compressed=False, symbol_column=False):
    directory.mkdir(parents=True, exist_ok=True)
    views = example_views() if views is None else views
    for name, rows in views.items():
        payload = csv_bytes(rows, symbol_column=symbol_column)
        suffix = ".csv.gz" if compressed else ".csv"
        if compressed:
            payload = gzip.compress(payload, mtime=0)
        (directory / (name + suffix)).write_bytes(payload)
    return views


def write_manifest(directory, views=None):
    """All eight views in the manifest, but only two physical payloads."""
    directory.mkdir(parents=True, exist_ok=True)
    views = example_views() if views is None else views
    physical = {}
    for name in ("raw", "split"):
        logical = csv_bytes(views[name])
        payload = gzip.compress(logical, mtime=0)
        filename = name + "_payload.csv.gz"
        (directory / filename).write_bytes(payload)
        physical[name] = {
            "file": filename,
            "bytes": len(payload),
            "gzip_sha256": hashlib.sha256(payload).hexdigest(),
            "logical_sha256": hashlib.sha256(logical).hexdigest(),
            "rows": len(views[name]),
            "first_session": views[name][0]["session"],
            "last_session": views[name][-1]["session"],
        }
    adjustments = []
    for name in ADJUSTMENTS:
        target = "split" if "split" in name or name == "all" else "raw"
        record = {"adjustment": name, **physical[target]}
        if name == target:
            record["storage"] = "file"
        else:
            record["storage"] = "exact_copy"
            record["equivalent_to"] = target
        adjustments.append(record)
    rows = views["raw"]
    manifest = {
        "format_version": 2,
        "boundary": "alpaca_standalone_resolved_pricing_export",
        "public_id": rows[0]["public_id"],
        "read_symbol": rows[-1]["tickers"],
        "csv_columns": CSV_COLUMNS,
        "adjustments": adjustments,
        "physical_payloads": 2,
        "security": {
            "public_id": rows[0]["public_id"],
            "tickers": sorted({r["tickers"] for r in rows}),
            "first_session": rows[0]["session"],
            "last_session": rows[-1]["session"],
            "adjustments": ADJUSTMENTS,
        },
        "input": {"record": {"company": "Example", "ticker": rows[-1]["tickers"]}},
        "range": {"start": None, "end": None, "inclusive": True},
    }
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return manifest


class ConverterIOTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.data_in = self.root / "input"
        self.data_in.mkdir()
        self.output = self.root / "result.parquet"

    def tearDown(self):
        self.temporary.cleanup()

    def run_cli(self, *args, data_in=None, output=None):
        return subprocess.run(
            [
                sys.executable,
                str(APP),
                "--data-in",
                str(data_in or self.data_in),
                "--blood-out",
                str(output or self.output),
                *map(str, args),
            ],
            text=True,
            capture_output=True,
            timeout=90,
        )

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)

    def assert_failure(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)

    def read_output(self, output=None):
        return pq.read_table(output or self.output)

    def test_preserves_individually_rounded_prices_fractional_raw_volume_and_dates(self):
        write_simple(self.data_in / "one")
        self.assert_success(self.run_cli())
        table = self.read_output()
        self.assertEqual(table.column_names, OUTPUT_COLUMNS)
        self.assertEqual(table.schema.field("date").type, pa.timestamp("ns"))
        self.assertEqual(table.schema.field("ohlcv_series_id").type, pa.int64())
        for name in OUTPUT_COLUMNS[9:]:
            self.assertEqual(table.schema.field(name).type, pa.float64(), name)
        rows = sorted(table.to_pylist(), key=lambda r: r["date"])
        first = rows[0]
        self.assertEqual(first["date"], dt.datetime(2024, 1, 2))
        self.assertIsNone(first["date"].tzinfo)
        self.assertEqual(first["symbol"], "OLD")
        self.assertEqual(rows[-1]["symbol"], "NEW")
        self.assertEqual(first["adjusted_open"], 33.33)
        self.assertEqual(first["adjusted_high"], 34.0)
        self.assertEqual(first["adjusted_low"], 33.0)
        self.assertEqual(first["adjusted_close"], 33.67)
        self.assertNotAlmostEqual(first["adjusted_open"], 100.0 * (33.67 / 101.0), places=5)
        self.assertEqual(first["raw_close"], 101.0)
        self.assertEqual(first["volume"], 3000000.25)
        self.assertEqual(first["dollar_volume"], 303000025.25)
        self.assertEqual(first["adjustment_ratio"], 33.67 / 101.0)
        self.assertAlmostEqual(first["split_adjustment_ratio"], 1.0 / 3.0, places=12)
        self.assertEqual(rows[-1]["split_adjustment_ratio"], 1.0)
        self.assertTrue(all(r["ohlcv_series_id"] > 0 for r in rows))
        self.assertEqual(len({r["ohlcv_series_id"] for r in rows}), 1)

    def test_simple_gzip_input_accepts_symbol_column(self):
        write_simple(self.data_in, compressed=True, symbol_column=True)
        self.assert_success(self.run_cli())
        self.assertEqual(self.read_output().num_rows, 3)

    def test_dividend_price_adjustment_does_not_change_split_factor_or_raw_volume(self):
        views = example_views()
        for row in views["all"][:2]:
            raw = next(r for r in views["raw"] if r["session"] == row["session"])
            for name in ("open", "high", "low", "close", "vwap"):
                row[name] = round(raw[name] * 0.3, 2)
        write_simple(self.data_in / "security", views)
        self.assert_success(self.run_cli())
        first = min(self.read_output().to_pylist(), key=lambda r: r["date"])
        self.assertEqual(first["adjusted_close"], 30.3)
        self.assertAlmostEqual(first["adjustment_ratio"], 0.3, places=15)
        self.assertAlmostEqual(first["split_adjustment_ratio"], 1 / 3, places=12)
        self.assertEqual(first["volume"], 3000000.25)
        self.assertEqual(first["dollar_volume"], 303000025.25)

    def test_manifest_resolves_all_eight_aliases_without_all_filename(self):
        security = self.data_in / "wrapper" / "security"
        write_manifest(security)
        self.assertFalse((security / "all.csv.gz").exists())
        self.assert_success(self.run_cli())
        first = min(self.read_output().to_pylist(), key=lambda r: r["date"])
        self.assertEqual(first["adjusted_open"], 33.33)
        self.assertEqual(first["volume"], 3000000.25)

    def test_series_ids_do_not_depend_on_directory_names_or_creation_order(self):
        write_simple(self.data_in / "z", example_views(SOURCE_ID, "AAA"))
        write_simple(self.data_in / "a", example_views(SECOND_ID, "BBB"))
        self.assert_success(self.run_cli())
        first_ids = {r["symbol"]: r["ohlcv_series_id"] for r in self.read_output().to_pylist()}
        other_input = self.root / "other_input"
        write_simple(other_input / "z", example_views(SECOND_ID, "BBB"))
        write_simple(other_input / "a", example_views(SOURCE_ID, "AAA"))
        other_output = self.root / "second.parquet"
        self.assert_success(self.run_cli(data_in=other_input, output=other_output))
        second_ids = {r["symbol"]: r["ohlcv_series_id"] for r in self.read_output(other_output).to_pylist()}
        self.assertEqual(first_ids, second_ids)
        self.assertEqual(len(set(first_ids.values())), 2)

    def test_hash_mismatch_is_rejected_before_output_commit(self):
        security = self.data_in / "security"
        manifest = write_manifest(security)
        manifest["adjustments"][0]["gzip_sha256"] = "0" * 64
        (security / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        result = self.run_cli()
        self.assert_failure(result)
        self.assertFalse(self.output.exists())
        self.assertRegex(result.stderr + result.stdout, r"(?i)(hash|sha256|checksum)")

    def test_manifest_path_traversal_is_rejected(self):
        security = self.data_in / "security"
        manifest = write_manifest(security)
        source = security / "raw_payload.csv.gz"
        shutil.copyfile(source, self.data_in / "outside.csv.gz")
        for record in manifest["adjustments"]:
            if record["file"] == "raw_payload.csv.gz":
                record["file"] = "../outside.csv.gz"
        (security / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        self.assert_failure(self.run_cli())
        self.assertFalse(self.output.exists())

    def test_invalid_security_does_not_publish_partial_output(self):
        write_simple(self.data_in / "a_good", example_views(SOURCE_ID, "GOOD"))
        broken = example_views(SECOND_ID, "BAD")
        broken["raw"][1]["close"] = 0
        write_simple(self.data_in / "z_bad", broken)
        self.assert_failure(self.run_cli())
        self.assertFalse(self.output.exists())

    def test_overwrite_failure_preserves_existing_file(self):
        sentinel = b"an existing output must survive a failed overwrite\n"
        self.output.write_bytes(sentinel)
        broken = example_views()
        broken["raw"][1]["close"] = 0
        write_simple(self.data_in / "security", broken)
        self.assert_failure(self.run_cli("--overwrite"))
        self.assertEqual(self.output.read_bytes(), sentinel)

    def test_existing_output_requires_overwrite(self):
        sentinel = b"keep this existing file"
        self.output.write_bytes(sentinel)
        write_simple(self.data_in / "security")
        self.assert_failure(self.run_cli())
        self.assertEqual(self.output.read_bytes(), sentinel)

    def test_skip_is_explicit_and_excludes_the_bad_security(self):
        write_simple(self.data_in / "a_good", example_views(SOURCE_ID, "GOOD"))
        broken = example_views(SECOND_ID, "BAD")
        broken["raw"][1]["close"] = 0
        write_simple(self.data_in / "z_bad", broken)
        self.assert_success(self.run_cli("--on-error", "skip"))
        rows = self.read_output().to_pylist()
        self.assertEqual(len(rows), 3)
        self.assertEqual({r["symbol"] for r in rows}, {"GOOD"})

    def test_date_filter_is_inclusive_and_applied_after_full_history_inference(self):
        views = example_views()
        # This observation alone cannot identify 1/3: its zero volume and tiny
        # unchanged prices allow many factors. The next bar constrains the same
        # regime to 1/3. Filtering early would lose that evidence.
        for name in ("raw", "split", "all"):
            first = views[name][0]
            first.update(open=0.001, high=0.001, low=0.001, close=0.001, volume=0, trade_count=0, vwap=0)
        write_simple(self.data_in / "security", views)
        self.assert_success(self.run_cli("--start-date", "2024-01-02", "--end-date", "2024-01-02"))
        rows = self.read_output().to_pylist()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], dt.datetime(2024, 1, 2))
        self.assertEqual(rows[0]["adjusted_close"], 0.001)
        self.assertEqual(rows[0]["volume"], 0.0)
        self.assertAlmostEqual(rows[0]["split_adjustment_ratio"], 1 / 3, places=12)


if __name__ == "__main__":
    unittest.main()

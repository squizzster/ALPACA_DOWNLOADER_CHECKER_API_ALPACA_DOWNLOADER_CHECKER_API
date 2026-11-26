"""Exact export membership and immutable wire publication from held catalogue metadata."""

import gzip
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from alpaca_end_of_day import ADJUSTMENTS, CatalogChangedError
from alpaca_end_of_day import _metadata_export as export

FIRST = "1" * 64
SECOND = "2" * 64
THIRD = "3" * 64
GENERATION = "selected-catalogue"
FILTER = {"start": "2022-01-03", "end": "2022-01-03", "inclusive": True}


def security_record():
    spans = [
        {"ticker": label, "first_session": "2022-01-03", "last_session": "2022-01-04"}
        for label in ("SECOND", "FIRST")
    ]
    return {
        "group_key": "held-group",
        "public": {
            "first_session": "2017-01-03",
            "last_session": "2022-01-04",
            "coverage": {"session_count": 4, "explained_missing_sessions": []},
            "adjustments": list(ADJUSTMENTS),
            "ticker_ranges": spans,
            "ticker_ownership_intervals": [
                {"ticker": "FIRST", "start_inclusive": None, "end_exclusive": None, "cusip": None}
            ],
            "observations": [
                {
                    "field": "exchange",
                    "value": "OTC",
                    "observed_at": "2025-01-01T09:00:00Z",
                    "effective_from": None,
                    "effective_through": None,
                }
            ],
            "external_identifiers": [],
        },
    }


class MetadataExportFormatTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        securities = {pid: SimpleNamespace(public_id=pid) for pid in (THIRD, SECOND, FIRST)}
        self.data = SimpleNamespace(
            store=self.root / "store",
            catalog_generation=lambda: GENERATION,
            get_security=lambda pid: securities[pid.lower()],
            resolve_ticker=lambda label: tuple(securities.values()) if label == "SHARED" else (),
            _securities=securities,
            _records={pid: security_record() for pid in securities},
        )

    def csv_selector(self, ids=(FIRST,), **override):
        path = self.root / "final_manifest.json"
        path.write_text(
            json.dumps(
                {
                    "format": "alpaca_public_id_eod_csv",
                    "schema_version": 2,
                    "completed_at": "2026-01-01T00:00:00Z",
                    "catalogue": {"generation": GENERATION},
                    "identities": {pid: {} for pid in ids},
                    "requested_range": FILTER,
                    **override,
                }
            )
        )
        return path

    def bh_selector(self, **override):
        path = self.root / "prices.parquet.report.json"
        path.write_text(
            json.dumps(
                {
                    "application": "alpaca_to_bloodhound",
                    "input_format": "alpaca_online_catalogue",
                    "catalogue": {"generation": GENERATION},
                    "output_written": True,
                    "status": "partial",
                    "date_filter": FILTER,
                    "output_sha256": "a" * 64,
                    "histories": [
                        {"public_id": FIRST, "ohlcv_series_id": (1 << 63) - 1, "status": "exported"},
                        {"public_id": SECOND, "ohlcv_series_id": 1, "status": "outside_date_filter"},
                    ],
                    "omitted_histories": [{"public_id": THIRD, "status": "rejected"}],
                    **override,
                }
            )
        )
        return path

    def read_run(self, run):
        manifest = json.loads(Path(run["manifest"]).read_text())
        with gzip.open(Path(run["manifest"]).parent / manifest["records_file"], "rt") as source:
            rows = [json.loads(line) for line in source]
        return manifest, rows

    def test_partial_report_is_the_only_input_and_exposes_only_exported_series(self):
        source = self.bh_selector()
        opened = []
        original = Path.open

        def named(path, *args, **kwargs):
            opened.append(path)
            return original(path, *args, **kwargs)

        with patch.object(Path, "open", named):
            choice = export.select(self.data, bloodhound_report=source)
            plan = export.plan(choice, 1)
            self.assertEqual(opened, [source])
            self.assertEqual((plan["identities"], plan["groups"]), (1, 1))
            output = self.root / "paired"
            run = export.write(self.data, output, choice, {})
            self.assertEqual(opened.count(source), 1)
            self.assertTrue(all(path == source or path.parent == output for path in opened))
        manifest, rows = self.read_run(run)
        self.assertEqual(manifest["identity_count"], 1)
        self.assertEqual(
            manifest["selection"],
            {
                "kind": "bloodhound",
                "report_ref": os.path.relpath(source, output),
                "status": "partial",
                "date_filter": {"start_inclusive": FILTER["start"], "end_inclusive": FILTER["end"]},
                "output_sha256": "a" * 64,
            },
        )
        self.assertEqual(
            [(row["public_id"], row["ohlcv_series_id"]) for row in rows], [(FIRST, "9223372036854775807")]
        )
        self.assertEqual(
            rows[0]["coverage"],
            {"first_session": "2017-01-03", "last_session": "2022-01-04", "session_count": 4},
        )
        self.assertEqual([span["ticker"] for span in rows[0]["ticker_ranges"]], ["FIRST", "SECOND"])
        self.assertIsNone(rows[0]["observations"][0]["effective_from"])

    def test_csv_uses_manifest_ids_once_no_named_digests_and_handles_empty(self):
        source = self.csv_selector((THIRD, FIRST))
        with patch.object(export, "_read_selector", wraps=export._read_selector) as read:
            choice = export.select(self.data, csv_manifest=source)
            forbidden = Mock()
            forbidden.get.side_effect = AssertionError("catalogue-only must not open enrichments")
            output = self.root / "csv"
            run = export.write(self.data, output, choice, forbidden, catalogue_only=True)
            read.assert_called_once_with(source)
        manifest, rows = self.read_run(run)
        self.assertEqual(manifest["extensions"], [])
        self.assertEqual(manifest["selection"]["manifest_ref"], os.path.relpath(source, output))
        self.assertEqual([row["public_id"] for row in rows], [FIRST, THIRD])
        self.assertTrue(all("ohlcv_series_id" not in row for row in rows))
        self.assertEqual(manifest["default_adjustments"], list(ADJUSTMENTS))
        self.assertNotIn("adjustments", rows[0]["coverage"])
        with self.assertRaises(FileExistsError):
            export.write(self.data, output, choice, forbidden, catalogue_only=True)
        empty = export.select(self.data, csv_manifest=self.csv_selector(()))
        manifest, rows = self.read_run(export.write(self.data, self.root / "empty", empty, {}))
        self.assertEqual((manifest["identity_count"], rows), (0, []))

    def test_ambiguous_ticker_selects_all_and_published_bytes_are_deterministic(self):
        ambiguous = export.select(self.data, symbols=["SHARED"])
        self.assertEqual(ambiguous.public_ids, (FIRST, SECOND, THIRD))
        self.assertEqual(ambiguous.selection, {"kind": "public_ids"})
        self.assertEqual(export.select(self.data).selection, {"kind": "all_online"})
        snapshots = [
            {"symbol": "TERMINAL", "observed_at": None, "exchange": "NASDAQ"},
            {"symbol": "TERMINAL", "observed_at": None, "exchange": "OTC", "tradable": False},
        ]
        selected = export.select(self.data, public_ids=[FIRST, FIRST])
        first = export.write(self.data, self.root / "one", selected, {FIRST: {"asset_snapshots": snapshots}})
        second = export.write(self.data, self.root / "two", selected, {FIRST: {"asset_snapshots": snapshots}})
        self.assertEqual(Path(first["manifest"]).read_bytes(), Path(second["manifest"]).read_bytes())
        self.assertEqual(Path(first["records_file"]).read_bytes(), Path(second["records_file"]).read_bytes())
        manifest, rows = self.read_run(first)
        self.assertEqual(manifest["extensions"], sorted(export.EXTENSIONS))
        self.assertEqual(rows[0]["asset_snapshots"], snapshots)
        self.assertNotIn("trade_observations", rows[0])
        self.assertNotIn("external_identifiers", rows[0])
        self.assertIs(rows[0]["asset_snapshots"][-1]["tradable"], False)
        with self.assertRaisesRegex(ValueError, "ticker is not available"):
            export.select(self.data, symbols=["UNKNOWN"])
        with self.assertRaisesRegex(ValueError, "only one metadata selector"):
            export.select(self.data, public_ids=[FIRST], symbols=["SHARED"])

    def test_bad_or_stale_selectors_and_interrupted_writer_leave_no_published_run(self):
        for source in (
            self.csv_selector(catalogue={"generation": "replaced"}),
            self.bh_selector(catalogue={"generation": "replaced"}),
        ):
            kwargs = {"csv_manifest" if source.name == "final_manifest.json" else "bloodhound_report": source}
            with self.assertRaisesRegex(CatalogChangedError, "matching catalogue generation"):
                export.select(self.data, **kwargs)
        for value in (0, 1 << 63, 1.5, True):
            source = self.bh_selector(
                histories=[{"public_id": FIRST, "status": "exported", "ohlcv_series_id": value}]
            )
            with self.assertRaisesRegex(ValueError, "positive signed int64"):
                export.select(self.data, bloodhound_report=source)
        with self.assertRaisesRegex(ValueError, "completed native online"):
            export.select(self.data, bloodhound_report=self.bh_selector(output_written=False))
        with self.assertRaisesRegex(ValueError, "completed native CSV"):
            export.select(self.data, csv_manifest=self.csv_selector(completed_at=None))
        selected = export.select(self.data)
        with self.assertRaisesRegex(ValueError, "outside the source"):
            export.write(self.data, self.data.store / "illegal", selected, {})
        self.assertFalse(self.data.store.exists())
        interrupted = self.root / "interrupted"
        with patch.object(export.os, "replace", side_effect=OSError("commit interrupted")):
            with self.assertRaisesRegex(OSError, "commit interrupted"):
                export.write(self.data, interrupted, selected, {})
        self.assertFalse((interrupted / "manifest.json").exists())
        complete = self.root / "recovered"
        self.assertEqual(self.read_run(export.write(self.data, complete, selected, {}))[0]["identity_count"], 3)

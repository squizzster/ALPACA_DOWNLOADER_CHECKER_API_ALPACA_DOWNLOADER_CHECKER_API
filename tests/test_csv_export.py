"""CSV output consumes published selections once and publishes its selector last."""

import csv
import fcntl
import hashlib
import io
import json
import os
from collections import Counter
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from decimal import Decimal
from pathlib import Path
from queue import Queue
from threading import Barrier, Event, Lock, get_ident
from threading import enumerate as threads
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import (
    ADJUSTMENTS,
    CatalogChangedError,
    DataLibrary,
    PublicIdUnavailableError,
    ReaderBusyError,
    StandaloneReader,
    _csv_export,
)
from libs.alpaca_cli import main
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.download import download_pricing
from libs.common.store_lock import StoreBusyError, operation_lock

FIELDS = ("open", "high", "low", "close", "volume", "trade_count", "vwap")
PREFIXES = (
    "raw",
    "split",
    "dividend",
    "spin_off",
    "split_dividend",
    "split_spin_off",
    "dividend_spin_off",
    "all",
)


class CsvExportTests(NativeCase):
    def lifecycle_records(self, stderr, *, status="success", exit_code=0, last_phase=None):
        records = [json.loads(line) for line in stderr.getvalue().splitlines()]
        self.assertGreaterEqual(len(records), 2)
        self.assertEqual(records[0]["phase"], "INIT")
        self.assertEqual(records[0]["schema"], "alpaca.lifecycle.progress")
        self.assertIn("ai_agent_read_this", records[0])
        for record in records[1:]:
            self.assertNotIn("ai_agent_read_this", record)
        self.assertEqual(records[-1]["phase"], "FINAL_EXIT")
        self.assertEqual(records[-1]["schema"], "alpaca.lifecycle.progress")
        self.assertEqual(records[-1]["status"], status)
        self.assertEqual(records[-1]["exit_code"], exit_code)
        self.assertEqual(sum(record["phase"] == "FINAL_EXIT" for record in records), 1)
        if last_phase is not None:
            self.assertEqual(records[-1]["last_phase"], last_phase)
        for record in records:
            self.assertEqual(record["schema_version"], 3)
            if record["phase"] == "FINAL_EXIT" or record["schema"] == "alpaca.lifecycle.error":
                self.assertIsNone(record["next_heart_beat_in_seconds"])
            else:
                self.assertIs(type(record["next_heart_beat_in_seconds"]), int)
                self.assertGreater(record["next_heart_beat_in_seconds"], 0)
            self.assertIs(type(record["elapsed_seconds"]), int)
            self.assertGreaterEqual(record["elapsed_seconds"], 0)
            self.assertIsInstance(record["message"], str)
            self.assertTrue(record["message"])
        return records

    def refresh(self, client):
        download_pricing(self.store, "TEST", "2026-10-14", client=client, refresh_all=True)
        result = run_lineage(
            self.store,
            ["TEST"],
            identity_client=IdentityClient({"TEST": asset("TEST")}),
            refresh=True,
        )
        self.assertEqual(result["failed"], 0, result)

    def exported(self, data, **kwargs):
        destination = self.directory / "exports/csv"
        result = data.write_csv_export(destination, **kwargs)
        manifest_path = Path(result["manifest"])
        run_directory = Path(result["output"])
        self.assertEqual(manifest_path, run_directory / "final_manifest.json")
        self.assertFalse((destination / "final_manifest.json").exists())
        manifest = json.loads(manifest_path.read_text())
        documents = {}
        for public_id, entry in manifest["identities"].items():
            csv_path = run_directory / entry["csv"]
            digest_path = run_directory / entry["digest"]
            encoded = csv_path.read_bytes()
            digest = json.loads(digest_path.read_text())
            reader = csv.DictReader(io.StringIO(encoded.decode()))
            rows = list(reader)
            self.assertEqual(reader.fieldnames, digest["columns"])
            self.assertEqual(digest["columns"], entry["columns"])
            self.assertEqual(digest["public_id"], public_id)
            self.assertEqual(digest["catalogue_generation"], data.catalog_generation())
            self.assertEqual(digest["rows"], len(rows))
            self.assertEqual(entry["rows"], len(rows))
            self.assertEqual(digest["csv"]["file"], csv_path.name)
            self.assertEqual(digest["csv"]["sha256"], hashlib.sha256(encoded).hexdigest())
            self.assertEqual(digest["csv"]["bytes"], len(encoded))
            self.assertEqual(csv_path.name, f"{public_id}_eod.csv")
            self.assertEqual(digest_path.name, f"{public_id}_digest.json")
            self.assertEqual(csv_path.parent.name, public_id)
            self.assertEqual(csv_path.parent.parent, run_directory)
            self.assertEqual(digest_path.parent, csv_path.parent)
            self.assertTrue(all(value is not None and value != "" for row in rows for value in row.values()))
            documents[public_id] = (digest, rows)
        return result, manifest, documents

    def test_equal_series_share_one_column_and_preserve_exact_values(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        result, manifest, documents = self.exported(data)
        digest, rows = documents[public_id]
        self.assertEqual(digest["schema_version"], 1)
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(digest["columns"], ["date", "raw_open", "raw_volume", "raw_trade_count"])
        self.assertEqual(rows[0]["raw_open"], str(self.bars[0]["o"]))
        self.assertEqual(rows[0]["raw_volume"], "9007199254740993.5")
        self.assertEqual(rows[0]["raw_trade_count"], "7")
        self.assertEqual([row["date"] for row in rows], [bar["t"][:10] for bar in self.bars])
        self.assertEqual(digest["range"], {"start": rows[0]["date"], "end": rows[-1]["date"], "inclusive": True})
        self.assertEqual(digest["security"]["public_id"], public_id)
        self.assertEqual(len(digest["column_aliases"]), 56)
        self.assertEqual(
            manifest["columns"], ["date", *(f"{prefix}_{field}" for prefix in PREFIXES for field in FIELDS)]
        )
        for adjustment, prefix in zip(ADJUSTMENTS, PREFIXES):
            self.assertEqual(manifest["adjustments"][adjustment]["prefix"], prefix)
            for field in FIELDS:
                expected = f"raw_{field}" if field in {"volume", "trade_count"} else "raw_open"
                self.assertEqual(digest["views"][adjustment]["columns"][field], expected)
                self.assertEqual(digest["column_aliases"][f"{prefix}_{field}"], expected)
                self.assertEqual(manifest["adjustments"][adjustment]["columns"][field], f"{prefix}_{field}")
        self.assertEqual(result["identities"], 1)
        self.assertEqual(result["rows"], len(self.bars))
        self.assertEqual(manifest["counts"]["identities"], 1)
        self.assertEqual(manifest["counts"]["rows"], len(self.bars))

    def test_one_late_changed_column_is_populated_for_every_session(self):
        class ChangedVolumeClient(FakeClient):
            def fetch(self, symbol, adjustment, start, end, **kwargs):
                rows = super().fetch(symbol, adjustment, start, end, **kwargs)
                if adjustment != "split":
                    return rows
                return [
                    {
                        **row,
                        "c": Decimal(str(row["c"]) + "0"),
                        "v": Decimal("42.5") if row["t"][:10] == "2026-10-13" else row["v"],
                    }
                    for row in rows
                ]

        self.refresh(ChangedVolumeClient(self.bars))
        self.add_symbol("OTHER")
        data = self.bootstrap()
        test_id = data.resolve_ticker("TEST")[0].public_id
        other_id = data.resolve_ticker("OTHER")[0].public_id
        _, _, documents = self.exported(data)
        digest, rows = documents[test_id]
        self.assertEqual(
            digest["columns"], ["date", "raw_open", "raw_volume", "raw_trade_count", "split_volume"]
        )
        self.assertEqual(digest["views"]["split"]["columns"]["close"], "raw_open")
        self.assertEqual(digest["views"]["split"]["columns"]["volume"], "split_volume")
        self.assertEqual(rows[0]["split_volume"], rows[0]["raw_volume"])
        self.assertEqual(rows[-1]["split_volume"], "42.5")
        self.assertNotEqual(rows[-1]["split_volume"], rows[-1]["raw_volume"])
        self.assertNotIn("split_volume", documents[other_id][0]["columns"])
        _, _, filtered = self.exported(data, public_ids=[test_id], end="2026-10-12")
        self.assertNotIn("split_volume", filtered[test_id][0]["columns"])
        self.assertEqual(filtered[test_id][0]["column_aliases"]["split_volume"], "raw_volume")
        self.assertEqual(filtered[test_id][1][-1]["date"], "2026-10-12")

    def test_columns_with_equal_first_and_last_values_but_different_middle_stay_distinct(self):
        class MiddleChangeClient(FakeClient):
            def fetch(self, symbol, adjustment, start, end, **kwargs):
                rows = super().fetch(symbol, adjustment, start, end, **kwargs)
                return [
                    {**row, "v": Decimal("123.5")}
                    if adjustment == "all" and row["t"][:10] == "2026-10-06"
                    else row
                    for row in rows
                ]

        self.refresh(MiddleChangeClient(self.bars))
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        _, _, documents = self.exported(data)
        digest, rows = documents[public_id]
        self.assertIn("all_volume", digest["columns"])
        self.assertEqual(rows[0]["all_volume"], rows[0]["raw_volume"])
        self.assertEqual(rows[-1]["all_volume"], rows[-1]["raw_volume"])
        self.assertEqual(next(row for row in rows if row["date"] == "2026-10-06")["all_volume"], "123.5")

    def test_all_eight_views_retain_their_specific_column_names_when_each_series_differs(self):
        class DistinctClient(FakeClient):
            def fetch(self, symbol, adjustment, start, end, **kwargs):
                index = ADJUSTMENTS.index(adjustment)
                rows = super().fetch(symbol, adjustment, start, end, **kwargs)
                return [
                    {
                        **row,
                        "o": Decimal(1000 * index + 101),
                        "h": Decimal(1000 * index + 103),
                        "l": Decimal(1000 * index + 100),
                        "c": Decimal(1000 * index + 102),
                        "v": Decimal(100000 + index),
                        "n": 2000000 + index,
                        "vw": Decimal(1000 * index) + Decimal("101.5"),
                    }
                    for row in rows
                ]

        self.refresh(DistinctClient(self.bars))
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        _, manifest, documents = self.exported(data)
        digest, rows = documents[public_id]
        self.assertEqual(digest["columns"], manifest["columns"])
        self.assertEqual(len(digest["columns"]), 57)
        self.assertTrue(all(logical == canonical for logical, canonical in digest["column_aliases"].items()))
        for index, prefix in enumerate(PREFIXES):
            self.assertEqual(rows[0][f"{prefix}_open"], str(1000 * index + 101))
            self.assertEqual(rows[0][f"{prefix}_trade_count"], str(2000000 + index))

    def test_plan_uses_only_opened_catalogue_and_unknown_ids_touch_no_source(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        with (
            patch.object(Path, "open", side_effect=AssertionError("plan opened a file")),
            patch.object(Path, "read_bytes", side_effect=AssertionError("plan read a file")),
            patch.object(os, "scandir", side_effect=AssertionError("directory discovery")),
            patch.object(data._reader, "_snapshot", side_effect=AssertionError("source selected")),
        ):
            plan = data.plan_csv_export(public_ids=[public_id])
            self.assertEqual(plan["identities"], 1)
            self.assertEqual(plan["groups"], 1)
            self.assertEqual(plan["sources"], 1)
            with self.assertRaises(ValueError):
                data.plan_csv_export(start="2026-10-12", end="2026-10-01")
        with (
            patch.object(os, "scandir", side_effect=AssertionError("directory discovery")),
            patch.object(data._reader, "_snapshot", side_effect=AssertionError("source selected")),
        ):
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.write_csv_export(self.directory / "exports/csv", public_ids=["0" * 64])

    def test_all_online_selection_excludes_unavailable_identity(self):
        self.add_symbol("BAD", [{**bar, "v": -1} for bar in self.bars])
        bad_id = StandaloneReader(self.store).resolve_ticker("BAD")["matches"][0]["public_id"]
        data = self.bootstrap()
        original_snapshot = data._reader._snapshot
        selected = []

        def select(symbol, **kwargs):
            selected.append(symbol)
            self.assertEqual(symbol, "TEST")
            return original_snapshot(symbol, **kwargs)

        with patch.object(data._reader, "_snapshot", side_effect=select):
            _, manifest, _ = self.exported(data)
        self.assertEqual(selected, ["TEST"])
        self.assertNotIn(bad_id, manifest["identities"])
        with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
            data.write_csv_export(self.directory / "exports/csv", public_ids=[bad_id])

    def test_renamed_lineage_exports_one_continuous_public_identity(self):
        bars = informative_bars("2026-09-25", "2026-10-14")
        for symbol, rows in (("OLD", informative_bars("2026-09-25", "2026-10-01")), ("NEW", bars)):
            download_pricing(self.store, symbol, "2026-10-14", client=FakeClient(rows))
        old, new = asset("OLD"), asset("NEW")
        result = run_lineage(
            self.store,
            ["NEW"],
            identity_client=IdentityClient(
                {"OLD": old, "NEW": new, "111111111": old, "222222222": new},
                [
                    {
                        "action_type": "name_change",
                        "id": "rename",
                        "process_date": "2026-10-01",
                        "old_symbol": "OLD",
                        "new_symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                    }
                ],
            ),
        )
        self.assertEqual(result["failed"], 0, result)
        data = self.bootstrap()
        public_id = data.resolve_ticker("NEW")[0].public_id
        self.assertEqual(public_id, data.resolve_ticker("OLD")[0].public_id)
        _, manifest, documents = self.exported(data, public_ids=[public_id])
        self.assertEqual(set(manifest["identities"]), {public_id})
        digest, rows = documents[public_id]
        self.assertEqual(digest["security"]["tickers"], ["NEW", "OLD"])
        self.assertEqual([row["date"] for row in rows], [bar["t"][:10] for bar in bars])
        self.assertNotIn("ticker", digest["columns"])
        self.assertNotIn("name", digest["columns"])

    def test_shared_group_reads_each_payload_once_and_never_discovers_or_reaudits(self):
        self.refresh(FakeClient(valid_bars(start="2026-09-25")))

        def split_identity(_, boundaries):
            for entry in boundaries["current_month"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["security_identity"]["security_id"] = "a" * 64

        self.publish_change(split_identity)
        self.add_symbol("OTHER")
        data = self.bootstrap()
        self.assertEqual(data.online_count(), 3)
        reads = Counter()
        original_open = Path.open
        pricing = self.store / "end_of_day/pricing/TEST"

        def opened(path, *args, **kwargs):
            if path.is_relative_to(pricing) and path.suffix in {".json", ".gz"}:
                reads[path] += 1
            return original_open(path, *args, **kwargs)

        with (
            patch.object(Path, "open", opened),
            patch.object(Path, "iterdir", side_effect=AssertionError("directory discovery")),
            patch.object(Path, "glob", side_effect=AssertionError("directory discovery")),
            patch.object(Path, "rglob", side_effect=AssertionError("directory discovery")),
            patch.object(os, "scandir", side_effect=AssertionError("directory discovery")),
            patch("libs.alpaca_pricing_download.storage.digest", side_effect=AssertionError("source hash")),
            patch(
                "libs.alpaca_pricing_download.storage.read_boundary", side_effect=AssertionError("source audit")
            ),
            patch(
                "libs.alpaca_serving_catalogue.producer.evaluate",
                side_effect=AssertionError("eligibility replay"),
            ),
            patch.object(data._reader, "_snapshot", wraps=data._reader._snapshot) as selected,
        ):
            _, manifest, documents = self.exported(data, workers=2)
        self.assertEqual(selected.call_count, 2)
        self.assertEqual(
            reads,
            Counter(
                {
                    pricing / "source.manifest.json": 1,
                    pricing / "2026/year.manifest.json": 1,
                    pricing / "2026/raw.csv.gz": 1,
                    pricing / "2026/Oct/month.manifest.json": 1,
                    pricing / "2026/Oct/raw.csv.gz": 1,
                }
            ),
        )
        self.assertEqual(manifest["counts"]["groups"], 2)
        self.assertEqual(sum(len(rows) for _, rows in documents.values()), 13 + len(self.bars))
        self.assertEqual(
            {row["date"] for _, rows in documents.values() for row in rows},
            {bar["t"][:10] for bar in valid_bars(start="2026-09-25")},
        )

    def test_final_manifest_selects_complete_outputs_and_failed_replacement_preserves_prior(self):
        data = self.bootstrap()
        destination = self.directory / "exports/csv"
        replace = os.replace
        selected_manifests, selected_paths = [], []

        def inspect_commit(source, target):
            if Path(target).name == "final_manifest.json":
                manifest = json.loads(Path(source).read_text())
                run_directory = Path(target).parent
                for public_id, entry in manifest["identities"].items():
                    csv_bytes = (run_directory / entry["csv"]).read_bytes()
                    digest = json.loads((run_directory / entry["digest"]).read_text())
                    self.assertEqual(digest["public_id"], public_id)
                    self.assertEqual(hashlib.sha256(csv_bytes).hexdigest(), digest["csv"]["sha256"])
                selected_manifests.append(manifest)
                selected_paths.append(Path(target))
            return replace(source, target)

        with patch.object(os, "replace", side_effect=inspect_commit):
            result, _, _ = self.exported(data)
        self.assertEqual(len(selected_manifests), 1)
        final_path = Path(result["manifest"])
        self.assertEqual(selected_paths, [final_path])
        original_manifest = final_path.read_bytes()
        original_entry = next(iter(selected_manifests[0]["identities"].values()))
        original_csv_path = final_path.parent / original_entry["csv"]
        original_csv = original_csv_path.read_bytes()
        failed_path = None

        def fail_commit(source, target):
            nonlocal failed_path
            if Path(target).name == "final_manifest.json":
                failed_path = Path(target)
                raise OSError("publication interrupted")
            return replace(source, target)

        with (
            patch.object(os, "replace", side_effect=fail_commit),
            self.assertRaisesRegex(OSError, "publication interrupted"),
        ):
            data.write_csv_export(destination)
        self.assertEqual(final_path.read_bytes(), original_manifest)
        self.assertEqual(original_csv_path.read_bytes(), original_csv)
        self.assertNotEqual(failed_path, final_path)
        self.assertFalse(failed_path.exists())

    def test_locked_output_refuses_without_touching_previous_selector_or_sources(self):
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        with (destination / ".export.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with (
                patch.object(data._reader, "_snapshot", side_effect=AssertionError("source read while busy")),
                self.assertRaisesRegex(ReaderBusyError, "destination is busy"),
            ):
                data.write_csv_export(destination)
            self.assertEqual(final_path.read_bytes(), previous)
        self.exported(data)

    def test_partial_csv_and_digest_writes_preserve_previous_completed_export(self):
        data = self.bootstrap()
        result, manifest, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        entry = next(iter(manifest["identities"].values()))
        completed = {
            final_path.parent / entry[name]: (final_path.parent / entry[name]).read_bytes()
            for name in ("csv", "digest")
        }
        original_open = Path.open

        class PartialWrite:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def write(self, content):
                self.stream.write(content[: max(1, len(content) // 2)])
                raise OSError("destination full")

        for suffix in ("_eod.csv", "_digest.json"):
            with self.subTest(suffix=suffix):
                interrupted = []

                def opened(path, mode="r", *args, **kwargs):
                    stream = original_open(path, mode, *args, **kwargs)
                    if path.is_relative_to(destination) and path.name.endswith(suffix) and mode == "xb":
                        interrupted.append(path)
                        return PartialWrite(stream)
                    return stream

                with patch.object(Path, "open", opened), self.assertRaisesRegex(OSError, "destination full"):
                    data.write_csv_export(destination)
                self.assertEqual(len(interrupted), 1)
                self.assertTrue(interrupted[0].read_bytes())
                self.assertEqual(final_path.read_bytes(), previous)
                for path, content in completed.items():
                    self.assertNotEqual(interrupted[0], path)
                    self.assertEqual(path.read_bytes(), content)
        self.exported(data)

    def test_range_outside_catalogue_coverage_publishes_empty_result_without_source_reads(self):
        data = self.bootstrap()
        with patch.object(data._reader, "_snapshot", side_effect=AssertionError("empty selection read source")):
            result, manifest, documents = self.exported(data, start="2030-01-01", end="2030-01-02")
        self.assertEqual(documents, {})
        self.assertEqual(manifest["identities"], {})
        for name in ("identities", "groups", "rows"):
            self.assertEqual(result[name], 0)
            self.assertEqual(manifest["counts"][name], 0)

    def test_cli_plans_without_outputs_then_exports_selected_id_and_dates(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        destination = self.directory / "cli-csv"
        arguments = [
            "export",
            "csv",
            "--store",
            str(self.store),
            "--output",
            str(destination),
            "--public-id",
            public_id,
            "--from",
            "2026-10-02",
            "--through",
            "2026-10-05",
            "--workers",
            "2",
        ]
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            patch.object(StandaloneReader, "_snapshot", side_effect=AssertionError("plan selected source")),
        ):
            self.assertEqual(main([*arguments, "--plan"]), 0)
        plan = json.loads(stdout.getvalue())
        self.assertEqual(plan["identities"], 1)
        self.assertEqual(plan["groups"], 1)
        self.assertEqual(plan["workers"], 2)
        self.assertEqual(plan["maximum_active_groups"], 1)
        self.assertFalse(destination.exists())
        self.assertEqual(len(self.lifecycle_records(stderr, last_phase="PLAN")), 2)

        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main([*arguments, "--heartbeat-seconds", "86400"]), 0, stderr.getvalue())
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["identities"], 1)
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["workers"], 2)
        self.assertEqual(len(self.lifecycle_records(stderr, last_phase="PUBLISH")), 2)
        manifest_path = Path(result["manifest"])
        self.assertEqual(manifest_path, Path(result["output"]) / "final_manifest.json")
        manifest = json.loads(manifest_path.read_text())
        self.assertEqual(set(manifest["identities"]), {public_id})
        entry = manifest["identities"][public_id]
        with (manifest_path.parent / entry["csv"]).open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual([row["date"] for row in rows], ["2026-10-02", "2026-10-05"])

    def test_cli_source_failure_reports_latest_phase_and_preserves_completed_export(self):
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()

        def source_failure(*args, **kwargs):
            try:
                raise OSError("selected payload is unreadable")
            except OSError as error:
                raise RuntimeError("source read failed") from error

        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            patch.object(StandaloneReader, "_snapshot", side_effect=source_failure),
        ):
            self.assertEqual(
                main(["export", "csv", "--store", str(self.store), "--output", str(destination)]), 1
            )
        records = self.lifecycle_records(stderr, status="error", exit_code=1, last_phase="EXPORT")
        self.assertEqual(len(records), 3)
        error = records[1]
        self.assertEqual(error["schema"], "alpaca.lifecycle.error")
        self.assertEqual(error["phase"], "EXPORT")
        self.assertEqual(error["error_type"], "RuntimeError")
        self.assertIn("source read failed", error["message"])
        self.assertIn("selected payload is unreadable", json.dumps(error))
        self.assertIn("traceback", error)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(final_path.read_bytes(), previous)

    def test_cli_keyboard_interrupt_finishes_aborted_without_error_or_publication(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        destination = self.directory / "interrupted-csv"
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            patch.object(StandaloneReader, "_snapshot", side_effect=KeyboardInterrupt),
            patch.object(os, "replace", wraps=os.replace) as replaced,
        ):
            self.assertEqual(
                main(
                    ["export", "csv", "--store", str(self.store), "--output", str(destination), "--workers", "2"]
                ),
                130,
            )
        records = self.lifecycle_records(stderr, status="aborted", exit_code=130, last_phase="EXPORT")
        self.assertEqual(len(records), 2)
        self.assertEqual(stdout.getvalue(), "")
        replaced.assert_not_called()
        self.assertFalse((destination / "final_manifest.json").exists())

    def test_cli_unexpected_catalogue_error_is_json_and_finishes_once(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            patch("alpaca_end_of_day.DataLibrary", side_effect=TypeError("unexpected catalogue error")),
        ):
            self.assertEqual(main(["export", "csv", "--store", str(self.store), "--plan"]), 1)
        records = self.lifecycle_records(stderr, status="error", exit_code=1, last_phase="CATALOGUE")
        self.assertEqual(len(records), 3)
        self.assertEqual(records[1]["schema"], "alpaca.lifecycle.error")
        self.assertEqual(records[1]["error_type"], "TypeError")
        self.assertEqual(records[1]["phase"], "CATALOGUE")
        self.assertEqual(stdout.getvalue(), "")

    def test_cli_busy_output_reports_error_exit_three_without_replacing_selector(self):
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        stdout, stderr = io.StringIO(), io.StringIO()
        with (destination / ".export.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with redirect_stdout(stdout), redirect_stderr(stderr):
                self.assertEqual(
                    main(["export", "csv", "--store", str(self.store), "--output", str(destination)]), 3
                )
        records = self.lifecycle_records(stderr, status="error", exit_code=3)
        self.assertEqual(len(records), 3)
        self.assertEqual(records[1]["error_type"], "ReaderBusyError")
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(final_path.read_bytes(), previous)

    def test_cli_stderr_write_and_flush_failures_do_not_change_successful_export(self):
        self.bootstrap()
        for broken_method in ("write", "flush"):
            with self.subTest(broken_method=broken_method):
                destination = self.directory / f"broken-{broken_method}"
                stdout, stderr = io.StringIO(), io.StringIO()
                with (
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                    patch.object(stderr, broken_method, side_effect=OSError("stderr unavailable")) as broken,
                ):
                    self.assertEqual(
                        main(["export", "csv", "--store", str(self.store), "--output", str(destination)]), 0
                    )
                self.assertGreater(broken.call_count, 0)
                result = json.loads(stdout.getvalue())
                self.assertEqual(result["identities"], 1)
                manifest_path = Path(result["manifest"])
                manifest = json.loads(manifest_path.read_text())
                self.assertEqual(manifest["counts"]["identities"], 1)
                entry = next(iter(manifest["identities"].values()))
                self.assertTrue((manifest_path.parent / entry["csv"]).is_file())
                self.assertTrue((manifest_path.parent / entry["digest"]).is_file())

    def test_cli_invalid_heartbeat_override_is_rejected_before_lifecycle_starts(self):
        for interval in ("0", "-1", "86401", "abc"):
            with self.subTest(interval=interval):
                stdout, stderr = io.StringIO(), io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
                    main(["export", "csv", "--plan", "--heartbeat-seconds", interval])
                self.assertEqual(caught.exception.code, 2)
                self.assertNotIn("alpaca.lifecycle", stderr.getvalue())
                self.assertEqual(stdout.getvalue(), "")

    def test_missing_required_payload_fails_without_recovery_or_replacing_previous_export(self):
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        payload = self.store / "end_of_day/pricing/TEST/2026/Oct/raw.csv.gz"
        payload.unlink()
        with (
            patch.object(os, "scandir", side_effect=AssertionError("failure triggered discovery")),
            self.assertRaises(CatalogChangedError),
        ):
            data.write_csv_export(destination)
        self.assertEqual(final_path.read_bytes(), previous)
        self.assertFalse(payload.exists())

    def test_stale_group_cannot_replace_previous_export_and_source_destination_is_rejected(self):
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["changed"]))
        with self.assertRaises(CatalogChangedError):
            data.write_csv_export(destination)
        self.assertEqual(final_path.read_bytes(), previous)
        with self.assertRaises(ValueError):
            data.write_csv_export(self.store / "exports")

    def test_two_workers_overlap_groups_and_preserve_bytes_order_and_callback_ownership(self):
        self.check_concurrent_export(2)

    def test_four_workers_overlap_groups_and_preserve_bytes_order_and_callback_ownership(self):
        self.check_concurrent_export(4)

    def test_three_workers_overlap_groups_and_preserve_bytes_order_and_callback_ownership(self):
        self.check_concurrent_export(3)

    def check_concurrent_export(self, workers):
        self.add_symbol("OTHER")
        self.add_symbol("THIRD")
        for symbol in ("FOURTH", "FIFTH")[: workers - 2]:
            self.add_symbol(symbol)
        data = self.bootstrap()
        serial_result, serial, documents = self.exported(data, workers=1)
        serial_directory = Path(serial_result["manifest"]).parent
        serial_bytes = {
            public_id: {field: (serial_directory / entry[field]).read_bytes() for field in ("csv", "digest")}
            for public_id, entry in serial["identities"].items()
        }
        first, second = list(serial["identities"])[:2]
        together, second_finished = Barrier(workers), Event()
        lock, active, opened, finished, callbacks = Lock(), set(), [], [], []
        open_group, write_identity, replace = data._reader._open_group, _csv_export._write_identity, os.replace

        @contextmanager
        def observe_group(symbol, **kwargs):
            with open_group(symbol, **kwargs) as group:
                with lock:
                    active.add(symbol)
                    opened.append(symbol)
                    ordinal = len(opened)
                    self.assertLessEqual(len(active), workers)
                try:
                    if ordinal <= workers:
                        together.wait(timeout=5)
                    yield group
                finally:
                    with lock:
                        active.remove(symbol)

        def reverse_completion(data, directory, security, *args):
            if security.public_id == first:
                self.assertTrue(second_finished.wait(5))
            result = write_identity(data, directory, security, *args)
            finished.append(security.public_id)
            if security.public_id == second:
                second_finished.set()
            return result

        def publish(source, target):
            self.assertFalse(active)
            self.assertFalse(any(thread.name.startswith("alpaca-csv-") for thread in threads()))
            return replace(source, target)

        def progress(stage, values):
            callbacks.append((get_ident(), stage, values.copy()))

        with (
            patch.object(data._reader, "_open_group", side_effect=observe_group),
            patch.object(_csv_export, "_write_identity", side_effect=reverse_completion),
            patch.object(os, "replace", side_effect=publish),
            patch(
                "alpaca_end_of_day.catalogue_storage.snapshot", side_effect=AssertionError("catalogue reread")
            ),
        ):
            result, parallel, parallel_documents = self.exported(data, workers=workers, progress=progress)
        self.assertEqual(result["workers"], workers)
        self.assertEqual(len(opened), workers + 1)
        self.assertLess(finished.index(second), finished.index(first))
        self.assertEqual(list(parallel["identities"]), list(serial["identities"]))
        self.assertEqual(parallel_documents, documents)
        self.assertEqual(parallel["counts"], serial["counts"])
        parallel_directory = Path(result["manifest"]).parent
        for public_id, entry in parallel["identities"].items():
            for field in ("csv", "digest"):
                self.assertEqual(
                    (parallel_directory / entry[field]).read_bytes(), serial_bytes[public_id][field]
                )
        for manifest in (serial, parallel):
            for field in ("export_directory", "started_at", "completed_at"):
                manifest.pop(field)
        self.assertEqual(serial, parallel)
        self.assertEqual({ident for ident, _, _ in callbacks}, {get_ident()})
        self.assertEqual([v["done"] for _, s, v in callbacks if s == "csv_export"], list(range(1, workers + 2)))
        self.assertEqual(callbacks[-1][1], "csv_export_publish")

    def test_parallel_failures_stop_dispatch_release_workers_and_preserve_selector(self):
        self.add_symbol("OTHER")
        self.add_symbol("THIRD")
        data = self.bootstrap()
        result, manifest, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        first = next(iter(manifest["identities"]))
        original_group, original_write, original_json = (
            _csv_export._export_group,
            _csv_export._write_identity,
            _csv_export._write_json,
        )
        for failure in ("csv", "digest", "source"):
            with self.subTest(failure=failure):
                together, opened, stop = Barrier(2), [], []

                def group(*args):
                    opened.append(args[4])
                    stop.append(args[-1])
                    together.wait(timeout=5)
                    yield from original_group(*args)

                def write(data, directory, security, *args):
                    if security.public_id != first:
                        self.assertTrue(stop[0].wait(5))
                        return original_write(data, directory, security, *args)
                    if failure == "csv":
                        raise OSError("CSV write failed")
                    if failure == "source":
                        raise CatalogChangedError("selected source changed")
                    return original_write(data, directory, security, *args)

                def write_json(path, document):
                    if failure == "digest" and path.name == f"{first}_digest.json":
                        raise OSError("digest write failed")
                    return original_json(path, document)

                with (
                    patch.object(_csv_export, "_export_group", side_effect=group),
                    patch.object(_csv_export, "_write_identity", side_effect=write),
                    patch.object(_csv_export, "_write_json", side_effect=write_json),
                    self.assertRaises((OSError, CatalogChangedError)),
                ):
                    data.write_csv_export(destination, workers=2)
                self.assertEqual(len(opened), 2)
                self.assertEqual(final_path.read_bytes(), previous)
                self.assertFalse(any(thread.name.startswith("alpaca-csv-") for thread in threads()))
        self.exported(data, workers=2)

    def test_full_result_queue_cancels_on_callback_error_or_keyboard_interrupt(self):
        self.add_symbol("OTHER")
        self.add_symbol("THIRD")
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        for error_type in (RuntimeError, KeyboardInterrupt):
            with self.subTest(error_type=error_type):
                full, together, opened, closed = Event(), Barrier(2), [], []

                class ObservedQueue(Queue):
                    def put(self, item, *args, **kwargs):
                        if self.maxsize == 4 and self.full():
                            full.set()
                        return super().put(item, *args, **kwargs)

                def group(data, directory, selected, groups, key, start, end, stopped):
                    opened.append(key)
                    try:
                        together.wait(timeout=5)
                        for _ in range(20):
                            if stopped.is_set():
                                return
                            yield "identity", groups[key][0], None
                    finally:
                        closed.append(key)

                def interrupt(stage, values):
                    if stage == "csv_export":
                        self.assertTrue(full.wait(5))
                        raise error_type("interrupted consumer")

                with (
                    patch.object(_csv_export, "Queue", ObservedQueue),
                    patch.object(_csv_export, "_export_group", side_effect=group),
                    self.assertRaisesRegex(error_type, "interrupted consumer"),
                ):
                    data.write_csv_export(destination, workers=2, progress=interrupt)
                self.assertEqual(len(opened), 2)
                self.assertCountEqual(closed, opened)
                self.assertFalse(any(thread.name.startswith("alpaca-csv-") for thread in threads()))
                self.assertEqual(final_path.read_bytes(), previous)

    def test_csv_busy_locks_fail_even_when_library_waits(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        data = DataLibrary(store=self.store, wait=True)
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        original_flock = fcntl.flock

        def nonblocking(descriptor, operation):
            if operation != fcntl.LOCK_UN:
                self.assertTrue(operation & fcntl.LOCK_NB, "CSV attempted a blocking lock")
            return original_flock(descriptor, operation)

        for path in (destination / ".export.lock", self.store / "end_of_day/pricing/TEST/.download.lock"):
            with self.subTest(path=path), path.open("rb") as lock:
                original_flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(fcntl, "flock", side_effect=nonblocking), self.assertRaises(ReaderBusyError):
                    data.write_csv_export(destination, workers=2)
                self.assertEqual(final_path.read_bytes(), previous)
                self.assertTrue(data._reader.wait)
        self.exported(data, workers=2)

    def test_store_writer_blocks_export_before_selection_or_output(self):
        data = self.bootstrap()
        destination = self.directory / "writer-blocked-csv"
        with (
            operation_lock(self.store, exclusive=True),
            patch.object(_csv_export, "_selection", side_effect=AssertionError("busy export selected work")),
            self.assertRaisesRegex(ReaderBusyError, "source store is busy"),
        ):
            data.write_csv_export(destination)
        self.assertFalse(destination.exists())

    def test_export_holds_store_lock_through_manifest_and_rejects_stale_catalogue(self):
        data = self.bootstrap()
        destination = self.directory / "store-locked-csv"
        replace = os.replace
        observed = []

        def publish(source, target):
            if Path(target).name == "final_manifest.json":
                with self.assertRaises(StoreBusyError):
                    with operation_lock(self.store, exclusive=True):
                        pass
                observed.append(Path(target))
            return replace(source, target)

        with patch.object(os, "replace", side_effect=publish):
            result = data.write_csv_export(destination)
        self.assertEqual(observed, [Path(result["manifest"])])
        with operation_lock(self.store, exclusive=True):
            pass

        stale_destination = self.directory / "stale-csv"
        with (
            patch.object(
                _csv_export.catalogue_storage,
                "selector_state",
                return_value={"generation": "changed"},
            ),
            patch.object(_csv_export, "_selection", side_effect=AssertionError("stale export selected work")),
            self.assertRaisesRegex(CatalogChangedError, "open a new DataLibrary"),
        ):
            data.write_csv_export(stale_destination)
        self.assertFalse(stale_destination.exists())

    def test_cli_store_writer_blocks_before_catalogue_selection(self):
        self.bootstrap()
        destination = self.directory / "cli-writer-blocked-csv"
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            operation_lock(self.store, exclusive=True),
            patch("alpaca_end_of_day.DataLibrary", side_effect=AssertionError("busy CLI opened catalogue")),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            self.assertEqual(
                main(["export", "csv", "--store", str(self.store), "--output", str(destination)]), 3
            )
        records = self.lifecycle_records(stderr, status="error", exit_code=3, last_phase="INIT")
        self.assertEqual(records[1]["error_type"], "StoreBusyError")
        self.assertEqual(stdout.getvalue(), "")
        self.assertFalse(destination.exists())

    def test_worker_arguments_validate_before_selection_and_csv_rejects_wait(self):
        data = self.bootstrap()
        destination = self.directory / "invalid-csv"
        for workers in (True, False, 0, 5, 1.0, "2", None):
            with (
                self.subTest(workers=workers),
                patch.object(
                    _csv_export, "_selection", side_effect=AssertionError("invalid workers reached selection")
                ),
            ):
                with self.assertRaises(ValueError):
                    data.plan_csv_export(workers=workers)
                with self.assertRaises(ValueError):
                    data.write_csv_export(destination, workers=workers)
        self.assertFalse(destination.exists())
        for arguments in (("--wait",), ("--workers", "0"), ("--workers", "5")):
            stderr = io.StringIO()
            with redirect_stderr(stderr), self.assertRaises(SystemExit) as caught:
                main(["export", "csv", "--plan", *arguments])
            self.assertEqual(caught.exception.code, 2)
            self.assertNotIn("alpaca.lifecycle", stderr.getvalue())

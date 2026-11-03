"""CSV output consumes published selections once and publishes its selector last."""

import csv
import fcntl
import hashlib
import io
import json
import os
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Lock, get_ident
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
            self.assertEqual(record["schema_version"], 4)
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

    def test_eight_price_views_keep_specific_names_while_invariant_activity_aliases(self):
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
                        "v": Decimal(100000),
                        "n": 2000000,
                        "vw": Decimal(1000 * index) + Decimal("101.5"),
                    }
                    for row in rows
                ]

        self.refresh(DistinctClient(self.bars))
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        _, manifest, documents = self.exported(data)
        digest, rows = documents[public_id]
        self.assertEqual(len(digest["columns"]), 43)
        self.assertEqual(set(digest["column_aliases"]), set(manifest["columns"]) - {"date"})
        for index, prefix in enumerate(PREFIXES):
            self.assertEqual(rows[0][f"{prefix}_open"], str(1000 * index + 101))
            self.assertEqual(digest["column_aliases"][f"{prefix}_trade_count"], "raw_trade_count")
        self.assertEqual(rows[0]["raw_trade_count"], "2000000")

    def test_plan_uses_only_opened_catalogue_and_unknown_ids_touch_no_source(self):
        data = self.bootstrap()
        public_id = data.list_securities()[0].public_id
        with (
            patch.object(Path, "open", side_effect=AssertionError("plan opened a file")),
            patch.object(Path, "read_bytes", side_effect=AssertionError("plan read a file")),
            patch.object(os, "scandir", side_effect=AssertionError("directory discovery")),
            patch.object(data._reader, "_snapshot", side_effect=AssertionError("source selected")),
        ):
            plan = data.plan_csv_export(public_ids=[public_id], memory_bytes=1024**3)
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

    def test_each_csv_is_written_once_from_a_complete_memory_buffer(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        writes = {}
        original_open = Path.open

        class ObservedWrite:
            def __init__(self, path, stream):
                self.path, self.stream = path, stream

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def write(self, content):
                writes.setdefault(self.path, []).append(len(content))
                return self.stream.write(content)

        def opened(path, mode="r", *args, **kwargs):
            stream = original_open(path, mode, *args, **kwargs)
            if path.name.endswith("_eod.csv") and mode == "xb":
                return ObservedWrite(path, stream)
            return stream

        with patch.object(Path, "open", opened):
            result, manifest, _ = self.exported(data, workers=2)
        directory = Path(result["output"])
        self.assertEqual(
            writes,
            {directory / entry["csv"]: [entry["csv_bytes"]] for entry in manifest["identities"].values()},
        )

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
            self.assertEqual(main(arguments), 0, stderr.getvalue())
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

    def test_cli_heartbeat_override_is_rejected_before_lifecycle_starts(self):
        for interval in ("1", "60", "86400", "0", "-1", "86401", "abc"):
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

    def test_export_trusts_selected_lineage_without_reauthenticating_its_generation(self):
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["changed"]))
        # A completed catalogue is authority. An upstream metadata change is
        # not permission for CSV to reopen the index or hash the selected body.
        _, _, documents = self.exported(data)
        self.assertTrue(documents)
        self.assertEqual(final_path.read_bytes(), previous)
        with self.assertRaises(ValueError):
            data.write_csv_export(self.store / "exports")

    def test_export_consumes_selected_bodies_once_without_metadata_rechecks(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        opened = Counter()
        original_open, original_stat = Path.open, Path.stat

        def open_file(path, *args, **kwargs):
            if path.is_relative_to(self.store):
                self.assertNotIn(path.name, {"store.manifest.json", "update.json", ".pending.json"})
                opened[path] += 1
            return original_open(path, *args, **kwargs)

        def stat(path, *args, **kwargs):
            if path != self.store and path.is_relative_to(self.store):
                raise AssertionError(f"export rechecked selected path: {path}")
            return original_stat(path, *args, **kwargs)

        with (
            patch.object(Path, "open", open_file),
            patch.object(Path, "stat", stat),
            patch("alpaca_end_of_day.standalone.check_store", side_effect=AssertionError("store recheck")),
            patch("alpaca_end_of_day.standalone.read_index", side_effect=AssertionError("index reselection")),
            patch("alpaca_end_of_day.standalone.reader_snapshot", side_effect=AssertionError("handshake")),
            patch("alpaca_end_of_day.standalone.read_update", side_effect=AssertionError("notice probe")),
            patch(
                "alpaca_end_of_day.standalone.check_unavailable_pricing",
                side_effect=AssertionError("absence recheck"),
            ),
            patch.object(
                _csv_export.catalogue_storage, "assert_group", side_effect=AssertionError("catalogue recheck")
            ),
            patch.object(
                _csv_export.catalogue_storage,
                "selector_state",
                wraps=_csv_export.catalogue_storage.selector_state,
            ) as token,
        ):
            result = data.write_csv_export(self.directory / "trusted-export", workers=2)
        self.assertEqual(result["groups"], 2)
        self.assertEqual(token.call_count, 1)  # Accept a previously opened DataLibrary under the store gate.
        self.assertTrue(opened)
        self.assertTrue(all(count == 1 for count in opened.values()), opened)

    def test_encoding_workers_overlap_but_disk_io_stays_on_the_coordinator(self):
        for symbol in ("OTHER", "THIRD", "FOURTH", "FIFTH"):
            self.add_symbol(symbol)
        data = self.bootstrap()
        serial_result, serial, documents = self.exported(data, workers=1)
        serial_directory = Path(serial_result["output"])
        serial_bytes = {
            public_id: {field: (serial_directory / entry[field]).read_bytes() for field in ("csv", "digest")}
            for public_id, entry in serial["identities"].items()
        }
        coordinator = get_ident()
        original_prepare, original_open = _csv_export._prepare_identity, Path.open
        for workers in (2, 3, 4):
            with self.subTest(workers=workers):
                barrier, lock, started, callbacks, io_events = Barrier(workers), Lock(), [], [], []

                def prepare(*args):
                    with lock:
                        started.append(get_ident())
                        ordinal = len(started)
                    if ordinal <= workers:
                        barrier.wait(5)
                    return original_prepare(*args)

                def opened(path, mode="r", *args, **kwargs):
                    if path.is_relative_to(self.store) or path.name.endswith(("_eod.csv", "_digest.json")):
                        self.assertEqual(get_ident(), coordinator)
                        io_events.append("write" if mode == "xb" else "read")
                    return original_open(path, mode, *args, **kwargs)

                def progress(stage, values):
                    self.assertEqual(get_ident(), coordinator)
                    callbacks.append((stage, values))

                with patch.object(_csv_export, "_prepare_identity", prepare), patch.object(Path, "open", opened):
                    result = data.write_csv_export(
                        self.directory / "parallel", workers=workers, progress=progress
                    )
                self.assertEqual(len(set(started)), workers)
                first_write = io_events.index("write")
                self.assertNotIn("read", io_events[first_write:])
                manifest = json.loads(Path(result["manifest"]).read_text())
                self.assertEqual(manifest["counts"], serial["counts"])
                self.assertEqual(list(manifest["identities"]), list(serial["identities"]))
                for public_id, entry in manifest["identities"].items():
                    self.assertEqual(
                        {
                            field: (Path(result["output"]) / entry[field]).read_bytes()
                            for field in ("csv", "digest")
                        },
                        serial_bytes[public_id],
                    )
                self.assertEqual(callbacks[-1][0], "csv_export_publish")
                self.assertFalse(any(thread.name.startswith("alpaca-csv") for thread in threads()))

    def test_batch_budget_splits_writes_without_reopening_shared_payloads(self):
        self.refresh(FakeClient(valid_bars(start="2026-09-25")))

        def split_identity(_, boundaries):
            for entry in boundaries["current_month"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["security_identity"]["security_id"] = "a" * 64

        self.publish_change(split_identity)
        self.add_symbol("OTHER")
        data = self.bootstrap()
        sizes, prepared_objects = [], []
        original_prepare = _csv_export._prepare_identity

        def prepare(*args):
            prepared = original_prepare(*args)
            if prepared:
                sizes.append(prepared.size)
                prepared_objects.append(prepared)
            return prepared

        with patch.object(_csv_export, "_prepare_identity", prepare):
            _, baseline, baseline_documents = self.exported(data, workers=1)
        budget = max(sizes)
        self.assertLess(budget, sum(sizes))
        reads, phases = Counter(), []
        phase = "read"
        original_open = Path.open

        def opened(path, *args, **kwargs):
            if path.is_relative_to(self.store):
                self.assertEqual(phase, "read")
            if path.name.endswith(("_eod.csv", "_digest.json")) and args and args[0] == "xb":
                self.assertEqual(phase, "write")
            if path.is_relative_to(self.store) and path.suffix == ".gz":
                reads[path] += 1
            return original_open(path, *args, **kwargs)

        def progress(stage, values):
            nonlocal phase
            if stage == "csv_export_buffer":
                phase = values["io_phase"]
                self.assertLessEqual(values["buffered_bytes"], budget)
                phases.append(values.copy())

        with patch.object(Path, "open", opened), patch.object(_csv_export, "_prepare_identity", prepare):
            _, manifest, documents = self.exported(data, workers=2, memory_bytes=budget, progress=progress)
        self.assertEqual(documents, baseline_documents)
        self.assertEqual(manifest["counts"], baseline["counts"])
        self.assertGreater(sum(p["io_phase"] == "write" for p in phases), 1)
        self.assertTrue(reads)
        self.assertTrue(all(count == 1 for count in reads.values()), reads)
        self.assertTrue(all(prepared.csv.closed for prepared in prepared_objects))

    def test_batch_failures_and_callback_interruptions_release_buffers_and_preserve_previous_export(self):
        self.add_symbol("OTHER")
        self.add_symbol("THIRD")
        data = self.bootstrap()
        result, _, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = Path(result["manifest"])
        previous = final_path.read_bytes()
        original_prepare = _csv_export._prepare_identity
        for failure in ("source", "encode", "csv", "digest", "read_callback", "write_callback", "interrupt"):
            with self.subTest(failure=failure):
                prepared_objects = []
                opened_groups = []
                original_group = _csv_export._export_group

                def group(*args):
                    opened_groups.append(args[3])
                    if failure == "source" and len(opened_groups) == 2:
                        raise CatalogChangedError("selected source failed")
                    yield from original_group(*args)

                def prepare(*args):
                    if failure == "encode":
                        raise MemoryError("encoding failed")
                    prepared = original_prepare(*args)
                    if prepared:
                        prepared_objects.append(prepared)
                    return prepared

                original_write, original_json = _csv_export._write_identity, _csv_export._write_json

                def write(*args):
                    if failure == "csv":
                        raise OSError("write failed")
                    return original_write(*args)

                def write_json(path, document):
                    if failure == "digest" and path.name.endswith("_digest.json"):
                        raise OSError("digest failed")
                    return original_json(path, document)

                def progress(stage, values):
                    if stage == "csv_export_buffer" and values["prepared"]:
                        if failure == "read_callback" and values["io_phase"] == "read":
                            raise RuntimeError("read callback failed")
                        if failure == "interrupt":
                            raise KeyboardInterrupt()
                    if failure == "write_callback" and stage == "csv_export":
                        raise RuntimeError("write callback failed")

                with (
                    patch.object(_csv_export, "_export_group", group),
                    patch.object(_csv_export, "_prepare_identity", prepare),
                    patch.object(_csv_export, "_write_identity", write),
                    patch.object(_csv_export, "_write_json", write_json),
                    self.assertRaises(
                        (CatalogChangedError, MemoryError, OSError, RuntimeError, KeyboardInterrupt)
                    ),
                ):
                    data.write_csv_export(destination, workers=2, progress=progress)
                self.assertEqual(final_path.read_bytes(), previous)
                self.assertTrue(all(prepared.csv.closed for prepared in prepared_objects))
                self.assertFalse(any(thread.name.startswith("alpaca-csv") for thread in threads()))
        self.exported(data)

    def test_memory_budget_defaults_overrides_and_oversized_identity(self):
        data = self.bootstrap()
        for available, expected in ((512, 1024), (6144, 3072), (32768, 5120)):
            with patch.object(_csv_export, "_available_memory", return_value=available * 1024**2):
                self.assertEqual(data.plan_csv_export()["memory_bytes"], expected * 1024**2)
        with patch.object(_csv_export, "_available_memory", side_effect=AssertionError("unneeded memory query")):
            self.assertEqual(data.plan_csv_export(memory_bytes=12345)["memory_bytes"], 12345)
        for value in (0, -1, True, 1.5, "1024"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                data.plan_csv_export(memory_bytes=value)
        with self.assertRaisesRegex(MemoryError, "one identity"):
            data.write_csv_export(self.directory / "too-small", memory_bytes=1)
        self.assertFalse(list((self.directory / "too-small").glob("*/final_manifest.json")))
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(
                main(["export", "csv", "--store", str(self.store), "--plan", "--memory-mib", "2"]), 0
            )
        self.assertEqual(json.loads(stdout.getvalue())["memory_bytes"], 2 * 1024**2)

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

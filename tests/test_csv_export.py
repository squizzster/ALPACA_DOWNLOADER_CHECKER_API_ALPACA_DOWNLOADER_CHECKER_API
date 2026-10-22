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
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import (
    ADJUSTMENTS,
    CatalogChangedError,
    PublicIdUnavailableError,
    ReaderBusyError,
    StandaloneReader,
)
from libs.alpaca_cli import main
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.download import download_pricing

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
        manifest = json.loads((destination / "final_manifest.json").read_text())
        documents = {}
        for public_id, entry in manifest["identities"].items():
            csv_path = destination / entry["csv"]
            digest_path = destination / entry["digest"]
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
            self.assertEqual(csv_path.parent.parent.parent, destination)
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
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.write_csv_export(self.directory / "exports/csv", public_ids=["0" * 64])
            with self.assertRaises(ValueError):
                data.plan_csv_export(start="2026-10-12", end="2026-10-01")

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
        bars = valid_bars(start="2026-09-25")
        for symbol, rows in (("OLD", valid_bars("2026-09-25", "2026-10-01")), ("NEW", bars)):
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
        data = self.bootstrap()
        self.assertEqual(data.online_count(), 2)
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
            _, manifest, documents = self.exported(data)
        self.assertEqual(selected.call_count, 1)
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
        self.assertEqual(manifest["counts"]["groups"], 1)
        self.assertEqual(sum(len(rows) for _, rows in documents.values()), 13)
        self.assertEqual(
            {row["date"] for _, rows in documents.values() for row in rows},
            {bar["t"][:10] for bar in valid_bars(start="2026-09-25")},
        )

    def test_final_manifest_selects_complete_outputs_and_failed_replacement_preserves_prior(self):
        data = self.bootstrap()
        destination = self.directory / "exports/csv"
        final_path = destination / "final_manifest.json"
        replace = os.replace
        selected_manifests = []

        def inspect_commit(source, target):
            if Path(target) == final_path:
                manifest = json.loads(Path(source).read_text())
                for public_id, entry in manifest["identities"].items():
                    csv_bytes = (destination / entry["csv"]).read_bytes()
                    digest = json.loads((destination / entry["digest"]).read_text())
                    self.assertEqual(digest["public_id"], public_id)
                    self.assertEqual(hashlib.sha256(csv_bytes).hexdigest(), digest["csv"]["sha256"])
                selected_manifests.append(manifest)
            return replace(source, target)

        with patch.object(os, "replace", side_effect=inspect_commit):
            self.exported(data)
        self.assertEqual(len(selected_manifests), 1)
        original_manifest = final_path.read_bytes()
        original_entry = next(iter(selected_manifests[0]["identities"].values()))
        original_csv = (destination / original_entry["csv"]).read_bytes()

        def fail_commit(source, target):
            if Path(target) == final_path:
                raise OSError("publication interrupted")
            return replace(source, target)

        with (
            patch.object(os, "replace", side_effect=fail_commit),
            self.assertRaisesRegex(OSError, "publication interrupted"),
        ):
            data.write_csv_export(destination)
        self.assertEqual(final_path.read_bytes(), original_manifest)
        self.assertEqual((destination / original_entry["csv"]).read_bytes(), original_csv)

    def test_locked_output_refuses_without_touching_previous_selector_or_sources(self):
        data = self.bootstrap()
        self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = destination / "final_manifest.json"
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
        _, manifest, _ = self.exported(data)
        destination = self.directory / "exports/csv"
        final_path = destination / "final_manifest.json"
        previous = final_path.read_bytes()
        entry = next(iter(manifest["identities"].values()))
        completed = {
            destination / entry[name]: (destination / entry[name]).read_bytes() for name in ("csv", "digest")
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
        self.assertFalse(destination.exists())

        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(main(arguments), 0, stderr.getvalue())
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["identities"], 1)
        self.assertEqual(result["rows"], 2)
        manifest = json.loads((destination / "final_manifest.json").read_text())
        self.assertEqual(set(manifest["identities"]), {public_id})
        entry = manifest["identities"][public_id]
        with (destination / entry["csv"]).open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual([row["date"] for row in rows], ["2026-10-02", "2026-10-05"])

    def test_missing_required_payload_fails_without_recovery_or_replacing_previous_export(self):
        data = self.bootstrap()
        self.exported(data)
        destination = self.directory / "exports/csv"
        previous = (destination / "final_manifest.json").read_bytes()
        payload = self.store / "end_of_day/pricing/TEST/2026/Oct/raw.csv.gz"
        payload.unlink()
        with (
            patch.object(os, "scandir", side_effect=AssertionError("failure triggered discovery")),
            self.assertRaises(CatalogChangedError),
        ):
            data.write_csv_export(destination)
        self.assertEqual((destination / "final_manifest.json").read_bytes(), previous)
        self.assertFalse(payload.exists())

    def test_stale_group_cannot_replace_previous_export_and_source_destination_is_rejected(self):
        data = self.bootstrap()
        self.exported(data)
        destination = self.directory / "exports/csv"
        previous = (destination / "final_manifest.json").read_bytes()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["changed"]))
        with self.assertRaises(CatalogChangedError):
            data.write_csv_export(destination)
        self.assertEqual((destination / "final_manifest.json").read_bytes(), previous)
        with self.assertRaises(ValueError):
            data.write_csv_export(self.store / "exports")

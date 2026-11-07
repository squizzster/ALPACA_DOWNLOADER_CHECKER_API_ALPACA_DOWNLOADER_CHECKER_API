"""Current completed export adapter, with forbidden I/O and value-parity checks."""

import csv
import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import alpaca_to_bloodhound as app
import current_csv
import pyarrow.parquet as pq
from test_converter import SECOND_ID, SOURCE_ID, example_views, write_manifest


def write_current(root, views=None, pid=SOURCE_ID, intervals=True, schema_version=1):
    root.mkdir(parents=True, exist_ok=True)
    source = example_views(pid) if views is None else views
    fields = ("open", "high", "low", "close", "volume", "trade_count", "vwap")
    aliases, physical, columns, mappings = {}, {}, ["date"], {}
    for name in app.ADJUSTMENTS:
        rows = source["split"] if name == "all" or "split" in name else source["raw"]
        prefix = name.replace("spin-off", "spin_off").replace(",", "_")
        mapping = {}
        for field in fields:
            logical = prefix + "_" + field
            values = tuple(str(r[field]) for r in rows)
            target = next((k for k, v in physical.items() if v == values), None)
            if target is None:
                target = logical
                physical[target] = values
                columns.append(target)
            aliases[logical] = target
            mapping[field] = target
        mappings[name] = {"columns": mapping}
    folder = root / pid
    folder.mkdir(exist_ok=True)
    csv_name, digest_name = f"{pid}_eod.csv", f"{pid}_digest.json"
    text = io.StringIO(newline="")
    writer = csv.writer(text)
    writer.writerow(columns)
    writer.writerows(zip([r["session"] for r in source["raw"]], *physical.values()))
    payload = text.getvalue().encode()
    (folder / csv_name).write_bytes(payload)
    security = {
        "public_id": pid,
        "tickers": ["OLD", "NEW"],
        "names": ["Undated latest name"],
        "first_session": "2016-01-04",
        "last_session": "2026-11-03",
        "ticker_ownership_intervals": [
            {"ticker": "OLD", "start_inclusive": None, "end_exclusive": "2024-01-04"},
            {"ticker": "NEW", "start_inclusive": "2024-01-04", "end_exclusive": None},
        ]
        if intervals
        else [],
    }
    if schema_version == 2:
        security["ticker_ranges"] = [
            {
                "ticker": "OLD",
                "first_session": source["raw"][0]["session"],
                "last_session": source["raw"][1]["session"],
            },
            {
                "ticker": "NEW",
                "first_session": source["raw"][2]["session"],
                "last_session": source["raw"][2]["session"],
            },
        ]
    digest = {
        "schema_version": schema_version,
        "api_version": 26,
        "public_id": pid,
        "catalogue_generation": "fixture",
        "columns": columns,
        "column_aliases": aliases,
        "views": mappings,
        "security": security,
        "range": {
            "start": source["raw"][0]["session"],
            "end": source["raw"][-1]["session"],
            "inclusive": True,
        },
        "csv": {"file": csv_name, "sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)},
    }
    (folder / digest_name).write_text(json.dumps(digest))
    path = root / "final_manifest.json"
    selector = (
        json.loads(path.read_text())
        if path.exists()
        else {
            "schema_version": schema_version,
            "api_version": 26,
            "format": "alpaca_public_id_eod_csv",
            "completed_at": "2026-11-05T00:00:00Z",
            "catalogue": {"generation": "fixture"},
            "identities": {},
        }
    )
    selector["identities"][pid] = {"csv": f"{pid}/{csv_name}", "digest": f"{pid}/{digest_name}"}
    path.write_text(json.dumps(selector))
    return digest


class CurrentCSVTests(unittest.TestCase):
    def test_completed_export_grid_is_not_sorted_again_for_each_view(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_current(root)
            history = app.discover_histories(root)[0]
            with patch.object(
                app.pd.DataFrame, "sort_values", side_effect=AssertionError("accepted grid sorted again")
            ):
                frames, _, _ = app.load_history(history)
            self.assertEqual(set(frames), set(app.ADJUSTMENTS))
            self.assertTrue(
                all(frame["session"].tolist() == frames["raw"]["session"].tolist() for frame in frames.values())
            )

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.source = self.root / "current"
        self.output = self.root / "result.parquet"

    def convert(self, *options):
        args = app.parser().parse_args(
            ["--data-in", str(self.source), "--blood-out", str(self.output), "--quiet", *options]
        )
        return app.convert(args)

    def change_digest(self, mutate, pid=SOURCE_ID):
        p = self.source / pid / f"{pid}_digest.json"
        d = json.loads(p.read_text())
        mutate(d)
        p.write_text(json.dumps(d))

    def test_current_and_legacy_conversion_have_identical_numeric_rows(self):
        write_current(self.source)
        report = self.convert()
        current = pq.read_table(self.output).to_pydict()
        legacy = self.root / "legacy"
        write_manifest(legacy / "security")
        legacy_out = self.root / "legacy.parquet"
        app.convert(
            app.parser().parse_args(["--data-in", str(legacy), "--blood-out", str(legacy_out), "--quiet"])
        )
        self.assertEqual(current, pq.read_table(legacy_out).to_pydict())
        self.assertEqual(current["symbol"], ["OLD", "OLD", "NEW"])
        self.assertEqual(current["company_name"], [None] * 3)
        self.assertEqual(report["input_format"], "alpaca_public_id_eod_csv")
        self.assertEqual(
            report["histories"][0]["payloads"][0]["checksum_authority"], "completed_export_digest_not_recomputed"
        )

    def test_cross_field_aliases_and_exact_source_values(self):
        views = example_views()
        for rows in views.values():
            for row in rows:
                row["high"] = row["open"] = row["close"]
        digest = write_current(self.source, views)
        self.assertEqual(digest["column_aliases"]["raw_high"], "raw_open")
        self.convert()
        rows = pq.read_table(self.output).to_pylist()
        for index, row in enumerate(rows):
            for field in app.OHLC:
                self.assertEqual(row["adjusted_" + field], float(views["all"][index][field]))
            self.assertEqual(row["volume"], float(views["raw"][index]["volume"]))
            self.assertEqual(row["raw_close"], float(views["raw"][index]["close"]))

    def test_selected_work_reads_selector_digest_and_payload_once_without_crawl_or_hash(self):
        write_current(self.source)
        self.change_digest(
            lambda d: d["security"]["ticker_ownership_intervals"].extend(
                [
                    {"ticker": "EXPIRED", "start_inclusive": None, "end_exclusive": "2024-01-02"},
                    {"ticker": "FUTURE", "start_inclusive": "2024-01-05", "end_exclusive": None},
                ]
            )
        )
        write_current(self.source, pid=SECOND_ID)
        reads = []
        read_text, read_bytes = Path.read_text, Path.read_bytes

        def text(path, *a, **kw):
            reads.append(path)
            return read_text(path, *a, **kw)

        def binary(path, *a, **kw):
            reads.append(path)
            return read_bytes(path, *a, **kw)

        with (
            patch.object(Path, "rglob", side_effect=AssertionError("unexpected directory crawl")),
            patch.object(Path, "read_text", text),
            patch.object(Path, "read_bytes", binary),
            patch.object(app, "sha256_file", wraps=app.sha256_file) as hashes,
            patch.object(
                current_csv, "_selected_ticker_intervals", wraps=current_csv._selected_ticker_intervals
            ) as interval_selection,
        ):
            self.convert("--public-id", SOURCE_ID)
        selected = [p for p in reads if p.is_relative_to(self.source)]
        self.assertEqual(len(selected), 3)
        self.assertEqual(len(set(selected)), 3)
        self.assertFalse(any(SECOND_ID in str(p) for p in selected))
        self.assertEqual(hashes.call_count, 1)  # Only newly produced output is hashed.
        self.assertEqual(interval_selection.call_count, 1)  # Shared by every input row and all eight views.

    def test_runtime_release_matches_owning_changelog(self):
        entries = json.loads((Path(app.__file__).parent / "change_log.json").read_text())
        self.assertEqual(len(entries), app.RUNTIME_VERSION)
        for entry in entries:
            self.assertNotIn("version", entry)
            self.assertTrue(entry["modifications"])

    def test_all_56_logical_series_are_delivered_intact(self):
        expected = example_views()
        write_current(self.source, expected)
        (history,) = app.discover_histories(self.source)
        frames, payloads, issues = app.load_history(history)
        self.assertFalse(issues)
        self.assertEqual(len(payloads), 1)
        for name in app.ADJUSTMENTS:
            rows = expected["split"] if name == "all" or "split" in name else expected["raw"]
            for field in ("open", "high", "low", "close", "volume", "trade_count", "vwap"):
                self.assertEqual(frames[name][field].tolist(), [float(row[field]) for row in rows])

    def test_physical_column_names_are_not_guessed(self):
        write_current(self.source)
        path = self.source / SOURCE_ID / f"{SOURCE_ID}_eod.csv"
        text = path.read_text().replace("raw_close", "physical_series_123")
        path.write_text(text)

        def rename(d):
            d["columns"] = ["physical_series_123" if c == "raw_close" else c for c in d["columns"]]
            d["column_aliases"] = {
                k: "physical_series_123" if v == "raw_close" else v for k, v in d["column_aliases"].items()
            }
            for spec in d["views"].values():
                spec["columns"] = {
                    k: "physical_series_123" if v == "raw_close" else v for k, v in spec["columns"].items()
                }

        self.change_digest(rename)
        self.convert()
        self.assertEqual(pq.read_table(self.output)["raw_close"].to_pylist(), [101, 91, 35])

    def test_unsupported_final_schema_fails_without_directory_discovery(self):
        write_current(self.source)
        path = self.source / "final_manifest.json"
        d = json.loads(path.read_text())
        d["schema_version"] = 3
        path.write_text(json.dumps(d))
        with patch.object(Path, "rglob", side_effect=AssertionError("unexpected fallback crawl")):
            with self.assertRaisesRegex(ValueError, "Unsupported"):
                self.convert()

    def test_overlapping_labels_are_not_chosen_arbitrarily(self):
        write_current(self.source)
        self.change_digest(
            lambda d: d["security"]["ticker_ownership_intervals"].append(
                {"ticker": "NEW", "start_inclusive": None, "end_exclusive": None}
            )
        )
        with self.assertRaisesRegex(ValueError, "rejected"):
            self.convert()

    def test_schema_two_uses_published_ranges_without_replaying_ownership(self):
        write_current(self.source, schema_version=2)
        self.change_digest(
            lambda d: d["security"].update(
                tickers=["NEW"],
                ticker_ownership_intervals=[
                    {"ticker": "OLD", "start_inclusive": None, "end_exclusive": None},
                    {"ticker": "NEW", "start_inclusive": None, "end_exclusive": None},
                ],
            )
        )
        with patch.object(
            current_csv, "_selected_ticker_intervals", side_effect=AssertionError("replayed ownership evidence")
        ):
            report = self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "OLD", "NEW"])
        self.assertEqual(
            report["histories"][0]["payloads"][0]["ticker_label_authority"], "completed_export_ticker_ranges"
        )

    def test_schema_two_ignores_missing_or_malformed_descriptive_ownership(self):
        write_current(self.source, schema_version=2)
        self.change_digest(lambda d: d["security"].update(ticker_ownership_intervals=None))
        self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "OLD", "NEW"])

    def test_schema_two_preserves_inclusive_ends_and_same_label_overlap(self):
        write_current(self.source, schema_version=2)
        self.change_digest(
            lambda d: d["security"]["ticker_ranges"].append(
                {"ticker": "OLD", "first_session": "2024-01-02", "last_session": "2024-01-02"}
            )
        )
        self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "OLD", "NEW"])

    def test_schema_two_actual_overlap_needs_explicit_dated_metadata(self):
        write_current(self.source, schema_version=2)
        self.change_digest(lambda d: d["security"]["ticker_ranges"][0].update(last_session="2024-01-04"))
        with self.assertRaisesRegex(ValueError, "rejected"):
            self.convert()
        self.assertFalse(self.output.exists())
        metadata = self.root / "metadata.csv"
        metadata.write_text(f"public_id,start_date,end_date,symbol\n{SOURCE_ID},2024-01-04,2024-01-04,NEW\n")
        self.convert("--metadata", str(metadata), "--overwrite")
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "OLD", "NEW"])

    def test_schema_two_gap_is_not_filled_from_ownership_or_single_ticker(self):
        write_current(self.source, schema_version=2)
        self.change_digest(
            lambda d: (
                d["security"]["ticker_ranges"][0].update(last_session="2024-01-02"),
                d["security"].update(tickers=["NEW"]),
            )
        )
        with patch.object(
            current_csv, "_selected_ticker_intervals", side_effect=AssertionError("fell back to ownership")
        ):
            with self.assertRaisesRegex(ValueError, "rejected"):
                self.convert()
        self.assertFalse(self.output.exists())

    def test_schema_two_missing_label_handoff_fails_before_payload_read(self):
        for value in ("missing", None):
            with self.subTest(value=value):
                write_current(self.source, schema_version=2)
                if value == "missing":
                    self.change_digest(lambda d: d["security"].pop("ticker_ranges"))
                else:
                    self.change_digest(lambda d: d["security"].update(ticker_ranges=value))
                with patch.object(Path, "read_bytes", side_effect=AssertionError("unexpected CSV read")):
                    with self.assertRaisesRegex(ValueError, "rejected"):
                        self.convert("--overwrite")
                report = json.loads(Path(str(self.output) + ".report.json").read_text())
                self.assertIn("ticker_ranges", report["omitted_histories"][0]["error"])

    def test_schema_two_empty_label_handoff_does_not_trigger_legacy_fallback(self):
        write_current(self.source, schema_version=2)
        self.change_digest(lambda d: d["security"].update(ticker_ranges=[]))
        with patch.object(
            current_csv, "_selected_ticker_intervals", side_effect=AssertionError("fell back to ownership")
        ):
            with self.assertRaisesRegex(ValueError, "rejected"):
                self.convert()

    def test_schema_two_selector_and_digest_versions_must_match(self):
        write_current(self.source, schema_version=2)
        self.change_digest(lambda d: d.update(schema_version=1))
        with patch.object(Path, "read_bytes", side_effect=AssertionError("unexpected CSV read")):
            with self.assertRaisesRegex(ValueError, "rejected"):
                self.convert()

    def test_schema_two_selected_handoff_is_consumed_once_without_source_checks(self):
        write_current(self.source, schema_version=2)
        write_current(self.source, pid=SECOND_ID, schema_version=2)
        reads = []
        read_text, read_bytes = Path.read_text, Path.read_bytes

        def text(path, *args, **kwargs):
            reads.append(path)
            return read_text(path, *args, **kwargs)

        def binary(path, *args, **kwargs):
            reads.append(path)
            return read_bytes(path, *args, **kwargs)

        with (
            patch.object(Path, "rglob", side_effect=AssertionError("unexpected crawl")),
            patch.object(Path, "read_text", text),
            patch.object(Path, "read_bytes", binary),
            patch.object(app, "sha256_file", wraps=app.sha256_file) as hashes,
            patch.object(
                current_csv, "_published_ticker_labels", wraps=current_csv._published_ticker_labels
            ) as labels,
            patch.object(
                current_csv, "_selected_ticker_intervals", side_effect=AssertionError("ownership replay")
            ),
        ):
            self.convert("--public-id", SOURCE_ID)
        source_reads = [path for path in reads if path.is_relative_to(self.source)]
        self.assertEqual(len(source_reads), 3)
        self.assertEqual(len(set(source_reads)), 3)
        self.assertFalse(any(SECOND_ID in str(path) for path in source_reads))
        self.assertEqual(hashes.call_count, 1)
        self.assertEqual(labels.call_count, 1)

    def test_current_action_mode_retains_effective_date_factors(self):
        write_current(self.source)
        examples = Path(app.__file__).parent / "examples"
        self.convert(
            "--split-source",
            "actions",
            "--split-events",
            str(examples / "split_events.csv"),
            "--action-coverage",
            str(examples / "action_coverage.csv"),
            "--horizon",
            "2024-01-04",
        )
        self.assertEqual(pq.read_table(self.output)["split_adjustment_ratio"].to_pylist(), [1 / 3, 1 / 3, 1])

    def test_single_ticker_without_intervals(self):
        write_current(self.source, intervals=False)
        self.change_digest(lambda d: d["security"].update(tickers=["ABC"]))
        self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["ABC"] * 3)

    def test_unlisted_past_and_future_intervals_do_not_block_single_ticker(self):
        write_current(self.source)
        self.change_digest(
            lambda d: d["security"].update(
                tickers=["ABC"],
                ticker_ownership_intervals=[
                    {"ticker": "EXPIRED", "start_inclusive": None, "end_exclusive": "2024-01-02"},
                    {"ticker": "FUTURE", "start_inclusive": "2024-01-05", "end_exclusive": None},
                ],
            )
        )
        self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["ABC"] * 3)

    def test_exclusive_end_and_inclusive_start_select_the_dated_labels(self):
        write_current(self.source)
        self.change_digest(
            lambda d: d["security"]["ticker_ownership_intervals"].extend(
                [
                    {"ticker": "EXPIRED", "start_inclusive": None, "end_exclusive": "2024-01-02"},
                    {"ticker": "FUTURE", "start_inclusive": "2024-01-05", "end_exclusive": None},
                ]
            )
        )
        self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "OLD", "NEW"])

    def test_interval_between_actual_input_sessions_does_not_reject_the_history(self):
        views = example_views()
        for rows in views.values():
            for row, session in zip(rows, ("2024-01-02", "2024-01-04", "2024-01-05"), strict=True):
                row["session"] = session
        write_current(self.source, views)
        self.change_digest(
            lambda d: d["security"]["ticker_ownership_intervals"].append(
                {"ticker": "UNUSED", "start_inclusive": "2024-01-03", "end_exclusive": "2024-01-04"}
            )
        )
        self.convert()
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "NEW", "NEW"])

    def test_unlisted_intervals_covering_an_input_session_still_fail(self):
        for interval in (
            {"ticker": "OTHER", "start_inclusive": None, "end_exclusive": "2024-01-03"},
            {"ticker": "OTHER", "start_inclusive": "2024-01-04", "end_exclusive": None},
            {"ticker": "OTHER", "start_inclusive": None, "end_exclusive": None},
            None,
        ):
            with self.subTest(interval=interval):
                write_current(self.source)
                self.change_digest(
                    lambda d: d["security"].update(tickers=["ABC"], ticker_ownership_intervals=[interval])
                )
                with self.assertRaisesRegex(ValueError, "rejected"):
                    self.convert("--overwrite")
                report = json.loads(Path(str(self.output) + ".report.json").read_text())
                self.assertEqual(
                    report["omitted_histories"][0]["error"], "Invalid published ticker ownership interval"
                )
                self.assertFalse(self.output.exists())

    def test_output_filter_does_not_hide_conflicting_input_intervals(self):
        write_current(self.source)
        self.change_digest(
            lambda d: d["security"].update(
                tickers=["ABC"],
                ticker_ownership_intervals=[
                    {"ticker": "OTHER", "start_inclusive": None, "end_exclusive": "2024-01-03"},
                ],
            )
        )
        with self.assertRaisesRegex(ValueError, "rejected"):
            self.convert("--start-date", "2024-01-04")
        self.assertFalse(self.output.exists())

    def test_irrelevant_intervals_do_not_choose_between_multiple_tickers(self):
        write_current(self.source)
        self.change_digest(
            lambda d: d["security"].update(
                ticker_ownership_intervals=[
                    {"ticker": "EXPIRED", "start_inclusive": None, "end_exclusive": "2024-01-02"},
                ]
            )
        )
        with self.assertRaisesRegex(ValueError, "rejected"):
            self.convert()
        report = json.loads(Path(str(self.output) + ".report.json").read_text())
        self.assertEqual(
            report["omitted_histories"][0]["error"],
            "Dated ticker is ambiguous or unavailable; provide --metadata with symbol intervals",
        )
        self.assertFalse(self.output.exists())

    def test_ambiguous_labels_fail_and_dated_metadata_resolves(self):
        write_current(self.source, intervals=False)
        with self.assertRaisesRegex(ValueError, "rejected"):
            self.convert()
        self.assertFalse(self.output.exists())
        metadata = self.root / "metadata.csv"
        metadata.write_text(
            f"public_id,start_date,end_date,symbol\n{SOURCE_ID},2024-01-02,2024-01-03,OLD\n{SOURCE_ID},2024-01-04,2024-01-04,NEW\n"
        )
        self.convert("--metadata", str(metadata), "--overwrite")
        self.assertEqual(pq.read_table(self.output)["symbol"].to_pylist(), ["OLD", "OLD", "NEW"])

    def test_generation_and_schema_mismatches_fail(self):
        for key, value in [("catalogue_generation", "changed"), ("public_id", SECOND_ID), ("schema_version", 2)]:
            with self.subTest(key=key):
                write_current(self.source)
                self.change_digest(lambda d: d.update({key: value}))
                with self.assertRaisesRegex(ValueError, "rejected"):
                    self.convert("--overwrite")
                self.assertFalse(self.output.exists())

    def test_unknown_id_fails_before_reading_any_digest(self):
        write_current(self.source)
        with self.assertRaisesRegex(ValueError, "absent"):
            self.convert("--public-id", SECOND_ID)

    def test_missing_alias_is_rejected_not_inferred(self):
        write_current(self.source)
        self.change_digest(lambda d: d["column_aliases"].pop("all_close"))
        with self.assertRaisesRegex(ValueError, "rejected"):
            self.convert()

    def test_outside_payload_and_digest_are_rejected(self):
        for name in ("csv", "digest"):
            write_current(self.source)
            p = self.source / "final_manifest.json"
            d = json.loads(p.read_text())
            d["identities"][SOURCE_ID][name] = "../outside.csv"
            p.write_text(json.dumps(d))
            with self.assertRaisesRegex(ValueError, "rejected"):
                self.convert("--overwrite")
            self.assertFalse(self.output.exists())

    def test_date_filter_and_split_basis_still_use_whole_history(self):
        write_current(self.source)
        self.convert("--start-date", "2024-01-02", "--end-date", "2024-01-02", "--price-basis", "split")
        rows = pq.read_table(self.output).to_pylist()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["split_adjustment_ratio"], 1 / 3)
        self.assertEqual(rows[0]["adjusted_open"], 100 * (1 / 3))

    def test_cli_with_current_input(self):
        write_current(self.source)
        result = subprocess.run(
            [
                sys.executable,
                str(Path(app.__file__)),
                "--data-in",
                str(self.source),
                "--blood-out",
                str(self.output),
                "--public-id",
                SOURCE_ID,
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_empty_completed_selection_has_controlled_failure(self):
        write_current(self.source)
        p = self.source / "final_manifest.json"
        d = json.loads(p.read_text())
        d["identities"] = {}
        p.write_text(json.dumps(d))
        with self.assertRaisesRegex(ValueError, "No rows"):
            self.convert()


if __name__ == "__main__":
    unittest.main()

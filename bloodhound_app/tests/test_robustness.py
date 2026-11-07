"""Regressions for malformed inputs and an interrupted two-file publication."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import alpaca_to_bloodhound as converter
import pyarrow.parquet as pq
from test_converter import APP, SECOND_ID, SOURCE_ID, example_views, write_manifest


class RobustnessTests(unittest.TestCase):
    def test_every_output_destination_rejects_all_explicit_input_aliases(self):
        from itertools import product

        for field, suffix, alias in product(
            ("metadata", "split_events", "action_coverage"),
            ("", ".report.json", ".failure.json"),
            ("direct", "hardlink", "symlink"),
        ):
            with (
                self.subTest(field=field, destination=suffix, alias=alias),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                source = root / "input"
                write_manifest(source / "healthy", example_views(SOURCE_ID, "GOOD"))
                output = root / "result.parquet"
                output.write_bytes(b"prior output")
                destination = Path(str(output) + suffix)
                inputs = {
                    name: root / (name + ".csv") for name in ("metadata", "split_events", "action_coverage")
                }
                if alias == "direct":
                    inputs[field] = destination
                elif alias == "symlink" and not suffix:
                    inputs[field] = root / (field + ".parquet")
                for name, path in inputs.items():
                    path.write_text("preserve " + name)
                if alias != "direct":
                    destination.unlink(missing_ok=True)
                    if alias == "hardlink":
                        destination.hardlink_to(inputs[field])
                    else:
                        destination.symlink_to(inputs[field])
                before = {path: path.read_bytes() for path in (*inputs.values(), output)}
                args = converter.parser().parse_args(
                    [
                        "--data-in",
                        str(source),
                        "--blood-out",
                        str(output),
                        "--overwrite",
                        "--metadata",
                        str(inputs["metadata"]),
                        "--split-source",
                        "actions",
                        "--split-events",
                        str(inputs["split_events"]),
                        "--action-coverage",
                        str(inputs["action_coverage"]),
                        "--horizon",
                        "2026-11-03",
                    ]
                )
                with mock.patch.object(
                    converter,
                    "write_json_atomic",
                    side_effect=AssertionError("input collision reached publication"),
                ):
                    with self.assertRaisesRegex(converter.ConversionError, "must not replace an input"):
                        converter.convert(args)
                self.assertEqual({path: path.read_bytes() for path in before}, before)

    def test_json_publication_uses_no_forced_synchronization(self):
        target = self.root / "report.json"
        with mock.patch.object(converter.os, "fsync", side_effect=AssertionError("forbidden fsync")):
            converter.write_json_atomic(target, {"complete": True})
        self.assertEqual(json.loads(target.read_text()), {"complete": True})

    def test_failure_output_cannot_replace_metadata_even_through_a_hardlink(self):
        for hardlink in (False, True):
            with self.subTest(hardlink=hardlink), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                source = root / "input"
                write_manifest(source / "healthy", example_views(SOURCE_ID, "GOOD"))
                output = root / "result.parquet"
                output.write_bytes(b"prior output")
                failure = Path(str(output) + ".failure.json")
                metadata = root / "metadata.csv" if hardlink else failure
                metadata.write_text("public_id,symbol\n")
                if hardlink:
                    failure.hardlink_to(metadata)
                before = metadata.read_bytes()
                args = converter.parser().parse_args(
                    [
                        "--data-in",
                        str(source),
                        "--blood-out",
                        str(output),
                        "--metadata",
                        str(metadata),
                        "--overwrite",
                    ]
                )
                with self.assertRaisesRegex(converter.ConversionError, "must not replace an input"):
                    converter.convert(args)
                self.assertEqual(metadata.read_bytes(), before)
                self.assertEqual(output.read_bytes(), b"prior output")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_extra_csv_field_is_rejected_instead_of_becoming_a_discarded_index(self):
        payload = (
            b"session,public_id,symbol,open,high,low,close,volume\nignored,2024-01-02,ID,ABC,10,11,9,10,100\n"
        )
        with self.assertRaises(converter.ConversionError):
            converter.read_frame(payload, self.root / "malformed.csv", "ID")

    def test_truncated_gzip_is_reported_and_skipped_with_explicit_skip(self):
        data_in = self.root / "input"
        write_manifest(data_in / "healthy", example_views(SOURCE_ID, "GOOD"))
        write_manifest(data_in / "broken", example_views(SECOND_ID, "BAD"))
        broken = data_in / "broken" / "raw_payload.csv.gz"
        broken.write_bytes(broken.read_bytes()[:-6])
        output = self.root / "result.parquet"
        completed = subprocess.run(
            [
                sys.executable,
                str(APP),
                "--data-in",
                str(data_in),
                "--blood-out",
                str(output),
                "--on-error",
                "skip",
                "--quiet",
            ],
            text=True,
            capture_output=True,
            timeout=90,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + "\n" + completed.stderr)
        self.assertNotIn("Traceback", completed.stderr)
        rows = pq.read_table(output).to_pylist()
        self.assertEqual(len(rows), 3)
        self.assertEqual({row["symbol"] for row in rows}, {"GOOD"})
        report = json.loads(Path(str(output) + ".report.json").read_text())
        self.assertEqual(report["status"], "partial")
        self.assertEqual(report["histories_rejected"], 1)
        self.assertEqual(report["omitted_histories"][0]["public_id"], SECOND_ID)

    def test_invalid_manifest_json_shapes_raise_a_controlled_input_error(self):
        malformed = (
            [],
            None,
            {"public_id": SOURCE_ID, "security": None, "adjustments": []},
            {"public_id": SOURCE_ID, "adjustments": {}},
            {"public_id": SOURCE_ID, "adjustments": [None]},
        )
        for index, manifest in enumerate(malformed):
            with self.subTest(manifest=manifest):
                folder = self.root / f"case_{index}"
                folder.mkdir()
                (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises(converter.ConversionError):
                    converter.discover_histories(folder)

    def test_duplicate_metadata_headers_are_rejected(self):
        metadata = self.root / "metadata.csv"
        metadata.write_text(
            "public_id,start_date,end_date,symbol,symbol\n"
            f"{SOURCE_ID},2024-01-02,2024-01-04,VALID,OVERWRITTEN\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(converter.ConversionError, "duplicate"):
            converter.DatedMetadata(metadata)

    def test_failed_report_commit_restores_the_previous_pair_without_orphans(self):
        output = self.root / "result.parquet"
        report_path = self.root / "result.parquet.report.json"
        staged_parquet = self.root / "new_output.partial"
        old_output = b"original parquet contents\n"
        old_report = b'{"status": "original report"}\n'
        output.write_bytes(old_output)
        report_path.write_bytes(old_report)
        staged_parquet.write_bytes(b"new parquet contents\n")
        replace = converter.os.replace
        failures = []

        def fail_final_report_only(source, destination):
            source, destination = Path(source), Path(destination)
            if destination == report_path and source.suffix == ".staged":
                failures.append((source, destination))
                raise OSError("simulated report commit failure")
            return replace(source, destination)

        with mock.patch.object(converter.os, "replace", side_effect=fail_final_report_only):
            with self.assertRaisesRegex(OSError, "simulated report commit failure"):
                converter.publish_pair(staged_parquet, output, report_path, {"status": "new report"})

        self.assertEqual(len(failures), 1)
        self.assertEqual(output.read_bytes(), old_output)
        self.assertEqual(report_path.read_bytes(), old_report)
        self.assertFalse(staged_parquet.exists())
        self.assertEqual(set(self.root.iterdir()), {output, report_path})


if __name__ == "__main__":
    unittest.main()

import copy
import csv
import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import eod_pipeline
from eod_pipeline import (
    PIPELINE_VERSION,
    DownloadIntegrityError,
    PipelineError,
    PipelineIntegrityError,
    inspect_cached_artifact,
    manifest_path,
    validate_cached_download,
    validate_pipeline_change_log,
    write_bars_csv_gz,
)


FIXTURES = Path(__file__).with_name("fixtures")


def load_bars(name="valid_bars.json"):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def identity(symbol="TEST", adjustment="raw"):
    return {
        "symbol": symbol,
        "adjustment": adjustment,
        "feed": "sip",
        "timeframe": "1Day",
        "request_from": "2024-01-01T00:00:00Z",
        "request_to": "2024-01-17T00:00:00Z",
        "asof": "2024-01-17",
    }


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def artifact_paths(directory, filename="raw.csv.gz"):
    data = Path(directory) / "TEST" / filename
    return (
        data,
        manifest_path(data),
        data.parent / "pipeline.manifest.json",
        data.parent / "integrity_failure.manifest.json",
    )


def create_valid_artifact(directory):
    data, sidecar, pipeline, failures = artifact_paths(directory)
    manifest = write_bars_csv_gz(data, load_bars(), identity())
    return data, sidecar, pipeline, failures, manifest


class PublicationTests(unittest.TestCase):
    def test_version_changelog_matches_pipeline(self):
        self.assertEqual(PIPELINE_VERSION, 11)
        self.assertEqual(len(validate_pipeline_change_log()), PIPELINE_VERSION)

    def test_pipeline_finishes_before_any_gzip_path_exists(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, pipeline, failures = artifact_paths(temporary)
            original = eod_pipeline._run_pipeline
            observed = []

            def guarded_run(bars, supplied_identity, write_row=None):
                self.assertFalse(data.parent.exists())
                self.assertFalse(data.exists())
                self.assertEqual(list(Path(temporary).rglob("*.tmp.*")), [])
                self.assertIsNone(write_row)
                observed.append(True)
                return original(bars, supplied_identity, write_row)

            with mock.patch.object(eod_pipeline, "_run_pipeline", guarded_run):
                manifest = write_bars_csv_gz(data, load_bars(), identity())

            self.assertEqual(observed, [True])
            self.assertTrue(data.exists())
            self.assertEqual(manifest["sha256"], sha256(data))
            self.assertTrue(sidecar.exists())
            self.assertTrue(pipeline.exists())
            self.assertTrue(failures.exists())

    def test_clean_publication_has_versionless_receipt_and_empty_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, pipeline_path, failure_path, manifest = (
                create_valid_artifact(temporary)
            )
            pipeline = read_json(pipeline_path)
            failures = read_json(failure_path)

            self.assertEqual(pipeline["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(pipeline["receipt_count"], 1)
            self.assertNotIn("pipeline_version", pipeline["receipts"][0])
            self.assertEqual(
                pipeline["receipts"][0]["download_sha256"], manifest["sha256"]
            )
            self.assertEqual(failures["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(failures["failure_count"], 0)
            self.assertEqual(failures["failures"], [])
            serialized = json.dumps({"pipeline": pipeline, "failures": failures})
            for forbidden in ('"status"', '"passed"', '"overall_status"'):
                self.assertNotIn(forbidden, serialized)
            self.assertEqual(inspect_cached_artifact(data, identity()).action, "cached")

    def test_failed_rows_are_preserved_with_exact_failure_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, pipeline_path, failure_path = artifact_paths(temporary)
            bars = load_bars("bad_price_bars.json")
            with self.assertRaises(PipelineIntegrityError):
                write_bars_csv_gz(data, bars, identity())

            with gzip.open(data, "rt", encoding="utf-8", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(len(rows), 10)
            self.assertEqual(rows[5]["high"], "90")

            failures = read_json(failure_path)
            self.assertEqual(failures["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(failures["failure_count"], 1)
            failure = failures["failures"][0]
            self.assertEqual(failure["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(failure["row_failure_count"], 1)
            self.assertEqual(failure["row_failures"][0]["data"], bars[5])
            self.assertIn(
                "high is below low",
                failure["row_failures"][0]["validation_error"],
            )
            self.assertEqual(failure["dataset_failure_count"], 0)
            self.assertEqual(failure["dataset_failures"], [])
            self.assertEqual(read_json(sidecar)["sha256"], sha256(data))
            self.assertEqual(
                read_json(pipeline_path)["receipts"][0]["download_sha256"],
                sha256(data),
            )
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action,
                "integrity_failed",
            )

    def test_existing_pair_requires_authorized_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, _, _ = create_valid_artifact(temporary)
            with self.assertRaisesRegex(PipelineError, "refusing to replace"):
                write_bars_csv_gz(data, load_bars(), identity())

    def test_authorized_clean_replacement_clears_failure_entry(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, failure_path = artifact_paths(temporary)
            with self.assertRaises(PipelineIntegrityError):
                write_bars_csv_gz(
                    data, load_bars("bad_price_bars.json"), identity()
                )
            write_bars_csv_gz(
                data, load_bars(), identity(), allow_replace=True
            )
            failures = read_json(failure_path)
            self.assertEqual(failures["failure_count"], 0)
            self.assertEqual(failures["failures"], [])

    def test_local_reprocessing_never_rewrites_download_pair(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, pipeline, failures, manifest = create_valid_artifact(
                temporary
            )
            before = (sha256(data), sha256(sidecar))
            pipeline.unlink()
            failures.unlink()
            inspection = inspect_cached_artifact(data, identity())
            self.assertEqual(inspection.action, "repipeline")
            receipt = validate_cached_download(data, identity(), manifest)
            self.assertEqual(receipt["download_sha256"], manifest["sha256"])
            self.assertNotIn("pipeline_version", receipt)
            self.assertEqual((sha256(data), sha256(sidecar)), before)

    def test_sha_mismatch_is_fatal_and_does_not_touch_derived_manifests(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline, failures, manifest = create_valid_artifact(temporary)
            before = (sha256(pipeline), sha256(failures))
            damaged = bytearray(data.read_bytes())
            damaged[20] ^= 1
            data.write_bytes(damaged)
            with self.assertRaisesRegex(
                DownloadIntegrityError, "SHA-256 mismatch"
            ):
                validate_cached_download(data, identity(), manifest)
            self.assertEqual((sha256(pipeline), sha256(failures)), before)

    def test_stale_manifest_generation_is_rebuilt_without_mixing(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline_path, failure_path, manifest = create_valid_artifact(
                temporary
            )
            pipeline = read_json(pipeline_path)
            pipeline["pipeline_version"] = PIPELINE_VERSION - 1
            pipeline["receipts"].append(
                {"data_file": "obsolete.csv.gz", "download_sha256": "0" * 64}
            )
            pipeline["receipt_count"] = 2
            write_json(pipeline_path, pipeline)
            failures = read_json(failure_path)
            failures["pipeline_version"] = PIPELINE_VERSION - 1
            failures["failures"] = [
                {
                    "data_file": "obsolete.csv.gz",
                    "pipeline_version": PIPELINE_VERSION - 1,
                }
            ]
            failures["failure_count"] = 1
            write_json(failure_path, failures)

            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )
            validate_cached_download(data, identity(), manifest)
            rebuilt_pipeline = read_json(pipeline_path)
            rebuilt_failures = read_json(failure_path)
            self.assertEqual(rebuilt_pipeline["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(
                [item["data_file"] for item in rebuilt_pipeline["receipts"]],
                ["raw.csv.gz"],
            )
            self.assertEqual(rebuilt_failures["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(rebuilt_failures["failures"], [])


class CacheDecisionTests(unittest.TestCase):
    def test_both_files_absent_downloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, _ = artifact_paths(temporary)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "download"
            )

    def test_data_without_sidecar_requires_repair(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, _ = artifact_paths(temporary)
            data.parent.mkdir(parents=True)
            data.write_bytes(b"data")
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action,
                "repair_required",
            )

    def test_sidecar_without_data_requires_repair(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, _, _ = artifact_paths(temporary)
            sidecar.parent.mkdir(parents=True)
            sidecar.write_text("{}\n", encoding="utf-8")
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action,
                "repair_required",
            )

    def test_malformed_sidecar_is_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, _, _, _ = create_valid_artifact(temporary)
            sidecar.write_text("{", encoding="utf-8")
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action,
                "download_integrity_failed",
            )

    def test_sidecar_without_sha_is_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, sidecar, _, _, _ = create_valid_artifact(temporary)
            manifest = read_json(sidecar)
            del manifest["sha256"]
            write_json(sidecar, manifest)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action,
                "download_integrity_failed",
            )

    def test_changed_download_stat_is_fatal(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, _, _ = create_valid_artifact(temporary)
            data.write_bytes(data.read_bytes() + b"changed")
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action,
                "download_integrity_failed",
            )

    def test_request_identity_change_requires_force(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, _, _ = create_valid_artifact(temporary)
            changed = identity()
            changed["asof"] = "2024-01-07"
            self.assertEqual(
                inspect_cached_artifact(data, changed).action,
                "refresh_required",
            )

    def test_missing_pipeline_manifest_reprocesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline, _, _ = create_valid_artifact(temporary)
            pipeline.unlink()
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )

    def test_stale_pipeline_manifest_version_reprocesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline_path, _, _ = create_valid_artifact(temporary)
            pipeline = read_json(pipeline_path)
            pipeline["pipeline_version"] = PIPELINE_VERSION - 1
            write_json(pipeline_path, pipeline)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )

    def test_missing_receipt_reprocesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline_path, _, _ = create_valid_artifact(temporary)
            pipeline = read_json(pipeline_path)
            pipeline["receipts"] = []
            pipeline["receipt_count"] = 0
            write_json(pipeline_path, pipeline)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )

    def test_receipt_for_different_sha_reprocesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline_path, _, _ = create_valid_artifact(temporary)
            pipeline = read_json(pipeline_path)
            pipeline["receipts"][0]["download_sha256"] = "0" * 64
            write_json(pipeline_path, pipeline)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )

    def test_missing_failure_manifest_reprocesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, failures, _ = create_valid_artifact(temporary)
            failures.unlink()
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )

    def test_stale_failure_manifest_version_reprocesses(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, failure_path, _ = create_valid_artifact(temporary)
            failures = read_json(failure_path)
            failures["pipeline_version"] = PIPELINE_VERSION - 1
            write_json(failure_path, failures)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "repipeline"
            )

    def test_stale_failure_entry_is_an_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, _, failure_path, _ = create_valid_artifact(temporary)
            failures = read_json(failure_path)
            failures["failures"] = [
                {
                    "data_file": "raw.csv.gz",
                    "download_sha256": "0" * 64,
                    "pipeline_version": PIPELINE_VERSION,
                }
            ]
            failures["failure_count"] = 1
            write_json(failure_path, failures)
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "error"
            )

    def test_malformed_derived_manifest_is_an_error(self):
        with tempfile.TemporaryDirectory() as temporary:
            data, _, pipeline, _, _ = create_valid_artifact(temporary)
            pipeline.write_text("{", encoding="utf-8")
            self.assertEqual(
                inspect_cached_artifact(data, identity()).action, "error"
            )


class ValidationTests(unittest.TestCase):
    def test_complete_fixture_has_no_failures(self):
        result = eod_pipeline._run_pipeline(load_bars(), identity())
        self.assertFalse(result.has_failures)

    def test_missing_session_is_a_dataset_failure(self):
        bars = load_bars("missing_session_bars.json")
        result = eod_pipeline._run_pipeline(bars, identity())
        self.assertEqual(result.row_failures, ())
        self.assertEqual(
            result.dataset_failures[0]["missing_sessions"], ["2024-01-10"]
        )

    def test_activity_fixture_has_one_exact_failure(self):
        bars = load_bars("bad_activity_bars.json")
        result = eod_pipeline._run_pipeline(bars, identity())
        self.assertEqual(len(result.row_failures), 1)
        self.assertEqual(result.row_failures[0]["data"], bars[6])
        self.assertIn(
            "trade_count exceeds volume",
            result.row_failures[0]["validation_error"],
        )
        self.assertEqual(result.dataset_failures, ())

    def test_individual_row_rules(self):
        cases = (
            (lambda row: row.update(h=90), "high is below low"),
            (lambda row: row.update(o=110), "open is outside"),
            (lambda row: row.update(c=110), "close is outside"),
            (lambda row: row.update(vw=110), "vwap is outside"),
            (lambda row: row.update(o=-1), "prices must be positive"),
            (lambda row: row.update(h=float("nan")), "high is not finite"),
            (lambda row: row.update(v=0), "volume is not a positive integer"),
            (lambda row: row.update(v=1000.5), "volume is not a positive integer"),
            (lambda row: row.update(n=0), "trade_count is not a positive integer"),
            (lambda row: row.update(n=1000.5), "trade_count is not a positive integer"),
            (lambda row: row.update(n=2000), "trade_count exceeds volume"),
            (lambda row: row.update(t="2024-01-03T06:00:00Z"), "not New York session midnight"),
            (lambda row: row.update(t="2024-01-02T05:00:00Z"), "not strictly increasing"),
            (lambda row: row.pop("h"), "missing fields: h"),
            (lambda row: row.update(o=True), "open is not numeric"),
        )
        for mutate, expected in cases:
            with self.subTest(expected=expected):
                bars = copy.deepcopy(load_bars())
                mutate(bars[1])
                result = eod_pipeline._run_pipeline(bars, identity())
                self.assertTrue(result.row_failures)
                self.assertIn(
                    expected, result.row_failures[0]["validation_error"]
                )


if __name__ == "__main__":
    unittest.main()

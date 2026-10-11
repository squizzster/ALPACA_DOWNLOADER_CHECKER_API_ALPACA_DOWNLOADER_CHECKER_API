import copy
import csv
import gzip
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from libs.alpaca_end_of_day_pipeline import (
    PIPELINE_VERSION,
    AlpacaDownloadIntegrityError,
    EndOfDayPipelineError,
    PricingDataIntegrityError,
    authenticate_and_revalidate_cached_download,
    download_manifest_path,
    download_publication,
    inspect_cached_download,
    validate_and_publish_alpaca_download,
    validate_end_of_day_bars,
    validate_pipeline_change_log,
)

TEST_FIXTURES_DIRECTORY = Path(__file__).with_name("fixtures")
DOWNLOAD_MANIFEST_KEYS = frozenset(
    {
        "manifest_version",
        "boundary",
        "pipeline_version",
        "verified",
        "file",
        "sha256",
        "bytes",
        "mtime_ns",
        "rows",
        "sessions",
        "session_sha256",
        "calendar_sha256",
        "from",
        "to",
        "symbol",
        "adjustment",
        "feed",
        "timeframe",
        "request_from",
        "request_to",
        "asof",
        "created_at",
    }
)
PIPELINE_RECEIPT_MANIFEST_KEYS = frozenset(
    {
        "manifest_type",
        "manifest_version",
        "symbol",
        "pipeline_version",
        "updated_at",
        "receipt_count",
        "receipts",
    }
)
PIPELINE_RECEIPT_KEYS = frozenset(
    {
        "data_file",
        "download_manifest",
        "adjustment",
        "download_sha256",
        "processed_at",
        "rows_seen",
        "sessions_seen",
        "from",
        "to",
    }
)
INTEGRITY_FAILURE_MANIFEST_KEYS = frozenset(
    {
        "manifest_type",
        "manifest_version",
        "symbol",
        "pipeline_version",
        "updated_at",
        "failure_count",
        "failures",
    }
)
INTEGRITY_FAILURE_KEYS = PIPELINE_RECEIPT_KEYS | {
    "pipeline_version",
    "row_failure_count",
    "dataset_failure_count",
    "row_failures",
    "dataset_failures",
}
ROW_FAILURE_KEYS = frozenset({"row_number", "csv_line", "data", "validation_error"})


def load_fixture_bars(fixture_filename: str = "ten_valid_daily_bars.json") -> list[dict]:
    return json.loads((TEST_FIXTURES_DIRECTORY / fixture_filename).read_text(encoding="utf-8"))


def test_download_request_identity(symbol: str = "TEST", adjustment: str = "raw") -> dict:
    return {
        "symbol": symbol,
        "adjustment": adjustment,
        "feed": "sip",
        "timeframe": "1Day",
        "request_from": "2024-01-01T00:00:00Z",
        "request_to": "2024-01-17T00:00:00Z",
        "asof": "2024-01-17",
    }


def file_sha256(file_path: Path) -> str:
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def read_json_document(json_path: Path) -> dict:
    return json.loads(json_path.read_text(encoding="utf-8"))


def write_json_document(json_path: Path, document: dict) -> None:
    json_path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class TestArtifactPaths:
    download_path: Path
    download_manifest_path: Path
    pipeline_receipt_manifest_path: Path
    integrity_failure_manifest_path: Path


@dataclass(frozen=True)
class PublishedTestDownload:
    paths: TestArtifactPaths
    download_manifest: dict


def test_artifact_paths(test_directory: str | Path, download_filename: str = "raw.csv.gz") -> TestArtifactPaths:
    download_path = Path(test_directory) / "TEST" / download_filename
    return TestArtifactPaths(
        download_path=download_path,
        download_manifest_path=download_manifest_path(download_path),
        pipeline_receipt_manifest_path=download_path.parent / "pipeline.manifest.json",
        integrity_failure_manifest_path=download_path.parent / "integrity_failure.manifest.json",
    )


def publish_valid_test_download(test_directory: str | Path) -> PublishedTestDownload:
    paths = test_artifact_paths(test_directory)
    published_download_manifest = validate_and_publish_alpaca_download(
        paths.download_path,
        load_fixture_bars(),
        test_download_request_identity(),
    )
    return PublishedTestDownload(paths=paths, download_manifest=published_download_manifest)


class PublicationTests(unittest.TestCase):
    def test_version_changelog_matches_pipeline(self):
        self.assertEqual(PIPELINE_VERSION, 19)
        self.assertEqual(len(validate_pipeline_change_log()), PIPELINE_VERSION)

    def test_pipeline_finishes_before_any_gzip_path_exists(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            original_validation_function = download_publication.validate_end_of_day_bars
            validation_calls_observed = []

            def assert_no_disk_artifacts_then_validate(alpaca_daily_bars, supplied_request_identity):
                self.assertFalse(artifact_paths.download_path.parent.exists())
                self.assertFalse(artifact_paths.download_path.exists())
                self.assertEqual(list(Path(test_directory).rglob("*.tmp.*")), [])
                validation_calls_observed.append(True)
                return original_validation_function(alpaca_daily_bars, supplied_request_identity)

            with mock.patch.object(
                download_publication,
                "validate_end_of_day_bars",
                assert_no_disk_artifacts_then_validate,
            ):
                download_manifest = validate_and_publish_alpaca_download(
                    artifact_paths.download_path,
                    load_fixture_bars(),
                    test_download_request_identity(),
                )

            self.assertEqual(validation_calls_observed, [True])
            self.assertTrue(artifact_paths.download_path.exists())
            self.assertEqual(
                download_manifest["sha256"],
                file_sha256(artifact_paths.download_path),
            )
            self.assertTrue(artifact_paths.download_manifest_path.exists())
            self.assertTrue(artifact_paths.pipeline_receipt_manifest_path.exists())
            self.assertTrue(artifact_paths.integrity_failure_manifest_path.exists())

    def test_clean_publication_has_versionless_receipt_and_empty_failures(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            receipt_manifest = read_json_document(published_download.paths.pipeline_receipt_manifest_path)
            failure_manifest = read_json_document(published_download.paths.integrity_failure_manifest_path)

            self.assertEqual(receipt_manifest["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(receipt_manifest["receipt_count"], 1)
            self.assertNotIn("pipeline_version", receipt_manifest["receipts"][0])
            self.assertEqual(
                receipt_manifest["receipts"][0]["download_sha256"],
                published_download.download_manifest["sha256"],
            )
            self.assertEqual(failure_manifest["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(failure_manifest["failure_count"], 0)
            self.assertEqual(failure_manifest["failures"], [])
            serialized_manifests = json.dumps({"pipeline": receipt_manifest, "failures": failure_manifest})
            for forbidden_field in ('"status"', '"passed"', '"overall_status"'):
                self.assertNotIn(forbidden_field, serialized_manifests)
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "cached")

    def test_external_manifest_keys_remain_stable(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            downloaded_bars = load_fixture_bars("ten_daily_bars_one_high_below_low.json")
            with self.assertRaises(PricingDataIntegrityError):
                validate_and_publish_alpaca_download(
                    artifact_paths.download_path,
                    downloaded_bars,
                    test_download_request_identity(),
                )

            download_manifest = read_json_document(artifact_paths.download_manifest_path)
            receipt_manifest = read_json_document(artifact_paths.pipeline_receipt_manifest_path)
            failure_manifest = read_json_document(artifact_paths.integrity_failure_manifest_path)

            self.assertEqual(frozenset(download_manifest), DOWNLOAD_MANIFEST_KEYS)
            self.assertEqual(frozenset(receipt_manifest), PIPELINE_RECEIPT_MANIFEST_KEYS)
            self.assertEqual(frozenset(receipt_manifest["receipts"][0]), PIPELINE_RECEIPT_KEYS)
            self.assertEqual(frozenset(failure_manifest), INTEGRITY_FAILURE_MANIFEST_KEYS)
            self.assertEqual(frozenset(failure_manifest["failures"][0]), INTEGRITY_FAILURE_KEYS)
            self.assertEqual(
                frozenset(failure_manifest["failures"][0]["row_failures"][0]),
                ROW_FAILURE_KEYS,
            )

    def test_failed_rows_are_preserved_with_exact_failure_evidence(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            downloaded_bars = load_fixture_bars("ten_daily_bars_one_high_below_low.json")
            with self.assertRaises(PricingDataIntegrityError):
                validate_and_publish_alpaca_download(
                    artifact_paths.download_path,
                    downloaded_bars,
                    test_download_request_identity(),
                )

            with gzip.open(
                artifact_paths.download_path,
                "rt",
                encoding="utf-8",
                newline="",
            ) as compressed_csv_file:
                preserved_csv_rows = list(csv.DictReader(compressed_csv_file))
            self.assertEqual(len(preserved_csv_rows), 10)
            self.assertEqual(preserved_csv_rows[5]["high"], "90")

            failure_manifest = read_json_document(artifact_paths.integrity_failure_manifest_path)
            self.assertEqual(failure_manifest["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(failure_manifest["failure_count"], 1)
            artifact_failure = failure_manifest["failures"][0]
            self.assertEqual(artifact_failure["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(artifact_failure["row_failure_count"], 1)
            self.assertEqual(artifact_failure["row_failures"][0]["data"], downloaded_bars[5])
            self.assertIn(
                "high is below low",
                artifact_failure["row_failures"][0]["validation_error"],
            )
            self.assertEqual(artifact_failure["dataset_failure_count"], 0)
            self.assertEqual(artifact_failure["dataset_failures"], [])
            self.assertEqual(
                read_json_document(artifact_paths.download_manifest_path)["sha256"],
                file_sha256(artifact_paths.download_path),
            )
            self.assertEqual(
                read_json_document(artifact_paths.pipeline_receipt_manifest_path)["receipts"][0][
                    "download_sha256"
                ],
                file_sha256(artifact_paths.download_path),
            )
            cache_inspection = inspect_cached_download(
                artifact_paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(
                cache_inspection.required_action,
                "recorded_pricing_data_failure",
            )

    def test_existing_pair_requires_authorized_replacement(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            with self.assertRaisesRegex(EndOfDayPipelineError, "refusing to replace"):
                validate_and_publish_alpaca_download(
                    published_download.paths.download_path,
                    load_fixture_bars(),
                    test_download_request_identity(),
                )

    def test_authorized_clean_replacement_clears_failure_entry(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            with self.assertRaises(PricingDataIntegrityError):
                validate_and_publish_alpaca_download(
                    artifact_paths.download_path,
                    load_fixture_bars("ten_daily_bars_one_high_below_low.json"),
                    test_download_request_identity(),
                )
            validate_and_publish_alpaca_download(
                artifact_paths.download_path,
                load_fixture_bars(),
                test_download_request_identity(),
                replacement_authorized=True,
            )
            failure_manifest = read_json_document(artifact_paths.integrity_failure_manifest_path)
            self.assertEqual(failure_manifest["failure_count"], 0)
            self.assertEqual(failure_manifest["failures"], [])

    def test_local_reprocessing_never_rewrites_download_pair(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            frozen_boundary_sha256_before = (
                file_sha256(published_download.paths.download_path),
                file_sha256(published_download.paths.download_manifest_path),
            )
            published_download.paths.pipeline_receipt_manifest_path.unlink()
            published_download.paths.integrity_failure_manifest_path.unlink()
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")
            pipeline_receipt = authenticate_and_revalidate_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
                published_download.download_manifest,
            )
            self.assertEqual(
                pipeline_receipt["download_sha256"],
                published_download.download_manifest["sha256"],
            )
            self.assertNotIn("pipeline_version", pipeline_receipt)
            self.assertEqual(
                (
                    file_sha256(published_download.paths.download_path),
                    file_sha256(published_download.paths.download_manifest_path),
                ),
                frozen_boundary_sha256_before,
            )

    def test_sha_mismatch_is_fatal_and_does_not_touch_derived_manifests(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            derived_manifest_sha256_before = (
                file_sha256(published_download.paths.pipeline_receipt_manifest_path),
                file_sha256(published_download.paths.integrity_failure_manifest_path),
            )
            damaged_download_bytes = bytearray(published_download.paths.download_path.read_bytes())
            damaged_download_bytes[20] ^= 1
            published_download.paths.download_path.write_bytes(damaged_download_bytes)
            with self.assertRaisesRegex(
                AlpacaDownloadIntegrityError,
                "SHA-256 mismatch",
            ):
                authenticate_and_revalidate_cached_download(
                    published_download.paths.download_path,
                    test_download_request_identity(),
                    published_download.download_manifest,
                )
            self.assertEqual(
                (
                    file_sha256(published_download.paths.pipeline_receipt_manifest_path),
                    file_sha256(published_download.paths.integrity_failure_manifest_path),
                ),
                derived_manifest_sha256_before,
            )

    def test_stale_manifest_generation_is_rebuilt_without_mixing(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            stale_receipt_manifest = read_json_document(published_download.paths.pipeline_receipt_manifest_path)
            stale_receipt_manifest["pipeline_version"] = PIPELINE_VERSION - 1
            stale_receipt_manifest["receipts"].append(
                {"data_file": "obsolete.csv.gz", "download_sha256": "0" * 64}
            )
            stale_receipt_manifest["receipt_count"] = 2
            write_json_document(
                published_download.paths.pipeline_receipt_manifest_path,
                stale_receipt_manifest,
            )
            stale_failure_manifest = read_json_document(published_download.paths.integrity_failure_manifest_path)
            stale_failure_manifest["pipeline_version"] = PIPELINE_VERSION - 1
            stale_failure_manifest["failures"] = [
                {
                    "data_file": "obsolete.csv.gz",
                    "pipeline_version": PIPELINE_VERSION - 1,
                }
            ]
            stale_failure_manifest["failure_count"] = 1
            write_json_document(
                published_download.paths.integrity_failure_manifest_path,
                stale_failure_manifest,
            )

            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")
            authenticate_and_revalidate_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
                published_download.download_manifest,
            )
            rebuilt_receipt_manifest = read_json_document(
                published_download.paths.pipeline_receipt_manifest_path
            )
            rebuilt_failure_manifest = read_json_document(
                published_download.paths.integrity_failure_manifest_path
            )
            self.assertEqual(
                rebuilt_receipt_manifest["pipeline_version"],
                PIPELINE_VERSION,
            )
            self.assertEqual(
                [receipt["data_file"] for receipt in rebuilt_receipt_manifest["receipts"]],
                ["raw.csv.gz"],
            )
            self.assertEqual(rebuilt_failure_manifest["pipeline_version"], PIPELINE_VERSION)
            self.assertEqual(rebuilt_failure_manifest["failures"], [])


class CliBoundaryTests(unittest.TestCase):
    def test_force_cannot_replace_a_sha_mismatched_download(self):
        with tempfile.TemporaryDirectory() as test_directory:
            end_of_day_data_directory = Path(test_directory) / "data" / "end_of_day"
            published_download = publish_valid_test_download(end_of_day_data_directory)
            damaged_download_bytes = bytearray(published_download.paths.download_path.read_bytes())
            damaged_download_bytes[20] ^= 1
            published_download.paths.download_path.write_bytes(damaged_download_bytes)

            completed_process = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parents[1] / "download_alpaca_end_of_day_bars.py"),
                    "--symbol",
                    "TEST",
                    "--adjustment",
                    "raw",
                    "--force",
                ],
                cwd=test_directory,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(completed_process.returncode, 2)
            self.assertIn("SHA-256 mismatch", completed_process.stderr)
            self.assertNotIn("set ALPACA_API", completed_process.stderr)


class CacheDecisionTests(unittest.TestCase):
    def test_both_files_absent_downloads(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            cache_inspection = inspect_cached_download(
                artifact_paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "download")

    def test_data_without_sidecar_requires_repair(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            artifact_paths.download_path.parent.mkdir(parents=True)
            artifact_paths.download_path.write_bytes(b"data")
            cache_inspection = inspect_cached_download(
                artifact_paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repair_required")

    def test_sidecar_without_data_requires_repair(self):
        with tempfile.TemporaryDirectory() as test_directory:
            artifact_paths = test_artifact_paths(test_directory)
            artifact_paths.download_manifest_path.parent.mkdir(parents=True)
            artifact_paths.download_manifest_path.write_text("{}\n", encoding="utf-8")
            cache_inspection = inspect_cached_download(
                artifact_paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repair_required")

    def test_malformed_sidecar_is_fatal(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            published_download.paths.download_manifest_path.write_text("{", encoding="utf-8")
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "invalid_frozen_download_boundary")

    def test_sidecar_without_sha_is_fatal(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            download_manifest = read_json_document(published_download.paths.download_manifest_path)
            del download_manifest["sha256"]
            write_json_document(published_download.paths.download_manifest_path, download_manifest)
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "invalid_frozen_download_boundary")

    def test_changed_download_stat_is_fatal(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            published_download.paths.download_path.write_bytes(
                published_download.paths.download_path.read_bytes() + b"changed"
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "invalid_frozen_download_boundary")

    def test_request_identity_change_requires_force(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            changed_request_identity = test_download_request_identity()
            changed_request_identity["asof"] = "2024-01-07"
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                changed_request_identity,
            )
            self.assertEqual(cache_inspection.required_action, "refresh_required")

    def test_missing_pipeline_manifest_reprocesses(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            published_download.paths.pipeline_receipt_manifest_path.unlink()
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")

    def test_stale_pipeline_manifest_version_reprocesses(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            receipt_manifest = read_json_document(published_download.paths.pipeline_receipt_manifest_path)
            receipt_manifest["pipeline_version"] = PIPELINE_VERSION - 1
            write_json_document(
                published_download.paths.pipeline_receipt_manifest_path,
                receipt_manifest,
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")

    def test_missing_receipt_reprocesses(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            receipt_manifest = read_json_document(published_download.paths.pipeline_receipt_manifest_path)
            receipt_manifest["receipts"] = []
            receipt_manifest["receipt_count"] = 0
            write_json_document(
                published_download.paths.pipeline_receipt_manifest_path,
                receipt_manifest,
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")

    def test_receipt_for_different_sha_reprocesses(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            receipt_manifest = read_json_document(published_download.paths.pipeline_receipt_manifest_path)
            receipt_manifest["receipts"][0]["download_sha256"] = "0" * 64
            write_json_document(
                published_download.paths.pipeline_receipt_manifest_path,
                receipt_manifest,
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")

    def test_missing_failure_manifest_reprocesses(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            published_download.paths.integrity_failure_manifest_path.unlink()
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")

    def test_stale_failure_manifest_version_reprocesses(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            failure_manifest = read_json_document(published_download.paths.integrity_failure_manifest_path)
            failure_manifest["pipeline_version"] = PIPELINE_VERSION - 1
            write_json_document(
                published_download.paths.integrity_failure_manifest_path,
                failure_manifest,
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "repipeline")

    def test_stale_failure_entry_is_an_error(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            failure_manifest = read_json_document(published_download.paths.integrity_failure_manifest_path)
            failure_manifest["failures"] = [
                {
                    "data_file": "raw.csv.gz",
                    "download_sha256": "0" * 64,
                    "pipeline_version": PIPELINE_VERSION,
                }
            ]
            failure_manifest["failure_count"] = 1
            write_json_document(
                published_download.paths.integrity_failure_manifest_path,
                failure_manifest,
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "error")

    def test_malformed_derived_manifest_is_an_error(self):
        with tempfile.TemporaryDirectory() as test_directory:
            published_download = publish_valid_test_download(test_directory)
            published_download.paths.pipeline_receipt_manifest_path.write_text(
                "{",
                encoding="utf-8",
            )
            cache_inspection = inspect_cached_download(
                published_download.paths.download_path,
                test_download_request_identity(),
            )
            self.assertEqual(cache_inspection.required_action, "error")


class ValidationTests(unittest.TestCase):
    def test_complete_fixture_has_no_failures(self):
        validation_result = validate_end_of_day_bars(
            load_fixture_bars(),
            test_download_request_identity(),
        )
        self.assertFalse(validation_result.has_failures)

    def test_missing_session_is_not_an_identity_blind_integrity_failure(self):
        daily_bars_with_missing_session = load_fixture_bars("nine_daily_bars_one_market_session_missing.json")
        validation_result = validate_end_of_day_bars(
            daily_bars_with_missing_session,
            test_download_request_identity(),
        )
        self.assertEqual(validation_result.row_failures, ())
        self.assertEqual(validation_result.dataset_failures, ())

    def test_unexpected_closed_session_remains_a_dataset_failure(self):
        daily_bars_with_closed_session = copy.deepcopy(load_fixture_bars())
        closed_session_bar = copy.deepcopy(daily_bars_with_closed_session[3])
        closed_session_bar["t"] = "2024-01-06T05:00:00Z"
        daily_bars_with_closed_session.insert(4, closed_session_bar)
        validation_result = validate_end_of_day_bars(
            daily_bars_with_closed_session,
            test_download_request_identity(),
        )
        self.assertEqual(validation_result.row_failures, ())
        self.assertEqual(
            validation_result.dataset_failures[0]["unexpected_sessions"],
            ["2024-01-06"],
        )

    def test_activity_fixture_has_one_exact_failure(self):
        daily_bars_with_invalid_trade_count = load_fixture_bars(
            "ten_daily_bars_one_trade_count_above_volume.json"
        )
        validation_result = validate_end_of_day_bars(
            daily_bars_with_invalid_trade_count,
            test_download_request_identity(),
        )
        self.assertEqual(len(validation_result.row_failures), 1)
        self.assertEqual(
            validation_result.row_failures[0]["data"],
            daily_bars_with_invalid_trade_count[6],
        )
        self.assertIn(
            "trade_count exceeds volume",
            validation_result.row_failures[0]["validation_error"],
        )
        self.assertEqual(validation_result.dataset_failures, ())

    def test_trade_count_above_volume_remains_invalid_without_split_adjustment(self):
        for adjustment in ("raw", "dividend", "spin-off", "dividend,spin-off"):
            with self.subTest(adjustment=adjustment):
                bars = copy.deepcopy(load_fixture_bars())
                bars[1].update(v=1, n=2000)

                validation_result = validate_end_of_day_bars(
                    bars,
                    test_download_request_identity(adjustment=adjustment),
                )

                self.assertEqual(1, len(validation_result.row_failures))
                self.assertIn(
                    "trade_count exceeds volume",
                    validation_result.row_failures[0]["validation_error"],
                )

    def test_split_adjusted_trade_count_above_scaled_volume_is_valid(self):
        for adjustment in ("split", "split,dividend", "split,spin-off", "all"):
            with self.subTest(adjustment=adjustment):
                bars = copy.deepcopy(load_fixture_bars())
                bars[1].update(v=1, n=2000)

                validation_result = validate_end_of_day_bars(
                    bars,
                    test_download_request_identity(adjustment=adjustment),
                )

                self.assertFalse(validation_result.has_failures)

    def test_split_adjusted_volume_may_round_to_zero_while_trades_remain(self):
        bars = copy.deepcopy(load_fixture_bars())
        bars[1].update(v=0, n=8)

        validation_result = validate_end_of_day_bars(
            bars,
            test_download_request_identity(adjustment="split"),
        )

        self.assertFalse(validation_result.has_failures)

    def test_split_adjusted_volume_remains_a_nonnegative_integer(self):
        for invalid_volume in (-1, 0.5):
            with self.subTest(invalid_volume=invalid_volume):
                bars = copy.deepcopy(load_fixture_bars())
                bars[1].update(v=invalid_volume)

                validation_result = validate_end_of_day_bars(
                    bars,
                    test_download_request_identity(adjustment="split"),
                )

                self.assertEqual(1, len(validation_result.row_failures))
                self.assertIn(
                    "volume is not a nonnegative integer",
                    validation_result.row_failures[0]["validation_error"],
                )

    def test_flat_explicit_zero_activity_bar_is_valid(self):
        for explicit_zero in (0, "0"):
            with self.subTest(explicit_zero=explicit_zero):
                daily_bars_with_zero_activity = copy.deepcopy(load_fixture_bars())
                daily_bars_with_zero_activity[1].update(
                    o="101",
                    h="101",
                    l="101",
                    c="101",
                    v=explicit_zero,
                    n=explicit_zero,
                    vw=explicit_zero,
                )
                validation_result = validate_end_of_day_bars(
                    daily_bars_with_zero_activity,
                    test_download_request_identity(),
                )
                self.assertFalse(validation_result.has_failures)

    def test_zero_activity_tuple_with_nonmatching_ohlc_is_invalid(self):
        daily_bars_with_nonflat_zero_activity = copy.deepcopy(load_fixture_bars())
        daily_bars_with_nonflat_zero_activity[1].update(v=0, n=0, vw=0)

        validation_result = validate_end_of_day_bars(
            daily_bars_with_nonflat_zero_activity,
            test_download_request_identity(),
        )

        self.assertEqual(1, len(validation_result.row_failures))
        self.assertIn(
            "zero-activity tuple requires matching OHLC prices",
            validation_result.row_failures[0]["validation_error"],
        )

    def test_individual_row_rules(self):
        invalid_bar_cases = (
            (lambda row: row.update(h=90), "high is below low"),
            (lambda row: row.update(o=110), "open is outside"),
            (lambda row: row.update(c=110), "close is outside"),
            (lambda row: row.update(vw=110), "vwap is outside"),
            (lambda row: row.update(vw=0), "prices must be positive"),
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
        for introduce_invalid_value, expected_error_text in invalid_bar_cases:
            with self.subTest(expected_error_text=expected_error_text):
                daily_bars_under_test = copy.deepcopy(load_fixture_bars())
                introduce_invalid_value(daily_bars_under_test[1])
                validation_result = validate_end_of_day_bars(
                    daily_bars_under_test,
                    test_download_request_identity(),
                )
                self.assertTrue(validation_result.row_failures)
                self.assertIn(
                    expected_error_text,
                    validation_result.row_failures[0]["validation_error"],
                )


if __name__ == "__main__":
    unittest.main()

import io
import json
import shutil
import tempfile
import unittest
from argparse import Namespace
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import libs.alpaca_end_of_day_security_correlation.security_master as security_master_module
import libs.alpaca_end_of_day_ticker_pipeline.online_status as online_status_module
from libs.alpaca_end_of_day_pipeline import (
    PricingDataIntegrityError,
    validate_and_publish_alpaca_download,
)
from libs.alpaca_end_of_day_reader import (
    READER_VERSION,
    EndOfDayReaderError,
    get_alpaca_end_of_day_security_availability,
    is_alpaca_end_of_day_security_online,
    list_alpaca_end_of_day_master_ids_by_availability_status,
    list_online_alpaca_end_of_day_master_ids,
    read_alpaca_end_of_day_security_history,
    run_end_of_day_reader_command,
    validate_reader_change_log,
)
from libs.alpaca_end_of_day_reader.history import BAR_FIELD_NAMES, _resolve_bar_candidates
from libs.alpaca_end_of_day_security_correlation import (
    SECURITY_MASTER_VERSION,
    SecurityCorrelationError,
    authenticate_security_master,
    build_security_correlation_report,
    lookup_security_master_identity,
    master_id_for_alpaca_asset_ids,
    migrate_legacy_security_master,
    publish_security_correlation_artifacts,
    publish_security_master_online_status,
    publish_security_master_update,
    security_master_path,
    validate_security_master_change_log,
)
from libs.alpaca_end_of_day_ticker_pipeline import evaluate_security_online_status
from libs.alpaca_end_of_day_warning_pipeline import (
    build_warning_pipeline_report,
    publish_warning_pipeline,
)
from libs.alpaca_security_lineage import inspect_local_downloads
from libs.common.atomic_writes import write_json_document_atomically
from libs.common.file_integrity import calculate_file_sha256

TEST_FIXTURES_DIRECTORY = Path(__file__).with_name("fixtures")
TEST_ASSET_UUID = "11111111-1111-4111-8111-111111111111"
TEST_MASTER_ID = master_id_for_alpaca_asset_ids([TEST_ASSET_UUID])
SECOND_ASSET_UUID = "22222222-2222-4222-8222-222222222222"
SECOND_MASTER_ID = master_id_for_alpaca_asset_ids([SECOND_ASSET_UUID])


def _request_identity(symbol: str = "TEST", adjustment: str = "raw") -> dict:
    return {
        "symbol": symbol,
        "adjustment": adjustment,
        "feed": "sip",
        "timeframe": "1Day",
        "request_from": "2024-01-01T00:00:00Z",
        "request_to": "2024-01-17T00:00:00Z",
        "asof": "2024-01-17",
    }


def _asset(
    asset_id: str = TEST_ASSET_UUID,
    symbol: str = "TEST",
    name: str = "Test Security",
) -> dict:
    return {
        "id": asset_id,
        "class": "us_equity",
        "exchange": "NASDAQ",
        "symbol": symbol,
        "name": name,
        "status": "active",
        "tradable": True,
        "lookup_symbols": [symbol],
        "lookup_cusips": [],
    }


def _lineage_report(
    repository_root: Path,
    symbol: str = "TEST",
    asset_id: str = TEST_ASSET_UUID,
    name: str = "Test Security",
) -> dict:
    asset = _asset(asset_id, symbol, name)
    return {
        "report_version": 1,
        "generated_at": "2024-01-17T12:00:00Z",
        "symbol": symbol,
        "local": inspect_local_downloads(repository_root, symbol),
        "lineage": {
            "api_request_count": 1,
            "query": {
                "input_symbol": symbol,
                "corporate_actions_start": "2016-01-01",
                "corporate_actions_end": "2024-01-17",
                "corporate_actions_data_quality": "complete",
            },
            "corporate_actions": [],
            "symbol_occupancy_evidence": [],
            "identities": [asset],
            "asset_lookups": [
                {
                    "lookup": symbol,
                    "lookup_kind": "symbol",
                    "found": True,
                    "asset": asset,
                }
            ],
        },
    }


def _publish_reader_fixture(
    repository_root: Path,
    *,
    with_integrity_failure: bool,
    fixture_name: str | None = None,
    symbol: str = "TEST",
    asset_id: str = TEST_ASSET_UUID,
    name: str = "Test Security",
    conflicting_first_close: bool = False,
    adjustment: str = "raw",
) -> Path:
    fixture_name = fixture_name or (
        "ten_daily_bars_one_high_below_low.json" if with_integrity_failure else "ten_valid_daily_bars.json"
    )
    bars = json.loads((TEST_FIXTURES_DIRECTORY / fixture_name).read_text(encoding="utf-8"))
    if conflicting_first_close:
        bars[0]["c"] = 103
    filename = adjustment.replace(",", "_") + ".csv.gz"
    download_path = repository_root / "data" / "end_of_day" / symbol / filename
    if with_integrity_failure:
        try:
            validate_and_publish_alpaca_download(
                download_path,
                bars,
                _request_identity(symbol, adjustment),
            )
        except PricingDataIntegrityError:
            pass
        else:
            raise AssertionError("test fixture did not produce its expected integrity failure")
    else:
        validate_and_publish_alpaca_download(
            download_path,
            bars,
            _request_identity(symbol, adjustment),
        )

    correlation_report = build_security_correlation_report(
        _lineage_report(repository_root, symbol, asset_id, name),
        repository_root,
    )
    correlation_receipt = publish_security_correlation_artifacts(
        repository_root,
        correlation_report,
    )
    warning_report = build_warning_pipeline_report(repository_root, symbol)
    publish_warning_pipeline(repository_root, warning_report)
    publish_security_master_update(
        repository_root,
        correlation_report,
        correlation_receipt,
        online_by_master_id=None,
    )
    evaluation = evaluate_security_online_status(
        repository_root,
        [master_id_for_alpaca_asset_ids([asset_id])],
    )
    publish_security_master_online_status(
        repository_root,
        evaluation.online_by_master_id,
        availability_status_by_master_id=evaluation.availability_status_by_master_id,
        expected_security_object_sha256_by_master_id=(evaluation.security_object_sha256_by_master_id),
        expected_source_object_sha256_by_symbol=evaluation.source_object_sha256_by_symbol,
    )
    return download_path


class EndOfDayReaderTests(unittest.TestCase):
    def test_version_changelog_matches_public_reader_contract(self) -> None:
        self.assertEqual(READER_VERSION, len(validate_reader_change_log()))

    def test_security_master_is_a_clean_digest_bound_identity_lookup(self) -> None:
        self.assertEqual(SECURITY_MASTER_VERSION, len(validate_security_master_change_log()))
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)

            authenticated = authenticate_security_master(repository_root)
            identity = lookup_security_master_identity(repository_root, TEST_MASTER_ID.upper())
            authenticated_identity = authenticated.identities_by_master_id[TEST_MASTER_ID]
            serialized_identities = json.dumps(list(authenticated.identities_by_master_id.values())).lower()
            root_document = json.loads(security_master_path(repository_root).read_text(encoding="utf-8"))

        self.assertEqual(identity, authenticated_identity)
        self.assertEqual(TEST_MASTER_ID, identity["master_id"])
        self.assertTrue(identity["online"])
        self.assertEqual("online", identity["availability_status"])
        self.assertEqual(["TEST"], identity["tickers"])
        self.assertEqual(["Test Security"], identity["names"])
        self.assertEqual(["raw"], identity["available_adjustments"])
        self.assertEqual("2024-01-02", identity["first_observed_session_date"])
        self.assertEqual("2024-01-16", identity["last_observed_session_date"])
        self.assertNotIn("warning", serialized_identities)
        self.assertNotIn("error", serialized_identities)
        self.assertNotIn("sources", root_document)
        self.assertNotIn("securities", root_document)
        self.assertEqual(1, root_document["source_count"])
        self.assertEqual(1, root_document["master_id_count"])
        self.assertLess(len(json.dumps(root_document)), 2_500)
        self.assertEqual(
            authenticated.master_sha256,
            authenticated.receipt["output"]["sha256"],
        )

    def test_online_evaluation_reuses_one_authenticated_master_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            with patch.object(
                online_status_module,
                "authenticate_security_master",
                wraps=online_status_module.authenticate_security_master,
            ) as authenticate:
                evaluation = evaluate_security_online_status(
                    repository_root,
                    [TEST_MASTER_ID],
                )

        self.assertEqual(1, authenticate.call_count)
        self.assertTrue(evaluation.online_by_master_id[TEST_MASTER_ID])

    def test_public_availability_requires_clean_readable_bars_and_never_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            files_before = {
                path.relative_to(repository_root): (path.stat().st_size, path.stat().st_mtime_ns)
                for path in repository_root.rglob("*")
                if path.is_file()
            }

            online = get_alpaca_end_of_day_security_availability(
                TEST_MASTER_ID.upper(),
                repository_root=repository_root,
            )
            offline = get_alpaca_end_of_day_security_availability(
                "f" * 64,
                repository_root=repository_root,
            )
            online_ids = list_online_alpaca_end_of_day_master_ids(
                repository_root=repository_root,
            )
            is_online = is_alpaca_end_of_day_security_online(
                TEST_MASTER_ID,
                repository_root=repository_root,
            )
            files_after = {
                path.relative_to(repository_root): (path.stat().st_size, path.stat().st_mtime_ns)
                for path in repository_root.rglob("*")
                if path.is_file()
            }

        self.assertEqual(
            {
                "master_id": TEST_MASTER_ID,
                "online": True,
                "availability_status": "online",
            },
            online,
        )
        self.assertEqual(
            {
                "master_id": "f" * 64,
                "online": False,
                "availability_status": "offline",
            },
            offline,
        )
        self.assertTrue(is_online)
        self.assertEqual([TEST_MASTER_ID], online_ids)
        self.assertEqual(files_before, files_after)

    def test_internal_market_session_gap_is_queryable_but_offline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                fixture_name="nine_daily_bars_one_market_session_missing.json",
            )

            availability = get_alpaca_end_of_day_security_availability(
                TEST_MASTER_ID,
                repository_root=repository_root,
            )
            lineage_gap_ids = list_alpaca_end_of_day_master_ids_by_availability_status(
                "lineage_gap",
                repository_root=repository_root,
            )
            online_ids = list_online_alpaca_end_of_day_master_ids(
                repository_root=repository_root,
            )
            history = read_alpaca_end_of_day_security_history(
                TEST_MASTER_ID,
                "raw",
                repository_root=repository_root,
            )

        self.assertEqual(
            {
                "master_id": TEST_MASTER_ID,
                "online": False,
                "availability_status": "lineage_gap",
            },
            availability,
        )
        self.assertEqual([TEST_MASTER_ID], lineage_gap_ids)
        self.assertEqual([], online_ids)
        self.assertEqual(
            {"online": False, "availability_status": "lineage_gap"},
            history["availability"],
        )
        self.assertEqual(9, history["bar_count"])
        self.assertEqual(0, history["integrity"]["error_count"])

    def test_sharded_source_object_tampering_is_detected_when_selected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            authenticated = authenticate_security_master(repository_root)
            descriptor = authenticated.sources_by_symbol.object_descriptor("TEST")
            source_path = security_master_path(repository_root).parent / descriptor["file"]
            source_path.write_bytes(source_path.read_bytes() + b"changed")

            with self.assertRaisesRegex(SecurityCorrelationError, "SHA-256 authentication"):
                lookup_security_master_identity(repository_root, TEST_MASTER_ID)

    def test_migrates_an_authenticated_version_one_master_without_changing_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            authenticated = authenticate_security_master(repository_root)
            sources = list(authenticated.sources_by_symbol.values())
            for source in sources:
                for identity in source["identities"]:
                    identity.pop("online")
                    identity.pop("availability_status")
            securities = security_master_module._aggregate_security_entries(
                sources,
                include_online=False,
                include_availability_status=False,
            )

            shutil.rmtree(security_master_path(repository_root).parent)
            legacy_path = security_master_module.legacy_security_master_path(repository_root)
            legacy_receipt_path = security_master_module.legacy_security_master_manifest_path(repository_root)
            legacy_path.parent.mkdir(parents=True, exist_ok=True)
            generation = security_master_module._legacy_generation_sha256(sources, securities)
            legacy_document = {
                "manifest_type": "end_of_day_security_master_index",
                "security_master_version": 1,
                "security_master_generation_sha256": generation,
                "created_at": "2026-10-08T00:00:00Z",
                "source_count": len(sources),
                "master_id_count": len(securities),
                "sources": sources,
                "securities": securities,
            }
            write_json_document_atomically(legacy_path, legacy_document, compact=False)
            legacy_stat = legacy_path.stat()
            legacy_receipt = {
                "manifest_type": "end_of_day_security_master_index_receipt",
                "security_master_version": 1,
                "security_master_generation_sha256": generation,
                "created_at": "2026-10-08T00:00:00Z",
                "source_count": len(sources),
                "master_id_count": len(securities),
                "output": {
                    "file": legacy_path.name,
                    "sha256": calculate_file_sha256(legacy_path),
                    "bytes": legacy_stat.st_size,
                    "mtime_ns": legacy_stat.st_mtime_ns,
                    "publication_sha256_verified": True,
                },
            }
            write_json_document_atomically(
                legacy_receipt_path,
                legacy_receipt,
                compact=False,
            )

            migration_receipt = migrate_legacy_security_master(repository_root)
            migrated_identity = lookup_security_master_identity(repository_root, TEST_MASTER_ID)

        self.assertEqual("published", migration_receipt["publication_status"])
        self.assertEqual(SECURITY_MASTER_VERSION, migration_receipt["security_master_version"])
        self.assertEqual(
            securities[0],
            {
                key: value
                for key, value in migrated_identity.items()
                if key not in {"online", "availability_status"}
            },
        )
        self.assertFalse(migrated_identity["online"])
        self.assertEqual("offline", migrated_identity["availability_status"])

    def test_master_update_replaces_one_source_without_losing_existing_identities(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                symbol="NEXT",
                asset_id=SECOND_ASSET_UUID,
                name="Next Security",
            )

            authenticated = authenticate_security_master(repository_root)
            original = lookup_security_master_identity(repository_root, TEST_MASTER_ID)
            second = lookup_security_master_identity(repository_root, SECOND_MASTER_ID)
            online_ids = list_online_alpaca_end_of_day_master_ids(
                repository_root=repository_root,
            )
            second_availability = get_alpaca_end_of_day_security_availability(
                SECOND_MASTER_ID,
                repository_root=repository_root,
            )

        self.assertEqual(2, authenticated.master["source_count"])
        self.assertEqual(2, authenticated.master["master_id_count"])
        self.assertEqual(["TEST"], original["source_symbols"])
        self.assertEqual(["NEXT"], second["source_symbols"])
        self.assertEqual(["Next Security"], second["names"])
        self.assertEqual(sorted((TEST_MASTER_ID, SECOND_MASTER_ID)), online_ids)
        self.assertEqual(
            {
                "master_id": SECOND_MASTER_ID,
                "online": True,
                "availability_status": "online",
            },
            second_availability,
        )

    def test_shared_master_id_is_offline_when_the_public_reader_finds_conflicting_bars(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                symbol="TEST.A",
                asset_id=TEST_ASSET_UUID,
                conflicting_first_close=True,
            )

            availability = get_alpaca_end_of_day_security_availability(
                TEST_MASTER_ID,
                repository_root=repository_root,
            )
            identity = lookup_security_master_identity(repository_root, TEST_MASTER_ID)
            with self.assertRaisesRegex(EndOfDayReaderError, "conflicting authenticated bars"):
                read_alpaca_end_of_day_security_history(
                    TEST_MASTER_ID,
                    "raw",
                    repository_root=repository_root,
                )

        self.assertEqual(
            {
                "master_id": TEST_MASTER_ID,
                "online": False,
                "availability_status": "offline",
            },
            availability,
        )
        self.assertEqual(["TEST", "TEST.A"], identity["source_symbols"])

    def test_shared_master_id_prefers_adjusted_conflicting_strict_superset_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                fixture_name="nine_daily_bars_one_market_session_missing.json",
                adjustment="all",
            )
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                fixture_name="nine_daily_bars_one_market_session_missing.json",
            )
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                symbol="TEST.A",
                asset_id=TEST_ASSET_UUID,
            )
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                symbol="TEST.A",
                asset_id=TEST_ASSET_UUID,
                conflicting_first_close=True,
                adjustment="all",
            )

            availability = get_alpaca_end_of_day_security_availability(
                TEST_MASTER_ID,
                repository_root=repository_root,
            )
            result = read_alpaca_end_of_day_security_history(
                TEST_MASTER_ID,
                "all",
                repository_root=repository_root,
            )

        self.assertEqual(
            {
                "master_id": TEST_MASTER_ID,
                "online": True,
                "availability_status": "online",
            },
            availability,
        )
        self.assertEqual(10, result["bar_count"])
        self.assertEqual("103", result["bars"][0]["close"])
        self.assertEqual(
            {
                "policy": "strict_superset_then_historical_ticker",
                "resolved_differing_overlap_count": 1,
                "resolutions": [
                    {
                        "selection_rule": "strict_superset_of_correlated_session_coverage",
                        "preferred_source_ids": ["TEST.A/all.csv.gz"],
                        "superseded_source_ids": ["TEST/all.csv.gz"],
                        "first_timestamp": "2024-01-02T05:00:00Z",
                        "last_timestamp": "2024-01-02T05:00:00Z",
                        "timestamp_count": 1,
                    }
                ],
            },
            result["source_selection"],
        )

    def test_conflicting_raw_bar_prefers_sole_historical_ticker_source(self) -> None:
        timestamp = "2024-01-02T05:00:00Z"

        def candidate(source_id: str, symbol: str, historical_symbol: str, close: str) -> dict:
            values = {field_name: "1" for field_name in BAR_FIELD_NAMES}
            values["timestamp"] = timestamp
            values["close"] = close
            return {
                "values": values,
                "provenance": {
                    "source_id": source_id,
                    "symbol": symbol,
                    "historical_symbol": historical_symbol,
                },
                "integrity_error_ids": [],
            }

        bars, selection = _resolve_bar_candidates(
            {
                timestamp: [
                    candidate("OLD/raw.csv.gz", "OLD", "OLD", "10"),
                    candidate("NEW/raw.csv.gz", "NEW", "OLD", "20"),
                ]
            },
            {
                "OLD/raw.csv.gz": frozenset((timestamp,)),
                "NEW/raw.csv.gz": frozenset((timestamp,)),
            },
            allow_strict_superset_selection=False,
        )

        self.assertEqual("10", bars[0]["close"])
        self.assertEqual("historical_ticker", selection["policy"])
        self.assertEqual(
            "requested_symbol_matches_historical_ticker",
            selection["resolutions"][0]["selection_rule"],
        )

    def test_conflicting_bar_rejects_multiple_historical_ticker_sources(self) -> None:
        timestamp = "2024-01-02T05:00:00Z"
        candidates = []
        for source_id, symbol, close in (("OLD/raw.csv.gz", "OLD", "10"), ("NEW/raw.csv.gz", "NEW", "20")):
            values = {field_name: "1" for field_name in BAR_FIELD_NAMES}
            values["timestamp"] = timestamp
            values["close"] = close
            candidates.append(
                {
                    "values": values,
                    "provenance": {
                        "source_id": source_id,
                        "symbol": symbol,
                        "historical_symbol": symbol,
                    },
                    "integrity_error_ids": [],
                }
            )

        with self.assertRaisesRegex(EndOfDayReaderError, "no source was preferred"):
            _resolve_bar_candidates(
                {timestamp: candidates},
                {candidate["provenance"]["source_id"]: frozenset((timestamp,)) for candidate in candidates},
                allow_strict_superset_selection=False,
            )

    def test_conflicting_bar_rejects_without_a_historical_ticker_source(self) -> None:
        timestamp = "2024-01-02T05:00:00Z"
        candidates = []
        for source_id, symbol, close in (("OLD/raw.csv.gz", "OLD", "10"), ("NEW/raw.csv.gz", "NEW", "20")):
            values = {field_name: "1" for field_name in BAR_FIELD_NAMES}
            values["timestamp"] = timestamp
            values["close"] = close
            candidates.append(
                {
                    "values": values,
                    "provenance": {
                        "source_id": source_id,
                        "symbol": symbol,
                        "historical_symbol": "OTHER",
                    },
                    "integrity_error_ids": [],
                }
            )

        with self.assertRaisesRegex(EndOfDayReaderError, "no source was preferred"):
            _resolve_bar_candidates(
                {timestamp: candidates},
                {candidate["provenance"]["source_id"]: frozenset((timestamp,)) for candidate in candidates},
                allow_strict_superset_selection=False,
            )

    def test_shared_master_id_never_prefers_a_conflicting_raw_superset_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                fixture_name="nine_daily_bars_one_market_session_missing.json",
            )
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                symbol="TEST.A",
                asset_id=TEST_ASSET_UUID,
                conflicting_first_close=True,
            )

            availability = get_alpaca_end_of_day_security_availability(
                TEST_MASTER_ID,
                repository_root=repository_root,
            )
            with self.assertRaisesRegex(EndOfDayReaderError, "conflicting authenticated bars"):
                read_alpaca_end_of_day_security_history(
                    TEST_MASTER_ID,
                    "raw",
                    repository_root=repository_root,
                )

        self.assertEqual(
            {
                "master_id": TEST_MASTER_ID,
                "online": False,
                "availability_status": "offline",
            },
            availability,
        )

    def test_rejects_a_well_formed_but_unpublished_master_id(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)

            with self.assertRaisesRegex(EndOfDayReaderError, "master ID is not published"):
                read_alpaca_end_of_day_security_history(
                    "f" * 64,
                    "raw",
                    repository_root=repository_root,
                )

    def test_returns_unchanged_security_scoped_rows_with_integrity_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=True)

            public_api_stdout = io.StringIO()
            with redirect_stdout(public_api_stdout):
                result = read_alpaca_end_of_day_security_history(
                    TEST_MASTER_ID.upper(),
                    "raw",
                    repository_root=repository_root,
                )

        self.assertEqual(
            {"master_id": TEST_MASTER_ID, "type": "raw"},
            result["query"],
        )
        self.assertEqual(
            {"online": False, "availability_status": "offline"},
            result["availability"],
        )
        self.assertEqual(TEST_MASTER_ID, result["security"]["master_id"])
        self.assertEqual([TEST_ASSET_UUID], result["security"]["alpaca_asset_ids"])
        self.assertEqual(["Test Security"], result["security"]["names"])
        self.assertEqual("2024-01-02", result["security"]["first_observed_session_date"])
        self.assertEqual("2024-01-16", result["security"]["last_observed_session_date"])
        self.assertNotIn("warning", json.dumps(result["security"]).lower())
        self.assertNotIn("error", json.dumps(result["security"]).lower())
        self.assertEqual(
            {
                "returned_value_source": "authenticated_frozen_download",
                "rows_with_recorded_integrity_errors_are_preserved": True,
            },
            result["value_policy"],
        )
        self.assertEqual({"overlay": None, "repaired_data": None}, result["repair"])
        self.assertEqual("", public_api_stdout.getvalue())
        self.assertEqual(10, result["bar_count"])
        self.assertEqual(10, result["selected_source_row_count"])
        self.assertEqual(1, result["segment_count"])
        self.assertEqual(1, result["integrity"]["error_count"])
        failed_bar = result["bars"][5]
        self.assertEqual(("90", "100"), (failed_bar["high"], failed_bar["low"]))
        self.assertEqual(
            [result["integrity"]["errors"][0]["error_id"]],
            failed_bar["integrity_error_ids"],
        )
        self.assertTrue(result["provenance"]["sources"][0]["download_sha256_authenticated"])

    def test_refuses_a_selected_frozen_download_with_changed_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            download_path = _publish_reader_fixture(repository_root, with_integrity_failure=False)
            download_path.write_bytes(download_path.read_bytes() + b"changed")

            with self.assertRaisesRegex(EndOfDayReaderError, "cannot read frozen download in state"):
                read_alpaca_end_of_day_security_history(
                    TEST_MASTER_ID,
                    "raw",
                    repository_root=repository_root,
                )

    def test_returns_applicable_security_scoped_warning_findings(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                fixture_name="nine_daily_bars_one_market_session_missing.json",
            )

            result = read_alpaca_end_of_day_security_history(
                TEST_MASTER_ID,
                "raw",
                repository_root=repository_root,
            )

        self.assertEqual(9, result["bar_count"])
        self.assertEqual(1, result["warnings"]["warning_count"])
        self.assertEqual(1, result["warnings"]["applicable_finding_count"])
        finding = result["warnings"]["findings"][0]
        self.assertEqual("security_segment_session_coverage", finding["check_id"])
        self.assertEqual(TEST_MASTER_ID, finding["canonical_security"]["security_id"])
        self.assertEqual([TEST_ASSET_UUID], finding["canonical_security"]["alpaca_asset_ids"])
        self.assertEqual(["2024-01-10"], finding["evidence"]["missing_sessions"])

    def test_cli_serializes_the_public_api_structure_only_to_stdout(self) -> None:
        expected_result = {
            "reader_version": READER_VERSION,
            "query": {"master_id": TEST_MASTER_ID, "type": "split,dividend"},
            "bars": [],
        }
        calls = []

        def reader(master_id, adjustment_type, *, repository_root):
            calls.append((master_id, adjustment_type, repository_root))
            return expected_result

        arguments = Namespace(adjustment_type="split,dividend", master_id=TEST_MASTER_ID)
        output_stream = io.StringIO()
        result = run_end_of_day_reader_command(
            arguments,
            repository_root=Path("/example"),
            output_stream=output_stream,
            reader=reader,
        )

        self.assertEqual(expected_result, result)
        self.assertEqual(expected_result, json.loads(output_stream.getvalue()))
        self.assertEqual(
            [(TEST_MASTER_ID, "split,dividend", Path("/example"))],
            calls,
        )

    def test_rejects_an_alpaca_uuid_instead_of_treating_it_as_a_master_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid master ID"):
            read_alpaca_end_of_day_security_history(TEST_ASSET_UUID, "raw")

    def test_master_index_is_the_only_correlation_discovery_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            unrelated_directory = repository_root / "data" / "end_of_day" / "UNRELATED"
            unrelated_directory.mkdir()
            (unrelated_directory / "security_correlation.manifest.json").write_text(
                "not json",
                encoding="utf-8",
            )

            result = read_alpaca_end_of_day_security_history(
                TEST_MASTER_ID,
                "raw",
                repository_root=repository_root,
            )

        self.assertEqual(TEST_MASTER_ID, result["query"]["master_id"])
        self.assertEqual(["TEST"], result["provenance"]["symbols"])

    def test_refuses_a_changed_master_index(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            index_path = security_master_path(repository_root)
            index_path.write_bytes(index_path.read_bytes() + b"changed")

            with self.assertRaisesRegex(EndOfDayReaderError, "security-master index stat"):
                read_alpaca_end_of_day_security_history(
                    TEST_MASTER_ID,
                    "raw",
                    repository_root=repository_root,
                )


if __name__ == "__main__":
    unittest.main()

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from libs.alpaca_end_of_day_pipeline import (
    PricingDataIntegrityError,
    validate_and_publish_alpaca_download,
)
from libs.alpaca_end_of_day_reader import (
    READER_VERSION,
    EndOfDayReaderError,
    parse_command_line_arguments,
    read_alpaca_end_of_day_security_history,
    run_end_of_day_reader_command,
    validate_reader_change_log,
)
from libs.alpaca_end_of_day_security_correlation import (
    SECURITY_MASTER_VERSION,
    authenticate_security_master,
    build_security_correlation_report,
    lookup_security_master_identity,
    master_id_for_alpaca_asset_ids,
    publish_security_correlation,
    security_master_path,
    validate_security_master_change_log,
)
from libs.alpaca_end_of_day_warning_pipeline import (
    build_warning_pipeline_report,
    publish_warning_pipeline,
)
from libs.alpaca_security_lineage import inspect_local_downloads

TEST_FIXTURES_DIRECTORY = Path(__file__).with_name("fixtures")
TEST_ASSET_UUID = "11111111-1111-4111-8111-111111111111"
TEST_MASTER_ID = master_id_for_alpaca_asset_ids([TEST_ASSET_UUID])
SECOND_ASSET_UUID = "22222222-2222-4222-8222-222222222222"
SECOND_MASTER_ID = master_id_for_alpaca_asset_ids([SECOND_ASSET_UUID])


def _request_identity(symbol: str = "TEST") -> dict:
    return {
        "symbol": symbol,
        "adjustment": "raw",
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
) -> Path:
    fixture_name = fixture_name or (
        "ten_daily_bars_one_high_below_low.json" if with_integrity_failure else "ten_valid_daily_bars.json"
    )
    bars = json.loads((TEST_FIXTURES_DIRECTORY / fixture_name).read_text(encoding="utf-8"))
    download_path = repository_root / "data" / "end_of_day" / symbol / "raw.csv.gz"
    if with_integrity_failure:
        try:
            validate_and_publish_alpaca_download(download_path, bars, _request_identity(symbol))
        except PricingDataIntegrityError:
            pass
        else:
            raise AssertionError("test fixture did not produce its expected integrity failure")
    else:
        validate_and_publish_alpaca_download(download_path, bars, _request_identity(symbol))

    correlation_report = build_security_correlation_report(
        _lineage_report(repository_root, symbol, asset_id, name),
        repository_root,
    )
    publish_security_correlation(repository_root, correlation_report)
    warning_report = build_warning_pipeline_report(repository_root, symbol)
    publish_warning_pipeline(repository_root, warning_report)
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

        self.assertEqual(identity, authenticated.identities_by_master_id[TEST_MASTER_ID])
        self.assertEqual(TEST_MASTER_ID, identity["master_id"])
        self.assertEqual(["TEST"], identity["tickers"])
        self.assertEqual(["Test Security"], identity["names"])
        self.assertEqual(["raw"], identity["available_adjustments"])
        self.assertEqual("2024-01-02", identity["first_observed_session_date"])
        self.assertEqual("2024-01-16", identity["last_observed_session_date"])
        serialized_identities = json.dumps(authenticated.master["securities"]).lower()
        self.assertNotIn("warning", serialized_identities)
        self.assertNotIn("error", serialized_identities)
        self.assertEqual(
            authenticated.master_sha256,
            authenticated.receipt["output"]["sha256"],
        )

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

        self.assertEqual(2, authenticated.master["source_count"])
        self.assertEqual(2, authenticated.master["master_id_count"])
        self.assertEqual(["TEST"], original["source_symbols"])
        self.assertEqual(["NEXT"], second["source_symbols"])
        self.assertEqual(["Next Security"], second["names"])

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

        arguments = parse_command_line_arguments(["--type", "split,dividend", "--master-id", TEST_MASTER_ID])
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

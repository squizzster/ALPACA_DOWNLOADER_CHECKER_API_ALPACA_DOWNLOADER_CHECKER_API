import csv
import io
import json
import tempfile
import unittest
from copy import deepcopy
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from alpaca_end_of_day import (
    API_CHANGE_LOG_PATH,
    API_VERSION,
    FLAT_ONLINE_CSV_COLUMNS,
    CatalogChangedError,
    DataLibrary,
    DataLibraryError,
    FlatCsvExport,
    PublicIdUnavailableError,
    Security,
    _IdentityTimeline,
)
from libs.alpaca_end_of_day_pipeline import validate_and_publish_alpaca_download
from libs.alpaca_end_of_day_reader import read_alpaca_end_of_day_security_history
from libs.alpaca_end_of_day_security_correlation import authenticate_security_master
from read_alpaca_end_of_day_ticker import (
    parse_command_line_arguments,
    run_end_of_day_reader_command,
)
from tests.test_alpaca_end_of_day_reader import (
    SECOND_ASSET_UUID,
    SECOND_MASTER_ID,
    TEST_ASSET_UUID,
    TEST_FIXTURES_DIRECTORY,
    TEST_MASTER_ID,
    _publish_reader_fixture,
    _request_identity,
)


def _publish_raw_and_all_fixture(
    repository_root: Path,
    *,
    symbol: str = "TEST",
    asset_id: str = TEST_ASSET_UUID,
    name: str = "Test Security",
) -> None:
    bars = json.loads((TEST_FIXTURES_DIRECTORY / "ten_valid_daily_bars.json").read_text(encoding="utf-8"))
    for bar in bars:
        for field_name in ("o", "h", "l", "c", "vw"):
            bar[field_name] /= 2
        bar["v"] *= 2
    request_identity = _request_identity(symbol)
    request_identity["adjustment"] = "all"
    validate_and_publish_alpaca_download(
        repository_root / "data" / "end_of_day" / symbol / "all.csv.gz",
        bars,
        request_identity,
    )
    _publish_reader_fixture(
        repository_root,
        with_integrity_failure=False,
        symbol=symbol,
        asset_id=asset_id,
        name=name,
    )


class AlpacaEndOfDayPublicApiTests(unittest.TestCase):
    def setUp(self) -> None:
        # These fixtures exercise the public transformation contract with the
        # established synthetic logical inputs. Native v6 storage/readback is
        # covered by the v6 boundary integration tests.
        self.catalogue_patcher = patch(
            "alpaca_end_of_day.load_catalogue",
            side_effect=authenticate_security_master,
        )

        def read_established_history(*args, master=None, inputs=None, **kwargs):
            del master, inputs
            return read_alpaca_end_of_day_security_history(*args, **kwargs)

        self.history_patcher = patch(
            "alpaca_end_of_day._read_published_history",
            side_effect=read_established_history,
        )
        self.catalogue_reader = self.catalogue_patcher.start()
        self.history_reader = self.history_patcher.start()

    def tearDown(self) -> None:
        self.history_patcher.stop()
        self.catalogue_patcher.stop()

    def test_public_api_version_has_one_changelog_entry_per_version(self) -> None:
        entries = json.loads(API_CHANGE_LOG_PATH.read_text(encoding="utf-8"))
        self.assertEqual(API_VERSION, len(entries))

    def test_catalog_exposes_only_online_securities(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                fixture_name="nine_daily_bars_one_market_session_missing.json",
                symbol="NEXT",
                asset_id=SECOND_ASSET_UUID,
                name="Next Security",
            )

            data = DataLibrary(repository_root)
            catalog = data.catalog()
            securities = data.list_securities()

            self.assertEqual(1, data.online_count())
            self.assertEqual(1, catalog.online_count)
            self.assertEqual(data.catalog_generation(), catalog.generation)
            self.assertIsNotNone(catalog.published_at.tzinfo)
            self.assertEqual((TEST_MASTER_ID,), tuple(item.public_id for item in securities))
            self.assertEqual(TEST_MASTER_ID, data.get_security(TEST_MASTER_ID).public_id)
            resolutions = data.resolve_ticker("test")
            self.assertEqual(1, len(resolutions))
            self.assertEqual(TEST_MASTER_ID, resolutions[0].public_id)
            self.assertEqual("TEST", resolutions[0].ticker)
            self.assertEqual(date(2024, 1, 2), resolutions[0].first_session)
            self.assertEqual(date(2024, 1, 16), resolutions[0].last_session)
            self.assertEqual(resolutions, data.resolve_ticker("TEST", on="2024-01-05"))
            self.assertEqual((), data.resolve_ticker("TEST", on="2023-01-05"))
            self.assertEqual((), data.resolve_ticker("NEXT"))
            self.assertEqual((), data.search_securities("Next Security"))
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.get_security(SECOND_MASTER_ID)
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.get_security("f" * 64)
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.get_history(SECOND_MASTER_ID)
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.get_history("f" * 64)

    def test_get_security_does_not_expose_an_offline_catalogue_identity(self) -> None:
        offline_master = SimpleNamespace(
            receipt={
                "security_master_generation_sha256": "published-generation",
                "created_at": "2026-10-13T00:00:00Z",
            },
            identities_by_master_id={
                TEST_MASTER_ID: {
                    "master_id": TEST_MASTER_ID,
                    "online": False,
                }
            },
        )
        with patch("alpaca_end_of_day.load_catalogue", return_value=offline_master):
            data = DataLibrary(Path("unused"))
            with self.assertRaisesRegex(PublicIdUnavailableError, "^public ID is not available$"):
                data.get_security(TEST_MASTER_ID)

    def test_history_uses_exact_types_and_inclusive_session_dates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)

            data = DataLibrary(repository_root)
            history = data.get_history(
                TEST_MASTER_ID.upper(),
                start="2024-01-04",
                end=date(2024, 1, 8),
            )
            serialized = history.to_dict()

        self.assertEqual(TEST_MASTER_ID, history.security.public_id)
        self.assertEqual("raw", history.adjustment)
        self.assertEqual(
            (date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8)),
            tuple(bar.session for bar in history.bars),
        )
        self.assertIsInstance(history.bars[0].open, Decimal)
        self.assertIsInstance(history.bars[0].volume, int)
        self.assertEqual("2024-01-04", serialized["range"]["start"])
        self.assertEqual("2024-01-08", serialized["range"]["end"])
        self.assertNotIn("online", json.dumps(serialized))
        self.assertNotIn("provenance", serialized)
        self.assertNotIn("integrity", serialized)

    def test_history_calls_reuse_the_open_catalogue_and_reader_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_raw_and_all_fixture(repository_root)
            data = DataLibrary(repository_root)

            data.get_history(TEST_MASTER_ID, "raw")
            data.get_history(TEST_MASTER_ID, "all")

        first, second = self.history_reader.call_args_list[-2:]
        self.assertIs(data._master, first.kwargs["master"])
        self.assertIs(data._master, second.kwargs["master"])
        self.assertIs(first.kwargs["inputs"], second.kwargs["inputs"])

    def test_ticker_resolution_preserves_reuse_across_public_ids_and_dates(self) -> None:
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
            first_record = deepcopy(authenticated.identities_by_master_id[TEST_MASTER_ID])
            second_record = deepcopy(authenticated.identities_by_master_id[SECOND_MASTER_ID])
            first_record["tickers"] = ["SHARED"]
            first_record["names"] = ["Owens Corning"]
            first_record["observed_history_ranges"] = [
                {
                    "ticker": "SHARED",
                    "first_session_date": "2024-01-02",
                    "last_session_date": "2024-01-08",
                    "adjustments": ["raw"],
                }
            ]
            second_record["tickers"] = ["SHARED"]
            second_record["names"] = ["Morningstar, Inc. Common Stock"]
            second_record["observed_history_ranges"] = [
                {
                    "ticker": "SHARED",
                    "first_session_date": "2024-01-09",
                    "last_session_date": "2024-01-16",
                    "adjustments": ["raw"],
                }
            ]
            public_master = SimpleNamespace(
                receipt=authenticated.receipt,
                identities_by_master_id={
                    TEST_MASTER_ID: first_record,
                    SECOND_MASTER_ID: second_record,
                },
            )

            with patch("alpaca_end_of_day.load_catalogue", return_value=public_master):
                data = DataLibrary(repository_root)
                all_resolutions = data.resolve_ticker("shared")
                first_date = data.resolve_ticker("SHARED", on="2024-01-05")
                second_date = data.resolve_ticker("SHARED", on=date(2024, 1, 12))
                search_results = data.search_securities("  ORNING  ")
                limited_search = data.search_securities("orning", limit=1)

        self.assertEqual((TEST_MASTER_ID, SECOND_MASTER_ID), tuple(item.public_id for item in all_resolutions))
        self.assertEqual((TEST_MASTER_ID,), tuple(item.public_id for item in first_date))
        self.assertEqual((SECOND_MASTER_ID,), tuple(item.public_id for item in second_date))
        self.assertEqual(
            ("Morningstar, Inc. Common Stock", "Owens Corning"),
            tuple(item.names[0] for item in search_results),
        )
        self.assertEqual(search_results[:1], limited_search)

    def test_library_rejects_a_history_from_a_newer_catalog_generation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            data = DataLibrary(repository_root)
            original_generation = data.catalog_generation()

            _publish_reader_fixture(
                repository_root,
                with_integrity_failure=False,
                symbol="NEXT",
                asset_id=SECOND_ASSET_UUID,
                name="Next Security",
            )

            self.assertNotEqual(original_generation, DataLibrary(repository_root).catalog_generation())
            with self.assertRaises(CatalogChangedError):
                data.get_history(TEST_MASTER_ID)

    def test_flat_csv_dump_is_date_ordered_with_raw_and_all_adjusted_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_raw_and_all_fixture(repository_root)
            _publish_raw_and_all_fixture(
                repository_root,
                symbol="NEXT",
                asset_id=SECOND_ASSET_UUID,
                name="Next Security",
            )
            output_path = repository_root / "exports" / "online.csv"
            arguments = parse_command_line_arguments(
                [
                    "--dump-flat-online-csv",
                    str(output_path),
                    "--start",
                    "2024-01-04",
                    "--end",
                    "2024-01-05",
                ]
            )

            with patch("alpaca_end_of_day._FLAT_CSV_SORT_CHUNK_ROWS", 2):
                receipt = run_end_of_day_reader_command(
                    arguments,
                    repository_root=repository_root,
                )

            with output_path.open(encoding="utf-8", newline="") as output_file:
                reader = csv.DictReader(output_file)
                rows = list(reader)

        self.assertIsInstance(receipt, FlatCsvExport)
        self.assertEqual(2, receipt.online_security_count)
        self.assertEqual(2, receipt.exported_security_count)
        self.assertEqual(4, receipt.row_count)
        self.assertEqual(list(FLAT_ONLINE_CSV_COLUMNS), reader.fieldnames)
        self.assertEqual(
            sorted((row["date"], row["unique_id"]) for row in rows),
            [(row["date"], row["unique_id"]) for row in rows],
        )
        test_row = next(
            row for row in rows if row["date"] == "2024-01-04" and row["unique_id"] == TEST_MASTER_ID
        )
        self.assertEqual("TEST", test_row["ticker"])
        self.assertEqual("Test Security", test_row["name"])
        self.assertEqual(Decimal("102"), Decimal(test_row["raw_open"]))
        self.assertEqual(Decimal("51"), Decimal(test_row["all_adjusted_open"]))
        self.assertEqual("1200", test_row["raw_volume"])
        self.assertEqual("2400", test_row["all_adjusted_volume"])
        self.assertEqual("120", test_row["raw_trade_count"])
        self.assertEqual("120", test_row["all_adjusted_trade_count"])

    def test_flat_csv_identity_prefers_the_ticker_whose_observed_range_continues(self) -> None:
        public_id = "a" * 64
        security = Security(
            public_id=public_id,
            tickers=("NEXT", "NEXTV"),
            names=("Next Security",),
            cusips=("123456789",),
            external_identifiers=(),
            exchanges=("NASDAQ",),
            asset_classes=("us_equity",),
            first_session=date(2025, 7, 1),
            last_session=date(2026, 10, 5),
            adjustments=("raw", "all"),
        )
        record = {
            "alpaca_identities": [
                {
                    "tickers": ["NEXT", "NEXTV"],
                    "names": ["Next Security"],
                    "cusips": ["123456789"],
                    "exchanges": ["NASDAQ"],
                    "asset_classes": ["us_equity"],
                    "ticker_ownership_periods": [],
                }
            ],
            "observed_history_ranges": [
                {
                    "ticker": "NEXT",
                    "first_session_date": "2025-07-01",
                    "last_session_date": "2026-10-05",
                },
                {
                    "ticker": "NEXTV",
                    "first_session_date": "2025-07-01",
                    "last_session_date": "2025-07-01",
                },
            ],
        }

        identity = _IdentityTimeline(record, security).for_session(date(2025, 7, 1))

        self.assertEqual("NEXT", identity.ticker)

        record["observed_history_ranges"][1]["last_session_date"] = "2026-10-05"
        with self.assertRaisesRegex(DataLibraryError, "has no unique ticker"):
            _IdentityTimeline(record, security).for_session(date(2025, 7, 1))

    def test_command_line_reader_is_a_client_of_the_public_library(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            repository_root = Path(temporary_directory)
            _publish_reader_fixture(repository_root, with_integrity_failure=False)
            arguments = parse_command_line_arguments(
                [
                    "--public-id",
                    TEST_MASTER_ID,
                    "--type",
                    "raw",
                    "--start",
                    "2024-01-04",
                    "--end",
                    "2024-01-05",
                ]
            )
            output = io.StringIO()

            history = run_end_of_day_reader_command(
                arguments,
                repository_root=repository_root,
                output_stream=output,
            )

        document = json.loads(output.getvalue())
        self.assertEqual(2, len(history.bars))
        self.assertEqual(API_VERSION, document["api_version"])
        self.assertEqual(TEST_MASTER_ID, document["security"]["public_id"])
        self.assertEqual(["2024-01-04", "2024-01-05"], [bar["session"] for bar in document["bars"]])


if __name__ == "__main__":
    unittest.main()

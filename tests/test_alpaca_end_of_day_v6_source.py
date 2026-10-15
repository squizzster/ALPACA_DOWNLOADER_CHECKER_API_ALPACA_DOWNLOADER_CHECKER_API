import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_end_of_day_pipeline.daily_bar_validation import validate_end_of_day_bars
from libs.alpaca_end_of_day_security_correlation import (
    CORRELATION_VERSION,
    OFFLINE_AVAILABILITY_STATUS,
    SECURITY_ID_SCHEME_VERSION,
    security_id_for_alpaca_asset_ids,
)
from libs.alpaca_end_of_day_security_correlation.artifact_paths import security_master_directory
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_v6 import (
    authenticate_monthly_source,
    initialize_v6_store,
    publish_monthly_source,
    publish_monthly_source_update,
)
from libs.alpaca_end_of_day_v6.catalogue import (
    AVAILABILITY_BOUNDARY,
    _authenticate_catalogue_store,
    _identity_candidate_from_sources,
    _load_catalogue_store,
    authenticate_identity_candidate,
    load_identity_candidate,
    publish_availability,
    publish_catalogue,
    publish_identity_candidate,
)
from libs.alpaca_end_of_day_v6.common import (
    PUBLICATION_PENDING,
    assert_publication_complete,
    publication_transaction,
)
from libs.alpaca_end_of_day_v6.correlation import (
    authenticate_correlation,
    load_correlation,
    publish_correlation,
)
from libs.alpaca_end_of_day_v6.downstream_input import (
    build_downstream_pricing_input,
    empty_verified_session_exclusions,
    episode_partition_loaders,
)
from libs.alpaca_end_of_day_v6.errors import V6Error
from libs.alpaca_end_of_day_v6.lineage import (
    acquire_and_publish_lineage,
    authenticate_lineage,
    load_lineage,
)
from libs.alpaca_end_of_day_v6.paths import legacy_engine_root
from libs.alpaca_end_of_day_v6.pipeline import _last_run_document, _load_reusable_boundaries
from libs.alpaca_end_of_day_v6.reader import PublishedHistoryInputs
from libs.alpaca_end_of_day_v6.source import load_monthly_source, read_monthly_adjustment_rows
from libs.alpaca_end_of_day_v6.validation import publish_validation, request_identity
from libs.alpaca_end_of_day_v6.versions import AVAILABILITY_VERSION
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES


def _bar(timestamp: str, value: int) -> dict:
    return {
        "t": timestamp,
        "o": value,
        "h": value + 1,
        "l": value - 1,
        "c": value,
        "v": value * 100,
        "n": value * 10,
        "vw": value,
    }


def _files(root: Path) -> dict[Path, tuple[int, int, bytes]]:
    return {
        path.relative_to(root): (path.stat().st_size, path.stat().st_mtime_ns, path.read_bytes())
        for path in root.rglob("*")
        if path.is_file() and path.name != ".publish.lock"
    }


def _candidate_source(
    symbol: str = "GOOG",
    *,
    asset_number: int = 1,
    correlation_generation: str = "a" * 64,
    last_session_date: str = "2026-10-01",
) -> dict:
    asset_id = f"00000000-0000-4000-8000-{asset_number:012x}"
    master_id = security_id_for_alpaca_asset_ids([asset_id])
    identity = {
        "master_id": master_id,
        "online": False,
        "availability_status": OFFLINE_AVAILABILITY_STATUS,
        "master_id_scheme": {
            "kind": "sha256",
            "version": SECURITY_ID_SCHEME_VERSION,
            "canonical_input": "newline_joined_sorted_unique_alpaca_asset_ids",
        },
        "alpaca_asset_ids": [asset_id],
        "tickers": [symbol],
        "names": [f"{symbol} Inc."],
        "cusips": [f"{asset_number:09d}"],
        "figi_identifiers": [],
        "exchanges": ["NASDAQ"],
        "asset_classes": ["us_equity"],
        "statuses": ["active"],
        "membership_bases": ["unique_uuid"],
        "first_observed_session_date": "2026-10-01",
        "last_observed_session_date": last_session_date,
        "available_adjustments": ["raw"],
        "observed_history_ranges": [
            {
                "ticker": symbol,
                "first_session_date": "2026-10-01",
                "last_session_date": last_session_date,
                "adjustments": ["raw"],
            }
        ],
        "identity_events": [],
        "alpaca_identities": [
            {
                "alpaca_asset_id": asset_id,
                "tickers": [symbol],
                "names": [f"{symbol} Inc."],
                "cusips": [f"{asset_number:09d}"],
                "figi_identifiers": [],
                "exchanges": ["NASDAQ"],
                "asset_classes": ["us_equity"],
                "statuses": ["active"],
                "ticker_ownership_periods": [],
            }
        ],
    }
    return {
        "symbol": symbol,
        "correlation_version": CORRELATION_VERSION,
        "correlation_generation_sha256": correlation_generation,
        "correlated_output_sha256": "b" * 64,
        "uncorrelated_output_sha256": "c" * 64,
        "verified_session_exclusions": empty_verified_session_exclusions(Path(), symbol),
        "identities": [identity],
    }


def _availability_report(candidate) -> dict:
    return {
        "schema_version": AVAILABILITY_VERSION,
        "boundary": AVAILABILITY_BOUNDARY,
        "candidate_generation_sha256": candidate.generation_sha256,
        "warning_set_sha256": "f" * 64,
        "decisions": [
            {
                "master_id": master_id,
                "online": False,
                "availability_status": OFFLINE_AVAILABILITY_STATUS,
            }
            for master_id in sorted(candidate.master.identities_by_master_id)
        ],
    }


class NativeV6MonthlySourceTests(unittest.TestCase):
    def test_missing_preliminary_episode_support_fails_closed(self) -> None:
        load_correlation, _load_rows = episode_partition_loaders(Path("unused"), {})
        with self.assertRaises(SecurityCorrelationError):
            load_correlation("RELATED")

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name) / "data_vtest"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _views(self, rows):
        return {adjustment: list(rows) for adjustment in ALPACA_ADJUSTMENT_MODES}

    def test_exact_copy_aliases_and_round_trip(self):
        rows = [
            _bar("2026-09-30T04:00:00Z", 10),
            _bar("2026-10-01T04:00:00Z", 11),
        ]
        result = publish_monthly_source(
            self.root,
            "GOOG",
            self._views(rows),
            "2026-10-12T00:00:00Z",
        )
        self.assertEqual(["2026-09", "2026-10"], result["changed_placements"])
        authenticated = authenticate_monthly_source(self.root, "GOOG")
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            self.assertEqual(2, len(authenticated.by_adjustment[adjustment].rows))

        october = self.root / "end_of_day" / "GOOG" / "2026" / "Oct"
        self.assertEqual(["raw.csv.gz"], sorted(path.name for path in october.glob("*.csv.gz")))
        manifest = json.loads((october / "month.manifest.json").read_text())
        self.assertEqual("file", manifest["adjustments"][0]["payload"]["storage"])
        self.assertTrue(
            all(entry["payload"]["storage"] == "exact_copy" for entry in manifest["adjustments"][1:])
        )

    def test_exact_copy_payload_is_read_once_per_month_boundary_operation(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )

        from libs.alpaca_end_of_day_v6 import source as source_module

        with patch.object(
            source_module,
            "_read_payload_rows",
            wraps=source_module._read_payload_rows,
        ) as read_payload:
            loaded = load_monthly_source(self.root, "GOOG")
        self.assertEqual(1, read_payload.call_count)
        self.assertEqual(8, len(loaded.adjustments))

        with patch.object(
            source_module,
            "_read_payload_rows",
            wraps=source_module._read_payload_rows,
        ) as read_payload:
            authenticated = authenticate_monthly_source(self.root, "GOOG")
        self.assertEqual(1, read_payload.call_count)
        self.assertEqual(8, len(authenticated.adjustments))

        payload_cache = {}
        with patch.object(
            source_module,
            "_read_payload_rows",
            wraps=source_module._read_payload_rows,
        ) as read_payload:
            logical_views = {
                adjustment: read_monthly_adjustment_rows(
                    self.root,
                    "GOOG",
                    adjustment,
                    payload_cache=payload_cache,
                )
                for adjustment in ALPACA_ADJUSTMENT_MODES
            }
        self.assertEqual(1, read_payload.call_count)
        self.assertTrue(all(len(rows) == 1 for rows in logical_views.values()))

    def test_trusted_source_load_uses_published_logical_digests(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )
        from libs.alpaca_end_of_day_v6 import source as source_module

        with patch.object(
            source_module,
            "_logical_digest",
            side_effect=AssertionError("consumer recalculated a published source digest"),
        ):
            loaded = load_monthly_source(self.root, "GOOG")

        self.assertEqual(8, len(loaded.adjustments))
        self.assertTrue(all(logical.logical_sha256 for logical in loaded.adjustments))

    def test_one_session_existing_month_update_is_local(self):
        september_and_october = [
            _bar("2026-09-30T04:00:00Z", 10),
            _bar("2026-10-01T04:00:00Z", 11),
        ]
        publish_monthly_source(
            self.root,
            "AAPL",
            self._views([_bar("2026-10-01T04:00:00Z", 90)]),
            "2026-10-12T00:00:00Z",
        )
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views(september_and_october),
            "2026-10-12T00:00:00Z",
        )
        before = _files(self.root)
        result = publish_monthly_source_update(
            self.root,
            "GOOG",
            self._views(
                [
                    september_and_october[1],
                    _bar("2026-10-02T04:00:00Z", 12),
                ]
            ),
            "2026-10-01T00:00:00Z",
            "2026-10-13T00:00:00Z",
        )
        after = _files(self.root)
        changed = {path for path in before.keys() | after.keys() if before.get(path) != after.get(path)}
        self.assertEqual(["2026-10"], result["changed_placements"])
        self.assertFalse(result["inventory_changed"])
        self.assertEqual(
            {
                Path("end_of_day/GOOG/2026/Oct/month.manifest.json"),
                Path("end_of_day/GOOG/2026/Oct/raw.csv.gz"),
            },
            changed,
        )

    def test_new_month_updates_only_ticker_inventory_and_new_month(self):
        rows = [_bar("2026-10-30T04:00:00Z", 10)]
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views(rows),
            "2026-11-01T00:00:00Z",
        )
        before = _files(self.root)
        result = publish_monthly_source_update(
            self.root,
            "GOOG",
            self._views([_bar("2026-11-02T05:00:00Z", 11)]),
            "2026-11-01T00:00:00Z",
            "2026-11-03T00:00:00Z",
        )
        after = _files(self.root)
        changed = {path for path in before.keys() | after.keys() if before.get(path) != after.get(path)}
        self.assertEqual(["2026-11"], result["changed_placements"])
        self.assertTrue(result["inventory_changed"])
        self.assertEqual(
            {
                Path("end_of_day/GOOG/source.manifest.json"),
                Path("end_of_day/GOOG/2026/Nov/month.manifest.json"),
                Path("end_of_day/GOOG/2026/Nov/raw.csv.gz"),
            },
            changed,
        )

    def test_payload_hash_is_recomputed(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )
        path = self.root / "end_of_day" / "GOOG" / "2026" / "Oct" / "raw.csv.gz"
        content = bytearray(path.read_bytes())
        content[len(content) // 2] ^= 1
        path.write_bytes(content)
        with self.assertRaisesRegex(V6Error, "authentication failed"):
            authenticate_monthly_source(self.root, "GOOG")

    def test_reader_consumes_monthly_rows_without_reauditing_the_source(self):
        rows = [_bar("2026-10-01T04:00:00Z", 10)]
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views(rows),
            "2026-10-12T00:00:00Z",
        )

        with (
            patch(
                "libs.alpaca_end_of_day_v6.source._authenticate_month_directory",
                side_effect=AssertionError("reader attempted to authenticate a published month"),
            ),
            patch(
                "libs.alpaca_end_of_day_v6.source.sha256_file",
                side_effect=AssertionError("reader attempted to hash a published payload"),
            ),
        ):
            observed = read_monthly_adjustment_rows(self.root, "GOOG", "raw")

        self.assertEqual([row["t"] for row in rows], [row["t"] for row in observed])

    def test_exact_copy_alias_records_direct_canonical_payload_provenance(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )
        manifest_path = self.root / "end_of_day" / "GOOG" / "2026" / "Oct" / "month.manifest.json"
        manifest = json.loads(manifest_path.read_text())
        split = next(entry for entry in manifest["adjustments"] if entry["adjustment"] == "split")
        self.assertEqual("exact_copy", split["payload"]["storage"])
        split["payload"]["equivalent_to"] = "split"
        manifest_path.write_text(json.dumps(manifest, separators=(",", ":")) + "\n")

        with self.assertRaisesRegex(V6Error, "exact-copy alias is not direct"):
            authenticate_monthly_source(self.root, "GOOG")

    def test_downstream_input_reconstructs_authenticated_logical_downloads(self):
        rows = [
            _bar("2026-09-30T04:00:00Z", 10),
            _bar("2026-10-01T04:00:00Z", 11),
        ]
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views(rows),
            "2026-10-12T00:00:00Z",
        )
        publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")

        view = build_downstream_pricing_input(self.root, "GOOG")
        self.assertEqual(
            set(ALPACA_ADJUSTMENT_MODES),
            {item.manifest["adjustment"] for item in view.downloads_by_filename.values()},
        )
        raw = view.downloads_by_filename["raw.csv.gz"]
        self.assertEqual([row["t"] for row in rows], [row["t"] for row in raw.rows])
        self.assertEqual(
            view.source.by_adjustment["raw"].logical_sha256,
            raw.manifest["sha256"],
        )
        self.assertEqual("2026-10-12T00:00:00Z", raw.manifest["request_to"])
        self.assertFalse(raw.path.exists())
        local = view.local_report()
        self.assertEqual(8, local["download_count"])
        self.assertTrue(local["download_sha256_authentication_performed"])

    def test_validation_receipt_is_published_only_after_its_output_passes_validation(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )

        with (
            patch(
                "libs.alpaca_end_of_day_v6.validation._validate_validation_output",
                side_effect=V6Error("invalid new output"),
            ),
            self.assertRaisesRegex(V6Error, "invalid new output"),
        ):
            publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")

        self.assertFalse((self.root / "validation" / "GOOG" / "validation.manifest.json").exists())

    def test_validation_reuses_unchanged_months_and_adjacent_seams(self):
        from libs.alpaca_end_of_day_v6 import validation as validation_module

        publish_monthly_source(
            self.root,
            "GOOG",
            self._views(
                [
                    _bar("2026-09-30T04:00:00Z", 10),
                    _bar("2026-10-01T04:00:00Z", 11),
                ]
            ),
            "2026-10-12T00:00:00Z",
        )
        with patch.object(
            validation_module,
            "validate_end_of_day_bars",
            wraps=validation_module.validate_end_of_day_bars,
        ) as validate:
            publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")
        self.assertEqual(16, validate.call_count)
        validation_root = self.root / "validation" / "GOOG"
        first = json.loads((validation_root / "validation.manifest.json").read_text())
        before_update = _files(validation_root)

        publish_monthly_source_update(
            self.root,
            "GOOG",
            self._views(
                [
                    _bar("2026-10-01T04:00:00Z", 11),
                    _bar("2026-10-02T04:00:00Z", 12),
                ]
            ),
            "2026-10-01T00:00:00Z",
            "2026-10-12T00:00:00Z",
        )
        with patch.object(
            validation_module,
            "validate_end_of_day_bars",
            wraps=validation_module.validate_end_of_day_bars,
        ) as validate:
            publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")
        self.assertEqual(8, validate.call_count)
        second = json.loads((validation_root / "validation.manifest.json").read_text())
        self.assertEqual(
            first["months"][0]["month_validation_generation_sha256"],
            second["months"][0]["month_validation_generation_sha256"],
        )
        self.assertNotEqual(
            first["months"][1]["month_validation_generation_sha256"],
            second["months"][1]["month_validation_generation_sha256"],
        )
        after_update = _files(validation_root)
        self.assertEqual(
            {
                Path("2026/Oct/month.validation.json"),
                Path("validation.json"),
                Path("validation.manifest.json"),
            },
            {
                path
                for path in before_update.keys() | after_update.keys()
                if before_update.get(path) != after_update.get(path)
            },
        )

        with patch.object(
            validation_module,
            "validate_end_of_day_bars",
            wraps=validation_module.validate_end_of_day_bars,
        ) as validate:
            publish_validation(self.root, "GOOG", "2026-10-13T00:00:00Z")
        self.assertEqual(0, validate.call_count)

        third = json.loads((validation_root / "validation.manifest.json").read_text())
        self.assertEqual(second["months"], third["months"])

        published_months = [
            json.loads((validation_root / entry["output"]["file"]).read_text()) for entry in third["months"]
        ]
        source = load_monthly_source(self.root, "GOOG")
        for adjustment_index, logical in enumerate(source.adjustments):
            expected = validate_end_of_day_bars(
                list(logical.rows),
                request_identity("GOOG", logical.adjustment, "2026-10-13T00:00:00Z"),
            )
            parts = [month["month"]["adjustments"][adjustment_index] for month in published_months]
            self.assertEqual(expected.rows_seen, sum(part["rows_seen"] for part in parts))
            self.assertEqual(expected.first_bar_timestamp, parts[0]["first_timestamp"])
            self.assertEqual(expected.last_bar_timestamp, parts[-1]["last_timestamp"])
            self.assertEqual(
                len(expected.observed_session_dates),
                sum(part["session_count"] for part in parts),
            )
            self.assertEqual([], list(expected.row_failures))
            self.assertEqual([], list(expected.dataset_failures))
            self.assertTrue(all(part["row_failures"] == [] for part in parts))
            self.assertTrue(all(part["dataset_failures"] == [] for part in parts))

    def test_validation_change_stops_after_the_immediate_successor_seam(self):
        from libs.alpaca_end_of_day_v6 import validation as validation_module

        publish_monthly_source(
            self.root,
            "GOOG",
            self._views(
                [
                    _bar("2026-08-03T04:00:00Z", 10),
                    _bar("2026-09-01T04:00:00Z", 11),
                    _bar("2026-10-01T04:00:00Z", 12),
                    _bar("2026-11-02T05:00:00Z", 13),
                ]
            ),
            "2026-12-01T00:00:00Z",
        )
        publish_validation(self.root, "GOOG", "2026-12-01T00:00:00Z")
        validation_root = self.root / "validation" / "GOOG"
        first = json.loads((validation_root / "validation.manifest.json").read_text())

        publish_monthly_source_update(
            self.root,
            "GOOG",
            self._views(
                [
                    _bar("2026-09-01T04:00:00Z", 11),
                    _bar("2026-09-02T04:00:00Z", 14),
                ]
            ),
            "2026-09-01T00:00:00Z",
            "2026-10-01T00:00:00Z",
        )
        with patch.object(
            validation_module,
            "validate_end_of_day_bars",
            wraps=validation_module.validate_end_of_day_bars,
        ) as validate:
            publish_validation(self.root, "GOOG", "2026-12-01T00:00:00Z")
        self.assertEqual(8, validate.call_count)
        second = json.loads((validation_root / "validation.manifest.json").read_text())
        first_by_month = {entry["placement"]: entry for entry in first["months"]}
        second_by_month = {entry["placement"]: entry for entry in second["months"]}

        self.assertEqual(first_by_month["2026-08"], second_by_month["2026-08"])
        self.assertNotEqual(
            first_by_month["2026-09"]["month_validation_generation_sha256"],
            second_by_month["2026-09"]["month_validation_generation_sha256"],
        )
        self.assertNotEqual(
            first_by_month["2026-10"]["month_validation_generation_sha256"],
            second_by_month["2026-10"]["month_validation_generation_sha256"],
        )
        self.assertEqual(
            first_by_month["2026-10"]["month_result_sha256"],
            second_by_month["2026-10"]["month_result_sha256"],
        )
        self.assertEqual(first_by_month["2026-11"], second_by_month["2026-11"])

    def test_lineage_receipt_binds_its_trusted_pricing_inputs(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )
        validation = publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")
        pricing = build_downstream_pricing_input(self.root, "GOOG")
        lineage = {
            "api_request_count": 1,
            "corporate_actions": [],
            "asset_lookups": [],
            "identities": [],
            "unresolved_cusips": [],
        }
        with patch(
            "libs.alpaca_end_of_day_v6.lineage.build_identity_report",
            return_value=lineage,
        ):
            receipt = acquire_and_publish_lineage(
                self.root,
                "GOOG",
                "2026-10-12T00:00:00Z",
                "key",
                "secret",
                pricing_input=pricing,
                client_factory=lambda *_args, **_kwargs: object(),
            )
        self.assertEqual(pricing.source.source_generation_sha256, receipt["source_generation_sha256"])
        self.assertEqual(
            validation["validation_generation_sha256"],
            receipt["validation_generation_sha256"],
        )
        authenticate_lineage(self.root, "GOOG")

        with patch(
            "libs.alpaca_end_of_day_v6.lineage.sha256_file",
            side_effect=AssertionError("consumer attempted to hash published lineage"),
        ):
            loaded_receipt, loaded_report = load_lineage(self.root, "GOOG")
        self.assertEqual(receipt, loaded_receipt)
        self.assertEqual(lineage, loaded_report["lineage"])

    def test_lineage_retains_an_identical_semantic_publication(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )
        publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")
        pricing = build_downstream_pricing_input(self.root, "GOOG")
        lineage = {
            "api_request_count": 1,
            "corporate_actions": [],
            "asset_lookups": [],
            "identities": [],
            "unresolved_cusips": [],
        }
        with patch(
            "libs.alpaca_end_of_day_v6.lineage.build_identity_report",
            return_value=lineage,
        ):
            first = acquire_and_publish_lineage(
                self.root,
                "GOOG",
                "2026-10-12T00:00:00Z",
                "key",
                "secret",
                pricing_input=pricing,
                client_factory=lambda *_args, **_kwargs: object(),
            )
        before = _files(self.root)

        with (
            patch(
                "libs.alpaca_end_of_day_v6.lineage.build_identity_report",
                return_value=lineage,
            ),
            patch("libs.alpaca_end_of_day_v6.lineage.datetime") as clock,
        ):
            clock.now.return_value = datetime(2099, 1, 1, tzinfo=timezone.utc)
            second = acquire_and_publish_lineage(
                self.root,
                "GOOG",
                "2026-10-12T00:00:00Z",
                "key",
                "secret",
                pricing_input=pricing,
                client_factory=lambda *_args, **_kwargs: object(),
            )

        self.assertEqual(first, second)
        self.assertEqual(before, _files(self.root))

    def test_reusable_publication_uses_receipt_identity_without_auditing_outputs(self):
        pricing = SimpleNamespace(
            source=SimpleNamespace(source_generation_sha256="1" * 64),
            validation_receipt={"validation_generation_sha256": "2" * 64},
        )
        lineage = ({"lineage_generation_sha256": "3" * 64}, {})
        correlation = {
            "source_generation_sha256": "1" * 64,
            "validation_generation_sha256": "2" * 64,
            "lineage_generation_sha256": "3" * 64,
            "correlation_generation_sha256": "4" * 64,
        }
        warning = {
            "source_generation_sha256": "1" * 64,
            "validation_generation_sha256": "2" * 64,
            "correlation_generation_sha256": "4" * 64,
        }
        publication = {
            "identity_candidate": {"candidate_generation_sha256": "5" * 64},
            "availability": {"availability_generation_sha256": "6" * 64},
            "catalogue": {"catalogue_generation_sha256": "7" * 64},
        }
        with (
            patch(
                "libs.alpaca_end_of_day_v6.pipeline.load_correlation_receipt",
                return_value=correlation,
            ),
            patch(
                "libs.alpaca_end_of_day_v6.pipeline.load_warning_receipt",
                return_value=warning,
            ),
            patch(
                "libs.alpaca_end_of_day_v6.pipeline.load_reusable_publication",
                return_value=publication,
            ) as load_publication,
        ):
            reusable = _load_reusable_boundaries(
                self.root,
                ["GOOG"],
                {"GOOG": pricing},
                {"GOOG": lineage},
            )

        self.assertIsNotNone(reusable)
        self.assertEqual(publication["catalogue"], reusable["catalogue"])
        load_publication.assert_called_once_with(self.root, {"GOOG": "4" * 64})

    def test_last_run_record_is_unlinked_operational_bookkeeping(self):
        document = _last_run_document(
            {
                "request_start": "2026-10-01T00:00:00Z",
                "request_end": "2026-10-14T00:00:00Z",
                "submitted_symbols": ["GOOG"],
                "canonical_symbols": ["GOOG"],
                "processing_symbols": ["GOOG"],
                "source": {
                    "GOOG": {
                        "changed_placements": [],
                        "inventory_changed": False,
                    }
                },
                "publication_reused": True,
            }
        )
        self.assertEqual("alpaca_v6_last_completed_run", document["kind"])
        self.assertEqual(0, document["stats"]["changed_month_count"])
        self.assertTrue(document["stats"]["downstream_publication_reused"])
        serialized = json.dumps(document)
        self.assertNotIn("sha256", serialized)
        self.assertNotIn("generation", serialized)
        self.assertNotIn("lineage", serialized)

    def test_availability_reader_reuses_the_loaded_pricing_view(self):
        publish_monthly_source(
            self.root,
            "GOOG",
            self._views([_bar("2026-10-01T04:00:00Z", 10)]),
            "2026-10-12T00:00:00Z",
        )
        publish_validation(self.root, "GOOG", "2026-10-12T00:00:00Z")
        pricing = build_downstream_pricing_input(self.root, "GOOG")
        inputs = PublishedHistoryInputs()
        inputs.seed_pricing_input(pricing)

        for filename, download in pricing.downloads_by_filename.items():
            with patch(
                "libs.alpaca_end_of_day_v6.reader._source_reader",
                side_effect=AssertionError("availability reopened monthly pricing"),
            ):
                rows, manifest, state = inputs.source(
                    self.root,
                    "GOOG",
                    {"data_file": filename},
                )
            self.assertIs(download.rows, rows)
            self.assertIs(download.manifest, manifest)
            self.assertEqual("validated", state)

    def test_publication_marker_survives_failure_and_clears_only_after_success(self):
        initialize_v6_store(self.root)
        marker = self.root / PUBLICATION_PENDING

        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            with publication_transaction(self.root, {"attempt": 1}):
                self.assertEqual({"attempt": 1}, json.loads(marker.read_text()))
                raise RuntimeError("interrupted")

        self.assertTrue(marker.is_file())
        with self.assertRaisesRegex(V6Error, "publication is incomplete"):
            assert_publication_complete(self.root)

        with publication_transaction(self.root, {"attempt": 2}):
            self.assertEqual({"attempt": 2}, json.loads(marker.read_text()))

        self.assertFalse(marker.exists())
        assert_publication_complete(self.root)

    def test_correlation_receipt_owns_the_later_identity_evidence(self):
        report = {
            "symbol": "GOOG",
            "source_generation_sha256": "1" * 64,
            "validation_generation_sha256": "2" * 64,
            "lineage_generation_sha256": "3" * 64,
            "verified_session_exclusions": empty_verified_session_exclusions(self.root, "GOOG"),
            "uuid_lineage_edges": [],
            "ticker_ownership_intervals": [],
            "evidence_issues": [],
            "source_row_count": 0,
            "correlated": {"segment_count": 0, "row_count": 0, "segments": []},
            "uncorrelated": {
                "segment_count": 0,
                "row_count": 0,
                "segments": [],
                "work_item_count": 0,
                "work_items": [],
            },
        }
        receipt = publish_correlation(self.root, report)
        self.assertEqual([], receipt["uuid_lineage_edges"])
        self.assertEqual([], receipt["ticker_ownership_intervals"])
        self.assertEqual([], receipt["evidence_issues"])
        self.assertEqual(report["verified_session_exclusions"], receipt["verified_session_exclusions"])
        authenticate_correlation(self.root, "GOOG")

        with patch(
            "libs.alpaca_end_of_day_v6.correlation.sha256_file",
            side_effect=AssertionError("consumer attempted to hash a published correlation"),
        ):
            loaded = load_correlation(self.root, "GOOG")
        self.assertEqual(
            receipt["correlation_generation_sha256"], loaded.receipt["correlation_generation_sha256"]
        )

    def test_identity_candidate_is_a_validated_restartable_disk_boundary(self):
        candidate = _identity_candidate_from_sources([_candidate_source()])
        receipt = publish_identity_candidate(self.root, candidate)
        output_path = self.root / "identity_candidate.json"
        manifest_path = self.root / "identity_candidate.manifest.json"
        self.assertTrue(output_path.is_file())
        self.assertTrue(manifest_path.is_file())
        self.assertEqual(output_path.stat().st_size, receipt["output"]["bytes"])
        self.assertEqual(
            candidate.generation_sha256, authenticate_identity_candidate(self.root).generation_sha256
        )

        with (
            patch(
                "libs.alpaca_end_of_day_v6.catalogue.sha256_file",
                side_effect=AssertionError("consumer attempted to hash a published candidate"),
            ),
            patch(
                "libs.alpaca_end_of_day_v6.catalogue._aggregate_security_entries",
                side_effect=AssertionError("consumer attempted to rebuild a published candidate"),
            ),
        ):
            loaded = load_identity_candidate(self.root)
        self.assertEqual(candidate.generation_sha256, loaded.generation_sha256)
        self.assertEqual(["GOOG"], list(loaded.master.sources_by_symbol))

        output_path.write_bytes(output_path.read_bytes() + b" ")
        with self.assertRaisesRegex(V6Error, "authentication failed"):
            authenticate_identity_candidate(self.root)

    def test_identity_candidate_receipt_is_written_only_after_output_validation(self):
        candidate = _identity_candidate_from_sources([_candidate_source()])
        with (
            patch(
                "libs.alpaca_end_of_day_v6.catalogue._validate_identity_candidate_output",
                side_effect=V6Error("invalid candidate output"),
            ),
            self.assertRaisesRegex(V6Error, "invalid candidate output"),
        ):
            publish_identity_candidate(self.root, candidate)
        self.assertTrue((self.root / "identity_candidate.json").is_file())
        self.assertFalse((self.root / "identity_candidate.manifest.json").exists())

    def test_affected_catalogue_can_add_a_new_symbol(self):
        initial_candidate = _identity_candidate_from_sources([_candidate_source()])
        publish_identity_candidate(self.root, initial_candidate)
        initial_candidate = load_identity_candidate(self.root)
        publish_availability(
            self.root,
            _availability_report(initial_candidate),
            initial_candidate,
        )
        first_receipt = publish_catalogue(self.root, initial_candidate)

        aapl_candidate = _identity_candidate_from_sources(
            [_candidate_source("AAPL", asset_number=2, correlation_generation="2" * 64)],
            mode="affected_closure",
            base_catalogue_generation_sha256=first_receipt["catalogue_generation_sha256"],
            replacement_symbols=("AAPL",),
        )
        publish_identity_candidate(self.root, aapl_candidate)
        aapl_candidate = load_identity_candidate(self.root)
        publish_availability(
            self.root,
            _availability_report(aapl_candidate),
            aapl_candidate,
        )
        publish_catalogue(self.root, aapl_candidate)

        master = _load_catalogue_store(self.root)
        self.assertEqual({"AAPL", "GOOG"}, set(master.sources_by_symbol))
        self.assertEqual(2, len(master.identities_by_master_id))

    def test_affected_catalogue_update_preserves_unrelated_objects_and_reclaims_by_delta(self):
        from libs.alpaca_end_of_day_v6 import catalogue as catalogue_module

        initial_candidate = _identity_candidate_from_sources(
            [
                _candidate_source("AAPL", asset_number=2, correlation_generation="2" * 64),
                _candidate_source(),
            ]
        )
        publish_identity_candidate(self.root, initial_candidate)
        initial_candidate = load_identity_candidate(self.root)
        publish_availability(
            self.root,
            _availability_report(initial_candidate),
            initial_candidate,
        )
        first_receipt = publish_catalogue(self.root, initial_candidate)
        first_master = _load_catalogue_store(self.root)
        master_directory = security_master_directory(legacy_engine_root(self.root))
        aapl_id = security_id_for_alpaca_asset_ids(["00000000-0000-4000-8000-000000000002"])
        aapl_source_descriptor = first_master.sources_by_symbol.object_descriptor("AAPL")
        aapl_identity_descriptor = first_master.identities_by_master_id.object_descriptor(aapl_id)
        aapl_source_path = master_directory / aapl_source_descriptor["file"]
        aapl_identity_path = master_directory / aapl_identity_descriptor["file"]
        aapl_before = {
            path: (path.stat().st_mtime_ns, path.read_bytes()) for path in (aapl_source_path, aapl_identity_path)
        }
        first_goog_descriptor = first_master.sources_by_symbol.object_descriptor("GOOG")
        first_goog_path = master_directory / first_goog_descriptor["file"]

        second_candidate = _identity_candidate_from_sources(
            [
                _candidate_source(
                    correlation_generation="d" * 64,
                    last_session_date="2026-10-02",
                )
            ],
            mode="affected_closure",
            base_catalogue_generation_sha256=first_receipt["catalogue_generation_sha256"],
            replacement_symbols=("GOOG",),
        )
        publish_identity_candidate(self.root, second_candidate)
        second_candidate = load_identity_candidate(self.root)
        publish_availability(
            self.root,
            _availability_report(second_candidate),
            second_candidate,
        )
        with patch.object(
            catalogue_module,
            "read_json_object",
            wraps=catalogue_module.read_json_object,
        ) as read_json:
            second_receipt = publish_catalogue(self.root, second_candidate)
        paths_read = {Path(call.args[0]) for call in read_json.call_args_list}
        self.assertNotIn(aapl_source_path, paths_read)
        self.assertNotIn(aapl_identity_path, paths_read)

        second_master = _load_catalogue_store(self.root)
        self.assertEqual({"AAPL", "GOOG"}, set(second_master.sources_by_symbol))
        self.assertEqual(2, len(second_master.identities_by_master_id))
        self.assertEqual(aapl_source_descriptor, second_master.sources_by_symbol.object_descriptor("AAPL"))
        self.assertEqual(
            aapl_identity_descriptor,
            second_master.identities_by_master_id.object_descriptor(aapl_id),
        )
        for path, before in aapl_before.items():
            self.assertEqual(before, (path.stat().st_mtime_ns, path.read_bytes()))
        retention = json.loads((self.root / "catalogue" / "retention.json").read_text())
        self.assertIn(first_goog_descriptor["file"], retention["reclaim_after_next_publication"])
        self.assertNotIn(aapl_source_descriptor["file"], retention["reclaim_after_next_publication"])
        self.assertEqual("affected_closure", second_receipt["mode"])
        _authenticate_catalogue_store(self.root)

        third_candidate = _identity_candidate_from_sources(
            [
                _candidate_source(
                    correlation_generation="e" * 64,
                    last_session_date="2026-10-03",
                )
            ],
            mode="affected_closure",
            base_catalogue_generation_sha256=second_receipt["catalogue_generation_sha256"],
            replacement_symbols=("GOOG",),
        )
        publish_identity_candidate(self.root, third_candidate)
        third_candidate = load_identity_candidate(self.root)
        publish_availability(
            self.root,
            _availability_report(third_candidate),
            third_candidate,
        )
        publish_catalogue(self.root, third_candidate)
        self.assertFalse(first_goog_path.exists())
        for path, before in aapl_before.items():
            self.assertEqual(before, (path.stat().st_mtime_ns, path.read_bytes()))
        _authenticate_catalogue_store(self.root)


if __name__ == "__main__":
    unittest.main()

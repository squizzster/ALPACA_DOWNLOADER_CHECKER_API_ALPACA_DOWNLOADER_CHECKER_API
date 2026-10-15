import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS,
    VALIDATOR_FILE,
    VERSION,
    DownloadError,
    digest,
    json_bytes,
    timestamp,
)
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views, request_end
from libs.alpaca_pricing_download.endpoints import endpoints_of
from libs.alpaca_pricing_download.handoff import consume_pricing_update


class PricingValidatorTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch("libs.alpaca_pricing_download.download.utc_today", return_value=date(2030, 1, 1))
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/pricing/GOOG"
        self.rows = [bar("2016-01-04", "100"), bar("2020-01-02", "100"), bar("2026-10-01", "100")]

    def run_download(self, rows=None, **kwargs):
        return download_pricing(
            self.store, "GOOG", "2026-10-14", client=FakeClient(self.rows if rows is None else rows), **kwargs
        )

    def consume(self):
        with consume_pricing_update(self.store, "GOOG"):
            pass

    def assert_validator(self):
        source = storage.load_source(self.root, "GOOG")
        self.assertEqual(
            storage.read_validator(self.root, source), endpoints_of(read_pricing_views(self.store, "GOOG"))
        )

    def test_validator_is_one_exact_source_bound_file(self):
        self.run_download()
        self.assert_validator()
        document = storage.read_document(self.root / VALIDATOR_FILE)
        self.assertEqual(set(document["adjustments"]), set(ADJUSTMENTS))
        self.assertEqual(document["adjustments"]["raw"]["first"][5], "9007199254740993")
        self.assertEqual(len(list(self.root.glob("validator*"))), 1)

    def test_normal_update_never_opens_cold_or_year_gzip(self):
        self.run_download()
        original = Path.read_bytes

        def no_cold(path):
            if path.name.endswith(".gz") and path.parent != self.root / "2026/Oct":
                self.fail(f"ordinary update opened cold payload {path}")
            return original(path)

        with patch.object(Path, "read_bytes", no_cold):
            result = self.run_download(self.rows + [bar("2026-10-13")])
        self.assertEqual(result["mode"], "update")
        self.assert_validator()

    def test_v4_source_and_validator_upgrade_without_bootstrap_or_cold_reads(self):
        self.run_download()
        source = storage.load_source(self.root, "GOOG")
        for entry in source["boundaries"].values():
            path = self.root / entry["manifest"]
            document = storage.read_document(path)
            document["schema_version"] = 4
            path.write_bytes(json_bytes(document))
            entry["sha256"] = digest(json_bytes(document))
        path = self.root / VALIDATOR_FILE
        document = storage.read_document(path)
        document["schema_version"] = 4
        path.write_bytes(json_bytes(document))
        source["schema_version"] = 4
        source["validator"]["sha256"] = digest(json_bytes(document))
        (self.root / "source.manifest.json").write_bytes(json_bytes(source))
        before = snapshot(self.root)
        original = Path.read_bytes

        def no_cold(path):
            if path.name.endswith(".gz") and path.parent != self.root / "2026/Oct":
                self.fail(f"v4 upgrade opened cold payload {path}")
            return original(path)

        with (
            patch.object(Path, "read_bytes", no_cold),
            patch(
                "libs.alpaca_pricing_download.download._stored_endpoints",
                side_effect=AssertionError("bootstrap"),
            ),
        ):
            self.assertEqual(self.run_download()["mode"], "update")
        after = snapshot(self.root)
        for name in ("completed_years.raw.csv.gz", "2026/raw.csv.gz"):
            self.assertEqual(before[name], after[name])
        self.assertEqual(storage.load_source(self.root, "GOOG")["schema_version"], VERSION)
        self.assert_validator()

    def test_legacy_source_bootstraps_once_without_cold_rewrites(self):
        self.run_download()
        source = storage.read_document(self.root / "source.manifest.json")
        source["schema_version"] = 3
        source.pop("validator")
        source.pop("history_start")
        (self.root / "source.manifest.json").write_bytes(json_bytes(source))
        (self.root / VALIDATOR_FILE).unlink()
        cold = (self.root / "completed_years.raw.csv.gz").stat()
        self.run_download()
        self.assert_validator()
        self.assertEqual((self.root / "completed_years.raw.csv.gz").stat().st_ino, cold.st_ino)
        self.assertEqual((self.root / "completed_years.raw.csv.gz").stat().st_mtime_ns, cold.st_mtime_ns)
        with patch(
            "libs.alpaca_pricing_download.download._stored_endpoints",
            side_effect=AssertionError("repeated bootstrap"),
        ):
            self.run_download()

    def test_validator_damage_fails_without_provider_calls(self):
        self.run_download()
        (self.root / VALIDATOR_FILE).write_text("{}")
        client = FakeClient(self.rows)
        with self.assertRaisesRegex(DownloadError, "validator checksum"):
            download_pricing(self.store, "GOOG", "2026-10-14", client=client)
        self.assertFalse(client.calls)

    def test_post_acquisition_probes_only_first_session_in_each_view(self):
        self.run_download()
        client = FakeClient(self.rows)
        download_pricing(self.store, "GOOG", "2026-10-14", client=client)
        self.assertEqual(len(client.calls), 32)
        self.assertEqual({call[2] for call in client.calls[-8:]}, {timestamp("2016-01-04T05:00:00Z")})
        self.assertEqual(set(client.asofs), {"2026-10-14"})

    def test_changed_provider_after_probes_restarts_once_as_full_history(self):
        self.run_download()
        revised = [bar(row["t"][:10], "50") for row in self.rows]
        client = FakeClient(self.rows)
        original = client.fetch

        def flip(symbol, adjustment, start, end, **kwargs):
            if not kwargs.get("asof"):
                client.bars = revised
            return original(symbol, adjustment, start, end, **kwargs)

        with patch.object(client, "fetch", side_effect=flip):
            result = download_pricing(self.store, "GOOG", "2026-10-14", client=client)
        self.assertTrue(result["restarted"])
        self.assertEqual(result["mode"], "full_refresh")
        self.assertTrue(all(row[4] == "50" for row in read_pricing_views(self.store, "GOOG")["raw"]))
        self.assert_validator()

    def test_changed_last_overlap_restarts_even_when_first_still_matches(self):
        self.run_download()
        client = FakeClient(self.rows)
        original = client.fetch

        def change_tail(symbol, adjustment, start, end, **kwargs):
            if not kwargs.get("asof"):
                client.bars = self.rows[:-1] + [bar("2026-10-01", "50")]
            return original(symbol, adjustment, start, end, **kwargs)

        with patch.object(client, "fetch", side_effect=change_tail):
            result = download_pricing(self.store, "GOOG", "2026-10-14", client=client)
        self.assertTrue(result["restarted"])
        self.assertEqual(result["mode"], "full_refresh")

    def test_repeated_instability_fails_without_publishing(self):
        self.run_download()
        before = snapshot(self.store)
        client = FakeClient(self.rows)
        original = client.fetch

        def unstable(symbol, adjustment, start, end, **kwargs):
            rows = original(symbol, adjustment, start, end, **kwargs)
            if len(client.calls) > 24 and kwargs.get("asof"):
                return [bar(row["t"][:10], "999") for row in rows]
            return rows

        with patch.object(client, "fetch", side_effect=unstable), self.assertRaisesRegex(DownloadError, "twice"):
            download_pricing(self.store, "GOOG", "2026-10-14", client=client)
        self.assertEqual(len(client.calls), 48)
        self.assertEqual(snapshot(self.store), before)

    def test_replacement_notice_includes_removed_edges_and_unconsumed_dates(self):
        self.run_download()
        self.consume()
        self.run_download(self.rows[1:2])
        self.assertEqual(storage.read_update(self.root), {"from": "2016-01-04", "to": "2026-10-01"})
        self.run_download(self.rows[1:2], refresh_all=True)
        self.assertEqual(storage.read_update(self.root), {"from": "2016-01-04", "to": "2026-10-01"})
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), 1)
        self.assert_validator()

    def test_force_discards_absent_rows_even_when_provider_probes_would_match(self):
        self.run_download()
        self.consume()
        self.run_download(self.rows[1:2], refresh_all=True)
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), 1)
        self.assertEqual(storage.read_update(self.root), {"from": "2016-01-04", "to": "2026-10-01"})

    def test_force_can_replace_corrupt_owned_gzip_without_decoding_it(self):
        self.run_download()
        (self.root / "completed_years.raw.csv.gz").write_bytes(b"damaged")
        result = self.run_download(refresh_all=True)
        self.assertEqual(result["mode"], "full_refresh")
        self.assert_validator()

    def test_legacy_force_uses_receipt_extent_without_decoding_corrupt_payload(self):
        self.run_download()
        source = storage.read_document(self.root / "source.manifest.json")
        source["schema_version"] = 3
        source.pop("validator")
        source.pop("history_start")
        (self.root / "source.manifest.json").write_bytes(json_bytes(source))
        (self.root / VALIDATOR_FILE).unlink()
        (self.root / "completed_years.raw.csv.gz").write_bytes(b"damaged")
        self.run_download(refresh_all=True)
        self.assert_validator()

    def test_force_retires_damaged_owned_payload_when_it_becomes_an_alias(self):
        client = FakeClient(self.rows)
        original = client.fetch

        def distinct(symbol, adjustment, start, end, **kwargs):
            rows = original(symbol, adjustment, start, end, **kwargs)
            return [bar(row["t"][:10], "999") for row in rows] if adjustment == "dividend" else rows

        with patch.object(client, "fetch", side_effect=distinct):
            download_pricing(self.store, "GOOG", "2026-10-14", client=client)
        obsolete = self.root / "completed_years.dividend.csv.gz"
        obsolete.write_bytes(b"damaged")
        self.run_download(refresh_all=True)
        self.assertFalse(obsolete.exists())
        self.assert_validator()

    def test_force_does_not_stop_after_recovering_previous_request(self):
        self.run_download()
        original = storage.os.replace

        def interrupted(source, target):
            if Path(target) == self.root / "source.manifest.json":
                raise OSError("interrupted")
            return original(source, target)

        with patch.object(storage.os, "replace", side_effect=interrupted), self.assertRaises(OSError):
            self.run_download([bar(row["t"][:10], "50") for row in self.rows], refresh_all=True)
        client = FakeClient([bar(row["t"][:10], "25") for row in self.rows])
        result = download_pricing(self.store, "GOOG", "2026-10-14", client=client, refresh_all=True)
        self.assertTrue(result["recovered"])
        self.assertEqual(len(client.calls), 16)
        self.assertEqual(read_pricing_views(self.store, "GOOG")["raw"][0][4], "25")
        self.assert_validator()

    def test_validator_installation_interruption_recovers_with_source(self):
        self.run_download()
        original = storage.os.replace

        def interrupted(source, target):
            if Path(target) == self.root / VALIDATOR_FILE:
                raise OSError("validator interrupted")
            return original(source, target)

        updated = self.rows + [bar("2026-10-13")]
        with patch.object(storage.os, "replace", side_effect=interrupted), self.assertRaises(OSError):
            self.run_download(updated)
        with patch.object(FakeClient, "fetch", side_effect=AssertionError("recovery fetched")):
            self.assertTrue(self.run_download(updated)["recovered"])
        self.assert_validator()

    def test_from_cannot_truncate_even_on_force(self):
        self.run_download()
        before = snapshot(self.store)
        for forced in (False, True):
            with self.assertRaisesRegex(DownloadError, "exclude previously stored"):
                self.run_download(from_date="2020-01-01", refresh_all=forced)
        self.assertEqual(snapshot(self.store), before)

    def test_custom_from_persists_and_earlier_from_backfills(self):
        self.run_download(from_date="2020-01-01")
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), 2)
        self.assertEqual(self.run_download(refresh_all=True)["request"]["start"], "2020-01-01T00:00:00Z")
        result = self.run_download(from_date="2016-01-01")
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), 3)

    def test_later_from_is_accepted_only_when_no_stored_observation_precedes_it(self):
        rows = [bar("2026-10-01")]
        self.run_download(rows)
        self.run_download(rows, from_date="2020-01-01")
        source = storage.load_source(self.root, "GOOG")
        self.assertEqual(source["history_start"], "2020-01-01T00:00:00Z")
        self.assertEqual(self.run_download(rows, refresh_all=True)["request"]["start"], "2020-01-01T00:00:00Z")

    def test_dates_are_strict_and_future_dates_fail_before_store_creation(self):
        for end in ("2031-01-01", "20261014", "2026-W42-3", "2026-02-30"):
            with self.subTest(end=end), self.assertRaises(DownloadError):
                download_pricing(self.store, "GOOG", end, client=FakeClient(self.rows))
        self.assertFalse(self.store.exists())
        self.assertEqual(request_end(None).date(), date(2030, 1, 1))

    def test_interrupted_store_initialization_retries_without_extra_durable_files(self):
        original = storage.write_file

        def interrupted(path, data):
            if path.name == ".store.manifest.pending":
                original(path, data[:10])
                raise OSError("initialization interrupted")
            return original(path, data)

        with patch.object(storage, "write_file", side_effect=interrupted), self.assertRaises(OSError):
            storage.initialize_store(self.store)
        self.assertFalse((self.store / "store.manifest.json").exists())
        storage.initialize_store(self.store)
        self.assertEqual([p.name for p in self.store.iterdir()], ["store.manifest.json"])

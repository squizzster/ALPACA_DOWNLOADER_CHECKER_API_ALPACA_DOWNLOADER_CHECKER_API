"""A batch consumes approved facts once and retains only freshness references."""

import fcntl
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_standalone_reader as fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import bar, snapshot

from alpaca_end_of_day import ReaderBusyError, StandaloneReader, StandaloneReaderError


class StandaloneGroupTests(unittest.TestCase):
    setUp = fixtures.StandaloneReaderTests.setUp
    download = fixtures.StandaloneReaderTests.download
    build = fixtures.StandaloneReaderTests.build
    publication = fixtures.StandaloneReaderTests.publication
    change_publication = fixtures.StandaloneReaderTests.change_publication
    renamed_pair = fixtures.StandaloneReaderTests.renamed_pair

    def test_group_cache_limit_respects_configured_cap_and_preserves_values(self):
        reader = StandaloneReader(self.store, cache_bytes=4 * 1024**2)
        with reader.open_group("TEST") as group:
            identifier = group.summary["securities"][0]["public_id"]
            expected = group.get_histories(public_id=identifier)
            group.set_cache_limit(0)
            self.assertEqual(group._cache.used_bytes, 0)
            self.assertEqual(group.get_histories(public_id=identifier), expected)
            group.set_cache_limit(1024**2)
            self.assertEqual(group.get_histories(public_id=identifier), expected)
            for value in (-1, True, None, 4 * 1024**2 + 1):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    group.set_cache_limit(value)
        with self.assertRaisesRegex(StandaloneReaderError, "closed"):
            group.set_cache_limit(0)
        self.assertEqual(reader.cache_info()["maximum_bytes"], 4 * 1024**2)

    def test_compact_summary_and_histories_equal_existing_reader_without_full_evidence(self):
        evidence = self.reader.get_evidence("TEST")
        identifier = evidence["securities"][0]["public_id"]
        expected = self.reader.get_histories("TEST", public_id=identifier)
        before = snapshot(self.store)
        opened, original = [], Path.read_bytes

        def read(path):
            opened.append(path)
            return original(path)

        with (
            patch.object(Path, "read_bytes", read),
            patch("alpaca_end_of_day.standalone._group_evidence", side_effect=AssertionError("full projection")),
            self.reader.open_group("TEST") as group,
        ):
            summary = group.summary
            actual = group.get_histories(public_id=identifier)
        for key in expected:
            self.assertEqual(actual[key], expected[key])
        self.assertEqual(summary["securities"], evidence["securities"])
        self.assertEqual(summary["generation"], evidence["generation"])
        self.assertNotIn("evidence", summary["sources"]["TEST"])
        self.assertNotIn("assignments", summary["sources"]["TEST"])
        self.assertEqual(sum(p.name == "lineage.manifest.json" for p in opened), 1)
        self.assertEqual(snapshot(self.store), before)

    def test_two_ids_share_payloads_and_one_snapshot_then_release_working_data(self):
        def change(_, boundaries):
            for entry in boundaries["current_month"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["security_identity"]["security_id"] = "a" * 64

        self.change_publication(change)
        reader = StandaloneReader(self.store, cache_bytes=4 * 1024**2)
        opened, original = [], Path.read_bytes

        def read(path):
            opened.append(path)
            return original(path)

        with (
            patch.object(Path, "read_bytes", read),
            patch.object(reader, "_snapshot", wraps=reader._snapshot) as selected,
            reader.open_group("TEST") as group,
        ):
            ids = [s["public_id"] for s in group.summary["securities"]]
            self.assertEqual(len(ids), 2)
            histories = [group.get_histories(public_id=identifier) for identifier in ids]
        self.assertEqual(sum(h["views"]["raw"]["bar_count"] for h in histories), len(self.bars))
        self.assertEqual(sum(p.suffix == ".gz" for p in opened), 3)
        selected.assert_called_once_with("TEST", reuse_documents=True)
        self.assertIsNone(group._cache)
        self.assertIsNone(group._assignments)
        self.assertIsNone(group._sources)
        with self.assertRaisesRegex(StandaloneReaderError, "closed"):
            group.get_histories(public_id=ids[0])

    def test_mutating_public_summary_or_history_cannot_change_snapshot(self):
        with self.reader.open_group("TEST") as group:
            expected = group.summary
            identifier = expected["securities"][0]["public_id"]
            summary = group.summary
            summary["securities"][0]["latest_tickers"].append("MUTATED")
            summary["sources"]["TEST"]["pricing_request"]["asof"] = "2000-01-01"
            first = group.get_histories(public_id=identifier)
            first["evidence"]["securities"][0]["tickers"].append("MUTATED")
            first["views"]["raw"]["bars"][0]["close"] = "9999"
            self.assertEqual(group.summary, expected)
            self.assertEqual(
                group.get_histories(public_id=identifier)["views"], self.reader.get_histories("TEST")["views"]
            )
        group.check_current()

    def test_freshness_after_close_never_opens_lineage_receipts_boundaries_or_prices(self):
        with self.reader.open_group("TEST") as group:
            pass
        original = Path.read_bytes

        def read(path):
            self.assertNotEqual(path.suffix, ".gz")
            self.assertNotIn("lineage", path.parts)
            return original(path)

        with (
            patch.object(Path, "read_bytes", read),
            patch("libs.alpaca_lineage_build.storage.read_receipt", side_effect=AssertionError("receipt")),
            patch("libs.alpaca_lineage_build.storage.read_boundaries", side_effect=AssertionError("boundary")),
        ):
            group.check_current()

    def test_changed_pricing_or_lineage_invalidates_the_retained_token(self):
        with self.reader.open_group("TEST") as group:
            pass
        self.download("TEST", self.bars + [bar("2026-10-14")], end="2026-10-15")
        with self.assertRaisesRegex(StandaloneReaderError, "pricing group changed"):
            group.check_current()
        self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        with self.assertRaisesRegex(StandaloneReaderError, "lineage group changed"):
            group.check_current()

    def test_unchanged_source_is_reused_without_rereading_approved_receipt(self):
        with self.reader.open_group("TEST") as group:
            pass
        with patch(
            "alpaca_end_of_day._standalone_group.load_source", side_effect=AssertionError("receipt reread")
        ):
            group.check_current()
        self.assertEqual(self.reader.cache_info()["ledger"]["source_receipt_reuses"], 1)

    def test_snapshot_holds_pricing_lock_and_final_check_respects_producer_lock(self):
        path = self.store / "end_of_day/pricing/TEST/.download.lock"
        with self.reader.open_group("TEST") as group:
            with path.open("rb") as lock:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        descriptor = os.open(path, os.O_RDONLY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            with self.assertRaises(ReaderBusyError):
                group.check_current()
            with self.assertRaises(ReaderBusyError):
                with self.reader.open_group("TEST"):
                    self.fail("entered locked group")
        finally:
            os.close(descriptor)

    def test_consumer_exception_preserves_identity_and_type_after_releasing_locks(self):
        error = ValueError("downstream publication failed")
        with self.assertRaises(ValueError) as raised:
            with self.reader.open_group("TEST"):
                raise error
        self.assertIs(raised.exception, error)
        with self.reader.open_group("TEST") as group:
            group.check_current()

    def test_group_preserves_existing_overlap_policy(self):
        self.renamed_pair(differing=True)
        expected = self.reader.get_histories("NEW")
        with self.reader.open_group("OLD") as group:
            actual = group.get_histories(public_id=expected["security"]["public_id"])
        self.assertEqual(actual["views"], expected["views"])

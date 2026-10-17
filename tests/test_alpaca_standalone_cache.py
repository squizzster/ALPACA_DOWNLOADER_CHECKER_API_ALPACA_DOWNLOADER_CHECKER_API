"""RAM reuse must never become a substitute for fresh publication selection."""

import fcntl
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_standalone_reader as fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import bar, snapshot

from alpaca_end_of_day import ReaderBusyError, StandaloneReader, StandaloneReaderError
from alpaca_end_of_day._standalone_cache import GenerationCache, _rows_size, _size


class CacheBudgetTests(unittest.TestCase):
    def test_resize_evicts_only_when_needed_and_keeps_accounting_bounded(self):
        cache = GenerationCache(4000)
        cache._load(("first",), lambda: b"a" * 300)
        cache._load(("second",), lambda: b"b" * 300)
        cache.resize(1300)
        self.assertLessEqual(cache.used_bytes, 1300)
        self.assertEqual(cache.info()["entries"], 1)
        self.assertEqual(cache.ledger.info()["evictions"], 1)
        cache.resize(0)
        self.assertEqual(cache.used_bytes, 0)
        self.assertEqual(cache.info()["entries"], 0)
        for value in (-1, True, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cache.resize(value)

    def test_lru_eviction_oversize_and_disabled_cache(self):
        cache = GenerationCache(1300)
        calls = []

        def load(key, value=b"a" * 300):
            def factory():
                calls.append(key)
                return value

            return cache._load((key,), factory)

        self.assertEqual(load("first"), b"a" * 300)
        load("first")
        self.assertEqual(calls, ["first"])
        load("second")
        self.assertEqual(cache.info()["entries"], 1)
        load("oversize", b"z" * 2000)
        self.assertEqual(cache.info()["entries"], 1)
        load("second")
        self.assertEqual(calls.count("second"), 1)
        load("first")
        self.assertEqual(calls.count("first"), 2)
        self.assertLessEqual(cache.info()["used_bytes"], 1300)
        disabled = GenerationCache(0)
        disabled._load(("unused",), lambda: b"abc")
        self.assertEqual(disabled.info()["used_bytes"], 0)

    def test_invalid_memory_budgets_are_rejected_before_reads(self):
        for value in (-1, None, True, 1.5, "2048"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "cache_bytes"):
                StandaloneReader("/nonexistent", cache_bytes=value)

    def test_fast_row_accounting_conservatively_bounds_unicode_and_shared_values(self):
        for text in ("", "123.456", "é", "\U0010ffff", "x" * 10000):
            rows = tuple(tuple(text for _ in range(9)) for _ in range(12))
            with self.subTest(text=text[:12]):
                self.assertGreaterEqual(_rows_size(rows), _size(rows))


class StandaloneCacheTests(unittest.TestCase):
    setUp = fixtures.StandaloneReaderTests.setUp
    download = fixtures.StandaloneReaderTests.download
    build = fixtures.StandaloneReaderTests.build
    publication = fixtures.StandaloneReaderTests.publication
    change_publication = fixtures.StandaloneReaderTests.change_publication
    renamed_pair = fixtures.StandaloneReaderTests.renamed_pair

    def cached(self):
        return StandaloneReader(self.store, cache_bytes=4 * 1024**2)

    def test_repeated_reads_equal_uncached_and_reuse_physical_aliases(self):
        expected = self.reader.get_histories("TEST", include_evidence=True)
        cached = self.cached()
        before = snapshot(self.store)
        opened = []
        original = Path.read_bytes

        def read(path):
            opened.append(path)
            return original(path)

        with patch.object(Path, "read_bytes", read):
            first = cached.get_histories("TEST", include_evidence=True)
            first_gz = [path for path in opened if path.suffix == ".gz"]
            opened.clear()
            second = cached.get_histories("TEST", include_evidence=True)
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        self.assertEqual(len(first_gz), 3)
        self.assertFalse(any(path.suffix == ".gz" for path in opened))
        self.assertFalse(any(path.name in {"year.manifest.json", "month.manifest.json"} for path in opened))
        self.assertIn(self.store / "end_of_day/pricing/TEST/source.manifest.json", opened)
        self.assertEqual(snapshot(self.store), before)
        self.assertGreater(cached.cache_info()["hits"], 0)
        self.assertLessEqual(cached.cache_info()["used_bytes"], 4 * 1024**2)

    def test_mutating_returned_evidence_and_rows_does_not_poison_cache(self):
        cached = self.cached()
        expected = self.reader.get_histories("TEST", include_evidence=True)
        result = cached.get_histories("TEST", include_evidence=True)
        result["evidence"]["sources"]["TEST"]["evidence"]["warnings"].append("caller mutation")
        result["views"]["raw"]["bars"][0]["close"] = "999"
        self.assertEqual(cached.get_histories("TEST", include_evidence=True), expected)

    def test_changed_lineage_generation_loads_fresh_metadata(self):
        cached = self.cached()
        old = cached.get_evidence("TEST")
        self.change_publication(lambda receipt, _: receipt["evidence"]["warnings"].append("new generation"))
        current = cached.get_evidence("TEST")
        self.assertNotEqual(current["generation"], old["generation"])
        self.assertEqual(current, self.reader.get_evidence("TEST"))

    def test_pricing_change_still_blocks_cached_reads_until_lineage_publishes(self):
        cached = self.cached()
        old = cached.get_histories("TEST")
        self.download("TEST", self.bars + [bar("2026-10-14")], end="2026-10-15")
        for method in (cached.get_histories, cached.get_generation):
            with self.assertRaisesRegex(StandaloneReaderError, "changed pricing"):
                method("TEST")
        self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        current = cached.get_histories("TEST")
        self.assertEqual(current, self.reader.get_histories("TEST"))
        self.assertNotEqual(current["generation"], old["generation"])
        self.assertEqual(current["views"]["raw"]["bars"][-1]["session"], "2026-10-14")

    def test_lightweight_generation_checks_do_not_load_assignments_or_prices(self):
        expected = self.reader.get_evidence("TEST")["generation"]
        for cached in (self.reader, self.cached()):
            with (
                patch(
                    "libs.alpaca_lineage_build.storage.read_boundaries", side_effect=AssertionError("boundary")
                ),
                patch("alpaca_end_of_day.standalone._group_evidence", side_effect=AssertionError("evidence")),
                patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("prices")),
            ):
                self.assertEqual(cached.get_generation("TEST"), expected)

    def test_warm_cache_and_lightweight_check_respect_live_locks(self):
        cached = self.cached()
        cached.get_histories("TEST")
        for path, directory in (
            (self.root, True),
            (self.store / "end_of_day/pricing/TEST/.download.lock", False),
        ):
            descriptor = os.open(path, os.O_RDONLY | (os.O_DIRECTORY if directory else 0))
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                for method in (cached.get_histories, cached.get_generation):
                    with self.assertRaises(ReaderBusyError):
                        method("TEST")
            finally:
                os.close(descriptor)

    def test_warm_cache_rejects_missing_or_symlinked_payload(self):
        cached = self.cached()
        cached.get_histories("TEST")
        payload = next((self.store / "end_of_day/pricing/TEST").glob("*.csv.gz"))
        moved = payload.with_suffix(".retained")
        payload.rename(moved)
        with self.assertRaises(StandaloneReaderError):
            cached.get_histories("TEST")
        payload.symlink_to(moved)
        with self.assertRaisesRegex(StandaloneReaderError, "symlink"):
            cached.get_histories("TEST")

    def test_cached_reader_preserves_overlap_source_policy(self):
        self.renamed_pair(differing=True)
        cached = self.cached()
        expected = self.reader.get_histories("NEW", include_evidence=True)
        self.assertEqual(cached.get_histories("NEW", include_evidence=True), expected)
        self.assertEqual(cached.get_histories("NEW", include_evidence=True), expected)

    def test_exact_view_aliases_reuse_assembly_without_mutable_results_or_raw_policy_leak(self):
        from alpaca_end_of_day import standalone

        self.renamed_pair()
        with patch.object(
            standalone, "_resolve_bar_candidates", wraps=standalone._resolve_bar_candidates
        ) as resolve:
            result = self.cached().get_histories("NEW")
        # Eight aliased views: one raw policy and one adjusted policy assembly.
        self.assertEqual(resolve.call_count, 2)
        self.assertEqual(
            [call.kwargs["allow_strict_superset_selection"] for call in resolve.call_args_list], [False, True]
        )
        expected_tickers = list(result["views"]["all"]["bars"][0]["tickers"])
        result["views"]["split"]["bars"][0]["close"] = "caller mutation"
        result["views"]["split"]["bars"][0]["tickers"].append("caller mutation")
        self.assertNotEqual(result["views"]["all"]["bars"][0]["close"], "caller mutation")
        self.assertEqual(result["views"]["all"]["bars"][0]["tickers"], expected_tickers)

    def test_same_payload_but_different_dated_label_does_not_reuse_assembly(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    if entry["adjustment"] == "all":
                        for segment in entry["segments"]:
                            segment["historical_symbol"] = "OTHER"

        self.change_publication(change)
        result = self.cached().get_histories("TEST")
        self.assertEqual(result["views"]["split"]["bars"][0]["tickers"], ["TEST"])
        self.assertEqual(result["views"]["all"]["bars"][0]["tickers"], ["OTHER"])

    def test_cached_and_uncached_parquet_bytes_match(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("install requirements-bloodhound.txt for Parquet integration tests")
        from libs.alpaca_bloodhound_export.command_line import export

        output = self.store.parent / "cache-equivalence.parquet"
        export(self.reader, output)
        expected = output.read_bytes()
        export(self.cached(), output, replace=True)
        self.assertEqual(output.read_bytes(), expected)

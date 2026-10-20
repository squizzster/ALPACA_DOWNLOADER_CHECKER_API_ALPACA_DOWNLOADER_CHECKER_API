"""Buffered acquisition commits only against the source selected before fetching."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import acquire, storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient


class AcquisitionPublicationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        self.client = BarsClient("test", "test")
        self.pages = []
        self.rows = [bar(day, "50") for day in ("2016-01-04", "2026-10-14", "2026-10-16")]
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def page(self, params):
        self.pages.append(dict(params))
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                symbol: [row for row in self.rows if start <= timestamp(row["t"]) <= end]
                for symbol in params["symbols"].split(",")
            }
        }

    def seed(self, store, mode):
        if mode != "initial_absence":
            download_pricing(
                store,
                "AAA",
                "2026-10-17" if mode == "same_span" else "2026-10-15",
                client=FakeClient([bar(row["t"][:10], "100") for row in self.rows]),
            )

    def acquire(self, store, mode):
        return acquire.acquire_selected(
            store,
            ["AAA"],
            "2026-10-17",
            client=self.client,
            invalidate=() if mode == "initial_absence" else {"AAA"},
        )

    def test_concurrent_publication_survives_all_buffered_acquisition_modes(self):
        original = acquire.acquire_full
        for mode in ("initial_absence", "same_span", "advancing_replacement"):
            with self.subTest(mode=mode):
                store = self.directory / mode
                self.seed(store, mode)
                concurrent = {}

                def publish_after_fetch(*args, **kwargs):
                    buffered = original(*args, **kwargs)
                    download_pricing(
                        store,
                        "AAA",
                        "2026-10-17" if mode == "same_span" else "2026-10-16",
                        client=FakeClient([bar(row["t"][:10], "75") for row in self.rows]),
                        refresh_all=True,
                    )
                    concurrent["files"] = snapshot(store)
                    concurrent["pages"] = len(self.pages)
                    return buffered

                with patch.object(acquire, "acquire_full", side_effect=publish_after_fetch):
                    result = self.acquire(store, mode)
                self.assertEqual(result["failed"], 1, result)
                self.assertIn("changed after acquisition selection", result["results"][0]["outcome"]["error"])
                self.assertEqual(snapshot(store), concurrent["files"])
                self.assertEqual(len(self.pages), concurrent["pages"])
                self.assertEqual(read_pricing_views(store, "AAA")["all"][0][4], "75")

    def test_uncontended_acquisition_consumes_buffer_once_in_all_modes(self):
        for mode in ("initial_absence", "same_span", "advancing_replacement"):
            with self.subTest(mode=mode):
                store = self.directory / mode
                self.seed(store, mode)
                self.pages.clear()
                result = self.acquire(store, mode)
                self.assertEqual(result["failed"], 0, result)
                self.assertEqual(len(self.pages), 16)  # Eight full views and their producer probes.
                for rows in read_pricing_views(store, "AAA").values():
                    self.assertEqual([row[4] for row in rows], ["50"] * 3)
                source = storage.load_source(store / "end_of_day/pricing/AAA", "AAA")
                self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-17T00:00:00Z")
                self.assertEqual({page["adjustment"] for page in self.pages}, set(ADJUSTMENTS))

    def test_exact_fallback_also_rejects_a_changed_source_before_fetching(self):
        store = self.directory / "fallback"

        def rejected_batch(*_args, **_kwargs):
            download_pricing(store, "AAA", "2026-10-16", client=FakeClient([bar("2016-01-04", "75")]))
            raise HTTPError("https://example.invalid", 400, "batch rejected", {}, None)

        with patch.object(acquire, "acquire_full", side_effect=rejected_batch):
            result = self.acquire(store, "initial_absence")
        self.assertEqual(result["failed"], 1)
        self.assertIn("changed after acquisition selection", result["results"][0]["outcome"]["error"])
        self.assertEqual(self.pages, [])
        self.assertEqual(read_pricing_views(store, "AAA")["all"][0][4], "75")

    def test_concurrent_journal_recovers_before_stale_selection_is_rejected(self):
        store = self.directory / "recovery"
        self.seed(store, "same_span")
        original = acquire.acquire_full

        def interrupt_after_fetch(*args, **kwargs):
            buffered = original(*args, **kwargs)
            with patch.object(storage, "_finish_publication", side_effect=OSError("publication interrupted")):
                with self.assertRaisesRegex(OSError, "publication interrupted"):
                    download_pricing(
                        store,
                        "AAA",
                        "2026-10-17",
                        client=FakeClient([bar(row["t"][:10], "75") for row in self.rows]),
                        refresh_all=True,
                    )
            self.assertTrue((store / "end_of_day/pricing/AAA/.pending.json").exists())
            return buffered

        with patch.object(acquire, "acquire_full", side_effect=interrupt_after_fetch):
            result = self.acquire(store, "same_span")
        self.assertEqual(result["failed"], 1)
        self.assertIn("changed after acquisition selection", result["results"][0]["outcome"]["error"])
        self.assertFalse((store / "end_of_day/pricing/AAA/.pending.json").exists())
        self.assertEqual([row[4] for row in read_pricing_views(store, "AAA")["all"]], ["75"] * 3)
        self.assertEqual(len(self.pages), 16)


if __name__ == "__main__":
    unittest.main()

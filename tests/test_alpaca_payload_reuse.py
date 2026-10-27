"""History replacement trusts recorded payload digests, including during recovery."""

import json
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_pricing_download import bar

from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.refresh import prepare_refresh, replace_prepared


class PayloadReuseTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/pricing/AAA"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        self.rows = {
            adjustment: [bar(day, "100") for day in ("2016-01-04", "2026-01-02", "2026-10-01")]
            for adjustment in ADJUSTMENTS
        }
        self.rows["all"][-1] = bar("2026-10-01", "50")
        self.client = BarsClient("test", "test")
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))
        self.initial = download_pricing(self.store, "AAA", "2026-10-14", client=self.client)
        source = storage.load_source(self.root, "AAA")
        self.payloads = {
            self.root / relative
            for relative in storage.owned_files(self.root, source)
            if relative.endswith(".csv.gz")
        }

    def page(self, params):
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                "AAA": [row for row in self.rows[params["adjustment"]] if start <= timestamp(row["t"]) <= end]
            }
        }

    def refresh(self):
        return replace_prepared(self.store, prepare_refresh(self.store, ["AAA"]), client=self.client)

    @staticmethod
    def snapshot(paths):
        return {path: (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) for path in paths}

    @contextmanager
    def protect_payloads(self, paths):
        original_open, original_stat, original_replace = Path.open, Path.stat, storage.os.replace

        def open_file(path, *args, **kwargs):
            self.assertNotIn(path, paths, "replacement reopened an unchanged payload")
            return original_open(path, *args, **kwargs)

        def stat_file(path, *args, **kwargs):
            self.assertNotIn(path, paths, "replacement checked an unchanged payload")
            return original_stat(path, *args, **kwargs)

        def replace_file(source, target):
            self.assertNotIn(Path(target), paths, "replacement rewrote an unchanged payload")
            return original_replace(source, target)

        with (
            patch.object(Path, "open", open_file),
            patch.object(Path, "stat", stat_file),
            patch.object(storage.os, "replace", replace_file),
        ):
            yield

    def test_identical_replacement_preserves_payloads_without_reopening_them(self):
        before = self.snapshot(self.payloads)
        with self.protect_payloads(self.payloads):
            result = self.refresh()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["files_written"], 0)
        self.assertEqual(result["results"][0]["source_sha256"], self.initial["source_sha256"])
        self.assertEqual(self.snapshot(self.payloads), before)

    def test_changed_view_replaces_only_its_changed_payloads(self):
        self.check_partial_replacement(interrupt=False)

    def test_interrupted_replacement_recovers_without_touching_reused_payloads(self):
        self.check_partial_replacement(interrupt=True)

    def check_partial_replacement(self, *, interrupt):
        self.rows["all"][-1] = bar("2026-10-01", "75")
        changed = {self.root / "2026/all.csv.gz", self.root / "2026/Oct/all.csv.gz"}
        retained = self.payloads - changed
        before = self.snapshot(self.payloads)
        self.assertEqual(len(retained), 3)
        with self.protect_payloads(retained):
            if interrupt:
                original_replace = storage.os.replace

                def stop_before_selector(source, target):
                    if Path(target) == self.root / "source.manifest.json":
                        raise OSError("interrupted before selector")
                    return original_replace(source, target)

                with patch.object(storage.os, "replace", stop_before_selector):
                    result = self.refresh()
                self.assertEqual(result["failed"], 1, result)
                journal = json.loads((self.root / ".pending.json").read_bytes())
                self.assertEqual(
                    {self.root / item["path"] for item in journal["writes"] if item["path"].endswith(".csv.gz")},
                    changed,
                )
                with storage.ticker_lock(self.root, exclusive=True):
                    self.assertTrue(storage.recover(self.root))
            else:
                result = self.refresh()
                self.assertEqual(result["failed"], 0, result)
        after = self.snapshot(self.payloads)
        for path in retained:
            self.assertEqual(after[path], before[path], path)
        for path in changed:
            self.assertNotEqual(after[path][0], before[path][0], path)
            self.assertNotEqual(after[path][1], before[path][1], path)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assertFalse((self.root / ".staging").exists())
        views = read_pricing_views(self.store, "AAA")
        self.assertEqual(views["all"][-1][4], "75")
        self.assertEqual(views["raw"][-1][4], "100")
        self.assertEqual(storage.read_update(self.root), {"from": "2016-01-04", "to": "2026-10-01"})

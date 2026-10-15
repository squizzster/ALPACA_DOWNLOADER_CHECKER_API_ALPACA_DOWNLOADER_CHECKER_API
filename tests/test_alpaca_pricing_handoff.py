import multiprocessing
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.handoff import consume_pricing_update


def download_worker(store, ready, entered, release):
    class BlockingClient(FakeClient):
        def fetch(self, *args, **kwargs):
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test downloader release timed out")
            return super().fetch(*args, **kwargs)

    ready.set()
    with patch("libs.alpaca_pricing_download.download.utc_today", return_value=date(2030, 1, 1)):
        download_pricing(
            store, "GOOG", "2026-10-15", client=BlockingClient([bar("2026-10-01"), bar("2026-10-14")])
        )


def consume_worker(store, ready, entered, release):
    ready.set()
    with consume_pricing_update(store, "GOOG") as batch:
        entered.set()
        if not release.wait(10):
            raise RuntimeError("test consumer release timed out")
        if batch is not None:
            batch.read_views()


class PricingHandoffTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch("libs.alpaca_pricing_download.download.utc_today", return_value=date(2030, 1, 1))
        )
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "store"
        self.root = self.store / "end_of_day/pricing/GOOG"
        self.notice = self.store / "end_of_day/manifests/GOOG/update.json"
        self.bars = [bar("2016-01-04"), bar("2026-10-01")]

    def download(self, end="2026-10-14", bars=None, **kwargs):
        return download_pricing(
            self.store, "GOOG", end, client=FakeClient(self.bars if bars is None else bars), **kwargs
        )

    def consume(self):
        with consume_pricing_update(self.store, "GOOG") as batch:
            self.assertIsNotNone(batch)
            return dict(batch.dates)

    def test_initial_notice_is_only_two_dates_and_success_removes_directory(self):
        self.download()
        self.assertFalse((self.store / "end_of_day/GOOG").exists())
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-10-01"})
        with consume_pricing_update(self.store, "GOOG") as batch:
            self.assertEqual(batch.dates, {"from": "2016-01-04", "to": "2026-10-01"})
            self.assertEqual(len(batch.read_views()["raw"]), 2)
            self.assertTrue(self.notice.exists())
        self.assertFalse(self.notice.parent.exists())
        self.assertTrue((self.root / ".download.lock").exists())
        with self.assertRaisesRegex(DownloadError, "no longer locked"):
            batch.read_views()

    def test_pending_range_extends_to_without_moving_from_or_adding_files(self):
        self.download()
        updated = self.bars + [bar("2026-10-14")]
        self.download("2026-10-15", updated)
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-10-14"})
        self.download("2026-11-03", updated + [bar("2026-11-02")])
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-11-02"})
        self.assertEqual([path.name for path in self.notice.parent.iterdir()], ["update.json"])

    def test_after_consumption_only_new_sessions_start_the_next_pending_range(self):
        self.download()
        self.consume()
        updated = self.bars + [bar("2026-10-14")]
        self.download("2026-10-15", updated)
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-14", "to": "2026-10-14"})
        self.download("2026-11-03", updated + [bar("2026-11-02")])
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-14", "to": "2026-11-02"})

    def test_after_consumption_new_manifest_excludes_unchanged_later_sessions(self):
        rows = self.bars + [bar("2026-10-05"), bar("2026-10-13")]
        self.download(bars=rows)
        self.consume()
        revised = rows[:2] + [bar("2026-10-05", "999")] + rows[3:]
        result = self.download(bars=revised)
        self.assertEqual(result["mode"], "update")
        self.assertEqual(result["consistency_check"], "matched")
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-05", "to": "2026-10-05"})

    def test_new_notice_spans_only_actual_changes_not_the_requested_month(self):
        rows = self.bars + [bar("2026-10-05"), bar("2026-10-07"), bar("2026-10-13")]
        self.download(bars=rows)
        self.consume()
        revised = rows[:2] + [bar("2026-10-05", "999"), bar("2026-10-07", "888")] + rows[4:]
        self.download(bars=revised)
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-05", "to": "2026-10-07"})

    def test_full_refresh_replaces_pending_incremental_range_with_all_history(self):
        self.download()
        self.consume()
        updated = self.bars + [bar("2026-10-14")]
        self.download("2026-10-15", updated)
        revised = [bar("2016-01-04", "999")] + updated[1:] + [bar("2026-10-15")]
        result = self.download("2026-10-16", revised)
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-10-15"})

    def test_explicit_refresh_notifies_even_if_all_replacement_bytes_match(self):
        self.download()
        self.consume()
        result = self.download(refresh_all=True)
        self.assertEqual(result["mode"], "full_refresh")
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-10-01"})

    def test_unchanged_update_does_not_recreate_consumed_manifest_directory(self):
        self.download()
        self.consume()
        self.download("2026-10-15")
        self.assertFalse(self.notice.parent.exists())
        with consume_pricing_update(self.store, "GOOG") as batch:
            self.assertIsNone(batch)
        self.assertFalse(self.notice.parent.exists())

    def test_unchanged_update_preserves_unconsumed_manifest_bytes_and_inode(self):
        self.download()
        before = snapshot(self.notice.parent)
        self.download("2026-10-15")
        self.assertEqual(before, snapshot(self.notice.parent))

    def test_empty_initial_source_and_unknown_consumer_create_no_notice_directory(self):
        self.download(bars=[])
        self.assertFalse((self.store / "end_of_day/manifests").exists())
        with consume_pricing_update(self.store, "OTHER") as batch:
            self.assertIsNone(batch)
        self.assertFalse((self.store / "end_of_day/pricing/OTHER").exists())
        self.assertFalse((self.store / "end_of_day/manifests").exists())

    def test_failed_consumer_leaves_original_notice_untouched(self):
        self.download()
        before = snapshot(self.store)
        with self.assertRaisesRegex(RuntimeError, "processing failed"):
            with consume_pricing_update(self.store, "GOOG") as batch:
                batch.read_views()
                raise RuntimeError("processing failed")
        self.assertEqual(before, snapshot(self.store))

    def test_failed_download_preserves_pending_work(self):
        self.download()
        before = snapshot(self.store)
        with patch.object(FakeClient, "fetch", side_effect=DownloadError("network failed")):
            with self.assertRaisesRegex(DownloadError, "network failed"):
                self.download("2026-10-15")
        self.assertEqual(before, snapshot(self.store))

    def test_staging_failure_creates_no_manifest_directory(self):
        self.download()
        self.consume()
        before = snapshot(self.store)
        with patch.object(storage, "write_file", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                self.download("2026-10-15", self.bars + [bar("2026-10-14")])
        self.assertEqual(before, snapshot(self.store))
        self.assertFalse(self.notice.parent.exists())

    def test_notice_publication_failure_is_recovered_without_download_or_lost_range(self):
        self.download()
        self.consume()
        original_replace = storage.os.replace

        def interrupt(source, target):
            if Path(target) == self.notice:
                raise OSError("notice interrupted")
            return original_replace(source, target)

        with patch.object(storage.os, "replace", interrupt):
            with self.assertRaisesRegex(OSError, "notice interrupted"):
                self.download("2026-10-15", self.bars + [bar("2026-10-14")])
        self.assertTrue((self.root / ".pending.json").exists())
        with self.assertRaisesRegex(DownloadError, "incomplete"):
            with consume_pricing_update(self.store, "GOOG"):
                self.fail("incomplete pricing became consumable")
        with patch.object(FakeClient, "fetch", side_effect=AssertionError("unexpected network")):
            result = self.download("2026-10-15")
        self.assertTrue(result["recovered"])
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-14", "to": "2026-10-14"})
        self.assertEqual(len(read_pricing_views(self.store, "GOOG")["raw"]), 3)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assertFalse((self.root / ".staging").exists())

    def test_crash_after_notice_installation_still_blocks_consumption_until_recovery(self):
        self.download()
        original_unlink = Path.unlink

        def interrupt(path, *args, **kwargs):
            if path == self.root / ".pending.json":
                raise OSError("cleanup interrupted")
            return original_unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", interrupt):
            with self.assertRaisesRegex(OSError, "cleanup interrupted"):
                self.download("2026-10-15", self.bars + [bar("2026-10-14")])
        with self.assertRaisesRegex(DownloadError, "incomplete"):
            with consume_pricing_update(self.store, "GOOG"):
                pass
        self.download("2026-10-15")
        self.assertEqual(self.consume(), {"from": "2016-01-04", "to": "2026-10-14"})

    def test_corrupt_or_symlinked_notice_fails_closed(self):
        self.download()
        self.notice.write_text('{"from":"2026-10-14","to":"2016-01-04"}')
        before = snapshot(self.store)
        with self.assertRaisesRegex(DownloadError, "range"):
            self.download("2026-10-15", self.bars + [bar("2026-10-14")])
        self.assertEqual(before, snapshot(self.store))
        self.notice.unlink()
        self.notice.symlink_to(self.root / "source.manifest.json")
        with self.assertRaisesRegex(DownloadError, "symlink"):
            with consume_pricing_update(self.store, "GOOG"):
                pass

    def spawn(self, target):
        context = multiprocessing.get_context("spawn")
        ready, entered, release = (context.Event() for _ in range(3))
        process = context.Process(target=target, args=(str(self.store), ready, entered, release))
        process.start()

        def cleanup():
            # A terminated waiter cannot acknowledge Event.notify_all(); do not
            # signal its synchronization object after the process has died.
            if process.is_alive():
                release.set()
            process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(5)
            process.close()

        self.addCleanup(cleanup)
        self.assertTrue(ready.wait(5))
        return process, entered, release

    def test_consumer_lock_blocks_downloader_before_any_network_request(self):
        self.download(bars=[bar("2026-10-01")])
        with consume_pricing_update(self.store, "GOOG"):
            process, entered, release = self.spawn(download_worker)
            self.assertFalse(entered.wait(0.25), "downloader fetched while consumer held lock")
        self.assertTrue(entered.wait(5))
        release.set()
        process.join(5)
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-14", "to": "2026-10-14"})

    def test_downloader_lock_blocks_other_process_consumer_until_publication(self):
        self.download(bars=[bar("2026-10-01")])
        downloader, downloading, finish_download = self.spawn(download_worker)
        self.assertTrue(downloading.wait(5))
        consumer, consuming, finish_consume = self.spawn(consume_worker)
        self.assertFalse(consuming.wait(0.25), "consumer read while downloader held lock")
        finish_download.set()
        downloader.join(5)
        self.assertEqual(downloader.exitcode, 0)
        self.assertTrue(consuming.wait(5))
        self.assertEqual(storage.read_document(self.notice), {"from": "2026-10-01", "to": "2026-10-14"})
        finish_consume.set()
        consumer.join(5)
        self.assertEqual(consumer.exitcode, 0)
        self.assertFalse(self.notice.parent.exists())

    def test_consumer_process_death_releases_lock_but_keeps_unprocessed_range(self):
        self.download()
        lock_inode = (self.root / ".download.lock").stat().st_ino
        process, entered, _release = self.spawn(consume_worker)
        self.assertTrue(entered.wait(5))
        process.terminate()
        process.join(5)
        self.assertFalse(process.is_alive())
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-10-01"})
        self.download("2026-10-15", self.bars + [bar("2026-10-14")])
        self.assertEqual((self.root / ".download.lock").stat().st_ino, lock_inode)
        self.assertEqual(storage.read_document(self.notice), {"from": "2016-01-04", "to": "2026-10-14"})

    def test_ticker_lock_does_not_block_a_different_ticker(self):
        self.download()
        with consume_pricing_update(self.store, "GOOG"):
            result = download_pricing(self.store, "OTHER", "2026-10-14", client=FakeClient(self.bars))
        self.assertEqual(result["symbol"], "OTHER")
        self.assertTrue((self.store / "end_of_day/manifests/OTHER/update.json").exists())


if __name__ == "__main__":
    unittest.main()

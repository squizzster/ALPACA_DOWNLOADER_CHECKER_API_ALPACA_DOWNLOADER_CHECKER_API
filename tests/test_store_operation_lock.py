"""The store gate is kernel-owned and covers complete exports and writes."""

import multiprocessing
import signal
import tempfile
import unittest
from pathlib import Path

from libs.common.store_lock import StoreBusyError, operation_lock


def hold_exclusive(store, entered, release):
    with operation_lock(store, exclusive=True):
        entered.set()
        release.wait(15)


class StoreOperationLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "data_pricing"
        self.store.mkdir()
        (self.store / "store.manifest.json").write_text("{}")

    def test_shared_exports_coexist_and_exclusive_writers_exclude_every_operation(self):
        with operation_lock(self.store, exclusive=False):
            with operation_lock(self.store, exclusive=False):
                pass
            with self.assertRaises(StoreBusyError):
                with operation_lock(self.store, exclusive=True):
                    pass
        with operation_lock(self.store, exclusive=True):
            for exclusive in (False, True):
                with self.subTest(exclusive=exclusive), self.assertRaises(StoreBusyError):
                    with operation_lock(self.store, exclusive=exclusive):
                        pass

    def test_store_alias_uses_the_same_lock(self):
        alias = self.store.parent / "alias"
        alias.symlink_to(self.store, target_is_directory=True)
        with operation_lock(self.store, exclusive=True), self.assertRaises(StoreBusyError):
            with operation_lock(alias, exclusive=False):
                pass

    def test_existing_lock_only_needs_read_access(self):
        with operation_lock(self.store, exclusive=True):
            pass
        lock = self.store.parent / f".{self.store.name}.operation.lock"
        lock.chmod(0o444)
        with operation_lock(self.store, exclusive=False):
            pass
        with operation_lock(self.store, exclusive=True):
            pass

    def test_sigkill_releases_the_kernel_lock_without_stale_state(self):
        context = multiprocessing.get_context("fork")
        entered, release = context.Event(), context.Event()
        process = context.Process(target=hold_exclusive, args=(self.store, entered, release))
        process.start()

        def cleanup():
            if process.is_alive():
                process.kill()
            process.join(5)

        self.addCleanup(cleanup)
        self.assertTrue(entered.wait(5))
        for exclusive in (False, True):
            with self.subTest(exclusive=exclusive), self.assertRaises(StoreBusyError):
                with operation_lock(self.store, exclusive=exclusive):
                    pass
        process.kill()
        process.join(5)
        self.assertEqual(process.exitcode, -signal.SIGKILL)
        self.assertTrue((self.store / "store.manifest.json").is_file())
        self.assertTrue((self.store.parent / f".{self.store.name}.operation.lock").is_file())
        with operation_lock(self.store, exclusive=False), operation_lock(self.store, exclusive=False):
            pass
        with operation_lock(self.store, exclusive=True):
            pass


if __name__ == "__main__":
    unittest.main()

"""The store gate is kernel-owned and covers complete exports and writes."""

import errno
import fcntl
import multiprocessing
import os
import signal
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from libs.common import store_lock
from libs.common.store_lock import StoreBusyError, operation_lock


def hold_exclusive(store, entered, release):
    with operation_lock(store, exclusive=True):
        entered.set()
        release.wait(15)


def hold_shared(store, entered, release):
    with operation_lock(store, exclusive=False):
        entered.set()
        release.wait(15)


def waiting_writer(store, waiting, blocked, entered, result):
    original = fcntl.flock
    calls, wait_calls = [], 0
    cancelled = False

    def observed(descriptor, flags):
        calls.append((descriptor, flags))
        if flags == fcntl.LOCK_EX:
            blocked.set()
        return original(descriptor, flags)

    def on_wait():
        nonlocal wait_calls
        wait_calls += 1
        waiting.set()

    try:
        with patch.object(store_lock.fcntl, "flock", side_effect=observed):
            try:
                with operation_lock(store, exclusive=True, wait=True, on_wait=on_wait):
                    entered.set()
            except KeyboardInterrupt:
                cancelled = True
        try:
            os.fstat(calls[0][0])
        except OSError as error:
            descriptor_closed = error.errno == errno.EBADF
        else:
            descriptor_closed = False
        result.send(
            {
                "calls": calls,
                "wait_calls": wait_calls,
                "cancelled": cancelled,
                "descriptor_closed": descriptor_closed,
            }
        )
    finally:
        result.close()


class StoreOperationLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "data_pricing"
        self.store.mkdir()
        (self.store / "store.manifest.json").write_text("{}")

    def shared_holder(self):
        context = multiprocessing.get_context("fork")
        entered, release = context.Event(), context.Event()
        process = context.Process(target=hold_shared, args=(self.store, entered, release))
        process.start()

        def cleanup():
            release.set()
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)

        self.addCleanup(cleanup)
        self.assertTrue(entered.wait(5))
        return context, process, release

    def writer_waiter(self, context):
        waiting, blocked, entered = context.Event(), context.Event(), context.Event()
        receive, send = context.Pipe(duplex=False)
        process = context.Process(target=waiting_writer, args=(self.store, waiting, blocked, entered, send))
        process.start()
        send.close()
        self.addCleanup(receive.close)

        def cleanup():
            if process.is_alive():
                process.kill()
            process.join(5)

        self.addCleanup(cleanup)
        self.assertTrue(waiting.wait(5))
        self.assertTrue(blocked.wait(5))
        self.assertFalse(entered.is_set())
        return process, entered, receive

    def test_waiting_writer_enters_only_after_shared_export_releases(self):
        context, holder, release = self.shared_holder()
        waiter, entered, receive = self.writer_waiter(context)
        # The waiting writer owns no lock yet; another export can still read.
        with operation_lock(self.store, exclusive=False):
            pass
        release.set()
        self.assertTrue(entered.wait(5))
        self.assertTrue(receive.poll(5))
        result = receive.recv()
        holder.join(5)
        waiter.join(5)
        self.assertEqual(holder.exitcode, 0)
        self.assertEqual(waiter.exitcode, 0)
        self.assertEqual(result["wait_calls"], 1)
        self.assertFalse(result["cancelled"])
        self.assertTrue(result["descriptor_closed"])
        self.assertEqual(len({descriptor for descriptor, _ in result["calls"]}), 1)
        self.assertEqual(
            [flags for _, flags in result["calls"]],
            [fcntl.LOCK_EX | fcntl.LOCK_NB, fcntl.LOCK_EX, fcntl.LOCK_UN],
        )
        with operation_lock(self.store, exclusive=True):
            pass

    def test_sigint_cancels_waiting_writer_without_entering_or_releasing_holder(self):
        context, holder, release = self.shared_holder()
        waiter, entered, receive = self.writer_waiter(context)
        os.kill(waiter.pid, signal.SIGINT)
        self.assertTrue(receive.poll(5))
        result = receive.recv()
        waiter.join(5)
        self.assertEqual(waiter.exitcode, 0)
        self.assertFalse(entered.is_set())
        self.assertTrue(holder.is_alive())
        self.assertEqual(result["wait_calls"], 1)
        self.assertTrue(result["cancelled"])
        self.assertTrue(result["descriptor_closed"])
        self.assertEqual(len({descriptor for descriptor, _ in result["calls"]}), 1)
        self.assertEqual([flags for _, flags in result["calls"]], [fcntl.LOCK_EX | fcntl.LOCK_NB, fcntl.LOCK_EX])
        with self.assertRaises(StoreBusyError):
            with operation_lock(self.store, exclusive=True):
                self.fail("cancelled waiter released another process's lock")
        release.set()
        holder.join(5)
        self.assertEqual(holder.exitcode, 0)
        with operation_lock(self.store, exclusive=True):
            pass

    def test_immediate_acquisition_does_not_report_waiting(self):
        on_wait = Mock()
        with operation_lock(self.store, exclusive=True, wait=True, on_wait=on_wait):
            pass
        on_wait.assert_not_called()

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

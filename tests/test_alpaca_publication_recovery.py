import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from sqlite_crash_fixture import leave_hot_journal

from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_pricing_download import check_summary as summary
from libs.alpaca_pricing_download import update_checks as checks
from libs.alpaca_pricing_download.storage import initialize_store
from libs.common.sqlite_connections import connect_sqlite


class PublicationRecoveryTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        initialize_store(self.store)

    def assert_needs_recovery(self, path):
        with closing(connect_sqlite(path, readonly=True)) as db:
            with self.assertRaises(sqlite3.OperationalError) as caught:
                db.execute("PRAGMA user_version").fetchone()
            self.assertEqual(caught.exception.sqlite_errorname, "SQLITE_READONLY_ROLLBACK")

    def assert_recovered(self, path):
        self.assertFalse(path.with_name(path.name + "-journal").exists())
        with closing(connect_sqlite(path, readonly=True)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertIsNone(
                db.execute("SELECT name FROM sqlite_master WHERE name='interrupted_write'").fetchone()
            )

    def test_connections_retain_transactions_and_disable_forced_writeback(self):
        path = self.store / "test.sqlite3"
        with closing(connect_sqlite(path, create=True)) as db:
            self.assertEqual(db.execute("PRAGMA synchronous").fetchone()[0], 0)
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            db.execute("CREATE TABLE data (value TEXT)")
            with db:
                db.execute("INSERT INTO data VALUES ('committed')")
            with self.assertRaises(RuntimeError), db:
                db.execute("INSERT INTO data VALUES ('rolled back')")
                raise RuntimeError("interrupted")
        with closing(connect_sqlite(path)) as db:
            self.assertEqual(db.execute("PRAGMA synchronous").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT value FROM data").fetchall(), [("committed",)])

    def test_work_hint_read_recovers_without_losing_committed_work(self):
        checks.begin_checks(self.store)
        checks.queue_check(self.store, "AAA")
        before = checks.read_checks(self.store)
        path = self.store / checks.CHECK_FILE
        leave_hot_journal(path, "DELETE FROM checks")
        self.assert_needs_recovery(path)
        self.assertEqual(checks.read_checks(self.store), before)
        self.assert_recovered(path)

    def test_summary_pending_read_recovers_committed_coverage(self):
        token = summary.begin_rebuild(self.store)
        summary.finish_rebuild(self.store, token, [])
        before = summary.read_snapshot(self.store)
        path = summary.summary_path(self.store)
        leave_hot_journal(path, "UPDATE state SET complete=0, revision=revision+1")
        self.assert_needs_recovery(path)
        self.assertEqual(summary.pending_symbols(self.store), [])
        self.assertEqual(summary.read_snapshot(self.store), before)
        self.assert_recovered(path)

    def test_lineage_writer_recovers_index_before_reading_version(self):
        with lineage.writer_lock(self.store) as root:
            before = lineage.read_index(root)
        path = root / "index.sqlite3"
        leave_hot_journal(path, "PRAGMA user_version=0")
        self.assert_needs_recovery(path)
        with self.assertRaises(lineage.LineageError):
            lineage._index_version(root)  # Consumers stay read-only and do not repair.
        with lineage.writer_lock(self.store):
            self.assertEqual(lineage.read_index(root), before)
        self.assert_recovered(path)

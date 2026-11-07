"""Accepted workflow outcomes commit once per batch without changing publications."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_lineage_build import universe_state as state
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.work_contract import WorkPolicy


class WorkBatchTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Path(directory.name)
        state.initialize(self.store)
        self.work = {
            f"S{number}": {"obligation_id": f"obligation-{number}", "status": "pending", "attempts": 0}
            for number in range(20)
        }
        state.put_work_group(self.store, self.work)

    def finish(self, batch, symbol):
        return state.record_outcome(
            self.store,
            symbol,
            status="complete",
            outcome={"source_generation": symbol},
            attempts=1,
            expected=self.work[symbol],
            batch=batch,
        )

    def test_twenty_outcomes_share_one_connection_and_commit(self):
        before = state.read_state(self.store)["revision"]
        with patch.object(state, "connect_sqlite", wraps=state.connect_sqlite) as connect:
            with state.outcome_batch(self.store) as batch:
                statements = []
                batch.connection.set_trace_callback(statements.append)
                for symbol in self.work:
                    self.finish(batch, symbol)
        connect.assert_called_once()
        self.assertEqual(statements.count("COMMIT"), 1)
        self.assertEqual(state.read_state(self.store)["revision"], before + 1)
        self.assertTrue(
            all(item["status"] == "complete" for item in state.read_state(self.store)["work"].values())
        )

    def test_interruption_before_commit_rolls_back_every_outcome(self):
        before = state.read_state(self.store)
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            with state.outcome_batch(self.store) as batch:
                self.finish(batch, "S0")
                self.finish(batch, "S1")
                raise RuntimeError("interrupted")
        self.assertEqual(state.read_state(self.store), before)

    def test_newer_obligation_prevents_stale_batch_acknowledgement(self):
        state.put_work(self.store, "S1", {**self.work["S1"], "obligation_id": "new-requirement"})
        before = state.read_state(self.store)
        with self.assertRaisesRegex(LineageError, "work changed"):
            with state.outcome_batch(self.store) as batch:
                self.finish(batch, "S0")
                self.finish(batch, "S1")
        self.assertEqual(state.read_state(self.store), before)

    def test_expired_batch_cannot_record_an_outcome(self):
        with state.outcome_batch(self.store) as batch:
            self.finish(batch, "S0")
        with self.assertRaisesRegex(LineageError, "no longer active"):
            self.finish(batch, "S1")

    def test_dependency_group_is_persisted_in_one_transaction_before_work(self):
        policy = WorkPolicy(self.store, {"symbols": {}}, {}, [], "2026-10-20")
        with patch.object(state, "connect_sqlite", wraps=state.connect_sqlite) as connect:
            policy.require_dependencies({"AAA", "BBB"}, "2026-10-20", {"BBB"})
        connect.assert_called_once()
        saved = state.read_state(self.store)["work"]
        self.assertNotIn("AAA", saved)
        self.assertEqual(saved["BBB"]["required_group"], ["AAA", "BBB"])
        self.assertTrue(saved["BBB"]["acquire"])

    def test_batch_rejects_another_store_thread_or_process(self):
        from concurrent.futures import ThreadPoolExecutor

        with state.outcome_batch(self.store) as batch:
            with self.assertRaisesRegex(LineageError, "another store"):
                state.record_outcome(self.store / "other", "S0", status="complete", outcome={}, batch=batch)
            with ThreadPoolExecutor(max_workers=1) as worker:
                with self.assertRaisesRegex(LineageError, "another store/owner"):
                    worker.submit(self.finish, batch, "S0").result()
            with patch.object(state.os, "getpid", return_value=-1):
                with self.assertRaisesRegex(LineageError, "another store/owner"):
                    self.finish(batch, "S0")
        self.assertEqual(state.read_state(self.store)["work"]["S0"]["status"], "pending")

    def test_group_failure_batches_requirements_and_outcomes_without_changing_retry_policy(self):
        policy = WorkPolicy(self.store, {"symbols": {}}, self.work, [], "2026-10-20")
        with patch.object(state, "connect_sqlite", wraps=state.connect_sqlite) as connect:
            policy.fail(set(self.work), {"reason": "incomplete"}, stage="completion")
        self.assertEqual(connect.call_count, 2)
        saved = state.read_state(self.store)["work"]
        for symbol, item in saved.items():
            self.assertEqual(item["status"], "retry")
            self.assertEqual(item["attempts"], 1)
            self.assertEqual(item["obligation_id"], self.work[symbol]["obligation_id"])
            self.assertIsNotNone(item["retry_after_utc"])

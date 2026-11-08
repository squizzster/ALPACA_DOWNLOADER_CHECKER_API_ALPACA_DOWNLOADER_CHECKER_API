"""Legacy projection assembly consumes accepted assignments once, without rule replay."""

from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.assignment_migration import migrate
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_serving_catalogue import policy, producer


class AssignmentMigrationTests(NativeCase):
    def legacy(self):
        def strip(receipt, boundaries):
            receipt["assignment_handoffs"] = {"current_month": receipt["assignment_handoffs"]["current_month"]}
            receipt["assignment_handoff_kinds"] = ["current_month"]
            receipt.pop("serving_ownership", None)
            boundaries.clear()

        self.publish_change(strip)
        self.bootstrap()

    def test_selected_migration_preserves_serving_decision_and_skips_completed_bodies(self):
        self.legacy()
        before = self.catalogue_groups("TEST")["TEST"]
        read = storage.read_boundaries
        reads = []

        def selected(root, receipt, **kwargs):
            self.assertEqual(kwargs["kinds"], {"completed_years", "current_year"})
            reads.append(receipt["symbol"])
            return read(root, receipt, **kwargs)

        with (
            patch.object(storage, "read_boundaries", side_effect=selected),
            patch(
                "libs.alpaca_pricing_download.check_summary.invalidate",
                side_effect=AssertionError("summary invalidated"),
            ),
            patch(
                "libs.alpaca_lineage_build.check_summary.refresh",
                side_effect=AssertionError("summary refreshed"),
            ),
            patch.object(storage, "_acknowledge", side_effect=AssertionError("upstream work consumed")),
            patch("libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("rules replayed")),
            patch(
                "libs.alpaca_pricing_download.storage.read_boundary", side_effect=AssertionError("prices read")
            ),
        ):
            result = migrate(self.store, ["TEST"])
        self.assertEqual(result["assignment_body_reads"], 2)
        self.assertEqual(reads, ["TEST"])
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("completed receipt reopened")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("completed bodies reopened")),
        ):
            again = migrate(self.store, ["TEST"])
        self.assertEqual(again["already_current_groups"], 1)
        with (
            patch.object(producer, "evaluate", side_effect=AssertionError("serving decision replayed")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("numerical history replayed")),
        ):
            served = producer.maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(self.catalogue_groups("TEST")["TEST"]["decisions"], before["decisions"])

    def test_interrupted_migration_recovers_journal_without_assembling_again(self):
        self.legacy()

        class Interrupted(BaseException):
            pass

        with (
            patch.object(storage, "finish_publication", side_effect=Interrupted),
            self.assertRaises(Interrupted),
        ):
            migrate(self.store, ["TEST"])
        with (
            patch.object(storage, "read_boundaries", side_effect=AssertionError("body reread on recovery")),
            patch(
                "libs.alpaca_pricing_download.check_summary.invalidate",
                side_effect=AssertionError("summary invalidated"),
            ),
            patch.object(storage, "_recovery_inputs", side_effect=AssertionError("upstream inputs reopened")),
        ):
            result = migrate(self.store, ["TEST"])
        self.assertTrue(result["recovered_publication"])
        self.assertEqual(result["assignment_body_reads"], 0)

    def test_changed_source_rejected_before_assignment_reads(self):
        from test_alpaca_pricing_download import FakeClient

        from libs.alpaca_pricing_download.download import download_pricing

        self.legacy()
        download_pricing(self.store, "TEST", "2026-10-15", client=FakeClient(self.bars))
        with (
            patch.object(storage, "read_boundaries", side_effect=AssertionError("stale body read")),
            self.assertRaises(LineageError),
        ):
            migrate(self.store, ["TEST"])

    def test_stale_policy_rejected_before_receipts_or_assignment_bodies(self):
        self.legacy()
        original = storage.read_index

        def stale(*args, **kwargs):
            index = original(*args, **kwargs)
            index["symbols"]["TEST"]["correlation_policy"] = {}
            return index

        with (
            patch.object(storage, "read_index", side_effect=stale),
            patch.object(storage, "read_receipt", side_effect=AssertionError("stale receipt read")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("stale body read")),
            self.assertRaisesRegex(LineageError, "current policy"),
        ):
            migrate(self.store, ["TEST"])

    def test_legacy_serving_queue_upgraded_before_selected_body_reads(self):
        from contextlib import closing

        from libs.alpaca_serving_catalogue import work
        from libs.common.sqlite_connections import connect_sqlite

        self.legacy()
        with closing(connect_sqlite(self.store / "end_of_day/lineage/index.sqlite3")) as connection, connection:
            connection.execute("UPDATE serving_work_state SET version=1")
        read = storage.read_boundaries
        upgraded = False
        upgrade = work.upgrade

        def accept(store):
            nonlocal upgraded
            upgrade(store)
            upgraded = True

        def bodies(*args, **kwargs):
            self.assertTrue(upgraded)
            return read(*args, **kwargs)

        with (
            patch.object(work, "upgrade", side_effect=accept) as called,
            patch.object(storage, "read_boundaries", side_effect=bodies),
        ):
            self.assertEqual(migrate(self.store, ["TEST"])["migrated_groups"], 1)
            self.assertEqual(called.call_count, 1)
        with patch.object(work, "upgrade", side_effect=AssertionError("completed migration upgraded queue")):
            self.assertEqual(migrate(self.store, ["TEST"])["already_current_groups"], 1)

    def test_empty_history_migrates_without_numerical_work(self):
        self.add_symbol("EMPTY", bars=[])

        def strip(receipt, boundaries):
            receipt.pop("assignment_handoffs", None)
            receipt.pop("assignment_handoff_kinds", None)
            receipt.pop("serving_ownership", None)
            boundaries.clear()

        self.publish_change(strip, "EMPTY")
        self.bootstrap()
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("empty history evaluated")):
            result = migrate(self.store, ["EMPTY"])
            self.assertEqual(result["migrated_groups"], 1)
            self.assertTrue(producer.maintain(self.store)["complete"])

    def test_mixed_group_keeps_completed_peers_compact_entry_without_recalculation(self):
        from copy import deepcopy

        from test_alpaca_durable_maintenance import fixture, seed_group

        from libs.alpaca_lineage_build import projection

        with fixture() as case:
            store = seed_group(case)
            root = store / "end_of_day/lineage"
            index = storage.read_index(root)
            receipts = {s: storage.read_receipt(root, s, index) for s in ("AAA", "BBB")}
            previous = deepcopy(receipts)
            for key in ("assignment_handoffs", "assignment_handoff_kinds", "serving_ownership"):
                receipts["AAA"].pop(key, None)
            with storage.pricing_locks(store, ("AAA", "BBB")):
                storage.publish(store, root, index, receipts, {"AAA": {}, "BBB": {}}, {}, previous=previous)
            before = index["symbols"]["BBB"]
            project = projection.project_receipt
            digest = storage.document_digest

            def projected(receipt):
                self.assertNotEqual(receipt.get("symbol"), "BBB")
                return project(receipt)

            def hashed(value):
                self.assertNotEqual(value.get("symbol"), "BBB")
                return digest(value)

            with (
                patch.object(projection, "project_receipt", side_effect=projected),
                patch.object(storage, "document_digest", side_effect=hashed),
            ):
                result = migrate(store, ["AAA"])
            self.assertEqual(result["migrated_groups"], 1)
            self.assertEqual(storage.read_index(root)["symbols"]["BBB"], before)

    def test_scope_must_be_explicit(self):
        with self.assertRaisesRegex(LineageError, "select symbols"):
            migrate(self.store)

    def test_limit_defers_other_groups_before_receipt_or_body_reads(self):
        self.add_symbol("OTHER")
        for symbol in ("TEST", "OTHER"):

            def strip(receipt, boundaries):
                receipt.pop("assignment_handoffs", None)
                receipt.pop("assignment_handoff_kinds", None)
                receipt.pop("serving_ownership", None)
                boundaries.clear()

            self.publish_change(strip, symbol=symbol)
        selected = []
        read = storage.read_receipt

        def receipt(root, symbol, index):
            selected.append(symbol)
            return read(root, symbol, index)

        with patch.object(storage, "read_receipt", side_effect=receipt):
            result = migrate(self.store, all_published=True, limit=1)
        self.assertEqual(result["migrated_groups"], 1)
        self.assertEqual(result["remaining_groups"], 1)
        self.assertEqual(len(selected), 1)
        again = migrate(self.store, all_published=True, limit=1)
        self.assertEqual(again["already_current_groups"], 1)
        self.assertEqual(again["remaining_groups"], 0)

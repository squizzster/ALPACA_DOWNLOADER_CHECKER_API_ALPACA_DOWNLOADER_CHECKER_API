"""Serving consumes the held writer's index without weakening pricing freshness."""

import fcntl
from pathlib import Path
from threading import Thread
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary, StandaloneReader, StandaloneReaderError, standalone
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue.producer import maintain


class ServingHandoffTests(NativeCase):
    def references(self, view):
        entry = view.index()["symbols"]["TEST"]
        return {"lineage": {"TEST": entry["sha256"]}, "pricing": {"TEST": entry["source_sha256"]}}

    def test_changed_groups_share_one_accepted_store_and_index(self):
        for symbol in ("OTHER", "THIRD"):
            self.add_symbol(symbol)
        before = [s.to_dict() for s in self.bootstrap().list_securities()]
        for symbol in ("TEST", "OTHER", "THIRD"):
            self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["metadata"]), symbol)
        with (
            patch.object(lineage, "check_store", wraps=lineage.check_store) as stores,
            patch.object(lineage, "read_index", wraps=lineage.read_index) as indexes,
            patch.object(lineage, "_index_version", wraps=lineage._index_version) as protocols,
            patch.object(standalone, "read_index", side_effect=AssertionError("accepted index reopened")),
            patch.object(standalone, "_rows", side_effect=AssertionError("unchanged prices reopened")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["evaluated_groups"], 3)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(stores.call_count, 1)
        self.assertEqual(indexes.call_count, 1)
        self.assertEqual(protocols.call_count, 1, "only the owning writer accepts the lock protocol")
        self.assertEqual(before, [s.to_dict() for s in DataLibrary(store=self.store).list_securities()])

    def test_empty_queue_does_not_load_the_index(self):
        self.bootstrap()
        with (
            patch.object(lineage, "read_index", side_effect=AssertionError("empty queue loaded membership")),
            patch(
                "libs.alpaca_serving_catalogue.storage.controls",
                side_effect=AssertionError("empty queue loaded catalogue groups"),
            ),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"])
        self.assertEqual(result["evaluated_groups"], 0)

    def test_complete_multisymbol_group_consumes_each_selected_receipt_once(self):
        from test_alpaca_serving_incremental import IncrementalServingTests

        self.add_symbol("OTHER")
        IncrementalServingTests.connect_group(self, "TEST", "OTHER")
        with (
            patch.object(standalone, "read_index", side_effect=AssertionError("group index reopened")),
            patch.object(lineage, "read_receipt", wraps=lineage.read_receipt) as receipts,
        ):
            result = maintain(self.store, bootstrap=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["evaluated_groups"], 1)
        self.assertEqual(sorted(call.args[1] for call in receipts.call_args_list), ["OTHER", "TEST"])
        self.assertEqual(DataLibrary(store=self.store).online_count(), 2)

    def test_view_expires_with_actual_writer_lock(self):
        reader = StandaloneReader(self.store)
        with (self.root / ".worker.lock").open("rb") as competing:
            with lineage.writer_snapshot(self.store) as view:
                refs = self.references(view)
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with reader._snapshot("TEST", expected=refs, writer_view=view, include_assignments=False):
                    pass
            fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(competing, fcntl.LOCK_UN)
        with (
            patch.object(Path, "open", side_effect=AssertionError("expired view touched storage")),
            self.assertRaisesRegex(StandaloneReaderError, "active writer lock"),
        ):
            with reader._snapshot("TEST", expected=refs, writer_view=view):
                self.fail("expired view was accepted")

    def test_view_rejects_other_store_thread_and_process(self):
        with lineage.writer_snapshot(self.store) as view:
            self.references(view)
            with self.assertRaisesRegex(lineage.LineageError, "another store"):
                view.select(self.directory / "other", ["TEST"])
            errors = []

            def another_thread():
                try:
                    view.select(self.store, ["TEST"])
                except lineage.LineageError as error:
                    errors.append(str(error))

            thread = Thread(target=another_thread)
            thread.start()
            thread.join()
            self.assertEqual(len(errors), 1)
            with (
                patch.object(lineage.os, "getpid", return_value=-1),
                self.assertRaisesRegex(lineage.LineageError, "active writer lock"),
            ):
                view.select(self.store, ["TEST"])

    def test_store_acceptance_and_context_cannot_cross_stores(self):
        accepted = lineage.accept_store(self.store)
        with self.assertRaisesRegex(lineage.LineageError, "another store"):
            with lineage.writer_snapshot(self.directory / "other", accepted_store=accepted):
                self.fail("wrong store accepted")
        context = OperationContext(self.directory / "other")
        with self.assertRaisesRegex(lineage.LineageError, "another store"):
            with lineage.writer_snapshot(self.store, context=context):
                self.fail("wrong context accepted")

    def test_pending_lineage_prevents_index_acceptance(self):
        (self.root / ".pending.json").write_text("{}")
        with lineage.writer_snapshot(self.store) as view:
            with self.assertRaisesRegex(lineage.LineageError, "publication incomplete"):
                view.index()

    def test_expected_references_and_selected_index_are_required(self):
        reader = StandaloneReader(self.store)
        with lineage.writer_snapshot(self.store) as view:
            with self.assertRaisesRegex(lineage.LineageError, "not been accepted"):
                view.select(self.store, ["TEST"])
            with self.assertRaisesRegex(StandaloneReaderError, "requires selected generation"):
                with reader._snapshot("TEST", writer_view=view):
                    self.fail("missing references accepted")
            refs = self.references(view)
            refs["lineage"]["TEST"] = "0" * 64
            with self.assertRaisesRegex(StandaloneReaderError, "lineage generation changed"):
                with reader._snapshot("TEST", expected=refs, writer_view=view):
                    self.fail("wrong generation accepted")

    def test_pricing_lock_spans_consumption(self):
        reader = StandaloneReader(self.store)
        with lineage.writer_snapshot(self.store) as view:
            refs = self.references(view)
            with (self.store / "end_of_day/pricing/TEST/.download.lock").open("rb") as competing:
                with reader._snapshot("TEST", expected=refs, writer_view=view, include_assignments=False):
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(competing, fcntl.LOCK_UN)

    def test_changed_pricing_and_pending_pricing_still_fail_closed(self):
        reader = StandaloneReader(self.store)
        with lineage.writer_snapshot(self.store) as view:
            refs = self.references(view)
            pending = self.store / "end_of_day/pricing/TEST/.pending.json"
            pending.write_text("{}")
            with self.assertRaisesRegex(StandaloneReaderError, "pricing publication incomplete"):
                with reader._snapshot("TEST", expected=refs, writer_view=view):
                    self.fail("pending pricing accepted")
            pending.unlink()
            download_pricing(
                self.store,
                "TEST",
                "2026-10-15",
                client=FakeClient(self.bars),
                refresh_all=True,
            )
            with (
                patch.object(standalone, "read_update", return_value=None),
                self.assertRaisesRegex(StandaloneReaderError, "lineage awaits changed pricing"),
            ):
                with reader._snapshot("TEST", expected=refs, writer_view=view):
                    self.fail("changed pricing generation accepted")

    def test_ordinary_reader_still_selects_and_rechecks(self):
        reader = StandaloneReader(self.store)
        with lineage.writer_snapshot(self.store) as view:
            refs = self.references(view)
            with (
                patch.object(standalone, "read_index", wraps=standalone.read_index) as entries,
                patch.object(lineage, "_index_version", wraps=lineage._index_version) as protocols,
            ):
                with reader._snapshot("TEST", expected=refs, include_assignments=False):
                    pass
            self.assertEqual(entries.call_count, 2)
            self.assertEqual(protocols.call_count, 2)

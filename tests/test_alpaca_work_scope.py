"""Unexpected owner access aborts before I/O; selected expansion still completes."""

import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import ActionCatalogueClient, fixture, rename
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar, snapshot

from libs.alpaca_lineage_build import storage, universe_plan, update
from libs.alpaca_lineage_build.universe_update import plan_update
from libs.alpaca_pricing_download import storage as pricing
from libs.common.debug_trace import in_context
from libs.common.work_scope import UnexpectedStoreWork, accept_expansion, check_path, selected_work


class WorkScopeTests(unittest.TestCase):
    def test_compact_planning_aborts_source_receipt_and_notice_reads_before_access(self):
        for stage in ("boundaries", "selection", "preview"):
            with self.subTest(stage=stage), fixture() as case:
                before = snapshot(case.store)
                if stage == "boundaries":
                    target = patch.object(
                        storage,
                        "read_index",
                        side_effect=lambda *a, **k: pricing.load_source(
                            case.store / "end_of_day/pricing/AAA", "AAA"
                        ),
                    )
                elif stage == "selection":

                    def receipt(index, *args, **kwargs):
                        return storage.read_receipt(case.store / "end_of_day/lineage", "AAA", index)

                    target = patch.object(universe_plan, "changed_evidence", side_effect=receipt)
                else:
                    target = patch.object(
                        universe_plan,
                        "check_plan",
                        side_effect=lambda *a, **k: pricing.read_update(case.store / "end_of_day/pricing/AAA"),
                    )
                with target, self.assertRaisesRegex(UnexpectedStoreWork, "stopped before access"):
                    plan_update(case.store, "2026-10-20", identity_client=case.identity, registry=case.registry)
                self.assertEqual(snapshot(case.store), before)
                self.assertEqual(case.pages, [])
                self.assertEqual(case.identity.catalogue_calls, 0 if stage == "boundaries" else 1)

    def test_unselected_access_aborts_update_and_clean_retry_completes(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.live["AAA"] = case.old + [bar("2026-10-19")]
            before = snapshot(case.store / "end_of_day/pricing")

            def unexpected(*args, **kwargs):
                return pricing.load_source(case.store / "end_of_day/pricing/HIST", "HIST")

            with patch.object(update, "append_ready", side_effect=unexpected):
                with self.assertRaisesRegex(UnexpectedStoreWork, "HIST"):
                    case.update("2026-10-20")
            self.assertEqual(snapshot(case.store / "end_of_day/pricing"), before)
            self.assertEqual(len(case.pages), 2)
            self.assertTrue(case.update("2026-10-20")["complete"])

    def test_metadata_selected_new_dependency_is_allowed_and_finishes_serving(self):
        with fixture() as case:
            aaa = {**asset("AAA"), "name": "Changed metadata"}
            bbb = asset("BBB")
            case.identity = ActionCatalogueClient(
                [aaa], [rename()], exact={"AAA": aaa, "BBB": bbb, "111111111": bbb}
            )
            case.live["BBB"] = case.old
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertIn("BBB", storage.read_index(case.store / "end_of_day/lineage")["symbols"])
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])

    def test_guard_is_lexical_follows_workers_and_cannot_escape_a_compact_parent(self):
        store = Path("/synthetic-store")
        a = store / "end_of_day/pricing/AAA/source.manifest.json"
        b = store / "end_of_day/lineage/BBB/lineage.manifest.json"
        with patch.object(Path, "stat", side_effect=AssertionError("scope filesystem access")):
            with self.assertRaises(UnexpectedStoreWork), selected_work(store, {"AAA"}):
                check_path(a)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    with self.assertRaises(UnexpectedStoreWork):
                        pool.submit(in_context(check_path), b).result()
                # A failed worker stops even previously allowed access, and the
                # scope exit propagates failure if a caller swallowed the future.
                with self.assertRaises(UnexpectedStoreWork):
                    check_path(a)
            with self.assertRaises(UnexpectedStoreWork), selected_work(store, {"AAA"}):
                accept_expansion(store, {"BBB"}, anchors={"BBB"}, authority="unselected")
            with self.assertRaises(UnexpectedStoreWork), selected_work(store, {"AAA"}):
                accept_expansion(store, {"AAA", "BBB"}, anchors={"AAA"}, authority="completed_dependencies")
                check_path(b)
                with self.assertRaises(UnexpectedStoreWork), selected_work(None, (), phase="compact"):
                    accept_expansion(store, {"AAA", "BBB", "CCC"}, anchors={"AAA"}, authority="escape")
                check_path(store / "end_of_day/pricing/CCC/source.manifest.json")
            with self.assertRaises(UnexpectedStoreWork), selected_work(store, {"AAA"}):
                check_path(Path("/other-store/end_of_day/pricing/AAA/source.manifest.json"))
            check_path(b)  # Scope expired on exit, including the failed worker.

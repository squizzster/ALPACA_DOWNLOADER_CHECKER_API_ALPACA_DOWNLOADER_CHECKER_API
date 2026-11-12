"""Correction/lineage materialization is lent to the subsequent append owner."""

import unittest
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import storage, update
from libs.alpaca_pricing_download import handoff
from libs.alpaca_pricing_download.download import read_pricing_views
from libs.alpaca_pricing_download.storage import load_source


class AppendPartitionHandoffTests(unittest.TestCase):
    def test_new_lineage_bodies_cross_json_to_serving_without_rereads(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = case.old + [bar("2026-10-19")]
            publish, read = storage.finish_publication, Path.read_bytes
            published = set()

            def completed(store, root, journal, **kwargs):
                result = publish(store, root, journal, **kwargs)
                published.update(root / relative for relative in journal["documents"])
                return result

            def no_reopen(path):
                self.assertNotIn(path, published, "completed lineage output reopened by its consumer")
                return read(path)

            with (
                patch.object(storage, "finish_publication", side_effect=completed),
                patch.object(Path, "read_bytes", no_reopen),
            ):
                result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertTrue(published)
            self.assertEqual(result["stages"]["serving_catalogue"]["checked_bars"], 8)

    def test_full_correction_then_append_reads_each_selected_payload_once(self):
        for added, target in (
            ("2026-10-19", "2026-10-20"),
            ("2026-11-02", "2026-11-03"),
            ("2027-01-04", "2027-01-05"),
        ):
            with self.subTest(target=target):
                self.check_correction_append(added, target)

    def check_correction_append(self, added, target):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = [
                {**case.old[0], "v": 12345},
                case.old[-1],
                bar(added),
            ]
            read, opened = handoff.read_artifact, set()

            def once(path, **kwargs):
                key = str(path), kwargs.get("selector_sha256")
                self.assertNotIn(key, opened, "accepted decoded partition read again")
                opened.add(key)
                return read(path, **kwargs)

            with patch.object(handoff, "read_artifact", side_effect=once):
                result = case.update(target)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["pending_work"], 0)
            views = read_pricing_views(case.store, "AAA")
            for rows in views.values():
                self.assertEqual(rows[0][5], "12345")
                self.assertEqual(rows[-1][0][:10], added)

    def test_different_store_or_boundary_generation_cannot_supply_append_rows(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = case.old + [bar("2026-10-19")]
            root = case.store / "end_of_day/pricing/AAA"
            source = load_source(root, "AAA")
            generation = source["boundaries"]["current_month"]["sha256"]
            stale = {
                (str(root), "current_month", "superseded"): (None, None),
                (str(root / "other-store"), "current_month", generation): (None, None),
            }
            with (
                patch.object(update, "append_partitions", return_value=stale),
                patch.object(handoff, "read_artifact", wraps=handoff.read_artifact) as reads,
            ):
                result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertTrue(reads.called, "changed applicability must select its actual assembly")
            for rows in read_pricing_views(case.store, "AAA").values():
                self.assertEqual(rows[-1][0][:10], "2026-10-19")


if __name__ == "__main__":
    unittest.main()

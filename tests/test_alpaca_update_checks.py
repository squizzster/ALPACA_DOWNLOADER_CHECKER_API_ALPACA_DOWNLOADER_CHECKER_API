import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_lineage_build.contract import versions
from libs.alpaca_lineage_build.selection import DependencyGraph
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.update_checks import (
    CHECK_FILE,
    acknowledge_checks,
    begin_checks,
    complete_checks,
    queue_check,
    read_checks,
)


class UpdateCheckTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Path(self.temporary.name) / "store"
        self.rows = [bar("2026-10-01"), bar("2026-10-02")]
        self.client = IdentityClient({name: asset(name) for name in ("GOOG", "OTHER")})

    def download(self, symbol="GOOG", end="2026-10-14", **kwargs):
        return download_pricing(self.store, symbol, end, client=FakeClient(self.rows), **kwargs)

    def build(self, **kwargs):
        result = pipeline.run_lineage(self.store, identity_client=self.client, **kwargs)
        self.assertEqual(result["failed"], 0, result)
        return result

    def test_five_identical_downloads_trust_complete_boundary_without_payload_or_network(self):
        self.download()
        before = snapshot(self.store)
        with (
            patch.object(FakeClient, "fetch", side_effect=AssertionError("provider request")),
            patch(
                "libs.alpaca_pricing_download.download.read_validator", side_effect=AssertionError("validator")
            ),
            patch("libs.alpaca_pricing_download.download.read_boundary", side_effect=AssertionError("prices")),
        ):
            for _ in range(4):
                result = self.download()
                self.assertEqual(result["mode"], "unchanged")
                self.assertEqual(result["requests"], 0)
                self.assertEqual(result["files_written"], 0)
        self.assertEqual(before, snapshot(self.store))

    def test_advancing_boundary_still_probes_old_first_and_last_in_every_view(self):
        self.download()
        client = FakeClient(self.rows)
        result = download_pricing(self.store, "GOOG", "2026-10-15", client=client)
        self.assertEqual(result["consistency_check"], "matched")
        self.assertEqual(len(client.calls), 32)
        self.assertEqual(
            {call[2].date().isoformat() for call in client.calls[:16]}, {"2026-10-01", "2026-10-02"}
        )

    def test_complete_work_list_avoids_all_individual_receipts_on_repeated_runs(self):
        self.download()
        self.build()
        self.assertTrue(read_checks(self.store)["complete"])
        requests = self.client.request_count
        with (
            patch.object(pipeline, "local_symbols", side_effect=AssertionError("source directory scan")),
            patch.object(pipeline, "process_group", side_effect=AssertionError("unchanged group opened")),
            patch.object(pipeline, "load_source", side_effect=AssertionError("source receipt")),
            patch.object(storage, "read_receipt", side_effect=AssertionError("lineage receipt")),
        ):
            for _ in range(4):
                self.assertEqual(self.build()["results"], [])
        self.assertEqual(self.client.request_count, requests)

    def test_deleted_work_list_rebuilds_from_source_generations_without_lineage_reads(self):
        self.download()
        self.build()
        before = snapshot(self.store / "end_of_day")
        (self.store / CHECK_FILE).unlink()
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("lineage receipt")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("prices")),
            patch.object(pipeline, "acquire", side_effect=AssertionError("metadata")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("calculation")),
        ):
            self.assertEqual(self.build()["results"], [])
        self.assertEqual(before, snapshot(self.store / "end_of_day"))
        self.assertTrue(read_checks(self.store)["complete"])
        self.assertEqual(read_checks(self.store)["checks"], {})

    def test_incomplete_large_index_reads_one_source_generation_and_no_lineage_receipts(self):
        self.download()
        self.build()
        (self.store / CHECK_FILE).unlink()
        index = storage.read_index(self.store / "end_of_day/lineage")
        current = versions()
        synthetic = {}
        for number in range(6000):
            symbol = f"T{number:04}"
            source = {"symbol": symbol, "generation": number}
            synthetic[symbol] = source
            index["symbols"][symbol] = {
                "sha256": "0" * 64,
                "source_sha256": storage.document_digest(source),
                "dependencies": [],
                "asset_ids": [],
                "group_members": [symbol],
                "versions": current,
            }
        opened = []
        load_source = pipeline.load_source

        def selected(root, symbol):
            opened.append(symbol)
            return synthetic.get(symbol) or load_source(root, symbol)

        with (
            patch.object(storage, "read_index", return_value=index),
            patch.object(pipeline, "load_source", side_effect=selected),
            patch.object(storage, "read_receipt", side_effect=AssertionError("lineage receipt")),
            patch.object(pipeline, "process_group", side_effect=AssertionError("unchanged group")),
        ):
            self.assertEqual(self.build()["results"], [])
        self.assertEqual(len(opened), 6001)
        self.assertTrue(read_checks(self.store)["complete"])

    def test_incomplete_recovery_processes_stale_work_before_source_census(self):
        self.download()
        self.build()
        (self.store / CHECK_FILE).unlink()
        root = self.store / "end_of_day/lineage"
        index = storage.read_index(root)
        receipt = storage.read_receipt(root, "GOOG", index)
        receipt["schema_version"] = 11
        receipt["versions"] = {**receipt["versions"], "lineage": 11, "correlation": 54}
        storage.atomic_json(root / "GOOG/lineage.manifest.json", receipt)
        storage._update_index(
            root,
            {
                "GOOG": {
                    **index["symbols"]["GOOG"],
                    "sha256": storage.document_digest(receipt),
                    "versions": receipt["versions"],
                }
            },
        )
        scan = pipeline._source_generation_changes

        def after_stale_work(*args, **kwargs):
            self.assertEqual(storage.read_index(root)["symbols"]["GOOG"]["versions"], versions())
            return scan(*args, **kwargs)

        with patch.object(pipeline, "_source_generation_changes", side_effect=after_stale_work):
            result = self.build()
        self.assertEqual(result["results"][0]["status"], "processed")

    def test_deleted_work_list_recovers_a_provenance_only_source_change(self):
        self.download()
        self.build()
        result = self.download(end="2026-10-15")
        self.assertFalse(result["changed"])
        self.assertFalse((self.store / "end_of_day/manifests/GOOG/update.json").exists())
        (self.store / CHECK_FILE).unlink()
        rebuilt = self.build()
        self.assertEqual(rebuilt["results"][0]["status"], "processed")
        self.assertTrue(read_checks(self.store)["complete"])

    def test_cache_recreated_during_source_census_stays_incomplete(self):
        self.download()
        self.build()
        (self.store / CHECK_FILE).unlink()
        load_source = pipeline.load_source
        replacement = []

        def recreate(root, symbol):
            source = load_source(root, symbol)
            if not replacement:
                (self.store / CHECK_FILE).unlink()
                replacement.append(begin_checks(self.store))
            return source

        with patch.object(pipeline, "load_source", side_effect=recreate):
            self.assertEqual(self.build()["results"], [])
        current = read_checks(self.store)
        self.assertEqual(current["epoch"], replacement[0]["epoch"])
        self.assertFalse(current["complete"])

    def test_hint_arriving_during_source_census_survives_completion(self):
        self.download()
        self.build()
        (self.store / CHECK_FILE).unlink()
        load_source = pipeline.load_source
        arrived = []

        def publish_after_selection(root, symbol):
            source = load_source(root, symbol)
            if not arrived:
                arrived.append(self.download("OTHER"))
            return source

        with patch.object(pipeline, "load_source", side_effect=publish_after_selection):
            self.assertEqual(self.build()["results"], [])
        current = read_checks(self.store)
        self.assertTrue(current["complete"])
        self.assertEqual(set(current["checks"]), {"OTHER"})

    def test_provenance_only_change_is_selected_without_opening_unrelated_group(self):
        self.download()
        self.download("OTHER")
        self.build()
        before = snapshot(self.store / "end_of_day/lineage/OTHER")
        prior_matches = StandaloneReader(self.store).resolve_ticker("GOOG")["matches"]
        result = self.download(end="2026-10-15")
        self.assertFalse(result["changed"])
        self.assertFalse((self.store / "end_of_day/manifests/GOOG/update.json").exists())
        self.assertEqual(set(read_checks(self.store)["checks"]), {"GOOG"})
        read_receipt = storage.read_receipt

        def selected_only(root, symbol, *args, **kwargs):
            self.assertNotEqual(symbol, "OTHER", "unrelated receipt opened")
            return read_receipt(root, symbol, *args, **kwargs)

        with patch.object(storage, "read_receipt", side_effect=selected_only):
            self.assertEqual(self.build()["results"][0]["processed"], ["GOOG"])
        self.assertEqual(before, snapshot(self.store / "end_of_day/lineage/OTHER"))
        self.assertEqual(StandaloneReader(self.store).resolve_ticker("GOOG")["matches"], prior_matches)
        self.assertEqual(read_checks(self.store)["checks"], {})

    def test_conservative_hint_is_acknowledged_without_republishing(self):
        self.download()
        self.build()
        before = snapshot(self.store / "end_of_day")
        queue_check(self.store, "GOOG")
        with patch.object(pipeline, "acquire", side_effect=AssertionError("metadata")):
            self.assertEqual(self.build()["results"][0]["status"], "unchanged")
        self.assertEqual(read_checks(self.store)["checks"], {})
        self.assertEqual(before, snapshot(self.store / "end_of_day"))

    def test_failed_scan_never_establishes_complete_work_list(self):
        self.download()
        with patch.object(pipeline, "acquire", side_effect=RuntimeError("metadata failure")):
            result = pipeline.run_lineage(self.store, identity_client=self.client)
        self.assertEqual(result["failed"], 1)
        self.assertFalse(read_checks(self.store)["complete"])
        self.assertIn("GOOG", read_checks(self.store)["checks"])
        self.build()
        self.assertTrue(read_checks(self.store)["complete"])

    def test_newer_hint_and_recreated_file_survive_old_acknowledgement(self):
        self.download()
        expected = read_checks(self.store)
        queue_check(self.store, "GOOG")
        acknowledge_checks(self.store, expected, ["GOOG"])
        self.assertIn("GOOG", read_checks(self.store)["checks"])
        (self.store / CHECK_FILE).unlink()
        replacement = begin_checks(self.store)
        queue_check(self.store, "GOOG")
        acknowledge_checks(self.store, expected, ["GOOG"])
        complete_checks(self.store, expected["epoch"])
        current = read_checks(self.store)
        self.assertEqual(current["epoch"], replacement["epoch"])
        self.assertFalse(current["complete"])
        self.assertIn("GOOG", current["checks"])

    def test_new_hint_during_full_run_remains_for_next_invocation(self):
        self.download()
        original = pipeline.process_group

        def arrival(*args, **kwargs):
            result = original(*args, **kwargs)
            self.download("OTHER")
            return result

        with patch.object(pipeline, "process_group", side_effect=arrival):
            self.build()
        self.assertTrue(read_checks(self.store)["complete"])
        self.assertEqual(set(read_checks(self.store)["checks"]), {"OTHER"})
        self.assertEqual(self.build()["results"][0]["processed"], ["OTHER"])

    def test_recovered_publication_requeues_after_work_list_deletion(self):
        self.download()
        self.build()
        target = self.store / "end_of_day/pricing/GOOG/source.manifest.json"
        replace = pricing_storage.os.replace

        def interrupt(source, destination):
            if Path(destination) == target:
                raise OSError("interrupted source publication")
            return replace(source, destination)

        with patch.object(pricing_storage.os, "replace", side_effect=interrupt), self.assertRaises(OSError):
            self.download(end="2026-10-15")
        (self.store / CHECK_FILE).unlink()
        with patch.object(FakeClient, "fetch", side_effect=AssertionError("provider request")):
            result = self.download(end="2026-10-15")
        self.assertTrue(result["recovered"])
        self.assertIn("GOOG", read_checks(self.store)["checks"])
        self.assertFalse(read_checks(self.store)["complete"])
        self.assertEqual(self.build()["results"][0]["status"], "processed")

    def test_cache_recreated_during_publication_retains_change(self):
        self.download()
        self.build()
        target = self.store / "end_of_day/pricing/GOOG/source.manifest.json"
        replace = pricing_storage.os.replace

        def recreate(source, destination):
            result = replace(source, destination)
            if Path(destination) == target:
                (self.store / CHECK_FILE).unlink()
                checks = begin_checks(self.store)
                complete_checks(self.store, checks["epoch"])
            return result

        with patch.object(pricing_storage.os, "replace", side_effect=recreate):
            self.download(end="2026-10-15")
        self.assertIn("GOOG", read_checks(self.store)["checks"])
        self.assertEqual(self.build()["results"][0]["status"], "processed")

    def test_lineage_recovery_after_hint_deletion_leaves_notice_for_fallback(self):
        self.download()
        with patch.object(storage, "_acknowledge", side_effect=OSError("interrupted acknowledgement")):
            result = pipeline.run_lineage(self.store, identity_client=self.client)
        self.assertEqual(result["failed"], 1)
        (self.store / CHECK_FILE).unlink()
        with storage.writer_lock(self.store) as root:
            self.assertTrue(storage.recover(self.store, root))
        self.assertTrue((self.store / "end_of_day/manifests/GOOG/update.json").exists())
        self.build()
        self.assertFalse((self.store / "end_of_day/manifests/GOOG/update.json").exists())
        self.assertTrue(read_checks(self.store)["complete"])

    def test_large_unchanged_index_requires_no_individual_receipt_checks(self):
        self.download()
        self.build()
        index = storage.read_index(self.store / "end_of_day/lineage")
        current = versions()
        index["symbols"].update(
            {f"T{number}": {"dependencies": [], "asset_ids": [], "versions": current} for number in range(6000)}
        )
        with (
            patch.object(storage, "read_index", return_value=index),
            patch.object(pipeline, "local_symbols", side_effect=AssertionError("source scan")),
            patch.object(pipeline, "process_group", side_effect=AssertionError("receipt opened")),
        ):
            self.assertEqual(self.build()["results"], [])

    def test_lookup_preserves_reverse_links_shared_uuids_and_link_removal(self):
        graph = DependencyGraph(
            {
                "A": {"dependencies": ["B"], "asset_ids": []},
                "C": {"dependencies": [], "asset_ids": ["one"]},
                "B": {"dependencies": [], "asset_ids": ["one"]},
                "D": {"dependencies": [], "asset_ids": []},
            }
        )
        self.assertEqual(graph.closure({"C"}), {"A", "B", "C"})
        self.assertEqual(graph.closure({"D"}), {"D"})
        graph.update({"B": {"dependencies": [], "asset_ids": ["two"]}})
        self.assertEqual(graph.closure({"C"}), {"C"})
        self.assertEqual(graph.closure({"B"}), {"A", "B"})


if __name__ == "__main__":
    unittest.main()

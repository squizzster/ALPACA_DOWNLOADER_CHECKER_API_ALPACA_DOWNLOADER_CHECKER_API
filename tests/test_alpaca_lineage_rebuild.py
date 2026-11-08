"""Explicit full retained-input repair has durable, generation-bound progress."""

import copy
import unittest
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture

from libs import alpaca_cli
from libs.alpaca_lineage_build import pipeline, storage
from libs.alpaca_lineage_build.contract import COMPUTATION_REVISION
from libs.alpaca_lineage_build.rebuild import rebuild
from libs.alpaca_pricing_download import storage as pricing_storage


class LineageRebuildTests(unittest.TestCase):
    def obsolete(self, case):
        root = case.store / "end_of_day/lineage"
        index = storage.read_index(root)
        updates = {}
        for symbol, entry in index["symbols"].items():
            receipt = storage.read_receipt(root, symbol, index)
            receipt.pop("computation_revision", None)
            storage.atomic_json(root / symbol / "lineage.manifest.json", receipt)
            updates[symbol] = {**entry, "sha256": storage.document_digest(receipt)}
            updates[symbol].pop("computation_revision", None)
            updates[symbol]["projection"] = copy.deepcopy(entry["projection"])
            updates[symbol]["projection"].pop("computation_revision", None)
        with storage.publication_lock(root, exclusive=True):
            storage._update_index(root, updates)

    def test_explicit_scope_and_provider_free_cli_plan(self):
        with fixture() as case:
            self.obsolete(case)
            with self.assertRaisesRegex(Exception, "all-published"):
                rebuild(case.store)
            args = alpaca_cli.build_parser().parse_args(
                ["lineage", "rebuild", "--all-published", "--plan", "--store", str(case.store)]
            )
            with (
                patch.object(alpaca_cli, "clients", side_effect=AssertionError("provider clients created")),
                patch.object(storage, "read_receipt", side_effect=AssertionError("plan opened receipt")),
            ):
                result = alpaca_cli.dispatch(args)
            self.assertEqual(result["mode"], "plan")
            self.assertEqual(result["selected_symbols"], 1)
            self.assertEqual(result["provider_requests"], 0)
            self.assertEqual(result["computation_revision"], COMPUTATION_REVISION)

    def test_obsolete_computation_rebuilds_from_accepted_inputs_then_retry_skips(self):
        with fixture() as case:
            self.obsolete(case)
            root = case.store / "end_of_day/lineage"
            before = storage.read_index(root)["symbols"]["AAA"]["source_sha256"]
            with (
                patch.object(pricing_storage, "publish", side_effect=AssertionError("pricing rewritten")),
                patch.object(pipeline, "correlate", wraps=pipeline.correlate) as evaluated,
            ):
                result = rebuild(case.store, all_published=True)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["remaining_symbols"], [])
            self.assertEqual(result["recomputed_symbols"], ["AAA"])
            self.assertEqual(evaluated.call_count, 1)
            self.assertEqual(result["provider_requests"], 0)
            self.assertEqual(result["pricing_writes"], 0)
            current = storage.read_index(root)["symbols"]["AAA"]
            self.assertEqual(current["source_sha256"], before)
            self.assertEqual(current["computation_revision"], COMPUTATION_REVISION)
            with (
                patch.object(storage, "read_receipt", side_effect=AssertionError("completed receipt reopened")),
                patch.object(
                    pipeline, "_current_sources", side_effect=AssertionError("completed source reopened")
                ),
                patch.object(pipeline, "correlate", side_effect=AssertionError("completed lineage repeated")),
            ):
                repeated = rebuild(case.store, all_published=True)
            self.assertTrue(repeated["complete"], repeated)
            self.assertEqual(repeated["selected_symbols"], 0)
            self.assertEqual(repeated["already_current_symbols"], 1)

    def test_interrupted_publication_recovers_without_repeating_completed_calculation(self):
        with fixture() as case:
            self.obsolete(case)
            original = storage._install

            class Interrupted(BaseException):
                pass

            def interrupt(root, journal, *args, **kwargs):
                original(root, journal, *args, **kwargs)
                raise Interrupted()

            with patch.object(storage, "_install", side_effect=interrupt), self.assertRaises(Interrupted):
                rebuild(case.store, all_published=True)
            with (
                patch.object(
                    pipeline, "correlate", side_effect=AssertionError("committed computation replayed")
                ),
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("committed prices reread")),
            ):
                result = rebuild(case.store, all_published=True)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["selected_symbols"], 0)

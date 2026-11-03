"""Accepted facts survive reader, publication and recovery handoffs without repeated I/O."""

import unittest
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as fixtures
from test_alpaca_pricing_download import bar, snapshot

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build import check_summary, pipeline, storage
from libs.alpaca_lineage_build.evidence import output_evidence
from libs.alpaca_pricing_download.handoff import consume_pricing_update


class BoundaryCarryTests(unittest.TestCase):
    setUp = fixtures.LineageBuildTests.setUp
    download = fixtures.LineageBuildTests.download
    run_build = fixtures.LineageBuildTests.run_build

    @contextmanager
    def reads(self):
        counts = Counter()
        original = Path.read_bytes

        def read(path):
            if path.is_relative_to(self.store):
                counts[str(path.relative_to(self.store))] += 1
            return original(path)

        with patch.object(Path, "read_bytes", read):
            yield counts

    def published(self):
        self.download()
        self.run_build(["GOOG"])
        check_summary.rebuild(self.store)

    def test_membership_selection_does_not_open_a_receipt(self):
        self.published()
        with patch.object(storage, "read_receipt", side_effect=AssertionError("rediscovered membership")):
            selected = storage.select_inputs(self.root, "GOOG")
        self.assertEqual(set(selected), {"GOOG"})

    def test_each_reader_operation_accepts_selected_bodies_once_without_a_persistent_cache(self):
        self.published()
        reader = StandaloneReader(self.store)
        for name, operation in (
            ("generation", lambda: reader.get_generation("GOOG")),
            ("history", lambda: reader.get_histories("GOOG", ("raw",))),
            ("resolve", lambda: reader.resolve_ticker("GOOG")),
            ("evidence", lambda: reader.get_evidence("GOOG")),
            ("offline", lambda: output_evidence(self.store, "GOOG", offline=True)),
        ):
            with self.subTest(operation=name), self.reads() as reads:
                operation()
                self.assertEqual(reads["end_of_day/lineage/GOOG/lineage.manifest.json"], 1)

    def test_unchanged_lineage_does_not_reproject_summary_membership_or_read_endpoints(self):
        self.published()
        before = snapshot(self.store)
        with (
            patch.object(check_summary, "project", side_effect=AssertionError("reprojected completed summary")),
            self.reads() as reads,
        ):
            result = self.run_build(["GOOG"])
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(reads["end_of_day/pricing/GOOG/validator_first_last_sessions.json"], 0)
        self.assertEqual(before, snapshot(self.store))

    def test_metadata_publication_reuses_the_completed_pricing_summary(self):
        self.published()
        before = snapshot(self.store / "end_of_day/pricing")
        client = fixtures.IdentityClient({"GOOG": {**fixtures.asset("GOOG"), "name": "Updated description"}})
        with self.reads() as reads:
            result = self.run_build(["GOOG"], client=client, refresh=True)
        self.assertEqual(result["results"][0]["status"], "processed")
        self.assertEqual(reads["end_of_day/pricing/GOOG/validator_first_last_sessions.json"], 0)
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing"))

    def test_recovery_carries_its_newly_selected_source_and_notice_through_acknowledgement(self):
        self.published()
        self.download(end="2026-10-15", bars=[*self.bars, bar("2026-10-02")])
        with patch.object(
            storage, "_acknowledge", side_effect=RuntimeError("interrupted before acknowledgement")
        ):
            result = pipeline.run_lineage(self.store, ["GOOG"], identity_client=fixtures.IdentityClient())
        self.assertEqual(result["failed"], 1)
        with self.reads() as reads, storage.writer_lock(self.store) as root:
            self.assertTrue(storage.recover(self.store, root))
        for path in ("end_of_day/pricing/GOOG/source.manifest.json", "end_of_day/manifests/GOOG/update.json"):
            self.assertEqual(reads[path], 1, path)
        # Recovery already carries newly installed receipts in its owning journal.
        # At most the idempotent installation comparison needs the existing body.
        self.assertLessEqual(reads["end_of_day/lineage/GOOG/lineage.manifest.json"], 1)
        self.assertFalse((self.root / ".pending.json").exists())

    def test_pricing_handoff_retains_its_values_and_expires_on_close(self):
        self.download()
        with self.reads() as reads, consume_pricing_update(self.store, "GOOG") as handoff:
            first = handoff.read_views()
            second = handoff.read_views()
            self.assertEqual(first, second)
            first.clear()  # A consumer cannot corrupt the retained view map.
            self.assertTrue(handoff.read_views())
        for path, count in reads.items():
            self.assertEqual(count, 1, path)
        with self.assertRaisesRegex(RuntimeError, "no longer locked"):
            handoff.read_views()

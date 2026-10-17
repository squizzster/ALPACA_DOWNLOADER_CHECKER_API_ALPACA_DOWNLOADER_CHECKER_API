"""Export consumes completed public reader projections, never another resolver."""

import fcntl
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_standalone_reader as reader_fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import snapshot

from alpaca_end_of_day import ReaderBusyError, StandaloneReader, StandaloneReaderError
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class StandaloneExportProjectionTests(unittest.TestCase):
    # Reuse only the synthetic publishing helpers, not the existing test suite.
    setUp = reader_fixtures.StandaloneReaderTests.setUp
    download = reader_fixtures.StandaloneReaderTests.download
    build = reader_fixtures.StandaloneReaderTests.build
    publication = reader_fixtures.StandaloneReaderTests.publication
    change_publication = reader_fixtures.StandaloneReaderTests.change_publication
    renamed_pair = reader_fixtures.StandaloneReaderTests.renamed_pair

    def test_roster_and_evidence_are_metadata_only_and_read_only(self):
        before = snapshot(self.store)
        original = Path.read_bytes

        def read(path):
            self.assertNotEqual(path.suffix, ".gz", "metadata projection opened pricing")
            return original(path)

        with patch.object(Path, "read_bytes", read):
            roster = self.reader.list_published_sources()
            evidence = self.reader.get_evidence("TEST")
        self.assertEqual(before, snapshot(self.store))
        self.assertEqual([item["symbol"] for item in roster["sources"]], ["TEST"])
        selected = storage.read_index(self.root)["symbols"]["TEST"]["sha256"]
        self.assertEqual(roster["sources"][0]["generation"], selected)
        self.assertEqual(evidence["sources"]["TEST"]["lineage_generation"], selected)
        self.assertEqual(evidence["source_symbols"], ["TEST"])
        self.assertEqual(evidence["securities"][0]["read_symbol"], "TEST")
        self.assertEqual(evidence["securities"][0]["latest_tickers"], ["TEST"])

    def test_projection_retains_published_facts_without_action_or_exchange_inference(self):
        result = self.reader.get_evidence("TEST")
        source = result["sources"]["TEST"]
        receipt, _ = self.publication()
        self.assertEqual(source["evidence"], receipt["evidence"])
        self.assertEqual(source["pricing_generation"], receipt["source_sha256"])
        self.assertEqual(source["pricing_request"]["asof"], "2026-10-14")
        self.assertEqual(source["history_start"], "2016-01-01T00:00:00Z")
        self.assertEqual(source["processed_through"], "2026-10-14T00:00:00Z")
        self.assertEqual(source["evidence"]["identities"][0]["exchange"], "NASDAQ")
        self.assertNotIn("dated_exchange", source)
        self.assertNotIn("security_actions", result["securities"][0])
        self.assertEqual(len(source["assignments"]), 7)  # One per observed month, not eight copies.
        self.assertTrue(all(item["adjustments"] == list(ADJUSTMENTS) for item in source["assignments"]))
        self.assertEqual([item["from"] for item in source["assignments"]], [r["t"][:10] for r in self.bars[:-1]])

    def test_history_evidence_is_the_same_single_group_snapshot(self):
        with (
            patch.object(self.reader, "_snapshot", wraps=self.reader._snapshot) as selected,
            patch.object(self.reader, "get_evidence", side_effect=AssertionError("second metadata snapshot")),
        ):
            result = self.reader.get_histories("TEST", include_evidence=True)
        selected.assert_called_once_with("TEST")
        self.assertEqual(result["generation"], result["evidence"]["generation"])
        self.assertEqual(result["security"]["public_id"], result["evidence"]["securities"][0]["public_id"])
        self.assertNotIn("evidence", self.reader.get_histories("TEST"))
        self.assertNotIn("evidence", self.reader.get_history("TEST"))
        missing = StandaloneReader(self.store / "missing")
        for value in (None, 0, 1, "yes"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "include_evidence"):
                missing.get_histories("TEST", include_evidence=value)

    def test_group_projects_one_continuation_and_its_latest_observed_label(self):
        self.renamed_pair()
        result = self.reader.get_evidence("OLD")
        self.assertEqual(result["source_symbols"], ["NEW", "OLD"])
        self.assertEqual(len(result["securities"]), 1)
        security = result["securities"][0]
        self.assertEqual(security["tickers"], ["NEW", "OLD"])
        self.assertEqual(security["latest_tickers"], ["NEW"])
        self.assertEqual(security["read_symbol"], "NEW")
        history = self.reader.get_histories(security["read_symbol"], public_id=security["public_id"])
        self.assertEqual(history["generation"], result["generation"])
        self.assertEqual(self.reader.get_evidence("NEW")["securities"], result["securities"])
        self.assertEqual(
            [item["symbol"] for item in self.reader.list_published_sources()["sources"]], ["NEW", "OLD", "TEST"]
        )

    def test_reused_ticker_does_not_collapse_ids_or_guess_latest_label_tie(self):
        alternate = security_id_for_alpaca_asset_ids(["unrelated-id"])

        def change(_, boundaries):
            for entry in boundaries["current_month"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["security_identity"]["security_id"] = alternate
                    if entry["adjustment"] == "all":
                        segment["historical_symbol"] = "ALTERNATE"

        self.change_publication(change)
        securities = self.reader.get_evidence("TEST")["securities"]
        self.assertEqual(len(securities), 2)
        selected = next(item for item in securities if item["public_id"] == alternate)
        self.assertEqual(selected["latest_tickers"], ["ALTERNATE", "TEST"])
        self.assertEqual(selected["read_symbol"], "TEST")

    def test_unresolved_assignments_are_not_invented_as_securities(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["branch"] = "uncorrelated"

        self.change_publication(change)
        result = self.reader.get_evidence("TEST")
        self.assertEqual(result["securities"], [])
        self.assertEqual(result["sources"]["TEST"]["assignments"], [])
        self.assertEqual(len(self.reader.list_published_sources()["sources"]), 1)

    def test_generation_changes_when_a_publication_changes(self):
        previous = self.reader.list_published_sources()
        self.download("OTHER", self.bars)
        self.build(["OTHER"], IdentityClient({"OTHER": asset("OTHER")}))
        current = self.reader.list_published_sources()
        self.assertNotEqual(previous["generation"], current["generation"])
        self.assertEqual(current["sources"][1], previous["sources"][0])
        old_group = self.reader.get_evidence("TEST")
        self.change_publication(lambda receipt, _: receipt["evidence"]["warnings"].append("new evidence"))
        self.assertNotEqual(old_group["generation"], self.reader.get_evidence("TEST")["generation"])

    def test_stale_group_remains_a_roster_entry_but_cannot_supply_evidence(self):
        self.change_publication(lambda receipt, _: receipt["versions"].update(correlation=1))
        self.assertEqual(len(self.reader.list_published_sources()["sources"]), 1)
        with self.assertRaisesRegex(StandaloneReaderError, "older rules"):
            self.reader.get_evidence("TEST")

    def test_roster_honors_publication_lock_and_incomplete_transaction(self):
        descriptor = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            with self.assertRaises(ReaderBusyError):
                self.reader.list_published_sources()
        finally:
            os.close(descriptor)
        with patch(
            "alpaca_end_of_day.standalone.read_index", side_effect=AssertionError("read interrupted index")
        ):
            with patch.object(Path, "exists", return_value=True):
                with self.assertRaisesRegex(StandaloneReaderError, "publication incomplete"):
                    self.reader.list_published_sources()

    def test_missing_store_is_not_created(self):
        missing = self.store.parent / "absent"
        with self.assertRaises(StandaloneReaderError):
            StandaloneReader(missing).list_published_sources()
        self.assertFalse(missing.exists())

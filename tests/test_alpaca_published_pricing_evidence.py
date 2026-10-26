"""Disposition consumes accepted pricing facts, never legacy filesystem proof."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_pricing_download as fixtures
from test_alpaca_security_lineage import MissingIdentityClient

from libs.alpaca_lineage_build import pipeline
from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.pricing import load_pricing
from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.lineage import build_identity_report
from libs.alpaca_security_lineage.symbol_disposition import classify_symbol_disposition


class PublishedPricingDispositionTests(unittest.TestCase):
    setUp = fixtures.PricingDownloadTests.setUp
    run_download = fixtures.PricingDownloadTests.run_download

    def view(self, **kwargs):
        with storage.ticker_lock(self.root, exclusive=True):
            return load_pricing(self.store, "GOOG", **kwargs)

    def test_current_empty_and_populated_publications_reach_the_published_disposition(self):
        for populated in (False, True):
            with self.subTest(populated=populated):
                self.store = self.store.parent / str(populated)
                self.root = self.store / "end_of_day/pricing/GOOG"
                self.run_download(bars=self.bars if populated else [])
                before = fixtures.snapshot(self.root)
                view = self.view()
                self.assertEqual(view.source["schema_version"], 31)
                expected = "historical_market_data_only" if populated else "unsupported_empty_symbol"
                self.assertNotIn("pipeline_receipt_matches", view.local["downloads"][0])
                with (
                    patch.object(Path, "read_bytes", side_effect=AssertionError("upstream read")),
                    patch.object(Path, "stat", side_effect=AssertionError("upstream stat")),
                ):
                    disposition = build_identity_report("GOOG", view.local, MissingIdentityClient())[
                        "symbol_disposition"
                    ]
                self.assertEqual(disposition["classification"], expected)
                self.assertTrue(disposition["local_market_data"]["complete_row_count_evidence"])
                result = run_lineage(self.store, ["GOOG"], identity_client=MissingIdentityClient())
                self.assertEqual(result["failed"], 0, result)
                root = self.store / "end_of_day/lineage"
                index = lineage_storage.read_index(root, "GOOG")
                receipt = lineage_storage.read_receipt(root, "GOOG", index)
                self.assertEqual(receipt["schema_version"], 34)
                self.assertEqual(receipt["evidence"]["symbol_disposition"]["classification"], expected)
                series = receipt["evidence"]["symbol_disposition"]["market_data_series_identity"]
                if populated:
                    self.assertFalse(series["is_security_identity"])
                else:
                    self.assertIsNone(series)
                self.assertEqual(before, fixtures.snapshot(self.root))
                completed = fixtures.snapshot(self.store)
                with (
                    patch.object(pipeline, "load_pricing", side_effect=AssertionError("pricing reopened")),
                    patch.object(pipeline, "acquire", side_effect=AssertionError("evidence reacquired")),
                    patch.object(pipeline, "correlate", side_effect=AssertionError("decision recalculated")),
                ):
                    repeated = run_lineage(self.store, ["GOOG"], identity_client=MissingIdentityClient())
                self.assertEqual(repeated["failed"], 0, repeated)
                self.assertEqual(repeated["results"][0]["status"], "unchanged")
                self.assertEqual(completed, fixtures.snapshot(self.store))

    def test_partial_reads_hand_off_complete_counts_including_retained_rows(self):
        self.run_download()
        result = run_lineage(self.store, ["GOOG"], identity_client=MissingIdentityClient())
        self.assertEqual(result["failed"], 0)
        root = self.store / "end_of_day/lineage"
        index = lineage_storage.read_index(root, "GOOG")
        receipt = lineage_storage.read_receipt(root, "GOOG", index)
        boundaries = lineage_storage.read_boundaries(root, receipt)
        for options in (
            {"active_only": True, "previous_boundaries": boundaries},
            {"resume_from": "2026-10-14", "previous": receipt, "previous_boundaries": boundaries},
        ):
            with self.subTest(options=tuple(options)):
                view = self.view(**options)
                self.assertLess(len(view.views["raw"]), len(self.bars))
                self.assertEqual(
                    view.local["published_pricing"]["row_counts"], dict.fromkeys(ADJUSTMENTS, len(self.bars))
                )
                disposition = build_identity_report("GOOG", view.local, MissingIdentityClient())[
                    "symbol_disposition"
                ]
                self.assertEqual(disposition["classification"], "historical_market_data_only")

    def test_invalid_current_handoff_cannot_inherit_legacy_completeness(self):
        self.run_download()
        local = self.view().local
        for descriptor in local["downloads"]:
            descriptor.update(
                sidecar_readable=True,
                gzip_present=True,
                stat_matches_sidecar=True,
                pipeline_receipt_matches=True,
            )
        base = local["published_pricing"]
        invalid = [
            {**base, "schema_version": 2},
            {**base, "schema_version": True},
            {**base, "symbol": "OTHER"},
            {**base, "generation": "not-a-generation"},
            {**base, "request_to": base["request_from"]},
            {**base, "asof": "invalid"},
            {**base, "feed": ""},
            {**base, "row_counts": {"raw": 5}},
            {**base, "row_counts": {**base["row_counts"], "raw": -1}},
            {**base, "row_counts": {**base["row_counts"], "raw": True}},
        ]
        lookup = [{"lookup_kind": "symbol", "lookup": "GOOG", "outcome": "not_found", "http_status": 404}]
        for handoff in invalid:
            with self.subTest(handoff=handoff):
                value = {**copy.deepcopy(local), "published_pricing": handoff}
                disposition = classify_symbol_disposition("GOOG", value, lookup, [], [])
                self.assertEqual(disposition["classification"], "unresolved_symbol_disposition")
                self.assertFalse(disposition["local_market_data"]["complete_row_count_evidence"])

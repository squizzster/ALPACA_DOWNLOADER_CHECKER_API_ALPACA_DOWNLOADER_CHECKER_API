"""Metadata coverage reports preserve accepted decisions and expose unknowns."""

import copy
import json
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from alpaca_end_of_day import LineageDebugError, debug_lineage
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_debug.coverage import coverage


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.store = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.root = self.store / "end_of_day/lineage"
        self.root.mkdir(parents=True)
        universe_state.initialize(self.store)
        universe_state.put_work(
            self.store,
            "RESOLVED",
            {
                "status": "retry",
                "reasons": ["sourced_facts"],
                "outcome": {"error": "missing producer handoff"},
                "obligation_id": "held-obligation",
                "pricing_correction": {"not_for_status_to_validate": True},
            },
        )
        self.index = {
            "projection_state": {"schema_version": 1, "complete": True, "revision": 10},
            "symbols": {
                name: {
                    "sha256": name + "-receipt",
                    "source_sha256": name + "-prices",
                    "projection": {
                        "schema_version": 1,
                        "source_rows": rows,
                        "unresolved_rows": unresolved,
                        "processed_through": "2026-11-12",
                    },
                }
                for name, rows, unresolved in (("RESOLVED", 8, 0), ("OPEN", 24, 24), ("EMPTY", 0, 0))
            },
        }
        self.boundaries = {
            "completed_years": {"adjustments": []},
            "current_year": {"adjustments": []},
            "current_month": {
                "adjustments": [
                    {
                        "adjustment": "raw",
                        "segments": [
                            {"branch": "uncorrelated", "row_count": 1, "reason_codes": [reason]}
                            for reason in (
                                "ambiguous_security_identity_candidates",
                                "no_security_identity_evidence",
                            )
                        ]
                        + [{"branch": "uncorrelated", "row_count": 1}],
                    }
                ]
            },
        }

    def guards(self):
        guards = ExitStack()
        for target in (
            "libs.alpaca_pricing_download.correction.validate_durable_observations",
            "libs.alpaca_lineage_build.storage.load_source",
            "libs.alpaca_lineage_build.transform.correlate",
        ):
            guards.enter_context(patch(target, side_effect=AssertionError("no upstream replay")))
        guards.enter_context(patch.object(storage, "check_store"))
        return guards

    def test_compact_report_reads_no_receipts_and_separates_work_from_allocation(self):
        with (
            self.guards(),
            patch.object(storage, "read_index", return_value=self.index) as index,
            patch.object(storage, "read_receipt", side_effect=AssertionError("not selected")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("not selected")),
        ):
            report = coverage(self.store)
        self.assertEqual(index.call_count, 1)
        self.assertEqual(report["summary"]["resolved"], 1)
        self.assertEqual(report["summary"]["empty"], 1)
        self.assertEqual(report["summary"]["sources_with_unclassified"], 1)
        self.assertEqual(report["summary"]["pending_work"], 1)
        self.assertEqual(report["sources"]["RESOLVED"]["allocation"], "resolved")
        self.assertEqual(report["pending_work"]["RESOLVED"]["obligation_id"], "held-obligation")
        self.assertEqual(report["expected_io"]["receipt_reads"], 0)

    def test_details_select_only_unresolved_metadata_once_and_keep_missing_reasons_unknown(self):
        before = copy.deepcopy(self.boundaries)
        with (
            self.guards(),
            patch.object(storage, "read_index", return_value=self.index),
            patch.object(storage, "read_receipt", return_value={"anchor_month": "2026-11"}) as receipts,
            patch.object(storage, "read_boundaries", return_value=self.boundaries) as boundaries,
        ):
            report = coverage(self.store, details=True)
        self.assertEqual(receipts.call_count, 1)
        self.assertEqual(receipts.call_args.args[1], "OPEN")
        self.assertEqual(boundaries.call_count, 1)
        self.assertEqual(
            report["sources"]["OPEN"]["unresolved_class_rows"],
            {"ambiguous": 1, "unsupported": 1, "unclassified": 1},
        )
        self.assertEqual(before, self.boundaries)

    def test_missing_required_metadata_is_incomplete_without_automatic_repair(self):
        with (
            self.guards(),
            patch.object(storage, "read_index", return_value=self.index),
            patch.object(storage, "read_receipt", side_effect=FileNotFoundError("selected receipt absent")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("no fallback")),
        ):
            report = coverage(self.store, details=True)
        self.assertFalse(report["execution_complete"])
        self.assertEqual(report["failures"][0]["symbol"], "OPEN")
        self.assertEqual(report["summary"]["resolved"], 1)

    def test_completed_details_survive_new_workflow_and_only_changed_generations_read_again(self):
        def request(**options):
            return json.loads(
                debug_lineage(
                    json.dumps(
                        {
                            "schema": "alpaca.lineage.debug.request",
                            "schema_version": 1,
                            "operation": "coverage",
                            "store": str(self.store),
                            "unresolved_details": True,
                            **options,
                        }
                    ).encode()
                )
            )

        saved = self.store.parent / (self.store.name + "-coverage.json")
        self.addCleanup(saved.unlink, missing_ok=True)
        with (
            self.guards(),
            patch.object(storage, "read_index", return_value=self.index),
            patch.object(storage, "read_receipt", return_value={"anchor_month": "2026-11"}),
            patch.object(storage, "read_boundaries", return_value=self.boundaries),
        ):
            first = request()
        saved.write_text(json.dumps(first))
        universe_state.put_work(self.store, "RESOLVED", {"status": "complete"})
        with (
            self.guards(),
            patch.object(storage, "read_index", return_value=self.index),
            patch.object(storage, "read_receipt", side_effect=AssertionError("completed metadata")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("completed explanations")),
        ):
            carried = request(reuse_report=str(saved))
        self.assertEqual(carried["reads"]["reused_sources"], 1)
        self.assertEqual(carried["expected_io"]["receipt_reads"], 0)
        self.assertEqual(carried["pending_work"], {})
        self.index["symbols"]["OPEN"]["sha256"] = "genuinely-new-receipt"
        with (
            self.guards(),
            patch.object(storage, "read_index", return_value=self.index),
            patch.object(storage, "read_receipt", return_value={"anchor_month": "2026-11"}) as receipts,
            patch.object(storage, "read_boundaries", return_value=self.boundaries),
        ):
            changed = request(reuse_report=str(saved))
        self.assertEqual(receipts.call_count, 1)
        self.assertEqual(changed["reads"]["reused_sources"], 0)

    def test_public_request_requires_explicit_boolean_detail_scope(self):
        with self.assertRaisesRegex(LineageDebugError, "boolean"):
            debug_lineage(
                json.dumps(
                    {
                        "schema": "alpaca.lineage.debug.request",
                        "schema_version": 1,
                        "operation": "coverage",
                        "store": str(self.store),
                        "unresolved_details": "true",
                    }
                ).encode()
            )


if __name__ == "__main__":
    unittest.main()

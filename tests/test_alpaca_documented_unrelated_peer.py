"""New common chronology must not reopen unrelated accepted unresolved peers."""

from unittest import TestCase
from unittest.mock import patch

import test_alpaca_documented_market_pipeline as fixtures
from test_alpaca_lineage_build import informative_bars
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_lineage_build import documented_work, pipeline, storage
from libs.alpaca_pricing_download.download import download_pricing


class DocumentedUnrelatedPeerTests(TestCase):
    def setUp(self):
        original = pipeline.correlate

        def unresolved_peer(*args, **kwargs):
            reports = original(*args, **kwargs)
            if "PEER" in reports:
                report = reports["PEER"]
                rows = report["correlated"]["segments"] + report["uncorrelated"]["segments"]
                report["correlated"] = {"row_count": 0, "segments": []}
                report["uncorrelated"] = {
                    "row_count": report["source_row_count"],
                    "segments": [
                        {
                            **{
                                key: row[key]
                                for key in (
                                    "adjustment",
                                    "row_start",
                                    "row_end",
                                    "row_count",
                                    "first_timestamp",
                                    "last_timestamp",
                                )
                            },
                            "reason_codes": ["unresolved_uuid_candidates"],
                            "candidate_securities": [],
                        }
                        for row in rows
                    ],
                }
            return reports

        with patch.object(pipeline, "correlate", side_effect=unresolved_peer):
            fixtures.DocumentedMarketPipelineTests.setUp(self)
        self.assertEqual(48, self.prior["PEER"]["unresolved_rows"])

    def test_new_documented_owner_keeps_unrelated_completed_unresolved_peer_closed(self):
        prepare = documented_work.prepare
        plans = []

        def selected(*args, **kwargs):
            plan = prepare(*args, **kwargs)
            plans.append(plan)
            return plan

        with patch.object(documented_work, "prepare", side_effect=selected):
            fixtures.DocumentedMarketPipelineTests.repair(self)
        self.assertTrue(plans)
        self.assertTrue(all(plan is not None for plan in plans))
        self.assertTrue(all(plan.dispositions["PEER"] == "retained" for plan in plans))
        current = storage.read_receipt(self.root, "PEER", storage.read_index(self.root))
        self.assertEqual(self.prior["PEER"]["unresolved_rows"], current["unresolved_rows"])
        self.assertEqual(self.prior["PEER"]["assignment_summary"], current["assignment_summary"])
        self.assertEqual(self.prior["PEER"]["boundaries"], current["boundaries"])

    def test_unrelated_peer_with_a_real_append_still_selects_owning_reconciliation(self):
        for symbol in ("FILL", "PEER"):
            download_pricing(
                self.store,
                symbol,
                "2024-01-11",
                from_date="2024-01-01",
                client=FakeClient(informative_bars("2024-01-02", "2024-01-11")),
            )
        before = storage.read_index(self.root)["symbols"]
        prepare = documented_work.prepare
        observed = []

        class OwningReconciliationSelected(BaseException):
            pass

        def selected(controls, *args, **kwargs):
            peer = controls["PEER"]
            self.assertNotEqual(peer.previous["source_sha256"], peer.source_sha256)
            self.assertTrue(peer.accepted_reuse[1], "append lost its completed prefix applicability")
            plan = prepare(controls, *args, **kwargs)
            observed.append(plan)
            self.assertIsNone(plan, "new peer sessions were hidden as an unchanged accepted source")
            # The documented-only owner has correctly declined this wider work.
            # Stop here rather than exercise or approve a different row producer.
            raise OwningReconciliationSelected()

        with (
            patch.object(documented_work, "prepare", side_effect=selected),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("premature price assembly")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("premature historical row replay")),
        ):
            try:
                result = pipeline.run_lineage(
                    self.store, ["FILL"], identity_client=self.client, event_supplements=self.facts
                )
            except OwningReconciliationSelected:
                pass
            else:
                self.fail(str(result))
        self.assertEqual([None], observed)
        self.assertEqual(before, storage.read_index(self.root)["symbols"])

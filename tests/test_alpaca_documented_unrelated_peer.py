"""A closed generic source completion preserves an unrelated ownerless peer."""

import copy
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient

from libs.alpaca_end_of_day_security_correlation import row_correlation
from libs.alpaca_lineage_build import metadata, pipeline, source_work, storage, transform
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_pricing_download.download import download_pricing


class DocumentedUnrelatedPeerTests(TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.client = IdentityClient(
            {
                "FILL": asset("FILL", "bank"),
                "PEER": asset("PEER", "peer"),
                "111111111": asset("FILL", "bank"),
                "333333333": asset("PEER", "peer"),
            },
            [
                {
                    "id": "distinct-target",
                    "action_type": "stock_merger",
                    "acquiree_symbol": "FILL",
                    "acquirer_symbol": "PEER",
                    "acquiree_cusip": "111111111",
                    "acquirer_cusip": "333333333",
                    "acquiree_rate": 1,
                    "acquirer_rate": 0.42,
                    "effective_date": "2024-01-05",
                    "process_date": "2024-01-05",
                }
            ],
        )
        for name, end in (("FILL", "2024-01-05"), ("PEER", "2024-01-10")):
            download_pricing(
                self.store,
                name,
                "2024-01-10",
                from_date="2024-01-01",
                client=FakeClient(informative_bars("2024-01-02", end)),
            )
        original = pipeline.correlate
        self.completed_reports = {}

        def closed_ownerless_fixture(*args, **kwargs):
            reports = original(*args, **kwargs)
            self.completed_reports.update(copy.deepcopy(reports))
            for report in reports.values():
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

        self.context = OperationContext(self.store)
        self.evidence_client = metadata.EvidenceClient(self.client)
        with patch.object(pipeline, "correlate", side_effect=closed_ownerless_fixture):
            result = pipeline.run_lineage(
                self.store,
                ["FILL"],
                identity_client=self.client,
                evidence_client=self.evidence_client,
                context=self.context,
            )
        self.assertEqual(0, result["failed"], result)
        index = storage.read_index(self.root)
        self.prior = {name: storage.read_receipt(self.root, name, index) for name in ("FILL", "PEER")}
        self.assertEqual(48, self.prior["PEER"]["unresolved_rows"])
        self.assertEqual(0, self.completed_reports["FILL"]["uncorrelated"]["row_count"])
        members = {
            identifier: segment["security_identity"]
            for segment in self.completed_reports["FILL"]["correlated"]["segments"]
            for identifier in segment["security_identity"]["member_alpaca_asset_ids"]
        }
        empty = {"version": 1, "certificates": [], "issues": []}
        relations = {"version": 1, "relations": [], "issues": []}
        self.plan = SourceWorkPlan(
            {"FILL": "B", "PEER": "A"},
            members,
            empty,
            empty,
            relations,
            relations,
            {},
            False,
            prepared_reports={"FILL": self.completed_reports["FILL"]},
        )
        self.context.completed_membership_plans[("FILL", "PEER")] = {
            "store": str(self.store),
            "plan": {"changes": []},
        }

    def install_completion(self, controls, current, auxiliary, *, cache, **_):
        self.completed_reports["FILL"]["retained_observation_routes"] = controls["FILL"].previous[
            "observation_routes"
        ]
        binding = tuple((name, id(value)) for name, value in sorted({**current, **auxiliary}.items()))
        key = (
            binding,
            tuple(
                (name, control.store, control.source_sha256, control.previous_receipt_generation)
                for name, control in sorted(controls.items())
            ),
        )
        cache.setdefault("plans", {})[key] = self.plan
        return self.plan

    def test_new_generic_completion_keeps_accepted_ownerless_peer_closed(self):
        load = pipeline.load_pricing

        def selected(store, name, **kwargs):
            self.assertEqual("FILL", name, "accepted ownerless peer prices reopened")
            return load(store, name, **kwargs)

        with (
            patch.object(
                pipeline, "prepare_group_evidence_and_source_work", side_effect=self.install_completion
            ),
            patch.object(pipeline, "load_pricing", side_effect=selected),
            patch.object(transform, "correlate", side_effect=AssertionError("completed row cascade repeated")),
            patch.object(row_correlation, "_decide_row", side_effect=AssertionError("completed owner repeated")),
        ):
            result = pipeline.run_lineage(
                self.store,
                ["FILL"],
                identity_client=self.client,
                evidence_client=self.evidence_client,
                context=self.context,
            )
        self.assertEqual(0, result["failed"], result)
        current = storage.read_receipt(self.root, "PEER", storage.read_index(self.root))
        for field in ("unresolved_rows", "assignment_summary", "boundaries"):
            self.assertEqual(self.prior["PEER"][field], current[field])
        filled = storage.read_receipt(self.root, "FILL", storage.read_index(self.root))
        self.assertEqual(0, filled["unresolved_rows"])

    def test_real_peer_append_invalidates_the_old_completion_binding(self):
        for name in ("FILL", "PEER"):
            rows = (
                informative_bars("2024-01-02", "2024-01-11")
                if name == "PEER"
                else [
                    *informative_bars("2024-01-02", "2024-01-05"),
                    *informative_bars("2024-01-10", "2024-01-11"),
                ]
            )
            download_pricing(self.store, name, "2024-01-11", client=FakeClient(rows))
        before = storage.read_index(self.root)["symbols"]
        observed = []

        def selected(controls, *args, **kwargs):
            peer = controls["PEER"]
            self.assertNotEqual(peer.previous["source_sha256"], peer.source_sha256)
            self.assertTrue(peer.accepted_reuse[1], "append lost its accepted prefix")
            observed.append(peer.source_sha256)
            return source_work.prepare_group_evidence_and_source_work(controls, *args, **kwargs)

        with (
            patch.object(pipeline, "prepare_group_evidence_and_source_work", side_effect=selected),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("premature price assembly")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("premature row replay")),
        ):
            result = pipeline.run_lineage(
                self.store,
                ["FILL"],
                identity_client=self.client,
                evidence_client=self.evidence_client,
                context=self.context,
            )
        self.assertEqual(1, result["failed"], result)
        self.assertIn("unchanged accepted source bindings", result["results"][0]["error"])
        self.assertEqual(1, len(observed))
        self.assertEqual(before, storage.read_index(self.root)["symbols"])

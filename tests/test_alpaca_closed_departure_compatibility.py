"""Compact v79 applicability preserves unrelated completed decisions."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_standalone_reader as reader_fixture
from test_alpaca_lineage_build import IdentityClient

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build import contract, pipeline, storage, universe_plan
from libs.alpaca_lineage_build.correlation_policy_contract import (
    RETAINED_V79_SEMANTIC_SHA256,
    retained_v79_policy,
    reusable_policy,
)
from libs.alpaca_lineage_build.metadata import EvidenceClient


def prior_policy():
    return {
        "schema_version": 4,
        "policy_id": "alpaca_security_correlation",
        "policy_version": "1.0.0",
        "policy_role": "production",
        "execution_profile": "correlation_v79_policy_engine",
        "implementation": {"correlation_version": 79, "lineage_version": 34},
        "semantic_sha256": RETAINED_V79_SEMANTIC_SHA256,
        "file_sha256": "1" * 64,
    }


def prior_decision(*, unresolved=0, assets=("old-id", "current-id"), compact=False):
    document = {
        "computation_revision": 2,
        "versions": {
            "lineage": 34,
            "identity_report": 4,
            "correlation": 79,
            "correlation_policy_sha256": RETAINED_V79_SEMANTIC_SHA256,
            "calendar_sha256": contract.us_market_hours_sha256(),
        },
        "correlation_policy": prior_policy(),
        "asset_ids": list(assets),
    }
    counts = {"source_rows": 80, "unresolved_rows": unresolved}
    document.update({"projection": counts} if compact else counts)
    return document


class ClosedDepartureCompatibilityTests(unittest.TestCase):
    def test_only_provably_unaffected_completed_decisions_reuse_without_io(self):
        for compact in (False, True):
            for unresolved, assets, expected in (
                (0, ("old", "current"), True),
                (80, ("only",), True),
                (80, ("old", "current", "unrelated"), True),
                (80, ("old", "current"), False),
            ):
                document = prior_decision(unresolved=unresolved, assets=assets, compact=compact)
                before = copy.deepcopy(document)
                with (
                    self.subTest(compact=compact, unresolved=unresolved, assets=assets),
                    patch.object(Path, "open", side_effect=AssertionError("accepted boundary reopened")),
                    patch.object(Path, "iterdir", side_effect=AssertionError("storage inventory")),
                ):
                    self.assertEqual(contract.reusable_decision(document), expected)
                self.assertEqual(document, before)
                self.assertFalse(contract.current_decision(document))
                self.assertFalse(reusable_policy(document["correlation_policy"]))

    def test_missing_applicability_evidence_does_not_reconstruct_it(self):
        document = prior_decision(unresolved=80)
        document.pop("asset_ids")
        document.pop("unresolved_rows")
        with patch.object(Path, "open", side_effect=AssertionError("missing proof triggered reads")):
            self.assertFalse(contract.reusable_decision(document))

    def test_changed_contract_or_policy_cannot_inherit_prior_approval(self):
        for key, value in (
            ("computation_revision", 1),
            ("computation_revision", True),
            ("versions", {**prior_decision()["versions"], "calendar_sha256": "f" * 64}),
            ("versions", {**prior_decision()["versions"], "lineage": 35}),
            ("versions", {**prior_decision()["versions"], "correlation": 78}),
        ):
            document = prior_decision()
            document[key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(contract.reusable_decision(document))
        for key, value in (
            ("policy_role", "candidate"),
            ("execution_profile", "correlation_v80_policy_engine"),
            ("implementation", {"correlation_version": 79, "lineage_version": 35}),
            ("semantic_sha256", "f" * 64),
            ("file_sha256", "malformed"),
        ):
            document = prior_decision()
            document["correlation_policy"][key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(contract.reusable_decision(document))

    def test_prior_policy_exception_does_not_follow_future_production_versions(self):
        with patch(
            "libs.alpaca_lineage_build.correlation_policy_contract.PRODUCTION_CORRELATION_VERSION",
            contract.PRODUCTION_CORRELATION_VERSION + 1,
        ):
            self.assertFalse(retained_v79_policy(prior_policy()))

    def test_compact_planning_selects_only_affected_prior_group(self):
        symbols = {}
        for name, unresolved in (("SAFE", 0), ("AFFECTED", 80)):
            symbols[name] = {
                **prior_decision(unresolved=unresolved, compact=True),
                "dependencies": [],
                "group_members": [name, "PEER"] if name == "AFFECTED" else [name],
            }
            symbols[name]["projection"]["symbol_lookups"] = []
            symbols[name]["projection"]["processed_through"] = "2026-10-14T00:00:00Z"
        accepted = {
            "summary": {"entries": []},
            "index": {"symbols": symbols},
            "state": {"work": {}, "registration_pending": []},
            "checks": {"checks": {}},
            "registry": set(symbols),
        }
        client = EvidenceClient(IdentityClient())
        with (
            patch.object(Path, "open", side_effect=AssertionError("compact planning opened body")),
            patch.object(Path, "iterdir", side_effect=AssertionError("compact planning inventory")),
        ):
            result = universe_plan.select_work(accepted, {"records": []}, client, "2026-10-14")
        self.assertEqual(set(result["work"]), {"AFFECTED"})
        self.assertEqual(client.request_count, 0)


class PriorClosedDepartureReaderTests(unittest.TestCase):
    setUp = reader_fixture.StandaloneReaderTests.setUp
    download = reader_fixture.StandaloneReaderTests.download
    build = reader_fixture.StandaloneReaderTests.build

    def test_completed_unaffected_prior_output_keeps_provenance_and_generation(self):
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "TEST", index)
        frozen = prior_decision(assets=receipt["asset_ids"])
        frozen.pop("source_rows")
        frozen.pop("unresolved_rows")
        receipt.update(frozen)
        receipt["schema_version"] = 34
        receipt.pop("current_source_ownership", None)
        entry = {
            **index["symbols"]["TEST"],
            **frozen,
            "sha256": storage.document_digest(receipt),
        }
        storage._install(
            self.root,
            {
                "schema_version": 34,
                "correlation_policy": prior_policy(),
                "retire": [],
                "documents": {"TEST/lineage.manifest.json": receipt},
                "index_updates": {"TEST": entry},
            },
        )
        with patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("resolution read prices")):
            self.assertEqual(len(StandaloneReader(self.store).resolve_ticker("TEST")["matches"]), 1)
        client = IdentityClient()
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("accepted rules repeated")),
            patch.object(
                storage, "read_boundaries", side_effect=AssertionError("accepted assignments reopened")
            ),
            patch.object(storage, "publish", side_effect=AssertionError("unnecessary header promotion")),
        ):
            result = pipeline.run_lineage(self.store, ["TEST"], identity_client=client)
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(result["results"][0]["reused_decision"])
        self.assertEqual(client.request_count, 0)
        selected = storage.read_index(self.root)["symbols"]["TEST"]
        self.assertEqual(selected["sha256"], entry["sha256"])
        self.assertEqual(selected["correlation_policy"], prior_policy())

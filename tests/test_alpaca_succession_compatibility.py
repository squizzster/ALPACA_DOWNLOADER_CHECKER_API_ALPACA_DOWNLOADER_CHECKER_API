"""Frozen revision-one reuse and current computation-revision invalidation."""

import copy
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import test_alpaca_standalone_reader as reader_fixture
from test_alpaca_lineage_build import IdentityClient

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build import (
    certificate_migration,
    contract,
    pipeline,
    storage,
    transform,
    universe_plan,
)
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.correlation_policy_contract import (
    RETAINED_V78_SEMANTIC_SHA256,
    expected_policy_identity,
    retained_v78_policy,
    reusable_policy,
)
from libs.alpaca_lineage_build.metadata import EvidenceClient


def retained_policy_runtime(test, *, computation_revision=1):
    """Freeze the original compatibility target instead of following today's policy."""
    policy = replace(
        load_authorized_policy(),
        execution_profile="correlation_v79_policy_engine",
        implementation_correlation_version=79,
        implementation_lineage_version=34,
    )
    for owner, name, value in (
        (contract, "COMPUTATION_REVISION", computation_revision),
        (contract, "CORRELATION_VERSION", 79),
        (contract, "VERSION", 34),
    ):
        test.enterContext(patch.object(owner, name, value))
    for name, value in (
        ("PRODUCTION_CORRELATION_VERSION", 79),
        ("PRODUCTION_LINEAGE_VERSION", 34),
        ("PRODUCTION_EXECUTION_PROFILE", "correlation_v79_policy_engine"),
    ):
        test.enterContext(patch(f"libs.alpaca_lineage_build.correlation_policy_contract.{name}", value))
        if hasattr(contract, name):
            test.enterContext(patch.object(contract, name, value))
    test.enterContext(
        patch("libs.alpaca_lineage_build.correlation_policy.load_authorized_policy", return_value=policy)
    )


def frozen_policy():
    return {
        "schema_version": 4,
        "policy_id": "alpaca_security_correlation",
        "policy_version": "1.0.0",
        "policy_role": "production",
        "execution_profile": "correlation_v78_policy_engine",
        "implementation": {"correlation_version": 78, "lineage_version": 34},
        "semantic_sha256": RETAINED_V78_SEMANTIC_SHA256,
        "file_sha256": "1" * 64,
    }


def frozen_decision():
    return {
        "computation_revision": 1,
        "versions": {
            "lineage": 34,
            "identity_report": 4,
            "correlation": 78,
            "correlation_policy_sha256": RETAINED_V78_SEMANTIC_SHA256,
            "calendar_sha256": contract.us_market_hours_sha256(),
        },
        "correlation_policy": frozen_policy(),
    }


class RevisionOneSuccessionCompatibilityTests(unittest.TestCase):
    """Keep the retired compatibility contract explicit, independent of current revision."""

    def setUp(self):
        retained_policy_runtime(self)

    def test_frozen_completed_decision_is_readable_but_not_current_execution(self):
        document = frozen_decision()
        before = copy.deepcopy(document)
        with (
            patch.object(Path, "open", side_effect=AssertionError("accepted boundary reopened")),
            patch.object(Path, "iterdir", side_effect=AssertionError("storage inventory")),
            patch.object(contract, "us_market_hours_sha256", wraps=contract.us_market_hours_sha256) as calendar,
        ):
            self.assertTrue(contract.reusable_decision(document))
        self.assertEqual(calendar.call_count, 1)
        self.assertEqual(document, before)
        self.assertTrue(contract.retained_v78_decision(document))
        self.assertFalse(contract.current_decision(document))
        self.assertFalse(reusable_policy(document["correlation_policy"]))
        self.assertFalse(certificate_migration.retained_version(document))

    def test_new_completed_output_requires_current_policy_and_versions(self):
        document = {
            "computation_revision": contract.COMPUTATION_REVISION,
            "versions": contract.versions(),
            "correlation_policy": {**expected_policy_identity(), "file_sha256": "2" * 64},
        }
        self.assertTrue(contract.current_decision(document))
        self.assertTrue(contract.reusable_decision(document))
        self.assertFalse(contract.retained_v78_decision(document))
        self.assertTrue(reusable_policy(document["correlation_policy"]))

    def test_changed_binding_or_policy_cannot_inherit_frozen_approval(self):
        changes = [
            ("computation_revision", 0),
            ("computation_revision", True),
            ("versions", {**frozen_decision()["versions"], "identity_report": 5}),
            ("versions", {**frozen_decision()["versions"], "calendar_sha256": "f" * 64}),
            ("versions", {**frozen_decision()["versions"], "lineage": 33}),
            ("versions", {**frozen_decision()["versions"], "correlation": 77}),
            ("versions", {**frozen_decision()["versions"], "correlation_policy_sha256": "f" * 64}),
        ]
        for key, value in changes:
            document = frozen_decision()
            document[key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(contract.reusable_decision(document))
        for key, value in (
            ("schema_version", 3),
            ("policy_id", "different_policy"),
            ("policy_version", "1.0.1"),
            ("policy_role", "candidate"),
            ("execution_profile", "correlation_v77_policy_engine"),
            ("implementation", {"correlation_version": 79, "lineage_version": 34}),
            ("semantic_sha256", "f" * 64),
            ("file_sha256", "malformed"),
        ):
            document = frozen_decision()
            document["correlation_policy"][key] = value
            with self.subTest(key=key, value=value):
                self.assertFalse(contract.reusable_decision(document))
        document = frozen_decision()
        document["correlation_policy"]["file_sha256"] = "e" * 64
        self.assertTrue(contract.reusable_decision(document))

    def test_compatibility_does_not_follow_future_production_versions(self):
        with patch("libs.alpaca_lineage_build.correlation_policy_contract.PRODUCTION_CORRELATION_VERSION", 80):
            self.assertFalse(retained_v78_policy(frozen_policy()))

    def test_compact_selection_keeps_completed_v78_group_closed(self):
        entry = {
            **frozen_decision(),
            "dependencies": [],
            "asset_ids": ["accepted-id"],
            "group_members": ["TEST"],
            "projection": {"symbol_lookups": []},
        }
        accepted = {
            "summary": {"entries": []},
            "index": {"symbols": {"TEST": entry}},
            "state": {"work": {}, "registration_pending": []},
            "checks": {"checks": {}},
            "registry": {"TEST"},
        }
        client = EvidenceClient(IdentityClient())
        with (
            patch.object(Path, "open", side_effect=AssertionError("compact selection opened a body")),
            patch.object(Path, "iterdir", side_effect=AssertionError("storage inventory")),
        ):
            result = universe_plan.select_work(accepted, {"records": []}, client, "2026-10-14")
        self.assertEqual(result["work"], {})
        self.assertEqual(client.request_count, 0)


class RevisionOneSuccessionReaderCompatibilityTests(unittest.TestCase):
    """Previously accepted v78/revision-one readers retain their original provenance."""

    def setUp(self):
        retained_policy_runtime(self)
        self.enterContext(patch.object(transform, "COMPUTATION_REVISION", 1))
        reader_fixture.StandaloneReaderTests.setUp(self)

    download = reader_fixture.StandaloneReaderTests.download
    build = reader_fixture.StandaloneReaderTests.build

    def test_existing_v78_reader_keeps_generation_and_original_provenance(self):
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "TEST", index)
        receipt.update(frozen_decision())
        entry = {
            **index["symbols"]["TEST"],
            **frozen_decision(),
            "sha256": storage.document_digest(receipt),
        }
        # A synthetic completed old publication, never production relabelling.
        storage._install(
            self.root,
            {
                "schema_version": 34,
                "correlation_policy": frozen_policy(),
                "retire": [],
                "documents": {"TEST/lineage.manifest.json": receipt},
                "index_updates": {"TEST": entry},
            },
        )
        with (
            patch.object(Path, "iterdir", side_effect=AssertionError("reader inventory")),
            patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("resolution read prices")),
        ):
            matches = StandaloneReader(self.store).resolve_ticker("TEST")["matches"]
        self.assertEqual(len(matches), 1)
        client = IdentityClient()
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("accepted decision repeated")),
            patch.object(
                storage, "read_boundaries", side_effect=AssertionError("accepted assignments reopened")
            ),
            patch.object(storage, "publish", side_effect=AssertionError("implicit header promotion")),
        ):
            repeated = pipeline.run_lineage(self.store, ["TEST"], identity_client=client)
        self.assertEqual(repeated["failed"], 0, repeated)
        self.assertTrue(repeated["results"][0]["reused_decision"])
        self.assertEqual(client.request_count, 0)
        selected = storage.read_index(self.root)["symbols"]["TEST"]
        self.assertEqual(selected["sha256"], entry["sha256"])
        self.assertEqual(selected["correlation_policy"], frozen_policy())


class CurrentSuccessionRevisionTests(unittest.TestCase):
    def test_revision_one_decisions_require_reconstruction_without_opening_inputs(self):
        self.assertEqual(contract.COMPUTATION_REVISION, 2)
        documents = [
            frozen_decision(),
            {
                "computation_revision": 1,
                "versions": contract.versions(),
                "correlation_policy": {**expected_policy_identity(), "file_sha256": "2" * 64},
            },
        ]
        with (
            patch.object(Path, "open", side_effect=AssertionError("obsolete decision opened inputs")),
            patch.object(Path, "iterdir", side_effect=AssertionError("obsolete decision inventoried storage")),
        ):
            for document in documents:
                with self.subTest(correlation=document["versions"]["correlation"]):
                    self.assertFalse(contract.reusable_decision(document))
                    self.assertFalse(contract.retained_v78_decision(document))

    def test_current_revision_accepts_only_its_completed_current_policy_decision(self):
        document = {
            "computation_revision": contract.COMPUTATION_REVISION,
            "versions": contract.versions(),
            "correlation_policy": {**expected_policy_identity(), "file_sha256": "2" * 64},
        }
        self.assertTrue(contract.current_decision(document))
        self.assertTrue(contract.reusable_decision(document))
        frozen = {**frozen_decision(), "computation_revision": contract.COMPUTATION_REVISION}
        self.assertFalse(contract.reusable_decision(frozen))

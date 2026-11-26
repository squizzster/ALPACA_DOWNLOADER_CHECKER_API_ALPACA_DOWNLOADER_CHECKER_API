"""New market facts preserve completed unrelated retained policy handoffs."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import IdentityClient, asset

from libs.alpaca_lineage_build import pipeline, storage, universe_plan
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import document_digest, reusable_decision
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_lineage_build.correlation_policy_contract import (
    RETAINED_V82_SEMANTIC_SHA256,
    RETAINED_V83_SEMANTIC_SHA256,
    RETAINED_V84_SEMANTIC_SHA256,
    RETAINED_V85_SEMANTIC_SHA256,
    RETAINED_V86_SEMANTIC_SHA256,
    RETAINED_V87_SEMANTIC_SHA256,
    RETAINED_V88_SEMANTIC_SHA256,
    RETAINED_V89_SEMANTIC_SHA256,
    RETAINED_V90_SEMANTIC_SHA256,
    RETAINED_V91_SEMANTIC_SHA256,
    RETAINED_V93_SEMANTIC_SHA256,
    RETAINED_V94_SEMANTIC_SHA256,
    RETAINED_V95_SEMANTIC_SHA256,
    RETAINED_V96_SEMANTIC_SHA256,
    RETAINED_V97_SEMANTIC_SHA256,
    RETAINED_V98_SEMANTIC_SHA256,
    RETAINED_V99_SEMANTIC_SHA256,
    RETAINED_V100_SEMANTIC_SHA256,
    RETAINED_V101_SEMANTIC_SHA256,
    RETAINED_V103_SEMANTIC_SHA256,
    RETAINED_V104_SEMANTIC_SHA256,
)


class MarketPolicyCompatibilityTests(TestCase):
    def test_unchanged_v82_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(82, RETAINED_V82_SEMANTIC_SHA256)

    def test_unchanged_v83_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(83, RETAINED_V83_SEMANTIC_SHA256)

    def test_unchanged_v84_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(84, RETAINED_V84_SEMANTIC_SHA256)

    def test_unchanged_v85_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(85, RETAINED_V85_SEMANTIC_SHA256)

    def test_unchanged_v86_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(86, RETAINED_V86_SEMANTIC_SHA256)

    def test_unchanged_v87_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(87, RETAINED_V87_SEMANTIC_SHA256)

    def test_unchanged_v88_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(88, RETAINED_V88_SEMANTIC_SHA256)

    def test_unchanged_v89_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(89, RETAINED_V89_SEMANTIC_SHA256)

    def test_unchanged_v90_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(90, RETAINED_V90_SEMANTIC_SHA256)

    def test_unchanged_v91_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(91, RETAINED_V91_SEMANTIC_SHA256)

    def test_unchanged_v93_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(93, RETAINED_V93_SEMANTIC_SHA256)

    def test_unchanged_v94_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(94, RETAINED_V94_SEMANTIC_SHA256)

    def test_unchanged_v95_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(95, RETAINED_V95_SEMANTIC_SHA256)

    def test_unchanged_v96_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(96, RETAINED_V96_SEMANTIC_SHA256)

    def test_unchanged_v97_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(97, RETAINED_V97_SEMANTIC_SHA256)

    def test_unchanged_v98_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(98, RETAINED_V98_SEMANTIC_SHA256)

    def test_unchanged_v99_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(99, RETAINED_V99_SEMANTIC_SHA256)

    def test_unchanged_v100_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(100, RETAINED_V100_SEMANTIC_SHA256)

    def test_unchanged_v101_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(101, RETAINED_V101_SEMANTIC_SHA256)

    def test_unchanged_v103_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(103, RETAINED_V103_SEMANTIC_SHA256)

    def test_unchanged_v104_publication_keeps_provenance_without_upstream_replay(self):
        self.unchanged(104, RETAINED_V104_SEMANTIC_SHA256)

    def unchanged(self, version, semantic):
        with fixture() as case:
            root = case.store / "end_of_day/lineage"
            index = storage.read_index(root)
            receipt = storage.read_receipt(root, "AAA", index)
            frozen = load_policy(f"resources/alpaca_security_correlation_v{version}_legacy_policy.yaml")
            receipt["versions"] = {
                **receipt["versions"],
                "correlation": version,
                "correlation_policy_sha256": semantic,
            }
            receipt["correlation_policy"] = frozen.provenance()
            generation = document_digest(receipt)
            entry = {
                **index["symbols"]["AAA"],
                "versions": receipt["versions"],
                "correlation_policy": receipt["correlation_policy"],
                "sha256": generation,
            }
            storage.atomic_json(root / "AAA/lineage.manifest.json", receipt)
            storage._update_index(root, {"AAA": entry})
            self.assertTrue(reusable_decision(receipt))
            with (
                patch.object(
                    storage, "read_receipt", side_effect=AssertionError("receipt reopened to select work")
                ),
                patch.object(
                    storage, "read_boundaries", side_effect=AssertionError("assignments reopened to select work")
                ),
            ):
                planned = universe_plan.boundaries(
                    case.store, case.registry, context=OperationContext(case.store)
                )
            self.assertEqual(set(), planned["changed_rules"])
            self.assertEqual({}, planned["legacy_memberships"])
            with (
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("old prices opened")),
                patch.object(pipeline, "correlate", side_effect=AssertionError("old correlation replayed")),
                patch.object(pipeline, "acquire", side_effect=AssertionError("old metadata reacquired")),
                patch.object(storage, "publish", side_effect=AssertionError("unchanged policy header promoted")),
                patch.object(storage, "read_boundaries", side_effect=AssertionError("old assignments opened")),
            ):
                result = pipeline.run_lineage(
                    case.store, ["AAA"], identity_client=IdentityClient({"AAA": asset("AAA")})
                )
            self.assertEqual(0, result["failed"], result)
            current = storage.read_index(root)["symbols"]["AAA"]
            self.assertEqual(generation, current["sha256"])
            self.assertEqual(frozen.provenance(), current["correlation_policy"])
            changed = deepcopy(receipt)
            changed["correlation_policy"]["semantic_sha256"] = "a" * 64
            self.assertFalse(reusable_decision(changed))

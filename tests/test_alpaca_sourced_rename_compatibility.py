"""Exact v80 decisions and already applicable v79 decisions retain provenance."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_lineage_build import contract
from libs.alpaca_lineage_build.correlation_policy_contract import RETAINED_V82_SEMANTIC_SHA256
from tests.test_alpaca_closed_departure_compatibility import prior_decision


def v80_decision():
    document = prior_decision(unresolved=80)
    document["versions"].update(
        lineage=35, correlation=80, correlation_policy_sha256=RETAINED_V82_SEMANTIC_SHA256
    )
    document["correlation_policy"].update(
        execution_profile="correlation_v80_policy_engine",
        implementation={"correlation_version": 80, "lineage_version": 35},
        semantic_sha256=RETAINED_V82_SEMANTIC_SHA256,
    )
    return document


class SourcedRenameCompatibilityTests(unittest.TestCase):
    def test_exact_v80_reuse_requires_no_evidence_or_body_and_keeps_provenance(self):
        document = v80_decision()
        document.pop("asset_ids")
        before = copy.deepcopy(document)
        with (
            patch.object(Path, "open", side_effect=AssertionError("receipt or payload reopened")),
            patch.object(Path, "iterdir", side_effect=AssertionError("store crawl")),
        ):
            self.assertTrue(contract.reusable_decision(document))
            self.assertFalse(contract.current_decision(document))
        self.assertEqual(before, document)

    def test_changed_v80_profile_calendar_revision_and_future_target_fail_closed(self):
        for field, value in (
            ("computation_revision", 1),
            ("versions", {**v80_decision()["versions"], "calendar_sha256": "f" * 64}),
            (
                "correlation_policy",
                {**v80_decision()["correlation_policy"], "execution_profile": "correlation_v81_policy_engine"},
            ),
        ):
            document = v80_decision()
            document[field] = value
            self.assertFalse(contract.reusable_decision(document))
        with patch.object(contract, "VERSION", 38):
            self.assertFalse(contract.reusable_decision(v80_decision()))

    def test_v79_complete_group_scope_excludes_exact_two_label_exception(self):
        for scope, expected in (
            ({"group_members": ["A", "B", "C"]}, True),
            ({"input_sources": {"A": "g", "B": "h"}, "unavailable_pricing": {"C": {}}}, True),
            ({"input_sources": {"A": "g"}}, True),
            ({"group_members": ["A", "B"]}, False),
            ({"dependencies": ["B", "C"]}, False),
            ({}, False),
        ):
            document = {**prior_decision(unresolved=80), **scope}
            with (
                self.subTest(scope=scope),
                patch.object(Path, "open", side_effect=AssertionError("missing scope reconstructed")),
            ):
                self.assertEqual(expected, contract.reusable_decision(document))

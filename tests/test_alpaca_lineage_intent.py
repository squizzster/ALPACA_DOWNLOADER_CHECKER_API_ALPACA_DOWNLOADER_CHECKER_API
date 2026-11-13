"""Generic producer intent is separate from policy semantics and legacy facts."""

import copy
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import yaml

from libs.alpaca_lineage_build import ruleset, storage
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import CANONICAL_POLICY_PATH
from libs.alpaca_lineage_debug.policy import load_policy, policy_applies, template


class ApplicationIntentTests(TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.path = self.directory / "candidate.yaml"
        self.document = yaml.safe_load(CANONICAL_POLICY_PATH.read_text())

    def load_ruleset(self, document):
        self.path.write_text(yaml.safe_dump(document))
        return ruleset.load_ruleset(self.path)

    def test_generic_producer_intent_changes_only_execution_identity(self):
        original = self.load_ruleset(self.document)
        changed = copy.deepcopy(self.document)
        changed["producers"]["membership_refinement"] = None
        candidate = self.load_ruleset(changed)
        self.assertEqual(original.policy.semantic_sha256, candidate.policy.semantic_sha256)
        self.assertNotEqual(original.execution_sha256, candidate.execution_sha256)
        self.assertIsNone(candidate.membership_refinement)
        changed["producers"]["membership_refinement"] = ruleset.REFINEMENT_NAME
        restored = self.load_ruleset(changed)
        self.assertEqual(original.execution_sha256, restored.execution_sha256)

    def test_generic_research_options_bind_reuse_without_factual_state(self):
        document = yaml.safe_load(template(combined=True))
        self.path.write_text(yaml.safe_dump(document))
        enabled = load_policy(self.path)
        document["producers"]["membership_refinement"] = None
        self.path.write_text(yaml.safe_dump(document))
        disabled = load_policy(self.path)
        self.assertFalse(policy_applies(enabled.as_report_object(), disabled))
        self.assertTrue(policy_applies(disabled.as_report_object(), disabled))
        self.assertFalse(hasattr(disabled, "documented_facts"))

    def test_old_date_refinement_factual_options_reject_before_store_access(self):
        original = yaml.safe_load(template())
        for intent in (None, False, True):
            document = copy.deepcopy(original)
            entry = {"symbols": ["FUTURE"], "supplements": [{"kind": "research_assertion"}]}
            if intent is not None:
                entry["refine_accepted_dates"] = intent
            document["research"]["documented_facts"] = [entry]
            self.path.write_text(yaml.safe_dump(document))
            with (
                self.subTest(intent=intent),
                patch.object(storage, "read_index", side_effect=AssertionError("factual intent read store")),
                self.assertRaises((LineageError, ValueError)),
            ):
                load_policy(self.path)

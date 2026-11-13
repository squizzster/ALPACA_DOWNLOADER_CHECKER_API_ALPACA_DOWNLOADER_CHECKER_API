"""Generic configuration acceptance never imports researched assertions."""

import copy
import hashlib
import tempfile
import unittest
from dataclasses import fields
from pathlib import Path
from unittest.mock import patch

import yaml

from libs.alpaca_lineage_build import correlation_policy as compiler
from libs.alpaca_lineage_build import ruleset
from libs.alpaca_lineage_build.contract import CorrelationPolicyError
from libs.alpaca_lineage_build.ruleset_contract import AcceptedRuleset
from libs.alpaca_lineage_debug.policy import control_view, rule_index, template
from libs.alpaca_lineage_debug.policy import load_policy as load_debug_policy


class CanonicalRulesetTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.document = yaml.safe_load(compiler.CANONICAL_POLICY_PATH.read_text())

    def load_document(self, document):
        path = self.directory / "ruleset.yaml"
        path.write_text(yaml.safe_dump(document, sort_keys=False))
        return ruleset.load_ruleset(path)

    def test_generic_container_preserves_completed_policy_identity(self):
        accepted = ruleset.load_ruleset(authorize=True)
        self.assertEqual({"ruleset_schema_version", "policy", "producers"}, set(self.document))
        self.assertEqual(2, self.document["ruleset_schema_version"])
        self.assertEqual(compiler.PRODUCTION_POLICY_SEMANTIC_SHA256, accepted.policy.semantic_sha256)
        self.assertIs(accepted.policy, compiler.authorize_production_policy(accepted.policy))
        self.assertEqual(
            ["policy", "membership_refinement", "execution_sha256"],
            [field.name for field in fields(AcceptedRuleset)],
        )
        self.assertEqual((), accepted.applications)
        self.assertEqual((), accepted.supplements)
        self.assertFalse((compiler.CANONICAL_POLICY_PATH.parent / "alpaca_sourced_event_facts.json").exists())

    def test_configuration_is_read_and_compiled_once_without_other_files(self):
        opens = []
        original_open = Path.open

        def opened(path, *args, **kwargs):
            self.assertEqual(compiler.CANONICAL_POLICY_PATH, path, "configuration attempted unrelated I/O")
            opens.append(path)
            return original_open(path, *args, **kwargs)

        with (
            patch.object(Path, "open", opened),
            patch.object(ruleset, "compile_policy_document", wraps=ruleset.compile_policy_document) as compile_,
        ):
            accepted = ruleset.load_ruleset()
        self.assertEqual([compiler.CANONICAL_POLICY_PATH], opens)
        self.assertEqual(1, compile_.call_count)
        self.assertEqual(ruleset.REFINEMENT_NAME, accepted.membership_refinement)

    def test_producer_selection_has_its_own_identity(self):
        original = self.load_document(self.document)
        self.document["producers"]["membership_refinement"] = None
        candidate = self.load_document(self.document)
        self.assertIsNone(candidate.membership_refinement)
        self.assertEqual(original.policy.semantic_sha256, candidate.policy.semantic_sha256)
        self.assertNotEqual(original.execution_sha256, candidate.execution_sha256)

    def test_formatting_and_flat_container_keep_semantic_policy_identity(self):
        path = self.directory / "ruleset.yaml"
        path.write_text(yaml.safe_dump(self.document))
        original = ruleset.load_ruleset(path)
        path.write_text("# formatting change\n" + path.read_text())
        changed = ruleset.load_ruleset(path)
        self.assertEqual(original.policy.semantic_sha256, changed.policy.semantic_sha256)
        self.assertEqual(original.execution_sha256, changed.execution_sha256)
        self.assertNotEqual(original.policy.file_sha256, changed.policy.file_sha256)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), changed.policy.file_sha256)
        flat = self.load_document(self.document["policy"])
        self.assertEqual(original.policy.semantic_sha256, flat.policy.semantic_sha256)
        self.assertIsNone(flat.membership_refinement)

    def test_bounded_strict_generic_envelope(self):
        mutations = [
            lambda value: value.update(ruleset_schema_version=True),
            lambda value: value.update(ruleset_schema_version=1),
            lambda value: value.update(unknown={}),
            lambda value: value["producers"].update(membership_refinement="import.anything"),
            lambda value: value["producers"].update(unknown=None),
        ]
        for mutation in mutations:
            document = copy.deepcopy(self.document)
            mutation(document)
            with self.subTest(mutation=mutation), self.assertRaises(CorrelationPolicyError):
                self.load_document(document)
        path = self.directory / "duplicate.yaml"
        path.write_text("ruleset_schema_version: 2\nruleset_schema_version: 2\n")
        with self.assertRaisesRegex(CorrelationPolicyError, "duplicate"):
            ruleset.load_ruleset(path)
        path.write_bytes(b"#" * (compiler.MAX_RULESET_BYTES + 1))
        with self.assertRaisesRegex(CorrelationPolicyError, "exceeds"):
            ruleset.load_ruleset(path)
        path.write_bytes(b"#" * (compiler.MAX_POLICY_BYTES + 1))
        with self.assertRaisesRegex(CorrelationPolicyError, "exceeds"):
            compiler.load_policy(path)

    def test_fact_ingress_is_rejected_at_every_configuration_entrypoint(self):
        payloads = [
            {**self.document, "evidence": {"applications": []}},
            {"schema_version": 1, "applications": []},
            {**self.document["policy"], "supplements": "/must/not/read/facts.json"},
            {**self.document["policy"], "assertion_overlays": []},
            {**self.document["policy"], "research_certificates": []},
            {**self.document["policy"], "research": {"documented_facts": []}},
        ]
        for document in payloads:
            for loader in (ruleset.load_ruleset, compiler.load_policy, load_debug_policy):
                path = self.directory / "injected.yaml"
                path.write_text(yaml.safe_dump(document))
                original_open = Path.open
                reads = []

                def opened(target, *args, **kwargs):
                    self.assertEqual(path, target, "fact rejection performed unrelated I/O")
                    reads.append(target)
                    return original_open(target, *args, **kwargs)

                with self.subTest(document=document, loader=loader), patch.object(Path, "open", opened):
                    with self.assertRaisesRegex(CorrelationPolicyError, "fact injection"):
                        loader(path)
                self.assertEqual([path], reads)
            with self.assertRaisesRegex(CorrelationPolicyError, "fact injection"):
                ruleset.accept_components(document)

    def test_computed_generic_predicate_declarations_remain_supported(self):
        path = compiler.CANONICAL_POLICY_PATH.with_name(
            "alpaca_security_correlation_evidence_quality_policy.yaml"
        )
        policy = compiler.load_policy(path)
        self.assertTrue(policy.evidence_fact_producers)
        self.assertTrue(policy.evidence_fact_types)
        self.assertTrue(policy.evidence_rules)

    def test_research_template_contains_only_generic_configuration(self):
        path = self.directory / "candidate.yaml"
        path.write_text(template(combined=True))
        candidate = load_debug_policy(path)
        document = yaml.safe_load(path.read_text())
        self.assertEqual({"ruleset_schema_version", "policy", "producers"}, set(document))
        self.assertEqual({}, document["policy"]["research"])
        self.assertEqual("research_candidate", candidate.policy_role)
        self.assertFalse(hasattr(candidate, "documented_facts"))
        control = control_view(candidate)
        self.assertIs(candidate.row_rules, control.row_rules)
        self.assertIsNone(getattr(control, "membership_refinement", None))
        self.assertNotIn("applications", rule_index(candidate)["ruleset"])
        with self.assertRaises(CorrelationPolicyError):
            compiler.authorize_production_policy(candidate)

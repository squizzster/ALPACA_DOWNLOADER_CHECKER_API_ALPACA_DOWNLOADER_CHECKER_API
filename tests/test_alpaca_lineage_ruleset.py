"""Canonical container migration preserves completed policy/application identities."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from libs.alpaca_lineage_build import correlation_policy as compiler
from libs.alpaca_lineage_build import ruleset, sourced_rules
from libs.alpaca_lineage_build.contract import CorrelationPolicyError, LineageError, document_digest
from libs.alpaca_lineage_debug.policy import control_view, rule_index, template
from libs.alpaca_lineage_debug.policy import load_policy as load_debug_policy
from libs.alpaca_security_lineage import event_supplements

LEGACY_APPLICATION_IDS = frozenset(
    """barrick-common-continuity-and-randgold-consideration
    eres-abacus-public-common-continuation
    gsk-new-ads-dated-when-issued-market-2022
    gsrt-nklr-dated-public-common-market-labels
    frc-frcb-dated-residual-common-market-labels
    glt-magn-dated-continuing-common-market-labels
    sivb-sivbq-dated-residual-common-market-labels
    ivc-ivcrq-dated-old-common-market-labels
    pear-thma-pearq-dated-public-common-market-labels
    cola-saiq-dated-public-ordinary-market-labels
    aebiv-aebi-dated-common-when-issued-market
    lilpv-lilap-dated-preference-market
    lilrv-lilar-dated-right-market
    gow-public-common-and-distinct-components
    qgen-own-common-capital-repayment-continuity
    brookline-public-common-dated-market
    brkl-distinct-fund-dated-market
    bbt-truist-continuing-public-common
    bhlb-beacon-continuing-public-common
    nexentis-dated-common-security
    cbio-gyre-and-crescent-dated-public-common
    dyns-snti-dated-public-common-holding-exchange
    aryd-adgm-public-parent-and-dated-common""".split()
)


class CanonicalRulesetTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.document = yaml.safe_load(compiler.CANONICAL_POLICY_PATH.read_text())

    def load_document(self, document):
        path = self.directory / "ruleset.yaml"
        path.write_text(yaml.safe_dump(document, sort_keys=False))
        return ruleset.load_ruleset(path)

    def test_container_migration_keeps_policy_and_all_legacy_application_identities(self):
        accepted = ruleset.load_ruleset(authorize=True)
        self.assertEqual(compiler.PRODUCTION_POLICY_SEMANTIC_SHA256, accepted.policy.semantic_sha256)
        self.assertIs(accepted.policy, compiler.authorize_production_policy(accepted.policy))
        historical = {
            item["id"]: item["fingerprint"]
            for item in accepted.applications
            if item["id"] in LEGACY_APPLICATION_IDS
        }
        self.assertEqual(LEGACY_APPLICATION_IDS, historical.keys())
        # Bound to the pre-migration JSON registry, without keeping a second live
        # registry or repeating its accepted facts merely to check the container.
        self.assertEqual(
            "7332877265240c33a1ebfe17430e0c3bb62bd413da280a97393956d7822dc104",
            document_digest(historical),
        )
        self.assertFalse((compiler.CANONICAL_POLICY_PATH.parent / "alpaca_sourced_event_facts.json").exists())

    def test_single_read_compilation_and_fact_acceptance_produce_shared_immutable_values(self):
        opens = []
        original_open = Path.open

        def opened(path, *args, **kwargs):
            opens.append(path)
            return original_open(path, *args, **kwargs)

        with (
            patch.object(Path, "open", opened),
            patch.object(ruleset, "compile_policy_document", wraps=ruleset.compile_policy_document) as compile_,
            patch.object(sourced_rules, "accept_supplements", wraps=sourced_rules.accept_supplements) as facts,
        ):
            accepted = ruleset.load_ruleset()
        self.assertEqual([compiler.CANONICAL_POLICY_PATH], opens)
        self.assertEqual(1, compile_.call_count)
        self.assertEqual(1, facts.call_count)
        first = accepted.applications[0]
        self.assertIs(first["facts"][0], accepted.supplements[0])
        with self.assertRaises(TypeError):
            first["id"] = "changed"
        with self.assertRaises(TypeError):
            first["facts"][0]["facts"]["schema_version"] = 999

    def test_date_intent_changes_only_the_named_application_not_policy_or_other_applications(self):
        original = self.load_document(self.document)
        changed = copy.deepcopy(self.document)
        changed["evidence"]["applications"][0]["refine_accepted_dates"] = False
        candidate = self.load_document(changed)
        self.assertEqual(original.policy.semantic_sha256, candidate.policy.semantic_sha256)
        self.assertEqual(original.execution_sha256, candidate.execution_sha256)
        self.assertNotEqual(original.applications[0]["fingerprint"], candidate.applications[0]["fingerprint"])
        self.assertFalse(candidate.applications[0]["refine_accepted_dates"])
        self.assertEqual(original.applications[1:], candidate.applications[1:])
        changed["evidence"]["applications"][0]["refine_accepted_dates"] = True
        explicit_default = self.load_document(changed)
        self.assertEqual(
            original.applications[0]["fingerprint"], explicit_default.applications[0]["fingerprint"]
        )

    def test_producer_selection_has_its_own_identity(self):
        original = self.load_document(self.document)
        refinement = None if original.membership_refinement else ruleset.REFINEMENT_NAME
        self.document["producers"]["membership_refinement"] = refinement
        candidate = self.load_document(self.document)
        self.assertEqual(refinement, candidate.membership_refinement)
        self.assertEqual(original.policy.semantic_sha256, candidate.policy.semantic_sha256)
        self.assertEqual(original.applications, candidate.applications)
        self.assertNotEqual(original.execution_sha256, candidate.execution_sha256)

    def test_formatting_changes_only_byte_provenance(self):
        path = self.directory / "ruleset.yaml"
        path.write_text(yaml.safe_dump(self.document))
        original = ruleset.load_ruleset(path)
        path.write_text("# formatting change\n" + path.read_text())
        changed = ruleset.load_ruleset(path)
        self.assertEqual(original.policy.semantic_sha256, changed.policy.semantic_sha256)
        self.assertEqual(original.applications, changed.applications)
        self.assertEqual(original.execution_sha256, changed.execution_sha256)
        self.assertNotEqual(original.policy.file_sha256, changed.policy.file_sha256)
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), changed.policy.file_sha256)

    def test_combined_document_exceeds_old_bound_but_retains_strict_ruleset_bound(self):
        self.assertGreater(compiler.CANONICAL_POLICY_PATH.stat().st_size, compiler.MAX_POLICY_BYTES)
        self.assertIsNotNone(ruleset.load_ruleset())
        path = self.directory / "too-large.yaml"
        path.write_bytes(b"#" * (compiler.MAX_RULESET_BYTES + 1))
        with self.assertRaisesRegex(CorrelationPolicyError, "exceeds"):
            ruleset.load_ruleset(path)
        path.write_bytes(b"#" * (compiler.MAX_POLICY_BYTES + 1))
        with self.assertRaisesRegex(CorrelationPolicyError, "exceeds"):
            compiler.load_policy(path)

    def test_strict_envelope_and_scoped_intent_reject_malformed_candidates_without_fallback(self):
        mutations = [
            lambda value: value.update(ruleset_schema_version=True),
            lambda value: value.update(unknown={}),
            lambda value: value["evidence"].update(unknown=[]),
            lambda value: value["producers"].update(membership_refinement="import.anything"),
            lambda value: value["evidence"]["applications"][0].update(refine_accepted_dates="false"),
            lambda value: value["evidence"]["applications"].append(
                copy.deepcopy(value["evidence"]["applications"][0])
            ),
        ]
        for mutation in mutations:
            document = copy.deepcopy(self.document)
            mutation(document)
            with self.subTest(mutation=mutation), self.assertRaises((LineageError, ValueError)):
                self.load_document(document)
        path = self.directory / "duplicate.yaml"
        path.write_text("ruleset_schema_version: 1\nruleset_schema_version: 1\n")
        with self.assertRaisesRegex(CorrelationPolicyError, "duplicate"):
            ruleset.load_ruleset(path)

    def test_explicit_legacy_import_and_research_template_remain_compatible(self):
        entry = self.document["evidence"]["applications"][0]
        path = self.directory / "selected.json"
        path.write_text(json.dumps({"schema_version": 1, "applications": [entry]}))
        applications, facts = sourced_rules.load_applications(path)
        self.assertEqual(document_digest(entry), applications[0]["fingerprint"])
        self.assertIs(applications[0]["facts"][0], facts[0])
        path = self.directory / "legacy.yaml"
        path.write_text(yaml.safe_dump(self.document["policy"]))
        legacy = ruleset.load_ruleset(path)
        self.assertEqual((), legacy.applications)
        self.assertEqual([], legacy.supplements)
        self.assertEqual(compiler.PRODUCTION_POLICY_SEMANTIC_SHA256, legacy.policy.semantic_sha256)
        path.write_text(template())
        self.assertEqual("research_candidate", load_debug_policy(path).policy_role)
        self.assertEqual(
            compiler.PRODUCTION_POLICY_SEMANTIC_SHA256,
            load_debug_policy(compiler.CANONICAL_POLICY_PATH).semantic_sha256,
        )

    def test_combined_research_template_accepts_facts_once_and_keeps_control_unaugmented(self):
        from alpaca_end_of_day import debug_lineage

        request = {"schema": "alpaca.lineage.debug.request", "schema_version": 1, "operation": "init"}
        response = json.loads(debug_lineage(json.dumps(request).encode()))
        combined = yaml.safe_load(response["yaml"])
        self.assertEqual(1, combined["ruleset_schema_version"])
        self.assertEqual("research_candidate", combined["policy"]["policy_role"])
        self.assertEqual({"control_mode": "documented_scopes"}, combined["policy"]["research"])
        combined["evidence"]["applications"][0]["refine_accepted_dates"] = False
        path = self.directory / "candidate.yaml"
        path.write_text(yaml.safe_dump(combined))
        with patch.object(
            event_supplements, "validate_supplements", wraps=event_supplements.validate_supplements
        ) as validate:
            candidate = load_debug_policy(path)
        validate.assert_called_once()
        self.assertTrue(candidate.documented_facts)
        self.assertFalse(candidate.ruleset.applications[0]["refine_accepted_dates"])
        self.assertEqual(
            len(candidate.ruleset.applications), len(rule_index(candidate)["ruleset"]["applications"])
        )
        control = control_view(candidate)
        self.assertIs(candidate.row_rules, control.row_rules)
        self.assertIs(candidate.ruleset.applications, control.ruleset_applications)
        self.assertIsNone(getattr(control, "membership_refinement", None))
        self.assertFalse(getattr(control, "documented_facts", ()))
        with self.assertRaises(CorrelationPolicyError):
            compiler.authorize_production_policy(candidate)
        combined["policy"]["research"]["documented_facts"] = []
        path.write_text(yaml.safe_dump(combined))
        with self.assertRaisesRegex(ValueError, "keeps facts in evidence"):
            load_debug_policy(path)

"""Lab candidates cannot turn authored assertions into fresh inference."""

import copy
import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import yaml

from libs.alpaca_lineage_build.contract import document_digest, versions
from libs.alpaca_lineage_build.correlation_policy import CANONICAL_POLICY_PATH
from libs.alpaca_lineage_policy_lab import comparison
from libs.alpaca_lineage_policy_lab.audit import audit
from libs.alpaca_lineage_policy_lab.contract import PolicyLabError
from libs.alpaca_lineage_policy_lab.policy import load_policy
from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence, accept_provider_evidence


class LabAssertionRejectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.policy = load_policy(CANONICAL_POLICY_PATH)

    def test_factual_yaml_never_reaches_compare_or_audit_storage(self):
        original = yaml.safe_load(CANONICAL_POLICY_PATH.read_text())
        for field in ("applications", "supplements", "assertion_overlays", "research_certificates"):
            document = copy.deepcopy(original)
            document[field] = [{"symbols": ["FUTURE"], "source": "/must/not/read/facts.json"}]
            path = self.directory / "candidate.yaml"
            path.write_text(yaml.safe_dump(document))
            with (
                self.subTest(field=field),
                patch.object(
                    comparison, "reader_snapshot", side_effect=AssertionError("candidate store access")
                ),
                patch(
                    "libs.alpaca_lineage_policy_lab.audit.check_store",
                    side_effect=AssertionError("candidate store access"),
                ),
                self.assertRaisesRegex(PolicyLabError, "fact injection"),
            ):
                load_policy(path)

    def test_direct_compiled_factual_candidates_fail_before_store_selection(self):
        for name, value in (
            ("documented_facts", ()),
            ("event_supplements", [{"kind": "asserted_owner"}]),
            ("assertion_overlays", [{"owner": "researched"}]),
            ("sourced_fact_applications", [{"id": "researched"}]),
            ("control_mode", "accepted_publication"),
        ):
            policy = replace(self.policy)
            object.__setattr__(policy, name, value)
            for operation in (
                lambda: comparison.compare("/must/not/open", "FUTURE", policy),
                lambda: audit("/must/not/open", policy),
            ):
                with (
                    self.subTest(name=name, operation=operation),
                    patch.object(
                        comparison, "_load_group", side_effect=AssertionError("candidate selected store")
                    ),
                    patch(
                        "libs.alpaca_lineage_policy_lab.audit.check_store",
                        side_effect=AssertionError("candidate selected store"),
                    ),
                    self.assertRaises((PolicyLabError, ValueError)),
                ):
                    operation()

    def test_supplied_receipt_assertions_fail_before_snapshot_boundaries_or_prices(self):
        for evidence in (
            {"event_supplements": [{"kind": "asserted_owner"}]},
            {"corporate_actions": [{"sourced_facts": {"unchanged_security": True}}]},
        ):
            with (
                self.subTest(evidence=evidence),
                patch.object(comparison, "reader_snapshot", side_effect=AssertionError("snapshot opened")),
                patch.object(comparison, "load_pricing", side_effect=AssertionError("prices opened")),
                self.assertRaisesRegex(ValueError, "fact injection"),
            ):
                with comparison._load_group(
                    Path("/must/not/open"), "FUTURE", held_receipts={"FUTURE": {"evidence": evidence}}
                ):
                    self.fail("factual receipt was admitted")

    def test_supplied_evidence_and_refinement_fail_before_digest_or_correlation(self):
        fact = {"event_supplements": [{"kind": "asserted_owner"}]}
        supplied = {
            "inputs": {},
            "receipts": {"FUTURE": {"evidence": fact}},
        }
        values = [supplied, {**supplied, "inference_evidence": {"FUTURE": fact}}]
        for selected in values:
            with (
                self.subTest(selected=selected),
                patch.object(comparison, "document_digest", side_effect=AssertionError("factual digest")),
                patch.object(comparison, "correlate", side_effect=AssertionError("factual correlation")),
                self.assertRaisesRegex(ValueError, "fact injection"),
            ):
                comparison._compare_loaded_group(
                    Path("/unused"), "FUTURE", self.policy, selected, max_differences=0
                )
        with self.assertRaisesRegex(PolicyLabError, "registered generic producer"):
            comparison._compare_loaded_group(
                Path("/unused"), "FUTURE", self.policy, {}, max_differences=0, refine=lambda *args: args
            )

    def test_provider_projection_is_completed_once_per_original_evidence_object(self):
        original = {}
        cache = {}
        with patch.object(comparison, "provider_evidence", wraps=comparison.provider_evidence) as project:
            first = comparison._select_provider_evidence({"FIRST": original, "SECOND": original}, cache=cache)
            second = comparison._select_provider_evidence({"THIRD": original}, cache=cache)
        self.assertEqual(1, project.call_count)
        self.assertIs(first["FIRST"], first["SECOND"])
        self.assertIs(first["FIRST"], second["THIRD"])

    def test_completed_provider_evidence_is_not_traversed_again(self):
        accepted = accept_provider_evidence({"corporate_actions": []})
        with (
            patch.object(ProviderEvidence, "items", side_effect=AssertionError("accepted evidence traversed")),
            patch.object(ProviderEvidence, "get", side_effect=AssertionError("accepted evidence inspected")),
        ):
            result = comparison._candidate_evidence({"FUTURE": accepted}, where="held generic evidence")
            self.assertIs(accepted, result["FUTURE"])
            self.assertIs(accepted, comparison.provider_evidence(accepted))

    def test_stored_legacy_provenance_stays_baseline_and_only_provider_observations_replay(self):
        source = {"symbol": "FUTURE"}
        generation = document_digest(source)
        original = {
            "query": {
                "input_symbol": "FUTURE",
                "corporate_actions_start": "2026-01-01",
                "corporate_actions_end": "2026-10-15",
            },
            "queried_symbols": ["FUTURE"],
            "asset_lookups": [],
            "provider_corporate_actions": [],
            "corporate_actions": [{"sourced_facts": {"unchanged_security": True}}],
            "event_supplements": [{"kind": "legacy_research"}],
        }
        receipt = {
            "versions": versions(),
            "row_partition_complete": True,
            "input_sources": {"FUTURE": generation},
            "evidence": original,
        }
        snapshot = {
            "receipts": {"FUTURE": receipt},
            "boundaries": {"FUTURE": {}},
            "entries": {"FUTURE": {"sha256": "a" * 64}},
            "locked": {"FUTURE"},
        }

        @contextmanager
        def selected(*args, **kwargs):
            yield snapshot

        with (
            patch.object(comparison, "reader_snapshot", selected),
            patch.object(comparison, "check_unavailable_pricing"),
            patch.object(comparison, "read_update", return_value=None),
            patch.object(comparison, "load_source", return_value=source),
            patch.object(comparison, "load_pricing", return_value=object()),
            patch.object(comparison, "provider_evidence", wraps=comparison.provider_evidence) as project,
        ):
            with comparison._load_group(self.directory, "FUTURE") as result:
                self.assertIs(receipt, result["receipts"]["FUTURE"])
                self.assertIs(original, result["receipts"]["FUTURE"]["evidence"])
                inference = result["inference_evidence"]["FUTURE"]
                self.assertNotIn("event_supplements", inference)
                self.assertEqual([], inference["corporate_actions"])
                self.assertIs(original["query"], inference["query"])
                self.assertEqual(document_digest(inference), result["evidence_generations"]["FUTURE"])
        self.assertEqual(1, project.call_count)
        self.assertTrue(original["event_supplements"])

"""Retired factual ingress and scoped continuation of completed legacy output."""

from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import sourced_rules
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.work_contract import WorkDeferred, WorkPolicy, obligation, restriction


class ApplicationComponentScopeTests(TestCase):
    def setUp(self):
        self.application = {
            "id": "legacy-application",
            "symbols": ["COMMON", "UNIT"],
            "supplements": [{"kind": "retained_research_provenance"}],
        }
        self.fingerprint = "a" * 64

    def test_application_and_json_imports_reject_before_files_or_fact_parsing(self):
        with (
            patch.object(Path, "open", side_effect=AssertionError("retired fact file opened")),
            patch(
                "libs.alpaca_security_lineage.event_supplements.accept_supplements",
                side_effect=AssertionError("fact parser"),
            ),
        ):
            for operation in (
                lambda: sourced_rules.accept_applications([self.application]),
                lambda: sourced_rules.load_applications("/must/not/read/facts.json"),
                lambda: sourced_rules.load_applications("/must/not/read/legacy.yaml"),
            ):
                with self.subTest(operation=operation), self.assertRaisesRegex(LineageError, "inadmissible"):
                    operation()

    def test_configure_install_and_incorporation_cannot_enable_factual_inference(self):
        client = SimpleNamespace(rule_applications=[self.application])
        operations = (
            lambda: sourced_rules.configure(client, [self.application], None, {}),
            lambda: sourced_rules.incorporated(client, {}, applications=[self.application]),
            lambda: sourced_rules.incorporated_group(client, {}, applications=[self.application]),
            lambda: sourced_rules.project_application_intents(client, {}),
            lambda: sourced_rules.install_ruleset_facts(client, SimpleNamespace()),
            lambda: sourced_rules.install_ruleset_facts(SimpleNamespace(), SimpleNamespace(supplements=[{}])),
        )
        with patch.object(Path, "open", side_effect=AssertionError("retired helper touched storage")):
            for operation in operations:
                with self.subTest(operation=operation), self.assertRaisesRegex(LineageError, "inadmissible"):
                    operation()

    def test_read_only_legacy_fingerprint_keeps_default_spelling_and_scope(self):
        original = sourced_rules.application_fingerprint(self.application)
        self.assertEqual(
            original, sourced_rules.application_fingerprint({**self.application, "refine_accepted_dates": True})
        )
        self.assertNotEqual(
            original, sourced_rules.application_fingerprint({**self.application, "refine_accepted_dates": False})
        )
        self.assertNotEqual(
            original, sourced_rules.application_fingerprint({**self.application, "symbols": ["OTHER"]})
        )
        with self.assertRaisesRegex(LineageError, "generic lineage recovery"):
            sourced_rules.accept_applications([self.application])

    def policy(self, *, published=False):
        item = {
            "obligation_id": "legacy-obligation",
            "status": "pending",
            "end_exclusive": "2024-01-10",
            "required_group": ["COMMON", "UNIT"],
            "required_sourced_facts": {"legacy-application": self.fingerprint},
        }
        entry = {
            "sha256": "closed-receipt",
            "source_sha256": "closed-pricing",
            "dependencies": [],
            "asset_ids": [],
            "group_members": ["COMMON", "UNIT"],
            "projection": {
                "processed_through": "2024-01-10",
                "update_obligation_id": "legacy-obligation",
                "pricing_request": {},
            },
        }
        index = {"symbols": {"UNIT": entry} if published else {}}
        work = {"UNIT": item, "PEER": {"obligation_id": "peer-obligation", "end_exclusive": "2024-01-10"}}
        return WorkPolicy(Path("/synthetic/no-store"), index, work, [], "2024-01-10"), item, entry

    def test_unresolved_legacy_obligation_requires_generic_recovery_and_independent_peer_continues(self):
        policy, item, _ = self.policy()
        self.assertEqual("generic_lineage_recovery_required", restriction(item, policy.end))
        self.assertFalse(policy.allowed({"COMMON", "UNIT"}))
        self.assertTrue(policy.allowed({"PEER"}))
        with self.assertRaises(WorkDeferred):
            policy.stamp({"UNIT": {}}, {})
        receipt = {"symbol": "PEER", "source_sha256": "peer-generation", "evidence": {"asset_lookups": []}}
        source = {"latest_request": {"end_exclusive": "2024-01-10"}}
        self.assertEqual({"PEER": "PEER"}, policy.stamp({"PEER": receipt}, {"PEER": source}))
        self.assertEqual("peer-obligation", receipt["update_obligation_id"])

    def test_peer_or_auxiliary_application_markers_cannot_manufacture_new_completion(self):
        for field in ("peer", "auxiliary", "changed_scope"):
            policy, item, _ = self.policy()
            receipt = {"evidence": {"sourced_fact_applications": {"legacy-application": self.fingerprint}}}
            if field == "auxiliary":
                receipt["unavailable_pricing"] = {"COMMON": {"evidence": receipt["evidence"]}}
            if field == "changed_scope":
                item["required_sourced_facts"] = {"legacy-application": "b" * 64}
            with self.subTest(field=field), self.assertRaises(WorkDeferred):
                policy.stamp({"UNIT": receipt}, {})
            self.assertNotIn("update_obligation_id", receipt)
        with self.assertRaisesRegex(LineageError, "generic lineage recovery"):
            obligation(item)

    def test_matching_completed_stamp_is_consumed_without_reopening_old_provenance(self):
        policy, _, entry = self.policy(published=True)
        with patch.object(sourced_rules, "items", side_effect=AssertionError("completed fact body reread")):
            self.assertIs(entry, policy.satisfying_entry("UNIT", {"UNIT": entry}, {}))
            self.assertIs(entry, policy.satisfying_entry("UNIT", {"UNIT": entry}, {}))
            changed = {**entry, "projection": {**entry["projection"], "processed_through": "2024-01-09"}}
            self.assertIsNone(policy.satisfying_entry("UNIT", {"UNIT": changed}, {}))

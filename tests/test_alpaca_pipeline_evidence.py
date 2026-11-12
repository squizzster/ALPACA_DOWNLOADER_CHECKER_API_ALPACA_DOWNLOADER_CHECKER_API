"""Fresh-process metadata handoffs preserve accepted work and real invalidation."""

import json
import subprocess
import sys
import unittest
from unittest.mock import patch

from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pipeline.evidence import export_evidence, import_evidence
from libs.alpaca_pricing_download.contract import failure_outcome
from libs.alpaca_security_lineage.alpaca_http import IdentityHttpError


class NoRequests:
    request_count = 0

    def get_asset_lookup(self, *args):
        raise AssertionError("repeated completed lookup")

    def get_corporate_actions(self, *args):
        raise AssertionError("repeated completed actions")


def asset():
    return {
        "id": "uuid-aaa",
        "symbol": "AAA",
        "class": "us_equity",
        "exchange": "NASDAQ",
        "name": "Example",
        "status": "active",
        "tradable": True,
    }


def observed_client():
    observation = {"catalogue_observed_at_utc": "2026-11-12T12:00:00+00:00", "records": [asset()]}
    evidence = EvidenceClient(NoRequests(), observation=observation)
    evidence.assets["OLD"] = {"outcome": "not_found", "http_status": 404, "asset": None}
    evidence._remember(
        ("AAA", "2026-01-01", "2026-01-31"),
        [
            {
                "id": "event-1",
                "action_type": "name_change",
                "process_date": "2026-01-10",
                "old_symbol": "OLD",
                "new_symbol": "AAA",
            },
        ],
    )
    return evidence


def transport(evidence, *, extra=None):
    encoder = Encoder()
    document = export_evidence(evidence, encoder.encode)
    external = encoder.encode(extra)
    wire = json.loads(json.dumps({"evidence": document, "objects": encoder.objects, "extra": external}))
    decoder = Decoder(wire["objects"])
    return import_evidence(wire["evidence"], NoRequests(), decoder.decode), decoder.decode(wire["extra"])


class PipelineEvidenceTests(unittest.TestCase):
    def test_positive_negative_unattempted_and_coverage_survive_without_producer_calls(self):
        original = observed_client()
        original.exact_lookup_reasons["DUP"] = "catalogue_ambiguous"
        original.catalogue["DUP"] = None
        with (
            patch.object(EvidenceClient, "__init__", side_effect=AssertionError("constructor replay")),
            patch.object(EvidenceClient, "_load_catalogue", side_effect=AssertionError("catalogue assembly")),
            patch.object(EvidenceClient, "_checked_actions", side_effect=AssertionError("action acceptance")),
            patch("libs.alpaca_lineage_build.metadata.accept_supplements", side_effect=AssertionError("facts")),
        ):
            restored, _ = transport(original)
            self.assertEqual(restored.observe_catalogue(), original.observation)
            self.assertEqual(restored.get_symbol_lookup("AAA")["asset"], asset())
            self.assertEqual(restored.get_asset_lookup("OLD")["outcome"], "not_found")
            self.assertIsNone(restored.observed_symbol_lookup("UNATTEMPTED"))
            self.assertNotIn("UNATTEMPTED", restored.assets)
            self.assertEqual(
                restored.get_corporate_actions("AAA", "2026-01-05", "2026-01-15"),
                original.actions["AAA", "2026-01-01", "2026-01-31"],
            )
            self.assertIsNone(restored.catalogue["DUP"])
            self.assertEqual(restored.exact_lookup_reasons["DUP"], "catalogue_ambiguous")

    def test_failed_lookup_catalogue_and_action_remain_failures_with_status(self):
        original = EvidenceClient(NoRequests())
        denied = IdentityHttpError(403, "access denied")
        denied.request_identifiers = ["DENIED"]
        original.assets["DENIED"] = denied
        original.actions["DENIED", "2026-01-01", "2026-01-31"] = denied
        original.catalogue_attempted = True
        original.catalogue_error = denied
        restored, _ = transport(original)
        for operation in (
            lambda: restored.get_asset_lookup("DENIED"),
            lambda: restored.get_corporate_actions("DENIED", "2026-01-01", "2026-01-31"),
            restored.observe_catalogue,
        ):
            with self.assertRaises(IdentityHttpError) as caught:
                operation()
            self.assertEqual(caught.exception.request_identifiers, ["DENIED"])
            self.assertEqual(failure_outcome(caught.exception), failure_outcome(denied))
        self.assertIs(restored.assets["DENIED"], restored.catalogue_error)

    def test_projection_aliases_survive_and_changed_observation_selects_new_work(self):
        original = observed_client()
        action = original.actions["AAA", "2026-01-01", "2026-01-31"][0]
        supplement = {"kind": "issuer_capital_repayment", "match": {"id": "event-1"}}
        projected = {**action, "accepted_projection": True}
        original.event_supplements["event-1"] = supplement
        original.event_projection.retain(action, supplement, projected)
        original.rule_applications = [
            {
                "id": "rule",
                "symbols": ("AAA",),
                "fingerprint": "accepted",
                "keys": frozenset({"event-1"}),
                "facts": (supplement,),
            }
        ]
        original.rule_fact_scopes = {"event-1": {"AAA"}}
        original.applied_event_supplements.add("event-1")
        restored, _ = transport(original)
        actions = restored.actions["AAA", "2026-01-01", "2026-01-31"]
        fact = restored.event_supplements["event-1"]
        with patch(
            "libs.alpaca_security_lineage.event_supplements._project_action",
            side_effect=AssertionError("completed projection replay"),
        ):
            selected, facts = restored.event_projection.bind([fact])(actions)
            self.assertEqual(selected, [projected])
            self.assertIs(facts[0], restored.rule_applications[0]["facts"][0])
        changed = {**actions[0], "new_symbol": "OTHER"}
        with patch(
            "libs.alpaca_security_lineage.event_supplements._project_action", return_value=changed
        ) as produce:
            self.assertEqual(restored.event_projection.bind([fact])([changed])[0], [changed])
            produce.assert_called_once_with(changed, fact)
        self.assertEqual(restored.rule_fact_scopes, {"event-1": {"AAA"}})

    def test_retained_evidence_identity_is_shared_with_other_handoff_sections(self):
        original = EvidenceClient(NoRequests())
        accepted = {"sourced_successions": {"schema_version": 1, "facts": []}}
        original.retain_event_supplements([accepted])
        restored, external = transport(original, extra=accepted)
        with patch(
            "libs.alpaca_lineage_build.metadata.canonical_extension",
            side_effect=AssertionError("retained fact validation replay"),
        ):
            restored.retain_event_supplements([external])
        self.assertIs(restored._retained_supplement_evidence[id(external)], external)

    def test_conflicts_and_restrictions_do_not_disappear(self):
        original = observed_client()
        original.action_conflicts.add(("name_change", "event-1"))
        original.restricted_identifiers.add("BLOCKED")
        restored, _ = transport(original)
        with self.assertRaisesRegex(LineageError, "conflicting corporate-action"):
            restored.get_corporate_actions("AAA", "2026-01-01", "2026-01-31")
        with self.assertRaisesRegex(LineageError, "deferred"):
            restored.get_asset_lookup("BLOCKED")

    def test_incomplete_or_incompatible_evidence_is_rejected(self):
        original = observed_client()
        encoder = Encoder()
        document = export_evidence(original, encoder.encode)
        for version in (True, 0, 2):
            with self.assertRaisesRegex(LineageError, "unsupported"):
                import_evidence(
                    {**document, "schema_version": version}, NoRequests(), Decoder(encoder.objects).decode
                )
        with self.assertRaisesRegex(LineageError, "incomplete"):
            import_evidence(document, NoRequests(), lambda value: {})

    def test_fresh_process_consumes_completed_observations_without_provider(self):
        encoder = Encoder()
        document = export_evidence(observed_client(), encoder.encode)
        payload = json.dumps({"evidence": document, "objects": encoder.objects}).encode()
        code = """
import json, sys
from libs.alpaca_pipeline.codec import Decoder
from libs.alpaca_pipeline.evidence import import_evidence
class NoRequests:
    def get_asset_lookup(self, *args): raise AssertionError("provider lookup")
    def get_corporate_actions(self, *args): raise AssertionError("provider actions")
doc = json.load(sys.stdin)
evidence = import_evidence(doc["evidence"], NoRequests(), Decoder(doc["objects"]).decode)
assert evidence.observe_catalogue()["records"][0]["symbol"] == "AAA"
assert evidence.get_symbol_lookup("AAA")["asset"]["id"] == "uuid-aaa"
assert evidence.get_asset_lookup("OLD")["outcome"] == "not_found"
assert len(evidence.get_corporate_actions("AAA", "2026-01-01", "2026-01-31")) == 1
print("completed")
"""
        completed = subprocess.run(
            [sys.executable, "-B", "-c", code], input=payload, capture_output=True, check=False
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode())
        self.assertEqual(completed.stdout, b"completed\n")


if __name__ == "__main__":
    unittest.main()

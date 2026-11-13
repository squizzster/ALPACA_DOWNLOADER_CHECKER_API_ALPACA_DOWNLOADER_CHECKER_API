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
    def test_active_authored_fact_fields_cannot_be_exported(self):
        for field in (
            "event_supplements",
            "retained_event_supplements",
            "rule_applications",
            "rule_fact_scopes",
            "application_projections",
            "_retained_supplement_evidence",
        ):
            with self.subTest(field=field):
                original = observed_client()
                setattr(original, field, {"authored": True})
                with self.assertRaisesRegex(LineageError, "research assertions"):
                    export_evidence(original, Encoder().encode)

    def test_positive_negative_unattempted_and_coverage_survive_without_producer_calls(self):
        original = observed_client()
        original.exact_lookup_reasons["DUP"] = "catalogue_ambiguous"
        original.catalogue["DUP"] = None
        with (
            patch.object(EvidenceClient, "__init__", side_effect=AssertionError("constructor replay")),
            patch.object(EvidenceClient, "_load_catalogue", side_effect=AssertionError("catalogue assembly")),
            patch.object(EvidenceClient, "_checked_actions", side_effect=AssertionError("action acceptance")),
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

    def test_event_projection_cannot_be_exported(self):
        from types import SimpleNamespace

        original = observed_client()
        original.event_projection = SimpleNamespace(accepted={"action": ("fact", "projected")})
        with self.assertRaisesRegex(LineageError, "event projections"):
            export_evidence(original, Encoder().encode)

    def test_closed_legacy_provenance_can_travel_without_active_fact_types(self):
        from libs.alpaca_security_lineage.event_supplements import _freeze

        closed = _freeze({"evidence": {"sourced_successions": {"schema_version": 1, "facts": []}}})
        restored, external = transport(observed_client(), extra=closed)
        self.assertEqual(external, closed)
        self.assertEqual(type(external), dict)
        self.assertEqual(restored.get_symbol_lookup("AAA")["asset"], asset())

    def test_restored_legacy_active_fields_are_rejected_before_observation(self):
        from libs.alpaca_pipeline.evidence import _FIELDS

        original = observed_client()
        state = {field: getattr(original, field) for field in _FIELDS}
        encoder = Encoder()
        state["accepted_event_projections"] = [{"authored": True}]
        document = {
            "schema": "alpaca.pipeline.evidence",
            "schema_version": 1,
            "state": encoder.encode(state),
            "provider_evidence_views": encoder.encode([]),
        }
        with self.assertRaisesRegex(LineageError, "research assertion state"):
            import_evidence(document, NoRequests(), Decoder(encoder.objects).decode)

    def test_authored_action_annotation_cannot_enter_provider_cache(self):
        original = observed_client()
        original.actions["AAA", "2026-01-01", "2026-01-31"][0]["identity_interpretation"] = {
            "automatic_exchange": True
        }
        with self.assertRaisesRegex(LineageError, "assertion|research|authored"):
            export_evidence(original, Encoder().encode)
        from libs.alpaca_pipeline.evidence import _FIELDS

        encoder = Encoder()
        state = {field: getattr(original, field) for field in _FIELDS}
        document = {
            "schema": "alpaca.pipeline.evidence",
            "schema_version": 1,
            "state": encoder.encode(state),
            "provider_evidence_views": encoder.encode([]),
        }
        with self.assertRaisesRegex(LineageError, "assertion|research|authored"):
            import_evidence(document, NoRequests(), Decoder(encoder.objects).decode)

    def test_provider_projection_cache_retains_original_alias_without_reassembly(self):
        original = observed_client()
        closed = {"sourced_successions": {"facts": [{"legacy": True}]}}
        projected = {"corporate_actions": [], "asset_lookups": []}
        original._provider_evidence_views[id(closed)] = (closed, projected)
        restored, (closed, projected) = transport(original, extra=(closed, projected))
        with patch(
            "libs.alpaca_lineage_build.metadata.provider_evidence",
            side_effect=AssertionError("projection replay"),
        ):
            self.assertIs(restored.provider_view(closed), projected)

    def test_completed_cache_restores_without_repeating_assertion_acceptance(self):
        from libs.alpaca_pipeline import evidence as evidence_transport
        from libs.alpaca_security_lineage.inference_evidence import ProviderEvidence

        original = observed_client()
        closed = {"sourced_successions": {"facts": [{"legacy": True}]}}
        projected = {"corporate_actions": [], "asset_lookups": []}
        original._provider_evidence_views[id(closed)] = closed, projected
        encoder = Encoder()
        guard = evidence_transport.reject_assertions
        with patch.object(evidence_transport, "reject_assertions", wraps=guard) as accept:
            document = export_evidence(original, encoder.encode)
        self.assertEqual(2, accept.call_count)  # Fresh cache state and its plain projection.
        external = encoder.encode((original.assets, original.catalogue, closed, projected))
        wire = json.loads(json.dumps({"document": document, "objects": encoder.objects, "extra": external}))
        with (
            patch.object(
                evidence_transport,
                "_require_provider_state",
                side_effect=AssertionError("cache acceptance repeated"),
            ),
            patch.object(
                ProviderEvidence, "__init__", side_effect=AssertionError("acceptance constructor repeated")
            ),
            patch.object(
                EvidenceClient, "_load_catalogue", side_effect=AssertionError("catalogue acceptance repeated")
            ),
        ):
            decoder = Decoder(wire["objects"])
            restored = import_evidence(wire["document"], NoRequests(), decoder.decode)
            assets, catalogue, closed, projected = decoder.decode(wire["extra"])
        self.assertIs(restored.assets, assets)
        self.assertIs(restored.catalogue, catalogue)
        self.assertIs(restored.provider_view(closed), projected)

    def test_plain_fresh_projection_rejects_assertions_before_cache_acceptance(self):
        from libs.alpaca_pipeline.evidence import _FIELDS

        original = observed_client()
        closed = {"completed": True}
        projected = {"identity_interpretation": {"authored": True}}
        original._provider_evidence_views[id(closed)] = closed, projected
        with self.assertRaisesRegex(LineageError, "assertion|research|authored"):
            export_evidence(original, Encoder().encode)
        encoder = Encoder()
        state = {field: getattr(original, field) for field in _FIELDS}
        document = {
            "schema": "alpaca.pipeline.evidence",
            "schema_version": 1,
            "state": encoder.encode(state),
            "provider_evidence_views": encoder.encode([(closed, projected)]),
        }
        with self.assertRaisesRegex(LineageError, "assertion|research|authored"):
            import_evidence(document, NoRequests(), Decoder(encoder.objects).decode)

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

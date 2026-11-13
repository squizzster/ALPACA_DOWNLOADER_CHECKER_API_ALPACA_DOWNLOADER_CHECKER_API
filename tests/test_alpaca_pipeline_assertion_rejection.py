"""Fresh JSON/candidate rejection does not inspect closed publication values."""

from dataclasses import fields
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_lineage_build import prepared_recovery, storage
from libs.alpaca_lineage_build.contract import COMPUTATION_REVISION, LineageError
from libs.alpaca_lineage_build.prepared_recovery import CANDIDATE_BOUNDARY, accept_candidate
from libs.alpaca_lineage_build.ruleset_contract import AcceptedRuleset
from libs.alpaca_pipeline.codec import Decoder, Encoder, HandoffError


def candidate():
    return {
        "version": 1,
        "boundary": CANDIDATE_BOUNDARY,
        "store": "/synthetic",
        "computation_revision": COMPUTATION_REVISION,
        "policy": {"policy_role": "production", "semantic_sha256": "a" * 64},
        "review": {"version": 1, "status": "approved", "conclusion": "Reviewed completed output."},
        "members": ["AAA"],
        "pricing_generations": {"AAA": "b" * 64},
        "receipt_generations": {"AAA": "c" * 64},
        "candidate_receipts": {"AAA": {"evidence": {}}},
        "candidate_boundaries": {"AAA": {}},
        "baseline_receipts": {"AAA": {}},
    }


class AssertionAdmissionTests(TestCase):
    def test_approved_facts_fail_before_validators_locks_or_publication(self):
        for field in ("sourced_market_occupancy", "event_supplements", "sourced_successions"):
            value = candidate()
            value["candidate_receipts"]["AAA"]["evidence"][field] = {"facts": [{"authored": True}]}
            with (
                self.subTest(field=field),
                patch.object(storage, "writer_lock", side_effect=AssertionError("storage lock")),
                patch.object(storage, "publish", side_effect=AssertionError("publication")),
                patch.object(prepared_recovery, "validate_handoff", side_effect=AssertionError("revalidation")),
                patch.object(
                    prepared_recovery, "accept_observation_choices", side_effect=AssertionError("choices")
                ),
                self.assertRaisesRegex(LineageError, "fact injection"),
            ):
                accept_candidate(value, "/synthetic")

    def test_reviewed_factual_scope_cannot_turn_into_a_completed_capability(self):
        value = candidate()
        value["candidate_receipts"]["AAA"]["research_admission"] = {
            "approved_scope": {"canonical_applications": {"authored": "accepted"}},
        }
        with self.assertRaisesRegex(LineageError, "factual applications"):
            accept_candidate(value, "/synthetic")

    def test_old_configuration_fields_cannot_restore_an_application_registry(self):
        encoder = Encoder()
        reference = encoder.encode(AcceptedRuleset(None, None, "accepted"))
        record = encoder.objects[reference["ref"]]
        self.assertEqual(set(record["fields"]), {field.name for field in fields(AcceptedRuleset)})
        record["fields"]["applications"] = None
        record["fields"]["supplements"] = None
        with self.assertRaisesRegex(HandoffError, "incomplete producer record"):
            Decoder(encoder.objects).decode(reference)

    def test_completed_recovery_values_preserve_research_provenance_without_inference(self):
        from libs.alpaca_lineage_build.recovery_handoffs import operation, recall, remember
        from libs.alpaca_pipeline.state import export_recovery, restore_recovery

        receipt = {"evidence": {"sourced_successions": {"facts": [{"legacy": True}]}}}
        encoder = Encoder()
        with operation("/synthetic"):
            remember("/synthetic", "receipt", ("AAA", "accepted"), receipt)
            document = export_recovery("/synthetic", encoder.encode)
        with (
            operation("/synthetic"),
            patch(
                "libs.alpaca_pipeline.evidence.reject_assertions", side_effect=AssertionError("old provenance")
            ),
            patch.object(storage, "read_receipt", side_effect=AssertionError("receipt reread")),
        ):
            restore_recovery("/synthetic", document, Decoder(encoder.objects).decode)
            self.assertEqual(recall("/synthetic", "receipt", ("AAA", "accepted")), receipt)

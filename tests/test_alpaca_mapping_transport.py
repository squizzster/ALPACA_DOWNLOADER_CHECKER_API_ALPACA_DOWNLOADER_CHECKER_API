"""Completed mapped acquisition state crosses stages without producer replay."""

import json
import unittest
from unittest.mock import patch

from libs.alpaca_lineage_build.numerical_mapping import (
    AcceptedAssessments,
    PendingNumericalWork,
    accept_assessments,
)
from libs.alpaca_pipeline.codec import Decoder, Encoder, HandoffError
from libs.alpaca_pipeline.source_state import export_sourced
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.mapping import MappingAcquisition, MappingCoverageIncomplete
from libs.alpaca_pricing_download.responses import CompletedBars


def roundtrip(value):
    encoder = Encoder()
    reference = encoder.encode(value)
    objects, reference = json.loads(json.dumps([encoder.objects, reference]))
    return Decoder(objects).decode(reference)


def partial_acquisition():
    start = timestamp("2030-01-02T05:00:00Z")
    middle = timestamp("2030-01-03T05:00:00Z")
    end = timestamp("2030-01-04T05:00:00Z")
    first = (start.isoformat(), "1", "1", "1", "1", "100", "1", "1")
    last = (middle.isoformat(), "1", "1", "1", "1", "100", "1", "1")
    witness = CompletedBars("NEXT", "all", start, middle, "2030-01-03", (first,))
    value = MappingAcquisition(
        "/synthetic",
        "NEXT",
        "selected-generation",
        start,
        end,
        "2030-01-03",
        "candidate",
        witness,
        required_sessions=("2030-01-02", "2030-01-03"),
    )
    raw = CompletedBars("NEXT", "raw", start, end, value.asof, (first, last))
    value.state.accept("raw", raw)
    value.state.advance()
    return value, witness, raw


class MappingTransportTests(unittest.TestCase):
    def test_partial_state_keeps_aliases_and_completed_decisions_without_replay(self):
        value, witness, raw = partial_acquisition()
        decision = value.state.decision
        with (
            patch.object(MappingAcquisition, "__init__", side_effect=AssertionError("constructor replay")),
            patch.object(MappingAcquisition, "document", side_effect=AssertionError("durable encoding")),
            patch.object(MappingAcquisition, "from_document", side_effect=AssertionError("durable ingress")),
            patch(
                "libs.alpaca_pricing_download.adaptive.decide_adaptive_adjustment_downloads",
                side_effect=AssertionError("selection repeated"),
            ),
            patch("libs.alpaca_pricing_download.adaptive.digest", side_effect=AssertionError("hash repeated")),
            patch(
                "libs.alpaca_pricing_download.contract.transform", side_effect=AssertionError("normalization")
            ),
        ):
            restored, alias, state, restored_raw = roundtrip([value, witness, value.state, raw])
        self.assertIs(restored.fragments[0], alias)
        self.assertIs(restored.state, state)
        self.assertIs(state.observed["raw"], restored_raw)
        self.assertEqual(state.decision, decision)
        self.assertEqual(state.hashes, value.state.hashes)
        self.assertEqual(restored.source_generation, "selected-generation")

    def test_source_work_cache_carries_mapping_state_without_identity_key_reconstruction(self):
        value, witness, _ = partial_acquisition()
        selected = {"source_work": {"numerical_mapping": {"selected": {"state": value, "witness": witness}}}}
        packed = export_sourced(selected, roots=[])
        restored = roundtrip(packed)["source_work"]["numerical_mapping"]["selected"]
        self.assertIs(restored["state"].fragments[0], restored["witness"])

    def test_unknown_acquisition_version_rejects_before_body_decode(self):
        value, _, _ = partial_acquisition()
        encoder = Encoder()
        reference = encoder.encode(value)
        record = encoder.objects[reference["ref"]]
        record["schema_version"] = 2
        record["fields"]["state"] = {"ref": len(encoder.objects) + 100}
        with self.assertRaisesRegex(HandoffError, "incomplete accepted acquisition record"):
            Decoder(encoder.objects).decode(reference)

    def test_missing_state_capability_fails_closed(self):
        value, _, _ = partial_acquisition()
        encoder = Encoder()
        reference = encoder.encode(value)
        del encoder.objects[reference["ref"]]["fields"]["fragments"]
        with self.assertRaisesRegex(HandoffError, "incomplete accepted acquisition record"):
            Decoder(encoder.objects).decode(reference)

    def test_completed_coverage_limit_keeps_its_owner_and_scope(self):
        error = MappingCoverageIncomplete(("2030-01-03",))
        restored = roundtrip(error)
        self.assertIs(type(restored), MappingCoverageIncomplete)
        self.assertEqual(restored.missing_sessions, ("2030-01-03",))

    def test_completed_assessment_keeps_acceptance_and_aliases_without_ingress_replay(self):
        receipt = {
            "numerical_mapping_assessments": {
                "version": 1,
                "decisions": [
                    {
                        "candidate_id": "candidate",
                        "binding": {"members": ["PREV", "NEXT"]},
                        "outcome": "provider_mapping_unavailable",
                        "witness": {"session": "2030-01-02"},
                        "result_generation": None,
                        "scope": {"kind": "one_session_all_witness", "session": "2030-01-02"},
                    }
                ],
            }
        }
        accepted = accept_assessments(receipt)
        with patch.object(AcceptedAssessments, "__init__", side_effect=AssertionError("acceptance replay")):
            restored, alias = roundtrip([receipt, accepted])
        self.assertIsInstance(alias, AcceptedAssessments)
        self.assertIs(restored["numerical_mapping_assessments"], alias)
        self.assertIs(accept_assessments(restored), alias)

    def test_suspended_request_and_completed_acquisition_share_one_transport(self):
        acquisition, witness, _ = partial_acquisition()
        pending = PendingNumericalWork(required={"NEXT": {"mapping_acquisition": acquisition}})
        restored, alias, observed = roundtrip([pending, acquisition, witness])
        self.assertIs(restored.required["NEXT"]["mapping_acquisition"], alias)
        self.assertIs(alias.fragments[0], observed)


if __name__ == "__main__":
    unittest.main()

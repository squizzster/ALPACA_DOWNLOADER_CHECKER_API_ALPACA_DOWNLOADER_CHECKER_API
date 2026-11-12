"""Producer acceptances cross JSON without replaying their completed work."""

import json
import unittest
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_lineage_build.assignment_handoff import _FrozenDict
from libs.alpaca_lineage_build.source_choice_handoff import AcceptedSourceBundles
from libs.alpaca_pipeline.codec import DATACLASSES, Decoder, Encoder, HandoffError
from libs.alpaca_pricing_download.adaptive import CompletedAdjustments
from libs.alpaca_pricing_download.endpoints import Endpoints, EndpointSelection, HistoryComparison
from libs.alpaca_pricing_download.full_publication import FullPublicationAcceptance


def roundtrip(value):
    encoder = Encoder()
    selected = encoder.encode(value)
    objects, selected = json.loads(json.dumps([encoder.objects, selected]))
    return Decoder(objects).decode(selected)


class PipelineCodecTests(unittest.TestCase):
    def test_complete_pricing_acceptances_keep_values_scope_and_aliases(self):
        row = ("2026-11-10T21:00:00Z", "1", "1", "1", "1", "1", "1", "1")
        comparison = HistoryComparison("unchanged", (), (row,))
        selection = EndpointSelection(
            "/synthetic", "AAA", "generation", "2026-11-11", "2026-11-10", (row,), comparison=comparison
        )
        publication = FullPublicationAcceptance(
            Path("/synthetic"),
            "AAA",
            "generation",
            "2016-01-04",
            "2026-11-10",
            "2026-11-11",
            row,
            row,
            (row,),
            None,
            None,
        )
        restored = roundtrip([selection, comparison, publication])
        self.assertEqual(restored, [selection, comparison, publication])
        self.assertIs(restored[0].comparison, restored[1])
        self.assertIs(restored[1].live_all[0], restored[2].first_all)

    def test_completed_derived_values_do_not_recalculate(self):
        now = datetime(2026, 11, 11, tzinfo=timezone.utc)
        accepted = CompletedAdjustments("AAA", now, now, "2026-11-10", (), (), (), "all_equal", False)
        endpoints = Endpoints({"all": ()}, tail_all=())
        for name, value in {
            "views": {"all": ()},
            "endpoints": endpoints,
            "extent": (None, None),
            "proof": {"accepted": True},
        }.items():
            object.__setattr__(accepted, name, value)
        with (
            patch(
                "libs.alpaca_pricing_download.adaptive.endpoints_of",
                side_effect=AssertionError("repeated endpoints"),
            ),
            patch(
                "libs.alpaca_pricing_download.adaptive.session_extent",
                side_effect=AssertionError("repeated extent"),
            ),
        ):
            restored, restored_endpoints = roundtrip([accepted, endpoints])
            self.assertIs(restored.endpoints, restored_endpoints)
            self.assertEqual(restored.extent, (None, None))
            self.assertEqual(restored.proof, {"accepted": True})
            self.assertEqual(restored.endpoints.tail_all, ())

    def test_uncomputed_derivations_stay_uncomputed(self):
        now = datetime(2026, 11, 11, tzinfo=timezone.utc)
        accepted = CompletedAdjustments("AAA", now, now, "2026-11-10", (), (), (), "all_equal", False)
        with patch(
            "libs.alpaca_pricing_download.adaptive.endpoints_of",
            side_effect=AssertionError("transport calculated endpoints"),
        ):
            restored = roundtrip(accepted)
        self.assertNotIn("endpoints", restored.__dict__)
        self.assertNotIn("views", restored.__dict__)

    def test_accepted_bundle_extra_acceptance_is_carried(self):
        observations = _FrozenDict({"accepted": ("exact",)})
        accepted = AcceptedSourceBundles(
            {"version": 1, "revision_id": "selected", "identities": {}}, observations
        )
        restored, body = roundtrip([accepted, observations])
        self.assertIs(restored.observations, body)
        self.assertEqual(restored, accepted)

    def test_all_allowlisted_dataclass_records_restore_without_unknown_types(self):
        # These values exercise transport, not owning domain validation.
        for name, cls in DATACLASSES.items():
            with self.subTest(name=name):
                value = cls(**{field.name: None for field in fields(cls)})
                self.assertEqual(roundtrip(value), value)

    def test_unknown_producer_type_fails_closed(self):
        with self.assertRaisesRegex(HandoffError, "unsupported handoff value"):
            roundtrip(object())


class PipelineStateCapabilityTests(unittest.TestCase):
    def context(self):
        from libs.alpaca_lineage_build.context import OperationContext

        with patch("libs.alpaca_lineage_build.context.validate_version"):
            return OperationContext("/synthetic")

    def evidence(self):
        from libs.alpaca_lineage_build.metadata import EvidenceClient
        from libs.alpaca_pipeline.evidence import _FIELDS

        result = object.__new__(EvidenceClient)
        for name in _FIELDS:
            setattr(result, name, {})
        return result

    def test_generation_bound_recovery_caches_cross_without_calculation(self):
        from libs.alpaca_pipeline.state import export_state, import_state

        context, evidence = self.context(), self.evidence()
        metadata = {"evidence": "already accepted"}
        context.sourced_operations[evidence] = {
            "metadata": {("AAA", "generation", False): metadata},
            "pricing_prefix": {("generation", "receipt"): (None, (True, True))},
            "source_work": {"metadata": {}, "plans": {}},
        }
        encoder = Encoder()
        document = export_state(context, None, encoder.encode, evidence=evidence)
        objects, document = json.loads(json.dumps([encoder.objects, document]))
        with (
            patch(
                "libs.alpaca_lineage_build.context.validate_version", side_effect=AssertionError("revalidated")
            ),
            patch(
                "libs.alpaca_lineage_build.selection.DependencyGraph.update",
                side_effect=AssertionError("recalculated graph"),
            ),
        ):
            restored, _ = import_state("/synthetic", document, Decoder(objects).decode, evidence=evidence)
        self.assertEqual(restored.sourced_operations[evidence], context.sourced_operations[evidence])

    def test_completed_identity_caches_bind_restored_producer_objects(self):
        from libs.alpaca_pipeline.state import export_state, import_state

        context, evidence = self.context(), self.evidence()
        report, prior = {"accepted": True}, {"prior": True}
        evidence.retained_symbols = {"AAA": report}
        context.index = {"held": prior}
        context.sourced_operations[evidence] = {
            "metadata": {("AAA", "generation", False): report, ("PRIOR", "generation", False): prior},
            "source_work": {
                "owner_applicability": {("AAA", id(prior), id(report)): True},
                "documented_continuities": {(("AAA", id(report)),): ({"complete": True}, None)},
                "donor_grid_ids": {("AAA", "generation", id(prior), ()): {"public-id"}},
                "completed_source_work": {
                    ("policy", (("AAA", "/synthetic", "generation", id(prior), id(report)),), ()): {
                        "controls": (),
                        "evidence": {id(report): report},
                        "outputs": {"complete": True},
                    }
                },
            },
        }
        encoder = Encoder()
        document = export_state(context, None, encoder.encode, evidence=evidence)
        objects, document = json.loads(json.dumps([encoder.objects, document]))
        restored, _ = import_state("/synthetic", document, Decoder(objects).decode, evidence=evidence)
        held = restored.sourced_operations[evidence]
        restored_report = held["metadata"]["AAA", "generation", False]
        restored_prior = held["metadata"]["PRIOR", "generation", False]
        self.assertTrue(
            held["source_work"]["owner_applicability"]["AAA", id(restored_prior), id(restored_report)]
        )
        self.assertEqual(
            held["source_work"]["documented_continuities"][(("AAA", id(restored_report)),)][0],
            {"complete": True},
        )
        scope = ("policy", (("AAA", "/synthetic", "generation", id(restored_prior), id(restored_report)),), ())
        completed = held["source_work"]["completed_source_work"][scope]
        self.assertIs(completed["evidence"][id(restored_report)], restored_report)
        self.assertEqual(completed["outputs"], {"complete": True})

    def test_missing_identity_reference_fails_closed(self):
        from libs.alpaca_lineage_build.contract import LineageError
        from libs.alpaca_pipeline.state import export_state

        context, evidence = self.context(), self.evidence()
        context.sourced_operations[evidence] = {
            "source_work": {"owner_applicability": {("AAA", "receipt", 123456789): True}}
        }
        with self.assertRaisesRegex(LineageError, "lost its held producer object"):
            export_state(context, None, Encoder().encode, evidence=evidence)

    def test_unknown_sourced_cache_cannot_silently_disappear(self):
        from libs.alpaca_lineage_build.contract import LineageError
        from libs.alpaca_pipeline.state import export_state

        context, evidence = self.context(), self.evidence()
        context.sourced_operations[evidence] = {"future_producer": {}}
        with self.assertRaisesRegex(LineageError, "unsupported sourced-operation cache"):
            export_state(context, None, Encoder().encode, evidence=evidence)


class PipelineObservedErrorTests(unittest.TestCase):
    def test_http_failure_keeps_domain_status_and_closed_transport(self):
        from urllib.error import HTTPError, URLError

        from libs.alpaca_pricing_download.contract import failure_outcome

        transport = HTTPError("https://example.invalid", 503, "unavailable", None, None)
        original = URLError(transport)
        original.__cause__ = transport
        restored = roundtrip(original)
        self.assertIsInstance(restored, URLError)
        self.assertIsInstance(restored.reason, HTTPError)
        self.assertIs(restored.reason, restored.__cause__)
        self.assertEqual(failure_outcome(restored), failure_outcome(original))
        self.assertTrue(restored.reason.fp is None or restored.reason.fp.closed)

    def test_domain_failure_preserves_classification_and_evidence(self):
        from libs.alpaca_pricing_download.contract import (
            BarsSymbolUnavailable,
            InitialPricingUnavailable,
            failure_outcome,
        )
        from libs.alpaca_pricing_download.failures import AcquisitionFailure, RetainedAcquisitionError

        rejected = BarsSymbolUnavailable("AAA", 400)
        initial = InitialPricingUnavailable({"start": "2026-11-01"}, rejected)
        initial.__cause__ = rejected
        failures = [
            rejected,
            initial,
            RetainedAcquisitionError(AcquisitionFailure("http_error", 503, "failed", symbol="AAA")),
        ]
        for error in failures:
            with self.subTest(error=type(error).__name__):
                restored = roundtrip(error)
                self.assertEqual(failure_outcome(restored), failure_outcome(error))


class PipelineSourcePlanTests(unittest.TestCase):
    def test_completed_plan_projection_keys_and_adapter_decisions_cross(self):
        from libs.alpaca_lineage_build.pricing import MetadataSourceControl
        from libs.alpaca_lineage_build.source_work import SourceWorkPlan
        from libs.alpaca_pipeline.source_state import export_sourced

        evidence, previous = {"metadata": "accepted"}, {"receipt": "accepted"}
        memberships, documented = {"asset": {"public_id": "held"}}, {"owners": []}
        projection_key = ("/synthetic", "AAA", "receipt", "generation", id(memberships), id(documented), True)
        plan = SourceWorkPlan(
            {"AAA": "A"},
            memberships,
            {},
            {},
            {},
            {},
            {},
            True,
            documented_membership=documented,
            closed_report_projections={projection_key: {"complete": True}},
        )
        control = MetadataSourceControl("/synthetic", "AAA", {}, "generation", {}, previous)
        control.observation_instant_keys = {"held": datetime(2026, 11, 11, tzinfo=timezone.utc)}
        control._accepted_binding_applies = True
        control.accepted_common_routes = ({"route": "accepted"},)
        bound = (("AAA", id(evidence)),)
        market_key = (bound, (("AAA", "receipt", "generation"),))
        cache = {
            "source_work": {
                "documented_plans": {market_key: plan},
                "completed_source_work": {
                    ("policy", (("AAA", "/synthetic", "generation", "receipt", id(evidence)),), ()): {
                        "controls": (control,),
                        "evidence": {id(evidence): evidence},
                    }
                },
            }
        }
        packed = export_sourced(cache, roots=(evidence, previous))
        restored, restored_evidence = roundtrip([packed, evidence])
        plans = restored["source_work"]["documented_plans"]
        restored_plan = plans[((("AAA", id(restored_evidence)),), (("AAA", "receipt", "generation"),))]
        key = (
            "/synthetic",
            "AAA",
            "receipt",
            "generation",
            id(restored_plan.memberships),
            id(restored_plan.documented_membership),
            True,
        )
        self.assertEqual(restored_plan.closed_report_projections[key], {"complete": True})
        scope = ("policy", (("AAA", "/synthetic", "generation", "receipt", id(restored_evidence)),), ())
        restored_control = restored["source_work"]["completed_source_work"][scope]["controls"][0]
        self.assertTrue(restored_control._accepted_binding_applies)
        self.assertEqual(restored_control.observation_instant_keys, control.observation_instant_keys)
        self.assertEqual(restored_control.accepted_common_routes, ({"route": "accepted"},))

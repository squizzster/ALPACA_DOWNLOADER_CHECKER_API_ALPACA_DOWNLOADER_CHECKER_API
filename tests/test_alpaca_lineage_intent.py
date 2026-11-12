"""Scoped application intent changes consume accepted chronology without rebinding."""

from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_debug as fixtures
import yaml
from test_alpaca_documented_markets import documented_fact
from test_alpaca_lineage_build import IdentityClient, asset

from libs.alpaca_lineage_build import documented_updates, metadata, pipeline, storage
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.sourced_rules import accept_applications
from libs.alpaca_lineage_debug.policy import DEFAULT_POLICY


class ApplicationIntentTests(TestCase):
    setUp = fixtures.DebugStoreTests.setUp

    def apply(self, refine):
        fact = documented_fact(
            "generic-chronology",
            ("GOOG", "111111111"),
            ("RENAMED", "111111111"),
            [("RENAMED", "111111111", "2016-01-01", None, "Continuing")],
        )
        application = {
            "id": "generic-chronology",
            "symbols": ["GOOG"],
            "supplements": [fact],
        }
        if refine is not None:
            application["refine_accepted_dates"] = refine
        applications, supplements = accept_applications([application])
        client = IdentityClient({"GOOG": asset("GOOG"), "111111111": asset("GOOG")})
        evidence = metadata.EvidenceClient(client, event_supplements=supplements)
        evidence.rule_applications = applications
        result = pipeline.run_lineage(self.store, ["GOOG"], identity_client=client, evidence_client=evidence)
        self.assertEqual(result["failed"], 0, result)
        root = self.store / "end_of_day/lineage"
        index = storage.read_index(root)
        receipt = storage.read_receipt(root, "GOOG", index)
        boundaries = storage.read_boundaries(root, receipt)
        labels = {
            segment["historical_symbol"]
            for document in boundaries.values()
            for view in document["adjustments"]
            for segment in view["segments"]
            if segment.get("security_identity")
        }
        return receipt, labels

    def test_unresolved_only_then_positive_refinement_needs_no_new_fact_or_owner_binding(self):
        first, labels = self.apply(False)
        self.assertIn("GOOG", labels)  # Accepted positives survive newly allocated unresolved dates.
        self.assertIs(first["evidence"]["sourced_fact_intents"]["generic-chronology"], False)
        candidate = yaml.safe_load(self.policy.read_text())
        candidate["research"].update(
            control_mode="accepted_publication",
            documented_facts=[
                {
                    "symbols": ["GOOG"],
                    "supplements": first["evidence"]["sourced_market_occupancy"]["facts"],
                    "refine_accepted_dates": True,
                }
            ],
        )
        self.policy.write_text(yaml.safe_dump(candidate))
        with patch.object(
            documented_updates.documented_markets,
            "bind",
            side_effect=AssertionError("research rebound accepted owner"),
        ):
            research = fixtures.request(
                "run",
                store=str(self.store),
                symbols=["GOOG"],
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
            )
        self.assertTrue(research["execution_complete"], research)
        self.assertGreater(research["groups"][0]["comparison"]["differences"]["semantic"]["count"], 0)
        self.assertFalse(research["groups"][0]["new_numerical_scope"])
        with (
            patch.object(
                documented_updates.documented_markets,
                "bind",
                side_effect=AssertionError("completed owner rebound"),
            ),
            patch.object(pipeline, "correlate", side_effect=AssertionError("accepted ownership replay")),
            patch.object(
                pipeline, "select_observations", side_effect=AssertionError("completed numerical replay")
            ),
        ):
            second, labels = self.apply(True)
        self.assertEqual(labels, {"RENAMED"})
        self.assertEqual(first["documented_identity_membership"], second["documented_identity_membership"])
        self.assertIs(second["evidence"]["sourced_fact_intents"]["generic-chronology"], True)

    def test_standalone_producer_consumes_canonical_facts_without_another_fact_acceptance(self):
        fact = documented_fact(
            "canonical-chronology",
            ("GOOG", "111111111"),
            ("RENAMED", "111111111"),
            [("RENAMED", "111111111", "2016-01-01", None, "Continuing")],
        )
        applications, supplements = accept_applications(
            [
                {
                    "id": "canonical-chronology",
                    "symbols": ["GOOG"],
                    "supplements": [fact],
                    "refine_accepted_dates": False,
                }
            ]
        )
        context = OperationContext(self.store)
        context.accept_ruleset()
        context._ruleset = replace(context._ruleset, applications=tuple(applications), supplements=supplements)
        client = IdentityClient({"GOOG": asset("GOOG"), "111111111": asset("GOOG")})
        with patch(
            "libs.alpaca_security_lineage.event_supplements.validate_supplements",
            side_effect=AssertionError("accepted canonical facts validated again"),
        ):
            result = pipeline.run_lineage(self.store, ["GOOG"], identity_client=client, context=context)
        self.assertEqual(result["failed"], 0, result)
        root = self.store / "end_of_day/lineage"
        receipt = storage.read_receipt(root, "GOOG", storage.read_index(root))
        self.assertIs(receipt["evidence"]["sourced_fact_intents"]["canonical-chronology"], False)

    def test_removing_unresolved_only_option_restores_default_refinement_without_rebinding(self):
        self.apply(False)
        with patch.object(
            documented_updates.documented_markets,
            "bind",
            side_effect=AssertionError("default restoration rebound owner"),
        ):
            receipt, labels = self.apply(None)
        self.assertEqual(labels, {"RENAMED"})
        self.assertIs(receipt["evidence"]["sourced_fact_intents"]["generic-chronology"], True)

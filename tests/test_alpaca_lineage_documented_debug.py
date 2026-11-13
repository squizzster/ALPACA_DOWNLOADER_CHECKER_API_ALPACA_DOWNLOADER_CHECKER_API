"""Diagnostic research accepts generic rules, never authored fact candidates."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import test_alpaca_lineage_debug as debug_fixtures
import yaml
from test_alpaca_lineage_debug import request

from alpaca_end_of_day import LineageDebugError
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_debug import VERSION, accept_report, documented, implementation_binding, research
from libs.alpaca_lineage_debug.policy import DEFAULT_POLICY, load_policy, policy_applies, template


class FactCandidateRejectionTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.policy = self.directory / "candidate.yaml"

    def test_removed_fact_inputs_fail_before_any_store_selection(self):
        original = yaml.safe_load(template())
        inputs = [
            {"documented_facts": []},
            {"documented_facts": [{"symbols": ["FUTURE"], "supplements": "/no/facts.json"}]},
            {"control_mode": "accepted_publication"},
            {"control_mode": "documented_scopes"},
            {"assertion_overlays": []},
        ]
        for options in inputs:
            value = copy.deepcopy(original)
            value["research"].update(options)
            self.policy.write_text(yaml.safe_dump(value))
            with (
                self.subTest(options=options),
                patch.object(research, "select", side_effect=AssertionError("fact input touched store")),
                patch.object(storage, "read_index", side_effect=AssertionError("fact input read selector")),
                self.assertRaises(LineageDebugError),
            ):
                request("run", store="/not/a/store", symbols=["FUTURE"], policy=str(self.policy))

    def test_direct_research_calls_reject_legacy_fact_objects_before_storage(self):
        for policy in (
            SimpleNamespace(documented_facts=()),
            SimpleNamespace(control_mode="accepted_publication"),
            SimpleNamespace(ruleset=SimpleNamespace(applications=({"id": "fact"},))),
        ):
            with (
                self.subTest(policy=policy),
                patch.object(research, "select", side_effect=AssertionError("legacy object touched store")),
                self.assertRaisesRegex(ValueError, "fact"),
            ):
                research.run("/not/a/store", policy, ["FUTURE"])
        with self.assertRaisesRegex(ValueError, "fact-backed"):
            documented.evaluate(None, None, None)

    def test_factual_saved_report_is_rejected_before_storage_even_with_current_binding(self):
        self.policy.write_text(template())
        policy = load_policy(self.policy)
        saved = {
            "schema": "alpaca.lineage.debug.report",
            "schema_version": 1,
            "debug_version": VERSION,
            "implementation_binding": implementation_binding(),
            "operation": "run",
            "candidate_policy": {**policy.as_report_object(), "research": {"documented_facts": []}},
        }
        with (
            patch.object(research, "select", side_effect=AssertionError("fact report touched store")),
            self.assertRaisesRegex(ValueError, "fact-backed"),
        ):
            research.run("/not/a/store", policy, ["FUTURE"], reuse_report=saved)

    def test_previous_generic_reports_remain_accepted_but_fact_options_do_not(self):
        report = {
            "schema": "alpaca.lineage.debug.report",
            "schema_version": 1,
            "debug_version": 5,
            "operation": "run",
            "implementation_binding": {
                "debug": 5,
                "policy_lab": 22,
                "correlation_runtime": 96,
                "lineage_runtime": 100,
                "membership_refinement": "accepted_same_security_containing_component_v1",
            },
        }
        accept_report(report, "run", allow_previous=True)
        self.policy.write_text(template())
        policy = load_policy(self.policy)
        saved_policy = policy.as_report_object()
        self.assertTrue(policy_applies(saved_policy, policy))
        saved_policy["research"]["documented_facts"] = []
        with self.assertRaisesRegex(ValueError, "unsupported research"):
            policy_applies(saved_policy, policy)
        report["candidate_policy"] = saved_policy
        with self.assertRaisesRegex(ValueError, "fact-backed"):
            accept_report(report, "run", allow_previous=True)

    def test_generic_report_can_retain_frozen_legacy_receipt_provenance(self):
        report = {
            "schema": "alpaca.lineage.debug.report",
            "schema_version": 1,
            "debug_version": VERSION,
            "implementation_binding": implementation_binding(),
            "operation": "screen",
            "candidate_policy": {"research": {"membership_refinement": None}},
            "candidate_groups": [
                {"receipts": {"FUTURE": {"evidence": {"event_supplements": [{"id": "legacy"}]}}}}
            ],
        }
        accept_report(report, "screen")
        report["candidate_policy"]["research"]["documented_facts"] = []
        with self.assertRaisesRegex(ValueError, "fact-backed"):
            accept_report(report, "screen")


class GenericDiagnosticCarryTests(unittest.TestCase):
    setUp = debug_fixtures.DebugStoreTests.setUp

    def test_generic_completed_peer_report_reuses_without_correlation_or_payload_reads(self):
        arguments = {
            "store": str(self.store),
            "symbols": ["GOOG"],
            "policy": str(self.policy),
            "control_policy": str(DEFAULT_POLICY),
        }
        completed = request("run", **arguments)
        self.assertTrue(completed["execution_complete"], completed)
        saved = self.directory / "completed.json"
        saved.write_text(json.dumps(completed))
        with (
            patch.object(research, "evaluate_group", side_effect=AssertionError("completed group replay")),
            patch.object(storage, "read_receipt", side_effect=AssertionError("completed receipt reread")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("completed boundary reread")),
            patch(
                "libs.alpaca_lineage_policy_lab.comparison.load_pricing", side_effect=AssertionError("prices")
            ),
        ):
            reused = request("run", **arguments, reuse_report=str(saved))
        self.assertTrue(reused["execution_complete"], reused)
        self.assertEqual([["GOOG"]], reused["reused_groups"])
        self.assertEqual(completed["groups"], reused["groups"])

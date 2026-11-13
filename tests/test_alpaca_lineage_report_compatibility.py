"""Known completed research survives integration without repeating its producers."""

import copy
import json
from dataclasses import fields, replace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_debug as fixtures

from alpaca_end_of_day import LineageDebugError
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_debug import accept_report, implementation_binding, paired
from libs.alpaca_lineage_debug.policy import (
    DEFAULT_POLICY,
    ResearchPolicy,
    compiler,
    control_view,
    load_policy,
    policy_applies,
)
from libs.alpaca_lineage_policy_lab import comparison


def historical_bindings():
    producer = "accepted_same_security_containing_component_v1"
    return [
        {
            "debug": 3,
            "policy_lab": 22,
            "correlation_runtime": 95,
            "lineage_runtime": 98,
            "membership_refinement": producer,
        },
        *(
            {"debug": version, "policy_lab": 21, "correlation_runtime": 95, "membership_refinement": producer}
            for version in (2, 3)
        ),
        {
            "debug": 4,
            "policy_lab": 22,
            "correlation_runtime": 96,
            "lineage_runtime": 99,
            "membership_refinement": producer,
        },
        *(
            {
                "debug": 6,
                "policy_lab": 23,
                "correlation_runtime": 97,
                "lineage_runtime": lineage,
                "membership_refinement": producer,
            }
            for lineage in (101, 102)
        ),
    ]


def report_for(binding):
    return {
        "schema": "alpaca.lineage.debug.report",
        "schema_version": 1,
        "debug_version": binding["debug"],
        "implementation_binding": binding,
        "operation": "run",
    }


class ReportBindingCompatibilityTests(TestCase):
    def test_only_explicit_known_previous_bindings_are_supported_for_reuse(self):
        accept_report(report_for(implementation_binding()), "run")
        for binding in historical_bindings():
            report = report_for(binding)
            with self.subTest(binding=binding):
                accept_report(report, "run", allow_previous=True)
                with self.assertRaisesRegex(ValueError, "incompatible contract"):
                    accept_report(report, "run")
        for field, value in (
            ("policy_lab", 23),
            ("correlation_runtime", 96),
            ("lineage_runtime", 99),
            ("membership_refinement", "unknown_producer"),
        ):
            binding = {**historical_bindings()[0], field: value}
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "incompatible contract"):
                accept_report(report_for(binding), "run", allow_previous=True)
        mismatched = {**report_for(historical_bindings()[0]), "debug_version": 2}
        with self.assertRaisesRegex(ValueError, "incompatible contract"):
            accept_report(mismatched, "run", allow_previous=True)
        # Compatibility is a deliberate claim by this integration, not blanket
        # permission for a future implementation to inherit old calculations.
        future = implementation_binding()
        future["lineage_runtime"] += 1
        with patch("libs.alpaca_lineage_debug.implementation_binding", return_value=future):
            with self.assertRaisesRegex(ValueError, "incompatible contract"):
                accept_report(report_for(historical_bindings()[0]), "run", allow_previous=True)

    def test_empty_control_research_options_match_absence_without_erasing_candidates(self):
        control = control_view(load_policy(DEFAULT_POLICY))
        values = {field.name: getattr(control, field.name) for field in fields(compiler.CompiledPolicy)}
        empty = ResearchPolicy(**values)
        self.assertTrue(policy_applies(control.as_report_object(), empty))
        self.assertTrue(policy_applies(empty.as_report_object(), control))
        recorded = {
            **control.as_report_object(),
            "research": {"membership_refinement": "accepted_same_security_containing_component_v1"},
        }
        self.assertFalse(policy_applies(recorded, control))
        for options in (
            {"control_mode": "accepted_publication"},
            {"documented_facts": [{"symbols": ["GOOG"], "supplements": []}]},
            {"unknown_option": False},
        ):
            recorded = {**control.as_report_object(), "research": options}
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, "research inputs"):
                policy_applies(recorded, control)
        changed_policy = replace(empty, semantic_sha256="f" * 64)
        self.assertFalse(policy_applies(control.as_report_object(), changed_policy))


class CompletedHistoricalReportReuseTests(TestCase):
    setUp = fixtures.DebugStoreTests.setUp

    def test_known_previous_reports_reuse_exact_results_and_retain_scope_invalidation(self):
        report = fixtures.request(
            "run",
            store=str(self.store),
            policy=str(self.policy),
            control_policy=str(DEFAULT_POLICY),
            symbols=["GOOG"],
        )
        path = self.directory / "previous-comparison.json"

        def consume():
            return fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                symbols=["GOOG"],
                reuse_report=str(path),
            )

        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("accepted receipt reopened")),
            patch.object(
                storage, "read_boundaries", side_effect=AssertionError("accepted assignments reopened")
            ),
            patch.object(comparison, "load_pricing", side_effect=AssertionError("accepted prices reopened")),
            patch.object(
                paired, "_compare_loaded_group", side_effect=AssertionError("accepted replay repeated")
            ),
        ):
            for binding in historical_bindings():
                previous = {
                    **report,
                    "debug_version": binding["debug"],
                    "implementation_binding": binding,
                    "control_policy": {
                        **report["control_policy"],
                        "research": {"membership_refinement": None},
                    },
                }
                path.write_text(json.dumps(previous))
                with self.subTest(binding=binding):
                    result = consume()
                    self.assertTrue(result["execution_complete"], result)
                    self.assertEqual(1, result["summary"]["reused_groups"])
                    self.assertEqual(report["groups"], result["groups"])
            for invalidation in ("store", "selector", "receipt"):
                changed = copy.deepcopy(previous)
                if invalidation == "store":
                    changed["store"] = "/different-store"
                elif invalidation == "selector":
                    changed["scope"]["selector"] = {}
                else:
                    changed["groups"][0]["baseline"]["receipt_generations"]["GOOG"] = "0" * 64
                path.write_text(json.dumps(changed))
                with (
                    self.subTest(invalidation=invalidation),
                    self.assertRaisesRegex(
                        LineageDebugError, "incompatible operation bindings|changed receipt generation"
                    ),
                ):
                    consume()

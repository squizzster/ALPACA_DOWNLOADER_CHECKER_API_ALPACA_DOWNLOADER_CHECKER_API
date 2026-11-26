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
            "debug": 28,
            "policy_lab": 27,
            "correlation_runtime": 111,
            "lineage_runtime": 130,
            "membership_refinement": producer,
        },
        {
            "debug": 27,
            "policy_lab": 27,
            "correlation_runtime": 110,
            "lineage_runtime": 129,
            "membership_refinement": producer,
        },
        {
            "debug": 26,
            "policy_lab": 27,
            "correlation_runtime": 100,
            "lineage_runtime": 123,
            "membership_refinement": producer,
        },
        {
            "debug": 25,
            "policy_lab": 27,
            "correlation_runtime": 100,
            "lineage_runtime": 122,
            "membership_refinement": producer,
        },
        {
            "debug": 24,
            "policy_lab": 27,
            "correlation_runtime": 100,
            "lineage_runtime": 121,
            "membership_refinement": producer,
        },
        {
            "debug": 23,
            "policy_lab": 26,
            "correlation_runtime": 99,
            "lineage_runtime": 120,
            "membership_refinement": producer,
        },
        {
            "debug": 22,
            "policy_lab": 26,
            "correlation_runtime": 99,
            "lineage_runtime": 119,
            "membership_refinement": producer,
        },
        {
            "debug": 21,
            "policy_lab": 26,
            "correlation_runtime": 99,
            "lineage_runtime": 118,
            "membership_refinement": producer,
        },
        {
            "debug": 17,
            "policy_lab": 26,
            "correlation_runtime": 99,
            "lineage_runtime": 114,
            "membership_refinement": producer,
        },
        {
            "debug": 20,
            "policy_lab": 26,
            "correlation_runtime": 99,
            "lineage_runtime": 117,
            "membership_refinement": producer,
        },
        {
            "debug": 19,
            "policy_lab": 26,
            "correlation_runtime": 99,
            "lineage_runtime": 116,
            "membership_refinement": producer,
        },
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
        {
            "debug": 8,
            "policy_lab": 23,
            "correlation_runtime": 97,
            "lineage_runtime": 103,
            "membership_refinement": producer,
        },
        {
            "debug": 7,
            "policy_lab": 23,
            "correlation_runtime": 97,
            "lineage_runtime": 102,
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
        {
            "debug": 9,
            "policy_lab": 23,
            "correlation_runtime": 97,
            "lineage_runtime": 104,
            "membership_refinement": producer,
        },
        {
            "debug": 12,
            "policy_lab": 24,
            "correlation_runtime": 97,
            "lineage_runtime": 107,
            "membership_refinement": producer,
        },
        {
            "debug": 13,
            "policy_lab": 24,
            "correlation_runtime": 97,
            "lineage_runtime": 108,
            "membership_refinement": producer,
        },
        {
            "debug": 13,
            "policy_lab": 24,
            "correlation_runtime": 97,
            "lineage_runtime": 109,
            "membership_refinement": producer,
        },
        {
            "debug": 13,
            "policy_lab": 24,
            "correlation_runtime": 97,
            "lineage_runtime": 110,
            "membership_refinement": producer,
        },
        {
            "debug": 15,
            "policy_lab": 24,
            "correlation_runtime": 97,
            "lineage_runtime": 112,
            "membership_refinement": producer,
        },
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

    def test_v93_control_has_original_semantics_and_enabled_producers(self):
        from libs.alpaca_lineage_build.correlation_policy_contract import RETAINED_V93_SEMANTIC_SHA256
        from libs.alpaca_lineage_build.policy_engine import _correlation_cache

        frozen = load_policy(DEFAULT_POLICY.with_name("alpaca_security_correlation_v93_legacy_policy.yaml"))
        current = load_policy(DEFAULT_POLICY)
        self.assertEqual(RETAINED_V93_SEMANTIC_SHA256, frozen.semantic_sha256)
        self.assertEqual(current.membership_refinement, frozen.membership_refinement)
        self.assertIsNotNone(frozen.membership_refinement)
        self.assertFalse(_correlation_cache(frozen).retired_label_observations)
        self.assertTrue(_correlation_cache(current).retired_label_observations)
        self.assertFalse(policy_applies(frozen.as_report_object(), current))

    def test_v104_control_keeps_original_successor_rules_and_provenance(self):
        from libs.alpaca_end_of_day_security_correlation.component_refinement import membership_policy_version
        from libs.alpaca_lineage_build.contract import versions
        from libs.alpaca_lineage_build.correlation_policy_contract import RETAINED_V104_SEMANTIC_SHA256
        from libs.alpaca_lineage_build.policy_engine import _correlation_cache
        from libs.alpaca_lineage_policy_lab.contract import replayable_lineage_versions

        frozen = load_policy(DEFAULT_POLICY.with_name("alpaca_security_correlation_v104_legacy_policy.yaml"))
        current = load_policy(DEFAULT_POLICY)
        self.assertEqual(RETAINED_V104_SEMANTIC_SHA256, frozen.semantic_sha256)
        self.assertEqual(frozen.row_rules, current.row_rules)
        self.assertEqual(frozen.dated_labels, current.dated_labels)
        self.assertEqual(frozen.membership_refinement, current.membership_refinement)
        for policy, closed_successor_bridge in ((frozen, False), (current, True)):
            with self.subTest(policy=policy.execution_profile):
                cache = _correlation_cache(policy)
                self.assertTrue(cache.processing_date_successor_seams)
                self.assertTrue(cache.former_ticker_occupation)
                self.assertEqual(closed_successor_bridge, cache.closed_successor_processing_date_bridges)
                self.assertEqual(
                    94,
                    membership_policy_version({"membership_version": policy.implementation_correlation_version}),
                )
        self.assertTrue(policy_applies(frozen.as_report_object(), frozen))
        self.assertFalse(policy_applies(frozen.as_report_object(), current))
        self.assertFalse(policy_applies(current.as_report_object(), frozen))
        published = {**versions(), "correlation": 104, "correlation_policy_sha256": frozen.semantic_sha256}
        self.assertTrue(replayable_lineage_versions(published))
        self.assertFalse(replayable_lineage_versions({**published, "correlation_policy_sha256": "a" * 64}))

    def test_empty_control_research_options_match_absence_without_erasing_candidates(self):
        control = replace(
            control_view(load_policy(DEFAULT_POLICY)), membership_refinement=None, dated_labels=None
        )
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
        frozen = DEFAULT_POLICY.with_name("alpaca_security_correlation_v91_legacy_policy.yaml")
        report = fixtures.request(
            "run",
            store=str(self.store),
            policy=str(frozen),
            control_policy=str(frozen),
            symbols=["GOOG"],
        )
        path = self.directory / "previous-comparison.json"

        def consume(candidate=frozen):
            return fixtures.request(
                "run",
                store=str(self.store),
                policy=str(candidate),
                control_policy=str(frozen),
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
            with self.assertRaisesRegex(LineageDebugError, "changed generic policy bindings"):
                consume(DEFAULT_POLICY)
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

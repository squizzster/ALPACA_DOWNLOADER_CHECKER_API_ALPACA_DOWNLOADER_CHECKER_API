"""A complete metadata screen carries selection and preserves unknown scope."""

import copy
import json
from contextlib import nullcontext
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_component_refinement as component_fixtures
import test_alpaca_lineage_debug as debug_fixtures
import yaml
from test_alpaca_pricing_download import FakeClient, bar

from alpaca_end_of_day import LineageDebugError
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_debug import paired, research
from libs.alpaca_lineage_debug.policy import DEFAULT_POLICY
from libs.alpaca_lineage_policy_lab import comparison
from libs.alpaca_pricing_download.download import download_pricing


class ScreenTests(TestCase):
    setUp = debug_fixtures.DebugStoreTests.setUp

    def enable(self):
        value = yaml.safe_load(self.policy.read_text())
        value["research"]["membership_refinement"] = "accepted_same_security_containing_component_v1"
        self.policy.write_text(yaml.safe_dump(value))

    def screen(self):
        return debug_fixtures.request(
            "screen", store=str(self.store), policy=str(self.policy), control_policy=str(DEFAULT_POLICY)
        )

    def test_single_source_exclusion_has_no_receipt_or_price_reads(self):
        self.enable()
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("unnecessary receipt")),
            patch.object(comparison, "load_pricing", side_effect=AssertionError("no prices")),
        ):
            result = self.screen()
        self.assertEqual(result["summary"]["proven_candidate_unaffected_groups"], 2)
        self.assertEqual(result["summary"]["receipt_reads"], 0)
        self.assertEqual(result["expected_io"]["pricing_sources_if_evaluated"], 0)
        path = self.directory / "screen.json"
        path.write_text(json.dumps(result))
        with (
            patch.object(storage, "read_index", side_effect=AssertionError("selected index repeated")),
            patch.object(storage, "check_store", side_effect=AssertionError("store acceptance repeated")),
            patch.object(comparison, "load_pricing", side_effect=AssertionError("unnecessary prices")),
        ):
            impact = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                all=True,
                screen_report=str(path),
            )
        self.assertTrue(impact["execution_complete"])
        self.assertEqual(impact["summary"]["evaluated_groups"], 0)
        self.assertEqual(impact["summary"]["control_drift_unassessed_groups"], 2)
        self.assertEqual(impact["scope"]["unevaluated_groups"], [])

    def test_screen_requires_same_cascade_and_named_candidate(self):
        with self.assertRaises(LineageDebugError):
            self.screen()
        self.enable()
        with self.assertRaises(LineageDebugError):
            debug_fixtures.request(
                "screen", store=str(self.store), policy=str(self.policy), control_policy=str(self.policy)
            )
        value = yaml.safe_load(self.policy.read_text())
        value["execution"]["established_fixed_point"]["row_rules"].reverse()
        self.policy.write_text(yaml.safe_dump(value))
        with self.assertRaises(LineageDebugError):
            self.screen()

    def test_positive_and_missing_handoffs_are_retained_without_payloads(self):
        self.enable()
        _, _, selected = component_fixtures.fixture()
        index = {
            "projection_state": {"revision": 1, "complete": True},
            "symbols": {source: {"group_members": ["NEW", "OLD"]} for source in ("NEW", "OLD")},
        }
        from libs.alpaca_lineage_debug import screen as owner

        for missing in (False, True):
            receipts = copy.deepcopy(selected["receipts"])
            if missing:
                receipts["OLD"].pop("identity_memberships")
            with (
                patch.object(
                    owner, "select", return_value=(self.store, self.store / "end_of_day/lineage", index)
                ),
                patch.object(storage, "publication_lock", return_value=nullcontext()),
                patch.object(storage, "assert_index_current"),
                patch.object(
                    storage, "read_receipt", side_effect=lambda root, source, index: receipts[source]
                ) as reads,
                patch.object(comparison, "load_pricing", side_effect=AssertionError("no prices")),
            ):
                result = self.screen()
            self.assertEqual(reads.call_count, 2)
            self.assertEqual(result["summary"]["proven_candidate_unaffected_groups"], 0)
            self.assertEqual(result["summary"]["unknown_groups"], int(missing))
            self.assertEqual(result["summary"]["candidate_groups"], int(not missing))

    def test_completed_pair_is_reused_without_receipts_prices_or_calculations(self):
        report = debug_fixtures.request(
            "run",
            store=str(self.store),
            policy=str(self.policy),
            control_policy=str(DEFAULT_POLICY),
            symbols=["GOOG"],
        )
        path = self.directory / "completed.json"
        path.write_text(json.dumps(report))
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("receipt repeated")),
            patch.object(comparison, "load_pricing", side_effect=AssertionError("prices repeated")),
            patch.object(paired, "_compare_loaded_group", side_effect=AssertionError("calculation repeated")),
        ):
            result = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                symbols=["GOOG"],
                reuse_report=str(path),
            )
        self.assertTrue(result["execution_complete"])
        self.assertEqual(result["summary"]["reused_groups"], 1)
        self.assertEqual(result["groups"], report["groups"])
        report["store"] = "/somewhere/else"
        path.write_text(json.dumps(report))
        with self.assertRaises(LineageDebugError):
            debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                symbols=["GOOG"],
                reuse_report=str(path),
            )

    def test_changed_selector_rejects_the_screen_without_reselection(self):
        self.enable()
        result = self.screen()
        path = self.directory / "screen.json"
        path.write_text(json.dumps(result))
        with (
            patch.object(storage, "assert_index_current", side_effect=RuntimeError("changed selector")),
            patch.object(storage, "read_index", side_effect=AssertionError("no reconstruction")),
        ):
            with self.assertRaisesRegex(LineageDebugError, "changed selector"):
                debug_fixtures.request(
                    "run",
                    store=str(self.store),
                    policy=str(self.policy),
                    control_policy=str(DEFAULT_POLICY),
                    all=True,
                    screen_report=str(path),
                )

    def test_invalidated_reused_peer_keeps_completed_other_groups(self):
        report = debug_fixtures.request(
            "run", store=str(self.store), policy=str(self.policy), control_policy=str(DEFAULT_POLICY), all=True
        )
        path = self.directory / "completed.json"
        path.write_text(json.dumps(report))

        def applicable(store, index, result, accepted_store):
            if result["group"] == ["AAA"]:
                raise ValueError("changed pricing generation")

        with (
            patch.object(research, "assert_reused_applicable", side_effect=applicable),
            patch.object(paired, "_compare_loaded_group", side_effect=AssertionError("completed work repeated")),
        ):
            result = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                all=True,
                reuse_report=str(path),
            )
        self.assertFalse(result["execution_complete"])
        self.assertEqual(result["summary"]["failed_groups"], 1)
        self.assertEqual(result["summary"]["reused_groups"], 1)
        self.assertEqual(result["groups"][0]["group"], ["GOOG"])

    def test_saved_report_contract_and_implementation_are_required(self):
        report = debug_fixtures.request(
            "run",
            store=str(self.store),
            policy=str(self.policy),
            control_policy=str(DEFAULT_POLICY),
            symbols=["GOOG"],
        )
        path = self.directory / "completed.json"
        for field, invalid in (
            ("schema_version", 999),
            ("debug_version", 999),
            ("operation", "screen"),
            ("implementation_binding", {}),
        ):
            with self.subTest(field=field):
                changed = {**report, field: invalid}
                path.write_text(json.dumps(changed))
                with self.assertRaisesRegex(LineageDebugError, "incompatible contract"):
                    debug_fixtures.request(
                        "run",
                        store=str(self.store),
                        policy=str(self.policy),
                        control_policy=str(DEFAULT_POLICY),
                        symbols=["GOOG"],
                        reuse_report=str(path),
                    )

    def test_formatting_does_not_invalidate_completed_policy_meaning(self):
        report = debug_fixtures.request(
            "run",
            store=str(self.store),
            policy=str(self.policy),
            control_policy=str(DEFAULT_POLICY),
            symbols=["GOOG"],
        )
        path = self.directory / "completed.json"
        path.write_text(json.dumps(report))
        self.policy.write_text("# format-only change\n" + self.policy.read_text())
        with patch.object(
            paired, "_compare_loaded_group", side_effect=AssertionError("formatting repeated work")
        ):
            result = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                symbols=["GOOG"],
                reuse_report=str(path),
            )
        self.assertEqual(result["summary"]["reused_groups"], 1)

    def test_successful_groups_from_an_incomplete_attempt_are_carried(self):
        original = paired._compare_loaded_group

        def fail_one(store, seed, *args, **kwargs):
            if seed == "AAA":
                raise ValueError("deliberate peer failure")
            return original(store, seed, *args, **kwargs)

        with patch.object(paired, "_compare_loaded_group", side_effect=fail_one):
            report = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                all=True,
            )
        self.assertFalse(report["execution_complete"])
        path = self.directory / "partial.json"
        path.write_text(json.dumps(report))
        with patch.object(paired, "_compare_loaded_group", wraps=original) as evaluate:
            result = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                all=True,
                reuse_report=str(path),
            )
        self.assertTrue(result["execution_complete"])
        self.assertEqual(result["summary"]["reused_groups"], 1)
        self.assertEqual(evaluate.call_count, 1)

    def test_accepted_store_cannot_be_lent_to_another_root(self):
        accepted = storage.accept_store(self.store)
        bad = storage.AcceptedStore(self.store, self.store / "wrong-root")
        with patch.object(storage, "check_store", side_effect=AssertionError("acceptance repeated")):
            with self.assertRaisesRegex(RuntimeError, "another reader scope"):
                with storage.reader_snapshot(self.store, lambda root: {}, accepted_store=bad):
                    pass
            with storage.reader_snapshot(
                self.store, lambda root: storage.select_inputs(root, "GOOG"), accepted_store=accepted
            ) as snapshot:
                self.assertIn("GOOG", snapshot["receipts"])

    def test_real_changed_price_generation_invalidates_only_its_completed_group(self):
        report = debug_fixtures.request(
            "run", store=str(self.store), policy=str(self.policy), control_policy=str(DEFAULT_POLICY), all=True
        )
        path = self.directory / "completed.json"
        path.write_text(json.dumps(report))
        rows = [bar(day) for day in ("2016-01-04", "2025-12-31", "2026-09-30", "2026-10-01", "2026-10-14")]
        download_pricing(self.store, "GOOG", "2026-10-15", client=FakeClient(rows))
        with (
            patch("libs.alpaca_pricing_download.storage.read_update", return_value=None),
            patch.object(comparison, "load_pricing", side_effect=AssertionError("invalidated prices reopened")),
            patch.object(
                paired, "_compare_loaded_group", side_effect=AssertionError("completed comparison replayed")
            ),
        ):
            result = debug_fixtures.request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                all=True,
                reuse_report=str(path),
            )
        self.assertFalse(result["execution_complete"])
        self.assertEqual(result["summary"]["reused_groups"], 1)
        self.assertEqual(result["summary"]["failed_groups"], 1)
        self.assertEqual(result["groups"][0]["group"], ["AAA"])
        self.assertIn("changed pricing generation", str(result["failures"]))

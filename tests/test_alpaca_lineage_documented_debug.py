"""New researched chronology consumes accepted owners without control replay."""

import copy
import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_lineage_debug as debug_fixtures
import yaml
from test_alpaca_documented_markets import documented_fact
from test_alpaca_lineage_debug import request

from libs.alpaca_lineage_build import documented_updates, storage
from libs.alpaca_lineage_debug import documented
from libs.alpaca_lineage_debug.policy import DEFAULT_POLICY, load_policy, template
from libs.alpaca_lineage_policy_lab import comparison


class DocumentedDiagnosticTests(TestCase):
    setUp = debug_fixtures.DebugStoreTests.setUp

    def configure(self, *, facts=True, combined=False):
        value = yaml.safe_load(self.policy.read_text())
        value = value.get("policy", value)
        fact = documented_fact(
            "researched-market",
            ("GOOG", "111111111"),
            ("RENAMED", "111111111"),
            [
                ("RENAMED", "111111111", "2026-01-01", None, "Continuing"),
            ],
        )
        value["research"].update(
            control_mode="accepted_publication",
            documented_facts=[{"symbols": ["GOOG"], "supplements": [fact]}] if facts else [],
        )
        if combined:
            configuration = yaml.safe_load(template(combined=True))
            configuration["policy"] = value
            configuration["policy"]["research"] = {"control_mode": "accepted_publication"}
            configuration["evidence"]["applications"] = (
                [{"id": "researched-market", "symbols": ["GOOG"], "supplements": [fact]}] if facts else []
            )
            value = configuration
        self.policy.write_text(yaml.safe_dump(value))
        return value

    def run_pair(self):
        return request(
            "run",
            store=str(self.store),
            symbols=["GOOG"],
            policy=str(self.policy),
            control_policy=str(DEFAULT_POLICY),
        )

    def test_metadata_only_chronology_keeps_completed_numerical_choices_closed(self):
        self.configure(combined=True)
        read_receipt = storage.read_receipt

        def receipt(*args, **kwargs):
            result = copy.deepcopy(read_receipt(*args, **kwargs))
            result["evidence"]["asset_lookups"].append(
                {
                    "lookup_kind": "cusip",
                    "lookup": "111111111",
                    "outcome": "found",
                    "http_status": 200,
                    "asset": {"id": "GOOG-id"},
                }
            )
            return result

        with (
            patch.object(storage, "read_receipt", side_effect=receipt) as reads,
            patch.object(comparison, "correlate", side_effect=AssertionError("accepted owner replay")),
            patch(
                "libs.alpaca_lineage_build.observations.select_observations",
                side_effect=AssertionError("metadata change repeated numerical decision"),
            ),
            patch.object(
                documented_updates.documented_markets, "bind", wraps=documented_updates.documented_markets.bind
            ) as bind,
        ):
            report = request("run", store=str(self.store), symbols=["GOOG"], policy=str(self.policy))
        self.assertEqual("canonical_ruleset_default", report["control_selection"])
        self.assertTrue(report["execution_complete"], report)
        self.assertEqual(reads.call_count, 1)
        self.assertEqual(bind.call_count, 1)
        group = report["groups"][0]
        self.assertEqual(group["control_kind"], "accepted_publication_no_replay")
        self.assertFalse(group["new_numerical_scope"])
        self.assertTrue(group["published_to_control"]["parity"])
        self.assertEqual(group["comparison"]["differences"]["semantic"]["count"], 32)
        self.assertEqual(
            group["comparison"]["differences"]["observation"]["changed_fields"], {"observation_tickers": 16}
        )
        self.assertTrue(group["candidate_handoff"]["reports"])
        self.assertTrue(group["candidate_handoff"]["boundaries"])
        json.dumps(group["candidate_handoff"])

    def test_missing_exact_lookup_fails_without_provider_fallback_or_control_replay(self):
        self.configure()
        with patch.object(comparison, "correlate", side_effect=AssertionError("control replay")):
            report = self.run_pair()
        self.assertFalse(report["execution_complete"])
        self.assertIn("owning exact metadata recovery", str(report["failures"]))

    def test_combined_rule_only_change_uses_paired_replay_and_mixed_change_is_explicit(self):
        value = self.configure(facts=False, combined=True)
        value["policy"]["research"]["control_mode"] = "documented_scopes"
        value["policy"]["execution"]["established_fixed_point"]["row_rules"].pop(0)
        self.policy.write_text(yaml.safe_dump(value))
        with patch.object(documented, "evaluate", side_effect=AssertionError("rule edit used accepted control")):
            report = request("run", store=str(self.store), symbols=["GOOG"], policy=str(self.policy))
        self.assertTrue(report["execution_complete"], report)
        self.assertEqual("paired_same_inputs", report["groups"][0]["mode"])
        self.assertFalse(report["groups"][0]["control_correlation_reused"])

        value = self.configure(facts=True, combined=True)
        value["policy"]["research"]["control_mode"] = "documented_scopes"
        value["policy"]["execution"]["established_fixed_point"]["row_rules"].pop(0)
        self.policy.write_text(yaml.safe_dump(value))
        report = request("run", store=str(self.store), symbols=["GOOG"], policy=str(self.policy))
        self.assertFalse(report["execution_complete"], report)
        self.assertIn("not both", str(report["failures"]))

    def test_new_unsupported_fact_producer_fails_before_price_materialization(self):
        value = yaml.safe_load(template(combined=True))
        fact = next(
            fact
            for application in value["evidence"]["applications"]
            for fact in application["supplements"]
            if fact["kind"] == "identity_preserving_rename"
        )
        value["evidence"]["applications"] = [
            {"id": "unsupported-rename", "symbols": ["GOOG"], "supplements": [fact]}
        ]
        self.policy.write_text(yaml.safe_dump(value))
        with patch.object(
            comparison, "load_pricing", side_effect=AssertionError("unsupported facts materialized prices")
        ):
            report = request("run", store=str(self.store), symbols=["GOOG"], policy=str(self.policy))
        self.assertFalse(report["execution_complete"], report)
        self.assertIn("unsupported research-fact producer", str(report["failures"]))

    def test_unchanged_publication_control_is_exact_and_has_no_rule_or_numerical_replay(self):
        self.configure(facts=False)
        with (
            patch.object(comparison, "correlate", side_effect=AssertionError("control replay")),
            patch(
                "libs.alpaca_lineage_build.observations.select_observations",
                side_effect=AssertionError("numerical replay"),
            ),
        ):
            report = self.run_pair()
        self.assertTrue(report["summary"]["parity"], report)

    def test_fact_scope_and_mode_are_strict_and_production_rejects_research(self):
        original = self.configure()
        for change in ("mode", "unknown", "scope", "kind"):
            value = copy.deepcopy(original)
            if change == "mode":
                value["research"]["control_mode"] = "replay"
            if change == "unknown":
                value["research"]["extra"] = True
            if change == "scope":
                value["research"]["documented_facts"][0]["symbols"] = []
            if change == "kind":
                value["research"]["documented_facts"][0]["supplements"][0]["facts"]["predecessor"][
                    "instrument_kind"
                ] = "warrant"
            self.policy.write_text(yaml.safe_dump(value))
            with self.subTest(change=change), self.assertRaises(ValueError):
                load_policy(self.policy)
        self.policy.write_text(yaml.safe_dump(original))
        load_policy(self.policy)
        with self.assertRaisesRegex(ValueError, "crosses"):
            documented.scoped_facts(SimpleNamespace(documented_facts=({"symbols": ("GOOG", "AAA")},)), ["GOOG"])

    def test_completed_plan_merge_preserves_other_owners_and_rejects_conflicts(self):
        old_owner = {"source_fact_id": "old", "identity": {"security_id": "old-id"}}
        old = {"version": 1, "owners": [old_owner], "source_routes": {"OLD": ["old-uuid"]}}
        new = {
            "version": 1,
            "owners": [{"source_fact_id": "new", "identity": {"security_id": "new-id"}}],
            "source_routes": {"NEW": ["new-uuid"]},
        }
        result = documented.merge_plan(old, new)
        self.assertIs(result["owners"][0], old_owner)
        self.assertEqual(set(result["source_routes"]), {"OLD", "NEW"})
        with self.assertRaisesRegex(ValueError, "changed accepted documented owner"):
            documented.merge_plan(
                old, {**new, "owners": [{"source_fact_id": "old", "identity": {"security_id": "different"}}]}
            )
        with self.assertRaisesRegex(ValueError, "source context"):
            documented.merge_plan(old, {**new, "source_routes": {"OLD": ["different"]}})

    def test_scope_compatibility_carries_unaffected_completed_results(self):
        from libs.alpaca_lineage_debug.policy import policy_applies_to_group

        before = load_policy(self.policy)
        self.configure()
        value = yaml.safe_load(self.policy.read_text())
        value["research"]["control_mode"] = "documented_scopes"
        self.policy.write_text(yaml.safe_dump(value))
        after = load_policy(self.policy)
        self.assertTrue(policy_applies_to_group(before.as_report_object(), after, ["AAA"]))
        self.assertFalse(policy_applies_to_group(before.as_report_object(), after, ["GOOG"]))

    def test_explanation_metadata_crosses_to_run_without_reopening_index_receipts_or_assignments(self):
        self.configure(facts=False)
        capture = request("explain", store=str(self.store), symbol="GOOG", include_metadata_handoff=True)
        path = self.directory / "selected.json"
        path.write_text(json.dumps(capture))
        with (
            patch.object(storage, "read_index", side_effect=AssertionError("selected index reread")),
            patch.object(storage, "read_receipt", side_effect=AssertionError("accepted receipt reread")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("accepted assignments reread")),
            patch.object(comparison, "correlate", side_effect=AssertionError("accepted control replay")),
        ):
            result = request(
                "run",
                store=str(self.store),
                policy=str(self.policy),
                control_policy=str(DEFAULT_POLICY),
                symbols=["GOOG"],
                metadata_handoff=str(path),
            )
        self.assertTrue(result["summary"]["parity"], result)
        capture["metadata_handoff"]["selector"]["revision"] += 1
        path.write_text(json.dumps(capture))
        from alpaca_end_of_day import LineageDebugError

        with patch.object(comparison, "load_pricing", side_effect=AssertionError("invalid handoff read prices")):
            with self.assertRaises(LineageDebugError):
                request(
                    "run",
                    store=str(self.store),
                    policy=str(self.policy),
                    control_policy=str(DEFAULT_POLICY),
                    symbols=["GOOG"],
                    metadata_handoff=str(path),
                )

    def test_session_filter_cannot_claim_complete_metadata_capture(self):
        from alpaca_end_of_day import LineageDebugError

        with patch.object(storage, "reader_snapshot", side_effect=AssertionError("invalid scope touched store")):
            with self.assertRaisesRegex(LineageDebugError, "unfiltered"):
                request(
                    "explain",
                    store=str(self.store),
                    symbol="GOOG",
                    session="2026-10-01",
                    include_metadata_handoff=True,
                )

    def test_materialization_preserves_membership_donors_and_unknown_scope(self):
        self.configure()
        policy = load_policy(self.policy)
        values = {
            name: {
                "identity_memberships": [{"member_alpaca_asset_ids": members}],
                "evidence": {
                    "asset_lookups": [{"lookup_kind": "cusip", "lookup": "111111111", "asset": {"id": "owner"}}]
                },
            }
            for name, members in [("GOOG", ["owner"]), ("PEER", ["owner", "later"]), ("FOREIGN", ["other"])]
        }
        self.assertEqual(documented.materialize_sources(policy, values, list(values)), {"GOOG", "PEER"})
        from dataclasses import replace

        membership = replace(policy, membership_refinement="accepted_same_security_containing_component_v1")
        self.assertEqual(documented.materialize_sources(membership, values, list(values)), set(values))
        self.assertEqual(
            documented.materialize_sources(membership, values, list(values), membership_pairs_handoff=[]),
            {"GOOG", "PEER"},
        )
        pairs = [{"source": "FOREIGN", "donor": "PEER"}]
        self.assertEqual(
            documented.materialize_sources(membership, values, list(values), membership_pairs_handoff=pairs),
            set(values),
        )

    def test_loader_does_not_materialize_an_excluded_peer(self):
        from contextlib import contextmanager

        index = storage.read_index(self.store / "end_of_day/lineage")
        selected = storage.SelectedIndex(
            {name: index["symbols"][name] for name in ["AAA", "GOOG"]},
            self.store / "end_of_day/lineage",
            index["projection_state"],
        )
        with storage.reader_snapshot(self.store, lambda root: selected) as captured:
            held = copy.deepcopy(captured)
        generations = {name: receipt["source_sha256"] for name, receipt in held["receipts"].items()}
        for receipt in held["receipts"].values():
            receipt["input_sources"] = generations

        @contextmanager
        def snapshot(*args, **kwargs):
            yield held

        original = comparison.load_pricing

        def only_selected(store, name, **kwargs):
            if name != "GOOG":
                raise AssertionError("foreign peer prices materialized")
            return original(store, name, **kwargs)

        with (
            patch.object(comparison, "reader_snapshot", side_effect=snapshot),
            patch.object(comparison, "load_pricing", side_effect=only_selected) as reads,
        ):
            with comparison._load_group(
                self.store, "GOOG", materialize_symbols=lambda receipts: {"GOOG"}
            ) as result:
                self.assertEqual(set(result["inputs"]), {"GOOG"})
                self.assertEqual(set(result["receipts"]), {"AAA", "GOOG"})
        self.assertEqual(reads.call_count, 1)

    def test_invalid_metadata_envelope_fails_before_store_access(self):
        from alpaca_end_of_day import LineageDebugError

        self.configure(facts=False)
        path = self.directory / "invalid-metadata.json"
        for value in (None, {"metadata_handoff": None}, {"metadata_handoff": []}, {"boundary": "wrong"}):
            path.write_text(json.dumps(value))
            with (
                self.subTest(value=value),
                patch.object(
                    storage, "publication_lock", side_effect=AssertionError("malformed handoff touched store")
                ),
            ):
                with self.assertRaises(LineageDebugError):
                    request(
                        "run",
                        store=str(self.store),
                        policy=str(self.policy),
                        control_policy=str(DEFAULT_POLICY),
                        symbols=["GOOG"],
                        metadata_handoff=str(path),
                    )

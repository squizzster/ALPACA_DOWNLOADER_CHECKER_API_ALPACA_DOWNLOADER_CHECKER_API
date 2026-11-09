"""Sourced succession enters the complete cascade through one certificate."""

import copy
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import row_correlation, sourced_succession
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy, load_policy
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from tests.test_alpaca_name_change_evidence import (
    bars,
    exact_same_symbol_handoff,
    inputs_for,
    raw_segments,
)


def succession_evidence():
    evidence = exact_same_symbol_handoff()
    evidence["corporate_actions"] = [
        {
            "id": "old-dividend",
            "action_type": "cash_dividend",
            "symbol": "HOLD",
            "cusip": "111111111",
            "ex_date": "2024-01-03",
            "rate": 1,
        },
        {
            "id": "new-dividend",
            "action_type": "cash_dividend",
            "symbol": "HOLD",
            "cusip": "222222222",
            "ex_date": "2024-01-08",
            "rate": 1,
        },
    ]
    for lookup in evidence["asset_lookups"]:
        if lookup["lookup_kind"] == "cusip":
            lookup["lookup"] = "111111111" if lookup["asset"]["id"] == "old-id" else "222222222"
    for occupant in evidence["symbol_occupancy_evidence"]:
        occupant["cusip"] = "111111111" if occupant["asset_id"] == "old-id" else "222222222"
        occupant["evidence"] = []
    evidence["sourced_successions"] = {
        "schema_version": 1,
        "facts": [
            {
                "kind": "holding_company_succession",
                "match": {"source_id": "issuer-exchange-holding-substitution", "symbol": "HOLD"},
                "facts": {
                    "schema_version": 1,
                    "fact_kind": "holding_company_succession",
                    "old_cusip": "111111111",
                    "new_cusip": "222222222",
                    "legal_effective_date": "2024-01-04",
                    "exchange_effective_date": "2024-01-05",
                    "old_rate": 1,
                    "new_rate": 1,
                    "share_class": "common_stock",
                    "automatic_exchange": True,
                    "unchanged_rights": True,
                },
                "sources": [
                    {
                        "url": "https://issuer.example/holding-substitution",
                        "title": "Automatic holding company substitution",
                        "authority": "issuer",
                        "document_date": "2024-01-04",
                    }
                ],
            }
        ],
    }
    return evidence


class SourcedSuccessionTests(unittest.TestCase):
    days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")

    def evaluate(self, evidence, *, version=79):
        return correlate(
            Path("."),
            inputs_for({"HOLD": bars(self.days)}),
            {"HOLD": evidence},
            policy=replace(load_authorized_policy(), implementation_correlation_version=version),
        )["HOLD"]

    def test_complete_history_preserves_exchange_boundary_and_independent_provenance(self):
        evidence = succession_evidence()
        original = copy.deepcopy(evidence)
        with (
            patch(
                "libs.alpaca_end_of_day_security_correlation.name_change._grid",
                side_effect=AssertionError("prepared historical rename prices"),
            ),
            patch.object(
                row_correlation,
                "certify_sourced_successions",
                wraps=sourced_succession.certify_sourced_successions,
            ) as certify,
        ):
            report = self.evaluate(evidence)
        self.assertEqual(1, certify.call_count)
        self.assertEqual(original, evidence)
        self.assertEqual(0, report["uncorrelated"]["row_count"])
        self.assertEqual(len(self.days) * len(ADJUSTMENTS), report["correlated"]["row_count"])
        segments = raw_segments(report)
        self.assertEqual(["old-id", "new-id"], [s["canonical_security"]["alpaca_asset_id"] for s in segments])
        self.assertEqual([3, 2], [s["row_count"] for s in segments])
        self.assertEqual(1, len({s["security_identity"]["security_id"] for s in segments}))
        self.assertEqual(
            {("new-id", "old-id")}, {tuple(s["security_identity"]["member_alpaca_asset_ids"]) for s in segments}
        )
        edge = report["uuid_lineage_edges"][0]
        self.assertEqual("2024-01-04", edge["legal_effective_date"])
        self.assertEqual("2024-01-05", edge["effective_date"])
        self.assertEqual("issuer-exchange-holding-substitution", edge["sourced_fact_id"])
        self.assertNotIn("corporate_action_id", edge)
        for segment in segments:
            interval = segment["ticker_ownership_interval"]
            self.assertEqual([], interval["supporting_corporate_action_ids"])
            self.assertEqual([edge["sourced_fact_id"]], interval["supporting_sourced_fact_ids"])

    def test_certificate_and_endpoint_selection_run_once_across_phases_and_inheritance(self):
        evidence = succession_evidence()
        rules = load_authorized_policy().ownership_rules
        cache = CorrelationCache(ownership_interval_rules=rules)
        with (
            patch.object(
                row_correlation,
                "certify_sourced_successions",
                wraps=sourced_succession.certify_sourced_successions,
            ) as certify,
            patch.object(
                sourced_succession, "exact_cusip_lookup_asset", wraps=sourced_succession.exact_cusip_lookup_asset
            ) as lookup,
        ):
            established = cache.lineage_analysis(evidence)
            cache.lineage_analysis(evidence, include_action_anchored_current_suffix=True)
            other = CorrelationCache(ownership_interval_rules=rules)
            other.inherit_continuity(cache)
            self.assertEqual(established, other.lineage_analysis(evidence))
            self.assertEqual(1, certify.call_count)
            self.assertEqual(["111111111", "222222222"], [c.args[1] for c in lookup.call_args_list])

    def test_later_preparation_failure_keeps_completed_certificate_for_retry(self):
        evidence = succession_evidence()
        cache = CorrelationCache(ownership_interval_rules=load_authorized_policy().ownership_rules)
        prepare = row_correlation.prepare_ticker_ownership
        with (
            patch.object(
                row_correlation,
                "certify_sourced_successions",
                wraps=sourced_succession.certify_sourced_successions,
            ) as certify,
            patch.object(
                row_correlation, "build_uuid_lineage_edges", wraps=row_correlation.build_uuid_lineage_edges
            ) as candidates,
        ):
            with patch.object(
                row_correlation, "prepare_ticker_ownership", side_effect=RuntimeError("peer failed")
            ):
                with self.assertRaisesRegex(RuntimeError, "peer failed"):
                    cache.lineage_analysis(evidence)
            with patch.object(row_correlation, "prepare_ticker_ownership", wraps=prepare):
                self.assertEqual(1, len(cache.lineage_analysis(evidence)[2]))
            self.assertEqual(1, certify.call_count)
            self.assertEqual(1, candidates.call_count)

    def test_nearest_counterexamples_reject_the_certificate(self):
        cases = {}
        for name in (
            "ambiguous mapping",
            "missing mapping",
            "third occupant",
            "unknown occupant",
            "structural conflict",
            "old dividend after boundary",
            "new dividend before boundary",
            "duplicate succession",
            "partial actions",
            "wrong current UUID",
            "active predecessor",
            "different exchange",
            "nontrading boundary",
            "explicit third dividend CUSIP",
        ):
            cases[name] = succession_evidence()
        cases["ambiguous mapping"]["asset_lookups"].append(
            copy.deepcopy(cases["ambiguous mapping"]["asset_lookups"][1])
        )
        cases["missing mapping"]["asset_lookups"].pop(1)
        cases["third occupant"]["symbol_occupancy_evidence"].append(
            {"symbol": "HOLD", "cusip": "333333333", "asset_id": "third-id", "evidence": []}
        )
        cases["unknown occupant"]["symbol_occupancy_evidence"].append(
            {"symbol": "HOLD", "cusip": "333333333", "asset_id": None, "evidence": []}
        )
        cases["structural conflict"]["corporate_actions"].append(
            {
                "id": "unrelated-transition",
                "action_type": "reorganization",
                "symbol": "HOLD",
                "effective_date": "2024-01-05",
            }
        )
        cases["old dividend after boundary"]["corporate_actions"][0]["ex_date"] = "2024-01-08"
        cases["new dividend before boundary"]["corporate_actions"][1]["ex_date"] = "2024-01-04"
        cases["duplicate succession"]["sourced_successions"]["facts"] *= 2
        cases["partial actions"]["query"]["corporate_actions_data_quality"] = "partial"
        cases["wrong current UUID"]["asset_lookups"][0]["asset"] = cases["wrong current UUID"]["identities"][0]
        cases["active predecessor"]["identities"][0]["status"] = "active"
        cases["different exchange"]["identities"][0]["exchange"] = "NASDAQ"
        cases["nontrading boundary"]["sourced_successions"]["facts"][0]["facts"]["exchange_effective_date"] = (
            "2024-01-06"
        )
        cases["explicit third dividend CUSIP"]["corporate_actions"][0]["cusip"] = "333333333"
        for name, evidence in cases.items():
            with self.subTest(name=name):
                certificates, issues = sourced_succession.certify_sourced_successions(evidence)
                self.assertEqual((), certificates)
                self.assertTrue(issues)

    def test_missing_dividend_cusip_adds_neither_support_nor_conflict(self):
        evidence = succession_evidence()
        evidence["corporate_actions"].insert(
            0,
            {
                "id": "unlabelled",
                "action_type": "cash_dividend",
                "symbol": "HOLD",
                "cusip": "",
                "ex_date": "2024-01-02",
                "rate": 1,
            },
        )
        self.assertEqual(0, self.evaluate(evidence)["uncorrelated"]["row_count"])

    def test_old_policy_does_not_activate_sourced_succession(self):
        with patch.object(
            row_correlation,
            "certify_sourced_successions",
            side_effect=AssertionError("old policy invoked new certificate"),
        ):
            report = self.evaluate(succession_evidence(), version=78)
        self.assertEqual([], report["uuid_lineage_edges"])
        self.assertGreater(report["uncorrelated"]["row_count"], 0)

    def test_changed_mapping_uses_a_new_certificate_scope(self):
        evidence = succession_evidence()
        original = CorrelationCache().lineage_analysis(evidence)
        changed = copy.deepcopy(evidence)
        changed["asset_lookups"][1]["asset"]["id"] = "replacement-id"
        self.assertTrue(original[2])
        self.assertEqual([], CorrelationCache().lineage_analysis(changed)[2])
        self.assertEqual(date(2024, 1, 5), original[2][0].effective_date)

    def test_v79_matches_frozen_v78_reports_without_sourced_facts(self):
        # The retained v77 YAML has the same execution stages as v78. Select
        # the frozen v78 implementation gates independently of today's YAML.
        frozen = replace(
            load_policy(
                Path(__file__).resolve().parents[1]
                / "resources/alpaca_security_correlation_v77_legacy_policy.yaml"
            ),
            implementation_correlation_version=78,
        )
        dividends = succession_evidence()
        dividends.pop("sourced_successions")
        split = copy.deepcopy(dividends)
        split["corporate_actions"] = [
            {
                "id": "ordinary-split-without-a-succession",
                "action_type": "forward_split",
                "symbol": "HOLD",
                "cusip": "222222222",
                "ex_date": "2024-01-05",
                "old_rate": 1,
                "new_rate": 2,
            }
        ]
        current = copy.deepcopy(dividends)
        current["identities"] = [item for item in current["identities"] if item["id"] == "new-id"]
        current["asset_lookups"] = [item for item in current["asset_lookups"] if item["asset"]["id"] == "new-id"]
        current["symbol_occupancy_evidence"] = [
            item for item in current["symbol_occupancy_evidence"] if item["asset_id"] == "new-id"
        ]
        current["corporate_actions"] = [current["corporate_actions"][1]]
        cases = {
            "positive provider rename": exact_same_symbol_handoff(),
            "two UUID dividend ambiguity": dividends,
            "two UUID split ambiguity": split,
            "one current UUID": current,
        }
        for name, evidence in cases.items():
            with self.subTest(name=name):
                inputs = inputs_for({"HOLD": bars(self.days)})
                older = correlate(Path("."), inputs, {"HOLD": evidence}, policy=frozen)["HOLD"]
                newer = correlate(Path("."), inputs, {"HOLD": evidence}, policy=load_authorized_policy())["HOLD"]
                older.pop("correlation_policy")
                newer.pop("correlation_policy")
                self.assertIsNone(older.pop("sourced_rename_membership"))
                self.assertEqual(
                    {"version": 1, "certificates": [], "issues": []}, newer.pop("sourced_rename_membership")
                )
                self.assertEqual(older, newer)
                if "ambiguity" in name:
                    self.assertGreater(newer["uncorrelated"]["row_count"], 0)
                    self.assertEqual([], newer["uuid_lineage_edges"])
                else:
                    self.assertEqual(0, newer["uncorrelated"]["row_count"])


if __name__ == "__main__":
    unittest.main()

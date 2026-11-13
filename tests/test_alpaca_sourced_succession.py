"""Authored succession rejects; provider support and completed generic work remain usable."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import row_correlation, sourced_succession
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.transform import correlate
from tests.test_alpaca_name_change_evidence import bars, exact_same_symbol_handoff, inputs_for, raw_segments


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

    def evaluate(self, evidence):
        return correlate(
            Path("/unused"),
            inputs_for({"HOLD": bars(self.days)}),
            {"HOLD": evidence},
            policy=load_authorized_policy(),
        )["HOLD"]

    def test_authored_succession_rejects_before_endpoint_or_price_work(self):
        with patch.object(Path, "open", side_effect=AssertionError("price access")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                sourced_succession.certify_sourced_successions(succession_evidence())

    def test_legacy_certificate_switch_cannot_reenable_inference(self):
        with self.assertRaisesRegex(Exception, "inadmissible"):
            CorrelationCache(sourced_succession_certificates=True)

    def test_no_facts_request_returns_empty_without_certification(self):
        evidence = succession_evidence()
        evidence.pop("sourced_successions")
        with patch.object(Path, "open", side_effect=AssertionError("price access")):
            self.assertEqual(((), ()), sourced_succession.certify_sourced_successions(evidence))

    def test_reasonably_supported_provider_rename_needs_no_research_certificate(self):
        with patch.object(
            row_correlation,
            "certify_sourced_successions",
            side_effect=AssertionError("retired certifier called"),
        ):
            report = self.evaluate(exact_same_symbol_handoff())
        self.assertEqual(0, report["uncorrelated"]["row_count"])
        self.assertTrue(report["uuid_lineage_edges"])
        self.assertEqual(1, len({s["security_identity"]["security_id"] for s in raw_segments(report)}))

    def test_dividends_or_ordinary_split_do_not_invent_an_owner_connection(self):
        original = succession_evidence()
        original.pop("sourced_successions")
        split = copy.deepcopy(original)
        split["corporate_actions"] = [
            {
                "id": "ordinary-split",
                "action_type": "forward_split",
                "symbol": "HOLD",
                "cusip": "222222222",
                "ex_date": "2024-01-05",
                "old_rate": 1,
                "new_rate": 2,
            }
        ]
        for evidence in (original, split):
            with self.subTest(actions=evidence["corporate_actions"]):
                report = self.evaluate(evidence)
                self.assertGreater(report["uncorrelated"]["row_count"], 0)
                self.assertEqual([], report["uuid_lineage_edges"])

    def test_one_current_uuid_is_supported_without_a_succession_assertion(self):
        evidence = succession_evidence()
        evidence.pop("sourced_successions")
        evidence["identities"] = [x for x in evidence["identities"] if x["id"] == "new-id"]
        evidence["asset_lookups"] = [x for x in evidence["asset_lookups"] if x["asset"]["id"] == "new-id"]
        evidence["symbol_occupancy_evidence"] = [
            x for x in evidence["symbol_occupancy_evidence"] if x["asset_id"] == "new-id"
        ]
        evidence["corporate_actions"] = [evidence["corporate_actions"][1]]
        self.assertEqual(0, self.evaluate(evidence)["uncorrelated"]["row_count"])

    def test_completed_generic_candidates_survive_phases_and_inheritance_once(self):
        evidence = exact_same_symbol_handoff()
        cache = CorrelationCache(ownership_interval_rules=load_authorized_policy().ownership_rules)
        with (
            patch.object(
                row_correlation, "build_uuid_lineage_edges", wraps=row_correlation.build_uuid_lineage_edges
            ) as candidates,
            patch.object(
                row_correlation, "certify_sourced_successions", side_effect=AssertionError("research accessed")
            ),
        ):
            established = cache.lineage_analysis(evidence)
            cache.lineage_analysis(evidence, include_action_anchored_current_suffix=True)
            following = CorrelationCache(ownership_interval_rules=load_authorized_policy().ownership_rules)
            following.inherit_continuity(cache)
            self.assertEqual(established, following.lineage_analysis(evidence))
            self.assertEqual(1, candidates.call_count)
            changed = copy.deepcopy(evidence)
            changed["asset_lookups"][1]["asset"]["id"] = "replacement-id"
            following.lineage_analysis(changed)
            self.assertEqual(2, candidates.call_count)

    def test_peer_failure_preserves_completed_generic_candidates_for_retry(self):
        evidence = exact_same_symbol_handoff()
        cache = CorrelationCache(ownership_interval_rules=load_authorized_policy().ownership_rules)
        with patch.object(
            row_correlation, "build_uuid_lineage_edges", wraps=row_correlation.build_uuid_lineage_edges
        ) as candidates:
            with patch.object(
                row_correlation, "prepare_ticker_ownership", side_effect=RuntimeError("peer failed")
            ):
                with self.assertRaisesRegex(RuntimeError, "peer failed"):
                    cache.lineage_analysis(evidence)
            self.assertTrue(cache.lineage_analysis(evidence)[2])
            self.assertEqual(1, candidates.call_count)


if __name__ == "__main__":
    unittest.main()

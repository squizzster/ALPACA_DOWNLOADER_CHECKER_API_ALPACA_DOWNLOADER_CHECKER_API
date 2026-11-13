"""Closed conversion metadata stays readable; fresh authored inference rejects."""

import copy
import unittest
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import security_relations as relations
from libs.alpaca_security_lineage.event_supplements import accept_supplements
from tests.test_alpaca_public_common_continuation import common_fact


def conversion_fact():
    return {
        "kind": "distinct_security_conversion",
        "match": {"source_id": "target-consideration"},
        "sources": common_fact()["sources"],
        "facts": {
            "schema_version": 1,
            "predecessor": {"symbol": "FUTURE", "cusip": "333333333", "instrument_kind": "ADS"},
            "successor": {"symbol": "OLD", "cusip": "111111111", "instrument_kind": "common_stock"},
            "legal_effective_date": "2024-01-02",
            "first_successor_trading": {"symbol": "OLD", "date": "2024-01-03"},
            "trading_boundary": {
                "status": "ambiguous_final_session",
                "preferred_last_session": "2023-12-28",
                "alternative_last_sessions": ["2023-12-29"],
            },
            "provider_event_match": None,
            "consideration_status": "complete",
            "consideration": [
                {
                    "kind": "stock",
                    "old_units": "1",
                    "old_unit_kind": "ADS",
                    "new_units": "6.128",
                    "new_unit_kind": "common_share",
                    "recipient_cusip": "111111111",
                }
            ],
            "acquirer_security_continues": True,
            "ownership_effective_dates": ["2024-01-02"],
            "settlement_dates": ["2025-01-01"],
        },
    }


def evidence(item=None):
    # Inert archived fixture, never accepted as a runtime assertion.
    item = copy.deepcopy(item or conversion_fact())
    return {
        "sourced_security_relations": {"schema_version": 1, "facts": [item]},
        "asset_lookups": [{"lookup_kind": "cusip", "lookup": "111111111", "asset": {"id": "acquirer-id"}}],
        "corporate_actions": [],
    }


def completed_relation(item=None):
    """Literal completed metadata fixture; this performs no event certification."""
    item = copy.deepcopy(item or conversion_fact())
    fact = item["facts"]
    endpoints = {}
    for role, identifier in (("predecessor", None), ("successor", "acquirer-id")):
        value = fact[role]
        endpoints[role] = {
            "instrument_kind": value["instrument_kind"],
            "cusip": value["cusip"],
            "historical_symbol": value["symbol"],
            "asset_id": identifier,
            "resolution": "resolved" if identifier else "external_identifiers_only",
            "candidate_asset_ids": [],
        }
    row = {
        "relation_id": "sourced:" + item["match"]["source_id"],
        "relation_kind": "distinct_security_conversion",
        "identity_union": False,
        "source_fact_id": item["match"]["source_id"],
        "corporate_action_id": (fact["provider_event_match"] or {}).get("id"),
        "effective_date": fact["legal_effective_date"],
        "first_successor_trading": fact["first_successor_trading"],
        "trading_boundary": fact["trading_boundary"],
        "predecessor": endpoints["predecessor"],
        "successor": endpoints["successor"],
        "consideration": fact["consideration"],
        "consideration_status": fact["consideration_status"],
        "continuing_acquirer": fact["acquirer_security_continues"],
        "ownership_effective_dates": fact["ownership_effective_dates"],
        "settlement_dates": fact["settlement_dates"],
        "endpoint_resolution": "partial",
        "source_provenance": item["sources"],
        **({"consideration_schema_version": 2} if fact["schema_version"] == 2 else {}),
    }
    return {"version": 1, "relations": [row], "issues": []}


class SecurityRelationTests(unittest.TestCase):
    def test_closed_conversion_keeps_external_target_and_separate_dates(self):
        result = relations.project(completed_relation(), {})
        row = result["relations"][0]
        self.assertFalse(row["identity_union"])
        self.assertIsNone(row["predecessor"]["asset_id"])
        self.assertIsNone(row["successor"]["security_id"])
        self.assertEqual("6.128", row["consideration"][0]["new_units"])
        self.assertEqual("2024-01-02", row["effective_date"])
        self.assertEqual("2024-01-03", row["first_successor_trading"]["date"])

    def test_projection_only_rebinds_completed_public_references_without_certification(self):
        certificate = completed_relation()
        with patch.object(relations, "certify_group", side_effect=AssertionError("recertification")):
            before = relations.project(certificate, {"acquirer-id": {"security_id": "old-public"}})
            after = relations.project(certificate, {"acquirer-id": {"security_id": "union-public"}})
        self.assertEqual("old-public", before["relations"][0]["successor"]["security_id"])
        self.assertEqual("union-public", after["relations"][0]["successor"]["security_id"])
        self.assertIsNone(after["relations"][0]["predecessor"]["security_id"])
        self.assertNotIn("security_id", certificate["relations"][0]["successor"])

    def test_closed_ambiguous_endpoint_does_not_select_one_public_owner(self):
        certificate = completed_relation()
        certificate["relations"][0]["successor"].update(
            asset_id=None, resolution="ambiguous", candidate_asset_ids=["acquirer-id", "rival"]
        )
        result = relations.project(
            certificate, {"acquirer-id": {"security_id": "one"}, "rival": {"security_id": "two"}}
        )
        endpoint = result["relations"][0]["successor"]
        self.assertEqual("ambiguous", endpoint["resolution"])
        self.assertIsNone(endpoint["security_id"])
        self.assertEqual(["acquirer-id", "rival"], endpoint["candidate_asset_ids"])

    def test_archived_relation_cannot_become_a_future_acquirer_exemption(self):
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            relations.acquirer_nonconflicts(completed_relation(), object())
        self.assertFalse(
            relations.acquirer_nonconflicts({"version": 1, "relations": [], "issues": []}, object())
        )

    def test_authored_consideration_is_rejected_even_with_well_formed_terms(self):
        for value in ("0", "NaN", "6.128"):
            item = conversion_fact()
            item["facts"]["consideration"][0]["new_units"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([item])


class ExactProviderBindingTests(unittest.TestCase):
    def test_matching_or_conflicting_provider_records_cannot_admit_authored_assertions(self):
        for changed in (False, True):
            facts = evidence()
            facts["corporate_actions"] = [
                {
                    "id": "real",
                    "action_type": "stock_merger",
                    "acquiree_cusip": "333333333",
                    "acquirer_cusip": "different" if changed else "111111111",
                }
            ]
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, "fact injection"):
                relations.certify_group([facts])

    def test_founder_or_derivative_terms_cannot_reenable_the_retired_ingress(self):
        for key, value in (("old_unit_kind", "founder_share"), ("new_unit_kind", "warrant")):
            item = conversion_fact()
            item["facts"]["consideration"][0][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([item])


class ReportedProviderRateTests(unittest.TestCase):
    def test_missing_or_bad_rates_do_not_reenable_authored_relation_certification(self):
        for value in (None, "0", "NaN", "-1"):
            facts = evidence()
            facts["corporate_actions"] = [{"id": "real", "action_type": "stock_merger", "acquirer_rate": value}]
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "fact injection"):
                relations.certify_group([facts])

    def test_authored_public_common_treatment_rejects_before_provider_ratio_interpretation(self):
        from libs.alpaca_end_of_day_security_correlation import public_common_continuation
        from tests.test_alpaca_public_common_continuation import evidence as common_evidence

        facts = common_evidence(common_fact())
        facts["corporate_actions"] = [
            {"id": "real", "action_type": "stock_merger", "acquiree_rate": "1", "acquirer_rate": "2"}
        ]
        with self.assertRaisesRegex(ValueError, "fact injection"):
            public_common_continuation.certify_group([facts])


class ComponentEntitlementTests(unittest.TestCase):
    def item(self, *, unit=False):
        item = conversion_fact()
        facts = item["facts"]
        facts["schema_version"] = 2
        facts["predecessor"]["instrument_kind"] = "unit" if unit else "right"
        term = facts["consideration"][0]
        term.update(
            old_unit_kind="unit" if unit else "right",
            new_units="0.2",
            predecessor_component={
                "cusip": "444444444",
                "instrument_kind": "right",
                "units_per_predecessor": "1",
            }
            if unit
            else None,
            rounding={
                "method": "floor",
                "aggregation_level": "holder_aggregate_entitlement",
                "cash_in_lieu": False,
            },
        )
        facts["acquirer_security_continues"] = False
        return item

    def test_closed_right_and_unit_terms_keep_rounding_without_identity_union(self):
        from libs.alpaca_lineage_build.continuation import _valid_relation

        for unit in (False, True):
            with (
                self.subTest(unit=unit),
                patch.object(relations, "certify_group", side_effect=AssertionError("closed terms recertified")),
            ):
                projected = relations.project(completed_relation(self.item(unit=unit)), {})["relations"][0]
                self.assertFalse(projected["identity_union"])
                self.assertEqual("0.2", projected["consideration"][0]["new_units"])
                self.assertEqual("floor", projected["consideration"][0]["rounding"]["method"])
                self.assertTrue(_valid_relation(projected))
                projected.pop("consideration_schema_version")
                self.assertFalse(_valid_relation(projected))

    def test_incomplete_or_unsupported_closed_terms_require_owning_recovery(self):
        from libs.alpaca_lineage_build.continuation import _valid_relation

        for change in ("missing_component", "wrong_aggregation", "cash"):
            projected = relations.project(completed_relation(self.item(unit=True)), {})["relations"][0]
            term = projected["consideration"][0]
            if change == "missing_component":
                term.pop("predecessor_component")
            elif change == "wrong_aggregation":
                term["rounding"]["aggregation_level"] = "per_price_bar"
            else:
                term["rounding"]["cash_in_lieu"] = True
            with self.subTest(change=change):
                self.assertFalse(_valid_relation(projected))

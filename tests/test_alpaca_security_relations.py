"""Separate external conversion evidence, public projection and exact acquirer scope."""

import copy
import unittest
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import security_relations as relations
from libs.alpaca_end_of_day_security_correlation.sourced_rename import RenameCertificate
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
    item = accept_supplements([item or conversion_fact()])[0]
    return {
        "sourced_security_relations": {"schema_version": 1, "facts": [item]},
        "asset_lookups": [{"lookup_kind": "cusip", "lookup": "111111111", "asset": {"id": "acquirer-id"}}],
        "corporate_actions": [],
    }


class SecurityRelationTests(unittest.TestCase):
    def test_external_target_and_separate_legal_and_trading_labels(self):
        result = relations.certify_group([evidence()])
        row = result["relations"][0]
        self.assertFalse(row["identity_union"])
        self.assertIsNone(row["predecessor"]["asset_id"])
        self.assertNotIn("security_id", row["successor"])
        self.assertEqual("6.128", row["consideration"][0]["new_units"])
        self.assertEqual("2024-01-02", row["effective_date"])
        self.assertEqual("2024-01-03", row["first_successor_trading"]["date"])

    def test_projection_only_rebinds_existing_membership(self):
        certificate = relations.certify_group([evidence()])
        with patch.object(relations, "certify_group", side_effect=AssertionError("recertification")):
            before = relations.project(certificate, {"acquirer-id": {"security_id": "old-public"}})
            after = relations.project(certificate, {"acquirer-id": {"security_id": "union-public"}})
        self.assertEqual("old-public", before["relations"][0]["successor"]["security_id"])
        self.assertEqual("union-public", after["relations"][0]["successor"]["security_id"])
        self.assertIsNone(after["relations"][0]["predecessor"]["security_id"])

    def test_ambiguous_endpoint_remains_external(self):
        facts = evidence()
        facts["asset_lookups"].append({"lookup_kind": "cusip", "lookup": "111111111", "asset": {"id": "rival"}})
        row = relations.certify_group([facts])["relations"][0]
        self.assertEqual("ambiguous", row["successor"]["resolution"])
        self.assertIsNone(row["successor"]["asset_id"])

    def test_exact_acquirer_scope_unknown_target_terms_and_deferred_payment(self):
        item = conversion_fact()
        action = {
            "id": "acquisition",
            "action_type": "stock_merger",
            "acquirer_cusip": "111111111",
            "acquiree_cusip": "333333333",
            "effective_date": "2024-01-02",
        }
        item["facts"].update(provider_event_match=action, consideration_status="unreported", consideration=[])
        facts = evidence(item)
        facts["corporate_actions"] = [action]
        certificate = relations.certify_group([facts])
        rename = RenameCertificate(
            "later-rename",
            "OLD",
            "NEW",
            "111111111",
            "222222222",
            "acquirer-id",
            "new-id",
            "2024-01-05",
            "2024-01-04",
            (("OLD", "2024-01-01", "2024-01-05"),),
        )
        self.assertEqual(1, len(relations.acquirer_nonconflicts(certificate, rename)))
        for key, value in (
            ("continuing_acquirer", False),
            ("ownership_effective_dates", []),
            ("ownership_effective_dates", ["2024-01-05"]),
        ):
            altered = copy.deepcopy(certificate)
            altered["relations"][0][key] = value
            self.assertFalse(relations.acquirer_nonconflicts(altered, rename))
        certificate["relations"][0]["corporate_action_id"] = None
        self.assertFalse(relations.acquirer_nonconflicts(certificate, rename))

    def test_nonpositive_and_conflicting_terms_rejected_at_ingress(self):
        for key, value in (("new_units", "0"), ("new_units", "NaN"), ("recipient_cusip", "999999999")):
            item = conversion_fact()
            item["facts"]["consideration"][0][key] = value
            with self.assertRaises(ValueError):
                accept_supplements([item])


class ExactProviderBindingTests(unittest.TestCase):
    def test_raw_roles_dates_and_terms_cannot_hide_behind_minimal_match(self):
        item = conversion_fact()
        item["facts"]["provider_event_match"] = {"id": "real", "action_type": "stock_merger"}
        action = {
            "id": "real",
            "action_type": "stock_merger",
            "acquiree_cusip": "333333333",
            "acquirer_cusip": "111111111",
            "effective_date": "2024-01-02",
            "acquiree_rate": "1",
            "acquirer_rate": "6.128",
        }
        for field, value in (
            ("acquiree_cusip", "111111111"),
            ("acquirer_cusip", "333333333"),
            ("effective_date", "2024-01-05"),
            ("acquirer_rate", "7"),
        ):
            facts = evidence(item)
            facts["corporate_actions"] = [{**action, field: value}]
            self.assertFalse(relations.certify_group([facts])["relations"], field)
        item["facts"]["provider_event_match"] = {**action, "acquirer_rate": "7"}
        facts = evidence(item)
        facts["corporate_actions"] = [item["facts"]["provider_event_match"]]
        self.assertFalse(relations.certify_group([facts])["relations"])

    def test_founder_or_derivative_consideration_units_are_not_public_common(self):
        for key, value in (("old_unit_kind", "founder_share"), ("new_unit_kind", "warrant")):
            item = conversion_fact()
            item["facts"]["consideration"][0][key] = value
            with self.assertRaises(ValueError):
                accept_supplements([item])


class ReportedProviderRateTests(unittest.TestCase):
    def test_known_bad_single_rate_is_not_unreported_consideration(self):
        for value in ("0", "NaN", "-1"):
            item = conversion_fact()
            item["facts"].update(
                provider_event_match={"id": "real", "action_type": "stock_merger"},
                consideration_status="unreported",
                consideration=[],
            )
            facts = evidence(item)
            facts["corporate_actions"] = [
                {
                    "id": "real",
                    "action_type": "stock_merger",
                    "acquiree_cusip": "333333333",
                    "acquirer_cusip": "111111111",
                    "effective_date": "2024-01-02",
                    "acquirer_rate": value,
                }
            ]
            self.assertFalse(relations.certify_group([facts])["relations"])

    def test_common_raw_ratio_cannot_contradict_public_unit_treatment(self):
        from libs.alpaca_end_of_day_security_correlation import public_common_continuation
        from tests.test_alpaca_public_common_continuation import evidence as common_evidence

        item = common_fact()
        item["facts"]["provider_event_match"] = {"id": "real", "action_type": "stock_merger"}
        facts = common_evidence(item)
        facts["corporate_actions"] = [
            {
                "id": "real",
                "action_type": "stock_merger",
                "acquiree_cusip": "111111111",
                "acquirer_cusip": "222222222",
                "effective_date": "2024-01-04",
                "acquiree_rate": "1",
                "acquirer_rate": "2",
            }
        ]
        self.assertFalse(public_common_continuation.certify_group([facts])["certificates"])


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

    def test_right_and_unit_entitlements_keep_component_rounding_without_identity_union(self):
        for unit in (False, True):
            with self.subTest(unit=unit):
                facts = evidence(self.item(unit=unit))
                accepted = relations.certify_group([facts])
                self.assertEqual(1, len(accepted["relations"]))
                relation = accepted["relations"][0]
                self.assertFalse(relation["identity_union"])
                self.assertEqual("0.2", relation["consideration"][0]["new_units"])
                self.assertEqual("floor", relation["consideration"][0]["rounding"]["method"])
                from libs.alpaca_lineage_build.continuation import _valid_relation

                projected = relations.project(accepted, {})["relations"][0]
                self.assertTrue(_valid_relation(projected))
                projected.pop("consideration_schema_version")
                self.assertFalse(_valid_relation(projected))
                self.assertFalse(relations.certify_group([facts], extended_terms=False)["relations"])

    def test_missing_component_or_account_rounding_cannot_be_promoted_to_exact_terms(self):
        for change in ("missing_component", "wrong_aggregation", "cash"):
            item = self.item(unit=True)
            term = item["facts"]["consideration"][0]
            if change == "missing_component":
                term["predecessor_component"] = None
            elif change == "wrong_aggregation":
                term["rounding"]["aggregation_level"] = "per_price_bar"
            else:
                term["rounding"]["cash_in_lieu"] = True
            with self.subTest(change=change), self.assertRaises(ValueError):
                accept_supplements([item])

"""Generic public holder facts: no ticker, price, legal-clause or exchange shortcut."""

import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import public_common_continuation as common
from libs.alpaca_lineage_build import source_work
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_security_lineage.event_supplements import accept_supplements
from libs.alpaca_security_lineage.sourced_facts import PUBLIC_COMMON_KIND


def common_fact():
    return {
        "kind": PUBLIC_COMMON_KIND,
        "match": {"source_id": "public-holder-transition"},
        "facts": {
            "schema_version": 1,
            "transaction_kind": "business_combination",
            "predecessor": {
                "symbol": "SPAC",
                "cusip": "111111111",
                "instrument_kind": "public_common_stock",
                "registrant_cik": "123",
            },
            "successor": {
                "symbol": "BUSINESS",
                "cusip": "222222222",
                "instrument_kind": "public_common_stock",
                "registrant_cik": "123",
            },
            "legal_effective_date": "2024-01-04",
            "exchange_effective_date": "2024-01-05",
            "continuing_registrant": True,
            "holder_scope": "non_redeemed_public_common",
            "public_common_participates_in_successor": True,
            "exchange_documents_common_transition": True,
            "provider_event_match": None,
            "public_share_treatment": {
                "kind": "continued_reclassified",
                "predecessor_units": "1",
                "successor_units": "1",
                "unit_basis": "accepted_public_holder_reconciliation",
                "express_exchange_clause": None,
                "rights_unchanged": None,
            },
        },
        "sources": [
            {
                "url": "https://issuer.example/closing",
                "title": "Public holder closing treatment",
                "authority": "issuer",
                "document_date": "2024-01-04",
            }
        ],
    }


def evidence(fact=None):
    fact = accept_supplements([fact or common_fact()])[0]
    return {
        "sourced_public_common_continuations": {"schema_version": 1, "facts": [fact]},
        "asset_lookups": [
            {
                "lookup_kind": "cusip",
                "lookup": cusip,
                "asset": {"id": identifier, "symbol": symbol, "class": "us_equity", "exchange": exchange},
                "outcome": "found",
            }
            for cusip, identifier, symbol, exchange in (
                ("111111111", "predecessor-id", "SPAC", "NASDAQ"),
                ("222222222", "successor-id", "LATER", "NYSE"),
            )
        ],
        "corporate_actions": [],
    }


class PublicCommonTests(unittest.TestCase):
    def test_changed_or_removed_membership_support_cannot_carry_old_union(self):
        original = evidence()
        completed = common.certify_group([original])
        control = SimpleNamespace(
            store="synthetic",
            source_sha256="accepted-generation",
            previous={"public_common_continuation_membership": completed},
        )
        for change in ("mapped_uuid", "removed", "changed_fact"):
            changed = copy.deepcopy(original)
            if change == "mapped_uuid":
                changed["asset_lookups"][1]["asset"]["id"] = "third-id"
            elif change == "removed":
                changed["sourced_public_common_continuations"]["facts"] = []
            else:
                fact = common_fact()
                fact["facts"]["successor"]["cusip"] = "333333333"
                changed = evidence(fact)
            with (
                self.subTest(change=change),
                patch.object(
                    source_work, "compose_memberships", side_effect=AssertionError("obsolete union reused")
                ),
            ):
                with self.assertRaisesRegex(LineageError, "membership support changed"):
                    source_work.prepare_group_evidence_and_source_work(
                        {"SOURCE": control}, {"SOURCE": changed}, {}, cache={}
                    )

    def test_one_document_nullable_rights_later_spelling_and_exchange(self):
        result = common.certify_group([evidence()])
        self.assertEqual([], result["issues"])
        certificate = common.CommonCertificate(**result["certificates"][0])
        self.assertEqual("predecessor-id", certificate.identity_edge().predecessor_asset_id)
        self.assertEqual("successor-id", certificate.identity_edge().successor_asset_id)
        self.assertIsNone(certificate.public_share_treatment["rights_unchanged"])

    def test_wrong_holder_or_instrument_and_unknown_units(self):
        for key, value in (
            ("holder_scope", "founders"),
            ("public_common_participates_in_successor", False),
            ("continuing_registrant", False),
        ):
            with self.subTest(key=key):
                item = common_fact()
                item["facts"][key] = value
                with self.assertRaises(ValueError):
                    accept_supplements([item])
        for instrument in ("ADS", "unit", "warrant", "private_common", "founder_shares"):
            item = common_fact()
            item["facts"]["predecessor"]["instrument_kind"] = instrument
            with self.assertRaises(ValueError):
                accept_supplements([item])
        for units in (None, "0.8", "2"):
            item = common_fact()
            item["facts"]["public_share_treatment"]["successor_units"] = units
            with self.assertRaises(ValueError):
                accept_supplements([item])

    def test_accepted_container_is_immutable_without_revalidation(self):
        accepted = accept_supplements([common_fact()])
        with patch(
            "libs.alpaca_security_lineage.event_supplements.validate_supplements",
            side_effect=AssertionError("revalidation"),
        ):
            self.assertIs(accepted, accept_supplements(accepted))
        with self.assertRaises(TypeError):
            accepted[0]["facts"]["holder_scope"] = "founders"

    def test_conflicting_unavailable_peer_and_cash_terminal(self):
        facts = evidence()
        peer = copy.deepcopy(facts)
        peer["asset_lookups"][0]["asset"]["id"] = "other-id"
        self.assertFalse(common.certify_group([facts, peer])["certificates"])
        facts["corporate_actions"] = [
            {"action_type": "cash_merger", "acquiree_cusip": "111111111", "effective_date": "2024-01-04"}
        ]
        self.assertFalse(common.certify_group([facts])["certificates"])

    def test_branch_and_incomplete_group_cannot_join(self):
        first = evidence()
        second = common_fact()
        second["match"]["source_id"] = "other-branch"
        second["facts"]["successor"]["cusip"] = "333333333"
        second = evidence(second)
        second["asset_lookups"].append(
            {"lookup_kind": "cusip", "lookup": "333333333", "asset": {"id": "branch-id"}}
        )
        self.assertFalse(common.certify_group([first, second])["certificates"])
        self.assertFalse(common.certify_group([first], complete=False)["certificates"])


class PublicCommonProviderBindingTests(unittest.TestCase):
    def test_explicit_event_must_exist_and_match_roles(self):
        item = common_fact()
        item["facts"]["provider_event_match"] = {"id": "transaction", "action_type": "stock_merger"}
        facts = evidence(item)
        self.assertFalse(common.certify_group([facts])["certificates"])
        facts["corporate_actions"] = [
            {
                "id": "transaction",
                "action_type": "stock_merger",
                "acquiree_cusip": "111111111",
                "acquirer_cusip": "222222222",
                "effective_date": "2024-01-04",
            }
        ]
        self.assertTrue(common.certify_group([facts])["certificates"])
        facts["corporate_actions"][0]["acquiree_cusip"] = "999999999"
        self.assertFalse(common.certify_group([facts])["certificates"])

    def test_later_exit_does_not_erase_earlier_continuation(self):
        facts = evidence()
        facts["corporate_actions"] = [
            {
                "id": "later",
                "action_type": "cash_merger",
                "acquiree_cusip": "111111111",
                "effective_date": "2025-01-02",
            }
        ]
        self.assertTrue(common.certify_group([facts])["certificates"])


class AcceptedComponentCompositionTests(unittest.TestCase):
    def test_complete_components_and_labels_survive_identifier_and_order_permutations(self):
        from datetime import date

        from libs.alpaca_end_of_day_security_correlation.security_identity import (
            security_identity_for_alpaca_assets,
        )
        from libs.alpaca_end_of_day_security_correlation.uuid_lineage import UuidLineageEdge
        from libs.alpaca_lineage_build import source_work_contract as source_work

        for names, reverse in ((("a", "a2", "b", "b2"), False), (("z", "x", "c", "d"), True)):
            a, a2, b, b2 = names
            left = security_identity_for_alpaca_assets(
                [a, a2], [], tickers=["FIRST", "EARLIER"], basis="accepted"
            ).as_report_object()
            right = security_identity_for_alpaca_assets(
                [b, b2], [], tickers=["SECOND", "LATEST"], basis="accepted"
            ).as_report_object()
            pairs = [(a, left), (a2, left), (b, right), (b2, right)]
            edge = UuidLineageEdge(
                "public_common_continuation",
                None,
                date(2024, 1, 5),
                a,
                "FIRST",
                "111111111",
                b,
                "SECOND",
                "222222222",
                "fact",
                date(2024, 1, 4),
            )
            with patch.object(
                source_work,
                "security_identity_for_alpaca_assets",
                wraps=source_work.security_identity_for_alpaca_assets,
            ) as calculate:
                result = source_work.compose_memberships(dict(reversed(pairs) if reverse else pairs), [edge])
            self.assertEqual(1, calculate.call_count)
            self.assertEqual({a, a2, b, b2}, set(result[a]["member_alpaca_asset_ids"]))
            self.assertEqual({"FIRST", "EARLIER", "SECOND", "LATEST"}, set(result[a]["member_tickers"]))
            self.assertTrue(all(value is result[a] for value in result.values()))


def parent_fact():
    item = common_fact()
    item["facts"].update(
        schema_version=2,
        continuing_registrant=False,
        listing_succession={
            "kind": "new_public_parent",
            "publicly_traded_predecessor_count": 1,
            "successor_common_is_listed": True,
            "operating_target_was_private": True,
        },
    )
    item["facts"]["successor"]["registrant_cik"] = "456"
    item["facts"]["public_share_treatment"]["kind"] = "exchanged_one_for_one"
    return item


class PublicParentTests(unittest.TestCase):
    def test_documented_public_listing_successor_is_distinct_from_same_legal_registrant(self):
        facts = evidence(parent_fact())
        self.assertEqual(1, len(common.certify_group([facts])["certificates"]))
        self.assertFalse(common.certify_group([facts], new_public_parent=False)["certificates"])
        fact = facts["sourced_public_common_continuations"]["facts"][0]
        self.assertNotEqual(
            fact["facts"]["predecessor"]["registrant_cik"], fact["facts"]["successor"]["registrant_cik"]
        )

    def test_one_for_one_acquisition_or_multiple_public_predecessors_cannot_use_parent_route(self):
        for key, value in (
            ("operating_target_was_private", False),
            ("publicly_traded_predecessor_count", 2),
            ("publicly_traded_predecessor_count", True),
            ("successor_common_is_listed", False),
        ):
            with self.subTest(key=key, value=value):
                item = parent_fact()
                item["facts"]["listing_succession"][key] = value
                with self.assertRaises(ValueError):
                    accept_supplements([item])
        for key, value in (("continuing_registrant", True), ("holder_scope", "founders")):
            item = parent_fact()
            item["facts"][key] = value
            with self.assertRaises(ValueError):
                accept_supplements([item])
        facts = evidence(parent_fact())
        facts["asset_lookups"].append({**facts["asset_lookups"][1], "asset": {"id": "competing-parent"}})
        self.assertFalse(common.certify_group([facts])["certificates"])

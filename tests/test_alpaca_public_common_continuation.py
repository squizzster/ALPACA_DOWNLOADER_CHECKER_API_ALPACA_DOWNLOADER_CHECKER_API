"""Fresh public-common assertions reject; completed component outputs remain closed."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import public_common_continuation as common
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
    fact = copy.deepcopy(fact or common_fact())
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


def completed_certificate():
    # Literal closed producer output: never run the retired fact certifier to seed it.
    return {
        "source_fact_id": "public-holder-transition",
        "predecessor": {"asset_id": "predecessor-id", "historical_symbol": "SPAC", "cusip": "111111111"},
        "successor": {"asset_id": "successor-id", "historical_symbol": "BUSINESS", "cusip": "222222222"},
        "legal_date": "2024-01-04",
        "boundary": "2024-01-05",
        "public_share_treatment": copy.deepcopy(common_fact()["facts"]["public_share_treatment"]),
    }


class PublicCommonTests(unittest.TestCase):
    def test_nullable_rights_and_one_document_do_not_authorize_input(self):
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            common.certify_group([evidence()])

    def test_changed_rights_and_unknown_units_also_reject_at_ingress(self):
        for key, value in (("rights_unchanged", False), ("successor_units", None), ("successor_units", "2")):
            fact = common_fact()
            fact["facts"]["public_share_treatment"][key] = value
            with self.subTest(key=key, value=value), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([fact])

    def test_parent_option_cannot_reenable_assertion_certification(self):
        for enabled in (True, False):
            with self.subTest(enabled=enabled), self.assertRaisesRegex(ValueError, "inadmissible"):
                common.certify_group([evidence(parent_fact())], new_public_parent=enabled)

    def test_competing_or_incomplete_group_does_not_accept_research(self):
        peer = evidence()
        peer["asset_lookups"][0]["asset"]["id"] = "competing-id"
        for group, complete in (([evidence(), peer], True), ([evidence()], False)):
            with self.subTest(complete=complete), self.assertRaisesRegex(ValueError, "inadmissible"):
                common.certify_group(group, complete=complete)

    def test_matching_provider_event_cannot_admit_an_external_assertion(self):
        fact = common_fact()
        fact["facts"]["provider_event_match"] = {"id": "transaction", "action_type": "stock_merger"}
        supplied = evidence(fact)
        supplied["corporate_actions"] = [
            {
                "id": "transaction",
                "action_type": "stock_merger",
                "acquiree_cusip": "111111111",
                "acquirer_cusip": "222222222",
                "effective_date": "2024-01-04",
            }
        ]
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            common.certify_group([supplied])

    def test_holder_and_instrument_assertions_are_never_runtime_evidence(self):
        for field, value in (
            ("holder_scope", "founders"),
            ("holder_scope", "non_redeemed_public_common"),
            ("continuing_registrant", True),
        ):
            fact = common_fact()
            fact["facts"][field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, "inadmissible"):
                common.certify_group([evidence(fact)])

    def test_empty_provider_only_request_performs_no_endpoint_or_price_work(self):
        provider = evidence()
        provider.pop("sourced_public_common_continuations")
        with patch.object(Path, "open", side_effect=AssertionError("price opened")):
            self.assertEqual({"version": 1, "certificates": [], "issues": []}, common.certify_group([provider]))

    def test_nested_bare_fact_cannot_bypass_named_envelope(self):
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            common.certify_group([{"unexpected": [common_fact()]}])

    def test_literal_completed_certificate_remains_inspectable(self):
        report = completed_certificate()
        with patch.object(common, "certify_group", side_effect=AssertionError("closed certificate rechecked")):
            certificate = common.CommonCertificate(**report)
            self.assertEqual(report, certificate.to_report())
            self.assertEqual("predecessor-id", certificate.identity_edge().predecessor_asset_id)
            self.assertEqual("successor-id", certificate.identity_edge().successor_asset_id)
            self.assertIsNone(certificate.public_share_treatment["rights_unchanged"])

    def test_researched_recipe_rejects_before_source_or_price_access(self):
        with patch.object(Path, "open", side_effect=AssertionError("source prices opened")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                common.source_history(
                    "BUSINESS", None, None, None, None, complete=True, informative=True, accepted_bounds={}
                )

    def test_completed_source_bounds_are_consumed_without_proof_or_storage(self):
        membership = {"version": 1, "certificates": [completed_certificate()], "issues": []}
        handoff = {
            "version": 1,
            "symbol": "BUSINESS",
            "source_fact_id": "public-holder-transition",
            "route": [],
            "first": "2024-01-02",
            "last": "2024-01-04",
        }
        with (
            patch.object(common, "certify_group", side_effect=AssertionError("closed proof repeated")),
            patch.object(Path, "open", side_effect=AssertionError("closed prices opened")),
        ):
            histories = common.histories(handoff, membership)
        self.assertEqual(1, len(histories))
        self.assertEqual("predecessor-id", histories[0].interval.asset_id)


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

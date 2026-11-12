"""A public holding can continue while its legal share class changes."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_documented_markets import documented_fact

from libs.alpaca_end_of_day_security_correlation import documented_markets as documented
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_lineage_build import source_work, transform
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_security_lineage.event_supplements import accept_supplements


def reclassification_fact():
    fact = documented_fact(
        "continued-public-reclassification",
        ("BEFORE", "111111111"),
        ("AFTER", "222222222"),
        [
            ("BEFORE", "111111111", "2024-01-01", "2024-01-05", "Earlier Common"),
            ("AFTER", "111111111", "2024-01-05", "2024-01-08", "Continuing Common"),
            ("AFTER", "222222222", "2024-01-08", None, "Continuing Ordinary Shares"),
        ],
    )
    fact["facts"].update(
        schema_version=4,
        identity_treatment="automatic_public_common_reclassification",
        public_share_reclassification={
            "legal_effective_at": "2024-01-04T16:45:00-05:00",
            "predecessor_units": "1",
            "successor_units": "1",
            "same_legal_entity": True,
            "public_holders_continue": True,
            "rights_unchanged": False,
        },
    )
    return fact


class PublicReclassificationTests(TestCase):
    def test_actual_one_for_one_public_treatment_allows_changed_governance(self):
        fact = accept_supplements([reclassification_fact()])[0]
        evidence = {
            "SOURCE": {
                "sourced_market_occupancy": {"schema_version": 1, "facts": [fact]},
                "asset_lookups": [
                    {
                        "lookup_kind": "cusip",
                        "lookup": cusip,
                        "outcome": "found",
                        "http_status": 200,
                        "asset": {"id": identifier},
                    }
                    for cusip, identifier in (("111111111", "earlier"), ("222222222", "later"))
                ],
            }
        }
        plan = documented.bind(evidence, allow_reclassification=True)
        self.assertEqual(["earlier", "later"], plan["owners"][0]["identity"]["member_alpaca_asset_ids"])
        self.assertEqual("2024-01-08", plan["owners"][0]["episodes"][-1]["start_inclusive"])
        with self.assertRaisesRegex(SecurityCorrelationError, "policy 89"):
            documented.bind(evidence)

    def test_unknown_holder_treatment_private_or_unlike_instruments_do_not_join(self):
        for field, value in (
            ("public_holders_continue", False),
            ("same_legal_entity", False),
            ("successor_units", "2"),
            ("legal_effective_at", "2024-01-04T16:45:00"),
        ):
            item = reclassification_fact()
            item["facts"]["public_share_reclassification"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                accept_supplements([item])
        for kind in ("right", "ETF", "private_common", "unit"):
            item = reclassification_fact()
            for role in ("predecessor", "successor"):
                item["facts"][role]["instrument_kind"] = kind
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                accept_supplements([item])
        item = reclassification_fact()
        item["facts"].pop("public_share_reclassification")
        with self.assertRaises(ValueError):
            accept_supplements([item])

    def test_frozen_policy_rejects_new_treatment_before_owning_execution(self):
        fact = accept_supplements([reclassification_fact()])[0]
        frozen = load_policy("resources/alpaca_security_correlation_v88_legacy_policy.yaml")
        evidence = {"SOURCE": {"sourced_market_occupancy": {"schema_version": 1, "facts": [fact]}}}
        with patch.object(
            transform, "evaluate_policy", side_effect=AssertionError("frozen rule acquired new semantics")
        ):
            with self.assertRaisesRegex(LineageError, "policy 89"):
                transform.correlate(None, {}, deepcopy(evidence), policy=frozen)
        with self.assertRaisesRegex(LineageError, "policy 89"):
            source_work.execute_source_work(
                None,
                None,
                {},
                evidence,
                {},
                None,
                frozen,
                {},
                loader=lambda *args, **kwargs: self.fail("frozen source work opened prices"),
            )

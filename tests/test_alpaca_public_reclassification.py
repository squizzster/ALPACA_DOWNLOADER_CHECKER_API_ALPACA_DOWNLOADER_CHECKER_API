"""Retained public metadata remains readable; fresh authored treatments reject."""

from unittest import TestCase
from unittest.mock import patch

from test_alpaca_documented_markets import documented_fact

from libs.alpaca_end_of_day_security_correlation import documented_markets as documented
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
    def test_completed_dated_metadata_survives_without_new_reclassification_inference(self):
        reports = {
            "SOURCE": {
                "correlated": {
                    "segments": [
                        {"historical_symbol": "BEFORE", "security_description": {"name": "Earlier Common"}}
                    ]
                },
                "public_common_continuation_membership": {"version": 1, "certificates": [], "issues": []},
            }
        }
        with patch.object(documented, "bind", side_effect=AssertionError("completed metadata rebound")):
            self.assertIs(reports, documented.project({}, reports, {}))
        self.assertEqual(
            "Earlier Common", reports["SOURCE"]["correlated"]["segments"][0]["security_description"]["name"]
        )

    def test_authored_reclassification_terms_are_rejected_regardless_of_holder_treatment(self):
        for kind in ("common_stock", "right", "ETF", "private_common", "unit"):
            item = reclassification_fact()
            for role in ("predecessor", "successor"):
                item["facts"][role]["instrument_kind"] = kind
            with self.subTest(kind=kind), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([item])
            evidence = {"SOURCE": {"sourced_market_occupancy": {"schema_version": 1, "facts": [item]}}}
            with self.assertRaisesRegex(ValueError, "fact injection"):
                documented.bind(evidence, allow_reclassification=True)

    def test_current_and_frozen_profiles_reject_authored_input_before_rows_or_prices(self):
        from libs.alpaca_end_of_day_security_correlation import correlation
        from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan

        evidence = {
            "SOURCE": {"sourced_market_occupancy": {"schema_version": 1, "facts": [reclassification_fact()]}}
        }
        for path in (
            "resources/alpaca_security_correlation_v88_legacy_policy.yaml",
            "resources/alpaca_security_correlation_policy.yaml",
        ):
            policy = load_policy(path)
            empty = {"version": 1, "certificates": [], "issues": []}
            relations = {"version": 1, "relations": [], "issues": []}
            plan = SourceWorkPlan({}, {}, empty, empty, relations, relations, {}, False)
            with (
                self.subTest(path=path),
                patch.object(
                    correlation, "correlate_download_rows", side_effect=AssertionError("authored row inference")
                ),
                self.assertRaises((ValueError, LineageError)),
            ):
                transform.correlate(None, {}, evidence, policy=policy)
            with self.assertRaises((ValueError, LineageError)):
                source_work.execute_source_work(
                    None,
                    None,
                    {},
                    evidence,
                    {},
                    plan,
                    policy,
                    {},
                    loader=lambda *args: self.fail("prices opened"),
                )

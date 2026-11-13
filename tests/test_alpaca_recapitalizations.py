"""Recapitalization assertions cannot rewrite provider observations or allocate rows."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import recapitalizations
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage import event_supplements


def cash_action():
    return {
        "id": "capital-event",
        "action_type": "cash_dividend",
        "symbol": "SAME",
        "ex_date": "2017-01-25",
        "rate": 1.04,
    }


def recap_fact(action=None):
    return {
        "kind": "issuer_capital_repayment",
        "match": deepcopy(action or cash_action()),
        "facts": {
            "schema_version": 1,
            "issuer_cik": "123",
            "symbol": "SAME",
            "share_class": "common_stock",
            "same_issuer": True,
            "same_public_common": True,
            "cash_purpose": "capital_repayment",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "old_units": "27",
            "new_units": "26",
            "capital_repayment_usd": "1.04",
            "legal_effective_date": "2017-01-25",
            "exchange_effective_date": "2017-01-25",
            "predecessor_history_start": "2016-01-01",
        },
        "sources": [
            {
                "url": "https://issuer.example/capital-repayment",
                "title": "Common-share capital repayment and consolidation",
                "authority": "issuer",
                "document_date": "2017-01-23",
            }
        ],
    }


class RecapitalizationTests(TestCase):
    def case(self):
        fact = recap_fact()
        event = cash_action()
        rows = (("2016-01-04T05:00:00Z", "1"), ("2017-01-25T05:00:00Z", "1"))
        inputs = {
            "SAME": SimpleNamespace(
                source={"latest_request": {"asof": "2018-01-01"}}, views={view: rows for view in ADJUSTMENTS}
            )
        }
        identity = {"security_id": "public", "member_alpaca_asset_ids": ["known"], "member_tickers": ["SAME"]}
        reports = {
            "SAME": {
                "correlated": {
                    "row_count": 8,
                    "segments": [
                        {
                            "adjustment": view,
                            "row_start": 2,
                            "row_end": 2,
                            "row_count": 1,
                            "first_timestamp": rows[1][0],
                            "last_timestamp": rows[1][0],
                            "from": "2017-01-25",
                            "to": "2017-01-25",
                            "historical_symbol": "SAME",
                            "security_identity": identity,
                            "canonical_security": {
                                "security_id": "public",
                                "alpaca_asset_id": "known",
                                "cusip": "222222222",
                            },
                            "warnings": [],
                        }
                        for view in ADJUSTMENTS
                    ],
                },
                "uncorrelated": {
                    "row_count": 8,
                    "segments": [
                        {
                            "adjustment": view,
                            "row_start": 1,
                            "row_end": 1,
                            "row_count": 1,
                            "first_timestamp": rows[0][0],
                            "last_timestamp": rows[0][0],
                            "reason_codes": ["outside_supported_name_change_history"],
                        }
                        for view in ADJUSTMENTS
                    ],
                },
            }
        }
        evidence = {
            "SAME": {
                "event_supplements": [fact],
                "corporate_actions": [event],
                "asset_lookups": [
                    {"lookup_kind": "symbol", "lookup": "SAME", "asset": {"id": "known"}},
                    {
                        "lookup_kind": "cusip",
                        "lookup": "111111111",
                        "asset": None,
                        "outcome": "not_found",
                        "http_status": 404,
                    },
                    {
                        "lookup_kind": "cusip",
                        "lookup": "222222222",
                        "asset": {"id": "known"},
                        "outcome": "found",
                        "http_status": 200,
                    },
                ],
            }
        }
        return inputs, reports, evidence

    def test_authored_recapitalization_rejects_before_action_interpretation(self):
        raw = cash_action()
        original = deepcopy(raw)
        with patch.object(event_supplements, "_project_action", side_effect=AssertionError("action rewritten")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                event_supplements.project_actions([raw], [recap_fact()])
        self.assertEqual(original, raw)

    def test_all_share_treatments_reject_without_schema_authorization(self):
        for field, value in (
            ("same_issuer", True),
            ("same_issuer", False),
            ("share_class", "common_stock"),
            ("share_class", "right"),
        ):
            fact = recap_fact()
            fact["facts"][field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, "inadmissible"):
                event_supplements.accept_supplements([fact])

    def test_research_cannot_fill_a_missing_provider_predecessor(self):
        inputs, reports, evidence = self.case()
        original = deepcopy(reports)
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            recapitalizations.project(inputs, reports, evidence)
        self.assertEqual(original, reports)

    def test_conflicting_mapping_cannot_bypass_projection_rejection(self):
        _, reports, evidence = self.case()
        evidence["SAME"]["asset_lookups"][0]["asset"]["id"] = "rival"
        with patch.object(Path, "open", side_effect=AssertionError("prices opened")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                recapitalizations.project(None, reports, evidence)

    def test_provider_only_projection_does_not_invent_continuity(self):
        inputs, reports, evidence = self.case()
        evidence["SAME"].pop("event_supplements")
        result = recapitalizations.project(inputs, reports, evidence)
        self.assertEqual(reports, result)
        self.assertEqual(8, result["SAME"]["uncorrelated"]["row_count"])

    def test_cash_dividend_remains_cash_and_creates_no_uuid_exchange(self):
        raw = cash_action()
        projected, applied = event_supplements.project_actions([raw], [])
        self.assertEqual([raw], projected)
        self.assertEqual([], applied)
        self.assertEqual("cash_dividend", projected[0]["action_type"])
        self.assertEqual(
            ([], []), build_uuid_lineage_edges({"corporate_actions": projected, "asset_lookups": []})
        )

    def test_duplicate_actual_provider_split_observations_keep_one_public_component(self):
        raw = {
            "id": "actual-split",
            "action_type": "reverse_split",
            "symbol": "SAME",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "old_rate": 27,
            "new_rate": 26,
            "ex_date": "2017-01-25",
            "process_date": "2017-01-25",
        }
        lookups = [
            {
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "lookup": cusip,
                "asset": {"id": identifier, "symbol": "SAME", "class": "us_equity", "exchange": "NYSE"},
            }
            for cusip, identifier in (("111111111", "old"), ("222222222", "new"))
        ]
        edges, issues = build_uuid_lineage_edges(
            {"corporate_actions": [raw, {**raw, "id": "other-provider-observation"}], "asset_lookups": lookups}
        )
        self.assertFalse(issues)
        identity = security_identity_for_alpaca_assets(
            ["new"], edges, tickers=["SAME"], basis="provider_uuid_lineage"
        ).as_report_object()
        self.assertEqual(["new", "old"], identity["member_alpaca_asset_ids"])

"""Documented own-common continuation is distinct from an acquisition label."""

from copy import deepcopy
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
    def test_metadata_interpretation_preserves_raw_action_and_first_acceptance(self):
        raw = cash_action()
        old = deepcopy(raw)
        fact = event_supplements.accept_supplements([recap_fact()])[0]
        owner = event_supplements.EventProjection()
        project = owner.bind([fact])
        interpreted, applied = project([raw])
        self.assertEqual(old, raw)
        self.assertEqual([fact], applied)
        self.assertEqual("reverse_split", interpreted[0]["action_type"])
        with patch.object(
            event_supplements,
            "_project_action",
            side_effect=AssertionError("accepted action interpretation repeated"),
        ):
            self.assertEqual(interpreted, project([raw])[0])
        with self.assertRaisesRegex(ValueError, "changed"):
            project([{**raw, "rate": 2}])

    def test_cash_acquisition_different_symbol_or_class_cannot_supply_this_fact(self):
        for field, value in (
            ("same_issuer", False),
            ("same_public_common", False),
            ("cash_purpose", "acquisition"),
            ("share_class", "right"),
        ):
            item = recap_fact()
            item["facts"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                event_supplements.accept_supplements([item])
        item = recap_fact()
        item["facts"]["symbol"] = "OTHER"
        with self.assertRaises(ValueError):
            event_supplements.accept_supplements([item])

    def case(self):
        fact = event_supplements.accept_supplements([recap_fact()])[0]
        event = event_supplements.project_actions([cash_action()], [fact])[0][0]
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

    def test_missing_predecessor_uuid_preserves_documented_owner_without_fabrication(self):
        inputs, reports, evidence = self.case()
        old = deepcopy(reports)
        chosen = recapitalizations.project(inputs, reports, evidence)
        self.assertEqual(old, reports)
        self.assertEqual(0, chosen["SAME"]["uncorrelated"]["row_count"])
        recovered = [s for s in chosen["SAME"]["correlated"]["segments"] if s["row_start"] == 1]
        self.assertEqual(8, len(recovered))
        self.assertTrue(
            all(
                s["canonical_security"]["alpaca_asset_id"] is None
                and s["canonical_security"]["cusip"] == "111111111"
                and s["security_identity"]["security_id"] == "public"
                for s in recovered
            )
        )
        self.assertTrue(all("provider_predecessor_uuid_unavailable" in s["warnings"] for s in recovered))

    def test_a_competing_mapping_or_actual_rival_owner_stays_unallocated(self):
        for change in ("current_mapping", "old_cusip", "rival"):
            inputs, reports, evidence = self.case()
            if change == "current_mapping":
                evidence["SAME"]["asset_lookups"][0]["asset"]["id"] = "unrelated"
            elif change == "old_cusip":
                evidence["SAME"]["asset_lookups"][1]["asset"] = {"id": "unrelated"}
            else:
                for s in reports["SAME"]["uncorrelated"]["segments"]:
                    s["candidate_securities"] = [{"asset_id": "unrelated"}]
            chosen = recapitalizations.project(inputs, reports, evidence)
            self.assertEqual(8, chosen["SAME"]["uncorrelated"]["row_count"])

    def test_unknown_lookup_is_not_an_accepted_negative(self):
        inputs, reports, evidence = self.case()
        evidence["SAME"]["asset_lookups"][1].pop("outcome")
        self.assertEqual(
            8, recapitalizations.project(inputs, reports, evidence)["SAME"]["uncorrelated"]["row_count"]
        )

    def test_alternatives_within_one_public_identity_do_not_invent_public_ambiguity(self):
        inputs, reports, evidence = self.case()
        for segment in reports["SAME"]["uncorrelated"]["segments"]:
            segment["candidate_securities"] = [{"asset_id": "known"}]
        self.assertEqual(
            0, recapitalizations.project(inputs, reports, evidence)["SAME"]["uncorrelated"]["row_count"]
        )

    def test_two_observations_of_one_consolidation_do_not_create_two_equity_generations(self):
        raw = {
            "id": "recorded",
            "action_type": "stock_and_cash_merger",
            "acquiree_symbol": "SAME",
            "acquirer_symbol": "SAME",
            "acquiree_cusip": "111111111",
            "acquirer_cusip": "222222222",
            "acquiree_rate": 27,
            "acquirer_rate": 26,
            "cash_rate": 1.04,
            "effective_date": "2017-01-25",
        }
        item = recap_fact(raw)
        fact = event_supplements.accept_supplements([item])[0]
        projected = event_supplements.project_actions([raw], [fact])[0][0]
        assets = [
            {
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "lookup": c,
                "asset": {"id": i, "symbol": "SAME", "class": "us_equity", "exchange": "NYSE"},
            }
            for c, i in (("111111111", "old"), ("222222222", "new"))
        ]
        lineage = {
            "corporate_actions": [projected, {**projected, "id": "provider-split"}],
            "asset_lookups": assets,
        }
        edges, issues = build_uuid_lineage_edges(lineage)
        self.assertFalse(issues)
        identity = security_identity_for_alpaca_assets(
            ["new"], edges, tickers=["SAME"], basis="explicit_uuid_lineage"
        ).as_report_object()
        self.assertEqual(["new", "old"], identity["member_alpaca_asset_ids"])

"""Dated market evidence owns labels and temporary-market source authority."""

import json
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import valid_bars
from test_alpaca_observation_authority import views_of

from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.market_occupancy import project
from libs.alpaca_lineage_build import transform
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_security_lineage.event_supplements import accept_supplements


def market_fact():
    return {
        "kind": "dated_market_occupancy",
        "match": {"source_id": "documented-market-dates"},
        "facts": {
            "schema_version": 1,
            "ownership_support": "existing_public_identity",
            "predecessor": {"symbol": "MAIN", "cusip": "111111111", "instrument_kind": "ADS"},
            "successor": {"symbol": "MAIN", "cusip": "222222222", "instrument_kind": "ADS"},
            "episodes": [
                {
                    "symbol": "MAIN",
                    "cusip": "111111111",
                    "start_inclusive": "2016-01-01",
                    "end_exclusive": "2026-10-02",
                    "market": "regular_way",
                },
                {
                    "symbol": "ALT",
                    "cusip": "222222222",
                    "start_inclusive": "2026-10-02",
                    "end_exclusive": "2026-10-07",
                    "market": "when_issued",
                },
                {
                    "symbol": "MAIN",
                    "cusip": "222222222",
                    "start_inclusive": "2026-10-07",
                    "end_exclusive": None,
                    "market": "regular_way",
                },
            ],
        },
        "sources": [
            {
                "url": "https://issuer.example/market-notice",
                "title": "Dated ADS markets",
                "authority": "issuer",
                "document_date": "2026-10-01",
            }
        ],
    }


def case():
    fact = accept_supplements([market_fact()])[0]
    inputs, reports, evidence = {}, {}, {}
    identity = {
        "security_id": "public",
        "member_alpaca_asset_ids": ["regular-id", "temporary-id"],
        "member_tickers": ["MAIN", "ALT"],
    }
    for name, first, last in (("MAIN", "2026-10-01", "2026-10-14"), ("ALT", "2026-10-02", "2026-10-07")):
        bars = valid_bars(first, last)
        for bar in bars:
            bar["n"] = 99 if name == "MAIN" else 7
            bar["v"] = bar["n"] * 100
        inputs[name] = SimpleNamespace(
            source_sha256=name,
            views=views_of(bars),
            source={
                "activity_consistent": True,
                "history_start": "2016-01-01T00:00:00Z",
                "latest_request": {"asof": "2026-10-14", "end_exclusive": "2026-10-14T00:00:00Z"},
            },
        )
        positive, negative = [], []
        for view in ADJUSTMENTS:
            if name == "ALT":
                negative.append(
                    {
                        "adjustment": view,
                        "row_start": 1,
                        "row_end": 1,
                        "first_timestamp": bars[0]["t"],
                        "last_timestamp": bars[0]["t"],
                        "reason_codes": ["outside_supported_name_change_history"],
                    }
                )
            start = 2 if name == "ALT" else 1
            positive.append(
                {
                    "adjustment": view,
                    "row_start": start,
                    "row_end": len(bars),
                    "first_timestamp": bars[start - 1]["t"],
                    "last_timestamp": bars[-1]["t"],
                    "historical_symbol": name,
                    "security_identity": identity,
                    "canonical_security": {
                        "security_id": "public",
                        "alpaca_asset_id": "temporary-id" if name == "ALT" else "regular-id",
                        "cusip": "222222222",
                    },
                    "ticker_ownership_interval": {},
                    "warnings": [],
                }
            )
        reports[name] = {
            "correlated": {
                "row_count": sum(s["row_end"] - s["row_start"] + 1 for s in positive),
                "segments": positive,
            },
            "uncorrelated": {"row_count": len(negative), "segments": negative},
        }
        evidence[name] = {
            "query": {"input_symbol": name},
            "sourced_market_occupancy": {"version": 1, "facts": [fact]},
            "asset_lookups": [
                {"lookup_kind": "cusip", "lookup": "111111111", "asset": {"id": "regular-id"}},
                {"lookup_kind": "cusip", "lookup": "222222222", "asset": {"id": "temporary-id"}},
                {
                    "lookup_kind": "symbol",
                    "lookup": name,
                    "asset": {"id": "temporary-id" if name == "ALT" else "regular-id"},
                },
            ],
        }
    return inputs, reports, evidence


def continuous_case():
    inputs, reports, evidence = case()
    fact = market_fact()
    fact["facts"]["schema_version"] = 2
    fact["facts"]["adjustment_basis"] = "continuous_same_security_and_cusip"
    fact["facts"]["predecessor"]["cusip"] = "222222222"
    fact["facts"]["episodes"][0]["cusip"] = "222222222"
    fact = accept_supplements([fact])[0]
    for name in inputs:
        values = {}
        for view, rows in inputs[name].views.items():
            revised = []
            for row in rows:
                row = list(row)
                row[5], row[6] = "700", "7"
                if name == "MAIN" and ("dividend" in view or view == "all"):
                    for column in (1, 2, 3, 4, 7):
                        row[column] = str(Decimal(row[column]) * Decimal("0.9"))
                revised.append(tuple(row))
            values[view] = tuple(revised)
        inputs[name].views = values
        evidence[name]["sourced_market_occupancy"]["facts"] = [fact]
    evidence["MAIN"]["corporate_actions"] = [
        {
            "id": "later-dividend",
            "action_type": "cash_dividend",
            "symbol": "MAIN",
            "cusip": "222222222",
            "ex_date": "2026-10-09",
            "rate": "0.25",
        }
    ]
    return inputs, reports, evidence


class DatedMarketTests(TestCase):
    def test_empty_market_suffixes_keep_accepted_binding_without_old_rows(self):
        inputs, reports, evidence = case()
        identity = reports["MAIN"]["correlated"]["segments"][0]["security_identity"]
        for name, view in inputs.items():
            view.accepted_identity_memberships = dict.fromkeys(identity["member_alpaca_asset_ids"], identity)
            view.views = None
            reports[name] = {
                "source_row_count": 0,
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 0, "segments": []},
            }
        with patch.object(transform, "evaluate_policy", return_value=reports):
            result = transform.correlate(
                None, inputs, evidence, policy=SimpleNamespace(implementation_correlation_version=83)
            )
        self.assertEqual(reports, result)

    def test_inactive_binding_cannot_resolve_fresh_unallocated_rows_or_ambiguous_membership(self):
        inputs, reports, evidence = case()
        identity = reports["MAIN"]["correlated"]["segments"][0]["security_identity"]
        for name in inputs:
            reports[name] = {
                "source_row_count": 0,
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 0, "segments": []},
            }
        accepted = {name: dict.fromkeys(identity["member_alpaca_asset_ids"], identity) for name in inputs}
        reports["ALT"]["source_row_count"] = 1
        reports["ALT"]["uncorrelated"] = {"row_count": 1, "segments": [{"row_start": 1, "row_end": 1}]}
        with self.assertRaisesRegex(SecurityCorrelationError, "lacks one observed"):
            project(inputs, reports, evidence, inactive_memberships=accepted)
        reports["ALT"] = {
            "source_row_count": 0,
            "correlated": {"row_count": 0, "segments": []},
            "uncorrelated": {"row_count": 0, "segments": []},
        }
        accepted["ALT"] = {
            key: {**identity, "security_id": "rival"} for key in identity["member_alpaca_asset_ids"]
        }
        with self.assertRaisesRegex(SecurityCorrelationError, "lacks one observed"):
            project(inputs, reports, evidence, inactive_memberships=accepted)

    def test_unknown_document_publication_date_does_not_reject_dated_market_evidence(self):
        fact = market_fact()
        fact["sources"][0]["document_date"] = None
        self.assertIsNone(accept_supplements([fact])[0]["sources"][0]["document_date"])

    def test_temporary_market_recovers_first_day_and_supplies_one_bundle_despite_later_route(self):
        inputs, reports, evidence = case()
        old = deepcopy(reports)
        projected = project(inputs, reports, evidence)
        self.assertEqual(old, reports)
        self.assertEqual(0, projected["ALT"]["uncorrelated"]["row_count"])
        self.assertEqual(24, projected["ALT"]["correlated"]["row_count"])
        chosen, corrections = select_observations(inputs, projected)
        self.assertFalse(corrections)
        self.assertEqual(
            {"selected"}, {s["observation_selection"] for s in chosen["ALT"]["correlated"]["segments"]}
        )
        for segment in chosen["MAIN"]["correlated"]["segments"]:
            self.assertEqual(
                "superseded" if segment["historical_symbol"] == "ALT" else "selected",
                segment["observation_selection"],
            )
        self.assertEqual(
            "2026-10-02",
            chosen["ALT"]["correlated"]["segments"][0]["ticker_ownership_interval"]["start_date_inclusive"],
        )

    def test_partial_temporary_source_cannot_displace_complete_bundle(self):
        inputs, reports, evidence = case()
        inputs["ALT"].views["all"] = inputs["ALT"].views["all"][1:]
        reports["ALT"]["uncorrelated"]["segments"] = [
            s for s in reports["ALT"]["uncorrelated"]["segments"] if s["adjustment"] != "all"
        ]
        for segment in reports["ALT"]["correlated"]["segments"]:
            if segment["adjustment"] == "all":
                segment.update(row_start=1, row_end=2)
        chosen, corrections = select_observations(inputs, project(inputs, reports, evidence))
        self.assertFalse(corrections)
        for segment in chosen["MAIN"]["correlated"]["segments"]:
            if segment["row_start"] == 2:
                self.assertEqual("selected", segment["observation_selection"])

    def test_actual_changed_mapping_cannot_acquire_direct_market_authority(self):
        inputs, reports, evidence = case()
        evidence["ALT"]["asset_lookups"][-1]["asset"]["id"] = "other-id"
        projected = project(inputs, reports, evidence)
        self.assertEqual(8, projected["ALT"]["uncorrelated"]["row_count"])
        self.assertTrue(
            all(
                not s["observation_authority"]["direct_market_source"]
                for s in projected["ALT"]["correlated"]["segments"]
            )
        )

    def test_malformed_or_overlapping_market_facts_fail_at_ingress(self):
        for change in ("class", "overlap", "unbounded_temporary"):
            with self.subTest(change=change):
                fact = market_fact()
                if change == "class":
                    fact["facts"]["successor"]["instrument_kind"] = "warrant"
                elif change == "overlap":
                    fact["facts"]["episodes"][1]["start_inclusive"] = "2026-10-01"
                else:
                    fact["facts"]["episodes"][1]["end_exclusive"] = None
                with self.assertRaises(ValueError):
                    accept_supplements([fact])


class AdjustmentContinuityTests(TestCase):
    def test_same_issue_keeps_complete_adjustments_and_temporary_market_labels(self):
        inputs, reports, evidence = continuous_case()
        projected = project(inputs, reports, evidence, allow_adjustment_continuity=True)
        chosen, corrections = select_observations(inputs, projected)
        self.assertFalse(corrections)
        for name in inputs:
            for segment in chosen[name]["correlated"]["segments"]:
                self.assertEqual(
                    "selected" if name == "MAIN" else "superseded", segment["observation_selection"]
                )
                if segment.get("observation_authority", {}).get("market") == "when_issued":
                    self.assertEqual("ALT", segment["historical_symbol"])

    def test_same_cusip_assertion_rejects_different_identifiers_at_ingress(self):
        fact = market_fact()
        fact["facts"].update(schema_version=2, adjustment_basis="continuous_same_security_and_cusip")
        with self.assertRaisesRegex(ValueError, "same security and CUSIP"):
            accept_supplements([fact])

    def test_later_route_without_positive_adjustment_evidence_cannot_win(self):
        for missing in (
            "explicit_basis",
            "same_dividend_cusip",
            "applicable_ex_date",
            "equal_raw",
            "temporary_omission",
        ):
            with self.subTest(missing=missing):
                inputs, reports, evidence = continuous_case()
                if missing == "explicit_basis":
                    for value in evidence.values():
                        fact = json.loads(json.dumps(value["sourced_market_occupancy"]["facts"][0]))
                        fact["facts"]["schema_version"] = 1
                        fact["facts"].pop("adjustment_basis", None)
                        value["sourced_market_occupancy"]["facts"] = [fact]
                elif missing == "same_dividend_cusip":
                    evidence["MAIN"]["corporate_actions"][0]["cusip"] = "333333333"
                elif missing == "applicable_ex_date":
                    evidence["MAIN"]["corporate_actions"][0]["ex_date"] = "2026-10-15"
                elif missing == "equal_raw":
                    row = list(inputs["ALT"].views["raw"][0])
                    row[4] = "99"
                    inputs["ALT"].views["raw"] = (tuple(row), *inputs["ALT"].views["raw"][1:])
                else:
                    rows = inputs["ALT"].views["dividend"]
                    row = list(rows[0])
                    row[4] = "99"
                    inputs["ALT"].views["dividend"] = (tuple(row), *rows[1:])
                chosen, corrections = select_observations(
                    inputs, project(inputs, reports, evidence, allow_adjustment_continuity=True)
                )
                self.assertFalse(corrections)
                first = next(
                    s
                    for s in chosen["ALT"]["correlated"]["segments"]
                    if s["adjustment"] == "raw" and s["row_start"] == 1
                )
                self.assertEqual("selected", first["observation_selection"])

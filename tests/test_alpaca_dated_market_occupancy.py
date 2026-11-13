"""Retired dated facts reject; closed source labels retain generic bundle behavior."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import valid_bars
from test_alpaca_observation_authority import views_of

from libs.alpaca_end_of_day_security_correlation import market_occupancy
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
    fact = market_fact()
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


def completed_market_reports(inputs, reports):
    """Literal completed source/label outputs, never produced by a fact recipe."""
    result = deepcopy(reports)
    for name, view in inputs.items():
        template = reports[name]["correlated"]["segments"][0]
        segments = []
        for adjustment, rows in view.views.items():
            for index, row in enumerate(rows, 1):
                day = row[0][:10]
                temporary = "2026-10-02" <= day < "2026-10-07"
                label = "ALT" if temporary else "MAIN"
                first, last = (
                    ("2026-10-02", "2026-10-07")
                    if temporary
                    else ("2016-01-01", "2026-10-02")
                    if day < "2026-10-02"
                    else ("2026-10-07", None)
                )
                segments.append(
                    {
                        **deepcopy(template),
                        "adjustment": adjustment,
                        "row_start": index,
                        "row_end": index,
                        "row_count": 1,
                        "first_timestamp": row[0],
                        "last_timestamp": row[0],
                        "historical_symbol": label,
                        "ticker_ownership_interval": {
                            "historical_symbol": label,
                            "start_date_inclusive": first,
                            "end_date_exclusive": last,
                        },
                        "observation_authority": {
                            "version": 1,
                            "market": "when_issued" if temporary else "regular_way",
                            "direct_market_source": name == label,
                            "from": first,
                            "to": last,
                        },
                    }
                )
        result[name]["correlated"] = {"row_count": len(segments), "segments": segments}
        result[name]["uncorrelated"] = {"row_count": 0, "segments": []}
    return result


class DatedMarketTests(TestCase):
    def test_empty_provider_suffixes_keep_completed_membership_without_rows(self):
        inputs, reports, evidence = case()
        identity = reports["MAIN"]["correlated"]["segments"][0]["security_identity"]
        for name, view in inputs.items():
            evidence[name].pop("sourced_market_occupancy")
            view.accepted_identity_memberships = dict.fromkeys(identity["member_alpaca_asset_ids"], identity)
            view.views = None
            reports[name] = {
                "source_row_count": 0,
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 0, "segments": []},
            }
        with patch.object(transform, "evaluate_policy", return_value=reports):
            self.assertEqual(
                reports,
                transform.correlate(
                    None, inputs, evidence, policy=SimpleNamespace(implementation_correlation_version=97)
                ),
            )

    def test_inactive_membership_cannot_authorize_a_new_fact(self):
        inputs, reports, evidence = case()
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            project(inputs, reports, evidence, inactive_memberships={"ALT": {"public": {}}})

    def test_unknown_document_date_does_not_make_research_admissible(self):
        fact = market_fact()
        fact["sources"][0]["document_date"] = None
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            accept_supplements([fact])

    def test_completed_temporary_labels_supply_a_coherent_numerical_bundle(self):
        inputs, reports, _ = case()
        completed = completed_market_reports(inputs, reports)
        original = deepcopy(completed)
        with patch.object(
            market_occupancy, "project", side_effect=AssertionError("completed labels recalculated")
        ):
            chosen, corrections = select_observations(inputs, completed)
        self.assertFalse(corrections)
        self.assertEqual(original, completed)
        self.assertEqual(
            {"selected"}, {s["observation_selection"] for s in chosen["ALT"]["correlated"]["segments"]}
        )
        for segment in chosen["MAIN"]["correlated"]["segments"]:
            self.assertEqual(
                "superseded" if segment["historical_symbol"] == "ALT" else "selected",
                segment["observation_selection"],
            )

    def test_partial_temporary_bundle_cannot_displace_a_complete_source(self):
        inputs, reports, _ = case()
        inputs["ALT"].views["all"] = inputs["ALT"].views["all"][1:]
        chosen, corrections = select_observations(inputs, completed_market_reports(inputs, reports))
        self.assertFalse(corrections)
        first = [
            s for s in chosen["MAIN"]["correlated"]["segments"] if s["first_timestamp"][:10] == "2026-10-02"
        ]
        self.assertEqual(set(ADJUSTMENTS), {s["adjustment"] for s in first})
        self.assertTrue(all(s["observation_selection"] == "selected" for s in first))

    def test_explicit_raw_market_plan_cannot_bypass_the_input_guard(self):
        plan = market_occupancy.MarketPlan((("old-fact", "public", ()),), {}, {})
        with patch.object(Path, "open", side_effect=AssertionError("price access")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                project(None, None, {}, plan=plan)

    def test_all_authored_market_shapes_reject_at_ingress(self):
        for change in ("valid", "different_class", "overlap", "unbounded"):
            fact = market_fact()
            if change == "different_class":
                fact["facts"]["successor"]["instrument_kind"] = "warrant"
            elif change == "overlap":
                fact["facts"]["episodes"][1]["start_inclusive"] = "2026-10-01"
            elif change == "unbounded":
                fact["facts"]["episodes"][1]["end_exclusive"] = None
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([fact])


class AdjustmentContinuityTests(TestCase):
    def test_adjusted_prices_do_not_turn_an_authored_basis_into_evidence(self):
        inputs, reports, evidence = continuous_case()
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            project(inputs, reports, evidence, allow_adjustment_continuity=True)

    def test_same_cusip_authored_assertion_always_rejects(self):
        for same in (True, False):
            fact = market_fact()
            fact["facts"].update(schema_version=2, adjustment_basis="continuous_same_security_and_cusip")
            if same:
                fact["facts"]["predecessor"]["cusip"] = "222222222"
            with self.subTest(same=same), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([fact])

    def test_changed_bundle_keeps_labels_and_one_source_for_all_views(self):
        inputs, reports, _ = continuous_case()
        completed = completed_market_reports(inputs, reports)
        with patch.object(market_occupancy, "project", side_effect=AssertionError("labels recomputed")):
            chosen, corrections = select_observations(inputs, completed)
        self.assertFalse(corrections)
        first = [s for s in chosen["ALT"]["correlated"]["segments"] if s["first_timestamp"][:10] == "2026-10-02"]
        self.assertEqual(set(ADJUSTMENTS), {s["adjustment"] for s in first})
        self.assertTrue(all(s["observation_selection"] == "selected" for s in first))
        self.assertTrue(all(s["historical_symbol"] == "ALT" for s in first))

"""Terminal dates constrain episodes; they do not establish unlimited ownership."""

from copy import deepcopy
from datetime import date
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_end_of_day_security_correlation import (
    asset,
    complete_query,
    occupancy,
    same_symbol_split_lineage,
)
from test_alpaca_lineage_build import IdentityClient
from test_alpaca_lineage_build import asset as provider_asset
from test_alpaca_pricing_download import FakeClient, snapshot

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_end_of_day_security_correlation import ownership_interval_rules as rules
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.download import download_pricing


def terminal_fixture():
    owner = asset("old-id", "OTHER", "Distinct Former Entity")
    rival = asset("live-id", "LIVE", "Continuing Entity")
    actions = [
        {
            "id": "cash",
            "action_type": "cash_merger",
            "acquiree_symbol": "LIVE",
            "acquiree_cusip": "old-cusip",
            "effective_date": "2023-04-03",
            "process_date": "2023-04-03",
            "rate": 10,
        },
        {
            "id": "before",
            "action_type": "cash_dividend",
            "symbol": "LIVE",
            "cusip": "live-cusip",
            "ex_date": "2023-03-01",
            "process_date": "2023-03-01",
            "rate": 0.1,
        },
        {
            "id": "after",
            "action_type": "cash_dividend",
            "symbol": "LIVE",
            "cusip": "live-cusip",
            "ex_date": "2023-06-01",
            "process_date": "2023-06-01",
            "rate": 0.1,
        },
    ]
    return {
        "query": complete_query("LIVE"),
        "corporate_actions": actions,
        "identities": [owner, rival],
        "symbol_occupancy_evidence": [
            occupancy(
                "LIVE",
                "old-cusip",
                "old-id",
                action_type="cash_merger",
                action_id="cash",
                process_date="2023-04-03",
                role="acquiree",
            ),
            occupancy(
                "LIVE",
                "live-cusip",
                "live-id",
                action_type="cash_dividend",
                action_id="before",
                process_date="2023-03-01",
                role="symbol",
            ),
        ],
        "asset_lookups": [
            {
                "lookup_kind": kind,
                "lookup": key,
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": value,
            }
            for kind, key, value in (
                ("symbol", "LIVE", rival),
                ("cusip", "live-cusip", rival),
                ("cusip", "old-cusip", owner),
            )
        ],
    }


class TerminalOccupancyTests(TestCase):
    def test_cross_terminal_occupancy_withholds_only_new_unbounded_inference(self):
        lineage = terminal_fixture()
        before = deepcopy(lineage)
        intervals, issues = rules.build_ticker_ownership_intervals(lineage)
        self.assertEqual(intervals, [])
        self.assertEqual(lineage, before)
        self.assertEqual(
            [i["reason_code"] for i in issues],
            ["cash_merger_unbounded_ownership_conflicts_with_dated_occupancy"],
        )
        self.assertEqual({a["action_id"] for a in issues[0]["competing_occupancy"]}, {"before", "after"})

    def test_earlier_only_or_later_only_occupancy_does_not_disprove_terminal_episode(self):
        for remove in ("before", "after"):
            with self.subTest(remove=remove):
                lineage = terminal_fixture()
                lineage["corporate_actions"] = [a for a in lineage["corporate_actions"] if a["id"] != remove]
                intervals, issues = rules.build_ticker_ownership_intervals(lineage)
                self.assertEqual(len(intervals), 1)
                self.assertEqual(issues, [])

    def test_terminal_boundary_is_exclusive(self):
        for after, veto in (("2023-04-02", False), ("2023-04-03", True), ("2023-04-04", True)):
            with self.subTest(after=after):
                lineage = terminal_fixture()
                lineage["corporate_actions"][-1]["ex_date"] = after
                intervals, _ = rules.build_ticker_ownership_intervals(lineage)
                self.assertEqual(not intervals, veto)
        lineage = terminal_fixture()
        lineage["corporate_actions"][1]["ex_date"] = "2023-04-03"
        self.assertEqual(len(rules.build_ticker_ownership_intervals(lineage)[0]), 1)

    def test_nonpositive_or_invalid_action_cannot_disprove_ownership(self):
        for rate in (0, -1, "NaN", "Infinity", "bad"):
            with self.subTest(rate=rate):
                lineage = terminal_fixture()
                lineage["corporate_actions"][-1]["rate"] = rate
                self.assertEqual(len(rules.build_ticker_ownership_intervals(lineage)[0]), 1)

    def test_missing_or_conflicting_exact_lookup_cannot_supply_rival_ownership(self):
        for mutation in ("missing", "duplicate", "different_uuid"):
            with self.subTest(mutation=mutation):
                lineage = terminal_fixture()
                if mutation == "missing":
                    lineage["asset_lookups"].pop(1)
                elif mutation == "duplicate":
                    lineage["asset_lookups"].append(deepcopy(lineage["asset_lookups"][1]))
                else:
                    lineage["asset_lookups"][1]["asset"] = asset("unknown", "LIVE", "Unknown")
                self.assertEqual(len(rules.build_ticker_ownership_intervals(lineage)[0]), 1)

    def test_same_uuid_cusip_change_is_not_a_rival(self):
        lineage = terminal_fixture()
        lineage["symbol_occupancy_evidence"][1]["asset_id"] = "old-id"
        lineage["asset_lookups"][1]["asset"] = lineage["identities"][0]
        self.assertEqual(len(rules.build_ticker_ownership_intervals(lineage)[0]), 1)

    def test_two_different_rivals_cannot_combine_their_dates(self):
        lineage = terminal_fixture()
        later = asset("third-id", "LIVE", "Third Entity")
        lineage["corporate_actions"][-1]["cusip"] = "third-cusip"
        lineage["identities"].append(later)
        lineage["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": "third-cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": later,
            }
        )
        lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "LIVE",
                "third-cusip",
                "third-id",
                action_type="cash_dividend",
                action_id="after",
                process_date="2023-06-01",
                role="symbol",
            )
        )
        self.assertEqual(len(rules.build_ticker_ownership_intervals(lineage)[0]), 1)

    def test_existing_dated_episode_is_retained_or_clamped_never_extended(self):
        lineage = terminal_fixture()
        mapping, _ = rules._asset_ids_by_symbol_and_cusip(lineage)
        context = rules._OwnershipIntervalRuleContext(tuple(lineage["corporate_actions"]), mapping, lineage)
        for old_end, expected_end in (("2023-03-15", "2023-03-15"), ("2023-04-15", "2023-04-03")):
            with self.subTest(end=old_end):
                established = rules.TickerOwnershipInterval(
                    "LIVE",
                    "old-cusip",
                    "old-id",
                    date(2020, 1, 1),
                    date.fromisoformat(old_end),
                    (rules.AppliedOwnershipIntervalRule(1, "name_change_ticker_transition", ("rename",)),),
                )
                result = rules._apply_cash_merger_terminal_rule(
                    context, (established,), 3, "cash_merger_acquiree_terminal_boundary"
                )
                self.assertEqual(len(result.intervals), 1)
                self.assertEqual(result.intervals[0].start_date_inclusive, established.start_date_inclusive)
                self.assertEqual(result.intervals[0].end_date_exclusive.isoformat(), expected_end)

    def test_group_cache_builds_uuid_authority_once_and_reuses_it(self):
        from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges

        lineage = terminal_fixture()
        cache = CorrelationCache()
        with (
            patch(
                "libs.alpaca_end_of_day_security_correlation.row_correlation.build_uuid_lineage_edges",
                wraps=build_uuid_lineage_edges,
            ) as owner,
            patch(
                "libs.alpaca_end_of_day_security_correlation.ownership_interval_rules.build_uuid_lineage_edges",
                side_effect=AssertionError("repeated authority"),
            ),
        ):
            first = cache.lineage_analysis(lineage)
            second = cache.lineage_analysis(lineage)
        self.assertEqual(first, second)
        self.assertEqual(owner.call_count, 1)

    def test_accepted_split_membership_outweighs_distinct_uuid_names_and_dividends(self):
        lineage = same_symbol_split_lineage()
        lineage["identities"][0]["name"] = "Former legal name"
        lineage["identities"][1]["name"] = "Current legal name"
        for symbol, cusip, identity, kind, action_id, day, role in (
            ("SPLT", "111111111", "old-split-asset-id", "cash_dividend", "before", "2026-09-01", "symbol"),
            ("SPLT", "222222222", "new-split-asset-id", "cash_merger", "cash", "2026-10-01", "acquiree"),
        ):
            lineage["symbol_occupancy_evidence"].append(
                occupancy(
                    symbol, cusip, identity, action_type=kind, action_id=action_id, process_date=day, role=role
                )
            )
        lineage["corporate_actions"].extend(
            [
                {
                    "id": "cash",
                    "action_type": "cash_merger",
                    "acquiree_symbol": "SPLT",
                    "acquiree_cusip": "222222222",
                    "effective_date": "2026-10-01",
                    "process_date": "2026-10-01",
                },
                *(
                    {
                        "id": key,
                        "action_type": "cash_dividend",
                        "symbol": "SPLT",
                        "cusip": "111111111",
                        "ex_date": day,
                        "rate": 1,
                    }
                    for key, day in (("before", "2026-09-01"), ("after", "2026-10-05"))
                ),
            ]
        )
        intervals, issues = rules.build_ticker_ownership_intervals(lineage)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(intervals[0].asset_id, "new-split-asset-id")
        self.assertEqual(issues, [])
        # A rejected split is not continuity. The same facts must then remain a
        # real rival, rather than a dependency link silently joining identities.
        lineage["corporate_actions"][0]["new_rate"] = 0
        self.assertEqual(rules.build_ticker_ownership_intervals(lineage)[0], [])


class TerminalGroupPublicationTests(NativeCase):
    def test_whole_group_keeps_distinct_histories_and_reuses_completed_decision(self):
        from decimal import Decimal

        from libs.alpaca_lineage_build import storage

        live = valid_bars("2016-01-01", "2026-10-14")
        other = valid_bars("2016-01-01", "2023-03-01")
        for rows, price in ((live, 20), (other, 40)):
            for i, row in enumerate(rows):
                row.update({key: Decimal(price + i % 13) for key in ("o", "h", "l", "c", "vw")})
        for symbol, rows in (("LIVE", live), ("OTHER", other)):
            download_pricing(self.store, symbol, "2026-10-14", client=FakeClient(rows))
        assets = {"LIVE": provider_asset("LIVE", "live-id"), "OTHER": provider_asset("OTHER", "old-id")}
        assets.update({"live-cusip": assets["LIVE"], "old-cusip": assets["OTHER"]})
        actions = terminal_fixture()["corporate_actions"]
        identity = IdentityClient(assets, actions)
        pricing = FakeClient([])
        preserved = {s: snapshot(self.store / "end_of_day/pricing" / s) for s in ("LIVE", "OTHER")}
        result = run_lineage(self.store, ["LIVE", "OTHER"], identity_client=identity, pricing_client=pricing)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(pricing.calls, [])
        for s in preserved:
            self.assertEqual(snapshot(self.store / "end_of_day/pricing" / s), preserved[s])
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "LIVE", index)
        self.assertTrue(any(a["id"] == "cash" for a in receipt["evidence"]["corporate_actions"]))
        reader = StandaloneReader(self.store)
        for symbol, expected, rows in (("LIVE", "live-id", live), ("OTHER", "old-id", other)):
            with self.subTest(symbol=symbol):
                result = reader.get_histories(symbol)
                for view in result["views"].values():
                    self.assertEqual(len(view["bars"]), len(rows))
                    self.assertEqual(view["bars"][0]["close"], str(rows[0]["c"]))
                resolved = reader.resolve_ticker(symbol)
                self.assertEqual(
                    {m["public_id"] for m in resolved["matches"]},
                    {security_id_for_alpaca_asset_ids([expected])},
                )
        before_requests = identity.request_count
        with patch(
            "libs.alpaca_lineage_build.pipeline.correlate",
            side_effect=AssertionError("replayed accepted decision"),
        ):
            repeat = run_lineage(self.store, ["LIVE"], identity_client=identity, pricing_client=pricing)
        self.assertEqual(repeat["failed"], 0, repeat)
        self.assertEqual(identity.request_count, before_requests)
        self.assertEqual(pricing.calls, [])
        data = self.bootstrap()
        actual = {s.public_id: s for s in data.list_securities()}
        for identity_id, label in (("live-id", "LIVE"), ("old-id", "OTHER")):
            self.assertEqual(actual[security_id_for_alpaca_asset_ids([identity_id])].tickers, (label,))

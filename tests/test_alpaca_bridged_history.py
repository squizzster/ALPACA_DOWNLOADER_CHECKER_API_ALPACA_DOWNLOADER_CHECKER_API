import copy
import unittest
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.predecessor_prefix import (
    INDEPENDENT_HISTORY_WARNING,
    assess_bridged_initial_prefix,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.terminal_episode import assess_bridged_traded_episodes
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge, asset, correlation_source


class BridgedHistoryTests(unittest.TestCase):
    def test_departure_on_first_inert_session_has_zero_distance(self):
        action = next(item for item in self.lineage["corporate_actions"] if item.get("id") == "out2")
        action["effective_date"] = self.sessions[105].isoformat()
        episodes = assess_bridged_traded_episodes("OLD", self.lineage, self.intervals, self.work)
        self.assertIn(
            (self.sessions[102], self.sessions[104], "old2"),
            [(episode.first, episode.last, episode.asset_id) for episode in episodes],
        )

    def setUp(self):
        self.sessions = tuple(
            date.fromisoformat(s)
            for s in expected_market_session_coverage("2024-01-03", "2025-12-31").session_dates[:208]
        )
        self.rows = []
        for i, day in enumerate(self.sessions):
            inert = 2 <= i < 102 or 105 <= i < 205
            price = 11 if i < 102 and inert else 114 if inert else 10 + i
            self.rows.append(
                dict(
                    t=day.isoformat() + "T12:00:00Z",
                    o=str(price),
                    h=str(price),
                    l=str(price),
                    c=str(price),
                    v="0" if inert else "100",
                    n="0" if inert else "10",
                    vw="0" if inert else str(price),
                )
            )
        self.lineage = {"identities": [], "corporate_actions": [], "symbol_occupancy_evidence": []}
        for number, position in ((1, 7), (2, 107)):
            add_name_change_edge(
                self.lineage,
                action_id="out" + str(number),
                process_date=self.sessions[position].isoformat(),
                predecessor=("old" + str(number), "OLD", "cusip" + str(number), "Old"),
                successor=("old" + str(number), "NEXT" + str(number), "cusip" + str(number), "Old"),
            )
        current = asset("current", "OLD", "Current")
        current.update(lookup_cusips=["current-cusip"], status="active")
        self.lineage["identities"].append(current)
        for number in (1, 2):
            next(item for item in self.lineage["identities"] if item["id"] == "old" + str(number))["symbol"] = (
                "NEXT" + str(number)
            )
        self.lineage["asset_lookups"] = [
            dict(
                lookup="cusip" + str(number),
                lookup_kind="cusip",
                found=True,
                outcome="found",
                http_status=200,
                asset=next(item for item in self.lineage["identities"] if item["id"] == "old" + str(number)),
            )
            for number in (1, 2)
        ]
        self.lineage["asset_lookups"].extend(
            [
                dict(lookup=key, lookup_kind=kind, found=True, outcome="found", http_status=200, asset=current)
                for key, kind in (("OLD", "symbol"), ("current-cusip", "cusip"))
            ]
        )
        self.arrival = dict(
            id="arrival",
            action_type="stock_merger",
            acquiree_symbol="FUND",
            acquiree_cusip="fund-cusip",
            acquirer_symbol="OLD",
            acquirer_cusip="current-cusip",
            acquiree_rate=1,
            acquirer_rate=1.1,
            effective_date=self.sessions[204].isoformat(),
        )
        self.lineage["corporate_actions"].append(self.arrival)
        self.intervals, _ = build_ticker_ownership_intervals(self.lineage)
        self.edges, _ = build_uuid_lineage_edges(self.lineage)
        self.work = []
        self.descriptors = {}
        self.segments = []
        self.donors = {}
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            filename = adjustment + ".csv.gz"
            source = {
                **correlation_source("OLD", asof=self.sessions[-1].isoformat()),
                "adjustment": adjustment,
                "data_file": filename,
                "request_to": (self.sessions[-1] + timedelta(days=1)).isoformat(),
            }
            self.work.append((SimpleNamespace(rows=copy.deepcopy(self.rows)), source, self.sessions))
            self.descriptors[filename] = dict(
                data_file=filename, adjustment=adjustment, download_sha256="generation"
            )
            self.donors[filename] = copy.deepcopy(self.rows[:8])
            self.segments.append(
                dict(
                    data_file=filename,
                    row_start=1,
                    row_end=8,
                    correlation_rule=dict(name="asof_selected_uuid_lineage_interval"),
                    historical_symbol="OLD",
                    canonical_security=dict(alpaca_asset_id="old1", cusip="cusip1"),
                    feed="sip",
                    timeframe="1Day",
                    download_sha256="generation",
                )
            )

    def prefix(self):
        return assess_bridged_initial_prefix(
            "OLD",
            self.lineage,
            self.intervals,
            self.edges,
            self.work,
            correlation_loader=lambda symbol: SimpleNamespace(
                correlated={"segments": self.segments if symbol == "NEXT1" else []},
                sources_by_filename=self.descriptors,
            ),
            source_rows_loader=lambda symbol, filename: (self.donors[filename], "generation"),
        )

    def episodes(self):
        return assess_bridged_traded_episodes("OLD", self.lineage, self.intervals, self.work)

    def test_three_owners_are_bounded_and_two_inert_bridges_remain_unmatched(self):
        prefix = self.prefix()
        episodes = self.episodes()
        self.assertEqual(1, len(prefix))
        self.assertEqual(2, len(episodes))
        result = correlate_download_rows(
            self.rows,
            self.work[0][1],
            self.lineage,
            self.intervals,
            independent_history=prefix,
            bridged_episodes=episodes,
        )
        self.assertEqual([2, 3, 3], [s["row_count"] for s in result["correlated"]])
        self.assertEqual(
            ["old1", "old2", "current"],
            [s["canonical_security"]["alpaca_asset_id"] for s in result["correlated"]],
        )
        self.assertEqual(200, sum(s["row_count"] for s in result["uncorrelated"]))

    def test_prefix_requires_whole_initial_match_and_independent_donor(self):
        self.donors["raw.csv.gz"][0]["v"] = "101"
        self.assertEqual((), self.prefix())
        self.donors["raw.csv.gz"][0]["v"] = "100"
        self.segments[0]["warnings"] = [INDEPENDENT_HISTORY_WARNING]
        self.assertEqual((), self.prefix())

    def test_missing_views_or_disagreeing_inert_separator_cannot_certify_history(self):
        self.work[-1][0].rows[50]["n"] = "1"
        self.assertEqual((), self.prefix())
        self.assertEqual(1, len(self.episodes()))
        self.work.pop()
        self.assertEqual((), self.episodes())

    def test_current_symbol_alone_remote_arrival_or_invalid_rate_is_insufficient(self):
        for change in (
            {"acquirer_cusip": "wrong"},
            {"effective_date": self.sessions[110].isoformat()},
            {"acquirer_rate": 0},
        ):
            original = self.arrival.copy()
            self.arrival.update(change)
            self.assertEqual(["old2"], [e.asset_id for e in self.episodes()])
            self.arrival.clear()
            self.arrival.update(original)

    def test_competing_start_inside_episode_and_late_departure_reject_only_affected_run(self):
        owner = self.intervals[0]
        self.intervals.append(
            replace(owner, asset_id="rival", start_date_inclusive=self.sessions[103], end_date_exclusive=None)
        )
        self.assertNotIn("old2", [e.asset_id for e in self.episodes()])
        self.intervals.pop()
        self.lineage["corporate_actions"][1]["process_date"] = self.sessions[140].isoformat()
        self.assertNotIn("old2", [e.asset_id for e in self.episodes()])

    def test_later_same_uuid_alias_needs_complete_nonbranching_name_path(self):
        add_name_change_edge(
            self.lineage,
            action_id="later",
            process_date=self.sessions[150].isoformat(),
            predecessor=("old2", "NEXT2", "cusip2", "Old"),
            successor=("old2", "LIVE", "cusip2", "Old"),
        )
        next(item for item in self.lineage["identities"] if item["id"] == "old2")["symbol"] = "LIVE"
        self.assertIn("old2", [e.asset_id for e in self.episodes()])
        self.lineage["corporate_actions"][-1]["new_symbol"] = "WRONG"
        self.assertNotIn("old2", [e.asset_id for e in self.episodes()])

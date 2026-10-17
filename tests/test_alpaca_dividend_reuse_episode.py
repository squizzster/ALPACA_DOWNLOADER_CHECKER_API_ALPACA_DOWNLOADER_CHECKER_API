"""A current dividend anchors an observed episode, not a fabricated inception."""

import unittest

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from tests import test_alpaca_bridged_history as bridged_fixtures
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge


class DividendReuseEpisodeTests(unittest.TestCase):
    def setUp(self):
        self.case = bridged_fixtures.BridgedHistoryTests()
        self.case.setUp()
        self.lineage = self.case.lineage
        self.lineage["corporate_actions"].remove(self.case.arrival)
        self.dividend = {
            "id": "current-dividend",
            "action_type": "cash_dividend",
            "symbol": "OLD",
            "cusip": "current-cusip",
            "ex_date": self.case.sessions[-1].isoformat(),
            "process_date": self.case.sessions[-1].isoformat(),
            "rate": "0.1",
        }
        self.lineage["corporate_actions"].append(self.dividend)
        self.lineage["query"] = {
            "corporate_actions_data_quality": "complete",
            "corporate_actions_start": self.case.sessions[0].isoformat(),
            "corporate_actions_end": self.case.work[0][1]["request_to"],
        }

    def current_episode(self):
        return [episode for episode in self.case.episodes() if episode.asset_id == "current"]

    def test_anchors_whole_observed_suffix_not_only_dividend_day(self):
        episodes = self.current_episode()
        self.assertEqual(1, len(episodes))
        self.assertEqual(self.case.sessions[205], episodes[0].first)
        self.assertEqual(self.case.sessions[207], episodes[0].last)
        result = correlate_download_rows(
            self.case.rows,
            self.case.work[0][1],
            self.lineage,
            self.case.intervals,
            bridged_episodes=self.case.episodes(),
        )
        current = [
            row for row in result["correlated"] if row["canonical_security"]["alpaca_asset_id"] == "current"
        ]
        self.assertEqual([3], [row["row_count"] for row in current])
        self.assertEqual(["current"], current[0]["security_identity"]["member_alpaca_asset_ids"])
        self.assertEqual(206, current[0]["row_start"])
        self.assertEqual(208, current[0]["row_end"])
        bridge = [
            row
            for row in result["uncorrelated"]
            if row["reason_codes"] == ["synthetic_carry_forward_between_unrelated_issues"]
        ]
        self.assertEqual([(106, 205)], [(row["row_start"], row["row_end"]) for row in bridge])

    def test_no_dividend_in_episode_or_incomplete_coverage_declines(self):
        for mutation in (
            lambda: self.dividend.update(ex_date=self.case.sessions[204].isoformat()),
            lambda: self.dividend.update(cusip="foreign-cusip"),
            lambda: self.dividend.update(rate="0"),
            lambda: self.lineage["query"].update(corporate_actions_data_quality="partial"),
            lambda: self.lineage["query"].update(corporate_actions_end=self.case.sessions[206].isoformat()),
        ):
            self.setUp()
            mutation()
            self.assertEqual([], self.current_episode())

    def test_conflicting_dividend_or_dated_occupancy_declines(self):
        self.lineage["corporate_actions"].append({**self.dividend, "id": "foreign", "cusip": "foreign-cusip"})
        self.assertEqual([], self.current_episode())
        self.lineage["corporate_actions"].pop()
        self.lineage["symbol_occupancy_evidence"].append(
            {
                "symbol": "OLD",
                "cusip": "foreign-cusip",
                "asset_id": "foreign-id",
                "evidence": [{"process_date": self.case.sessions[206].isoformat()}],
            }
        )
        self.assertEqual([], self.current_episode())

    def test_return_to_old_label_is_not_unrelated_reuse(self):
        add_name_change_edge(
            self.lineage,
            action_id="return",
            process_date=self.case.sessions[203].isoformat(),
            predecessor=("old2", "NEXT2", "cusip2", "Old"),
            successor=("current", "OLD", "current-cusip", "Current"),
        )
        self.assertEqual([], self.current_episode())

    def test_suffix_structural_event_is_not_ignored(self):
        self.lineage["corporate_actions"].append(
            {
                "id": "new-arrival",
                "action_type": "name_change",
                "old_symbol": "ELSE",
                "new_symbol": "OLD",
                "process_date": self.case.sessions[206].isoformat(),
            }
        )
        self.assertEqual([], self.current_episode())

    def test_raw_malformed_departure_is_not_an_accepted_terminal_boundary(self):
        departure = next(action for action in self.lineage["corporate_actions"] if action["id"] == "out2")
        departure.pop("new_cusip")
        next(asset for asset in self.lineage["identities"] if asset["id"] == "old2")["symbol"] = "OLD"
        self.case.intervals, _ = build_ticker_ownership_intervals(self.lineage)
        self.assertEqual([], self.current_episode())

    def test_known_departure_does_not_require_resolving_the_former_issues_successor(self):
        next(asset for asset in self.lineage["identities"] if asset["id"] == "old2")["symbol"] = "OLD"
        self.lineage["symbol_occupancy_evidence"] = [
            item for item in self.lineage["symbol_occupancy_evidence"] if item["symbol"] != "NEXT2"
        ]
        self.case.intervals, _ = build_ticker_ownership_intervals(self.lineage)
        self.assertEqual(1, len(self.current_episode()))

    def test_missing_view_different_bridge_or_incomplete_tail_cannot_anchor_suffix(self):
        self.case.work[-1][0].rows[160]["v"] = "100"
        self.assertEqual([], self.current_episode())
        self.setUp()
        self.case.work.pop()
        self.assertEqual([], self.current_episode())
        self.setUp()
        for _, source, _ in self.case.work:
            source["request_to"] = self.case.sessions[-1].replace(year=2026).isoformat()
        self.assertEqual([], self.current_episode())


if __name__ == "__main__":
    unittest.main()

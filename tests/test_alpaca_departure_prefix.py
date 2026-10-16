import unittest
from dataclasses import replace

from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    AppliedOwnershipIntervalRule,
    TickerOwnershipInterval,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.terminal_episode import assess_dated_departure_prefix
from tests import test_alpaca_terminal_episode as terminal_fixtures
from tests.test_alpaca_end_of_day_security_correlation import correlation_source


class DeparturePrefixTests(unittest.TestCase):
    def setUp(self):
        fixture = terminal_fixtures.TerminalEpisodeTests()
        fixture.setUp()
        self.lineage, self.work, self.sessions = fixture.lineage, fixture.work, fixture.sessions
        self.boundary = self.sessions[2]
        self.interval = TickerOwnershipInterval(
            "OLD",
            "444444444",
            "old-owner",
            None,
            self.boundary,
            (AppliedOwnershipIntervalRule(1, "name_change_ticker_transition", ("old-departure",)),),
        )
        self.lineage["corporate_actions"].append(
            {
                "id": "old-departure",
                "action_type": "name_change",
                "process_date": self.boundary.isoformat(),
                "old_symbol": "OLD",
                "old_cusip": "444444444",
                "new_symbol": "RENAMED",
                "new_cusip": "444444444",
            }
        )
        self.lineage["asset_lookups"].append(
            {
                "lookup_kind": "cusip",
                "lookup": "444444444",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": {"id": "old-owner", "class": "us_equity", "symbol": "RENAMED"},
            }
        )
        for download, _, _ in self.work:
            for row in download.rows[2:102]:
                row.update(o="21", h="21", l="21", c="21", vw="21")

    def assess(self, intervals=None):
        return assess_dated_departure_prefix("OLD", self.lineage, intervals or [self.interval], self.work)

    def test_complete_first_traded_episode_is_owned_without_claiming_carry_forward_or_later_prices(self):
        prefix = self.assess()
        self.assertEqual(
            (self.sessions[0], self.sessions[1], "old-owner"),
            (prefix.first, prefix.last, prefix.interval.asset_id),
        )
        result = correlate_download_rows(
            self.work[0][0].rows,
            correlation_source("OLD", asof="2024-01-01"),
            self.lineage,
            [self.interval],
            departure_prefix=prefix,
        )
        assigned = result["correlated"][0]
        self.assertEqual(2, assigned["row_count"])
        self.assertEqual("old-owner", assigned["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(
            "latest_dated_ownership_interval_overlapping_claims", assigned["correlation_rule"]["name"]
        )
        self.assertIn("dated_departure_before_synthetic_carry_forward", assigned["warnings"])

    def test_misaligned_departure_does_not_rehabilitate_a_mixed_earlier_prefix(self):
        self.assertIsNone(self.assess([replace(self.interval, end_date_exclusive=self.sessions[-1])]))

    def test_competing_departure_or_positive_owner_inside_prefix_vetoes(self):
        other = replace(self.interval, asset_id="other", start_date_inclusive=self.sessions[0])
        self.assertIsNone(self.assess([self.interval, other]))
        other = replace(self.interval, asset_id="other", end_date_exclusive=self.sessions[1])
        self.assertIsNone(self.assess([self.interval, other]))

    def test_bad_endpoint_or_missing_view_does_not_qualify(self):
        lookup = self.lineage["asset_lookups"][-1]
        lookup["asset"]["id"] = "wrong"
        self.assertIsNone(self.assess())
        lookup["asset"]["id"] = "old-owner"
        self.work.pop()
        self.assertIsNone(self.assess())

    def test_flat_bridge_must_carry_the_actual_last_traded_close_in_every_view(self):
        self.work[-1][0].rows[2]["c"] = "20"
        self.assertIsNone(self.assess())

    def test_initial_or_uninformative_source_is_not_a_supported_historical_prefix(self):
        for download, _, _ in self.work:
            for row in download.rows[:2]:
                row.update(o="21", h="21", l="21", c="21", v="0", n="0")
        self.assertIsNone(self.assess())

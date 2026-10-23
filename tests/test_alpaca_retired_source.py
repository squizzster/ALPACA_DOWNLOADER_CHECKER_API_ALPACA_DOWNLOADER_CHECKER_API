import copy
import unittest
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.predecessor_prefix import IndependentHistoricalInterval
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.source_continuity import (
    retired_history_lower_bound,
    retired_source_identity,
)
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS as ALPACA_ADJUSTMENT_MODES
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests import test_alpaca_predecessor_prefix as prefix_fixtures
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge, asset, correlation_source


class RetiredSourceTests(unittest.TestCase):
    def setUp(self):
        self.lineage = {"identities": [], "corporate_actions": [], "symbol_occupancy_evidence": []}
        for old, new, when in (
            ("PRE", "OLD", "2024-08-16"),
            ("OLD", "MID", "2025-01-02"),
            ("MID", "LIVE", "2025-02-03"),
        ):
            add_name_change_edge(
                self.lineage,
                action_id=old + new,
                process_date=when,
                predecessor=("one", old, "one-cusip", "Continuing"),
                successor=("one", new, "one-cusip", "Continuing"),
            )
        add_name_change_edge(
            self.lineage,
            action_id="other",
            process_date="2025-10-30",
            predecessor=("two", "PRE", "two-cusip", "Other"),
            successor=("other", "OTHER", "other-cusip", "Other"),
        )
        current = self.lineage["identities"][0]
        current.update(symbol="LIVE", exchange="OTC", status="inactive")
        self.lineage["asset_lookups"] = [
            {
                "lookup": "OLD",
                "lookup_kind": "symbol",
                "found": False,
                "outcome": "not_found",
                "http_status": 404,
            },
            {
                "lookup": "LIVE",
                "lookup_kind": "symbol",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": current,
            },
            {
                "lookup": "one-cusip",
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": current,
            },
        ]
        self.intervals, _ = build_ticker_ownership_intervals(self.lineage)
        self.edges, _ = build_uuid_lineage_edges(self.lineage)

    def assess(self, edges=None):
        return retired_source_identity(
            "OLD", self.lineage, self.intervals, self.edges if edges is None else edges, "2026-10-06"
        )

    def history(self, first_inert, last_inert):
        sessions = tuple(
            date.fromisoformat(day)
            for day in expected_market_session_coverage("2024-01-03", "2025-06-01").session_dates
        )
        rows = [
            dict(
                t=day.isoformat() + "T12:00:00Z",
                **dict.fromkeys(
                    ("o", "h", "l", "c", "vw"), str(10 if first_inert <= i <= last_inert else 20 + i)
                ),
                v="0" if first_inert <= i <= last_inert else "100",
                n="0" if first_inert <= i <= last_inert else "10",
            )
            for i, day in enumerate(sessions)
        ]
        work = [
            (SimpleNamespace(rows=copy.deepcopy(rows)), {"adjustment": mode}, sessions)
            for mode in ALPACA_ADJUSTMENT_MODES
        ]
        return rows, sessions, work

    def test_retired_source_cannot_infer_history_before_its_dated_label(self):
        for first in (0, 2):
            with self.subTest(first_inert=first):
                rows, sessions, work = self.history(first, first + 99)
                lower = retired_history_lower_bound("OLD", self.intervals, work)
                self.assertEqual(sessions[first + 99] + timedelta(days=1), lower)
                result = correlate_download_rows(
                    rows,
                    correlation_source("OLD", asof="2026-10-06"),
                    self.lineage,
                    self.intervals,
                    retired_lower_bound=lower,
                )
                start = sessions.index(date(2024, 8, 16))
                self.assertEqual(start, result["uncorrelated"][0]["row_count"])
                self.assertEqual(start + 1, result["correlated"][0]["row_start"])

    def test_later_halt_does_not_erase_supported_predecessor_history(self):
        _, _, work = self.history(175, 300)
        self.assertIsNone(retired_history_lower_bound("OLD", self.intervals, work))

    def test_short_nonshared_or_informative_runs_do_not_invent_a_boundary(self):
        _, _, short = self.history(0, 98)
        self.assertIsNone(retired_history_lower_bound("OLD", self.intervals, short))
        _, _, work = self.history(0, 99)
        self.assertIsNone(retired_history_lower_bound("OLD", self.intervals, work[:-1]))
        work[-1][0].rows[50]["n"] = "1"
        self.assertIsNone(retired_history_lower_bound("OLD", self.intervals, work))

    def test_lower_bound_falls_through_to_independent_earlier_ownership(self):
        rows, sessions, work = self.history(2, 101)
        lower = retired_history_lower_bound("OLD", self.intervals, work)
        owner = next(
            item for item in self.intervals if item.historical_symbol == "PRE" and item.asset_id == "one"
        )
        result = correlate_download_rows(
            rows,
            correlation_source("OLD", asof="2026-10-06"),
            self.lineage,
            self.intervals,
            retired_lower_bound=lower,
            independent_history=(IndependentHistoricalInterval(sessions[0], sessions[1], owner),),
        )
        self.assertEqual(2, result["correlated"][0]["row_count"])
        self.assertEqual(10, result["correlated"][0]["correlation_rule"]["order"])
        self.assertEqual(sessions.index(date(2024, 8, 16)) - 2, result["uncorrelated"][0]["row_count"])

    def test_retired_endpoint_selection_does_not_certify_an_earlier_foreign_label(self):
        selected = self.assess()
        self.assertEqual(("LIVE", "one-cusip", "one"), selected.identity_key())
        source = correlation_source("OLD", asof="2026-10-06")
        with patch(
            "libs.alpaca_end_of_day_security_correlation.row_correlation.retired_source_identity",
            return_value=None,
        ):
            before = correlate_download_rows(prefix_fixtures.rows(), source, self.lineage, self.intervals)
        after = correlate_download_rows(prefix_fixtures.rows(), source, self.lineage, self.intervals)
        self.assertEqual(2, before["uncorrelated"][0]["row_count"])
        self.assertEqual(before, after)
        self.assertEqual(2, after["uncorrelated"][0]["row_count"])
        self.assertEqual(
            before["correlated"][-1]["correlation_rule"], after["correlated"][-1]["correlation_rule"]
        )

    def test_reused_requested_label_and_wrong_current_endpoint_do_not_infer_owner(self):
        self.lineage["identities"].append(asset("reused", "OLD", "New unrelated security"))
        self.assertIsNone(self.assess())
        self.lineage["identities"].pop()
        self.lineage["asset_lookups"][1]["asset"] = {**self.lineage["identities"][0], "id": "different"}
        self.assertIsNone(self.assess())

    def test_missing_link_different_uuid_and_fork_cannot_be_bridged(self):
        old_mid = next(edge for edge in self.edges if edge.predecessor_symbol == "OLD")
        self.assertIsNone(self.assess([edge for edge in self.edges if edge != old_mid]))
        self.assertIsNone(
            self.assess(
                [
                    replace(edge, successor_asset_id="different") if edge == old_mid else edge
                    for edge in self.edges
                ]
            )
        )
        self.assertIsNone(self.assess([*self.edges, replace(old_mid, successor_symbol="OTHER")]))

    def test_duplicate_current_symbol_or_cusip_endpoint_and_cycle_are_ambiguous(self):
        saved = copy.deepcopy(self.lineage["asset_lookups"])
        for position in (1, 2):
            self.lineage["asset_lookups"].append(copy.deepcopy(saved[position]))
            self.assertIsNone(self.assess())
            self.lineage["asset_lookups"].pop()
        cycle = [
            replace(edge, successor_symbol="OLD") if edge.predecessor_symbol == "MID" else edge
            for edge in self.edges
        ]
        self.assertIsNone(self.assess(cycle))

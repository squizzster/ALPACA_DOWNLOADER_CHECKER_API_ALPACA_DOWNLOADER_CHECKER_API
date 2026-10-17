import copy
import unittest
from dataclasses import replace
from datetime import date, timedelta
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.ownership_interval_rules import (
    AppliedOwnershipIntervalRule,
    TickerOwnershipInterval,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.terminal_episode import assess_terminal_traded_episode
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from libs.download_alpaca_end_of_day_bars import ALPACA_ADJUSTMENT_MODES
from tests.test_alpaca_end_of_day_security_correlation import correlation_source


class TerminalEpisodeTests(unittest.TestCase):
    def setUp(self):
        self.sessions = tuple(
            date.fromisoformat(s)
            for s in expected_market_session_coverage("2023-01-03", "2023-12-31").session_dates[:105]
        )
        self.rows = [
            {
                "t": session.isoformat() + "T12:00:00Z",
                "o": str(price),
                "h": str(price),
                "l": str(price),
                "c": str(price),
                "vw": str(price),
                "v": "0" if inactive else "100",
                "n": "0" if inactive else "10",
            }
            for index, session in enumerate(self.sessions)
            for inactive in (2 <= index < 102,)
            for price in (10 if inactive else 20 + index,)
        ]
        self.lineage = {
            "identities": [{"id": "current-lookup", "symbol": "OLD"}, {"id": "terminal", "symbol": "OLD"}],
            "asset_lookups": [
                {
                    "lookup": "111111111",
                    "lookup_kind": "cusip",
                    "found": True,
                    "outcome": "found",
                    "http_status": 200,
                    "asset": {"id": "terminal", "class": "us_equity", "symbol": "OLD", "exchange": "OTC"},
                }
            ],
            "corporate_actions": [
                {
                    "id": "terminal-event",
                    "action_type": "stock_merger",
                    "acquiree_symbol": "OLD",
                    "acquiree_cusip": "111111111",
                    "acquirer_symbol": "NEW",
                    "acquirer_cusip": "222222222",
                    "effective_date": self.sessions[-1].isoformat(),
                }
            ],
        }
        self.work = [
            (SimpleNamespace(rows=copy.deepcopy(self.rows)), {"adjustment": a}, self.sessions)
            for a in ALPACA_ADJUSTMENT_MODES
        ]

    def assess(self, intervals=()):
        return assess_terminal_traded_episode("OLD", self.lineage, intervals, self.work)

    def test_only_resumed_episode_uses_exact_terminal_owner_and_otc_is_not_a_historical_veto(self):
        episode = self.assess()
        self.assertEqual(
            (self.sessions[102], self.sessions[-1], "terminal"), (episode.first, episode.last, episode.asset_id)
        )
        result = correlate_download_rows(
            self.rows, correlation_source("OLD", asof="2024-01-01"), self.lineage, [], terminal_episode=episode
        )
        self.assertEqual(3, result["correlated"][0]["row_count"])
        self.assertEqual("terminal", result["correlated"][0]["canonical_security"]["alpaca_asset_id"])
        self.assertEqual(["terminal"], result["correlated"][0]["security_identity"]["member_alpaca_asset_ids"])
        self.assertEqual(102, result["uncorrelated"][0]["row_count"])

    def test_inactivity_alone_and_current_symbol_identity_cannot_choose_owner(self):
        self.lineage["corporate_actions"] = []
        self.assertIsNone(self.assess())

    def test_nonflat_nonzero_short_or_uninformative_bridge_does_not_qualify(self):
        for case in ("nonflat", "traded", "short", "no resumed variation", "trailing"):
            saved = copy.deepcopy(self.work)
            for download, _, _ in self.work:
                if case == "nonflat":
                    download.rows[50]["o"] = "9"
                elif case == "traded":
                    download.rows[50]["v"] = download.rows[50]["n"] = "1"
                elif case == "short":
                    download.rows[2]["v"] = download.rows[2]["n"] = "1"
                elif case == "no resumed variation":
                    for row in download.rows[102:]:
                        for key in ("o", "h", "l", "c"):
                            row[key] = "20"
                else:
                    for row in download.rows[102:]:
                        row.update(o="10", h="10", l="10", c="10", v="0", n="0")
            with self.subTest(case=case):
                self.assertIsNone(self.assess())
            self.work = saved

    def test_all_views_and_complete_resumed_grid_are_required(self):
        self.work[-1][0].rows[50]["n"] = "1"
        self.assertIsNone(self.assess())
        self.work[-1][0].rows[50]["n"] = "0"
        self.work.pop()
        self.assertIsNone(self.assess())

    def test_competing_dated_owner_or_terminal_endpoint_remains_undecided(self):
        interval = TickerOwnershipInterval("OLD", "other", "other", self.sessions[102], None, ())
        self.assertIsNone(self.assess([interval]))
        interval = TickerOwnershipInterval("OLD", "other", "other", None, self.sessions[-1], ())
        self.assertIsNone(self.assess([interval]))
        self.lineage["asset_lookups"].append(
            {
                **copy.deepcopy(self.lineage["asset_lookups"][0]),
                "lookup": "333333333",
                "asset": {"id": "other", "class": "us_equity", "symbol": "OLD"},
            }
        )
        self.lineage["corporate_actions"].append(
            {**self.lineage["corporate_actions"][0], "id": "competing", "acquiree_cusip": "333333333"}
        )
        self.assertIsNone(self.assess())

    def test_small_processing_discrepancy_is_tolerated_but_remote_event_is_not(self):
        event = self.lineage["corporate_actions"][0]
        for delta in (-1, 1, 3, 4):
            event["effective_date"] = (self.sessions[-1] + timedelta(days=delta)).isoformat()
            self.assertIsNotNone(self.assess())
        event["effective_date"] = (self.sessions[-1] + timedelta(days=30)).isoformat()
        self.assertIsNone(self.assess())

    def add_current_reuse_suffix(self):
        later = tuple(
            date.fromisoformat(s)
            for s in expected_market_session_coverage("2024-01-02", "2024-01-10").session_dates
        )
        current = {
            "id": "current-lookup",
            "symbol": "OLD",
            "class": "us_equity",
            "exchange": "NASDAQ",
            "name": "Current occupant",
            "status": "active",
        }
        self.lineage["identities"][0] = current
        self.lineage["asset_lookups"][0]["asset"]["status"] = "inactive"
        self.lineage["asset_lookups"].append(
            {
                "lookup": "OLD",
                "lookup_kind": "symbol",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": current,
            }
        )
        self.lineage["corporate_actions"][0]["effective_date"] = "2023-09-01"
        for download, source, _ in self.work:
            source.update({**correlation_source("OLD", asof="2024-01-09"), "adjustment": source["adjustment"]})
            source["request_to"] = "2024-01-10T00:00:00Z"
            download.rows.extend(
                {**self.rows[-1], "t": session.isoformat() + "T12:00:00Z", "c": str(40 + index)}
                for index, session in enumerate(later)
            )
        self.work = [(download, source, self.sessions + later) for download, source, _ in self.work]
        return later

    def test_delayed_terminal_inside_gap_owns_only_final_prior_episode(self):
        later = self.add_current_reuse_suffix()
        episode = self.assess()
        self.assertEqual((self.sessions[102], self.sessions[-1]), (episode.first, episode.last))
        self.assertTrue(episode.terminated_during_reuse_gap)
        result = correlate_download_rows(
            self.work[0][0].rows,
            self.work[0][1],
            self.lineage,
            [],
            terminal_episode=episode,
        )
        self.assertEqual([3, len(later)], [item["row_count"] for item in result["correlated"]])
        self.assertEqual(102, result["uncorrelated"][0]["row_count"])
        self.assertIn("terminal_action_inside_later_reuse_gap", result["correlated"][0]["warnings"])

    def initial_run_before_reuse(self):
        self.sessions = self.sessions[:5]
        self.rows = [
            {**row, "v": "100", "n": "10", "c": str(20 + index)} for index, row in enumerate(self.rows[:5])
        ]
        self.work = [
            (SimpleNamespace(rows=copy.deepcopy(self.rows)), {"adjustment": adjustment}, self.sessions)
            for adjustment in ALPACA_ADJUSTMENT_MODES
        ]
        later = self.add_current_reuse_suffix()
        when = self.sessions[-1] + timedelta(days=1)
        self.lineage["corporate_actions"][0]["effective_date"] = when.isoformat()
        owner = TickerOwnershipInterval("OLD", "111111111", "terminal", None, when, ())
        return owner, later

    def test_complete_initial_terminal_run_and_current_suffix_keep_separate_owners(self):
        owner, later = self.initial_run_before_reuse()
        episode = self.assess([owner])
        self.assertIsNotNone(episode)
        self.assertTrue(episode.initial_before_reuse_gap)
        self.assertEqual((self.sessions[0], self.sessions[-1]), (episode.first, episode.last))
        result = correlate_download_rows(
            self.work[0][0].rows, self.work[0][1], self.lineage, [owner], terminal_episode=episode
        )
        self.assertEqual([5, len(later)], [item["row_count"] for item in result["correlated"]])
        self.assertEqual([], result["uncorrelated"])
        self.assertIn("complete_initial_terminal_episode", result["correlated"][0]["warnings"])
        self.assertNotIn("synthetic_carry_forward_boundary", result["correlated"][0]["warnings"])

    def test_initial_run_requires_nearby_terminal_not_merely_event_inside_gap(self):
        owner, _ = self.initial_run_before_reuse()
        self.lineage["corporate_actions"][0]["effective_date"] = "2023-09-01"
        owner = replace(owner, end_date_exclusive=date(2023, 9, 1))
        self.assertIsNone(self.assess([owner]))

    def test_initial_same_day_stock_terminal_still_falls_through(self):
        owner, _ = self.initial_run_before_reuse()
        when = self.sessions[-1]
        self.lineage["corporate_actions"][0]["effective_date"] = when.isoformat()
        self.assertIsNone(self.assess([replace(owner, end_date_exclusive=when)]))

    def initial_cash_terminal(self):
        owner, later = self.initial_run_before_reuse()
        event = self.lineage["corporate_actions"][0]
        event.update(action_type="cash_merger", effective_date=self.sessions[-1].isoformat(), rate="24")
        event.pop("acquirer_symbol")
        event.pop("acquirer_cusip")
        owner = replace(
            owner,
            end_date_exclusive=self.sessions[-1],
            applied_rules=(
                AppliedOwnershipIntervalRule(3, "cash_merger_acquiree_terminal_boundary", (event["id"],)),
            ),
        )
        return owner, later

    def test_cash_closing_day_trades_belong_to_departing_episode_without_changing_interval(self):
        owner, later = self.initial_cash_terminal()
        episode = self.assess([owner])
        self.assertEqual((self.sessions[0], self.sessions[-1]), (episode.first, episode.last))
        self.assertFalse(owner.contains(episode.last))
        result = correlate_download_rows(
            self.work[0][0].rows, self.work[0][1], self.lineage, [owner], terminal_episode=episode
        )
        self.assertEqual([5, len(later)], [item["row_count"] for item in result["correlated"]])
        self.assertEqual([], result["uncorrelated"])
        self.assertEqual(
            [["terminal"], ["current-lookup"]],
            [item["security_identity"]["member_alpaca_asset_ids"] for item in result["correlated"]],
        )
        self.assertEqual(4, result["correlated"][0]["correlation_rule"]["order"])
        self.assertNotIn("terminal_action_inside_later_reuse_gap", result["correlated"][0]["warnings"])

    def test_cash_terminal_flat_but_traded_final_session_is_not_synthetic(self):
        owner, _ = self.initial_cash_terminal()
        for download, _, _ in self.work:
            download.rows[4].update(o="24", h="24", l="24", c="24")
        self.assertEqual(self.sessions[-1], self.assess([owner]).last)

    def test_untraded_cash_boundary_keeps_supported_prefix_and_leaves_last_row_unresolved(self):
        owner, later = self.initial_cash_terminal()
        for key in ("v", "n"):
            with self.subTest(missing=key):
                self.work[-1][0].rows[4][key] = "0"
                episode = self.assess([owner])
                self.assertEqual(self.sessions[-2], episode.last)
                result = correlate_download_rows(
                    self.work[0][0].rows, self.work[0][1], self.lineage, [owner], terminal_episode=episode
                )
                self.assertEqual([4, len(later)], [item["row_count"] for item in result["correlated"]])
                self.assertEqual([1], [item["row_count"] for item in result["uncorrelated"]])
                self.work[-1][0].rows[4][key] = "100"

    def test_cash_terminal_does_not_reach_past_event_or_accept_unlinked_invalid_action(self):
        owner, _ = self.initial_cash_terminal()
        event = self.lineage["corporate_actions"][0]
        self.assertIsNone(self.assess([replace(owner, applied_rules=())]))
        for value in ("0", "-1", "NaN", "Infinity", None):
            with self.subTest(rate=value):
                event["rate"] = value
                self.assertIsNone(self.assess([owner]))
        event["rate"] = "24"
        event["effective_date"] = self.sessions[-2].isoformat()
        self.assertIsNone(self.assess([replace(owner, end_date_exclusive=self.sessions[-2])]))

    def test_cash_terminal_competing_stock_transition_or_owner_still_vetoes(self):
        owner, _ = self.initial_cash_terminal()
        competing = replace(
            owner,
            asset_id="current-lookup",
            cusip="other",
            start_date_inclusive=self.sessions[-1],
            end_date_exclusive=None,
        )
        self.assertIsNone(self.assess([owner, competing]))
        self.lineage["corporate_actions"].append(
            {**self.lineage["corporate_actions"][0], "id": "stock-exchange", "action_type": "stock_merger"}
        )
        self.assertIsNone(self.assess([owner]))

    def test_repeated_cash_evidence_does_not_create_competing_owners(self):
        owner, _ = self.initial_cash_terminal()
        next_day = self.sessions[-1] + timedelta(days=1)
        self.lineage["corporate_actions"].append(
            {**self.lineage["corporate_actions"][0], "id": "settlement", "effective_date": next_day.isoformat()}
        )
        intervals = [owner, replace(owner, end_date_exclusive=next_day)]
        episode = self.assess(intervals)
        self.assertEqual(self.sessions[-1], episode.last)
        self.assertFalse(episode.terminated_during_reuse_gap)
        for download, _, _ in self.work:
            download.rows[4].update(v="0", n="0")
        # Existing next-day terminal evidence still covers the full prefix; a
        # second same-owner observation is not a new ownership ambiguity.
        self.assertEqual(self.sessions[-1], self.assess(intervals).last)

    def test_unknown_competing_cash_issue_cannot_evade_same_day_conflict_guard(self):
        owner, _ = self.initial_cash_terminal()
        self.lineage["corporate_actions"].append(
            {
                **self.lineage["corporate_actions"][0],
                "id": "unknown-issue",
                "acquiree_cusip": "333333333",
            }
        )
        self.assertIsNone(self.assess([owner]))

    def test_cash_terminal_preserves_extra_occupant_later_birth_and_missing_view_guards(self):
        owner, _ = self.initial_cash_terminal()
        self.assertIsNone(self.assess([replace(owner, start_date_inclusive=self.sessions[2])]))
        self.lineage["identities"].append({"id": "third", "symbol": "OLD"})
        self.assertIsNone(self.assess([owner]))
        self.lineage["identities"].pop()
        self.work.pop()
        self.assertIsNone(self.assess([owner]))

    def test_initial_run_rejects_later_owner_birth_extra_occupant_and_missing_endpoint(self):
        owner, _ = self.initial_run_before_reuse()
        self.assertIsNone(self.assess([replace(owner, start_date_inclusive=self.sessions[2])]))
        self.assertIsNone(self.assess([]))
        self.lineage["identities"].append({"id": "earlier", "symbol": "OLD"})
        self.assertIsNone(self.assess([owner]))

    def test_initial_run_rejects_incomplete_grid_or_uninformative_prices(self):
        owner, _ = self.initial_run_before_reuse()
        saved = copy.deepcopy(self.work)
        for download, _, _ in self.work:
            for row in download.rows[:5]:
                row.update(o="10", h="10", l="10", c="10", v="0", n="0")
        self.assertIsNone(self.assess([owner]))
        self.work = saved
        self.work = [
            (SimpleNamespace(rows=download.rows[:2] + download.rows[3:]), source, sessions[:2] + sessions[3:])
            for download, source, sessions in self.work
        ]
        self.assertIsNone(self.assess([owner]))

    def test_delayed_terminal_outside_gap_extra_owner_or_later_birth_does_not_decide(self):
        self.add_current_reuse_suffix()
        event = self.lineage["corporate_actions"][0]
        event["effective_date"] = "2024-01-03"
        self.assertIsNone(self.assess())
        event["effective_date"] = "2023-09-01"
        self.lineage["input_symbol_occupants"] = [{"symbol": "OLD", "asset_id": "third"}]
        self.assertIsNone(self.assess())
        del self.lineage["input_symbol_occupants"]
        interval = TickerOwnershipInterval("OLD", "111111111", "terminal", date(2023, 8, 1), None, ())
        self.assertIsNone(self.assess([interval]))
        self.work[-1][0].rows.pop()
        download, source, sessions = self.work[-1]
        self.work[-1] = (download, source, sessions[:-1])
        self.assertIsNone(self.assess())

    def named_departure_alias(self):
        self.add_current_reuse_suffix()
        self.lineage["asset_lookups"][0]["asset"]["symbol"] = "RENAMED"
        self.lineage["identities"][1]["symbol"] = "RENAMED"
        self.lineage["corporate_actions"][0] = {
            "id": "rename",
            "action_type": "name_change",
            "old_symbol": "OLD",
            "old_cusip": "111111111",
            "new_symbol": "RENAMED",
            "new_cusip": "111111111",
            "process_date": "2023-09-01",
        }
        self.lineage["symbol_occupancy_evidence"] = [
            {
                "symbol": "OLD",
                "cusip": "111111111",
                "asset_id": "terminal",
                "evidence": [{"action_id": "rename", "role": "old", "action_type": "name_change"}],
            }
        ]

    def test_dated_same_uuid_alias_accepts_current_spelling_without_rewriting_history(self):
        self.named_departure_alias()
        episode = self.assess()
        self.assertEqual("terminal", episode.asset_id)
        self.assertEqual(self.sessions[102], episode.first)
        self.assertEqual(self.sessions[-1], episode.last)

    def test_missing_named_endpoint_wrong_occupancy_or_different_uuid_cannot_use_alias_exception(self):
        self.named_departure_alias()
        action = self.lineage["corporate_actions"][0]
        for change in ({"new_symbol": "OTHER"}, {"new_cusip": "unknown"}, {"id": "different-action"}):
            saved = copy.deepcopy(action)
            action.update(change)
            self.assertIsNone(self.assess())
            action.clear()
            action.update(saved)
        self.lineage["asset_lookups"].append(
            {
                **copy.deepcopy(self.lineage["asset_lookups"][0]),
                "lookup": "222222222",
                "asset": {"id": "different", "symbol": "RENAMED", "class": "us_equity"},
            }
        )
        action["new_cusip"] = "222222222"
        self.assertIsNone(self.assess())

    def test_adjustment_step_before_shared_inert_suffix_does_not_defeat_boundary(self):
        sessions = tuple(
            date.fromisoformat(s)
            for s in expected_market_session_coverage("2023-01-03", "2023-12-31").session_dates[:205]
        )
        self.lineage["corporate_actions"][0]["effective_date"] = sessions[-1].isoformat()
        self.work = []
        for adjustment in ALPACA_ADJUSTMENT_MODES:
            rows = [
                {
                    **self.rows[0],
                    "t": session.isoformat() + "T12:00:00Z",
                    **{key: str(10 if 2 <= index < 202 else 20 + index) for key in ("o", "h", "l", "c", "vw")},
                    "v": "0" if 2 <= index < 202 else "100",
                    "n": "0" if 2 <= index < 202 else "10",
                }
                for index, session in enumerate(sessions)
            ]
            if adjustment == "dividend":
                for row in rows[2:21]:
                    row.update(o="9", h="9", l="9", c="9")
            self.work.append((SimpleNamespace(rows=rows), {"adjustment": adjustment}, sessions))
        episode = self.assess()
        self.assertEqual(sessions[202], episode.first)
        self.assertEqual(sessions[-1], episode.last)

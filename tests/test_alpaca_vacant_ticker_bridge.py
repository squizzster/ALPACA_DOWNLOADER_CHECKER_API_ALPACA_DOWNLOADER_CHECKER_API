"""A proven ticker vacancy excludes padding without assigning its rows."""

import unittest
from datetime import date, timedelta
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.terminal_episode import (
    VACANT_TICKER_BRIDGE_REASON,
    assess_vacant_ticker_bridges,
)
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests.test_alpaca_end_of_day_security_correlation import (
    add_name_change_edge,
    correlation_source,
    occupancy,
)


class VacantTickerBridgeTests(unittest.TestCase):
    def setUp(self):
        self.sessions = tuple(
            date.fromisoformat(value)
            for value in expected_market_session_coverage("2024-01-02", "2024-10-01").session_dates
        )
        self.departure = self.sessions[1]
        self.arrival = self.sessions[-1]
        self.gap = self.sessions[1:-1]
        self.dividend_dates = (self.gap[50], self.gap[100])
        self.lineage = {
            "query": {
                "input_symbol": "OLD",
                "corporate_actions_start": "2023-01-01",
                "corporate_actions_end": "2025-01-01",
                "corporate_actions_data_quality": "complete",
            },
            "corporate_actions": [],
            "asset_lookups": [],
            "identities": [],
            "symbol_occupancy_evidence": [],
            "input_symbol_occupants": [],
            "unresolved_cusips": [],
        }
        add_name_change_edge(
            self.lineage,
            action_id="departure",
            process_date=self.departure.isoformat(),
            predecessor=("old-id", "OLD", "old-cusip", "Old issue"),
            successor=("old-id", "LEFT", "old-cusip", "Old issue"),
        )
        add_name_change_edge(
            self.lineage,
            action_id="arrival",
            process_date=self.arrival.isoformat(),
            predecessor=("new-id", "RIGHT", "new-cusip", "New issue"),
            successor=("new-id", "OLD", "new-cusip", "New issue"),
        )
        next(item for item in self.lineage["identities"] if item["id"] == "old-id")["symbol"] = "LEFT"
        next(item for item in self.lineage["identities"] if item["id"] == "new-id")["symbol"] = "OLD"
        for number, when in enumerate(self.dividend_dates, 1):
            action_id = f"dividend-{number}"
            process = (when + timedelta(days=8)).isoformat()
            self.lineage["corporate_actions"].append(
                {
                    "id": action_id,
                    "action_type": "cash_dividend",
                    "symbol": "OLD",
                    "cusip": "new-cusip",
                    "ex_date": when.isoformat(),
                    "process_date": process,
                    "rate": "0.11",
                }
            )
            self.lineage["symbol_occupancy_evidence"].append(
                occupancy(
                    "OLD",
                    "new-cusip",
                    "new-id",
                    action_type="cash_dividend",
                    action_id=action_id,
                    process_date=process,
                    role="symbol",
                )
            )
        self.work = []
        for adjustment in ADJUSTMENTS:
            rows = []
            for position, session in enumerate(self.sessions):
                inert = 0 < position < len(self.sessions) - 1
                price = 10 if position < len(self.sessions) - 1 else 30
                if inert and ("dividend" in adjustment or adjustment == "all"):
                    price += sum(session >= when for when in self.dividend_dates)
                rows.append(
                    {
                        "t": session.isoformat() + "T12:00:00Z",
                        "o": str(price),
                        "h": str(price),
                        "l": str(price),
                        "c": str(price),
                        "v": "0" if inert else "100",
                        "n": "0" if inert else "10",
                        "vw": "0" if inert else str(price),
                    }
                )
            source = {
                **correlation_source("OLD", asof=self.arrival.isoformat()),
                "adjustment": adjustment,
                "data_file": adjustment + ".csv.gz",
                "request_to": (self.arrival + timedelta(days=1)).isoformat(),
            }
            self.work.append((SimpleNamespace(rows=rows), source, self.sessions))
        self.donor_rows = {}
        self.reports = {}
        for symbol, asset_id, cusip in (
            ("LEFT", "old-id", "old-cusip"),
            ("RIGHT", "new-id", "new-cusip"),
        ):
            descriptors = {}
            for adjustment in ADJUSTMENTS:
                filename = f"{symbol}-{adjustment}.csv.gz"
                descriptors[filename] = {
                    "adjustment": adjustment,
                    "data_file": filename,
                    "download_sha256": f"{symbol}-{adjustment}-generation",
                }
            filename = f"{symbol}-raw.csv.gz"
            rows = [
                {
                    "t": session.isoformat() + "T12:00:00Z",
                    "o": str(20 + position),
                    "h": str(20 + position),
                    "l": str(20 + position),
                    "c": str(20 + position),
                    "v": "100",
                    "n": "10",
                    "vw": str(20 + position),
                }
                for position, session in enumerate(self.gap)
            ]
            self.donor_rows[(symbol, filename)] = (
                rows,
                descriptors[filename]["download_sha256"],
            )
            self.reports[symbol] = SimpleNamespace(
                sources_by_filename=descriptors,
                correlated={
                    "segments": [
                        {
                            "data_file": filename,
                            "adjustment": "raw",
                            "row_start": 1,
                            "row_end": len(rows),
                            "historical_symbol": symbol,
                            "canonical_security": {
                                "alpaca_asset_id": asset_id,
                                "cusip": cusip,
                            },
                            "feed": "sip",
                            "timeframe": "1Day",
                            "download_sha256": descriptors[filename]["download_sha256"],
                        }
                    ]
                },
            )
        self.extra_issues = []

    def assess(self):
        edges, edge_issues = build_uuid_lineage_edges(self.lineage)
        intervals, interval_issues = build_ticker_ownership_intervals(
            self.lineage,
            uuid_lineage_edges=edges,
        )
        return assess_vacant_ticker_bridges(
            "OLD",
            self.lineage,
            intervals,
            edges,
            self.work,
            evidence_issues=[*edge_issues, *interval_issues, *self.extra_issues],
            correlation_loader=self.reports.__getitem__,
            source_rows_loader=lambda symbol, filename: self.donor_rows[(symbol, filename)],
        ), intervals

    def test_complete_vacancy_is_one_non_observation_not_lineage(self):
        bridges, intervals = self.assess()
        self.assertEqual(1, len(bridges))
        self.assertEqual((self.gap[0], self.gap[-1]), (bridges[0].first, bridges[0].last))
        result = correlate_download_rows(
            self.work[0][0].rows,
            self.work[0][1],
            self.lineage,
            intervals,
            vacant_ticker_bridges=bridges,
        )
        certified = [
            segment
            for segment in result["uncorrelated"]
            if segment["reason_codes"] == [VACANT_TICKER_BRIDGE_REASON]
        ]
        self.assertEqual([len(self.gap)], [segment["row_count"] for segment in certified])
        self.assertEqual([], certified[0]["candidate_securities"])

    def test_dividend_adjustment_dates_are_not_ownership_and_unknown_changes_veto(self):
        self.assertEqual(1, len(self.assess()[0]))
        all_view = next(item for item in self.work if item[1]["adjustment"] == "all")
        position = 75
        for row in all_view[0].rows[position:]:
            for field in ("o", "h", "l", "c"):
                row[field] = str(int(row[field]) + 1)
        self.assertEqual((), self.assess()[0])

    def test_incomplete_or_informative_bridge_and_bad_vwap_veto(self):
        for mutation in (
            lambda: self.work.pop(),
            lambda: self.work[0][0].rows[80].update(v="1", n="1", vw="10"),
            lambda: self.work[0][0].rows[80].update(vw="10"),
            lambda: self.lineage["query"].update(corporate_actions_data_quality="partial"),
        ):
            self.setUp()
            mutation()
            self.assertEqual((), self.assess()[0])

    def test_third_occupant_structural_action_or_evidence_issue_veto(self):
        for mutation in (
            lambda: self.lineage["unresolved_cusips"].append("unresolved-cusip"),
            lambda: self.lineage["symbol_occupancy_evidence"].append(
                occupancy(
                    "OLD",
                    "third-cusip",
                    "third-id",
                    action_type="cash_dividend",
                    action_id="third",
                    process_date=self.gap[75].isoformat(),
                    role="symbol",
                )
            ),
            lambda: self.lineage["corporate_actions"].append(
                {
                    "id": "structural",
                    "action_type": "stock_merger",
                    "acquiree_symbol": "OLD",
                    "acquiree_cusip": "old-cusip",
                    "acquirer_symbol": "OTHER",
                    "acquirer_cusip": "other-cusip",
                    "effective_date": self.gap[75].isoformat(),
                    "process_date": self.gap[75].isoformat(),
                }
            ),
            lambda: self.extra_issues.append({"reason_code": "conflict"}),
        ):
            self.setUp()
            mutation()
            self.assertEqual((), self.assess()[0])

    def test_each_independent_owner_must_trade_and_own_every_gap_session(self):
        for mutation in (
            lambda: self.donor_rows[("LEFT", "LEFT-raw.csv.gz")][0].pop(50),
            lambda: self.donor_rows[("RIGHT", "RIGHT-raw.csv.gz")][0][50].update(v="0", n="0", vw="0"),
            lambda: (
                self.reports["RIGHT"]
                .correlated["segments"][0]["canonical_security"]
                .update(alpaca_asset_id="wrong")
            ),
        ):
            self.setUp()
            mutation()
            self.assertEqual((), self.assess()[0])


class ActionAnchoredVacantTickerBridgeTests(unittest.TestCase):
    def setUp(self):
        self.sessions = tuple(
            date.fromisoformat(value)
            for value in expected_market_session_coverage("2024-01-02", "2024-10-01").session_dates
        )
        self.departure = self.sessions[1]
        self.arrival = self.sessions[-2]
        self.gap = self.sessions[1:-2]
        self.old_id = "departed-id"
        self.current_id = "current-id"
        self.old_cusip = "111111111"
        self.current_cusip = "222222222"
        self.lineage = {
            "query": {
                "input_symbol": "OLD",
                "corporate_actions_start": "2023-01-01",
                "corporate_actions_end": "2025-01-01",
                "corporate_actions_data_quality": "complete",
            },
            "corporate_actions": [],
            "asset_lookups": [],
            "identities": [],
            "symbol_occupancy_evidence": [],
            "input_symbol_occupants": [],
            "unresolved_cusips": [],
        }
        add_name_change_edge(
            self.lineage,
            action_id="departure",
            process_date=self.departure.isoformat(),
            predecessor=(self.old_id, "OLD", self.old_cusip, "Departed issue"),
            successor=(self.old_id, "LEFT", self.old_cusip, "Departed issue"),
        )
        current = {
            "id": self.current_id,
            "class": "us_equity",
            "exchange": "NASDAQ",
            "symbol": "OLD",
            "name": "Current issue",
            "status": "active",
            "tradable": True,
            "lookup_symbols": ["OLD"],
            "lookup_cusips": [self.current_cusip],
        }
        self.lineage["identities"].append(current)
        self.lineage["asset_lookups"].extend(
            {
                "lookup_kind": kind,
                "lookup": value,
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": current,
            }
            for kind, value in (("symbol", "OLD"), ("cusip", self.current_cusip))
        )
        self.lineage["corporate_actions"].append(
            {
                "id": "component-arrival",
                "action_type": "unit_split",
                "old_symbol": "UNIT",
                "old_cusip": "333333333",
                "old_rate": 1,
                "new_symbol": "COMMON",
                "new_cusip": "444444444",
                "new_rate": 1,
                "alternate_symbol": "OLD",
                "alternate_cusip": self.current_cusip,
                "alternate_rate": 0.5,
                "effective_date": self.arrival.isoformat(),
                "process_date": self.arrival.isoformat(),
            }
        )
        self.lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "OLD",
                self.current_cusip,
                self.current_id,
                action_type="unit_split",
                action_id="component-arrival",
                process_date=self.arrival.isoformat(),
                role="alternate",
            )
        )
        self.work = []
        for adjustment in ADJUSTMENTS:
            rows = []
            for position, session in enumerate(self.sessions):
                inert = 0 < position < len(self.sessions) - 2
                price = 10 if position < len(self.sessions) - 2 else 30 + position
                rows.append(
                    {
                        "t": session.isoformat() + "T12:00:00Z",
                        "o": str(price),
                        "h": str(price),
                        "l": str(price),
                        "c": str(price),
                        "v": "0" if inert else "100",
                        "n": "0" if inert else "10",
                        "vw": "0" if inert else str(price),
                    }
                )
            source = {
                **correlation_source("OLD", asof=(self.sessions[-1] + timedelta(days=1)).isoformat()),
                "adjustment": adjustment,
                "data_file": adjustment + ".csv.gz",
                "request_to": (self.sessions[-1] + timedelta(days=1)).isoformat(),
            }
            self.work.append((SimpleNamespace(rows=rows), source, self.sessions))

    def assess(self, *, enabled=True):
        edges, edge_issues = build_uuid_lineage_edges(self.lineage)
        intervals, interval_issues = build_ticker_ownership_intervals(
            self.lineage,
            uuid_lineage_edges=edges,
            include_action_anchored_current_suffix=True,
            allow_closed_prior_action_anchored_suffix=True,
        )
        bridges = assess_vacant_ticker_bridges(
            "OLD",
            self.lineage,
            intervals,
            edges,
            self.work,
            evidence_issues=[*edge_issues, *interval_issues],
            correlation_loader=lambda _symbol: None,
            source_rows_loader=lambda _symbol, _filename: ([], "unused"),
            include_action_anchored_arrivals=enabled,
        )
        return bridges, intervals

    def test_closed_departure_and_action_anchored_arrival_exclude_only_the_inert_gap(self):
        bridges, intervals = self.assess()

        self.assertEqual(1, len(bridges))
        self.assertEqual((self.gap[0], self.gap[-1]), (bridges[0].first, bridges[0].last))
        self.assertEqual(
            [
                (self.old_id, None, self.departure),
                (self.current_id, self.arrival, None),
            ],
            [
                (item.asset_id, item.start_date_inclusive, item.end_date_exclusive)
                for item in intervals
                if item.historical_symbol == "OLD"
            ],
        )
        raw_download, raw_source, _ = next(item for item in self.work if item[1]["adjustment"] == "raw")
        result = correlate_download_rows(
            raw_download.rows,
            raw_source,
            self.lineage,
            intervals,
            vacant_ticker_bridges=bridges,
        )
        excluded = [
            item for item in result["uncorrelated"] if item["reason_codes"] == [VACANT_TICKER_BRIDGE_REASON]
        ]
        self.assertEqual([len(self.gap)], [item["row_count"] for item in excluded])
        self.assertEqual([], excluded[0]["candidate_securities"])

    def test_action_anchored_vacancy_retains_material_vetoes(self):
        mutations = (
            lambda: self.lineage["query"].update(corporate_actions_data_quality="partial"),
            lambda: self.work[0][0].rows[50].update(v="1", n="1", vw="10"),
            lambda: self.work.pop(),
            lambda: self.lineage["symbol_occupancy_evidence"].append(
                occupancy(
                    "OLD",
                    "555555555",
                    "third-id",
                    action_type="cash_dividend",
                    action_id="third-action",
                    process_date=self.gap[50].isoformat(),
                    role="symbol",
                )
            ),
            lambda: (
                self.lineage["unresolved_cusips"].append("unresolved-gap-cusip"),
                self.lineage["symbol_occupancy_evidence"].append(
                    occupancy(
                        "OLD",
                        "unresolved-gap-cusip",
                        None,
                        action_type="unit_split",
                        action_id="unresolved-gap-action",
                        process_date=self.gap[50].isoformat(),
                        role="alternate",
                    )
                ),
            ),
            lambda: self.lineage["corporate_actions"].append(
                {
                    "id": "competing-action",
                    "action_type": "reverse_split",
                    "symbol": "OLD",
                    "old_cusip": self.old_cusip,
                    "new_cusip": "666666666",
                    "effective_date": self.gap[50].isoformat(),
                    "process_date": self.gap[50].isoformat(),
                }
            ),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.setUp()
                mutation()
                self.assertEqual((), self.assess()[0])
        self.setUp()
        self.assertEqual((), self.assess(enabled=False)[0])

    def test_dated_unresolved_cusip_before_the_gap_does_not_veto_the_bounded_certificate(self):
        self.lineage["unresolved_cusips"].append("legacy-unresolved-cusip")
        self.lineage["symbol_occupancy_evidence"].append(
            occupancy(
                "OLD",
                "legacy-unresolved-cusip",
                None,
                action_type="unit_split",
                action_id="legacy-unresolved-action",
                process_date=(self.departure - timedelta(days=1)).isoformat(),
                role="alternate",
            )
        )

        self.assertEqual(1, len(self.assess()[0]))


if __name__ == "__main__":
    unittest.main()

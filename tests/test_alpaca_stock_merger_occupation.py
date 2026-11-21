"""Exact cross-label stock merger ownership without a cross-security join."""

from copy import deepcopy
from datetime import date
from unittest import TestCase

from libs.alpaca_end_of_day_security_correlation import ownership_interval_rules, stock_merger_occupation
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges


def evidence(symbol, *, settlement=True):
    old = {"symbol": "RETIRED", "id": "old-asset", "class": "us_equity", "status": "inactive", "tradable": False}
    new = {
        "symbol": symbol,
        "id": "new-asset",
        "class": "us_equity",
        "status": "active",
        "tradable": True,
        "exchange": "NASDAQ",
    }
    escrow = {**old, "symbol": "ESCROW", "id": "escrow-asset"}
    rename = {
        "id": "historical-rename",
        "action_type": "name_change",
        "old_symbol": "FORMER",
        "new_symbol": symbol,
        "old_cusip": "111111111",
        "new_cusip": "111111111",
        "process_date": "2026-01-02",
    }
    merger = {
        "id": "terminal-merger",
        "action_type": "stock_merger",
        "acquiree_symbol": old["symbol"],
        "acquirer_symbol": symbol,
        "acquiree_cusip": "111111111",
        "acquirer_cusip": "222222222",
        "acquiree_rate": 1,
        "acquirer_rate": "0.10",
        "effective_date": "2026-03-02",
        "payable_date": "2026-03-02",
        "process_date": "2026-03-02",
    }
    later = {
        **merger,
        "id": "settlement",
        "acquiree_symbol": escrow["symbol"],
        "acquiree_cusip": "333333333",
        "effective_date": "2026-05-04",
        "payable_date": "2026-05-04",
        "process_date": "2026-05-04",
    }

    def lookup(kind, value, asset):
        return {
            "lookup_kind": kind,
            "lookup": value,
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": asset,
        }

    def occupancy(label, cusip, asset, action):
        return {
            "symbol": label,
            "cusip": cusip,
            "asset_id": asset["id"],
            "evidence": [
                {
                    "symbol": label,
                    "cusip": cusip,
                    "action_type": action["action_type"],
                    "action_id": action["id"],
                    "process_date": action["process_date"],
                }
            ],
        }

    return {
        "query": {
            "input_symbol": symbol,
            "corporate_actions_start": "2025-01-01",
            "corporate_actions_end": "2026-10-01",
            "corporate_actions_data_quality": "complete",
        },
        "identities": [old, new, escrow],
        "asset_lookups": [
            lookup("symbol", symbol, new),
            lookup("cusip", "111111111", old),
            lookup("cusip", "222222222", new),
            lookup("cusip", "333333333", escrow),
        ],
        "corporate_actions": [rename, merger, *([later] if settlement else [])],
        "symbol_occupancy_evidence": [
            occupancy("FORMER", "111111111", old, rename),
            occupancy(symbol, "111111111", old, rename),
            occupancy(old["symbol"], "111111111", old, merger),
            occupancy(symbol, "222222222", new, merger),
            *([occupancy(escrow["symbol"], "333333333", escrow, later)] if settlement else []),
        ],
    }


class StockMergerOccupationTests(TestCase):
    @staticmethod
    def intervals(lineage, *, enabled):
        edges, _ = build_uuid_lineage_edges(lineage)
        rows, _ = ownership_interval_rules.build_ticker_ownership_intervals(
            lineage, uuid_lineage_edges=edges, allow_cross_label_stock_merger_occupation=enabled
        )
        return rows, edges

    def test_active_issue_is_assigned_its_dated_own_label_independently(self):
        for symbol in ("ACTIVE", "LATER"):
            with self.subTest(symbol=symbol):
                source = evidence(symbol)
                old, old_edges = self.intervals(source, enabled=False)
                new, edges = self.intervals(source, enabled=True)
                self.assertEqual(edges, old_edges)
                self.assertEqual(new[0], old[0])
                self.assertEqual([e.predecessor_asset_id for e in edges], ["old-asset"])
                self.assertEqual(
                    [
                        (
                            row.asset_id,
                            row.start_date_inclusive,
                            row.end_date_exclusive,
                            row.applied_rules[-1].name,
                            row.applied_rules[-1].supporting_action_ids,
                        )
                        for row in new
                        if row.historical_symbol == symbol
                    ],
                    [
                        (
                            "old-asset",
                            date(2026, 1, 2),
                            date(2026, 3, 2),
                            stock_merger_occupation.NAME,
                            ("terminal-merger",),
                        ),
                        (
                            "new-asset",
                            date(2026, 3, 2),
                            None,
                            stock_merger_occupation.NAME,
                            ("terminal-merger",),
                        ),
                    ],
                )

    def test_conflicting_or_irrelevant_anchors_preserve_previous_intervals(self):
        source = evidence("ACTIVE", settlement=False)
        cases = {}
        for label in ("dates disagree", "retiree still active", "different current lookup", "rival remains"):
            cases[label] = deepcopy(source)
        cases["dates disagree"]["corporate_actions"][-1]["process_date"] = "2026-03-03"
        cases["retiree still active"]["asset_lookups"][1]["asset"]["tradable"] = True
        cases["different current lookup"]["asset_lookups"][0]["asset"] = {
            **source["asset_lookups"][0]["asset"],
            "id": "rival",
        }
        cases["rival remains"]["symbol_occupancy_evidence"][1]["evidence"].append(
            {"process_date": "2026-06-01", "action_type": "cash_dividend"}
        )
        repeated = deepcopy(source)
        repeated["corporate_actions"].append(
            {
                **repeated["corporate_actions"][-1],
                "id": "conflicting-event",
                "effective_date": "2026-03-03",
                "payable_date": "2026-03-03",
                "process_date": "2026-03-03",
            }
        )
        cases["two distinct purported cutovers"] = repeated
        for label, lineage in cases.items():
            with self.subTest(label=label):
                self.assertEqual(self.intervals(lineage, enabled=True), self.intervals(lineage, enabled=False))

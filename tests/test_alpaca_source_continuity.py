import copy
import unittest

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge, correlation_source


def evidence(sibling_date="2024-01-04"):
    result = {"corporate_actions": [], "identities": [], "symbol_occupancy_evidence": []}
    current = ("current", "HOLD", "222222222", "Continuing Corporation Class A")
    for name, when, old in (
        ("continuation", "2024-01-04", ("prior", "HOLD", "111111111", current[3])),
        ("convergence", sibling_date, ("sibling", "OTHER", "333333333", "Different PLC ADS")),
    ):
        add_name_change_edge(result, action_id=name, process_date=when, predecessor=old, successor=current)
    result["asset_lookups"] = [
        {
            "lookup_kind": "cusip",
            "lookup": item["lookup_cusips"][0],
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": copy.deepcopy(item),
        }
        for item in result["identities"]
    ]
    return result


def resolve(lineage):
    intervals, _ = build_ticker_ownership_intervals(lineage)
    return correlate_download_rows(
        [{"t": value + "T12:00:00Z"} for value in ("2024-01-02", "2024-01-03", "2024-01-04")],
        correlation_source("HOLD", asof="2024-02-01"),
        lineage,
        intervals,
    )


class SourceContinuityTests(unittest.TestCase):
    def test_matching_complete_source_chain_survives_simultaneous_or_staggered_convergence(self):
        for when in ("2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10"):
            with self.subTest(when=when):
                result = resolve(evidence(when))
                self.assertEqual([], result["uncorrelated"])
                self.assertEqual(
                    ["prior", "current"],
                    [s["canonical_security"]["alpaca_asset_id"] for s in result["correlated"]],
                )
                self.assertEqual([2, 1], [s["row_count"] for s in result["correlated"]])
                self.assertTrue(
                    all(
                        s["correlation_rule"]["name"] == "asof_selected_uuid_lineage_interval"
                        for s in result["correlated"]
                    )
                )
                self.assertEqual(
                    ["current", "prior", "sibling"],
                    result["correlated"][0]["security_identity"]["member_alpaca_asset_ids"],
                )

    def test_equally_matching_branch_or_disagreeing_chain_remains_ambiguous(self):
        for identity, name in (
            ("sibling", "Continuing Corporation Class A"),
            ("prior", "Former unrelated company"),
            ("prior", "Continuing Corporation Class B"),
        ):
            lineage = evidence()
            next(item for item in lineage["asset_lookups"] if item["asset"]["id"] == identity)["asset"][
                "name"
            ] = name
            self.assertEqual(2, resolve(lineage)["uncorrelated"][0]["row_count"])

    def test_exact_complete_lookups_are_required(self):
        for change in ("missing", "duplicate", "failed", "wrong UUID", "OTC"):
            with self.subTest(change=change):
                lineage = evidence()
                lookup = lineage["asset_lookups"][0]
                if change == "missing":
                    lineage["asset_lookups"].pop(0)
                elif change == "duplicate":
                    lineage["asset_lookups"].append(copy.deepcopy(lookup))
                elif change == "failed":
                    lookup["http_status"] = 404
                elif change == "wrong UUID":
                    lookup["asset"]["id"] = "unrelated"
                else:
                    lookup["asset"]["exchange"] = "OTC"
                self.assertEqual(2, resolve(lineage)["uncorrelated"][0]["row_count"])

    def test_preexisting_successor_under_foreign_label_vetoes_backfilled_names(self):
        lineage = evidence()
        add_name_change_edge(
            lineage,
            action_id="older-history",
            process_date="2023-01-03",
            predecessor=("ancestor", "EARLIER", "444444444", "Continuing Corporation Class A"),
            successor=("current", "FOREIGN", "222222222", "Continuing Corporation Class A"),
        )
        result = resolve(lineage)
        self.assertEqual("FOREIGN", result["correlated"][0]["historical_symbol"])
        self.assertEqual("current", result["correlated"][0]["canonical_security"]["alpaca_asset_id"])

    def test_undated_extra_source_occupant_keeps_ambiguity(self):
        lineage = evidence()
        lineage["symbol_occupancy_evidence"].append(
            {"symbol": "HOLD", "cusip": "555555555", "asset_id": "extra", "evidence": []}
        )
        self.assertEqual(2, resolve(lineage)["uncorrelated"][0]["row_count"])

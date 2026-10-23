"""A later ticker label must not erase an authenticated earlier split."""

import copy
import unittest

from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import correlate_download_rows
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import build_uuid_lineage_edges
from tests.test_alpaca_end_of_day_security_correlation import (
    add_name_change_edge,
    certified_row_inputs,
    correlation_source,
    same_symbol_split_lineage,
)


def renamed_endpoint_lineage(index=1):
    lineage = same_symbol_split_lineage()
    endpoint = lineage["asset_lookups"][index]["asset"]
    cusip = lineage["asset_lookups"][index]["lookup"]
    add_name_change_edge(
        lineage,
        action_id="later-rename",
        process_date="2026-10-02",
        predecessor=(endpoint["id"], "SPLT", cusip, "Security"),
        successor=(endpoint["id"], "LATER", cusip, "Security"),
    )
    endpoint["symbol"] = "LATER"
    return lineage


def split_edges(lineage):
    return [edge for edge in build_uuid_lineage_edges(lineage)[0] if edge.edge_type == "reverse_split"]


class SplitEndpointLabelTests(unittest.TestCase):
    def test_later_rename_preserves_split_for_either_endpoint_and_action_order(self):
        for index in (0, 1):
            for reverse in (False, True):
                with self.subTest(endpoint=index, reverse=reverse):
                    lineage = renamed_endpoint_lineage(index)
                    if reverse:
                        lineage["corporate_actions"].reverse()
                    edges = split_edges(lineage)
                    self.assertEqual(1, len(edges))
                    self.assertEqual("SPLT", edges[0].predecessor_symbol)
                    self.assertEqual("SPLT", edges[0].successor_symbol)
                    self.assertEqual("old-split-asset-id", edges[0].predecessor_asset_id)
                    self.assertEqual("new-split-asset-id", edges[0].successor_asset_id)

    def test_unexplained_label_and_material_conflicts_do_not_qualify(self):
        mutations = {
            "missing path": lambda value: value["corporate_actions"].pop(),
            "rename before split": lambda value: value["corporate_actions"][1].update(process_date="2026-09-28"),
            "same day": lambda value: value["corporate_actions"][1].update(process_date="2026-09-29"),
            "other current label": lambda value: value["asset_lookups"][1]["asset"].update(symbol="OTHER"),
            "changed CUSIP": lambda value: value["corporate_actions"][1].update(new_cusip="333333333"),
            "changed UUID": lambda value: value["symbol_occupancy_evidence"][1].update(asset_id="other-id"),
            "no accepted occupancy": lambda value: value.update(symbol_occupancy_evidence=[]),
            "wrong ratio": lambda value: value["corporate_actions"][0].update(old_rate=1, new_rate=10),
            "ambiguous exact lookup": lambda value: value["asset_lookups"].append(
                copy.deepcopy(value["asset_lookups"][1])
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                lineage = renamed_endpoint_lineage()
                mutate(lineage)
                self.assertEqual([], split_edges(lineage))

    def test_unique_multihop_name_path_is_supported_but_branch_is_not(self):
        lineage = renamed_endpoint_lineage()
        add_name_change_edge(
            lineage,
            action_id="last-rename",
            process_date="2026-10-05",
            predecessor=("new-split-asset-id", "LATER", "222222222", "Security"),
            successor=("new-split-asset-id", "FINAL", "222222222", "Security"),
        )
        lineage["asset_lookups"][1]["asset"]["symbol"] = "FINAL"
        self.assertEqual(1, len(split_edges(lineage)))
        add_name_change_edge(
            lineage,
            action_id="rival-rename",
            process_date="2026-10-06",
            predecessor=("new-split-asset-id", "SPLT", "222222222", "Security"),
            successor=("new-split-asset-id", "RIVAL", "222222222", "Security"),
        )
        self.assertEqual([], split_edges(lineage))

    def test_cycle_and_out_of_order_path_do_not_qualify(self):
        for target, when in (("SPLT", "2026-10-05"), ("FINAL", "2026-10-01")):
            with self.subTest(target=target):
                lineage = renamed_endpoint_lineage()
                lineage["asset_lookups"][1]["asset"]["symbol"] = "FINAL"
                add_name_change_edge(
                    lineage,
                    action_id="next-rename",
                    process_date=when,
                    predecessor=("new-split-asset-id", "LATER", "222222222", "Security"),
                    successor=("new-split-asset-id", target, "222222222", "Security"),
                )
                self.assertEqual([], split_edges(lineage))

    def test_membership_joins_without_overriding_dated_row_owner(self):
        lineage = renamed_endpoint_lineage()
        intervals, _ = build_ticker_ownership_intervals(lineage)
        result = correlate_download_rows(
            [{"t": "2026-09-30T04:00:00Z"}, {"t": "2026-10-02T04:00:00Z"}],
            correlation_source("LATER", asof="2026-10-06"),
            lineage,
            intervals,
            **certified_row_inputs(
                [{"t": "2026-09-30T04:00:00Z"}, {"t": "2026-10-02T04:00:00Z"}],
                correlation_source("LATER", asof="2026-10-06"),
                lineage,
                intervals,
            ),
        )
        self.assertEqual([], result["uncorrelated"])
        self.assertEqual(["SPLT", "LATER"], [row["historical_symbol"] for row in result["correlated"]])
        for row in result["correlated"]:
            self.assertEqual("new-split-asset-id", row["canonical_security"]["alpaca_asset_id"])
            self.assertEqual(
                ["new-split-asset-id", "old-split-asset-id"],
                row["security_identity"]["member_alpaca_asset_ids"],
            )


if __name__ == "__main__":
    unittest.main()

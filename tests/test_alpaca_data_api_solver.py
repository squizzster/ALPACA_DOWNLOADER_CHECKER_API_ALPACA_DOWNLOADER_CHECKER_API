import copy
import json
import unittest
import urllib.error
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_pricing_download.api_plan import execute_plan
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, DownloadError
from libs.alpaca_pricing_download.http import execute_bar_plan


def need(
    symbols, *, why="range_update", start="2026-10-10", end="2026-10-12", asof="2026-10-15", adjustments=None
):
    return {
        "kind": "bars",
        "why": why,
        "symbols": symbols,
        "asof": asof,
        "adjustments": adjustments or ["all"],
        "ranges": [{"start": start, "end_exclusive": end}],
    }


def plan(*needs, **options):
    return build_plan({"steps": [{"needs": list(needs)}]}, **options)


def bar(day, value="1"):
    return {
        "t": day + "T04:00:00Z",
        "o": Decimal(value),
        "h": Decimal(value),
        "l": Decimal(value),
        "c": Decimal(value),
        "v": 9007199254740993,
        "n": 1,
        "vw": Decimal(value),
    }


class ApiSolverTests(unittest.TestCase):
    def test_large_typed_symbol_needs_share_catalogue_but_cusips_stay_exact(self):
        symbols = [f"S{i}" for i in range(100)]
        typed = {"kind": "asset_lookup", "why": "identity", "lookup_kind": "symbol"}
        result = plan(
            {**typed, "identifiers": symbols[:50]},
            {**typed, "identifiers": symbols[50:]},
            {**typed, "lookup_kind": "cusip", "identifiers": ["S0"]},
        )
        actions = result["steps"][0]["actions"]
        self.assertEqual([a["strategy"] for a in actions], ["asset_catalogue", "asset_lookup"])
        self.assertEqual(actions[0]["request"]["params"], {"asset_class": "us_equity"})
        self.assertEqual(len(actions[0]["deliver"]), 2)
        self.assertTrue(actions[1]["request"]["url"].endswith("/S0"))

    def test_untyped_and_small_symbol_batches_remain_exact_and_steps_stay_separate(self):
        symbols = [f"S{i}" for i in range(100)]
        need = {"kind": "asset_lookup", "why": "identity", "identifiers": symbols}
        self.assertEqual(len(plan(need)["steps"][0]["actions"]), 100)
        small = {**need, "lookup_kind": "symbol", "identifiers": symbols[:50]}
        result = build_plan({"steps": [{"needs": [small]}, {"needs": [small]}]})
        self.assertEqual([len(s["actions"]) for s in result["steps"]], [50, 50])
        with self.assertRaises(ValueError):
            plan({**need, "lookup_kind": "unknown"})

    def test_corporate_action_operating_cap_is_independent_of_bar_cap(self):
        result = plan(
            {
                "kind": "corporate_actions",
                "why": "lineage",
                "symbols": [f"S{i}" for i in range(1001)],
                "start": "2016-01-01",
                "end": "2026-10-15",
            }
        )
        self.assertEqual(
            [len(a["request"]["params"]["symbols"].split(",")) for a in result["steps"][0]["actions"]], [1000, 1]
        )

    def test_catalogue_array_transport_and_seeded_exact_lookup(self):
        import io

        from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient

        client = AlpacaIdentityClient("test", "test")
        catalogue = plan(
            {
                "kind": "asset_lookup",
                "lookup_kind": "symbol",
                "why": "identity",
                "identifiers": [f"S{i}" for i in range(100)],
            }
        )
        response = io.BytesIO(b'[{"id":"one","symbol":"A"}]')
        response.headers = {}
        with patch("urllib.request.urlopen", return_value=response):
            self.assertEqual(client.execute_plan(catalogue)[0][0], [{"id": "one", "symbol": "A"}])
        client.seed_asset_lookup("A", asset={"id": "one", "symbol": "A"}, http_status=200)
        with patch.object(client, "request_json", side_effect=AssertionError("seed must be consumed")):
            values = client.execute_plan(plan({"kind": "asset_lookup", "why": "identity", "identifiers": ["A"]}))
            self.assertEqual(values[0][0]["asset"]["id"], "one")

    def test_mixed_strategy_dispatch_obeys_steps_and_stops_on_failure(self):
        from unittest.mock import Mock

        lookup = {"kind": "asset_lookup", "why": "identity", "identifiers": ["A"]}
        result = build_plan({"steps": [{"needs": [lookup]}, {"needs": [need(["A"])]}]})
        calls = []
        metadata, bars = Mock(), Mock()
        metadata.execute_plan.side_effect = lambda value: calls.append("metadata")
        bars.execute_plan.side_effect = lambda value: calls.append("bars")
        execute_plan(result, metadata=metadata, bars=bars)
        self.assertEqual(calls, ["metadata", "bars"])
        bars.reset_mock()
        metadata.execute_plan.side_effect = DownloadError("failed metadata")
        with self.assertRaises(DownloadError):
            execute_plan(result, metadata=metadata, bars=bars)
        bars.execute_plan.assert_not_called()

    def test_metadata_plan_executes_deduplicated_lookups_then_paginated_actions(self):
        from libs.alpaca_security_lineage.alpaca_http import AlpacaIdentityClient

        lookup = {"kind": "asset_lookup", "why": "identity", "identifiers": ["A", "B", "A"]}
        actions = {
            "kind": "corporate_actions",
            "why": "lineage",
            "symbols": ["A", "B"],
            "start": "2026-10-01",
            "end": "2026-10-14",
        }
        result = build_plan({"steps": [{"needs": [lookup]}, {"needs": [actions]}]})
        client = AlpacaIdentityClient("test", "test")
        responses = [
            {"symbol": "A"},
            None,
            {"corporate_actions": {"cash_dividends": [{"symbol": "A"}]}, "next_page_token": "next"},
            {"corporate_actions": {}},
        ]
        with patch.object(client, "request_json", side_effect=responses) as request:
            values = client.execute_plan(result)
        self.assertEqual(request.call_count, 4)
        self.assertEqual(values[0][1]["http_status"], 404)
        self.assertEqual(values[1][0], [{"symbol": "A", "action_type": "cash_dividend"}])
        self.assertIn("page_token=next", request.call_args_list[-1].args[0])

    def test_two_thousand_tickers_two_days_share_one_action_without_io(self):
        document = {"steps": [{"needs": [need([f"S{i}" for i in range(2000)])]}]}
        before = copy.deepcopy(document)
        with (
            patch.object(Path, "open", side_effect=AssertionError("filesystem")),
            patch("urllib.request.urlopen", side_effect=AssertionError("network")),
        ):
            result = build_plan(document)
            json.dumps(result, allow_nan=False)
        self.assertEqual(document, before)
        self.assertEqual(len(result["steps"][0]["actions"]), 1)
        action = result["steps"][0]["actions"][0]
        self.assertEqual(action["strategy"], "bars_multi_range")
        self.assertEqual(len(action["request"]["params"]["symbols"].split(",")), 2000)
        self.assertEqual(action["request"]["params"]["end"], "2026-10-12T03:59:59.999999Z")

    def test_one_ticker_ten_years_uses_one_range_per_adjustment(self):
        result = plan(need(["GOOG"], start="2016-01-01", adjustments=list(ADJUSTMENTS)))
        actions = result["steps"][0]["actions"]
        self.assertEqual(len(actions), 8)
        self.assertTrue(all(action["strategy"] == "bars_single_range" for action in actions))
        self.assertEqual({action["request"]["params"]["adjustment"] for action in actions}, set(ADJUSTMENTS))

    def test_three_full_histories_share_eight_actions(self):
        result = plan(
            *(
                need([symbol], why="changed_data", start="2016-01-01", adjustments=list(ADJUSTMENTS))
                for symbol in ("KNDI", "KWR", "AMZN")
            )
        )
        self.assertEqual(len(result["steps"][0]["actions"]), 8)
        self.assertEqual(result["steps"][0]["actions"][0]["request"]["params"]["symbols"], "AMZN,KNDI,KWR")

    def test_symbol_cap_and_url_size_are_respected(self):
        result = plan(need([f"S{i}" for i in range(8001)]))
        self.assertEqual(
            [len(action["request"]["params"]["symbols"].split(",")) for action in result["steps"][0]["actions"]],
            [4000, 4000, 1],
        )
        limited = plan(need(["A" + str(i) + "X" * 80 for i in range(50)]), max_url_bytes=3000)
        self.assertGreater(len(limited["steps"][0]["actions"]), 1)

    def test_nearby_sparse_boundaries_share_request_with_exact_delivery_ranges(self):
        result = plan(
            need(["A"], why="boundary_check", start="2026-10-12", end="2026-10-13"),
            need(["B"], why="boundary_check", start="2026-10-14", end="2026-10-15"),
        )
        actions = result["steps"][0]["actions"]
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["strategy"], "bars_boundary_batch")
        calls = []

        def get_page(parameters):
            calls.append(parameters)
            return {
                "bars": {
                    "A": [bar("2026-10-12"), bar("2026-10-13")],
                    "B": [bar("2026-10-13"), bar("2026-10-14")],
                }
            }

        rows = execute_bar_plan(result, get_page)
        self.assertEqual(len(calls), 1)
        self.assertEqual([row[0][:10] for row in rows[(0, 0, 0, "A", "all")].rows], ["2026-10-12"])
        self.assertEqual([row[0][:10] for row in rows[(0, 1, 0, "B", "all")].rows], ["2026-10-14"])

    def test_asof_reason_and_ordered_steps_never_merge(self):
        original = need(["A"])
        document = {
            "steps": [
                {"needs": [original, {**original, "asof": "2026-10-17"}, {**original, "why": "boundary_check"}]},
                {"needs": [original]},
            ]
        }
        result = build_plan(document)
        self.assertEqual([len(step["actions"]) for step in result["steps"]], [3, 1])

    def test_asset_lookups_deduplicate_exact_identifiers_within_step(self):
        lookup = {"kind": "asset_lookup", "why": "identity", "identifiers": ["GOOG", "037833100", "GOOG"]}
        result = plan(lookup, lookup)
        actions = result["steps"][0]["actions"]
        self.assertEqual(len(actions), 2)
        self.assertTrue(
            all(action["strategy"] == "asset_lookup" and len(action["deliver"]) == 2 for action in actions)
        )

    def test_corporate_action_queries_batch_matching_ranges(self):
        base = {
            "kind": "corporate_actions",
            "why": "lineage",
            "symbols": ["A"],
            "start": "2026-10-01",
            "end": "2026-10-14",
        }
        result = plan(base, {**base, "symbols": ["B"]})
        actions = result["steps"][0]["actions"]
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0]["strategy"], "corporate_actions_batch")
        self.assertEqual(actions[0]["request"]["params"]["symbols"], "A,B")
        self.assertEqual(actions[0]["request"]["params"]["limit"], 1000)

    def test_pagination_exhausts_before_next_ordered_step(self):
        item = need(["A"], start="2026-10-14", end="2026-10-15")
        result = build_plan({"steps": [{"needs": [item]}, {"needs": [item]}]})
        calls = []

        def get_page(parameters):
            calls.append(parameters)
            if len(calls) == 1:
                return {"bars": {}, "next_page_token": "next"}
            return {"bars": {"A": [bar("2026-10-14")]}}

        rows = execute_bar_plan(result, get_page)
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[1]["page_token"], "next")
        self.assertNotIn("page_token", calls[2])
        self.assertEqual(len(rows), 2)

    def test_failed_step_blocks_dependent_step(self):
        result = build_plan({"steps": [{"needs": [need(["A"])]}, {"needs": [need(["B"])]}]})
        calls, errors = [], []

        def get_page(parameters):
            calls.append(parameters)
            raise DownloadError("failure")

        rows = execute_bar_plan(result, get_page, on_failure=lambda step, action, error: errors.append(step))
        self.assertEqual(rows, {})
        self.assertEqual(len(calls), 1)
        self.assertEqual(errors, [0])

    def test_uri_rejection_uses_solver_split_without_mutating_plan(self):
        result = plan(need(["A", "B"], start="2026-10-14", end="2026-10-15"))
        original = copy.deepcopy(result)
        metrics = {}

        def get_page(parameters):
            if "," in parameters["symbols"]:
                raise urllib.error.HTTPError("test", 414, "too long", {}, None)
            return {"bars": {parameters["symbols"]: [bar("2026-10-14")]}}

        rows = execute_bar_plan(result, get_page, metrics=metrics)
        self.assertEqual(result, original)
        self.assertEqual(len(rows), 2)
        self.assertEqual(metrics, {"page_requests": 3, "oversize_splits": 1})

    def test_invalid_need_is_rejected_before_execution(self):
        for item in (
            {**need(["A"]), "kind": "guess"},
            {**need(["A"]), "why": ""},
            {**need(["A"]), "adjustments": ["unknown"]},
            need(["A"], start="2026-10-12", end="2026-10-10"),
        ):
            with self.subTest(item=item), self.assertRaises(ValueError):
                plan(item)


if __name__ == "__main__":
    unittest.main()

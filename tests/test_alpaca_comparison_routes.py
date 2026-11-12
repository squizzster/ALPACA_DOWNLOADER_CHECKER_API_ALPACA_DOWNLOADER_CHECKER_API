"""Missing endpoints use accepted dated routes without repeating completed work."""

import copy
import unittest
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_live_check import bar, entry, snapshot

from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build.comparison_routes import historical_route
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import live_check
from libs.alpaca_pricing_download.contract import FIELDS


def receipt():
    return {
        "symbol": "NEW",
        "source_sha256": "generation",
        "identity_intervals": [{"from": "2026-10-13", "to": "2026-10-14", "security_id": "public"}],
        "identity_memberships": [
            {"security_id": "public", "member_alpaca_asset_ids": ["uuid"], "member_tickers": ["OLD", "NEW"]}
        ],
        "evidence": {
            "asset_lookups": [
                {"lookup_kind": "symbol", "lookup": "NEW", "found": True, "asset": {"id": "uuid"}},
                {"lookup_kind": "cusip", "lookup": "cusip", "found": True, "asset": {"id": "uuid"}},
            ],
            "symbol_changes": [
                {
                    "new_symbol": "NEW",
                    "old_symbol": "OLD",
                    "new_cusip": "cusip",
                    "old_cusip": "cusip",
                    "process_date": "2026-10-14",
                }
            ],
        },
    }


class ComparisonRouteTests(unittest.TestCase):
    def test_route_consumes_dated_positive_owner_and_current_generation(self):
        value = receipt()
        self.assertEqual(
            historical_route(value, "2026-10-13", "generation"),
            {"symbol": "OLD", "asof": "2026-10-13", "owner": "public", "asset_id": "uuid"},
        )
        self.assertIsNone(historical_route(value, "2026-10-14", "generation"))
        self.assertIsNone(historical_route(value, "2026-10-13", "changed"))

    def test_unknown_foreign_competing_or_unlike_evidence_cannot_select_route(self):
        for change in ("unknown", "foreign", "competing", "unlike"):
            value = receipt()
            if change == "unknown":
                value["identity_memberships"] = []
            if change == "foreign":
                value["identity_memberships"][0]["member_alpaca_asset_ids"] = ["foreign"]
            if change == "competing":
                value["evidence"]["symbol_changes"].append(copy.deepcopy(value["evidence"]["symbol_changes"][0]))
            if change == "unlike":
                value["evidence"]["symbol_changes"][0]["old_cusip"] = "different"
            with self.subTest(change=change):
                self.assertIsNone(historical_route(value, "2026-10-13", "generation"))

    def test_later_reuse_of_predecessor_label_cannot_redirect_old_owner(self):
        value = receipt()
        value["evidence"]["symbol_changes"].append(
            {
                "new_symbol": "OLD",
                "old_symbol": "FOREIGN",
                "old_cusip": "foreign",
                "new_cusip": "foreign",
                "process_date": "2026-10-15",
            }
        )
        value["identity_intervals"] = []  # Request routing does not allocate rows.
        self.assertEqual(historical_route(value, "2026-10-13", "generation")["symbol"], "OLD")

    def test_missing_first_consumes_alternate_mapping_and_retained_tail_once(self):
        selected = entry("NEW")
        selected["first_all"] = [str(v) for v in (bar("2026-10-13")[k] for k in FIELDS)]
        calls, routes = [], []

        def get_page(params):
            key = params["symbols"], params["start"][:10], params["asof"]
            self.assertNotIn(key, calls, "completed request repeated")
            calls.append(key)
            if params["symbols"] == "OLD":
                return {"bars": {"OLD": [bar("2026-10-13")]}}
            return {"bars": {} if params["start"][:10] == "2026-10-13" else {"NEW": [bar()]}}

        def route(value, day):
            routes.append(day)
            return historical_route(receipt(), day, "generation")

        with patch.object(live_check, "compare_history", wraps=live_check.compare_history) as compare:
            result = live_check.verify(snapshot(selected), get_page, missing_route=route)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["matched"], ["NEW"])
        self.assertEqual(routes, ["2026-10-13"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(compare.call_count, 1)
        self.assertEqual(result["mapped_endpoints"][0]["mapped_symbol"], "OLD")

    def test_missing_mapped_endpoint_preserves_incomplete_result_without_retry(self):
        calls = []

        def get_page(params):
            calls.append(params["symbols"])
            return {"bars": {}}

        result = live_check.verify(
            snapshot(entry("NEW")), get_page, missing_route=lambda entry, day: {"symbol": "OLD", "asof": day}
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["missing"], ["NEW"])
        self.assertEqual(calls, ["NEW", "OLD"])
        self.assertEqual(result["comparisons"], {})

    def test_route_failure_retains_completed_independent_peer(self):
        def get_page(params):
            return {"bars": {"PEER": [bar()]}} if "PEER" in params["symbols"] else {"bars": {}}

        def broken(entry, day):
            raise RuntimeError("selected lineage unavailable")

        result = live_check.verify(snapshot(entry("NEW"), entry("PEER")), get_page, missing_route=broken)
        self.assertEqual(result["matched"], ["PEER"])
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertFalse(result["complete"])

    def test_completed_comparison_never_requests_or_resolves_an_alternative(self):
        def forbidden(*args):
            raise AssertionError("accepted endpoint revisited")

        result = live_check.verify(
            snapshot(entry("NEW")), lambda p: {"bars": {"NEW": [bar()]}}, missing_route=forbidden
        )
        self.assertTrue(result["complete"])


class PublishedComparisonRouteTests(NativeCase):
    def test_native_renamed_receipt_supplies_route_without_assignment_or_price_reads(self):
        self.add_symbol("OTHER")
        current = asset("OTHER")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-10-06",
            "old_symbol": "TEST",
            "new_symbol": "OTHER",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        result = run_lineage(
            self.store,
            ["TEST", "OTHER"],
            refresh=True,
            identity_client=IdentityClient({"TEST": current, "OTHER": current, "111111111": current}, [action]),
        )
        self.assertEqual(result["failed"], 0, result)
        index = lineage_storage.read_index(self.root)
        value = lineage_storage.read_receipt(self.root, "OTHER", index)
        with patch.object(
            lineage_storage, "read_boundaries", side_effect=AssertionError("assignments reopened")
        ):
            route = historical_route(value, "2026-10-01", value["source_sha256"])
        self.assertIsNotNone(route, value)
        self.assertEqual(route["symbol"], "TEST")
        self.assertEqual(route["asof"], "2026-10-01")

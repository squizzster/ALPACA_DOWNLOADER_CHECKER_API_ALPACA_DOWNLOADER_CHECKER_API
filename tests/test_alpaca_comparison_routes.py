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


def retired_receipt():
    return {
        "symbol": "MID",
        "source_sha256": "generation",
        "last_observed_symbol_lookup": {
            "lookup_kind": "symbol",
            "lookup": "MID",
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": {"id": "uuid", "symbol": "MID", "class": "us_equity"},
        },
        "identity_memberships": [
            {
                "security_id": "public",
                "member_alpaca_asset_ids": ["uuid"],
                "member_tickers": ["OLD", "MID", "CURRENT"],
            }
        ],
        "evidence": {
            "asset_lookups": [
                {
                    "lookup_kind": "symbol",
                    "lookup": "MID",
                    "found": False,
                    "outcome": "not_found",
                    "http_status": 404,
                    "asset": None,
                },
                {
                    "lookup_kind": "symbol",
                    "lookup": "CURRENT",
                    "found": True,
                    "outcome": "found",
                    "http_status": 200,
                    "asset": {"id": "uuid"},
                },
                {
                    "lookup_kind": "cusip",
                    "lookup": "cusip",
                    "found": True,
                    "outcome": "found",
                    "http_status": 200,
                    "asset": {"id": "uuid"},
                },
            ],
            "symbol_changes": [
                {
                    "new_symbol": "MID",
                    "old_symbol": "OLD",
                    "new_cusip": "cusip",
                    "old_cusip": "cusip",
                    "process_date": "2026-06-17",
                },
                {
                    "new_symbol": "CURRENT",
                    "old_symbol": "MID",
                    "new_cusip": "cusip",
                    "old_cusip": "cusip",
                    "process_date": "2026-11-12",
                },
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

    def test_retired_source_uses_unambiguous_membership_and_cusip_bound_route(self):
        self.assertEqual(
            historical_route(retired_receipt(), "2025-01-23", "generation"),
            {"symbol": "OLD", "asof": "2025-01-23", "owner": "public", "asset_id": "uuid"},
        )

    def test_retired_source_rejects_ambiguous_membership_or_identifier_evidence(self):
        for change in (
            "unconfirmed_absence",
            "missing_held_lookup",
            "malformed_held_lookup",
            "foreign_held_lookup",
            "competing_owner",
            "foreign_cusip",
            "conflicting_chain",
        ):
            value = retired_receipt()
            if change == "unconfirmed_absence":
                value["evidence"]["asset_lookups"][0].pop("http_status")
            elif change == "missing_held_lookup":
                value.pop("last_observed_symbol_lookup")
            elif change == "malformed_held_lookup":
                value["last_observed_symbol_lookup"] = "invalid"
            elif change == "foreign_held_lookup":
                value["last_observed_symbol_lookup"]["asset"]["id"] = "foreign"
            elif change == "competing_owner":
                value["identity_memberships"].append(
                    {
                        "security_id": "competitor",
                        "member_alpaca_asset_ids": ["uuid"],
                        "member_tickers": ["MID"],
                    }
                )
            elif change == "foreign_cusip":
                value["evidence"]["asset_lookups"][2]["asset"]["id"] = "foreign"
            else:
                value["symbol"] = "CURRENT"
                value["identity_memberships"][0]["member_alpaca_asset_ids"].append("other")
                value["evidence"]["symbol_changes"][1]["new_cusip"] = "other-cusip"
                value["evidence"]["symbol_changes"][1]["old_cusip"] = "other-cusip"
                value["evidence"]["asset_lookups"].append(
                    {
                        "lookup_kind": "cusip",
                        "lookup": "other-cusip",
                        "found": True,
                        "asset": {"id": "other"},
                    }
                )
            with self.subTest(change=change):
                self.assertIsNone(historical_route(value, "2025-01-23", "generation"))

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
        tail_days = (
            "2026-10-01",
            "2026-10-02",
            "2026-10-05",
            "2026-10-06",
            "2026-10-07",
            "2026-10-08",
            "2026-10-09",
            "2026-10-12",
            "2026-10-13",
            "2026-10-14",
        )
        selected = entry("MID")
        first_bar = bar("2025-01-23", t="2025-01-23T05:00:00Z")
        selected["first_all"] = [str(first_bar[key]) for key in FIELDS]
        selected["tail_all"] = [[str(bar(day)[key]) for key in FIELDS] for day in tail_days]
        selected["last_all"] = selected["tail_all"][-1]
        selected_snapshot = snapshot(selected)
        selected_snapshot["entries"][0]["tail_all"] = copy.deepcopy(selected["tail_all"])
        calls, routes = [], []

        def get_page(params):
            key = params["symbols"], params["start"][:10], params["end"][:10], params["asof"]
            self.assertNotIn(key, calls, "completed request repeated")
            calls.append(key)
            if params["symbols"] == "OLD":
                return {"bars": {"OLD": [first_bar]}}
            return {
                "bars": {} if params["start"][:10] == "2025-01-23" else {"MID": [bar(day) for day in tail_days]}
            }

        def route(value, day):
            routes.append(day)
            result = historical_route(retired_receipt(), day, "generation")
            if result is not None:
                result["lineage_generation"] = "lineage-generation"
            return result

        with patch.object(live_check, "compare_history", wraps=live_check.compare_history) as compare:
            result = live_check.verify(selected_snapshot, get_page, missing_route=route)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["matched"], ["MID"])
        self.assertEqual(routes, ["2025-01-23"])
        self.assertEqual(
            calls,
            [
                ("MID", "2025-01-23", "2025-01-24", "2026-10-15"),
                ("MID", "2026-10-01", "2026-10-14", "2026-10-15"),
                ("OLD", "2025-01-23", "2025-01-24", "2025-01-23"),
            ],
        )
        self.assertEqual(compare.call_count, 1)
        self.assertEqual(
            result["mapped_endpoints"],
            [
                {
                    "symbol": "MID",
                    "endpoint": "first",
                    "session": "2025-01-23",
                    "mapped_symbol": "OLD",
                    "asof": "2025-01-23",
                    "owner": "public",
                    "asset_id": "uuid",
                    "lineage_generation": "lineage-generation",
                }
            ],
        )

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

"""Producer-owned group retirement, including later refresh and interrupted commit."""

from pathlib import Path
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset

from alpaca_end_of_day import StandaloneReader, StandaloneReaderError
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.projection import migrate_index
from libs.alpaca_lineage_build.universe_state import initialize
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_serving_catalogue.producer import maintain


class GroupRetirementTests(NativeCase):
    def merged(self):
        self.add_symbol("OLD", valid_bars("2023-12-01", "2023-12-13"))
        self.add_symbol("NEW", valid_bars("2023-12-13", "2023-12-22"))
        old, new = asset("OLD"), asset("NEW")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2023-12-13",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
        }
        client = IdentityClient({"OLD": old, "NEW": new, "111111111": old, "222222222": new}, [action])
        result = run_lineage(self.store, ["OLD", "NEW"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 0, result)
        self.bootstrap()

    def assert_split(self):
        index = storage.read_index(self.root)
        for symbol in ("OLD", "NEW"):
            receipt = storage.read_receipt(self.root, symbol, index)
            self.assertEqual(set(receipt["input_sources"]), {symbol})
            self.assertEqual(index["symbols"][symbol]["group_members"], [symbol])
            self.assertTrue(StandaloneReader(self.store).get_history(symbol)["bars"])
        result = maintain(self.store)
        self.assertTrue(result["complete"], result)

    def test_disappearing_link_retires_both_members_before_a_later_refresh(self):
        self.merged()
        client = IdentityClient({name: asset(name) for name in ("OLD", "NEW")})
        result = run_lineage(self.store, ["OLD"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["processed"], ["NEW", "OLD"])
        self.assert_split()
        original = Path.read_bytes

        def selected_only(path):
            self.assertFalse(path.is_relative_to(self.store / "end_of_day/pricing/NEW"), path)
            self.assertFalse(path.is_relative_to(self.root / "NEW"), path)
            return original(path)

        with patch.object(Path, "read_bytes", selected_only):
            result = run_lineage(self.store, ["OLD"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["processed"], ["OLD"])
        self.assert_split()

    def test_interrupted_split_recovers_the_complete_retirement_without_new_evidence(self):
        self.merged()
        client = IdentityClient({name: asset(name) for name in ("OLD", "NEW")})
        with patch.object(storage, "_update_index", side_effect=RuntimeError("interrupted split")):
            result = run_lineage(self.store, ["OLD"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 1, result)
        with self.assertRaises(StandaloneReaderError):
            StandaloneReader(self.store).get_history("NEW")
        client = IdentityClient()
        result = run_lineage(self.store, ["OLD"], identity_client=client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(client.request_count, 0)
        self.assert_split()

    def test_work_policy_retains_previous_membership_when_current_links_are_absent(self):
        self.merged()
        for symbol in ("OLD", "NEW"):

            def disconnected(receipt, _):
                receipt["dependencies"] = []
                receipt["asset_ids"] = [receipt["symbol"] + "-id"]

            self.publish_change(disconnected, symbol)
        initialize(self.store)
        index = storage.read_index(self.root)
        policy = WorkPolicy(self.store, index, {}, [], "2026-10-14")
        policy.update_index(index["symbols"])
        self.assertEqual(policy.graph.closure({"OLD"}), {"OLD", "NEW"})
        client = IdentityClient({name: asset(name) for name in ("OLD", "NEW")})
        result = run_lineage(self.store, ["OLD"], identity_client=client, refresh=True, work_policy=policy)
        self.assertEqual(result["failed"], 0, result)
        self.assert_split()

    def test_missing_membership_fails_closed_until_explicit_index_migration(self):
        self.merged()
        index = storage.read_index(self.root)
        entry = dict(index["symbols"]["OLD"])
        entry.pop("group_members")
        with storage.publication_lock(self.root, exclusive=True):
            storage._update_index(self.root, {"OLD": entry})
        client = IdentityClient()
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("implicit receipt reconstruction")),
            self.assertRaisesRegex(LineageError, "lineage migrate-index"),
        ):
            run_lineage(self.store, ["OLD"], identity_client=client)
        self.assertEqual(client.request_count, 0)
        result = migrate_index(self.store)
        self.assertEqual(result["receipts_read"], 1)
        self.assertEqual(storage.read_index(self.root)["symbols"]["OLD"]["group_members"], ["NEW", "OLD"])
        client = IdentityClient({name: asset(name) for name in ("OLD", "NEW")})
        result = run_lineage(self.store, ["OLD"], identity_client=client, refresh=True)
        self.assertEqual(result["failed"], 0, result)
        self.bootstrap()
        self.assert_split()

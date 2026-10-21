"""Synthetic discovery, interruption, bounded I/O and cutoff contracts."""

import copy
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlite_crash_fixture import leave_hot_journal
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.catalogue import discovery_labels, observe_catalogue
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.projection import migrate_index, require_complete
from libs.alpaca_lineage_build.universe_update import (
    plan_update,
    rebuild_universe,
    recover_publications,
    run_update,
)
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.storage import load_source
from libs.alpaca_recovery_registry import load_labels, publish_union
from libs.common.sqlite_connections import connect_sqlite


class CatalogueClient(IdentityClient):
    def __init__(self, records, *, exact=None):
        super().__init__(exact or {row["symbol"]: row for row in records})
        self.records = records
        self.catalogue_calls = 0

    def execute_plan(self, plan):
        result = []
        for step in plan["steps"]:
            values = []
            for action in step["actions"]:
                if action["strategy"] == "asset_catalogue":
                    self.catalogue_calls += 1
                    self.request_count += 1
                    values.append(copy.deepcopy(self.records))
                elif action["strategy"] == "asset_lookup":
                    values.append(self.get_asset_lookup(action["deliver"][0]["identifier"]))
                else:
                    self.request_count += 1
                    values.append([])
            result.append(values)
        return result


class UniverseUpdateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.store = self.directory / "store"
        self.registry = self.directory / "ticker.csv"
        self.registry.write_text("AAA,HIST\n")
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        self.old = [bar("2016-01-04"), bar("2026-10-16")]
        download_pricing(self.store, "AAA", "2026-10-17", client=FakeClient(self.old))
        self.assertEqual(
            run_lineage(self.store, identity_client=IdentityClient({"AAA": asset("AAA")}))["failed"], 0
        )
        rebuild(self.store)
        rebuild_universe(self.store)
        from libs.alpaca_serving_catalogue.producer import maintain

        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        self.records = [
            asset("AAA"),
            asset("NEW"),
            {**asset("EMPTY"), "exchange": "OTC", "tradable": False},
            {**asset("IGNORED"), "exchange": "BATS"},
        ]
        self.identity = CatalogueClient(self.records)
        self.pricing = BarsClient("fake", "fake")
        self.pages = []
        self.live = {"AAA": self.old, "NEW": [bar("2026-10-15")], "EMPTY": []}
        self.enterContext(patch.object(self.pricing, "_get", side_effect=self.page))

    def page(self, params):
        self.pages.append(dict(params))
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                s: [row for row in self.live.get(s, []) if start <= timestamp(row["t"]) <= end]
                for s in params["symbols"].split(",")
            }
        }

    def update(self, end="2026-10-17", **kwargs):
        return run_update(
            self.store,
            end,
            pricing_client=self.pricing,
            identity_client=self.identity,
            registry=self.registry,
            **kwargs,
        )

    def test_discovery_registers_nontradable_and_empty_then_repeat_does_no_acquisition(self):
        result = self.update()
        self.assertTrue(result["complete"], result)
        self.assertEqual(load_labels(self.registry), ("AAA", "EMPTY", "HIST", "NEW"))
        self.assertEqual(self.identity.catalogue_calls, 1)
        self.assertEqual(len(read_pricing_views(self.store, "NEW")["raw"]), 1)
        self.assertEqual(read_pricing_views(self.store, "EMPTY")["raw"], ())
        self.pages.clear()
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("unselected receipt")),
            patch.object(Path, "iterdir", side_effect=AssertionError("universe crawl")),
        ):
            repeated = self.update()
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(self.identity.catalogue_calls, 2)
        self.assertTrue(all(p["adjustment"] == "all" and p["start"][:10] >= "2026-10-15" for p in self.pages))

    def test_summary_advancement_publishes_lineage_after_checked_append(self):
        self.records[:] = [asset("AAA")]
        self.live["AAA"] = [*self.old, bar("2026-10-19")]
        lineage_targets = []

        def observed(*args, **kwargs):
            source = load_source(self.store / "end_of_day/pricing/AAA", "AAA")
            lineage_targets.append(source["latest_request"]["end_exclusive"])
            return run_lineage(*args, **kwargs)

        with (
            patch("libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=observed),
            patch("libs.alpaca_lineage_build.update.run_lineage", side_effect=observed),
        ):
            result = self.update("2026-10-20")
        self.assertTrue(result["complete"], result)
        self.assertTrue(lineage_targets)
        self.assertEqual(set(lineage_targets), {"2026-10-20T00:00:00Z"})
        self.assertEqual(result["pending_work"], 0)
        views = read_pricing_views(self.store, "AAA")
        self.assertEqual(len(views), 8)
        for rows in views.values():
            self.assertEqual([row[0][:10] for row in rows], ["2016-01-04", "2026-10-16", "2026-10-19"])
        entry = storage.read_index(self.store / "end_of_day/lineage")["symbols"]["AAA"]
        self.assertEqual(entry["projection"]["processed_through"], "2026-10-20T00:00:00Z")
        self.assertEqual(universe_state.read_state(self.store)["work"]["AAA"]["status"], "complete")

    def test_daily_update_recovers_catalogue_and_work_transactions_before_selection(self):
        from libs.alpaca_serving_catalogue.storage import path as catalogue_path

        leave_hot_journal(catalogue_path(self.store), "DELETE FROM groups")
        leave_hot_journal(self.store / "end_of_day/lineage/index.sqlite3", "DELETE FROM serving_work")
        result = self.update()
        self.assertTrue(result["complete"], result)
        self.assertTrue(result["stages"]["serving_catalogue"]["complete"])

    def test_resume_compares_identity_facts_from_full_and_compact_exact_lookups(self):
        class Interrupted(BaseException):
            pass

        current = {**asset("AAA"), "marginable": True, "attributes": []}
        self.identity = CatalogueClient(
            [current, {**asset("AAA", "older-id"), "status": "inactive"}],
            exact={"AAA": current},
        )
        self.live["AAA"] = [*self.old, bar("2026-10-19")]
        with (
            patch("libs.alpaca_lineage_build.universe_update.update_everything", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            self.update("2026-10-20")
        before = universe_state.read_state(self.store)["work"]["AAA"]
        plan = plan_update(self.store, "2026-10-20", identity_client=self.identity, registry=self.registry)
        selected = plan["work"]["AAA"]
        self.assertNotIn("changed_metadata", selected["reasons"])
        self.assertFalse(selected["refresh_metadata"])
        self.assertEqual(selected["obligation_id"], before["obligation_id"])
        self.assertTrue(self.update("2026-10-20")["complete"])

    def test_preview_performs_no_writes_or_receipt_reads(self):
        before = snapshot(self.directory)

        def readonly_connection(path, **kwargs):
            self.assertTrue(kwargs.get("readonly"), str(path))
            return connect_sqlite(path, **kwargs)

        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("receipt read")),
            patch.object(Path, "iterdir", side_effect=AssertionError("universe crawl")),
            patch("libs.alpaca_lineage_build.storage.connect_sqlite", side_effect=readonly_connection),
            patch("libs.alpaca_lineage_build.universe_state.connect_sqlite", side_effect=readonly_connection),
            patch("libs.alpaca_pricing_download.check_summary.connect_sqlite", side_effect=readonly_connection),
            patch("libs.alpaca_pricing_download.update_checks.connect_sqlite", side_effect=readonly_connection),
        ):
            report = plan_update(self.store, "2026-10-17", identity_client=self.identity, registry=self.registry)
        self.assertFalse(report["blockers"])
        self.assertEqual(set(report["work"]), {"NEW", "EMPTY"})
        self.assertEqual(before, snapshot(self.directory))
        self.assertFalse(self.pages)

    def test_preview_blocks_incomplete_catalogue_even_after_all_work_is_acknowledged(self):
        from libs.alpaca_serving_catalogue import storage as catalogue_storage
        from libs.alpaca_serving_catalogue.producer import maintain, status

        with (
            patch.object(catalogue_storage, "finish_bootstrap", side_effect=RuntimeError("interrupted finish")),
            self.assertRaisesRegex(RuntimeError, "interrupted finish"),
        ):
            maintain(self.store, rebuild=True)
        state = status(self.store)
        self.assertFalse(state["coverage_complete"])
        self.assertEqual(state["pending_symbols"], [])
        before = snapshot(self.store)
        report = plan_update(self.store, "2026-10-17", identity_client=self.identity, registry=self.registry)
        self.assertTrue(any("serving catalogue incomplete" in value for value in report["blockers"]), report)
        self.assertEqual(snapshot(self.store), before)
        requests = self.identity.request_count, len(self.pages)
        with self.assertRaisesRegex(RuntimeError, "serving catalogue incomplete"):
            self.update()
        self.assertEqual((self.identity.request_count, len(self.pages)), requests)
        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])
        report = plan_update(self.store, "2026-10-17", identity_client=self.identity, registry=self.registry)
        self.assertEqual(report["blockers"], [])

    def test_publication_before_acknowledgement_does_not_repeat_acquisition(self):
        original = universe_state.record_outcome

        def stopped(store, symbol, **kwargs):
            if kwargs["status"] == "complete":
                raise OSError("interrupted acknowledgement")
            return original(store, symbol, **kwargs)

        with patch.object(universe_state, "record_outcome", side_effect=stopped), self.assertRaises(OSError):
            self.update()
        self.pages.clear()
        result = self.update()
        self.assertTrue(result["complete"], result)
        self.assertFalse(result["stages"]["acquisition"]["results"])

    def test_registration_interruption_retains_work_after_csv_membership_changes(self):
        original = universe_state.register

        def stopped(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError("interrupted after registry union")

        with patch.object(universe_state, "register", side_effect=stopped), self.assertRaises(OSError):
            self.update()
        self.assertIn("NEW", load_labels(self.registry))
        self.assertFalse(self.pages)
        self.assertTrue(self.update()["complete"])

    def test_changed_status_and_id_force_reconciliation_with_unchanged_pricing(self):
        self.records[:] = [{**asset("AAA", "replacement-id"), "exchange": "TXSE"}]
        self.identity = CatalogueClient(self.records)
        before = storage.read_index(self.store / "end_of_day/lineage")["symbols"]["AAA"]["sha256"]
        result = self.update()
        self.assertTrue(result["complete"], result)
        entry = storage.read_index(self.store / "end_of_day/lineage")["symbols"]["AAA"]
        self.assertNotEqual(before, entry["sha256"])
        self.assertEqual(entry["projection"]["symbol_lookups"][0]["asset"]["id"], "replacement-id")

    def test_older_target_preserves_retained_histories_and_bounds_new_acquisition(self):
        before = snapshot(self.store / "end_of_day/pricing/AAA")
        result = self.update("2026-10-16")
        self.assertTrue(result["complete"], result)
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing/AAA"))
        self.assertTrue(all(timestamp(p["end"]) < timestamp("2026-10-16T00:00:00Z") for p in self.pages))
        self.assertIn("AAA", result["stages"]["maintenance"]["stages"]["refresh"]["already_beyond_target"])

    def test_later_empty_coverage_fetches_only_new_interval(self):
        self.assertTrue(self.update()["complete"])
        self.pages.clear()
        result = self.update("2026-10-20")
        self.assertTrue(result["complete"], result)
        empty = [p for p in self.pages if "EMPTY" in p["symbols"].split(",")]
        self.assertEqual(len(empty), 8)
        self.assertTrue(
            all(
                p["start"] == "2026-10-17T00:00:00+00:00"
                or timestamp(p["start"]) == timestamp("2026-10-17T00:00:00Z")
                for p in empty
            )
        )

    def test_known_empty_history_on_other_exchange_stays_maintained(self):
        known = {**asset("KNOWN"), "exchange": "BATS"}
        download_pricing(self.store, "KNOWN", "2026-10-17", client=FakeClient([]))
        self.assertEqual(
            run_lineage(self.store, ["KNOWN"], identity_client=IdentityClient({"KNOWN": known}))["failed"], 0
        )
        self.records[:] = [asset("AAA"), known, {**asset("IGNORED"), "exchange": "BATS"}]
        result = self.update("2026-10-20")
        self.assertTrue(result["complete"], result)
        self.assertEqual(set(result["plan"]["work"]), {"KNOWN", "AAA"})
        self.assertIn("advance_summary_coverage", result["plan"]["work"]["AAA"]["reasons"])
        self.assertNotIn("IGNORED", load_labels(self.registry))
        index = storage.read_index(self.store / "end_of_day/lineage")
        self.assertEqual(index["symbols"]["KNOWN"]["projection"]["processed_through"][:10], "2026-10-20")

    def test_preview_counts_empty_advancement_separately_from_full_acquisition(self):
        self.assertTrue(self.update()["complete"])
        self.records.append(asset("LATER"))
        self.identity = CatalogueClient(self.records)
        report = plan_update(self.store, "2026-10-20", identity_client=self.identity, registry=self.registry)
        io = report["expected_io"]
        self.assertEqual(io["initial_or_invalidated_actions_estimate"], 8)
        self.assertEqual(io["empty_coverage_actions_estimate"], 8)
        self.assertEqual(io["acquisition_actions_estimate"], 16)
        self.assertEqual(io["selected_lineage_labels"], 4)
        self.assertEqual(io["existing_lineage_receipts_selected"], 3)

    def test_catalogue_absence_never_removes_history_or_registry_labels(self):
        self.records[:] = [asset("NEW")]
        before = load_labels(self.registry)
        result = self.update()
        self.assertTrue(result["complete"], result)
        self.assertTrue(set(before) <= set(load_labels(self.registry)))
        self.assertEqual(len(read_pricing_views(self.store, "AAA")["raw"]), 2)

    def test_duplicate_catalogue_records_use_exact_evidence_without_losing_records(self):
        self.records[:] = [asset("AAA"), {**asset("AAA", "old-id"), "status": "inactive"}]
        self.identity = CatalogueClient(self.records, exact={"AAA": asset("AAA")})
        result = self.update()
        self.assertTrue(result["complete"], result)
        self.assertFalse(result["plan"]["work"])
        self.assertEqual(result["plan"]["catalogue_records"], 2)
        self.assertEqual(result["plan"]["expected_io"]["selection_metadata_http_requests_observed"], 2)

    def test_missing_work_state_is_not_silently_initialized(self):
        (self.store / universe_state.STATE_FILE).unlink()
        before = snapshot(self.directory)
        report = plan_update(self.store, "2026-10-17", identity_client=self.identity, registry=self.registry)
        self.assertTrue(report["blockers"])
        self.assertEqual(before, snapshot(self.directory))
        with self.assertRaisesRegex(LineageError, "universe rebuild"):
            self.update()

    def test_projection_migration_is_resumable_and_never_reads_prices(self):
        root = self.store / "end_of_day/lineage"
        with connect_sqlite(root / "index.sqlite3") as connection:
            row = connection.execute("SELECT receipt FROM symbols WHERE symbol='AAA'").fetchone()
            entry = json.loads(row[0])
            entry.pop("projection")
            connection.execute("UPDATE symbols SET receipt=? WHERE symbol='AAA'", (json.dumps(entry),))
            connection.execute("UPDATE projection_state SET complete=0")
        connection.close()
        with self.assertRaises(LineageError):
            require_complete(storage.read_index(root))
        with patch(
            "libs.alpaca_pricing_download.storage.load_source", side_effect=AssertionError("pricing manifest")
        ):
            with self.assertRaisesRegex(OSError, "stop"):
                migrate_index(
                    self.store, batch_size=1, progress=lambda *_: (_ for _ in ()).throw(OSError("stop"))
                )
            result = migrate_index(self.store, batch_size=1)
        self.assertEqual(result["receipts_read"], 0)
        require_complete(storage.read_index(root))

    def test_access_denial_is_durable_and_only_explicit_retry_reacquires(self):
        from urllib.error import HTTPError

        ordinary_page = self.page

        def denied(params):
            if any(symbol != "AAA" for symbol in params["symbols"].split(",")):
                raise HTTPError("https://example.invalid", 403, "access denied", {}, None)
            return ordinary_page(params)

        with patch.object(self.pricing, "_get", side_effect=denied):
            result = self.update()
        self.assertFalse(result["complete"])
        state = universe_state.read_state(self.store)
        self.assertEqual({item["status"] for item in state["work"].values()}, {"blocked"})
        self.pages.clear()
        repeated = self.update()
        self.assertFalse(repeated["complete"])
        self.assertEqual(len(repeated["plan"]["deferred"]), 2)
        self.assertTrue(all(p["symbols"] == "AAA" for p in self.pages))
        self.assertTrue(self.update(retry_blocked=True)["complete"])

    def test_blocked_lineage_is_not_retried_by_publication_recovery(self):
        def denied(_store, symbols, **_kwargs):
            return {
                "failed": len(symbols),
                "results": [
                    {"symbol": symbol, "status": "failed", "outcome": {"outcome": "access_denied"}}
                    for symbol in symbols
                ],
            }

        with patch("libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=denied):
            self.assertFalse(self.update()["complete"])
        with patch(
            "libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=AssertionError("blocked retry")
        ):
            result = self.update()
        self.assertFalse(result["complete"])
        self.assertEqual(len(result["plan"]["deferred"]), 2)
        self.assertTrue(self.update(retry_blocked=True)["complete"])

    def test_interrupted_identity_replacement_reuses_published_evidence_contract(self):
        self.records[:] = [asset("AAA", "replacement-id")]
        self.identity = CatalogueClient(self.records)
        with patch(
            "libs.alpaca_lineage_build.universe_update.run_lineage",
            side_effect=OSError("interrupted before lineage"),
        ):
            with self.assertRaisesRegex(OSError, "interrupted before lineage"):
                self.update()
        self.pages.clear()
        result = self.update()
        self.assertTrue(result["complete"], result)
        self.assertTrue(all(page["start"][:10] >= "2026-10-16" for page in self.pages))
        self.assertEqual(result["stages"]["acquisition"]["results"][0]["status"], "covered")

    def test_changed_metadata_on_newer_group_is_incomplete_without_rewind(self):
        self.records[:] = [{**asset("AAA"), "exchange": "TXSE"}]
        before = snapshot(self.store / "end_of_day/pricing/AAA")
        result = self.update("2026-10-16")
        self.assertFalse(result["complete"])
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing/AAA"))
        self.assertNotEqual(universe_state.read_state(self.store)["work"]["AAA"]["status"], "complete")
        self.assertFalse(self.pages)

    def test_catalogue_failure_cannot_register_or_report_complete_discovery(self):
        self.records[:] = [{"symbol": "BAD"}]
        before = snapshot(self.directory)
        execute = self.identity.execute_plan

        def observe_after_recovery(plan):
            recovered.assert_called_once_with(self.store.absolute())
            return execute(plan)

        with (
            patch(
                "libs.alpaca_lineage_build.universe_update.recover_publications", wraps=recover_publications
            ) as recovered,
            patch.object(self.identity, "execute_plan", side_effect=observe_after_recovery),
            self.assertRaisesRegex(LineageError, "catalogue record"),
        ):
            self.update()
        self.assertEqual(before, snapshot(self.directory))
        self.assertFalse(self.pages)

    def test_interrupted_state_initialization_can_be_repeated_without_losing_work(self):
        path = self.store / universe_state.STATE_FILE
        path.unlink()
        with patch.object(universe_state.os, "replace", side_effect=OSError("interrupted")):
            with self.assertRaisesRegex(OSError, "interrupted"):
                universe_state.initialize(self.store)
        self.assertFalse(path.exists())
        self.assertEqual(universe_state.initialize(self.store)["work"], {})


class RecoveryRegistryTests(unittest.TestCase):
    def test_catalogue_retains_inactive_records_without_names(self):
        records = [asset("AAA"), {**asset("OLD"), "status": "inactive", "name": ""}]
        observation = observe_catalogue(CatalogueClient(records))
        self.assertEqual(observation["records"], records)
        self.assertEqual(discovery_labels(observation), {"AAA"})

    def test_concurrent_unions_preserve_every_addition(self):
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / "ticker.csv"
            registry.write_text("HIST\n")
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda s: publish_union([s], registry), ["AAA", "BBB", "CCC", "AAA"]))
            self.assertEqual(load_labels(registry), ("AAA", "BBB", "CCC", "HIST"))

    def test_catalogue_need_has_no_exchange_filter_and_preserves_all_statuses(self):
        plan = build_plan({"steps": [{"needs": [{"kind": "asset_catalogue", "why": "discovery"}]}]})
        self.assertEqual(plan["steps"][0]["actions"][0]["request"]["params"], {"asset_class": "us_equity"})
        observation = observe_catalogue(
            CatalogueClient([{**asset("AAA"), "tradable": False}, {**asset("OLD"), "status": "inactive"}])
        )
        self.assertEqual(discovery_labels(observation), {"AAA"})

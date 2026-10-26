"""Canonical dependency registration and offline producer-status acceptance."""

import io
import json
import unittest
from contextlib import ExitStack, closing, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import ActionCatalogueClient, fixture, rename, seed_group
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs import alpaca_cli
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import check_summary, update_checks
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_recovery_registry import load_labels
from libs.common.sqlite_connections import connect_sqlite


class CommandCoverageTests(unittest.TestCase):
    def invoke(self, case, *arguments, store=None):
        output, errors = io.StringIO(), io.StringIO()
        with (
            patch.object(alpaca_cli, "DEFAULT_REGISTRY", case.registry),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            code = alpaca_cli.main([*arguments, "--store", str(store or case.store)])
        return code, json.loads(output.getvalue()) if output.getvalue() else None, errors.getvalue()

    def identity(self):
        dep = {**asset("DEP"), "exchange": "BATS"}
        return IdentityClient(
            {"AAA": asset("AAA"), "DEP": dep, "111111111": dep},
            [{**rename("AAA", "DEP"), "process_date": "2026-10-01"}],
        )

    def assert_registered(self, case, symbol):
        self.assertIn(symbol, load_labels(case.registry))
        self.assertNotIn(symbol, universe_state.read_state(case.store)["registration_pending"])

    def test_lineage_update_and_refresh_register_dependencies_before_request(self):
        for operation in ("update", "refresh"):
            with self.subTest(operation=operation), fixture() as case:
                if operation == "update":
                    download_pricing(
                        case.store,
                        "AAA",
                        "2026-10-17",
                        client=FakeClient([bar("2016-01-04", "50"), case.old[-1]]),
                        refresh_all=True,
                    )
                pricing, identity = FakeClient(case.old), self.identity()
                original = pricing.fetch

                def checked(symbol, *args, **kwargs):
                    self.assert_registered(case, symbol)
                    return original(symbol, *args, **kwargs)

                with (
                    patch.object(alpaca_cli, "clients", return_value=(pricing, identity)),
                    patch.object(pricing, "fetch", side_effect=checked),
                ):
                    code, result, errors = self.invoke(
                        case, "lineage", operation, "--symbol", "AAA", "--through", "2026-10-16"
                    )
                self.assertEqual(code, 0, (result, errors))
                self.assertIn("DEP", {request[0] for request in pricing.calls})
                self.assertEqual(len(read_pricing_views(case.store, "DEP")["raw"]), 2)

    def test_pricing_refresh_registers_new_lineage_dependency_before_request(self):
        with fixture() as case:
            corrected = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
            case.live = {"AAA": corrected, "DEP": corrected}
            dep = {**asset("DEP"), "exchange": "BATS"}
            identity = ActionCatalogueClient(
                [asset("AAA"), dep],
                [{**rename("AAA", "DEP"), "process_date": "2026-10-01"}],
                exact={"AAA": asset("AAA"), "DEP": dep, "111111111": dep},
            )

            def checked(params):
                for symbol in params["symbols"].split(","):
                    self.assert_registered(case, symbol)
                return case.page(params)

            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, identity)),
                patch.object(case.pricing, "_get", side_effect=checked),
            ):
                code, result, errors = self.invoke(case, "pricing", "refresh")
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual(read_pricing_views(case.store, "DEP")["all"][0][4], "50")
            self.assertFalse(universe_state.read_state(case.store)["registration_pending"])

    def test_registration_precedes_existing_group_catchup(self):
        with fixture() as case:
            store = seed_group(case, end="2026-10-15")
            download_pricing(store, "BBB", "2026-10-17", client=FakeClient(case.old))
            case.registry.write_text("HIST\n")
            case.live["BBB"] = case.old

            def checked(params):
                self.assertTrue({"AAA", "BBB"}.issubset(load_labels(case.registry)))
                self.assertFalse(universe_state.read_state(store)["registration_pending"])
                return case.page(params)

            identity = IdentityClient(
                {"AAA": asset("AAA"), "BBB": asset("BBB"), "111111111": asset("BBB")}, [rename()]
            )
            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, identity)),
                patch.object(case.pricing, "_get", side_effect=checked),
                patch.object(universe_state, "register", wraps=universe_state.register) as registration,
            ):
                code, result, errors = self.invoke(
                    case, "lineage", "update", "--symbol", "BBB", "--through", "2026-10-16", store=store
                )
            self.assertEqual(code, 0, (result, errors))
            self.assertTrue(case.pages)
            self.assertEqual(registration.call_count, 1)

    def test_disabled_acquisition_still_registers_discovered_dependency(self):
        with fixture() as case:
            pricing = FakeClient(case.old)
            with patch.object(alpaca_cli, "clients", return_value=(pricing, self.identity())):
                code, result, errors = self.invoke(
                    case,
                    "lineage",
                    "refresh",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    "--no-pricing-acquisition",
                )
            self.assertEqual(code, 1, (result, errors))
            self.assertFalse(pricing.calls)
            self.assert_registered(case, "DEP")
            self.assertIn("waiting_for_pricing", {item["status"] for item in result["results"]})

    def test_selected_pricing_refresh_registers_before_replacement(self):
        with fixture() as case:
            case.registry.write_text("HIST\n")
            case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]

            def checked(params):
                self.assert_registered(case, "AAA")
                return case.page(params)

            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, case.identity)),
                patch.object(case.pricing, "_get", side_effect=checked),
            ):
                code, result, errors = self.invoke(case, "pricing", "refresh", "--symbol", "AAA")
            self.assertEqual(code, 0, (result, errors))
            self.assertTrue(case.pages)
            self.assertEqual(read_pricing_views(case.store, "AAA")["raw"][0][4], "50")
            _, report = self.inspect_without_writes(case)
            self.assertIn("AAA", report["scheduling"]["checks"])

    def test_registered_unchanged_lineage_does_not_republish_registration(self):
        with fixture() as case:
            original = universe_state.read_state(case.store)
            registry = case.registry.read_bytes()
            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, case.identity)),
                patch.object(universe_state, "register", side_effect=AssertionError("redundant registration")),
            ):
                code, result, errors = self.invoke(case, "lineage", "update", "--symbol", "AAA")
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual(universe_state.read_state(case.store), original)
            self.assertEqual(case.registry.read_bytes(), registry)

    def test_explicit_initialization_has_no_lineage_or_summary_prerequisite(self):
        with fixture() as case:
            case.store = case.directory / "bootstrap"
            with patch.object(alpaca_cli, "clients", side_effect=AssertionError("network")):
                code, result, errors = self.invoke(case, "universe", "rebuild")
            self.assertEqual(code, 0, (result, errors))
            self.assertFalse((case.store / "end_of_day").exists())
            pricing = FakeClient(case.old)
            with patch.object(alpaca_cli, "clients", return_value=(pricing, self.identity())):
                code, result, errors = self.invoke(
                    case, "lineage", "update", "--symbol", "AAA", "--through", "2026-10-16"
                )
                self.assertEqual(code, 0, (result, errors))
                code, result, errors = self.invoke(case, "lineage", "update", "--through", "2026-10-16")
                self.assertEqual(code, 0, (result, errors))
            code, result, errors = self.invoke(case, "summary", "rebuild")
            self.assertEqual(code, 0, (result, errors))
            with patch.object(alpaca_cli, "clients", side_effect=AssertionError("network")):
                code, result, errors = self.invoke(case, "catalogue", "bootstrap")
            self.assertEqual(code, 0, (result, errors))
            code, result, errors = self.invoke(case, "status")
            self.assertEqual(code, 0, (result, errors))
            self.assertTrue(result["ready_for_update"])
            self.assertTrue(result["processing_complete"])

    def test_missing_work_state_fails_before_dependency_requests(self):
        with fixture() as case:
            (case.store / universe_state.STATE_FILE).unlink()
            pricing, identity = FakeClient(case.old), self.identity()
            before = snapshot(case.store)
            registry = case.registry.read_bytes()
            with patch.object(alpaca_cli, "clients", return_value=(pricing, identity)):
                code, _, errors = self.invoke(case, "lineage", "refresh", "--symbol", "AAA")
            self.assertEqual(code, 1)
            self.assertIn("universe rebuild", errors)
            self.assertFalse(pricing.calls)
            self.assertEqual(identity.request_count, 0)
            self.assertEqual(snapshot(case.store), before)
            self.assertEqual(case.registry.read_bytes(), registry)

    def test_explicit_rebuild_preserves_intentions_and_rejects_incompatible_state(self):
        with fixture() as case:
            universe_state.put_work(case.store, "AAA", {"status": "blocked", "outcome": "access_denied"})
            with (
                patch.object(universe_state, "publish_union", side_effect=OSError("interrupted registration")),
                self.assertRaises(OSError),
            ):
                universe_state.register(case.store, ["DEP"], registry=case.registry)
            before = snapshot(case.store)
            state = universe_state.read_state(case.store)
            with patch.object(alpaca_cli, "clients", side_effect=AssertionError("network")):
                code, result, errors = self.invoke(case, "universe", "rebuild")
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual((result["pending_work"], result["registration_pending"]), (1, 1))
            self.assertEqual(universe_state.read_state(case.store), state)
            self.assertEqual(snapshot(case.store), before)
            with closing(connect_sqlite(case.store / universe_state.STATE_FILE)) as db, db:
                db.execute("PRAGMA user_version=999")
            before = snapshot(case.store)
            code, _, errors = self.invoke(case, "universe", "rebuild")
            self.assertEqual(code, 1)
            self.assertIn("incompatible universe work state", errors)
            self.assertEqual(snapshot(case.store), before)

    def test_registration_interruption_recovers_before_dependency_acquisition(self):
        for boundary in ("before_union", "after_union"):
            with self.subTest(boundary=boundary), fixture() as case:
                pricing, identity = FakeClient(case.old), self.identity()
                original = universe_state.publish_union

                def interrupted(labels, registry):
                    if boundary == "after_union":
                        original(labels, registry)
                    raise OSError("interrupted registration")

                with (
                    patch.object(alpaca_cli, "clients", return_value=(pricing, identity)),
                    patch.object(universe_state, "publish_union", side_effect=interrupted),
                ):
                    code, _, _ = self.invoke(
                        case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16"
                    )
                self.assertEqual(code, 1)
                self.assertFalse(pricing.calls)
                self.assertIn("DEP", universe_state.read_state(case.store)["registration_pending"])
                original_fetch = pricing.fetch

                def checked(symbol, *args, **kwargs):
                    self.assert_registered(case, symbol)
                    return original_fetch(symbol, *args, **kwargs)

                with (
                    patch.object(alpaca_cli, "clients", return_value=(pricing, identity)),
                    patch.object(pricing, "fetch", side_effect=checked),
                ):
                    code, result, errors = self.invoke(
                        case, "lineage", "refresh", "--symbol", "AAA", "--through", "2026-10-16"
                    )
                self.assertEqual(code, 0, (result, errors))
                self.assertIn("DEP", load_labels(case.registry))

    def inspect_without_writes(self, case):
        before = snapshot(case.store)
        calls = []

        def readonly(path, **kwargs):
            self.assertTrue(kwargs.get("readonly"), str(path))
            self.assertFalse(kwargs.get("create"), str(path))
            calls.append(Path(path).name)
            return connect_sqlite(path, **kwargs)

        with ExitStack() as guards:
            guards.enter_context(patch.object(alpaca_cli, "clients", side_effect=AssertionError("network")))
            guards.enter_context(patch.object(Path, "iterdir", side_effect=AssertionError("directory crawl")))
            guards.enter_context(patch.object(storage, "read_receipt", side_effect=AssertionError("receipt")))
            guards.enter_context(
                patch.object(pricing_storage, "load_source", side_effect=AssertionError("source"))
            )
            guards.enter_context(
                patch.object(storage, "recover", side_effect=AssertionError("lineage recovery"))
            )
            guards.enter_context(
                patch.object(pricing_storage, "recover", side_effect=AssertionError("pricing recovery"))
            )
            for module in (storage, universe_state, check_summary, update_checks):
                guards.enter_context(patch.object(module, "connect_sqlite", side_effect=readonly))
            code, report, errors = self.invoke(case, "status")
        self.assertEqual(snapshot(case.store), before)
        records = [json.loads(line) for line in errors.splitlines()]
        self.assertEqual(records[0]["phase"], "INIT")
        self.assertEqual(records[-1]["phase"], "FINAL_EXIT")
        self.assertEqual(records[-1]["exit_code"], code)
        self.assertEqual(records[-1]["last_phase"], "STATUS")
        self.assertLessEqual(len(calls), 4)
        return code, report

    def test_status_exposes_independent_producer_pending_and_missing_scheduling(self):
        with fixture() as case:
            download_pricing(case.store, "AAA", "2026-10-20", client=FakeClient(case.old + [bar("2026-10-19")]))
            code, report = self.inspect_without_writes(case)
            self.assertEqual(code, 1)
            self.assertEqual(set(report["scheduling"]["checks"]), {"AAA"})
            self.assertFalse(report["processing_complete"])
            self.assertTrue(report["ready_for_update"])
            self.assertFalse(report["universe"]["work"])
            (case.store / update_checks.CHECK_FILE).unlink()
            code, report = self.inspect_without_writes(case)
            self.assertEqual(code, 1)
            self.assertFalse(report["scheduling"]["complete"])
            self.assertFalse(report["ready_for_update"])
            self.assertFalse(report["pricing_publications"]["selection_coverage_complete"])

    def test_status_exposes_summary_pending_owners_even_with_incomplete_coverage(self):
        with fixture() as case:
            check_summary.begin_rebuild(case.store)
            check_summary.invalidate(case.store, ["AAA"], owner="lineage")
            code, report = self.inspect_without_writes(case)
            self.assertEqual(code, 1)
            self.assertFalse(report["summary"]["coverage_complete"])
            self.assertEqual(report["summary"]["pending_inputs"], [{"symbol": "AAA", "owner": "lineage"}])
            self.assertFalse(report["ready_for_update"])

    def test_status_missing_boundaries_remain_unknown_without_reconstruction(self):
        for boundary in ("store", "lineage", "projection", "summary", "universe"):
            with self.subTest(boundary=boundary), fixture() as case:
                if boundary == "store":
                    case.store = case.directory / "absent"
                elif boundary == "projection":
                    with closing(connect_sqlite(case.store / "end_of_day/lineage/index.sqlite3")) as db, db:
                        db.execute("UPDATE projection_state SET complete=0")
                else:
                    path = {
                        "lineage": "end_of_day/lineage/index.sqlite3",
                        "summary": check_summary.summary_path(case.store),
                        "universe": universe_state.STATE_FILE,
                    }[boundary]
                    (case.store / path).unlink()
                code, report = self.inspect_without_writes(case)
                self.assertEqual(code, 1)
                self.assertFalse(report["processing_complete"])
                self.assertFalse(report["ready_for_update"])
                self.assertTrue(report["blockers"])
                if boundary == "projection":
                    self.assertIn("lineage migrate-index", " ".join(report["blockers"]))

    def test_status_exposes_real_pricing_and_lineage_journals_without_recovery(self):
        for owner in ("pricing", "lineage"):
            with self.subTest(owner=owner), fixture() as case:
                if owner == "pricing":
                    replace = pricing_storage.os.replace

                    def interrupt(source, target):
                        if Path(target).suffix == ".gz":
                            raise OSError("publication interrupted")
                        return replace(source, target)

                    with (
                        patch.object(pricing_storage.os, "replace", side_effect=interrupt),
                        self.assertRaises(OSError),
                    ):
                        download_pricing(
                            case.store, "AAA", "2026-10-20", client=FakeClient(case.old + [bar("2026-10-19")])
                        )
                else:
                    download_pricing(
                        case.store, "AAA", "2026-10-20", client=FakeClient(case.old + [bar("2026-10-19")])
                    )
                    with patch.object(storage, "_install", side_effect=OSError("publication interrupted")):
                        self.assertEqual(
                            run_lineage(case.store, identity_client=IdentityClient({"AAA": asset("AAA")}))[
                                "failed"
                            ],
                            1,
                        )
                code, report = self.inspect_without_writes(case)
                self.assertEqual(code, 1)
                self.assertFalse(report["processing_complete"])
                if owner == "pricing":
                    self.assertEqual(report["pricing_publications"]["pending_symbols"], ["AAA"])
                else:
                    self.assertTrue(report["lineage"]["publication_pending"])

    def test_status_distinguishes_empty_and_unresolved_from_unfinished_processing(self):
        with fixture() as case:
            for symbol in ("EMPTY", "UNKNOWN"):
                download_pricing(
                    case.store, symbol, "2026-10-17", client=FakeClient([] if symbol == "EMPTY" else case.old)
                )
            result = run_lineage(
                case.store, identity_client=IdentityClient({"AAA": asset("AAA"), "EMPTY": asset("EMPTY")})
            )
            self.assertEqual(result["failed"], 0, result)
            code, report = self.inspect_without_writes(case)
            self.assertEqual(code, 1)
            self.assertFalse(report["processing_complete"])
            self.assertEqual(report["serving_catalogue"]["pending_symbols"], ["EMPTY", "UNKNOWN"])
            code, result, errors = self.invoke(case, "catalogue", "update")
            self.assertEqual(code, 0, (result, errors))
            code, report = self.inspect_without_writes(case)
            self.assertEqual(code, 0)
            self.assertTrue(report["processing_complete"])
            self.assertEqual(
                report["lineage"]["published_histories"],
                {
                    "nonempty": 2,
                    "empty": 1,
                    "with_unresolved_rows": 2,
                    "with_all_rows_assigned": 0,
                    "unknown_projection": 0,
                },
            )

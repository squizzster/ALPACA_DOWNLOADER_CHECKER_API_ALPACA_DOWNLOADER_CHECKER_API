"""Accepted coverage, dependency recovery and canonical command restrictions."""

import io
import json
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_durable_maintenance import (
    ActionCatalogueClient,
    ProcessInterrupted,
    fixture,
    rename,
    seed_group,
)
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot
from test_alpaca_universe_update import CatalogueClient
from test_alpaca_update_obligation_recovery import source

from libs import alpaca_cli
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.universe_update import run_update
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_recovery_registry import load_labels
from libs.common.sqlite_connections import connect_sqlite


def invoke(case, *arguments, pricing=None, identity=None):
    output, errors = io.StringIO(), io.StringIO()
    with (
        patch.object(alpaca_cli, "DEFAULT_REGISTRY", case.registry),
        patch.object(alpaca_cli, "clients", return_value=(pricing or case.pricing, identity or case.identity)),
        redirect_stdout(output),
        redirect_stderr(errors),
    ):
        code = alpaca_cli.main([*arguments, "--store", str(case.store)])
    return code, json.loads(output.getvalue()) if output.getvalue() else None, errors.getvalue()


def dependency_identity():
    dep = {**asset("DEP"), "exchange": "BATS"}
    return IdentityClient(
        {"AAA": asset("AAA"), "DEP": dep, "111111111": dep},
        [{**rename("AAA", "DEP"), "process_date": "2026-10-01"}],
    )


class Phase4FinalizationTests(unittest.TestCase):
    def test_initial_lineage_registration_cannot_lose_unpriced_seed(self):
        with fixture() as case:
            original = universe_state.register

            def interrupted(store, labels=(), **kwargs):
                labels = list(labels)
                result = original(store, labels, **kwargs)
                if "NEW" in labels:
                    raise ProcessInterrupted()
                return result

            with (
                patch.object(universe_state, "register", side_effect=interrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                invoke(case, "lineage", "update", "--symbol", "NEW", "--through", "2026-10-16")
            self.assertIn("NEW", universe_state.read_state(case.store)["work"])
            case.live["NEW"] = list(case.old)
            case.identity = CatalogueClient([asset("AAA")])
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(universe_state.read_state(case.store)["work"]["NEW"]["status"], "complete")

    def test_pricing_refresh_acknowledges_its_owned_publications(self):
        for selected in (False, True):
            with self.subTest(selected=selected), fixture() as case:
                case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
                code, result, errors = invoke(
                    case, "pricing", "refresh", *(["--symbol", "AAA"] if selected else [])
                )
                self.assertEqual(code, 0, (result, errors))
                if selected:
                    self.assertNotEqual(
                        universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete"
                    )
                    with patch.object(
                        case.pricing, "_get", side_effect=AssertionError("completed replacement repeated")
                    ):
                        code, result, errors = invoke(case, "lineage", "update", "--through", "2026-10-16")
                    self.assertEqual(code, 0, (result, errors))
                self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")

    def test_correction_approval_survives_inactivity_observed_during_lineage(self):
        for standalone in (False, True):
            with self.subTest(standalone=standalone), fixture() as case:
                case.identity = CatalogueClient(
                    [{**asset("OTHER"), "status": "inactive"}],
                    exact={"AAA": {**asset("AAA"), "status": "inactive"}},
                )
                case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
                if standalone:
                    code, result, errors = invoke(case, "pricing", "refresh")
                    self.assertEqual(code, 0, (result, errors))
                else:
                    result = case.update()
                    self.assertTrue(result["complete"], result)
                work = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(work["status"], "complete")
                self.assertEqual(source(case)["history_correction_id"], work["correction_id"])
                entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
                self.assertEqual(entry["projection"]["update_obligation_id"], work["obligation_id"])
                self.assertEqual(
                    entry["projection"]["last_observed_symbol_lookup"]["asset"]["status"], "inactive"
                )
                with patch.object(
                    case.pricing, "_get", side_effect=AssertionError("completed correction repeated")
                ):
                    self.assertTrue(case.update()["complete"])

    def test_standalone_lineage_reports_remaining_accepted_coverage(self):
        with fixture() as case:
            case.identity = CatalogueClient([asset("AAA")])
            with (
                patch(
                    "libs.alpaca_lineage_build.universe_update.update_everything",
                    side_effect=ProcessInterrupted(),
                ),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-20")
            code, result, errors = invoke(
                case, "lineage", "update", "--symbol", "AAA", "--through", "2026-10-19"
            )
            self.assertEqual(code, 1, (result, errors))
            self.assertFalse(result["complete"])
            self.assertEqual(result["pending_work"], ["AAA"])
            self.assertEqual(
                universe_state.read_state(case.store)["work"]["AAA"]["pricing_end_exclusive"], "2026-10-20"
            )

    def test_standalone_acknowledges_completed_dependency_publication_without_acquisition(self):
        with fixture() as case:
            pricing, identity = FakeClient(case.old), dependency_identity()
            with (
                patch.object(universe_state, "record_outcome", side_effect=ProcessInterrupted()),
                self.assertRaises(ProcessInterrupted),
            ):
                invoke(
                    case,
                    "lineage",
                    "refresh",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    pricing=pricing,
                    identity=identity,
                )
            with patch.object(pricing, "fetch", side_effect=AssertionError("repeat acquisition")):
                code, result, errors = invoke(
                    case,
                    "lineage",
                    "update",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    pricing=pricing,
                    identity=identity,
                )
            self.assertEqual(code, 0, (result, errors))
            self.assertFalse(result["pending_work"])
            self.assertTrue(
                all(w["status"] == "complete" for w in universe_state.read_state(case.store)["work"].values())
            )

    def test_unrelated_work_and_due_transport_retry_remain_usable(self):
        with fixture() as case:
            universe_state.put_work(
                case.store, "UNRELATED", {"status": "blocked", "end_exclusive": "2026-10-17"}
            )
            universe_state.put_work(
                case.store,
                "AAA",
                {
                    "status": "retry",
                    "end_exclusive": "2026-10-17",
                    "retry_after_utc": "2000-01-01",
                    "obligation_id": "accepted-transport-retry",
                    "refresh_metadata": True,
                },
            )
            download_pricing(
                case.store,
                "AAA",
                "2026-10-17",
                client=FakeClient([bar("2016-01-04", "50"), case.old[-1]]),
                refresh_all=True,
            )
            code, result, errors = invoke(
                case, "lineage", "update", "--symbol", "AAA", "--through", "2026-10-16"
            )
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")
            self.assertEqual(universe_state.read_state(case.store)["work"]["UNRELATED"]["status"], "blocked")

    def test_pricing_acquisition_failure_remains_blocked_and_bootstrap_is_explicit(self):
        with fixture() as case:
            (case.store / universe_state.STATE_FILE).unlink()
            with patch(
                "libs.alpaca_pricing_download.command_line.BarsClient",
                side_effect=AssertionError("uninitialized request"),
            ):
                code, _, errors = invoke(
                    case, "pricing", "acquire", "--symbol", "DEP", "--through", "2026-10-16"
                )
            self.assertEqual(code, 1)
            self.assertIn("universe rebuild", errors)
            self.assertEqual(invoke(case, "universe", "rebuild")[0], 0)
            pricing = FakeClient(case.old)
            with (
                patch("libs.alpaca_pricing_download.command_line.BarsClient", return_value=pricing),
                patch.object(
                    pricing, "fetch", side_effect=HTTPError("https://invalid", 403, "denied", {}, None)
                ) as requests,
            ):
                self.assertEqual(
                    invoke(case, "pricing", "acquire", "--symbol", "DEP", "--through", "2026-10-16")[0], 1
                )
                count = requests.call_count
                self.assertGreater(count, 0)
                self.assertEqual(
                    invoke(case, "pricing", "acquire", "--symbol", "DEP", "--through", "2026-10-16")[0], 1
                )
                self.assertEqual(requests.call_count, count)
            self.assertEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "blocked")

    def test_restriction_checks_keep_independent_group_limits_separate(self):
        with fixture() as case:
            entries = {f"S{i}": {"dependencies": [], "asset_ids": []} for i in range(250)}
            policy = WorkPolicy(case.store, {"symbols": entries}, {}, [{"symbol": "BLOCKED"}], "2026-10-17")
            self.assertTrue(policy.allowed(entries))
            self.assertFalse(policy.allowed([*entries, "BLOCKED"]))

    def test_previous_lineage_execution_versions_remain_readable_without_republication(self):
        from alpaca_end_of_day import StandaloneReader

        with fixture() as case:
            for version in (12, 13, 14, 15, 16, 17, 18):
                with closing(connect_sqlite(case.store / "end_of_day/lineage/index.sqlite3")) as db, db:
                    db.execute(f"PRAGMA user_version={version}")
                before = snapshot(case.store)
                self.assertTrue(StandaloneReader(case.store).resolve_ticker("AAA"))
                self.assertEqual(snapshot(case.store), before)

    def test_maintenance_target_survives_every_checkpoint_and_later_inactivity(self):
        for boundary in (
            "universe_update.update_everything",
            "refresh.verify",
            "update.append_ready",
            "universe_state.record_outcome",
        ):
            for changed in (False, True):
                with self.subTest(boundary=boundary, changed=changed), fixture() as case:
                    current = {**asset("AAA"), **({"exchange": "TXSE"} if changed else {})}
                    case.identity = CatalogueClient([current])
                    case.live["AAA"] = [*case.old, bar("2026-10-19")]
                    with (
                        patch("libs.alpaca_lineage_build." + boundary, side_effect=ProcessInterrupted()),
                        self.assertRaises(ProcessInterrupted),
                    ):
                        case.update("2026-10-20")
                    accepted = universe_state.read_state(case.store)["work"]["AAA"]
                    self.assertEqual(accepted["pricing_end_exclusive"], "2026-10-20")
                    case.identity = CatalogueClient([{**current, "status": "inactive"}])
                    result = case.update("2026-10-20")
                    self.assertTrue(result["complete"], result)
                    self.assertEqual(source(case)["latest_request"]["end_exclusive"][:10], "2026-10-20")

    def test_older_retry_defers_accepted_coverage_even_with_override_and_omission(self):
        with fixture() as case:
            case.identity = CatalogueClient([asset("AAA")])
            with (
                patch(
                    "libs.alpaca_lineage_build.universe_update.update_everything",
                    side_effect=ProcessInterrupted(),
                ),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update("2026-10-20")
            case.identity = CatalogueClient([{**asset("OTHER"), "status": "inactive"}])
            before = source(case)
            case.pages.clear()
            result = case.update("2026-10-17", retry_blocked=True)
            self.assertFalse(result["complete"])
            self.assertFalse(case.pages)
            self.assertEqual(source(case), before)
            self.assertEqual(
                universe_state.read_state(case.store)["work"]["AAA"]["pricing_end_exclusive"], "2026-10-20"
            )
            case.live["AAA"] = [*case.old, bar("2026-10-19")]
            result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)

    def test_standalone_dependency_denial_is_durable_and_visible_after_relationship_disappears(self):
        with fixture() as case:
            pricing = FakeClient(case.old)
            fetch = pricing.fetch

            def deny(symbol, *args, **kwargs):
                if symbol == "DEP":
                    self.assertIn(symbol, load_labels(case.registry))
                    work = universe_state.read_state(case.store)["work"]
                    self.assertEqual(set(work["DEP"]["required_group"]), {"AAA", "DEP"})
                    raise HTTPError("https://invalid", 403, "denied dependency", {}, None)
                return fetch(symbol, *args, **kwargs)

            with patch.object(pricing, "fetch", side_effect=deny):
                code, result, errors = invoke(
                    case,
                    "lineage",
                    "refresh",
                    "--symbol",
                    "AAA",
                    "--through",
                    "2026-10-16",
                    pricing=pricing,
                    identity=dependency_identity(),
                )
            self.assertEqual(code, 1, (result, errors))
            self.assertEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "blocked")
            before = snapshot(case.store)
            code, result, errors = invoke(case, "status")
            self.assertEqual(code, 1, errors)
            self.assertFalse(result["processing_complete"])
            self.assertIn("DEP", result["universe"]["work"])
            self.assertEqual(snapshot(case.store), before)
            case.identity = CatalogueClient([asset("AAA")])
            case.pages.clear()
            result = case.update()
            self.assertFalse(result["complete"], result)
            self.assertFalse(case.pages)
            case.live["DEP"] = list(case.old)
            result = case.update(retry_blocked=True)
            self.assertTrue(result["complete"], result)
            self.assertEqual(
                set(storage.read_index(case.store / "end_of_day/lineage")["symbols"]), {"AAA", "DEP"}
            )
            self.assertTrue(
                all(
                    item["status"] == "complete"
                    for item in universe_state.read_state(case.store)["work"].values()
                )
            )

    def test_dependency_intention_survives_registration_and_pricing_interruptions(self):
        for command in ("daily", "standalone"):
            for boundary in ("registration", "acquisition", "lineage"):
                with self.subTest(command=command, boundary=boundary), fixture() as case:
                    dep = {**asset("DEP"), "exchange": "BATS"}
                    identity = ActionCatalogueClient(
                        [{**asset("AAA"), "exchange": "TXSE"}, dep],
                        [rename("AAA", "DEP")],
                        exact={"AAA": asset("AAA"), "DEP": dep, "111111111": dep},
                    )
                    case.live["DEP"] = list(case.old)
                    register = universe_state.register

                    def interrupt_registration(store, labels=(), **kwargs):
                        labels = list(labels)
                        result = register(store, labels, **kwargs)
                        if "DEP" in labels:
                            raise ProcessInterrupted()
                        return result

                    publish = storage.publish

                    def interrupt_publication(*args, **kwargs):
                        if "DEP" in args[3]:
                            raise ProcessInterrupted()
                        return publish(*args, **kwargs)

                    target, replacement = {
                        "registration": (
                            "libs.alpaca_lineage_build.universe_state.register",
                            interrupt_registration,
                        ),
                        "acquisition": (
                            "libs.alpaca_lineage_build.pipeline.download_pricing",
                            ProcessInterrupted(),
                        ),
                        "lineage": ("libs.alpaca_lineage_build.storage.publish", interrupt_publication),
                    }[boundary]
                    with patch(target, side_effect=replacement), self.assertRaises(ProcessInterrupted):
                        if command == "daily":
                            run_update(
                                case.store,
                                "2026-10-17",
                                pricing_client=case.pricing,
                                identity_client=identity,
                                registry=case.registry,
                            )
                        else:
                            invoke(
                                case,
                                "lineage",
                                "refresh",
                                "--symbol",
                                "AAA",
                                "--through",
                                "2026-10-16",
                                identity=identity,
                            )
                    work = universe_state.read_state(case.store)["work"]
                    self.assertIn("DEP", work)
                    self.assertIn("AAA", work["DEP"]["required_group"])
                    case.identity = CatalogueClient([asset("AAA")])
                    result = case.update()
                    self.assertTrue(result["complete"], result)
                    self.assertEqual(source(case, "DEP")["latest_request"]["end_exclusive"][:10], "2026-10-17")

    def test_canonical_commands_cannot_bypass_blocked_siblings_or_cutoffs(self):
        for restriction in ("blocked", "delay", "cutoff"):
            for command in (
                ("lineage", "update"),
                ("lineage", "refresh"),
                ("pricing", "refresh"),
                ("pricing", "check"),
                ("pricing", "acquire"),
            ):
                with self.subTest(restriction=restriction, command=command), fixture() as case:
                    case.store = seed_group(case)
                    universe_state.put_work(
                        case.store,
                        "BBB",
                        {
                            "status": "blocked" if restriction == "blocked" else "retry",
                            "end_exclusive": "2099-01-01" if restriction == "cutoff" else "2026-10-17",
                            "retry_after_utc": "2099-01-01" if restriction == "delay" else None,
                        },
                    )
                    download_pricing(
                        case.store,
                        "AAA",
                        "2026-10-17",
                        client=FakeClient([bar("2016-01-04", "50"), case.old[-1]]),
                        refresh_all=True,
                    )
                    with (
                        patch.object(
                            case.pricing, "_get", side_effect=AssertionError("blocked pricing request")
                        ),
                        patch.object(
                            case.identity, "execute_plan", side_effect=AssertionError("blocked identity request")
                        ),
                        patch("libs.alpaca_pricing_download.command_line.BarsClient", return_value=case.pricing),
                    ):
                        args = [*command, "--symbol", "AAA"]
                        if command[0] == "lineage" or command[1] == "acquire":
                            args += ["--through", "2026-10-16"]
                        code, result, errors = invoke(case, *args)
                    self.assertEqual(code, 1, (result, errors))
                    self.assertEqual(
                        universe_state.read_state(case.store)["work"]["BBB"]["status"],
                        "blocked" if restriction == "blocked" else "retry",
                    )


if __name__ == "__main__":
    unittest.main()

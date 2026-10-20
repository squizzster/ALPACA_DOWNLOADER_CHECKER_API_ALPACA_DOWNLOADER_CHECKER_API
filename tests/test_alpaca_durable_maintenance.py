"""Interrupted obligations, restricted groups and retained-history maintenance."""

import copy
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from urllib.error import HTTPError

import test_alpaca_universe_update as universe_fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.universe_update import rebuild_universe, run_update
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient


class ProcessInterrupted(BaseException):
    """Stop the process without entering an ordinary retryable-error handler."""


@contextmanager
def fixture():
    case = universe_fixture.UniverseUpdateTests()
    case.setUp()
    try:
        yield case
    finally:
        case.doCleanups()


def rename(left="AAA", right="BBB"):
    return {
        "action_type": "name_change",
        "id": "rename",
        "process_date": "2026-09-01",
        "old_symbol": left,
        "new_symbol": right,
        "old_cusip": "111111111",
        "new_cusip": "111111111",
    }


class ActionCatalogueClient(CatalogueClient):
    def __init__(self, records, actions=(), exact=None):
        super().__init__(records, exact=exact)
        self.actions = list(actions)

    def execute_plan(self, plan):
        result = []
        for step in plan["steps"]:
            values = []
            for action in step["actions"]:
                if action["strategy"] == "corporate_actions_batch":
                    self.request_count += 1
                    params = action["request"]["params"]
                    labels = set(params["symbols"].split(","))
                    values.append(
                        [
                            copy.deepcopy(a)
                            for a in self.actions
                            if labels.intersection(v for k, v in a.items() if k.endswith("symbol"))
                            and params["start"] <= a["process_date"] <= params["end"]
                        ]
                    )
                else:
                    values.append(super().execute_plan({**plan, "steps": [{"actions": [action]}]})[0][0])
            result.append(values)
        return result


def seed_group(case, *, inactive_second=False, end="2026-10-17"):
    store = case.directory / "group"
    second = {**asset("BBB"), "status": "inactive" if inactive_second else "active"}
    identities = IdentityClient({"AAA": asset("AAA"), "BBB": second, "111111111": second}, [rename()])
    for label in ("AAA", "BBB"):
        download_pricing(store, label, end, client=FakeClient(case.old))
    result = run_lineage(store, identity_client=identities)
    assert not result["failed"], result
    rebuild(store)
    rebuild_universe(store)
    return store


def maintenance_denial_is_not_durable():
    with fixture() as case:
        case.records[:] = [asset("AAA")]
        case.identity = CatalogueClient(case.records)
        attempts = []

        def deny_tail(params):
            if params["start"][:10] >= "2026-10-17":
                attempts.append(dict(params))
                raise HTTPError("https://example.invalid", 403, "access denied", {}, None)
            return case.page(params)

        with patch.object(case.pricing, "_get", side_effect=deny_tail):
            first = case.update("2026-10-20")
            first_attempts = len(attempts)
            state = universe_state.read_state(case.store)
            second = case.update("2026-10-20")
        return {
            "first_complete": first["complete"],
            "first_denied_requests": first_attempts,
            "work_after_failure": state["work"],
            "second_deferred": second["plan"]["deferred"],
            "repeated_denied_requests": len(attempts) - first_attempts,
            "maintenance_failure": first["stages"]["maintenance"],
        }


def failed_mapping_acquisition_acknowledged_through_sibling():
    with fixture() as case:
        store = seed_group(case)
        current = [asset("AAA", "replacement-id"), asset("BBB")]
        identity = ActionCatalogueClient(
            current, [rename()], exact={"AAA": current[0], "BBB": current[1], "111111111": current[1]}
        )
        attempts = []
        case.live = {"AAA": case.old, "BBB": [bar("2016-01-04", "50"), bar("2026-10-16", "50")]}

        def reject_full_aaa(params):
            if "AAA" in params["symbols"].split(",") and params["start"][:10] < "2026-10-15":
                attempts.append(dict(params))
                raise HTTPError("https://example.invalid", 400, "invalid historical request", {}, None)
            return case.page(params)

        arguments = dict(pricing_client=case.pricing, identity_client=identity, registry=case.registry)
        with patch.object(case.pricing, "_get", side_effect=reject_full_aaa):
            first = run_update(store, "2026-10-17", **arguments)
            first_attempts = len(attempts)
            first_index = storage.read_index(store / "end_of_day/lineage")
            first_work = universe_state.read_state(store)["work"]["AAA"]
            second = run_update(store, "2026-10-17", **arguments)
        own_lookup = next(
            x for x in first_index["symbols"]["AAA"]["projection"]["symbol_lookups"] if x["lookup"] == "AAA"
        )
        return {
            "first_complete": first["complete"],
            "first_work_status": first_work["status"],
            "failed_acquisitions": [
                x for x in first["stages"]["acquisition"]["results"] if x["status"] in ("failed", "blocked")
            ],
            "aaa_published_asset_after_failure": own_lookup["asset"]["id"],
            "second_complete": second["complete"],
            "second_already_published": second["plan"]["already_published"],
            "full_aaa_requests_first": first_attempts,
            "full_aaa_requests_second": len(attempts) - first_attempts,
            "final_aaa_closes": [r[4] for r in read_pricing_views(store, "AAA")["raw"]],
        }


def newly_eligible_history_appended_without_check():
    with fixture() as case:
        store = seed_group(case, inactive_second=True)
        current = [asset("AAA")]
        identity = ActionCatalogueClient(
            current, [rename()], exact={"AAA": asset("AAA"), "BBB": asset("BBB"), "111111111": asset("BBB")}
        )
        corrected = [bar("2016-01-04", "50"), bar("2026-10-16", "50"), bar("2026-10-19", "50")]
        case.live = {"AAA": corrected, "BBB": corrected}
        case.pages.clear()
        result = run_update(
            store, "2026-10-20", pricing_client=case.pricing, identity_client=identity, registry=case.registry
        )
        return {
            "complete": result["complete"],
            "initial_work": result["plan"]["work"],
            "checked_tickers": result["stages"]["maintenance"]["stages"]["refresh"]["check"]["stats"]["tickers"],
            "bbb_requested_starts": sorted({p["start"] for p in case.pages if "BBB" in p["symbols"].split(",")}),
            "bbb_stored_closes": [r[4] for r in read_pricing_views(store, "BBB")["all"]],
            "provider_closes": [r["c"] for r in corrected],
        }


def partially_populated_active_source_stalls():
    with fixture() as case:
        case.records[:] = [asset("AAA"), asset("PARTIAL")]
        case.identity = CatalogueClient(case.records)
        case.live["PARTIAL"] = [bar("2026-10-16")]

        def missing_all(params):
            response = case.page(params)
            if params["adjustment"] == "all" and "PARTIAL" in response["bars"]:
                response["bars"]["PARTIAL"] = []
            return response

        with patch.object(case.pricing, "_get", side_effect=missing_all):
            first = case.update()
        before = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["PARTIAL"]["projection"]
        case.live["PARTIAL"].append(bar("2026-10-19"))
        case.pages.clear()
        second = case.update("2026-10-20")
        after = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["PARTIAL"]["projection"]
        return {
            "first_complete": first["complete"],
            "source_rows": before["source_rows"],
            "second_complete": second["complete"],
            "source_coverage_after_later_target": after["processed_through"],
            "pricing_requests_for_partial": sum("PARTIAL" in p["symbols"].split(",") for p in case.pages),
            "stored_all_rows": len(read_pricing_views(case.store, "PARTIAL")["all"]),
        }


def metadata_selection_denial_is_not_durable():
    with fixture() as case:
        records = [asset("AAA"), {**asset("AAA", "older-id"), "status": "inactive"}]
        case.identity = CatalogueClient(records)
        errors = []
        with patch.object(
            case.identity,
            "get_asset_lookup",
            side_effect=HTTPError("https://example.invalid", 403, "access denied", {}, None),
        ) as calls:
            for _ in range(2):
                try:
                    case.update()
                except OSError as error:
                    errors.append(str(error))
            exact_calls = calls.call_count
        state = universe_state.read_state(case.store)
        return {
            "exact_lookup_attempts_without_retry_override": exact_calls,
            "errors": errors,
            "durable_work": state["work"],
            "accepted_observation": state["last_catalogue_observation"],
        }


def lost_refresh():
    case = universe_fixture.UniverseUpdateTests()
    case.setUp()
    try:
        root = case.store / "end_of_day/lineage"
        before = storage.read_index(root)["symbols"]["AAA"]["sha256"]
        case.records[:] = [{**asset("AAA"), "exchange": "TXSE"}]
        case.identity = CatalogueClient(case.records)
        with patch(
            "libs.alpaca_lineage_build.universe_update.run_lineage",
            side_effect=OSError("interrupted before refresh"),
        ):
            try:
                case.update()
            except OSError:
                pass
        pending = universe_state.read_state(case.store)["work"]["AAA"]
        case.records[:] = [asset("UNRELATED")]
        case.identity = CatalogueClient(case.records)
        result = case.update()
        after = storage.read_index(root)["symbols"]["AAA"]
        return {
            "pending_refresh_before_restart": pending["refresh_metadata"],
            "same_lineage_generation_after_restart": before == after["sha256"],
            "stored_exchange_after_restart": after["projection"]["symbol_lookups"][0]["asset"]["exchange"],
            "acknowledged_as_published": result["plan"]["already_published"],
            "update_reported_complete": result["complete"],
            "final_work_status": universe_state.read_state(case.store)["work"]["AAA"]["status"],
        }
    finally:
        case.doCleanups()


def unchecked_dependency_append():
    case = universe_fixture.UniverseUpdateTests()
    case.setUp()
    try:
        # Separate store: both active labels are connected by accepted action evidence.
        store = case.directory / "dependency_store"
        old = [bar("2016-01-04", "100"), bar("2026-10-14", "100")]
        corrected = [bar("2016-01-04", "50"), bar("2026-10-14", "50"), bar("2026-10-16", "50")]
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-09-01",
            "old_symbol": "AAA",
            "new_symbol": "BBB",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        identities = IdentityClient(
            {"AAA": asset("AAA"), "BBB": asset("BBB"), "111111111": asset("BBB")}, [action]
        )
        for label in ("AAA", "BBB"):
            download_pricing(store, label, "2026-10-15", client=FakeClient(old))
        seeded = run_lineage(store, identity_client=identities)
        assert seeded["failed"] == 0, seeded
        rebuild(store)
        rebuild_universe(store)
        # A successful independent update advances BBB; AAA still needs maintenance.
        download_pricing(store, "BBB", "2026-10-17", client=FakeClient(corrected))
        client = BarsClient("fake", "fake")
        pages = []

        def page(params):
            pages.append(dict(params))
            start, end = timestamp(params["start"]), timestamp(params["end"])
            return {
                "bars": {
                    s: [r for r in corrected if start <= timestamp(r["t"]) <= end]
                    for s in params["symbols"].split(",")
                }
            }

        with patch.object(client, "_get", side_effect=page):
            result = run_update(
                store,
                "2026-10-17",
                pricing_client=client,
                identity_client=CatalogueClient([asset("AAA"), asset("BBB")]),
                registry=case.registry,
            )
        rows = read_pricing_views(store, "AAA")["all"]
        return {
            "update_reported_complete": result["complete"],
            "live_history_check": result["stages"]["maintenance"],
            "stored_all_rows": [list(row) for row in rows],
            "provider_historical_close": "50",
            "aaa_requested_starts": sorted({p["start"] for p in pages if "AAA" in p["symbols"].split(",")}),
        }
    finally:
        case.doCleanups()


def blocked_refresh_retried_in_maintenance():
    case = universe_fixture.UniverseUpdateTests()
    case.setUp()
    try:
        case.records[:] = [{**asset("AAA"), "exchange": "TXSE"}]
        case.identity = CatalogueClient(case.records)
        original = case.identity.execute_plan
        attempts = []

        def deny_actions(plan):
            if any(
                a["strategy"] == "corporate_actions_batch" for step in plan["steps"] for a in step["actions"]
            ):
                attempts.append(copy.deepcopy(plan))
                raise HTTPError("https://example.invalid", 403, "access denied", {}, None)
            return original(plan)

        with patch.object(case.identity, "execute_plan", side_effect=deny_actions):
            case.update()
            first_attempts = len(attempts)
            blocked = universe_state.read_state(case.store)["work"]["AAA"]["status"]
            case.live["AAA"].append(bar("2026-10-19"))
            second = case.update("2026-10-20")
        return {
            "status_after_first_update": blocked,
            "deferred_on_second_update": second["plan"]["deferred"],
            "denied_metadata_calls_on_first": first_attempts,
            "denied_metadata_calls_despite_deferral": len(attempts) - first_attempts,
            "second_maintenance": second["stages"]["maintenance"],
        }
    finally:
        case.doCleanups()


def absent_catalogue_stalls_known_empty_history():
    case = universe_fixture.UniverseUpdateTests()
    case.setUp()
    try:
        first = case.update()
        assert first["complete"], first
        case.records[:] = [row for row in case.records if row["symbol"] != "EMPTY"]
        case.identity = CatalogueClient(case.records)
        case.live["EMPTY"] = [bar("2026-10-19")]
        case.pages.clear()
        result = case.update("2026-10-20")
        entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["EMPTY"]
        return {
            "update_reported_complete": result["complete"],
            "known_active_empty_coverage_after_later_target": entry["projection"]["processed_through"],
            "stored_empty_row_count": len(read_pricing_views(case.store, "EMPTY")["all"]),
            "bar_calls_for_empty": sum("EMPTY" in p["symbols"].split(",") for p in case.pages),
            "provider_now_has_later_bar": True,
        }
    finally:
        case.doCleanups()


class DurableMaintenanceTests(unittest.TestCase):
    def test_dependency_catchup_denial_remains_a_failed_standalone_result(self):
        with fixture() as case:
            store = seed_group(case, end="2026-10-15")
            download_pricing(store, "BBB", "2026-10-17", client=FakeClient(case.old))
            with patch.object(
                case.pricing, "_get", side_effect=HTTPError("https://invalid", 403, "denied", {}, None)
            ):
                result = run_lineage(store, ["BBB"], pricing_client=case.pricing, identity_client=case.identity)
            self.assertGreater(result["failed"], 0, result)
            self.assertEqual(result["results"][0]["outcome"]["outcome"], "access_denied")

    def test_interrupted_correction_is_not_lost_when_the_live_endpoint_later_matches(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
            with (
                patch(
                    "libs.alpaca_lineage_build.refresh.replace_prepared",
                    side_effect=ProcessInterrupted("before correction"),
                ),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update()
            self.assertTrue(universe_state.read_state(case.store)["work"]["AAA"]["correction_id"])
            case.live["AAA"][-1] = case.old[-1]
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(read_pricing_views(case.store, "AAA")["all"][0][4], "50")

    def test_completed_correction_is_reused_after_interruption_before_lineage(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50")]
            with (
                patch(
                    "libs.alpaca_lineage_build.refresh.run_lineage",
                    side_effect=ProcessInterrupted("after correction"),
                ),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update()
            case.pages.clear()
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertTrue(
                all(p["adjustment"] == "all" and p["start"][:10] == "2026-10-16" for p in case.pages)
            )

    def test_old_publication_acknowledgement_cannot_complete_a_new_refresh(self):
        with fixture() as case:
            case.records[:] = [{**asset("AAA"), "exchange": "TXSE"}]
            case.identity = CatalogueClient(case.records)
            with (
                patch.object(universe_state, "record_outcome", side_effect=OSError("before acknowledgement")),
                self.assertRaises(OSError),
            ):
                case.update()
            old = universe_state.read_state(case.store)["work"]["AAA"]
            case.records[:] = [{**asset("AAA"), "exchange": "NYSE"}]
            case.identity = CatalogueClient(case.records)
            with (
                patch(
                    "libs.alpaca_lineage_build.universe_update.run_lineage",
                    side_effect=OSError("before new refresh"),
                ),
                self.assertRaises(OSError),
            ):
                case.update()
            pending = universe_state.read_state(case.store)["work"]["AAA"]
            self.assertNotEqual(old["obligation_id"], pending["obligation_id"])
            self.assertEqual(pending["status"], "pending")
            case.records[:] = [{**asset("IGNORED"), "exchange": "BATS"}]
            case.identity = CatalogueClient(case.records)
            result = case.update()
            self.assertTrue(result["complete"], result)
            entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
            self.assertEqual(entry["projection"]["symbol_lookups"][0]["asset"]["exchange"], "NYSE")

    def test_mapping_deferred_by_older_cutoff_is_retained_when_catalogue_later_omits_it(self):
        with fixture() as case:
            case.records[:] = [asset("AAA", "replacement-id")]
            case.identity = CatalogueClient(case.records)
            first = case.update("2026-10-16")
            self.assertFalse(first["complete"])
            self.assertFalse(case.pages)
            self.assertTrue(universe_state.read_state(case.store)["work"]["AAA"]["invalidate_pricing"])
            case.records[:] = [{**asset("IGNORED"), "exchange": "BATS"}]
            case.identity = CatalogueClient(case.records)
            second = case.update()
            self.assertTrue(second["complete"], second)
            entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
            lookup = next(row for row in entry["projection"]["symbol_lookups"] if row["lookup"] == "AAA")
            self.assertEqual(lookup["asset"]["id"], "replacement-id")

    def test_failed_new_dependency_stays_restricted_when_another_seed_discovers_it(self):
        with fixture() as case:
            records = [{**asset("AAA"), "exchange": "TXSE"}]
            dependency = {**asset("DEP"), "exchange": "BATS"}
            identity = ActionCatalogueClient(
                records, [rename("AAA", "DEP")], exact={"DEP": dependency, "111111111": dependency}
            )
            attempts = []

            def denied(params):
                if "DEP" in params["symbols"].split(","):
                    attempts.append(params)
                    raise HTTPError("https://invalid", 403, "denied dependency", {}, None)
                return case.page(params)

            case.identity = identity
            with patch.object(case.pricing, "_get", side_effect=denied):
                self.assertFalse(case.update()["complete"])
                count = len(attempts)
                self.assertGreater(count, 0)
                self.assertEqual(universe_state.read_state(case.store)["work"]["DEP"]["status"], "blocked")
                records.append(asset("NEW"))
                identity.actions.append(rename("NEW", "DEP"))
                # Distinct events, both refer to the same accepted dependency.
                identity.actions[-1]["id"] = "second-rename"
                result = case.update()
                self.assertFalse(result["complete"])
                self.assertEqual(len(attempts), count)

    def test_missing_asset_lookup_does_not_erase_last_positive_maintenance_fact(self):
        with fixture() as case:
            self.assertTrue(case.update()["complete"])
            case.records[:] = [asset("AAA"), asset("NEW")]
            case.identity = CatalogueClient(case.records)
            case.live["EMPTY"] = [bar("2026-10-19")]
            self.assertTrue(case.update("2026-10-20")["complete"])
            entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["EMPTY"]
            self.assertEqual(entry["projection"]["last_observed_symbol_lookup"]["asset"]["status"], "active")
            case.live["EMPTY"].append(bar("2026-10-20"))
            case.pages.clear()
            result = case.update("2026-10-21")
            self.assertTrue(result["complete"], result)
            self.assertEqual(len(read_pricing_views(case.store, "EMPTY")["all"]), 2)

    def test_history_denial_after_lineage_publication_is_not_acknowledged_on_restart(self):
        with fixture() as case:
            case.records[:] = [{**asset("AAA"), "exchange": "TXSE"}]
            case.identity = CatalogueClient(case.records)
            with patch.object(
                case.pricing, "_get", side_effect=HTTPError("https://invalid", 403, "denied", {}, None)
            ):
                first = case.update()
            self.assertFalse(first["complete"])
            work = universe_state.read_state(case.store)["work"]["AAA"]
            entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
            self.assertNotEqual(work["obligation_id"], entry["projection"]["update_obligation_id"])
            case.pages.clear()
            second = case.update()
            self.assertFalse(second["complete"])
            self.assertEqual(second["plan"]["already_published"], [])
            self.assertFalse(case.pages)
            self.assertTrue(case.update(retry_blocked=True)["complete"])

    def test_newly_eligible_member_after_append_gets_checked_before_completion(self):
        with fixture() as case:
            inactive = {**asset("BBB"), "status": "inactive"}
            download_pricing(case.store, "BBB", "2026-10-20", client=FakeClient(case.old + [bar("2026-10-19")]))
            self.assertEqual(
                run_lineage(case.store, ["BBB"], identity_client=IdentityClient({"BBB": inactive}))["failed"], 0
            )
            case.live = {
                "AAA": case.old + [bar("2026-10-19")],
                "BBB": [bar("2016-01-04", "50"), bar("2026-10-16", "50"), bar("2026-10-19", "50")],
            }
            case.identity = ActionCatalogueClient(
                [asset("AAA")],
                [{**rename(), "process_date": "2026-10-19"}],
                exact={"AAA": asset("AAA"), "BBB": asset("BBB"), "111111111": asset("BBB")},
            )
            result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertIn("refresh_new_members_1", result["stages"]["maintenance"]["stages"])
            self.assertEqual([r[4] for r in read_pricing_views(case.store, "BBB")["all"]], ["50"] * 3)

    def test_interrupted_refresh_is_performed_when_next_catalogue_omits_label(self):
        result = lost_refresh()
        self.assertTrue(result["pending_refresh_before_restart"])
        self.assertFalse(result["same_lineage_generation_after_restart"])
        self.assertEqual(result["stored_exchange_after_restart"], "TXSE")
        self.assertEqual(result["acknowledged_as_published"], [])
        self.assertTrue(result["update_reported_complete"])

    def test_dependency_catchup_corrects_retained_prices_before_advancing(self):
        result = unchecked_dependency_append()
        self.assertTrue(result["update_reported_complete"], result)
        self.assertEqual([row[4] for row in result["stored_all_rows"]], ["50"] * 3)
        self.assertTrue(any(start.startswith("2016-") for start in result["aaa_requested_starts"]))

    def test_metadata_denial_stays_blocked_during_later_maintenance(self):
        result = blocked_refresh_retried_in_maintenance()
        self.assertEqual(result["status_after_first_update"], "blocked")
        self.assertEqual(result["denied_metadata_calls_despite_deferral"], 0)
        self.assertFalse(result["second_maintenance"]["complete"])

    def test_catalogue_omission_does_not_stop_active_empty_history(self):
        result = absent_catalogue_stalls_known_empty_history()
        self.assertTrue(result["update_reported_complete"])
        self.assertEqual(result["known_active_empty_coverage_after_later_target"][:10], "2026-10-20")
        self.assertEqual(result["stored_empty_row_count"], 1)
        self.assertEqual(result["bar_calls_for_empty"], 8)

    def test_maintenance_denial_creates_durable_restricted_work(self):
        result = maintenance_denial_is_not_durable()
        self.assertFalse(result["first_complete"])
        self.assertEqual(result["work_after_failure"]["AAA"]["status"], "blocked")
        self.assertEqual(result["repeated_denied_requests"], 0)

    def test_failed_identity_replacement_cannot_publish_through_sibling(self):
        result = failed_mapping_acquisition_acknowledged_through_sibling()
        self.assertFalse(result["first_complete"])
        self.assertEqual(result["aaa_published_asset_after_failure"], "AAA-id")
        self.assertFalse(result["second_complete"])
        self.assertEqual(result["second_already_published"], [])
        self.assertEqual(result["full_aaa_requests_second"], 0)

    def test_newly_eligible_member_gets_its_own_check_and_correction(self):
        result = newly_eligible_history_appended_without_check()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_tickers"], 2)
        self.assertEqual(result["bbb_stored_closes"], ["50"] * 3)

    def test_partial_adjustment_history_is_maintained_outside_summary(self):
        result = partially_populated_active_source_stalls()
        self.assertEqual(result["source_rows"], 7)
        self.assertTrue(result["second_complete"])
        self.assertEqual(result["source_coverage_after_later_target"][:10], "2026-10-20")
        self.assertGreater(result["pricing_requests_for_partial"], 0)
        self.assertEqual(result["stored_all_rows"], 2)

    def test_selection_lookup_denial_is_durable_before_acquisition(self):
        result = metadata_selection_denial_is_not_durable()
        self.assertEqual(result["exact_lookup_attempts_without_retry_override"], 1)
        self.assertEqual(result["durable_work"]["AAA"]["status"], "blocked")

    def test_identity_obligation_survives_each_publication_boundary(self):
        for boundary in ("before_acquisition", "after_pricing", "after_lineage"):
            with self.subTest(boundary=boundary), fixture() as case:
                case.records[:] = [asset("AAA", "replacement-id")]
                case.identity = CatalogueClient(case.records)
                if boundary == "before_acquisition":
                    target = "libs.alpaca_lineage_build.universe_update.acquire_selected"
                elif boundary == "after_pricing":
                    target = "libs.alpaca_lineage_build.universe_update.run_lineage"
                else:
                    target = "libs.alpaca_lineage_build.universe_state.record_outcome"
                with patch(target, side_effect=OSError("interrupted")), self.assertRaises(OSError):
                    case.update()
                pending = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertTrue(pending["invalidate_pricing"])
                case.records[:] = [{**asset("UNRELATED"), "exchange": "BATS"}]
                case.identity = CatalogueClient(case.records, exact={"AAA": asset("AAA", "replacement-id")})
                case.pages.clear()
                result = case.update()
                self.assertTrue(result["complete"], result)
                entry = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
                self.assertEqual(entry["projection"]["update_obligation_id"], pending["obligation_id"])
                self.assertEqual(
                    entry["projection"]["acquisition_evidence_sha256"], pending["evidence_fingerprint"]
                )
                if boundary != "before_acquisition":
                    self.assertTrue(
                        all(p["adjustment"] == "all" and p["start"][:10] >= "2026-10-16" for p in case.pages)
                    )

    def test_older_invocation_cannot_reopen_accepted_larger_target(self):
        with fixture() as case:
            case.records[:] = [{**asset("AAA"), "exchange": "TXSE"}]
            case.identity = CatalogueClient(case.records)
            with (
                patch("libs.alpaca_lineage_build.universe_update.run_lineage", side_effect=OSError("stop")),
                self.assertRaises(OSError),
            ):
                case.update("2026-10-20")
            pending = universe_state.read_state(case.store)["work"]["AAA"]
            case.pages.clear()
            with patch.object(
                case.identity, "get_asset_lookup", side_effect=AssertionError("restricted lookup")
            ):
                result = case.update("2026-10-17", retry_blocked=True)
            self.assertFalse(result["complete"])
            self.assertFalse(case.pages)
            self.assertEqual(
                universe_state.read_state(case.store)["work"]["AAA"]["obligation_id"], pending["obligation_id"]
            )

    def test_delayed_retry_and_explicit_access_retry_finish_real_publications(self):
        for code in (403, 503):
            with self.subTest(code=code), fixture() as case:
                case.records[:] = [asset("AAA")]
                case.identity = CatalogueClient(case.records)
                case.live["AAA"].append(bar("2026-10-19"))
                original = case.page

                def failed(params):
                    if params["start"][:10] >= "2026-10-17":
                        raise HTTPError("https://invalid", code, "denied", {}, None)
                    return original(params)

                with patch.object(case.pricing, "_get", side_effect=failed):
                    self.assertFalse(case.update("2026-10-20")["complete"])
                case.pages.clear()
                self.assertFalse(case.update("2026-10-20")["complete"])
                self.assertFalse(case.pages)
                if code == 403:
                    result = case.update("2026-10-20", retry_blocked=True)
                else:
                    with patch.object(universe_state, "now_utc", return_value="2099-01-01T00:00:00+00:00"):
                        result = case.update("2026-10-20")
                self.assertTrue(result["complete"], result)
                self.assertEqual(len(read_pricing_views(case.store, "AAA")["all"]), 3)

    def test_explicit_inactivity_reconciles_without_empty_maintenance(self):
        with fixture() as case:
            case.update()
            case.records[:] = [asset("AAA"), asset("NEW"), {**asset("EMPTY"), "status": "inactive"}]
            case.identity = CatalogueClient(case.records)
            result = case.update()
            self.assertTrue(result["complete"], result)
            case.pages.clear()
            result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertFalse(any("EMPTY" in p["symbols"].split(",") for p in case.pages))

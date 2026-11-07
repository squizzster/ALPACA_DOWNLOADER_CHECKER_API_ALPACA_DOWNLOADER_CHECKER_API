"""Accepted pricing and lineage must forbid repeated historical work."""

import unittest
from unittest.mock import patch
from urllib.error import HTTPError

import test_alpaca_lineage_build as lineage_fixture
from test_alpaca_durable_maintenance import ActionCatalogueClient, fixture, rename, seed_group
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import pipeline, pricing, storage
from libs.alpaca_lineage_build.transform import ASSIGNMENT_FIELDS
from libs.alpaca_lineage_build.universe_update import run_update
from libs.alpaca_pricing_download import acquire, append, check_summary, download
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.context import accept_context
from libs.alpaca_pricing_download.contract import DownloadError


class RetainedAcceptanceTests(unittest.TestCase):
    def test_recovery_reuses_catalogue_lookups_and_actions_through_serving(self):
        with fixture() as case:
            case.records[:] = [lineage_fixture.asset("AAA")]
            case.identity = CatalogueClient(case.records)
            corrected = [bar("2016-01-04", "50"), case.old[-1], bar("2026-10-19")]
            download.download_pricing(case.store, "AAA", "2026-10-20", client=FakeClient(corrected))
            case.live["AAA"] = corrected
            check_summary.invalidate(case.store, ["AAA"], owner="lineage")
            execute = case.identity.execute_plan
            case.records[:] = [{**lineage_fixture.asset("AAA"), "exchange": "TXSE"}]
            requests = []

            def execute_plan(plan):
                for step in plan["steps"]:
                    for action in step["actions"]:
                        if action["strategy"] == "corporate_actions_batch":
                            requests.append(dict(action["request"]["params"]))
                return execute(plan)

            with patch.object(case.identity, "execute_plan", side_effect=execute_plan):
                result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
            self.assertIn("recovery_lineage", result["stages"])
            self.assertEqual(case.identity.catalogue_calls, 1)
            self.assertEqual(len(requests), 1, requests)
            self.assertEqual(requests[0]["start"], "2026-10-01")
            self.assertEqual(requests[0]["end"], "2026-10-20")
            root = case.store / "end_of_day/lineage"
            receipt = storage.read_receipt(root, "AAA", storage.read_index(root))
            current = next(item for item in receipt["evidence"]["asset_lookups"] if item["lookup"] == "AAA")
            self.assertEqual(current["asset"]["exchange"], "TXSE")

    def test_recovery_exact_lookup_survives_selection_with_ambiguous_catalogue(self):
        with fixture() as case:
            case.records[:] = [lineage_fixture.asset("AAA"), lineage_fixture.asset("AAA", "other-id")]
            case.identity = CatalogueClient(case.records, exact={"AAA": lineage_fixture.asset("AAA")})
            corrected = [bar("2016-01-04", "50"), case.old[-1], bar("2026-10-19")]
            download.download_pricing(case.store, "AAA", "2026-10-20", client=FakeClient(corrected))
            case.live["AAA"] = corrected
            check_summary.invalidate(case.store, ["AAA"], owner="lineage")
            with patch.object(case.identity, "get_asset_lookup", wraps=case.identity.get_asset_lookup) as lookup:
                result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
            self.assertIn("recovery_lineage", result["stages"])
            self.assertEqual(case.identity.catalogue_calls, 1)
            self.assertEqual(result["plan"]["catalogue_records"], 2)
            self.assertEqual([call.args[0] for call in lookup.call_args_list].count("AAA"), 1)

    def test_non_summary_dependency_checks_only_all_then_appends_through_serving(self):
        with fixture() as case:
            store = seed_group(case, inactive_second=True)
            assets = {
                "AAA": lineage_fixture.asset("AAA"),
                "BBB": {**lineage_fixture.asset("BBB"), "status": "inactive"},
            }
            case.live = {s: case.old + [bar("2026-10-19")] for s in assets}
            result = run_update(
                store,
                "2026-10-20",
                pricing_client=case.pricing,
                identity_client=ActionCatalogueClient(
                    list(assets.values()), [rename()], exact={**assets, "111111111": assets["BBB"]}
                ),
                registry=case.registry,
            )
            self.assertTrue(result["complete"], result)
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
            for symbol in assets:
                requests = [p for p in case.pages if symbol in p["symbols"].split(",")]
                old = [p for p in requests if p["start"][:10] < "2026-10-17"]
                self.assertEqual(len(old), 2, (symbol, requests))
                self.assertEqual({p["adjustment"] for p in old}, {"all"})
                self.assertEqual(len(requests), 4, (symbol, requests))
                self.assertTrue(
                    all(len(rows) == 3 for rows in download.read_pricing_views(store, symbol).values())
                )

    def test_empty_dependency_suffix_closes_history_and_is_not_selected_again(self):
        with fixture() as case:
            store = seed_group(case, inactive_second=True)
            assets = {
                "AAA": lineage_fixture.asset("AAA"),
                "BBB": {**lineage_fixture.asset("BBB"), "status": "inactive"},
            }
            case.live = {"AAA": case.old + [bar("2026-10-19")], "BBB": list(case.old)}
            root = store / "end_of_day/pricing/BBB"
            before = pricing_storage.load_source(root, "BBB")
            result = run_update(
                store,
                "2026-10-20",
                pricing_client=case.pricing,
                identity_client=ActionCatalogueClient(
                    list(assets.values()), [rename()], exact={**assets, "111111111": assets["BBB"]}
                ),
                registry=case.registry,
            )
            self.assertTrue(result["complete"], result)
            requests = [p for p in case.pages if "BBB" in p["symbols"].split(",")]
            self.assertEqual(len(requests), 2, requests)
            self.assertEqual({p["adjustment"] for p in requests}, {"raw", "all"})
            self.assertTrue(all(p["start"][:10] >= "2026-10-17" for p in requests))
            self.assertEqual(pricing_storage.load_source(root, "BBB"), before)

    def test_failed_suffix_keeps_completed_comparison_and_validator_in_operation(self):
        with fixture() as case:
            context = accept_context(case.store)
            case.live["AAA"] = case.old + [bar("2026-10-19")]

            def failed_suffix(params):
                if params["start"][:10] >= "2026-10-17":
                    raise DownloadError("suffix unavailable")
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=failed_suffix):
                first = acquire.acquire_selected(
                    case.store, ["AAA"], "2026-10-20", client=case.pricing, context=context
                )
            self.assertEqual(first["failed"], 1)
            case.pages.clear()
            with (
                patch.object(
                    acquire, "read_validator", side_effect=AssertionError("accepted validator reopened")
                ),
                patch.object(
                    download, "_endpoints_match", side_effect=AssertionError("accepted comparison repeated")
                ),
            ):
                second = acquire.acquire_selected(
                    case.store, ["AAA"], "2026-10-20", client=case.pricing, context=context
                )
            self.assertEqual(second["failed"], 0, second)
            self.assertEqual(len(case.pages), 2)
            self.assertTrue(all(p["start"][:10] == "2026-10-17" for p in case.pages))

    def test_empty_suffix_does_not_open_any_pricing_payload(self):
        with fixture() as case:
            with patch.object(download, "read_boundary", side_effect=AssertionError("unchanged payload read")):
                result = acquire.acquire_selected(case.store, ["AAA"], "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 0, result)
            self.assertFalse(result["results"][0]["changed"])
            self.assertEqual(len(case.pages), 4)

    def test_empty_baseline_survives_failed_suffix_without_validator_reread(self):
        with fixture() as case:
            download.download_pricing(case.store, "EMPTY", "2026-10-17", client=FakeClient([]))
            case.live["EMPTY"] = []
            context = accept_context(case.store)
            with patch.object(case.pricing, "_get", side_effect=DownloadError("suffix unavailable")):
                first = acquire.acquire_selected(
                    case.store, ["EMPTY"], "2026-10-20", client=case.pricing, context=context
                )
            self.assertEqual(first["failed"], 1)
            with (
                patch.object(
                    acquire, "read_validator", side_effect=AssertionError("accepted validator reopened")
                ),
                patch.object(
                    append, "read_validator", side_effect=AssertionError("publication reread validator")
                ),
            ):
                second = acquire.acquire_selected(
                    case.store, ["EMPTY"], "2026-10-20", client=case.pricing, context=context
                )
            self.assertEqual(second["failed"], 0, second)
            self.assertEqual(len(case.pages), 2)

    def test_standalone_failed_suffix_keeps_the_completed_approval(self):
        with fixture() as case:
            context = accept_context(case.store)
            client = FakeClient(case.old)
            fetch = client.fetch

            def unavailable(symbol, adjustment, start, end, **kwargs):
                if start.date().isoformat() >= "2026-10-17":
                    raise DownloadError("suffix unavailable")
                return fetch(symbol, adjustment, start, end, **kwargs)

            with patch.object(client, "fetch", side_effect=unavailable), self.assertRaises(DownloadError):
                download.download_pricing(case.store, "AAA", "2026-10-20", client=client, context=context)
            client.calls.clear()
            with (
                patch.object(download, "read_validator", side_effect=AssertionError("validator reread")),
                patch.object(download, "_endpoints_match", side_effect=AssertionError("comparison replay")),
            ):
                result = download.download_pricing(
                    case.store, "AAA", "2026-10-20", client=client, context=context
                )
            self.assertFalse(result["changed"])
            self.assertEqual(len(client.calls), 2)

    def test_batch_omission_rechecks_only_the_missing_all_endpoint(self):
        with fixture() as case:
            download.download_pricing(case.store, "BBB", "2026-10-17", client=FakeClient(case.old))
            case.live["BBB"] = case.old

            def omitted(params):
                document = case.page(params)
                if params["symbols"] == "AAA,BBB" and params["start"][:10] == "2016-01-04":
                    document["bars"].pop("AAA", None)
                return document

            with patch.object(case.pricing, "_get", side_effect=omitted):
                result = acquire.acquire_selected(case.store, ["AAA", "BBB"], "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual(result["requests"]["omission_probes"], 1)
            self.assertEqual(result["requests"]["omission_recovered"], 1)
            singletons = [p for p in case.pages if p["symbols"] == "AAA"]
            self.assertEqual(len(singletons), 1)
            self.assertEqual(singletons[0]["adjustment"], "all")

    def test_singleton_missing_endpoint_fails_without_a_second_request(self):
        with fixture() as case:
            case.live["AAA"] = case.old[1:]
            result = acquire.acquire_selected(case.store, ["AAA"], "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 1, result)
            self.assertEqual(len(case.pages), 2)
            self.assertEqual(result["requests"].get("omission_probes", 0), 0)

    def test_414_singleton_absence_is_not_requested_again(self):
        with fixture() as case:
            download.download_pricing(case.store, "BBB", "2026-10-17", client=FakeClient(case.old))
            case.live["AAA"] = case.old[1:]
            case.live["BBB"] = case.old

            def split(params):
                if "," in params["symbols"]:
                    raise HTTPError("https://invalid", 414, "split", {}, None)
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=split):
                result = acquire.acquire_selected(case.store, ["AAA", "BBB"], "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 1, result)
            requests = [p for p in case.pages if p["symbols"] == "AAA"]
            self.assertEqual(len(requests), 2)
            self.assertEqual(result["requests"].get("omission_probes", 0), 0)

    def test_exact_suffix_fallback_keeps_approval_and_successful_peer_views(self):
        with fixture() as case:
            download.download_pricing(case.store, "BBB", "2026-10-17", client=FakeClient(case.old))
            case.live = {s: case.old + [bar("2026-10-19")] for s in ("AAA", "BBB")}

            def rejected_all_batch(params):
                if (
                    "," in params["symbols"]
                    and params["start"][:10] >= "2026-10-17"
                    and params["adjustment"] == "all"
                ):
                    raise HTTPError("https://invalid", 400, "batch rejected", {}, None)
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=rejected_all_batch):
                result = acquire.acquire_selected(case.store, ["AAA", "BBB"], "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 0, result)
            old = [p for p in case.pages if p["start"][:10] < "2026-10-17"]
            self.assertEqual(len(old), 2)
            self.assertTrue(all(p["symbols"] == "AAA,BBB" and p["adjustment"] == "all" for p in old))
            suffix = [p for p in case.pages if p not in old]
            self.assertEqual(len(suffix), 3)  # Completed raw batch plus two all singleton fallbacks.
            self.assertEqual({p["adjustment"] for p in suffix if "," not in p["symbols"]}, {"all"})
            for symbol in ("AAA", "BBB"):
                self.assertTrue(
                    all(len(rows) == 3 for rows in download.read_pricing_views(case.store, symbol).values())
                )

    def test_no_price_change_reuses_group_unresolved_and_empty_lineage(self):
        for scenario in ("group", "unresolved", "empty"):
            with self.subTest(scenario=scenario):
                self.check_lineage_reuse(scenario)

    def check_lineage_reuse(self, scenario):
        case = lineage_fixture.LineageBuildTests()
        case.setUp()
        try:
            symbols = ("GOOG", "OTHER") if scenario == "group" else ("GOOG",)
            assets = (
                {}
                if scenario == "unresolved"
                else {s: lineage_fixture.asset(s, "shared-id" if len(symbols) > 1 else None) for s in symbols}
            )
            client = lineage_fixture.IdentityClient(assets=assets)
            rows = [] if scenario == "empty" else lineage_fixture.calendar_bars("2026-10-01", "2026-10-14")
            for symbol in symbols:
                case.download(symbol=symbol, bars=rows)
            case.run_build(symbols=list(symbols), client=client)
            for symbol in symbols:
                self.assertFalse(case.download(symbol=symbol, end="2026-10-15", bars=rows)["changed"])
            with (
                patch.object(
                    pricing, "load_published_views", side_effect=AssertionError("accepted history reopened")
                ),
                patch.object(pipeline, "correlate", side_effect=AssertionError("accepted lineage replayed")),
            ):
                result = case.run_build(symbols=[symbols[0]], client=client)
            self.assertTrue(all(r["reused_decision"] for r in result["results"]))

            def decisions():
                return {
                    s: {
                        a: [
                            {k: v for k, v in item.items() if k in ASSIGNMENT_FIELDS}
                            for item in case.assignments(s, adjustment=a)
                        ]
                        for a in lineage_fixture.ADJUSTMENTS
                    }
                    for s in symbols
                }

            accepted = decisions()
            case.run_build(symbols=[symbols[0]], client=client, refresh=True)
            self.assertEqual(decisions(), accepted)
        finally:
            case.doCleanups()


if __name__ == "__main__":
    unittest.main()

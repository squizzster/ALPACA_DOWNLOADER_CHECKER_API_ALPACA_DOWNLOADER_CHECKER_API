"""Real producer pipelines with doubles only at the provider boundary."""

import os
import signal
import traceback
import unittest
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import ActionCatalogueClient, fixture, rename, seed_group
from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import observations, refresh, update
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.context import accept_context
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.refresh import refresh_existing
from libs.alpaca_lineage_build.universe_update import rebuild_universe, run_update
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import acquire, append, download
from libs.alpaca_pricing_download import refresh as pricing_refresh
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.check_summary import read_snapshot
from libs.alpaca_pricing_download.contract import DownloadError, timestamp
from libs.alpaca_pricing_download.download import read_pricing_views
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.debug_trace import DebugTrace


class UpdateHandoffRegressions(unittest.TestCase):
    def test_late_repair_dependency_advance_invalidates_earlier_comparison(self):
        for differed in (False, True):
            with self.subTest(differed=differed), fixture() as case:
                store = case.directory / "late-dependency"
                initial = {
                    "AAA": [bar("2026-01-05"), bar("2026-01-07")],
                    "BBB": [bar("2026-01-05"), bar("2026-01-06")],
                    "CCC": [bar("2026-01-05"), bar("2026-01-06")],
                }
                assets = {s: asset(s) for s in initial}
                for symbol, rows in initial.items():
                    download.download_pricing(
                        store, symbol, "2026-01-08" if symbol == "AAA" else "2026-01-07", client=FakeClient(rows)
                    )
                self.assertEqual(run_lineage(store, identity_client=IdentityClient(assets))["failed"], 0)
                rebuild(store)
                self.assertTrue(maintain(store, bootstrap=True)["complete"])
                selected = serving_storage.select_late_bar_repairs(store, serving_storage.controls(store))
                self.assertEqual(set(selected["sources"]), {"AAA"})
                context = accept_context(store)
                first = bar("2026-01-05", "50") if differed else initial["BBB"][0]
                case.live = {**initial, "BBB": [first, bar("2026-01-06")], "CCC": [bar("2026-01-06")]}
                with self.assertRaisesRegex(DownloadError, "live check incomplete"):
                    refresh_existing(
                        store,
                        pricing_client=case.pricing,
                        identity_client=IdentityClient(assets),
                        end_date="2026-01-08",
                        context=context,
                    )
                self.assertEqual(
                    context.history_comparisons["BBB"][2].decision, "full_history" if differed else "unchanged"
                )
                case.live["AAA"] = [bar("2026-01-05"), bar("2026-01-06"), bar("2026-01-07")]
                case.live["BBB"] = [first, bar("2026-01-06"), bar("2026-01-07")]
                identity = IdentityClient(
                    {**assets, "111111111": assets["BBB"]}, [{**rename(), "process_date": "2026-01-06"}]
                )
                result = refresh_existing(
                    store,
                    pricing_client=case.pricing,
                    identity_client=identity,
                    end_date="2026-01-08",
                    context=context,
                    late_bar_repairs=selected,
                    symbols={"AAA", "BBB"},
                )
                self.assertTrue(result["complete"], result)
                self.assertEqual([r["symbol"] for r in result["pricing"]["results"]], ["AAA"])
                self.assertEqual(result["lineage"]["results"][0]["pricing_acquisition_waves"], 1)
                self.assertEqual(result["lineage"]["results"][0]["pricing_replacements"], {})
                self.assertEqual(result["check"]["stats"]["tickers"], 1)
                self.assertEqual(result["check"]["stats"]["matched"], 1)
                rows = read_pricing_views(store, "BBB")["all"]
                self.assertEqual(len(rows), 3)
                self.assertEqual(rows[0][1], str(first["o"]))

    def test_scope_restriction_during_reconciliation_removes_prior_append_approval(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)
            context = accept_context(store)
            policy = WorkPolicy(
                store, context.select_index(context.accept_store().root), {}, [], "2026-10-17", context=context
            )
            arguments = dict(
                pricing_client=case.pricing,
                identity_client=IdentityClient(assets),
                end_date="2026-10-17",
                context=context,
                work_policy=policy,
            )
            self.assertTrue(refresh_existing(store, symbols={"AAA"}, **arguments)["complete"])
            case.live["BBB"] = [bar("2016-01-04", "50"), case.old[-1]]
            reconcile = refresh.run_lineage

            def restrict(*args, **kwargs):
                result = reconcile(*args, **kwargs)
                policy.blocked.add("AAA")
                return result

            ready = []
            with patch.object(refresh, "run_lineage", side_effect=restrict):
                result = refresh_existing(store, on_ready=ready.append, **arguments)
            self.assertTrue(result["complete"], result)
            self.assertEqual([e["symbol"] for e in ready[0]["entries"]], ["BBB"])
            self.assertIn("AAA", result["deferred_symbols"])

    def test_partial_match_cannot_approve_its_blocked_dependency_group(self):
        with fixture() as case:
            store = seed_group(case)
            assets = {s: asset(s) for s in ("AAA", "BBB")}
            case.live = {"AAA": case.old + [bar("2026-10-19")], "BBB": [case.old[-1]]}
            result = run_update(
                store,
                "2026-10-20",
                pricing_client=case.pricing,
                identity_client=ActionCatalogueClient(
                    list(assets.values()), [rename()], exact={**assets, "111111111": assets["BBB"]}
                ),
                registry=case.registry,
            )
            self.assertFalse(result["complete"], result)
            for symbol in assets:
                self.assertEqual(len(read_pricing_views(store, symbol)["all"]), 2)

    def test_successful_correction_is_retained_when_peer_correction_fails(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)
            for symbol in assets:
                case.live[symbol] = [bar("2016-01-04", "50"), case.old[-1], bar("2026-10-19")]
            publish = pricing_refresh.download_pricing

            def fail_peer(store, symbol, *args, **kwargs):
                if symbol == "BBB":
                    raise OSError("synthetic peer correction failure")
                return publish(store, symbol, *args, **kwargs)

            with patch.object(pricing_refresh, "download_pricing", side_effect=fail_peer):
                result = run_update(
                    store,
                    "2026-10-20",
                    pricing_client=case.pricing,
                    identity_client=CatalogueClient(list(assets.values())),
                    registry=case.registry,
                )
            self.assertFalse(result["complete"], result)
            self.assertEqual(read_pricing_views(store, "AAA")["all"][0][1], "50")
            self.assertNotEqual(read_pricing_views(store, "BBB")["all"][0][1], "50")
            self.assertEqual(
                result["stages"]["maintenance_after_discovery"]["stages"]["refresh"]["check"]["stats"].get(
                    "checked", 0
                ),
                0,
            )

    def test_new_correction_obligation_reuses_comparison_but_not_append_approval(self):
        with fixture() as case:
            context = accept_context(case.store)
            policy = WorkPolicy(
                case.store,
                context.select_index(context.accept_store().root),
                {},
                [],
                "2026-10-17",
                context=context,
            )
            arguments = dict(
                pricing_client=case.pricing,
                identity_client=IdentityClient({"AAA": asset("AAA")}),
                end_date="2026-10-17",
                context=context,
                work_policy=policy,
                on_ready=lambda ready: None,
            )
            self.assertTrue(refresh_existing(case.store, **arguments)["complete"])
            self.assertIsNone(context.history_checks["AAA"][2])
            policy.require_correction(pricing_refresh.prepare_refresh(case.store, {"AAA"}))
            correction = policy.work["AAA"]["correction_id"]
            result = refresh_existing(case.store, **arguments)
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["check"]["stats"]["checked"], 0)
            self.assertEqual([item["status"] for item in result["pricing"]["results"]], ["replaced"])
            self.assertEqual(context.history_checks["AAA"][2], correction)

    def test_changed_summary_token_cannot_retain_comparisons(self):
        with fixture() as case:
            context = accept_context(case.store)
            with patch.object(refresh, "assert_current", side_effect=[None, DownloadError("summary changed")]):
                with self.assertRaisesRegex(DownloadError, "summary changed"):
                    refresh_existing(
                        case.store,
                        pricing_client=case.pricing,
                        identity_client=IdentityClient({"AAA": asset("AAA")}),
                        end_date="2026-10-17",
                        context=context,
                    )
            self.assertEqual(context.history_checks, {})
            self.assertEqual(context.history_comparisons, {})

    def seed_independent_pair(self, case, symbols=("AAA", "BBB")):
        store = case.directory / "independent"
        assets = {s: asset(s) for s in symbols}
        for symbol in assets:
            download.download_pricing(store, symbol, "2026-10-17", client=FakeClient(case.old))
        self.assertEqual(run_lineage(store, identity_client=IdentityClient(assets))["failed"], 0)
        rebuild(store)
        rebuild_universe(store)
        self.assertTrue(maintain(store, bootstrap=True)["complete"])
        case.live = {s: case.old + [bar("2026-10-19")] for s in assets}
        case.pages.clear()
        return store, assets

    def test_partial_check_retains_difference_without_approving_its_uncorrected_history(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case, ("AAA", "BBB", "CCC"))
            case.live["BBB"] = [bar("2016-01-04", "50"), case.old[-1], bar("2026-10-19")]
            case.live["CCC"] = [case.old[-1]]
            rounds = []
            original = update.refresh_existing

            def observed(*args, **kwargs):
                before = len(case.pages)
                try:
                    return original(*args, **kwargs)
                finally:
                    context = kwargs["context"]
                    rounds.append(
                        (dict(context.history_checks), dict(context.history_comparisons), case.pages[before:])
                    )

            with patch.object(update, "refresh_existing", side_effect=observed):
                result = run_update(
                    store,
                    "2026-10-20",
                    pricing_client=case.pricing,
                    identity_client=CatalogueClient(list(assets.values())),
                    registry=case.registry,
                )
            self.assertFalse(result["complete"], result)
            self.assertEqual(set(rounds[0][0]), {"AAA"})
            self.assertEqual(
                {s: item[2].decision for s, item in rounds[0][1].items()},
                {"AAA": "unchanged", "BBB": "full_history"},
            )
            # Replacement and its independent output checks remain. The daily
            # summary comparisons themselves must not be repeated.
            self.assertEqual(
                result["stages"]["maintenance_after_discovery"]["stages"]["refresh"]["check"]["stats"][
                    "checked"
                ],
                0,
            )
            self.assertFalse(any("AAA" in p["symbols"].split(",") for p in rounds[1][2]))
            rows = read_pricing_views(store, "BBB")["all"]
            self.assertEqual((rows[0][1], len(rows)), ("50", 3))

    def test_successful_append_is_retained_when_peer_append_fails(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)
            publish = append._publish_append

            def fail_peer(store, entry, *args, **kwargs):
                if entry["symbol"] == "BBB":
                    raise OSError("synthetic peer publication failure")
                return publish(store, entry, *args, **kwargs)

            with patch.object(append, "_publish_append", side_effect=fail_peer):
                result = run_update(
                    store,
                    "2026-10-20",
                    pricing_client=case.pricing,
                    identity_client=CatalogueClient(list(assets.values())),
                    registry=case.registry,
                )
            self.assertFalse(result["complete"], result)
            self.assertEqual(len(read_pricing_views(store, "AAA")["all"]), 3)
            self.assertEqual(len(read_pricing_views(store, "BBB")["all"]), 2)
            self.assertEqual(len([p for p in case.pages if "AAA" in p["symbols"].split(",")]), 4, case.pages)

    def test_batch_omission_update_completes_append_lineage_and_serving(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)

            def page(params):
                result = case.page(params)
                if params["symbols"] == "AAA,BBB" and params["start"][:10] == "2016-01-04":
                    result["bars"].pop("AAA", None)
                return result

            with patch.object(case.pricing, "_get", side_effect=page):
                result = run_update(
                    store,
                    "2026-10-20",
                    pricing_client=case.pricing,
                    identity_client=CatalogueClient(list(assets.values())),
                    registry=case.registry,
                )
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["pending_work"], 0)
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
            checks = [p for p in case.pages if p["start"][:10] < "2026-10-17"]
            self.assertEqual(len(checks), 3, checks)
            self.assertEqual(checks[-1], {**checks[0], "symbols": "AAA"})
            for symbol in assets:
                self.assertEqual(len(read_pricing_views(store, symbol)["all"]), 3)

    def test_partial_check_retains_matches_across_full_update_phases(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)
            case.live["BBB"] = [case.old[-1]]
            rounds = []
            original = update.refresh_existing

            def observed(*args, **kwargs):
                before = len(case.pages)
                try:
                    return original(*args, **kwargs)
                finally:
                    rounds.append((dict(kwargs["context"].history_checks), case.pages[before:]))

            with patch.object(update, "refresh_existing", side_effect=observed):
                result = run_update(
                    store,
                    "2026-10-20",
                    pricing_client=case.pricing,
                    identity_client=CatalogueClient(list(assets.values())),
                    registry=case.registry,
                )
            self.assertFalse(result["complete"], result)
            self.assertIn("live check incomplete: 1 missing", result["stages"]["maintenance"]["error"])
            self.assertGreaterEqual(len(rounds), 2)
            self.assertEqual(set(rounds[0][0]), {"AAA"})
            self.assertFalse(any("AAA" in p["symbols"].split(",") for p in rounds[1][1]))
            self.assertEqual(len(read_pricing_views(store, "AAA")["all"]), 3)
            self.assertEqual(len(read_pricing_views(store, "BBB")["all"]), 2)

    def test_partial_check_approval_rejects_changed_generation_and_restart(self):
        for invalidation in ("generation", "restart"):
            with self.subTest(invalidation=invalidation), fixture() as case:
                store, assets = self.seed_independent_pair(case)
                case.live["BBB"] = [case.old[-1]]
                context = accept_context(store)
                arguments = dict(
                    pricing_client=case.pricing,
                    identity_client=IdentityClient(assets),
                    end_date="2026-10-17",
                    context=context,
                )
                with self.assertRaisesRegex(DownloadError, "live check incomplete"):
                    refresh_existing(store, **arguments)
                self.assertEqual(set(context.history_checks), {"AAA"})
                if invalidation == "generation":
                    case.live["AAA"] = [case.old[0], bar("2026-10-15"), case.old[-1]]
                    download.download_pricing(
                        store, "AAA", "2026-10-17", refresh_all=True, client=FakeClient(case.live["AAA"])
                    )
                    self.assertEqual(
                        run_lineage(
                            store, ["AAA"], identity_client=arguments["identity_client"], context=context
                        )["failed"],
                        0,
                    )
                else:
                    arguments["context"] = accept_context(store)
                case.pages.clear()
                self.assertTrue(refresh_existing(store, symbols={"AAA"}, **arguments)["complete"])
                self.assertEqual(len(case.pages), 2, case.pages)

    def test_daily_update_replaces_history_when_only_first_session_changes(self):
        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.live["AAA"] = [bar("2016-01-04", "50"), case.old[-1], bar("2026-10-19")]
            result = case.update("2026-10-20")
            self.assertTrue(result["complete"], result)
            self.assertEqual(
                result["stages"]["maintenance"]["stages"]["refresh"]["check"]["stats"]["differed"], 1
            )
            for rows in read_pricing_views(case.store, "AAA").values():
                self.assertEqual(rows[0][1], "50")
                self.assertEqual(len(rows), 3)
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])

    def test_live_approval_respects_selected_scope_and_interior_generation_change(self):
        with fixture() as case:
            context = accept_context(case.store)
            identity = IdentityClient({"AAA": asset("AAA")})
            arguments = dict(
                pricing_client=case.pricing, identity_client=identity, end_date="2026-10-17", context=context
            )
            first = []
            self.assertTrue(refresh_existing(case.store, on_ready=first.append, **arguments)["complete"])
            baseline = read_snapshot(case.store)["entries"]
            case.pages.clear()
            empty = []
            self.assertTrue(
                refresh_existing(case.store, symbols=set(), on_ready=empty.append, **arguments)["complete"]
            )
            self.assertEqual(empty[0]["entries"], [])
            self.assertEqual(case.pages, [])
            case.live["AAA"] = [case.old[0], bar("2026-10-15"), case.old[-1]]
            download.download_pricing(
                case.store, "AAA", "2026-10-17", refresh_all=True, client=FakeClient(case.live["AAA"])
            )
            self.assertEqual(
                run_lineage(case.store, ["AAA"], identity_client=identity, context=context)["failed"], 0
            )
            refreshed = read_snapshot(case.store)["entries"]
            self.assertEqual(refreshed[0]["first_all"], baseline[0]["first_all"])
            self.assertEqual(refreshed[0]["last_all"], baseline[0]["last_all"])
            self.assertNotEqual(refreshed[0]["tail_all"], baseline[0]["tail_all"])
            ready = []
            self.assertTrue(refresh_existing(case.store, on_ready=ready.append, **arguments)["complete"])
            self.assertEqual(len(case.pages), 2, "same endpoints cannot lend an older generation's approval")

    def test_endpoint_handoff_rejects_changed_scope_before_publication(self):
        for field, value in (
            ("store", "/unrelated-store"),
            ("symbol", "OTHER"),
            ("end", "2026-10-21T00:00:00Z"),
            ("asof", "2020-01-01"),
            ("generation", "stale"),
        ):
            with self.subTest(field=field), fixture() as case:
                original = acquire.download_pricing
                before = snapshot(case.store)

                def wrong_scope(*args, **kwargs):
                    kwargs["endpoint_selection"] = replace(kwargs["endpoint_selection"], **{field: value})
                    return original(*args, **kwargs)

                with patch.object(acquire, "download_pricing", side_effect=wrong_scope):
                    result = acquire_selected(case.store, ["AAA"], "2026-10-20", client=case.pricing)
                self.assertEqual(result["failed"], 1, result)
                self.assertIn("endpoint selection", result["results"][0]["outcome"]["error"])
                self.assertEqual(snapshot(case.store), before)

    def test_sigint_restart_finishes_selected_dependency_and_publication_journal(self):
        for interruption in ("dependency_request", "publication_journal"):
            with self.subTest(interruption=interruption), fixture() as case:
                store = seed_group(case, inactive_second=True)
                assets = {"AAA": asset("AAA"), "BBB": {**asset("BBB"), "status": "inactive"}}
                exact = {**assets, "111111111": assets["BBB"]}
                case.live = {s: case.old + [bar("2026-10-19")] for s in assets}
                arguments = dict(
                    pricing_client=case.pricing,
                    identity_client=ActionCatalogueClient(list(assets.values()), [rename()], exact=exact),
                    registry=case.registry,
                )
                trace_path = case.directory / "interrupted.jsonl"
                diagnostic = case.directory / "child-error.txt"
                child = os.fork()
                if child == 0:
                    trace = DebugTrace(trace_path)
                    phase = None

                    def progress(stage, details):
                        nonlocal phase
                        if details.get("state") == "started":
                            phase = stage

                    def page(params):
                        if interruption == "dependency_request" and (phase or "").endswith("boundary_check"):
                            signal.raise_signal(signal.SIGINT)
                        return case.page(params)

                    def finish(*args, **kwargs):
                        signal.raise_signal(signal.SIGINT)

                    try:
                        with trace.activate(), patch.object(case.pricing, "_get", side_effect=page):
                            if interruption == "publication_journal":
                                with patch.object(pricing_storage, "_finish_publication", side_effect=finish):
                                    run_update(store, "2026-10-20", progress=progress, **arguments)
                            else:
                                run_update(store, "2026-10-20", progress=progress, **arguments)
                    except KeyboardInterrupt:
                        trace.close("aborted", 130)
                        os._exit(130)
                    except BaseException:
                        diagnostic.write_text(traceback.format_exc())
                        os._exit(1)
                    os._exit(2)  # The selected interruption must actually occur.
                _, status = os.waitpid(child, 0)
                self.assertEqual(
                    os.waitstatus_to_exitcode(status), 130, diagnostic.read_text() if diagnostic.exists() else ""
                )
                if interruption == "publication_journal":
                    self.assertTrue((store / "end_of_day/pricing/AAA/.pending.json").exists())
                result = run_update(store, "2026-10-20", **arguments)
                self.assertTrue(result["complete"], result)
                self.assertEqual(result["pending_work"], 0)
                self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
                for symbol in assets:
                    self.assertEqual(len(read_pricing_views(store, symbol)["all"]), 3)
                    self.assertFalse((store / f"end_of_day/pricing/{symbol}/.pending.json").exists())
                case.pages.clear()
                repeated = run_update(store, "2026-10-20", **arguments)
                self.assertTrue(repeated["complete"], repeated)
                self.assertTrue(
                    all(
                        p["adjustment"] == "all" and p["start"][:10] in {"2016-01-04", "2026-10-16"}
                        for p in case.pages
                    )
                )

    def test_summary_member_is_checked_before_its_empty_dependency_advances(self):
        for changed_metadata in (False, True):
            with self.subTest(changed_metadata=changed_metadata):
                self.check_summary_routing(changed_metadata)

    def check_summary_routing(self, changed_metadata):
        with fixture() as case:
            store = case.directory / "linked"
            assets = {s: asset(s) for s in ("AAA", "BBB")}
            exact = {**assets, "111111111": assets["BBB"]}
            for symbol, rows in (("AAA", case.old), ("BBB", [])):
                download.download_pricing(store, symbol, "2026-10-17", client=FakeClient(rows))
            self.assertEqual(run_lineage(store, identity_client=IdentityClient(exact, [rename()]))["failed"], 0)
            rebuild(store)
            rebuild_universe(store)
            self.assertTrue(maintain(store, bootstrap=True)["complete"])
            self.assertEqual([e["symbol"] for e in read_snapshot(store)["entries"]], ["AAA"])
            case.live = {"AAA": case.old + [bar("2026-10-19")], "BBB": []}
            if changed_metadata:
                assets["AAA"] = {**assets["AAA"], "name": "Changed name"}
                exact["AAA"] = assets["AAA"]
            identity = ActionCatalogueClient(list(assets.values()), [rename()], exact=exact)
            result = run_update(
                store,
                "2026-10-20",
                pricing_client=case.pricing,
                identity_client=identity,
                registry=case.registry,
            )
            self.assertTrue(result["complete"], result)
            aaa = [p for p in case.pages if "AAA" in p["symbols"].split(",")]
            checks = [p for p in aaa if p["start"][:10] < "2026-10-17"]
            self.assertEqual(len(checks), 2, checks)
            self.assertTrue(all(p["adjustment"] == "all" for p in checks))
            self.assertEqual({p["start"][:10] for p in checks}, {"2016-01-04", "2026-10-16"})
            self.assertTrue(
                all((timestamp(p["end"]) - timestamp(p["start"])).total_seconds() < 86400 for p in checks)
            )
            self.assertEqual(len(aaa), 4)
            self.assertEqual(len(read_pricing_views(store, "AAA")["raw"]), 3)
            self.assertEqual(read_pricing_views(store, "BBB")["raw"], ())
            self.assertTrue(result["stages"]["serving_catalogue"]["complete"])

    def test_retained_acquisition_keeps_endpoint_requests_exact(self):
        with fixture() as case:
            phase, checks = None, []

            def progress(stage, details):
                nonlocal phase
                if details.get("state") == "started":
                    phase = stage

            def page(params):
                if phase in {"boundary_check", "acquisition_check"}:
                    checks.append(dict(params))
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=page):
                result = acquire_selected(
                    case.store, ["AAA"], "2026-10-20", client=case.pricing, progress=progress
                )
            self.assertEqual(result["failed"], 0, result)
            self.assertTrue(checks)
            for params in checks:
                self.assertLess(
                    timestamp(params["end"]) - timestamp(params["start"]), timedelta(hours=26), params
                )
            self.assertEqual(len(read_pricing_views(case.store, "AAA")["all"]), len(case.old))

    def test_retained_execution_consumes_endpoints_and_comparison_once(self):
        with fixture() as case:
            with (
                patch.object(download, "read_validator", side_effect=AssertionError("validator reread")),
                patch.object(download, "_endpoints_match", wraps=download._endpoints_match) as compare,
            ):
                result = acquire_selected(case.store, ["AAA"], "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 0, result)
            # Accepted history must not be confirmed again during append.
            self.assertEqual(compare.call_count, 0)
            self.assertEqual(result["results"][0]["consistency_check"], "matched")

    def test_descriptive_metadata_change_keeps_history_closed_and_reuses_active_partition(self):
        case = NativeCase()
        case.setUp()
        self.addCleanup(case.doCleanups)
        download.download_pricing(
            case.store, "TEST", "2026-10-15", client=FakeClient(valid_bars(end="2026-10-15"))
        )
        reads = Counter()
        original = Path.read_bytes

        def read(path):
            if "/pricing/TEST/" in str(path):
                reads[str(path)] += 1
            return original(path)

        with patch.object(Path, "read_bytes", read):
            result = run_lineage(
                case.store,
                ["TEST"],
                identity_client=IdentityClient({"TEST": {**asset("TEST"), "name": "Changed name"}}),
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(result["results"][0]["reused_closed_boundaries"])
        hot = {p: n for p, n in reads.items() if "/2026/Oct/" in p and p.endswith((".json", ".csv.gz"))}
        self.assertTrue(hot, reads)
        self.assertTrue(all(n == 1 for n in hot.values()), hot)
        cold_payloads = {
            path: count for path, count in reads.items() if path.endswith(".csv.gz") and "/2026/Oct/" not in path
        }
        self.assertEqual(cold_payloads, {})

    def test_single_source_never_builds_multi_source_rankings(self):
        case = NativeCase()
        case.setUp()
        self.addCleanup(case.doCleanups)
        with patch.object(observations, "_own_interval", side_effect=AssertionError("unused route ranking")):
            result = run_lineage(
                case.store,
                ["TEST"],
                refresh=True,
                identity_client=IdentityClient({"TEST": asset("TEST")}),
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["unresolved_rows"], 0)
        self.assertEqual(case.bootstrap().online_count(), 1)

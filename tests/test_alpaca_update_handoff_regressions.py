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
from libs.alpaca_pricing_download import acquire, append, download, full_publication
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
    def test_completed_full_acquisition_is_not_checked_again_by_daily_maintenance(self):
        from alpaca_end_of_day import DataLibrary, StandaloneReader

        for scenario in ("initial", "mapping_only", "replacement"):
            with self.subTest(scenario=scenario), fixture() as case:
                if scenario == "initial":
                    case.records[:] = [asset("AAA"), asset("NEW")]
                    case.live["NEW"] = valid_bars("2026-10-15", "2026-10-16")
                    expected_symbol, expected_changed = "NEW", True
                else:
                    case.records[:] = [{**asset("AAA"), "id": "changed-AAA-provider-id"}]
                    expected_symbol = "AAA"
                    expected_changed = scenario == "replacement"
                    if expected_changed:
                        case.live["AAA"] = [bar("2016-01-04", "50"), case.old[-1]]
                case.identity = CatalogueClient(case.records)
                accepted = {}
                record = full_publication.accept_full_publication

                def completed(context, source, generation, endpoints):
                    proof = record(context, source, generation, endpoints)
                    accepted[proof.symbol] = proof
                    return proof

                def request(params):
                    repeated = accepted.keys() & set(params["symbols"].split(","))
                    self.assertFalse(repeated, f"completed full acquisition checked again: {repeated}")
                    return case.page(params)

                with (
                    patch.object(full_publication, "accept_full_publication", side_effect=completed),
                    patch.object(case.pricing, "_get", side_effect=request),
                ):
                    result = case.update()
                self.assertTrue(result["complete"], result)
                self.assertIn(expected_symbol, accepted)
                publication = next(
                    item
                    for item in result["stages"]["acquisition"]["results"]
                    if item["symbol"] == expected_symbol
                )
                self.assertEqual(publication["changed"], expected_changed)
                self.assertEqual(
                    result["stages"]["maintenance_after_discovery"]["stages"]["refresh"]["check"]["stats"].get(
                        "checked", 0
                    ),
                    0,
                )
                self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
                self.assertEqual(
                    StandaloneReader(case.store).get_history(expected_symbol, "all")["bar_count"],
                    len(case.live[expected_symbol]) if scenario == "initial" else 1,
                )
                if scenario == "initial":
                    self.assertTrue(DataLibrary(store=case.store).resolve_ticker(expected_symbol))

    def test_full_publication_acceptance_is_bound_to_every_actual_selector_and_requirement(self):
        from copy import deepcopy

        with fixture() as case:
            context = accept_context(case.store)
            download.download_pricing(
                case.store,
                "AAA",
                "2026-10-17",
                client=FakeClient(case.old),
                refresh_all=True,
                context=context.pricing(),
                acquisition_evidence_sha256="a" * 64,
            )
            proof = context.pricing().full_publications["AAA"]
            self.assertEqual(
                run_lineage(
                    case.store, ["AAA"], identity_client=IdentityClient({"AAA": asset("AAA")}), context=context
                )["failed"],
                0,
            )
            entry = next(e for e in context.select_summary()["entries"] if e["symbol"] == "AAA")
            index = context.select_index(context.accept_store().root)["symbols"]["AAA"]
            arguments = {
                "store": context.store,
                "entry": entry,
                "lineage_entry": index,
                "target": "2026-10-17",
                "invalidate_mapping": True,
                "acquisition_fingerprint": "a" * 64,
            }
            self.assertTrue(proof.applies(**arguments))
            changed_generation = {**index, "source_sha256": "changed"}
            changed_summary = {**entry, "source_sha256": "changed"}
            changed_first = deepcopy(entry)
            changed_first["first_all"][1] = "50"
            changed_last = deepcopy(entry)
            changed_last["last_all"][1] = "50"
            changed_tail = deepcopy(entry)
            changed_tail["tail_all"][-1][1] = "50"
            for change in (
                {"store": case.directory / "other"},
                {"target": "2026-10-20"},
                {"lineage_entry": changed_generation},
                {"entry": changed_summary},
                {"entry": {**entry, "asof": "2026-10-16"}},
                {"entry": changed_first},
                {"entry": changed_last},
                {"entry": changed_tail},
                {"acquisition_fingerprint": "another-UUID-proof"},
                {"correction_id": "new-correction"},
            ):
                with self.subTest(change=change):
                    self.assertFalse(proof.applies(**{**arguments, **change}))
            self.assertTrue(proof.applies(**arguments))

    def test_full_publication_survives_peer_interruption_without_reopening_accepted_inputs(self):
        with fixture() as case:
            context = accept_context(case.store)
            publication = download.download_pricing(
                case.store,
                "GOOD",
                "2026-10-17",
                client=FakeClient(valid_bars("2026-10-15", "2026-10-16")),
                context=context.pricing(),
            )
            proof = context.pricing().full_publications["GOOD"]
            publication["request"]["asof"] = "caller mutation"
            self.assertEqual(proof.asof, "2026-10-17")
            with (
                patch.object(download, "publish", side_effect=InterruptedError("publication incomplete")),
                self.assertRaisesRegex(InterruptedError, "publication incomplete"),
            ):
                download.download_pricing(
                    case.store,
                    "FAILED",
                    "2026-10-17",
                    client=FakeClient(case.old),
                    context=context.pricing(),
                )
            self.assertEqual(set(context.pricing().full_publications), {"GOOD"})
            self.assertEqual(
                run_lineage(
                    case.store,
                    ["GOOD"],
                    identity_client=IdentityClient({"GOOD": asset("GOOD")}),
                    context=context,
                )["failed"],
                0,
            )
            original = Path.read_bytes

            def no_accepted_price_read(path):
                if "GOOD" in path.parts and (path.suffix == ".gz" or path.name.startswith("validator_")):
                    raise AssertionError("accepted publication payload or validator reopened")
                return original(path)

            with (
                patch.object(refresh, "verify", side_effect=AssertionError("completed history checked again")),
                patch.object(Path, "read_bytes", no_accepted_price_read),
            ):
                result = refresh_existing(
                    case.store,
                    pricing_client=case.pricing,
                    identity_client=IdentityClient({"GOOD": asset("GOOD")}),
                    end_date="2026-10-17",
                    symbols={"GOOD"},
                    context=context,
                )
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["check"]["stats"].get("checked", 0), 0)

    def test_consumed_publication_cache_cannot_bypass_changed_mapping_or_target(self):
        for invalidation in ("mapping", "target"):
            with self.subTest(invalidation=invalidation), fixture() as case:
                context = accept_context(case.store)
                download.download_pricing(
                    case.store,
                    "AAA",
                    "2026-10-17",
                    client=FakeClient(case.old),
                    refresh_all=True,
                    acquisition_evidence_sha256="a" * 64,
                    context=context.pricing(),
                )
                self.assertEqual(
                    run_lineage(
                        case.store,
                        ["AAA"],
                        identity_client=IdentityClient({"AAA": asset("AAA")}),
                        context=context,
                    )["failed"],
                    0,
                )
                index = context.select_index(context.accept_store().root)
                requirement = {
                    "status": "pending",
                    "end_exclusive": "2026-10-17",
                    "required_group": ["AAA"],
                    "invalidate_pricing": True,
                    "acquisition_evidence_fingerprint": "a" * 64,
                }
                policy = WorkPolicy(
                    case.store,
                    index,
                    {"AAA": requirement},
                    [],
                    "2026-10-17",
                    context=context,
                )
                first = refresh_existing(
                    case.store,
                    pricing_client=case.pricing,
                    identity_client=case.identity,
                    end_date="2026-10-17",
                    symbols={"AAA"},
                    work_policy=policy,
                    context=context,
                )
                self.assertTrue(first["complete"], first)
                self.assertIn("AAA", context.history_checks)
                target = "2026-10-17"
                if invalidation == "mapping":
                    policy.work["AAA"]["acquisition_evidence_fingerprint"] = "b" * 64
                else:
                    target = "2026-10-20"
                with (
                    patch.object(
                        refresh, "verify", side_effect=AssertionError("new scope requires owning check")
                    ),
                    self.assertRaisesRegex(AssertionError, "new scope requires owning check"),
                ):
                    refresh_existing(
                        case.store,
                        pricing_client=case.pricing,
                        identity_client=case.identity,
                        end_date=target,
                        symbols={"AAA"},
                        work_policy=policy,
                        context=context,
                    )

    def test_missing_required_mapping_handoff_selects_recovery_without_provider_work(self):
        for cached in (False, True):
            with self.subTest(cached=cached), fixture() as case:
                context = accept_context(case.store)
                download.download_pricing(
                    case.store,
                    "AAA",
                    "2026-10-17",
                    client=FakeClient(case.old),
                    refresh_all=True,
                    acquisition_evidence_sha256="a" * 64,
                    context=context.pricing(),
                )
                self.assertEqual(
                    run_lineage(
                        case.store,
                        ["AAA"],
                        identity_client=IdentityClient({"AAA": asset("AAA")}),
                        context=context,
                    )["failed"],
                    0,
                )
                policy = WorkPolicy(
                    case.store,
                    context.select_index(context.accept_store().root),
                    {
                        "AAA": {
                            "status": "pending",
                            "end_exclusive": "2026-10-17",
                            "required_group": ["AAA"],
                            "invalidate_pricing": True,
                            "acquisition_evidence_fingerprint": "a" * 64,
                        }
                    },
                    [],
                    "2026-10-17",
                    context=context,
                )
                if cached:
                    result = refresh_existing(
                        case.store,
                        pricing_client=case.pricing,
                        identity_client=case.identity,
                        end_date="2026-10-17",
                        symbols={"AAA"},
                        work_policy=policy,
                        context=context,
                    )
                    self.assertTrue(result["complete"], result)
                    self.assertIn("AAA", context.history_checks)
                policy.work["AAA"].pop("acquisition_evidence_fingerprint")
                request_count = len(case.pages)
                with (
                    patch.object(refresh, "verify", side_effect=AssertionError("unbound comparison")),
                    patch.object(case.pricing, "_get", side_effect=AssertionError("unbound provider request")),
                    patch.object(refresh, "prepare_refresh", side_effect=AssertionError("unbound replacement")),
                    self.assertRaisesRegex(DownloadError, "missing accepted mapping-evidence handoff.*recovery"),
                ):
                    refresh_existing(
                        case.store,
                        pricing_client=case.pricing,
                        identity_client=case.identity,
                        end_date="2026-10-17",
                        symbols={"AAA"},
                        work_policy=policy,
                        context=context,
                    )
                self.assertEqual(len(case.pages), request_count)

    def test_index_only_generation_change_revokes_consumed_producer_approval(self):
        with fixture() as case:
            context = accept_context(case.store)
            download.download_pricing(
                case.store,
                "AAA",
                "2026-10-17",
                client=FakeClient(case.old),
                refresh_all=True,
                context=context.pricing(),
            )
            self.assertEqual(
                run_lineage(
                    case.store,
                    ["AAA"],
                    identity_client=IdentityClient({"AAA": asset("AAA")}),
                    context=context,
                )["failed"],
                0,
            )
            index = context.select_index(context.accept_store().root)
            changed_index = {
                **index,
                "symbols": {
                    **index["symbols"],
                    "AAA": {**index["symbols"]["AAA"], "source_sha256": "new-lineage-generation"},
                },
            }
            ready = []
            with (
                patch.object(context, "select_index", side_effect=[index, changed_index]),
                patch.object(refresh, "verify", side_effect=AssertionError("accepted history checked again")),
            ):
                refresh_existing(
                    case.store,
                    pricing_client=case.pricing,
                    identity_client=case.identity,
                    end_date="2026-10-17",
                    symbols={"AAA"},
                    context=context,
                    on_ready=ready.append,
                )
            self.assertEqual(len(ready), 1)
            self.assertEqual(ready[0]["entries"], [])
            self.assertEqual(ready[0]["approved_scopes"], {})
            self.assertNotIn("AAA", context.history_checks)

    def test_new_peer_failure_comparison_cannot_rebind_an_older_successful_approval(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)
            context = accept_context(store)
            first = refresh_existing(
                store,
                pricing_client=case.pricing,
                identity_client=IdentityClient(assets),
                end_date="2026-10-17",
                symbols={"AAA"},
                context=context,
            )
            self.assertTrue(first["complete"], first)
            approved = context.history_checks["AAA"]
            case.live = {"AAA": [bar("2016-01-04", "50"), case.old[-1]], "BBB": [case.old[-1]]}
            with self.assertRaisesRegex(DownloadError, "live check incomplete"):
                refresh_existing(
                    store,
                    pricing_client=case.pricing,
                    identity_client=IdentityClient(assets),
                    end_date="2026-10-20",
                    context=context,
                )
            self.assertIs(context.history_checks["AAA"], approved)
            self.assertEqual(context.history_scope("check", "AAA", approved).target, "2026-10-17")
            compared = context.history_comparisons["AAA"]
            self.assertEqual(context.history_scope("comparison", "AAA", compared).target, "2026-10-20")
            self.assertEqual(compared[2].decision, "full_history")

    def test_last_maintenance_completes_successful_peers_and_legacy_handoff(self):
        self.check_last_maintenance_peers()

    def test_changed_generation_does_not_inherit_a_peer_failure_legacy_handoff(self):
        self.check_last_maintenance_peers(invalidate_legacy=True)

    def check_last_maintenance_peers(self, *, invalidate_legacy=False):
        from libs.alpaca_lineage_build import pipeline, universe_state
        from libs.alpaca_pricing_download import history_tail
        from libs.alpaca_pricing_download.contract import VALIDATOR_FILE, digest, json_bytes

        with fixture() as case:
            store, assets = self.seed_independent_pair(case, ("AAA", "BBB", "CCC"))
            root = store / "end_of_day/pricing/CCC"
            source = pricing_storage.load_source(root, "CCC")
            validator = pricing_storage.read_document(root / VALIDATOR_FILE)
            validator["schema_version"] = 31
            validator.pop("tail_all")
            (root / VALIDATOR_FILE).write_bytes(json_bytes(validator))
            source["schema_version"] = 31
            source["validator"]["sha256"] = digest(json_bytes(validator))
            (root / "source.manifest.json").write_bytes(json_bytes(source))
            self.assertEqual(run_lineage(store, ["CCC"], identity_client=IdentityClient(assets))["failed"], 0)
            rebuild(store)
            context = accept_context(store)
            policy = WorkPolicy(
                store, context.select_index(context.accept_store().root), {}, [], "2026-10-17", context=context
            )
            for symbol in assets:
                policy.require(symbol, "2026-10-17", reason="test_maintenance", history_check=True)
            case.live = {
                "AAA": [bar("2016-01-04", "50"), case.old[-1]],
                "BBB": [bar("2016-01-04", "60"), case.old[-1]],
                "CCC": case.old,
            }
            process, correlate = pipeline.process_group, pipeline.correlate
            publish_tail, read = history_tail.publish_legacy_tail, Path.read_bytes
            reconciled, upgraded = [], []
            changing_generation = False

            def selected(store, root, seed, **kwargs):
                reconciled.append(seed)
                return process(store, root, seed, **kwargs)

            def fail_peer(store, inputs, *args, **kwargs):
                nonlocal changing_generation
                if "BBB" in inputs:
                    if invalidate_legacy:
                        changing_generation = True
                        try:
                            download.download_pricing(
                                store,
                                "CCC",
                                "2026-10-17",
                                refresh_all=True,
                                client=FakeClient([bar("2016-01-04", "75"), case.old[-1]]),
                            )
                        finally:
                            changing_generation = False
                    raise RuntimeError("synthetic independent lineage failure")
                return correlate(store, inputs, *args, **kwargs)

            def published(*args, **kwargs):
                value = publish_tail(*args, **kwargs)
                upgraded.append(value[0]["source_sha256"])
                return value

            def no_reauthentication(path):
                if upgraded and not changing_generation and "CCC" in path.parts:
                    self.assertNotEqual(path.suffix, ".gz", "accepted legacy payload reopened")
                    self.assertNotEqual(path.name, VALIDATOR_FILE, "accepted legacy validator reopened")
                return read(path)

            with (
                patch.object(pipeline, "process_group", side_effect=selected),
                patch.object(pipeline, "correlate", side_effect=fail_peer),
                patch.object(history_tail, "publish_legacy_tail", side_effect=published),
                patch.object(Path, "read_bytes", no_reauthentication),
            ):
                result = update.update_everything(
                    store,
                    "2026-10-17",
                    pricing_client=case.pricing,
                    identity_client=IdentityClient(assets),
                    work_policy=policy,
                    context=context,
                    recover_pending=False,
                )
                policy.acknowledge()
            self.assertFalse(result["complete"], result)
            self.assertEqual(result["failed"], 1, result)
            expected = {"AAA": 1, "BBB": 1} if invalidate_legacy else {"AAA": 1, "BBB": 1, "CCC": 1}
            self.assertEqual(Counter(reconciled), Counter(expected))
            self.assertEqual(len(upgraded), 1)
            self.assertEqual(set(policy.history_approved), {"AAA"} if invalidate_legacy else {"AAA", "CCC"})
            if not invalidate_legacy:
                self.assertEqual(policy.history_approved["CCC"], upgraded[0])
            work = universe_state.read_state(store)["work"]
            self.assertEqual(
                {s: work[s]["status"] for s in assets},
                {"AAA": "complete", "BBB": "retry", "CCC": "pending" if invalidate_legacy else "complete"},
            )
            serving = maintain(store)
            # BBB's pricing publication remains unaccepted after its lineage
            # failure. Successful peers publish, but the operation is incomplete.
            self.assertFalse(serving["complete"], serving)
            self.assertGreaterEqual(serving["pending_export_inputs"]["pricing"], 1)
            self.assertEqual(serving["pending_symbols"], ["CCC"] if invalidate_legacy else [])
            self.assertEqual(serving["evaluated_groups"], 1 if invalidate_legacy else 2)
            from libs.alpaca_pricing_download.update_checks import read_checks

            self.assertEqual(set(read_checks(store)["checks"]), {"BBB", "CCC"} if invalidate_legacy else {"BBB"})
            ccc_calls = [p for p in case.pages if "CCC" in p["symbols"].split(",")]
            self.assertEqual(len(ccc_calls), 2, ccc_calls)
            self.assertEqual({p["adjustment"] for p in ccc_calls}, {"all"})

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
                completed = set()
                full = full_publication.accept_full_publication
                appended = full_publication.accept_approved_append

                def record_full(*args, **kwargs):
                    accepted = full(*args, **kwargs)
                    completed.add(accepted.symbol)
                    return accepted

                def record_append(*args, **kwargs):
                    accepted = appended(*args, **kwargs)
                    completed.add(accepted.symbol)
                    return accepted

                def request(params):
                    if "BBB" in completed and "BBB" in params["symbols"].split(","):
                        raise AssertionError("completed dependency publication checked again")
                    return case.page(params)

                with (
                    patch.object(full_publication, "accept_full_publication", side_effect=record_full),
                    patch.object(full_publication, "accept_approved_append", side_effect=record_append),
                    patch.object(case.pricing, "_get", side_effect=request),
                ):
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
                self.assertIn("BBB", completed)
                self.assertEqual(result["check"]["stats"].get("tickers", 0), 0)
                self.assertEqual(result["check"]["stats"].get("matched", 0), 0)
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

    def test_all_failed_refresh_does_not_enter_append(self):
        with fixture() as case:
            store, assets = self.seed_independent_pair(case)
            for symbol in assets:
                case.live[symbol] = [bar("2016-01-04", "50"), case.old[-1]]
            with (
                patch.object(pricing_refresh, "download_pricing", side_effect=OSError("correction failed")),
                patch.object(update, "append_ready", side_effect=AssertionError("empty append selected")),
            ):
                result = update.update_everything(
                    store,
                    "2026-10-17",
                    pricing_client=case.pricing,
                    identity_client=IdentityClient(assets),
                )
            self.assertFalse(result["complete"], result)
            self.assertNotIn("append", result["stages"])

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

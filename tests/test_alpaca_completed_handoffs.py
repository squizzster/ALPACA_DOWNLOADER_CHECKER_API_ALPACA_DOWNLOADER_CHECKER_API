"""Completed boundaries avoid repeated work without weakening invalidation or recovery."""

import copy
import unittest
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as lineage_fixture
from test_alpaca_durable_maintenance import ProcessInterrupted, fixture, rename, seed_group
from test_alpaca_lineage_batching import END, START, Provider, event
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_lineage_build import context as lineage_context
from libs.alpaca_lineage_build import pipeline, storage, universe_state, universe_update
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import append, check_summary, download, http, responses
from libs.alpaca_pricing_download import context as pricing_context
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.update_checks import queue_check


class CompletedDecisionTests(unittest.TestCase):
    def pending_approval(self, case):
        index = storage.read_index(case.store / "end_of_day/lineage")
        policy = WorkPolicy(case.store, index, {}, [], "2026-10-17")
        policy.require("AAA", "2026-10-17", reason="metadata_refresh", refresh=True, history_check=True)
        result = pipeline.run_lineage(case.store, ["AAA"], identity_client=case.identity, work_policy=policy)
        self.assertEqual(result["failed"], 0, result)
        return policy

    def test_completed_decision_waits_for_approval_without_recalculating(self):
        with fixture() as case:
            policy = self.pending_approval(case)
            root = case.store / "end_of_day/lineage"
            receipt = storage.read_receipt(root, "AAA", storage.read_index(root))
            self.assertEqual(
                receipt["evidence_requirements"], {"AAA": policy.work["AAA"]["evidence_fingerprint"]}
            )
            with (
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("pricing decoded again")),
                patch.object(pipeline, "correlate", side_effect=AssertionError("decision recalculated")),
                patch.object(pipeline, "acquire", side_effect=AssertionError("evidence reacquired")),
            ):
                result = pipeline.run_lineage(
                    case.store, ["AAA"], identity_client=case.identity, work_policy=policy
                )
                self.assertTrue(result["results"][0]["reused_decision"])
                policy.acknowledge()
                self.assertEqual(policy.pending_work, {"AAA"})
                policy.history_approved["AAA"] = receipt["source_sha256"]
                result = pipeline.run_lineage(
                    case.store, ["AAA"], identity_client=case.identity, work_policy=policy
                )
                self.assertEqual(result["failed"], 0, result)
                self.assertTrue(result["results"][0]["reused_decision"])
                policy.acknowledge()
                self.assertEqual(policy.pending_work, set())
            approved = storage.read_receipt(root, "AAA", storage.read_index(root))
            self.assertEqual(approved["boundaries"], receipt["boundaries"])
            self.assertEqual(approved["evidence_requirements"], receipt["evidence_requirements"])

    def test_adding_history_approval_does_not_renew_a_completed_evidence_requirement(self):
        with fixture() as case:
            policy = WorkPolicy(
                case.store, storage.read_index(case.store / "end_of_day/lineage"), {}, [], "2026-10-17"
            )
            policy.require("AAA", "2026-10-17", reason="metadata_refresh", refresh=True)
            result = pipeline.run_lineage(case.store, ["AAA"], identity_client=case.identity, work_policy=policy)
            self.assertEqual(result["failed"], 0, result)
            previous_id = policy.work["AAA"]["obligation_id"]
            policy.require("AAA", "2026-10-17", reason="history_check", history_check=True)
            self.assertNotEqual(policy.work["AAA"]["obligation_id"], previous_id)
            with (
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("pricing decoded again")),
                patch.object(pipeline, "correlate", side_effect=AssertionError("decision recalculated")),
                patch.object(pipeline, "acquire", side_effect=AssertionError("evidence reacquired")),
            ):
                result = pipeline.run_lineage(
                    case.store, ["AAA"], identity_client=case.identity, work_policy=policy
                )
            self.assertTrue(result["results"][0]["reused_decision"])
            policy.acknowledge()
            self.assertEqual(policy.pending_work, {"AAA"})

    def test_approval_publication_recovers_before_and_after_index_selection(self):
        for boundary in ("finish_publication", "_acknowledge"):
            with self.subTest(boundary=boundary), fixture() as case:
                policy = self.pending_approval(case)
                policy.history_approved["AAA"] = policy.index["symbols"]["AAA"]["source_sha256"]
                with (
                    patch.object(storage, boundary, side_effect=ProcessInterrupted()),
                    self.assertRaises(ProcessInterrupted),
                ):
                    pipeline.run_lineage(case.store, ["AAA"], identity_client=case.identity, work_policy=policy)
                restarted = WorkPolicy(
                    case.store,
                    storage.read_index(case.store / "end_of_day/lineage"),
                    universe_state.read_state(case.store)["work"],
                    [],
                    "2026-10-17",
                )
                with (
                    patch.object(pipeline, "load_pricing", side_effect=AssertionError("pricing decoded again")),
                    patch.object(pipeline, "correlate", side_effect=AssertionError("decision recalculated")),
                ):
                    result = pipeline.run_lineage(
                        case.store, ["AAA"], identity_client=case.identity, work_policy=restarted
                    )
                    self.assertTrue(result["recovered"], result)
                    self.assertEqual(result["failed"], 0, result)
                    restarted.acknowledge()
                self.assertEqual(restarted.pending_work, set())

    def test_daily_restart_reuses_evidence_while_finishing_history_approval(self):
        with fixture() as case:
            case.identity = CatalogueClient([asset("AAA")])
            self.pending_approval(case)
            with (
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("pricing decoded again")),
                patch.object(pipeline, "correlate", side_effect=AssertionError("decision recalculated")),
                patch.object(pipeline, "acquire", side_effect=AssertionError("evidence reacquired")),
            ):
                result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(len(case.pages), 1)
            self.assertEqual(len(result["stages"]["approval"]), 1)
            self.assertTrue(result["stages"]["approval"][0]["results"][0]["reused_decision"])

    def test_changed_source_or_explicit_refresh_invalidates_completed_evidence(self):
        for change in ("source", "refresh"):
            with self.subTest(change=change), fixture() as case:
                policy = self.pending_approval(case)
                if change == "source":
                    download.download_pricing(
                        case.store,
                        "AAA",
                        "2026-10-17",
                        client=FakeClient([bar("2016-01-04", "50"), case.old[-1]]),
                        refresh_all=True,
                    )
                with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as calculate:
                    result = pipeline.run_lineage(
                        case.store,
                        ["AAA"],
                        identity_client=case.identity,
                        work_policy=policy,
                        refresh=change == "refresh",
                    )
                self.assertEqual(result["failed"], 0, result)
                self.assertEqual(calculate.call_count, 1)


class AcceptedOperationTests(unittest.TestCase):
    def test_completed_append_approval_is_selected_without_reopening_the_group(self):
        for pending in (False, True):
            with self.subTest(pending_check=pending), fixture() as case:
                case.identity = CatalogueClient([asset("AAA")])
                case.live["AAA"] = [*case.old, bar("2026-10-19")]
                maintenance = universe_update.update_everything

                def completed(*args, **kwargs):
                    value = maintenance(*args, **kwargs)
                    self.assertTrue(value["complete"], value)
                    if pending:
                        queue_check(case.store, "AAA")
                    return value

                with (
                    patch.object(universe_update, "update_everything", side_effect=completed),
                    patch.object(universe_update, "run_lineage", wraps=pipeline.run_lineage) as approvals,
                ):
                    result = case.update("2026-10-20")
                self.assertTrue(result["complete"], result)
                self.assertEqual(approvals.call_count, int(pending))
                self.assertEqual(bool(result["stages"].get("approval")), pending)
                self.assertEqual(result["stages"]["discovery_lineage"], [])
                self.assertEqual(len(case.pages), 9)

    def test_progress_covers_phases_and_reconciles_page_and_http_totals(self):
        with fixture() as case:
            case.live["AAA"] = [bar("2016-01-04", "50"), bar("2026-10-16", "50"), bar("2026-10-19", "50")]
            events = []

            def requested(params):
                case.pricing.request_count += 1
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=requested):
                result = case.update(
                    "2026-10-20", progress=lambda stage, values: events.append((stage, copy.deepcopy(values)))
                )
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["requests"]["pricing_http_requests"], len(case.pages))
            self.assertEqual(result["requests"]["metadata_http_requests"], case.identity.request_count)
            pages = sum(values.get("page_requests", 0) for _, values in events)
            pages += sum(values["requests"]["page_requests"] for stage, values in events if stage == "checked")
            self.assertEqual(pages, len(case.pages))
            stages = {stage for stage, _ in events}
            self.assertTrue(
                {
                    "catalogue",
                    "registration",
                    "acquisition_selection",
                    "initial_or_invalidated_history",
                    "discovery_lineage_lineage_selection",
                    "discovery_lineage_lineage_correlation",
                    "discovery_lineage_lineage_publication",
                    "changed_data",
                    "append_acquisition",
                    "lineage_group",
                    "completion",
                }
                <= stages,
                stages,
            )
            self.assertNotIn("approval_lineage_group", stages, "append already published its approval")

    def test_completed_acquisition_reports_reuse_without_requests(self):
        with fixture() as case:
            events = []
            with patch.object(case.pricing, "_get", side_effect=AssertionError("acquisition repeated")):
                result = acquire_selected(
                    case.store,
                    ["AAA"],
                    "2026-10-17",
                    client=case.pricing,
                    progress=lambda stage, values: events.append((stage, values)),
                )
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual(events[0][0], "acquisition_selection")
            self.assertEqual(events[0][1]["reused"], 1)
            self.assertFalse(result["requests"])

    def test_covered_update_reads_membership_once_and_uses_only_live_comparison(self):
        with fixture() as case:
            case.identity = CatalogueClient([asset("AAA")])
            with (
                patch.object(storage, "read_index", wraps=storage.read_index) as indexes,
                patch.object(check_summary, "read_snapshot", wraps=check_summary.read_snapshot) as summaries,
                patch.object(
                    lineage_context, "validate_version", wraps=lineage_context.validate_version
                ) as lineage_versions,
                patch.object(
                    pricing_context, "validate_version", wraps=pricing_context.validate_version
                ) as pricing_versions,
                patch.object(storage, "read_receipt", side_effect=AssertionError("receipt census")),
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("payload census")),
            ):
                result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(indexes.call_count, 1)
            self.assertEqual(summaries.call_count, 1)
            self.assertEqual(lineage_versions.call_count, 1)
            self.assertEqual(pricing_versions.call_count, 1)
            self.assertEqual(len(case.pages), 1)

    def test_publications_refresh_membership_without_reloading_unchanged_revisions(self):
        with fixture() as case:
            revisions = []
            read = check_summary.read_snapshot

            def snapshot(*args, **kwargs):
                value = read(*args, **kwargs)
                revisions.append(value["token"])
                return value

            with (
                patch.object(check_summary, "read_snapshot", side_effect=snapshot),
                patch.object(
                    pricing_context, "initialize_store", wraps=pricing_context.initialize_store
                ) as initialize,
            ):
                result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(len(revisions), len(set(revisions)))
            self.assertGreater(len(revisions), 1)
            self.assertEqual(initialize.call_count, 1)

    def test_concurrent_index_publication_rejects_discovery_without_receipt_reads(self):
        with fixture() as case:
            case.identity = CatalogueClient([asset("AAA")])
            original = case.identity.execute_plan

            def observe(plan):
                result = original(plan)
                root = case.store / "end_of_day/lineage"
                with storage.publication_lock(root, exclusive=True):
                    storage._update_index(root, {})
                return result

            with (
                patch.object(case.identity, "execute_plan", side_effect=observe),
                patch.object(storage, "read_receipt", side_effect=AssertionError("receipt census")),
                self.assertRaisesRegex(RuntimeError, "changed during discovery"),
            ):
                case.update()

    def test_known_dependency_catchup_preserves_metadata_continuation(self):
        with fixture() as case:
            store = seed_group(case)
            assets = {s: asset(s) for s in ("AAA", "BBB")}
            assets["111111111"] = assets["BBB"]
            identity = IdentityClient(assets, [rename(), event("BBB", "new", process_date="2026-10-19")])
            policy = WorkPolicy(store, storage.read_index(store / "end_of_day/lineage"), {}, [], "2026-10-20")
            pricing = FakeClient([*case.old, bar("2026-10-19")])
            download.download_pricing(store, "AAA", "2026-10-20", client=pricing)
            policy.require_dependencies({"AAA", "BBB"}, "2026-10-20", {"BBB"})
            self.assertFalse(any(item["refresh_metadata"] for item in policy.work.values()))
            result = pipeline.run_lineage(
                store,
                ["AAA"],
                identity_client=identity,
                pricing_client=pricing,
                work_policy=policy,
                end_date="2026-10-20",
            )
            self.assertEqual(result["failed"], 0, result)
            self.assertTrue(identity.calls)
            self.assertTrue(all(start == "2026-10-01" for _, start, _ in identity.calls), identity.calls)
            receipt = storage.read_receipt(
                store / "end_of_day/lineage", "BBB", storage.read_index(store / "end_of_day/lineage")
            )
            self.assertEqual({a["id"] for a in receipt["evidence"]["corporate_actions"]}, {"rename", "new"})
            policy.require("BBB", "2026-10-20", reason="explicit_refresh", refresh=True)
            policy.require_dependencies({"AAA", "BBB", "NEW"}, "2026-10-20", {"NEW"})
            self.assertTrue(policy.work["BBB"]["refresh_metadata"])
            self.assertTrue(policy.work["NEW"]["refresh_metadata"])


class ExpandedPricingTests(unittest.TestCase):
    setUp = lineage_fixture.LineageBuildTests.setUp
    download = lineage_fixture.LineageBuildTests.download

    def test_expansion_reuses_only_unchanged_generations_and_scopes(self):
        for concurrent in (False, True):
            with self.subTest(concurrent=concurrent):
                self.store = self.store.parent / ("changed" if concurrent else "unchanged")
                for symbol in ("AAA", "BBB"):
                    self.download(symbol)
                identity = IdentityClient(
                    {"AAA": asset("AAA"), "BBB": asset("BBB"), "111111111": asset("BBB")}, [rename()]
                )
                locks, changed = storage.pricing_locks, False

                @contextmanager
                def selected(*args, **kwargs):
                    nonlocal changed
                    with locks(*args, **kwargs) as held:
                        yield held
                    if concurrent and not changed:
                        changed = True
                        self.download("AAA", bars=[bar("2016-01-04", "50"), *self.bars[1:]], refresh_all=True)

                with (
                    patch.object(storage, "pricing_locks", side_effect=selected),
                    patch.object(pipeline, "load_pricing", wraps=pipeline.load_pricing) as loads,
                    patch.object(pipeline, "correlate", wraps=pipeline.correlate) as calculate,
                ):
                    result = pipeline.run_lineage(self.store, ["AAA"], identity_client=identity)
                self.assertEqual(result["failed"], 0, result)
                self.assertEqual(
                    Counter(call.args[1] for call in loads.call_args_list),
                    {"AAA": 2 if concurrent else 1, "BBB": 1},
                )
                self.assertEqual(set(calculate.call_args.args[1]), {"AAA", "BBB"})
                if concurrent:
                    self.assertEqual(calculate.call_args.args[1]["AAA"].views["all"][0][4], "50")


class CompletedResponsesTests(unittest.TestCase):
    def test_contained_action_intervals_reuse_only_successful_same_operation_responses(self):
        provider = Provider(actions=[event("A", "old", process_date="2016-02-01"), event("A", "new")])
        client = EvidenceClient(provider.client)
        self.assertEqual(len(client.get_corporate_actions("A", START, END)), 2)
        for lower in ("2026-09-01", "2026-10-01"):
            self.assertEqual(client.get_corporate_actions("A", lower, END), [event("A", "new")])
        self.assertEqual(len(provider.calls), 1)
        client.get_corporate_actions("A", START, "2026-10-16")
        self.assertEqual(len(provider.calls), 2)
        EvidenceClient(provider.client).get_corporate_actions("A", START, END)
        self.assertEqual(len(provider.calls), 3)
        failed = EvidenceClient(provider.client)
        with patch.object(provider.client, "execute_plan", side_effect=RuntimeError("incomplete")):
            failed.prefetch_corporate_actions([("A", START, END)])
        self.assertEqual(failed.get_corporate_actions("A", "2026-10-01", END), [event("A", "new")])
        self.assertEqual(len(provider.calls), 4)
        provider.actions[-1]["process_date"] = "2026-10-02"
        with self.assertRaisesRegex(RuntimeError, "conflicting corporate-action"):
            client.get_corporate_actions("A", START, "2026-10-17")

    def test_acquired_responses_are_normalized_once_and_store_is_initialized_once(self):
        with fixture() as case:
            with (
                patch.object(http, "transform", wraps=http.transform) as normalize,
                patch.object(responses, "transform", side_effect=AssertionError("normalization repeated")),
                patch.object(
                    pricing_context, "initialize_store", wraps=pricing_context.initialize_store
                ) as initialize,
            ):
                result = acquire_selected(case.store, ["NEW", "EMPTY"], "2026-10-17", client=case.pricing)
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual(normalize.call_count, sum(len(p["symbols"].split(",")) for p in case.pages))
            self.assertEqual(initialize.call_count, 1)

    def test_incomplete_or_malformed_pages_never_produce_completed_bars(self):
        need = {
            "kind": "bars",
            "why": "test",
            "symbols": ["AAA"],
            "adjustments": ["all"],
            "asof": "2026-10-17",
            "ranges": [{"start": "2026-10-01", "end_exclusive": "2026-10-17"}],
        }
        plan = build_plan({"steps": [{"needs": [need]}]})
        bad = [
            {"bars": {"AAA": [bar("2026-10-02")]}, "next_page_token": "more"},
            {"bars": {"AAA": [bar("2026-10-02")]}},
            {"bars": {"AAA": [bar("2026-10-01")]}},
            {"bars": {"AAA": [{"t": "2026-10-03T05:00:00Z"}]}},
            {"bars": {"WRONG": []}},
        ]
        for last in bad:
            with self.subTest(last=last):
                pages = iter([{"bars": {"AAA": [bar("2026-10-02")]}, "next_page_token": "more"}, last])
                with self.assertRaises(DownloadError):
                    http.execute_bar_plan(plan, lambda _: next(pages))

    def test_standalone_entry_still_validates_package_and_store(self):
        with fixture() as case:
            with (
                patch.object(pricing_context, "validate_version", side_effect=DownloadError("invalid contract")),
                self.assertRaisesRegex(DownloadError, "invalid contract"),
            ):
                download.download_pricing(case.store, "AAA", "2026-10-17", client=case.pricing)
            (case.store / "store.manifest.json").write_text("{}")
            with self.assertRaisesRegex(DownloadError, "another format"):
                acquire_selected(case.store, ["AAA"], "2026-10-17", client=case.pricing)


class PublicationHandoffTests(unittest.TestCase):
    def test_same_month_append_retains_cold_references_and_reuses_publication_results(self):
        with fixture() as case:
            case.live["AAA"] = [*case.old, bar("2026-10-19")]
            snapshot = check_summary.read_snapshot(case.store)
            snapshot["approved_generations"] = append.select_append_sources(
                case.store, snapshot["entries"], "2026-10-20"
            )
            publish = pricing_storage.publish
            read = Path.read_bytes

            def no_cold(path):
                self.assertNotIn(path.name, {"completed_years.manifest.json", "year.manifest.json"})
                return read(path)

            def install(*args, **kwargs):
                with (
                    patch.object(
                        pricing_storage, "load_source", side_effect=AssertionError("published source reloaded")
                    ),
                    patch.object(
                        pricing_storage,
                        "read_validator",
                        side_effect=AssertionError("published endpoints reloaded"),
                    ),
                    patch.object(
                        pricing_storage,
                        "boundary_document",
                        side_effect=AssertionError("inventory rediscovered"),
                    ),
                    patch.object(Path, "read_bytes", no_cold),
                ):
                    return publish(*args, **kwargs)

            with (
                patch.object(download, "publish", side_effect=install),
                patch.object(append, "load_source", wraps=append.load_source) as source_reads,
            ):
                result = append.append_ready(case.store, snapshot, "2026-10-20", client=case.pricing)
            self.assertEqual(result["failed"], 0, result)
            self.assertEqual(source_reads.call_count, 1)
            self.assertEqual(
                check_summary.price_entry(
                    case.store / "end_of_day/pricing/AAA",
                    pricing_storage.load_source(case.store / "end_of_day/pricing/AAA", "AAA"),
                )["asof"],
                "2026-10-20",
            )


if __name__ == "__main__":
    unittest.main()

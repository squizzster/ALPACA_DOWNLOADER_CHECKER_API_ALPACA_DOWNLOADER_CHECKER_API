"""The two computation paths consume one completed publication in owner order."""

import json
import unittest
from contextlib import ExitStack
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from libs.alpaca_lineage_build import group_engine, pipeline
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.metadata import EvidenceClient


class CompletedGroupPublicationTests(unittest.TestCase):
    def publication(self):
        self.calls = []
        self.receipts = {
            "AAA": {"unresolved_rows": 2, "boundaries": {"current_month": {}, "completed_years": {}}}
        }
        self.boundaries = {"AAA": {"current_month": {"rows": ["already assembled"]}}}
        self.index = {"symbols": {"AAA": {"sha256": "published-generation"}}}
        self.completion = object()
        self.policy = SimpleNamespace(
            stamp=Mock(side_effect=lambda *args: self.calls.append("stamp") or self.completion),
            update_index=Mock(side_effect=lambda *args: self.calls.append("index")),
            accept_completion=Mock(side_effect=lambda *args: self.calls.append("acknowledge")),
        )
        self.cache = SimpleNamespace(published=Mock(side_effect=lambda *args: self.calls.append("cache")))
        self.graph = SimpleNamespace(update=Mock(side_effect=lambda *args: self.calls.append("graph")))
        self.kwargs = {
            "acquisition_waves": 1,
            "checks": {"held": "checks"},
            "graph": self.graph,
            "index": self.index,
            "notices": {"AAA": {"held": "notice"}},
            "previous": {"AAA": {"held": "receipt"}},
            "pricing_replacements": {"AAA": "replacement"},
            "receipt_cache": self.cache,
            "root": "held-lineage-root",
            "seed": "AAA",
            "sources": {"AAA": {"held": "source"}},
            "store": "held-store",
            "unavailable": {"AUX": {}},
            "work_policy": self.policy,
        }
        return group_engine.CompletedGroupPublication(self.receipts, self.boundaries, True, True, 37, 0)

    def test_completed_publication_carries_exact_owner_result_before_acknowledgement(self):
        publication = self.publication()
        with (
            patch.object(
                group_engine.storage, "publish", side_effect=lambda *args, **kwargs: self.calls.append("publish")
            ) as publish,
            patch.object(group_engine.storage, "read_boundaries", side_effect=AssertionError("accepted reread")),
        ):
            result = group_engine.publish_completed_group(publication, **self.kwargs)
        self.assertEqual(self.calls, ["stamp", "publish", "cache", "index", "acknowledge"])
        self.assertIs(publish.call_args.args[3], self.receipts)
        self.assertIs(publish.call_args.args[4], self.boundaries)
        self.assertIs(publish.call_args.kwargs["checks"], self.kwargs["checks"])
        self.policy.accept_completion.assert_called_once_with(
            self.receipts, self.index["symbols"], self.completion
        )
        self.graph.update.assert_not_called()
        self.assertEqual(
            result,
            {
                "symbol": "AAA",
                "status": "processed",
                "processed": ["AAA"],
                "acknowledged": ["AAA"],
                "unresolved_rows": 2,
                "pricing_acquisition_waves": 1,
                "reused_decision": True,
                "reused_closed_boundaries": True,
                "historical_rows_reused": 37,
                "pricing_rows_processed": 0,
                "unavailable_pricing": ["AUX"],
                "pricing_replacements": {"AAA": "replacement"},
            },
        )

    def test_publication_failure_does_not_consume_completion(self):
        publication = self.publication()
        with patch.object(group_engine.storage, "publish", side_effect=OSError("publication interrupted")):
            with self.assertRaisesRegex(OSError, "publication interrupted"):
                group_engine.publish_completed_group(publication, **self.kwargs)
        self.assertEqual(self.calls, ["stamp"])
        self.cache.published.assert_not_called()
        self.policy.update_index.assert_not_called()
        self.policy.accept_completion.assert_not_called()

    def test_acknowledgement_failure_keeps_publication_and_cache_visible(self):
        publication = self.publication()
        self.policy.accept_completion.side_effect = RuntimeError("acknowledgement interrupted")
        with patch.object(
            group_engine.storage, "publish", side_effect=lambda *args, **kwargs: self.calls.append("publish")
        ):
            with self.assertRaisesRegex(RuntimeError, "acknowledgement interrupted"):
                group_engine.publish_completed_group(publication, **self.kwargs)
        self.assertEqual(self.calls, ["stamp", "publish", "cache", "index"])
        self.cache.published.assert_called_once_with(self.receipts)

    def test_without_work_policy_updates_graph_from_publisher_index(self):
        publication = self.publication()
        self.kwargs["work_policy"] = None
        with patch.object(
            group_engine.storage, "publish", side_effect=lambda *args, **kwargs: self.calls.append("publish")
        ):
            group_engine.publish_completed_group(publication, **self.kwargs)
        self.assertEqual(self.calls, ["publish", "cache", "graph"])
        self.graph.update.assert_called_once_with({"AAA": self.index["symbols"]["AAA"]})

    def test_observer_gets_held_new_boundaries_and_only_missing_selected_old_partition(self):
        publication = self.publication()
        observer = Mock(side_effect=lambda **kwargs: self.calls.append("observe"))
        inputs = {"AAA": object()}
        with (
            patch.object(group_engine.storage, "publish"),
            patch.object(
                group_engine.storage,
                "read_boundaries",
                return_value={"completed_years": {"held": "accepted partition"}},
            ) as read,
        ):
            group_engine.publish_completed_group(publication, inputs=inputs, observer=observer, **self.kwargs)
        read.assert_called_once_with(self.kwargs["root"], self.receipts["AAA"], kinds={"completed_years"})
        self.assertIs(observer.call_args.kwargs["inputs"], inputs)
        self.assertIs(
            observer.call_args.kwargs["boundaries"]["AAA"]["current_month"],
            self.boundaries["AAA"]["current_month"],
        )
        self.assertIsNone(observer.call_args.kwargs["reports"])
        self.assertTrue(observer.call_args.kwargs["resumed"])
        self.assertEqual(self.calls[-1], "observe")

    def test_acknowledgement_retry_never_repeats_stamp_publication_or_index_projection(self):
        publication = self.publication()
        completion = {}
        self.policy.accept_completion.side_effect = RuntimeError("acknowledgement interrupted")
        with patch.object(group_engine.storage, "publish"):
            with self.assertRaisesRegex(RuntimeError, "acknowledgement interrupted"):
                group_engine.publish_completed_group(publication, completion=completion, **self.kwargs)
        self.policy.stamp.side_effect = AssertionError("completed stamp repeated")
        self.cache.published.side_effect = AssertionError("completed cache handoff repeated")
        self.policy.update_index.side_effect = AssertionError("completed index projection repeated")
        self.policy.accept_completion.side_effect = None
        with patch.object(
            group_engine.storage, "publish", side_effect=AssertionError("completed publication repeated")
        ):
            result = group_engine.publish_completed_group(publication, completion=completion, **self.kwargs)
        self.assertEqual(result["status"], "processed")
        self.assertEqual(self.policy.accept_completion.call_count, 2)
        self.assertIs(self.policy.accept_completion.call_args.args[2], self.completion)

    def test_membership_result_lends_computed_trace_without_heavy_input_anchors(self):
        publication = self.publication()
        changes = [{"source": "AAA", "row_start": 1, "row_end": 2}]
        binding = {"pricing_generations": {"AAA": "held-generation"}}
        publication = replace(
            publication,
            membership_refinement={
                "changes": changes,
                "scope_binding": binding,
                "input_evidence": object(),
                "input_receipts": object(),
            },
        )
        with patch.object(group_engine.storage, "publish"):
            result = group_engine.publish_completed_group(publication, **self.kwargs)
        self.assertEqual(set(result["membership_refinement"]), {"changes", "scope_binding"})
        self.assertIs(result["membership_refinement"]["changes"], changes)
        self.assertIs(result["membership_refinement"]["scope_binding"], binding)

    def test_completed_expansion_releases_only_subset_assembly_roots(self):
        own = ("assembly", "store", ("AAA", "BBB"), "current-generation")
        prior = ("assembly", "store", ("AAA",), "superseded-generation")
        failed_peer = ("assembly", "store", ("CCC",), "held-generation")
        other_store = ("assembly", "another-store", ("AAA",), "held-generation")
        pending_publication = ("completed", "store", ("AAA",), "held-generation")
        context = SimpleNamespace(
            group_completions=dict.fromkeys((own, prior, failed_peer, other_store, pending_publication), {})
        )
        group_engine.release_completed_assemblies(context, "store", {"AAA", "BBB"})
        self.assertEqual(set(context.group_completions), {failed_peer, other_store, pending_publication})


class CompleteComponentEvaluationTests(unittest.TestCase):
    def inputs(self):
        self.inputs_by_name = {
            name: SimpleNamespace(source_sha256="prices-" + name, materialization_scope="selected")
            for name in ("AAA", "BBB", "CCC")
        }
        self.evidence = {name: {"identities": [{"id": "asset-" + name}]} for name in self.inputs_by_name}
        self.previous = {name: {"held": name} for name in self.inputs_by_name}
        self.deps = {"AAA": ["BBB"], "BBB": ["AAA"], "CCC": []}
        self.policy = object()

    def test_replacement_components_receive_exact_inputs_once(self):
        self.inputs()
        correlate = Mock(side_effect=lambda store, inputs, evidence, **kwargs: dict.fromkeys(inputs, {}))
        services = SimpleNamespace(correlate=correlate, event=Mock())
        components, reports = group_engine.correlate_components(
            "held-store",
            self.inputs_by_name,
            self.evidence,
            self.previous,
            self.deps,
            {},
            policy=self.policy,
            retain=False,
            services=services,
        )
        self.assertEqual(correlate.call_count, 2)
        self.assertEqual(components, {"AAA": {"AAA", "BBB"}, "BBB": {"AAA", "BBB"}, "CCC": {"CCC"}})
        self.assertEqual(set(reports), set(self.inputs_by_name))
        for call in correlate.call_args_list:
            selected = set(call.args[1])
            self.assertEqual(selected, set(call.args[2]))
            self.assertEqual(selected, set(call.kwargs["selected_group"]))
            self.assertEqual(selected, set(call.kwargs["pricing_generations"]))
            self.assertEqual(selected, set(call.kwargs["evidence_generations"]))
            self.assertIs(call.kwargs["policy"], self.policy)
            for name, view in call.args[1].items():
                self.assertIs(view, self.inputs_by_name[name])
                self.assertIs(call.args[2][name], self.evidence[name])

    def test_accepted_components_never_enter_correlation_or_price_scope_reporting(self):
        self.inputs()
        services = SimpleNamespace(
            correlate=Mock(side_effect=AssertionError("accepted correlation repeated")),
            event=Mock(side_effect=AssertionError("accepted price scope reopened")),
        )
        with patch.object(
            group_engine, "retained_decision_report", side_effect=lambda prior, **kwargs: prior
        ) as held:
            _, reports = group_engine.correlate_components(
                "held-store",
                self.inputs_by_name,
                self.evidence,
                self.previous,
                self.deps,
                {},
                policy=self.policy,
                retain=True,
                services=services,
            )
        self.assertEqual(held.call_count, 3)
        for name in self.previous:
            self.assertIs(reports[name], self.previous[name])

    def test_failed_component_retains_completed_peer_and_group_selection(self):
        self.inputs()
        completion = {}

        def fail_peer(store, inputs, evidence, **kwargs):
            if "CCC" in inputs:
                raise RuntimeError("independent component interrupted")
            return dict.fromkeys(inputs, {"held": "completed report"})

        services = SimpleNamespace(correlate=Mock(side_effect=fail_peer), event=Mock())
        args = ("held-store", self.inputs_by_name, self.evidence, self.previous, self.deps, {})
        with self.assertRaisesRegex(RuntimeError, "independent component interrupted"):
            group_engine.correlate_components(
                *args, policy=self.policy, retain=False, services=services, completion=completion
            )

        def remaining_only(store, inputs, evidence, **kwargs):
            self.assertEqual(set(inputs), {"CCC"}, "completed independent component repeated")
            return dict.fromkeys(inputs, {"new": "completed peer"})

        services.correlate.side_effect = remaining_only
        with patch.object(
            group_engine, "DependencyGraph", side_effect=AssertionError("completed group selection repeated")
        ):
            _, reports = group_engine.correlate_components(
                *args, policy=self.policy, retain=False, services=services, completion=completion
            )
        self.assertEqual(set(reports), set(self.inputs_by_name))
        self.assertEqual(reports["AAA"], {"held": "completed report"})


class OrdinaryGroupRetryTests(unittest.TestCase):
    def prepared(self):
        import test_alpaca_lineage_build as fixture

        case = fixture.LineageBuildTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        rows = fixture.informative_bars("2026-10-01", "2026-10-05")
        assets = {name: fixture.asset(name, "same-asset") for name in ("GOOG", "PEER")}
        for name in assets:
            case.download(symbol=name, bars=rows)
        case.run_build(symbols=list(assets), client=fixture.IdentityClient(assets))
        for name in assets:
            case.download(
                symbol=name,
                end="2026-10-16",
                bars=rows + fixture.informative_bars("2026-10-14", "2026-10-16"),
            )
        client = fixture.IdentityClient(assets)
        evidence = EvidenceClient(client)
        context = OperationContext(case.store)
        return case, client, evidence, context

    def run_group(self, case, client, evidence, context):
        return pipeline.run_lineage(
            case.store,
            ["GOOG"],
            identity_client=client,
            evidence_client=evidence,
            context=context,
        )

    def forbid_completed(self):
        return (
            patch.object(pipeline, "correlate", side_effect=AssertionError("completed correlation repeated")),
            patch.object(
                pipeline, "select_observations", side_effect=AssertionError("completed selection repeated")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("completed prices reopened")),
            patch.object(pipeline, "acquire", side_effect=AssertionError("completed metadata acquired again")),
        )

    def round_trip(self, case, context, evidence):
        from libs.alpaca_pipeline.codec import Decoder, Encoder
        from libs.alpaca_pipeline.state import export_state, import_state

        encoder = Encoder()
        state = export_state(context, None, encoder.encode, evidence=evidence)
        document = json.loads(json.dumps({"state": state, "objects": encoder.objects}))
        decoder = Decoder(document["objects"])
        restored, policy = import_state(case.store, document["state"], decoder.decode, evidence=evidence)
        self.assertIsNone(policy)
        return restored

    def test_publication_retry_consumes_completed_choices_and_all_constructed_parts(self):
        case, client, evidence, context = self.prepared()
        with patch.object(group_engine.storage, "publish", side_effect=OSError("publication interrupted")):
            failed = self.run_group(case, client, evidence, context)
        self.assertEqual(failed["failed"], 1, failed)
        self.assertTrue(any(value.get("numerical") for value in context.group_completions.values()))
        context = self.round_trip(case, context, evidence)
        with ExitStack() as stack:
            for guard in self.forbid_completed():
                stack.enter_context(guard)
            stack.enter_context(
                patch.object(
                    group_engine,
                    "make_publication",
                    side_effect=AssertionError("completed construction repeated"),
                )
            )
            completed = self.run_group(case, client, evidence, context)
        self.assertEqual(completed["failed"], 0, completed)
        self.assertEqual(context.group_completions, {})

    def test_peer_construction_retry_preserves_first_constructed_source(self):
        case, client, evidence, context = self.prepared()
        original = group_engine.make_publication
        completed_sources = []

        def interrupt_second(view, *args, **kwargs):
            if completed_sources:
                raise OSError("second construction interrupted")
            result = original(view, *args, **kwargs)
            completed_sources.append(view.symbol)
            return result

        with patch.object(group_engine, "make_publication", side_effect=interrupt_second):
            failed = self.run_group(case, client, evidence, context)
        self.assertEqual(failed["failed"], 1, failed)
        self.assertEqual(len(completed_sources), 1)

        def remaining_only(view, *args, **kwargs):
            if view.symbol in completed_sources:
                raise AssertionError("completed source construction repeated")
            return original(view, *args, **kwargs)

        with ExitStack() as stack:
            for guard in self.forbid_completed():
                stack.enter_context(guard)
            stack.enter_context(patch.object(group_engine, "make_publication", side_effect=remaining_only))
            completed = self.run_group(case, client, evidence, context)
        self.assertEqual(completed["failed"], 0, completed)
        self.assertEqual(context.group_completions, {})

    def test_returned_publication_survives_cache_delivery_failure_without_republishing(self):
        case, client, evidence, context = self.prepared()
        with patch.object(
            group_engine.storage.ReceiptCache, "published", side_effect=OSError("cache delivery interrupted")
        ):
            failed = self.run_group(case, client, evidence, context)
        self.assertEqual(failed["failed"], 1, failed)
        self.assertTrue(
            any(value.get("effects", {}).get("published") for value in context.group_completions.values())
        )
        context = self.round_trip(case, context, evidence)
        with ExitStack() as stack:
            for guard in self.forbid_completed():
                stack.enter_context(guard)
            stack.enter_context(
                patch.object(
                    group_engine,
                    "make_publication",
                    side_effect=AssertionError("completed construction repeated"),
                )
            )
            stack.enter_context(
                patch.object(
                    group_engine.storage, "publish", side_effect=AssertionError("completed publication repeated")
                )
            )
            completed = self.run_group(case, client, evidence, context)
        self.assertEqual(completed["failed"], 0, completed)
        self.assertEqual(context.group_completions, {})

    def test_changed_pricing_generation_selects_fresh_computation_and_construction(self):
        import test_alpaca_lineage_build as fixture

        case, client, evidence, context = self.prepared()
        with patch.object(group_engine.storage, "publish", side_effect=OSError("publication interrupted")):
            failed = self.run_group(case, client, evidence, context)
        self.assertEqual(failed["failed"], 1, failed)
        rows = (
            fixture.informative_bars("2026-10-01", "2026-10-05")
            + fixture.informative_bars("2026-10-14", "2026-10-16")
            + fixture.informative_bars("2026-10-19", "2026-10-20")
        )
        for name in ("GOOG", "PEER"):
            case.download(symbol=name, end="2026-10-20", bars=rows)
        with (
            patch.object(pipeline, "correlate", wraps=pipeline.correlate) as correlate,
            patch.object(group_engine, "make_publication", wraps=group_engine.make_publication) as construct,
        ):
            completed = self.run_group(case, client, evidence, context)
        self.assertEqual(completed["failed"], 0, completed)
        self.assertGreater(correlate.call_count, 0)
        self.assertEqual(construct.call_count, 2)
        self.assertEqual(context.group_completions, {})


if __name__ == "__main__":
    unittest.main()

"""Prepare's actual JSON boundary, fresh-process consumer and forbidden work."""

import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_pipeline.codec import Decoder, HandoffError
from libs.alpaca_pipeline.prepare import consume_prepared, encode, prepare
from libs.alpaca_pricing_download import check_summary
from libs.alpaca_pricing_download.download import download_pricing

ROOT = Path(__file__).resolve().parents[1]


class PrepareTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.UniverseUpdateTests(
            "test_summary_advancement_publishes_lineage_after_checked_append"
        )
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.records[:] = [asset("AAA")]

    def request(self, *, through="2026-10-17", debug=None):
        f = self.fixture
        return encode(
            {
                "schema": "alpaca.pipeline.request",
                "schema_version": 1,
                "operation": "daily_update",
                "store": str(f.store),
                "registry": str(f.registry),
                "through": through,
                "options": {},
                "debug": debug or {"enabled": False},
            }
        )

    def prepare(self, **kwargs):
        f = self.fixture
        return prepare(self.request(**kwargs), pricing_client=f.pricing, identity_client=f.identity)

    def consume(self, document):
        f = self.fixture
        return json.loads(consume_prepared(document, pricing_client=f.pricing, identity_client=f.identity))

    def test_prepare_has_no_ordinary_acquisition_and_carries_committed_work(self):
        f = self.fixture
        before = set(f.directory.rglob("*"))
        with patch(
            "libs.alpaca_lineage_build.universe_state.revision", side_effect=AssertionError("post-commit reread")
        ):
            document = self.prepare(through="2026-10-19")
        self.assertEqual(f.pages, [])
        self.assertEqual(f.identity.catalogue_calls, 1)
        value = json.loads(document)
        decoder = Decoder(value["objects"])
        selection = decoder.decode(value["prepared"]["selection"])
        stored = universe_state.read_state(f.store)
        for symbol, item in selection["work"].items():
            self.assertEqual(item["observation_id"], stored["work"][symbol]["observation_id"])
            self.assertEqual(item["obligation_id"], stored["work"][symbol]["obligation_id"])
        self.assertEqual(decoder.decode(value["prepared"]["workflow_revision"]), stored["revision"])
        self.assertFalse(
            any("handoff" in p.name or p.suffix == ".json" for p in set(f.directory.rglob("*")) - before)
        )

    def test_consumption_does_not_repeat_preparation_or_accept_summary_again(self):
        document = self.prepare()
        with (
            patch(
                "libs.alpaca_lineage_build.universe_plan.select_work",
                side_effect=AssertionError("selection replay"),
            ),
            patch(
                "libs.alpaca_lineage_build.universe_update.observe_catalogue",
                side_effect=AssertionError("catalogue replay"),
            ),
            patch(
                "libs.alpaca_lineage_build.work_contract.work_graph", side_effect=AssertionError("graph replay")
            ),
            patch.object(
                check_summary, "validate_entry", side_effect=AssertionError("summary acceptance replay")
            ),
        ):
            result = self.consume(document)
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.fixture.identity.catalogue_calls, 1)

    def test_fresh_process_consumes_actual_append_without_preparation(self):
        document = self.prepare(through="2026-10-19")
        script = """
import json, sys
from unittest.mock import patch
sys.path.insert(0, 'tests')
from test_alpaca_universe_update import CatalogueClient
from test_alpaca_pricing_download import bar
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pipeline.prepare import consume_prepared
from libs.alpaca_pricing_download.check_summary import validate_entry
def validate_new(entry):
    assert entry['end_exclusive'][:10] > '2026-10-17', 'old summary replay'
    return validate_entry(entry)
identity = CatalogueClient([])
execute = identity.execute_plan
def new_metadata(plan):
    assert all(action['strategy'] != 'asset_catalogue' for step in plan['steps'] for action in step['actions']), 'catalogue replay'
    return execute(plan)
pricing = BarsClient('fake','fake')
rows = [bar('2016-01-04'),bar('2026-10-16'),bar('2026-10-19')]
def page(params):
    assert params['adjustment'] == 'all' or params['start'] >= '2026-10-17', params
    start,end = timestamp(params['start']),timestamp(params['end'])
    return {'bars': {s:[r for r in rows if start <= timestamp(r['t']) <= end] for s in params['symbols'].split(',')}}
with patch.object(pricing,'_get',side_effect=page), patch.object(identity,'execute_plan',side_effect=new_metadata), patch('libs.alpaca_lineage_build.universe_plan.select_work',side_effect=AssertionError('selection replay')), patch('libs.alpaca_lineage_build.work_contract.work_graph',side_effect=AssertionError('graph replay')), patch('libs.alpaca_pricing_download.check_summary.validate_entry',side_effect=validate_new):
    sys.stdout.buffer.write(consume_prepared(sys.stdin.buffer.read(),pricing_client=pricing,identity_client=identity))
"""
        child = subprocess.run(
            [sys.executable, "-B", "-c", script], input=document, capture_output=True, cwd=ROOT, timeout=30
        )
        self.assertEqual(child.returncode, 0, child.stderr.decode())
        result = json.loads(child.stdout)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["pending_work"], 0)

    def test_changed_runtime_rejects_without_loading_clients_or_replaying_policy(self):
        value = json.loads(self.prepare())
        value["operation"]["runtime"]["lineage"] += 1
        with (
            patch("libs.alpaca_pipeline.prepare.clients", side_effect=AssertionError("stale client creation")),
            patch(
                "libs.alpaca_lineage_build.correlation_policy.load_authorized_policy",
                side_effect=AssertionError("policy replay"),
            ),
            self.assertRaisesRegex(HandoffError, "pipeline implementation changed"),
        ):
            consume_prepared(encode(value))

    def test_changed_workflow_token_stops_before_provider_requests(self):
        document = self.prepare()
        universe_state.register(self.fixture.store, registry=self.fixture.registry)
        with patch.object(self.fixture.pricing, "_get", side_effect=AssertionError("stale acquisition")):
            with self.assertRaisesRegex(HandoffError, "stale_handoff"):
                self.consume(document)

    def test_changed_summary_token_stops_without_reselection(self):
        document = self.prepare()
        check_summary.invalidate(self.fixture.store, ["AAA"], owner="lineage")
        with patch(
            "libs.alpaca_lineage_build.universe_plan.select_work", side_effect=AssertionError("reselection")
        ):
            with self.assertRaises((RuntimeError, HandoffError)):
                self.consume(document)

    def test_pending_recovery_carries_catalogue_and_completed_artifacts(self):
        f = self.fixture
        download_pricing(f.store, "AAA", "2026-10-17", client=FakeClient(f.old))
        check_summary.invalidate(f.store, ["AAA"], owner="lineage")
        document = self.prepare()
        self.assertEqual(f.identity.catalogue_calls, 1)
        with (
            patch(
                "libs.alpaca_lineage_build.universe_plan.select_work",
                side_effect=AssertionError("selection replay"),
            ),
            patch(
                "libs.alpaca_lineage_build.universe_update.run_lineage",
                side_effect=AssertionError("recovery replay"),
            ),
        ):
            result = self.consume(document)
        self.assertTrue(result["complete"], result)
        self.assertEqual(f.identity.catalogue_calls, 1)

    def test_invalid_or_truncated_input_has_no_effects(self):
        with patch("libs.alpaca_pipeline.prepare.clients", side_effect=AssertionError("client creation")):
            for document in (b"", b"{", b"{}", b'{"schema":1,"schema":2}', b'{"value":NaN}'):
                with self.subTest(document=document), self.assertRaises(HandoffError):
                    prepare(document)
            with self.assertRaises(HandoffError):
                consume_prepared(b"{")

    def test_debug_captures_exact_bytes_only_when_enabled(self):
        f = self.fixture
        debug = f.directory / "debug"
        request = self.request(debug={"enabled": True, "directory": str(debug)})
        output = prepare(request, pricing_client=f.pricing, identity_client=f.identity)
        operation_id = json.loads(output)["operation"]["id"]
        self.assertEqual((debug / operation_id / "prepare.input.json").read_bytes(), request)
        self.assertEqual((debug / operation_id / "prepare.output.json").read_bytes(), output)
        self.assertEqual(f.pages, [])
        self.assertTrue(self.consume(output)["complete"])

    def test_public_api_and_cli_use_same_implementation(self):
        import io

        import alpaca_pipeline_prepare
        from alpaca_end_of_day import prepare_pipeline

        output = b'{"prepared":true}'
        with patch("libs.alpaca_pipeline.prepare.prepare", return_value=output) as owner:
            self.assertEqual(prepare_pipeline(b"input"), output)
            owner.assert_called_once_with(b"input")
        with patch.object(alpaca_pipeline_prepare, "prepare_pipeline", return_value=output) as owner:
            stdin, stdout = io.TextIOWrapper(io.BytesIO(b"input")), io.TextIOWrapper(io.BytesIO())
            with patch.object(sys, "stdin", stdin), patch.object(sys, "stdout", stdout):
                self.assertEqual(alpaca_pipeline_prepare.main(), 0)
                self.assertEqual(stdout.buffer.getvalue(), output)
            owner.assert_called_once_with(b"input")

    def test_latest_session_correction_after_json_boundary(self):
        f = self.fixture
        f.live["AAA"] = [f.old[0], bar("2026-10-16", "124")]
        document = self.prepare()
        self.assertEqual(f.pages, [])
        result = self.consume(document)
        self.assertTrue(result["complete"], result)
        from libs.alpaca_pricing_download.download import read_pricing_views

        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][-1][4], "124")
        self.assertEqual(f.identity.catalogue_calls, 1)

    def test_earlier_selected_difference_selects_replacement_after_json(self):
        f = self.fixture
        f.live["AAA"] = [bar("2016-01-04", "124"), f.old[1]]
        result = self.consume(self.prepare())
        self.assertTrue(result["complete"], result)
        from libs.alpaca_pricing_download.download import read_pricing_views

        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][0][4], "124")
        self.assertEqual(f.identity.catalogue_calls, 1)

    def test_failed_selection_is_not_retried_and_independent_group_completes(self):
        f = self.fixture
        f.records[:] = [asset("AAA"), {**asset("AAA", "old-id"), "status": "inactive"}, asset("NEW")]
        with patch.object(f.identity, "get_asset_lookup", side_effect=RuntimeError("selected lookup failed")):
            document = self.prepare()
        with patch.object(
            f.identity, "get_asset_lookup", side_effect=AssertionError("failed observation replay")
        ):
            result = self.consume(document)
        self.assertFalse(result["complete"], result)
        self.assertGreater(result["failed"], 0)
        from libs.alpaca_pricing_download.download import read_pricing_views

        self.assertEqual(len(read_pricing_views(f.store, "NEW")["raw"]), 1)
        self.assertEqual(f.identity.catalogue_calls, 1)

    def test_unfinished_baseline_fails_without_selecting_ordinary_work(self):
        check_summary.invalidate(self.fixture.store, ["AAA"], owner="lineage")
        with (
            patch(
                "libs.alpaca_lineage_build.universe_update.run_lineage",
                return_value={"failed": 1, "results": []},
            ),
            patch(
                "libs.alpaca_lineage_build.universe_plan.select_work",
                side_effect=AssertionError("unusable baseline"),
            ),
            self.assertRaisesRegex(HandoffError, "baseline preparation incomplete"),
        ):
            self.prepare()
        self.assertEqual(self.fixture.pages, [])

    def test_missing_carried_acceptance_fails_before_lock_or_provider(self):
        value = json.loads(self.prepare())
        del value["prepared"]["state"]["context"]["summary"]
        with patch(
            "libs.alpaca_pipeline.execution.operation_lock",
            side_effect=AssertionError("lock before complete input"),
        ):
            with self.assertRaises(RuntimeError):
                self.consume(encode(value))

    def test_null_acceptance_fails_without_reauthentication(self):
        value = json.loads(self.prepare())
        value["prepared"]["state"]["context"]["_accepted_store"] = None
        with (
            patch(
                "libs.alpaca_lineage_build.storage.accept_store",
                side_effect=AssertionError("repeated store acceptance"),
            ),
            patch(
                "libs.alpaca_pipeline.execution.operation_lock",
                side_effect=AssertionError("incomplete input lock"),
            ),
            self.assertRaisesRegex(HandoffError, "missing completed baseline acceptance"),
        ):
            self.consume(encode(value))

    def test_changed_pricing_generation_rejects_old_handoff(self):
        document = self.prepare()
        f = self.fixture
        download_pricing(
            f.store,
            "AAA",
            "2026-10-17",
            client=FakeClient([bar("2016-01-04", "125"), f.old[1]]),
            refresh_all=True,
        )
        with patch.object(f.pricing, "_get", side_effect=AssertionError("stale acquisition")):
            with self.assertRaises((RuntimeError, HandoffError)):
                self.consume(document)

    def test_history_consumer_registers_dependency_discovered_after_preparation(self):
        f = self.fixture
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        f.identity.assets["OLD"] = asset("OLD")
        document = self.prepare(through="2026-10-19")
        event = {
            "action_type": "name_change",
            "id": "new-rename",
            "process_date": "2026-10-19",
            "old_symbol": "OLD",
            "new_symbol": "AAA",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        execute = f.identity.execute_plan

        def with_actions(plan):
            values = execute(plan)
            for step, results in zip(plan["steps"], values):
                for index, action in enumerate(step["actions"]):
                    params = action["request"].get("params", {})
                    if (
                        "symbols" in params
                        and "start" in params
                        and params["start"] <= event["process_date"] <= params["end"]
                    ):
                        results[index] = [event] if {"AAA", "OLD"} & set(params["symbols"].split(",")) else []
            return values

        with patch.object(f.identity, "execute_plan", side_effect=with_actions):
            result = self.consume(document)
        from libs.alpaca_recovery_registry import load_labels

        self.assertIn("OLD", load_labels(f.registry))
        self.assertIn("OLD", universe_state.read_state(f.store)["work"])
        self.assertEqual(f.identity.catalogue_calls, 1)
        self.assertTrue(result["complete"], result)

    def test_public_stage_error_and_debug_failure_are_explicit(self):
        from alpaca_end_of_day import PipelineStageError, prepare_pipeline

        with self.assertRaises(PipelineStageError):
            prepare_pipeline(b"{}")
        f = self.fixture
        debug = f.directory / "debug-failure"
        with patch(
            "libs.alpaca_lineage_build.universe_update.recover_publications",
            side_effect=RuntimeError("interrupted recovery"),
        ):
            with self.assertRaisesRegex(RuntimeError, "interrupted recovery"):
                self.prepare(debug={"enabled": True, "directory": str(debug)})
        captures = list(debug.glob("*/*"))
        self.assertEqual({p.name for p in captures}, {"prepare.input.json", "prepare.error.json"})
        error = json.loads(next(p for p in captures if p.name.endswith("error.json")).read_bytes())
        self.assertEqual(error["stage"], "prepare")
        self.assertEqual(error["message"], "interrupted recovery")


class SourcedRecoveryTransportTests(unittest.TestCase):
    def test_actual_sourced_repair_crosses_json_and_consumes_completed_output_and_rows(self):
        import test_alpaca_documented_scope_pipeline as sourced_fixture

        from libs.alpaca_lineage_build import pipeline, recovery_handoffs, source_work
        from libs.alpaca_pipeline.codec import Encoder
        from libs.alpaca_pipeline.evidence import export_evidence, import_evidence
        from libs.alpaca_pipeline.state import export_recovery, export_state, import_state, restore_recovery

        original = pipeline.run_lineage
        transported = []

        def cross_boundary(*args, **kwargs):
            result = original(*args, **kwargs)
            context, evidence = kwargs["context"], kwargs["evidence_client"]
            caches = context.sourced_operations.get(evidence, {}).get("source_work", {})
            if result["failed"] or not caches.get("completed_source_work"):
                return result
            encoder = Encoder()
            body = {
                "evidence": export_evidence(evidence, encoder.encode),
                "state": export_state(context, None, encoder.encode, evidence=evidence),
                "recovery": export_recovery(context.store, encoder.encode),
            }
            wire = encode({"body": body, "objects": encoder.objects})
            received = json.loads(wire)
            decoder = Decoder(received["objects"])
            restored_evidence = import_evidence(received["body"]["evidence"], evidence.client, decoder.decode)
            restored_context, _ = import_state(
                context.store, received["body"]["state"], decoder.decode, evidence=restored_evidence
            )
            cache = restored_context.sourced_operations[restored_evidence]["source_work"]
            scope, completed = next(
                (key, value) for key, value in cache["completed_source_work"].items() if "outputs" in value
            )
            controls = {control.symbol: control for control in completed["controls"]}
            current = {name: completed["evidence"][token] for name, _store, _source, _receipt, token in scope[1]}
            auxiliary = {name: completed["evidence"][token] for name, token in scope[2]}
            plans = [
                value
                for name in ("documented_plans", "market_plans", "recapitalization_plans", "plans")
                for value in cache.get(name, {}).values()
                if value is not None
            ]
            self.assertTrue(plans, "real producer did not retain its source-work plan")
            with patch.object(
                source_work, "_execute_source_work", side_effect=AssertionError("completed source work repeated")
            ):
                outputs = source_work.execute_source_work(
                    context.store,
                    context.store / "end_of_day/lineage",
                    controls,
                    current,
                    auxiliary,
                    plans[0],
                    restored_context.accept_correlation_policy(),
                    cache,
                    loader=lambda *a, **k: self.fail("completed source prices reread"),
                )
            self.assertIs(outputs, completed["outputs"])
            # Hand only decoded objects to the actual following serving owner.
            selected = recovery_handoffs._selected.get()
            selected[1].clear()
            restore_recovery(context.store, received["body"]["recovery"], decoder.decode)
            transported.append(len(wire))
            return result

        case = sourced_fixture.DocumentedScopePipelineTests(
            "test_equal_bundle_reuses_donor_approval_after_component_extension"
        )
        self.addCleanup(case.doCleanups)
        with patch.object(pipeline, "run_lineage", side_effect=cross_boundary):
            case.exercise(price_client=sourced_fixture.EqualClient, expected_checks=0)
        self.assertEqual(len(transported), 1)
        self.assertGreater(transported[0], 0)

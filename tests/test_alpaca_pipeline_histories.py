"""Independent history production and fresh-process serving consume completed work."""

import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_pipeline.codec import Decoder, HandoffError
from libs.alpaca_pipeline.prepare import encode, prepare
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_recovery_registry import load_labels

ROOT = Path(__file__).resolve().parents[1]


class HistoriesTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.UniverseUpdateTests(
            "test_summary_advancement_publishes_lineage_after_checked_append"
        )
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.records[:] = [asset("AAA")]

    def prepared(self, through="2026-10-19"):
        f = self.fixture
        return prepare(
            encode(
                {
                    "schema": "alpaca.pipeline.request",
                    "schema_version": 1,
                    "operation": "daily_update",
                    "store": str(f.store),
                    "registry": str(f.registry),
                    "through": through,
                    "options": {},
                    "debug": {"enabled": False},
                }
            ),
            pricing_client=f.pricing,
            identity_client=f.identity,
        )

    def histories(self, document):
        from libs.alpaca_pipeline.histories import produce_histories

        f = self.fixture
        with patch(
            "libs.alpaca_serving_catalogue.producer.maintain",
            side_effect=AssertionError("serving entered history production"),
        ):
            return produce_histories(document, pricing_client=f.pricing, identity_client=f.identity)

    @staticmethod
    def unpack(document):
        value = json.loads(document)
        decoder = Decoder(value["objects"])
        return value, decoder, decoder.decode(value["histories"]["result"])

    def serve(self, document):
        from libs.alpaca_pipeline.histories import consume_histories

        return json.loads(consume_histories(document))

    def test_history_finishes_append_without_serving_or_repeating_preparation(self):
        f = self.fixture
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        document = self.prepared()
        before = set(f.directory.rglob("*"))
        with (
            patch(
                "libs.alpaca_lineage_build.universe_plan.select_work",
                side_effect=AssertionError("selection repeated"),
            ),
            patch(
                "libs.alpaca_lineage_build.universe_update.prepare_locked",
                side_effect=AssertionError("preparation repeated"),
            ),
            patch(
                "libs.alpaca_lineage_build.work_contract.work_graph",
                side_effect=AssertionError("accepted graph reconstructed"),
            ),
        ):
            output = self.histories(document)
        value, decoder, result = self.unpack(output)
        self.assertEqual(value["schema"], "alpaca.pipeline.histories")
        self.assertEqual(value["operation"], json.loads(document)["operation"])
        self.assertTrue(result["execution_complete"], result)
        self.assertTrue(decoder.decode(value["histories"]["history_complete"]), result)
        self.assertFalse(result["complete"], "history completion must not report serving completion")
        self.assertNotIn("serving_catalogue", result["stages"])
        self.assertEqual(result["pending_work"], 0)
        self.assertEqual(f.identity.catalogue_calls, 1)
        self.assertEqual(
            [row[0][:10] for row in read_pricing_views(f.store, "AAA")["all"]],
            ["2016-01-04", "2026-10-16", "2026-10-19"],
        )
        added = set(f.directory.rglob("*")) - before
        self.assertFalse(any("handoff" in path.name for path in added))
        self.assertFalse(any(path.name.endswith((".input.json", ".output.json")) for path in added))
        self.assertTrue(self.serve(output)["complete"])

    def test_fresh_process_serving_consumes_carried_receipts_assignments_and_prices(self):
        f = self.fixture
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        output = self.histories(self.prepared())
        script = """
import sys
from unittest.mock import patch
from libs.alpaca_pipeline.histories import consume_histories
def forbidden(*args, **kwargs):
    raise AssertionError('completed upstream work repeated')
with patch('libs.alpaca_pipeline.prepare.clients', side_effect=forbidden), patch('libs.alpaca_lineage_build.universe_update.prepare_locked', side_effect=forbidden), patch('libs.alpaca_lineage_build.universe_update.update_everything', side_effect=forbidden), patch('libs.alpaca_lineage_build.universe_update.run_lineage', side_effect=forbidden), patch('libs.alpaca_lineage_build.storage.selected_document', side_effect=forbidden), patch('alpaca_end_of_day.standalone.read_artifact', side_effect=forbidden):
    sys.stdout.buffer.write(consume_histories(sys.stdin.buffer.read()))
"""
        child = subprocess.run(
            [sys.executable, "-B", "-c", script],
            input=output,
            capture_output=True,
            cwd=ROOT,
            timeout=30,
        )
        self.assertEqual(child.returncode, 0, child.stderr.decode())
        result = json.loads(child.stdout)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["stages"]["serving_catalogue"]["provider_requests"], 0)

    def test_latest_session_correction_preserves_local_provider_scope(self):
        f = self.fixture
        f.live["AAA"] = [f.old[0], bar("2026-10-16", "124")]
        output = self.histories(self.prepared("2026-10-17"))
        value, decoder, result = self.unpack(output)
        self.assertTrue(decoder.decode(value["histories"]["history_complete"]), result)
        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][-1][4], "124")
        for page in f.pages:
            if page["adjustment"] != "all":
                self.assertGreaterEqual(page["start"], "2026-10-16", page)
        self.assertTrue(self.serve(output)["complete"])

    def test_earlier_selected_difference_finishes_replacement_before_handoff(self):
        f = self.fixture
        f.live["AAA"] = [bar("2016-01-04", "124"), f.old[1]]
        output = self.histories(self.prepared("2026-10-17"))
        value, decoder, result = self.unpack(output)
        self.assertTrue(decoder.decode(value["histories"]["history_complete"]), result)
        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][0][4], "124")
        self.assertTrue(self.serve(output)["complete"])

    def test_discovered_dependency_crosses_actual_expanded_storage_scope(self):
        f = self.fixture
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        f.identity.assets["OLD"] = asset("OLD")
        document = self.prepared()
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
            output = self.histories(document)
        value, decoder, result = self.unpack(output)
        self.assertTrue(decoder.decode(value["histories"]["history_complete"]), result)
        self.assertIn("OLD", decoder.decode(value["histories"]["storage_symbols"]))
        self.assertIn("OLD", load_labels(f.registry))
        self.assertEqual(universe_state.read_state(f.store)["work"]["OLD"]["status"], "complete")
        self.assertTrue(self.serve(output)["complete"])
        self.assertEqual(f.identity.catalogue_calls, 1)

    def test_failed_peer_keeps_complete_independent_publication_and_failure(self):
        f = self.fixture
        f.records[:] = [asset("AAA"), {**asset("AAA", "old-id"), "status": "inactive"}, asset("NEW")]
        with patch.object(f.identity, "get_asset_lookup", side_effect=RuntimeError("selected lookup failed")):
            document = self.prepared("2026-10-17")
        with patch.object(
            f.identity, "get_asset_lookup", side_effect=AssertionError("failed observation replay")
        ):
            output = self.histories(document)
        value, decoder, result = self.unpack(output)
        self.assertTrue(result["execution_complete"], result)
        self.assertFalse(decoder.decode(value["histories"]["history_complete"]))
        self.assertGreater(result["failed"], 0)
        self.assertEqual(len(read_pricing_views(f.store, "NEW")["raw"]), 1)
        completed = self.serve(output)
        self.assertFalse(completed["complete"], completed)
        self.assertGreater(completed["failed"], 0)
        self.assertEqual(f.identity.catalogue_calls, 1)

    def test_workflow_change_after_history_stops_serving_without_history_replay(self):
        output = self.histories(self.prepared("2026-10-17"))
        universe_state.register(self.fixture.store, registry=self.fixture.registry)
        with (
            patch(
                "libs.alpaca_serving_catalogue.producer.maintain",
                side_effect=AssertionError("stale input entered serving"),
            ),
            self.assertRaisesRegex((HandoffError, RuntimeError), "stale_handoff"),
        ):
            self.serve(output)

    def test_acquisition_peer_failure_preserves_completed_append_for_serving(self):
        f = self.fixture
        f.records[:] = [asset("AAA"), asset("NEW")]
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        document = self.prepared()
        page = f.page

        def failed_peer(params):
            if "NEW" in params["symbols"].split(","):
                raise RuntimeError("new peer unavailable")
            return page(params)

        with patch.object(f.pricing, "_get", side_effect=failed_peer):
            output = self.histories(document)
        value, decoder, result = self.unpack(output)
        self.assertTrue(result["execution_complete"], result)
        self.assertFalse(decoder.decode(value["histories"]["history_complete"]))
        self.assertGreater(result["failed"], 0)
        self.assertEqual(universe_state.read_state(f.store)["work"]["AAA"]["status"], "complete")
        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][-1][0][:10], "2026-10-19")
        with (
            patch.object(f.pricing, "_get", side_effect=AssertionError("completed append replay")),
            patch(
                "libs.alpaca_lineage_build.universe_update.update_everything",
                side_effect=AssertionError("history maintenance replay"),
            ),
        ):
            completed = self.serve(output)
        self.assertFalse(completed["complete"], completed)
        self.assertGreater(completed["failed"], 0)
        self.assertEqual(completed["stages"]["serving_catalogue"]["failed"], 0)

    def test_missing_or_malformed_history_input_fails_before_store_lock(self):
        output = self.histories(self.prepared("2026-10-17"))
        documents = [b"", b"{", b"{}", b'{"schema":1,"schema":2}']
        for field in ("result", "history_complete", "storage_symbols", "context", "recovery"):
            value = json.loads(output)
            del value["histories"][field]
            documents.append(encode(value))
        value = json.loads(output)
        value["histories"]["recovery"]["values"] = None
        documents.append(encode(value))
        value = json.loads(output)
        value["histories"]["context"]["_accepted_store"] = None
        documents.append(encode(value))
        value = json.loads(output)
        value["histories"]["context"]["_accepted_store"] = "invalid-store-record"
        documents.append(encode(value))
        value = json.loads(output)
        result = value["objects"][value["histories"]["result"]["ref"]]
        result["items"] = [pair for pair in result["items"] if pair[0] != "execution_complete"]
        documents.append(encode(value))
        with patch(
            "libs.alpaca_pipeline.histories.operation_lock", side_effect=AssertionError("incomplete input lock")
        ):
            for document in documents:
                with self.subTest(document=document[:100]), self.assertRaises((HandoffError, RuntimeError)):
                    self.serve(document)

    def test_changed_lineage_selector_rejects_without_rereading_receipts_or_serving(self):
        f = self.fixture
        output = self.histories(self.prepared("2026-10-17"))
        root = f.store / "end_of_day/lineage"
        with storage.publication_lock(root, exclusive=True):
            storage._update_index(root, {})
        with (
            patch.object(storage, "read_receipt", side_effect=AssertionError("stale receipt read")),
            patch(
                "libs.alpaca_serving_catalogue.producer.maintain",
                side_effect=AssertionError("stale serving publication"),
            ),
            self.assertRaises((HandoffError, RuntimeError)),
        ):
            self.serve(output)

    def test_incomplete_result_or_scope_cannot_start_serving_effects(self):
        output = self.histories(self.prepared("2026-10-17"))
        variants = []
        fields = ("stages", "failed", "failures", "pending_work", "requests")
        for name in fields:
            value = json.loads(output)
            result = value["objects"][value["histories"]["result"]["ref"]]
            result["items"] = [pair for pair in result["items"] if pair[0] != name]
            variants.append(("missing " + name, encode(value)))
        for name, invalid in (
            ("stages", None),
            ("failed", True),
            ("failed", -1),
            ("failures", "missing failure list"),
            ("pending_work", True),
            ("pending_work", -1),
            ("requests", None),
        ):
            value = json.loads(output)
            result = value["objects"][value["histories"]["result"]["ref"]]
            for pair in result["items"]:
                if pair[0] == name:
                    pair[1] = invalid
            variants.append(("invalid " + name, encode(value)))
        for invalid in (42, ""):
            value = json.loads(output)
            symbols = value["objects"][value["histories"]["storage_symbols"]["ref"]]
            symbols["items"].append(invalid)
            variants.append(("invalid scope member", encode(value)))
        with (
            patch(
                "libs.alpaca_pipeline.histories.operation_lock",
                side_effect=AssertionError("incomplete result reached store lock"),
            ),
            patch(
                "libs.alpaca_serving_catalogue.producer.maintain",
                side_effect=AssertionError("incomplete result authorized serving effects"),
            ),
        ):
            for label, document in variants:
                with self.subTest(case=label), self.assertRaises((HandoffError, RuntimeError)):
                    self.serve(document)

    def test_changed_serving_selector_rejects_without_reassessment(self):
        from libs.alpaca_serving_catalogue import storage as serving_storage

        output = self.histories(self.prepared("2026-10-17"))
        serving_storage.commit(self.fixture.store, [], [])
        with (
            patch(
                "libs.alpaca_serving_catalogue.producer.maintain",
                side_effect=AssertionError("stale serving reassessment"),
            ),
            self.assertRaisesRegex((HandoffError, RuntimeError), "stale_handoff: serving generation"),
        ):
            self.serve(output)

    def test_pricing_change_after_noop_history_cannot_report_stale_completion(self):
        f = self.fixture
        output = self.histories(self.prepared("2026-10-17"))
        download_pricing(
            f.store,
            "AAA",
            "2026-10-17",
            client=FakeClient([bar("2016-01-04", "125"), f.old[1]]),
            refresh_all=True,
        )
        with (
            patch(
                "libs.alpaca_serving_catalogue.producer.maintain",
                side_effect=AssertionError("stale pricing entered serving"),
            ),
            self.assertRaises((HandoffError, RuntimeError)),
        ):
            self.serve(output)

    def test_unavailable_owner_publication_boundary_cannot_emit_ready_history(self):
        from libs.alpaca_lineage_build import universe_update

        f = self.fixture
        document = self.prepared("2026-10-17")
        original = universe_update.update_everything
        calls = []

        def interrupted_publication(*args, **kwargs):
            result = original(*args, **kwargs)
            calls.append(result)
            if len(calls) == 2:
                (f.store / "end_of_day/lineage/.pending.json").write_text("{}")
            return result

        with (
            patch.object(universe_update, "update_everything", side_effect=interrupted_publication),
            self.assertRaisesRegex((HandoffError, RuntimeError), "lineage publication remains pending"),
        ):
            self.histories(document)
        self.assertEqual(len(calls), 2)


class SourcedHistoryHandoffTests(unittest.TestCase):
    def test_sourced_history_minimal_handoff_serves_without_finished_source_caches(self):
        import test_alpaca_documented_scope_pipeline as fixture

        from libs.alpaca_lineage_build import pipeline, recovery_handoffs, source_work
        from libs.alpaca_pipeline.codec import Encoder
        from libs.alpaca_pipeline.state import (
            export_recovery,
            export_serving_context,
            import_serving_context,
            restore_recovery,
        )

        original_lineage, original_serving = pipeline.run_lineage, fixture.maintain
        completed = {}
        transported = []

        def record_owner(*args, **kwargs):
            result = original_lineage(*args, **kwargs)
            completed["context"] = kwargs["context"]
            return result

        def serve_transported(store, **kwargs):
            if kwargs.get("bootstrap"):
                return original_serving(store, **kwargs)
            context = completed["context"]
            context.select_serving()
            transport = Encoder()
            document = {
                "context": export_serving_context(context, transport.encode),
                "recovery": export_recovery(store, transport.encode),
            }
            wire = encode({"body": document, "objects": transport.objects})
            value = json.loads(wire)
            decoder = Decoder(value["objects"])
            restored = import_serving_context(store, value["body"]["context"], decoder.decode)
            self.assertEqual(restored.sourced_operations, {})
            self.assertEqual(restored.history_checks, {})
            self.assertIsNone(restored._pricing)
            with (
                recovery_handoffs.operation(store),
                patch.object(
                    source_work,
                    "_execute_source_work",
                    side_effect=AssertionError("completed source work repeated"),
                ),
                patch.object(
                    storage,
                    "selected_document",
                    side_effect=AssertionError("carried receipt or assignment reopened"),
                ),
            ):
                restore_recovery(store, value["body"]["recovery"], decoder.decode)
                result = original_serving(store, context=restored, **kwargs)
            transported.append(len(wire))
            return result

        case = fixture.DocumentedScopePipelineTests(
            "test_equal_bundle_reuses_donor_approval_after_component_extension"
        )
        self.addCleanup(case.doCleanups)
        with (
            patch.object(pipeline, "run_lineage", side_effect=record_owner),
            patch.object(fixture, "maintain", side_effect=serve_transported),
        ):
            case.exercise(price_client=fixture.EqualClient, expected_checks=0)
        self.assertEqual(len(transported), 1)
        self.assertGreater(transported[0], 0)


if __name__ == "__main__":
    unittest.main()

"""Public serving boundary, DEBUG equivalence and delivery after publication."""

import io
import json
import select
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar

import alpaca_pipeline_serve
from alpaca_end_of_day import PipelineStageError, serve_pipeline
from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build import universe_update
from libs.alpaca_pipeline.execution import controller
from libs.alpaca_pipeline.histories import load_histories, produce_histories
from libs.alpaca_pipeline.prepare import encode, prepare
from libs.alpaca_pipeline.serve import serve
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue import work
from libs.common.store_lock import operation_lock

ROOT = Path(__file__).resolve().parents[1]


class ServeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = self.new_fixture()

    def new_fixture(self):
        fixture = fixtures.UniverseUpdateTests("test_summary_advancement_publishes_lineage_after_checked_append")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.records[:] = [asset("AAA")]
        fixture.live["AAA"] = [*fixture.old, bar("2026-10-19")]
        return fixture

    @staticmethod
    def histories(f, debug=None):
        document = prepare(
            encode(
                {
                    "schema": "alpaca.pipeline.request",
                    "schema_version": 1,
                    "operation": "daily_update",
                    "store": str(f.store),
                    "registry": str(f.registry),
                    "through": "2026-10-19",
                    "options": {},
                    "debug": debug or {"enabled": False},
                }
            ),
            pricing_client=f.pricing,
            identity_client=f.identity,
        )
        return produce_histories(document, pricing_client=f.pricing, identity_client=f.identity)

    def test_public_api_and_cli_share_bytes_contract_and_stage_errors(self):
        with patch("libs.alpaca_pipeline.serve.serve", return_value=b"result") as owner:
            self.assertEqual(serve_pipeline(b"input"), b"result")
            owner.assert_called_once_with(b"input")
        stdin = io.TextIOWrapper(io.BytesIO(b"input"))
        stdout = io.TextIOWrapper(io.BytesIO())
        self.addCleanup(stdin.close)
        self.addCleanup(stdout.close)
        with (
            patch.object(alpaca_pipeline_serve, "serve_pipeline", return_value=b"result") as owner,
            patch.object(sys, "stdin", stdin),
            patch.object(sys, "stdout", stdout),
        ):
            self.assertEqual(alpaca_pipeline_serve.main(), 0)
            self.assertEqual(stdout.buffer.getvalue(), b"result")
            owner.assert_called_once_with(b"input")
        with self.assertRaises(PipelineStageError):
            serve_pipeline(b"{}")

    def test_public_fresh_process_uses_only_handoff_without_upstream_work(self):
        document = self.histories(self.fixture)
        script = """
import sys
from unittest.mock import patch
from alpaca_end_of_day import serve_pipeline
def forbidden(*args, **kwargs):
    raise AssertionError('serving repeated completed upstream work')
with patch('libs.alpaca_pipeline.prepare.clients', side_effect=forbidden), patch('libs.alpaca_lineage_build.universe_update.prepare_locked', side_effect=forbidden), patch('libs.alpaca_lineage_build.universe_update.histories_locked', side_effect=forbidden), patch('libs.alpaca_lineage_build.storage.selected_document', side_effect=forbidden), patch('alpaca_end_of_day.standalone.read_artifact', side_effect=forbidden):
    sys.stdout.buffer.write(serve_pipeline(sys.stdin.buffer.read()))
"""
        child = subprocess.run(
            [sys.executable, "-B", "-c", script],
            input=document,
            capture_output=True,
            cwd=ROOT,
            timeout=30,
        )
        self.assertEqual(child.returncode, 0, child.stderr.decode())
        result = json.loads(child.stdout)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["stages"]["serving_catalogue"]["provider_requests"], 0)
        self.assertEqual(work.read(self.fixture.store), {})

    def test_standalone_consumer_does_not_refresh_accepted_selectors_again(self):
        document = self.histories(self.fixture)
        original = universe_update.serve_locked

        def consume(*args, **kwargs):
            # The stage has already checked its real lock gap. New serving work
            # consumes those held selections without repeating selector reads.
            with (
                patch.object(lineage_storage, "index_state", side_effect=AssertionError("lineage reread")),
                patch.object(
                    serving_storage, "selector_state", side_effect=AssertionError("serving selector reread")
                ),
            ):
                return original(*args, **kwargs)

        with patch.object(universe_update, "serve_locked", side_effect=consume):
            result = json.loads(serve(document))
        self.assertTrue(result["complete"], result)

    def test_coordinated_serving_consumes_selectors_without_any_gap_reads(self):
        with controller(self.fixture.store):
            document = self.histories(self.fixture)
            with (
                patch.object(lineage_storage, "index_state", side_effect=AssertionError("lineage reread")),
                patch.object(
                    serving_storage, "selector_state", side_effect=AssertionError("serving selector reread")
                ),
            ):
                result = json.loads(serve(document))
        self.assertTrue(result["complete"], result)

    def test_generic_context_without_selected_handoff_retains_current_selection(self):
        from libs.alpaca_serving_catalogue.producer import maintain

        f = self.fixture
        document = self.histories(f)
        _, _, _, context, _, _ = load_histories(document)
        self.assertTrue(json.loads(serve(document))["complete"])
        changed = serving_storage.commit(f.store, [], [])
        with patch.object(serving_storage, "selector_state", wraps=serving_storage.selector_state) as selected:
            result = maintain(f.store, context=context)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["generation"], changed["generation"])
        self.assertGreater(selected.call_count, 0)

    def test_debug_captures_exact_bytes_with_identical_reads_and_processing(self):
        def execute(f, debug):
            document = self.histories(f, debug)
            selected = []
            selector = serving_storage.selector_state

            def record_selector(*args, **kwargs):
                selected.append(kwargs)
                return selector(*args, **kwargs)

            with patch.object(serving_storage, "selector_state", side_effect=record_selector):
                output = serve(document)
            return document, output, json.loads(output), selected

        normal = execute(self.fixture, {"enabled": False})
        other = self.new_fixture()
        directory = other.directory / "debug"
        debug = execute(other, {"enabled": True, "directory": str(directory)})
        self.assertEqual(normal[3], debug[3])
        self.assertEqual(normal[2]["requests"], debug[2]["requests"])
        self.assertEqual(
            {k: v for k, v in normal[2]["stages"]["serving_catalogue"].items() if k != "generation"},
            {k: v for k, v in debug[2]["stages"]["serving_catalogue"].items() if k != "generation"},
        )
        captures = directory / json.loads(debug[0])["operation"]["id"]
        self.assertEqual((captures / "serve.input.json").read_bytes(), debug[0])
        self.assertEqual((captures / "serve.output.json").read_bytes(), debug[1])
        self.assertEqual(
            json.loads((captures / "serve.effects.json").read_bytes())["serving_catalogue"],
            debug[2]["stages"]["serving_catalogue"],
        )
        self.assertFalse(any(self.fixture.directory.rglob("serve.*.json")))

    def test_debug_failure_records_error_and_does_not_deliver_success(self):
        f = self.fixture
        directory = f.directory / "debug"
        document = self.histories(f, {"enabled": True, "directory": str(directory)})
        with (
            patch("libs.alpaca_lineage_build.universe_update.serve_locked", side_effect=RuntimeError("stopped")),
            self.assertRaisesRegex(RuntimeError, "stopped"),
        ):
            serve(document)
        captures = directory / json.loads(document)["operation"]["id"]
        self.assertTrue((captures / "serve.input.json").exists())
        self.assertEqual(json.loads((captures / "serve.error.json").read_bytes())["stage"], "serve")
        self.assertFalse((captures / "serve.output.json").exists())
        with operation_lock(f.store, exclusive=True):
            self.assertTrue(work.read(f.store))

    def test_broken_stdout_reports_delivery_failure_after_committed_catalogue(self):
        f = self.fixture
        document = self.histories(f)

        class BrokenOutput:
            @property
            def buffer(self):
                return self

            def write(self, value):
                raise BrokenPipeError("serving delivery failed")

        stdin = io.TextIOWrapper(io.BytesIO(document))
        self.addCleanup(stdin.close)
        errors = io.StringIO()
        with (
            patch.object(sys, "stdin", stdin),
            patch.object(sys, "stdout", BrokenOutput()),
            patch.object(sys, "stderr", errors),
        ):
            self.assertEqual(alpaca_pipeline_serve.main(), 1)
        self.assertIn("serving delivery failed", errors.getvalue())
        with operation_lock(f.store, exclusive=True):
            self.assertEqual(work.read(f.store), {})
            self.assertTrue(serving_storage.selector_state(f.store)["complete"])

    def test_slow_stdout_does_not_hold_operation_gate(self):
        document = self.histories(self.fixture)
        script = """
import sys
from unittest.mock import patch
import alpaca_pipeline_serve
from libs.alpaca_lineage_build import universe_update
original = universe_update.serve_locked
def produce(*args, **kwargs):
    result = original(*args, **kwargs)
    result['delivery_padding'] = 'x' * 262144
    return result
original_output = sys.stdout
class Output:
    @property
    def buffer(self): return self
    def write(self, value):
        sys.stderr.write('delivering-output\\n'); sys.stderr.flush()
        return original_output.buffer.write(value)
    def flush(self): return original_output.buffer.flush()
with patch.object(universe_update, 'serve_locked', side_effect=produce), patch.object(sys, 'stdout', Output()):
    status = alpaca_pipeline_serve.main()
raise SystemExit(status)
"""
        child = subprocess.Popen(
            [sys.executable, "-B", "-u", "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
        )

        def finish():
            if child.poll() is None:
                child.kill()
            child.wait(timeout=10)
            for stream in (child.stdin, child.stdout, child.stderr):
                if not stream.closed:
                    stream.close()

        self.addCleanup(finish)
        child.stdin.write(document)
        child.stdin.close()
        ready, _, _ = select.select([child.stderr], [], [], 15)
        self.assertTrue(ready, "serving process did not reach output delivery")
        self.assertEqual(child.stderr.readline(), b"delivering-output\n")
        self.assertIsNone(child.poll())
        with operation_lock(self.fixture.store, exclusive=True):
            output = child.stdout.read()
        self.assertEqual(child.wait(timeout=15), 0, child.stderr.read().decode())
        self.assertTrue(json.loads(output)["complete"])
        self.assertGreater(len(output), 262144)


if __name__ == "__main__":
    unittest.main()

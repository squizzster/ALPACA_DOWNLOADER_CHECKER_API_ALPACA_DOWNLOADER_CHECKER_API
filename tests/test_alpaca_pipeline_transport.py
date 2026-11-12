"""CLI transport cannot hold production locks or replay committed preparation."""

import io
import json
import select
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_lineage_build import asset

import alpaca_pipeline_prepare
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_pipeline.prepare import encode, prepare
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.common.store_lock import operation_lock

ROOT = Path(__file__).resolve().parents[1]

# Only clients are replaced. CLI -> public API -> Prepare is the production path.
CHILD = """
import sys
from unittest.mock import patch
sys.path.insert(0, 'tests')
import alpaca_pipeline_prepare
from test_alpaca_universe_update import CatalogueClient
from test_alpaca_lineage_build import asset
from libs.alpaca_pricing_download.http import BarsClient
identity = CatalogueClient([{**asset('AAA'), 'name': 'A' * 262144}])
pricing = BarsClient('fake', 'fake')
original_in, original_out = sys.stdin, sys.stdout
class Input:
    @property
    def buffer(self): return self
    def read(self):
        sys.stderr.write('reading-input\\n'); sys.stderr.flush()
        return original_in.buffer.read()
class Output:
    @property
    def buffer(self): return self
    def write(self, value):
        sys.stderr.write('delivering-output\\n'); sys.stderr.flush()
        return original_out.buffer.write(value)
    def flush(self): return original_out.buffer.flush()
with patch('libs.alpaca_pipeline.prepare.clients', return_value=(pricing, identity)), patch.object(pricing, '_get', side_effect=AssertionError('ordinary pricing during Prepare')), patch.object(sys, 'stdin', Input()), patch.object(sys, 'stdout', Output()):
    status = alpaca_pipeline_prepare.main()
raise SystemExit(status)
"""


class PrepareTransportTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.UniverseUpdateTests(
            "test_summary_advancement_publishes_lineage_after_checked_append"
        )
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.records[:] = [asset("AAA")]

    def request(self, debug=None):
        f = self.fixture
        return encode(
            {
                "schema": "alpaca.pipeline.request",
                "schema_version": 1,
                "operation": "daily_update",
                "store": str(f.store),
                "registry": str(f.registry),
                "through": "2026-10-17",
                "options": {},
                "debug": debug or {"enabled": False},
            }
        )

    def start(self):
        child = subprocess.Popen(
            [sys.executable, "-B", "-u", "-c", CHILD],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=ROOT,
        )
        self.addCleanup(self.finish, child)
        return child

    @staticmethod
    def finish(child):
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream is not None and not stream.closed:
                stream.close()

    def marker(self, child, expected):
        ready, _, _ = select.select([child.stderr], [], [], 10)
        self.assertTrue(ready, "child did not reach " + expected)
        self.assertEqual(child.stderr.readline(), (expected + "\n").encode())

    def test_cli_reads_complete_input_before_store_lock(self):
        child = self.start()
        data = self.request()
        child.stdin.write(data[: len(data) // 2])
        child.stdin.flush()
        self.marker(child, "reading-input")
        # The child is blocked inside read(), and its store gate is still free.
        with operation_lock(self.fixture.store, exclusive=True):
            self.assertIsNone(child.poll())
        child.stdin.write(data[len(data) // 2 :])
        child.stdin.close()
        self.marker(child, "delivering-output")
        result = child.stdout.read()
        self.assertEqual(child.wait(timeout=15), 0, child.stderr.read().decode())
        self.assertEqual(json.loads(result)["schema"], "alpaca.pipeline.prepared")

    def test_large_handoff_slow_reader_does_not_hold_store_gate(self):
        child = self.start()
        child.stdin.write(self.request())
        child.stdin.close()
        self.marker(child, "reading-input")
        self.marker(child, "delivering-output")
        # Output is larger than the pipe, so delivery cannot finish before reads.
        self.assertIsNone(child.poll())
        with operation_lock(self.fixture.store, exclusive=True):
            chunks = []
            while chunk := child.stdout.read(16384):
                chunks.append(chunk)
                time.sleep(0.001)
        result = b"".join(chunks)
        self.assertGreater(len(result), 262144)
        self.assertEqual(child.wait(timeout=15), 0, child.stderr.read().decode())
        self.assertEqual(json.loads(result)["schema"], "alpaca.pipeline.prepared")

    def test_broken_stdout_reports_failure_after_committed_registration_without_replay(self):
        f = self.fixture
        f.records.append(asset("NEW"))
        before = universe_state.read_state(f.store)["revision"]

        class BrokenOutput:
            @property
            def buffer(self):
                return self

            def write(self, value):
                raise BrokenPipeError("delivery failed")

        errors = io.StringIO()
        stdin = io.TextIOWrapper(io.BytesIO(self.request()))
        with (
            patch("libs.alpaca_pipeline.prepare.clients", return_value=(f.pricing, f.identity)),
            patch.object(sys, "stdin", stdin),
            patch.object(sys, "stdout", BrokenOutput()),
            patch.object(sys, "stderr", errors),
        ):
            self.assertEqual(alpaca_pipeline_prepare.main(), 1)
        stdin.close()
        state = universe_state.read_state(f.store)
        self.assertGreater(state["revision"], before)
        self.assertIn("NEW", state["work"])
        self.assertIn("NEW", f.registry.read_text())
        self.assertEqual(f.identity.catalogue_calls, 1)
        self.assertEqual(f.pages, [])
        self.assertIn("delivery failed", errors.getvalue())
        with operation_lock(f.store, exclusive=True):
            pass

    def test_debug_records_same_effects_with_no_additional_storage_or_provider_reads(self):
        f = self.fixture
        normal_reads, debug_reads = [], []

        # Observe exact owner boundary reads without reading any payload for diagnostics.
        def run(debug, calls):
            originals = (storage.read_index, pricing_storage.load_source)
            with (
                patch.object(
                    storage,
                    "read_index",
                    side_effect=lambda *a, **k: (calls.append(("index", str(a[0]))), originals[0](*a, **k))[1],
                ),
                patch.object(
                    pricing_storage,
                    "load_source",
                    side_effect=lambda *a, **k: (calls.append(("source", str(a[0]))), originals[1](*a, **k))[1],
                ),
            ):
                return json.loads(
                    prepare(self.request(debug), pricing_client=f.pricing, identity_client=f.identity)
                )

        normal = run({"enabled": False}, normal_reads)
        debug_dir = f.directory / "debug"
        debug = run({"enabled": True, "directory": str(debug_dir)}, debug_reads)
        self.assertEqual(normal["requests"], debug["requests"])
        self.assertEqual(normal_reads, debug_reads)
        self.assertEqual(f.identity.catalogue_calls, 2)
        self.assertEqual(f.pages, [])
        # Compare actual producer decisions, excluding operation identity and bookkeeping revision.
        from libs.alpaca_pipeline.codec import Decoder

        n, d = Decoder(normal["objects"]), Decoder(debug["objects"])
        for field in ("maintenance_symbols", "storage_symbols", "selection"):
            self.assertEqual(n.decode(normal["prepared"][field]), d.decode(debug["prepared"][field]))
        normal_result = n.decode(normal["prepared"]["result"])
        debug_result = d.decode(debug["prepared"]["result"])
        for result in (normal_result, debug_result):
            result["stages"]["registration"].pop("revision")
            result["plan"].pop("catalogue_observed_at_utc")
        self.assertEqual(normal_result, debug_result)
        captures = list(debug_dir.glob("*/*"))
        self.assertEqual(
            {p.name for p in captures}, {"prepare.input.json", "prepare.output.json", "prepare.effects.json"}
        )


if __name__ == "__main__":
    unittest.main()

"""History CLI delivery, DEBUG parity and failure after committed publication."""

import io
import json
import select
import sqlite3
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar

import alpaca_pipeline_histories
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_pipeline.codec import Decoder, HandoffError
from libs.alpaca_pipeline.histories import consume_histories, produce_histories
from libs.alpaca_pipeline.prepare import encode, prepare
from libs.alpaca_pricing_download import check_summary
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.common.store_lock import operation_lock

ROOT = Path(__file__).resolve().parents[1]
CHILD = """
import sys
from unittest.mock import patch
sys.path.insert(0, 'tests')
import alpaca_pipeline_histories
from test_alpaca_universe_update import CatalogueClient
from test_alpaca_pricing_download import bar
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.contract import timestamp
identity = CatalogueClient([])
pricing = BarsClient('fake', 'fake')
rows = [bar('2016-01-04'), bar('2026-10-16'), bar('2026-10-19')]
def page(params):
    assert params['adjustment'] == 'all' or params['start'] >= '2026-10-17', params
    start, end = timestamp(params['start']), timestamp(params['end'])
    return {'bars': {s: [r for r in rows if start <= timestamp(r['t']) <= end]
                     for s in params['symbols'].split(',')}}
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
with patch('libs.alpaca_pipeline.prepare.clients', return_value=(pricing, identity)), patch.object(pricing, '_get', side_effect=page), patch.object(sys, 'stdin', Input()), patch.object(sys, 'stdout', Output()):
    status = alpaca_pipeline_histories.main()
raise SystemExit(status)
"""


class HistoryTransportTests(unittest.TestCase):
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
    def prepared(f, debug=None):
        return prepare(
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

    def start(self):
        child = subprocess.Popen(
            [sys.executable, "-B", "-u", "-c", CHILD],
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
                if stream is not None and not stream.closed:
                    stream.close()

        self.addCleanup(finish)
        return child

    def marker(self, child, expected):
        ready, _, _ = select.select([child.stderr], [], [], 10)
        self.assertTrue(ready, "child did not reach " + expected)
        self.assertEqual(child.stderr.readline(), (expected + "\n").encode())

    def test_real_cli_waits_for_complete_input_without_holding_gate(self):
        data = self.prepared(self.fixture)
        child = self.start()
        child.stdin.write(data[:256])
        child.stdin.flush()
        self.marker(child, "reading-input")
        with operation_lock(self.fixture.store, exclusive=True):
            self.assertIsNone(child.poll())
        child.stdin.write(data[256:])
        child.stdin.close()
        self.marker(child, "delivering-output")
        output = child.stdout.read()
        self.assertEqual(child.wait(timeout=15), 0, child.stderr.read().decode())
        self.assertEqual(json.loads(output)["schema"], "alpaca.pipeline.histories")
        self.assertTrue(json.loads(consume_histories(output))["complete"])

    def test_large_output_releases_gate_before_slow_consumer(self):
        self.fixture.records[0]["name"] = "A" * 262144
        data = self.prepared(self.fixture)
        child = self.start()
        child.stdin.write(data)
        child.stdin.close()
        self.marker(child, "reading-input")
        self.marker(child, "delivering-output")
        self.assertIsNone(child.poll())
        with operation_lock(self.fixture.store, exclusive=True):
            output = child.stdout.read()
        self.assertGreater(len(output), 262144)
        self.assertEqual(child.wait(timeout=15), 0, child.stderr.read().decode())
        self.assertTrue(json.loads(consume_histories(output))["complete"])

    def test_broken_stdout_keeps_publication_and_reports_failure_without_replay(self):
        f = self.fixture
        data = self.prepared(f)

        class BrokenOutput:
            @property
            def buffer(self):
                return self

            def write(self, value):
                raise BrokenPipeError("history delivery failed")

        stdin = io.TextIOWrapper(io.BytesIO(data))
        self.addCleanup(stdin.close)
        errors = io.StringIO()
        with (
            patch("libs.alpaca_pipeline.prepare.clients", return_value=(f.pricing, f.identity)),
            patch.object(sys, "stdin", stdin),
            patch.object(sys, "stdout", BrokenOutput()),
            patch.object(sys, "stderr", errors),
            patch(
                "libs.alpaca_serving_catalogue.producer.maintain",
                side_effect=AssertionError("dependent stage after delivery failure"),
            ),
        ):
            self.assertEqual(alpaca_pipeline_histories.main(), 1)
        self.assertIn("history delivery failed", errors.getvalue())
        self.assertEqual(universe_state.count_pending(f.store), 0)
        self.assertEqual(f.identity.catalogue_calls, 1)
        with operation_lock(f.store, exclusive=True):
            index = storage.read_index(f.store / "end_of_day/lineage")
            self.assertEqual(index["symbols"]["AAA"]["projection"]["source_rows"], 24)

    def test_api_and_cli_share_actual_bytes_contract_and_public_errors(self):
        from alpaca_end_of_day import PipelineStageError, produce_histories_pipeline

        with patch("libs.alpaca_pipeline.histories.produce_histories", return_value=b"result") as owner:
            self.assertEqual(produce_histories_pipeline(b"input"), b"result")
            owner.assert_called_once_with(b"input")
        stdin = io.TextIOWrapper(io.BytesIO(b"input"))
        stdout = io.TextIOWrapper(io.BytesIO())
        self.addCleanup(stdin.close)
        self.addCleanup(stdout.close)
        with (
            patch.object(
                alpaca_pipeline_histories, "produce_histories_pipeline", return_value=b"result"
            ) as owner,
            patch.object(sys, "stdin", stdin),
            patch.object(sys, "stdout", stdout),
        ):
            self.assertEqual(alpaca_pipeline_histories.main(), 0)
            self.assertEqual(stdout.buffer.getvalue(), b"result")
            owner.assert_called_once_with(b"input")
        with self.assertRaises(PipelineStageError):
            produce_histories_pipeline(b"{}")

    def test_debug_records_exact_bytes_without_additional_reads_requests_or_calculations(self):
        def run(f, debug):
            data = self.prepared(f, debug)
            reads = []
            originals = (storage.read_index, pricing_storage.load_source)
            with (
                patch.object(
                    storage,
                    "read_index",
                    side_effect=lambda *a, **k: (reads.append("index"), originals[0](*a, **k))[1],
                ),
                patch.object(
                    pricing_storage,
                    "load_source",
                    side_effect=lambda *a, **k: (reads.append("source"), originals[1](*a, **k))[1],
                ),
            ):
                output = produce_histories(data, pricing_client=f.pricing, identity_client=f.identity)
            value = json.loads(output)
            decoder = Decoder(value["objects"])
            result = decoder.decode(value["histories"]["result"])
            return data, output, value, result, reads

        normal = run(self.fixture, {"enabled": False})
        other = self.new_fixture()
        directory = other.directory / "debug"
        debug = run(other, {"enabled": True, "directory": str(directory)})
        self.assertEqual(normal[4], debug[4])
        self.assertEqual(self.fixture.pages, other.pages)
        self.assertEqual(normal[3]["requests"], debug[3]["requests"])
        self.assertEqual(normal[3]["failed"], debug[3]["failed"])
        operation_id = debug[2]["operation"]["id"]
        captures = directory / operation_id
        self.assertEqual((captures / "histories.input.json").read_bytes(), debug[0])
        self.assertEqual((captures / "histories.output.json").read_bytes(), debug[1])
        self.assertEqual(
            json.loads((captures / "histories.effects.json").read_bytes())["requests"], debug[3]["requests"]
        )
        self.assertEqual(
            json.loads(consume_histories(normal[1]))["stages"]["serving_catalogue"]["checked_bars"],
            json.loads(consume_histories(debug[1]))["stages"]["serving_catalogue"]["checked_bars"],
        )

    def test_incomplete_history_boundary_emits_debug_error_and_no_result(self):
        f = self.fixture
        directory = f.directory / "debug"
        data = self.prepared(f, {"enabled": True, "directory": str(directory)})
        with (
            patch("libs.alpaca_lineage_build.universe_update.histories_locked", return_value=None),
            self.assertRaisesRegex(HandoffError, "completion boundary unavailable"),
        ):
            produce_histories(data, pricing_client=f.pricing, identity_client=f.identity)
        captures = directory / json.loads(data)["operation"]["id"]
        self.assertTrue((captures / "histories.input.json").exists())
        self.assertTrue((captures / "histories.error.json").exists())
        self.assertFalse((captures / "histories.output.json").exists())

    def test_summary_publisher_carries_revisions_without_boundary_rereads(self):
        f = self.fixture
        snapshot = check_summary.read_snapshot(f.store)
        token = snapshot["token"]
        with check_summary.generation_handoff(f.store, token) as produced:
            with (
                patch.object(check_summary, "_token", side_effect=AssertionError("selector reread")),
                patch.object(check_summary, "_entries", side_effect=AssertionError("summary body reread")),
            ):
                check_summary.invalidate(f.store, ["AAA"])
                check_summary.replace_entries(f.store, {"AAA": snapshot["entries"][0]})
                self.assertEqual(produced["token"], (token[0], token[1] + 2))
        check_summary.assert_generation(f.store, produced["token"])

    def test_summary_transaction_rollback_does_not_publish_a_generation(self):
        f = self.fixture
        token = check_summary.read_snapshot(f.store)["token"]
        advance = check_summary._advance

        def interrupted(db, **kwargs):
            advance(db, **kwargs)
            raise RuntimeError("before summary commit")

        with check_summary.generation_handoff(f.store, token) as produced:
            with (
                patch.object(check_summary, "_advance", side_effect=interrupted),
                self.assertRaisesRegex(RuntimeError, "before summary commit"),
            ):
                check_summary.invalidate(f.store, ["AAA"])
            self.assertEqual(produced["token"], token)
            check_summary.invalidate(f.store, ["AAA"])
            self.assertEqual(produced["token"], (token[0], token[1] + 1))
        # Pending work is allowed for generation applicability, never membership
        # selection. This keeps a failed peer from erasing independent success.
        check_summary.assert_generation(f.store, produced["token"])

    def test_summary_generation_carries_owning_legacy_schema_transition(self):
        f = self.fixture
        with sqlite3.connect(check_summary.summary_path(f.store)) as db:
            db.execute("ALTER TABLE entries RENAME TO current_entries")
            db.execute(
                "CREATE TABLE entries (symbol TEXT PRIMARY KEY, asof TEXT NOT NULL, "
                "end_exclusive TEXT NOT NULL, last_all TEXT NOT NULL, first_all TEXT NOT NULL)"
            )
            db.execute(
                "INSERT INTO entries SELECT symbol, asof, end_exclusive, last_all, first_all FROM current_entries"
            )
            db.execute("DROP TABLE current_entries")
            db.execute("PRAGMA user_version=3")
        token = check_summary.read_snapshot(f.store)["token"]
        with check_summary.generation_handoff(f.store, token) as produced:
            check_summary.invalidate(f.store, ["AAA"])
            self.assertEqual(produced["token"], (token[0], token[1] + 2))
        check_summary.assert_generation(f.store, produced["token"])


if __name__ == "__main__":
    unittest.main()

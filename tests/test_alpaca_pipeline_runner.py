"""Daily execution carries native contracts under one uninterrupted store gate."""

import io
import json
import multiprocessing
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
from contextvars import copy_context
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import test_alpaca_universe_update as fixtures
from test_alpaca_lineage_build import asset
from test_alpaca_pricing_download import bar

from libs.alpaca_lineage_build import universe_state, universe_update
from libs.alpaca_pipeline.codec import HandoffError
from libs.alpaca_pipeline.handoffs import Histories, Prepared
from libs.alpaca_pipeline.prepare import encode
from libs.alpaca_pricing_download.download import read_pricing_views
from libs.common.store_lock import operation_lock

ROOT = Path(__file__).resolve().parents[1]
PROBE = """
import sys
from libs.common.store_lock import StoreBusyError, operation_lock
try:
    with operation_lock(sys.argv[1], exclusive=True):
        pass
except StoreBusyError:
    raise SystemExit(2)
"""


def inherited_ownership(store, send):
    from libs.alpaca_pipeline.execution import controller_owned
    from libs.common.store_lock import exclusive_owned

    try:
        send.send((exclusive_owned(store), controller_owned(store)))
    finally:
        send.close()


class RunnerTests(unittest.TestCase):
    def fixture(self):
        fixture = fixtures.UniverseUpdateTests("test_summary_advancement_publishes_lineage_after_checked_append")
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.records[:] = [asset("AAA")]
        return fixture

    def setUp(self):
        self.f = self.fixture()

    def request(self, *, fixture=None, debug=None):
        f = fixture or self.f
        return encode(
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
        )

    def probe(self, expected):
        child = subprocess.run(
            [sys.executable, "-B", "-c", PROBE, str(self.f.store)],
            cwd=ROOT,
            capture_output=True,
            timeout=10,
        )
        self.assertEqual(child.returncode, expected, child.stderr.decode())

    def run_pipeline(self, document=None, *, fixture=None):
        from libs.alpaca_pipeline.runner import run

        f = fixture or self.f
        return run(
            self.request(fixture=f) if document is None else document,
            pricing_client=f.pricing,
            identity_client=f.identity,
        )

    def test_append_carries_native_values_with_continuous_exclusion_and_narrow_serving_context(self):
        from libs.alpaca_pipeline import runner

        f = self.f
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        documents, contexts = [], []
        prepare_locked = universe_update.prepare_locked
        histories_locked = universe_update.histories_locked
        serve_locked = universe_update.serve_locked

        def prepared(*args, **kwargs):
            result = prepare_locked(*args, **kwargs)
            contexts.append(result["context"])
            return result

        def histories(*args, **kwargs):
            contexts.append(args[2]["context"])
            return histories_locked(*args, **kwargs)

        def serving(*args, **kwargs):
            contexts.append(kwargs["context"])
            with (
                patch(
                    "libs.alpaca_lineage_build.storage.selected_document",
                    side_effect=AssertionError("carried receipt reopened"),
                ),
                patch(
                    "alpaca_end_of_day.standalone.read_artifact",
                    side_effect=AssertionError("carried pricing payload reopened"),
                ),
                patch(
                    "libs.alpaca_serving_catalogue.storage.selector_state",
                    side_effect=AssertionError("accepted serving selector reread"),
                ),
            ):
                return serve_locked(*args, **kwargs)

        def boundary(name, original):
            def wrapped(data, **kwargs):
                self.assertIsInstance(data, {"prepare": dict, "histories": Prepared, "serve": Histories}[name])
                output = original(data, **kwargs)
                self.assertIsInstance(output, {"prepare": Prepared, "histories": Histories, "serve": dict}[name])
                documents.append((name, data, output))
                # A real competing process cannot enter either boundary gap.
                self.probe(2)
                return output

            return wrapped

        with (
            patch.object(runner, "prepare_native", side_effect=boundary("prepare", runner.prepare_native)),
            patch.object(
                runner,
                "produce_histories_native",
                side_effect=boundary("histories", runner.produce_histories_native),
            ),
            patch.object(runner, "serve_native", side_effect=boundary("serve", runner.serve_native)),
            patch.object(universe_update, "prepare_locked", side_effect=prepared),
            patch.object(universe_update, "histories_locked", side_effect=histories),
            patch.object(universe_update, "serve_locked", side_effect=serving),
            patch(
                "libs.alpaca_pipeline.histories.assert_prepared_current",
                side_effect=AssertionError("no lock gap exists to check"),
            ),
            patch(
                "libs.alpaca_pipeline.serve.assert_histories_current",
                side_effect=AssertionError("no lock gap exists to check"),
            ),
        ):
            output = self.run_pipeline()
        self.assertEqual([name for name, _, _ in documents], ["prepare", "histories", "serve"])
        self.assertIs(documents[0][2], documents[1][1])
        self.assertIs(documents[1][2], documents[2][1])
        self.assertIs(contexts[0], contexts[1])
        self.assertIsNot(contexts[1], contexts[2])
        self.assertIsNone(contexts[2]._pricing)
        self.assertEqual(contexts[2].sourced_operations, {})
        self.assertIs(documents[0][2].operation, documents[1][2].operation)
        result = json.loads(output)
        self.assertEqual(json.loads(encode(documents[2][2])), result)
        self.assertTrue(result["complete"], result)
        self.assertEqual(f.identity.catalogue_calls, 1)
        observations = [
            (params["start"], params["end"], params["adjustment"], params["symbols"]) for params in f.pages
        ]
        self.assertEqual(len(observations), len(set(observations)), observations)
        historical = [params for params in f.pages if params["start"] < "2026-10-17"]
        self.assertEqual(len(historical), 2, historical)
        self.assertTrue(all(params["adjustment"] == "all" for params in historical), historical)
        self.assertTrue(
            all(params["start"] >= "2026-10-17" for params in f.pages if params["adjustment"] != "all")
        )
        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][-1][0][:10], "2026-10-19")
        self.assertFalse(any(f.directory.rglob("*.input.json")))
        self.assertFalse(any(f.directory.rglob("*.output.json")))
        self.probe(0)

    def test_ordinary_update_routes_native_stages_without_a_codec_or_json_round_trip(self):
        from libs.alpaca_pipeline import runner

        f = self.f
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        run = runner.run_native
        requests = []

        def observed(data, **kwargs):
            self.assertIs(type(data), dict)
            requests.append(data)
            return run(data, **kwargs)

        with (
            patch.object(runner, "run_native", side_effect=observed),
            patch("libs.alpaca_pipeline.prepare.Encoder", side_effect=AssertionError("bulk encoding")),
            patch("libs.alpaca_pipeline.prepare.Decoder", side_effect=AssertionError("bulk decoding")),
            patch("libs.alpaca_pipeline.histories.Encoder", side_effect=AssertionError("bulk encoding")),
            patch("libs.alpaca_pipeline.histories.Decoder", side_effect=AssertionError("bulk decoding")),
            patch("libs.alpaca_pipeline.runner.encode", side_effect=AssertionError("JSON round trip")),
            patch.object(runner, "prepare_native", wraps=runner.prepare_native) as prepare,
            patch.object(runner, "produce_histories_native", wraps=runner.produce_histories_native) as histories,
            patch.object(runner, "serve_native", wraps=runner.serve_native) as serve,
            patch.object(universe_update, "continue_locked", side_effect=AssertionError("direct object bypass")),
        ):
            result = universe_update.run_update(
                f.store,
                "2026-10-20",
                pricing_client=f.pricing,
                identity_client=f.identity,
                registry=f.registry,
            )
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["end_exclusive"], "2026-10-20")
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["end_exclusive"], "2026-10-20")
        self.assertEqual((prepare.call_count, histories.call_count, serve.call_count), (1, 1, 1))
        self.assertIsInstance(histories.call_args.args[0], Prepared)
        self.assertIsInstance(serve.call_args.args[0], Histories)
        self.assertEqual(f.identity.catalogue_calls, 1)
        self.assertEqual(read_pricing_views(f.store, "AAA")["all"][-1][0][:10], "2026-10-19")
        self.probe(0)

    def test_existing_exclusive_weekend_cutoff_is_carried_without_session_rounding(self):
        from libs.alpaca_pipeline import runner

        histories, serving = runner.produce_histories_native, runner.serve_native
        ends = []

        def observed(original):
            def stage(data, **kwargs):
                ends.append(data.operation["end_exclusive"])
                return original(data, **kwargs)

            return stage

        with (
            patch.object(runner, "produce_histories_native", side_effect=observed(histories)),
            patch.object(runner, "serve_native", side_effect=observed(serving)),
            patch(
                "libs.alpaca_pipeline.prepare.pricing_session_end",
                side_effect=AssertionError("existing exclusive cutoff rounded again"),
            ),
        ):
            result = universe_update.run_update(
                self.f.store,
                "2026-10-18",
                pricing_client=self.f.pricing,
                identity_client=self.f.identity,
                registry=self.f.registry,
            )
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["end_exclusive"], "2026-10-18")
        self.assertEqual(ends, ["2026-10-18", "2026-10-18"])
        self.assertEqual(self.f.identity.catalogue_calls, 1)

    def test_native_only_completed_values_and_aliases_cross_all_owners_without_traversal(self):
        from libs.alpaca_lineage_build import recovery_handoffs

        class CompletedValue:
            def __iter__(self):
                raise AssertionError("completed value traversed")

            def __deepcopy__(self, memo):
                raise AssertionError("completed value copied")

            def __reduce_ex__(self, protocol):
                raise AssertionError("completed value serialized")

        value = CompletedValue()
        prepare_locked, histories_locked, serve_locked = (
            universe_update.prepare_locked,
            universe_update.histories_locked,
            universe_update.serve_locked,
        )
        observed = []

        def prepared(*args, **kwargs):
            result = prepare_locked(*args, **kwargs)
            values = recovery_handoffs._selected.get()[1]
            values["native", "one"] = value
            values["native", "alias"] = value
            return result

        def consume(original):
            def stage(*args, **kwargs):
                values = recovery_handoffs._selected.get()[1]
                self.assertIs(values["native", "one"], value)
                self.assertIs(values["native", "alias"], value)
                observed.append(original.__name__)
                return original(*args, **kwargs)

            return stage

        with (
            patch.object(universe_update, "prepare_locked", side_effect=prepared),
            patch.object(universe_update, "histories_locked", side_effect=consume(histories_locked)),
            patch.object(universe_update, "serve_locked", side_effect=consume(serve_locked)),
        ):
            result = self.run_pipeline()
        self.assertTrue(json.loads(result)["complete"])
        self.assertEqual(observed, ["histories_locked", "serve_locked"])

    def test_controller_reuses_cli_owned_gate_without_another_descriptor(self):
        from libs.alpaca_pipeline import execution

        with operation_lock(self.f.store, exclusive=True):
            with patch.object(
                execution, "operation_lock", side_effect=AssertionError("controller reopened operation gate")
            ):
                output = self.run_pipeline()
            self.probe(2)
        self.assertTrue(json.loads(output)["complete"])
        self.probe(0)

    def test_controller_ownership_is_revoked_in_copies_and_never_lent_to_threads_or_children(self):
        from libs.alpaca_pipeline.execution import controller, controller_owned
        from libs.common.store_lock import exclusive_owned

        with controller(self.f.store):
            copied = copy_context()
            self.assertTrue(exclusive_owned(self.f.store))
            self.assertTrue(controller_owned(self.f.store))
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertFalse(pool.submit(copied.run, exclusive_owned, self.f.store).result(timeout=5))
                self.assertFalse(pool.submit(copied.run, controller_owned, self.f.store).result(timeout=5))
            fork = multiprocessing.get_context("fork")
            receive, send = fork.Pipe(duplex=False)
            child = fork.Process(target=inherited_ownership, args=(self.f.store, send))
            child.start()
            send.close()
            try:
                self.assertTrue(receive.poll(5), "fork child did not report inherited ownership")
                self.assertEqual(receive.recv(), (False, False))
                child.join(5)
                self.assertEqual(child.exitcode, 0)
            finally:
                receive.close()
                if child.is_alive():
                    child.kill()
                    child.join(5)
        self.assertFalse(copied.run(exclusive_owned, self.f.store))
        self.assertFalse(copied.run(controller_owned, self.f.store))
        self.probe(0)

    def test_cli_main_uses_real_pipeline_and_one_gate_and_carries_debug_configuration(self):
        from libs import alpaca_cli
        from libs.alpaca_pipeline import execution, runner

        f = self.f
        f.live["AAA"] = [*f.old, bar("2026-10-19")]
        update = universe_update.run_update
        output, errors = io.StringIO(), io.StringIO()
        debug = f.directory / "update.jsonl"

        def fixture_registry(*args, **kwargs):
            # Only redirect the registry destination; the CLI and all domain
            # stage implementations execute unchanged against this fixture.
            return update(*args, registry=f.registry, **kwargs)

        with (
            patch.object(alpaca_cli, "clients", return_value=(f.pricing, f.identity)),
            patch.object(alpaca_cli, "commit_registry") as bookkeeping,
            patch.object(universe_update, "run_update", side_effect=fixture_registry),
            patch.object(runner, "prepare_native", wraps=runner.prepare_native) as prepare,
            patch.object(runner, "produce_histories_native", wraps=runner.produce_histories_native) as histories,
            patch.object(runner, "serve_native", wraps=runner.serve_native) as serve,
            patch.object(
                execution, "operation_lock", side_effect=AssertionError("CLI gate reacquired by controller")
            ),
            patch(
                "libs.alpaca_pipeline.prepare.pricing_session_end",
                side_effect=AssertionError("CLI cutoff resolved again"),
            ),
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            status = alpaca_cli.main(
                ["update", "--store", str(f.store), "--through", "2026-10-19", "--debug", str(debug)]
            )
        self.assertEqual(status, 0, errors.getvalue())
        result = json.loads(output.getvalue())
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["pricing_cutoff"]["end_exclusive"], "2026-10-20")
        self.assertEqual((prepare.call_count, histories.call_count, serve.call_count), (1, 1, 1))
        bookkeeping.assert_called_once()
        self.assertEqual(f.identity.catalogue_calls, 1)
        self.assertTrue(debug.exists())
        captured = list(Path(str(debug) + ".handoffs").glob("*/serve.output.json"))
        self.assertEqual(len(captured), 1)
        self.assertTrue(json.loads(captured[0].read_bytes())["complete"])
        self.probe(0)

    def test_missing_first_bar_finishes_pipeline_with_warning_without_rechecking_peers(self):
        from libs import alpaca_cli
        from libs.alpaca_pipeline import runner

        f = self.f
        f.live["AAA"] = [f.old[-1], bar("2026-10-19")]
        update = universe_update.run_update
        output, errors = io.StringIO(), io.StringIO()

        def fixture_registry(*args, **kwargs):
            return update(*args, registry=f.registry, **kwargs)

        with (
            patch.object(alpaca_cli, "clients", return_value=(f.pricing, f.identity)),
            patch.object(alpaca_cli, "commit_registry", side_effect=AssertionError("partial registry commit")),
            patch.object(universe_update, "run_update", side_effect=fixture_registry),
            patch.object(runner, "prepare_native", wraps=runner.prepare_native) as prepare,
            patch.object(runner, "produce_histories_native", wraps=runner.produce_histories_native) as histories,
            patch.object(runner, "serve_native", wraps=runner.serve_native) as serve,
            redirect_stdout(output),
            redirect_stderr(errors),
        ):
            code = alpaca_cli.main(["update", "--store", str(f.store), "--through", "2026-10-19"])
        self.assertEqual(code, 0, errors.getvalue())
        result = json.loads(output.getvalue())
        self.assertTrue(result["execution_complete"])
        self.assertFalse(result["complete"], "missing data must remain explicit")
        self.assertEqual(result["outcome"], "completed_with_warnings")
        self.assertEqual(result["pending_work"], 1)
        self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
        self.assertEqual(result["warnings"][0]["missing_endpoints"][0]["endpoint"], "first")
        self.assertEqual((prepare.call_count, histories.call_count, serve.call_count), (1, 1, 1))
        self.assertEqual(f.identity.catalogue_calls, 1)
        records = [json.loads(line) for line in errors.getvalue().splitlines()]
        self.assertTrue(any(r.get("level") == "warning" for r in records))
        self.assertEqual(records[-1]["exit_code"], 0)
        first_requests = [p for p in f.pages if p["start"][:10] == f.old[0]["t"][:10]]
        self.assertEqual(len(first_requests), 1, f.pages)

    def test_failure_and_interruption_stop_dependents_and_release_gate(self):
        from libs.alpaca_pipeline import runner

        for exception in (RuntimeError("histories failed"), KeyboardInterrupt()):
            with (
                self.subTest(exception=type(exception).__name__),
                patch.object(runner, "produce_histories_native", side_effect=exception) as failed,
                patch.object(runner, "serve_native", side_effect=AssertionError("dependent stage executed")),
                self.assertRaises(type(exception)),
            ):
                self.run_pipeline()
            self.assertEqual(failed.call_count, 1, "a failed stage must never be replayed")
            self.probe(0)
        self.assertGreater(universe_state.read_state(self.f.store)["revision"], 0)

    def test_malformed_predecessor_return_has_no_history_or_serving_effects(self):
        from libs.alpaca_pipeline import runner

        with (
            patch.object(runner, "prepare_native", return_value=b"{"),
            patch.object(
                universe_update, "histories_locked", side_effect=AssertionError("malformed history effects")
            ),
            patch.object(runner, "serve_native", side_effect=AssertionError("malformed downstream execution")),
            self.assertRaises((HandoffError, RuntimeError)),
        ):
            self.run_pipeline()
        self.assertEqual(self.f.identity.catalogue_calls, 0)
        self.assertEqual(self.f.pages, [])
        self.probe(0)

    def test_missing_carried_evidence_fails_despite_predecessor_state_still_in_process(self):
        from libs.alpaca_pipeline import runner

        histories = runner.produce_histories_native

        def missing_evidence(data, **kwargs):
            values = dict(data.prepared)
            del values["evidence"]
            return histories(replace(data, prepared=values), **kwargs)

        with (
            patch.object(runner, "produce_histories_native", side_effect=missing_evidence),
            patch.object(
                universe_update, "histories_locked", side_effect=AssertionError("missing evidence recovered")
            ),
            patch.object(runner, "serve_native", side_effect=AssertionError("missing evidence entered serving")),
            self.assertRaises((HandoffError, RuntimeError)),
        ):
            self.run_pipeline()
        self.assertEqual(self.f.identity.catalogue_calls, 1)
        self.assertEqual(self.f.pages, [])
        self.probe(0)

    def test_debug_records_compact_native_boundaries_without_bulk_encoding_or_changed_work(self):
        normal = self.f
        debug = self.fixture()
        for f in (normal, debug):
            f.live["AAA"] = [*f.old, bar("2026-10-19")]
        normal_output = self.run_pipeline(fixture=normal)
        self.assertFalse(any(normal.directory.rglob("*.input.json")))
        self.assertFalse(any(normal.directory.rglob("*.output.json")))
        directory = debug.directory / "debug"
        request = self.request(fixture=debug, debug={"enabled": True, "directory": str(directory)})
        with (
            patch("libs.alpaca_pipeline.prepare.Encoder", side_effect=AssertionError("DEBUG bulk encoding")),
            patch("libs.alpaca_pipeline.histories.Encoder", side_effect=AssertionError("DEBUG bulk encoding")),
            patch("libs.alpaca_pipeline.histories.Decoder", side_effect=AssertionError("DEBUG bulk decoding")),
        ):
            debug_output = self.run_pipeline(request, fixture=debug)
        self.assertTrue(json.loads(normal_output)["complete"])
        self.assertTrue(json.loads(debug_output)["complete"])
        self.assertEqual(normal.pages, debug.pages)
        self.assertEqual(normal.identity.request_count, debug.identity.request_count)
        self.assertEqual(json.loads(normal_output)["requests"], json.loads(debug_output)["requests"])
        self.assertEqual(read_pricing_views(normal.store, "AAA"), read_pricing_views(debug.store, "AAA"))
        captures = list(directory.iterdir())
        self.assertEqual(len(captures), 1)
        capture = captures[0]
        recorded_request = json.loads((capture / "prepare.input.json").read_bytes())
        self.assertEqual(recorded_request["through"], json.loads(request)["through"])
        for stage in ("prepare", "histories"):
            compact = (capture / (stage + ".output.json")).read_bytes()
            self.assertLess(len(compact), 16384)
            self.assertEqual(json.loads(compact)["schema"], "alpaca.pipeline.native")
            self.assertNotIn("objects", json.loads(compact))
        self.assertEqual(
            (capture / "prepare.output.json").read_bytes(), (capture / "histories.input.json").read_bytes()
        )
        self.assertEqual(
            (capture / "histories.output.json").read_bytes(), (capture / "serve.input.json").read_bytes()
        )
        self.assertEqual((capture / "serve.output.json").read_bytes(), debug_output)
        self.assertEqual(len(list(capture.glob("*.input.json"))), 3)
        self.assertEqual(len(list(capture.glob("*.output.json"))), 3)
        self.assertFalse(any(debug.store.rglob("*.input.json")))


if __name__ == "__main__":
    unittest.main()

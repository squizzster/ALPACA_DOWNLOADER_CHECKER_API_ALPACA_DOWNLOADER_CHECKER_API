"""Read-only visibility, real shared-rule replay and forbidden-work regressions."""

import copy
import io
import json
import subprocess
import sys
import tempfile
import unittest
import weakref
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import yaml
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from alpaca_end_of_day import LineageDebugError, debug_lineage, lineage_debug_cli
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.correlation_policy import authorize_production_policy
from libs.alpaca_lineage_build.correlation_policy import load_policy as load_production
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_debug import inspection, paired, research
from libs.alpaca_lineage_debug.policy import load_policy, template
from libs.alpaca_pipeline.codec import Encoder
from libs.alpaca_pricing_download.download import download_pricing

ROOT = Path(__file__).resolve().parents[1]


def request(operation, **kwargs):
    return json.loads(
        debug_lineage(
            json.dumps(
                {"schema": "alpaca.lineage.debug.request", "schema_version": 1, "operation": operation, **kwargs}
            ).encode()
        )
    )


class DebugPolicyTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.policy = self.directory / "candidate.yaml"
        self.policy.write_text(template())

    def write(self, value):
        self.policy.write_text(yaml.safe_dump(value))

    def test_rules_describe_actual_registry_and_numerical_owner_without_store_access(self):
        with patch.object(storage, "read_index", side_effect=AssertionError("no store")):
            report = request("rules")
        self.assertEqual(len(report["ownership_rules"]), 4)
        self.assertEqual(len(report["row_rules"]), 15)
        self.assertFalse(report["numerical_source_selection"]["yaml_selectable"])
        for rule in report["row_rules"] + report["ownership_rules"]:
            code = rule["implementation"]
            self.assertTrue((ROOT / code["file"]).is_file())
            self.assertGreater(code["line"], 0)

    def test_research_rules_are_ordered_subsets_of_actual_functions(self):
        value = yaml.safe_load(self.policy.read_text())
        stage = value["execution"]["established_fixed_point"]
        stage["ownership_rules"] = stage["ownership_rules"][::-1][:2]
        stage["row_rules"] = stage["row_rules"][::-1][:4]
        self.write(value)
        compiled = load_policy(self.policy)
        self.assertEqual([rule.name for rule in compiled.established_ownership_rules], stage["ownership_rules"])
        self.assertEqual([rule.name for rule in compiled.row_rules[:-1]], stage["row_rules"])
        self.assertEqual(compiled.row_rules[-1].name, "unresolved_uuid_candidates")
        with self.assertRaises(RuntimeError):
            authorize_production_policy(compiled)
        with self.assertRaises(RuntimeError):
            load_production(self.policy)

    def test_semantic_identity_ignores_format_but_tracks_reordering(self):
        baseline = load_policy(self.policy)
        self.policy.write_text("# a comment\n" + self.policy.read_text())
        formatted = load_policy(self.policy)
        self.assertEqual(baseline.semantic_sha256, formatted.semantic_sha256)
        self.assertNotEqual(baseline.file_sha256, formatted.file_sha256)
        value = yaml.safe_load(self.policy.read_text())
        value["execution"]["established_fixed_point"]["row_rules"].reverse()
        self.write(value)
        self.assertNotEqual(formatted.semantic_sha256, load_policy(self.policy).semantic_sha256)

    def test_research_input_remains_bounded_and_strictly_typed(self):
        original = yaml.safe_load(template())
        for value in (None, [], {**original, "execution": []}, {**original, "schema_version": 4.0}):
            with self.subTest(value=value):
                self.write(value)
                with self.assertRaises(LineageDebugError):
                    request("rules", policy=str(self.policy))
        self.policy.write_text("#" * 65537)
        with self.assertRaisesRegex(LineageDebugError, "exceeds"):
            request("rules", policy=str(self.policy))

    def test_failed_output_is_an_error_without_retrying_the_diagnostic(self):
        class BrokenOutput(io.StringIO):
            def write(self, value):
                raise BrokenPipeError("deliberate failed delivery")

        from libs.alpaca_lineage_debug import command_line

        with (
            patch.object(command_line, "debug_lineage", wraps=debug_lineage) as calls,
            redirect_stdout(BrokenOutput()),
            redirect_stderr(io.StringIO()),
        ):
            code = lineage_debug_cli(["rules"])
        self.assertEqual(code, 2)
        self.assertEqual(calls.call_count, 1)

    def test_bad_policy_cannot_inject_code_change_defaults_or_weaken_production(self):
        original = yaml.safe_load(self.policy.read_text())
        for change in ("unknown", "duplicate", "default", "phase", "implementation", "production", "extra"):
            with self.subTest(change=change):
                value = copy.deepcopy(original)
                stage = value["execution"]["established_fixed_point"]
                if change == "unknown":
                    stage["row_rules"] = ["os.system"]
                elif change == "duplicate":
                    stage["row_rules"] *= 2
                elif change == "default":
                    stage["row_default"] = "pick_first"
                elif change == "phase":
                    value["execution"]["frozen_baseline"] = False
                elif change == "implementation":
                    value["execution"]["observation_selection"]["implementation"] = "pick_first"
                elif change == "production":
                    value["policy_role"] = "production"
                    stage["row_rules"].reverse()
                else:
                    value["arbitrary"] = True
                self.write(value)
                with self.assertRaises(LineageDebugError):
                    request("rules", policy=str(self.policy))
        self.policy.write_text(template() + "\npolicy_role: production\n")
        with self.assertRaises(LineageDebugError):
            request("rules", policy=str(self.policy))
        self.policy.write_text("!!python/object/apply:os.system ['false']")
        with self.assertRaises(LineageDebugError):
            request("rules", policy=str(self.policy))

    def test_cli_uses_public_json_boundary_and_init_does_not_overwrite(self):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            self.assertEqual(lineage_debug_cli(["rules", "--format", "json"]), 0)
        self.assertEqual(json.loads(out.getvalue()), request("rules"))
        before = self.policy.read_bytes()
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(lineage_debug_cli(["init", "--output", str(self.policy)]), 2)
        self.assertEqual(self.policy.read_bytes(), before)
        process = subprocess.run(
            [sys.executable, "-B", str(ROOT / "alpaca-lineage-debug"), "rules", "--format", "json"],
            cwd="/tmp",
            capture_output=True,
            check=False,
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(json.loads(process.stdout), request("rules"))
        self.assertIn(b"FINAL_EXIT", process.stderr)

    def test_requests_fail_before_work(self):
        for raw in (b"{", b"[]", b'{"schema":"a","schema":"b"}', "not bytes"):
            with self.subTest(raw=raw), self.assertRaises(LineageDebugError):
                debug_lineage(raw)
        for kwargs in (
            {"all": True, "symbols": ["GOOG"]},
            {"symbols": []},
            {"all": True, "max_differences": True},
            {"all": True, "timeout_seconds": float("nan")},
            {"all": True, "unexpected": True},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(LineageDebugError):
                request("run", store="/not/a/store", **kwargs)


class DebugStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.store = self.directory / "store"
        self.policy = self.directory / "candidate.yaml"
        self.policy.write_text(template())
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2026, 10, 15, 12, tzinfo=timezone.utc),
            )
        )
        rows = [bar(day) for day in ("2016-01-04", "2025-12-31", "2026-09-30", "2026-10-01")]
        for symbol in ("AAA", "GOOG"):
            download_pricing(self.store, symbol, "2026-10-14", client=FakeClient(rows))
        result = run_lineage(
            self.store,
            ["AAA", "GOOG"],
            identity_client=IdentityClient({name: asset(name) for name in ("AAA", "GOOG")}),
        )
        self.assertEqual(result["failed"], 0, result)

    def test_explanation_is_read_only_and_forbids_prices_rule_execution_and_requests(self):
        before = snapshot(self.store)
        with (
            patch("libs.alpaca_lineage_build.pricing.load_pricing", side_effect=AssertionError("no prices")),
            patch("libs.alpaca_lineage_build.pipeline.run_lineage", side_effect=AssertionError("no pipeline")),
            patch("libs.alpaca_lineage_build.transform.correlate", side_effect=AssertionError("no replay")),
            patch.object(storage, "read_receipt", wraps=storage.read_receipt) as receipts,
            patch.object(storage, "read_boundaries", wraps=storage.read_boundaries) as boundaries,
        ):
            report = request("explain", store=str(self.store), symbol="GOOG", session="2016-01-04")
        self.assertEqual(receipts.call_count, 1)
        self.assertEqual(boundaries.call_count, 1)
        self.assertEqual(boundaries.call_args.kwargs["kinds"], ("completed_years",))
        self.assertEqual(report["group"], ["GOOG"])
        self.assertEqual(before, snapshot(self.store))
        self.assertIsInstance(report["sources"]["GOOG"]["assignment_handoffs"], dict)
        self.assertIsInstance(report["sources"]["GOOG"]["identity_memberships"], list)
        segment = report["sources"]["GOOG"]["segments"][0]
        self.assertFalse(segment["rule_path"]["predicate_failure_reasons_recorded"])
        self.assertEqual(segment["rule_path"]["basis"], "published_selected_rule")

    def test_actual_all_group_replay_reads_selector_sources_and_payloads_once(self):
        from libs.alpaca_lineage_policy_lab import comparison

        before = snapshot(self.store)
        with (
            patch.object(storage, "read_index", wraps=storage.read_index) as index,
            patch.object(comparison, "load_pricing", wraps=comparison.load_pricing) as prices,
            patch.object(research, "_compare_loaded_group", wraps=research._compare_loaded_group) as evaluate,
            patch.object(storage, "read_receipt", wraps=storage.read_receipt) as receipts,
            patch.object(storage, "read_boundaries", wraps=storage.read_boundaries) as boundaries,
        ):
            report = request("run", store=str(self.store), policy=str(self.policy), all=True)
        self.assertEqual(report["status"], "complete", report)
        self.assertEqual(report["summary"]["evaluated_groups"], 2)
        self.assertEqual(index.call_count, 1)
        self.assertEqual(prices.call_count, 2)
        self.assertEqual(evaluate.call_count, 2)
        self.assertEqual(receipts.call_count, 2)
        self.assertEqual(boundaries.call_count, 2)
        self.assertEqual(before, snapshot(self.store))

    def test_changed_rules_reach_the_real_shared_engine_and_report_differences(self):
        value = yaml.safe_load(self.policy.read_text())
        stage = value["execution"]["established_fixed_point"]
        stage["row_rules"] = []
        stage["ownership_rules"] = []
        self.policy.write_text(yaml.safe_dump(value))
        before = snapshot(self.store)
        report = request("run", store=str(self.store), policy=str(self.policy), symbols=["GOOG", "GOOG"])
        self.assertEqual(report["status"], "complete", report)
        self.assertEqual(report["scope"]["selected_groups"], 1)
        self.assertEqual(report["summary"]["different_groups"], 1)
        self.assertEqual(report["groups"][0]["candidate_policy"]["row_rules"], ["unresolved_uuid_candidates"])
        self.assertEqual(before, snapshot(self.store))

    def test_two_requested_aliases_evaluate_their_complete_group_only_once(self):
        rows = [bar(day) for day in ("2016-01-04", "2025-12-31", "2026-09-30", "2026-10-01")]
        download_pricing(self.store, "OLD", "2026-10-14", client=FakeClient(rows))
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-09-01",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        client = IdentityClient(
            {"GOOG": asset("GOOG"), "OLD": asset("GOOG"), "111111111": asset("GOOG")}, [action]
        )
        built = run_lineage(self.store, ["GOOG", "OLD"], identity_client=client, refresh=True)
        self.assertEqual(built["failed"], 0, built)
        with (
            patch.object(paired, "_compare_loaded_group", wraps=paired._compare_loaded_group) as evaluate,
            patch("libs.alpaca_lineage_debug.api.load_policy", wraps=load_policy) as configuration,
        ):
            report = request("run", store=str(self.store), symbols=["GOOG", "OLD"])
        self.assertEqual(report["scope"]["selected_groups"], 1, report)
        self.assertEqual(evaluate.call_count, 1)
        self.assertEqual(configuration.call_count, 1)
        self.assertEqual(report["groups"][0]["group"], ["GOOG", "OLD"])

    def test_standalone_replay_matches_public_api_and_never_changes_store(self):
        before = snapshot(self.store)
        process = subprocess.run(
            [
                sys.executable,
                "-B",
                str(ROOT / "alpaca-lineage-debug"),
                "run",
                "--store",
                str(self.store),
                "--tickers",
                "GOOG",
                "--format",
                "json",
            ],
            capture_output=True,
            check=False,
        )
        self.assertIn(process.returncode, (0, 1), process.stderr)
        cli = json.loads(process.stdout)
        api = request("run", store=str(self.store), symbols=["GOOG"])
        del cli["elapsed_seconds"], api["elapsed_seconds"]
        self.assertEqual(cli, api)
        self.assertEqual(before, snapshot(self.store))

    def test_cli_difference_and_incomplete_outcomes_are_distinct(self):
        value = yaml.safe_load(self.policy.read_text())
        value["execution"]["established_fixed_point"]["row_rules"] = []
        self.policy.write_text(yaml.safe_dump(value))
        for tickers, expected in ((["GOOG"], 1), (["GOOG", "MISSING"], 2)):
            with redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()):
                code = lineage_debug_cli(
                    [
                        "run",
                        "--store",
                        str(self.store),
                        "--policy",
                        str(self.policy),
                        "--tickers",
                        *tickers,
                        "--format",
                        "json",
                    ]
                )
            self.assertEqual(code, expected)
            self.assertEqual(json.loads(out.getvalue())["summary"]["evaluated_groups"], 1)

    def test_peer_error_preserves_completed_results_without_retry(self):
        real = paired._compare_loaded_group

        def evaluate(store, symbol, *args, **kwargs):
            if symbol == "AAA":
                raise RuntimeError("deliberate peer failure")
            return real(store, symbol, *args, **kwargs)

        with patch.object(paired, "_compare_loaded_group", side_effect=evaluate) as calls:
            report = request("run", store=str(self.store), symbols=["AAA", "GOOG", "MISSING"])
        self.assertEqual(calls.call_count, 2)
        self.assertEqual(report["summary"]["evaluated_groups"], 1)
        self.assertEqual(report["summary"]["failed_groups"], 2)
        self.assertFalse(report["execution_complete"])
        self.assertEqual(report["groups"][0]["group"], ["GOOG"])

    def test_failed_peer_releases_materialized_prices_before_the_next_group(self):
        from contextlib import contextmanager

        real_load, real_evaluate = research._load_group, paired._compare_loaded_group
        prior_prices = []

        @contextmanager
        def load(*args, **kwargs):
            if prior_prices:
                self.assertIsNone(prior_prices[0](), "failed peer's prices survived into the next load")
            with real_load(*args, **kwargs) as selected:
                yield selected

        def evaluate(store, symbol, policy, selected, **kwargs):
            if symbol == "AAA":
                prior_prices.append(weakref.ref(selected["inputs"]["AAA"]))
                raise RuntimeError("deliberate peer failure")
            return real_evaluate(store, symbol, policy, selected, **kwargs)

        with (
            patch.object(research, "_load_group", load),
            patch.object(paired, "_compare_loaded_group", evaluate),
        ):
            report = request("run", store=str(self.store), all=True)
        self.assertEqual(report["summary"]["evaluated_groups"], 1)

    def test_reordering_is_used_by_the_shared_engine_not_just_displayed(self):
        value = yaml.safe_load(self.policy.read_text())
        stage = value["execution"]["established_fixed_point"]
        stage["row_rules"].reverse()
        self.policy.write_text(yaml.safe_dump(value))
        report = request("run", store=str(self.store), policy=str(self.policy), symbols=["GOOG"])
        self.assertTrue(report["execution_complete"])
        self.assertEqual(report["summary"]["different_groups"], 1)
        self.assertEqual(report["groups"][0]["candidate_policy"]["row_rules"][:-1], stage["row_rules"])

    def test_changed_selector_stops_without_reselection_or_replay(self):
        with (
            patch.object(storage, "assert_index_current", side_effect=RuntimeError("selector changed")),
            patch.object(storage, "read_index", wraps=storage.read_index) as reads,
            patch.object(research, "_compare_loaded_group", side_effect=AssertionError("no replay")),
        ):
            with self.assertRaisesRegex(LineageDebugError, "selector changed"):
                request("run", store=str(self.store), all=True)
        self.assertEqual(reads.call_count, 1)

    def test_deadline_has_no_false_completion_or_implicit_retry(self):
        with patch.object(research, "_compare_loaded_group", side_effect=AssertionError("deadline before work")):
            report = request("run", store=str(self.store), all=True, timeout_seconds=0.000000001)
        self.assertEqual(report["status"], "timed_out")
        self.assertEqual(report["summary"]["evaluated_groups"], 0)
        self.assertFalse(report["summary"]["parity"])


class DebugSavedInputsTests(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def test_saved_handoff_selects_metadata_without_decoding_unrelated_price_objects(self):
        transport = Encoder()
        index = transport.encode({"symbols": {"GOOG": {"generation": "held"}, "AAA": {"generation": "other"}}})
        document = {
            "schema": "alpaca.pipeline.histories",
            "schema_version": 1,
            "operation": {"runtime": {"pipeline": -1}},
            "histories": {
                "context": {"index": index},
                "result": transport.encode({"complete": False}),
                "storage_symbols": transport.encode({"GOOG"}),
            },
            "objects": transport.objects + [{"kind": "untrusted_python", "fields": {"ref": -1}}],
        }
        path = self.directory / "saved.json"
        path.write_text(json.dumps(document))
        with (
            patch("libs.alpaca_pipeline.codec.Decoder.decode", side_effect=AssertionError("no restore")),
            patch.object(storage, "read_index", side_effect=AssertionError("no current store")),
        ):
            report = request("handoff", input=str(path), symbols=["GOOG"])
        self.assertEqual(report["selected_index_entries"], {"GOOG": {"generation": "held"}})
        self.assertEqual(report["operation"]["runtime"]["pipeline"], -1)
        document["histories"]["context"]["index"] = {"ref": -1}
        path.write_text(json.dumps(document))
        with self.assertRaises(LineageDebugError):
            request("handoff", input=str(path), symbols=["GOOG"])

    def test_native_capture_reports_its_limits_without_restoring_values(self):
        document = {
            "schema": "alpaca.pipeline.native",
            "schema_version": 1,
            "stage": "histories",
            "operation": {"runtime": {"protocol": 8}},
            "counts": {"storage_symbols": 20000, "recovery_values": 40000, "failed": 0},
            "requests": {"pricing_http_requests": 12},
            "history_complete": True,
        }
        path = self.directory / "native.json"
        path.write_text(json.dumps(document))
        with (
            patch("libs.alpaca_pipeline.codec.Decoder.decode", side_effect=AssertionError("restored data")),
            patch.object(storage, "read_index", side_effect=AssertionError("store accessed")),
        ):
            report = request("handoff", input=str(path), symbols=["GOOG"])
        self.assertEqual(report["native_summary"], document)
        self.assertEqual(report["selected_index_entries"], {})
        self.assertTrue(report["limits"])
        document["schema_version"] = 2
        path.write_text(json.dumps(document))
        with self.assertRaises(LineageDebugError):
            request("handoff", input=str(path))

    def export(self, ranges):
        identities = {}
        for public_id, periods in ranges.items():
            entry = {"csv": public_id + ".csv", "digest": public_id + ".json", "rows": 10}
            identities[public_id] = entry
            (self.directory / entry["digest"]).write_text(
                json.dumps(
                    {
                        "public_id": public_id,
                        "catalogue_generation": "g1",
                        "security": {"ticker_ranges": periods, "names": [public_id + " company"]},
                    }
                )
            )
        manifest = {
            "schema_version": 2,
            "format": "alpaca_public_id_eod_csv",
            "completed_at": "completed",
            "catalogue": {"generation": "g1"},
            "identities": identities,
        }
        path = self.directory / "final_manifest.json"
        path.write_text(json.dumps(manifest))
        return path

    def test_export_preserves_dated_instances_and_exact_inclusive_overlap_without_csv_reads(self):
        def period(first, last, ticker="REUSED"):
            return {"ticker": ticker, "first_session": first, "last_session": last}

        path = self.export(
            {
                "one": [period("2016-01-04", "2020-01-02"), period("2016-01-04", "2020-01-02", "ALIAS")],
                "two": [period("2020-01-02", "2022-01-03")],
                "three": [period("2024-01-02", "2026-01-02")],
            }
        )
        before = snapshot(self.directory)
        report = request("export", manifest=str(path))
        self.assertEqual(before, snapshot(self.directory))
        self.assertEqual(report["summary"]["reported_tickers"], 1)
        ticker = report["tickers"][0]
        self.assertEqual(ticker["instance_count"], 3)
        self.assertEqual(ticker["instances"][0]["names"], ["one company"])
        self.assertEqual(len(ticker["overlaps"]), 1)
        self.assertEqual(ticker["overlaps"][0]["from"], "2020-01-02")
        self.assertEqual(ticker["overlaps"][0]["to"], "2020-01-02")
        # There are deliberately no CSV files: metadata is sufficient.
        self.assertFalse((self.directory / "one.csv").exists())

    def test_export_missing_label_capability_is_reported_without_reconstruction(self):
        path = self.export({"one": []})
        digest = self.directory / "one.json"
        value = json.loads(digest.read_bytes())
        del value["security"]["ticker_ranges"]
        digest.write_text(json.dumps(value))
        report = request("export", manifest=str(path))
        self.assertFalse(report["execution_complete"])
        self.assertEqual(report["missing_dated_label_handoffs"], ["one"])

    def test_export_rejects_changed_generation_and_path_escape(self):
        path = self.export({"one": []})
        value = json.loads((self.directory / "one.json").read_bytes())
        value["catalogue_generation"] = "g2"
        (self.directory / "one.json").write_text(json.dumps(value))
        with self.assertRaisesRegex(LineageDebugError, "selected manifest"):
            request("export", manifest=str(path))
        value = json.loads(path.read_bytes())
        value["identities"]["one"]["digest"] = "../outside.json"
        path.write_text(json.dumps(value))
        with self.assertRaisesRegex(LineageDebugError, "escapes"):
            request("export", manifest=str(path))

    def test_metadata_overlap_does_not_invent_exact_session_or_price_comparison(self):
        segment = {
            "adjustment": "raw",
            "from": "2016-01-04",
            "to": "2016-01-29",
            "observation_selection": "selected",
            "security_identity": {"security_id": "same"},
            "first_timestamp": "2016-01-04T05:00:00Z",
        }
        receipt = {"anchor_month": "2026-10", "row_correlation_rule_order": []}
        boundaries = {
            kind: {
                "adjustments": [
                    {
                        "adjustment": "raw",
                        "segments": [copy.deepcopy(segment)] if kind == "completed_years" else [],
                    }
                ]
            }
            for kind in ("completed_years", "current_year", "current_month")
        }
        from contextlib import contextmanager

        @contextmanager
        def selected(*args, **kwargs):
            yield {
                "entries": {"AAA": {}, "BBB": {}},
                "receipts": {"AAA": receipt, "BBB": receipt},
                "boundaries": {"AAA": boundaries, "BBB": boundaries},
            }

        with patch.object(storage, "reader_snapshot", selected):
            report = inspection.explain("unused", "AAA")
        self.assertEqual(len(report["selected_segment_overlaps"]), 1)
        self.assertTrue(report["selected_segment_overlaps"][0]["same_first_timestamp"])
        self.assertIn("not established", report["selected_segment_overlaps"][0]["meaning"])


if __name__ == "__main__":
    unittest.main()

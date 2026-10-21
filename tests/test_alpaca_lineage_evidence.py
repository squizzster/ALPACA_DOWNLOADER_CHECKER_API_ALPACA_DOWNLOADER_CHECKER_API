import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, calendar_bars
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_cli import build_parser, main
from libs.alpaca_end_of_day_security_correlation.row_correlation import row_correlation_rule_order
from libs.alpaca_end_of_day_security_correlation.terminal_fallback import TERMINAL_FALLBACK_RULE_NAME
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.evidence import output_evidence, rule_path, validate_evidence_version
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import initialize_store
from libs.common.us_equity_market_session_calendar import MarketCalendarError


class LineageEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2026, 10, 15, 12, tzinfo=timezone.utc),
            )
        )
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.store = Path(temporary) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.bars = [bar(day) for day in ("2016-01-04", "2025-12-31", "2026-09-30", "2026-10-01")]

    def download(self, rows=None, end="2026-10-14"):
        download_pricing(self.store, "GOOG", end, client=FakeClient(self.bars if rows is None else rows))

    def publish(self):
        self.download()
        result = run_lineage(self.store, ["GOOG"], identity_client=IdentityClient())
        self.assertEqual(result["failed"], 0, result)

    def output(self, **kwargs):
        result = output_evidence(self.store, "GOOG", **kwargs)
        self.assertEqual(result["workflow"]["failed"], 0, result["workflow"])
        return result["sources"]["GOOG"]

    def test_actual_calculation_is_captured_without_second_resolver(self):
        self.download()
        before = snapshot(self.store / "end_of_day/pricing")
        captured = {}

        def real_calculation(*args, **kwargs):
            reports = correlate(*args, **kwargs)
            captured.update(reports)
            return reports

        with patch("libs.alpaca_lineage_build.pipeline.correlate", side_effect=real_calculation) as calculate:
            result = self.output(identity_client=IdentityClient())
        self.assertEqual(calculate.call_count, 1)
        self.assertEqual(result["status"], "calculated")
        self.assertEqual(result["actual_calculation_scope"], "full_history")
        self.assertEqual(
            result["actual_report"], {k: v for k, v in captured["GOOG"].items() if k != "lineage_report"}
        )
        self.assertEqual(result["receipt"]["evidence"], captured["GOOG"]["lineage_report"]["lineage"])
        self.assertEqual(before, snapshot(self.store / "end_of_day/pricing"))
        self.assertFalse((self.store / "end_of_day/manifests/GOOG/update.json").exists())
        self.assertTrue(result["acknowledged_pricing_notice"])
        self.assertEqual(len(result["pricing"]["session_grids"]), 1)
        self.assertEqual(set(result["pricing"]["adjustment_session_grids"].values()), {0})
        self.assertEqual(sum(s["row_count"] for s in result["assignments"]), len(self.bars) * 8)
        self.assertTrue(all(s["decision_origin"] == "calculated" for s in result["assignments"]))
        selected = result["assignments"][0]["rule_path"]
        self.assertTrue(any(step["outcome"] == "fell_through" for step in selected["steps"]))
        self.assertEqual(sum(step["outcome"] == "selected" for step in selected["steps"]), 1)
        json.dumps(result)  # The whole diagnostic, not just its envelope, is JSON.

    def test_unchanged_reuses_publication_without_calculation_or_requests(self):
        self.publish()
        before = snapshot(self.store)
        client = IdentityClient()
        with patch("libs.alpaca_lineage_build.pipeline.correlate", side_effect=AssertionError("no replay")):
            result = self.output(identity_client=client)
        self.assertEqual(result["status"], "reused_publication")
        self.assertIsNone(result["actual_report"])
        self.assertEqual(client.request_count, 0)
        self.assertEqual(before, snapshot(self.store))
        self.assertTrue(all(s["rule_path"]["basis"] == "published_selected_rule" for s in result["assignments"]))

    def test_offline_keeps_notice_and_reports_changed_pricing(self):
        self.publish()
        self.download(self.bars + [bar("2026-10-14")], "2026-10-15")
        before = snapshot(self.store)
        with patch("libs.alpaca_lineage_build.evidence.run_lineage", side_effect=AssertionError("offline")):
            result = self.output(offline=True)
        self.assertEqual(before, snapshot(self.store))
        self.assertEqual(result["freshness"]["changed_pricing_inputs"], ["GOOG"])
        self.assertEqual(result["freshness"]["pending_pricing_inputs"], ["GOOG"])
        self.assertIsNotNone(result["pending_pricing_notice"])
        self.assertIsNone(result["acknowledged_pricing_notice"])
        self.assertEqual(result["pricing"]["session_grids"][0]["rows"], len(self.bars) + 1)
        self.assertEqual(sum(s["row_count"] for s in result["assignments"]), len(self.bars) * 8)

    def test_offline_reports_old_version_without_applying_new_rules(self):
        self.publish()
        receipt = storage.read_receipt(self.root, "GOOG", storage.read_index(self.root))
        installed = {**receipt["versions"], "correlation": receipt["versions"]["correlation"] + 1}
        before = snapshot(self.store)
        with patch("libs.alpaca_lineage_build.evidence.versions", return_value=installed):
            result = self.output(offline=True)
        self.assertFalse(result["freshness"]["matches_installed_versions"])
        self.assertEqual(result["receipt"]["versions"], receipt["versions"])
        self.assertEqual(before, snapshot(self.store))

    def test_resume_identifies_reused_closed_decisions_and_current_calculation(self):
        self.bars = calendar_bars("2025-12-31", "2026-10-14")
        self.publish()
        self.download(self.bars + [bar("2026-10-14")], "2026-10-15")
        with patch("libs.alpaca_lineage_build.pipeline.correlate", wraps=correlate) as calculate:
            result = self.output(identity_client=IdentityClient())
        self.assertEqual(calculate.call_count, 1)
        self.assertEqual(result["status"], "calculated_with_closed_reuse")
        self.assertEqual(result["actual_calculation_scope"], "added_sessions")
        self.assertEqual(result["actual_report"]["source_row_count"], 8)
        self.assertEqual(result["pricing"]["session_grids"][0]["rows"], len(self.bars) + 1)
        for segment in result["assignments"]:
            expected = "calculated" if segment["from"] >= "2026-10-14" else "reused_publication"
            self.assertEqual(segment["decision_origin"], expected)

    def test_empty_advance_reports_retained_decisions_without_an_actual_calculation(self):
        self.publish()
        self.download(self.bars, "2026-10-15")
        with patch("libs.alpaca_lineage_build.pipeline.correlate", side_effect=AssertionError("no replay")):
            result = self.output(identity_client=IdentityClient())
        self.assertEqual(result["status"], "reused_publication")
        self.assertIsNone(result["actual_calculation_scope"])
        self.assertIsNone(result["actual_report"])
        self.assertEqual(result["calculation_paths"], [])
        self.assertTrue(all(s["decision_origin"] == "reused_publication" for s in result["assignments"]))

    def test_cli_runs_actual_missing_seed_and_dependency_downloads(self):
        from cli_support import initialize_command_registration

        initialize_command_registration(self)
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-09-01",
            "old_symbol": "OLD",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        identity = IdentityClient(
            {"GOOG": asset("GOOG"), "OLD": asset("GOOG"), "111111111": asset("GOOG")}, [action]
        )
        pricing = FakeClient(self.bars)
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            patch.dict("os.environ", {"ALPACA_API_KEY_ID": "fake", "ALPACA_API_SECRET_KEY": "fake"}),
            patch("libs.alpaca_cli.AlpacaIdentityClient", return_value=identity),
            patch("libs.alpaca_cli.BarsClient", return_value=pricing),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            code = main(["lineage", "refresh", "--symbol", "GOOG", "--store", str(self.store)])
            stdout.seek(0)
            stdout.truncate(0)
            self.assertEqual(main(["lineage", "inspect", "--symbol", "GOOG", "--store", str(self.store)]), 0)
        self.assertEqual(code, 0, stderr.getvalue())
        result = json.loads(stdout.getvalue())
        self.assertEqual(set(result["sources"]), {"GOOG", "OLD"})
        self.assertEqual({call[0] for call in pricing.calls}, {"GOOG", "OLD"})
        self.assertGreater(identity.request_count, 0)
        for symbol in result["sources"]:
            self.assertFalse((self.store / f"end_of_day/manifests/{symbol}/update.json").exists())
            self.assertEqual(result["mode"], "offline")

    def test_offline_cli_needs_no_credentials_or_clients(self):
        self.publish()
        before = snapshot(self.store)
        stdout = io.StringIO()
        with (
            patch.dict("os.environ", {}, clear=True),
            redirect_stdout(stdout),
            patch(
                "libs.alpaca_cli.AlpacaIdentityClient",
                side_effect=AssertionError("no client"),
            ),
            patch(
                "libs.alpaca_cli.BarsClient",
                side_effect=AssertionError("no client"),
            ),
        ):
            self.assertEqual(main(["lineage", "inspect", "--symbol", "GOOG", "--store", str(self.store)]), 0)
        self.assertEqual(json.loads(stdout.getvalue())["mode"], "offline")
        self.assertEqual(before, snapshot(self.store))

    def test_failed_workflow_does_not_invent_a_calculation(self):
        initialize_store(self.store)
        client = IdentityClient()
        result = output_evidence(self.store, "GOOG", identity_client=client)
        self.assertEqual(result["workflow"]["failed"], 1)
        self.assertEqual(result["workflow"]["results"][0]["status"], "waiting_for_pricing")
        self.assertEqual(result["sources"], {})
        self.assertEqual(client.request_count, 0)

    def test_failed_publication_is_not_presented_as_a_published_decision(self):
        self.download()
        with patch(
            "libs.alpaca_lineage_build.pipeline.storage.publish", side_effect=RuntimeError("interrupted")
        ):
            result = output_evidence(self.store, "GOOG", identity_client=IdentityClient())
        self.assertEqual(result["workflow"]["failed"], 1)
        self.assertEqual(result["sources"], {})
        self.assertTrue((self.store / "end_of_day/manifests/GOOG/update.json").exists())

    def test_refresh_uses_existing_full_metadata_workflow(self):
        self.publish()
        client = IdentityClient()
        with patch("libs.alpaca_lineage_build.pipeline.correlate", wraps=correlate) as calculate:
            result = self.output(identity_client=client, refresh=True)
        self.assertGreater(client.request_count, 0)
        self.assertEqual(calculate.call_count, 1)
        self.assertEqual(result["status"], "calculated")
        self.assertEqual(result["actual_calculation_scope"], "full_history")

    def test_offline_rejects_incomplete_publication_without_recovery(self):
        self.publish()
        for directory in (self.root, self.store / "end_of_day/pricing/GOOG"):
            with self.subTest(directory=directory):
                journal = directory / ".pending.json"
                # Synthetic interrupted publication, never a production fixture.
                storage.atomic_json(journal, {"unfinished": True})
                before = snapshot(self.store)
                with self.assertRaisesRegex(RuntimeError, "publication incomplete"):
                    self.output(offline=True)
                self.assertEqual(before, snapshot(self.store))
                journal.unlink()

    def test_invalid_label_and_conflicting_options_create_nothing(self):
        with self.assertRaisesRegex(RuntimeError, "invalid ticker"):
            output_evidence(self.store, "../bad", offline=True)
        with self.assertRaisesRegex(ValueError, "cannot refresh"):
            output_evidence(self.store, "GOOG", offline=True, refresh=True)
        self.assertFalse(self.store.exists())
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            build_parser().parse_args(["GOOG", "--offline", "--refresh-metadata"])

    def test_terminal_trace_preserves_recorded_order_but_uses_registry_position(self):
        # Older publications recorded an obsolete terminal order number. Display
        # it honestly without confusing it with today's row rule at that position.
        selection = {"name": TERMINAL_FALLBACK_RULE_NAME, "order": 8}
        path = rule_path({"correlation_rule": selection}, row_correlation_rule_order(), fresh=False)
        self.assertEqual(path["recorded_selection"], selection)
        self.assertEqual(path["steps"][-1]["order"], 15)
        self.assertEqual(path["steps"][-1]["outcome"], "selected")
        self.assertEqual(path["steps"][-2]["outcome"], "selected_before_terminal_promotion")
        self.assertTrue(all(step["outcome"] == "fell_through" for step in path["steps"][:-2]))

    def test_evidence_contract_version_is_runtime_validated(self):
        validate_evidence_version()
        with patch("libs.alpaca_lineage_build.evidence.EVIDENCE_VERSION", 99):
            with self.assertRaisesRegex(RuntimeError, "changelog/version mismatch"):
                validate_evidence_version()

    def test_diagnostic_calendar_error_does_not_fail_successful_publication(self):
        self.download()
        with patch(
            "libs.alpaca_lineage_build.evidence.compare_observed_to_expected_sessions",
            side_effect=MarketCalendarError("outside coverage"),
        ):
            result = self.output(identity_client=IdentityClient())
        self.assertEqual(result["status"], "calculated")
        self.assertEqual(
            result["pricing"]["session_grids"][0]["calendar_between_observed_endpoints"],
            {"error": "outside coverage"},
        )


if __name__ == "__main__":
    unittest.main()

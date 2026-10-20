import copy
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlite_crash_fixture import leave_hot_journal
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_pricing_download import snapshot as files_snapshot

from libs.alpaca_lineage_build import check_summary as projection
from libs.alpaca_lineage_build import contract as lineage_contract
from libs.alpaca_lineage_build import pipeline, update
from libs.alpaca_lineage_build import storage as lineage_storage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import check_summary, storage
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.update_checks import CHECK_FILE, queue_check, read_checks


class UpdateEverythingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        clock = patch(
            "libs.alpaca_pricing_download.download.utc_now",
            return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
        )
        clock.start()
        cls.addClassCleanup(clock.stop)
        temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(temporary.cleanup)
        cls.base = Path(temporary.name) / "base"
        cls.assets = {symbol: asset(symbol) for symbol in ("AAA", "BBB", "ZZZ")}
        cls.assets["ZZZ"]["status"] = "inactive"
        cls.old = [bar("2016-01-04"), bar("2026-10-14")]
        for symbol in cls.assets:
            download_pricing(cls.base, symbol, "2026-10-15", client=FakeClient(cls.old))
        result = run_lineage(cls.base, identity_client=IdentityClient(cls.assets))
        if result["failed"]:
            raise AssertionError(result)
        projection.rebuild(cls.base)
        if len(check_summary.read_snapshot(cls.base)["entries"]) != 2:
            raise AssertionError("fixture must exclude the inactive source")

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        shutil.copytree(self.base, self.store)
        self.live = {
            symbol: copy.deepcopy(self.old) + [bar("2026-10-15"), bar("2026-10-16")] for symbol in self.assets
        }
        self.calls = []
        self.client = BarsClient("test", "test")
        self.identity = IdentityClient(self.assets)
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def page(self, params):
        self.calls.append(dict(params))
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                symbol: [row for row in self.live[symbol] if start <= timestamp(row["t"]) <= end]
                for symbol in params["symbols"].split(",")
            }
        }

    def run_update(self, end="2026-10-17", **kwargs):
        return update.update_everything(
            self.store, end, pricing_client=self.client, identity_client=self.identity, **kwargs
        )

    def assert_finished(self):
        ready = check_summary.read_snapshot(self.store)
        self.assertEqual({entry["symbol"] for entry in ready["entries"]}, {"AAA", "BBB"})
        for entry in ready["entries"]:
            self.assertEqual(entry["asof"], "2026-10-17")
            self.assertEqual(entry["last_all"][0][:10], "2026-10-16")
            self.assertFalse((self.store / f"end_of_day/manifests/{entry['symbol']}/update.json").exists())

    def test_complete_update_and_repeat_trust_boundaries_and_skip_inactive_sources(self):
        inactive = files_snapshot(self.store / "end_of_day/pricing/ZZZ")
        result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["stages"]["refresh"]["check"]["stats"]["matched"], 2)
        self.assertEqual(result["stages"]["append"]["requests"]["page_requests"], 8)
        self.assertEqual(len(self.calls), 9)
        self.assertTrue(all(call["symbols"] == "AAA,BBB" for call in self.calls))
        self.assertTrue(all(call["start"][:10] == "2026-10-15" for call in self.calls[1:]))
        self.assertEqual(files_snapshot(self.store / "end_of_day/pricing/ZZZ"), inactive)
        self.assert_finished()
        self.calls.clear()
        before = files_snapshot(self.store)
        identity_calls = self.identity.request_count
        with (
            patch.object(Path, "iterdir", side_effect=AssertionError("directory scan")),
            patch.object(storage, "load_source", side_effect=AssertionError("unnecessary source read")),
        ):
            repeated = self.run_update()
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(len(self.calls), 1, "integrated update must always recheck live")
        self.assertEqual(self.calls[0]["start"][:10], "2026-10-16")
        self.assertEqual(self.identity.request_count, identity_calls)
        self.assertEqual(files_snapshot(self.store), before)

    def test_recovery_progress_needs_no_lineage_boundaries_for_unchanged_sources(self):
        for symbol in self.assets:
            queue_check(self.store, symbol)
        events = []
        with patch.object(lineage_storage, "read_boundaries", side_effect=AssertionError("progress read")):
            result = update.finish_pending(
                self.store,
                pricing_client=self.client,
                identity_client=self.identity,
                progress=lambda stage, counts: events.append((stage, counts)),
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertIn(("recovery", {"processed": len(self.assets)}), events)
        self.assertTrue(
            any(stage == "lineage_group" and values.get("reused_decision") for stage, values in events)
        )
        self.assertEqual({row["status"] for row in result["lineage"]["results"]}, {"unchanged"})

    def test_changed_history_is_replaced_in_its_old_span_before_append(self):
        self.live["AAA"][0] = bar("2016-01-04", "456")
        self.live["AAA"][1]["v"] = 42
        result = self.run_update()
        self.assertTrue(result["complete"], result)
        refresh = result["stages"]["refresh"]
        self.assertEqual(refresh["check"]["stats"]["differed"], 1)
        replaced = refresh["pricing"]["results"]
        self.assertEqual([row["symbol"] for row in replaced], ["AAA"])
        self.assertEqual(replaced[0]["request"]["end_exclusive"], "2026-10-15T00:00:00Z")
        self.assertEqual(len(self.calls), 25)  # Live check + full history/first checks + append.
        rows = read_pricing_views(self.store, "AAA")["all"]
        self.assertEqual(rows[0][1], "456")
        self.assertEqual(rows[1][5], "42")
        self.assertEqual([row[0][:10] for row in rows], ["2016-01-04", "2026-10-14", "2026-10-15", "2026-10-16"])
        self.assert_finished()

    def test_complete_empty_hints_still_catch_up_changed_rules_without_metadata_requests(self):
        checks = read_checks(self.store)
        self.assertTrue(checks["complete"])
        self.assertEqual(checks["checks"], {})
        pricing = files_snapshot(self.store / "end_of_day/pricing")
        with (
            patch.object(lineage_contract, "CORRELATION_VERSION", lineage_contract.CORRELATION_VERSION + 1),
            patch.object(pipeline, "acquire", side_effect=AssertionError("metadata reacquisition")),
            patch.object(pipeline, "local_symbols", side_effect=AssertionError("receipt discovery")),
        ):
            result = self.run_update("2026-10-15")
            self.assertTrue(result["complete"], result)
            processed = {
                symbol
                for row in result["stages"]["recovery"]["lineage"]["results"]
                for symbol in row["processed"]
            }
            self.assertEqual(processed, set(self.assets))
            index = lineage_storage.read_index(self.store / "end_of_day/lineage")
            self.assertTrue(
                all(lineage_contract.reusable_versions(entry["versions"]) for entry in index["symbols"].values())
            )
            repeated = self.run_update("2026-10-15")
            self.assertTrue(repeated["complete"], repeated)
            self.assertEqual(repeated["stages"]["recovery"]["lineage"]["results"], [])
        self.assertEqual(len(self.calls), 2, "each invocation only needs its fresh live history check")
        self.assertEqual(self.identity.request_count, 0)
        self.assertEqual(files_snapshot(self.store / "end_of_day/pricing"), pricing)

    def test_missing_live_boundary_stops_before_any_publication_or_append(self):
        self.live["AAA"] = []
        before = files_snapshot(self.store)
        result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_stage"], "refresh")
        self.assertIn("live check incomplete", result["error"])
        self.assertEqual(files_snapshot(self.store), before)

    def test_malformed_append_page_preserves_all_publications(self):
        before = files_snapshot(self.store)

        def malformed(params):
            response = self.page(params)
            if params["start"][:10] == "2026-10-15" and params["adjustment"] == "dividend":
                response["bars"]["AAA"].insert(0, bar("2026-10-14"))
            return response

        with patch.object(self.client, "_get", side_effect=malformed):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["stages"]["append"]["failed"], 2)
        self.assertEqual(files_snapshot(self.store), before)

    def test_changed_summary_during_append_prevents_stale_publication(self):
        before = files_snapshot(self.store / "end_of_day")
        original = check_summary.read_snapshot(self.store)["entries"][0]
        changed = False

        def concurrent(params):
            nonlocal changed
            if params["start"][:10] == "2026-10-15" and not changed:
                changed = True
                check_summary.invalidate(self.store, ["AAA"])
                check_summary.replace_entries(self.store, {"AAA": original})
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=concurrent):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_stage"], "append")
        self.assertIn("baseline changed", result["error"])
        self.assertEqual(files_snapshot(self.store / "end_of_day"), before)

    def test_handoff_changed_between_stages_is_rejected_before_append_requests(self):
        refresh = update.refresh_existing
        entry = check_summary.read_snapshot(self.store)["entries"][0]
        before = files_snapshot(self.store / "end_of_day")

        def concurrent(*args, **kwargs):
            result = refresh(*args, **kwargs)
            check_summary.invalidate(self.store, ["AAA"])
            check_summary.replace_entries(self.store, {"AAA": entry})
            return result

        with patch.object(update, "refresh_existing", side_effect=concurrent):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed_stage"], "append")
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(files_snapshot(self.store / "end_of_day"), before)

    def test_failed_history_replacement_stops_before_append(self):
        self.live["AAA"][1]["v"] = 42
        before = files_snapshot(self.store)

        def fail_full_history(params):
            if params["start"][:4] == "2016":
                raise OSError("full history request failed")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=fail_full_history):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertEqual(result["stages"]["refresh"]["pricing"]["failed"], 1)
        self.assertNotIn("append", result["stages"])
        self.assertEqual(files_snapshot(self.store), before)

    def test_same_command_recovers_interrupted_pricing_then_rechecks_live(self):
        original = storage.os.replace

        def interrupt(source, target):
            path = Path(target)
            if "AAA" in path.parts and path.suffix == ".gz":
                raise OSError("interrupted pricing install")
            return original(source, target)

        with patch.object(storage.os, "replace", side_effect=interrupt):
            failed = self.run_update()
        self.assertFalse(failed["complete"])
        self.assertTrue((self.store / "end_of_day/pricing/AAA/.pending.json").exists())
        self.calls.clear()
        recovered = self.run_update()
        self.assertTrue(recovered["complete"], recovered)
        self.assertEqual(recovered["stages"]["recovery"]["pricing_recovered"], ["AAA"])
        self.assertEqual(len(self.calls), 1, "recovery must not reacquire completed new sessions")
        self.assert_finished()

    def test_same_command_recovers_interrupted_databases_before_work_selection(self):
        leave_hot_journal(check_summary.summary_path(self.store), "UPDATE state SET complete=0")
        leave_hot_journal(self.store / CHECK_FILE, "DELETE FROM checks")
        leave_hot_journal(self.store / "end_of_day/lineage/index.sqlite3", "PRAGMA user_version=0")
        result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertEqual(len(self.calls), 9)
        self.assert_finished()

    def test_deleted_work_list_restores_pending_lineage_before_the_live_check(self):
        download_pricing(self.store, "AAA", "2026-10-17", client=FakeClient(self.live["AAA"]))
        notice = self.store / "end_of_day/manifests/AAA/update.json"
        self.assertTrue(notice.exists())
        (self.store / CHECK_FILE).unlink()

        def after_recovery(params):
            self.assertFalse(notice.exists(), "known coverage must be restored before history checking")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=after_recovery):
            result = self.run_update()
        self.assertTrue(result["complete"], result)
        self.assertTrue(read_checks(self.store)["complete"])
        recovery = result["stages"]["recovery"]["lineage"]["results"]
        self.assertTrue(any(row["symbol"] == "AAA" and row["status"] == "processed" for row in recovery))
        self.assert_finished()

    def test_same_command_recovers_interrupted_lineage_before_using_summary(self):
        with patch.object(lineage_storage, "_install", side_effect=OSError("interrupted lineage install")):
            failed = self.run_update()
        self.assertFalse(failed["complete"])
        self.assertTrue((self.store / "end_of_day/lineage/.pending.json").exists())
        self.calls.clear()
        recovered = self.run_update()
        self.assertTrue(recovered["complete"], recovered)
        self.assertTrue(recovered["stages"]["recovery"]["lineage"]["recovered"])
        self.assertEqual(len(self.calls), 1)
        self.assert_finished()

    def test_midnight_during_live_check_does_not_extend_the_frozen_target(self):
        now = datetime(2026, 10, 17, 3, 59, tzinfo=timezone.utc)

        def crossing(params):
            nonlocal now
            now = datetime(2026, 10, 17, 4, 1, tzinfo=timezone.utc)
            return self.page(params)

        with (
            patch("libs.alpaca_pricing_download.download.utc_now", side_effect=lambda: now),
            patch.object(self.client, "_get", side_effect=crossing),
        ):
            result = self.run_update(None)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["end_exclusive"], "2026-10-16")
        self.assertTrue(all(timestamp(call["end"]) < timestamp("2026-10-16T00:00:00Z") for call in self.calls))
        self.assertTrue(
            all(row["asof"] == "2026-10-16" for row in check_summary.read_snapshot(self.store)["entries"])
        )

    def test_missing_summary_is_not_silently_rebuilt_or_scanned(self):
        check_summary.summary_path(self.store).unlink()
        before = files_snapshot(self.store)
        with patch.object(Path, "iterdir", side_effect=AssertionError("scan")):
            result = self.run_update()
        self.assertFalse(result["complete"])
        self.assertIn("summary is missing", result["error"])
        self.assertEqual(self.calls, [])
        self.assertEqual(files_snapshot(self.store), before)

    def test_canonical_update_dispatches_frozen_target_to_universe_owner(self):
        from libs.alpaca_cli import main

        with (
            patch("libs.alpaca_cli.clients", return_value=(self.client, self.identity)),
            patch(
                "libs.alpaca_lineage_build.universe_update.run_update", return_value={"complete": True}
            ) as run,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(main(["update", "--store", str(self.store), "--through", "2026-10-16"]), 0)
        self.assertEqual(run.call_args.args, (self.store, "2026-10-17"))

    def test_conflicting_cli_overrides_fail_before_network_or_publication(self):
        from libs.alpaca_cli import main

        before = files_snapshot(self.store)
        for flags in (
            ["--append-ready"],
            ["--from", "2016-01-01"],
            ["--force-download"],
            ["--all"],
            ["--symbol", "AAA"],
        ):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                main(["update", "--store", str(self.store), *flags])
        self.assertEqual(self.calls, [])
        self.assertEqual(files_snapshot(self.store), before)

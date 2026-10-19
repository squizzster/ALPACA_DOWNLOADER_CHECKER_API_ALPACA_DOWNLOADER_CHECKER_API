import copy
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from itertools import islice, product
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_pricing_download import snapshot as file_snapshot

from libs.alpaca_data_api_solver import build_plan
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import append, command_line, storage
from libs.alpaca_pricing_download.check_summary import read_snapshot
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, DownloadError, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient


class AppendTests(unittest.TestCase):
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
        cls.assets = {symbol: asset(symbol) for symbol in ("AAA", "BBB")}
        cls.old = [bar("2016-01-04"), bar("2026-10-14")]
        for symbol in cls.assets:
            download_pricing(cls.base, symbol, "2026-10-15", client=FakeClient(cls.old))
        result = run_lineage(cls.base, list(cls.assets), identity_client=IdentityClient(cls.assets))
        if result["failed"]:
            raise AssertionError(result)
        rebuild(cls.base)

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        shutil.copytree(self.base, self.store)
        self.ready = read_snapshot(self.store)
        self.live = {symbol: [bar("2026-10-15"), bar("2026-10-16")] for symbol in self.assets}
        self.calls = []
        self.client = BarsClient("test", "test")
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def page(self, params):
        self.calls.append(params)
        start, end = timestamp(params["start"]), timestamp(params["end"])
        self.assertGreaterEqual(start, timestamp("2026-10-15T00:00:00Z"), "history must not be requested")
        return {
            "bars": {
                symbol: [row for row in self.live[symbol] if start <= timestamp(row["t"]) <= end]
                for symbol in params["symbols"].split(",")
            }
        }

    def run_append(self, end="2026-10-17", **kwargs):
        return append.append_ready(self.store, self.ready, end, client=self.client, **kwargs)

    def test_eight_calls_for_shared_two_day_append_preserves_history_and_handoff(self):
        root = self.store / "end_of_day/pricing/AAA"
        cold = {str(path): (path.read_bytes(), path.stat().st_ino) for path in root.glob("completed_years*")}
        year = {str(path): (path.read_bytes(), path.stat().st_ino) for path in (root / "2026").glob("year*")}
        with (
            patch.object(storage, "read_boundary", side_effect=AssertionError("no reauthentication")),
            patch.object(self.client, "fetch", side_effect=AssertionError("no individual fetches")),
        ):
            result = self.run_append()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(self.calls), 8)
        self.assertTrue(all(call["symbols"] == "AAA,BBB" for call in self.calls))
        self.assertEqual({call["adjustment"] for call in self.calls}, set(ADJUSTMENTS))
        for path, previous in {**cold, **year}.items():
            self.assertEqual((Path(path).read_bytes(), Path(path).stat().st_ino), previous)
        for symbol in self.assets:
            views = read_pricing_views(self.store, symbol)
            for rows in views.values():
                self.assertEqual(
                    [row[0][:10] for row in rows], ["2016-01-04", "2026-10-14", "2026-10-15", "2026-10-16"]
                )
            notice = self.store / f"end_of_day/manifests/{symbol}/update.json"
            self.assertEqual(json.loads(notice.read_text()), {"from": "2026-10-15", "to": "2026-10-16"})
        lineage = run_lineage(self.store, list(self.assets), identity_client=IdentityClient(self.assets))
        self.assertEqual(lineage["failed"], 0, lineage)
        current = read_snapshot(self.store)
        self.assertTrue(
            all(
                entry["asof"] == "2026-10-17" and entry["last_all"][0][:10] == "2026-10-16"
                for entry in current["entries"]
            )
        )

    def test_repeat_ready_boundary_needs_no_ticker_reads_or_network(self):
        self.run_append()
        self.ready = read_snapshot(self.store)
        before = file_snapshot(self.store)
        with (
            patch.object(append, "_publish_append", side_effect=AssertionError("ticker read")),
            patch.object(self.client, "execute_plan", side_effect=AssertionError("API")),
        ):
            result = self.run_append()
        self.assertEqual(result["failed"], 0)
        self.assertTrue(all(row["status"] == "unchanged" for row in result["results"]))
        self.assertEqual(file_snapshot(self.store), before)

    def test_empty_response_advances_coverage_without_inventing_bars(self):
        self.live["AAA"] = []
        result = self.run_append()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["added_rows"], dict.fromkeys(ADJUSTMENTS, 0))
        self.assertEqual(len(read_pricing_views(self.store, "AAA")["all"]), 2)
        entry = next(row for row in read_snapshot(self.store)["entries"] if row["symbol"] == "AAA")
        self.assertEqual(entry["asof"], "2026-10-17")
        self.assertEqual(entry["last_all"][0][:10], "2026-10-14")

    def test_explicit_empty_baseline_can_share_the_internal_append_path(self):
        download_pricing(self.store, "EMPTY", "2026-10-15", client=FakeClient([]))
        self.live["EMPTY"] = []
        self.calls.clear()
        result = append.append_entries(
            self.store,
            [{"symbol": "EMPTY", "asof": "2026-10-15", "last_all": None}],
            client=self.client,
            end_by_symbol={"EMPTY": timestamp("2026-10-17T00:00:00Z")},
        )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(self.calls), 8)
        self.assertTrue(all(call["symbols"] == "EMPTY" for call in self.calls))
        source = storage.load_source(self.store / "end_of_day/pricing/EMPTY", "EMPTY")
        self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-17T00:00:00Z")
        self.assertTrue(all(not rows for rows in read_pricing_views(self.store, "EMPTY").values()))

    def test_request_failure_publishes_no_partial_adjustments(self):
        before = file_snapshot(self.store)
        original = self.page

        def fail(params):
            if params["adjustment"] == "all":
                raise DownloadError("synthetic failure " + "X" * 70000)
            return original(params)

        with patch.object(self.client, "_get", side_effect=fail):
            result = self.run_append()
        self.assertEqual(result["failed"], 2)
        self.assertTrue(all(len(row["error"]) <= 300 for row in result["results"]))
        self.assertEqual(file_snapshot(self.store), before)

    def test_pagination_finishes_each_view_before_publication(self):
        def paged(params):
            self.calls.append(params)
            if "page_token" not in params:
                return {"bars": {"AAA": self.live["AAA"]}, "next_page_token": "second"}
            self.assertEqual(params["page_token"], "second")
            return {"bars": {"BBB": self.live["BBB"]}, "next_page_token": None}

        with patch.object(self.client, "_get", side_effect=paged):
            result = self.run_append()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["requests"]["page_requests"], 16)
        for symbol in self.assets:
            self.assertTrue(all(len(rows) == 4 for rows in read_pricing_views(self.store, symbol).values()))

    def test_separate_memory_cohorts_publish_against_the_original_handoff(self):
        with patch.object(append, "MAX_ROWS", 16):
            result = self.run_append()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["requests"]["page_requests"], 16)
        self.assertEqual([call["symbols"] for call in self.calls], ["AAA"] * 8 + ["BBB"] * 8)

    def test_interrupted_append_uses_normal_recovery_without_downloading_again(self):
        self.ready["entries"] = [self.ready["entries"][0]]
        root = self.store / "end_of_day/pricing/AAA"
        original = storage.os.replace

        def interrupt(source, target):
            if Path(target).suffix == ".gz":
                raise OSError("interrupted append")
            return original(source, target)

        with patch.object(storage.os, "replace", side_effect=interrupt):
            result = self.run_append()
        self.assertEqual(result["failed"], 1)
        self.assertTrue((root / ".pending.json").exists())
        with self.assertRaises(DownloadError):
            read_snapshot(self.store)
        with patch.object(self.client, "execute_plan", side_effect=AssertionError("no redownload")):
            recovered = download_pricing(self.store, "AAA", "2026-10-17", client=self.client)
        self.assertTrue(recovered["recovered"])
        self.assertFalse((root / ".pending.json").exists())
        self.assertEqual(len(read_pricing_views(self.store, "AAA")["all"]), 4)
        self.assertEqual(read_snapshot(self.store)["entries"][0]["asof"], "2026-10-17")

    def test_process_death_before_journal_does_not_block_append_retry(self):
        root = self.store / "end_of_day/pricing/AAA"
        before = file_snapshot(root)
        child = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                """
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from libs.alpaca_pricing_download import append, storage
from libs.alpaca_pricing_download.check_summary import read_snapshot
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp

store = Path(sys.argv[1])
ready = read_snapshot(store)
need = append.append_needs(ready['entries'], timestamp('2026-10-17T00:00:00Z'))[0][0]
original = storage.write_file
def interrupted(path, data):
    original(path, data)
    os._exit(91)
with patch.object(storage, 'write_file', side_effect=interrupted):
    bars = [{key: value if key == 't' else Decimal(str(value)) for key, value in row.items()}
            for row in json.loads(sys.argv[2])]
    append._publish_append(store, ready['entries'][0], need, dict.fromkeys(ADJUSTMENTS, bars))
""",
                str(self.store),
                json.dumps(self.live["AAA"], default=str),
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        self.assertEqual(child.returncode, 91, child.stderr)
        self.assertTrue((root / ".staging").is_dir())
        self.assertFalse((root / ".pending.json").exists())
        self.assertEqual(
            {name: data for name, data in file_snapshot(root).items() if not name.startswith(".staging/")},
            before,
        )
        result = self.run_append()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(self.calls), 8)
        self.assertFalse((root / ".staging").exists())
        self.assertEqual(len(read_pricing_views(self.store, "AAA")["all"]), 4)

    def test_append_cleanup_refuses_unowned_staging_content(self):
        root = self.store / "end_of_day/pricing/AAA"
        (root / ".staging").mkdir()
        (root / ".staging/unowned").write_text("keep")
        before = file_snapshot(root)
        result = self.run_append()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(file_snapshot(root), before)

    def test_changed_handoff_token_stops_before_requests(self):
        download_pricing(self.store, "AAA", "2026-10-16", client=FakeClient(self.old + [bar("2026-10-15")]))
        with (
            patch.object(self.client, "execute_plan", side_effect=AssertionError("API")),
            self.assertRaises(DownloadError),
        ):
            self.run_append()

    def test_stale_source_cannot_be_overwritten_by_acquired_append(self):
        need = append.append_needs(self.ready["entries"], timestamp("2026-10-17T00:00:00Z"))[0][0]
        download_pricing(self.store, "AAA", "2026-10-16", client=FakeClient(self.old + [bar("2026-10-15")]))
        before = file_snapshot(self.store)
        with self.assertRaisesRegex(DownloadError, "coverage changed"):
            append._publish_append(
                self.store, self.ready["entries"][0], need, dict.fromkeys(ADJUSTMENTS, self.live["AAA"])
            )
        self.assertEqual(file_snapshot(self.store), before)

    def test_baseline_change_is_isolated_and_no_old_prices_are_replaced(self):
        self.ready["entries"][0]["last_all"][1] = "999"
        before = file_snapshot(self.store / "end_of_day/pricing/AAA")
        result = self.run_append()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][1]["status"], "updated")
        self.assertEqual(file_snapshot(self.store / "end_of_day/pricing/AAA"), before)

    def test_month_and_year_rollover_preserve_approved_month_and_cold_history(self):
        for end, dates in (
            ("2026-11-03", ("2026-10-30", "2026-11-02")),
            ("2027-01-05", ("2026-12-31", "2027-01-04")),
        ):
            with self.subTest(end=end):
                self.ready = read_snapshot(self.store)
                self.live = {symbol: [bar(day) for day in dates] for symbol in self.assets}
                result = self.run_append(end)
                self.assertEqual(result["failed"], 0, result)
                for symbol in self.assets:
                    days = [row[0][:10] for row in read_pricing_views(self.store, symbol)["all"]]
                    self.assertEqual(days[:2], ["2016-01-04", "2026-10-14"])
                    self.assertEqual(days[-2:], list(dates))
                    self.assertEqual(len(days), len(set(days)))

    def test_cli_ready_selection_and_all_use_the_summary(self):
        with (
            patch.object(command_line, "BarsClient", return_value=self.client),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            code = command_line.main(
                ["--store", str(self.store), "--append-ready", "--all", "--to", "2026-10-16"]
            )
        self.assertEqual(code, 0)
        self.assertEqual(len(self.calls), 8)
        self.assertTrue(all(call["symbols"] == "AAA,BBB" for call in self.calls))
        for extra in (
            ("-s", "MISSING"),
            ("-s", "AAA", "--from", "2020-01-01"),
            ("-s", "AAA", "--force-download"),
        ):
            with (
                redirect_stderr(io.StringIO()),
                patch.object(self.client, "execute_plan", side_effect=AssertionError("API")),
            ):
                self.assertEqual(command_line.main(["--store", str(self.store), "--append-ready", *extra]), 1)


class AppendPlanTests(unittest.TestCase):
    def test_large_ready_rosters_use_eight_existing_actions_without_history_or_io(self):
        row = ("2026-10-14T04:00:00Z", "1", "1", "1", "1", "1", "1", "1")
        for count in (5000, 8000):
            symbols = (
                "S" + "".join(letters)
                for letters in islice(product("ABCDEFGHIJKLMNOPQRSTUVWXYZ", repeat=3), count)
            )
            entries = [{"symbol": symbol, "asof": "2026-10-15", "last_all": list(row)} for symbol in symbols]
            with patch.object(Path, "open", side_effect=AssertionError("file access")):
                needs, unchanged = append.append_needs(entries, timestamp("2026-10-17T00:00:00Z"))
                plan = build_plan(
                    {"steps": [{"needs": needs}]}, max_symbols=append.MAX_SYMBOLS, merge_ranges=False
                )
            self.assertEqual(unchanged, [])
            self.assertEqual(len(plan["steps"][0]["actions"]), 8)
            self.assertTrue(
                all(action["strategy"] == "bars_multi_range" for action in plan["steps"][0]["actions"])
            )
            self.assertTrue(
                all(
                    action["request"]["params"]["start"] == "2026-10-15T00:00:00Z"
                    for action in plan["steps"][0]["actions"]
                )
            )

    def test_mixed_ranges_remain_exact_and_memory_cohorts_are_bounded(self):
        row = ["2026-10-13T04:00:00Z", "1", "1", "1", "1", "1", "1", "1"]
        entries = [
            {"symbol": "AAA", "asof": "2026-10-14", "last_all": row},
            {"symbol": "BBB", "asof": "2026-10-15", "last_all": row},
        ]
        needs, _ = append.append_needs(entries, timestamp("2026-10-17T00:00:00Z"))
        plan = build_plan({"steps": [{"needs": needs}]}, max_symbols=append.MAX_SYMBOLS, merge_ranges=False)
        self.assertEqual(len(plan["steps"][0]["actions"]), 16)
        with patch.object(append, "MAX_ROWS", 24):
            self.assertEqual([len(c) for c in append._cohorts(needs)], [1, 1])
        with self.assertRaisesRegex(DownloadError, "duplicate"):
            append.append_needs([entries[0], copy.deepcopy(entries[0])], timestamp("2026-10-17T00:00:00Z"))

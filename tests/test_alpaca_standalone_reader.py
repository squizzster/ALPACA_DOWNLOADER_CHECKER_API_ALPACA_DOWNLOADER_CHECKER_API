import csv
import fcntl
import io
import json
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing, contextmanager, redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, calendar_bars
from test_alpaca_pricing_download import FakeClient, bar, snapshot
from test_alpaca_security_lineage import FakeIdentityClient

from alpaca_end_of_day import AmbiguousTickerError, ReaderBusyError, StandaloneReader, StandaloneReaderError
from alpaca_end_of_day.standalone_cli import main
from libs.alpaca_end_of_day_security_correlation.security_identity import security_id_for_alpaca_asset_ids
from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.contract import LineageBusyError, LineageError, reusable_versions, versions
from libs.alpaca_lineage_build.evidence import output_evidence
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.reader import read_lineage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing


def hold_lock(path, directory, entered, release):
    descriptor = os.open(path, os.O_RDONLY | (os.O_DIRECTORY if directory else 0))
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        entered.set()
        if not release.wait(15):
            raise RuntimeError("lock test timed out")
    finally:
        os.close(descriptor)


def waiting_reader(store, started, complete, result):
    started.set()
    try:
        result.put(StandaloneReader(store, wait=True).get_history("TEST")["bar_count"])
    except Exception as error:
        result.put(str(error))
    finally:
        complete.set()


def paused_worker(store, entered, release):
    class PausedClient(IdentityClient):
        def get_corporate_actions(self, *args):
            entered.set()
            if not release.wait(15):
                raise RuntimeError("worker test timed out")
            return super().get_corporate_actions(*args)

    result = run_lineage(store, ["OTHER"], refresh=True, identity_client=PausedClient({"OTHER": asset("OTHER")}))
    if result["failed"]:
        raise RuntimeError(result)


def selected_reader(store, selected, result):
    original = storage.publication_lock

    @contextmanager
    def gate(*args, **kwargs):
        with original(*args, **kwargs):
            yield
        selected.set()

    try:
        with patch.object(storage, "publication_lock", gate):
            result.put(StandaloneReader(store, wait=True).get_history("TEST")["bar_count"])
    except Exception as error:
        result.put(str(error))


class StandaloneReaderTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.bars = [
            bar(day)
            for day in (
                "2025-12-31",
                "2026-01-02",
                "2026-03-02",
                "2026-05-01",
                "2026-07-01",
                "2026-09-01",
                "2026-10-01",
                "2026-10-13",
            )
        ]
        self.download("TEST", self.bars)
        self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        self.reader = StandaloneReader(self.store)

    def download(self, symbol, bars, end="2026-10-14"):
        return download_pricing(self.store, symbol, end, client=FakeClient(bars))

    def build(self, symbols, client):
        result = run_lineage(self.store, symbols, identity_client=client)
        self.assertEqual(result["failed"], 0, result)

    def publication(self, symbol="TEST"):
        index = storage.read_index(self.root, symbol)
        receipt = storage.read_receipt(self.root, symbol, index)
        return receipt, storage.read_boundaries(self.root, receipt)

    def change_publication(self, change):
        receipt, boundaries = self.publication()
        change(receipt, boundaries)
        for kind, boundary in boundaries.items():
            receipt["boundaries"][kind]["sha256"] = storage.document_digest(boundary)
        storage.publish(
            self.store, self.root, storage.read_index(self.root), {"TEST": receipt}, {"TEST": boundaries}, {}
        )

    def renamed_pair(self, *, differing=False):
        old_rows = calendar_bars("2023-12-01", "2023-12-13")
        new_rows = old_rows + calendar_bars("2023-12-13", "2023-12-22")
        self.download("OLD", old_rows)
        if differing:
            new_rows = [bar(row["t"][:10], "200.0000000000000000001") for row in new_rows]
        self.download("NEW", new_rows)
        old, new = asset("OLD"), asset("NEW")
        self.build(
            ["NEW"],
            IdentityClient(
                {"OLD": old, "NEW": new, "111111111": old, "222222222": new},
                [
                    {
                        "action_type": "name_change",
                        "id": "rename",
                        "process_date": "2023-12-13",
                        "old_symbol": "OLD",
                        "new_symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                    }
                ],
            ),
        )
        return old_rows, new_rows

    def test_exact_values_all_views_and_overlay_without_writes(self):
        before = snapshot(self.store)
        for adjustment in ADJUSTMENTS:
            with self.subTest(adjustment=adjustment):
                result = self.reader.get_history(" test ", adjustment)
                self.assertEqual(result["bar_count"], len(self.bars))
                self.assertEqual([r["session"] for r in result["bars"]], [r["t"][:10] for r in self.bars])
                self.assertEqual(result["bars"][0]["close"], str(self.bars[0]["c"]))
                self.assertEqual(result["bars"][0]["volume"], "9007199254740993")
                self.assertEqual(result["security"]["public_id"], security_id_for_alpaca_asset_ids(["TEST-id"]))
        self.assertEqual(before, snapshot(self.store))

    def test_inclusive_bounds_and_empty_range(self):
        result = self.reader.get_history("TEST", start=date(2026, 1, 2), end="2026-10-01")
        self.assertEqual([b["session"] for b in result["bars"]], [r["t"][:10] for r in self.bars[1:-1]])
        self.assertEqual(self.reader.get_history("TEST", start="2027-01-01")["bars"], [])

    def test_multiple_views_share_one_snapshot_and_deduplicate_physical_reads(self):
        before = snapshot(self.store)
        original, opened = Path.read_bytes, []

        def observe(path):
            if path.suffix == ".gz":
                opened.append(path)
            return original(path)

        with (
            patch.object(self.reader, "_snapshot", wraps=self.reader._snapshot) as select,
            patch.object(Path, "read_bytes", observe),
        ):
            result = self.reader.get_histories("TEST", start="2026-10-01")
        select.assert_called_once_with("TEST")
        self.assertEqual(list(result["views"]), list(ADJUSTMENTS))
        self.assertEqual(len(opened), 3)  # Eight aliases, three physical boundaries.
        self.assertEqual(len(set(opened)), 3)
        self.assertTrue(all(view["bar_count"] == 2 for view in result["views"].values()))
        for adjustment, view in result["views"].items():
            single = self.reader.get_history("TEST", adjustment, start="2026-10-01")
            self.assertEqual(view, {"bar_count": single["bar_count"], "bars": single["bars"]})
            self.assertEqual(result["generation"], single["generation"])
        self.assertEqual(before, snapshot(self.store))
        opened.clear()
        with patch.object(Path, "read_bytes", observe):
            self.reader.get_histories("TEST")
        self.assertEqual(len(opened), 3)  # No persistent decoded cache across calls.

    def test_multiple_views_validate_before_reading_and_preserve_requested_order(self):
        missing = StandaloneReader(self.store / "absent")
        for adjustments in ([], (), "raw", None, ["raw", "bad"], [None], [["raw"]]):
            with self.subTest(adjustments=adjustments), self.assertRaises(ValueError):
                missing.get_histories("TEST", adjustments)
        result = self.reader.get_histories("TEST", ["all", "raw", "split,dividend", "raw"], start="2027-01-01")
        self.assertEqual(list(result["views"]), ["all", "raw", "split,dividend"])
        self.assertTrue(all(view == {"bar_count": 0, "bars": []} for view in result["views"].values()))

    def test_unavailable_requested_view_fails_before_any_payload_read(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    if entry["adjustment"] == "all":
                        for segment in entry["segments"]:
                            segment["branch"] = "uncorrelated"

        self.change_publication(change)
        with patch("alpaca_end_of_day.standalone._rows", side_effect=AssertionError("payload read")):
            with self.assertRaisesRegex(StandaloneReaderError, "adjustment is unavailable"):
                self.reader.get_histories("TEST", ["raw", "all"])

    def test_bad_arguments_before_store_access(self):
        reader = StandaloneReader(self.store / "absent")
        for kwargs in (
            {"adjustment": "invalid"},
            {"public_id": "not-an-id"},
            {"start": "2026-02-30"},
            {"start": "20260101"},
            {"start": datetime(2026, 1, 1)},
            {"start": "2026-02-01", "end": "2026-01-01"},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                reader.get_history("TEST", **kwargs)

    def test_resolution_reads_no_payload_and_does_not_recompute_or_authenticate(self):
        original = Path.read_bytes

        def no_gzip(path):
            self.assertNotEqual(path.suffix, ".gz")
            return original(path)

        with (
            patch.object(Path, "read_bytes", no_gzip),
            patch("libs.alpaca_lineage_build.pipeline.correlate", side_effect=AssertionError("rules rerun")),
            patch("libs.alpaca_pricing_download.storage.read_boundary", side_effect=AssertionError("audit")),
            patch("libs.alpaca_pricing_download.storage.digest", side_effect=AssertionError("payload hash")),
        ):
            result = self.reader.resolve_ticker("TEST")
        self.assertEqual(len(result["matches"]), 1)
        self.assertNotIn("online", result["matches"][0])

    def test_history_reads_only_one_payload_per_boundary_and_follows_alias(self):
        opened = []
        original = Path.read_bytes

        def record(path):
            if path.suffix == ".gz":
                opened.append(path)
            return original(path)

        with patch.object(Path, "read_bytes", record):
            self.reader.get_history("TEST", "all")
        self.assertEqual(len(opened), 3)
        self.assertTrue(all(path.name in {"completed_years.raw.csv.gz", "raw.csv.gz"} for path in opened))

    def test_distinct_adjusted_payload_is_selected(self):
        class AdjustedClient(FakeClient):
            def fetch(self, symbol, adjustment, start, end, *, asof=None):
                rows = super().fetch(symbol, adjustment, start, end, asof=asof)
                return [{**row, "c": row["c"] * 2} for row in rows] if adjustment == "all" else rows

        download_pricing(self.store, "TEST", "2026-10-14", client=AdjustedClient(self.bars), refresh_all=True)
        self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        self.assertEqual(self.reader.get_history("TEST", "raw")["bars"][0]["close"], str(self.bars[0]["c"]))
        self.assertEqual(self.reader.get_history("TEST", "all")["bars"][0]["close"], str(self.bars[0]["c"] * 2))
        both = self.reader.get_histories("TEST", ["raw", "all"])["views"]
        self.assertEqual(both["raw"]["bars"][0]["close"], str(self.bars[0]["c"]))
        self.assertEqual(both["all"]["bars"][0]["close"], str(self.bars[0]["c"] * 2))

    def test_source_alias_does_not_hide_resolved_history_or_become_a_trading_label(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["historical_symbol"] = "ACTUAL"

        self.change_publication(change)
        self.assertEqual(len(self.reader.resolve_ticker("TEST")["matches"]), 1)
        result = self.reader.get_history("TEST")
        self.assertEqual(result["bar_count"], len(self.bars))
        self.assertEqual(result["security"]["tickers"], ["ACTUAL"])
        self.assertTrue(all(row["tickers"] == ["ACTUAL"] for row in result["bars"]))

    def test_read_holds_pricing_lock_but_releases_publication_gate_during_prices(self):
        from alpaca_end_of_day import standalone

        original = standalone._rows

        def check_locked(*args):
            for path, directory in (
                (self.root, True),
                (self.store / "end_of_day/pricing/TEST/.download.lock", False),
            ):
                descriptor = os.open(path, os.O_RDONLY | (os.O_DIRECTORY if directory else 0))
                try:
                    if directory:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    else:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)
            return original(*args)

        with patch.object(standalone, "_rows", side_effect=check_locked):
            self.assertEqual(self.reader.get_history("TEST")["bar_count"], len(self.bars))

    def test_metadata_uses_only_selected_group_not_universe_scan(self):
        original = storage.read_index
        with patch.object(storage, "read_index", wraps=original) as read_index:
            self.reader.resolve_ticker("TEST")
        self.assertEqual(read_index.call_count, 2)
        self.assertTrue(all(call.args == (self.root, "TEST") for call in read_index.call_args_list))

    def test_unrelated_worker_and_all_reader_paths_can_run_and_publish_together(self):
        self.download("OTHER", self.bars)
        self.build(["OTHER"], IdentityClient({"OTHER": asset("OTHER")}))
        ctx = multiprocessing.get_context("spawn")
        entered, release = ctx.Event(), ctx.Event()
        worker = ctx.Process(target=paused_worker, args=(self.store, entered, release))
        worker.start()
        try:
            self.assertTrue(entered.wait(10))
            self.assertEqual(self.reader.get_history("TEST")["bar_count"], len(self.bars))
            self.assertEqual(read_lineage(self.store, "TEST")["receipt"]["symbol"], "TEST")
            self.assertIsInstance(output_evidence(self.store, "TEST", offline=True), dict)
            with self.assertRaises(ReaderBusyError):
                self.reader.resolve_ticker("OTHER")
            with storage.reader_snapshot(self.store, lambda root: storage.select_inputs(root, "TEST")):
                release.set()
                worker.join(10)
                self.assertEqual(worker.exitcode, 0, "unrelated publication waited for the reader")
                self.assertEqual(self.reader.get_history("TEST")["bar_count"], len(self.bars))
        finally:
            if worker.is_alive():
                release.set()
                worker.terminate()
            worker.join(5)
            worker.close()

    def test_waiting_reader_does_not_hold_publication_gate_needed_by_its_writer(self):
        ctx = multiprocessing.get_context("spawn")
        selected, result = ctx.Event(), ctx.Queue()
        worker = ctx.Process(target=selected_reader, args=(self.store, selected, result))
        receipt, boundaries = self.publication()
        try:
            with storage.writer_lock(self.store), storage.pricing_locks(self.store, ["TEST"]):
                worker.start()
                self.assertTrue(selected.wait(10))
                # Reader has selected a group and is now waiting for our price lock.
                # Publishing must finish before we release that price lock.
                with storage.publication_lock(self.root, exclusive=True, wait=False):
                    pass  # A lock-order regression fails instead of hanging this test.
                storage.publish(
                    self.store,
                    self.root,
                    storage.read_index(self.root),
                    {"TEST": receipt},
                    {"TEST": boundaries},
                    {},
                )
            worker.join(10)
            self.assertEqual(worker.exitcode, 0)
            self.assertEqual(result.get(timeout=2), len(self.bars))
        finally:
            if worker.is_alive():
                worker.terminate()
            worker.join(5)
            worker.close()
            result.close()
            result.join_thread()

    def test_snapshot_reselects_changed_closure_before_exposing_any_data(self):
        self.download("OTHER", self.bars)
        self.build(["OTHER"], IdentityClient({"OTHER": asset("OTHER")}))
        single = storage.select_inputs(self.root, "TEST")
        expanded = {**single, **storage.select_inputs(self.root, "OTHER")}
        with patch.object(
            storage, "select_inputs", side_effect=[single, expanded, expanded, expanded]
        ) as select:
            with storage.reader_snapshot(self.store, lambda root: storage.select_inputs(root, "TEST")) as snap:
                self.assertEqual(snap["locked"], {"TEST", "OTHER"})
                self.assertEqual(set(snap["receipts"]), {"TEST", "OTHER"})
        self.assertEqual(select.call_count, 4)

    def test_publication_holds_gate_and_prices_through_install_and_acknowledge(self):
        original = storage.finish_publication

        def verify(store, root, journal):
            for path, directory in ((root, True), (store / "end_of_day/pricing/TEST/.download.lock", False)):
                fd = os.open(path, os.O_RDONLY | (os.O_DIRECTORY if directory else 0))
                try:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                finally:
                    os.close(fd)
            return original(store, root, journal)

        with patch.object(storage, "finish_publication", side_effect=verify) as finish:
            result = run_lineage(
                self.store, ["TEST"], identity_client=IdentityClient({"TEST": asset("TEST")}), refresh=True
            )
        self.assertEqual(result["failed"], 0, result)
        finish.assert_called_once()

    def test_snapshot_churn_has_bounded_retries_and_releases_locks(self):
        selected = storage.select_inputs(self.root, "TEST")
        with patch.object(storage, "select_inputs", side_effect=[selected, {}] * 3) as select:
            with self.assertRaises(LineageBusyError):
                with storage.reader_snapshot(self.store, lambda root: storage.select_inputs(root, "TEST")):
                    self.fail("unstable selection must not be exposed")
        self.assertEqual(select.call_count, 6)
        with (self.store / "end_of_day/pricing/TEST/.download.lock").open("rb") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def v2_publication(self):
        def downgrade(receipt, boundaries):
            receipt["schema_version"] = 2
            receipt["versions"]["lineage"] = 2
            for boundary in boundaries.values():
                boundary["schema_version"] = 2

        with patch.object(storage, "VERSION", 2):
            self.change_publication(downgrade)

    def test_v2_protocol_upgrade_reuses_receipts_without_prices_requests_or_rewrites(self):
        self.v2_publication()
        with self.assertRaisesRegex(StandaloneReaderError, "protocol needs upgrade"):
            self.reader.resolve_ticker("TEST")
        before = snapshot(self.store)
        client = IdentityClient({"TEST": asset("TEST")})
        with (
            patch("libs.alpaca_lineage_build.pipeline.load_pricing", side_effect=AssertionError("price read")),
            patch("libs.alpaca_lineage_build.pipeline.correlate", side_effect=AssertionError("rules")),
        ):
            result = run_lineage(self.store, ["TEST"], identity_client=client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["status"], "unchanged")
        self.assertEqual(client.request_count, 0)
        after = snapshot(self.store)
        self.assertEqual(set(before), set(after))
        self.assertEqual(
            [path for path in before if before[path] != after[path]], ["end_of_day/lineage/index.sqlite3"]
        )
        with closing(sqlite3.connect(self.root / "index.sqlite3")) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], storage.VERSION)
        self.assertEqual(self.publication()[0]["versions"]["lineage"], 2)
        self.assertEqual(self.reader.get_history("TEST")["bar_count"], len(self.bars))
        self.assertEqual(read_lineage(self.store, "TEST")["receipt"]["versions"]["lineage"], 2)
        with patch.object(storage, "SUPPORTED_VERSIONS", (1, 2)):
            with self.assertRaisesRegex(LineageError, "unsupported lineage index"):
                storage.read_index(self.root)

    def test_v2_compatibility_does_not_accept_other_changed_versions(self):
        current = versions()
        self.assertTrue(reusable_versions(current))
        self.assertTrue(reusable_versions({**current, "lineage": 2}))
        for name in current:
            self.assertFalse(reusable_versions({**current, name: "changed"}))
        self.assertFalse(reusable_versions({**current, "lineage": 1}))
        self.v2_publication()
        with storage.writer_lock(self.store):
            pass

        def change(receipt, _):
            receipt["versions"]["correlation"] = 1

        self.change_publication(change)
        with self.assertRaisesRegex(StandaloneReaderError, "older rules"):
            self.reader.resolve_ticker("TEST")

    def test_index_failure_remains_a_typed_reader_error_without_repair(self):
        before = snapshot(self.store)
        with patch.object(storage.sqlite3, "connect", side_effect=sqlite3.DatabaseError("damaged index")):
            with self.assertRaisesRegex(StandaloneReaderError, "cannot load lineage dependency index"):
                self.reader.resolve_ticker("TEST")
        self.assertEqual(before, snapshot(self.store))

    def test_v2_month_resume_keeps_cold_payloads_closed(self):
        bars = [bar("2025-12-31")] + calendar_bars("2026-01-02", "2026-10-14")
        download_pricing(self.store, "TEST", "2026-10-14", client=FakeClient(bars), refresh_all=True)
        self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        self.v2_publication()
        self.download("TEST", bars + [bar("2026-10-14")], end="2026-10-15")
        original, opened = Path.read_bytes, []

        def observe(path):
            if path.suffix == ".gz":
                opened.append(path)
            return original(path)

        with patch.object(Path, "read_bytes", observe):
            self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        self.assertTrue(opened)
        self.assertTrue(all(path.parent.name == "Oct" for path in opened), opened)

    def test_month_append_overrides_snapshot_and_generation_changes(self):
        old = self.reader.get_history("TEST")
        self.download("TEST", self.bars + [bar("2026-10-14")], end="2026-10-15")
        self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
        new = self.reader.get_history("TEST")
        self.assertEqual(new["bar_count"], len(self.bars) + 1)
        self.assertNotEqual(old["generation"], new["generation"])

    def test_month_and_year_rollover(self):
        rows = list(self.bars)
        for last, end in (("2026-11-02", "2026-11-03"), ("2027-01-04", "2027-01-05")):
            rows.append(bar(last))
            self.download("TEST", rows, end=end)
            self.build(["TEST"], IdentityClient({"TEST": asset("TEST")}))
            self.assertEqual(self.reader.get_history("TEST")["bar_count"], len(rows))

    def test_changed_pricing_and_notice_are_not_consumed(self):
        self.download("TEST", self.bars + [bar("2026-10-14")], end="2026-10-15")
        before = snapshot(self.store)
        with self.assertRaisesRegex(StandaloneReaderError, "changed pricing"):
            self.reader.get_history("TEST")
        self.assertEqual(before, snapshot(self.store))

    def test_changed_source_digest_without_notice_is_rejected(self):
        path = self.store / "end_of_day/pricing/TEST/source.manifest.json"
        source = json.loads(path.read_bytes())
        source["latest_request"]["asof"] = "2026-10-15"
        storage.atomic_json(path, source)
        with self.assertRaisesRegex(StandaloneReaderError, "changed pricing"):
            self.reader.resolve_ticker("TEST")

    def test_pending_publications_are_not_recovered(self):
        for path in (self.root / ".pending.json", self.store / "end_of_day/pricing/TEST/.pending.json"):
            with self.subTest(path=path):
                storage.atomic_json(path, {})
                before = snapshot(self.store)
                with self.assertRaisesRegex(StandaloneReaderError, "publication incomplete"):
                    self.reader.get_history("TEST")
                self.assertEqual(before, snapshot(self.store))
                path.unlink()

    def test_missing_store_and_missing_lock_never_created(self):
        missing = self.store / "missing"
        with self.assertRaises(StandaloneReaderError):
            StandaloneReader(missing).resolve_ticker("TEST")
        self.assertFalse(missing.exists())
        lock = self.store / "end_of_day/pricing/TEST/.download.lock"
        lock.unlink()
        with self.assertRaises(StandaloneReaderError):
            self.reader.get_history("TEST")
        self.assertFalse(lock.exists())

    def test_symlink_store_and_source_are_rejected(self):
        link = self.store.parent / "link"
        link.symlink_to(self.store, target_is_directory=True)
        with self.assertRaises(StandaloneReaderError):
            StandaloneReader(link).get_history("TEST")
        source = self.store / "end_of_day/pricing/TEST/source.manifest.json"
        renamed = source.with_name("renamed.json")
        source.rename(renamed)
        source.symlink_to(renamed)
        with self.assertRaises(StandaloneReaderError):
            self.reader.get_history("TEST")

    def test_old_rule_versions_are_not_reinterpreted(self):
        self.change_publication(lambda receipt, _: receipt["versions"].update(correlation=1))
        with self.assertRaisesRegex(StandaloneReaderError, "older rules"):
            self.reader.get_history("TEST")

    def test_partial_unresolved_does_not_remove_supported_identity(self):
        def change(_, boundaries):
            for entry in boundaries["completed_years"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["branch"] = "uncorrelated"
                    segment.pop("security_identity", None)

        self.change_publication(change)
        history = self.reader.get_history("TEST")
        self.assertEqual(history["bar_count"], len(self.bars) - 1)
        self.assertEqual(history["bars"][0]["session"], "2026-01-02")

    def test_entirely_unresolved_is_not_guessed(self):
        def change(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment["branch"] = "uncorrelated"

        self.change_publication(change)
        self.assertEqual(self.reader.resolve_ticker("TEST")["matches"], [])
        with self.assertRaisesRegex(StandaloneReaderError, "no resolved"):
            self.reader.get_history("TEST")

    def test_reused_ticker_requires_explicit_id_and_does_not_merge(self):
        alternate = security_id_for_alpaca_asset_ids(["unrelated-id"])

        def change(_, boundaries):
            for entry in boundaries["current_month"]["adjustments"]:
                for segment in entry["segments"]:
                    segment["security_identity"]["security_id"] = alternate

        self.change_publication(change)
        self.assertEqual(len(self.reader.resolve_ticker("TEST")["matches"]), 2)
        with self.assertRaises(AmbiguousTickerError):
            self.reader.get_history("TEST", start="2026-10-01")
        result = self.reader.get_history("TEST", public_id=alternate.upper())
        self.assertEqual(result["bar_count"], 2)
        with self.assertRaisesRegex(StandaloneReaderError, "unavailable"):
            self.reader.get_history("TEST", public_id="a" * 64)

    def test_rename_is_one_id_with_dated_tickers_and_deduplicated_overlap(self):
        old, new = self.renamed_pair()
        result = self.reader.get_history("NEW")
        self.assertEqual(result["bar_count"], len(new))
        self.assertEqual(result["security"]["tickers"], ["NEW", "OLD"])
        self.assertEqual(result["bars"][0]["tickers"], ["OLD"])
        self.assertEqual(result["bars"][-1]["tickers"], ["NEW"])
        self.assertEqual(result["bars"], self.reader.get_history("OLD")["bars"])

    def test_established_overlap_policy_and_window_independent_selection(self):
        self.renamed_pair(differing=True)
        raw = self.reader.get_history("NEW", "raw")
        adjusted = self.reader.get_history("NEW", "all")
        self.assertEqual(raw["bars"][0]["close"], str(bar("2023-12-01")["c"]))
        self.assertEqual(adjusted["bars"][0]["close"], "200.0000000000000000001")
        narrow = self.reader.get_history("NEW", "all", start="2023-12-01", end="2023-12-01")
        self.assertEqual(narrow["bars"], adjusted["bars"][:1])
        both = self.reader.get_histories("NEW", ["raw", "all"], start="2023-12-01", end="2023-12-01")
        self.assertEqual(both["views"]["raw"]["bars"], raw["bars"][:1])
        self.assertEqual(both["views"]["all"]["bars"], adjusted["bars"][:1])

    def test_unsettled_overlap_fails_without_silently_choosing(self):
        self.renamed_pair(differing=True)
        # Remove the one historical-ticker preference in this synthetic
        # publication; raw prices now have no established selection rule.
        receipt, boundaries = self.publication("OLD")
        for boundary in boundaries.values():
            for entry in boundary["adjustments"]:
                for segment in entry["segments"]:
                    segment["historical_symbol"] = "FORMER"
        for kind, boundary in boundaries.items():
            receipt["boundaries"][kind]["sha256"] = storage.document_digest(boundary)
        storage.publish(
            self.store, self.root, storage.read_index(self.root), {"OLD": receipt}, {"OLD": boundaries}, {}
        )
        with self.assertRaisesRegex(StandaloneReaderError, "conflicting authenticated bars"):
            self.reader.get_history("NEW", "raw")

    def test_changed_related_input_blocks_whole_read(self):
        old, _ = self.renamed_pair()
        self.download("OLD", old, end="2026-10-15")
        with self.assertRaisesRegex(StandaloneReaderError, "changed pricing"):
            self.reader.get_history("NEW")

    def test_fb_meta_and_unrelated_reused_fb_use_real_pipeline(self):
        prices = {
            "META": calendar_bars("2021-07-01", "2026-10-14"),
            "FB": calendar_bars("2021-07-01", "2022-06-09") + calendar_bars("2025-06-26", "2026-10-14"),
            "METV": calendar_bars("2021-07-01", "2026-10-14"),
        }
        for symbol, rows in prices.items():
            self.download(symbol, rows)
        self.build(["META"], FakeIdentityClient())
        resolved = self.reader.resolve_ticker("FB")["matches"]
        self.assertEqual(
            {r["public_id"] for r in resolved},
            {security_id_for_alpaca_asset_ids(["meta-id"]), security_id_for_alpaca_asset_ids(["fb-etf-id"])},
        )
        with self.assertRaises(AmbiguousTickerError):
            self.reader.get_history("FB")
        # This fixture also contains the earlier META-labelled ETF that became
        # METV. The reader must not confuse that with FB -> META either.
        with self.assertRaises(AmbiguousTickerError):
            self.reader.get_history("META")
        meta = self.reader.get_history("META", public_id=security_id_for_alpaca_asset_ids(["meta-id"]))
        self.assertEqual(meta["bar_count"], len(prices["META"]))
        self.assertEqual(meta["security"]["tickers"], ["FB", "META"])
        fb = self.reader.get_history("FB", public_id=security_id_for_alpaca_asset_ids(["fb-etf-id"]))
        self.assertTrue(all(row["session"] >= "2025-06-26" for row in fb["bars"]))

    def start_lock_holder(self, path, directory):
        context = multiprocessing.get_context("fork")
        entered, release = context.Event(), context.Event()
        process = context.Process(target=hold_lock, args=(path, directory, entered, release))
        process.start()

        def cleanup():
            if process.is_alive():
                release.set()
            process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(5)

        self.addCleanup(cleanup)
        self.assertTrue(entered.wait(5))
        return context, process, release

    def test_nonblocking_lineage_and_pricing_locks_and_process_death(self):
        for path, directory in (
            (self.root, True),
            (self.store / "end_of_day/pricing/TEST/.download.lock", False),
        ):
            _, process, _ = self.start_lock_holder(path, directory)
            with self.assertRaises(ReaderBusyError):
                self.reader.get_history("TEST")
            process.terminate()
            process.join(5)
            self.assertEqual(self.reader.get_history("TEST")["bar_count"], len(self.bars))

    def test_wait_option_resumes_after_writer_releases(self):
        context, writer, release = self.start_lock_holder(self.root, True)
        started, complete, result = context.Event(), context.Event(), context.Queue()
        reader = context.Process(target=waiting_reader, args=(self.store, started, complete, result))
        reader.start()
        try:
            self.assertTrue(started.wait(5))
            self.assertFalse(complete.wait(0.1))
            release.set()
            self.assertTrue(complete.wait(5))
            self.assertEqual(result.get(timeout=2), len(self.bars))
        finally:
            release.set()
            reader.join(5)
            writer.join(5)
            if reader.is_alive():
                reader.terminate()
                reader.join(5)
            result.close()
            result.join_thread()

    def test_cli_json_csv_resolution_and_errors(self):
        common = ["--store", str(self.store), "-s", "TEST"]
        for options in ([], ["--resolve"], ["--format", "csv", "--from", "2026-10-13"]):
            out = io.StringIO()
            with redirect_stdout(out):
                self.assertEqual(main(common + options), 0)
            if "csv" in options:
                rows = list(csv.DictReader(io.StringIO(out.getvalue())))
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["volume"], "9007199254740993")
            else:
                self.assertIn("generation", json.loads(out.getvalue()))
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            main(common + ["--resolve", "--format", "csv"])
        self.assertEqual(error.exception.code, 2)
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            self.assertEqual(main(common + ["--public-id", "wrong"]), 1)
        self.assertEqual(out.getvalue(), "")

    def test_real_cli_needs_no_credentials_and_writes_no_files(self):
        before = snapshot(self.store)
        script = Path(__file__).resolve().parents[1] / "read_alpaca_pricing.py"
        result = subprocess.run(
            [sys.executable, "-B", str(script), "--store", str(self.store), "--symbol", "TEST"],
            env={key: value for key, value in os.environ.items() if not key.startswith("ALPACA_")},
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["bar_count"], len(self.bars))
        self.assertEqual(before, snapshot(self.store))

    def test_cli_multiple_repeated_types_json_csv_and_all_types(self):
        common = ["--store", str(self.store), "-s", "TEST", "--from", "2026-10-13"]
        for options, names in (
            (["--type", "raw", "all"], ["raw", "all"]),
            (["--type", "all", "--type", "split,dividend", "raw", "all"], ["all", "split,dividend", "raw"]),
            (["--all-types"], list(ADJUSTMENTS)),
        ):
            for format in ("json", "csv"):
                with self.subTest(options=options, format=format):
                    output = io.StringIO()
                    with redirect_stdout(output):
                        self.assertEqual(main(common + options + ["--format", format]), 0)
                    if format == "json":
                        result = json.loads(output.getvalue())
                        self.assertEqual(result["boundary"], "standalone_resolved_pricing_views")
                        self.assertEqual(list(result["views"]), names)
                        self.assertTrue(all(view["bar_count"] == 1 for view in result["views"].values()))
                    else:
                        rows = list(csv.DictReader(io.StringIO(output.getvalue())))
                        self.assertEqual([row["adjustment"] for row in rows], names)
                        self.assertTrue(all(row["volume"] == "9007199254740993" for row in rows))
                        self.assertTrue(all(row["session"] == "2026-10-13" for row in rows))

    def test_cli_duplicate_single_type_keeps_existing_output_shape(self):
        for format in ("json", "csv"):
            outputs = []
            for selection in ([], ["--type", "raw"], ["--type", "raw", "--type", "raw"]):
                output = io.StringIO()
                with redirect_stdout(output):
                    self.assertEqual(
                        main(["--store", str(self.store), "-s", "TEST", "--format", format] + selection), 0
                    )
                outputs.append(output.getvalue())
            self.assertEqual(outputs, [outputs[0]] * 3)

    def test_cli_rejects_conflicting_or_invalid_type_requests_without_output(self):
        for options in (
            ["--type", "raw", "--all-types"],
            ["--resolve", "--all-types"],
            ["--resolve", "--type", "raw", "all"],
            ["--type", "raw,all"],
            ["--type"],
        ):
            output = io.StringIO()
            with redirect_stdout(output), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main(["--store", str(self.store), "-s", "TEST"] + options)
            self.assertEqual(error.exception.code, 2)
            self.assertEqual(output.getvalue(), "")

    def test_cli_failure_in_later_view_never_prints_partial_json_or_csv(self):
        from alpaca_end_of_day import standalone

        original = standalone._history

        def fail_later(assignments, sources, store, public_id, adjustment, cache, retained=None):
            if adjustment == "all":
                raise StandaloneReaderError("conflicting adjusted history")
            return original(assignments, sources, store, public_id, adjustment, cache, retained)

        for format in ("json", "csv"):
            output, error = io.StringIO(), io.StringIO()
            with (
                patch.object(standalone, "_history", side_effect=fail_later),
                redirect_stdout(output),
                redirect_stderr(error),
            ):
                status = main(
                    ["--store", str(self.store), "-s", "TEST", "--type", "raw", "all", "--format", format]
                )
            self.assertEqual(status, 1)
            self.assertEqual(output.getvalue(), "")
            self.assertIn("conflicting adjusted history", error.getvalue())

    def test_busy_cli_returns_three_without_partial_output(self):
        self.start_lock_holder(self.root, True)
        output, error = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(error):
            status = main(["--store", str(self.store), "-s", "TEST", "--type", "raw", "all"])
        self.assertEqual(status, 3)
        self.assertEqual(output.getvalue(), "")
        self.assertIn("busy", error.getvalue())

import contextlib
import io
import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from cli_support import initialize_command_registration
from cli_support import pricing_main as main
from cli_support import pricing_parser as build_parser
from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import command_line
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.completion import COMPLETION_FILE, completed_batch, remember_batch, request_key
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.storage import initialize_store
from libs.alpaca_pricing_download.update_checks import CHECK_FILE, begin_checks, queue_check, read_checks


class PricingCliTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2026, 10, 14, 12, tzinfo=timezone.utc),
            )
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.store = self.directory / "store"
        self.client = FakeClient([bar("2016-01-04"), bar("2026-10-13")])
        initialize_command_registration(self)

    def invoke(self, *arguments):
        output, errors = io.StringIO(), io.StringIO()
        with (
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
            patch.object(command_line, "BarsClient", return_value=self.client),
        ):
            code = main(["--store", str(self.store), *arguments])
        return code, json.loads(output.getvalue()) if output.getvalue() else None, errors.getvalue()

    def test_repeated_symbols_are_processed_and_duplicates_normalized(self):
        code, result, _ = self.invoke(
            "--symbol", "goog", "--symbol", "AAPL", "--symbol", "GOOG", "--through", "2026-10-13"
        )
        self.assertEqual(code, 0)
        self.assertEqual(result["requested"], 2)
        self.assertEqual([value["symbol"] for value in result["results"]], ["GOOG", "AAPL"])
        self.assertTrue(
            all(value["request"]["end_exclusive"] == "2026-10-14T00:00:00Z" for value in result["results"])
        )

    def test_worker_option_defaults_to_three_and_refuses_outside_one_to_three(self):
        parser = build_parser()
        base = ["--symbol", "GOOG", "--store", str(self.store)]
        self.assertEqual(parser.parse_args(base).workers, 3)
        for value in ("0", "4", "-1", "many"):
            with (
                self.subTest(value=value),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                parser.parse_args([*base, "--workers", value])

    def test_worker_counts_are_bounded_and_share_one_client_with_ordered_results(self):
        symbols = ("GOOG", "AAPL", "MSFT", "AMZN", "NVDA", "META")
        arguments = [item for symbol in symbols for item in ("--symbol", symbol)]
        for count in (1, 2, 3):
            with self.subTest(workers=count):
                barrier = threading.Barrier(count, timeout=5)
                mutex = threading.Lock()
                active, peak = 0, 0
                clients = []

                def download(_store, symbol, _end, *, client, **_kwargs):
                    nonlocal active, peak
                    with mutex:
                        active += 1
                        peak = max(peak, active)
                        clients.append(client)
                    try:
                        barrier.wait()
                        return {"symbol": symbol, "mode": "full", "has_data": True, "changed": True}
                    finally:
                        with mutex:
                            active -= 1

                with (
                    patch.object(command_line, "download_pricing", side_effect=download),
                    patch.object(command_line, "completed_batch", return_value=False),
                ):
                    code, result, progress = self.invoke(*arguments, "--workers", str(count))
                self.assertEqual(code, 0)
                self.assertEqual(peak, count)
                self.assertEqual([value["symbol"] for value in result["results"]], list(symbols))
                self.assertTrue(all(client is self.client for client in clients))
                self.assertIn("[6/6]", progress)

    def test_one_worker_owns_every_request_for_its_ticker_through_publication(self):
        barrier = threading.Barrier(3, timeout=5)
        mutex = threading.Lock()
        owners, calls = {}, {}
        original = self.client.fetch

        def fetch(symbol, *args, **kwargs):
            with mutex:
                first = symbol not in owners
                owners.setdefault(symbol, set()).add(threading.get_ident())
                calls[symbol] = calls.get(symbol, 0) + 1
            if first:
                barrier.wait()
            return original(symbol, *args, **kwargs)

        with patch.object(self.client, "fetch", side_effect=fetch):
            code, result, _ = self.invoke(
                "--symbol", "GOOG", "--symbol", "AAPL", "--symbol", "MSFT", "--symbol", "GOOG"
            )
        self.assertEqual(code, 0)
        self.assertEqual(result["requested"], 3)
        self.assertTrue(all(len(threads) == 1 for threads in owners.values()))
        self.assertEqual(len(set.union(*owners.values())), 3)
        self.assertTrue(all(count == 16 for count in calls.values()))
        for symbol in owners:
            self.assertEqual(len(read_pricing_views(self.store, symbol)["raw"]), 2)
            notice = self.store / f"end_of_day/manifests/{symbol}/update.json"
            self.assertEqual(json.loads(notice.read_text()), {"from": "2016-01-04", "to": "2026-10-13"})

    def test_csv_and_line_file_inputs(self):
        path = self.directory / "tickers.csv"
        path.write_text("\ufeffGOOG,AAPL\nMSFT\nGOOG,\n\n")
        code, result, _ = self.invoke("--symbols-file", str(path))
        self.assertEqual(code, 0)
        self.assertEqual(result["requested"], 3)

    def test_all_selects_tracked_csv_without_acquiring_universe(self):
        args = build_parser().parse_args(["--recovery-universe", "--store", str(self.store)])
        from libs.alpaca_cli import symbols

        selected = symbols(args)
        self.assertGreaterEqual(len(selected), 18092)
        self.assertEqual(len(selected), len(set(selected)))
        self.assertIn("GOOG", selected)
        self.assertFalse((self.store / "end_of_day").exists())

    def test_from_conflict_is_only_that_ticker_and_force_cannot_bypass(self):
        download_pricing(self.store, "GOOG", "2026-10-14", client=self.client)
        before = snapshot(self.store / "end_of_day/pricing/GOOG")
        path = self.directory / "tickers.txt"
        path.write_text("GOOG\nAAPL\n")
        code, result, _ = self.invoke("--symbols-file", str(path), "--from", "2020-01-01")
        self.assertEqual(code, 1)
        self.assertEqual((result["succeeded"], result["failed"]), (1, 1))
        self.assertIn("exclude previously stored", result["results"][0]["error"])
        self.assertEqual(snapshot(self.store / "end_of_day/pricing/GOOG"), before)
        self.assertEqual(len(read_pricing_views(self.store, "AAPL")["raw"]), 1)

    def test_invalid_ticker_and_download_failure_do_not_stop_remaining(self):
        original = self.client.fetch

        def fails(symbol, *args, **kwargs):
            if symbol == "BROKEN":
                raise OSError("synthetic transport failure")
            return original(symbol, *args, **kwargs)

        with patch.object(self.client, "fetch", side_effect=fails):
            code, result, _ = self.invoke("--symbol", "BAD/TICKER", "--symbol", "BROKEN", "--symbol", "GOOG")
        self.assertEqual(code, 1)
        self.assertEqual([value["status"] for value in result["results"]], ["failed", "failed", "updated"])
        self.assertFalse((self.store / COMPLETION_FILE).exists())

    def test_successful_batch_repeats_without_individual_receipts_or_provider_calls(self):
        self.assertEqual(self.invoke("--symbol", "GOOG", "--symbol", "MSFT")[0], 0)
        before = snapshot(self.store)
        with (
            patch.object(command_line, "download_pricing", side_effect=AssertionError("individual receipt")),
            patch.object(self.client, "fetch", side_effect=AssertionError("provider request")),
        ):
            for _ in range(4):
                code, result, _ = self.invoke("--symbol", "GOOG", "--symbol", "MSFT")
                self.assertEqual(code, 0)
                self.assertTrue(all(item["status"] == "unchanged" for item in result["results"]))
        self.assertEqual(before, snapshot(self.store))

    def test_deleting_either_cache_falls_back_to_completed_source_receipts(self):
        self.assertEqual(self.invoke("--symbol", "GOOG")[0], 0)
        before = snapshot(self.store / "end_of_day")
        for filename in (COMPLETION_FILE, CHECK_FILE):
            with self.subTest(filename=filename):
                (self.store / filename).unlink()
                with (
                    patch.object(command_line, "download_pricing", wraps=download_pricing) as download,
                    patch.object(self.client, "fetch", side_effect=AssertionError("provider request")),
                ):
                    self.assertEqual(self.invoke("--symbol", "GOOG")[0], 0)
                self.assertEqual(download.call_count, 1)
                self.assertTrue((self.store / COMPLETION_FILE).exists())
                self.assertEqual(before, snapshot(self.store / "end_of_day"))
        self.assertFalse(read_checks(self.store)["complete"])

    def test_advanced_boundary_and_force_bypass_completed_batch(self):
        self.assertEqual(self.invoke("--symbol", "GOOG")[0], 0)
        self.client.calls.clear()
        with patch(
            "libs.alpaca_pricing_download.download.utc_now",
            return_value=datetime(2026, 10, 15, 12, tzinfo=timezone.utc),
        ):
            code, result, _ = self.invoke("--symbol", "GOOG")
            self.assertEqual(code, 0)
            self.assertEqual(result["results"][0]["consistency_check"], "matched")
            self.assertEqual(len(self.client.calls), 32)
            self.client.calls.clear()
            result = download_pricing(self.store, "GOOG", "2026-10-15", client=self.client, refresh_all=True)
            self.assertEqual(result["mode"], "full_refresh")
            self.assertTrue(self.client.calls)

    def test_other_publisher_invalidates_memo_and_prevents_claiming_batch_completion(self):
        self.assertEqual(self.invoke("--symbol", "GOOG")[0], 0)
        request = request_key(["GOOG"], "2026-10-14", None)
        self.assertTrue(completed_batch(self.store, request))
        before = read_checks(self.store, ())
        own = queue_check(self.store, "GOOG")
        queue_check(self.store, "OTHER")
        self.assertFalse(completed_batch(self.store, request))
        remember_batch(self.store, request, before, [own])
        self.assertFalse(completed_batch(self.store, request))
        self.assertEqual(self.invoke("--symbol", "GOOG")[0], 0)
        self.assertTrue(completed_batch(self.store, request))

    def test_interrupted_force_invalidates_memo_before_recovery_journal_is_installed(self):
        self.assertEqual(self.invoke("--symbol", "GOOG")[0], 0)
        request = request_key(["GOOG"], "2026-10-14", None)
        self.client.bars = [bar("2016-01-04", "50"), bar("2026-10-13", "50")]
        with patch.object(pricing_storage, "_finish_publication", side_effect=OSError("stopped before install")):
            with self.assertRaises(OSError):
                download_pricing(self.store, "GOOG", "2026-10-14", client=self.client, refresh_all=True)
        self.assertTrue((self.store / "end_of_day/pricing/GOOG/.pending.json").exists())
        self.assertFalse(completed_batch(self.store, request))
        with patch.object(self.client, "fetch", side_effect=AssertionError("recovery reacquired pricing")):
            code, result, _ = self.invoke("--symbol", "GOOG")
        self.assertEqual(code, 0)
        self.assertEqual(result["results"][0]["status"], "recovered")
        self.assertTrue(completed_batch(self.store, request))

    def test_large_completed_batch_uses_shared_cache_without_opening_ticker_receipts(self):
        initialize_store(self.store)
        before = begin_checks(self.store)
        symbols = [f"T{number}" for number in range(6000)]
        request = request_key(symbols, "2026-10-14", None)
        remember_batch(self.store, request, before, [])
        with (
            patch.object(command_line, "selected_symbols", return_value=symbols),
            patch.object(command_line, "download_pricing", side_effect=AssertionError("individual receipt")),
        ):
            code, result, _ = self.invoke("--recovery-universe")
        self.assertEqual(code, 0)
        self.assertEqual(result["succeeded"], 6000)

    def test_selection_and_end_modes_are_mutually_exclusive(self):
        for args in (
            ["--symbol", "GOOG", "--recovery-universe"],
            ["--recovery-universe", "--symbols-file", "a"],
            ["--symbol", "GOOG", "--through", "2026-10-13", "--request-end", "2026-10-14"],
        ):
            with (
                self.subTest(args=args),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                build_parser().parse_args(["--store", str(self.store), *args])

    def test_malformed_source_receipt_is_a_ticker_failure_not_batch_abort(self):
        self.invoke("--symbol", "GOOG")
        path = self.store / "end_of_day/pricing/GOOG/source.manifest.json"
        source = json.loads(path.read_text())
        source.pop("latest_request")
        path.write_text(json.dumps(source))
        code, result, _ = self.invoke("--symbol", "GOOG", "--symbol", "MSFT")
        self.assertEqual(code, 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][1]["status"], "updated")

    def test_bad_global_input_fails_without_provider_calls(self):
        before = snapshot(self.store)
        empty = self.directory / "empty.txt"
        empty.write_text("")
        for args in (
            ("--symbols-file", str(empty)),
            ("--symbols-file", str(empty) + ".missing"),
            ("--symbol", "GOOG", "--through", "2026-10-14"),
            ("--symbol", "GOOG", "--from", "20261013"),
        ):
            with self.subTest(args=args):
                code, result, _ = self.invoke(*args)
                self.assertEqual(code, 1)
                self.assertIsNone(result)
        self.assertFalse(self.client.calls)
        self.assertEqual(snapshot(self.store), before)

    def test_obsolete_end_and_refresh_aliases_are_rejected(self):
        for flag in ("--request-end", "--refresh-all", "--force-download", "--all", "-s"):
            with (
                self.subTest(flag=flag),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                self.invoke("--symbol", "GOOG", flag)
        self.assertFalse(self.client.calls)

    def test_empty_and_unchanged_results_are_reported(self):
        self.client.bars = []
        code, result, _ = self.invoke("--symbol", "GOOG")
        self.assertEqual(code, 0)
        self.assertEqual(result["results"][0]["status"], "empty")
        self.client.bars = [bar("2026-10-13")]
        download_pricing(self.store, "GOOG", "2026-10-14", client=self.client, refresh_all=True)
        _, result, _ = self.invoke("--symbol", "GOOG")
        self.assertEqual(result["results"][0]["status"], "unchanged")

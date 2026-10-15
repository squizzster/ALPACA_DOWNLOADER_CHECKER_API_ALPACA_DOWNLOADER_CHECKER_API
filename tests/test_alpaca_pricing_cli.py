import contextlib
import io
import json
import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import command_line
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views


class PricingCliTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch("libs.alpaca_pricing_download.download.utc_today", return_value=date(2026, 10, 14))
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.store = self.directory / "store"
        self.client = FakeClient([bar("2016-01-04"), bar("2026-10-13")])

    def invoke(self, *arguments):
        output, errors = io.StringIO(), io.StringIO()
        with (
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(errors),
            patch.object(command_line, "BarsClient", return_value=self.client),
        ):
            code = command_line.main(["--store", str(self.store), *arguments])
        return code, json.loads(output.getvalue()) if output.getvalue() else None, errors.getvalue()

    def test_repeated_symbols_are_processed_and_duplicates_normalized(self):
        code, result, _ = self.invoke("-s", "goog", "--symbol", "AAPL", "-s", "GOOG", "--to", "2026-10-13")
        self.assertEqual(code, 0)
        self.assertEqual(result["requested"], 2)
        self.assertEqual([value["symbol"] for value in result["results"]], ["GOOG", "AAPL"])
        self.assertTrue(
            all(value["request"]["end_exclusive"] == "2026-10-14T00:00:00Z" for value in result["results"])
        )

    def test_worker_option_defaults_to_three_and_refuses_outside_one_to_three(self):
        parser = command_line.build_parser()
        base = ["-s", "GOOG", "--store", str(self.store)]
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
        arguments = [item for symbol in symbols for item in ("-s", symbol)]
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

                with patch.object(command_line, "download_pricing", side_effect=download):
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
            code, result, _ = self.invoke("-s", "GOOG", "-s", "AAPL", "-s", "MSFT", "-s", "GOOG")
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
        code, result, _ = self.invoke("--file", str(path))
        self.assertEqual(code, 0)
        self.assertEqual(result["requested"], 3)

    def test_all_selects_tracked_csv_without_acquiring_universe(self):
        args = command_line.build_parser().parse_args(["--all", "--store", str(self.store)])
        selected = command_line.selected_symbols(args)
        self.assertEqual(len(selected), 18092)
        self.assertEqual(len(selected), len(set(selected)))
        self.assertIn("GOOG", selected)
        self.assertFalse(self.store.exists())

    def test_from_conflict_is_only_that_ticker_and_force_cannot_bypass(self):
        download_pricing(self.store, "GOOG", "2026-10-14", client=self.client)
        before = snapshot(self.store / "end_of_day/pricing/GOOG")
        path = self.directory / "tickers.txt"
        path.write_text("GOOG\nAAPL\n")
        code, result, _ = self.invoke("--file", str(path), "--from", "2020-01-01", "--force-download")
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
            code, result, _ = self.invoke("-s", "BAD/TICKER", "-s", "BROKEN", "-s", "GOOG")
        self.assertEqual(code, 1)
        self.assertEqual([value["status"] for value in result["results"]], ["failed", "failed", "updated"])

    def test_selection_and_end_modes_are_mutually_exclusive(self):
        for args in (
            ["-s", "GOOG", "--all"],
            ["--all", "--file", "a"],
            ["-s", "GOOG", "--to", "2026-10-13", "--request-end", "2026-10-14"],
        ):
            with (
                self.subTest(args=args),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                command_line.build_parser().parse_args(["--store", str(self.store), *args])

    def test_malformed_source_receipt_is_a_ticker_failure_not_batch_abort(self):
        self.invoke("-s", "GOOG")
        path = self.store / "end_of_day/pricing/GOOG/source.manifest.json"
        source = json.loads(path.read_text())
        source.pop("latest_request")
        path.write_text(json.dumps(source))
        code, result, _ = self.invoke("-s", "GOOG", "-s", "MSFT")
        self.assertEqual(code, 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][1]["status"], "updated")

    def test_bad_global_input_fails_without_provider_calls(self):
        empty = self.directory / "empty.txt"
        empty.write_text("")
        for args in (
            ("--file", str(empty)),
            ("--file", str(empty) + ".missing"),
            ("-s", "GOOG", "--to", "2026-10-14"),
            ("-s", "GOOG", "--from", "20261013"),
        ):
            with self.subTest(args=args):
                code, result, _ = self.invoke(*args)
                self.assertEqual(code, 1)
                self.assertIsNone(result)
        self.assertFalse(self.client.calls)
        self.assertFalse(self.store.exists())

    def test_legacy_end_and_refresh_options_remain_supported(self):
        code, result, _ = self.invoke("-s", "GOOG", "--request-end", "2026-10-14", "--refresh-all")
        self.assertEqual(code, 0)
        self.assertEqual(result["results"][0]["request"]["end_exclusive"], "2026-10-14T00:00:00Z")

    def test_empty_and_unchanged_results_are_reported(self):
        self.client.bars = []
        code, result, _ = self.invoke("-s", "GOOG")
        self.assertEqual(code, 0)
        self.assertEqual(result["results"][0]["status"], "empty")
        self.client.bars = [bar("2026-10-13")]
        self.invoke("-s", "GOOG")
        _, result, _ = self.invoke("-s", "GOOG")
        self.assertEqual(result["results"][0]["status"], "unchanged")

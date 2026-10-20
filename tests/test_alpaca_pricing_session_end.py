import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from cli_support import pricing_main as main
from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_pricing_download import command_line
from libs.alpaca_pricing_download.contract import DownloadError, timestamp
from libs.alpaca_pricing_download.download import download_pricing, request_end


class SessionEndTests(unittest.TestCase):
    def test_automatic_end_waits_for_new_york_midnight_and_skips_closed_dates(self):
        cases = (
            ("2026-10-18T23:36:00Z", "2026-10-16"),  # Sunday: Friday is complete.
            ("2026-10-16T23:36:00Z", "2026-10-15"),  # Friday after close is still Friday.
            ("2026-10-17T00:30:00Z", "2026-10-15"),  # Crossing UTC midnight changes nothing.
            ("2026-10-17T03:59:59Z", "2026-10-15"),
            ("2026-10-17T04:00:00Z", "2026-10-16"),
            ("2026-10-19T20:30:00Z", "2026-10-16"),  # Monday after close is not complete.
            ("2026-09-08T04:00:00Z", "2026-09-04"),  # Labor Day holiday.
            ("2026-11-27T23:59:00Z", "2026-11-25"),  # Early close does not bypass midnight.
            ("2026-11-28T05:00:00Z", "2026-11-27"),
        )
        for instant, expected_session in cases:
            with self.subTest(instant=instant):
                end = request_end(None, now=timestamp(instant))
                self.assertEqual(
                    (end.date().toordinal() - 1), datetime.fromisoformat(expected_session).toordinal()
                )

    def test_month_year_and_daylight_saving_boundaries(self):
        cases = (
            ("2026-09-01T00:30:00Z", "2026-08-29T00:00:00Z"),
            ("2026-09-01T04:00:00Z", "2026-09-01T00:00:00Z"),
            ("2026-09-02T04:00:00Z", "2026-09-02T00:00:00Z"),
            ("2027-01-01T04:59:59Z", "2026-12-31T00:00:00Z"),
            ("2027-01-01T05:00:00Z", "2027-01-01T00:00:00Z"),
            ("2026-03-07T04:59:59Z", "2026-03-06T00:00:00Z"),
            ("2026-03-07T05:00:00Z", "2026-03-07T00:00:00Z"),
            ("2026-03-10T03:59:59Z", "2026-03-07T00:00:00Z"),
            ("2026-03-10T04:00:00Z", "2026-03-10T00:00:00Z"),
            ("2026-11-07T04:59:59Z", "2026-11-06T00:00:00Z"),
            ("2026-11-07T05:00:00Z", "2026-11-07T00:00:00Z"),
        )
        for instant, expected in cases:
            with self.subTest(instant=instant):
                self.assertEqual(request_end(None, now=timestamp(instant)), timestamp(expected))

    def test_explicit_ends_cannot_bypass_midnight(self):
        for instant, accepted in (
            ("2026-10-17T00:30:00Z", False),
            ("2026-10-17T03:59:59Z", False),
            ("2026-10-17T04:00:00Z", True),
        ):
            with self.subTest(instant=instant):
                if accepted:
                    self.assertEqual(
                        request_end("2026-10-17", now=timestamp(instant)), timestamp("2026-10-17T00:00:00Z")
                    )
                else:
                    with self.assertRaisesRegex(DownloadError, "New York"):
                        request_end("2026-10-17", now=timestamp(instant))
        # Explicit completed non-session boundaries retain their existing meaning.
        self.assertEqual(
            request_end("2026-10-18", now=timestamp("2026-10-18T23:36:00Z")), timestamp("2026-10-18T00:00:00Z")
        )

    def test_no_guess_when_calendar_is_out_of_range_or_clock_is_naive(self):
        for instant in ("2016-01-01T12:00:00Z", "2100-01-01T12:00:00Z"):
            with self.subTest(instant=instant), self.assertRaises(DownloadError):
                request_end(None, now=timestamp(instant))
        with self.assertRaisesRegex(DownloadError, "timezone"):
            request_end(None, now=datetime(2026, 10, 18))

    def test_direct_download_defaults_to_completed_session(self):
        client = FakeClient([bar("2026-10-15"), bar("2026-10-16")])
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "libs.alpaca_pricing_download.download.utc_now", return_value=timestamp("2026-10-17T00:30:00Z")
            ),
        ):
            result = download_pricing(Path(temporary) / "store", "DELL", None, client=client)
            self.assertEqual(result["request"]["end_exclusive"], "2026-10-16T00:00:00Z")
            self.assertEqual(result["request"]["asof"], "2026-10-16")

    def test_cli_freezes_all_tickers_across_new_york_midnight(self):
        moments = iter([timestamp("2026-10-17T03:59:59Z")])
        client = FakeClient([bar("2026-10-15"), bar("2026-10-16")])
        output = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as temporary,
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                side_effect=lambda: next(moments, timestamp("2026-10-17T04:00:01Z")),
            ),
            patch.object(command_line, "BarsClient", return_value=client),
            contextlib.redirect_stdout(output),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            from libs import alpaca_cli
            from libs.alpaca_lineage_build.universe_update import rebuild_universe

            rebuild_universe(Path(temporary) / "store")
            registry = Path(temporary) / "registry.csv"
            registry.write_text("HIST\n")
            self.enterContext(patch.object(alpaca_cli, "DEFAULT_REGISTRY", registry))
            code = main(
                [
                    "--store",
                    str(Path(temporary) / "store"),
                    "--symbol",
                    "DELL",
                    "--symbol",
                    "HPE",
                    "--symbol",
                    "SPCX",
                ]
            )
            result = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(len(result["results"]), 3)
        self.assertTrue(
            all(row["request"]["end_exclusive"] == "2026-10-16T00:00:00Z" for row in result["results"])
        )
        self.assertEqual(set(client.asofs), {"2026-10-16"})

    def test_cli_rejects_incomplete_explicit_date_before_acquisition(self):
        for option, value in (("--through", "2026-10-16"), ("--through", "2026-10-17")):
            with (
                self.subTest(option=option),
                tempfile.TemporaryDirectory() as temporary,
                patch(
                    "libs.alpaca_pricing_download.download.utc_now",
                    return_value=timestamp("2026-10-17T00:30:00Z"),
                ),
                patch.object(
                    command_line, "BarsClient", side_effect=AssertionError("must reject before client creation")
                ),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                store = Path(temporary) / "store"
                self.assertEqual(main(["--store", str(store), "--symbol", "DELL", option, value]), 1)
                self.assertFalse(store.exists())

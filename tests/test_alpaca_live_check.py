import copy
import io
import json
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_pricing_download import live_check
from libs.alpaca_pricing_download.contract import FIELDS


def bar(day="2026-10-14", **changes):
    return {
        "t": day + "T04:00:00Z",
        "o": Decimal("123.123456789012345678901234567890"),
        "h": Decimal("124"),
        "l": Decimal("122"),
        "c": Decimal("123"),
        "v": 9007199254740993,
        "n": 7,
        "vw": Decimal("123"),
        **changes,
    }


def entry(symbol, day="2026-10-14", asof="2026-10-15"):
    values = bar(day)
    return {"symbol": symbol, "asof": asof, "last_all": [str(values[key]) for key in FIELDS]}


def snapshot(*entries):
    return {"entries": list(entries), "sha256": "a" * 64, "token": ("epoch", 1)}


class LiveCheckTests(unittest.TestCase):
    def test_plan_batches_exact_dates_and_saved_identity_mapping(self):
        value = snapshot(
            entry("C"), entry("A"), entry("B"), entry("D", asof="2026-10-17"), entry("E", "2026-10-13")
        )
        original = copy.deepcopy(value)
        actions = live_check.plan(value, batch_size=2)["steps"][0]["actions"]
        seen = {}
        for action in actions:
            symbols = action["request"]["params"]["symbols"].split(",")
            self.assertLessEqual(len(symbols), 2)
            seen.update({symbol: action["request"]["params"]["asof"] for symbol in symbols})
        self.assertEqual(seen, {symbol: "2026-10-17" if symbol == "D" else "2026-10-15" for symbol in "ABCDE"})
        self.assertEqual(value, original)
        with self.assertRaises(ValueError):
            live_check.plan(value, batch_size=0)

    def test_exact_comparisons_classification_missing_and_no_filesystem_access(self):
        value = snapshot(*(entry(symbol) for symbol in ("A", "B", "C", "D")))
        response = {
            "bars": {
                "A": [bar(t="2026-10-14T00:00:00-04:00", c=Decimal("123.000"))],
                "B": [bar(o=Decimal("125"))],
                "C": [bar(v=9007199254740994)],
            }
        }
        calls = []

        def get_page(parameters):
            calls.append(parameters["symbols"])
            if parameters["symbols"] == "D":
                return {"bars": {}}
            return response

        with (
            patch("builtins.open", side_effect=AssertionError("file access")),
            patch.object(Path, "open", side_effect=AssertionError("file access")),
            patch.object(Path, "iterdir", side_effect=AssertionError("directory scan")),
        ):
            result = live_check.verify(value, get_page)
        self.assertEqual(calls, ["A,B,C,D"])
        self.assertEqual(
            result["stats"],
            {
                "tickers": 4,
                "last_session_dates": 1,
                "checked": 4,
                "matched": 1,
                "differed": 2,
                "ohlc_changed": 1,
                "other_fields_only": 1,
                "missing": 1,
                "failed": 0,
                "request_failures": 0,
            },
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["missing"], ["D"])
        self.assertEqual([item["symbol"] for item in result["differences"]], ["B", "C"])
        self.assertEqual(
            result["differences"][1]["fields"],
            {"volume": {"stored": "9007199254740993", "live": "9007199254740994"}},
        )
        self.assertEqual(result["baseline_sha256"], value["sha256"])

    def test_underfilled_pages_are_exhausted_and_request_contract_preserved(self):
        calls = []

        def get_page(parameters):
            calls.append(parameters)
            if len(calls) == 1:
                return {"bars": {"A": [bar()]}, "next_page_token": "next"}
            return {"bars": {"B": [bar()]}, "next_page_token": None}

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertTrue(result["complete"])
        self.assertEqual(result["stats"]["matched"], 2)
        self.assertEqual(result["requests"]["page_requests"], 2)
        self.assertNotIn("page_token", calls[0])
        self.assertEqual(calls[1]["page_token"], "next")
        self.assertEqual(
            calls[0],
            {
                "symbols": "A,B",
                "timeframe": "1Day",
                "feed": "sip",
                "currency": "USD",
                "start": "2026-10-14T04:00:00Z",
                "end": "2026-10-14T23:59:59.999999Z",
                "asof": "2026-10-15",
                "adjustment": "all",
                "limit": 10000,
                "sort": "asc",
            },
        )

    def test_repeated_token_or_invalid_response_fails_whole_batch(self):
        for response in (
            {"bars": {}, "next_page_token": "repeated"},
            {"bars": {"UNREQUESTED": []}},
            {"bars": {"A": [bar("2026-10-13")]}},
            {"bars": {"A": [bar(), bar()]}},
            {"bars": {"A": "invalid"}},
        ):
            with self.subTest(response=response):
                result = live_check.verify(snapshot(entry("A")), lambda parameters: response)
                self.assertFalse(result["complete"])
                self.assertEqual(result["stats"]["failed"], 1)
                self.assertEqual(result["stats"]["checked"], 0)
                self.assertEqual(result["failures"][0]["symbols"], ["A"])

    def test_multiple_distinct_daily_rows_are_not_compared_as_one(self):
        result = live_check.verify(
            snapshot(entry("A")), lambda parameters: {"bars": {"A": [bar(), bar(t="2026-10-14T05:00:00Z")]}}
        )
        self.assertEqual(result["stats"]["failed"], 1)
        self.assertEqual(result["stats"]["matched"], 0)
        self.assertIn("multiple bars", result["failures"][0]["reason"])

    def test_uri_too_long_splits_only_affected_batch(self):
        def get_page(parameters):
            if "," in parameters["symbols"]:
                error = urllib.error.HTTPError("https://example.test", 414, "too long", {}, None)
                raise live_check.DownloadError("request failed") from error
            return {"bars": {parameters["symbols"]: [bar()]}}

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertTrue(result["complete"])
        self.assertEqual(result["stats"]["matched"], 2)
        self.assertEqual(result["requests"], {"planned_batches": 1, "page_requests": 3, "oversize_splits": 1})

    def test_terminal_http_error_does_not_split_or_claim_missing(self):
        def get_page(parameters):
            raise urllib.error.HTTPError("https://example.test", 400, "bad request", {}, None)

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertEqual(result["stats"]["failed"], 2)
        self.assertEqual(result["stats"]["missing"], 0)
        self.assertEqual(result["requests"]["page_requests"], 1)
        self.assertEqual(
            result["failures"],
            [
                {
                    "symbols": ["A", "B"],
                    "reason": "HTTP 400",
                    "outcome": {
                        "outcome": "request_failed",
                        "http_status": 400,
                        "error": "HTTP Error 400: bad request",
                    },
                }
            ],
        )

    def test_canonical_check_loads_snapshot_once_without_publishing(self):
        from libs.alpaca_cli import main
        from libs.alpaca_pricing_download import check_summary

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            accepted = snapshot(entry("A"))
            accepted["token"] = ("epoch", 1)
            output = io.StringIO()
            with (
                patch.object(check_summary, "read_snapshot", return_value=accepted) as read,
                patch.object(check_summary, "assert_current"),
                patch("libs.alpaca_lineage_build.registration.command_policy") as policy,
                patch("libs.alpaca_cli.clients") as clients,
                redirect_stdout(output),
            ):
                clients.return_value = (unittest.mock.Mock(), unittest.mock.Mock())
                clients.return_value[0]._get.return_value = {"bars": {"A": [bar()]}}
                self.assertEqual(main(["pricing", "check", "--store", str(root / "store")]), 0)
                read.assert_called_once_with(root / "store")
                policy.return_value.check.assert_called_once()
            self.assertEqual(json.loads(output.getvalue())["stats"]["matched"], 1)
            self.assertEqual(list(root.iterdir()), [])

    def test_live_check_preserves_experiment_boundary_and_version(self):
        live_check.validate_version()
        result = live_check.verify(snapshot(entry("A")), lambda parameters: {"bars": {"A": [bar()]}})
        self.assertEqual(result["boundary"], "alpaca_last_session_live_check_experiment")
        self.assertEqual(result["schema_version"], live_check.VERSION)


if __name__ == "__main__":
    unittest.main()

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

from test_alpaca_pricing_download import single_session_request

from libs.alpaca_pricing_download import live_check
from libs.alpaca_pricing_download.contract import FIELDS
from libs.common.debug_trace import DebugTrace


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
    return {
        "symbol": symbol,
        "asof": asof,
        "end_exclusive": "2026-10-15T00:00:00Z",
        "last_all": [str(values[key]) for key in FIELDS],
        "first_all": [str(values[key]) for key in FIELDS],
        "tail_all": [[str(values[key]) for key in FIELDS]],
        "tail_version": 32,
    }


def snapshot(*entries):
    # These transport fixtures contain one or two stored rows. Publish their
    # complete short tail, including tests that replace the first observation.
    entries = copy.deepcopy(list(entries))
    for value in entries:
        value["tail_all"] = (
            [value["first_all"]]
            if value["first_all"] == value["last_all"]
            else [value["first_all"], value["last_all"]]
        )
    return {"entries": entries, "sha256": "a" * 64, "token": ("epoch", 1)}


class LiveCheckTests(unittest.TestCase):
    def test_debug_names_each_missing_endpoint_without_truncating_the_roster(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "trace.jsonl"
            trace = DebugTrace(path)
            try:
                with trace.activate():
                    result = live_check.verify(
                        snapshot(*(entry(f"S{i}") for i in range(12))), lambda params: {"bars": {}}
                    )
            finally:
                trace.close("complete", 0)
            events = [json.loads(line) for line in path.read_text().splitlines()]
            missing = [item["details"] for item in events if item["stage"] == "missing_history_endpoint"]
            self.assertEqual(sorted(missing, key=lambda item: item["symbol"]), result["missing_endpoints"])
            self.assertEqual(len(missing), 12)

    def test_414_singleton_empty_result_does_not_trigger_another_probe(self):
        calls = []

        def get_page(params):
            calls.append(dict(params))
            if "," in params["symbols"]:
                raise urllib.error.HTTPError("https://example.test", 414, "too long", {}, None)
            return {"bars": {}}

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertEqual([p["symbols"] for p in calls], ["A,B", "A", "B"])
        self.assertEqual(result["missing"], ["A", "B"])
        self.assertNotIn("omission_probes", result["requests"])

    def test_omitted_endpoint_confirmation_exhausts_pages_and_compares_changed_value(self):
        calls = []

        def get_page(params):
            calls.append(dict(params))
            if params["symbols"] == "A,B":
                return {"bars": {"B": [bar()]}}
            if "page_token" not in params:
                return {"bars": {}, "next_page_token": "last"}
            return {"bars": {"A": [bar(c=Decimal("99"))]}, "next_page_token": None}

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertTrue(result["complete"])
        self.assertEqual(result["matched"], ["B"])
        self.assertEqual(result["stats"]["differed"], 1)
        self.assertEqual(result["differences"][0]["fields"]["close"]["live"], "99")
        self.assertEqual([p["symbols"] for p in calls], ["A,B", "A", "A"])
        self.assertEqual(result["requests"]["page_requests"], 3)

    def test_batch_omission_probes_only_the_missing_known_endpoint(self):
        a, b = entry("A"), entry("B")
        a["first_all"] = b["first_all"] = entry("X", "2016-07-05")["last_all"]
        calls = []

        def get_page(params):
            calls.append(dict(params))
            symbols = params["symbols"].split(",")
            day = params["start"][:10]
            return {
                "bars": {
                    s: [bar(day)] for s in symbols if not (s == "A" and len(symbols) > 1 and day == "2016-07-05")
                }
            }

        result = live_check.verify(snapshot(a, b), get_page)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["matched"], ["A", "B"])
        self.assertEqual(len(calls), 3, calls)
        self.assertEqual(calls[-1], {**calls[0], "symbols": "A"})
        self.assertEqual(result["requests"]["omission_probes"], 1)
        self.assertEqual(result["requests"]["omission_recovered"], 1)

    def test_omission_confirmation_is_bounded_and_reports_exact_missing_endpoint(self):
        calls = []

        def get_page(params):
            calls.append(dict(params))
            return {"bars": {}}

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertFalse(result["complete"])
        self.assertEqual([p["symbols"] for p in calls], ["A,B", "A", "B"])
        self.assertEqual(result["matched"], [])
        self.assertEqual(
            result["missing_endpoints"],
            [
                {"symbol": s, "endpoint": "first_and_last", "session": "2026-10-14", "asof": "2026-10-15"}
                for s in ("A", "B")
            ],
        )

    def test_failed_omission_confirmation_does_not_discard_a_matching_peer(self):
        for failure in ("http", "pagination", "unexpected_symbol", "duplicate"):
            with self.subTest(failure=failure):
                calls = []

                def get_page(params):
                    calls.append(dict(params))
                    if params["symbols"] == "A,B":
                        return {"bars": {"A": [bar()]}}
                    if failure == "http":
                        raise urllib.error.HTTPError("https://example.test", 400, "bad request", {}, None)
                    if failure == "pagination":
                        return {"bars": {}, "next_page_token": "same"}
                    if failure == "unexpected_symbol":
                        return {"bars": {"A": [bar()]}}
                    return {"bars": {"B": [bar(), bar()]}}

                result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
                self.assertEqual(result["matched"], ["A"])
                self.assertEqual(result["stats"]["failed"], 1)
                self.assertEqual(result["missing"], [])
                self.assertEqual(result["failures"][0]["symbols"], ["B"])
                self.assertTrue(all(p["symbols"] == "B" for p in calls[1:]))

    def test_failed_batch_is_not_reinterpreted_as_an_omission(self):
        calls = []

        def get_page(params):
            calls.append(dict(params))
            return {"bars": {}, "next_page_token": "same"}

        result = live_check.verify(snapshot(entry("A"), entry("B")), get_page)
        self.assertEqual(result["stats"]["failed"], 2)
        self.assertEqual([p["symbols"] for p in calls], ["A,B", "A,B"])

    def test_first_last_windows_stay_exact_and_multiple_differences_count_one_symbol(self):
        a, b = entry("A"), entry("B")
        a["first_all"] = b["first_all"] = entry("X", "2016-07-05")["last_all"]
        calls = []

        def get_page(params):
            calls.append(params)
            day = params["start"][:10]
            return {"bars": {"A": [bar(day, c=Decimal("99"))], "B": [bar(day)]}}

        result = live_check.verify(snapshot(a, b), get_page)
        self.assertTrue(result["complete"])
        self.assertEqual(result["stats"]["checked"], 2)
        self.assertEqual(result["stats"]["matched"], 1)
        self.assertEqual(result["stats"]["differed"], 1)
        self.assertEqual({item["endpoint"] for item in result["differences"]}, {"first", "last"})
        self.assertEqual(len(calls), 2)
        self.assertEqual({p["start"][:10] for p in calls}, {"2016-07-05", "2026-10-14"})
        self.assertTrue(all(single_session_request(p) and p["adjustment"] == "all" for p in calls))

    def test_missing_first_endpoint_cannot_approve_matching_last(self):
        value = entry("A")
        value["first_all"] = entry("A", "2016-07-05")["last_all"]
        result = live_check.verify(
            snapshot(value),
            lambda params: {"bars": {"A": [] if params["start"][:10] == "2016-07-05" else [bar()]}},
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["stats"]["matched"], 0)
        self.assertEqual(result["missing"], ["A"])

    def test_full_new_york_session_is_used_for_first_and_cutoff_caps_tail(self):
        from libs.alpaca_pricing_download.contract import timestamp

        value = entry("A")
        value["first_all"] = entry("A", "2016-07-05")["last_all"]
        calls = []

        def page(params):
            calls.append(params)
            day = params["start"][:10]
            price = "123" if day != "2016-07-05" or params["end"].startswith("2016-07-06T03:59:59") else "99"
            return {"bars": {"A": [bar(day, c=Decimal(price))]}}

        result = live_check.verify(snapshot(value), page)
        self.assertEqual(result["stats"]["matched"], 1)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(timestamp(p["end"]) < timestamp(value["end_exclusive"]) for p in calls))

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

    def test_historical_identity_mapping_does_not_bound_the_observation_window(self):
        value = snapshot(entry("OLD", day="2026-10-14", asof="2025-09-04"))
        calls = []

        def get_page(parameters):
            calls.append(parameters)
            return {"bars": {"OLD": [bar("2026-10-14")]}}

        result = live_check.verify(value, get_page)
        self.assertTrue(result["complete"])
        self.assertEqual(result["stats"]["matched"], 1)
        self.assertEqual(calls[0]["asof"], "2025-09-04")
        self.assertEqual(calls[0]["start"], "2026-10-14T04:00:00Z")
        self.assertEqual(calls[0]["end"], "2026-10-14T23:59:59.999999Z")

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
        self.assertEqual(calls, ["A,B,C,D", "D"])
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
                "latest_session": 0,
                "full_history": 2,
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
        self.assertEqual(
            result["requests"], {"planned_batches": 1, "page_requests": 3, "oversize_splits": 1, "batches": 3}
        )

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

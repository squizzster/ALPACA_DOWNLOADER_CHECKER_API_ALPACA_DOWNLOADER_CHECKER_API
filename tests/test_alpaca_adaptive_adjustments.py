"""Restored empirical policy: exact outputs, bounded requests and completed handoffs."""

import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_durable_maintenance import fixture
from test_alpaca_pricing_download import bar, snapshot

from libs.alpaca_pricing_download import acquire, adaptive, download, storage
from libs.alpaca_pricing_download.adjustment_policy import AdaptiveAdjustmentDecision
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, DownloadError, timestamp, transform
from libs.alpaca_pricing_download.http import BarsClient

PATTERN_GROUPS = {
    "all_equal": (tuple(ADJUSTMENTS),),
    "dividend_only": (
        ("raw", "split", "spin-off", "split,spin-off"),
        ("dividend", "split,dividend", "dividend,spin-off", "all"),
    ),
    "split_only": (
        ("raw", "dividend", "spin-off", "dividend,spin-off"),
        ("split", "split,dividend", "split,spin-off", "all"),
    ),
    "split_and_dividend": (
        ("raw", "spin-off"),
        ("split", "split,spin-off"),
        ("dividend", "dividend,spin-off"),
        ("split,dividend", "all"),
    ),
    "dividend_and_spin_off": (
        ("raw", "split"),
        ("dividend", "split,dividend"),
        ("spin-off", "split,spin-off"),
        ("dividend,spin-off", "all"),
    ),
    "spin_off_only": (
        ("raw", "split", "dividend", "split,dividend"),
        ("spin-off", "split,spin-off", "dividend,spin-off", "all"),
    ),
    "all_distinct": tuple((adjustment,) for adjustment in ADJUSTMENTS),
    "observed_pattern_8": (
        ("raw", "dividend"),
        ("split", "split,dividend"),
        ("spin-off", "dividend,spin-off"),
        ("split,spin-off", "all"),
    ),
    "observed_pattern_9": (
        ("raw", "dividend"),
        ("split", "split,dividend", "split,spin-off", "all"),
        ("spin-off", "dividend,spin-off"),
    ),
}

EXPECTED_DOWNLOAD_COUNTS = {
    "all_equal": 2,
    "dividend_only": 3,
    "split_only": 4,
    "split_and_dividend": 5,
    "dividend_and_spin_off": 5,
    "spin_off_only": 4,
    "all_distinct": 8,
    "observed_pattern_8": 5,
    "observed_pattern_9": 5,
}


def pattern_rows(groups):
    return {
        name: [bar(day, str(100 + i)) for day in ("2016-01-04", "2026-10-14", "2026-10-16")]
        for i, group in enumerate(groups)
        for name in group
    }


class AdaptiveTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.pages = []
        self.live = {}
        self.client = BarsClient("test", "test")
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def page(self, params):
        self.pages.append(dict(params))
        return {
            "bars": {
                symbol: [
                    row
                    for row in self.live[symbol][params["adjustment"]]
                    if timestamp(params["start"]) <= timestamp(row["t"]) <= timestamp(params["end"])
                ]
                for symbol in params["symbols"].split(",")
            }
        }

    def assert_views(self, symbol, rows):
        actual = download.read_pricing_views(self.store, symbol)
        for name in ADJUSTMENTS:
            self.assertEqual(
                actual[name],
                transform(rows[name], timestamp("2016-01-01T00:00:00Z"), timestamp("2026-10-17T00:00:00Z")),
                name,
            )

    def test_direct_download_all_historical_patterns_and_unseen_fallback(self):
        cases = dict(PATTERN_GROUPS)
        cases["unseen_full_fallback"] = (
            ("raw", "dividend", "split"),
            ("all",),
            ("spin-off",),
            ("split,dividend",),
            ("split,spin-off",),
            ("dividend,spin-off",),
        )
        for index, (pattern, groups) in enumerate(cases.items()):
            with self.subTest(pattern=pattern):
                symbol = "CASE" + str(index)
                self.live[symbol] = pattern_rows(groups)
                self.pages.clear()
                result = download.download_pricing(self.store, symbol, "2026-10-17", client=self.client)
                expected = EXPECTED_DOWNLOAD_COUNTS.get(pattern, 8)
                history = [p for p in self.pages if p["start"][:10] == "2016-01-01"]
                self.assertEqual(len(history), expected)
                self.assertEqual([p["adjustment"] for p in history[:2]], ["raw", "all"])
                self.assertEqual(len(self.pages), expected * 2)  # Direct views' producer confirmations only.
                self.assertEqual(result["adjustment_acquisition"]["pattern"], pattern)
                self.assert_views(symbol, self.live[symbol])
                source = storage.load_source(self.store / "end_of_day/pricing" / symbol, symbol)
                self.assertEqual(source["latest_adjustment_acquisition"], result["adjustment_acquisition"])

    def test_mixed_batched_patterns_stop_each_ticker_at_its_completed_decision(self):
        symbols = []
        for index, groups in enumerate(PATTERN_GROUPS.values()):
            symbol = "CASE" + str(index)
            symbols.append(symbol)
            self.live[symbol] = pattern_rows(groups)
        result = acquire.acquire_selected(self.store, symbols, "2026-10-17", client=self.client)
        self.assertEqual(result["failed"], 0, result)
        for symbol, pattern in zip(symbols, PATTERN_GROUPS, strict=True):
            history = [
                p for p in self.pages if symbol in p["symbols"].split(",") and p["start"][:10] == "2016-01-01"
            ]
            self.assertEqual(len(history), EXPECTED_DOWNLOAD_COUNTS[pattern], (symbol, history))
            self.assert_views(symbol, self.live[symbol])

    def test_interior_difference_is_not_mistaken_for_complete_payload_equality(self):
        self.live["AAA"] = pattern_rows(PATTERN_GROUPS["dividend_only"])
        for name in ADJUSTMENTS:
            self.live["AAA"][name][0] = bar("2016-01-04")
            self.live["AAA"][name][-1] = bar("2026-10-16")
        result = download.download_pricing(self.store, "AAA", "2026-10-17", client=self.client)
        self.assertEqual(list(result["adjustment_acquisition"]["observed_sha256"]), ["raw", "all", "dividend"])
        self.assert_views("AAA", self.live["AAA"])

    def test_failed_second_stage_does_not_publish_or_infer_missing_views(self):
        self.live["AAA"] = pattern_rows(PATTERN_GROUPS["all_equal"])
        download.download_pricing(self.store, "AAA", "2026-10-15", client=self.client)
        before = snapshot(self.store)

        def fail(params):
            if params["start"][:10] == "2026-10-15" and params["adjustment"] == "all":
                raise DownloadError("second stage incomplete")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=fail):
            result = acquire.acquire_selected(self.store, ["AAA"], "2026-10-17", client=self.client)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(snapshot(self.store), before)

    def test_inconsistent_presence_fetches_each_view_without_inventing_more_absences(self):
        self.live["AAA"] = pattern_rows(PATTERN_GROUPS["all_equal"])
        self.live["AAA"]["all"] = []
        result = download.download_pricing(self.store, "AAA", "2026-10-17", client=self.client)
        self.assertEqual(result["adjustment_acquisition"]["pattern"], "inconsistent_presence_full_fallback")
        self.assertEqual(len(result["adjustment_acquisition"]["observed_sha256"]), 8)
        self.assert_views("AAA", self.live["AAA"])

    def test_new_suffix_selects_its_own_pattern_without_touching_accepted_history(self):
        self.live["AAA"] = pattern_rows(PATTERN_GROUPS["all_equal"])
        download.download_pricing(self.store, "AAA", "2026-10-15", client=self.client)
        before = snapshot(self.store / "end_of_day/pricing/AAA")
        new = pattern_rows(PATTERN_GROUPS["dividend_only"])
        for name in ADJUSTMENTS:
            self.live["AAA"][name][-1] = new[name][-1]
        self.pages.clear()
        result = acquire.acquire_selected(self.store, ["AAA"], "2026-10-17", client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(self.pages), 5)  # Two all endpoints, then raw/all/dividend suffix.
        self.assertEqual([p["adjustment"] for p in self.pages], ["all", "all", "raw", "all", "dividend"])
        self.assertTrue(all(p["start"][:10] == "2026-10-15" for p in self.pages[2:]))
        self.assert_views("AAA", self.live["AAA"])
        after = snapshot(self.store / "end_of_day/pricing/AAA")
        for path, value in before.items():
            if path.startswith("completed_years.") or path.startswith("2026/year."):
                self.assertEqual(after[path], value)

    def test_initial_exact_fallback_keeps_completed_raw_observation(self):
        for symbol in ("AAA", "BBB"):
            self.live[symbol] = pattern_rows(PATTERN_GROUPS["all_equal"])

        def reject_second_batch(params):
            if params["symbols"] == "AAA,BBB" and params["adjustment"] == "all":
                raise HTTPError("fixture", 400, "batch rejected", {}, None)
            return self.page(params)

        with (
            patch.object(self.client, "_get", side_effect=reject_second_batch),
            patch.object(adaptive, "digest", wraps=adaptive.digest) as hashed,
        ):
            result = acquire.acquire_selected(self.store, ["AAA", "BBB"], "2026-10-17", client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(hashed.call_count, 4)
        raw_history = [p for p in self.pages if p["adjustment"] == "raw" and p["start"][:10] == "2016-01-01"]
        self.assertEqual([p["symbols"] for p in raw_history], ["AAA,BBB"])
        for symbol in self.live:
            self.assert_views(symbol, self.live[symbol])

    def test_malformed_later_page_cannot_complete_an_adaptive_observation(self):
        self.live["AAA"] = pattern_rows(PATTERN_GROUPS["all_equal"])
        download.download_pricing(self.store, "AAA", "2026-10-15", client=self.client)
        before = snapshot(self.store)

        def malformed(params):
            result = self.page(params)
            if params["start"][:10] == "2026-10-15" and params["adjustment"] == "all":
                if "page_token" not in params:
                    result["next_page_token"] = "more"
                else:
                    result = {"bars": {"WRONG": []}}
            return result

        with patch.object(self.client, "_get", side_effect=malformed):
            result = acquire.acquire_selected(self.store, ["AAA"], "2026-10-17", client=self.client)
        self.assertEqual(result["failed"], 1, result)
        self.assertEqual(snapshot(self.store), before)

    def test_completed_decision_crosses_downloader_without_rehash_or_recalculation(self):
        from libs.alpaca_pricing_download import refresh

        self.live["AAA"] = pattern_rows(PATTERN_GROUPS["all_equal"])
        needs = [
            {
                "kind": "bars",
                "why": "initial_or_invalidated_history",
                "symbols": ["AAA"],
                "adjustments": list(ADJUSTMENTS),
                "asof": "2026-10-17",
                "ranges": [{"start": "2016-01-01T00:00:00Z", "end_exclusive": "2026-10-17T00:00:00Z"}],
            }
        ]
        client, _ = refresh.acquire_full(needs, self.client)
        count = len(self.pages)
        with (
            patch.object(adaptive, "digest", side_effect=AssertionError("accepted acquisition rehashed")),
            patch.object(
                adaptive,
                "decide_adaptive_adjustment_downloads",
                side_effect=AssertionError("accepted decision repeated"),
            ),
        ):
            download.download_pricing(self.store, "AAA", "2026-10-17", client=client)
        self.assertEqual(len(self.pages), count)
        self.assert_views("AAA", self.live["AAA"])

    def test_complete_update_matches_all_eight_oracle_and_finishes_serving(self):
        def all_eight(observed):
            missing = tuple(name for name in ADJUSTMENTS if name not in observed)
            return AdaptiveAdjustmentDecision(
                missing,
                None if missing else "unseen_full_fallback",
                tuple((name, name) for name in ADJUSTMENTS),
                True,
            )

        outputs = []
        request_counts = []
        for oracle in (False, True):
            with fixture() as case:
                for symbol in case.live:
                    case.live[symbol] = copy.deepcopy(case.old + [bar("2026-10-19")])
                with patch.object(
                    adaptive,
                    "decide_adaptive_adjustment_downloads",
                    all_eight if oracle else adaptive.decide_adaptive_adjustment_downloads,
                ):
                    result = case.update("2026-10-20")
                self.assertTrue(result["complete"], result)
                self.assertTrue(result["stages"]["serving_catalogue"]["complete"])
                outputs.append({symbol: download.read_pricing_views(case.store, symbol) for symbol in case.live})
                request_counts.append(len(case.pages))
        self.assertEqual(outputs[0], outputs[1])
        self.assertLess(request_counts[0], request_counts[1])

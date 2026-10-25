"""Retained acquisitions share requests without changing the ordinary producer checks."""

import copy
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import acquire, append, storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, BarsSymbolUnavailable, timestamp
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.http import BarsClient


class RetainedBatchTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, 12, tzinfo=timezone.utc),
            )
        )
        self.symbols = ["AAA", "BBB", "CCC", "DDD"]
        self.old = [bar("2016-01-04"), bar("2026-10-14")]
        self.live = {}
        for symbol in self.symbols:
            download_pricing(self.store, symbol, "2026-10-15", client=FakeClient(self.old))
            self.live[symbol] = {name: copy.deepcopy(self.old + [bar("2026-10-16")]) for name in ADJUSTMENTS}
        self.pages, self.progress = [], []
        self.phase = None
        self.client = BarsClient("test", "test")
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def observe(self, stage, values):
        self.progress.append((stage, values))
        if values.get("state") == "started":
            self.phase = stage

    def page(self, params):
        self.pages.append((self.phase, dict(params)))
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                symbol: [
                    row for row in self.live[symbol][params["adjustment"]] if start <= timestamp(row["t"]) <= end
                ]
                for symbol in params["symbols"].split(",")
            }
        }

    def run_acquire(self, symbols=None):
        return acquire.acquire_selected(
            self.store,
            self.symbols if symbols is None else symbols,
            "2026-10-17",
            client=self.client,
            progress=self.observe,
        )

    def test_four_histories_use_32_shared_requests_instead_of_128_and_keep_cold_payloads(self):
        before = snapshot(self.store)
        original = Path.read_bytes
        original_iterdir = Path.iterdir

        def selected_reads(path):
            if path.name.endswith(".csv.gz") and (
                path.name.startswith("completed_years.") or path.parent.name == "2026"
            ):
                self.fail("retained endpoint check reopened a cold payload")
            return original(path)

        def selected_staging(path):
            if path.name != ".staging" or path.parent.name not in self.symbols:
                self.fail("unexpected store discovery")
            return original_iterdir(path)

        with (
            patch.object(self.client, "fetch", side_effect=AssertionError("unexpected individual request")),
            patch.object(Path, "read_bytes", selected_reads),
            patch.object(Path, "iterdir", selected_staging),
        ):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(self.pages), 32)
        self.assertEqual(result["requests"]["page_requests"], 32)
        self.assertTrue(all(params["symbols"] == "AAA,BBB,CCC,DDD" for _, params in self.pages))
        self.assertEqual(
            [stage for stage, values in self.progress if values.get("state") == "started"],
            ["boundary_check", "retained_history_acquisition", "acquisition_check"],
        )
        for item in result["results"]:
            self.assertEqual((item["mode"], item["consistency_check"]), ("update", "matched"))
            self.assertEqual(len(read_pricing_views(self.store, item["symbol"])["all"]), 3)
        after = snapshot(self.store)
        for path, value in before.items():
            if "completed_years." in path or "/2026/year." in path:
                self.assertEqual(after[path], value)
        self.pages.clear()
        repeated = self.run_acquire()
        self.assertEqual(repeated["failed"], 0)
        self.assertTrue(all(item["status"] == "covered" for item in repeated["results"]))
        self.assertEqual(self.pages, [])

    def test_one_adjustment_mismatch_replaces_only_its_ticker(self):
        self.live["AAA"]["spin-off"][0]["c"] += 1
        result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        by_symbol = {item["symbol"]: item for item in result["results"]}
        self.assertEqual(by_symbol["AAA"]["mode"], "full_refresh")
        self.assertEqual(by_symbol["AAA"]["consistency_check"], "mismatch")
        self.assertTrue(all(by_symbol[s]["mode"] == "update" for s in self.symbols[1:]))
        self.assertEqual(
            read_pricing_views(self.store, "AAA")["spin-off"][0][4], str(self.live["AAA"]["spin-off"][0]["c"])
        )

    def test_sparse_tail_retains_first_rows_and_checks_each_view(self):
        for views in self.live.values():
            for name in ADJUSTMENTS:
                views[name] = views[name][:1]
        # Keep the original first/last checks matching, then return an empty tail.
        original = self.page

        def page(params):
            if self.phase == "boundary_check":
                self.pages.append((self.phase, dict(params)))
                start, end = timestamp(params["start"]), timestamp(params["end"])
                return {
                    "bars": {
                        s: [row for row in self.old if start <= timestamp(row["t"]) <= end]
                        for s in params["symbols"].split(",")
                    }
                }
            return original(params)

        # Removing the retained last row from the tail is an overlap failure;
        # the ordinary full retry must resolve it, including the removed notice.
        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(all(item["restarted"] for item in result["results"]))
        for symbol in self.symbols:
            self.assertEqual(len(read_pricing_views(self.store, symbol)["all"]), 1)
            notice = storage.read_document(self.store / f"end_of_day/manifests/{symbol}/update.json")
            self.assertEqual(notice["to"], "2026-10-14")

    def test_first_session_change_retries_fresh_and_counts_fallback_requests(self):
        original = self.page
        changed = False

        def page(params):
            nonlocal changed
            if self.phase == "acquisition_check" and not changed:
                changed = True
                for name in ADJUSTMENTS:
                    self.live["AAA"][name][0] = bar("2016-01-04", "75")
            return original(params)

        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        by_symbol = {item["symbol"]: item for item in result["results"]}
        self.assertTrue(by_symbol["AAA"]["restarted"])
        self.assertEqual(by_symbol["AAA"]["consistency_check"], "changed_during_acquisition")
        self.assertFalse(any(by_symbol[s]["restarted"] for s in self.symbols[1:]))
        self.assertEqual(read_pricing_views(self.store, "AAA")["all"][0][4], "75")
        self.assertEqual(result["requests"]["page_requests"], len(self.pages))

    def test_repeated_change_does_not_publish(self):
        before = snapshot(self.store)
        original = self.page

        def page(params):
            document = original(params)
            if self.phase == "acquisition_check" and params["start"][:10] == "2016-01-04":
                document["bars"] = {s: [bar("2016-01-04", "75")] for s in document["bars"]}
            return document

        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 4, result)
        self.assertTrue(all("twice" in item["outcome"]["error"] for item in result["results"]))
        self.assertEqual(snapshot(self.store), before)

    def test_stale_source_and_interrupted_publication_are_guarded_before_replay(self):
        original = acquire.acquire_retained
        for interrupted in (False, True):
            with self.subTest(interrupted=interrupted):
                concurrent = {}

                def publish_after_fetch(*args, **kwargs):
                    batches = original(*args, **kwargs)
                    buffered = next(batches)

                    def publish():
                        download_pricing(
                            self.store,
                            "AAA",
                            "2026-10-15",
                            client=FakeClient([bar("2016-01-04", "75"), self.old[-1]]),
                            refresh_all=True,
                        )

                    if interrupted:
                        with patch.object(storage, "_finish_publication", side_effect=OSError("stopped")):
                            with self.assertRaises(OSError):
                                publish()
                    else:
                        publish()
                    concurrent["pages"] = len(self.pages)
                    yield buffered
                    yield from batches

                with patch.object(acquire, "acquire_retained", side_effect=publish_after_fetch):
                    result = self.run_acquire(["AAA"])
                self.assertEqual(result["failed"], 1, result)
                self.assertIn("changed after acquisition selection", result["results"][0]["outcome"]["error"])
                self.assertEqual(len(self.pages), concurrent["pages"])
                self.assertFalse((self.store / "end_of_day/pricing/AAA/.pending.json").exists())
                self.assertEqual(read_pricing_views(self.store, "AAA")["all"][0][4], "75")
                # Restore a different selected generation for the next subcase.
                download_pricing(self.store, "AAA", "2026-10-15", client=FakeClient(self.old), refresh_all=True)

    def test_exact_rejection_falls_back_only_from_multi_symbol_requests(self):
        original = self.page
        rejected = []

        def page(params):
            if "AAA" in params["symbols"].split(","):
                rejected.append(params["symbols"])
                if params["symbols"] == "AAA":
                    raise BarsSymbolUnavailable("AAA", 400)
                raise HTTPError("https://example.invalid", 400, "batch rejected", {}, None)
            return original(params)

        before = snapshot(self.store / "end_of_day/pricing/AAA")
        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 1, result)
        self.assertEqual(result["results"][0]["outcome"]["outcome"], "invalid_symbol")
        self.assertEqual(rejected.count("AAA"), 1)
        self.assertEqual(snapshot(self.store / "end_of_day/pricing/AAA"), before)
        for symbol in self.symbols[1:]:
            self.assertEqual(len(read_pricing_views(self.store, symbol)["all"]), 3)

    def test_failure_in_each_stage_excludes_only_affected_ticker_and_no_later_requests(self):
        original = self.page
        for phase in ("boundary_check", "retained_history_acquisition", "acquisition_check"):
            for status in (403, 500):
                with self.subTest(phase=phase, status=status):
                    self.store = self.store.parent / f"{phase}_{status}"
                    for symbol in self.symbols:
                        download_pricing(self.store, symbol, "2026-10-15", client=FakeClient(self.old))
                    calls = []

                    def page(params):
                        if "," in params["symbols"]:
                            raise HTTPError("https://example.invalid", 414, "split", {}, None)
                        calls.append((self.phase, params["symbols"]))
                        if params["symbols"] == "AAA" and self.phase == phase:
                            raise HTTPError("https://example.invalid", status, "failed", {}, None)
                        return original(params)

                    before = snapshot(self.store / "end_of_day/pricing/AAA")
                    with patch.object(self.client, "_get", side_effect=page):
                        result = self.run_acquire()
                    self.assertEqual(result["failed"], 1, result)
                    failed = result["results"][0]
                    self.assertEqual(failed["outcome"]["http_status"], status)
                    self.assertEqual(failed["status"], "blocked" if status == 403 else "failed")
                    phases = ["boundary_check", "retained_history_acquisition", "acquisition_check"]
                    self.assertFalse(
                        any(s == "AAA" and p in phases[phases.index(phase) + 1 :] for p, s in calls)
                    )
                    self.assertEqual(snapshot(self.store / "end_of_day/pricing/AAA"), before)
                    for symbol in self.symbols[1:]:
                        self.assertEqual(len(read_pricing_views(self.store, symbol)["all"]), 3)

    def test_access_denial_remains_blocked_after_an_earlier_batch_failure(self):
        before = snapshot(self.store)
        for first_status in (400, 500):
            with self.subTest(first_status=first_status):

                def page(params):
                    self.pages.append((self.phase, params))
                    status = first_status if params["adjustment"] == "all" else 403
                    raise HTTPError("https://example.invalid", status, "rejected", {}, None)

                with patch.object(self.client, "_get", side_effect=page):
                    result = self.run_acquire()
                self.assertEqual(result["failed"], 4, result)
                self.assertTrue(all(item["status"] == "blocked" for item in result["results"]))
                self.assertTrue(all(item["outcome"]["http_status"] == 403 for item in result["results"]))
                self.assertTrue(
                    all("," in params["symbols"] and phase == "boundary_check" for phase, params in self.pages)
                )
                self.assertEqual(snapshot(self.store), before)

    def test_partial_views_check_retained_and_new_first_sessions(self):
        self.store = self.store.parent / "partial"

        class PartialClient(FakeClient):
            def fetch(self, symbol, adjustment, start, end, *, asof=None):
                return super().fetch(symbol, adjustment, start, end, asof=asof) if adjustment == "raw" else []

        download_pricing(self.store, "AAA", "2026-10-15", client=PartialClient(self.old))
        self.live["AAA"] = dict.fromkeys(ADJUSTMENTS, [])
        self.live["AAA"]["raw"] = self.old + [bar("2026-10-16")]
        self.live["AAA"]["all"] = [bar("2026-10-16")]
        with patch.object(self.client, "fetch", side_effect=AssertionError("unbuffered partial view")):
            result = self.run_acquire(["AAA"])
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["mode"], "update")
        views = read_pricing_views(self.store, "AAA")
        self.assertEqual(len(views["raw"]), 3)
        self.assertEqual(len(views["all"]), 1)
        self.assertFalse(views["split"])

    def test_pagination_finishes_before_following_stages(self):
        original = self.page

        def page(params):
            document = original(params)
            rows = [(s, row) for s, values in document["bars"].items() for row in values]
            offset = int(params.get("page_token", "0"))
            result = {"bars": {}}
            for symbol, row in rows[offset : offset + 2]:
                result["bars"].setdefault(symbol, []).append(row)
            if offset + 2 < len(rows):
                result["next_page_token"] = str(offset + 2)
            return result

        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        self.assertGreater(len(self.pages), 32)
        self.assertEqual(result["requests"]["page_requests"], len(self.pages))
        self.assertTrue(any("page_token" in params for _, params in self.pages))
        for symbol in self.symbols:
            self.assertEqual(len(read_pricing_views(self.store, symbol)["all"]), 3)

    def test_endpoint_cohorts_use_endpoint_cost_and_acquisitions_use_actual_ranges(self):
        with patch.object(append, "MAX_ROWS", 30000):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        started = [
            values
            for phase, values in self.progress
            if phase == "boundary_check" and values.get("state") == "started"
        ]
        self.assertEqual([values["symbols"] for values in started], [4])
        acquisitions = [
            values
            for phase, values in self.progress
            if phase == "retained_history_acquisition" and values.get("state") == "started"
        ]
        self.assertEqual([values["symbols"] for values in acquisitions], [4])

    def test_mixed_replacements_are_bounded_after_shared_endpoint_checks(self):
        for symbol in ("AAA", "CCC"):
            self.live[symbol]["raw"][0]["c"] += 1
        with patch.object(append, "MAX_ROWS", 30000):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        endpoints = [
            v for phase, v in self.progress if phase == "boundary_check" and v.get("state") == "started"
        ]
        self.assertEqual([v["symbols"] for v in endpoints], [4])
        acquisitions = [
            v
            for phase, v in self.progress
            if phase == "retained_history_acquisition" and v.get("state") == "started"
        ]
        self.assertEqual([v["symbols"] for v in acquisitions], [2, 2])
        self.assertEqual(
            [item["symbol"] for item in result["results"] if item["mode"] == "full_refresh"], ["AAA", "CCC"]
        )


if __name__ == "__main__":
    unittest.main()

"""Retained acquisitions trust first/latest all and request only missing sessions."""

import copy
import gc
import tempfile
import unittest
import weakref
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_pricing_download import FakeClient, bar, snapshot

from libs.alpaca_pricing_download import acquire, append, http, refresh, storage
from libs.alpaca_pricing_download.contract import (
    ADJUSTMENTS,
    BarsSymbolUnavailable,
    DownloadError,
    failure_outcome,
    timestamp,
)
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.failures import AcquisitionFailure
from libs.alpaca_pricing_download.http import BarsClient


class RetainedBatchTests(unittest.TestCase):
    def test_each_suffix_stage_failure_preserves_every_successful_view(self):
        for index, failed_view in enumerate(ADJUSTMENTS):
            with self.subTest(failed_view=failed_view):
                symbol = f"STAGE{index}"
                download_pricing(self.store, symbol, "2026-10-15", client=FakeClient(self.old))
                self.live[symbol] = {
                    name: [*self.old, bar("2026-10-16", str(100 + offset))]
                    for offset, name in enumerate(ADJUSTMENTS)
                }
                failed, successful = [], set()

                def page(params):
                    if params["start"][:10] == "2026-10-15":
                        name = params["adjustment"]
                        if name == failed_view and not failed:
                            failed.append(True)
                            raise DownloadError("injected suffix stage failure")
                        self.assertNotIn(name, successful, "successful view repeated after peer failure")
                        successful.add(name)
                    return self.page(params)

                with patch.object(self.client, "_get", side_effect=page):
                    result = acquire.acquire_selected(
                        self.store,
                        [symbol],
                        "2026-10-17",
                        client=self.client,
                        progress=self.observe,
                        select_append_work=True,
                    )
                self.assertEqual(result["failed"], 0, result)
                self.assertTrue(failed)
                self.assertEqual(successful, set(ADJUSTMENTS))
                published = read_pricing_views(self.store, symbol)
                for offset, name in enumerate(ADJUSTMENTS):
                    self.assertEqual(published[name][-1][4], str(100 + offset))

    def test_repeated_boundary_failures_do_not_expire_the_completed_suffix(self):
        from libs.alpaca_pricing_download.context import accept_context

        context = accept_context(self.store)
        successful = set()
        for attempt in range(3):

            def page(params):
                if params["start"][:10] == "2026-10-15":
                    self.assertNotIn(params["adjustment"], successful)
                    successful.add(params["adjustment"])
                elif attempt < 2:
                    raise DownloadError("boundary remains unavailable")
                return self.page(params)

            with patch.object(self.client, "_get", side_effect=page):
                result = acquire.acquire_selected(
                    self.store,
                    ["AAA"],
                    "2026-10-17",
                    client=self.client,
                    context=context,
                    progress=self.observe,
                    select_append_work=True,
                )
            self.assertEqual(result["failed"], int(attempt < 2), result)
        self.assertEqual(successful, {"raw", "all"})

    def test_changed_suffix_cutoff_cannot_consume_a_shorter_completed_response(self):
        from libs.alpaca_pricing_download.context import accept_context

        context = accept_context(self.store)
        suffixes = []
        for attempt, cutoff in enumerate(("2026-10-17", "2026-10-20")):
            if attempt:
                for name in ADJUSTMENTS:
                    self.live["AAA"][name].append(bar("2026-10-19"))

            def page(params):
                if params["start"][:10] == "2026-10-15":
                    suffixes.append((params["adjustment"], params["end"][:10]))
                elif not attempt:
                    raise DownloadError("boundary unavailable for first cutoff")
                return self.page(params)

            with patch.object(self.client, "_get", side_effect=page):
                result = acquire.acquire_selected(
                    self.store,
                    ["AAA"],
                    cutoff,
                    client=self.client,
                    context=context,
                    progress=self.observe,
                    select_append_work=True,
                )
            self.assertEqual(result["failed"], int(not attempt), result)
        self.assertEqual(
            set(suffixes), {(name, day) for name in ("raw", "all") for day in ("2026-10-16", "2026-10-19")}
        )
        self.assertEqual(len(read_pricing_views(self.store, "AAA")["raw"]), 4)

    def test_suffix_handoff_is_invalidated_by_a_changed_source_generation(self):
        from libs.alpaca_pricing_download.context import accept_context

        context = accept_context(self.store)
        failed, suffix_raw = [], []

        def page(params):
            if params["start"][:10] == "2026-10-15" and params["adjustment"] == "raw":
                suffix_raw.append(dict(params))
            elif self.phase == "boundary_check" and not failed:
                failed.append(True)
                raise DownloadError("injected boundary failure")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=page):
            first = acquire.acquire_selected(
                self.store,
                ["AAA"],
                "2026-10-17",
                client=self.client,
                context=context,
                progress=self.observe,
                select_append_work=True,
            )
            self.assertEqual(first["failed"], 1)
            changed = [self.old[0], {**self.old[1], "v": 12345}]
            download_pricing(self.store, "AAA", "2026-10-15", client=FakeClient(changed), refresh_all=True)
            self.live["AAA"] = {name: [*changed, {**bar("2026-10-16"), "v": 777}] for name in ADJUSTMENTS}
            second = acquire.acquire_selected(
                self.store,
                ["AAA"],
                "2026-10-17",
                client=self.client,
                context=context,
                progress=self.observe,
                select_append_work=True,
            )
        self.assertEqual(second["failed"], 0, second)
        self.assertEqual(len(suffix_raw), 2)
        self.assertEqual(read_pricing_views(self.store, "AAA")["raw"][-1][5], "777")

    def test_full_acquisition_projects_endpoints_once_through_publication(self):
        from libs.alpaca_pricing_download import adaptive

        original = adaptive.endpoints_of
        calls = []

        def once(views):
            self.assertFalse(calls, "completed acquisition endpoints projected again")
            calls.append(views)
            return original(views)

        with patch.object(adaptive, "endpoints_of", side_effect=once):
            result = acquire.acquire_selected(
                self.store, ["AAA"], "2026-10-17", client=self.client, invalidate={"AAA"}, progress=self.observe
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(calls), 1)

    def test_suffix_partial_success_survives_selection_failure(self):
        failed, raw = [], []

        def page(params):
            if params["start"][:10] == "2026-10-15":
                if params["adjustment"] == "raw":
                    self.assertFalse(raw, "successful raw suffix downloaded again")
                    raw.append(dict(params))
                elif self.phase == "append_work_selection" and not failed:
                    failed.append(True)
                    raise DownloadError("injected all suffix failure")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=page):
            result = acquire.acquire_selected(
                self.store,
                ["AAA"],
                "2026-10-17",
                client=self.client,
                progress=self.observe,
                select_append_work=True,
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(len(raw), 1)
        self.assertTrue(failed)
        self.assertEqual(len(read_pricing_views(self.store, "AAA")["all"]), 3)

    def test_completed_suffix_survives_later_boundary_failure_in_same_context(self):
        from libs.alpaca_pricing_download.context import accept_context

        context = accept_context(self.store)
        failed, successful = [], set()

        def page(params):
            if params["start"][:10] == "2026-10-15":
                key = params["adjustment"]
                self.assertNotIn(key, successful, "completed suffix downloaded again on retry")
                successful.add(key)
            elif self.phase == "boundary_check" and not failed:
                failed.append(True)
                raise DownloadError("injected boundary failure")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=page):
            first = acquire.acquire_selected(
                self.store,
                ["AAA"],
                "2026-10-17",
                client=self.client,
                context=context,
                progress=self.observe,
                select_append_work=True,
            )
            self.assertEqual(first["failed"], 1)
            second = acquire.acquire_selected(
                self.store,
                ["AAA"],
                "2026-10-17",
                client=self.client,
                context=context,
                progress=self.observe,
                select_append_work=True,
            )
        self.assertEqual(second["failed"], 0, second)
        self.assertEqual(successful, {"raw", "all"})

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

    def test_empty_suffix_selection_does_not_check_or_advance_retained_history(self):
        before = snapshot(self.store)
        for views in self.live.values():
            for name in ADJUSTMENTS:
                views[name] = copy.deepcopy(self.old)
        with patch.object(acquire, "read_validator", side_effect=AssertionError("validator read")):
            result = acquire.acquire_selected(
                self.store,
                self.symbols,
                "2026-10-17",
                client=self.client,
                progress=self.observe,
                select_append_work=True,
            )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual({item["status"] for item in result["results"]}, {"not_selected"})
        self.assertEqual(result["requests"]["append_work_candidates"], 4)
        self.assertEqual(result["requests"]["append_work_selected"], 0)
        self.assertEqual(result["requests"]["append_work_skipped"], 4)
        self.assertEqual(len(self.pages), 2)
        self.assertEqual({params["adjustment"] for _, params in self.pages}, {"raw", "all"})
        self.assertTrue(all(stage == "append_work_selection" for stage, _ in self.pages))
        self.assertEqual(snapshot(self.store), before)

    def test_positive_suffix_is_consumed_once_after_retained_approval(self):
        selected = []

        def accepted(symbols):
            selected.append(symbols)
            self.assertEqual(len(self.pages), 2)
            self.assertTrue(all(stage == "append_work_selection" for stage, _ in self.pages))

        result = acquire.acquire_selected(
            self.store,
            self.symbols,
            "2026-10-17",
            client=self.client,
            progress=self.observe,
            select_append_work=True,
            on_append_work_selected=accepted,
        )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["requests"]["append_work_candidates"], 4)
        self.assertEqual(result["requests"]["append_work_selected"], 4)
        self.assertEqual(result["requests"]["append_work_skipped"], 0)
        self.assertEqual(selected, [set(self.symbols)])
        self.assertEqual(len(self.pages), 4)
        suffix = [(stage, params) for stage, params in self.pages if stage == "append_work_selection"]
        self.assertEqual(len(suffix), 2)
        self.assertEqual({params["adjustment"] for _, params in suffix}, {"raw", "all"})
        self.assertTrue(all(stage == "append_work_selection" for stage, _ in suffix))
        boundary = [(stage, params) for stage, params in self.pages if stage == "boundary_check"]
        self.assertEqual(len(boundary), 2)
        self.assertTrue(all(stage == "boundary_check" for stage, _ in boundary))
        for item in result["results"]:
            self.assertEqual(item["status"], "published")
            self.assertEqual(len(read_pricing_views(self.store, item["symbol"])["all"]), 3)

    def test_retained_failures_do_not_keep_completed_responses_across_cohorts(self):
        selected = {}
        for symbol in ("AAA", "BBB"):
            root = self.store / "end_of_day/pricing" / symbol
            source = storage.load_source(root, symbol)
            selected[symbol] = source, storage.read_validator(root, source)
        # Distinct new views exercise the full eight-view fallback and its late failure.
        for i, name in enumerate(ADJUSTMENTS):
            self.live["AAA"][name][-1] = bar("2026-10-16", str(100 + i))
        observations, second_batch_alive = [], []
        original_completed = http.CompletedBars
        acquisition_pages = 0

        def completed(*args, **kwargs):
            value = original_completed(*args, **kwargs)
            if self.phase == "retained_history_acquisition" and value.symbol == "AAA":
                observations.append(weakref.ref(value))
            return value

        def page(params):
            nonlocal acquisition_pages
            if self.phase == "retained_history_acquisition":
                if params["symbols"] == "AAA":
                    acquisition_pages += 1
                    if acquisition_pages == 8:
                        raise DownloadError("eighth view failed")
                elif params["symbols"] == "BBB" and not second_batch_alive:
                    gc.collect()
                    second_batch_alive.append(sum(ref() is not None for ref in observations))
            return self.page(params)

        with (
            patch.object(append, "MAX_SYMBOLS", 1),
            patch.object(http, "CompletedBars", completed),
            patch.object(self.client, "_get", page),
        ):
            batches = refresh.acquire_retained(
                selected,
                timestamp("2026-10-17T00:00:00Z"),
                client=self.client,
                metrics={},
                progress=self.observe,
            )
            members, consumer, failures = next(batches)
            self.assertEqual(members, ["AAA"])
            failure = failures["AAA"][0]
            self.assertIsInstance(failure, AcquisitionFailure)
            self.assertEqual(failure_outcome(failure.exception())["error"], "eighth view failed")
            del consumer
            members, consumer, retained = next(batches)
            self.assertEqual(members, ["BBB"])
            self.assertIs(retained, failures)
            del consumer, retained
            batches.close()
        gc.collect()
        self.assertEqual(len(observations), 7)
        self.assertEqual(second_batch_alive, [0])
        self.assertTrue(all(ref() is None for ref in observations))
        self.assertIn("AAA", failures)  # Failure facts remain useful after buffers are released.

    def test_failure_records_preserve_outcomes_and_create_fresh_exceptions(self):
        for error in (
            DownloadError("transport exhausted"),
            HTTPError("fixture", 403, "denied", {}, None),
            BarsSymbolUnavailable("AAA", 400),
        ):
            with self.subTest(error=type(error).__name__):
                expected = failure_outcome(error)
                record = AcquisitionFailure.from_error(error)
                first, second = record.exception(), record.exception()
                self.assertIsNot(first, second)
                self.assertEqual(failure_outcome(first), expected)
                try:
                    raise first
                except DownloadError:
                    pass
                self.assertIsNone(second.__traceback__)
                self.assertEqual(failure_outcome(record.exception()), expected)

    def test_four_histories_use_two_all_checks_and_two_suffix_requests(self):
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
        self.assertEqual(len(self.pages), 4)
        self.assertEqual(result["requests"]["page_requests"], 4)
        self.assertTrue(all(params["symbols"] == "AAA,BBB,CCC,DDD" for _, params in self.pages))
        self.assertEqual(
            [stage for stage, values in self.progress if values.get("state") == "started"],
            ["boundary_check", "retained_history_acquisition", "retained_history_acquisition"],
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

    def test_all_endpoint_mismatch_replaces_only_its_ticker(self):
        self.live["AAA"]["all"][0]["c"] += 1
        result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        by_symbol = {item["symbol"]: item for item in result["results"]}
        self.assertEqual(by_symbol["AAA"]["mode"], "full_refresh")
        self.assertEqual(by_symbol["AAA"]["consistency_check"], "mismatch")
        self.assertTrue(all(by_symbol[s]["mode"] == "update" for s in self.symbols[1:]))
        self.assertEqual(
            read_pricing_views(self.store, "AAA")["all"][0][4], str(self.live["AAA"]["all"][0]["c"])
        )

    def test_empty_suffix_trusts_approval_and_keeps_retained_rows(self):
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

        # Once accepted, later empty responses cannot reopen or remove history.
        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        self.assertFalse(any(item["restarted"] or item["changed"] for item in result["results"]))
        self.assertEqual(len(self.pages), 4)
        for symbol in self.symbols:
            self.assertEqual(len(read_pricing_views(self.store, symbol)["all"]), 2)

    def test_provider_change_after_approval_does_not_trigger_historical_reverification(self):
        original = self.page
        changed = False

        def page(params):
            nonlocal changed
            if self.phase == "retained_history_acquisition" and not changed:
                changed = True
                for name in ADJUSTMENTS:
                    self.live["AAA"][name][0] = bar("2016-01-04", "75")
            return original(params)

        with patch.object(self.client, "_get", side_effect=page):
            result = self.run_acquire()
        self.assertEqual(result["failed"], 0, result)
        by_symbol = {item["symbol"]: item for item in result["results"]}
        self.assertFalse(any(item["restarted"] for item in by_symbol.values()))
        self.assertEqual(by_symbol["AAA"]["consistency_check"], "matched")
        self.assertEqual(len(self.pages), 4)
        self.assertEqual(read_pricing_views(self.store, "AAA")["all"][0][4], str(self.old[0]["c"]))
        self.assertEqual(result["requests"]["page_requests"], len(self.pages))

    def test_repeated_change_in_selected_replacement_does_not_publish(self):
        for symbol in self.symbols:
            self.live[symbol]["all"][0]["c"] += 1
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

    def test_split_failure_in_each_stage_fails_original_action_without_publication(self):
        original = self.page
        for phase in ("boundary_check", "retained_history_acquisition", "acquisition_check"):
            for status in (403, 500):
                with self.subTest(phase=phase, status=status):
                    self.store = self.store.parent / f"{phase}_{status}"
                    for symbol in self.symbols:
                        download_pricing(self.store, symbol, "2026-10-15", client=FakeClient(self.old))
                    if phase == "acquisition_check":
                        for symbol in self.symbols:
                            self.live[symbol]["all"][0]["c"] += 1
                    calls = []

                    def page(params):
                        if "," in params["symbols"]:
                            raise HTTPError("https://example.invalid", 414, "split", {}, None)
                        calls.append((self.phase, params["symbols"]))
                        if params["symbols"] == "AAA" and self.phase == phase:
                            raise HTTPError("https://example.invalid", status, "failed", {}, None)
                        return original(params)

                    before = snapshot(self.store / "end_of_day/pricing")
                    with patch.object(self.client, "_get", side_effect=page):
                        result = self.run_acquire()
                    self.assertEqual(result["failed"], len(self.symbols), result)
                    failed = result["results"][0]
                    self.assertEqual(failed["outcome"]["http_status"], status)
                    self.assertEqual(failed["status"], "blocked" if status == 403 else "failed")
                    phases = ["boundary_check", "retained_history_acquisition", "acquisition_check"]
                    self.assertFalse(any(p in phases[phases.index(phase) + 1 :] for p, s in calls))
                    self.assertEqual(snapshot(self.store / "end_of_day/pricing"), before)
                    for symbol in self.symbols[1:]:
                        self.assertEqual(len(read_pricing_views(self.store, symbol)["all"]), 2)

    def test_independent_action_failure_excludes_only_affected_ticker(self):
        original = self.page
        for phase in ("boundary_check", "retained_history_acquisition", "acquisition_check"):
            for status in (403, 500):
                with self.subTest(phase=phase, status=status):
                    self.store = self.store.parent / f"{phase}_{status}"
                    for symbol in self.symbols:
                        download_pricing(self.store, symbol, "2026-10-15", client=FakeClient(self.old))
                    if phase == "acquisition_check":
                        for symbol in self.symbols:
                            self.live[symbol]["all"][0]["c"] += 1
                    calls = []

                    def page(params):
                        if "," in params["symbols"]:
                            raise HTTPError("https://example.invalid", 414, "split", {}, None)
                        calls.append((self.phase, params["symbols"]))
                        if params["symbols"] == "AAA" and self.phase == phase:
                            raise HTTPError("https://example.invalid", status, "failed", {}, None)
                        return original(params)

                    before = snapshot(self.store / "end_of_day/pricing/AAA")
                    with (
                        patch.object(append, "MAX_SYMBOLS", 1),
                        patch.object(self.client, "_get", side_effect=page),
                    ):
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
                    status = first_status if params["start"][:10] == "2016-01-04" else 403
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

    def test_missing_all_baseline_fails_without_historical_requests(self):
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
        self.assertEqual(result["failed"], 1, result)
        self.assertIn("no all-adjusted baseline", result["results"][0]["outcome"]["error"])
        self.assertEqual(self.pages, [])
        views = read_pricing_views(self.store, "AAA")
        self.assertEqual(len(views["raw"]), 2)
        self.assertFalse(views["all"])

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
        self.assertEqual(len(self.pages), 8)
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
            if phase == "retained_history_acquisition"
            and values.get("state") == "started"
            and values.get("adaptive_stage") == 1
        ]
        self.assertEqual([values["symbols"] for values in acquisitions], [4])

    def test_mixed_replacements_are_bounded_after_shared_endpoint_checks(self):
        for symbol in ("AAA", "CCC"):
            self.live[symbol]["all"][0]["c"] += 1
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
            if phase == "retained_history_acquisition"
            and v.get("state") == "started"
            and v.get("adaptive_stage") == 1
        ]
        self.assertEqual([v["symbols"] for v in acquisitions], [2, 2])
        self.assertEqual(
            [item["symbol"] for item in result["results"] if item["mode"] == "full_refresh"], ["AAA", "CCC"]
        )


if __name__ == "__main__":
    unittest.main()

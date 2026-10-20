"""Accepted acquisition ranges and selections survive input changes and interruption."""

import json
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_durable_maintenance import ProcessInterrupted, fixture
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_phase4_finalization import invoke
from test_alpaca_pricing_download import FakeClient, bar
from test_alpaca_universe_update import CatalogueClient

from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.registration import command_policy
from libs.alpaca_lineage_build.universe_update import plan_update
from libs.alpaca_pricing_download import acquire, command_line
from libs.alpaca_pricing_download.completion import COMPLETION_FILE
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_recovery_registry import load_labels, publish_union


def source(case, symbol="SHORT"):
    return storage.read_document(case.store / f"end_of_day/pricing/{symbol}/source.manifest.json")


def seed_short_history(case, *, empty=False):
    rows = [] if empty else [bar("2026-10-16", "100")]
    download_pricing(case.store, "SHORT", "2026-10-17", from_date="2026-10-01", client=FakeClient(rows))
    result = run_lineage(case.store, ["SHORT"], identity_client=IdentityClient({"SHORT": asset("SHORT")}))
    if result["failed"]:
        raise AssertionError(result)
    case.identity = CatalogueClient([asset("AAA"), asset("SHORT")])
    case.live["SHORT"] = [bar("2016-01-04", "50"), *rows]


def accept_then_interrupt(case, symbol="SHORT", start="2016-01-01"):
    with patch.object(command_line, "download_pricing", side_effect=ProcessInterrupted()):
        try:
            invoke(case, "pricing", "acquire", "--symbol", symbol, "--from", start, "--through", "2026-10-16")
        except ProcessInterrupted:
            return
    raise AssertionError("acquisition did not reach its interruption")


class AcquisitionIntentionTests(unittest.TestCase):
    def assert_complete_range(self, case, start="2016-01-01", symbol="SHORT"):
        held = source(case, symbol)
        self.assertEqual(held["history_start"], start + "T00:00:00Z")
        work = universe_state.read_state(case.store)["work"][symbol]
        self.assertEqual(work["pricing_start"], start)
        self.assertEqual(work["status"], "complete")
        for rows in read_pricing_views(case.store, symbol).values():
            self.assertEqual(
                [row[4] for row in rows],
                [str(row["c"]) for row in case.live[symbol] if row["t"][:10] >= start],
            )

    def test_rejected_starts_cannot_register_or_poison_existing_work(self):
        for start in ("2015-01-01", "2026-10-17", "2027-01-01"):
            with self.subTest(start=start), fixture() as case:
                seed_short_history(case)
                accept_then_interrupt(case)
                before = universe_state.read_state(case.store)
                registry = case.registry.read_bytes()
                with patch.object(case.pricing, "_get", side_effect=AssertionError("rejected request")):
                    code, result, errors = invoke(
                        case,
                        "pricing",
                        "acquire",
                        "--symbol",
                        "SHORT",
                        "--symbol",
                        "UNEXPECTED",
                        "--from",
                        start,
                        "--through",
                        "2026-10-16",
                    )
                self.assertEqual(code, 1, (result, errors))
                self.assertIn("from date must be", errors)
                self.assertEqual(universe_state.read_state(case.store), before)
                self.assertEqual(case.registry.read_bytes(), registry)
                self.assertTrue(case.update()["complete"])
                self.assert_complete_range(case)

    def test_work_and_batch_owners_reject_impossible_ranges_before_publication(self):
        with fixture() as case:
            before = universe_state.read_state(case.store)
            policy = command_policy(case.store, "2026-10-17")
            for start in ("2015-01-01", "2026-10-17", "2027-01-01"):
                with self.subTest(start=start):
                    with self.assertRaisesRegex(DownloadError, "from date must be"):
                        policy.require("INVALID", "2026-10-17", reason="acquisition", acquire=True, start=start)
                    self.assertEqual(universe_state.read_state(case.store), before)
                    absent = case.directory / "uncreated"
                    with self.assertRaisesRegex(DownloadError, "from date must be"):
                        acquire.acquire_selected(
                            absent, ["INVALID"], "2026-10-17", client=None, from_dates={"INVALID": start}
                        )
                    self.assertFalse(absent.exists())

    def test_supported_start_edges_remain_executable(self):
        for start in ("2016-01-01", "2026-10-16"):
            with self.subTest(start=start), fixture() as case:
                case.live["EDGE"] = list(case.old)
                case.identity = CatalogueClient([asset("AAA"), asset("EDGE")])
                with patch.object(command_line, "BarsClient", return_value=case.pricing):
                    code, result, errors = invoke(
                        case,
                        "pricing",
                        "acquire",
                        "--symbol",
                        "EDGE",
                        "--from",
                        start,
                        "--through",
                        "2026-10-16",
                    )
                self.assertEqual(code, 0, (result, errors))
                code, result, errors = invoke(case, "lineage", "update", "--through", "2026-10-16")
                self.assertEqual(code, 0, (result, errors))
                self.assert_complete_range(case, start, "EDGE")

    def test_interrupted_extension_recovers_through_each_command_for_empty_and_nonempty_sources(self):
        for command in ("daily", "lineage", "pricing"):
            for empty in (False, True):
                with self.subTest(command=command, empty=empty), fixture() as case:
                    seed_short_history(case, empty=empty)
                    accept_then_interrupt(case)
                    self.assertEqual(
                        universe_state.read_state(case.store)["work"]["SHORT"]["pricing_start"], "2016-01-01"
                    )
                    if command == "daily":
                        result = case.update()
                        self.assertTrue(result["complete"], result)
                    else:
                        if command == "pricing":
                            with patch.object(command_line, "BarsClient", return_value=case.pricing):
                                code, result, errors = invoke(
                                    case, "pricing", "acquire", "--symbol", "SHORT", "--through", "2026-10-16"
                                )
                            self.assertEqual(code, 0, (result, errors))
                        code, result, errors = invoke(
                            case, "lineage", "update", "--symbol", "SHORT", "--through", "2026-10-16"
                        )
                        self.assertEqual(code, 0, (result, errors))
                    self.assert_complete_range(case)

    def test_disabled_acquisition_cannot_acknowledge_unextended_source(self):
        with fixture() as case:
            seed_short_history(case)
            accept_then_interrupt(case)
            before = source(case)
            with patch.object(case.pricing, "_get", side_effect=AssertionError("disabled acquisition")):
                code, result, errors = invoke(
                    case,
                    "lineage",
                    "update",
                    "--symbol",
                    "SHORT",
                    "--through",
                    "2026-10-16",
                    "--no-pricing-acquisition",
                )
            self.assertEqual(code, 1, (result, errors))
            self.assertFalse(result["complete"])
            self.assertEqual(source(case), before)
            work = universe_state.read_state(case.store)["work"]["SHORT"]
            self.assertEqual(work["pricing_start"], "2016-01-01")
            self.assertNotEqual(work["status"], "complete")
            self.assertTrue(case.update()["complete"])
            self.assert_complete_range(case)

    def test_initial_custom_starts_survive_recovery_without_unrequested_history(self):
        with fixture() as case:
            starts = {"FIRST": "2020-01-01", "SECOND": "2025-01-01"}
            case.identity = CatalogueClient([asset("AAA")])
            for symbol, start in starts.items():
                case.live[symbol] = [bar("2016-01-04", "50"), bar("2020-01-02", "75"), bar("2026-10-16", "100")]
                accept_then_interrupt(case, symbol, start)
            report = plan_update(case.store, "2026-10-17", identity_client=case.identity, registry=case.registry)
            self.assertEqual({s: report["work"][s]["pricing_start"] for s in starts}, starts)
            result = case.update()
            self.assertTrue(result["complete"], result)
            for symbol, start in starts.items():
                self.assert_complete_range(case, start, symbol)

    def test_explicit_retry_and_later_target_preserve_start_after_inactivity_or_omission(self):
        for inactive in (False, True):
            with self.subTest(inactive=inactive), fixture() as case:
                seed_short_history(case)
                with (
                    patch.object(command_line, "BarsClient", return_value=case.pricing),
                    patch.object(
                        case.pricing, "_get", side_effect=HTTPError("https://invalid", 403, "denied", {}, None)
                    ),
                ):
                    code, _, _ = invoke(
                        case,
                        "pricing",
                        "acquire",
                        "--symbol",
                        "SHORT",
                        "--from",
                        "2016-01-01",
                        "--through",
                        "2026-10-16",
                    )
                self.assertEqual(code, 1)
                case.identity = CatalogueClient(
                    [asset("AAA"), *([{**asset("SHORT"), "status": "inactive"}] if inactive else [])]
                )
                case.live["SHORT"].append(bar("2026-10-19", "120"))
                case.pages.clear()
                deferred = case.update("2026-10-20")
                self.assertFalse(deferred["complete"])
                self.assertFalse(any("SHORT" in p["symbols"].split(",") for p in case.pages))
                older = case.update("2026-10-16", retry_blocked=True)
                self.assertFalse(older["complete"])
                self.assertEqual(
                    universe_state.read_state(case.store)["work"]["SHORT"]["pricing_start"], "2016-01-01"
                )
                final = case.update("2026-10-20", retry_blocked=True)
                self.assertTrue(final["complete"], final)
                # Explicit inactivity preserves the accepted end; omission continues maintenance.
                rows = read_pricing_views(case.store, "SHORT")["raw"]
                self.assertEqual(rows[0][4], "50")
                self.assertEqual(source(case)["history_start"], "2016-01-01T00:00:00Z")
                self.assertEqual(universe_state.read_state(case.store)["work"]["SHORT"]["status"], "complete")

    def test_completed_extension_is_reused_after_pricing_and_lineage_interruptions(self):
        for boundary in ("pricing", "lineage"):
            with self.subTest(boundary=boundary), fixture() as case:
                seed_short_history(case)
                accept_then_interrupt(case)
                target = (
                    "libs.alpaca_lineage_build.universe_update.run_lineage"
                    if boundary == "pricing"
                    else "libs.alpaca_lineage_build.universe_state.record_outcome"
                )
                with patch(target, side_effect=ProcessInterrupted()), self.assertRaises(ProcessInterrupted):
                    case.update()
                self.assertEqual(source(case)["history_start"], "2016-01-01T00:00:00Z")
                case.pages.clear()
                final = case.update()
                self.assertTrue(final["complete"], final)
                self.assertFalse(any(p["start"][:10] == "2016-01-01" for p in case.pages))
                self.assert_complete_range(case)

    def test_newer_retained_span_defers_extension_under_an_older_cutoff(self):
        with fixture() as case:
            seed_short_history(case)
            accept_then_interrupt(case)
            download_pricing(
                case.store,
                "SHORT",
                "2026-10-20",
                client=FakeClient([bar("2026-10-16", "100"), bar("2026-10-19", "120")]),
            )
            run_lineage(case.store, ["SHORT"], identity_client=IdentityClient({"SHORT": asset("SHORT")}))
            before = source(case)
            case.pages.clear()
            result = case.update()
            self.assertFalse(result["complete"])
            self.assertEqual(source(case), before)
            self.assertIn(
                {"symbol": "SHORT", "reason": "required_history_extension_exceeds_requested_cutoff"},
                result["plan"]["deferred"],
            )
            self.assertFalse(any("SHORT" in p["symbols"].split(",") for p in case.pages))
            self.assertEqual(
                universe_state.read_state(case.store)["work"]["SHORT"]["pricing_start"], "2016-01-01"
            )

    def test_pending_earlier_start_cannot_reuse_a_shorter_completed_batch(self):
        with fixture() as case:
            seed_short_history(case)
            with patch.object(command_line, "BarsClient", return_value=case.pricing):
                code, result, errors = invoke(
                    case,
                    "pricing",
                    "acquire",
                    "--symbol",
                    "SHORT",
                    "--from",
                    "2026-10-01",
                    "--through",
                    "2026-10-16",
                )
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual(
                invoke(case, "lineage", "update", "--symbol", "SHORT", "--through", "2026-10-16")[0], 0
            )
            accept_then_interrupt(case)
            with patch.object(command_line, "BarsClient", return_value=case.pricing):
                code, result, errors = invoke(
                    case,
                    "pricing",
                    "acquire",
                    "--symbol",
                    "SHORT",
                    "--from",
                    "2026-10-01",
                    "--through",
                    "2026-10-16",
                )
            self.assertEqual(code, 0, (result, errors))
            self.assertEqual(source(case)["history_start"], "2016-01-01T00:00:00Z")
            self.assertEqual(
                invoke(case, "lineage", "update", "--symbol", "SHORT", "--through", "2026-10-16")[0], 0
            )
            self.assert_complete_range(case)

    def test_extended_buffer_cannot_overwrite_a_concurrent_source(self):
        with fixture() as case:
            seed_short_history(case)
            original = acquire.acquire_full
            concurrent = {}

            def publish_after_fetch(*args, **kwargs):
                acquired = original(*args, **kwargs)
                download_pricing(
                    case.store,
                    "SHORT",
                    "2026-10-17",
                    client=FakeClient([bar("2026-10-16", "150")]),
                    refresh_all=True,
                )
                concurrent.update(source(case))
                return acquired

            with patch.object(acquire, "acquire_full", side_effect=publish_after_fetch):
                result = acquire.acquire_selected(
                    case.store, ["SHORT"], "2026-10-17", client=case.pricing, from_dates={"SHORT": "2016-01-01"}
                )
            self.assertEqual(result["failed"], 1, result)
            self.assertEqual(source(case), concurrent)
            self.assertEqual(read_pricing_views(case.store, "SHORT")["raw"][0][4], "150")

    def test_file_and_recovery_selections_are_frozen_before_registration(self):
        for recovery in (False, True):
            with self.subTest(recovery=recovery), fixture() as case:
                if recovery:
                    directory = case.directory / "resources"
                    directory.mkdir()
                    case.registry = directory / "ticker.csv"
                    case.registry.write_text("SELECTED\n")
                    selection_file = case.registry
                    args = ["--recovery-universe"]
                else:
                    selection_file = case.directory / "selection.csv"
                    selection_file.write_text("SELECTED\n")
                    args = ["--symbols-file", str(selection_file)]
                original = command_line.selected_symbols
                selections, requests = [], []

                def changed_input(arguments):
                    selected = original(arguments)
                    selections.append(selected)
                    if recovery:
                        publish_union(["UNEXPECTED"], selection_file)
                    else:
                        selection_file.write_text("UNEXPECTED\n")
                    return selected

                pricing = FakeClient(case.old)
                fetch = pricing.fetch

                def observed(symbol, *args, **kwargs):
                    requests.append(symbol)
                    self.assertIn(symbol, load_labels(case.registry))
                    self.assertIn(symbol, universe_state.read_state(case.store)["work"])
                    return fetch(symbol, *args, **kwargs)

                with (
                    patch.object(
                        command_line,
                        "__file__",
                        str(case.directory / "libs/alpaca_pricing_download/command_line.py"),
                    ),
                    patch.object(command_line, "selected_symbols", side_effect=changed_input),
                    patch.object(command_line, "BarsClient", return_value=pricing),
                    patch.object(pricing, "fetch", side_effect=observed),
                ):
                    code, result, errors = invoke(case, "pricing", "acquire", *args, "--through", "2026-10-16")
                self.assertEqual(code, 0, (result, errors))
                self.assertEqual(selections, [["SELECTED"]])
                self.assertEqual(set(requests), {"SELECTED"})
                self.assertEqual([r["symbol"] for r in result["results"]], ["SELECTED"])
                self.assertEqual(
                    json.loads((case.store / COMPLETION_FILE).read_text())["request"]["symbols"], ["SELECTED"]
                )
                self.assertFalse((case.store / "end_of_day/pricing/UNEXPECTED/source.manifest.json").exists())


if __name__ == "__main__":
    unittest.main()

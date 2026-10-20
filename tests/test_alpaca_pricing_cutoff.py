"""User cutoff dates select sessions before producers accept or execute work."""

import io
import unittest
from contextlib import redirect_stderr
from functools import partial
from pathlib import Path
from unittest.mock import patch

from test_alpaca_durable_maintenance import fixture
from test_alpaca_lineage_build import asset
from test_alpaca_phase4_finalization import dependency_identity
from test_alpaca_phase4_finalization import invoke as invoke_cli
from test_alpaca_pricing_download import bar, snapshot
from test_alpaca_universe_update import CatalogueClient

from libs import alpaca_cli
from libs.alpaca_lineage_build import storage, universe_state
from libs.alpaca_lineage_build.registration import command_policy
from libs.alpaca_lineage_build.universe_update import plan_update, run_update
from libs.alpaca_pricing_download import command_line
from libs.alpaca_pricing_download.contract import DownloadError, timestamp
from libs.alpaca_pricing_download.download import pricing_session_end, request_end
from libs.alpaca_recovery_registry import DEFAULT_REGISTRY, publish_union


def invoke(case, *args, **kwargs):
    # CLI registry patching does not replace domain functions' bound defaults.
    with (
        patch(
            "libs.alpaca_lineage_build.universe_update.plan_update", partial(plan_update, registry=case.registry)
        ),
        patch(
            "libs.alpaca_lineage_build.universe_update.run_update", partial(run_update, registry=case.registry)
        ),
    ):
        return invoke_cli(case, *args, **kwargs)


class PricingCutoffTests(unittest.TestCase):
    def setUp(self):
        def isolated_union(labels, path=DEFAULT_REGISTRY):
            self.assertNotEqual(Path(path).resolve(), DEFAULT_REGISTRY.resolve(), "test escaped its registry")
            return publish_union(labels, path)

        self.enterContext(patch.object(universe_state, "publish_union", side_effect=isolated_union))

    def test_explicit_cutoffs_use_exchange_calendar_including_exceptional_closures(self):
        cases = (
            ("2026-10-16", "2026-10-17"),
            ("2026-10-17", "2026-10-17"),
            ("2026-10-18", "2026-10-17"),
            ("2026-10-19", "2026-10-20"),
            ("2026-04-03", "2026-04-03"),  # Good Friday -> Thursday.
            ("2026-07-05", "2026-07-03"),  # Observed holiday and weekend.
            ("2026-09-07", "2026-09-05"),  # Labor Day -> Friday.
            ("2026-11-27", "2026-11-28"),  # An early close is still a session.
            ("2027-01-03", "2027-01-01"),  # New Year holiday and weekend.
            ("2018-12-05", "2018-12-05"),  # National day of mourning -> Tuesday.
        )
        for through, end in cases:
            with self.subTest(through=through):
                self.assertEqual(
                    pricing_session_end(through, now=timestamp("2030-01-01T12:00:00Z")),
                    timestamp(end + "T00:00:00Z"),
                )

    def test_explicit_cutoff_preserves_new_york_completion_and_calendar_limits(self):
        for through, instant in (
            ("2026-10-16", "2026-10-17T03:59:59Z"),
            ("2026-10-20", "2026-10-20T12:00:00Z"),
            ("2016-01-01", "2026-10-20T12:00:00Z"),
            ("2099-12-30", "2100-01-01T12:00:00Z"),
        ):
            with self.subTest(through=through, instant=instant), self.assertRaises(DownloadError):
                pricing_session_end(through, now=timestamp(instant))
        self.assertEqual(
            pricing_session_end("2026-10-16", now=timestamp("2026-10-17T04:00:00Z")),
            timestamp("2026-10-17T00:00:00Z"),
        )
        # Accepted/journaled boundaries must not be silently reinterpreted.
        self.assertEqual(
            request_end("2026-10-18", now=timestamp("2026-10-20T12:00:00Z")),
            timestamp("2026-10-18T00:00:00Z"),
        )

    def test_friday_saturday_and_sunday_previews_select_identical_work_without_writes(self):
        with fixture() as case:
            before = snapshot(case.directory)
            plans = []
            with (
                patch.object(storage, "read_receipt", side_effect=AssertionError("unselected receipt")),
                patch.object(case.pricing, "_get", side_effect=AssertionError("preview pricing request")),
            ):
                for through in ("2026-10-16", "2026-10-17", "2026-10-18"):
                    code, result, errors = invoke(case, "update", "--plan", "--through", through)
                    self.assertEqual(code, 0, (result, errors))
                    self.assertEqual(result["pricing_cutoff"]["requested_through"], through)
                    self.assertEqual(result["pricing_cutoff"]["pricing_through_session"], "2026-10-16")
                    self.assertEqual(result["end_exclusive"], "2026-10-17")
                    self.assertNotIn("AAA", result["work"])
                    self.assertEqual(result["expected_io"]["append_actions_before_reconciliation"], 0)
                    plans.append(result)
            self.assertEqual(snapshot(case.directory), before)

            # Each preview assigns fresh obligation IDs; requirements and I/O must match.
            def requirements(plan):
                return {
                    symbol: {key: value for key, value in item.items() if key != "obligation_id"}
                    for symbol, item in plan["work"].items()
                }

            for plan in plans[1:]:
                self.assertEqual(requirements(plan), requirements(plans[0]))
                self.assertEqual(plan["expected_io"], plans[0]["expected_io"])

    def test_saturday_update_does_not_advance_or_republish_covered_friday_histories(self):
        with fixture() as case:
            case.identity = CatalogueClient([asset("AAA")])
            pricing_before = snapshot(case.store / "end_of_day/pricing/AAA")
            index_before = storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"]
            code, result, errors = invoke(case, "update", "--through", "2026-10-17")
            self.assertEqual(code, 0, (result, errors))
            self.assertTrue(result["complete"])
            self.assertEqual(result["plan"]["work"], {})
            self.assertEqual(result["stages"]["acquisition"]["results"], [])
            self.assertEqual(snapshot(case.store / "end_of_day/pricing/AAA"), pricing_before)
            self.assertEqual(
                storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"], index_before
            )
            self.assertTrue(case.pages, "normal live history comparison still runs")
            self.assertTrue(all(p["adjustment"] == "all" and p["asof"] == "2026-10-17" for p in case.pages))
            self.assertNotIn("AAA", universe_state.read_state(case.store)["work"])

    def test_saturday_discovery_publishes_full_and_empty_histories_only_through_friday(self):
        with fixture() as case:
            case.live["NEW"] = [bar("2026-10-16"), bar("2026-10-19")]
            code, result, errors = invoke(case, "update", "--through", "2026-10-17")
            self.assertEqual(code, 0, (result, errors))
            self.assertTrue(result["complete"])
            for symbol in ("NEW", "EMPTY"):
                source = storage.read_document(case.store / f"end_of_day/pricing/{symbol}/source.manifest.json")
                self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-17T00:00:00Z")
                self.assertEqual(source["latest_request"]["asof"], "2026-10-17")
                work = universe_state.read_state(case.store)["work"][symbol]
                self.assertEqual(work["pricing_end_exclusive"], "2026-10-17")
                self.assertEqual(work["status"], "complete")
            self.assertTrue(all(timestamp(p["end"]) < timestamp("2026-10-17T00:00:00Z") for p in case.pages))

    def test_snapped_cutoff_cannot_shrink_older_accepted_calendar_or_later_session_work(self):
        for accepted_end in ("2026-10-18", "2026-10-20"):
            with self.subTest(accepted_end=accepted_end), fixture() as case:
                case.identity = CatalogueClient([asset("AAA")])
                policy = command_policy(case.store, accepted_end)
                policy.require("AAA", accepted_end, reason="explicit_pricing_acquisition", acquire=True)
                before = universe_state.read_state(case.store)["work"]["AAA"]
                with patch.object(case.pricing, "_get", side_effect=AssertionError("deferred pricing request")):
                    code, result, errors = invoke(case, "update", "--through", "2026-10-17")
                self.assertEqual(code, 1, (result, errors))
                self.assertFalse(result["complete"])
                after = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(after["pricing_end_exclusive"], accepted_end)
                self.assertEqual(after["obligation_id"], before["obligation_id"])
                self.assertNotEqual(after["status"], "complete")

    def test_standalone_acquisition_accepts_the_resolved_range_before_network(self):
        with fixture() as case, patch.object(command_line, "BarsClient", return_value=case.pricing):

            def page(params):
                work = universe_state.read_state(case.store)["work"]["NEW"]
                self.assertEqual(work["pricing_end_exclusive"], "2026-10-17")
                return case.page(params)

            with patch.object(case.pricing, "_get", side_effect=page):
                code, result, errors = invoke(
                    case, "pricing", "acquire", "--symbol", "NEW", "--through", "2026-10-17"
                )
            self.assertEqual(code, 0, (result, errors))
            source = storage.read_document(case.store / "end_of_day/pricing/NEW/source.manifest.json")
            self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-17T00:00:00Z")

    def test_standalone_lineage_passes_resolved_cutoff_to_discovered_dependencies(self):
        with fixture() as case:
            case.live["DEP"] = [*case.old, bar("2026-10-19")]
            code, result, errors = invoke(
                case,
                "lineage",
                "refresh",
                "--symbol",
                "AAA",
                "--through",
                "2026-10-17",
                identity=dependency_identity(),
            )
            self.assertEqual(code, 0, (result, errors))
            source = storage.read_document(case.store / "end_of_day/pricing/DEP/source.manifest.json")
            self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-17T00:00:00Z")
            self.assertEqual(
                universe_state.read_state(case.store)["work"]["DEP"]["pricing_end_exclusive"], "2026-10-17"
            )

    def test_start_after_resolved_session_is_rejected_before_registration(self):
        with fixture() as case:
            before = universe_state.read_state(case.store)
            registry = case.registry.read_bytes()
            with patch.object(command_line, "BarsClient", side_effect=AssertionError("invalid range client")):
                code, result, errors = invoke(
                    case,
                    "pricing",
                    "acquire",
                    "--symbol",
                    "UNEXPECTED",
                    "--from",
                    "2026-10-17",
                    "--through",
                    "2026-10-17",
                )
            self.assertEqual(code, 1, (result, errors))
            self.assertIn("from date must be", errors)
            self.assertEqual(universe_state.read_state(case.store), before)
            self.assertEqual(case.registry.read_bytes(), registry)

    def test_combined_build_and_update_use_same_historical_holiday_cutoff(self):
        for operation in ("update", "build"):
            with self.subTest(operation=operation):
                entry = (
                    "libs.alpaca_end_of_day_v6.pipeline.run_v6_pipeline"
                    if operation == "update"
                    else "libs.alpaca_end_of_day_clean_room.generation.run_clean_room_generation"
                )
                selection = ["--symbol", "AAA"] if operation == "update" else ["--recovery-universe"]
                args = alpaca_cli.build_parser().parse_args(
                    ["catalogue", operation, *selection, "--through", "2026-04-03"]
                )
                with patch(entry, return_value={}) as producer, redirect_stderr(io.StringIO()):
                    alpaca_cli.dispatch(args)
                self.assertEqual(producer.call_args.kwargs["request_end_timestamp"], "2026-04-03T00:00:00Z")

    def test_reader_filters_and_pinned_resume_do_not_reselect_cutoffs(self):
        cases = (
            (
                ["read", "--symbol", "AAA", "--through", "2026-10-17"],
                "alpaca_end_of_day.standalone_cli.run_arguments",
            ),
            (
                ["catalogue", "resume"],
                "libs.alpaca_end_of_day_clean_room.generation.resume_clean_room_generation",
            ),
        )
        for argv, entry in cases:
            with (
                self.subTest(argv=argv),
                patch(entry, return_value=0),
                patch.object(
                    alpaca_cli,
                    "pricing_session_end",
                    side_effect=AssertionError("must preserve accepted inputs"),
                ),
            ):
                args = alpaca_cli.build_parser().parse_args(argv)
                self.assertEqual(alpaca_cli.dispatch(args), 0)
                if argv[0] == "read":
                    self.assertEqual(args.end, "2026-10-17")

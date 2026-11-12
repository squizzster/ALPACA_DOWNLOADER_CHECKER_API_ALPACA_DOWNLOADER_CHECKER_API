"""Trigger evidence must survive transport, recovery and replacement acceptance."""

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import correction, refresh
from libs.alpaca_pricing_download.context import accept_context
from libs.alpaca_pricing_download.contract import DownloadError, timestamp, transform
from libs.alpaca_pricing_download.download import download_pricing, read_pricing_views
from libs.alpaca_pricing_download.endpoints import compare_history
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.late_bars import session_ranges


class CorrectionObservationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.old = [bar("2026-01-05"), bar("2026-01-12")]
        self.end = "2026-01-13"
        self.client = BarsClient("test", "test")
        self.calls = []

    def seed(self, symbol="AAA", mapping=None):
        return download_pricing(
            self.store,
            symbol,
            self.end,
            client=FakeClient(self.old),
            mapping_asof=mapping,
            refresh_all=bool(mapping),
        )

    def selection(self, specs):
        requests = {r["symbol"]: r for r in refresh.prepare_refresh(self.store, specs)}
        return {
            "catalogue_generation": "catalogue",
            "inspected_groups": len(specs),
            "selected_groups": len(specs),
            "selected_identities": len(specs),
            "sources": {
                s: {"source_sha256": requests[s]["source_sha256"], "sessions": days} for s, days in specs.items()
            },
        }

    def page(self, params):
        self.calls.append(dict(params))
        start, end = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                s: [r for r in self.live if start <= timestamp(r["t"]) <= end]
                for s in params["symbols"].split(",")
            }
        }

    def work(self):
        universe_state.initialize(self.store)
        return WorkPolicy(self.store, {"symbols": {}}, {}, [], self.end)

    def test_cli_equal_refresh_accepts_completion_and_new_invocation_renews(self):
        from test_alpaca_durable_maintenance import fixture

        from libs import alpaca_cli

        with fixture() as case:
            args = alpaca_cli.build_parser().parse_args(
                ["pricing", "refresh", "--symbol", "AAA", "--store", str(case.store)]
            )
            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, case.identity)),
                patch.object(alpaca_cli, "DEFAULT_REGISTRY", case.registry),
            ):
                result = alpaca_cli.dispatch(args)
                self.assertEqual(result["failed"], 0, result)
                first = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(first["status"], "complete", first)
                self.assertTrue(first["correction_completion"])
                case.pages.clear()
                result = alpaca_cli.dispatch(args)
                second = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(second["status"], "complete", second)
                self.assertNotEqual(first["correction_id"], second["correction_id"])
                self.assertTrue(case.pages)

    def test_cli_interrupted_equal_refresh_consumes_receipt_before_requests(self):
        from test_alpaca_durable_maintenance import fixture

        from libs import alpaca_cli

        class Interrupted(BaseException):
            pass

        with fixture() as case:
            args = alpaca_cli.build_parser().parse_args(
                ["pricing", "refresh", "--symbol", "AAA", "--store", str(case.store)]
            )
            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, case.identity)),
                patch.object(alpaca_cli, "DEFAULT_REGISTRY", case.registry),
            ):
                with patch.object(WorkPolicy, "accept_correction_completions", side_effect=Interrupted):
                    with self.assertRaises(Interrupted):
                        alpaca_cli.dispatch(args)
                case.pages.clear()
                with patch.object(
                    case.pricing, "execute_plan", side_effect=AssertionError("acquisition repeated")
                ):
                    result = alpaca_cli.dispatch(args)
                self.assertEqual(result["failed"], 0, result)
                self.assertEqual(case.pages, [])
                self.assertEqual(universe_state.read_state(case.store)["work"]["AAA"]["status"], "complete")

    def test_cli_changed_publication_retry_does_not_renew_pending_lineage_work(self):
        from test_alpaca_durable_maintenance import fixture

        from libs import alpaca_cli

        class Interrupted(BaseException):
            pass

        with fixture() as case:
            case.live["AAA"] = [bar("2016-01-04", "500"), case.old[-1]]
            args = alpaca_cli.build_parser().parse_args(
                ["pricing", "refresh", "--symbol", "AAA", "--store", str(case.store)]
            )
            with (
                patch.object(alpaca_cli, "clients", return_value=(case.pricing, case.identity)),
                patch.object(alpaca_cli, "DEFAULT_REGISTRY", case.registry),
            ):
                with patch.object(WorkPolicy, "accept_correction_completions", side_effect=Interrupted):
                    with self.assertRaises(Interrupted):
                        alpaca_cli.dispatch(args)
                first = universe_state.read_state(case.store)["work"]["AAA"]
                with patch.object(
                    case.pricing, "execute_plan", side_effect=AssertionError("completed prices reacquired")
                ):
                    result = alpaca_cli.dispatch(args)
                second = universe_state.read_state(case.store)["work"]["AAA"]
                self.assertEqual(result["failed"], 0, result)
                self.assertEqual(first["correction_id"], second["correction_id"])
                self.assertNotEqual(second["status"], "complete")  # Changed lineage still must run.

    def test_durable_ingress_rejects_malformed_rows_but_preserves_conflicts(self):
        self.seed()
        work = self.work()
        prepared = refresh.prepare_refresh(self.store, ["AAA"])
        row = ["2026-01-05T05:00:00Z", "1", "2", "1", "2", "10", "2", "1"]
        prepared[0]["correction_observations"] = {"all": [row, [*row[:4], "3", *row[5:]]]}
        work.require_correction(prepared)
        accepted = universe_state.read_state(self.store)["work"]["AAA"]
        self.assertEqual(len(accepted["pricing_correction"]["observations"]["all"]), 2)
        invalid = [
            {"unknown": [row]},
            {"all": [row[:-1]]},
            {"all": [[row[0], 1, *row[2:]]]},
            {"all": [[row[0], "NaN", *row[2:]]]},
            {"all": [[row[0], "Infinity", *row[2:]]]},
            {"all": [["not-a-date", *row[1:]]]},
            {"all": [row, ["2026-01-02T05:00:00Z", *row[1:]]]},
        ]
        for observations in invalid:
            with self.subTest(observations=observations):
                bad = copy.deepcopy(accepted)
                bad["pricing_correction"]["observations"] = observations
                universe_state.put_work(self.store, "AAA", bad)
                with self.assertRaises(DownloadError):
                    universe_state.read_state(self.store)

    def test_daily_conflict_has_one_failure_owner_and_keeps_serving_failure_distinct(self):
        from test_alpaca_durable_maintenance import fixture
        from test_alpaca_lineage_build import asset
        from test_alpaca_universe_update import CatalogueClient

        for serving_failure in (False, True):
            with self.subTest(serving_failure=serving_failure), fixture() as case:
                case.records[:] = [asset("AAA")]
                case.identity = CatalogueClient(case.records)
                work = WorkPolicy(case.store, {"symbols": {}}, {}, [], "2026-10-17")
                prepared = refresh.prepare_refresh(case.store, ["AAA"])
                prepared[0]["correction_observations"] = {
                    "all": [["2016-01-04T05:00:00Z", "999", "999", "999", "999", "1", "1", "999"]]
                }
                work.require_correction(prepared)
                if serving_failure:
                    with patch(
                        "libs.alpaca_serving_catalogue.producer.maintain",
                        side_effect=RuntimeError("independent serving failure"),
                    ):
                        result = case.update()
                else:
                    result = case.update()
                self.assertFalse(result["complete"])
                self.assertEqual(result["failed"], 1 + serving_failure, result)
                self.assertEqual(len(result["failures"]), result["failed"])
                self.assertEqual(result["failures"][0]["outcome"]["outcome"], "provider_observation_conflict")

    def test_contiguous_market_sessions_keep_mapping_and_unselected_gaps(self):
        self.seed("AAA")
        self.seed("BBB", "2026-01-06")
        self.live = [bar(d) for d in ("2026-01-06", "2026-01-07", "2026-01-08", "2026-01-12")]
        selected = self.selection(
            {"AAA": [r["t"][:10] for r in self.live], "BBB": [r["t"][:10] for r in self.live]}
        )
        with patch.object(self.client, "_get", side_effect=self.page):
            prepared, report = refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
        self.assertEqual(report["requested_sessions"], 8)
        self.assertEqual(report["returned_sessions"], 8)
        self.assertEqual(len(self.calls), 4)
        self.assertEqual({p["asof"] for p in self.calls}, {self.end, "2026-01-06"})
        self.assertTrue(all("," not in p["symbols"] for p in self.calls))
        self.assertEqual([len(r["correction_observations"]["raw"]) for r in prepared], [4, 4])
        self.assertEqual(len(session_ranges(["2026-01-16", "2026-01-20"])), 1)  # MLK holiday
        self.assertEqual(len(session_ranges(["2026-03-06", "2026-03-09"])), 1)  # DST weekend
        with self.assertRaisesRegex(DownloadError, "non-market"):
            session_ranges(["2026-01-17"])

    def test_completed_action_is_durable_before_next_request_interrupts(self):
        self.seed()
        self.live = [bar("2026-01-06"), bar("2026-01-08")]
        selected = self.selection({"AAA": ["2026-01-06", "2026-01-08"]})
        work, context = self.work(), accept_context(self.store)

        def interrupted(params):
            if timestamp(params["start"]).date().isoformat() == "2026-01-08":
                pending = universe_state.read_state(self.store)["work"]["AAA"]
                self.assertEqual(len(pending["pricing_correction"]["observations"]["raw"]), 1)
                self.assertEqual(len(context.late_bar_observations), 1)
                raise KeyboardInterrupt("after completed independent action")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=interrupted), self.assertRaises(KeyboardInterrupt):
            refresh.probe_late_bar_repairs(
                self.store, selected, client=self.client, context=context, on_positive=work.require_correction
            )
        with patch.object(self.client, "_get", side_effect=self.page):
            prepared, report = refresh.probe_late_bar_repairs(
                self.store, selected, client=self.client, context=context, on_positive=work.require_correction
            )
        self.assertEqual(report["reused_ranges"], 1)
        self.assertEqual(len(prepared[0]["correction_observations"]["raw"]), 2)
        self.assertEqual(prepared[0]["required_correction_id"], work.work["AAA"]["correction_id"])

    def test_evidence_union_renews_completion_authority_and_rejects_competing_work(self):
        seeded = self.seed()
        work = self.work()
        request = refresh.prepare_refresh(self.store, ["AAA"])[0]
        rows = transform(self.old, timestamp(request["start"]), timestamp(request["end_exclusive"]))
        request["correction_observations"] = {"all": rows}
        work.require_correction([request])
        before = copy.deepcopy(work.work["AAA"])
        completion = {"correction_id": before["correction_id"], "source_sha256": seeded["source_sha256"]}
        extra = {**request, "correction_observations": {"raw": [rows[0]]}}
        work.require_correction([extra])
        self.assertEqual(set(extra["correction_observations"]), {"raw", "all"})
        self.assertNotEqual(extra["required_correction_id"], before["correction_id"])
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            work.accept_correction_completions(
                [
                    {
                        "symbol": "AAA",
                        "source_sha256": seeded["source_sha256"],
                        "correction_completion": completion,
                    }
                ]
            )
        # An older in-memory policy must not overwrite a concurrently renewed row.
        with self.assertRaisesRegex(RuntimeError, "current obligation"):
            universe_state.accept_correction_completion(self.store, "AAA", before, completion)
        stable = extra["required_correction_id"]
        work.require_correction([extra])
        self.assertEqual(extra["required_correction_id"], stable)

    def test_observed_session_must_have_one_acquired_row_and_no_conflicting_witness(self):
        request = {"start": "2026-01-01T00:00:00+00:00", "end_exclusive": "2026-02-01T00:00:00+00:00"}
        rows = transform([bar("2026-01-06")], timestamp(request["start"]), timestamp(request["end_exclusive"]))
        other = [*rows[0]]
        other[0] = "2026-01-06T06:00:00Z"
        with self.assertRaises(correction.ProviderObservationConflict):
            correction.accept_observations("AAA", {"raw": [rows[0], other]}, {"raw": rows})
        equivalent = [*rows[0]]
        equivalent[1] += ".0" if "." not in equivalent[1] else "0"
        merged = correction.merge_observations({"raw": rows}, {"raw": [equivalent]})
        self.assertEqual(len(merged["raw"]), 1)
        different = [*rows[0]]
        different[1] = "999"
        conflict = correction.merge_observations(merged, {"raw": [different]})
        with self.assertRaises(correction.ProviderObservationConflict):
            correction.accept_observations("AAA", {"raw": rows}, conflict)

    def test_failure_retains_independent_complete_actions_and_retries_only_unfinished(self):
        self.seed("AAA")
        self.seed("BBB")
        self.live = [bar("2026-01-06")]
        selected = self.selection({"AAA": ["2026-01-06"], "BBB": ["2026-01-08"]})
        context = accept_context(self.store)

        def fail(params):
            if params["symbols"] == "BBB":
                raise DownloadError("incomplete independent action")
            return self.page(params)

        with patch.object(self.client, "_get", side_effect=fail):
            prepared, first = refresh.probe_late_bar_repairs(
                self.store, selected, client=self.client, context=context
            )
        self.assertEqual(len(first["failures"]), 1)
        self.assertEqual([r["symbol"] for r in prepared], ["AAA"])
        self.calls.clear()
        with patch.object(self.client, "_get", side_effect=self.page):
            recovered, second = refresh.probe_late_bar_repairs(
                self.store, selected, client=self.client, context=context
            )
        self.assertEqual(second["reused_ranges"], 1)
        self.assertEqual([p["symbols"] for p in self.calls], ["BBB"])
        self.assertEqual(recovered, prepared)
        # Fresh invocations still observe fresh negatives; no new freshness policy.
        self.calls.clear()
        with patch.object(self.client, "_get", side_effect=self.page):
            refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
        self.assertEqual(len(self.calls), 2)
        changed = copy.deepcopy(selected)
        changed["sources"]["AAA"]["source_sha256"] = "0" * 64
        with patch.object(self.client, "execute_plan", side_effect=AssertionError("stale requests")):
            _, report = refresh.probe_late_bar_repairs(self.store, changed, client=self.client, context=context)
        self.assertEqual(report["stale_sources"], ["AAA"])

    def test_coalesced_response_routes_only_selected_sessions(self):
        self.seed()
        self.live = [bar("2026-01-10")]  # Saturday lies inside a Friday/Monday range.
        selected = self.selection({"AAA": ["2026-01-09", "2026-01-12"]})
        with patch.object(self.client, "_get", side_effect=self.page):
            prepared, report = refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(report["returned_sessions"], 0)
        self.assertEqual(prepared, [])

    def test_completed_plan_ranges_are_consumed_and_exact_override_keeps_sessions(self):
        self.seed()
        self.live = []
        days = ["2026-01-06", "2026-01-07", "2026-01-12"]
        selected = self.selection({"AAA": days})
        request = refresh.prepare_refresh(self.store, ["AAA"])[0]
        selected["sources"]["AAA"]["planned"] = {
            **{key: request[key] for key in ("asof", "start", "end_exclusive")},
            "sessions": days,
            "ranges": session_ranges(days, cutoff=request["end_exclusive"]),
        }
        with (
            patch.object(refresh, "session_ranges", side_effect=AssertionError("accepted plan recomputed")),
            patch.object(self.client, "_get", side_effect=self.page),
        ):
            refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
        self.assertEqual(len(self.calls), 2)
        self.calls.clear()
        selected["sources"]["AAA"].pop("planned")
        selected["window_policy"] = "exact_sessions"
        with patch.object(self.client, "_get", side_effect=self.page):
            refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
        self.assertEqual(len(self.calls), 3)
        self.assertTrue(all(timestamp(p["end"]) < timestamp(request["end_exclusive"]) for p in self.calls))

    def test_late_observation_acquires_only_selected_sessions_and_merges_complete_views(self):
        self.seed()
        self.live = [bar("2026-01-06")]
        selected = self.selection({"AAA": ["2026-01-06"]})
        with patch.object(self.client, "_get", side_effect=self.page):
            prepared, _ = refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
        work = self.work()
        work.require_correction(prepared)
        pending = universe_state.read_state(self.store)["work"]["AAA"]
        # Lost process state: durable rows and decision suffice, without another probe.
        with patch.object(refresh, "fetch_history", side_effect=AssertionError("comparison repeated")):
            recovered = refresh.check_retained_source(
                self.store, "AAA", self.end, client=self.client, pending=pending
            )
        self.assertEqual(
            recovered["correction_observations"], json.loads(json.dumps(prepared[0]["correction_observations"]))
        )
        before = (self.store / "end_of_day/pricing/AAA/source.manifest.json").read_bytes()
        self.calls.clear()
        self.live = [bar("2026-01-06")]
        with patch.object(self.client, "_get", side_effect=self.page):
            result = refresh.replace_prepared(self.store, [recovered], client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["mode"], "selected_sessions")
        self.assertTrue(result["results"][0]["changed"])
        self.assertNotEqual(before, (self.store / "end_of_day/pricing/AAA/source.manifest.json").read_bytes())
        views = read_pricing_views(self.store, "AAA")
        self.assertTrue(
            all(
                [row[0][:10] for row in views[name]] == ["2026-01-05", "2026-01-06", "2026-01-12"]
                for name in views
            )
        )
        self.assertTrue(all(timestamp(call["start"]).date().isoformat() == "2026-01-06" for call in self.calls))
        self.assertNotIn("raw", {call["adjustment"] for call in self.calls})

    def test_full_comparison_uses_exact_equal_replacement_to_resolve_batch_dependent_value(self):
        self.seed()
        rows = transform(self.old, timestamp("2016-01-01T00:00:00Z"), timestamp(self.end + "T00:00:00Z"))
        other = transform(
            [bar("2026-01-05", "500"), self.old[-1]],
            timestamp("2016-01-01T00:00:00Z"),
            timestamp(self.end + "T00:00:00Z"),
        )
        decision = compare_history(rows[0], rows, other)
        prepared = refresh.prepare_refresh(self.store, ["AAA"], comparisons={"AAA": decision})
        work = self.work()
        work.require_correction(prepared)
        pending = universe_state.read_state(self.store)["work"]["AAA"]
        with patch.object(refresh, "fetch_history", side_effect=AssertionError("comparison repeated")):
            recovered = refresh.check_retained_source(
                self.store, "AAA", self.end, client=self.client, pending=pending
            )
        self.live = self.old
        before = (self.store / "end_of_day/pricing/AAA/source.manifest.json").read_bytes()
        with patch.object(self.client, "_get", side_effect=self.page):
            result = refresh.replace_prepared(self.store, [recovered], client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(result["results"][0]["correction_completion"])
        self.assertFalse(result["results"][0]["changed"])
        self.assertEqual(before, (self.store / "end_of_day/pricing/AAA/source.manifest.json").read_bytes())
        # A real source change invalidates the old decision, including full-history scope.
        self.live = [bar("2026-01-05", "600"), self.old[-1]]
        download_pricing(self.store, "AAA", self.end, refresh_all=True, client=FakeClient(self.live))
        fresh = FakeClient(self.live)
        recovered = refresh.check_retained_source(self.store, "AAA", self.end, client=fresh, pending=pending)
        self.assertTrue(recovered["matched"])
        self.assertTrue(fresh.calls)

    def test_full_comparison_conflict_remains_unresolved_when_exact_replacement_differs(self):
        self.seed()
        rows = transform(self.old, timestamp("2016-01-01T00:00:00Z"), timestamp(self.end + "T00:00:00Z"))
        observed = transform(
            [bar("2026-01-05", "500"), self.old[-1]],
            timestamp("2016-01-01T00:00:00Z"),
            timestamp(self.end + "T00:00:00Z"),
        )
        decision = compare_history(rows[0], rows, observed)
        prepared = refresh.prepare_refresh(self.store, ["AAA"], comparisons={"AAA": decision})
        work = self.work()
        work.require_correction(prepared)
        self.live = [bar("2026-01-05", "600"), self.old[-1]]
        before = (self.store / "end_of_day/pricing/AAA/source.manifest.json").read_bytes()

        def differing_context(params):
            from test_alpaca_pricing_download import single_session_request

            response = self.page(params)
            if params["adjustment"] == "all" and single_session_request(params):
                response["bars"]["AAA"] = [bar("2026-01-05", "500")]
            return response

        with patch.object(self.client, "_get", side_effect=differing_context):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["results"][0]["outcome"]["outcome"], "provider_observation_conflict")
        self.assertEqual(before, (self.store / "end_of_day/pricing/AAA/source.manifest.json").read_bytes())

    def test_consistent_trigger_is_consumed_without_reconfirming_its_session(self):
        self.seed()
        self.live = [bar("2026-01-06"), self.old[-1]]
        selected = self.selection({"AAA": ["2026-01-06"]})
        with patch.object(self.client, "_get", side_effect=self.page):
            prepared, _ = refresh.probe_late_bar_repairs(self.store, selected, client=self.client)
            with patch(
                "libs.alpaca_pricing_download.download.accept_observations",
                wraps=correction.accept_observations,
            ) as accept:
                result = refresh.replace_prepared(self.store, prepared, client=self.client)
            self.assertEqual(accept.call_count, 1)
        self.assertEqual(result["failed"], 0, result)
        exact_raw = [p for p in self.calls if p["adjustment"] == "raw" and p["start"][:10] == "2026-01-06"]
        self.assertEqual(len(exact_raw), 1)

    def test_equal_correction_commits_only_completion_and_recovers_without_producer_replay(self):
        self.seed()
        path = self.store / "end_of_day/pricing/AAA/source.manifest.json"
        before = path.read_bytes()
        prepared = refresh.prepare_refresh(self.store, ["AAA"])
        work = self.work()
        work.require_correction(prepared)
        self.live = self.old
        commit = correction.complete

        class Interrupted(BaseException):
            pass

        def interrupt(*args):
            commit(*args)
            raise Interrupted()

        with (
            patch.object(self.client, "_get", side_effect=self.page),
            patch("libs.alpaca_pricing_download.download.complete_correction", side_effect=interrupt),
            self.assertRaises(Interrupted),
        ):
            refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(before, path.read_bytes())
        pending = universe_state.read_state(self.store)["work"]["AAA"]
        with patch.object(refresh, "fetch_history", side_effect=AssertionError("completed check repeated")):
            recovered = refresh.check_retained_source(
                self.store, "AAA", self.end, client=self.client, pending=pending
            )
        with (
            patch.object(
                self.client, "execute_plan", side_effect=AssertionError("completed acquisition repeated")
            ),
            patch(
                "libs.alpaca_pricing_download.download.completed_correction",
                side_effect=AssertionError("receipt reread"),
            ),
        ):
            result = refresh.replace_prepared(self.store, [recovered], client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["files_written"], 0)
        self.assertEqual(before, path.read_bytes())
        work.accept_correction_completions(result["results"])
        self.assertEqual(
            work.work["AAA"]["correction_completion"], result["results"][0]["correction_completion"]
        )

    def test_equal_correction_finishes_daily_work_without_lineage_or_serving_publication(self):
        from test_alpaca_durable_maintenance import fixture
        from test_alpaca_lineage_build import asset
        from test_alpaca_universe_update import CatalogueClient

        from libs.alpaca_lineage_build import storage as lineage_storage

        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            index = lineage_storage.read_index(case.store / "end_of_day/lineage")
            work = WorkPolicy(case.store, index, {}, [], "2026-10-17")
            work.require_correction(refresh.prepare_refresh(case.store, ["AAA"]))
            before = index["symbols"]["AAA"]
            with patch(
                "libs.alpaca_lineage_build.storage.publish",
                side_effect=AssertionError("equal correction republished lineage"),
            ):
                result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["stages"]["serving_catalogue"]["published_components"], 0)
            self.assertEqual(
                lineage_storage.read_index(case.store / "end_of_day/lineage")["symbols"]["AAA"], before
            )

    def test_incomplete_pagination_proves_no_partial_positive_or_negative(self):
        self.seed("AAA")
        self.seed("BBB")
        selected = self.selection({"AAA": ["2026-01-06", "2026-01-07"], "BBB": ["2026-01-08"]})
        context = accept_context(self.store)

        def page(params):
            if params["symbols"] == "BBB":
                return {"bars": {}}
            if params.get("page_token"):
                raise DownloadError("second page incomplete")
            return {"bars": {"AAA": [bar("2026-01-06")]}, "next_page_token": "second"}

        with patch.object(self.client, "_get", side_effect=page):
            prepared, result = refresh.probe_late_bar_repairs(
                self.store, selected, client=self.client, context=context
            )
        self.assertEqual(prepared, [])
        self.assertEqual(result["returned_sessions"], 0)
        self.assertEqual(len(context.late_bar_observations), 1)  # Only the independent completed empty action.
        self.live = []
        with patch.object(self.client, "_get", side_effect=self.page):
            _, result = refresh.probe_late_bar_repairs(self.store, selected, client=self.client, context=context)
        self.assertEqual([p["symbols"] for p in self.calls], ["AAA"])
        self.assertEqual(result["failures"], [])

    def test_equal_replacement_after_append_preserves_source_and_scope_changes_invalidate_completion(self):
        self.seed()
        self.live = [*self.old, bar("2026-01-13")]
        download_pricing(self.store, "AAA", "2026-01-14", client=FakeClient(self.live))
        path = self.store / "end_of_day/pricing/AAA/source.manifest.json"
        before = path.read_bytes()
        value = download_pricing(
            self.store,
            "AAA",
            "2026-01-14",
            client=FakeClient(self.live),
            refresh_all=True,
            history_correction_id="done",
        )
        self.assertEqual(path.read_bytes(), before)
        completion = value["correction_completion"]
        download_pricing(self.store, "AAA", "2026-01-15", client=FakeClient([*self.live, bar("2026-01-14")]))
        with self.assertRaisesRegex(DownloadError, "completion generation changed"):
            download_pricing(
                self.store,
                "AAA",
                "2026-01-15",
                client=FakeClient([]),
                refresh_all=True,
                history_correction_id="done",
                correction_completion=completion,
            )

    def test_full_binding_rejects_changed_range_mapping_store_and_missing_evidence(self):
        self.seed()
        prepared = refresh.prepare_refresh(self.store, ["AAA"])
        work = self.work()
        prepared[0]["correction_cause"] = "provider_late_observations"
        prepared[0]["correction_observations"] = {"raw": [["2026-01-06T05:00:00Z", *(["1"] * 7)]]}
        work.require_correction(prepared)
        contract = work.work["AAA"]["pricing_correction"]
        request = prepared[0]
        args = dict(
            store=self.store,
            symbol="AAA",
            generation=request["source_sha256"],
            start=request["start"],
            end=request["end_exclusive"],
            asof=request["asof"],
        )
        self.assertTrue(correction.applicable_contract(contract, **args))
        for key, value in (
            ("end_exclusive", "2026-01-14T00:00:00Z"),
            ("asof", "2026-01-01"),
            ("store", "/another-store"),
            ("source_sha256", "0" * 64),
        ):
            self.assertFalse(correction.applicable_contract({**contract, key: value}, **args))
        with self.assertRaisesRegex(DownloadError, "missing correction trigger"):
            correction.applicable_contract({**contract, "observations": {}}, **args)

    def test_legacy_latest_decision_retains_its_original_comparison_mapping(self):
        self.seed()
        request = refresh.prepare_refresh(self.store, ["AAA"])[0]
        contract = {
            "schema_version": 1,
            "scope": "latest_session",
            "source_sha256": request["source_sha256"],
            "start": request["start"],
            "asof": "2026-01-14",
            "end_exclusive": "2026-01-14T00:00:00Z",
            "comparison_end_exclusive": request["end_exclusive"],
            "selection": {"comparison_asof": request["asof"]},
        }
        self.assertTrue(
            correction.applicable_contract(
                contract,
                store=self.store,
                symbol="AAA",
                generation=request["source_sha256"],
                start=request["start"],
                end=request["end_exclusive"],
                asof=request["asof"],
            )
        )

    def test_daily_full_obligation_reselects_after_generation_change_without_old_replacement(self):
        from test_alpaca_durable_maintenance import ProcessInterrupted, fixture
        from test_alpaca_lineage_build import IdentityClient, asset
        from test_alpaca_universe_update import CatalogueClient

        from libs.alpaca_lineage_build.pipeline import run_lineage

        with fixture() as case:
            case.records[:] = [asset("AAA")]
            case.identity = CatalogueClient(case.records)
            case.live["AAA"] = [bar("2016-01-04", "50"), case.old[-1]]
            with (
                patch("libs.alpaca_lineage_build.refresh.replace_prepared", side_effect=ProcessInterrupted),
                self.assertRaises(ProcessInterrupted),
            ):
                case.update()
            case.live["AAA"] = [bar("2016-01-04", "600"), case.old[-1]]
            download_pricing(
                case.store, "AAA", "2026-10-17", client=FakeClient(case.live["AAA"]), refresh_all=True
            )
            self.assertEqual(
                run_lineage(case.store, ["AAA"], identity_client=IdentityClient({"AAA": asset("AAA")}))[
                    "failed"
                ],
                0,
            )
            case.pages.clear()
            result = case.update()
            self.assertTrue(result["complete"], result)
            self.assertTrue(case.pages)
            self.assertTrue(all(p["adjustment"] == "all" for p in case.pages))

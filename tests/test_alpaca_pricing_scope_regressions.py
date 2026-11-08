"""Corrections retain every witness and exactly the authority they establish."""

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from test_alpaca_pricing_download import FakeClient, bar

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.work_contract import WorkPolicy
from libs.alpaca_pricing_download import activity, append, correction, download, handoff, refresh, storage
from libs.alpaca_pricing_download.check_summary import price_entry
from libs.alpaca_pricing_download.context import accept_context
from libs.alpaca_pricing_download.contract import timestamp, transform
from libs.alpaca_pricing_download.endpoints import EndpointSelection, compare_history
from libs.alpaca_pricing_download.http import BarsClient


class PricingScopeRegressions(unittest.TestCase):
    def setUp(self):
        self.enterContext(
            patch.object(download, "utc_now", return_value=datetime(2030, 1, 1, tzinfo=timezone.utc))
        )
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.end = "2026-11-07"
        self.old = [bar(day) for day in ("2016-01-04", "2026-02-02", "2026-10-01", "2026-11-05")]
        self.live, self.calls = self.old, []
        self.client = BarsClient("test", "test")
        self.enterContext(patch.object(self.client, "_get", side_effect=self.page))

    def page(self, params):
        self.calls.append(dict(params))
        start, stop = timestamp(params["start"]), timestamp(params["end"])
        return {
            "bars": {
                symbol: [row for row in self.live if start <= timestamp(row["t"]) <= stop]
                for symbol in params["symbols"].split(",")
            }
        }

    def seed(self, symbol="AAA"):
        return download.download_pricing(self.store, symbol, self.end, client=FakeClient(self.old))

    def rows(self, bars):
        return transform(bars, timestamp("2016-01-01T00:00:00Z"), timestamp(self.end + "T00:00:00Z"))

    def work(self):
        universe_state.initialize(self.store)
        return WorkPolicy(self.store, {"symbols": {}}, {}, [], self.end)

    def selection(self, days, symbols=("AAA",)):
        prepared = refresh.prepare_refresh(self.store, symbols)
        return {
            "catalogue_generation": "accepted",
            "inspected_groups": len(symbols),
            "selected_groups": len(symbols),
            "selected_identities": len(symbols),
            "sources": {
                item["symbol"]: {"source_sha256": item["source_sha256"], "sessions": days} for item in prepared
            },
        }

    def test_all_context_disposition_accounts_for_every_witness_and_order(self):
        seeded = self.seed()
        source_path = self.store / "end_of_day/pricing/AAA/source.manifest.json"
        before = source_path.read_bytes()
        mismatch = self.rows([bar("2016-01-04", "500")])
        raw = self.rows([bar("2026-11-05", "600")])
        cases = [
            {"all": mismatch, "raw": raw},
            {"raw": raw, "all": mismatch},
            {"all": self.rows([bar("2016-01-04", "500"), bar("2026-10-02", "600")])},
            {"all": [*mismatch, *self.rows([bar("2016-01-04", "600")])]},
        ]
        for observations in cases:
            with (
                self.subTest(observations=observations),
                self.assertRaises(correction.ProviderObservationConflict),
            ):
                download.download_pricing(
                    self.store,
                    "AAA",
                    self.end,
                    client=FakeClient(self.old),
                    refresh_all=True,
                    correction_observations=observations,
                    correction_cause="selected_all_rows_changed",
                    history_correction_id="selected",
                )
            self.assertEqual(source_path.read_bytes(), before)
            self.assertFalse((self.store / "end_of_day/pricing/AAA/correction.complete.json").exists())
        accepted = download.download_pricing(
            self.store,
            "AAA",
            self.end,
            client=FakeClient(self.old),
            refresh_all=True,
            correction_observations={"all": mismatch},
            correction_cause="selected_all_rows_changed",
            history_correction_id="single-context",
        )
        self.assertTrue(accepted["correction_completion"])
        self.assertEqual(accepted["source_sha256"], seeded["source_sha256"])

    def test_accumulated_all_constraints_survive_selection_and_are_reconciled_once(self):
        self.seed()
        root = self.store / "end_of_day/pricing/AAA"
        before = (root / "source.manifest.json").read_bytes()
        self.live = [bar("2016-01-04", "600"), *self.old[1:]]
        points = storage.read_validator(root, storage.load_source(root, "AAA"))
        decision = compare_history(points["all"][0], points.tail_all, self.rows(self.live))
        prepared = refresh.prepare_refresh(self.store, ["AAA"])
        item = prepared[0]
        item.update(
            correction_cause="selected_all_rows_changed",
            comparison=decision,
            correction_observations={"all": self.rows([bar("2016-01-04", "500"), *self.old[1:]])},
            endpoint_selection=EndpointSelection(
                str(self.store),
                "AAA",
                item["source_sha256"],
                item["end_exclusive"],
                item["asof"],
                tuple(points.items()),
                matched=False,
                tail_all=points.tail_all,
                comparison=decision,
            ),
        )
        self.work().require_correction(prepared)
        with (
            patch.object(refresh, "accept_observations", wraps=correction.accept_observations) as accepted,
            patch.object(download, "accept_observations", side_effect=AssertionError("reconciled twice")),
        ):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 1, result)
        self.assertEqual(accepted.call_count, 1)
        self.assertEqual(result["results"][0]["outcome"]["outcome"], "provider_observation_conflict")
        self.assertEqual(before, (root / "source.manifest.json").read_bytes())
        self.assertEqual([call["adjustment"] for call in self.calls], ["raw", "all"])

    def test_selected_session_reads_only_its_partition_and_validates_only_new_rows(self):
        self.seed()
        self.live = [bar("2026-10-02")]
        prepared, _ = refresh.probe_late_bar_repairs(
            self.store,
            self.selection(["2026-10-02"]),
            client=self.client,
        )
        self.work().require_correction(prepared)
        original = download.read_boundary
        opened, checked, payloads = [], [], []

        def read(root, source, kind, **kwargs):
            self.assertEqual(kind, "current_year", "an unselected historical partition was opened")
            opened.append(kind)
            return original(root, source, kind, **kwargs)

        def validate(views):
            checked.extend(row[0][:10] for row in views["raw"])
            activity_check(views)

        activity_check = activity.validate_activity
        artifact_read = handoff.read_artifact

        def payload(path, **kwargs):
            self.assertEqual(path.parent, self.store / "end_of_day/pricing/AAA/2026")
            self.assertNotIn(path, payloads, "accepted partition decoded more than once")
            payloads.append(path)
            return artifact_read(path, **kwargs)

        with (
            patch.object(download, "read_boundary", side_effect=read),
            patch.object(activity, "validate_activity", side_effect=validate),
            patch.object(handoff, "read_artifact", side_effect=payload),
        ):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(opened, ["current_year"])
        self.assertEqual(len(payloads), 1)  # Producer-selected physical aliases decode once.
        self.assertEqual(checked, ["2026-10-02"])
        self.assertEqual([call["adjustment"] for call in self.calls], ["raw", "all"])
        item = result["results"][0]
        self.assertEqual(item["completed_scope"], "selected_sessions")
        self.assertTrue(item["retained_history_unchanged"])
        self.assertEqual(item["updated_boundaries"], ["current_year"])
        source = storage.load_source(self.store / "end_of_day/pricing/AAA", "AAA")
        self.assertEqual(source["history_correction_scope"], "selected_sessions")
        all_rows = download.read_pricing_views(self.store, "AAA")["all"]
        self.assertEqual(
            [row[0][:10] for row in all_rows],
            ["2016-01-04", "2026-02-02", "2026-10-01", "2026-10-02", "2026-11-05"],
        )

    def test_selected_partition_interruption_recovers_without_reacquisition_or_old_reads(self):
        self.seed()
        self.live = [bar("2026-10-02")]
        prepared, _ = refresh.probe_late_bar_repairs(
            self.store,
            self.selection(["2026-10-02"]),
            client=self.client,
        )
        work = self.work()
        work.require_correction(prepared)

        class Interrupted(BaseException):
            pass

        with (
            patch.object(storage, "_finish_publication", side_effect=Interrupted),
            self.assertRaises(Interrupted),
        ):
            refresh.replace_prepared(self.store, prepared, client=self.client)
        root = self.store / "end_of_day/pricing/AAA"
        self.assertTrue((root / ".pending.json").exists())
        with (
            storage.ticker_lock(root, exclusive=True),
            patch.object(download, "read_boundary", side_effect=AssertionError("accepted history reopened")),
        ):
            storage.recover(root)
        recovered = refresh.prepare_refresh(
            self.store, ["AAA"], correction_ids={"AAA": work.work["AAA"]["correction_id"]}
        )
        with (
            patch.object(
                self.client, "execute_plan", side_effect=AssertionError("completed acquisition repeated")
            ),
            patch.object(download, "read_boundary", side_effect=AssertionError("accepted history reopened")),
        ):
            result = refresh.replace_prepared(self.store, recovered, client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(result["results"][0]["completed_scope"], "selected_sessions")
        self.assertFalse((root / ".pending.json").exists())

    def test_batch_rejection_retains_completed_raw_stage_for_exact_fallback(self):
        self.seed("AAA")
        self.seed("BBB")
        self.live = [bar("2016-01-04", "500"), *self.old[1:]]
        original = self.page

        def reject(params):
            if params["symbols"] == "AAA,BBB" and params["adjustment"] == "all":
                self.calls.append(dict(params))
                raise HTTPError("fixture", 400, "batch rejected", {}, None)
            return original(params)

        prepared = refresh.prepare_refresh(self.store, ["AAA", "BBB"])
        self.work().require_correction(prepared)
        with patch.object(self.client, "_get", side_effect=reject):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 0, result)
        acquisition_raw = [
            call for call in self.calls if call["adjustment"] == "raw" and call["start"][:10] == "2016-01-01"
        ]
        self.assertEqual([call["symbols"] for call in acquisition_raw], ["AAA,BBB"])
        self.assertEqual(
            {call["symbols"] for call in self.calls if call["adjustment"] == "all"}, {"AAA,BBB", "AAA", "BBB"}
        )
        for item in result["results"]:
            self.assertTrue(item["changed"])
            self.assertEqual(item["completed_scope"], "full_history")

    def test_probe_retry_consumes_checkpoint_without_repeating_owner_merge(self):
        self.seed()
        self.live = [bar("2026-10-02"), bar("2026-10-06")]
        selected = self.selection(["2026-10-02", "2026-10-06"])
        work, context = self.work(), accept_context(self.store)

        class Interrupted(BaseException):
            pass

        original = self.page

        def interrupt(params):
            if params["start"][:10] == "2026-10-06":
                raise Interrupted()
            return original(params)

        with patch.object(self.client, "_get", side_effect=interrupt), self.assertRaises(Interrupted):
            refresh.probe_late_bar_repairs(
                self.store,
                selected,
                client=self.client,
                context=context,
                on_positive=work.require_correction,
                correction_requirements=work.work,
            )
        self.calls.clear()
        with patch.object(work, "require_correction", wraps=work.require_correction) as owner:
            prepared, report = refresh.probe_late_bar_repairs(
                self.store,
                selected,
                client=self.client,
                context=context,
                on_positive=owner,
                correction_requirements=work.work,
            )
        self.assertEqual(owner.call_count, 1)  # Only the newly completed second range.
        self.assertEqual([call["start"][:10] for call in self.calls], ["2026-10-06"])
        self.assertEqual(report["reused_ranges"], 1)
        self.assertEqual(len(prepared[0]["correction_observations"]["raw"]), 2)
        self.calls.clear()
        with patch.object(
            work, "require_correction", side_effect=AssertionError("completed merge repeated")
        ) as owner:
            again, report = refresh.probe_late_bar_repairs(
                self.store,
                selected,
                client=self.client,
                context=context,
                on_positive=owner,
                correction_requirements=work.work,
            )
        self.assertEqual(self.calls, [])
        self.assertEqual(again[0]["required_correction_id"], prepared[0]["required_correction_id"])

    def test_changed_requirement_reuses_response_but_renews_owning_handoff_once(self):
        self.seed()
        self.live = [bar("2026-10-02")]
        selected = self.selection(["2026-10-02"])
        work, context = self.work(), accept_context(self.store)
        prepared, _ = refresh.probe_late_bar_repairs(
            self.store,
            selected,
            client=self.client,
            context=context,
            on_positive=work.require_correction,
            correction_requirements=work.work,
        )
        previous = prepared[0]["required_correction_id"]
        item = {**work.work["AAA"], "correction_id": "renewed-requirement"}
        work.work["AAA"] = universe_state.put_work(self.store, "AAA", item)
        with (
            patch.object(
                self.client, "execute_plan", side_effect=AssertionError("completed response requested")
            ),
            patch.object(work, "require_correction", wraps=work.require_correction) as owner,
        ):
            renewed, _ = refresh.probe_late_bar_repairs(
                self.store,
                selected,
                client=self.client,
                context=context,
                on_positive=owner,
                correction_requirements=work.work,
            )
        self.assertEqual(owner.call_count, 1)
        self.assertNotEqual(renewed[0]["required_correction_id"], previous)
        self.assertEqual(renewed[0]["required_correction_id"], "renewed-requirement")

    def test_selected_sessions_across_partitions_read_each_selected_body_once(self):
        self.seed()
        days = ["2016-01-05", "2026-10-02", "2026-11-06"]
        self.live = [bar(day) for day in days]
        prepared, _ = refresh.probe_late_bar_repairs(self.store, self.selection(days), client=self.client)
        self.work().require_correction(prepared)
        original, opened = download.read_boundary, []

        def read(root, source, kind, **kwargs):
            opened.append(kind)
            return original(root, source, kind, **kwargs)

        with patch.object(download, "read_boundary", side_effect=read):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertCountEqual(opened, ["completed_years", "current_year", "current_month"])
        source = storage.load_source(self.store / "end_of_day/pricing/AAA", "AAA")
        points = storage.read_validator(self.store / "end_of_day/pricing/AAA", source)
        self.assertEqual(
            [row[0][:10] for row in points.tail_all], sorted([r["t"][:10] for r in self.old] + days)
        )
        self.assertEqual(len(self.calls), 6)  # Three completed raw probes, three all acquisitions.

    def test_selected_acquisition_generation_change_rejects_before_partition_assembly(self):
        self.seed()
        self.live = [bar("2026-10-02")]
        prepared, _ = refresh.probe_late_bar_repairs(
            self.store,
            self.selection(["2026-10-02"]),
            client=self.client,
        )
        self.work().require_correction(prepared)

        def changed():
            download.download_pricing(
                self.store,
                "AAA",
                self.end,
                client=FakeClient(self.old),
                refresh_all=True,
                mapping_asof="2026-11-06",
            )

        with patch.object(download, "read_boundary", side_effect=AssertionError("stale payload opened")):
            result = refresh.replace_prepared(self.store, prepared, client=self.client, before_publish=changed)
        self.assertEqual(result["failed"], 1, result)
        self.assertIn("source changed", result["results"][0]["error"])

    def test_batch_rejection_selected_session_fallback_keeps_raw_probe(self):
        self.seed("AAA")
        self.seed("BBB")
        self.live = [bar("2026-10-02")]
        prepared, _ = refresh.probe_late_bar_repairs(
            self.store,
            self.selection(["2026-10-02"], ("AAA", "BBB")),
            client=self.client,
        )
        self.work().require_correction(prepared)
        original = self.page

        def reject(params):
            if params["symbols"] == "AAA,BBB" and params["adjustment"] == "all":
                self.calls.append(dict(params))
                raise HTTPError("fixture", 400, "batch rejected", {}, None)
            return original(params)

        with patch.object(self.client, "_get", side_effect=reject):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual(
            [(call["symbols"], call["adjustment"]) for call in self.calls],
            [("AAA,BBB", "raw"), ("AAA,BBB", "all"), ("AAA", "all"), ("BBB", "all")],
        )

    def test_access_denial_is_terminal_instead_of_symbol_fallback(self):
        self.seed("AAA")
        self.seed("BBB")
        prepared = refresh.prepare_refresh(self.store, ["AAA", "BBB"])
        self.work().require_correction(prepared)
        original = self.page

        def denied(params):
            if params["adjustment"] == "all":
                self.calls.append(dict(params))
                raise HTTPError("fixture", 403, "denied", {}, None)
            return original(params)

        with patch.object(self.client, "_get", side_effect=denied):
            result = refresh.replace_prepared(self.store, prepared, client=self.client)
        self.assertEqual(result["failed"], 2, result)
        self.assertEqual([call["symbols"] for call in self.calls], ["AAA,BBB", "AAA,BBB"])

    def test_ordinary_append_and_rollover_validate_only_new_acquisition_rows(self):
        for target, day in (
            ("2026-11-10", "2026-11-09"),
            ("2026-12-02", "2026-12-01"),
            ("2027-01-05", "2027-01-04"),
        ):
            with self.subTest(target=target):
                self.store = self.store.parent / target
                seeded = self.seed()
                root = self.store / "end_of_day/pricing/AAA"
                source = storage.load_source(root, "AAA")
                entry = price_entry(root, source, source_sha256=seeded["source_sha256"])
                self.live = [bar(day)]
                checked = []
                validate = activity.validate_activity

                def new_only(views):
                    checked.extend(row[0][:10] for row in views["raw"])
                    self.assertTrue(
                        all(row[0][:10] == day for rows in views.values() for row in rows),
                        "accepted row revalidated",
                    )
                    validate(views)

                with patch.object(activity, "validate_activity", side_effect=new_only):
                    result = append.append_entries(
                        self.store,
                        [entry],
                        target,
                        client=self.client,
                        approved_generations={"AAA": seeded["source_sha256"]},
                        workers=1,
                    )
                self.assertEqual(result["failed"], 0, result)
                self.assertEqual(checked, [day])


if __name__ == "__main__":
    unittest.main()

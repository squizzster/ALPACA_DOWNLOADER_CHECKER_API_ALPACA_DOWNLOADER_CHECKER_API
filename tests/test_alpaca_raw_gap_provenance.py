"""Unresolved lineage must not turn present raw observations into price repairs."""

import copy
import json
import unittest
from contextlib import closing
from unittest.mock import patch

import test_alpaca_end_of_day_api as native
import test_alpaca_serving_incremental as incremental
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import producer, storage, work
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.sqlite_connections import connect_sqlite


class RawGapProvenanceTests(unittest.TestCase):
    def test_present_unassigned_raw_is_not_scheduled_again_in_multi_identity_group(self):
        case = incremental.IncrementalServingTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.add_symbol("OTHER")
        day = "2026-10-06"
        days = [row["t"][:10] for row in case.bars]

        def unassign(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    replacement = []
                    for segment in entry["segments"]:
                        selected = [d for d in days if segment["from"] <= d <= segment["to"]]
                        if day not in selected:
                            replacement.append(segment)
                            continue
                        index = selected.index(day)
                        for lo, hi in ((0, index), (index, index + 1), (index + 1, len(selected))):
                            if lo == hi:
                                continue
                            part = copy.deepcopy(segment)
                            part.update(
                                row_start=segment["row_start"] + lo,
                                row_end=segment["row_start"] + hi - 1,
                                row_count=hi - lo,
                                **{"from": selected[lo], "to": selected[hi - 1]},
                            )
                            if lo == index:
                                part["branch"] = "uncorrelated"
                                part.pop("security_identity", None)
                                part["candidate_securities"] = [{"asset_id": "TEST-id"}]
                            replacement.append(part)
                    entry["segments"] = replacement

        case.publish_change(unassign)
        case.connect_group("TEST", "OTHER")
        case.bootstrap()
        controls = storage.controls(case.store)
        self.assertEqual(storage.select_late_bar_repairs(case.store, controls)["sources"], {})
        group = case.catalogue_groups("OTHER")["OTHER"]
        self.assertTrue(any("unexplained_missing_sessions" in d["reasons"] for d in group["decisions"]))
        self.assertEqual(group["history_validation"]["raw_gap_presence"]["TEST"]["present_sessions"], [day])

    def legacy_projection(self):
        case = native.NativeCase()
        case.setUp()
        self.addCleanup(case.doCleanups)
        day = "2026-10-06"

        class AdjustedPresence(FakeClient):
            def fetch(self, symbol, adjustment, start, end, **kwargs):
                rows = super().fetch(symbol, adjustment, start, end, **kwargs)
                return [row for row in rows if row["t"][:10] != day] if adjustment == "raw" else rows

        download_pricing(case.store, "GAP", "2026-10-14", client=AdjustedPresence(case.bars))
        self.assertEqual(
            run_lineage(case.store, ["GAP"], identity_client=IdentityClient({"GAP": asset("GAP")}))["failed"], 0
        )
        case.bootstrap()
        expected = case.catalogue_groups("GAP")["GAP"]
        with closing(connect_sqlite(storage.path(case.store))) as db, db:
            body = copy.deepcopy(expected)
            body["history_validation"].pop("raw_gap_presence")
            selection = json.loads(
                db.execute("SELECT selection FROM groups WHERE group_key='GAP'").fetchone()[0]
            )
            selection["late_bar_repairs"] = {
                "version": 1,
                "selected_groups": 0,
                "selected_identities": 0,
                "sources": {},
            }
            db.execute(
                "UPDATE groups SET body=?, selection=? WHERE group_key='GAP'",
                (storage.encoded(body), storage.encoded(selection)),
            )
            selection = json.loads(
                db.execute("SELECT selection FROM groups WHERE group_key='TEST'").fetchone()[0]
            )
            selection["late_bar_repairs"]["version"] = 1
            db.execute("UPDATE groups SET selection=? WHERE group_key='TEST'", (storage.encoded(selection),))
        return case, day, expected

    def test_legacy_empty_projection_is_converted_using_raw_not_adjusted_presence(self):
        case, day, expected = self.legacy_projection()
        # An unrelated legitimate obligation must survive projection conversion.
        case.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new metadata"]))
        pending = work.read(case.store)
        with (
            patch.object(producer, "evaluate", side_effect=AssertionError("numeric replay")),
            patch.object(StandaloneReader, "_open_group", side_effect=AssertionError("lineage reopened")),
        ):
            result = producer.maintain(case.store, rebuild=True)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(work.read(case.store), pending)
        selected = storage.select_late_bar_repairs(case.store, storage.controls(case.store))
        self.assertEqual(selected["sources"]["GAP"]["sessions"], [day])
        current = case.catalogue_groups("GAP")["GAP"]
        self.assertEqual(current["decision_generation"], expected["decision_generation"])
        self.assertEqual(current["decisions"], expected["decisions"])

    def test_projection_conversion_resumes_without_reopening_completed_raw_scope(self):
        case, day, expected = self.legacy_projection()
        publish = storage.publish_projection

        def interrupt(*args, **kwargs):
            publish(*args, **kwargs)
            raise RuntimeError("interrupted after projection commit")

        with patch.object(storage, "publish_projection", side_effect=interrupt):
            with self.assertRaisesRegex(RuntimeError, "after projection commit"):
                producer.maintain(case.store, rebuild=True)
        with (
            patch(
                "alpaca_end_of_day._standalone_group.raw_source_presence",
                side_effect=AssertionError("raw reread"),
            ),
            patch.object(producer, "evaluate", side_effect=AssertionError("numeric replay")),
        ):
            result = producer.maintain(case.store, rebuild=True)
        self.assertEqual(result["projection_groups"], 1)
        self.assertEqual(result["projection_source_reads"], 0)
        selected = storage.select_late_bar_repairs(case.store, storage.controls(case.store))
        self.assertEqual(selected["sources"]["GAP"]["sessions"], [day])
        self.assertEqual(
            case.catalogue_groups("GAP")["GAP"]["decision_generation"], expected["decision_generation"]
        )

    def test_projection_conversion_rejects_changed_source_before_raw_reads(self):
        case, _, _ = self.legacy_projection()
        download_pricing(case.store, "GAP", "2026-10-14", client=FakeClient(case.bars), refresh_all=True)
        before = storage.controls(case.store)
        with patch(
            "alpaca_end_of_day._standalone_group.raw_source_presence",
            side_effect=AssertionError("stale raw read"),
        ):
            with self.assertRaisesRegex(CatalogueError, "raw projection input changed"):
                producer.maintain(case.store, rebuild=True)
        self.assertEqual(storage.controls(case.store), before)

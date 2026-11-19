"""Published ranges survive an earlier gap and rejoin after its actual fill."""

import csv
import io
import unittest
from contextlib import closing
from unittest.mock import patch

import test_alpaca_serving_infill as infill
from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_universe_update import CatalogueClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_lineage_build.refresh import refresh_existing
from libs.alpaca_lineage_build.universe_update import rebuild_universe, run_update
from libs.alpaca_pricing_download.contract import NEW_YORK, timestamp
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_serving_catalogue import policy, producer, storage, work
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.sqlite_connections import connect_sqlite


def repairs(store):
    controls = storage.controls(store)
    with patch.object(storage, "read_groups", side_effect=AssertionError("opened a decided group")):
        result = storage.select_late_bar_repairs(store, controls)
    return {symbol: item["sessions"] for symbol, item in result["sources"].items()}


class ServingGapRejoinTests(NativeCase):
    def test_public_range_and_raw_retry_survive_restart_until_zero_volume_fill(self):
        end = "2026-10-31"
        bars = valid_bars(end=end)
        days = [bar["t"][:10] for bar in bars]
        gap = days[2]
        sparse = [bar for bar in bars if bar["t"][:10] != gap]
        self.add_symbol("TEST", bars, end=end)
        self.add_symbol("LATER", sparse, end=end)
        assets = {s: asset(s) for s in ("LATER", "TEST")}
        self.assertEqual(run_lineage(self.store, identity_client=IdentityClient(assets))["failed"], 0)

        # Split the sole October segment into two completed lineage selections.
        # A subsequent lineage publication assigns the earlier positions.
        def only_recent(_, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if view["segments"]:
                        (old,) = view["segments"]
                        early = dict(
                            old,
                            row_end=2,
                            row_count=2,
                            last_timestamp=sparse[1]["t"],
                            to=days[1],
                            observation_selection="superseded",
                        )
                        recent = dict(
                            old,
                            row_start=3,
                            row_count=old["row_count"] - 2,
                            first_timestamp=sparse[2]["t"],
                            **{"from": days[3]},
                        )
                        view["segments"] = [early, recent]

        self.publish_change(only_recent, "LATER")
        initial = self.bootstrap()
        (resolution,) = initial.resolve_ticker("LATER")
        public_id = resolution.public_id
        accepted = {k: v.bars for k, v in initial.get_histories(public_id).items()}
        self.assertEqual(days[3:], [b.session.isoformat() for b in accepted["raw"]])

        def include_earlier(_, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment.pop("observation_selection", None)

        self.publish_change(include_earlier, "LATER")

        class Interrupted(BaseException):
            pass

        check_bar = policy._bar_reasons

        def check_prepend(bar, adjustment):
            self.assertIn(bar["session"], days[:2], "verified the published suffix again")
            return check_bar(bar, adjustment)

        with (
            patch.object(policy, "_bar_reasons", side_effect=check_prepend),
            patch.object(work, "acknowledge", side_effect=Interrupted),
            self.assertRaises(Interrupted),
        ):
            maintain(self.store)
        self.assertEqual({"LATER": [gap]}, repairs(self.store))
        with patch.object(producer, "evaluate", side_effect=AssertionError("replayed committed range")):
            recovered = maintain(self.store)
        self.assertTrue(recovered["complete"])
        self.assertEqual(recovered["published_components"], 0)
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute(
                "UPDATE groups SET selection=json_remove(selection, '$.late_bar_repairs') WHERE group_key='LATER'"
            )
        with self.assertRaisesRegex(storage.CatalogueError, "catalogue rebuild"):
            repairs(self.store)
        with (
            patch.object(producer, "evaluate", side_effect=AssertionError("replayed partial history")),
            patch(
                "alpaca_end_of_day._standalone_group.raw_source_presence",
                side_effect=AssertionError("reopened accepted raw"),
            ),
        ):
            migration = maintain(self.store, rebuild=True)
        self.assertEqual((migration["projection_groups"], migration["projection_source_reads"]), (1, 0))
        self.assertEqual({"LATER": [gap]}, repairs(self.store))
        partial = DataLibrary(store=self.store)
        self.assertEqual(accepted, {k: v.bars for k, v in partial.get_histories(public_id).items()})
        self.assertEqual(partial.resolve_ticker("LATER", on=gap), ())
        self.assertEqual(partial.get_security(public_id).stored_session_count, len(days[3:]))
        stream = io.StringIO()
        partial.write_flat_online_csv(stream)
        exported = [row for row in csv.DictReader(io.StringIO(stream.getvalue())) if row["ticker"] == "LATER"]
        self.assertEqual(days[3:], [row["date"] for row in exported])

        rebuild(self.store)
        rebuild_universe(self.store)
        registry = self.directory / "tickers.csv"
        registry.write_text("LATER,TEST\n")
        live = {"TEST": bars, "LATER": sparse}
        requests = []
        client = BarsClient("fake", "fake")

        def page(params):
            start, stop = timestamp(params["start"]), timestamp(params["end"])
            dates = [v.astimezone(NEW_YORK).date().isoformat() for v in (start, stop)]
            requests.append((params["symbols"], params["adjustment"], *dates))
            return {
                "bars": {
                    symbol: [r for r in live[symbol] if start <= timestamp(r["t"]) <= stop]
                    for symbol in params["symbols"].split(",")
                }
            }

        def update():
            result = run_update(
                self.store,
                end,
                pricing_client=client,
                identity_client=CatalogueClient(list(assets.values())),
                registry=registry,
            )
            self.assertTrue(result["complete"], result.get("failures"))
            return result["stages"]["serving_catalogue"]

        comparison = [
            ("LATER,TEST", "all", days[0], days[0]),
            ("LATER,TEST", "all", days[-10], days[-1]),
        ]
        with patch.object(client, "_get", side_effect=page):
            self.assertEqual(0, update()["published_components"])
            self.assertCountEqual([*comparison, ("LATER", "raw", gap, gap)], requests)
            self.assertEqual({"LATER": [gap]}, repairs(self.store))
            requests.clear()
            live["LATER"] = [{**bar, "v": 0, "n": 0, "vw": 0} if bar["t"][:10] == gap else bar for bar in bars]

            def check_insert(bar, adjustment):
                self.assertEqual(bar["session"], gap, "verified a retained price after the selected insertion")
                return check_bar(bar, adjustment)

            with (
                patch.object(policy, "_bar_reasons", side_effect=check_insert),
                patch.object(work, "acknowledge", side_effect=Interrupted),
                self.assertRaises(Interrupted),
            ):
                update()
            self.assertEqual(repairs(self.store), {})  # Public range and resolved retry committed together.
            self.assertCountEqual(
                [*comparison, ("LATER", "raw", gap, gap), ("LATER", "all", gap, gap)], requests
            )
            requests.clear()
            with patch.object(producer, "evaluate", side_effect=AssertionError("replayed the joined range")):
                self.assertEqual(0, update()["published_components"])
            self.assertCountEqual(comparison, requests)
        self.assertEqual(repairs(self.store), {})
        full = DataLibrary(store=self.store)
        self.assertEqual(full.get_security(public_id).stored_session_count, len(days))
        for name, history in full.get_histories(public_id).items():
            self.assertEqual([b.session.isoformat() for b in history.bars], days)
            self.assertEqual(history.bars[3:], accepted[name])
            self.assertEqual((history.bars[2].volume, history.bars[2].trade_count), (0, 0))

    def test_insertion_expires_a_comparison_that_required_the_replaced_lineage_route(self):
        end, gap, first = "2026-10-31", "2026-10-16", "2026-10-01"
        bars = valid_bars(end=end)
        sparse = [bar for bar in bars if bar["t"][:10] != gap]
        for symbol in ("TEST", "OTHER"):
            self.add_symbol(symbol, sparse, end=end)
        current = asset("OTHER")
        rename = {
            "action_type": "name_change",
            "id": "same-issue",
            "process_date": "2026-10-06",
            "old_symbol": "TEST",
            "new_symbol": "OTHER",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        identity = IdentityClient({"TEST": current, "OTHER": current, "111111111": current}, [rename])
        built = run_lineage(self.store, ["OTHER", "TEST"], refresh=True, identity_client=identity)
        self.assertEqual(built["failed"], 0)
        rebuild(self.store)
        self.bootstrap()
        selected = storage.select_late_bar_repairs(self.store, storage.controls(self.store))
        client, context = BarsClient("fake", "fake"), OperationContext(self.store)
        live = {"TEST": sparse, "OTHER": [b for b in sparse if b["t"][:10] != first]}
        requests = []

        def page(params):
            start, stop = timestamp(params["start"]), timestamp(params["end"])
            symbol = params["symbols"]
            requests.append((symbol, params["adjustment"], params["asof"], start.date().isoformat()))
            return {"bars": {symbol: [bar for bar in live[symbol] if start <= timestamp(bar["t"]) <= stop]}}

        def refresh(repairs=None):
            return refresh_existing(
                self.store,
                end_date=end,
                pricing_client=client,
                identity_client=identity,
                context=context,
                symbols={"OTHER"},
                late_bar_repairs=repairs,
            )

        with patch.object(client, "_get", side_effect=page):
            self.assertTrue(refresh()["complete"])
            self.assertIn(("TEST", "all", first, first), requests)
            original_route = context.history_comparisons["OTHER"][3]
            live["OTHER"] = [bar for bar in bars if bar["t"][:10] != first]
            requests.clear()
            result = refresh(selected)
        self.assertTrue(result["complete"])
        self.assertIn(("TEST", "all", first, first), requests)
        self.assertNotEqual(original_route, context.history_comparisons["OTHER"][3])
        self.assertEqual(requests.count(("OTHER", "raw", end, gap)), 1)


class AssignedGapRangeTests(unittest.TestCase):
    def setUp(self):
        self.case = infill.ServingInfillTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def test_only_contiguous_range_expands_after_each_of_multiple_fills(self):
        case = self.case
        case.initial({7, 8}, set())
        positions = {0, 2, 4, 5}
        for addition, first in ((set(), 7), ({6}, 4), ({3}, 2), ({1}, 0)):
            positions |= addition
            case.select("TEST", positions)
            checks, reads = case.allow_new_only(addition or positions)
            with checks, reads:
                result = maintain(case.store)
            self.assertTrue(result["complete"], result.get("failures"))
            data = DataLibrary(store=case.store)
            self.assertEqual(data.get_security(case.public_id).first_session.isoformat(), case.days[first])
            self.assertEqual(
                [b.session.isoformat() for b in data.get_history(case.public_id).bars], case.days[first:]
            )
            self.assertEqual(repairs(case.store), {})  # Raw already exists locally for each gap.

    def test_newer_gap_cannot_move_publication_start_past_previously_accepted_rows(self):
        case = self.case
        case.initial({2, 3}, set())
        case.select("TEST", {6, 7})
        self.assertTrue(maintain(case.store)["complete"])
        self.assertFalse(DataLibrary(store=case.store).list_securities())

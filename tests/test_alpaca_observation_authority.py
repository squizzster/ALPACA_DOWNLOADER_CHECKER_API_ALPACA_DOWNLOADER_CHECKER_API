"""Producer-owned mapped observations, coherent views and bounded reuse."""

import csv
import io
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, snapshot

from alpaca_end_of_day import StandaloneReader
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import storage
from libs.alpaca_pricing_download.acquire import acquire_selected
from libs.alpaca_pricing_download.activity import validate_activity
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, FIELDS, DownloadError, timestamp
from libs.alpaca_pricing_download.download import _endpoint_window, download_pricing
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_pricing_download.refresh import PlannedClient, prepare_refresh
from libs.alpaca_pricing_download.responses import CompletedBars, fetch_rows
from libs.alpaca_pricing_download.storage import load_source


def views_of(bars):
    return {name: tuple(tuple(str(bar[field]) for field in FIELDS) for bar in bars) for name in ADJUSTMENTS}


class ActivityContractTests(TestCase):
    def setUp(self):
        self.views = views_of(valid_bars())

    def changed(self, name, column, value):
        views = deepcopy(self.views)
        row = list(views[name][1])
        row[column] = value
        views[name] = (views[name][0], tuple(row), *views[name][2:])
        return views

    def test_every_adjustment_preserves_execution_count(self):
        for name in ADJUSTMENTS[1:]:
            with self.subTest(adjustment=name):
                with self.assertRaisesRegex(DownloadError, "trade count"):
                    validate_activity(self.changed(name, 6, "8"))

    def test_every_unsplit_view_preserves_exact_share_units(self):
        for name in ("dividend", "spin-off", "dividend,spin-off"):
            with self.subTest(adjustment=name):
                with self.assertRaisesRegex(DownloadError, "share volume"):
                    validate_activity(self.changed(name, 5, "9007199254740993.6"))

    def test_split_volume_may_round_to_zero(self):
        for name in ("split", "split,dividend", "split,spin-off", "all"):
            with self.subTest(adjustment=name):
                validate_activity(self.changed(name, 5, "0"))

    def test_decimal_spelling_does_not_change_activity(self):
        validate_activity(self.changed("dividend", 5, "9007199254740993.5000"))
        validate_activity(self.changed("all", 6, "7.000"))

    def test_missing_observation_is_not_a_synthetic_zero(self):
        views = {**self.views, "all": self.views["all"][1:]}
        validate_activity(views)
        self.assertEqual(len(views["all"]), len(self.views["all"]) - 1)


class ObservationSelectionTests(TestCase):
    def fixture(self, *, retired=False, old_count=2, new_count=9):
        days = valid_bars("2026-10-01", "2026-10-14")
        inputs, reports = {}, {}
        for symbol in ("OLD", "NEW"):
            bars = deepcopy(days if symbol == "NEW" else days[:4])
            bars[1]["n"] = old_count if symbol == "OLD" else new_count
            bars[1]["v"] = bars[1]["n"] * 100
            if retired and symbol == "NEW":
                bars = bars[:-2]
            views = views_of(bars)
            inputs[symbol] = SimpleNamespace(
                views=views,
                source_sha256=symbol.lower() * 16,
                source={
                    "history_start": "2016-01-01T00:00:00Z",
                    "latest_request": {"asof": "2026-10-14", "end_exclusive": "2026-10-14T00:00:00Z"},
                },
            )
            segments = []
            for name in ADJUSTMENTS:
                for start, end, label, lower, upper in (
                    (1, min(4, len(bars)), "OLD", None, "2026-10-07"),
                    (5, len(bars), "NEW", "2026-10-07", "2026-10-12" if retired else None),
                ):
                    if start > end:
                        continue
                    segments.append(
                        {
                            "adjustment": name,
                            "row_start": start,
                            "row_end": end,
                            "historical_symbol": label,
                            "security_identity": {"security_id": "id"},
                            "ticker_ownership_interval": {
                                "start_date_inclusive": lower,
                                "end_date_exclusive": upper,
                            },
                        }
                    )
            reports[symbol] = {"correlated": {"segments": segments}}
        return inputs, reports

    def test_one_source_supplies_every_view_independent_of_larger_counts(self):
        for old_count, new_count in ((2, 9), (9, 2)):
            with self.subTest(old_count=old_count):
                inputs, reports = self.fixture(old_count=old_count, new_count=new_count)
                before = deepcopy(reports)
                chosen, work = select_observations(inputs, reports)
                self.assertFalse(work)
                self.assertEqual(reports, before)
                self.assertEqual(
                    {s["observation_selection"] for s in chosen["OLD"]["correlated"]["segments"]}, {"superseded"}
                )
                self.assertEqual(
                    {s["observation_selection"] for s in chosen["NEW"]["correlated"]["segments"]}, {"selected"}
                )
                self.assertTrue(
                    all(
                        s["security_identity"]["security_id"] == "id"
                        for r in chosen.values()
                        for s in r["correlated"]["segments"]
                    )
                )

    def test_retired_preferred_route_requires_only_its_bound_mapping_replacement(self):
        inputs, reports = self.fixture(retired=True)
        _, work = select_observations(inputs, reports)
        self.assertEqual(set(work), {"NEW"})
        self.assertEqual(work["NEW"]["mapping_asof"], "2026-10-09")
        self.assertEqual(work["NEW"]["selected_source_sha256"], inputs["NEW"].source_sha256)
        self.assertEqual(work["NEW"]["end_exclusive"], "2026-10-14")
        inputs["NEW"].source["latest_request"]["asof"] = "2026-10-09"
        self.assertEqual(select_observations(inputs, reports)[1], {})

    def test_equal_raw_cannot_certify_an_out_of_occupancy_adjustment_route(self):
        inputs, reports = self.fixture(retired=True, old_count=7, new_count=7)
        self.assertEqual(select_observations(inputs, reports)[1], {})
        for name in ("all", "split"):
            inputs["NEW"].views[name] = tuple(
                (row[0], *(str(Decimal(v) * 15) for v in row[1:5]), row[5], row[6], row[7])
                for row in inputs["NEW"].views[name]
            )
        self.assertEqual(set(select_observations(inputs, reports)[1]), {"NEW"})

    def test_selected_source_preserves_independently_accepted_label_ambiguity(self):
        inputs, reports = self.fixture(old_count=7, new_count=7)
        for segment in reports["NEW"]["correlated"]["segments"]:
            segment["historical_symbol"] = "NEW"
            segment["ticker_ownership_interval"]["start_date_inclusive"] = None
        chosen, corrections = select_observations(inputs, reports)
        self.assertFalse(corrections)
        raw = [s for s in chosen["NEW"]["correlated"]["segments"] if s["adjustment"] == "raw"]
        self.assertEqual(raw[0]["observation_tickers"], ["NEW", "OLD"])
        self.assertEqual(raw[-1]["observation_tickers"], ["NEW"])
        self.assertEqual((raw[0]["row_start"], raw[0]["row_end"]), (1, 4))

    def test_sole_retired_candidate_does_not_trigger_speculative_replacement(self):
        inputs, reports = self.fixture(retired=True)
        inputs.pop("OLD")
        reports.pop("OLD")
        self.assertEqual(select_observations(inputs, reports)[1], {})

    def test_same_identity_cusip_change_carries_the_accepted_departure(self):
        inputs, reports = self.fixture()
        for segment in reports["NEW"]["correlated"]["segments"]:
            segment["security_identity"]["member_alpaca_asset_ids"] = ["before-split", "after-split"]
        interval = {
            "historical_symbol": "NEW",
            "asset_id": "after-split",
            "start_date_inclusive": None,
            "end_date_exclusive": "2026-10-14",
        }
        reports["NEW"]["ticker_ownership_intervals"] = [interval]
        self.assertEqual(select_observations(inputs, reports)[1]["NEW"]["mapping_asof"], "2026-10-13")
        interval["asset_id"] = "unrelated-occupant"
        self.assertEqual(select_observations(inputs, reports)[1], {})
        interval["asset_id"] = "after-split"
        interval["end_date_exclusive"] = "2026-10-09"
        self.assertEqual(select_observations(inputs, reports)[1], {})

    def test_reused_source_cannot_be_globally_remapped_to_one_occupant(self):
        inputs, reports = self.fixture(retired=True)
        segments = []
        for segment in reports["NEW"]["correlated"]["segments"]:
            if segment["row_start"] == 1:
                segments.append(
                    {**segment, "row_end": 1, "security_identity": {"security_id": "different-occupant"}}
                )
                segment = {**segment, "row_start": 2}
            segments.append(segment)
        reports["NEW"]["correlated"]["segments"] = segments
        with self.assertRaisesRegex(LineageError, "unambiguous mapping"):
            select_observations(inputs, reports)

    def test_mapping_interval_is_start_inclusive_end_exclusive(self):
        for asof, correction in (
            ("2026-10-06", True),
            ("2026-10-07", False),
            ("2026-10-11", False),
            ("2026-10-12", True),
        ):
            with self.subTest(asof=asof):
                inputs, reports = self.fixture(retired=True)
                inputs["NEW"].source["latest_request"]["asof"] = asof
                self.assertEqual(bool(select_observations(inputs, reports)[1]), correction)

    def reused_source(self, *, later_label="NEW"):
        inputs, reports = self.fixture(retired=True)
        earlier_count = len(inputs["NEW"].views["raw"])
        later = views_of(valid_bars("2026-10-12", "2026-10-14"))
        for name in ADJUSTMENTS:
            inputs["NEW"].views[name] += later[name]
            reports["NEW"]["correlated"]["segments"].append(
                {
                    "adjustment": name,
                    "row_start": earlier_count + 1,
                    "row_end": earlier_count + len(later[name]),
                    "historical_symbol": later_label,
                    "security_identity": {"security_id": "later-id"},
                    "ticker_ownership_interval": {},
                }
            )
        return inputs, reports

    def test_older_occupant_closure_does_not_remap_later_own_label_history(self):
        inputs, reports = self.reused_source()
        before = deepcopy(reports)
        chosen, work = select_observations(inputs, reports)
        self.assertEqual(work, {})
        self.assertEqual(reports, before)
        selected = chosen["NEW"]["correlated"]["segments"]
        self.assertEqual({s["security_identity"]["security_id"] for s in selected}, {"id", "later-id"})
        self.assertEqual({s["observation_selection"] for s in selected}, {"selected"})
        self.assertEqual(
            {s["observation_selection"] for s in chosen["OLD"]["correlated"]["segments"]},
            {"superseded"},
        )

    def test_later_occupant_need_not_win_the_numerical_source_selection(self):
        inputs, reports = self.reused_source()
        inputs["LATER"] = deepcopy(inputs["NEW"])
        inputs["LATER"].views = views_of(valid_bars("2026-10-12", "2026-10-15"))
        reports["LATER"] = {
            "correlated": {
                "segments": [
                    {
                        "adjustment": name,
                        "row_start": 1,
                        "row_end": len(inputs["LATER"].views[name]),
                        "historical_symbol": "LATER",
                        "security_identity": {"security_id": "later-id"},
                        "ticker_ownership_interval": {},
                    }
                    for name in ADJUSTMENTS
                ]
            }
        }
        chosen, work = select_observations(inputs, reports)
        self.assertEqual(work, {})
        self.assertEqual(
            {
                s["observation_selection"]
                for s in chosen["NEW"]["correlated"]["segments"]
                if s["security_identity"]["security_id"] == "later-id"
            },
            {"superseded"},
        )

    def test_later_borrowed_label_cannot_excuse_unsafe_source_replacement(self):
        inputs, reports = self.reused_source(later_label="BORROWED")
        with self.assertRaisesRegex(LineageError, "unambiguous mapping"):
            select_observations(inputs, reports)

    def test_equal_date_different_occupant_is_not_a_later_own_label(self):
        inputs, reports = self.reused_source()
        for name in ADJUSTMENTS:
            values = inputs["NEW"].views[name]
            inputs["NEW"].views[name] = (*values[:-2], (values[-3][0], *values[-2][1:]), values[-1])
        for segment in reports["NEW"]["correlated"]["segments"]:
            if segment["security_identity"]["security_id"] == "later-id":
                segment["row_end"] = segment["row_start"]
        with self.assertRaisesRegex(LineageError, "unambiguous mapping"):
            select_observations(inputs, reports)

    def test_current_pricing_guarantee_is_not_rechecked_at_lineage(self):
        inputs, reports = self.fixture()
        for view in inputs.values():
            view.source["activity_consistent"] = True
        with patch(
            "libs.alpaca_pricing_download.activity.validate_activity", side_effect=AssertionError("rechecked")
        ):
            select_observations(inputs, reports)
            select_observations({"NEW": inputs["NEW"]}, {"NEW": reports["NEW"]})

    def test_legacy_single_source_gets_the_new_guarantee_at_migration(self):
        inputs, reports = self.fixture()
        views = inputs["NEW"].views
        row = list(views["all"][1])
        row[6] = "33"
        views["all"] = (views["all"][0], tuple(row), *views["all"][2:])
        with self.assertRaisesRegex(DownloadError, "trade count"):
            select_observations({"NEW": inputs["NEW"]}, {"NEW": reports["NEW"]})

    def test_partial_bundle_cannot_displace_complete_source(self):
        inputs, reports = self.fixture()
        reports["NEW"]["correlated"]["segments"] = [
            s for s in reports["NEW"]["correlated"]["segments"] if s["adjustment"] != "all"
        ]
        chosen, _ = select_observations(inputs, reports)
        self.assertEqual(
            {s["observation_selection"] for s in chosen["OLD"]["correlated"]["segments"]}, {"selected"}
        )

    def test_no_own_label_authority_keeps_real_disagreement_explicit(self):
        inputs, reports = self.fixture()
        for report in reports.values():
            for segment in report["correlated"]["segments"]:
                segment["historical_symbol"] = "OTHER"
        chosen, _ = select_observations(inputs, reports)
        self.assertIn("conflict", {s["observation_selection"] for s in chosen["OLD"]["correlated"]["segments"]})


class MappedProducerTests(NativeCase):
    def test_interior_single_source_count_conflict_never_reaches_publication(self):
        outer = self

        class Disagreeing(FakeClient):
            def fetch(self, symbol, adjustment, *args, **kwargs):
                rows = super().fetch(symbol, adjustment, *args, **kwargs)
                return [
                    {**row, "n": 9} if adjustment != "raw" and row["t"] == outer.bars[2]["t"] else row
                    for row in rows
                ]

        before = snapshot(self.store)
        with self.assertRaisesRegex(DownloadError, "trade count"):
            download_pricing(self.store, "TEST", "2026-10-14", client=Disagreeing(self.bars), refresh_all=True)
        self.assertEqual(snapshot(self.store), before)
        self.assertEqual(self.bootstrap().list_securities()[0].stored_session_count, len(self.bars))

    def test_mapping_cutoff_is_independent_and_repeat_is_request_free(self):
        client = FakeClient(self.bars)
        result = download_pricing(
            self.store, "TEST", "2026-10-14", client=client, refresh_all=True, mapping_asof="2026-10-02"
        )
        self.assertEqual(result["request"]["asof"], "2026-10-02")
        self.assertEqual(set(client.asofs), {"2026-10-02"})
        calls = len(client.calls)
        before = snapshot(self.store)
        self.assertEqual(download_pricing(self.store, "TEST", "2026-10-14", client=client)["mode"], "unchanged")
        self.assertEqual(len(client.calls), calls)
        self.assertEqual(snapshot(self.store), before)
        download_pricing(self.store, "TEST", "2026-10-15", client=client)
        self.assertEqual(set(client.asofs), {"2026-10-02"})

    def test_endpoint_after_mapping_date_has_a_complete_window(self):
        row = views_of(self.bars)["raw"][-1]
        start, end = _endpoint_window(row, "2026-10-02")
        self.assertLess(start, end)
        self.assertEqual((end - start).days, 1)

    def test_endpoint_window_retains_independent_utc_cutoff(self):
        row = views_of(self.bars)["raw"][-1]
        cutoff = timestamp("2026-10-14T00:00:00Z")
        start, end = _endpoint_window(row, "2026-10-02", cutoff=cutoff)
        self.assertLess(start, end)
        self.assertEqual(end, cutoff)

    def test_legacy_append_cannot_certify_untouched_cold_partitions(self):
        source_path = self.store / "end_of_day/pricing/TEST/source.manifest.json"
        source = json.loads(source_path.read_bytes())
        source.pop("activity_consistent")
        source["schema_version"] = 28
        source_path.write_text(json.dumps(source))
        download_pricing(self.store, "TEST", "2026-10-15", client=FakeClient(self.bars))
        self.assertFalse(load_source(source_path.parent, "TEST")["activity_consistent"])
        download_pricing(self.store, "TEST", "2026-10-15", client=FakeClient(self.bars), refresh_all=True)
        self.assertTrue(load_source(source_path.parent, "TEST")["activity_consistent"])

    def test_mapping_change_requires_full_replacement_and_bound_generation(self):
        before = snapshot(self.store)
        for kwargs, message in (
            ({"mapping_asof": "2026-10-02"}, "complete replacement"),
            (
                {"mapping_asof": "2026-10-02", "refresh_all": True, "selected_source_sha256": "0" * 64},
                "source changed",
            ),
        ):
            with self.subTest(kwargs=kwargs):
                client = FakeClient(self.bars)
                with self.assertRaisesRegex(DownloadError, message):
                    download_pricing(self.store, "TEST", "2026-10-14", client=client, **kwargs)
                self.assertEqual(client.calls, [])
                self.assertEqual(snapshot(self.store), before)

    def test_exact_span_refresh_preserves_historical_mapping(self):
        download_pricing(
            self.store,
            "TEST",
            "2026-10-14",
            client=FakeClient(self.bars),
            refresh_all=True,
            mapping_asof="2026-10-02",
        )
        prepared = prepare_refresh(self.store, ["TEST"])
        self.assertEqual(prepared[0]["asof"], "2026-10-02")
        # The ordinary fallback client still exercises the exact-span contract.
        download_pricing(
            self.store,
            "TEST",
            "2026-10-14",
            client=FakeClient(self.bars),
            refresh_all=True,
            expected_source_sha256=prepared[0]["source_sha256"],
        )

    def test_interrupted_mapped_publication_recovers_without_reacquisition(self):
        replace = storage.os.replace

        def interrupt(source, target):
            if Path(target).name == "source.manifest.json":
                raise OSError("interrupted mapped selector")
            return replace(source, target)

        with patch.object(storage.os, "replace", side_effect=interrupt):
            with self.assertRaisesRegex(OSError, "mapped selector"):
                download_pricing(
                    self.store,
                    "TEST",
                    "2026-10-14",
                    client=FakeClient(self.bars),
                    refresh_all=True,
                    mapping_asof="2026-10-02",
                )
        client = FakeClient([])
        result = download_pricing(self.store, "TEST", "2026-10-14", client=client)
        self.assertTrue(result["recovered"])
        self.assertEqual(result["request"]["asof"], "2026-10-02")
        self.assertFalse(client.calls)
        self.assertTrue(load_source(self.store / "end_of_day/pricing/TEST", "TEST")["activity_consistent"])

    def test_empty_pinned_source_extends_coverage_without_resetting_its_mapping(self):
        download_pricing(
            self.store, "EMPTY", "2026-10-14", client=FakeClient([]), refresh_all=True, mapping_asof="2026-10-02"
        )
        client = BarsClient("test", "test")
        with patch.object(client, "_get", return_value={"bars": {}}) as requests:
            result = acquire_selected(self.store, ["EMPTY"], "2026-10-15", client=client)
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(requests.call_args_list)
        self.assertTrue(all(call.args[0]["asof"] == "2026-10-02" for call in requests.call_args_list))
        source = load_source(self.store / "end_of_day/pricing/EMPTY", "EMPTY")
        self.assertEqual(source["latest_request"]["end_exclusive"], "2026-10-15T00:00:00Z")
        self.assertEqual(source["latest_request"]["asof"], "2026-10-02")

    def test_mapped_completed_history_and_endpoint_are_consumed_once_each(self):
        start, end = timestamp("2026-10-01T00:00:00Z"), timestamp("2026-10-14T00:00:00Z")
        rows = views_of(self.bars)["raw"]
        completed = CompletedBars("TEST", "raw", start, end, "2026-10-02", rows)
        client = FakeClient([])
        cache = {
            (False, "TEST", "raw", start, end, "2026-10-02"): completed,
            (True, "TEST", "raw", start, end, "2026-10-02"): completed,
        }
        planned = PlannedClient(client, cache)
        with patch(
            "libs.alpaca_pricing_download.responses.transform", side_effect=AssertionError("renormalized")
        ):
            self.assertEqual(
                fetch_rows(planned, "TEST", "raw", start, end, asof="2026-10-02", acquisition=True), rows
            )
            self.assertEqual(fetch_rows(planned, "TEST", "raw", start, end, asof="2026-10-02"), rows)
        self.assertFalse(cache)
        self.assertFalse(client.calls)

    def test_complete_rename_pipeline_corrects_only_retired_route_then_trusts_handoff(self):
        bars = valid_bars("2026-10-01", "2026-10-12")
        for index, row in enumerate(bars):
            row.update({key: Decimal(10 + index) for key in ("o", "h", "l", "c", "vw")})
        old, new = deepcopy(bars[:4]), deepcopy(bars)
        old[1].update(n=453, v=95381)
        new[1].update(n=2, v=200)
        assets = {name: asset("NEXT", "same-id") for name in ("OLD", "NEW", "NEXT", "same-cusip")}
        actions = [
            {
                "id": "first",
                "action_type": "name_change",
                "old_symbol": "OLD",
                "new_symbol": "NEW",
                "old_cusip": "same-cusip",
                "new_cusip": "same-cusip",
                "process_date": "2026-10-07",
            },
            {
                "id": "second",
                "action_type": "name_change",
                "old_symbol": "NEW",
                "new_symbol": "NEXT",
                "old_cusip": "same-cusip",
                "new_cusip": "same-cusip",
                "process_date": "2026-10-12",
            },
        ]
        for symbol, values in (("OLD", old), ("NEW", new), ("NEXT", [])):
            download_pricing(self.store, symbol, "2026-10-14", client=FakeClient(values))
        preserved = snapshot(self.store / "end_of_day/pricing/OLD")
        fixed = deepcopy(new)
        fixed[1] = old[1]

        class Mapped(FakeClient):
            def fetch(self, symbol, adjustment, start, end, *, asof=None):
                if symbol != "NEW" or asof != "2026-10-09":
                    raise AssertionError((symbol, adjustment, asof))
                values = super().fetch(symbol, adjustment, start, end, asof=asof)
                if "split" in adjustment or adjustment == "all":
                    return [
                        {
                            **r,
                            **{key: Decimal(r[key]) * 15 for key in ("o", "h", "l", "c", "vw")},
                            "v": Decimal(r["v"]) / 15,
                        }
                        for r in values
                    ]
                return values

        client = Mapped(fixed)
        result = run_lineage(
            self.store, ["NEW"], identity_client=IdentityClient(assets, actions), pricing_client=client
        )
        self.assertEqual(result["failed"], 0, result)
        self.assertEqual({call[0] for call in client.calls}, {"NEW"})
        self.assertEqual(snapshot(self.store / "end_of_day/pricing/OLD"), preserved)
        source = load_source(self.store / "end_of_day/pricing/NEW", "NEW")
        self.assertEqual(source["latest_request"]["asof"], "2026-10-09")
        reader = StandaloneReader(self.store)
        with patch(
            "libs.alpaca_lineage_build.pipeline.select_observations",
            side_effect=AssertionError("consumer replay"),
        ):
            history = reader.get_histories("NEW", start="2026-10-02", end="2026-10-02")
            for view in history["views"].values():
                self.assertEqual(view["bars"][0]["trade_count"], "453")
            self.assertEqual(Decimal(history["views"]["all"]["bars"][0]["close"]), Decimal(fixed[1]["c"]) * 15)
            repeat = run_lineage(
                self.store, ["NEW"], identity_client=IdentityClient(assets, actions), pricing_client=client
            )
        self.assertEqual(repeat["results"][0]["status"], "unchanged")
        data = self.bootstrap()
        (security,) = [s for s in data.list_securities() if "NEW" in s.tickers]
        output = data.write_csv_export(self.directory / "exports", public_ids=[security.public_id])
        manifest = json.loads(Path(output["manifest"]).read_text())
        entry = manifest["identities"][security.public_id]
        rows = list(csv.DictReader(io.StringIO((Path(output["output"]) / entry["csv"]).read_text())))
        corrected = next(row for row in rows if row["date"] == "2026-10-02")
        digest = json.loads((Path(output["output"]) / entry["digest"]).read_text())
        for name in ADJUSTMENTS:
            logical = name.replace(",", "_").replace("-", "_") + "_trade_count"
            self.assertEqual(corrected[digest["column_aliases"][logical]], "453")

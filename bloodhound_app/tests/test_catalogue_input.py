"""Exact compatibility of native catalogue inputs with the completed CSV adapter."""

import copy
import json
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import alpaca_to_bloodhound as app
import catalogue_input
import numpy as np
import pyarrow.parquet as pq
from test_converter import SECOND_ID, SOURCE_ID, example_views
from test_current_csv import write_current


class CatalogueInputTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = self.root / "store"
        self.store.mkdir()

    def arguments(self, output, *extra, csv=None):
        return app.parser().parse_args(
            [
                "--data-in" if csv else "--store",
                str(csv or self.store),
                "--blood-out",
                str(output),
                "--quiet",
                *extra,
            ]
        )

    def source(self, views, public_id=SOURCE_ID, *, labels=None):
        csv = self.root / f"csv-{public_id}"
        digest = write_current(csv, views=views, pid=public_id, schema_version=2)
        ranges = digest["security"]["ticker_ranges"] if labels is None else labels
        rows = {
            name: {
                "bars": [
                    {field: str(row[field]) for field in catalogue_input.FIELDS} | {"session": row["session"]}
                    for row in views["split" if name == "all" or "split" in name else "raw"]
                ]
            }
            for name in app.ADJUSTMENTS
        }
        security = SimpleNamespace(public_id=public_id, tickers=("OLD", "NEW"))
        return csv, SimpleNamespace(
            security=security,
            views=rows,
            ticker_ranges=tuple(ranges),
            catalogue_generation="fixture",
            error=None,
        )

    def library(self, items):
        owner = self

        class Scope:
            catalogue = SimpleNamespace(generation="fixture", to_dict=lambda: {"generation": "fixture"})
            securities = {item.security.public_id: item.security for item in items}
            ticker_ranges = {item.security.public_id: item.ticker_ranges for item in items}

            def iter_histories(self, **kwargs):
                owner.iterations += 1
                if owner.iterations > 1:
                    raise AssertionError("history stream repeated")
                yield from items

        @contextmanager
        def opened(**kwargs):
            yield Scope()

        self.iterations = 0
        library = SimpleNamespace(store=self.store, open_history_export=opened)
        return SimpleNamespace(DataLibrary=lambda **kwargs: library)

    def compare(self, views, *extra):
        csv, native = self.source(views)
        old_output, new_output = self.root / "csv.parquet", self.root / "native.parquet"
        old = app.convert(self.arguments(old_output, *extra, "--overwrite", csv=csv))
        with (
            patch.dict(sys.modules, {"alpaca_end_of_day": self.library([native])}),
            patch.object(app, "discover_histories", side_effect=AssertionError("CSV discovery")),
            patch.object(app.pd.DataFrame, "sort_values", side_effect=AssertionError("accepted grid sorting")),
            patch.object(app.pd.Series, "equals", side_effect=AssertionError("accepted grid comparison")),
        ):
            new = app.convert(self.arguments(new_output, *extra, "--overwrite"))
        self.assertEqual(pq.read_table(old_output).to_pydict(), pq.read_table(new_output).to_pydict())
        self.assertEqual(pq.read_schema(old_output), pq.read_schema(new_output))
        self.assertEqual(old["serialized_validation"], new["serialized_validation"])
        self.assertEqual(
            old["histories"][0]["split_factor_details"], new["histories"][0]["split_factor_details"]
        )
        self.assertEqual(self.iterations, 1)
        return pq.read_table(new_output), new

    def test_individual_rounding_fractional_volume_dated_labels_and_filters_are_exact(self):
        for basis in ("all", "split"):
            for filtered in (False, True):
                with self.subTest(basis=basis, filtered=filtered):
                    extra = ["--price-basis", basis]
                    if filtered:
                        extra += ["--end-date", "2024-01-02"]
                    table, report = self.compare(example_views(), *extra)
                    self.assertEqual(table["symbol"].to_pylist(), ["OLD"] if filtered else ["OLD", "OLD", "NEW"])
                    self.assertEqual(report["histories"][0]["input_rows"], 3)
                    self.assertEqual(table["volume"][0].as_py(), 3000000.25)
                    if basis == "all":
                        self.assertEqual(table["adjusted_open"][0].as_py(), 33.33)

    def test_small_large_price_and_share_scales_keep_identical_float64_results(self):
        for scale, split in ((1e-8, 1 / 3), (1e8, 1 / 3), (1, 1e-6), (1, 1e6)):
            with self.subTest(scale=scale, split=split):
                views = example_views()
                for index, raw in enumerate(views["raw"]):
                    ratio = split if index < 2 else 1
                    for field in (*app.OHLC, "vwap"):
                        raw[field] *= scale
                        views["split"][index][field] = raw[field] * ratio
                    views["split"][index]["volume"] = raw["volume"] / ratio
                views["all"] = copy.deepcopy(views["split"])
                table, _ = self.compare(views)
                self.assertTrue(np.isfinite(table["split_adjustment_ratio"].to_numpy()).all())
                # Each case uses a fresh completed input handoff.
                import shutil

                shutil.rmtree(self.root / f"csv-{SOURCE_ID}")

    def test_multi_identity_input_is_read_once_and_serialized_in_stable_series_order(self):
        _, first = self.source(example_views(), SOURCE_ID)
        _, second = self.source(example_views(SECOND_ID), SECOND_ID)
        items = sorted([first, second], key=lambda item: app.series_id(item.security.public_id), reverse=True)
        output = self.root / "many.parquet"
        with patch.dict(sys.modules, {"alpaca_end_of_day": self.library(items)}):
            report = app.convert(self.arguments(output))
        self.assertEqual(report["serialized_validation"]["series"], 2)
        identifiers = pq.read_table(output)["ohlcv_series_id"].to_pylist()
        self.assertEqual(identifiers, sorted(identifiers))
        self.assertEqual(self.iterations, 1)

    def test_metadata_override_and_explicit_action_scaling_remain_compatible(self):
        metadata = self.root / "metadata.csv"
        metadata.write_text(
            f"public_id,start_date,end_date,symbol\n{SOURCE_ID},2024-01-02,2024-01-04,OVERRIDE\n"
        )
        events = self.root / "events.csv"
        events.write_text(
            f"public_id,event_id,effective_date,old_shares,new_shares\n{SOURCE_ID},split,2024-01-04,1,3\n"
        )
        coverage = self.root / "coverage.csv"
        coverage.write_text(
            f"public_id,start_date,end_date,source\n{SOURCE_ID},2024-01-02,2024-01-04,Synthetic fixture\n"
        )
        table, report = self.compare(
            example_views(),
            "--metadata",
            str(metadata),
            "--split-source",
            "actions",
            "--split-events",
            str(events),
            "--action-coverage",
            str(coverage),
            "--horizon",
            "2024-01-04",
        )
        self.assertEqual(set(table["symbol"].to_pylist()), {"OVERRIDE"})
        self.assertEqual(table["split_adjustment_ratio"].to_pylist(), [1 / 3, 1 / 3, 1.0])
        self.assertEqual(report["split_factor_evidence"], "action_records_with_declared_coverage")

    def test_native_input_retains_precise_numeric_and_cross_view_rejections(self):
        for problem in ("price", "volume", "trade_count", "geometry", "nonfinite", "joint_factor"):
            with self.subTest(problem=problem):
                views = example_views()
                csv, native = self.source(views)
                if problem == "price":
                    native.views["raw"]["bars"][0]["open"] = "0"
                elif problem == "volume":
                    native.views["raw"]["bars"][0]["volume"] = "-1"
                elif problem == "trade_count":
                    native.views["split"]["bars"][0]["trade_count"] = "18"
                elif problem == "geometry":
                    native.views["all"]["bars"][0]["low"] = "999"
                elif problem == "nonfinite":
                    native.views["raw"]["bars"][0]["close"] = "inf"
                else:
                    for field in (*app.OHLC, "vwap"):
                        native.views["dividend"]["bars"][0][field] = str(
                            float(native.views["raw"]["bars"][0][field]) * 100
                        )
                output = self.root / f"{problem}.parquet"
                with (
                    patch.dict(sys.modules, {"alpaca_end_of_day": self.library([native])}),
                    self.assertRaises(app.ConversionError),
                ):
                    app.convert(self.arguments(output))
                self.assertFalse(output.exists())
                report = json.loads(Path(str(output) + ".report.json").read_text())
                self.assertEqual(report["histories_rejected"], 1)
                import shutil

                shutil.rmtree(csv)

    def test_invalid_output_and_memory_fail_before_opening_catalogue(self):
        for extra, output in (
            (["--memory-mib", "0"], self.root / "memory.parquet"),
            ([], self.store / "bad.parquet"),
        ):
            with (
                patch.dict(
                    sys.modules,
                    {
                        "alpaca_end_of_day": SimpleNamespace(
                            DataLibrary=lambda **kw: self.fail("catalogue opened")
                        )
                    },
                ),
                self.assertRaises(app.ConversionError),
            ):
                app.convert(self.arguments(output, *extra))

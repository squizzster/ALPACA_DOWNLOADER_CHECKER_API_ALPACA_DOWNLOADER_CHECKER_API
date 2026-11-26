"""Serving and public-export handoff, including legacy recovery and invalidation."""

import gzip
import json
from contextlib import ExitStack, closing
from pathlib import Path
from unittest import skipUnless
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_catalogue_bloodhound_export import HAS_CONVERTER

from alpaca_end_of_day import CatalogChangedError, DataLibrary, ReaderBusyError, standalone
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_serving_catalogue import metadata_projection as projection
from libs.alpaca_serving_catalogue import storage
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.sqlite_connections import connect_sqlite
from libs.common.store_lock import operation_lock


class MetadataProjectionTests(NativeCase):
    def forbid_historical_reads(self, *, receipts=True):
        guards = ExitStack()
        for method in ("read_receipt", "read_boundaries", "load_source", "_rows", "read_index"):
            guards.enter_context(patch.object(standalone, method, side_effect=AssertionError(method)))
        if receipts:
            guards.enter_context(
                patch.object(
                    projection, "selected_metadata_receipts", side_effect=AssertionError("unnecessary migration")
                )
            )
        return guards

    def forget_projection(self):
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("DROP TABLE metadata_evidence")

    def test_native_serving_publishes_and_reader_never_reopens_group(self):
        data = self.bootstrap()
        with (
            self.forbid_historical_reads(),
            patch(
                "libs.alpaca_serving_catalogue.metadata_evidence.project_group_evidence",
                side_effect=AssertionError("repeated attribution"),
            ),
        ):
            self.assertEqual(0, data.plan_metadata_export()["groups_to_prepare"])
            report = data.write_metadata_export(self.directory / "metadata")
            record = json.loads(gzip.decompress(Path(report["records_file"]).read_bytes()))
            self.assertEqual(record["public_id"], data.list_securities()[0].public_id)
            self.assertEqual(record["asset_snapshots"][0]["symbol"], "TEST")
            self.assertNotIn("id", record["asset_snapshots"][0])
        with operation_lock(self.store, exclusive=True), self.assertRaises(ReaderBusyError):
            data.write_metadata_export(self.directory / "busy")

    def test_explicit_legacy_recovery_resumes_at_first_unfinished_group(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        self.forget_projection()
        with self.forbid_historical_reads():
            self.assertEqual(2, data.plan_metadata_export()["groups_to_prepare"])
            self.assertEqual(2, projection.prepare(self.store, plan=True)["selected_receipts"])
            self.assertEqual(
                2, data.write_metadata_export(self.directory / "core", catalogue_only=True)["identities"]
            )
            with self.assertRaisesRegex((RuntimeError, CatalogueError), "prepare-metadata"):
                data.write_metadata_export(self.directory / "missing")
            self.assertFalse((self.directory / "missing").exists())
        reads = []
        original = lineage.selected_document

        def read(path, token):
            self.assertEqual(path.name, "lineage.manifest.json")
            reads.append(path.parent.name)
            return original(path, token)

        with (
            self.forbid_historical_reads(receipts=False),
            patch.object(lineage, "selected_document", side_effect=read),
        ):
            with self.assertRaises(KeyboardInterrupt):
                projection.prepare(self.store, progress=lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
            remaining = projection.prepare(self.store)
            self.assertEqual(1, remaining["receipt_reads"])
            self.assertEqual(["OTHER", "TEST"], reads)
            self.assertEqual(0, projection.prepare(self.store)["receipt_reads"])
            self.assertEqual(2, data.write_metadata_export(self.directory / "resumed")["identities"])

    def test_legacy_migration_rejects_unserved_descriptive_revision(self):
        self.bootstrap()
        self.forget_projection()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new description"]))
        with (
            patch.object(lineage, "selected_document", side_effect=AssertionError("read stale input")),
            self.assertRaisesRegex(lineage.LineageError, "selected lineage metadata changed"),
        ):
            projection.prepare(self.store)
        self.assertEqual(1, DataLibrary(store=self.store).plan_metadata_export()["groups_to_prepare"])

    def test_changed_catalogue_and_dirty_inputs_reject_before_projection(self):
        from libs.alpaca_serving_catalogue import export_inputs

        data = self.bootstrap()
        export_inputs.invalidate(self.store, "lineage", {"TEST": "b" * 64})
        with (
            patch.object(projection, "require", side_effect=AssertionError("invalid projection selected")),
            self.assertRaises(CatalogChangedError),
        ):
            data.write_metadata_export(self.directory / "dirty")

    @skipUnless(HAS_CONVERTER, "install requirements-bloodhound.txt")
    def test_offline_exact_pairing_with_both_native_price_exports(self):
        from bloodhound_app import alpaca_to_bloodhound as converter

        data = self.bootstrap()
        csv = data.write_csv_export(self.directory / "price-csv")
        parquet = self.directory / "prices.parquet"
        args = converter.parser().parse_args(
            ["--store", str(self.store), "--blood-out", str(parquet), "--quiet"]
        )
        prices = converter.convert(args)
        public_id = data.list_securities()[0].public_id
        with self.forbid_historical_reads():
            text = data.write_metadata_export(self.directory / "csv-meta", csv_manifest=csv["manifest"])
            binary = data.write_metadata_export(
                self.directory / "pq-meta", bloodhound_report=parquet.with_suffix(".parquet.report.json")
            )
        csv_row = json.loads(gzip.decompress(Path(text["records_file"]).read_bytes()))
        pq_row = json.loads(gzip.decompress(Path(binary["records_file"]).read_bytes()))
        self.assertEqual(binary["selection"]["output_sha256"], prices["output_sha256"])
        self.assertEqual(pq_row.pop("ohlcv_series_id"), str(prices["histories"][0]["ohlcv_series_id"]))
        self.assertEqual(csv_row["public_id"], public_id)
        self.assertEqual(csv_row, pq_row)

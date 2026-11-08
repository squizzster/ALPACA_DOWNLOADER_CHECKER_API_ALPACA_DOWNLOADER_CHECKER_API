"""Export consumes durable producer invalidation without auditing selected inputs."""

import json
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import CatalogChangedError, DataLibrary, standalone
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download import storage as pricing
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import export_inputs, producer, storage, work
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.sqlite_connections import connect_sqlite
from libs.common.store_lock import operation_lock


class ExportApplicabilityTests(NativeCase):
    def legacy_applicability(self):
        with closing(connect_sqlite(storage.path(self.store))) as connection, connection:
            for table in ("export_inputs", "export_input_state", "export_input_bindings"):
                connection.execute(f"DROP TABLE {table}")

    def changed_prices(self):
        return [{**bar, **{field: Decimal("500") for field in ("o", "h", "l", "c", "vw")}} for bar in self.bars]

    def replace_prices(self, symbol="TEST"):
        with operation_lock(self.store, exclusive=True):
            return download_pricing(
                self.store, symbol, "2026-10-14", client=FakeClient(self.changed_prices()), refresh_all=True
            )

    def assert_export_blocked_before_bodies(self, data, name):
        destination = self.directory / name
        with (
            patch.object(standalone, "load_source", side_effect=AssertionError("invalid source opened")),
            patch.object(standalone, "read_receipt", side_effect=AssertionError("invalid receipt opened")),
            patch.object(
                standalone, "read_boundaries", side_effect=AssertionError("invalid assignments opened")
            ),
            patch.object(standalone, "_rows", side_effect=AssertionError("invalid prices opened")),
            self.assertRaises(CatalogChangedError),
        ):
            data.write_csv_export(destination)
        self.assertFalse(destination.exists())

    def test_changed_pricing_rejects_both_open_and_fresh_library_before_payloads(self):
        data = self.bootstrap()
        self.assertTrue(self.replace_prices()["changed"])
        self.assert_export_blocked_before_bodies(data, "old-library")
        self.assert_export_blocked_before_bodies(DataLibrary(store=self.store), "fresh-library")

    def test_changed_assignment_rejects_export_but_descriptive_change_remains_accepted(self):
        data = self.bootstrap()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["descriptive only"]))
        result = data.write_csv_export(self.directory / "descriptive")
        self.assertTrue(Path(result["manifest"]).exists())

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["observation_tickers"] = ["NEW", "TEST"]

        self.publish_change(relabel)
        self.assert_export_blocked_before_bodies(data, "ownership-changed")
        self.assert_export_blocked_before_bodies(DataLibrary(store=self.store), "ownership-fresh")

    def test_unrelated_changed_source_does_not_invalidate_selected_export(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        selected = data.resolve_ticker("TEST")[0].public_id
        self.replace_prices("OTHER")
        result = data.write_csv_export(self.directory / "selected", public_ids=[selected])
        self.assertEqual(result["identities"], 1)
        self.assert_export_blocked_before_bodies(data, "all-sources")

    def test_interrupted_pricing_invalidates_before_first_install_and_recovers(self):
        data = self.bootstrap()
        original = pricing.os.replace
        installs = []

        class Interrupted(BaseException):
            pass

        def interrupted(source, target):
            if Path(target).suffix == ".gz":
                with self.assertRaises(CatalogueError):
                    export_inputs.select(self.store, data.catalog_generation(), {"TEST"})
                installs.append(str(target))
                raise Interrupted
            return original(source, target)

        with patch.object(pricing.os, "replace", side_effect=interrupted), self.assertRaises(Interrupted):
            self.replace_prices()
        self.assertEqual(len(installs), 1)
        self.assert_export_blocked_before_bodies(DataLibrary(store=self.store), "interrupted")
        root = self.store / "end_of_day/pricing/TEST"
        with pricing.ticker_lock(root, exclusive=True):
            self.assertTrue(pricing.recover(root))
        self.assert_export_blocked_before_bodies(DataLibrary(store=self.store), "pricing-recovered")
        result = run_lineage(self.store, ["TEST"], identity_client=IdentityClient({"TEST": asset("TEST")}))
        self.assertEqual(result["failed"], 0, result)
        self.assertTrue(producer.maintain(self.store)["complete"])
        exported = DataLibrary(store=self.store).write_csv_export(self.directory / "completed")
        self.assertTrue(Path(exported["manifest"]).exists())

    def test_interrupted_lineage_blocks_export_until_owning_recovery_and_serving(self):
        self.bootstrap()
        original = lineage.atomic_json

        class Interrupted(BaseException):
            pass

        def interrupted(path, value, **kwargs):
            if path.name == "lineage.manifest.json":
                raise Interrupted
            return original(path, value, **kwargs)

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["observation_tickers"] = ["NEW", "TEST"]

        with patch.object(lineage, "atomic_json", side_effect=interrupted), self.assertRaises(Interrupted):
            self.publish_change(relabel)
        self.assert_export_blocked_before_bodies(DataLibrary(store=self.store), "lineage-interrupted")
        with lineage.writer_snapshot(self.store) as view:
            self.assertTrue(lineage.recover(self.store, view.root))
        self.assert_export_blocked_before_bodies(DataLibrary(store=self.store), "lineage-recovered")
        self.assertTrue(producer.maintain(self.store)["complete"])
        result = DataLibrary(store=self.store).write_csv_export(self.directory / "served")
        self.assertTrue(Path(result["manifest"]).exists())

    def test_legacy_applicability_fails_closed_and_explicit_rebuild_reuses_numbers(self):
        data = self.bootstrap()
        self.legacy_applicability()
        self.assert_export_blocked_before_bodies(data, "legacy")
        with patch.object(standalone, "_rows", side_effect=AssertionError("accepted numbers reread")):
            result = producer.maintain(self.store, rebuild=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(result["reused_history_prefixes"], 1)
        exported = DataLibrary(store=self.store).write_csv_export(self.directory / "upgraded")
        self.assertTrue(Path(exported["manifest"]).exists())

    def test_legacy_rebuild_retains_completed_peer_across_failure(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        self.legacy_applicability()
        evaluate = producer.evaluate

        def fail_selected(group, previous, **kwargs):
            if "TEST" in group.references["pricing"]:
                raise RuntimeError("selected peer failed")
            return evaluate(group, previous, **kwargs)

        with patch.object(producer, "evaluate", side_effect=fail_selected):
            result = producer.maintain(self.store, rebuild=True)
        self.assertFalse(result["complete"])
        self.assertEqual(result["failed"], 1)
        self.assertEqual(set(export_inputs.bound_groups(self.store)), {"OTHER"})

        def only_remaining(group, previous, **kwargs):
            self.assertEqual(set(group.references["pricing"]), {"TEST"}, "completed peer reevaluated")
            return evaluate(group, previous, **kwargs)

        with (
            patch.object(producer, "evaluate", side_effect=only_remaining),
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted prices reread")),
        ):
            retry = producer.maintain(self.store, rebuild=True)
        self.assertTrue(retry["complete"], retry)
        self.assertEqual(retry["evaluated_groups"], 1)
        self.assertEqual(retry["reused_groups"], 1)

    def test_committed_serving_clears_applicability_before_acknowledgement_retry(self):
        self.bootstrap()
        self.replace_prices()
        result = run_lineage(self.store, ["TEST"], identity_client=IdentityClient({"TEST": asset("TEST")}))
        self.assertEqual(result["failed"], 0, result)

        class Interrupted(BaseException):
            pass

        with patch.object(work, "acknowledge", side_effect=Interrupted), self.assertRaises(Interrupted):
            producer.maintain(self.store)
        exported = DataLibrary(store=self.store).write_csv_export(self.directory / "committed")
        self.assertTrue(Path(exported["manifest"]).exists())
        with patch.object(producer, "evaluate", side_effect=AssertionError("completed decision replayed")):
            retry = producer.maintain(self.store)
        self.assertTrue(retry["complete"], retry)

    def test_older_completed_publication_cannot_clear_newer_invalidation(self):
        data = self.bootstrap()
        refs = next(iter(data._groups.values()))["references"]
        export_inputs.invalidate(self.store, "pricing", {"TEST": "f" * 64})
        with closing(connect_sqlite(storage.path(self.store))) as connection, connection:
            export_inputs.accept(connection, refs)
        self.assert_export_blocked_before_bodies(data, "newer-input")

    def test_equal_pricing_outcome_does_not_invalidate_exports(self):
        data = self.bootstrap()
        result = download_pricing(
            self.store, "TEST", "2026-10-14", client=FakeClient(self.bars), refresh_all=True
        )
        self.assertFalse(result["changed"])
        result = data.write_csv_export(self.directory / "equal")
        manifest = json.loads(Path(result["manifest"]).read_text())
        self.assertEqual(manifest["catalogue"]["generation"], data.catalog_generation())

    def test_same_generation_payload_repair_consumes_its_own_invalidation(self):
        data = self.bootstrap()
        before = next(iter(data._groups.values()))["references"]["pricing"]["TEST"]
        with operation_lock(self.store, exclusive=True):
            result = download_pricing(
                self.store,
                "TEST",
                "2026-10-14",
                client=FakeClient(self.bars),
                refresh_all=True,
                repair_payloads=True,
            )
        self.assertEqual(result["source_sha256"], before)
        exported = data.write_csv_export(self.directory / "same-generation-repair")
        self.assertTrue(Path(exported["manifest"]).exists())

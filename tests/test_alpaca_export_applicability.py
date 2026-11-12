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

    def test_completed_lineage_intention_is_reconciled_without_upstream_work(self):
        data = self.bootstrap()
        export_inputs.invalidate(self.store, "lineage", {"TEST": "f" * 64})
        with (
            patch.object(lineage, "read_receipt", side_effect=AssertionError("receipt reread")),
            patch.object(producer, "evaluate", side_effect=AssertionError("decision repeated")),
            patch.object(storage, "read_groups", side_effect=AssertionError("checkpoint reread")),
        ):
            result = producer.maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reconciled_export_inputs"], 1)
        exported = data.write_csv_export(self.directory / "reconciled")
        self.assertTrue(Path(exported["manifest"]).exists())

    def test_pending_lineage_journal_prevents_intention_reconciliation(self):
        self.bootstrap()
        export_inputs.invalidate(self.store, "lineage", {"TEST": "f" * 64})
        (self.root / ".pending.json").write_text("{}")
        with self.assertRaisesRegex(CatalogueError, "publication pending"):
            producer.maintain(self.store)
        self.assertEqual(export_inputs.pending_lineage(self.store), {"TEST": "f" * 64})

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


class ServingCompletionApplicabilityTests(NativeCase):
    """An empty scheduling queue cannot hide applicable publication invalidation."""

    changed_prices = ExportApplicabilityTests.changed_prices
    replace_prices = ExportApplicabilityTests.replace_prices

    def accepted_serving_context(self):
        from libs.alpaca_lineage_build.context import accept_context

        context = accept_context(self.store)
        context.accept_store()
        context.serving = storage.controls(self.store)
        return context

    def no_materialization(self):
        from contextlib import ExitStack

        stack = ExitStack()
        for owner, name in (
            (lineage, "read_index"),
            (lineage, "read_receipt"),
            (lineage, "read_boundaries"),
            (storage, "selector_state"),
            (storage, "controls"),
            (storage, "read_groups"),
            (pricing, "load_source"),
            (standalone, "read_artifact"),
            (standalone, "_rows"),
            (producer, "evaluate"),
        ):
            stack.enter_context(patch.object(owner, name, side_effect=AssertionError("unneeded " + name)))
        return stack

    def test_empty_queue_with_applicable_pricing_invalidation_is_incomplete_then_owner_finishes(self):
        self.bootstrap()
        context = self.accepted_serving_context()
        self.assertTrue(self.replace_prices()["changed"])
        self.assertEqual(work.read(self.store), {})
        with (
            self.no_materialization(),
            patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
        ):
            result = producer.maintain(self.store, context=context, selected_controls=context.serving)
        self.assertFalse(result["complete"], result)
        self.assertEqual(result["pending_symbols"], [])
        self.assertEqual(result["pending_export_inputs"], {"pricing": 1, "lineage": 0})
        self.assertTrue(result["catalogue_complete"], "published coverage and current applicability differ")
        self.assertEqual(counts.call_count, 1)

        built = run_lineage(self.store, ["TEST"], identity_client=IdentityClient({"TEST": asset("TEST")}))
        self.assertEqual(built["failed"], 0, built)
        with patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts:
            completed = producer.maintain(self.store)
        self.assertTrue(completed["complete"], completed)
        self.assertEqual(completed["pending_export_inputs"], {"pricing": 0, "lineage": 0})
        self.assertEqual(counts.call_count, 1)

    def test_unmatched_lineage_intention_with_missing_queue_is_incomplete_until_selected_publication(self):
        self.bootstrap()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new description"]))
        queued = work.read(self.store)
        self.assertEqual(set(queued), {"TEST"})
        export_inputs.invalidate(self.store, "lineage", queued)
        # Exercise the nearest missing-scheduling case: the producer decision
        # and export invalidation are authoritative even when its queue is empty.
        work.acknowledge(self.store, queued)
        with patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts:
            result = producer.maintain(self.store)
        self.assertFalse(result["complete"], result)
        self.assertEqual(result["pending_symbols"], [])
        self.assertEqual(result["pending_export_inputs"], {"pricing": 0, "lineage": 1})
        self.assertEqual(counts.call_count, 1)
        self.assertEqual(export_inputs.pending_lineage(self.store), queued)

        # Reestablish the explicitly selected synthetic obligation through its
        # owner; completion reporting itself must never seed or rebuild it.
        work.seed(self.store, lineage.read_index(self.root)["symbols"])
        with patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts:
            completed = producer.maintain(self.store)
        self.assertTrue(completed["complete"], completed)
        self.assertEqual(completed["pending_export_inputs"], {"pricing": 0, "lineage": 0})
        self.assertEqual(counts.call_count, 1)

    def changed_component_with_held_intention(self):
        self.bootstrap()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["new description"]))
        export_inputs.invalidate(self.store, "lineage", {"TEST": "a" * 64})

    def test_new_publication_atomically_consumes_held_superseded_intention(self):
        self.changed_component_with_held_intention()
        with patch.object(work, "acknowledge", side_effect=RuntimeError("after commit")):
            first = producer.maintain(self.store)
        self.assertEqual(first["failed"], 1)
        self.assertEqual(export_inputs.pending_lineage(self.store), {})
        with (
            patch.object(producer, "evaluate", side_effect=AssertionError("decision repeated")),
            patch.object(lineage, "read_receipt", side_effect=AssertionError("receipt repeated")),
            patch.object(lineage, "read_boundaries", side_effect=AssertionError("assignments repeated")),
            patch.object(pricing, "load_source", side_effect=AssertionError("source repeated")),
            patch.object(standalone, "_rows", side_effect=AssertionError("prices repeated")),
        ):
            result = producer.maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["evaluated_groups"], 0)

    def test_focused_publication_consumes_only_its_selected_component_intention(self):
        self.add_symbol("OTHER")
        self.changed_component_with_held_intention()
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["other"]), symbol="OTHER")
        export_inputs.invalidate(self.store, "lineage", {"OTHER": "c" * 64})
        with patch.object(export_inputs, "pending_lineage", wraps=export_inputs.pending_lineage) as selected:
            result = producer.maintain(self.store, symbols={"TEST"})
        self.assertTrue(result["complete"], result)
        self.assertEqual(selected.call_count, 1)
        self.assertEqual(export_inputs.pending_lineage(self.store), {"OTHER": "c" * 64})
        self.assertEqual(set(work.read(self.store)), {"OTHER"})

    def test_new_publication_preserves_intention_renewed_after_selection(self):
        self.changed_component_with_held_intention()
        commit = storage.commit

        def renew(*args, **kwargs):
            export_inputs.invalidate(self.store, "lineage", {"TEST": "b" * 64})
            return commit(*args, **kwargs)

        with patch.object(storage, "commit", side_effect=renew):
            result = producer.maintain(self.store)
        self.assertFalse(result["complete"], result)
        self.assertEqual(result["published_components"], 1)
        self.assertEqual(export_inputs.pending_lineage(self.store), {"TEST": "b" * 64})

    def test_publication_rollback_preserves_selected_intention_and_selector(self):
        self.changed_component_with_held_intention()
        before = storage.selector_state(self.store)["generation"]
        with patch.object(storage, "_write_state", side_effect=RuntimeError("rollback")):
            result = producer.maintain(self.store)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(export_inputs.pending_lineage(self.store), {"TEST": "a" * 64})
        self.assertEqual(storage.selector_state(self.store)["generation"], before)

    def test_counts_cover_both_kinds_and_ignore_never_served_sources_without_selector_reads(self):
        self.bootstrap()
        export_inputs.invalidate(self.store, "pricing", {"TEST": "a" * 64, "UNSERVED": "b" * 64})
        export_inputs.invalidate(self.store, "lineage", {"TEST": "c" * 64, "UNSERVED": "d" * 64})
        with self.no_materialization():
            self.assertEqual(export_inputs.pending_counts(self.store), {"pricing": 1, "lineage": 1})
            self.assertEqual(
                export_inputs.pending_counts(self.store, symbols={"UNSERVED"}), {"pricing": 0, "lineage": 0}
            )
            self.assertEqual(
                export_inputs.pending_counts(self.store, symbols=set()), {"pricing": 0, "lineage": 0}
            )

    def test_focused_and_explicit_empty_work_ignore_unrelated_applicable_invalidation(self):
        self.add_symbol("OTHER")
        self.bootstrap()
        context = self.accepted_serving_context()
        export_inputs.invalidate(self.store, "pricing", {"OTHER": "a" * 64})
        for symbols in ({"TEST"}, set()):
            with (
                self.subTest(symbols=symbols),
                self.no_materialization(),
                patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
            ):
                result = producer.maintain(
                    self.store, symbols=symbols, context=context, selected_controls=context.serving
                )
            self.assertTrue(result["complete"], result)
            self.assertEqual(result["pending_export_inputs"], {"pricing": 0, "lineage": 0})
            self.assertEqual(counts.call_count, 1)
        with (
            self.no_materialization(),
            patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
        ):
            complete_scope = producer.maintain(self.store, context=context, selected_controls=context.serving)
        self.assertFalse(complete_scope["complete"], complete_scope)
        self.assertEqual(complete_scope["pending_export_inputs"], {"pricing": 1, "lineage": 0})
        self.assertEqual(counts.call_count, 1)

    def published_pair(self):
        self.add_symbol("OTHER")
        current = asset("OTHER")
        action = {
            "action_type": "name_change",
            "id": "rename",
            "process_date": "2026-10-06",
            "old_symbol": "TEST",
            "new_symbol": "OTHER",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
        }
        result = run_lineage(
            self.store,
            ["TEST", "OTHER"],
            refresh=True,
            identity_client=IdentityClient({"TEST": current, "OTHER": current, "111111111": current}, [action]),
        )
        self.assertEqual(result["failed"], 0, result)
        self.bootstrap()
        context = self.accepted_serving_context()
        self.assertEqual(context.serving["members"]["TEST"], context.serving["members"]["OTHER"])
        return context

    def test_focused_counts_include_all_sources_in_its_completed_serving_component(self):
        context = self.published_pair()
        export_inputs.invalidate(self.store, "pricing", {"OTHER": "a" * 64})
        with (
            self.no_materialization(),
            patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
        ):
            result = producer.maintain(
                self.store, symbols={"TEST"}, context=context, selected_controls=context.serving
            )
        self.assertFalse(result["complete"], result)
        self.assertEqual(result["pending_export_inputs"], {"pricing": 1, "lineage": 0})
        self.assertEqual(counts.call_count, 1)

    def test_focused_peer_intention_reconciles_completed_component_without_upstream_reads(self):
        context = self.published_pair()
        # Selection and applicability cover the full accepted component even
        # when the completed intention belongs only to its unnamed peer.
        context.index = lineage.read_index(self.root)
        export_inputs.invalidate(self.store, "lineage", {"OTHER": "f" * 64})
        self.assertEqual(work.read(self.store), {})
        with (
            self.no_materialization(),
            patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
        ):
            result = producer.maintain(
                self.store,
                symbols={"TEST"},
                context=context,
                selected_controls=context.serving,
                selected_index=context.index,
            )
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reconciled_export_inputs"], 1)
        self.assertEqual(result["pending_export_inputs"], {"pricing": 0, "lineage": 0})
        self.assertEqual(counts.call_count, 1)

    def test_shared_component_applicability_is_calculated_once_for_peer_intentions(self):
        context = self.published_pair()
        context.index = lineage.read_index(self.root)
        export_inputs.invalidate(self.store, "lineage", {"TEST": "e" * 64, "OTHER": "f" * 64})
        with (
            self.no_materialization(),
            patch.object(producer, "_matches", wraps=producer._matches) as matches,
            patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
        ):
            result = producer.maintain(
                self.store, context=context, selected_index=context.index, selected_controls=context.serving
            )
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reconciled_export_inputs"], 2)
        self.assertEqual(result["pending_export_inputs"], {"pricing": 0, "lineage": 0})
        self.assertEqual(matches.call_count, 1)
        self.assertEqual(counts.call_count, 1)

    def test_matched_lineage_intention_reconciles_once_without_payload_or_decision_replay(self):
        self.bootstrap()
        export_inputs.invalidate(self.store, "lineage", {"TEST": "f" * 64})
        with (
            patch.object(lineage, "read_receipt", side_effect=AssertionError("accepted receipt reopened")),
            patch.object(storage, "read_groups", side_effect=AssertionError("accepted decision reopened")),
            patch.object(pricing, "load_source", side_effect=AssertionError("accepted manifest reopened")),
            patch.object(standalone, "read_artifact", side_effect=AssertionError("accepted payload reopened")),
            patch.object(producer, "evaluate", side_effect=AssertionError("accepted decision replayed")),
            patch.object(export_inputs, "pending_counts", wraps=export_inputs.pending_counts) as counts,
        ):
            result = producer.maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["reconciled_export_inputs"], 1)
        self.assertEqual(result["pending_export_inputs"], {"pricing": 0, "lineage": 0})
        self.assertEqual(counts.call_count, 1)

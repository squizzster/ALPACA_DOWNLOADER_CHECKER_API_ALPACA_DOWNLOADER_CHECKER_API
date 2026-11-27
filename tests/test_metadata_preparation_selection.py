"""Compact metadata preparation selection and scoped public-record consumption."""

import sqlite3
from contextlib import closing
from copy import deepcopy
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import PublicIdUnavailableError
from libs.alpaca_serving_catalogue import export_inputs, metadata_projection, storage
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.sqlite_connections import connect_sqlite


class PreparationSelectionTests(NativeCase):
    def deny_column_reads(self, *columns):
        def connect(path, **options):
            db = connect_sqlite(path, **options)
            db.set_authorizer(
                lambda action, table, column, *_: (
                    sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and (table, column) in columns
                    else sqlite3.SQLITE_OK
                )
            )
            return db

        return patch.object(metadata_projection, "connect_sqlite", side_effect=connect)

    def test_universe_and_id_plans_use_only_compact_controls(self):
        self.add_symbol("SECOND")
        data = self.bootstrap()
        pid = data.resolve_ticker("TEST")[0].public_id
        with (
            patch("alpaca_end_of_day.DataLibrary", side_effect=AssertionError("loaded typed catalogue")),
            self.deny_column_reads(("groups", "body"), ("securities", "body"), ("metadata_evidence", "body")),
            patch.object(
                metadata_projection, "selected_metadata_receipts", side_effect=AssertionError("opened receipts")
            ),
        ):
            self.assertEqual(metadata_projection.prepare(self.store, plan=True)["selected_groups"], 2)
            report = metadata_projection.prepare(self.store, public_ids=[pid.upper(), pid], plan=True)
            self.assertEqual((report["selected_groups"], report["pending_groups"]), (1, 0))
            with self.assertRaises(PublicIdUnavailableError):
                metadata_projection.prepare(self.store, public_ids=["0" * 64], plan=True)

    def test_held_selectors_read_only_their_own_evidence_rows(self):
        self.add_symbol("SECOND")
        data = self.bootstrap()
        groups = deepcopy(data._groups)
        with self.deny_column_reads(
            ("groups", "selection"), ("groups", "decision_generation"), ("securities", "body")
        ):
            for full, selected in ((True, groups), (False, {"TEST": groups["TEST"]})):
                with self.subTest(full=full):
                    rows = metadata_projection.selected(self.store, selected, bodies=True, all_online=full)
                    self.assertEqual(set(rows), set(selected))
            groups["TEST"]["references"]["lineage"]["TEST"] = "1" * 64
            self.assertEqual(
                set(metadata_projection.selected(self.store, groups, bodies=True, all_online=True)), {"SECOND"}
            )
            with self.assertRaisesRegex(CatalogueError, "prepare-metadata"):
                metadata_projection.require(self.store, {"TEST": groups["TEST"]})

    def test_reused_historical_symbol_selects_every_matching_group(self):
        self.add_symbol("SECOND")

        def rename(_, boundaries):
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    for segment in entry["segments"]:
                        segment.update(historical_symbol="1SHARED", observation_tickers=["1SHARED"])

        for symbol in ("TEST", "SECOND"):
            self.publish_change(rename, symbol)
        self.bootstrap()
        with (
            self.deny_column_reads(("groups", "body")),
            patch("alpaca_end_of_day.DataLibrary", side_effect=AssertionError("loaded universe")),
        ):
            self.assertEqual(
                metadata_projection.prepare(self.store, symbols=[" 1shared "], plan=True)["selected_groups"], 2
            )
            with self.assertRaisesRegex(ValueError, "not available"):
                metadata_projection.prepare(self.store, symbols=["1shared", "ABSENT"], plan=True)

    def test_listing_migration_selects_only_its_two_fields_after_generation_binding(self):
        self.add_symbol("SECOND")
        data = self.bootstrap()
        pid = data.resolve_ticker("TEST")[0].public_id
        target = {"historical_listings": [{"symbol": "held"}], "trade_observations": [{"session": "held"}]}
        stored = {pid: {**target, "asset_snapshots": ["private"], "cross_ticker_listing_changes": ["private"]}}
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("UPDATE metadata_evidence SET body=? WHERE group_key='TEST'", (storage.encoded(stored),))
            db.execute("UPDATE metadata_evidence SET body='unselected' WHERE group_key='SECOND'")
            selected = {"TEST": data._groups["TEST"]}
            for all_online in (True, False):
                result = metadata_projection._selected(
                    db, selected, bodies=True, all_online=all_online, listing_fields=True
                )
                self.assertEqual(result, {"TEST": {pid: target}})
            self.assertEqual(
                metadata_projection._selected(db, selected, bodies=True, all_online=False), {"TEST": stored}
            )

    def test_execution_skips_unrelated_records_and_uses_one_catalogue_transaction(self):
        self.add_symbol("SECOND")
        data = self.bootstrap()
        pid = data.resolve_ticker("TEST")[0].public_id
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("DELETE FROM metadata_evidence WHERE group_key='TEST'")
            # Only a selected online body may be evaluated by SQLite's JSON functions.
            db.execute("UPDATE securities SET body='unrelated' WHERE group_key='SECOND'")
        with (
            self.deny_column_reads(("groups", "body")),
            patch.object(export_inputs, "select", side_effect=AssertionError("reopened catalogue")),
            patch.object(storage, "_state", wraps=storage._state) as state,
        ):
            report = metadata_projection.prepare(self.store, public_ids=[pid])
            self.assertEqual((report["prepared_groups"], report["receipt_reads"]), (1, 1))
            self.assertEqual(state.call_count, 1)

    def test_dirty_scope_rejects_before_loading_assignment_fields(self):
        self.bootstrap()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("DELETE FROM metadata_evidence")
        export_inputs.invalidate(self.store, "lineage", {"TEST": "b" * 64})
        with (
            self.deny_column_reads(("securities", "body")),
            self.assertRaisesRegex(CatalogueError, "selected export inputs changed"),
        ):
            metadata_projection.prepare(self.store)

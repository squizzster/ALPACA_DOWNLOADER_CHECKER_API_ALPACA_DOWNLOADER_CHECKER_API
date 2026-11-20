"""Workflow controls keep current obligations and scope selected reads before decoding."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sqlite_crash_fixture import leave_hot_journal

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.universe_update import rebuild_universe
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.storage import initialize_store
from libs.common.sqlite_connections import connect_sqlite


class SelectedWorkStateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name)
        universe_state.initialize(self.store)
        universe_state.accept(self.store, {"catalogue_observed_at_utc": "2026-11-08T12:00:00Z"}, [], {})
        self.path = self.store / universe_state.STATE_FILE
        self.selected = {"status": "pending", "obligation_id": "selected", "required_group": ["TEST"]}
        self.invalid_unselected = "{" + "unselected-invalid-body" * 50_000
        with closing(connect_sqlite(self.path)) as connection, connection:
            connection.executemany(
                "INSERT INTO work VALUES (?, ?)",
                [("TEST", json.dumps(self.selected)), ("UNSELECTED", self.invalid_unselected)],
            )
            connection.execute("INSERT INTO registration VALUES ('GLOBAL-INTENT', 0)")
            connection.execute("INSERT INTO registration VALUES ('DONE', 1)")
            connection.execute("UPDATE state SET revision=7 WHERE id=1")

    def observed_connection(self, statements):
        def connect(*args, **kwargs):
            connection = connect_sqlite(*args, **kwargs)
            connection.set_trace_callback(statements.append)
            return connection

        return connect

    def test_selected_rows_are_indexed_before_decode_and_keep_global_controls(self):
        statements, decoded = [], []
        original = universe_state._read_work

        def read_work(value):
            self.assertNotEqual(value, self.invalid_unselected, "unselected body reached ingress")
            decoded.append(value)
            return original(value)

        with (
            patch.object(universe_state, "connect_sqlite", side_effect=self.observed_connection(statements)),
            patch.object(universe_state, "_read_work", side_effect=read_work),
        ):
            result = universe_state.read_state(self.store, symbols=["TEST", "TEST", "MISSING"])
        self.assertEqual(result["work"], {"TEST": self.selected})
        self.assertEqual(len(decoded), 1)
        self.assertEqual(result["registration_pending"], ["GLOBAL-INTENT"])
        self.assertEqual(result["revision"], 7)
        self.assertEqual(result["schema_version"], universe_state.STATE_VERSION)
        self.assertEqual(
            result["last_catalogue_observation"], {"id": 1, "observed_at_utc": "2026-11-08T12:00:00Z"}
        )
        queries = [
            statement for statement in statements if statement.startswith("SELECT symbol, body FROM work")
        ]
        self.assertEqual(len(queries), 1)
        self.assertIn("WHERE symbol IN ('MISSING','TEST')", queries[0])
        with closing(connect_sqlite(self.path, readonly=True)) as connection:
            explanation = connection.execute(
                "EXPLAIN QUERY PLAN SELECT symbol, body FROM work WHERE symbol IN (?, ?)", ("MISSING", "TEST")
            ).fetchall()
        self.assertTrue(
            any("SEARCH work USING INDEX" in row[-1].replace("SEARCH TABLE ", "SEARCH ") for row in explanation),
            explanation,
        )

    def test_empty_scope_reads_no_work_bodies(self):
        statements = []
        with (
            patch.object(universe_state, "connect_sqlite", side_effect=self.observed_connection(statements)),
            patch.object(universe_state, "_read_work", side_effect=AssertionError("empty scope decoded work")),
        ):
            result = universe_state.read_state(self.store, symbols=[])
        self.assertEqual(result["work"], {})
        self.assertEqual(result["registration_pending"], ["GLOBAL-INTENT"])
        self.assertFalse(any(statement.startswith("SELECT symbol, body FROM work") for statement in statements))

    def test_full_read_keeps_existing_invalid_body_failure(self):
        with self.assertRaises(LineageError):
            universe_state.read_state(self.store)

    def test_selected_invalid_correction_still_fails_its_own_ingress(self):
        selected = {
            **self.selected,
            "pricing_correction": {"schema_version": 3, "observations": {"all": [["invalid-row"]]}},
        }
        with closing(connect_sqlite(self.path)) as connection, connection:
            connection.execute("UPDATE work SET body=? WHERE symbol='TEST'", (json.dumps(selected),))
        with self.assertRaisesRegex(DownloadError, "invalid durable correction row"):
            universe_state.read_state(self.store, symbols=["TEST"], pending_only=True)

    def test_selected_pending_reader_never_decodes_completed_correction_evidence(self):
        complete = {
            "status": "complete",
            "pricing_correction": {"schema_version": 3, "observations": {"all": [["invalid-row"]]}},
        }
        with closing(connect_sqlite(self.path)) as connection, connection:
            connection.execute("INSERT INTO work VALUES ('DONE', ?)", (json.dumps(complete),))
        original = universe_state._read_work
        statements, decoded = [], []

        def read_work(value):
            self.assertEqual(json.loads(value)["status"], "pending", "completed work reached ingress")
            decoded.append(value)
            return original(value)

        with (
            patch.object(universe_state, "connect_sqlite", side_effect=self.observed_connection(statements)),
            patch.object(universe_state, "_read_work", side_effect=read_work),
        ):
            result = universe_state.read_state(self.store, symbols=["DONE", "TEST"], pending_only=True)
        self.assertEqual(result["work"], {"TEST": self.selected})
        self.assertEqual(len(decoded), 1)
        queries = [
            statement for statement in statements if statement.startswith("SELECT symbol, body FROM work")
        ]
        self.assertEqual(len(queries), 1)
        self.assertIn(
            "WHERE symbol IN ('DONE','TEST') AND json_extract(body, '$.status') IS NOT 'complete'", queries[0]
        )

    def test_selected_reader_observes_changed_obligation_without_unselected_decode(self):
        first = universe_state.read_state(self.store, symbols=["TEST"])
        changed = {**self.selected, "obligation_id": "renewed", "required_group": ["TEST", "PEER"]}
        universe_state.put_work(self.store, "TEST", changed)
        second = universe_state.read_state(self.store, symbols=["TEST"])
        self.assertEqual(first["work"]["TEST"]["obligation_id"], "selected")
        self.assertEqual(second["work"], {"TEST": changed})
        self.assertEqual(second["revision"], first["revision"] + 1)

    def test_empty_scope_does_not_bypass_incomplete_selector(self):
        with closing(connect_sqlite(self.path)) as connection, connection:
            connection.execute("UPDATE state SET complete=0 WHERE id=1")
        with self.assertRaisesRegex(LineageError, "incomplete universe work state"):
            universe_state.read_state(self.store, symbols=[])

    def test_scope_values_are_bound_parameters(self):
        with patch.object(universe_state, "_read_work", side_effect=AssertionError("unselected body decoded")):
            result = universe_state.read_state(self.store, symbols=["TEST') OR 1=1 --"])
        self.assertEqual(result["work"], {})


class WorkflowObservationHeadTests(unittest.TestCase):
    def test_acceptance_keeps_only_current_head_and_each_obligations_origin(self):
        from test_alpaca_lineage_build import asset

        from libs.alpaca_lineage_build.catalogue import completed_catalogue

        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory)
            universe_state.initialize(store)
            observation = completed_catalogue([asset("PEER"), asset("ACTIVE")], {"steps": []})
            obligation = {"status": "pending", "obligation_id": "same-work"}
            peer = {"status": "complete", "obligation_id": "completed"}
            encode = json.dumps

            def work_only(value):
                self.assertNotIn("records", value, "workflow serialized the provider catalogue")
                return encode(value)

            def accept(day, work):
                observed = {**observation, "catalogue_observed_at_utc": f"2026-11-{day:02}T12:00:00Z"}
                return universe_state.accept(store, observed, list(work), work, handoff=True)

            with patch.object(universe_state.json, "dumps", side_effect=work_only):
                first = accept(1, {"ACTIVE": obligation, "PEER": peer})
                second = accept(2, {"ACTIVE": obligation})
                self.assertEqual(first["observation_id"], 1)
                self.assertEqual(second["work"]["ACTIVE"]["observation_id"], 2)
                self.assertEqual(second["work"]["ACTIVE"]["obligation_observation_id"], 1)
                renewed = accept(3, {"ACTIVE": {**obligation, "obligation_id": "renewed"}})
                self.assertEqual(renewed["work"]["ACTIVE"]["obligation_observation_id"], 3)
                for day in range(4, 25):
                    accept(day, {})
            state = universe_state.read_state(store)
            self.assertEqual(
                state["last_catalogue_observation"], {"id": 24, "observed_at_utc": "2026-11-24T12:00:00Z"}
            )
            self.assertEqual(state["work"], {"ACTIVE": renewed["work"]["ACTIVE"], "PEER": first["work"]["PEER"]})
            self.assertEqual(state["registration_pending"], ["ACTIVE", "PEER"])
            self.assertEqual(state["revision"], 24)
            with closing(connect_sqlite(store / universe_state.STATE_FILE, readonly=True)) as connection:
                tables = {
                    row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
                }
                self.assertEqual(tables, {"registration", "work", "state", "observation_head"})
                self.assertEqual(
                    connection.execute("SELECT * FROM observation_head").fetchall(),
                    [(24, "2026-11-24T12:00:00Z")],
                )
            with self.assertRaises(TypeError):
                accept(25, {"UNCOMMITTED": {"obligation_id": object()}})
            self.assertEqual(universe_state.read_state(store), state)


class WorkflowMigrationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name)
        initialize_store(self.store)
        self.path = self.store / universe_state.STATE_FILE
        self.registration = [("DONE", 1), ("PENDING", 0)]
        self.work = [
            ("DONE", '{"status":"complete","obligation_id":"done","observation_id":3}'),
            (
                "PENDING",
                '{ "status": "pending", "obligation_id": "original", "observation_id": 11, '
                '"obligation_observation_id": 3, "retired_uuid_observation": {"status": 404} }',
            ),
        ]
        with closing(connect_sqlite(self.path, create=True)) as connection, connection:
            connection.executescript(
                "PRAGMA user_version=1;"
                "CREATE TABLE state (id INTEGER PRIMARY KEY, complete INTEGER, revision INTEGER);"
                "CREATE TABLE observations (id INTEGER PRIMARY KEY, observed_at TEXT, body TEXT);"
                "CREATE TABLE registration (symbol TEXT PRIMARY KEY, complete INTEGER NOT NULL);"
                "CREATE TABLE work (symbol TEXT PRIMARY KEY, body TEXT NOT NULL);"
                "INSERT INTO state VALUES (1,1,37);"
            )
            connection.executemany(
                "INSERT INTO observations VALUES (?, ?, ?)",
                [(i, f"2026-11-{i:02}T12:00:00Z", "archived" * 16_384) for i in (3, 7, 11, 23)],
            )
            connection.executemany("INSERT INTO registration VALUES (?, ?)", self.registration)
            connection.executemany("INSERT INTO work VALUES (?, ?)", self.work)

    @staticmethod
    def no_catalogue_bodies(*args, **kwargs):
        connection = connect_sqlite(*args, **kwargs)

        def authorize(action, table, column, database, caller):
            if action == sqlite3.SQLITE_READ and (table, column) == ("observations", "body"):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        return connection

    def test_explicit_migration_drops_bodies_and_preserves_current_controls(self):
        original_size = self.path.stat().st_size
        with self.assertRaisesRegex(LineageError, "rebuild"):
            universe_state.require_initialized(self.store)
        with (
            patch.object(universe_state, "connect_sqlite", side_effect=self.no_catalogue_bodies),
            patch.object(universe_state, "_read_work", side_effect=AssertionError("migration decoded work")),
        ):
            result = rebuild_universe(self.store)
        self.assertEqual(result["pending_work"], 1)
        self.assertEqual(result["registration_pending"], 1)
        state = universe_state.read_state(self.store)
        self.assertEqual(state["revision"], 37)
        self.assertEqual(
            state["last_catalogue_observation"], {"id": 23, "observed_at_utc": "2026-11-23T12:00:00Z"}
        )
        self.assertEqual(state["registration_pending"], ["PENDING"])
        with closing(connect_sqlite(self.path, readonly=True)) as connection:
            self.assertEqual(
                connection.execute("SELECT * FROM registration ORDER BY symbol").fetchall(), self.registration
            )
            self.assertEqual(connection.execute("SELECT * FROM work ORDER BY symbol").fetchall(), self.work)
        self.assertLess(self.path.stat().st_size, original_size // 2)
        self.assertEqual(rebuild_universe(self.store), result)
        self.assertEqual(universe_state.read_state(self.store), state)
        updated = universe_state.accept(
            self.store,
            {"catalogue_observed_at_utc": "2026-11-24T12:00:00Z"},
            [],
            {"PENDING": state["work"]["PENDING"]},
            handoff=True,
        )
        self.assertEqual(updated["observation_id"], 24)
        self.assertEqual(updated["work"]["PENDING"]["obligation_observation_id"], 3)
        self.assertEqual(updated["revision"], 38)

    def test_interrupted_migration_resumes_from_last_commit_without_repeating_vacuum(self):
        def interrupt(sqlite_action, operation=None, argument=None):
            def connect(*args, **kwargs):
                connection = connect_sqlite(*args, **kwargs)

                def authorize(action, first, second, database, caller):
                    if action == sqlite_action and (
                        operation is None or (first, second) == (operation, argument)
                    ):
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                connection.set_authorizer(authorize)
                return connection

            return patch.object(universe_state, "connect_sqlite", side_effect=connect)

        # VACUUM attaches a temporary database before copying the remaining tables.
        vacuum = (sqlite3.SQLITE_ATTACH,)
        with interrupt(sqlite3.SQLITE_PRAGMA, "user_version", "-2"), self.assertRaises(sqlite3.DatabaseError):
            rebuild_universe(self.store)
        with closing(connect_sqlite(self.path, readonly=True)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone(), (1,))
            self.assertEqual(connection.execute("SELECT count(*) FROM observations").fetchone(), (4,))
        for operation in (vacuum, (sqlite3.SQLITE_PRAGMA, "user_version", "2")):
            with self.subTest(operation=operation):
                with interrupt(*operation), self.assertRaises(sqlite3.DatabaseError):
                    rebuild_universe(self.store)
                with closing(connect_sqlite(self.path, readonly=True)) as connection:
                    self.assertEqual(connection.execute("PRAGMA user_version").fetchone(), (-2,))
                    free_pages = connection.execute("PRAGMA freelist_count").fetchone()[0]
                    self.assertEqual(free_pages > 0, operation == vacuum)
                    self.assertEqual(
                        connection.execute("SELECT * FROM work ORDER BY symbol").fetchall(), self.work
                    )
                with self.assertRaisesRegex(LineageError, "rebuild"):
                    universe_state.require_initialized(self.store)
        with interrupt(*vacuum):
            self.assertEqual(rebuild_universe(self.store)["pending_work"], 1)

    def test_migration_before_first_catalogue_preserves_empty_head(self):
        with closing(connect_sqlite(self.path)) as connection, connection:
            connection.execute("DELETE FROM observations")
            connection.execute("DELETE FROM work")
        rebuild_universe(self.store)
        self.assertIsNone(universe_state.read_state(self.store)["last_catalogue_observation"])
        marker = {"catalogue_observed_at_utc": "2026-11-23T12:00:00Z"}
        self.assertEqual(universe_state.accept(self.store, marker, [], {}), 1)

    def test_interrupted_legacy_transaction_is_rolled_back_before_migration(self):
        leave_hot_journal(self.path, "DELETE FROM work WHERE symbol='PENDING'")
        result = rebuild_universe(self.store)
        self.assertEqual(result["pending_work"], 1)
        with closing(connect_sqlite(self.path, readonly=True)) as connection:
            self.assertEqual(connection.execute("SELECT * FROM work ORDER BY symbol").fetchall(), self.work)

    def test_incomplete_or_unknown_versions_preserve_original_work(self):
        for version, complete in [(1, 0), (99, 1)]:
            with self.subTest(version=version):
                with closing(connect_sqlite(self.path)) as connection, connection:
                    connection.execute(f"PRAGMA user_version={version}")
                    connection.execute("UPDATE state SET complete=?", (complete,))
                with self.assertRaises(LineageError):
                    universe_state.initialize(self.store)
                with closing(connect_sqlite(self.path, readonly=True)) as connection:
                    self.assertEqual(connection.execute("PRAGMA user_version").fetchone(), (version,))
                    self.assertEqual(
                        connection.execute("SELECT * FROM work ORDER BY symbol").fetchall(), self.work
                    )

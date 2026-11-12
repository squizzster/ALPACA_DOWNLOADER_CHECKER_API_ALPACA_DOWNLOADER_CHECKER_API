"""Selected workflow ingress never opens unrelated durable correction bodies."""

import json
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_lineage_build import universe_state
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import DownloadError
from libs.common.sqlite_connections import connect_sqlite


class SelectedWorkStateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name)
        universe_state.initialize(self.store)
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
            connection.execute(
                "INSERT INTO observations VALUES (1, '2026-11-08T12:00:00Z', 'unneeded observation body')"
            )
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

"""Synthetic conversion contracts; no downloads or production-data fixtures."""

import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from cli_support import export_main as main

from alpaca_end_of_day import StandaloneReaderError
from libs.alpaca_bloodhound_export.command_line import SnapshotChangedError, export, schema
from libs.alpaca_bloodhound_export.factors import ADJUSTMENTS
from libs.alpaca_bloodhound_export.memory import ExportMemoryError, OutputMemory
from libs.alpaca_bloodhound_export.projection import ExportError, checked_ids, project_history, series_id

HAS_ARROW = importlib.util.find_spec("pyarrow") is not None
ROOT = Path(__file__).resolve().parents[1]
EXPECTED_COLUMNS = (
    "date",
    "ohlcv_series_id",
    "symbol",
    "cik",
    "composite_figi",
    "share_class_figi",
    "company_name",
    "security_type",
    "exchange",
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "raw_close",
    "volume",
    "dollar_volume",
    "adjustment_ratio",
    "split_adjustment_ratio",
)


def public_id(value):
    return f"{value:064x}"


def history(value=2**53, source="SOURCE", latest="LATEST"):
    """Two observations with an intentional missing session and exact scale factors."""
    identifier = public_id(value)
    security = {
        "public_id": identifier,
        "tickers": sorted({"EARLIER", latest}),
        "latest_tickers": [latest],
        "read_symbol": source,
    }
    generation = f"group:{source}"
    evidence = {
        "generation": generation,
        "source_symbols": [source],
        "sources": {
            source: {
                "lineage_generation": f"receipt:{source}",
                "pricing_generation": f"pricing:{source}",
                "pricing_request": {"asof": "2024-01-05"},
                "history_start": "2024-01-01T00:00:00Z",
                "processed_through": "2024-01-05T00:00:00Z",
            },
        },
        "securities": [security],
    }
    views = {}
    for adjustment in ADJUSTMENTS:
        split = adjustment == "all" or "split" in adjustment.split(",")
        dividend = adjustment == "all" or "dividend" in adjustment.split(",")
        factor = (0.5 if split else 1.0) * (0.9 if dividend else 1.0)
        bars = []
        for session, label in (("2024-01-02", "EARLIER"), ("2024-01-04", latest)):
            row = {
                key: str(number * factor)
                for key, number in {"open": 100, "high": 110, "low": 90, "close": 105, "vwap": 102}.items()
            }
            row.update(session=session, tickers=[label], volume="226.25" if split else "113.125")
            bars.append(row)
        views[adjustment] = {"bars": bars, "bar_count": len(bars)}
    return {"generation": generation, "security": security, "evidence": evidence, "views": views}


class ReaderDouble:
    """Published-group boundary double; deliberately no network or source mutation API."""

    def __init__(self, store, histories):
        self.store = Path(store)
        self.histories = {item["security"]["public_id"]: item for item in histories}
        self.roster_calls = 0
        self.history_calls = []
        self.changed_roster_at_end = False
        self.changed_group_at_end = False
        self.errors = {}
        self.cache_limits = []

    def list_published_sources(self):
        self.roster_calls += 1
        sources = {}
        for item in self.histories.values():
            for symbol, source in item["evidence"]["sources"].items():
                generation = source["lineage_generation"]
                if self.changed_roster_at_end and self.roster_calls > 1:
                    generation += ":changed"
                sources[symbol] = {"symbol": symbol, "generation": generation}
        return {"generation": "roster:fixture", "sources": list(sources.values())}

    def get_evidence(self, symbol):
        selected = [item for item in self.histories.values() if symbol in item["evidence"]["source_symbols"]]
        if not selected:
            raise StandaloneReaderError("no completed source")
        evidence = copy.deepcopy(selected[0]["evidence"])
        evidence["securities"] = [item["security"] for item in selected]
        if self.changed_group_at_end and self.history_calls:
            evidence["generation"] += ":changed"
        return evidence

    def get_histories(self, symbol, *, public_id, include_evidence):
        self.history_calls.append((symbol, public_id, include_evidence))
        if public_id in self.errors:
            raise StandaloneReaderError(self.errors[public_id])
        item = copy.deepcopy(self.histories[public_id])
        item["evidence"] = self.get_evidence(symbol)
        return item

    def get_generation(self, symbol):
        return self.get_evidence(symbol)["generation"]

    def cache_info(self):
        return {"fixture": True}

    @contextmanager
    def open_group(self, symbol):
        owner = self

        class Group:
            summary = owner.get_evidence(symbol)

            def set_cache_limit(self, cache_bytes):
                owner.cache_limits.append(cache_bytes)

            def get_histories(self, *, public_id):
                locator = owner.histories[public_id]["security"]["read_symbol"]
                return owner.get_histories(locator, public_id=public_id, include_evidence=True)

            def check_current(self):
                if owner.get_generation(symbol) != self.summary["generation"]:
                    raise ExportError("selected pricing/lineage group changed during export; retry export")

        yield Group()


class BloodhoundProjectionTests(unittest.TestCase):
    def test_id_mapping_preserves_large_int64_and_rejects_real_modulus_collision(self):
        self.assertEqual(series_id(public_id(2**53)), 2**53 + 1)
        self.assertEqual(series_id(public_id(2**63 - 2)), 2**63 - 1)
        self.assertEqual(checked_ids([public_id(2**53)]), {public_id(2**53): 2**53 + 1})
        with self.assertRaisesRegex(ExportError, "collision"):
            checked_ids([public_id(0), public_id(2**63 - 1)])
        for value in ("ABCDEF", "f" * 63, "F" * 64, None):
            with self.subTest(value=value), self.assertRaises(ExportError):
                series_id(value)

    def test_exact_all_prices_fractional_raw_volume_and_no_gap_filling(self):
        source = history()
        before = copy.deepcopy(source)
        rows, mapping = project_history(source, series_id(source["security"]["public_id"]))
        self.assertEqual(source, before)
        self.assertEqual(tuple(rows[0]), EXPECTED_COLUMNS)
        self.assertEqual([row["date"] for row in rows], [datetime(2024, 1, 2), datetime(2024, 1, 4)])
        self.assertEqual({row["symbol"] for row in rows}, {"LATEST"})
        for row in rows:
            self.assertEqual(
                [row[f"adjusted_{key}"] for key in ("open", "high", "low", "close")],
                [45.0, 49.5, 40.5, 47.25],
            )
            self.assertEqual(row["raw_close"], 105.0)
            self.assertEqual(row["volume"], 113.125)
            self.assertEqual(row["dollar_volume"], 105.0 * 113.125)
            self.assertEqual(row["adjustment_ratio"], 47.25 / 105.0)
            self.assertAlmostEqual(row["split_adjustment_ratio"], 0.5, places=14)
            self.assertEqual(row["security_type"], "UNKNOWN")
            self.assertEqual(row["exchange"], "UNKNOWN")
            self.assertTrue(
                all(row[key] is None for key in ("cik", "composite_figi", "share_class_figi", "company_name"))
            )
        self.assertEqual(mapping["public_id"], source["security"]["public_id"])

    def test_latest_label_survives_clipping_before_rename(self):
        source = history()
        identifier = series_id(source["security"]["public_id"])
        full, _ = project_history(source, identifier)
        clipped, mapping = project_history(source, identifier, end="2024-01-02")
        self.assertEqual(clipped, full[:1])
        self.assertEqual(clipped[0]["symbol"], "LATEST")
        self.assertEqual(mapping["full_history_last_session"], "2024-01-04")
        self.assertEqual(mapping["last_session"], "2024-01-02")

    def test_invalid_all_prices_geometry_or_zero_close_cannot_be_published(self):
        for field, value in (("high", "40"), ("close", "0"), ("open", "nan")):
            source = history()
            source["views"]["all"]["bars"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                project_history(source, series_id(source["security"]["public_id"]))

    def test_ambiguous_latest_or_cross_view_labels_are_rejected(self):
        source = history()
        source["evidence"]["securities"][0]["latest_tickers"] = ["ONE", "TWO"]
        with self.assertRaisesRegex(ExportError, "latest.*ambiguous"):
            project_history(source, series_id(source["security"]["public_id"]))
        source = history()
        source["views"]["split"]["bars"][0]["tickers"] = ["UNRELATED"]
        with self.assertRaisesRegex(ExportError, "ticker assignments"):
            project_history(source, series_id(source["security"]["public_id"]))

    def test_non_session_and_duplicate_dates_rejected_without_inventing_rows(self):
        for session in ("2024-01-01", "2024-01-04"):
            source = history()
            for view in source["views"].values():
                view["bars"][0]["session"] = session
            with self.subTest(session=session), self.assertRaises(ValueError):
                project_history(source, series_id(source["security"]["public_id"]))


@unittest.skipUnless(HAS_ARROW, "install requirements-bloodhound.txt for Parquet integration tests")
class BloodhoundParquetTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.store = self.root / "source"
        self.store.mkdir()
        self.output = self.root / "result.parquet"
        self.reader = ReaderDouble(self.store, [history()])

    def read_file(self):
        import pyarrow.parquet as pq

        table = pq.read_table(self.output)
        metadata = json.loads(pq.read_metadata(self.output).metadata[b"alpaca_bloodhound"])
        return table, metadata

    def assert_no_output(self):
        self.assertFalse(self.output.exists())
        self.assertEqual(list(self.root.iterdir()), [self.store])

    def test_acquisition_creates_no_output_and_serialization_is_one_packed_write(self):
        import pyarrow.parquet as pq

        self.reader = ReaderDouble(self.store, [history(100, "FIRST"), history(200, "SECOND")])
        original_history = self.reader.get_histories
        original_write = pq.ParquetWriter.write_table
        written = []

        def read(*args, **kwargs):
            self.assertEqual(set(self.root.iterdir()), {self.store})
            return original_history(*args, **kwargs)

        def write(writer, table, **kwargs):
            self.assertEqual(len(self.reader.history_calls), 2)
            written.append(table.num_rows)
            return original_write(writer, table, **kwargs)

        with (
            patch.object(self.reader, "get_histories", side_effect=read),
            patch.object(pq.ParquetWriter, "write_table", write),
        ):
            export(self.reader, self.output)
        self.assertEqual(written, [4])
        self.assertEqual(pq.read_metadata(self.output).num_row_groups, 1)

    def test_memory_exhaustion_aborts_even_all_passed_without_creating_a_file(self):
        messages = []
        with (
            patch("libs.alpaca_bloodhound_export.command_line.tempfile.mkstemp", side_effect=AssertionError),
            self.assertRaisesRegex(ExportMemoryError, "no identities omitted"),
        ):
            export(self.reader, self.output, include_all_passed=True, memory_bytes=1, progress=messages.append)
        self.assertFalse(any(message.startswith("rejected ") for message in messages))
        self.assert_no_output()

    def test_retained_output_reduces_available_read_cache(self):
        with patch.object(self.reader, "cache_info", return_value={"maximum_bytes": 200000}):
            export(self.reader, self.output, memory_bytes=200000)
        self.assertEqual(self.reader.cache_limits[0], 150000)
        self.assertLess(self.reader.cache_limits[-1], self.reader.cache_limits[0])

    def test_freshness_is_checked_after_serialization(self):
        import pyarrow.parquet as pq

        original = pq.ParquetWriter.write_table

        def write(writer, table, **kwargs):
            result = original(writer, table, **kwargs)
            self.reader.changed_roster_at_end = True
            return result

        with (
            patch.object(pq.ParquetWriter, "write_table", write),
            self.assertRaisesRegex(ExportError, "selected lineage publications changed"),
        ):
            export(self.reader, self.output, include_all_passed=True)
        self.assert_no_output()

    def test_exact_eighteen_column_schema_and_roundtrip_large_integer(self):
        import pyarrow as pa

        result = export(self.reader, self.output)
        table, metadata = self.read_file()
        self.assertEqual(tuple(table.column_names), EXPECTED_COLUMNS)
        self.assertEqual(table.schema.field("date").type, pa.timestamp("ns"))
        self.assertEqual(table.schema.field("ohlcv_series_id").type, pa.int64())
        for name in EXPECTED_COLUMNS[2:9]:
            self.assertEqual(table.schema.field(name).type, pa.string())
        for name in EXPECTED_COLUMNS[9:]:
            self.assertEqual(table.schema.field(name).type, pa.float64())
        self.assertTrue(table.schema.equals(schema(), check_metadata=False))
        self.assertEqual(table["ohlcv_series_id"].to_pylist(), [2**53 + 1] * 2)
        self.assertEqual(table["volume"].to_pylist(), [113.125] * 2)
        self.assertEqual(table["date"].cast(pa.int64()).to_pylist(), [1704153600000000000, 1704326400000000000])
        self.assertEqual(result["rows"], 2)
        self.assertEqual(metadata["identities"][0]["public_id"], public_id(2**53))
        self.assertNotIn("partial", metadata)
        self.assertEqual(metadata["converter_version"], 7)
        self.assertEqual(metadata["price_basis"], "alpaca_all_preserved")
        self.assertEqual(metadata["volume_basis"], "alpaca_raw_as_traded")
        self.assertEqual(metadata["dated_type_and_exchange"], "UNKNOWN")
        self.assertEqual(set(self.root.iterdir()), {self.store, self.output})
        self.assertEqual(list(self.store.iterdir()), [])

    def test_reused_label_remains_two_series_and_two_full_identity_mappings(self):
        self.reader = ReaderDouble(
            self.store, [history(100, "FIRST", "REUSED"), history(200, "SECOND", "REUSED")]
        )
        export(self.reader, self.output)
        table, metadata = self.read_file()
        self.assertEqual(set(table["symbol"].to_pylist()), {"REUSED"})
        self.assertEqual(set(table["ohlcv_series_id"].to_pylist()), {101, 201})
        self.assertEqual(
            {item["public_id"] for item in metadata["identities"]}, {public_id(100), public_id(200)}
        )
        self.assertEqual(metadata["published_identities"], 2)

    def test_requested_id_and_dates_select_without_forwarding_a_clipped_reader_window(self):
        self.reader = ReaderDouble(self.store, [history(100, "FIRST"), history(200, "SECOND")])
        export(self.reader, self.output, public_ids=[public_id(200)], start="2024-01-02", end="2024-01-02")
        table, metadata = self.read_file()
        self.assertEqual(self.reader.history_calls, [("SECOND", public_id(200), True)])
        self.assertEqual(table["ohlcv_series_id"].to_pylist(), [201])
        self.assertEqual(table["symbol"].to_pylist(), ["LATEST"])
        self.assertEqual(metadata["identities"][0]["full_history_last_session"], "2024-01-04")

    def test_fixed_input_snapshot_produces_identical_parquet_bytes(self):
        export(self.reader, self.output)
        first = self.output.read_bytes()
        self.reader = ReaderDouble(self.store, [history()])
        export(self.reader, self.output, replace=True)
        self.assertEqual(self.output.read_bytes(), first)
        self.assertEqual(set(self.root.iterdir()), {self.store, self.output})

    def test_clipping_reuses_the_full_history_factor_calculation(self):
        source = history()
        with patch("libs.alpaca_bloodhound_export.projection.infer_split_factors") as infer:
            infer.return_value = ([0.5, 0.5], {"synthetic_test": True})
            self.reader = ReaderDouble(self.store, [source])
            export(self.reader, self.output, end="2024-01-02")
        infer.assert_called_once()
        received = infer.call_args.args[0]
        self.assertEqual([row["session"] for row in received["raw"]["bars"]], ["2024-01-02", "2024-01-04"])
        table, _ = self.read_file()
        self.assertEqual(table["split_adjustment_ratio"].to_pylist(), [0.5])

    def test_failure_after_written_rows_leaves_no_output_or_temporary_file(self):
        self.reader = ReaderDouble(self.store, [history(100, "AGOOD"), history(200, "ZBAD")])
        self.reader.errors[public_id(200)] = "conflicting source observations"
        with self.assertRaisesRegex(ExportError, "conflicting source"):
            export(self.reader, self.output)
        self.assertEqual(len(self.reader.history_calls), 2)
        self.assert_no_output()

    def test_existing_destination_not_overwritten_and_failed_replace_retains_original(self):
        original = b"already published fixture"
        self.output.write_bytes(original)
        with self.assertRaisesRegex(ExportError, "output exists"):
            export(self.reader, self.output)
        self.assertEqual(self.output.read_bytes(), original)
        self.assertEqual(self.reader.roster_calls, 0)
        self.reader.errors[public_id(2**53)] = "bad fixture"
        with self.assertRaises(ExportError):
            export(self.reader, self.output, replace=True)
        self.assertEqual(self.output.read_bytes(), original)
        self.assertEqual(set(self.root.iterdir()), {self.store, self.output})

    def test_include_all_passed_keeps_rejections_out_of_parquet_and_result(self):
        self.reader = ReaderDouble(
            self.store, [history(100, "GOOD"), history(200, "BAD"), history(300, "LATER")]
        )
        self.reader.errors[public_id(200)] = "conflicting source observations"
        messages = []
        result = export(self.reader, self.output, include_all_passed=True, progress=messages.append)
        table, metadata = self.read_file()
        self.assertEqual(table.num_rows, 4)
        self.assertEqual(set(table["ohlcv_series_id"].to_pylist()), {101, 301})
        self.assertEqual(metadata["published_identities"], 2)
        self.assertEqual(set(metadata["groups"]), {"group:GOOD", "group:LATER"})
        for field in ("partial", "failures", "selected_identities", "sources_without_resolved_ids"):
            self.assertNotIn(field, metadata)
            self.assertNotIn(field, result)
        self.assertNotIn(public_id(200), json.dumps(metadata))
        self.assertNotIn("conflicting source observations", json.dumps(metadata))
        self.assertTrue(any(f"public_id={public_id(200)}" in message for message in messages))
        self.assertIn("1 source/identity checks rejected", messages[-1])
        self.assertEqual(len(self.reader.history_calls), 3)

    def test_changed_final_publications_abort_even_with_include_all_passed(self):
        for changed in ("changed_roster_at_end", "changed_group_at_end"):
            with self.subTest(changed=changed):
                self.reader = ReaderDouble(self.store, [history()])
                setattr(self.reader, changed, True)
                with self.assertRaisesRegex(ExportError, "changed during export"):
                    export(self.reader, self.output, include_all_passed=True)
                self.assert_no_output()

    def test_failed_selected_source_is_still_generation_pinned_for_all_passed_export(self):
        self.reader = ReaderDouble(self.store, [history(100, "GOOD"), history(200, "BAD")])
        original_evidence = self.reader.get_evidence
        original_roster = self.reader.list_published_sources

        def evidence(symbol):
            if symbol == "BAD":
                raise StandaloneReaderError("temporarily unavailable selected source")
            return original_evidence(symbol)

        def roster():
            result = original_roster()
            if self.reader.roster_calls > 1:
                for source in result["sources"]:
                    if source["symbol"] == "BAD":
                        source["generation"] += ":changed"
            return result

        with (
            patch.object(self.reader, "get_evidence", side_effect=evidence),
            patch.object(self.reader, "list_published_sources", side_effect=roster),
            self.assertRaisesRegex(ExportError, "selected lineage publications changed"),
        ):
            export(self.reader, self.output, include_all_passed=True)
        self.assertEqual(self.reader.history_calls, [("GOOD", public_id(100), True)])
        self.assert_no_output()

    def test_selection_or_history_generation_change_always_aborts(self):
        for phase in ("selection", "history"):
            with self.subTest(phase=phase):
                self.reader = ReaderDouble(self.store, [history(100, "GOOD"), history(200, "BAD")])
                original_evidence = self.reader.get_evidence
                original_histories = self.reader.get_histories

                def evidence(symbol):
                    result = original_evidence(symbol)
                    if symbol == "BAD" and phase == "selection":
                        result["sources"]["BAD"]["lineage_generation"] += ":changed"
                    return result

                def histories(symbol, **kwargs):
                    result = original_histories(symbol, **kwargs)
                    if symbol == "BAD" and phase == "history":
                        result["generation"] += ":changed"
                    return result

                with (
                    patch.object(self.reader, "get_evidence", side_effect=evidence),
                    patch.object(self.reader, "get_histories", side_effect=histories),
                    self.assertRaisesRegex(SnapshotChangedError, "publication changed after"),
                ):
                    export(self.reader, self.output, include_all_passed=True)
                self.assertEqual(len(self.reader.history_calls), 0 if phase == "selection" else 1)
                self.assert_no_output()

    def test_late_collision_discards_staged_groups_before_publication(self):
        self.reader = ReaderDouble(self.store, [history(0, "ONE"), history(2**63 - 1, "TWO")])
        with self.assertRaisesRegex(ExportError, "collision"):
            export(self.reader, self.output)
        self.assertEqual(self.reader.history_calls, [("ONE", public_id(0), True)])
        self.assert_no_output()

    def test_source_datastore_output_is_rejected(self):
        output = self.store / "forbidden.parquet"
        with self.assertRaisesRegex(ExportError, "outside the source"):
            export(self.reader, output)
        self.assertFalse(output.exists())
        self.assertEqual(self.reader.roster_calls, 0)

    def test_cli_single_fixture_roundtrip(self):
        from io import StringIO

        stdout, stderr = StringIO(), StringIO()
        with (
            patch(
                "libs.alpaca_bloodhound_export.command_line.StandaloneReader", return_value=self.reader
            ) as reader,
            patch("sys.stdout", stdout),
            patch("sys.stderr", stderr),
        ):
            status = main(["--store", str(self.store), "--output", str(self.output), "--symbol", " source "])
        reader.assert_called_once_with(self.store, wait=False, cache_bytes=2048 * 1024**2)
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout.getvalue())["rows"], 2)
        self.assertIn("processed 1/1 source locators; converted 1 identities", stderr.getvalue())
        table, metadata = self.read_file()
        self.assertEqual(table.num_rows, 2)
        self.assertNotIn("partial", metadata)

    def test_cli_can_disable_cache_and_rejects_invalid_budgets_before_store_access(self):
        from io import StringIO

        for value in ("-1", "6145", "not-an-integer"):
            with (
                self.subTest(value=value),
                patch("libs.alpaca_bloodhound_export.command_line.StandaloneReader") as reader,
                patch("sys.stderr", StringIO()),
                self.assertRaises(SystemExit) as error,
            ):
                main(["--output", str(self.output), "--cache-mib", value])
            self.assertEqual(error.exception.code, 2)
            reader.assert_not_called()
            self.assert_no_output()
        with (
            patch(
                "libs.alpaca_bloodhound_export.command_line.StandaloneReader", return_value=self.reader
            ) as reader,
            patch("sys.stdout", StringIO()),
            patch("sys.stderr", StringIO()),
        ):
            status = main(["--store", str(self.store), "--output", str(self.output), "--cache-mib", "0"])
        self.assertEqual(status, 0)
        reader.assert_called_once_with(self.store, wait=False, cache_bytes=0)

    def test_cli_accepts_explicit_six_gib_cache(self):
        from io import StringIO

        with (
            patch(
                "libs.alpaca_bloodhound_export.command_line.StandaloneReader", return_value=self.reader
            ) as reader,
            patch("sys.stdout", StringIO()),
            patch("sys.stderr", StringIO()),
        ):
            status = main(["--store", str(self.store), "--output", str(self.output), "--cache-mib", "6144"])
        self.assertEqual(status, 0)
        reader.assert_called_once_with(self.store, wait=False, cache_bytes=6144 * 1024**2)

    def test_cli_rejections_go_only_to_stderr_and_success_summary_stays_clean(self):
        from io import StringIO

        self.reader = ReaderDouble(self.store, [history(100, "GOOD"), history(200, "BAD")])
        self.reader.errors[public_id(200)] = "conflicting source observations"
        stdout, stderr = StringIO(), StringIO()
        with (
            patch("libs.alpaca_bloodhound_export.command_line.StandaloneReader", return_value=self.reader),
            patch("sys.stdout", stdout),
            patch("sys.stderr", stderr),
        ):
            status = main(["--output", str(self.output), "--include-all-passed"])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"output": str(self.output), "rows": 2, "identities": 1})
        self.assertIn(f"rejected source=BAD public_id={public_id(200)}", stderr.getvalue())
        self.assertIn("conflicting source observations", stderr.getvalue())
        _, metadata = self.read_file()
        self.assertNotIn("BAD", json.dumps(metadata))

    def test_one_bad_session_rejects_whole_id_but_not_other_id_in_same_group(self):
        good, bad = history(100, "SHARED"), history(200, "SHARED")
        bad["views"]["split"]["bars"][-1]["open"] = "9999"
        self.reader = ReaderDouble(self.store, [good, bad])
        export(self.reader, self.output, include_all_passed=True)
        table, metadata = self.read_file()
        self.assertEqual(table["ohlcv_series_id"].to_pylist(), [101, 101])
        self.assertEqual([item["public_id"] for item in metadata["identities"]], [public_id(100)])
        self.assertEqual(set(metadata["groups"]), {"group:SHARED"})

    def test_all_rejected_produces_no_output_and_failed_replace_keeps_existing(self):
        self.reader.errors[public_id(2**53)] = "invalid history"
        with self.assertRaisesRegex(ExportError, "no valid observations"):
            export(self.reader, self.output, include_all_passed=True)
        self.assert_no_output()
        self.output.write_bytes(b"existing export")
        with self.assertRaisesRegex(ExportError, "no valid observations"):
            export(self.reader, self.output, include_all_passed=True, replace=True)
        self.assertEqual(self.output.read_bytes(), b"existing export")
        self.assertEqual(set(self.root.iterdir()), {self.store, self.output})

    def test_unreadable_source_does_not_prevent_other_sources_passing(self):
        self.reader = ReaderDouble(self.store, [history(100, "GOOD"), history(200, "BAD")])
        original = self.reader.get_evidence

        def evidence(symbol):
            if symbol == "BAD":
                raise StandaloneReaderError("no completed source")
            return original(symbol)

        messages = []
        with patch.object(self.reader, "get_evidence", side_effect=evidence):
            export(self.reader, self.output, include_all_passed=True, progress=messages.append)
        table, metadata = self.read_file()
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(set(metadata["groups"]), {"group:GOOD"})
        self.assertTrue(any("rejected source=BAD" in message for message in messages))

    def test_unassigned_source_is_reported_only_on_diagnostic_stream(self):
        self.reader = ReaderDouble(self.store, [history(100, "GOOD"), history(200, "UNASSIGNED")])
        original = self.reader.get_evidence

        def evidence(symbol):
            result = original(symbol)
            if symbol == "UNASSIGNED":
                result["securities"] = []
            return result

        messages = []
        with patch.object(self.reader, "get_evidence", side_effect=evidence):
            export(self.reader, self.output, include_all_passed=True, progress=messages.append)
        _, metadata = self.read_file()
        self.assertNotIn("UNASSIGNED", json.dumps(metadata))
        self.assertTrue(any("source=UNASSIGNED: no resolved public IDs" in message for message in messages))

    def test_zero_window_identity_has_no_mapping_or_group_in_output(self):
        good, outside = history(100, "GOOD"), history(200, "OUTSIDE")
        for view in outside["views"].values():
            for row, session in zip(view["bars"], ("2023-12-27", "2023-12-28"), strict=True):
                row["session"] = session
        self.reader = ReaderDouble(self.store, [good, outside])
        messages = []
        export(self.reader, self.output, include_all_passed=True, start="2024-01-01", progress=messages.append)
        table, metadata = self.read_file()
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(metadata["published_identities"], 1)
        self.assertNotIn("OUTSIDE", json.dumps(metadata))
        self.assertTrue(any("no observations in requested window" in message for message in messages))

    def test_writer_failure_aborts_even_with_include_all_passed(self):
        with (
            patch("pyarrow.parquet.ParquetWriter.write_table", side_effect=OSError("disk full")),
            self.assertRaisesRegex(OSError, "disk full"),
        ):
            export(self.reader, self.output, include_all_passed=True)
        self.assert_no_output()


class BloodhoundCliHelpTests(unittest.TestCase):
    def test_standalone_help_has_output_selection_and_include_all_passed(self):
        command = ROOT / "alpaca-eod"
        result = subprocess.run(
            [sys.executable, "-B", str(command), "export", "bloodhound", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        for option in (
            "--output",
            "--store",
            "--public-id",
            "--symbol",
            "--include-all-passed",
            "--replace",
            "--cache-mib",
            "--memory-mib",
        ):
            self.assertIn(option, result.stdout)
        self.assertNotIn("--allow-partial", result.stdout)


@unittest.skipUnless(HAS_ARROW, "install requirements-bloodhound.txt for Arrow memory tests")
class OutputMemoryTests(unittest.TestCase):
    def test_columnar_concatenation_shares_buffers_and_releases_on_exit(self):
        import pyarrow as pa

        first = pa.table({"value": [1, 2]})
        second = pa.table({"value": [3]})
        with OutputMemory(1024**2) as memory:
            memory.add(first)
            memory.add(second)
            combined = memory.table()
            self.assertEqual(combined.to_pydict(), {"value": [1, 2, 3]})
            self.assertEqual(
                combined.column(0).chunk(0).buffers()[1].address,
                first.column(0).chunk(0).buffers()[1].address,
            )
            self.assertGreater(memory.used_bytes, memory.data_bytes)
            self.assertEqual(memory.data_bytes, first.get_total_buffer_size() + second.get_total_buffer_size())
        self.assertEqual(memory.used_bytes, 0)
        self.assertEqual(memory._tables, [])

    def test_bad_memory_limits_are_rejected(self):
        for value in (0, -1, True, None, "8192", 1.5):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "memory_bytes"):
                OutputMemory(value)


if __name__ == "__main__":
    unittest.main()

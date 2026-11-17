"""Direct Parquet uses CSV's accepted catalogue stream, never a CSV intermediary."""

import io
import json
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from importlib.util import find_spec
from pathlib import Path
from unittest import skipUnless
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import (
    CatalogChangedError,
    DataLibrary,
    PublicIdUnavailableError,
    ReaderBusyError,
    _catalogue_export,
)
from libs.alpaca_cli import main
from libs.alpaca_serving_catalogue import export_inputs
from libs.common.store_lock import StoreBusyError, operation_lock

HAS_CONVERTER = all(find_spec(name) is not None for name in ("numpy", "pandas", "pyarrow"))
if HAS_CONVERTER:
    import pyarrow.parquet as pq

    from bloodhound_app import alpaca_to_bloodhound as converter
    from bloodhound_app import catalogue_input


@skipUnless(HAS_CONVERTER, "install requirements-bloodhound.txt")
class CatalogueBloodhoundTests(NativeCase):
    def arguments(self, output, *extra, source=None):
        return converter.parser().parse_args(
            [
                "--data-in" if source is not None else "--store",
                str(source if source is not None else self.store),
                "--blood-out",
                str(output),
                "--quiet",
                *extra,
            ]
        )

    def test_direct_and_csv_conversion_have_identical_values_schema_ids_and_factors(self):
        data = self.bootstrap()
        csv = data.write_csv_export(self.directory / "csv")
        old_output = self.directory / "via-csv.parquet"
        direct_output = self.directory / "direct.parquet"
        old = converter.convert(self.arguments(old_output, source=csv["output"]))
        with (
            patch.object(DataLibrary, "write_csv_export", side_effect=AssertionError("intermediate CSV")),
            patch.object(converter, "discover_histories", side_effect=AssertionError("CSV discovery")),
            patch.object(converter, "read_frame", side_effect=AssertionError("CSV parsing")),
            patch.object(converter, "series_id", wraps=converter.series_id) as ids,
        ):
            direct = converter.convert(self.arguments(direct_output))
        self.assertEqual(ids.call_count, data.online_count())
        self.assertEqual(pq.read_table(old_output).to_pydict(), pq.read_table(direct_output).to_pydict())
        self.assertEqual(pq.read_schema(old_output), pq.read_schema(direct_output))
        self.assertEqual(old["serialized_validation"], direct["serialized_validation"])
        self.assertEqual(
            old["histories"][0]["split_factor_details"], direct["histories"][0]["split_factor_details"]
        )
        self.assertEqual(direct["input_format"], "alpaca_online_catalogue")
        self.assertEqual(direct["catalogue"]["generation"], data.catalog_generation())
        self.assertEqual(direct["histories_rejected"], 0)

    def test_source_bodies_read_once_no_authentication_and_no_reads_during_output(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        output = self.directory / "once.parquet"
        opened, phase = Counter(), {"writing": False}
        original_open, original_write = Path.open, pq.ParquetWriter.write_table
        original_hash, original_publish = converter.sha256_file, converter.publish_pair

        def open_file(path, *args, **kwargs):
            mode = args[0] if args else kwargs.get("mode", "r")
            if path.is_relative_to(self.store) and "r" in mode and not path.name.endswith(".lock"):
                self.assertFalse(phase["writing"], path)
                opened[path] += 1
            return original_open(path, *args, **kwargs)

        def write(writer, *args, **kwargs):
            phase["writing"] = True
            with self.assertRaises(StoreBusyError), operation_lock(self.store, exclusive=True):
                pass
            return original_write(writer, *args, **kwargs)

        def hashed(path):
            self.assertFalse(path.is_relative_to(self.store), path)
            return original_hash(path)

        def publish(*args):
            with self.assertRaises(StoreBusyError), operation_lock(self.store, exclusive=True):
                pass
            return original_publish(*args)

        with (
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            patch.object(Path, "open", open_file),
            patch.object(pq.ParquetWriter, "write_table", write),
            patch.object(converter, "sha256_file", hashed),
            patch.object(converter, "publish_pair", publish),
            patch("alpaca_end_of_day.standalone.read_index", side_effect=AssertionError("index census")),
            patch("alpaca_end_of_day.standalone.check_store", side_effect=AssertionError("store recheck")),
            patch.object(export_inputs, "select", wraps=export_inputs.select) as token,
        ):
            report = converter.convert(self.arguments(output))
        self.assertEqual(report["histories_exported"], 2)
        self.assertEqual(token.call_count, 1)
        self.assertTrue(opened)
        self.assertTrue(all(count == 1 for count in opened.values()), opened)
        # The format is serialized once after all selected source assembly.
        self.assertTrue(phase["writing"])
        with operation_lock(self.store, exclusive=True):
            pass

    def test_busy_and_dirty_inputs_fail_before_bodies_or_output(self):
        data = self.bootstrap()
        output = self.directory / "blocked.parquet"
        with (
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            operation_lock(self.store, exclusive=True),
            self.assertRaises(ReaderBusyError),
        ):
            converter.convert(self.arguments(output))
        self.assertFalse(output.exists())
        export_inputs.invalidate(self.store, "pricing", {"TEST": "f" * 64})
        with (
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            patch.object(data._reader, "_open_group", side_effect=AssertionError("dirty source read")),
            self.assertRaises(CatalogChangedError),
        ):
            converter.convert(self.arguments(output))
        self.assertFalse(output.exists())

    def test_unavailable_id_has_no_source_read_or_internal_status(self):
        data = self.bootstrap()
        with (
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            patch.object(data._reader, "_open_group", side_effect=AssertionError("unavailable source")),
            self.assertRaisesRegex(PublicIdUnavailableError, "public ID is not available"),
        ):
            converter.convert(self.arguments(self.directory / "absent.parquet", "--public-id", "0" * 64))

    def test_existing_precision_checks_reject_native_trade_count_conflict_and_preserve_pair(self):
        self.bootstrap()
        output = self.directory / "existing.parquet"
        converter.convert(self.arguments(output))
        report_path = Path(str(output) + ".report.json")
        prior = output.read_bytes(), report_path.read_bytes()
        original = catalogue_input.load

        def conflicting(*args, **kwargs):
            frames, provenance = original(*args, **kwargs)
            frames["split"].loc[0, "trade_count"] += 1
            return frames, provenance

        with (
            patch.object(catalogue_input, "load", conflicting),
            self.assertRaisesRegex(converter.ConversionError, "histories rejected"),
        ):
            converter.convert(self.arguments(output, "--overwrite"))
        self.assertEqual((output.read_bytes(), report_path.read_bytes()), prior)
        failure = json.loads(Path(str(output) + ".failure.json").read_text())
        self.assertEqual(failure["omitted_histories"][0]["source_issues"][0]["kind"], "trade_count_mismatch")

    def test_memory_failure_is_fatal_even_with_skip_and_never_repeats_acquisition(self):
        data = self.bootstrap()
        output = self.directory / "memory.parquet"
        original_add = catalogue_input.OutputTables.add

        def full(collector, *args):
            collector.maximum_bytes = 1
            return original_add(collector, *args)

        with (
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            patch.object(catalogue_input.OutputTables, "add", full),
            patch.object(data._reader, "_open_group", wraps=data._reader._open_group) as reads,
            self.assertRaisesRegex(MemoryError, "--memory-mib"),
        ):
            converter.convert(self.arguments(output, "--on-error", "skip"))
        self.assertEqual(reads.call_count, 1)
        self.assertFalse(output.exists())
        self.assertFalse(list(self.directory.glob("*.partial")))

    def test_interruption_closes_stream_and_releases_store_gate(self):
        data = self.bootstrap()
        with data.open_history_export() as exported:
            stream = exported.iter_histories()
            next(stream)
        with self.assertRaisesRegex(RuntimeError, "active, unused"):
            exported.iter_histories()
        self.assertEqual(list(stream), [])
        with operation_lock(self.store, exclusive=True):
            pass

    def test_canonical_cli_exports_online_catalogue_with_no_csv(self):
        self.bootstrap()
        output = self.directory / "cli.parquet"
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            patch.object(DataLibrary, "write_csv_export", side_effect=AssertionError("intermediate CSV")),
        ):
            code = main(["export", "bloodhound", "--store", str(self.store), "--output", str(output)])
        self.assertEqual(code, 0, stderr.getvalue())
        result = json.loads(stdout.getvalue())
        self.assertEqual(result["rows"], len(self.bars))
        self.assertEqual(result["identities"], 1)
        self.assertEqual(result["status"], "complete")
        self.assertEqual(json.loads(stderr.getvalue().splitlines()[-1])["status"], "success")
        self.assertEqual(pq.read_schema(output).remove_metadata(), converter.BLOODHOUND_SCHEMA)

    def test_cli_cache_options_and_invalid_budgets_preserve_selection_contract(self):
        data = self.bootstrap()
        for cache in (0, 6144):
            output = self.directory / f"cache-{cache}.parquet"
            stdout, stderr = io.StringIO(), io.StringIO()
            with (
                patch("alpaca_end_of_day.DataLibrary", return_value=data) as factory,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                code = main(
                    [
                        "export",
                        "bloodhound",
                        "--store",
                        str(self.store),
                        "--output",
                        str(output),
                        "--cache-mib",
                        str(cache),
                        "--symbol",
                        " test ",
                    ]
                )
            self.assertEqual(code, 0, stderr.getvalue())
            factory.assert_called_once_with(store=self.store, wait=False, cache_bytes=cache * 1024**2)
        for value in ("-1", "6145", "not-an-integer"):
            with (
                self.subTest(value=value),
                patch("alpaca_end_of_day.DataLibrary", side_effect=AssertionError("invalid store access")),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as error,
            ):
                main(
                    [
                        "export",
                        "bloodhound",
                        "--output",
                        str(self.directory / "bad.parquet"),
                        "--cache-mib",
                        value,
                    ]
                )
            self.assertEqual(error.exception.code, 2)

    def test_cli_explicit_skip_reports_rejections_without_contaminating_stdout(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        rejected = data.resolve_ticker("OTHER")[0].public_id
        original = catalogue_input.load

        def conflicting(history, *args, **kwargs):
            frames, provenance = original(history, *args, **kwargs)
            if history.public_id == rejected:
                frames["split"].loc[0, "trade_count"] += 1
            return frames, provenance

        output = self.directory / "partial.parquet"
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            patch.object(catalogue_input, "load", conflicting),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            code = main(
                [
                    "export",
                    "bloodhound",
                    "--store",
                    str(self.store),
                    "--output",
                    str(output),
                    "--include-all-passed",
                ]
            )
        self.assertEqual(code, 0, stderr.getvalue())
        result = json.loads(stdout.getvalue())
        self.assertEqual((result["identities"], result["rejected"], result["status"]), (1, 1, "partial"))
        self.assertIn("trade_count_mismatch", stderr.getvalue())
        self.assertEqual(set(pq.read_table(output)["symbol"].to_pylist()), {"TEST"})

    def test_unreadable_group_retains_peer_results_without_retry_and_requires_explicit_skip(self):
        self.add_symbol("OTHER")
        data = self.bootstrap()
        bad_id = data.resolve_ticker("OTHER")[0].public_id
        bad_group = data._records[bad_id]["group_key"]
        original, calls = _catalogue_export.read_group, Counter()

        def unreadable(data, selected, groups, key, start, end):
            calls[key] += 1
            if key == bad_group:
                raise OSError("unreadable selected payload")
            yield from original(data, selected, groups, key, start, end)

        output = self.directory / "read-failed.parquet"
        with (
            patch.object(_catalogue_export, "read_group", unreadable),
            self.assertRaisesRegex(converter.ConversionError, "histories rejected"),
        ):
            converter.convert(self.arguments(output))
        self.assertFalse(output.exists())
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(count == 1 for count in calls.values()))
        calls.clear()
        output = self.directory / "read-partial.parquet"
        with patch.object(_catalogue_export, "read_group", unreadable):
            report = converter.convert(self.arguments(output, "--on-error", "skip"))
        self.assertEqual((report["histories_exported"], report["histories_rejected"]), (1, 1))
        self.assertEqual(report["status"], "partial")
        self.assertEqual(set(pq.read_table(output)["symbol"].to_pylist()), {"TEST"})
        self.assertTrue(all(count == 1 for count in calls.values()))

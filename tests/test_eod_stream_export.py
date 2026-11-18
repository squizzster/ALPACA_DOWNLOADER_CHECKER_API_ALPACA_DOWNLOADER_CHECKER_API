"""The gzip producer consumes each accepted catalogue projection once."""

import gzip
import io
import json
import os
import sys
from collections import Counter
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from unittest import skipUnless
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase, valid_bars

from alpaca_end_of_day import DataLibrary
from libs.alpaca_cli import main
from libs.alpaca_serving_catalogue import export_inputs
from libs.common.store_lock import StoreBusyError, operation_lock

HAS_CONVERTER = all(find_spec(name) is not None for name in ("numpy", "pandas", "pyarrow"))
if HAS_CONVERTER:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from bloodhound_app import alpaca_to_bloodhound as converter
    from bloodhound_app import eod_stream


def read_events(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


@skipUnless(HAS_CONVERTER, "install requirements-bloodhound.txt")
class EodStreamTests(NativeCase):
    def test_cli_wire_protocol_matches_parquet_with_one_selected_handoff(self):
        self.add_symbol("OTHER", bars=valid_bars(start="2026-10-06"))
        data = self.bootstrap()
        output = self.directory / "trial.jsonl.gz"
        parquet = self.directory / "trial.parquet"
        arguments = ["--store", str(self.store), "--from", "2026-10-02", "--through", "2026-10-09"]
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(main(["export", "bloodhound", "--output", str(parquet), *arguments]), 0)
        expected = pq.read_table(parquet).to_pylist()
        for row in expected:
            row["date"] = row["date"].date().isoformat()
        expected.sort(key=lambda row: (row["date"], row["ohlcv_series_id"]))

        opened = Counter()
        publishing = False
        original_open, original_publish = Path.open, eod_stream._publish

        def open_file(path, *args, **kwargs):
            mode = args[0] if args else kwargs.get("mode", "r")
            if path.is_relative_to(self.store) and "r" in mode and not path.name.endswith(".lock"):
                self.assertFalse(publishing, path)
                opened[path] += 1
            return original_open(path, *args, **kwargs)

        def publish(*args, **kwargs):
            nonlocal publishing
            publishing = True
            with self.assertRaises(StoreBusyError), operation_lock(self.store, exclusive=True):
                pass
            return original_publish(*args, **kwargs)

        with (
            redirect_stdout(io.StringIO()) as stdout,
            redirect_stderr(io.StringIO()) as stderr,
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            patch.object(Path, "open", open_file),
            patch(
                "alpaca_end_of_day._standalone_group._group_securities",
                side_effect=AssertionError("membership replay"),
            ),
            patch(
                "alpaca_end_of_day.standalone.document_digest", side_effect=AssertionError("group digest replay")
            ),
            patch.object(eod_stream, "_publish", publish),
            patch.object(export_inputs, "select", wraps=export_inputs.select) as select,
            patch.object(eod_stream, "history_split_factors", wraps=converter.history_split_factors) as factors,
            patch.object(converter, "sha256_file", side_effect=AssertionError("reopened or hashed input")),
            patch.object(converter, "discover_histories", side_effect=AssertionError("CSV discovery")),
            patch.object(pq.ParquetWriter, "write_table", side_effect=AssertionError("Parquet intermediary")),
            patch.object(DataLibrary, "write_csv_export", side_effect=AssertionError("CSV intermediary")),
        ):
            self.assertEqual(main(["export", "eod_stream", "--output", str(output), *arguments]), 0)
        self.assertTrue(publishing)
        self.assertEqual(select.call_count, 1)
        self.assertEqual(factors.call_count, 2)
        self.assertTrue(opened)
        self.assertTrue(all(count == 1 for count in opened.values()), opened)
        self.assertNotIn("event_type", stdout.getvalue() + stderr.getvalue())
        summary = json.loads(stdout.getvalue())
        self.assertEqual(summary["rows"], len(expected))
        self.assertEqual(summary["identities"], 2)

        events = read_events(output)
        observations, run_ids = [], set()
        fields = {"blood_id", "event_type", "run_id"}
        index = 0
        while index < len(events):
            start = events[index]
            run_id = start["run_id"]
            self.assertEqual(start["event_type"], "START_EOD_MARKER")
            self.assertNotIn(run_id, run_ids)
            self.assertRegex(run_id, r"^[0-9a-f]{32}$")
            if index == 0:
                self.assertEqual(set(start), fields | {"source", "date_created"})
                self.assertEqual(start["source"], "alpaca")
                self.assertRegex(start["date_created"], r"^\d{4}-\d{2}-\d{2}T.+Z$")
            else:
                self.assertEqual(set(start), fields)
            run_ids.add(run_id)
            index += 1
            dates = set()
            while events[index]["event_type"] == "EOD":
                row = events[index]
                self.assertEqual(row["run_id"], run_id)
                dates.add(row["date"])
                observations.append({key: value for key, value in row.items() if key not in fields})
                index += 1
            self.assertEqual(len(dates), 1)
            self.assertEqual(
                events[index],
                {"blood_id": start["blood_id"], "event_type": "FINISH_EOD_MARKER", "run_id": run_id},
            )
            index += 1
        self.assertTrue(all(event["blood_id"] == "stock_engine_test_v1.0" for event in events))
        self.assertEqual(observations, expected)
        self.assertIsNone(observations[0]["cik"])
        self.assertEqual(summary["days"], len(run_ids))

    def test_empty_window_publishes_empty_gzip_and_selected_invalidations_fail(self):
        self.bootstrap()
        output = self.directory / "empty.jsonl.gz"
        selected = {"start": "2025-01-01", "end": "2025-01-31", "symbols": ["TEST"]}
        with patch.object(eod_stream, "load_history", side_effect=AssertionError("disjoint source body")):
            result = eod_stream.export(self.store, output, **selected)
        self.assertEqual((result["days"], result["rows"], result["identities"]), (0, 0, 0))
        self.assertEqual(gzip.decompress(output.read_bytes()), b"")

        export_inputs.invalidate(self.store, "pricing", {"TEST": "f" * 64})
        with (
            patch.object(eod_stream, "load_history", side_effect=AssertionError("dirty history opened")),
            patch.object(eod_stream, "_publish", side_effect=AssertionError("dirty stream published")),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(
                main(
                    [
                        "export",
                        "eod_stream",
                        "--store",
                        str(self.store),
                        "--output",
                        str(output),
                        "--replace",
                        "--from",
                        "2026-10-01",
                        "--through",
                        "2026-10-31",
                        "--symbol",
                        "TEST",
                    ]
                ),
                1,
            )
        self.assertEqual(gzip.decompress(output.read_bytes()), b"")

    def test_default_full_history_and_one_sided_windows(self):
        data = self.bootstrap()
        output = self.directory / "coverage.jsonl.gz"
        observed = {}
        original = eod_stream.load_history

        def load(history, *args, **kwargs):
            bars = history.catalogue_history.views
            frames, payloads, issues = original(history, *args, **kwargs)
            observed["bars"] = len({id(v["bars"]) for v in bars.values()})
            observed["frames"] = len({id(frame) for frame in frames.values()})
            return frames, payloads, issues

        with (
            patch.object(eod_stream, "load_history", load),
            patch("alpaca_end_of_day.DataLibrary", return_value=data),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(
                main(["export", "eod_stream", "--store", str(self.store), "--output", str(output)]), 0
            )
        full = [e for e in read_events(output) if e["event_type"] == "EOD"]
        self.assertEqual(len(full), len(self.bars))
        self.assertLess(observed["bars"], 8)
        self.assertEqual(observed["frames"], observed["bars"])
        for bounds, expected in (({"start": full[1]["date"]}, full[1:]), ({"end": full[-2]["date"]}, full[:-1])):
            with patch("alpaca_end_of_day.DataLibrary", return_value=data):
                result = eod_stream.export(self.store, output, replace=True, **bounds)
            self.assertEqual(result["retained_sessions"], len(full))
            rows = [e for e in read_events(output) if e["event_type"] == "EOD"]
            self.assertEqual([{**e, "run_id": None} for e in rows], [{**e, "run_id": None} for e in expected])

    def test_ordered_blocks_cross_days_with_exact_ids_nullable_fields_and_stable_ties(self):
        date1, date2 = datetime(2016, 1, 4), datetime(2026, 11, 9)
        large = 2**62 + 1

        def table(ident, dates, labels):
            return pa.table({"date": dates, "ohlcv_series_id": [ident] * len(dates), "label": labels})

        tables = [
            (large, table(large, [date1, date1, date2], ["first", None, "last"])),
            (-large, table(-large, [date1, date2], ["early", "late"])),
        ]
        destination = self.directory / "ordered.jsonl.gz"
        with (
            patch.object(eod_stream, "GATHER_ROWS", 2),
            patch.object(eod_stream, "DECODE_ROWS", 1),
            patch.object(eod_stream.shutil, "which", return_value=None),
            patch.object(eod_stream.subprocess, "Popen", side_effect=AssertionError("fallback spawned a child")),
            patch.object(eod_stream.pc, "sort_indices", wraps=eod_stream.pc.sort_indices) as sort,
            patch.object(eod_stream.pc, "take", wraps=eod_stream.pc.take) as gather,
        ):
            days, _, first, last = eod_stream._publish(destination, tables, "created", replace=False)
        self.assertEqual((days, first, last), (2, "2016-01-04", "2026-11-09"))
        self.assertEqual(tables, [])
        self.assertEqual(sort.call_count, 1)
        self.assertEqual([len(call.args[1]) for call in gather.call_args_list], [2, 2, 1])
        events = read_events(destination)
        self.assertEqual(
            [(event["ohlcv_series_id"], event["label"]) for event in events if event["event_type"] == "EOD"],
            [(-large, "early"), (large, "first"), (large, None), (-large, "late"), (large, "last")],
        )
        self.assertEqual(sum(event["event_type"] == "START_EOD_MARKER" for event in events), 2)

    def test_compressor_failure_or_interrupted_encoding_cannot_install_output(self):
        executable = self.directory / "pigz"
        executable.write_text(
            f"#!{sys.executable}\n"
            "import os, sys\n"
            "assert sys.argv[1:] == ['-n', '-6', '-p', '4', '-c']\n"
            "assert 'PIGZ' not in os.environ and 'GZIP' not in os.environ\n"
            "sys.stdin.buffer.read()\n"
            "sys.exit(17)\n"
        )
        executable.chmod(0o700)
        output = self.directory / "prior.jsonl.gz"
        prior = gzip.compress(b"prior")
        output.write_bytes(prior)
        with (
            patch.object(eod_stream.shutil, "which", return_value=str(executable)),
            patch.dict(os.environ, {"PIGZ": "-d", "GZIP": "-9"}),
        ):
            with self.assertRaisesRegex(eod_stream.ConversionError, "exit status 17"):
                eod_stream._publish(output, [], "created", replace=True)
            with (
                patch.object(eod_stream, "_write_events", side_effect=KeyboardInterrupt),
                self.assertRaises(KeyboardInterrupt),
            ):
                eod_stream._publish(output, [], "created", replace=True)
        self.assertEqual(output.read_bytes(), prior)
        self.assertEqual(list(self.directory.glob(".*.partial")), [])

    def test_writer_preserves_duplicates_assigns_fresh_runs_and_cleans_partial_output(self):
        output = self.directory / "duplicates.jsonl.gz"
        row = {"date": datetime(2026, 10, 7), "ohlcv_series_id": 5, "raw_close": 12.5}
        table = pa.Table.from_pylist([row, row])
        created = "2026-11-17T01:02:03Z"
        first_days, output_bytes, start, end = eod_stream._publish(output, [(5, table)], created, replace=False)
        first = read_events(output)
        self.assertEqual((start, end), ("2026-10-07", "2026-10-07"))
        self.assertEqual(first_days, 1)
        self.assertEqual(output_bytes, output.stat().st_size)
        self.assertEqual(
            [event["event_type"] for event in first], ["START_EOD_MARKER", "EOD", "EOD", "FINISH_EOD_MARKER"]
        )
        self.assertEqual(first[1], first[2])
        eod_stream._publish(output, [(5, table)], created, replace=True)
        second = read_events(output)
        self.assertNotEqual(first[0]["run_id"], second[0]["run_id"])
        prior = output.read_bytes()

        for value in (float("nan"), float("inf"), float("-inf")):
            invalid = pa.Table.from_pylist([{**row, "raw_close": value}])
            with self.assertRaisesRegex(ValueError, "JSON compliant"):
                eod_stream._publish(output, [(5, invalid)], created, replace=True)
            self.assertEqual(output.read_bytes(), prior)

        with (
            patch.object(eod_stream.os, "replace", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            eod_stream._publish(output, [(5, table)], created, replace=True)
        self.assertEqual(output.read_bytes(), prior)
        self.assertEqual(list(self.directory.glob(".*.partial")), [])

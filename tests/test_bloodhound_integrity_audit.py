"""The public audit command uses only an export pair and the repository calendar."""

import csv
import hashlib
import io
import json
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from unittest import TestCase, skipUnless
from unittest.mock import patch

from libs.alpaca_cli import main


@skipUnless(
    all(find_spec(name) for name in ("pyarrow", "duckdb", "numpy")), "install requirements-bloodhound-audit.txt"
)
class BloodhoundIntegrityTests(TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.parquet = self.root / "quoted ' [one].parquet"
        self.report = self.parquet.with_name(self.parquet.name + ".report.json")
        self.output = self.root / "audit"
        self.sid = 2**55 + 1

    def export_pair(self, prices, *, start=None, end=None, aliases=None, invalid=False):
        import pyarrow as pa
        import pyarrow.parquet as pq

        from libs.alpaca_bloodhound_export.command_line import schema

        series = []
        for day, symbol, adjusted in prices:
            series.append(
                dict(
                    date=datetime.fromisoformat(day),
                    ohlcv_series_id=self.sid,
                    symbol=symbol,
                    security_type="UNKNOWN",
                    exchange="UNKNOWN",
                    adjusted_open=adjusted,
                    adjusted_close=adjusted,
                    adjusted_high=adjusted,
                    adjusted_low=adjusted,
                    raw_close=10,
                    volume=2,
                    dollar_volume=999 if invalid else 20,
                    adjustment_ratio=adjusted / 10,
                    split_adjustment_ratio=adjusted / 10,
                )
            )
        pq.write_table(pa.Table.from_pylist(series, schema=schema()), self.parquet)
        content = self.parquet.read_bytes()
        first, last = prices[0][0], prices[-1][0]
        history = dict(
            status="exported",
            ohlcv_series_id=self.sid,
            dated_symbols=aliases or sorted({p[1] for p in prices}),
            first_session="2020-01-02" if start else first,
            last_session="2026-09-01" if end else last,
            output_rows=len(series),
        )
        outside = [dict(status="outside_date_filter", output_rows=0)] if start else []
        self.report.write_text(
            json.dumps(
                dict(
                    input_format="alpaca_online_catalogue",
                    status="complete",
                    histories=[history, *outside],
                    histories_discovered=1 + len(outside),
                    histories_exported=1,
                    omitted_histories=[],
                    output_sha256=hashlib.sha256(content).hexdigest(),
                    output_bytes=len(content),
                    output_rows=len(series),
                    output_written=True,
                    date_filter=dict(start=start, end=end),
                )
            )
        )
        return content

    def run_cli(self, *extra):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            patch("libs.alpaca_cli.operation_lock", side_effect=AssertionError("opened datastore")),
        ):
            code = main(
                [
                    "audit",
                    "bloodhound",
                    "--parquet",
                    str(self.parquet),
                    "--output",
                    str(self.output),
                    "--threads",
                    "1",
                    *extra,
                ]
            )
        return code, stdout.getvalue(), stderr.getvalue()

    def test_candidate_is_reviewable_and_all_outputs_are_offline(self):
        content = self.export_pair([("2026-03-17", "OLD", 30), ("2026-03-18", "NEW", 10)])
        code, out, err = self.run_cli("--series-id", str(self.sid))
        self.assertEqual(code, 0, err)
        summary = json.loads(out)
        self.assertEqual(summary["basis_candidates"], 1)
        self.assertEqual(summary["counts"], {"PASS": 42, "SKIP": 1})
        result = json.loads((self.output / "audit.json").read_text())
        self.assertEqual(result["facts"]["selected_series_ticker_changes"][0]["prev_symbol"], "OLD")
        with (self.output / "basis_shift_candidates.csv").open() as f:
            self.assertEqual([r["ohlcv_series_id"] for r in csv.DictReader(f)], [str(self.sid)])
        self.assertEqual(self.parquet.read_bytes(), content)
        report = (self.output / "report.md").read_text()
        self.assertIn(str(self.sid), report)
        self.assertNotEqual(self.run_cli()[0], 0)
        self.assertEqual((self.output / "report.md").read_text(), report)

    def test_single_session_filtered_export_handles_source_bounds_and_absent_candidates(self):
        self.export_pair(
            [("2026-03-18", "NEW", 10)], start="2026-03-18", end="2026-03-18", aliases=["OLD", "NEW"]
        )
        code, out, err = self.run_cli()
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["basis_candidates"], 0)
        self.assertIsNone(json.loads((self.output / "audit.json").read_text())["facts"]["prior_session"])

    def test_invalid_arithmetic_writes_evidence_and_fails(self):
        self.export_pair([("2026-03-18", "NEW", 10)], invalid=True)
        code, out, err = self.run_cli()
        self.assertEqual(code, 1, err)
        self.assertFalse(json.loads(out)["complete"])
        self.assertIn("dollar_volume_arithmetic", (self.output / "report.md").read_text())
        self.assertTrue((self.output / "invalid_rows.csv").is_file())

    def test_digest_mismatch_fails_with_structured_results(self):
        self.export_pair([("2026-03-18", "NEW", 10)])
        report = json.loads(self.report.read_text())
        report["output_sha256"] = "0" * 64
        self.report.write_text(json.dumps(report))
        code, out, err = self.run_cli()
        self.assertEqual(code, 1, err)
        self.assertEqual(json.loads(out)["counts"]["FAIL"], 1)

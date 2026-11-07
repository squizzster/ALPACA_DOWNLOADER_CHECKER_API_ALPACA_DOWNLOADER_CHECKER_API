"""CLI integration of declared action coverage and source-view cross-checking."""

import csv
import datetime as dt
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from test_converter import APP, SOURCE_ID, write_manifest


class ActionCLITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.data_in = self.root / "input"
        self.output = self.root / "result.parquet"
        self.events = self.root / "events.csv"
        self.coverage = self.root / "coverage.csv"
        write_manifest(self.data_in / "security")
        self.write_events()
        self.write_coverage()

    def csv_file(self, path, headers, rows):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(rows)

    def write_events(self, *, effective="2024-01-04", old="1", new="3"):
        self.csv_file(
            self.events,
            ["public_id", "event_id", "effective_date", "old_shares", "new_shares", "source"],
            [[SOURCE_ID, "test-split", effective, old, new, "Synthetic continuing-security split record"]],
        )

    def write_coverage(self, *, public_id=SOURCE_ID, start="2024-01-02", end="2024-01-04"):
        self.csv_file(
            self.coverage,
            ["public_id", "start_date", "end_date", "source"],
            [[public_id, start, end, "Synthetic complete action inventory"]],
        )

    def run_cli(self, *extra):
        return subprocess.run(
            [
                sys.executable,
                str(APP),
                "--data-in",
                str(self.data_in),
                "--blood-out",
                str(self.output),
                "--split-source",
                "actions",
                "--split-events",
                str(self.events),
                "--action-coverage",
                str(self.coverage),
                "--horizon",
                "2024-01-04",
                "--quiet",
                *extra,
            ],
            text=True,
            capture_output=True,
            timeout=90,
        )

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)

    def assert_failure(self, result):
        self.assertNotEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)
        self.assertFalse(self.output.exists())

    def test_actions_preserve_prices_and_effective_session_with_declared_provenance(self):
        self.assert_success(self.run_cli())
        table = pq.read_table(self.output)
        rows = table.to_pylist()
        self.assertEqual([row["split_adjustment_ratio"] for row in rows], [1 / 3, 1 / 3, 1])
        self.assertEqual(rows[-1]["date"], dt.datetime(2024, 1, 4))
        self.assertEqual(rows[0]["adjusted_open"], 33.33)
        self.assertEqual(rows[0]["adjusted_close"], 33.67)
        self.assertEqual(rows[0]["raw_close"], 101.0)
        self.assertEqual(rows[0]["volume"], 3000000.25)
        self.assertEqual(rows[0]["dollar_volume"], 303000025.25)
        self.assertEqual(
            table.schema.metadata[b"split_factor_evidence"], b"action_records_with_declared_coverage"
        )
        self.assertEqual(table.schema.metadata[b"split_horizon"], b"2024-01-04")
        report = json.loads(Path(str(self.output) + ".report.json").read_text())
        self.assertFalse(report["corporate_actions_independently_certified"])
        details = report["histories"][0]["split_factor_details"]
        self.assertEqual(details["method"], "action_records_with_declared_coverage")
        self.assertEqual(details["coverage"]["source"], "Synthetic complete action inventory")
        self.assertIn("view_consistency", details)

    def test_wrong_action_ratio_is_rejected_against_source_views(self):
        self.write_events(new="4")
        self.assert_failure(self.run_cli())
        report = json.loads(Path(str(self.output) + ".report.json").read_text())
        self.assertEqual(report["histories_rejected"], 1)
        self.assertEqual(report["status"], "failed")

    def test_wrong_effective_day_is_rejected(self):
        self.write_events(effective="2024-01-03")
        self.assert_failure(self.run_cli())

    def test_missing_series_coverage_is_rejected(self):
        self.write_coverage(public_id="another-security")
        self.assert_failure(self.run_cli())
        report = json.loads(Path(str(self.output) + ".report.json").read_text())
        self.assertIn("no declared coverage interval", report["omitted_histories"][0]["error"])

    def test_date_filter_does_not_relax_full_source_coverage_requirement(self):
        self.write_coverage(start="2024-01-03")
        self.assert_failure(self.run_cli("--start-date", "2024-01-03", "--end-date", "2024-01-03"))

    def test_actions_and_date_filter_use_later_split_event(self):
        self.assert_success(self.run_cli("--start-date", "2024-01-02", "--end-date", "2024-01-02"))
        rows = pq.read_table(self.output).to_pylist()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], dt.datetime(2024, 1, 2))
        self.assertEqual(rows[0]["split_adjustment_ratio"], 1 / 3)

    def test_explicit_split_price_basis_uses_raw_ohlc_and_action_factor(self):
        self.assert_success(self.run_cli("--price-basis", "split"))
        first = pq.read_table(self.output).to_pylist()[0]
        self.assertEqual(first["adjusted_open"], 100.0 * (1 / 3))
        self.assertEqual(first["adjusted_close"], 101.0 * (1 / 3))
        self.assertEqual(first["volume"], 3000000.25)
        self.assertEqual(first["dollar_volume"], 303000025.25)


if __name__ == "__main__":
    unittest.main()

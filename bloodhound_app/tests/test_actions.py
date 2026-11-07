"""Action factors: event boundaries, exact products, coverage, and rejection."""

import csv
import tempfile
import unittest
from pathlib import Path

import numpy as np
from bloodhound_actions import ActionEvidenceError, load_action_evidence


class ActionEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.events = self.root / "events.csv"
        self.coverage = self.root / "coverage.csv"
        self.write_coverage(
            [["ABC", "2016-01-01", "2026-10-14", "Provider event export, explicitly checked complete"]]
        )

    def write_csv(self, path, header, rows):
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(rows)

    def write_events(self, rows, extra=()):
        self.write_csv(
            self.events, ["public_id", "event_id", "effective_date", "old_shares", "new_shares", *extra], rows
        )

    def write_coverage(self, rows):
        self.write_csv(self.coverage, ["public_id", "start_date", "end_date", "source"], rows)

    def load(self, horizon="2026-10-14"):
        return load_action_evidence(self.events, self.coverage, horizon)

    def test_forward_splits_exclude_effective_session(self):
        self.write_events(
            [
                ["ABC", "first", "2021-07-20", "1", "4"],
                ["ABC", "second", "2024-06-10", "1", "10"],
            ]
        )
        factors, diagnostics = self.load().factors_for(
            "ABC", np.array(["2016-01-04", "2021-07-20", "2024-06-10", "2026-10-14"])
        )
        np.testing.assert_array_equal(factors, [0.025, 0.1, 1, 1])
        self.assertEqual(factors.dtype, np.dtype("float64"))
        self.assertEqual(diagnostics["method"], "action_records_with_declared_coverage")
        self.assertEqual(diagnostics["events_applied"], 2)

    def test_reverse_split_and_unsorted_session_input(self):
        self.write_events([["ABC", "reverse", "2021-08-02", "8", "1"]])
        factors, _ = self.load().factors_for("ABC", np.array(["2021-08-02", "2016-01-04", "2021-07-30"]))
        np.testing.assert_array_equal(factors, [1, 8, 8])

    def test_exact_rational_cancellation(self):
        self.write_events(
            [
                ["ABC", "one", "2019-01-02", "0.1", "0.3"],
                ["ABC", "two", "2020-01-02", "3", "1"],
            ]
        )
        factors, _ = self.load().factors_for("ABC", np.array(["2016-01-04", "2020-01-02"]))
        np.testing.assert_array_equal(factors, [1, 1])

    def test_equivalent_duplicates_count_once(self):
        self.write_events(
            [
                ["ABC", "one", "2020-01-02", "1", "4"],
                ["ABC", "one", "2020-01-02", "2", "8"],
                ["ABC", "alias", "2020-01-02", "3", "12"],
            ]
        )
        factors, diagnostics = self.load().factors_for("ABC", np.array(["2016-01-04"]))
        np.testing.assert_array_equal(factors, [0.25])
        self.assertEqual(diagnostics["input_event_records"], 3)
        self.assertEqual(diagnostics["unique_events"], 1)
        self.assertEqual(diagnostics["deduplicated_event_records"], 2)

    def test_different_events_on_same_date_multiply(self):
        self.write_events(
            [
                ["ABC", "one", "2020-01-02", "1", "4"],
                ["ABC", "two", "2020-01-02", "1", "2"],
            ]
        )
        factors, diagnostics = self.load().factors_for("ABC", np.array(["2016-01-04", "2020-01-02"]))
        np.testing.assert_array_equal(factors, [0.125, 1])
        self.assertEqual(diagnostics["same_date_multiple_distinct_events"], ["2020-01-02"])

    def test_conflicting_reused_id_is_rejected(self):
        for changed in (["ABC", "same", "2020-01-02", "1", "2"], ["ABC", "same", "2020-01-03", "1", "4"]):
            with self.subTest(changed=changed):
                self.write_events([["ABC", "same", "2020-01-02", "1", "4"], changed])
                with self.assertRaisesRegex(ActionEvidenceError, "conflicting records"):
                    self.load()

    def test_empty_events_need_explicit_full_coverage(self):
        self.write_events([])
        factors, diagnostics = self.load().factors_for("ABC", np.array(["2016-01-04", "2026-10-14"]))
        np.testing.assert_array_equal(factors, [1, 1])
        self.assertEqual(diagnostics["unique_events"], 0)
        self.assertTrue(diagnostics["coverage_is_declared_not_independently_certified"])
        with self.assertRaisesRegex(ActionEvidenceError, "missing events do not establish factor 1"):
            self.load().factors_for("MISSING", np.array(["2016-01-04"]))

    def test_coverage_must_include_first_session_and_horizon(self):
        self.write_events([])
        for coverage in (
            ["ABC", "2017-01-01", "2026-10-14", "Provider"],
            ["ABC", "2016-01-01", "2025-01-01", "Provider"],
        ):
            with self.subTest(coverage=coverage):
                self.write_coverage([coverage])
                with self.assertRaisesRegex(ActionEvidenceError, "no declared coverage interval"):
                    self.load().factors_for("ABC", np.array(["2016-01-04"]))

    def test_partial_coverage_intervals_are_not_stitched(self):
        self.write_events([])
        self.write_coverage(
            [
                ["ABC", "2016-01-01", "2020-01-01", "Provider A"],
                ["ABC", "2020-01-01", "2026-10-14", "Provider B"],
            ]
        )
        with self.assertRaisesRegex(ActionEvidenceError, "no declared coverage interval"):
            self.load().factors_for("ABC", np.array(["2016-01-04"]))

    def test_horizon_must_follow_last_source_session(self):
        self.write_events([])
        with self.assertRaisesRegex(ActionEvidenceError, "precedes last source session"):
            self.load("2020-01-01").factors_for("ABC", np.array(["2021-01-04"]))

    def test_horizon_filters_future_events_and_includes_horizon_event(self):
        self.write_events(
            [
                ["ABC", "horizon", "2020-01-02", "1", "4"],
                ["ABC", "future", "2021-01-04", "1", "10"],
            ]
        )
        factors, diagnostics = self.load("2020-01-02").factors_for("ABC", np.array(["2016-01-04", "2020-01-02"]))
        np.testing.assert_array_equal(factors, [0.25, 1])
        self.assertEqual(diagnostics["events_after_horizon"], 1)

    def test_unsupported_and_empty_event_types_rejected(self):
        for event_type in ("cash_dividend", "spin_off", "merger", ""):
            with self.subTest(event_type=event_type):
                self.write_events([["ABC", "one", "2020-01-02", "1", "4", event_type]], extra=["event_type"])
                with self.assertRaisesRegex(ActionEvidenceError, "unsupported event_type"):
                    self.load()

    def test_same_security_stock_dividend_supported(self):
        self.write_events([["ABC", "one", "2020-01-02", "1", "1.05", "stock_dividend"]], extra=["event_type"])
        factors, _ = self.load().factors_for("ABC", np.array(["2016-01-04"]))
        self.assertEqual(factors[0], 20 / 21)

    def test_invalid_dates_and_share_quantities_rejected(self):
        for effective in ("2020-1-02", "20200102", "2020-02-30", "2020-01-02T00:00:00"):
            with self.subTest(effective=effective):
                self.write_events([["ABC", "one", effective, "1", "4"]])
                with self.assertRaises(ActionEvidenceError):
                    self.load()
        for invalid in ("0", "-1", "NaN", "Infinity", "1/0"):
            with self.subTest(invalid=invalid):
                self.write_events([["ABC", "one", "2020-01-02", invalid, "4"]])
                with self.assertRaises(ActionEvidenceError):
                    self.load()

    def test_missing_source_and_invalid_coverage_rejected(self):
        self.write_events([])
        for coverage in (
            ["ABC", "2016-01-01", "2026-10-14", ""],
            ["ABC", "2026-10-14", "2016-01-01", "Provider"],
        ):
            with self.subTest(coverage=coverage):
                self.write_coverage([coverage])
                with self.assertRaises(ActionEvidenceError):
                    self.load()

    def test_float_underflow_and_overflow_rejected(self):
        for old, new in (("1e400", "1"), ("1", "1e400")):
            with self.subTest(old=old, new=new):
                self.write_events([["ABC", "one", "2020-01-02", old, new]])
                with self.assertRaises(ActionEvidenceError):
                    self.load().factors_for("ABC", np.array(["2016-01-04"]))

    def test_header_and_invalid_session_checks(self):
        self.events.write_text("", encoding="utf-8")
        with self.assertRaisesRegex(ActionEvidenceError, "CSV header"):
            self.load()
        self.events.write_text(
            "public_id,event_id,effective_date,old_shares,new_shares,new_shares\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(ActionEvidenceError, "duplicate CSV column"):
            self.load()
        self.write_events([])
        for sessions in (np.array([]), np.array([["2020-01-01"]]), np.array(["2020-1-01"])):
            with self.subTest(sessions=sessions):
                with self.assertRaises(ActionEvidenceError):
                    self.load().factors_for("ABC", sessions)


if __name__ == "__main__":
    unittest.main()

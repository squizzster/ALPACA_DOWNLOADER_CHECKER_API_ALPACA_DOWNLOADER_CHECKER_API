import copy
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import test_alpaca_end_of_day_security_correlation as correlation_fixtures
from test_alpaca_end_of_day_security_correlation import asset, correlation_source

from libs.alpaca_end_of_day_security_correlation import row_correlation
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_end_of_day_security_correlation.evidence import build_ticker_ownership_intervals
from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache, correlate_download_rows
from libs.alpaca_lineage_build import transform
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import ADJUSTMENTS


class CorrelationCacheTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"t": day + "T12:00:00Z"} for day in ("2026-01-02", "2026-01-05", "2026-01-06")]
        self.lineage = {
            "identities": [asset("first-id", "EXAMPLE", "Example")],
            "corporate_actions": [],
            "symbol_occupancy_evidence": [],
            "asset_lookups": [
                {"lookup_kind": "symbol", "lookup": "EXAMPLE", "asset": asset("first-id", "EXAMPLE", "Example")}
            ],
        }
        self.source = correlation_source("EXAMPLE", asof="2026-10-15")

    def correlate(self, rows, source, cache=None):
        intervals, _ = build_ticker_ownership_intervals(self.lineage)
        return correlate_download_rows(rows, source, self.lineage, intervals, cache=cache)

    def test_reuses_decisions_without_reusing_view_rows_or_provenance(self):
        cache = CorrelationCache()
        with patch.object(row_correlation, "_decide_row", wraps=row_correlation._decide_row) as decide:
            raw = self.correlate(self.rows, self.source, cache)
            changed_rows = [{**row, "c": "123.45"} for row in self.rows[1:]]
            source = {**self.source, "adjustment": "all", "data_file": "all.csv.gz", "download_sha256": "b" * 64}
            adjusted = self.correlate(changed_rows, source, cache)
            self.assertEqual(len(self.rows), decide.call_count)
        self.assertEqual(self.correlate(changed_rows, source), adjusted)
        self.assertEqual(3, raw["correlated"][0]["row_count"])
        self.assertEqual(2, adjusted["correlated"][0]["row_count"])
        self.assertEqual("all.csv.gz", adjusted["correlated"][0]["data_file"])
        self.assertEqual("b" * 64, adjusted["correlated"][0]["download_sha256"])

    def test_changed_identity_evidence_cannot_reuse_prior_decisions(self):
        cache = CorrelationCache()
        before = self.correlate(self.rows, self.source, cache)
        self.assertTrue(before["correlated"])
        self.lineage["identities"].append(asset("second-id", "EXAMPLE", "Different identity"))
        self.lineage["asset_lookups"].append(
            {"lookup_kind": "symbol", "lookup": "EXAMPLE", "asset": self.lineage["identities"][-1]}
        )
        after = self.correlate(self.rows, self.source, cache)
        self.assertEqual(self.correlate(self.rows, self.source), after)
        self.assertFalse(after["correlated"])
        self.assertEqual(3, after["uncorrelated"][0]["row_count"])

    def test_dates_reuse_parsing_but_each_grid_is_validated(self):
        cache = CorrelationCache()
        with patch.object(
            row_correlation, "_parse_session_date", wraps=row_correlation._parse_session_date
        ) as parse:
            self.assertEqual(
                (date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6)), cache.session_dates(self.rows)
            )
            self.assertEqual(cache.session_dates(self.rows), cache.session_dates(copy.deepcopy(self.rows)))
            self.assertEqual(3, parse.call_count)
        for rows in (self.rows[::-1], [self.rows[0], self.rows[0]], [{"t": []}], [{"t": "2026-01-02"}]):
            with self.subTest(rows=rows), self.assertRaises(SecurityCorrelationError):
                cache.session_dates(rows)

    def test_changed_asof_cannot_reuse_another_selected_identity(self):
        self.lineage = correlation_fixtures.RowCorrelationTests._overlapping_asof_lineage()
        rows = correlation_fixtures.complete_market_session_rows("2024-02-26", "2024-03-04")
        cache = CorrelationCache()
        for asof, expected_id in (("2026-10-15", "selected-id"), ("2024-02-29", "other-id")):
            with self.subTest(asof=asof):
                source = correlation_source("CURRENT", asof=asof)
                result = self.correlate(rows, source, cache)
                self.assertEqual(self.correlate(rows, source), result)
                self.assertEqual(expected_id, result["correlated"][0]["canonical_security"]["alpaca_asset_id"])

    def test_new_cache_reads_new_generation_of_the_same_rows(self):
        cache = CorrelationCache()
        cache.session_dates(self.rows)
        self.rows[0]["t"] = "2025-12-31T12:00:00Z"
        self.assertEqual(date(2025, 12, 31), CorrelationCache().session_dates(self.rows)[0])


class PublicationIndexTests(unittest.TestCase):
    def setUp(self):
        self.timestamps = (
            "2025-12-30T05:00:00Z",
            "2026-01-01T01:00:00Z",  # Still December in New York.
            "2026-01-02T05:00:00Z",
            "2026-02-27T05:00:00Z",
            "2026-03-02T05:00:00Z",
            "2026-03-09T04:00:00Z",  # Daylight saving time.
            "2026-04-01T03:30:00Z",  # Still March in New York.
        )
        self.view = SimpleNamespace(symbol="EXAMPLE", views={})
        self.report = {"correlated": {"segments": []}, "uncorrelated": {"segments": []}}
        for adjustment in ADJUSTMENTS:
            timestamps = self.timestamps if adjustment != "all" else self.timestamps[1:]
            self.view.views[adjustment] = [(stamp,) for stamp in timestamps]
            for branch, start, end in (
                ("correlated", 1, len(timestamps) - 1),
                ("uncorrelated", len(timestamps), len(timestamps)),
            ):
                self.report[branch]["segments"].append(
                    {
                        "adjustment": adjustment,
                        "row_start": start,
                        "row_end": end,
                        "historical_symbol": "EXAMPLE",
                    }
                )

    def test_month_index_preserves_new_york_boundaries_offsets_and_branches(self):
        original = copy.deepcopy(self.report)
        with patch.object(transform, "timestamp", wraps=transform.timestamp) as parse:
            indexes = transform._row_indexes(self.view)
            report = transform._split_months(self.report, indexes)
            documents = {
                kind: transform.boundary(self.view, report, kind, "2026-03", indexes)
                for kind in ("completed_years", "current_year", "current_month")
            }
            self.assertEqual(len(self.timestamps), parse.call_count)
        self.assertEqual(original, self.report)
        for kind, expected_rows in (("completed_years", 2), ("current_year", 5), ("current_month", 3)):
            for entry in documents[kind]["adjustments"]:
                expected = expected_rows - (kind == "completed_years" and entry["adjustment"] == "all")
                self.assertEqual(expected, entry["rows"])
                self.assertEqual(
                    list(range(1, expected + 1)),
                    [
                        row
                        for segment in entry["segments"]
                        for row in range(segment["row_start"], segment["row_end"] + 1)
                    ],
                )
                self.assertTrue(all(segment["from"][:7] == segment["to"][:7] for segment in entry["segments"]))
        month = documents["current_month"]["adjustments"][0]["segments"]
        self.assertEqual(
            [("correlated", 2), ("uncorrelated", 1)], [(s["branch"], s["row_count"]) for s in month]
        )
        self.assertEqual("2026-03-31", month[-1]["to"])

    def test_empty_view_and_missing_assignment(self):
        self.view.views["all"] = []
        for branch in self.report.values():
            branch["segments"] = [s for s in branch["segments"] if s["adjustment"] != "all"]
        indexes = transform._row_indexes(self.view)
        report = transform._split_months(self.report, indexes)
        result = transform.boundary(self.view, report, "current_month", "2026-03", indexes)
        empty = next(entry for entry in result["adjustments"] if entry["adjustment"] == "all")
        self.assertEqual({"adjustment": "all", "rows": 0, "segments": []}, empty)
        report["uncorrelated"]["segments"] = []
        with self.assertRaisesRegex(LineageError, "does not exhaust"):
            transform.boundary(self.view, report, "current_month", "2026-03", indexes)

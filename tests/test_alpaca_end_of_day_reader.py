"""Shared history source-selection rules used by the standalone reader."""

import unittest

from libs.alpaca_end_of_day_reader import EndOfDayReaderError
from libs.alpaca_end_of_day_reader.history import BAR_FIELD_NAMES, _resolve_bar_candidates


class EndOfDayReaderTests(unittest.TestCase):
    def test_conflicting_raw_bar_prefers_sole_historical_ticker_source(self) -> None:
        timestamp = "2024-01-02T05:00:00Z"

        def candidate(source_id: str, symbol: str, historical_symbol: str, close: str) -> dict:
            values = {field_name: "1" for field_name in BAR_FIELD_NAMES}
            values["timestamp"] = timestamp
            values["close"] = close
            return {
                "values": values,
                "provenance": {
                    "source_id": source_id,
                    "symbol": symbol,
                    "historical_symbol": historical_symbol,
                },
                "integrity_error_ids": [],
            }

        bars, selection = _resolve_bar_candidates(
            {
                timestamp: [
                    candidate("OLD/raw.csv.gz", "OLD", "OLD", "10"),
                    candidate("NEW/raw.csv.gz", "NEW", "OLD", "20"),
                ]
            },
            {
                "OLD/raw.csv.gz": frozenset((timestamp,)),
                "NEW/raw.csv.gz": frozenset((timestamp,)),
            },
            allow_strict_superset_selection=False,
        )

        self.assertEqual("10", bars[0]["close"])
        self.assertEqual("historical_ticker", selection["policy"])
        self.assertEqual(
            "requested_symbol_matches_historical_ticker",
            selection["resolutions"][0]["selection_rule"],
        )

    def test_conflicting_bar_rejects_multiple_historical_ticker_sources(self) -> None:
        timestamp = "2024-01-02T05:00:00Z"
        candidates = []
        for source_id, symbol, close in (("OLD/raw.csv.gz", "OLD", "10"), ("NEW/raw.csv.gz", "NEW", "20")):
            values = {field_name: "1" for field_name in BAR_FIELD_NAMES}
            values["timestamp"] = timestamp
            values["close"] = close
            candidates.append(
                {
                    "values": values,
                    "provenance": {
                        "source_id": source_id,
                        "symbol": symbol,
                        "historical_symbol": symbol,
                    },
                    "integrity_error_ids": [],
                }
            )

        with self.assertRaisesRegex(EndOfDayReaderError, "no source was preferred"):
            _resolve_bar_candidates(
                {timestamp: candidates},
                {candidate["provenance"]["source_id"]: frozenset((timestamp,)) for candidate in candidates},
                allow_strict_superset_selection=False,
            )

    def test_conflicting_bar_rejects_without_a_historical_ticker_source(self) -> None:
        timestamp = "2024-01-02T05:00:00Z"
        candidates = []
        for source_id, symbol, close in (("OLD/raw.csv.gz", "OLD", "10"), ("NEW/raw.csv.gz", "NEW", "20")):
            values = {field_name: "1" for field_name in BAR_FIELD_NAMES}
            values["timestamp"] = timestamp
            values["close"] = close
            candidates.append(
                {
                    "values": values,
                    "provenance": {
                        "source_id": source_id,
                        "symbol": symbol,
                        "historical_symbol": "OTHER",
                    },
                    "integrity_error_ids": [],
                }
            )

        with self.assertRaisesRegex(EndOfDayReaderError, "no source was preferred"):
            _resolve_bar_candidates(
                {timestamp: candidates},
                {candidate["provenance"]["source_id"]: frozenset((timestamp,)) for candidate in candidates},
                allow_strict_superset_selection=False,
            )

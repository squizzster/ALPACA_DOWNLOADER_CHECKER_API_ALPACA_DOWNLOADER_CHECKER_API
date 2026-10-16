import copy
import unittest
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation.predecessor_prefix import (
    INDEPENDENT_HISTORY_WARNING,
    assess_independent_donor_prefixes,
)
from libs.alpaca_end_of_day_security_correlation.row_correlation import (
    correlate_download_rows,
    download_session_dates,
)
from tests import test_alpaca_predecessor_prefix as prefix_fixtures


class IndependentDonorPrefixTests(unittest.TestCase):
    def setUp(self):
        self.fixture = prefix_fixtures.PredecessorPrefixTests()
        self.fixture.setUp()
        for download, _, _ in self.fixture.work:
            download.rows[:0] = [
                {**download.rows[0], "t": day + "T04:00:00Z", "c": "100"} for day in ("2024-08-12", "2024-08-13")
            ]
        self.fixture.work = [
            (download, source, download_session_dates(download.rows))
            for download, source, _ in self.fixture.work
        ]

    def assess(self):
        return assess_independent_donor_prefixes(
            "OLD",
            self.fixture.lineage,
            self.fixture.intervals,
            self.fixture.edges,
            self.fixture.work,
            correlation_loader=lambda symbol: SimpleNamespace(
                correlated={"segments": self.fixture.segments if symbol == "NEXT" else []},
                sources_by_filename=self.fixture.descriptors,
            ),
            source_rows_loader=lambda symbol, filename: (self.fixture.donor_rows[filename], "generation"),
        )

    def test_complete_independently_owned_donor_proves_only_bounded_middle(self):
        proof = self.assess()
        self.assertEqual(1, len(proof))
        result = correlate_download_rows(
            self.fixture.work[0][0].rows,
            self.fixture.work[0][1],
            self.fixture.lineage,
            self.fixture.intervals,
            independent_history=proof,
        )
        self.assertEqual(2, result["correlated"][0]["row_count"])
        self.assertEqual(3, sum(item["row_count"] for item in result["uncorrelated"]))
        self.assertIn(INDEPENDENT_HISTORY_WARNING, result["correlated"][0]["warnings"])

    def leading_pad(self):
        for filename, donor in self.fixture.donor_rows.items():
            pad = {
                **donor[0],
                "t": "2024-08-13T04:00:00Z",
                "o": "99",
                "h": "99",
                "l": "99",
                "c": "99",
                "v": "0",
                "n": "0",
                "vw": "0",
            }
            donor.insert(0, pad)
            for download, source, _ in self.fixture.work:
                if source["data_file"] == filename:
                    download.rows[1] = copy.deepcopy(pad)
        for segment in self.fixture.segments:
            segment["row_end"] = 3

    def test_complete_matching_leading_pad_is_excluded_without_date_hardcoding(self):
        self.leading_pad()
        proof = self.assess()
        self.assertEqual(1, len(proof))
        self.assertEqual("2024-08-14", proof[0].first.isoformat())
        self.assertEqual("2024-08-15", proof[0].last.isoformat())

    def test_informative_mismatching_or_unowned_earlier_donor_is_not_trimmed_into_island(self):
        self.leading_pad()
        filename = self.fixture.work[0][1]["data_file"]
        self.fixture.donor_rows[filename][0]["c"] = "98"
        self.assertEqual((), self.assess())
        self.fixture.donor_rows[filename][0]["c"] = "99"
        self.fixture.segments[0]["row_start"] = 2
        self.assertEqual((), self.assess())

    def test_recursive_stale_or_missing_view_donor_does_not_qualify(self):
        self.fixture.segments[0]["warnings"] = [INDEPENDENT_HISTORY_WARNING]
        self.assertEqual((), self.assess())
        self.fixture.segments[0]["warnings"] = []
        self.fixture.descriptors["raw.csv.gz"]["download_sha256"] = "stale"
        self.assertEqual((), self.assess())
        self.fixture.descriptors["raw.csv.gz"]["download_sha256"] = "generation"
        self.fixture.work.pop()
        self.assertEqual((), self.assess())

    def test_positive_competing_dated_owner_vetoes_transfer(self):
        self.fixture.intervals.append(
            replace(
                self.fixture.intervals[0],
                historical_symbol="OLD",
                asset_id="rival",
                start_date_inclusive=date(2024, 8, 14),
                end_date_exclusive=None,
            )
        )
        self.assertEqual((), self.assess())

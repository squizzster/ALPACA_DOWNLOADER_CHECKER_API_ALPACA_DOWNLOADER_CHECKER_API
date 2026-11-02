"""Equality belongs to the publisher, including overridden year/month overlap."""

import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import test_alpaca_lineage_build as fixture

from libs.alpaca_lineage_build import pipeline
from libs.alpaca_pricing_download.changes import equal_effective_replacement
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, build_boundary


class EffectiveDigestTests(unittest.TestCase):
    def partitions(self):
        def rows(*days):
            return tuple((day + "T05:00:00Z", "10", "10", "10", "10", "100", "10", "10") for day in days)

        old = {
            "completed_years": {a: rows("2025-12-31") for a in ADJUSTMENTS},
            "current_year": {a: rows("2026-10-01") for a in ADJUSTMENTS},
            "current_month": {a: rows("2026-10-01", "2026-10-14") for a in ADJUSTMENTS},
        }
        manifests = {kind: build_boundary("TEST", kind, "2026-10", views, {})[0] for kind, views in old.items()}
        new = {**old, "current_year": deepcopy(old["current_month"])}
        return new, manifests, rows

    def test_every_view_and_removed_rows_remain_part_of_equality(self):
        new, manifests, _ = self.partitions()
        prior = {"anchor_month": "2026-10"}
        self.assertTrue(equal_effective_replacement(prior, "2026-10", new, manifests))
        for adjustment in ADJUSTMENTS:
            for kind in new:
                with self.subTest(adjustment=adjustment, kind=kind):
                    changed = deepcopy(new)
                    changed[kind][adjustment] = ()
                    self.assertFalse(equal_effective_replacement(prior, "2026-10", changed, manifests))
        self.assertFalse(equal_effective_replacement(prior, "2027-01", new, manifests))

    def test_new_nonoverridden_row_outside_recorded_year_bounds_is_not_ignored(self):
        new, manifests, rows = self.partitions()
        for adjustment in ADJUSTMENTS:
            new["current_year"][adjustment] = rows("2026-09-30") + new["current_year"][adjustment]
        self.assertFalse(equal_effective_replacement({"anchor_month": "2026-10"}, "2026-10", new, manifests))

    def test_changed_values_in_each_view_and_overridden_overlap_are_not_proved_equal(self):
        new, manifests, _ = self.partitions()
        for adjustment in ADJUSTMENTS:
            for kind in new:
                with self.subTest(adjustment=adjustment, kind=kind):
                    changed = deepcopy(new)
                    row = changed[kind][adjustment][0]
                    changed[kind][adjustment] = ((row[0], "11", *row[2:]), *changed[kind][adjustment][1:])
                    # Even overridden year overlap cannot be ignored without a
                    # separately recorded prefix digest. Ordinary ancestry owns it.
                    self.assertFalse(
                        equal_effective_replacement({"anchor_month": "2026-10"}, "2026-10", changed, manifests)
                    )

    def test_empty_views_require_actual_empty_effective_history(self):
        new = {
            kind: {a: () for a in ADJUSTMENTS} for kind in ("completed_years", "current_year", "current_month")
        }
        manifests = {kind: build_boundary("TEST", kind, "2026-10", views, {})[0] for kind, views in new.items()}
        prior = {"anchor_month": "2026-10"}
        self.assertTrue(equal_effective_replacement(prior, "2026-10", new, manifests))
        _, _, rows = self.partitions()
        new["current_year"]["raw"] = rows("2026-09-30")
        self.assertFalse(equal_effective_replacement(prior, "2026-10", new, manifests))


class PricingEqualityTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.LineageBuildTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.download()
        self.case.run_build()
        self.rows = self.case.bars + [fixture.bar("2026-10-14")]
        self.case.download(end="2026-10-15", bars=self.rows)
        self.case.run_build()

    def test_identical_full_refresh_after_append_needs_no_old_prices_or_correlation(self):
        read = Path.read_bytes

        def no_prices(path):
            if path.suffix == ".gz":
                self.fail(f"equality reopened old prices: {path}")
            return read(path)

        for _ in range(2):
            with patch.object(Path, "read_bytes", no_prices):
                result = self.case.download(end="2026-10-15", bars=self.rows, refresh_all=True)
            self.assertFalse(result["changed"])
            self.assertIsNone(result["pending_update"])
            with patch.object(pipeline, "correlate", side_effect=AssertionError("unchanged lineage replay")):
                self.case.run_build()

    def test_real_correction_and_pending_notice_survive_equal_retry(self):
        corrected = [fixture.bar("2016-01-04", "12"), *self.rows[1:]]
        changed = self.case.download(end="2026-10-15", bars=corrected, refresh_all=True)
        self.assertTrue(changed["changed"])
        equal = self.case.download(end="2026-10-15", bars=corrected, refresh_all=True)
        self.assertFalse(equal["changed"])
        self.assertEqual(equal["pending_update"], changed["pending_update"])
        with patch.object(pipeline, "correlate", wraps=pipeline.correlate) as owner:
            self.case.run_build()
        self.assertEqual(owner.call_count, 1)

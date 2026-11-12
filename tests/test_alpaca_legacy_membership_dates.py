"""Selected whole legacy donors gain dates without replaying numeric approval."""

import copy
import unittest
from unittest.mock import patch

import test_alpaca_public_common_pipeline as common_fixture
from test_alpaca_lineage_build import IdentityClient, informative_bars
from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations
from test_alpaca_public_common_continuation import common_fact

from alpaca_end_of_day import DataLibrary, _standalone_group, standalone
from libs.alpaca_end_of_day_security_correlation import correlation, public_common_continuation
from libs.alpaca_lineage_build import pipeline
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy, storage, work
from libs.alpaca_serving_catalogue.producer import maintain


class LegacyMembershipDateTests(unittest.TestCase):
    def setUp(self):
        common_fixture.PublicCommonPipelineTests.setUp(self)
        if self._testMethodName == "test_cross_year_donor_uses_current_physical_offsets":
            with direct_adjustment_observations():
                download_pricing(
                    self.store,
                    "SPAC",
                    "2024-01-09",
                    from_date="2023-12-01",
                    refresh_all=True,
                    client=FakeClient(informative_bars("2023-12-28", "2024-01-05")),
                )
                download_pricing(
                    self.store,
                    "BUSINESS",
                    "2024-01-09",
                    from_date="2023-12-01",
                    refresh_all=True,
                    client=FakeClient(informative_bars("2024-01-05", "2024-01-09")),
                )
            self.assertEqual(
                0,
                pipeline.run_lineage(
                    self.store, ["SPAC", "BUSINESS"], identity_client=IdentityClient(self.assets)
                )["failed"],
            )
            self.assertEqual(0, maintain(self.store)["failed"])
        controls = storage.controls(self.store)
        groups = storage.read_groups(self.store, controls["groups"], generation=controls["state"]["generation"])
        self.donors = copy.deepcopy(groups)
        for group in groups.values():
            for record in group["history_validation"]["prefix_records"]:
                record.pop("session_dates")
                record.pop("session_projection_version")
        storage.commit(self.store, groups, list(groups.values()))
        result = pipeline.run_lineage(
            self.store,
            ["BUSINESS"],
            identity_client=IdentityClient(self.assets),
            event_supplements=[common_fact()],
        )
        self.assertEqual(0, result["failed"], result)

    def _groups(self):
        controls = storage.controls(self.store)
        return storage.read_groups(self.store, controls["groups"], generation=controls["state"]["generation"])

    def test_selected_raw_dates_complete_legacy_transfer_and_persist(self):
        read = _standalone_group._rows
        reads = []

        def raw_only(root, source, kind, adjustment, *args):
            self.assertEqual("raw", adjustment)
            self.assertIn(source["symbol"], {"SPAC", "BUSINESS"})
            reads.append((source["symbol"], kind, adjustment))
            return read(root, source, kind, adjustment, *args)

        with (
            patch.object(_standalone_group, "_rows", side_effect=raw_only),
            patch.object(standalone, "_rows", side_effect=AssertionError("numerical history reopened")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted OHLC checked")),
            patch.object(
                public_common_continuation, "certify_group", side_effect=AssertionError("lineage recertified")
            ),
            patch.object(correlation, "correlate_download_rows", side_effect=AssertionError("owners replayed")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(0, result["checked_bars"])
        self.assertEqual({("SPAC", "current_month", "raw"), ("BUSINESS", "current_month", "raw")}, set(reads))
        self.assertEqual(2, len(reads))
        self.assertEqual(1, DataLibrary(store=self.store).online_count())
        groups = self._groups()
        self.assertEqual(1, len(groups))
        record = next(iter(groups.values()))["history_validation"]["prefix_records"][0]
        self.assertEqual(1, record["session_projection_version"])
        self.assertEqual(
            ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"], record["session_dates"]
        )
        self.assertEqual([], record["price_reasons"])
        with (
            patch.object(_standalone_group, "_rows", side_effect=AssertionError("completed dates reopened")),
            patch.object(standalone, "_rows", side_effect=AssertionError("completed history reopened")),
        ):
            repeated = maintain(self.store)
        self.assertTrue(repeated["complete"], repeated)
        self.assertEqual(0, repeated["checked_bars"])

    def _rejected_before_assembly(self, change, expected):
        original = storage.read_groups

        def altered(*args, **kwargs):
            groups = original(*args, **kwargs)
            change(groups)
            return groups

        with (
            patch.object(storage, "read_groups", side_effect=altered),
            patch.object(_standalone_group, "_rows", side_effect=AssertionError("rejected donor read")),
            patch.object(standalone, "_rows", side_effect=AssertionError("rejected history read")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("rejected donor validated")),
        ):
            result = maintain(self.store)
        self.assertFalse(result["complete"], result)
        self.assertIn(expected, str(result))
        self.assertTrue(work.read(self.store))

    def test_declared_projection_missing_dates_stays_rejected(self):
        def change(groups):
            for group in groups.values():
                for record in group["history_validation"]["prefix_records"]:
                    record["session_projection_version"] = 1

        self._rejected_before_assembly(change, "date projection")

    def test_changed_donor_generation_stays_rejected(self):
        def change(groups):
            for group in groups.values():
                for source in group["history_validation"]["inputs"].values():
                    source["pricing_generation"] = "f" * 64

        self._rejected_before_assembly(change, "source generation changed")

    def test_changed_donor_subrange_stays_rejected(self):
        def change(groups):
            for group in groups.values():
                for source in group["history_validation"]["inputs"].values():
                    source["views"]["raw"]["ownership"][0]["rows"] -= 1

        self._rejected_before_assembly(change, "changed donor subrange")

    def test_committed_projection_survives_acknowledgement_interruption(self):
        class Interrupted(BaseException):
            pass

        with (
            patch.object(work, "acknowledge", side_effect=Interrupted()),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("numeric replay")),
            self.assertRaises(Interrupted),
        ):
            maintain(self.store)
        records = [
            record
            for group in self._groups().values()
            for record in group["history_validation"]["prefix_records"]
        ]
        self.assertTrue(all(record.get("session_projection_version") == 1 for record in records))
        with (
            patch.object(_standalone_group, "_rows", side_effect=AssertionError("committed dates replayed")),
            patch.object(standalone, "_rows", side_effect=AssertionError("committed history replayed")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("numeric replay")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual({}, work.read(self.store))
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

    def test_disjoint_negative_donor_preserves_unaligned_view_capability(self):
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("negative history replayed")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("negative numeric replay")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(0, result["checked_bars"])
        self.assertEqual(0, DataLibrary(store=self.store).online_count())
        record = next(iter(self._groups().values()))["history_validation"]["prefix_records"][0]
        self.assertIn("incompatible_sessions", record["price_reasons"])
        self.assertEqual(["2024-01-02", "2024-01-04", "2024-01-05", "2024-01-08"], record["session_dates"])

    def test_cross_year_donor_uses_current_physical_offsets(self):
        project = _standalone_group.GroupSnapshot._serving_transferred_session_dates
        seen = []

        def selected(group, runs, offsets):
            seen.extend(
                (run["source"], run["boundary"], offsets[run["source"]]["raw"][run["boundary"]]) for run in runs
            )
            return project(group, runs, offsets)

        with (
            patch.object(_standalone_group.GroupSnapshot, "_serving_transferred_session_dates", new=selected),
            patch.object(standalone, "_rows", side_effect=AssertionError("numeric history read")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("numeric replay")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertIn(("SPAC", "completed_years", 0), seen)
        self.assertIn(("SPAC", "current_month", 2), seen)
        record = next(iter(self._groups().values()))["history_validation"]["prefix_records"][0]
        self.assertEqual(
            ["2023-12-28", "2023-12-29", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"],
            record["session_dates"],
        )
        self.assertEqual(0, result["checked_bars"])
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

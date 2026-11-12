"""Published ranges consume accepted numerical history and bounded metadata."""

from unittest.mock import patch

import test_alpaca_serving_incremental as incremental_fixture
import test_alpaca_serving_suffix as suffix_fixture
from test_alpaca_end_of_day_api import NativeCase, valid_bars

from alpaca_end_of_day import DataLibrary, _standalone_group, standalone
from libs.alpaca_serving_catalogue import policy, storage, work
from libs.alpaca_serving_catalogue.producer import maintain


class ServingProjectionRecoveryTests(NativeCase):
    def legacy_checkpoint(self):
        self.bootstrap()
        previous = self.catalogue_groups("TEST")
        for record in previous["TEST"]["history_validation"]["prefix_records"]:
            record.pop("session_dates")
            record.pop("session_projection_version")
        storage.commit(self.store, previous, list(previous.values()))
        suffix_fixture.ServingSuffixTests.overlap(self)

    def test_historical_metadata_is_bounded_by_selected_history_without_uncertainty(self):
        self.add_symbol("ALIAS", valid_bars("2026-09-29", "2026-10-15"), end="2026-10-15")
        incremental_fixture.IncrementalServingTests.connect_alias(self)

        def dated_metadata(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["canonical_security"]["cusip"] = "123456789"
                        segment["ticker_ownership_interval"] = {
                            "historical_symbol": "TEST",
                            "cusip": "123456789",
                            "start_date_inclusive": None,
                            "end_date_exclusive": None,
                        }

        self.publish_change(dated_metadata, "ALIAS")
        result = maintain(self.store, bootstrap=True)
        self.assertTrue(result["complete"], result)
        (security,) = DataLibrary(store=self.store).list_securities()
        self.assertEqual("2026-10-01", str(security.first_session))
        self.assertEqual("2026-10-13", str(security.last_session))
        record = self.catalogue_groups("ALIAS")["ALIAS"]["securities"][0]
        dated = [o for o in record["observations"] if o["effective_from"] is not None]
        self.assertTrue(dated)
        self.assertTrue(
            all("2026-10-01" <= o["effective_from"] <= o["effective_through"] <= "2026-10-13" for o in dated)
        )

    def test_new_legacy_partial_range_assembles_dates_once_without_numeric_or_calendar_replay(self):
        self.legacy_checkpoint()
        raw_read = _standalone_group._rows
        reads = []

        def selected_raw(root, source, kind, adjustment, *args):
            self.assertEqual((root.name, adjustment), ("TEST", "raw"))
            reads.append(kind)
            return raw_read(root, source, kind, adjustment, *args)

        with (
            patch.object(_standalone_group, "_rows", side_effect=selected_raw),
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted numeric history reopened")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted numeric check replayed")),
            patch.object(
                policy,
                "expected_market_session_coverage",
                side_effect=AssertionError("accepted calendar replayed"),
            ),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(0, result["checked_bars"])
        self.assertEqual(["current_month"], reads)
        (security,) = DataLibrary(store=self.store).list_securities()
        self.assertEqual("2026-10-06", str(security.first_session))
        completed = self.catalogue_groups("ALIAS")["ALIAS"]
        self.assertEqual(1, completed["history_validation"]["prefix_records"][0]["session_projection_version"])
        self.publish_change(lambda receipt, _: receipt["evidence"].update(warnings=["metadata only"]))
        with (
            patch.object(_standalone_group, "_rows", side_effect=AssertionError("accepted dates reconstructed")),
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted history reopened")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted numeric check replayed")),
        ):
            self.assertTrue(maintain(self.store)["complete"])

    def test_declared_projection_missing_dates_does_not_use_legacy_assembly(self):
        self.legacy_checkpoint()
        read = storage.read_groups

        def incomplete(*args, **kwargs):
            selected = read(*args, **kwargs)
            for group in selected.values():
                for record in group["history_validation"]["prefix_records"]:
                    record["session_projection_version"] = 1
            return selected

        with (
            patch.object(storage, "read_groups", side_effect=incomplete),
            patch.object(
                _standalone_group, "_rows", side_effect=AssertionError("missing current projection rebuilt")
            ),
            patch.object(
                standalone, "_rows", side_effect=AssertionError("missing current projection history read")
            ),
        ):
            result = maintain(self.store)
        self.assertFalse(result["complete"])
        self.assertIn("date projection", str(result["failures"]))
        self.assertTrue(work.read(self.store))

    def test_committed_legacy_dates_survive_acknowledgement_interruption(self):
        self.legacy_checkpoint()

        class Interrupted(BaseException):
            pass

        with (
            patch.object(work, "acknowledge", side_effect=Interrupted()),
            patch.object(
                policy, "_bar_reasons", side_effect=AssertionError("accepted numerical check replayed")
            ),
            self.assertRaises(Interrupted),
        ):
            maintain(self.store)
        with (
            patch.object(
                _standalone_group, "_rows", side_effect=AssertionError("committed dates reconstructed")
            ),
            patch.object(standalone, "_rows", side_effect=AssertionError("committed history reopened")),
            patch.object(policy, "evaluate", side_effect=AssertionError("committed evaluation repeated")),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual({}, work.read(self.store))

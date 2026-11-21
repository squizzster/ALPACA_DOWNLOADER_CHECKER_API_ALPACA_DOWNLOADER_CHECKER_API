"""Publish current clean ranges from accepted observations after an older gap."""

from contextlib import closing
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import asset

from alpaca_end_of_day import DataLibrary, standalone
from alpaca_end_of_day._standalone_group import GroupSnapshot
from libs.alpaca_serving_catalogue import gap_suffix, policy, producer, reuse, storage
from libs.alpaca_serving_catalogue import gaps as raw_gaps
from libs.common.sqlite_connections import connect_sqlite


class SupportedGapSuffixTests(NativeCase):
    def setup_gap(self, symbol="RECENT", *, metadata=None):
        day = self.bars[2]["t"][:10]
        later = self.bars[3]["t"][:10]
        self.add_symbol(symbol, self.bars[:2] + self.bars[3:], metadata=metadata)
        return day, later

    def previous_gap_policy(self, group, *args, **kwargs):
        return policy.evaluate(group, *args, **kwargs, supported_gap_suffix=False)

    def test_first_publication_exposes_only_the_current_supported_range(self):
        day, later = self.setup_gap()
        data = self.bootstrap()
        (recent,) = data.resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)
        self.assertEqual(data.get_security(recent.public_id).stored_session_count, len(self.bars) - 3)
        group = self.catalogue_groups("RECENT")["RECENT"]
        decision = group["decisions"][0]
        self.assertEqual(decision["publication_start_after_missing_session"], day)
        self.assertEqual(decision["supported_gap_suffix_version"], 1)
        self.assertEqual(group["history_validation"]["prefix_records"][0]["missing_sessions"], [day])
        selected = storage.select_late_bar_repairs(self.store, storage.controls(self.store))
        self.assertEqual(selected["sources"]["RECENT"]["sessions"], [day])
        data = DataLibrary(store=self.store)
        self.assertFalse(data.resolve_ticker("RECENT", on=day))
        self.assertEqual(
            {view.bars[0].session.isoformat() for view in data.get_histories(recent.public_id).values()}, {later}
        )

    def test_exact_checkpoint_adoption_carries_prior_outcomes_through_a_retry(self):
        day, later = self.setup_gap()
        self.setup_gap("OTHER")
        with patch.object(producer, "evaluate", side_effect=self.previous_gap_policy):
            data = self.bootstrap()
        self.assertFalse(data.resolve_ticker("RECENT"))
        self.assertFalse(data.resolve_ticker("OTHER"))
        old = self.catalogue_groups("RECENT", "OTHER")
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("reopened historical prices")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("rechecked accepted values")),
            patch.object(GroupSnapshot, "serving_histories", side_effect=AssertionError("replayed selection")),
            patch.object(raw_gaps, "complete_presence", side_effect=AssertionError("rechecked gap provenance")),
            patch.object(storage, "_late_bar_projection", side_effect=AssertionError("replayed gap projection")),
            patch.object(
                reuse, "validate_history_validation", side_effect=AssertionError("revalidated completed history")
            ),
            patch.object(producer, "evaluate", side_effect=AssertionError("recomputed old eligibility")),
        ):
            result = producer.adopt_supported_gap_suffixes(self.store, ("RECENT",))
            self.assertEqual(result["published_components"], 1)
            self.assertEqual(result["numerical_bars_checked"], 0)
            with patch.object(storage, "read_groups", side_effect=AssertionError("reopened completed decision")):
                self.assertEqual(
                    producer.adopt_supported_gap_suffixes(self.store, ("RECENT",))["published_components"], 0
                )
        new = self.catalogue_groups("RECENT", "OTHER")
        self.assertEqual(old["OTHER"], new["OTHER"])
        self.assertEqual(old["RECENT"]["references"], new["RECENT"]["references"])
        self.assertEqual(old["RECENT"]["history_validation"], new["RECENT"]["history_validation"])
        self.assertEqual(new["RECENT"]["decisions"][0]["publication_start_after_missing_session"], day)
        (recent,) = DataLibrary(store=self.store).resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)

    def test_changed_lineage_keeps_the_old_checkpoint_outside_recovery(self):
        self.setup_gap()
        with patch.object(producer, "evaluate", side_effect=self.previous_gap_policy):
            self.bootstrap()
        self.publish_change(lambda receipt, _: receipt.update(processed_through="2026-10-15"), "RECENT")
        with patch.object(
            standalone.StandaloneReader, "_open_group", side_effect=AssertionError("stale body used")
        ):
            result = producer.adopt_supported_gap_suffixes(self.store, ("RECENT",))
        self.assertEqual(result["published_components"], 0)
        self.assertEqual(result["skipped"], [{"symbols": ["RECENT"], "reason": "input_generation_changed"}])

    def test_inactive_security_does_not_acquire_a_current_range(self):
        self.setup_gap("RETIRED", metadata={**asset("RETIRED"), "status": "inactive", "tradable": False})
        self.assertFalse(self.bootstrap().resolve_ticker("RETIRED"))
        self.assertEqual(
            self.catalogue_groups("RETIRED")["RETIRED"]["decisions"][0]["reasons"],
            ["unexplained_missing_sessions"],
        )

    def test_unassigned_trailing_sessions_block_the_current_range(self):
        self.setup_gap()
        before, last = self.bars[-2]["t"], self.bars[-1]["t"]

        def unassign(_, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if not view["segments"]:
                        continue
                    original = view["segments"][-1]
                    left = {
                        **original,
                        "row_end": original["row_end"] - 1,
                        "row_count": original["row_count"] - 1,
                        "last_timestamp": before,
                        "to": before[:10],
                    }
                    right = {
                        **original,
                        "row_start": original["row_end"],
                        "row_count": 1,
                        "first_timestamp": last,
                        "from": last[:10],
                        "branch": "uncorrelated",
                        "reason_codes": ["test_unresolved"],
                        "candidate_securities": [{"asset_id": "RECENT-id"}],
                    }
                    right.pop("security_identity", None)
                    view["segments"][-1:] = [left, right]

        self.publish_change(unassign, "RECENT")
        self.assertFalse(self.bootstrap().resolve_ticker("RECENT"))
        self.assertEqual(
            producer.adopt_supported_gap_suffixes(self.store, ("RECENT",))["published_components"], 0
        )

    def test_interrupted_fresh_projection_keeps_original_checkpoint_and_reuses_it(self):
        _, later = self.setup_gap()
        with patch.object(producer, "evaluate", side_effect=self.previous_gap_policy):
            self.bootstrap()
        before = self.catalogue_groups("RECENT")["RECENT"]
        with (
            patch.object(GroupSnapshot, "serving_histories", side_effect=AssertionError("replayed history")),
            patch.object(storage, "_write_state", side_effect=OSError("interrupt before selector")),
            self.assertRaisesRegex(OSError, "interrupt before selector"),
        ):
            producer.adopt_supported_gap_suffixes(self.store, ("RECENT",))
        self.assertEqual(self.catalogue_groups("RECENT")["RECENT"], before)
        self.assertFalse(DataLibrary(store=self.store).resolve_ticker("RECENT"))
        with patch.object(GroupSnapshot, "serving_histories", side_effect=AssertionError("replayed history")):
            self.assertEqual(
                producer.adopt_supported_gap_suffixes(self.store, ("RECENT",))["published_components"], 1
            )
        (recent,) = DataLibrary(store=self.store).resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)

    def test_unmarked_legacy_dates_are_assembled_from_the_completed_calendar_decision(self):
        day, later = self.setup_gap()
        self.setup_gap("DECLARED")
        with patch.object(producer, "evaluate", side_effect=self.previous_gap_policy):
            self.bootstrap()
        old = self.catalogue_groups("RECENT", "DECLARED")
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute(
                "UPDATE groups SET body=json_remove(body, '$.history_validation.prefix_records[0].session_dates') "
                "WHERE group_key IN ('RECENT','DECLARED')"
            )
            db.execute(
                "UPDATE groups SET body=json_remove(body, "
                "'$.history_validation.prefix_records[0].session_projection_version') WHERE group_key='RECENT'"
            )
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("opened retained prices")),
            patch.object(
                GroupSnapshot, "serving_session_dates", side_effect=AssertionError("reopened raw timestamps")
            ),
            patch.object(
                GroupSnapshot, "serving_histories", side_effect=AssertionError("replayed numerical history")
            ),
            patch.object(gap_suffix, "legacy_session_dates", wraps=gap_suffix.legacy_session_dates) as compose,
        ):
            repaired = producer.adopt_supported_gap_suffixes(self.store, ("RECENT", "DECLARED"))
        self.assertEqual(repaired["published_components"], 1)
        self.assertEqual(
            repaired["skipped"], [{"symbols": ["DECLARED"], "reason": "no_accepted_gap_checkpoint"}]
        )
        compose.assert_called_once()
        published = self.catalogue_groups("RECENT", "DECLARED")
        self.assertEqual(published["RECENT"]["history_validation"], old["RECENT"]["history_validation"])
        self.assertEqual(published["RECENT"]["decisions"][0]["publication_start_after_missing_session"], day)
        (recent,) = DataLibrary(store=self.store).resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)

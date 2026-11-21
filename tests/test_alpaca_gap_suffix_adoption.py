"""One-time adoption of a previous gap-only decision through the ordinary serving owner."""

from contextlib import closing
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_serving_catalogue import export_inputs, gap_suffix, policy, producer, storage
from libs.common.sqlite_connections import connect_sqlite


def old_gap_policy(group, *args, **kwargs):
    return policy.evaluate(group, *args, **kwargs, supported_gap_suffix=False)


class LegacyGapSuffixTests(NativeCase):
    def legacy_groups(self):
        day = self.bars[2]["t"][:10]
        later = self.bars[3]["t"][:10]
        sparse = self.bars[:2] + self.bars[3:]
        self.add_symbol("RECENT", sparse)
        self.add_symbol("SAME", sparse)

        def relabel(_, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if not view["segments"]:
                        continue
                    (whole,) = view["segments"]
                    early = {
                        **whole,
                        "row_end": 2,
                        "row_count": 2,
                        "last_timestamp": sparse[1]["t"],
                        "to": sparse[1]["t"][:10],
                        "historical_symbol": "EARLY",
                        "observation_tickers": ["EARLY"],
                    }
                    late = {
                        **whole,
                        "row_start": 3,
                        "row_count": len(sparse) - 2,
                        "first_timestamp": sparse[2]["t"],
                        "from": later,
                    }
                    view["segments"] = [early, late]

        self.publish_change(relabel, "RECENT")
        with patch.object(producer, "evaluate", side_effect=old_gap_policy):
            self.bootstrap()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("UPDATE state SET body=json_remove(body, '$.gap_suffix_reviews')")
            db.execute(
                "UPDATE groups SET body=json_remove(body, '$.history_validation.gap_suffix_version', "
                "'$.history_validation.prefix_records[0].session_dates', "
                "'$.history_validation.prefix_records[0].session_projection_version') WHERE group_key IN ('RECENT','SAME')"
            )
        return day, later

    def policy_eight_groups(self):
        day, later = self.legacy_groups()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute("UPDATE state SET body=json_set(body, '$.policy.version', 8)")
            db.execute(
                "UPDATE groups SET selection=json_remove(json_set(selection, '$.policy.version', 8), '$.late_bar_repairs'), "
                "body=json_remove(json_set(body, '$.policy.version', 8), '$.history_validation.raw_gap_presence')"
            )
        return day, later

    def test_selected_legacy_range_survives_restart_and_its_retry_stays_visible(self):
        day, later = self.legacy_groups()
        before = storage.controls(self.store)
        self.assertEqual(producer.status(self.store, controls=before)["pending_symbols"], ["RECENT"])
        self.assertNotIn("gap_suffix_reviews", storage.selector_state(self.store))
        with patch.object(producer, "evaluate", side_effect=OSError("interrupted before publication")):
            unfinished = producer.maintain(self.store)
        self.assertFalse(unfinished["complete"])
        self.assertEqual(unfinished["pending_symbols"], ["RECENT"])
        carried = storage.controls(self.store)
        (public_id,) = carried["state"]["gap_suffix_reviews"]["RECENT"]
        self.assertEqual(carried["state"]["gap_suffix_reviews"]["RECENT"][public_id], later)
        with (
            patch.object(gap_suffix, "legacy_start", side_effect=AssertionError("legacy decision repeated")),
            patch.object(standalone, "_rows", side_effect=AssertionError("legacy price read")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted price checked")),
        ):
            result = producer.maintain(self.store)
            self.assertTrue(result["complete"], result)
            self.assertEqual((result["published_components"], result["checked_bars"]), (1, 0))
            self.assertEqual(producer.maintain(self.store)["published_components"], 0)
        data = DataLibrary(store=self.store)
        self.assertEqual(data.get_security(public_id).first_session.isoformat(), later)
        self.assertFalse(data.resolve_ticker("EARLY"))
        self.assertFalse(data.resolve_ticker("SAME"))
        after = storage.controls(self.store)
        self.assertEqual(after["state"]["gap_suffix_reviews"], {})
        for key in ("TEST", "SAME"):
            self.assertEqual(before["groups"][key], after["groups"][key])
        selected = storage.select_late_bar_repairs(self.store, after)
        self.assertEqual(
            {s: v["sessions"] for s, v in selected["sources"].items()}, {"RECENT": [day], "SAME": [day]}
        )

    def test_rebuild_restores_legacy_gap_and_export_boundaries_together(self):
        day, later = self.legacy_groups()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute(
                "UPDATE groups SET selection=json_remove(selection, '$.late_bar_repairs'), "
                "body=json_remove(body, '$.history_validation.raw_gap_presence')"
            )
            db.execute("DROP TABLE export_input_state")
            db.execute("DROP TABLE export_input_bindings")
            db.execute("DROP TABLE export_inputs")
        with (
            patch.object(producer, "evaluate", side_effect=old_gap_policy),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("legacy price checked again")),
            patch(
                "libs.alpaca_pricing_download.storage.load_source",
                side_effect=AssertionError("reopened a manifest for a known absence"),
            ),
            patch(
                "alpaca_end_of_day._standalone_group.raw_source_presence",
                side_effect=AssertionError("rechecked a known absence"),
            ),
        ):
            rebuilt = producer.maintain(self.store, rebuild=True)
        self.assertTrue(rebuilt["complete"], rebuilt)
        self.assertTrue(export_inputs.ready(self.store))
        controls = storage.controls(self.store)
        self.assertEqual(set(export_inputs.bound_groups(self.store)), set(controls["groups"]))
        result = storage.select_late_bar_repairs(self.store, controls)
        self.assertEqual(
            {s: v["sessions"] for s, v in result["sources"].items()}, {"RECENT": [day], "SAME": [day]}
        )
        data = DataLibrary(store=self.store)
        (recent,) = data.resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)
        self.assertFalse(data.resolve_ticker("SAME"))

    def test_policy_rebuild_can_apply_an_identical_pre_projection_gap_checkpoint(self):
        day, later = self.policy_eight_groups()
        with (
            patch.object(producer, "evaluate", side_effect=old_gap_policy),
            patch.object(standalone, "_rows", side_effect=AssertionError("historical prices reread")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("historical prices checked")),
            patch(
                "alpaca_end_of_day._standalone_group.raw_source_presence",
                side_effect=AssertionError("accepted all-view absence checked again"),
            ),
        ):
            rebuilt = producer.maintain(self.store, rebuild=True)
            self.assertTrue(rebuilt["complete"], rebuilt)
            self.assertEqual(rebuilt["checked_bars"], 0)
            self.assertEqual(producer.maintain(self.store)["published_components"], 0)
        data = DataLibrary(store=self.store)
        (recent,) = data.resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)
        self.assertFalse(data.resolve_ticker("SAME"))
        selected = storage.select_late_bar_repairs(self.store, storage.controls(self.store))
        self.assertEqual(
            {s: v["sessions"] for s, v in selected["sources"].items()}, {"RECENT": [day], "SAME": [day]}
        )

    def test_policy_rebuild_rejects_a_changed_assignment_binding(self):
        _, later = self.policy_eight_groups()

        def change_earlier_label(_, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    if view["segments"]:
                        view["segments"][0].update(historical_symbol="OTHER", observation_tickers=["OTHER"])

        self.publish_change(change_earlier_label, "RECENT")
        rebuilt = producer.maintain(self.store, rebuild=True)
        self.assertTrue(rebuilt["complete"], rebuilt)
        self.assertEqual(rebuilt["full_history_evaluations"], 1)
        (recent,) = DataLibrary(store=self.store).resolve_ticker("RECENT")
        self.assertEqual(recent.first_session.isoformat(), later)

    def test_lost_selector_reuses_negative_raw_presence(self):
        day, _ = self.legacy_groups()
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            db.execute(
                "UPDATE groups SET selection=json_remove(selection, '$.late_bar_repairs') WHERE group_key='RECENT'"
            )
        with patch(
            "alpaca_end_of_day._standalone_group.raw_source_presence",
            side_effect=AssertionError("rechecked raw"),
        ):
            restored = producer.maintain(self.store, rebuild=True)
        self.assertEqual(restored["projection_source_reads"], 0)
        self.assertEqual(restored["published_components"], 0)
        repairs = storage.select_late_bar_repairs(self.store, storage.controls(self.store))
        self.assertEqual(repairs["sources"]["RECENT"]["sessions"], [day])

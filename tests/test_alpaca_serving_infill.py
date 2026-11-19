import json
from copy import deepcopy
from unittest.mock import Mock, patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary, StandaloneReader, standalone
from alpaca_end_of_day._standalone_group import GroupSnapshot
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_serving_catalogue import gaps, policy, work
from libs.alpaca_serving_catalogue.producer import maintain


class ServingInfillTests(NativeCase):
    public_id = "d" * 64

    def setUp(self):
        super().setUp()
        self.days = [bar["t"][:10] for bar in self.bars]
        self.add_symbol("EARLY", self.bars)
        entries = lineage.read_index(self.root)["symbols"]
        self.sources = {name: entries[name]["source_sha256"] for name in ("EARLY", "TEST")}

    def initial(self, early, recent):
        self.select("EARLY", early)
        self.select("TEST", recent)
        self.bootstrap()

    def select(self, symbol, positions):
        # Exercise the real lineage-publication-to-serving boundary. The fixture
        # emits selected and superseded observations just as the upstream owner does.
        def revise(receipt, boundaries):
            receipt["input_sources"] = self.sources
            receipt["dependencies"] = sorted(self.sources.keys() - {symbol})
            for boundary in boundaries.values():
                for entry in boundary["adjustments"]:
                    selected = (
                        positions.get(entry["adjustment"], positions["raw"])
                        if isinstance(positions, dict)
                        else positions
                    )
                    singles = []
                    for segment in entry["segments"]:
                        for pos in range(segment["row_start"] - 1, segment["row_end"]):
                            instant = self.bars[pos]["t"]
                            label = "EDGE" if pos in (0, len(self.days) - 1) else "MID"
                            if entry["adjustment"] == "all" and pos == 2:
                                label = "ALT"
                            row = deepcopy(segment)
                            row["security_identity"]["security_id"] = self.public_id
                            row.update(
                                row_start=pos + 1,
                                row_end=pos + 1,
                                row_count=1,
                                first_timestamp=instant,
                                last_timestamp=instant,
                                historical_symbol=label,
                                observation_tickers=[label],
                                observation_selection="selected" if pos in selected else "superseded",
                                **{"from": instant[:10], "to": instant[:10]},
                            )
                            singles.append(row)
                    entry["segments"] = singles

        self.publish_change(revise, symbol)

    def record(self):
        return self.catalogue_groups("EARLY")["EARLY"]["history_validation"]["prefix_records"][0]

    def allow_new_only(self, positions):
        allowed = {self.days[pos] for pos in positions}
        check, read = policy._bar_reasons, standalone._rows

        def check_new(bar, view):
            self.assertIn(bar["session"], allowed, "accepted numerical outcome checked again")
            return check(bar, view)

        def read_selected(root, source, *args, **kwargs):
            self.assertEqual(source["symbol"], "TEST", "unchanged source payload reopened")
            return read(root, source, *args, **kwargs)

        return patch.object(policy, "_bar_reasons", side_effect=check_new), patch.object(
            standalone, "_rows", side_effect=read_selected
        )

    def test_partial_then_complete_interior_fill_and_postcommit_resume(self):
        self.initial({0}, {len(self.days) - 1})
        partial = {2, len(self.days) - 2}
        last = len(self.days) - 1
        self.select("TEST", partial | {last})
        checks, reads = self.allow_new_only(partial)
        with checks, reads:
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual((result["full_history_evaluations"], result["checked_bars"]), (0, 8 * len(partial)))
        prior = self.record()
        self.assertEqual(
            prior["missing_sessions"], [d for n, d in enumerate(self.days) if n not in partial | {0, last}]
        )
        self.assertEqual([b["session"] for b in prior["gap_bars"]], [self.days[i] for i in (0, 2, last - 1)])
        self.assertEqual(prior["before_last_raw_bar"]["session"], self.days[last - 1])
        self.assertEqual(prior["source_sessions_in_gaps"], prior["missing_sessions"])
        self.assertFalse(DataLibrary(store=self.store).list_securities())
        self.select("TEST", set(range(1, last + 1)))
        checks, reads = self.allow_new_only(set(range(1, last)) - partial)

        class Interrupted(BaseException):
            pass

        with (
            checks,
            reads,
            patch.object(work, "acknowledge", side_effect=Interrupted),
            self.assertRaises(Interrupted),
        ):
            maintain(self.store)
        committed = self.catalogue_groups("EARLY")["EARLY"]
        with patch.object(policy, "evaluate", side_effect=AssertionError("committed group replayed")):
            self.assertTrue(maintain(self.store)["complete"])
        self.assertEqual(
            self.catalogue_groups("EARLY")["EARLY"]["decision_generation"], committed["decision_generation"]
        )
        record = self.record()
        self.assertEqual(record["missing_sessions"], [])
        self.assertEqual(record["gap_bars"], [])
        self.assertEqual(record["session_dates"], self.days)
        data = DataLibrary(store=self.store)
        self.assertEqual(data.list_securities()[0].stored_session_count, len(self.days))
        self.assertEqual(len(data.get_histories(self.public_id)), 8)
        self.assertEqual(
            [(x["ticker"], x["first_session"], x["last_session"]) for x in record["ticker_ranges"]],
            [
                ("ALT", self.days[2], self.days[2]),
                ("EDGE", self.days[0], self.days[0]),
                ("EDGE", self.days[-1], self.days[-1]),
                ("MID", self.days[1], self.days[-2]),
            ],
        )

    def test_duplicate_of_accepted_session_requires_owning_reconciliation(self):
        self.initial({0}, {len(self.days) - 1})
        self.select("TEST", {0, len(self.days) - 1})
        before = self.catalogue_groups("EARLY")["EARLY"]["decision_generation"]
        checks, reads = self.allow_new_only(set())
        with checks, reads:
            result = maintain(self.store)
        self.assertFalse(result["complete"])
        self.assertIn("overlap", str(result["failures"]))
        self.assertEqual(self.catalogue_groups("EARLY")["EARLY"]["decision_generation"], before)

    def test_new_contributor_prepends_to_a_contiguous_checkpoint(self):
        self.initial({3, 4, 5}, set())
        self.select("TEST", {0, 1, 2})
        checks, reads = self.allow_new_only({0, 1, 2})
        with checks, reads:
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual((result["full_history_evaluations"], result["checked_bars"]), (0, 24))
        self.assertEqual(self.record()["session_dates"], self.days[:6])

    def test_prepend_infill_and_append_share_one_accepted_checkpoint(self):
        self.initial({2}, {5})
        fresh = {0, 1, 3, 4, 6, 7, 8}
        self.select("TEST", fresh | {5})
        checks, reads = self.allow_new_only(fresh)
        with checks, reads:
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual((result["full_history_evaluations"], result["checked_bars"]), (0, 8 * len(fresh)))
        with StandaloneReader(self.store)._open_group("EARLY", include_unresolved=True) as group:
            expected = policy.evaluate(group)
        actual = self.catalogue_groups("EARLY")["EARLY"]
        for key in ("decisions", "securities", "history_validation"):
            self.assertEqual(actual[key], json.loads(json.dumps(expected[key])), key)

    def test_unmarked_gap_projection_keeps_uninterrupted_and_disjoint_label_runs(self):
        # BB's unchanged interval spans the entire history. AA disappears for
        # exactly one accepted session whose date is no label or gap endpoint.
        def bars(labels):
            return [{"session": day, "tickers": list(names)} for day, names in labels]

        held = bars(
            [("02", "B"), ("05", "AB"), ("06", "AB"), ("07", "B"), ("08", "AB"), ("09", "AB"), ("10", "B")]
        )
        fresh = bars([("01", "A"), ("03", "C"), ("04", "B"), ("11", "AB")])
        prior = {
            "first_session": "02",
            "last_session": "10",
            "ticker_ranges": policy._ticker_ranges(held),
            "gap_bars": held[:2],
        }
        self.assertEqual(
            policy._infilled_ranges(prior, fresh),
            policy._ticker_ranges(sorted(held + fresh, key=lambda b: b["session"])),
        )

    def test_component_merge_reuses_presence_on_remaining_unfilled_dates(self):
        last = len(self.days) - 1
        self.initial({0}, {last})
        self.add_symbol("PEER", self.bars)
        self.sources["PEER"] = lineage.read_index(self.root)["symbols"]["PEER"]["source_sha256"]
        self.select("TEST", {2, last})
        self.select("EARLY", {0})
        self.select("PEER", set())
        checks, reads = self.allow_new_only({2})
        with (
            checks,
            reads,
            patch.object(
                GroupSnapshot,
                "_selected_gap_presence",
                side_effect=AssertionError("known all-view presence looked up"),
            ),
            patch.object(
                GroupSnapshot,
                "source_raw_presence",
                side_effect=AssertionError("known source-specific presence looked up"),
            ),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        record = self.record()
        self.assertEqual(record["source_sessions_in_gaps"], record["missing_sessions"])
        self.assertNotIn(self.days[2], record["missing_sessions"])
        self.assertTrue(record["missing_sessions"])

    def test_adjusted_only_infill_keeps_actual_incompatibility_at_its_date(self):
        last = len(self.days) - 1
        self.initial({0}, {last})
        self.select("TEST", {"raw": {last}, "all": {2, last}})
        checks, reads = self.allow_new_only({2})
        with checks, reads:
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        record = self.record()
        self.assertIn("incompatible_sessions", record["before_last_price_reasons"])
        self.assertIn(self.days[2], record["missing_sessions"])
        self.assertEqual(record["session_dates"], [self.days[0], self.days[last]])

    def test_peer_revision_reopens_only_gaps_at_or_after_its_changed_date(self):
        earlier, changed = self.days[2:4]
        days = {earlier, changed}
        old = {"A": {"pricing_generation": "before"}, "B": {"pricing_generation": "stable"}}
        current = {**old, "A": {"pricing_generation": "changed"}}
        checkpoint = {
            "inputs": old,
            "prefix_records": [{"missing_sessions": sorted(days), "source_sessions_in_gaps": []}],
        }
        snapshot = Mock()
        snapshot.retained_pricing_insertions.return_value = None
        snapshot.preserves_pricing_prefix.side_effect = lambda source, old, through: (
            source == "A" and through < changed
        )

        def selected(queries, raw):
            self.assertEqual(queries, {"A": {changed}})
            self.assertEqual(raw, {"A": {earlier: False}, "B": dict.fromkeys(days, False)})
            return {changed}

        snapshot._selected_gap_presence.side_effect = selected
        self.assertEqual(
            gaps.refresh_presence(snapshot, [{"history_validation": checkpoint}], current, days, {}), {changed}
        )

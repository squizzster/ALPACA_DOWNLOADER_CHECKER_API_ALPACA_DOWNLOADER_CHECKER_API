"""Policy migration preserves completed facts only for identical histories."""

import json
import unittest
from contextlib import closing
from copy import deepcopy
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary, StandaloneReader, standalone
from libs.alpaca_lineage_build import storage as lineage
from libs.alpaca_serving_catalogue import policy, reuse, storage, work
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.sqlite_connections import connect_sqlite


def legacy_group(group):
    """The genuine policy-5 difference: scalar labels in ownership runs."""
    result = deepcopy(group)
    result["policy"]["version"] = 5
    for source in result["history_validation"]["inputs"].values():
        for view in source["views"].values():
            for run in view["ownership"]:
                if run["owner"][0] is not None:
                    (run["owner"][1],) = run["owner"][1]
    return result


class ServingMigrationTests(NativeCase):
    def previous(self):
        self.bootstrap()
        return legacy_group(self.catalogue_groups("TEST")["TEST"])

    def assert_same_decision(self, actual, expected):
        for key in ("securities", "decisions", "history_validation", "references", "policy"):
            self.assertEqual(actual[key], expected[key], key)

    def evaluate(self, previous):
        with StandaloneReader(self.store)._open_group("TEST", include_unresolved=True) as group:
            return policy.evaluate(group, (previous,))

    def test_identical_history_reuses_all_views_without_payload_reads_or_numeric_checks(self):
        previous = self.previous()
        frozen = deepcopy(previous)
        expected = self.evaluate({**previous, "policy": {"version": -1}})
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("completed history reopened")),
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("completed numeric check repeated")),
        ):
            result = self.evaluate(previous)
        self.assert_same_decision(result, expected)
        self.assertEqual(result["validation_work"]["reused_history_prefixes"], 1)
        self.assertEqual(result["validation_work"]["checked_bars"], 0)
        self.assertEqual(previous, frozen, "accepted prior checkpoint must not be mutated")

    def test_current_metadata_is_projected_while_identical_history_is_reused(self):
        previous = self.previous()

        def rename(receipt, boundaries):
            for lookup in receipt["evidence"]["asset_lookups"]:
                if lookup.get("asset"):
                    lookup["asset"]["name"] = "Updated company name"

        self.publish_change(rename)
        expected = self.evaluate({**previous, "policy": {"version": -1}})
        with patch.object(standalone, "_rows", side_effect=AssertionError("metadata reopened history")):
            result = self.evaluate(previous)
        self.assert_same_decision(result, expected)
        self.assertEqual(result["securities"][0]["names"], ["Updated company name"])

    def test_changed_published_labels_require_full_evaluation(self):
        previous = self.previous()

        def relabel(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["observation_tickers"] = ["OLD", "TEST"]

        self.publish_change(relabel)
        expected = self.evaluate({**previous, "policy": {"version": -1}})
        result = self.evaluate(previous)
        self.assert_same_decision(result, expected)
        self.assertEqual(result["validation_work"]["full_history_evaluations"], 1)
        self.assertEqual(result["validation_work"]["checked_bars"], len(self.bars) * 8)

    def test_exact_transition_and_complete_input_changes_cannot_be_promoted(self):
        previous = self.previous()
        with StandaloneReader(self.store)._open_group("TEST", include_unresolved=True) as group:
            inputs, _ = group.serving_inputs()
        cases = []
        for key, value in (("version", 4), ("version", 6), ("calendar_sha256", "different")):
            prior = deepcopy(previous)
            prior["policy"][key] = value
            cases.append((key + str(value), prior, inputs))
        for field, value in (("pricing_generation", "changed"),):
            current = deepcopy(inputs)
            current["TEST"][field] = value
            cases.append((field, previous, current))
        for field, value in (("rows", 999), ("last_session", "2026-10-15")):
            current = deepcopy(inputs)
            current["TEST"]["views"]["all"][field] = value
            cases.append((field, previous, current))
        for owner in (
            (None, None),
            ("another-id", ["TEST"]),
            (inputs["TEST"]["views"]["all"]["ownership"][0]["owner"][0], ["NEW"]),
        ):
            current = deepcopy(inputs)
            current["TEST"]["views"]["all"]["ownership"][0]["owner"] = list(owner)
            cases.append((str(owner), previous, current))
        current = deepcopy(inputs)
        current["OTHER"] = current.pop("TEST")
        cases.append(("source replaced", previous, current))
        current = deepcopy(inputs)
        current["OTHER"] = deepcopy(current["TEST"])
        cases.append(("source added", previous, current))
        prior = deepcopy(previous)
        prior["history_validation"]["inputs"]["OTHER"] = deepcopy(prior["history_validation"]["inputs"]["TEST"])
        cases.append(("source removed", prior, inputs))
        for name, prior, current in cases:
            with self.subTest(name=name):
                self.assertIsNone(reuse._unchanged_history_checkpoint(prior, current))
        with patch.object(reuse, "policy", return_value={**reuse.policy(), "version": 7}):
            self.assertIsNone(reuse._unchanged_history_checkpoint(previous, inputs))

    def test_complete_unchanged_multisource_checkpoint_is_translated(self):
        previous = self.previous()
        with StandaloneReader(self.store)._open_group("TEST", include_unresolved=True) as group:
            inputs, _ = group.serving_inputs()
        inputs["OTHER"] = deepcopy(inputs["TEST"])
        previous["history_validation"]["inputs"]["OTHER"] = deepcopy(
            previous["history_validation"]["inputs"]["TEST"]
        )
        self.assertEqual(reuse._unchanged_history_checkpoint(previous, inputs)["inputs"], inputs)

    def test_complete_two_identity_group_matches_full_evaluation_without_price_reads(self):
        self.add_symbol("OTHER")
        entries = lineage.read_index(self.root)["symbols"]
        sources = {name: entry["source_sha256"] for name, entry in entries.items()}
        for name in sources:

            def connect(receipt, boundaries):
                receipt["input_sources"] = sources
                receipt["dependencies"] = sorted(set(sources) - {receipt["symbol"]})

            self.publish_change(connect, name)
        self.bootstrap()
        previous = legacy_group(self.catalogue_groups("OTHER")["OTHER"])
        expected = self.evaluate({**previous, "policy": {"version": -1}})
        with patch.object(standalone, "_rows", side_effect=AssertionError("unchanged group reopened")):
            result = self.evaluate(previous)
        self.assert_same_decision(result, expected)
        self.assertEqual(result["validation_work"]["reused_history_prefixes"], 2)

    def test_published_conflict_cannot_be_overridden_by_a_completed_prefix(self):
        previous = self.previous()

        def conflict(receipt, boundaries):
            for boundary in boundaries.values():
                for view in boundary["adjustments"]:
                    for segment in view["segments"]:
                        segment["observation_selection"] = "conflict"

        self.publish_change(conflict)
        expected = self.evaluate({**previous, "policy": {"version": -1}})
        with patch.object(standalone, "_rows", side_effect=AssertionError("published conflict reopened prices")):
            result = self.evaluate(previous)
        self.assert_same_decision(result, expected)
        self.assertFalse(result["decisions"][0]["eligible"])
        self.assertEqual(result["decisions"][0]["reasons"], ["conflicting_daily_history"])

    def test_rebuild_publishes_current_checkpoint_and_repeat_reuses_it(self):
        previous = self.previous()
        # Synthetic retained store, with its actual old selector and checkpoint.
        with closing(connect_sqlite(storage.path(self.store))) as db, db:
            state = json.loads(db.execute("SELECT body FROM state WHERE id=1").fetchone()[0])
            state["policy"]["version"] = 5
            selection = json.loads(
                db.execute("SELECT selection FROM groups WHERE group_key='TEST'").fetchone()[0]
            )
            selection["policy"]["version"] = 5
            db.execute("UPDATE state SET body=? WHERE id=1", (storage.encoded(state),))
            db.execute(
                "UPDATE groups SET selection=?,body=? WHERE group_key='TEST'",
                (storage.encoded(selection), storage.encoded(previous)),
            )
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("migration reopened history")),
            patch.object(
                policy, "_bar_reasons", side_effect=AssertionError("migration repeated numeric checks")
            ),
        ):
            result = maintain(self.store, rebuild=True)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        self.assertEqual(result["reused_history_prefixes"], 1)
        self.assertEqual(self.catalogue_groups("TEST")["TEST"]["policy"], reuse.policy())
        self.assertEqual(DataLibrary(store=self.store).online_count(), 1)
        self.assertEqual(work.read(self.store), {})
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("repeat reopened history")),
            patch.object(
                reuse,
                "_unchanged_history_checkpoint",
                side_effect=AssertionError("repeat retried migration"),
            ),
        ):
            repeat = maintain(self.store, rebuild=True)
        self.assertTrue(repeat["complete"], repeat)
        self.assertEqual(repeat["reused_groups"], 1)
        self.assertEqual(repeat["evaluated_groups"], 0)


if __name__ == "__main__":
    unittest.main()

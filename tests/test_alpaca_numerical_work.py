"""Numerical requirements survive planning and cannot be acknowledged by pricing."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_numerical_completion_integration as numerical_fixtures
from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import asset
from test_alpaca_universe_update import CatalogueClient

from alpaca_end_of_day import DataLibrary
from libs.alpaca_lineage_build import numerical_completion, numerical_recovery, storage, universe_state
from libs.alpaca_lineage_build.check_summary import rebuild
from libs.alpaca_lineage_build.universe_update import run_update
from libs.alpaca_lineage_build.work_contract import WorkDeferred, WorkPolicy, obligation, satisfied
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pricing_download.contract import ADJUSTMENTS, timestamp
from libs.alpaca_pricing_download.http import BarsClient


class NumericalWorkTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name)
        universe_state.initialize(self.store)
        self.document = {
            "schema_version": 1,
            "boundary": numerical_recovery.BOUNDARY,
            "mode": "numerical_only",
            "store": str(self.store),
            "correlation_policy_sha256": "c" * 64,
            "groups": [
                {
                    "members": ["TEST"],
                    "pricing_generations": {"TEST": "a" * 64},
                    "receipt_generations": {"TEST": "b" * 64},
                    "scopes": {
                        "TEST": [
                            {
                                "adjustment": view,
                                "boundary": "completed_years",
                                "public_id": "d" * 64,
                                "row_start": 1,
                                "row_end": 2,
                            }
                            for view in ADJUSTMENTS
                        ]
                    },
                }
            ],
        }
        self.selection = numerical_recovery.accept_selection(self.document, self.store)
        self.marker = self.selection.groups[0].completion()
        self.required = {**self.marker, "selection": self.document}
        self.item = {
            "end_exclusive": "2026-10-14",
            "status": "pending",
            "obligation_id": "obligation",
            "required_group": ["TEST"],
            "required_numerical_completion": self.required,
        }
        self.entry = {
            "sha256": "b" * 64,
            "source_sha256": "a" * 64,
            "dependencies": [],
            "asset_ids": [],
            "group_members": ["TEST"],
            "projection": {
                "processed_through": "2026-10-14",
                "update_obligation_id": "obligation",
            },
        }

    def test_pricing_approval_and_matching_stamp_do_not_satisfy_missing_numerical_work(self):
        self.assertFalse(satisfied(self.item, self.entry, False, approved=True))
        self.entry["projection"]["retained_recovery"] = {**self.marker, "operation_id": "different"}
        self.assertFalse(satisfied(self.item, self.entry, False, approved=True))
        self.entry["projection"]["retained_recovery"] = self.marker
        self.assertTrue(satisfied(self.item, self.entry, False))

    def test_changing_scope_renews_the_obligation(self):
        self.assertEqual(obligation(self.item, self.item)["obligation_id"], "obligation")
        changed = {**self.item, "required_numerical_completion": {**self.required, "operation_id": "new"}}
        self.assertNotEqual(obligation(changed, self.item)["obligation_id"], "obligation")

    def test_unchanged_gate_and_stamp_require_actual_numerical_completion(self):
        policy = WorkPolicy(self.store, {"symbols": {"TEST": self.entry}}, {"TEST": self.item}, [], "2026-10-14")
        source = {"latest_request": {"end_exclusive": "2026-10-14"}}
        self.assertFalse(policy.decision_inputs_satisfied({"TEST": source}))
        receipt = {"source_sha256": "a" * 64, "evidence": {"asset_lookups": []}}
        with self.assertRaisesRegex(WorkDeferred, "numerical completion"):
            policy.stamp({"TEST": receipt}, {"TEST": source})
        self.entry["projection"]["retained_recovery"] = self.marker
        receipt["retained_recovery"] = self.marker
        self.assertTrue(policy.decision_inputs_satisfied({"TEST": source}))
        self.assertEqual(policy.stamp({"TEST": receipt}, {"TEST": source}), {"TEST": "TEST"})

    def test_scope_is_accepted_once_and_completed_marker_keeps_it_closed(self):
        policy = WorkPolicy(self.store, {"symbols": {"TEST": self.entry}}, {"TEST": self.item}, [], "2026-10-14")
        with patch.object(
            numerical_recovery, "accept_selection", wraps=numerical_recovery.accept_selection
        ) as accept:
            first = policy.numerical_selection("TEST")
            self.assertIs(first, policy.numerical_selection("TEST"))
            accept.assert_called_once()
        self.entry["projection"]["retained_recovery"] = self.marker
        with patch.object(
            numerical_recovery, "accept_selection", side_effect=AssertionError("scope accepted again")
        ):
            self.assertIsNone(policy.numerical_selection("TEST"))

    def test_accepted_numerical_scope_crosses_value_codec_without_ingress(self):
        encoder = Encoder()
        root = encoder.encode((self.selection, self.selection.groups[0]))
        with patch.object(
            numerical_recovery, "accept_selection", side_effect=AssertionError("ingress replayed")
        ):
            restored = Decoder(encoder.objects).decode(root)
        self.assertEqual(restored[0], self.selection)
        self.assertIs(restored[0].groups[0], restored[1])

    def test_baseline_selection_decodes_only_pending_numerical_rows(self):
        universe_state.put_work_group(
            self.store,
            {
                "TEST": self.item,
                "DONE": {**self.item, "status": "complete"},
                "ORDINARY": {
                    "status": "pending",
                    "pricing_correction": {"observations": {"all": [["invalid"]]}},
                },
            },
        )
        with patch(
            "libs.alpaca_pricing_download.correction.validate_durable_observations",
            side_effect=AssertionError("unrelated pricing observations validated"),
        ):
            selected = universe_state.read_numerical_work(self.store)
        self.assertEqual(selected["work"], {"TEST": self.item})


class NumericalUpdateIntegrationTests(NativeCase):
    legacy_group = numerical_fixtures.NumericalCompletionIntegrationTests.legacy_group
    selection = numerical_fixtures.NumericalCompletionIntegrationTests.selection

    def update_with_numerical_scope(self, *, invalid=False):
        self.legacy_group()
        universe_state.initialize(self.store)
        rebuild(self.store)
        from libs.alpaca_pricing_download.update_checks import complete_checks, read_checks

        checks = read_checks(self.store)
        self.assertFalse(checks["checks"])
        complete_checks(self.store, checks["epoch"])
        selected = self.selection()
        numerical_recovery.enroll_selection(selected, index=self.baseline_index)
        if invalid:
            document = {**selected.normalized, "groups": [dict(selected.normalized["groups"][0])]}
            document["groups"][0]["pricing_generations"] = {
                **document["groups"][0]["pricing_generations"],
                "MAIN": "f" * 64,
            }
            selected = numerical_recovery.accept_selection(document, self.store)
            work = universe_state.read_state(self.store, symbols=self.members)["work"]
            requirement = {**selected.groups[0].completion(), "selection": selected.normalized}
            universe_state.put_work_group(
                self.store,
                {
                    name: obligation({**item, "required_numerical_completion": requirement}, item)
                    for name, item in work.items()
                },
            )
        marker = selected.groups[0].completion()
        registry = self.directory / "ticker.csv"
        registry.write_text("MAIN,OLD,TEST,UNKNOWN,ZERO\n")
        pricing = BarsClient("fake", "fake")
        rows = valid_bars("2026-10-01", "2026-10-15")
        calls = []

        def page(params):
            symbols = set(params["symbols"].split(","))
            self.assertLessEqual(symbols, {"TEST"} if invalid else {"TEST", "MAIN", "OLD", "ZERO"})
            if not invalid:
                current = storage.read_index(self.root)
                self.assertEqual(current["symbols"]["MAIN"]["projection"].get("retained_recovery"), marker)
            calls.append(params)
            first, last = timestamp(params["start"]), timestamp(params["end"])
            available = {"TEST": rows, "MAIN": self.main_rows, "OLD": self.old_rows, "ZERO": []}
            return {
                "bars": {
                    name: [row for row in available[name] if first <= timestamp(row["t"]) <= last]
                    for name in symbols
                }
            }

        with (
            patch.object(pricing, "_get", side_effect=page),
            patch.object(numerical_completion, "complete", wraps=numerical_completion.complete) as calculate,
        ):
            result = run_update(
                self.store,
                "2026-10-15",
                pricing_client=pricing,
                identity_client=CatalogueClient([asset("TEST")]),
                registry=registry,
            )
        self.assertTrue(calls, result)
        library = DataLibrary(store=self.store)
        test_id = library.resolve_ticker("TEST")[0].public_id
        self.assertEqual(
            library.get_history(test_id).bars[-1].session.isoformat(),
            "2026-10-14",
            {key: result.get(key) for key in ("failed", "failures", "pending_work", "deferred")},
        )
        return result, calculate.call_count

    def test_prepare_finishes_numerical_work_before_independent_price_advancement(self):
        result, calls = self.update_with_numerical_scope()
        self.assertTrue(result["complete"], result)
        self.assertEqual(calls, 1)
        self.assertTrue(
            all(item["status"] == "complete" for item in universe_state.read_state(self.store)["work"].values())
        )

    def test_invalid_numerical_binding_leaves_only_its_group_pending(self):
        result, calls = self.update_with_numerical_scope(invalid=True)
        self.assertFalse(result["complete"], result)
        self.assertGreater(result["failed"], 0)
        self.assertEqual(calls, 0)
        work = universe_state.read_state(self.store)["work"]
        self.assertEqual(work["TEST"]["status"], "complete")
        for name in ("MAIN", "OLD", "ZERO"):
            self.assertNotEqual(work[name]["status"], "complete")
            self.assertEqual(
                storage.read_index(self.root)["symbols"][name]["source_sha256"],
                self.baseline_index["symbols"][name]["source_sha256"],
            )


if __name__ == "__main__":
    unittest.main()

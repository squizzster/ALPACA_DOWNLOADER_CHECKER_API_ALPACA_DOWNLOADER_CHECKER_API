"""A historical source correction carries all unaffected numerical work."""

import tempfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_dated_market_occupancy import market_fact
from test_alpaca_end_of_day_api import valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import DataLibrary, _standalone_group, standalone
from libs.alpaca_lineage_build import pipeline, recovery_handoffs, storage
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain


class ScopedSourceServingTests(TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Path(directory.name) / "store"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        for name, start, end in (("MAIN", "2026-10-01", "2026-10-14"), ("ALT", "2026-10-02", "2026-10-07")):
            rows = valid_bars(start, end)
            for row in rows:
                row["n"] = 99 if name == "MAIN" else 7
            download_pricing(self.store, name, "2026-10-14", client=FakeClient(rows))
        self.owner = asset("MAIN", "same-id")
        self.client = IdentityClient(
            {"MAIN": self.owner, "ALT": self.owner, "111111111": self.owner, "222222222": self.owner}
        )
        self.assertEqual(
            0, pipeline.run_lineage(self.store, ["MAIN", "ALT"], identity_client=self.client)["failed"]
        )
        self.assertTrue(maintain(self.store, bootstrap=True)["complete"])

    def apply(self, *, context=None):
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("full history loaded")),
            patch.object(pipeline, "correlate", side_effect=AssertionError("historical correlation replayed")),
        ):
            result = pipeline.run_lineage(
                self.store,
                ["MAIN"],
                identity_client=self.client,
                event_supplements=[market_fact()],
                context=context,
            )
        self.assertEqual(0, result["failed"], result)

    def test_ordinary_projection_and_serving_check_only_three_changed_sessions(self):
        checked = []
        validate = policy._bar_reasons

        def changed_only(bar, adjustment):
            self.assertIn(bar["session"], {"2026-10-02", "2026-10-05", "2026-10-06"})
            checked.append((bar["session"], adjustment))
            return validate(bar, adjustment)

        with recovery_handoffs.operation(self.store):
            self.apply()
            with (
                patch.object(policy, "_bar_reasons", side_effect=changed_only),
                patch.object(
                    _standalone_group, "_rows", side_effect=AssertionError("held selected rows reread")
                ),
                patch.object(
                    standalone, "_rows", side_effect=AssertionError("accepted numerical history reread")
                ),
            ):
                result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(24, result["checked_bars"])
        self.assertEqual(0, result["full_history_evaluations"])
        self.assertEqual(1, result["reused_history_prefixes"])
        self.assertTrue(checked)
        library = DataLibrary(store=self.store)
        self.assertEqual(1, library.online_count())

    def test_declared_incomplete_revision_fails_without_reconstruction(self):
        with recovery_handoffs.operation(self.store):
            self.apply()
        from libs.alpaca_lineage_build.continuation import validate_handoff

        root = self.store / "end_of_day/lineage"
        receipt = storage.read_receipt(root, "MAIN", storage.read_index(root))
        malformed = deepcopy(receipt)
        malformed["numerical_source_revision"].pop("source_membership")
        with self.assertRaisesRegex(LineageError, "source revision"):
            validate_handoff(malformed)

    def test_downstream_failure_keeps_completed_revision_validation(self):
        with recovery_handoffs.operation(self.store):
            self.apply()
            with patch.object(policy, "_metadata", side_effect=ValueError("stop after completed revision")):
                stopped = maintain(self.store)
            self.assertFalse(stopped["complete"], stopped)
            with (
                patch.object(policy, "_bar_reasons", side_effect=AssertionError("completed revision rechecked")),
                patch.object(_standalone_group, "_rows", side_effect=AssertionError("accepted prices reread")),
            ):
                result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(0, result["checked_bars"])

    def test_equal_values_consume_the_completed_equality_without_numerical_checks(self):
        rows = valid_bars()
        for row in rows:
            row["n"] = 7
        download_pricing(self.store, "MAIN", "2026-10-14", client=FakeClient(rows), refresh_all=True)
        self.assertEqual(0, pipeline.run_lineage(self.store, ["MAIN"], identity_client=self.client)["failed"])
        self.assertTrue(maintain(self.store)["complete"])
        with recovery_handoffs.operation(self.store):
            self.apply()
            with (
                patch.object(
                    policy, "_bar_reasons", side_effect=AssertionError("accepted equal values rechecked")
                ),
                patch.object(
                    _standalone_group, "_rows", side_effect=AssertionError("accepted equal prices reread")
                ),
            ):
                result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(0, result["checked_bars"])
        self.assertEqual(0, result["full_history_evaluations"])

    def test_frozen_journal_recovery_assembles_only_the_unfinished_selected_scope(self):
        class Interrupted(BaseException):
            pass

        with (
            recovery_handoffs.operation(self.store),
            patch.object(storage, "finish_publication", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            self.apply()
        root = self.store / "end_of_day/lineage"
        self.assertTrue((root / ".pending.json").exists())
        read = _standalone_group._rows
        seen = []

        def selected_only(path, source, kind, adjustment, *args, **kwargs):
            self.assertEqual("ALT", path.name, "unaffected numerical source opened")
            seen.append((kind, adjustment))
            return read(path, source, kind, adjustment, *args, **kwargs)

        validate = policy._bar_reasons

        def changed_only(bar, adjustment):
            self.assertIn(bar["session"], {"2026-10-02", "2026-10-05", "2026-10-06"})
            return validate(bar, adjustment)

        with recovery_handoffs.operation(self.store):
            with (
                patch.object(pipeline, "correlate", side_effect=AssertionError("journal correlation replayed")),
                patch.object(
                    pipeline, "select_observations", side_effect=AssertionError("journal choice replayed")
                ),
            ):
                self.assertTrue(storage.recover(self.store, root))
            with (
                patch.object(_standalone_group, "_rows", side_effect=selected_only),
                patch.object(policy, "_bar_reasons", side_effect=changed_only),
            ):
                result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(24, result["checked_bars"])
        self.assertTrue(seen)

    def test_prepared_publication_consumes_held_bundles_without_repeating_selection(self):
        from libs.alpaca_lineage_build import assignment_handoff, market_assignments, market_observations
        from libs.alpaca_lineage_build.context import OperationContext
        from libs.alpaca_lineage_build.prepared_recovery import accept_candidate, publish_prepared
        from libs.alpaca_lineage_build.source_work import assignment_documents

        class Prepared(BaseException):
            pass

        root = self.store / "end_of_day/lineage"
        index = storage.read_index(root)
        prior = {name: storage.read_receipt(root, name, index) for name in ("MAIN", "ALT")}
        baseline = {name: assignment_documents(root, receipt, {}) for name, receipt in prior.items()}
        captured = {}

        def capture(store, root, index, receipts, boundaries, notices, **kwargs):
            self.assertTrue(any(receipt.get("numerical_source_revision") for receipt in receipts.values()))
            captured.update(receipts=receipts, boundaries=boundaries)
            raise Prepared()

        context = OperationContext(self.store)
        with recovery_handoffs.operation(self.store):
            for name, receipt in prior.items():
                recovery_handoffs.remember(
                    self.store, "receipt", (name, index["symbols"][name]["sha256"]), receipt
                )
                for kind, body in baseline[name].items():
                    recovery_handoffs.remember(
                        self.store, "assignment", (name, kind, receipt["boundaries"][kind]["sha256"]), body
                    )
            with patch.object(storage, "publish", side_effect=capture), self.assertRaises(Prepared):
                self.apply(context=context)
            receipt = captured["receipts"]["MAIN"]
            revision = receipt["numerical_source_revision"]
            bundle = recovery_handoffs.recall(self.store, "source_choice_bundle_handoff", revision["id"])
            self.assertIsNotNone(bundle)
            value = {
                "version": 1,
                "boundary": "alpaca_retained_candidate_handoff",
                "store": str(self.store),
                "members": ["ALT", "MAIN"],
                "computation_revision": 2,
                "policy": context.accept_correlation_policy().as_report_object(),
                "pricing_generations": revision["pricing_generations"],
                "receipt_generations": revision["previous_receipt_generations"],
                "baseline_receipts": prior,
                "baseline_boundaries": baseline,
                "candidate_receipts": captured["receipts"],
                "candidate_boundaries": captured["boundaries"],
                "selected_observation_bundles": bundle,
                "comparison": {"comparison": {"mapping_corrections": {}, "candidate_partition_complete": True}},
                "review": {
                    "version": 1,
                    "status": "approved",
                    "conclusion": "Exact synthetic three-session correction.",
                },
            }
            selection = accept_candidate(value, self.store)
            with (
                patch.object(
                    market_observations, "select_observations", side_effect=AssertionError("choice repeated")
                ),
                patch.object(
                    market_assignments, "project_group", side_effect=AssertionError("construction repeated")
                ),
                patch.object(assignment_handoff, "produce", side_effect=AssertionError("handoff reconstructed")),
                patch.object(
                    _standalone_group, "_rows", side_effect=AssertionError("held selected values reread")
                ),
            ):
                published = publish_prepared(selection, context)
                served = maintain(self.store, context=context)
        self.assertEqual(1, published["published_prepared_groups"])
        self.assertTrue(served["complete"], served)
        self.assertEqual(24, served["checked_bars"])

    def test_later_append_preserves_repaired_historical_source_choices(self):
        from libs.alpaca_lineage_build import observations

        with recovery_handoffs.operation(self.store):
            self.apply()
            self.assertTrue(maintain(self.store)["complete"])
        for name, start, end in (("MAIN", "2026-10-01", "2026-10-15"), ("ALT", "2026-10-02", "2026-10-07")):
            rows = valid_bars(start, end)
            for row in rows:
                row["n"] = 99 if name == "MAIN" else 7
            download_pricing(self.store, name, "2026-10-15", client=FakeClient(rows))
        select = observations._select_new_observations
        active = []

        def new_only(inputs, reports, **kwargs):
            for name, report in reports.items():
                for segment in report["correlated"]["segments"]:
                    for position in range(segment["row_start"] - 1, segment["row_end"]):
                        self.assertEqual(
                            "2026-10-14", inputs[name].views[segment["adjustment"]][position][0][:10]
                        )
                        active.append(position)
            return select(inputs, reports, **kwargs)

        with patch.object(observations, "_select_new_observations", side_effect=new_only):
            result = pipeline.run_lineage(self.store, ["MAIN"], identity_client=self.client)
        self.assertEqual(0, result["failed"], result)
        self.assertTrue(active)
        root = self.store / "end_of_day/lineage"
        index = storage.read_index(root)
        alt = storage.read_receipt(root, "ALT", index)
        parts = storage.read_boundaries(root, alt)
        self.assertEqual(
            {"selected"},
            {
                segment["observation_selection"]
                for part in parts.values()
                for entry in part["adjustments"]
                for segment in entry["segments"]
                if segment["branch"] == "correlated"
            },
        )
        checked = []
        validate = policy._bar_reasons

        def appended_only(bar, adjustment):
            self.assertEqual("2026-10-14", bar["session"])
            checked.append(bar["session"])
            return validate(bar, adjustment)

        with patch.object(policy, "_bar_reasons", side_effect=appended_only):
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(8, served["checked_bars"])

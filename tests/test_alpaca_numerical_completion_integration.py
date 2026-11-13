"""Complete legacy numerical contests without replaying accepted ownership."""

from collections import Counter
from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch
from uuid import uuid4

from test_alpaca_end_of_day_api import NativeCase, valid_bars
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient

from alpaca_end_of_day import CatalogChangedError, DataLibrary
from libs.alpaca_lineage_build import pipeline, storage, transform
from libs.alpaca_pricing_download import storage as pricing_storage
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue.producer import maintain


class NumericalCompletionIntegrationTests(NativeCase):
    def legacy_group(self, *, changed_labels=False, continuous_main=False):
        """Retain a real earlier eligible result with its old duplicate choices.

        The current producer first constructs the supported identity, prices and
        serving outcome. The fixture then installs the legacy representation:
        both overlapping source observations are selected, and the old serving
        checkpoint records their ownership but no numerical-choice capability.
        This is retained output, not evidence supplied to fresh inference.
        """
        self.main_rows = valid_bars("2025-01-02", "2025-01-10")
        self.old_rows = self.main_rows[:3]
        owner = asset("MAIN", "continuing-identity")
        actions = [
            {
                "action_type": "name_change",
                "id": "rename-observation",
                "process_date": "2025-01-07",
                "old_symbol": "OLD",
                "new_symbol": "MAIN",
                "old_cusip": "111111111",
                "new_cusip": "111111111",
            }
        ]
        for name, rows in (("MAIN", self.main_rows), ("OLD", self.old_rows), ("ZERO", [])):
            download_pricing(self.store, name, "2026-10-14", client=FakeClient(rows))
        result = pipeline.run_lineage(
            self.store,
            ["MAIN", "OLD", "ZERO"],
            identity_client=IdentityClient(
                {name: owner for name in ("MAIN", "OLD", "ZERO", "111111111")}, actions
            ),
        )
        self.assertEqual(result["failed"], 0, result)
        download_pricing(self.store, "UNKNOWN", "2026-10-14", client=FakeClient(self.main_rows))
        result = pipeline.run_lineage(self.store, ["UNKNOWN"], identity_client=IdentityClient({}))
        self.assertEqual(result["failed"], 0, result)
        library = self.bootstrap()
        self.public_id = library.search_securities("MAIN")[0].public_id
        self.expected = library.get_histories(self.public_id)
        old_controls = serving_storage.controls(self.store)
        self.group_key = old_controls["members"]["MAIN"]
        accepted = serving_storage.read_groups(
            self.store, [self.group_key], generation=old_controls["state"]["generation"]
        )[self.group_key]
        index = storage.read_index(self.root)
        old_receipt = storage.read_receipt(self.root, "OLD", index)
        old_documents = storage.read_boundaries(self.root, old_receipt)

        def duplicate(receipt, boundaries):
            for kind, boundary in boundaries.items():
                for view in boundary["adjustments"]:
                    if receipt["symbol"] == "MAIN":
                        donor = next(
                            entry
                            for entry in old_documents[kind]["adjustments"]
                            if entry["adjustment"] == view["adjustment"]
                        )["segments"]
                        # The retained representation accepted the predecessor
                        # prefix through both mapped sources. Current ownership
                        # inference is deliberately not part of this fixture.
                        view["segments"] = [
                            deepcopy(donor[0]) if segment["branch"] == "uncorrelated" else segment
                            for segment in view["segments"]
                        ]
                    for segment in view["segments"]:
                        if segment["branch"] == "correlated":
                            segment["observation_selection"] = "selected"
                            if (
                                changed_labels
                                and receipt["symbol"] == "MAIN"
                                and segment["historical_symbol"] == "OLD"
                            ):
                                segment["observation_tickers"] = ["OLD", "OTHER"]
                    if continuous_main and receipt["symbol"] == "MAIN" and view["segments"]:
                        first, last = view["segments"][0], view["segments"][-1]
                        view["segments"] = [
                            {
                                **deepcopy(last),
                                "from": first["from"],
                                "first_timestamp": first["first_timestamp"],
                                "row_start": first["row_start"],
                                "row_count": sum(segment["row_count"] for segment in view["segments"]),
                            }
                        ]
            if receipt["symbol"] == "MAIN":
                receipt["correlated_rows"] = receipt["source_rows"]
                receipt["unresolved_rows"] = 0
                receipt["row_partition_complete"] = True

        for name in ("MAIN", "OLD"):
            self.publish_change(duplicate, name)

        def unmarked(receipt, boundaries):
            for handoff in receipt["assignment_handoffs"].values():
                handoff.pop("observation_choices", None)
                handoff.pop("observation_choices_version", None)
            boundaries.clear()

        for name in ("MAIN", "OLD", "ZERO"):
            self.publish_change(unmarked, name)
        index = storage.read_index(self.root)
        self.members = tuple(index["symbols"]["MAIN"]["group_members"])
        self.assertEqual(set(self.members), {"MAIN", "OLD", "ZERO"})
        self.receipts = {name: storage.read_receipt(self.root, name, index) for name in self.members}
        self.baseline_index = deepcopy(index)
        legacy = deepcopy(accepted)
        legacy["decision_generation"] = uuid4().hex
        legacy["references"]["lineage"] = {name: index["symbols"][name]["sha256"] for name in self.members}
        for name, receipt in self.receipts.items():
            legacy["history_validation"]["inputs"][name]["views"] = deepcopy(
                receipt["serving_ownership"]["views"]
            )
        legacy["history_validation"]["observation_choice_handoffs"] = {"version": 1, "sources": {}}
        if continuous_main:
            for record in legacy["history_validation"]["prefix_records"]:
                if record["public_id"] == self.public_id:
                    for item in record["ticker_ranges"]:
                        if item["ticker"] == "MAIN":
                            item["first_session"] = record["first_session"]
        legacy["validation_work"] = {
            "reused_history_prefixes": 1,
            "full_history_evaluations": 0,
            "checked_bars": 0,
        }
        serving_storage.commit(self.store, {self.group_key: accepted}, [legacy])
        self.assertTrue(legacy["decisions"][0]["eligible"])
        with self.assertRaisesRegex(CatalogChangedError, "selected serving history"):
            DataLibrary(store=self.store).get_histories(self.public_id)

    def selection(self):
        from libs.alpaca_lineage_build.numerical_recovery import accept_selection

        scopes = {}
        for name in ("MAIN", "OLD"):
            scopes[name] = [
                {
                    "adjustment": view,
                    "boundary": "completed_years",
                    "public_id": self.public_id,
                    "row_start": 1,
                    "row_end": len(self.old_rows),
                }
                for view in ADJUSTMENTS
            ]
        return accept_selection(
            {
                "boundary": "alpaca_lineage_numerical_recovery",
                "schema_version": 1,
                "mode": "numerical_only",
                "store": str(self.store),
                "correlation_policy_sha256": self.receipts["MAIN"]["correlation_policy"]["semantic_sha256"],
                "groups": [
                    {
                        "members": list(self.members),
                        "pricing_generations": {
                            name: receipt["source_sha256"] for name, receipt in self.receipts.items()
                        },
                        "receipt_generations": {
                            name: self.baseline_index["symbols"][name]["sha256"] for name in self.members
                        },
                        "scopes": scopes,
                    }
                ],
            },
            self.store,
        )

    def no_ownership_or_pricing_work(self):
        stack = ExitStack()
        for owner, name in (
            (pipeline, "correlate"),
            (pipeline, "acquire"),
            (pipeline, "download_pricing"),
            (transform, "correlate"),
            (pricing_storage, "publish"),
        ):
            stack.enter_context(patch.object(owner, name, side_effect=AssertionError(name)))
        for target in (
            "libs.alpaca_pricing_download.http.BarsClient._get",
            "libs.alpaca_security_lineage.alpaca_http.AlpacaIdentityClient.request_json",
        ):
            stack.enter_context(patch(target, side_effect=AssertionError("provider request")))
        return stack

    def test_equal_legacy_completion_reaches_public_reader_and_preserves_independent_outcomes(self):
        from libs.alpaca_lineage_build import numerical_completion, numerical_recovery

        self.legacy_group()
        selection = self.selection()
        read_artifact = pricing_storage.read_artifact
        assignment_reads = Counter()

        def selected_artifact(path, *, purpose, **kwargs):
            if (
                purpose == "published_document"
                and path.is_relative_to(self.root)
                and path.name != "lineage.manifest.json"
            ):
                self.assertIn(path.parent.name, {"MAIN", "OLD"}, "empty or independent assignment opened")
                self.assertIn(
                    path.name,
                    {"completed_years.manifest.json", "completed_years.manifest.json.projection.json"},
                    "unselected assignment boundary opened",
                )
                assignment_reads[path] += 1
                self.assertEqual(assignment_reads[path], 1, "selected assignment body reread")
            return read_artifact(path, purpose=purpose, **kwargs)

        with (
            self.no_ownership_or_pricing_work(),
            patch.object(numerical_completion, "complete", wraps=numerical_completion.complete) as completed,
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted equal bars rechecked")),
            patch(
                "libs.alpaca_serving_catalogue.reuse.project_dated_labels",
                side_effect=AssertionError("accepted labels reprojected"),
            ),
        ):
            with patch.object(pricing_storage, "read_artifact", side_effect=selected_artifact):
                result = numerical_recovery.resume(selection)
            self.assertEqual({path.parent.name for path in assignment_reads}, {"MAIN", "OLD"})
            self.assertEqual(result.get("failed", 0), 0, result)
            served = maintain(self.store)
        self.assertEqual(completed.call_count, 1)
        self.assertTrue(served["complete"], served)
        self.assertEqual(served["checked_bars"], 0)
        self.assertEqual(served["full_history_evaluations"], 0)
        library = DataLibrary(store=self.store)
        actual = library.get_histories(self.public_id)
        for view in ADJUSTMENTS:
            self.assertEqual(actual[view].bars, self.expected[view].bars)
        self.assertEqual(library.resolve_ticker("UNKNOWN"), ())
        self.assertEqual(len(library.resolve_ticker("TEST")), 1)
        final_index = storage.read_index(self.root)
        for name in ("TEST", "UNKNOWN"):
            self.assertEqual(final_index["symbols"][name], self.baseline_index["symbols"][name])
        for name, before in self.receipts.items():
            after = storage.read_receipt(self.root, name, final_index)
            for field in ("identity_memberships", "identity_intervals", "security_relations", "source_sha256"):
                self.assertEqual(after[field], before[field])
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(numerical_completion, "complete", side_effect=AssertionError("selection repeated")),
            patch.object(storage, "read_receipt", side_effect=AssertionError("completed receipt reread")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("completed assignments reread")),
        ):
            repeated = numerical_recovery.resume(selection)
        self.assertEqual(repeated["previously_completed_groups"], 1)
        self.assertEqual(repeated["published_prepared_groups"], 0)

    def test_numerical_only_completion_rejects_a_changed_accepted_label_union(self):
        from libs.alpaca_lineage_build import numerical_recovery
        from libs.alpaca_lineage_build.contract import LineageError

        self.legacy_group(changed_labels=True)
        before = storage.read_index(self.root)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "publish", side_effect=AssertionError("changed labels published")),
            self.assertRaisesRegex(LineageError, "changed accepted dated labels"),
        ):
            numerical_recovery.resume(self.selection())
        self.assertEqual(storage.read_index(self.root), before)

    def test_competing_prefix_inside_longer_accepted_segment_preserves_its_suffix(self):
        from libs.alpaca_lineage_build import numerical_completion, numerical_recovery

        self.legacy_group(continuous_main=True)
        selected = self.selection()
        before = storage.read_boundaries(self.root, self.receipts["MAIN"])["completed_years"]
        self.assertTrue(all(len(view["segments"]) == 1 for view in before["adjustments"]))
        self.assertTrue(all(view["segments"][0]["row_count"] == 5 for view in before["adjustments"]))
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(numerical_completion, "complete", wraps=numerical_completion.complete) as complete,
            patch.object(policy, "_bar_reasons", side_effect=AssertionError("accepted equal bars rechecked")),
            patch(
                "libs.alpaca_serving_catalogue.reuse.project_dated_labels",
                side_effect=AssertionError("accepted labels reprojected"),
            ),
        ):
            result = numerical_recovery.resume(selected)
            served = maintain(self.store)
        self.assertEqual(result["published_prepared_groups"], 1)
        self.assertTrue(served["complete"], served)
        self.assertEqual(complete.call_count, 1)
        self.assertEqual(complete.call_args.kwargs["positions"]["MAIN"]["all"], [1, 2, 3])
        actual = DataLibrary(store=self.store).get_histories(self.public_id)
        for view in ADJUSTMENTS:
            self.assertEqual(actual[view].bars, self.expected[view].bars)
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "MAIN", index)
        self.assertEqual(receipt["source_sha256"], self.receipts["MAIN"]["source_sha256"])
        after = storage.read_boundaries(self.root, receipt)["completed_years"]
        for view in after["adjustments"]:
            suffix = next(segment for segment in view["segments"] if segment["row_end"] == 5)
            self.assertEqual(suffix["row_start"], 4)
            self.assertEqual(suffix["observation_selection"], "selected")

    def test_omitted_contributing_peer_rejects_before_numerical_payload_reads(self):
        from libs.alpaca_lineage_build import numerical_completion, numerical_recovery, source_observations
        from libs.alpaca_lineage_build.contract import LineageError

        self.legacy_group()
        raw = self.selection().normalized
        raw["groups"][0]["scopes"].pop("OLD")
        selected = numerical_recovery.accept_selection(raw, self.store)
        before = storage.read_index(self.root)
        read_artifact = pricing_storage.read_artifact

        def selected_artifact(path, *, purpose, **kwargs):
            if purpose == "published_document" and path.is_relative_to(self.root):
                self.assertFalse(
                    path.parent.name == "OLD" and path.name != "lineage.manifest.json",
                    "omitted peer assignment opened instead of rejecting incomplete scope",
                )
            return read_artifact(path, purpose=purpose, **kwargs)

        with (
            self.no_ownership_or_pricing_work(),
            patch.object(pricing_storage, "read_artifact", side_effect=selected_artifact),
            patch.object(
                source_observations,
                "counterpart_positions",
                side_effect=AssertionError("incomplete prices read"),
            ),
            patch.object(numerical_completion, "complete", side_effect=AssertionError("incomplete contest run")),
            patch.object(storage, "publish", side_effect=AssertionError("incomplete contest published")),
            self.assertRaisesRegex(LineageError, "numerical.*scope"),
        ):
            numerical_recovery.resume(selected)
        self.assertEqual(storage.read_index(self.root), before)

    def test_partial_competitor_scope_rejects_before_numerical_payload_reads(self):
        from libs.alpaca_lineage_build import numerical_completion, numerical_recovery, source_observations
        from libs.alpaca_lineage_build.contract import LineageError

        self.legacy_group()
        raw = self.selection().normalized
        for interval in raw["groups"][0]["scopes"]["OLD"]:
            interval["row_end"] = 2
        selected = numerical_recovery.accept_selection(raw, self.store)
        before = storage.read_index(self.root)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(
                source_observations,
                "counterpart_positions",
                side_effect=AssertionError("incomplete prices read"),
            ),
            patch.object(numerical_completion, "complete", side_effect=AssertionError("incomplete contest run")),
            patch.object(storage, "publish", side_effect=AssertionError("incomplete contest published")),
            self.assertRaisesRegex(LineageError, "numerical.*scope"),
        ):
            numerical_recovery.resume(selected)
        self.assertEqual(storage.read_index(self.root), before)

    def test_changed_pricing_binding_rejects_before_loading_retained_assignments(self):
        from libs.alpaca_lineage_build import numerical_recovery
        from libs.alpaca_lineage_build.contract import LineageError

        self.legacy_group()
        selected = self.selection()
        entry = deepcopy(self.baseline_index["symbols"]["MAIN"])
        entry["source_sha256"] = "f" * 64
        with storage.publication_lock(self.root, exclusive=True):
            storage._update_index(self.root, {"MAIN": entry})
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(storage, "read_receipt", side_effect=AssertionError("invalidated receipt read")),
            patch.object(storage, "read_boundaries", side_effect=AssertionError("invalidated assignments read")),
            self.assertRaisesRegex(LineageError, "changed"),
        ):
            numerical_recovery.resume(selected)

    def test_selected_public_id_is_not_an_ownership_assertion(self):
        from libs.alpaca_lineage_build import numerical_recovery, source_observations
        from libs.alpaca_lineage_build.contract import LineageError

        self.legacy_group()
        value = deepcopy(self.selection().normalized)
        unrelated = DataLibrary(store=self.store).resolve_ticker("TEST")[0].public_id
        for intervals in value["groups"][0]["scopes"].values():
            for interval in intervals:
                interval["public_id"] = unrelated
        selected = numerical_recovery.accept_selection(value, self.store)
        with (
            self.no_ownership_or_pricing_work(),
            patch.object(
                source_observations, "counterpart_positions", side_effect=AssertionError("foreign prices read")
            ),
            patch.object(storage, "publish", side_effect=AssertionError("ownership assertion published")),
            self.assertRaisesRegex(LineageError, "accepted row ownership"),
        ):
            numerical_recovery.resume(selected)

    def interrupted_completion(self, *, after_install):
        from alpaca_end_of_day import _standalone_group, standalone
        from libs.alpaca_lineage_build import (
            assignment_handoff,
            numerical_completion,
            numerical_recovery,
            recovery_handoffs,
        )
        from libs.alpaca_lineage_build.context import OperationContext

        class Interrupted(BaseException):
            pass

        self.legacy_group()
        selected = self.selection()
        context = OperationContext(self.store)
        install = storage._install

        def interrupt(*args, **kwargs):
            if after_install:
                install(*args, **kwargs)
            raise Interrupted()

        with recovery_handoffs.operation(self.store), self.no_ownership_or_pricing_work():
            with patch.object(storage, "_install", side_effect=interrupt), self.assertRaises(Interrupted):
                numerical_recovery.resume(selected, context=context)
            with (
                patch.object(
                    numerical_completion, "complete", side_effect=AssertionError("completed selection replayed")
                ),
                patch.object(
                    assignment_handoff, "_valid_run", side_effect=AssertionError("choice schema replayed")
                ),
                patch.object(policy, "_bar_reasons", side_effect=AssertionError("equal history rechecked")),
            ):
                with (
                    patch.object(
                        storage, "read_receipt", side_effect=AssertionError("completed receipt reread")
                    ),
                    patch.object(
                        storage, "read_boundaries", side_effect=AssertionError("completed assignments reread")
                    ),
                ):
                    numerical_recovery.resume(selected, context=context)
                # A consumer can borrow a memo through read_receipt; prohibit
                # actual selected-body I/O and schema calculation, not that
                # wrapper's immediate return of an accepted handoff.
                with (
                    patch.object(
                        storage, "selected_document", side_effect=AssertionError("accepted body reopened")
                    ),
                    patch.object(standalone, "_rows", side_effect=AssertionError("accepted prices reread")),
                    patch.object(_standalone_group, "_rows", side_effect=AssertionError("held prices reread")),
                ):
                    result = maintain(self.store, context=context)
            self.assertTrue(result["complete"], result)
        actual = DataLibrary(store=self.store).get_histories(self.public_id)
        for view in ADJUSTMENTS:
            self.assertEqual(actual[view].bars, self.expected[view].bars)

    def test_interruption_before_install_consumes_the_frozen_numerical_result(self):
        self.interrupted_completion(after_install=False)

    def test_interruption_after_install_consumes_the_frozen_numerical_result(self):
        self.interrupted_completion(after_install=True)

"""Closed source decisions retain numerical and publication handoffs across peers."""

import copy
import json
import tempfile
from contextlib import ExitStack
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, direct_adjustment_observations

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation import row_correlation
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import metadata, observations, pipeline, source_work, storage, transform
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain


class SplitClient(FakeClient):
    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        rows = super().fetch(symbol, adjustment, start, end, asof=asof)
        if symbol == "BEFORE":
            rows = [row for row in rows if row["t"][:10] < "2024-01-05"]
        elif adjustment == "all" or "split" in adjustment:
            rows = [
                {**row, **{field: Decimal("70") for field in ("o", "h", "l", "c", "vw")}, "v": 10}
                if row["t"][:10] < "2024-01-08"
                else row
                for row in rows
            ]
        return rows


class EqualClient(FakeClient):
    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        rows = super().fetch(symbol, adjustment, start, end, asof=asof)
        return [row for row in rows if symbol != "BEFORE" or row["t"][:10] < "2024-01-05"]


class MixedClient(EqualClient):
    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        rows = super().fetch(symbol, adjustment, start, end, asof=asof)
        if symbol == "AFTER" and (adjustment == "all" or "split" in adjustment):
            return [
                {**row, **{field: Decimal("70") for field in ("o", "h", "l", "c", "vw")}, "v": 10}
                if row["t"][:10] == "2024-01-04"
                else row
                for row in rows
            ]
        return rows


class DocumentedScopePipelineTests(TestCase):
    def test_nested_component_rebind_and_new_contest_keep_the_accepted_later_span(self):
        self.exercise()

    def test_interrupted_construction_keeps_completed_choices_and_peer_publication(self):
        self.exercise("construction")

    def test_interrupted_publication_consumes_completed_outputs_without_reconstruction(self):
        self.exercise("publication")

    def test_returned_source_publication_survives_delivery_failure_without_republication(self):
        self.exercise("cache_delivery", price_client=EqualClient, expected_checks=0)

    def test_returned_source_publication_crosses_json_after_delivery_failure(self):
        self.exercise("cache_delivery_json", price_client=EqualClient, expected_checks=0)

    def test_equal_bundle_reuses_donor_approval_after_component_extension(self):
        self.exercise(price_client=EqualClient, expected_checks=0)

    def test_mixed_bundle_only_checks_the_actual_changed_session(self):
        self.exercise(price_client=MixedClient, expected_checks=8)

    def test_equal_bundle_survives_construction_interruption(self):
        self.exercise("construction", price_client=EqualClient, expected_checks=0)

    def test_equal_bundle_survives_publication_interruption(self):
        self.exercise("publication", price_client=EqualClient, expected_checks=0)

    def test_durable_equal_projection_survives_loss_of_operation_rows(self):
        self.exercise(price_client=EqualClient, expected_checks=0, close_operation=True)

    def test_append_before_serving_keeps_completed_equal_prefix_approval(self):
        self.exercise(price_client=EqualClient, expected_checks=8, append_before_serving=True)

    def test_explicit_reclassification_keeps_completed_numerical_choices(self):
        self.exercise(price_client=EqualClient, expected_checks=0, reclassification=True)

    def test_serving_retry_keeps_completed_preparation_and_assembled_history(self):
        self.exercise(price_client=EqualClient, expected_checks=0, serving_interruption=True)

    def exercise(
        self,
        interruption=None,
        *,
        price_client=SplitClient,
        expected_checks=24,
        close_operation=False,
        append_before_serving=False,
        reclassification=False,
        serving_interruption=False,
    ):
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory) / "store"
            before = security_identity_for_alpaca_assets(
                ["old"], [], tickers=["BEFORE", "AFTER"], basis="accepted_earlier_source_scope"
            ).as_report_object()
            after = security_identity_for_alpaca_assets(
                ["old", "new"], [], tickers=["BEFORE", "AFTER"], basis="accepted_later_source_scope"
            ).as_report_object()
            bars = [
                {**bar(day, "70" if day >= "2024-01-08" else "10"), "v": 10 if day >= "2024-01-08" else 70}
                for day in ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09")
            ]
            client = IdentityClient(
                {
                    "BEFORE": asset("BEFORE", "old"),
                    "AFTER": asset("AFTER", "new"),
                    "111111111": asset("BEFORE", "old"),
                    "222222222": asset("AFTER", "new"),
                },
                [
                    {
                        "id": "rename",
                        "action_type": "name_change",
                        "old_symbol": "BEFORE",
                        "new_symbol": "AFTER",
                        "old_cusip": "111111111",
                        "new_cusip": "111111111",
                        "process_date": "2024-01-05",
                    },
                    {
                        "id": "split",
                        "action_type": "reverse_split",
                        "symbol": "AFTER",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                        "old_rate": 7,
                        "new_rate": 1,
                        "effective_date": "2024-01-08",
                        "process_date": "2024-01-08",
                    },
                ],
            )
            with (
                patch(
                    "libs.alpaca_pricing_download.download.utc_now",
                    return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                ),
                direct_adjustment_observations(),
            ):
                for name, cutoff in (("BEFORE", "2024-01-05"), ("AFTER", "2024-01-10")):
                    download_pricing(store, name, cutoff, from_date="2024-01-01", client=price_client(bars))
            context = OperationContext(store)
            evidence_client = metadata.EvidenceClient(client)
            correlate = pipeline.correlate
            held_reports = {}

            def accepted_scope_fixture(*args, **kwargs):
                reports = correlate(*args, **kwargs)
                inputs = args[1]
                for name, report in reports.items():
                    positive, negative = [], []
                    identity = before if name == "BEFORE" else after
                    for adjustment, rows in inputs[name].views.items():
                        for index, row in enumerate(rows, start=1):
                            day = row[0][:10]
                            segment = {
                                "adjustment": adjustment,
                                "row_start": index,
                                "row_end": index,
                                "row_count": 1,
                                "first_timestamp": row[0],
                                "last_timestamp": row[0],
                            }
                            if name == "AFTER" and day < "2024-01-05":
                                negative.append(
                                    {
                                        **segment,
                                        "reason_codes": ["overlapping_ownership_intervals_without_source_scope"],
                                        "candidate_securities": [],
                                    }
                                )
                            else:
                                old = day < "2024-01-08"
                                positive.append(
                                    {
                                        **segment,
                                        "historical_symbol": name,
                                        "security_identity": identity,
                                        "canonical_security": {
                                            "security_id": identity["security_id"],
                                            "alpaca_asset_id": "old" if old else "new",
                                            "cusip": "111111111" if old else "222222222",
                                        },
                                        "ticker_ownership_interval": {
                                            "historical_symbol": name,
                                            "start_date_inclusive": "2024-01-01"
                                            if name == "BEFORE"
                                            else "2024-01-05",
                                            "end_date_exclusive": "2024-01-05" if name == "BEFORE" else None,
                                        },
                                        "correlation_rule": {"name": "accepted_source_scope_fixture"},
                                        "warnings": [],
                                    }
                                )
                    report["correlated"] = {"row_count": len(positive), "segments": positive}
                    report["uncorrelated"] = {"row_count": len(negative), "segments": negative}
                    held_reports[name] = copy.deepcopy(report)
                return reports

            with patch.object(pipeline, "correlate", side_effect=accepted_scope_fixture):
                initial = pipeline.run_lineage(
                    store,
                    ["AFTER"],
                    identity_client=client,
                    pricing_client=price_client(bars),
                    context=context,
                    evidence_client=evidence_client,
                )
            self.assertEqual(0, initial["failed"], initial)
            self.assertTrue(maintain(store, bootstrap=True)["complete"])
            # This fixture starts at a completed source-decision boundary. Its
            # synthetic producer owns the selected full-source positions; no
            # researched document, asserted event or live inference supplies them.
            # Numerical contests and publication still run through their actual
            # owners below, including every interruption/no-repeat assertion.
            completed_reports = copy.deepcopy(held_reports)
            prefix = {
                (segment["adjustment"], segment["first_timestamp"]): segment
                for segment in completed_reports["BEFORE"]["correlated"]["segments"]
            }
            changes = []
            for segment in completed_reports["BEFORE"]["correlated"]["segments"]:
                change = {
                    "source": "BEFORE",
                    "adjustment": segment["adjustment"],
                    "row_start": segment["row_start"],
                    "row_end": segment["row_end"],
                    "old_public_id": before["security_id"],
                    "new_public_id": after["security_id"],
                }
                changes.append(change)
                segment["security_identity"] = after
                segment["canonical_security"] = {
                    **segment["canonical_security"],
                    "security_id": after["security_id"],
                }
                segment["completed_membership_refinement"] = change
            recovered = []
            for old in completed_reports["AFTER"]["uncorrelated"]["segments"]:
                template = prefix[old["adjustment"], old["first_timestamp"]]
                recovered.append(
                    {
                        **{
                            key: value
                            for key, value in old.items()
                            if key not in {"reason_codes", "candidate_securities"}
                        },
                        **{
                            key: copy.deepcopy(template[key])
                            for key in transform.ASSIGNMENT_FIELDS
                            if key in template
                        },
                        "security_identity": after,
                        "canonical_security": {
                            **template["canonical_security"],
                            "security_id": after["security_id"],
                        },
                    }
                )
            completed_reports["AFTER"]["correlated"]["segments"] = sorted(
                [*recovered, *completed_reports["AFTER"]["correlated"]["segments"]],
                key=lambda segment: (segment["adjustment"], segment["row_start"]),
            )
            completed_reports["AFTER"]["correlated"]["row_count"] += len(recovered)
            completed_reports["AFTER"]["uncorrelated"] = {"row_count": 0, "segments": []}
            ruleset = context.accept_ruleset()
            empty = {"version": 1, "certificates": [], "issues": []}
            relations = {"version": 1, "relations": [], "issues": []}
            completed_plan = SourceWorkPlan(
                {"BEFORE": "B", "AFTER": "B"},
                {"old": after, "new": after},
                empty,
                empty,
                relations,
                relations,
                {},
                False,
                prepared_reports=completed_reports,
                membership_refinement={"changes": changes, "execution_sha256": ruleset.execution_sha256},
            )
            # An explicit bound generic producer completion selects source work.
            # The plan object below is the fixture producer's completed output.
            context.completed_membership_plans[("AFTER", "BEFORE")] = {
                "store": str(store),
                "plan": {"changes": changes},
            }

            def completed_source_decisions(controls, current, auxiliary, *, cache, **_):
                for name, control in controls.items():
                    completed_reports[name]["retained_observation_routes"] = control.previous[
                        "observation_routes"
                    ]
                binding = tuple((name, id(value)) for name, value in sorted({**current, **auxiliary}.items()))
                key = (
                    binding,
                    tuple(
                        (name, control.store, control.source_sha256, control.previous_receipt_generation)
                        for name, control in sorted(controls.items())
                    ),
                )
                cache.setdefault("plans", {})[key] = completed_plan
                return completed_plan

            select = observations._select_new_observations

            def newly_joined_only(inputs, reports, **kwargs):
                for report in reports.values():
                    for segment in report["correlated"]["segments"]:
                        self.assertLess(
                            segment["last_timestamp"][:10], "2024-01-05", "accepted later choice repeated"
                        )
                return select(inputs, reports, **kwargs)

            def repair():
                return pipeline.run_lineage(
                    store, ["AFTER"], identity_client=client, evidence_client=evidence_client, context=context
                )

            handoffs = ExitStack()
            self.addCleanup(handoffs.close)
            handoffs.enter_context(operation(store))
            handoffs.enter_context(
                patch.object(
                    pipeline,
                    "prepare_group_evidence_and_source_work",
                    side_effect=completed_source_decisions,
                )
            )
            with (
                patch.object(
                    transform, "correlate", side_effect=AssertionError("accepted owner cascade repeated")
                ),
                patch.object(
                    row_correlation, "_decide_row", side_effect=AssertionError("accepted row owner repeated")
                ),
                patch.object(observations, "_select_new_observations", side_effect=newly_joined_only),
            ):
                if interruption:

                    class Interrupted(BaseException):
                        pass

                    owner, name = (
                        (transform, "make_publication")
                        if interruption == "construction"
                        else (storage.ReceiptCache, "published")
                        if interruption.startswith("cache_delivery")
                        else (storage, "publish")
                    )
                    with patch.object(owner, name, side_effect=Interrupted()), self.assertRaises(Interrupted):
                        repair()
                    from libs.alpaca_end_of_day_security_correlation import documented_markets

                    if interruption.startswith("cache_delivery"):
                        self.assertTrue(
                            any(
                                value.get("effects", {}).get("published")
                                for value in context.group_completions.values()
                            )
                        )
                    if interruption == "cache_delivery_json":
                        from libs.alpaca_pipeline.codec import Decoder, Encoder
                        from libs.alpaca_pipeline.state import export_state, import_state

                        encoder = Encoder()
                        state = export_state(context, None, encoder.encode, evidence=evidence_client)
                        document = json.loads(json.dumps({"state": state, "objects": encoder.objects}))
                        decoder = Decoder(document["objects"])
                        context, _ = import_state(
                            store, document["state"], decoder.decode, evidence=evidence_client
                        )
                    with ExitStack() as guards:
                        for owner, name in (
                            (documented_markets, "bind"),
                            (observations, "_select_new_observations"),
                            (pipeline, "load_pricing"),
                            (source_work, "project_closed_source"),
                        ):
                            guards.enter_context(
                                patch.object(
                                    owner,
                                    name,
                                    side_effect=AssertionError("completed producer repeated: " + name),
                                )
                            )
                        if interruption == "publication" or interruption.startswith("cache_delivery"):
                            guards.enter_context(
                                patch.object(
                                    transform,
                                    "make_publication",
                                    side_effect=AssertionError("completed publication reconstructed"),
                                )
                            )
                        if interruption.startswith("cache_delivery"):
                            guards.enter_context(
                                patch.object(
                                    storage,
                                    "publish",
                                    side_effect=AssertionError("completed source publication repeated"),
                                )
                            )
                        repaired = repair()
                else:
                    repaired = repair()
            self.assertEqual(0, repaired["failed"], repaired)
            if append_before_serving:
                with (
                    patch(
                        "libs.alpaca_pricing_download.download.utc_now",
                        return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                    ),
                    direct_adjustment_observations(),
                ):
                    for name in ("BEFORE", "AFTER"):
                        download_pricing(
                            store,
                            name,
                            "2024-01-11",
                            from_date="2024-01-01",
                            client=price_client([*bars, {**bar("2024-01-10", "70"), "v": 10}]),
                        )
                select = observations._select_new_observations

                def append_only(inputs, reports, **kwargs):
                    for report in reports.values():
                        for segment in report["correlated"]["segments"]:
                            self.assertGreaterEqual(segment["first_timestamp"][:10], "2024-01-10")
                    return select(inputs, reports, **kwargs)

                with patch.object(observations, "_select_new_observations", side_effect=append_only):
                    appended = repair()
                self.assertEqual(0, appended["failed"], appended)
            check = policy._bar_reasons

            def new_binding_only(row, adjustment):
                if append_before_serving:
                    self.assertEqual("2024-01-10", row["session"], "completed prefix approval lost on append")
                    return check(row, adjustment)
                self.assertLess(row["session"], "2024-01-05", "accepted later eligibility repeated")
                if expected_checks == 0:
                    self.fail("completed equal numerical approval repeated")
                if expected_checks == 8:
                    self.assertEqual("2024-01-04", row["session"], "completed equal date rechecked")
                return check(row, adjustment)

            if close_operation:
                handoffs.close()
            from alpaca_end_of_day import standalone

            with ExitStack() as guards:
                guards.enter_context(patch.object(policy, "_bar_reasons", side_effect=new_binding_only))
                if not close_operation:
                    guards.enter_context(
                        patch.object(
                            standalone, "read_artifact", side_effect=AssertionError("held pricing rows reread")
                        )
                    )
                if serving_interruption:
                    from alpaca_end_of_day._standalone_group import GroupSnapshot
                    from libs.alpaca_serving_catalogue import source_equivalence

                    with patch.object(
                        source_equivalence,
                        "accepted_dates",
                        side_effect=ValueError("stop after completed preparation"),
                    ):
                        stopped = maintain(store)
                    self.assertFalse(stopped["complete"], stopped)
                    with (
                        patch.object(
                            policy,
                            "select_prefixes",
                            side_effect=AssertionError("completed prefix projection repeated"),
                        ),
                        patch.object(
                            GroupSnapshot,
                            "serving_inputs",
                            side_effect=AssertionError("completed input projection repeated"),
                        ),
                        patch.object(
                            GroupSnapshot,
                            "serving_histories",
                            side_effect=AssertionError("completed history assembly repeated"),
                        ),
                    ):
                        served = maintain(store)
                else:
                    served = maintain(store)
            self.assertTrue(served["complete"], served)
            self.assertEqual(expected_checks, served["checked_bars"])
            library = DataLibrary(store=store)
            self.assertEqual(1, library.online_count())
            self.assertEqual(
                7 if append_before_serving else 6, len(library.get_history(after["security_id"], "all").bars)
            )
            root = store / "end_of_day/lineage"
            index = storage.read_index(root)
            for name in ("BEFORE", "AFTER"):
                receipt = storage.read_receipt(root, name, index)
                self.assertEqual(
                    [after["security_id"]], [item["security_id"] for item in receipt["identity_memberships"]]
                )

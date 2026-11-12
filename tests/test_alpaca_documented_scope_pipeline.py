"""A dated common continuation reconciles nested accepted source scopes."""

import tempfile
from contextlib import ExitStack
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_documented_markets import documented_fact
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, direct_adjustment_observations
from test_alpaca_public_reclassification import reclassification_fact

from alpaca_end_of_day import DataLibrary
from libs.alpaca_end_of_day_security_correlation import row_correlation
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import metadata, observations, pipeline, source_work, storage, transform
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.event_supplements import accept_supplements, supplement_key
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
            facts = [
                documented_fact(
                    "old-cusip",
                    ("BEFORE", "111111111"),
                    ("AFTER", "111111111"),
                    [
                        ("BEFORE", "111111111", "2024-01-01", "2024-01-05", "Earlier Common"),
                        ("AFTER", "111111111", "2024-01-05", "2024-01-08", "Continuing Common"),
                    ],
                ),
                documented_fact(
                    "new-cusip",
                    ("AFTER", "222222222"),
                    ("AFTER", "222222222"),
                    [("AFTER", "222222222", "2024-01-08", None, "Continuing Common")],
                ),
            ]
            accepted = accept_supplements([reclassification_fact()] if reclassification else facts)
            evidence_client.event_supplements.update((supplement_key(fact), fact) for fact in accepted)
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
                        else (storage, "publish")
                    )
                    with patch.object(owner, name, side_effect=Interrupted()), self.assertRaises(Interrupted):
                        repair()
                    from libs.alpaca_end_of_day_security_correlation import documented_markets

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
                        if interruption == "publication":
                            guards.enter_context(
                                patch.object(
                                    transform,
                                    "make_publication",
                                    side_effect=AssertionError("completed publication reconstructed"),
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

"""Completed generic dated outputs preserve accepted numerical authority."""

import tempfile
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_end_of_day_security_correlation import documented_markets, documented_refinement
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import (
    membership_transfer,
    metadata,
    observations,
    pipeline,
    source_work,
    storage,
    transform,
)
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_lineage_build.source_choice_coverage import AcceptedBundleCoverage
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain


class HoldingExchangeTests(TestCase):
    @staticmethod
    def evidence():
        return {
            "CURRENT": {
                "sourced_market_occupancy": {"schema_version": 1, "facts": [{"id": "legacy-research"}]},
                "asset_lookups": [
                    {"lookup_kind": "cusip", "lookup": "111111111", "outcome": "found", "asset": {"id": "old"}},
                    {"lookup_kind": "cusip", "lookup": "222222222", "outcome": "found", "asset": {"id": "new"}},
                ],
            }
        }

    def test_researched_public_exchange_cannot_create_new_dated_ownership(self):
        with (
            patch.object(Path, "open", side_effect=AssertionError("research document opened")),
            self.assertRaisesRegex(ValueError, "fact injection"),
        ):
            documented_markets.bind(self.evidence(), allow_holding_exchange=True)

    def test_legacy_enabling_flags_cannot_authorize_factual_candidates(self):
        for option in ("allow_holding_exchange", "allow_reclassification", "allow_containing_membership"):
            with self.subTest(option=option), self.assertRaisesRegex(ValueError, "fact injection"):
                documented_markets.bind(self.evidence(), **{option: True})

    def test_frozen_and_current_policies_reject_assertions_before_prices_or_row_rules(self):
        from libs.alpaca_end_of_day_security_correlation import correlation

        for path in (
            "resources/alpaca_security_correlation_v89_legacy_policy.yaml",
            "resources/alpaca_security_correlation_policy.yaml",
        ):
            policy = load_policy(path)
            with (
                self.subTest(path=path),
                patch.object(
                    correlation, "correlate_download_rows", side_effect=AssertionError("factual row inference")
                ),
                patch.object(pipeline, "load_pricing", side_effect=AssertionError("factual pricing input")),
                self.assertRaises((ValueError, LineageError)),
            ):
                transform.correlate(None, {}, self.evidence(), policy=policy)
            with self.assertRaises((ValueError, LineageError)):
                empty = {"version": 1, "certificates": [], "issues": []}
                relations = {"version": 1, "relations": [], "issues": []}
                plan = SourceWorkPlan({}, {}, empty, empty, relations, relations, {}, False)
                source_work.execute_source_work(
                    None,
                    None,
                    {},
                    self.evidence(),
                    {},
                    plan,
                    policy,
                    {},
                    loader=lambda *args: self.fail("prices"),
                )


class DatedIdentityRefinementPipelineTests(TestCase):
    def test_dated_labels_identifiers_and_public_component_preserve_numerical_outcomes(self):
        self.exercise()

    def test_append_before_first_serving_keeps_original_donors(self):
        self.exercise(append=True)

    def test_publication_interruption_keeps_completed_refinement(self):
        self.exercise(interruption=True)

    def test_newly_allocated_peer_does_not_reopen_accepted_later_bundle(self):
        self.exercise(peer=True)

    def test_newly_allocated_peer_scope_survives_publication_interruption(self):
        self.exercise(peer=True, interruption=True)

    def exercise(self, *, append=False, interruption=False, peer=False):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as scope:
            store = Path(temporary) / "store"
            scope.enter_context(
                patch(
                    "libs.alpaca_pricing_download.download.utc_now",
                    return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                )
            )
            scope.enter_context(direct_adjustment_observations())
            client = IdentityClient(
                {
                    "CURRENT": asset("CURRENT", "parent"),
                    "DUPLICATE": asset("CURRENT", "old"),
                    "111111111": asset("CURRENT", "old"),
                    "222222222": asset("CURRENT", "mid"),
                    "333333333": asset("CURRENT", "parent"),
                },
                [
                    {
                        "id": "split",
                        "action_type": "reverse_split",
                        "symbol": "CURRENT",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                        "old_rate": 10,
                        "new_rate": 1,
                        "effective_date": "2024-01-04",
                        "process_date": "2024-01-04",
                    },
                    {
                        "id": "parent",
                        "action_type": "name_change",
                        "old_symbol": "CURRENT",
                        "new_symbol": "CURRENT",
                        "old_cusip": "222222222",
                        "new_cusip": "333333333",
                        "process_date": "2024-01-08",
                    },
                ],
            )
            prices = informative_bars("2024-01-02", "2024-01-10")
            download_pricing(store, "CURRENT", "2024-01-10", from_date="2024-01-01", client=FakeClient(prices))
            if peer:
                download_pricing(
                    store, "DUPLICATE", "2024-01-10", from_date="2024-01-01", client=FakeClient(prices)
                )
            seeds = ["CURRENT", "DUPLICATE"] if peer else ["CURRENT"]
            context = OperationContext(store)
            evidence_client = metadata.EvidenceClient(client)
            old_id = security_identity_for_alpaca_assets(
                ["old", "mid"], [], tickers=["OLD", "CURRENT"], basis="accepted_earlier_component"
            ).as_report_object()
            parent_id = security_identity_for_alpaca_assets(
                ["parent"], [], tickers=["CURRENT"], basis="accepted_parent_component"
            ).as_report_object()
            duplicate_id = security_identity_for_alpaca_assets(
                ["old"], [], tickers=["OLD"], basis="accepted_predecessor_scope"
            ).as_report_object()
            correlate = pipeline.correlate
            held_reports = {}

            def accepted_fixture(*args, **kwargs):
                reports = correlate(*args, **kwargs)
                for name, report in reports.items():
                    segments, negative = [], []
                    for adjustment, rows in args[1][name].views.items():
                        for index, row in enumerate(rows, 1):
                            day = row[0][:10]
                            parent = day >= "2024-01-08"
                            identity = duplicate_id if name == "DUPLICATE" else parent_id if parent else old_id
                            label = "OLD" if day < "2024-01-05" else "CURRENT"
                            common = {
                                "adjustment": adjustment,
                                "row_start": index,
                                "row_end": index,
                                "row_count": 1,
                                "first_timestamp": row[0],
                                "last_timestamp": row[0],
                            }
                            if name == "DUPLICATE" and day >= "2024-01-04":
                                negative.append(
                                    {
                                        **common,
                                        "reason_codes": ["unresolved_uuid_candidates"],
                                        "candidate_securities": [],
                                    }
                                )
                                continue
                            segments.append(
                                {
                                    **common,
                                    "historical_symbol": label,
                                    "security_identity": identity,
                                    "canonical_security": {
                                        "security_id": identity["security_id"],
                                        "alpaca_asset_id": "parent" if parent else "old",
                                        "cusip": "333333333" if parent else "111111111",
                                    },
                                    "ticker_ownership_interval": {
                                        "historical_symbol": label,
                                        "start_date_inclusive": "2024-01-01",
                                        "end_date_exclusive": None,
                                    },
                                    "correlation_rule": {"name": "accepted_dated_fixture"},
                                    "warnings": [],
                                }
                            )
                    report["correlated"] = {"row_count": len(segments), "segments": segments}
                    report["uncorrelated"] = {"row_count": len(negative), "segments": negative}
                held_reports.update(deepcopy(reports))
                return reports

            with patch.object(pipeline, "correlate", side_effect=accepted_fixture):
                initial = pipeline.run_lineage(
                    store, seeds, identity_client=client, evidence_client=evidence_client, context=context
                )
            self.assertEqual(0, initial["failed"], initial)
            self.assertTrue(maintain(store, bootstrap=True)["complete"])
            root = store / "end_of_day/lineage"
            prior_index = storage.read_index(root)
            prior = storage.read_receipt(root, "CURRENT", prior_index)
            self.assertEqual(2, len(prior["identity_memberships"]))
            # Begin at a completed generic dated-decision boundary. The fixture
            # supplies its accepted positions, labels and component; it supplies
            # no research assertion to the runtime inference engine.
            whole = security_identity_for_alpaca_assets(
                ["old", "mid", "parent"],
                [],
                tickers=["OLD", "CURRENT"],
                basis="completed_provider_component",
            ).as_report_object()
            completed_reports = deepcopy(held_reports)
            changes = []
            for name, report in completed_reports.items():
                for segment in report["correlated"]["segments"]:
                    old_identity = segment["security_identity"]
                    change = {
                        "source": name,
                        "adjustment": segment["adjustment"],
                        "row_start": segment["row_start"],
                        "row_end": segment["row_end"],
                        "old_public_id": old_identity["security_id"],
                        "new_public_id": whole["security_id"],
                    }
                    changes.append(change)
                    day = segment["first_timestamp"][:10]
                    if peer and day < "2024-01-04":
                        segment["completed_membership_refinement"] = change
                    dated = (
                        ("old", "111111111", "OLD")
                        if day < "2024-01-04"
                        else (
                            ("mid", "222222222", "CURRENT")
                            if day < "2024-01-08"
                            else ("parent", "333333333", "CURRENT")
                        )
                    )
                    segment["security_identity"] = whole
                    segment["historical_symbol"] = dated[2]
                    segment["canonical_security"] = {
                        "security_id": whole["security_id"],
                        "alpaca_asset_id": dated[0],
                        "cusip": dated[1],
                    }
                    segment["correlation_rule"] = {"name": "completed_provider_dated_decision"}
                    segment["observation_tickers"] = [dated[2]]
                    segment["dated_identity_refinement"] = {
                        "producer": "completed_generic_dated_output",
                        "numerical_selection_unchanged": True,
                    }
            empty = {"version": 1, "certificates": [], "issues": []}
            relations = {"version": 1, "relations": [], "issues": []}
            completed_plan = SourceWorkPlan(
                {name: "B" for name in seeds},
                {key: whole for key in ("old", "mid", "parent")},
                empty,
                empty,
                relations,
                relations,
                {},
                False,
                prepared_reports=completed_reports,
                membership_refinement=None,
            )
            context.completed_membership_plans[tuple(sorted(seeds))] = {
                "store": str(store),
                "plan": {"changes": changes},
            }

            def completed_decisions(controls, current, auxiliary, *, cache, **_):
                for name, control in controls.items():
                    completed_reports[name]["retained_observation_routes"] = control.previous[
                        "observation_routes"
                    ]
                key = (
                    tuple((name, id(value)) for name, value in sorted({**current, **auxiliary}.items())),
                    tuple(
                        (name, control.store, control.source_sha256, control.previous_receipt_generation)
                        for name, control in sorted(controls.items())
                    ),
                )
                cache.setdefault("plans", {})[key] = completed_plan
                return completed_plan

            scope.enter_context(
                patch.object(
                    pipeline,
                    "prepare_group_evidence_and_source_work",
                    side_effect=completed_decisions,
                )
            )
            scope.enter_context(operation(store))

            def repair():
                return pipeline.run_lineage(
                    store, seeds, identity_client=client, evidence_client=evidence_client, context=context
                )

            choose = observations._select_new_observations

            def joined_only(inputs, reports, **kwargs):
                self.assertTrue(peer, "accepted numerical choice repeated")
                for report in reports.values():
                    for segment in report["correlated"]["segments"]:
                        self.assertLess(
                            segment["last_timestamp"][:10], "2024-01-04", "accepted sole later bundle reopened"
                        )
                return choose(inputs, reports, **kwargs)

            with (
                patch.object(
                    transform, "correlate", side_effect=AssertionError("accepted correlation repeated")
                ),
                patch.object(
                    observations,
                    "_select_new_observations",
                    side_effect=joined_only,
                ),
            ):
                if interruption:

                    class Interrupted(BaseException):
                        pass

                    with (
                        patch.object(storage, "publish", side_effect=Interrupted()),
                        self.assertRaises(Interrupted),
                    ):
                        repair()
                    with (
                        patch.object(
                            documented_refinement, "refine", side_effect=AssertionError("refinement repeated")
                        ),
                        patch.object(
                            transform,
                            "make_publication",
                            side_effect=AssertionError("publication reconstructed"),
                        ),
                        patch.object(
                            pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")
                        ),
                        patch.object(
                            storage,
                            "selected_document",
                            side_effect=AssertionError("accepted receipt body reread"),
                        ),
                    ):
                        repaired = repair()
                else:
                    repaired = repair()
            self.assertEqual(0, repaired["failed"], repaired)
            current = storage.read_receipt(root, "CURRENT", storage.read_index(root))
            transfer = current["membership_only_rebind"]
            self.assertEqual(3, transfer["version"])
            self.assertEqual(
                prior_index["symbols"]["CURRENT"]["sha256"], transfer["previous_receipt_generation"]
            )
            self.assertTrue(any(run["old_labels"] != run["labels"] for run in transfer["runs"]))
            self.assertEqual(48, sum(run["row_end"] - run["row_start"] + 1 for run in transfer["runs"]))
            damaged = deepcopy(current)
            damaged["membership_only_rebind"]["runs"][0].pop("old_labels")
            with self.assertRaisesRegex(LineageError, "invalid membership-only transfer"):
                validate_handoff(damaged)
            truncated = {key: {"rows": 1} for key in ADJUSTMENTS}
            with self.assertRaisesRegex(LineageError, "changed membership-transfer donor positions"):
                membership_transfer.carry_prefix(current, {}, truncated)
            documents = source_work.assignment_documents(root, current, {})
            for kind, document in documents.items():
                for entry in document["adjustments"]:
                    for segment in entry["segments"]:
                        if kind == "current_year" and segment["from"][:7] >= current["anchor_month"]:
                            continue
                        if segment["from"] in {"2024-01-04", "2024-01-05"}:
                            self.assertEqual("mid", segment["canonical_security"]["alpaca_asset_id"])
                            self.assertEqual("222222222", segment["canonical_security"]["cusip"])
                            self.assertEqual("CURRENT", segment["historical_symbol"])
                        self.assertEqual("selected", segment["observation_selection"])
            if append:
                download_pricing(
                    store,
                    "CURRENT",
                    "2024-01-11",
                    client=FakeClient(informative_bars("2024-01-02", "2024-01-11")),
                )
                choose = observations._select_new_observations

                def suffix_only(inputs, reports, **kwargs):
                    for report in reports.values():
                        for segment in report["correlated"]["segments"]:
                            self.assertGreaterEqual(segment["first_timestamp"][:10], "2024-01-10")
                    return choose(inputs, reports, **kwargs)

                with patch.object(observations, "_select_new_observations", side_effect=suffix_only):
                    appended = repair()
                self.assertEqual(0, appended["failed"], appended)
                final = storage.read_receipt(root, "CURRENT", storage.read_index(root))
                self.assertEqual(
                    transfer["previous_receipt_generation"],
                    final["membership_only_rebind"]["previous_receipt_generation"],
                )
                self.assertEqual(
                    prior["source_sha256"], final["membership_only_rebind"]["previous_pricing_generation"]
                )
            check = policy._bar_reasons

            def fresh_only(bar, adjustment):
                self.assertTrue(append, "accepted numerical outcome repeated")
                self.assertEqual("2024-01-10", bar["session"])
                return check(bar, adjustment)

            with (
                patch.object(policy, "_bar_reasons", side_effect=fresh_only),
                patch.object(standalone, "read_artifact", side_effect=AssertionError("held prices reread")),
            ):
                served = maintain(store)
            self.assertTrue(served["complete"], served)
            self.assertEqual(8 if append else 0, served["checked_bars"])
            library = DataLibrary(store=store)
            securities = library.list_securities()
            self.assertEqual(1, len(securities))
            expected = ["OLD", "OLD", "CURRENT", "CURRENT", "CURRENT", "CURRENT"]
            if append:
                expected.append("CURRENT")
            for adjustment in ADJUSTMENTS:
                history = library.get_history(securities[0].public_id, adjustment)
                self.assertEqual(len(expected), len(history.bars))
                for label, row in zip(expected, history.bars):
                    self.assertEqual(
                        [securities[0].public_id],
                        [item.public_id for item in library.resolve_ticker(label, on=row.session)],
                    )
            self.assertEqual((), library.resolve_ticker("OLD", on="2024-01-04"))


class DatedRefinementScopeTests(TestCase):
    def fixture(self):
        return {"version": 1, "owners": [{"source_fact_id": "legacy-research"}]}, {
            "adjustment": "raw",
            "row_start": 1,
            "row_end": 6,
            "row_count": 6,
            "historical_symbol": "OLD",
            "canonical_security": {"alpaca_asset_id": "old"},
            "security_identity": {"security_id": "accepted-owner"},
        }

    def test_authored_plan_rejects_before_rows_and_preserves_every_input_state(self):
        plan, original = self.fixture()

        class ForbiddenRows:
            def __iter__(self):
                raise AssertionError("authored plan accessed rows")

        inputs = {"CURRENT": SimpleNamespace(views={"raw": ForbiddenRows()})}
        for state in ("selected", "superseded", "conflict", "incomplete", None):
            segment = {**original, "observation_selection": state}
            reports = {"CURRENT": {"correlated": {"segments": [segment]}}}
            before = deepcopy(reports)
            with self.subTest(state=state), self.assertRaisesRegex(ValueError, "inadmissible"):
                documented_refinement.refine(inputs, reports, plan)
            self.assertEqual(before, reports)

    def test_empty_plan_lends_completed_reports_without_inspecting_inputs(self):
        _, segment = self.fixture()
        reports = {"CURRENT": {"correlated": {"segments": [segment]}}}
        for empty in (None, {}):
            with self.subTest(empty=empty):
                self.assertIs(reports, documented_refinement.refine(object(), reports, empty))
                self.assertIs(segment, reports["CURRENT"]["correlated"]["segments"][0])

    def test_timezone_boundaries_cannot_turn_research_into_dated_authority(self):
        plan, segment = self.fixture()
        segment.update(first_timestamp="2024-01-04T01:00:00Z", last_timestamp="2024-01-04T02:00:00Z")
        reports = {"CURRENT": {"correlated": {"segments": [segment]}}}
        with (
            patch(
                "libs.alpaca_pricing_download.contract.timestamp",
                side_effect=AssertionError("research date calculated"),
            ),
            self.assertRaisesRegex(ValueError, "inadmissible"),
        ):
            documented_refinement.refine({}, reports, plan)

    def test_competing_chronologies_and_foreign_owners_do_not_enable_factual_inference(self):
        plan, segment = self.fixture()
        for alternative in ("competing", "foreign"):
            selected = deepcopy(plan)
            selected["owners"].append({"source_fact_id": alternative})
            reports = {"OTHER": {"correlated": {"segments": [segment]}}}
            with self.subTest(alternative=alternative), self.assertRaisesRegex(ValueError, "inadmissible"):
                documented_refinement.refine({}, reports, selected)
            self.assertEqual("accepted-owner", segment["security_identity"]["security_id"])


class ExactAcceptedBundleCoverageTests(TestCase):
    def test_no_other_materialized_source_does_not_probe_its_own_accepted_history(self):
        inputs, protected, target = self.fixture()
        inputs = {"NEW": SimpleNamespace()}
        with patch(
            "libs.alpaca_lineage_build.source_choice_coverage.timestamp",
            side_effect=AssertionError("accepted history inspected without a counterpart"),
        ):
            self.assertFalse(AcceptedBundleCoverage(inputs, protected).covers("NEW", target))

    def fixture(self, *, target_days=None):
        days = ("2024-01-02", "2024-01-03", "2024-01-05")

        class TimestampOnly:
            def __init__(self, day):
                self.stamp = day + "T05:00:00Z"

            def __getitem__(self, field):
                if field != 0:
                    raise AssertionError("accepted bundle values were accessed")
                return self.stamp

        inputs = {
            name: SimpleNamespace(
                views=dict.fromkeys(ADJUSTMENTS, tuple(TimestampOnly(day) for day in selected))
            )
            for name, selected in (("DONOR", days), ("NEW", target_days or days))
        }
        segments = [
            {
                "adjustment": view,
                "row_start": 1,
                "row_end": 3,
                "security_identity": {"security_id": "joined"},
                "observation_selection": "selected",
            }
            for view in ADJUSTMENTS
        ]
        return inputs, {"DONOR": segments}, segments[0]

    def test_exact_counterparts_consume_only_timestamps_and_borrow_completed_instant_keys(self):
        inputs, protected, target = self.fixture()
        from libs.alpaca_pricing_download.contract import timestamp

        instants = {row[0]: timestamp(row[0]) for row in inputs["DONOR"].views["raw"]}
        inputs["DONOR"].observation_instant_keys = instants
        with patch(
            "libs.alpaca_lineage_build.source_choice_coverage.timestamp",
            side_effect=AssertionError("completed instant calculation repeated"),
        ):
            coverage = AcceptedBundleCoverage(inputs, protected)
            self.assertTrue(coverage.covers("NEW", target))
            self.assertIs(instants, coverage.instants)

    def test_equal_sparse_endpoints_and_counts_cannot_prove_an_interior_session(self):
        inputs, protected, target = self.fixture(target_days=("2024-01-02", "2024-01-04", "2024-01-05"))
        self.assertFalse(AcceptedBundleCoverage(inputs, protected).covers("NEW", target))

    def test_unknown_or_incomplete_donors_do_not_manufacture_bundle_approval(self):
        for state in (None, "superseded", "conflict", "incomplete"):
            inputs, protected, target = self.fixture()
            protected["DONOR"][-1]["observation_selection"] = state
            with self.subTest(state=state):
                self.assertFalse(AcceptedBundleCoverage(inputs, protected).covers("NEW", target))

    def test_complementary_partial_sources_cannot_make_one_complete_bundle(self):
        inputs, protected, target = self.fixture()
        inputs["OTHER"] = inputs["DONOR"]
        protected["OTHER"] = protected["DONOR"][4:]
        protected["DONOR"] = protected["DONOR"][:4]
        self.assertFalse(AcceptedBundleCoverage(inputs, protected).covers("NEW", target))

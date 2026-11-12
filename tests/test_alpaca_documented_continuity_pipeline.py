"""Compose a public-parent exchange with chronology and keep superseded peers closed."""

import json
import tempfile
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_documented_markets import documented_fact
from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations
from test_alpaca_public_common_continuation import parent_fact

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_end_of_day_security_correlation import documented_refinement, public_common_continuation
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import (
    documented_continuity,
    metadata,
    observations,
    pipeline,
    source_work,
    source_work_contract,
    storage,
    transform,
)
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_security_lineage.event_supplements import accept_supplements, supplement_key
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain


def continuity_facts():
    parent = parent_fact()
    parent["facts"]["predecessor"]["symbol"] = "OLD"
    parent["facts"]["successor"]["symbol"] = "NEW"
    parent["facts"].update(legal_effective_date="2024-01-05", exchange_effective_date="2024-01-08")
    dated = [
        documented_fact(identifier, (label, cusip), (label, cusip), [(label, cusip, first, last, name)])
        for identifier, label, cusip, first, last, name in (
            ("original-listed", "OLD", "111111111", "2024-01-01", "2024-01-08", "Original Public Common"),
            ("new-parent", "NEW", "222222222", "2024-01-08", "2024-01-10", "New Public Parent"),
            ("parent-split", "NEW", "333333333", "2024-01-10", None, "New Public Parent"),
        )
    ]
    for item in dated[1:]:
        item["facts"]["registrant_cik"] = "456"
    return [parent, *dated]


class DocumentedContinuityPipelineTests(TestCase):
    def test_public_parent_chronology_recovers_prefix_without_opening_superseded_peer_prices(self):
        self.exercise()

    def test_publication_interruption_keeps_completed_parent_and_metadata_projections(self):
        self.exercise(interruption=True)

    def exercise(self, *, interruption=False):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as scope:
            store = Path(directory) / "store"
            root = store / "end_of_day/lineage"
            scope.enter_context(
                patch(
                    "libs.alpaca_pricing_download.download.utc_now",
                    return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                )
            )
            scope.enter_context(direct_adjustment_observations())
            prices = informative_bars("2024-01-02", "2024-01-12")
            for name, rows in (("NEW", prices), ("OLD", prices[:4])):
                download_pricing(store, name, "2024-01-12", from_date="2024-01-01", client=FakeClient(rows))
            client = IdentityClient(
                {
                    "OLD": asset("OLD", "old"),
                    "NEW": asset("NEW", "latest"),
                    "111111111": asset("OLD", "old"),
                    "222222222": asset("NEW", "middle"),
                    "333333333": asset("NEW", "latest"),
                },
                [
                    {
                        "id": "parent",
                        "action_type": "name_change",
                        "old_symbol": "OLD",
                        "new_symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                        "process_date": "2024-01-08",
                    },
                    {
                        "id": "split",
                        "action_type": "reverse_split",
                        "symbol": "NEW",
                        "old_cusip": "222222222",
                        "new_cusip": "333333333",
                        "old_rate": 3,
                        "new_rate": 1,
                        "process_date": "2024-01-10",
                        "effective_date": "2024-01-10",
                    },
                ],
            )
            identities = {
                "OLD": security_identity_for_alpaca_assets(
                    ["old", "middle"], [], tickers=["OLD", "NEW"], basis="accepted_predecessor_scope"
                ).as_report_object(),
                "NEW": security_identity_for_alpaca_assets(
                    ["middle", "latest"], [], tickers=["NEW"], basis="accepted_successor_scope"
                ).as_report_object(),
            }
            correlate, select = pipeline.correlate, pipeline.select_observations

            def retained_fixture(*args, **kwargs):
                reports = correlate(*args, **kwargs)
                for name, report in reports.items():
                    positive, negative = [], []
                    for adjustment, rows in args[1][name].views.items():
                        for index, row in enumerate(rows, 1):
                            segment = {
                                "adjustment": adjustment,
                                "row_start": index,
                                "row_end": index,
                                "row_count": 1,
                                "first_timestamp": row[0],
                                "last_timestamp": row[0],
                            }
                            if name == "NEW" and row[0][:10] < "2024-01-08":
                                negative.append(
                                    {
                                        **segment,
                                        "reason_codes": ["outside_supported_name_change_history"],
                                        "candidate_securities": [],
                                    }
                                )
                            else:
                                positive.append(
                                    {
                                        **segment,
                                        "historical_symbol": name,
                                        "security_identity": identities[name],
                                        "canonical_security": {
                                            "security_id": identities[name]["security_id"],
                                            "alpaca_asset_id": "old" if name == "OLD" else "middle",
                                            "cusip": "111111111" if name == "OLD" else "222222222",
                                        },
                                        "ticker_ownership_interval": {
                                            "historical_symbol": name,
                                            "start_date_inclusive": "2024-01-01"
                                            if name == "OLD"
                                            else "2024-01-08",
                                            "end_date_exclusive": "2024-01-08" if name == "OLD" else None,
                                        },
                                        "correlation_rule": {"name": "retained_fixture"},
                                        "warnings": [],
                                    }
                                )
                    report["correlated"] = {"row_count": len(positive), "segments": positive}
                    report["uncorrelated"] = {"row_count": len(negative), "segments": negative}
                return reports

            def retained_supersession(*args, **kwargs):
                reports, corrections = select(*args, **kwargs)
                # Model the retained defect: the predecessor had completed
                # supersession while the successor's prefix was withheld.
                for segment in reports["OLD"]["correlated"]["segments"]:
                    segment["observation_selection"] = "superseded"
                return reports, corrections

            context = OperationContext(store)
            evidence_client = metadata.EvidenceClient(client)
            with (
                patch.object(pipeline, "correlate", side_effect=retained_fixture),
                patch.object(pipeline, "select_observations", side_effect=retained_supersession),
            ):
                initial = pipeline.run_lineage(
                    store, ["NEW"], identity_client=client, evidence_client=evidence_client, context=context
                )
            self.assertEqual(0, initial["failed"], initial)
            self.assertTrue(maintain(store, bootstrap=True)["complete"])
            prior = {name: storage.read_receipt(root, name, storage.read_index(root)) for name in ("OLD", "NEW")}
            self.assertEqual(32, prior["NEW"]["unresolved_rows"])
            facts = accept_supplements(continuity_facts())
            evidence_client.event_supplements.update((supplement_key(fact), fact) for fact in facts)
            scope.enter_context(operation(store))
            load, choose = pipeline.load_pricing, observations._select_new_observations
            loaded, plans = [], []
            prepare = pipeline.prepare_group_evidence_and_source_work

            def capture(*args, **kwargs):
                plan = prepare(*args, **kwargs)
                plans.append(plan)
                return plan

            def selected_prices(store, name, **kwargs):
                self.assertEqual("NEW", name, "fully superseded peer prices opened")
                loaded.append(name)
                return load(store, name, **kwargs)

            def new_prefix(inputs, reports, **kwargs):
                self.assertEqual({"NEW"}, set(inputs))
                for report in reports.values():
                    for segment in report["correlated"]["segments"]:
                        self.assertLess(
                            segment["last_timestamp"][:10], "2024-01-08", "accepted later choice repeated"
                        )
                return choose(inputs, reports, **kwargs)

            def repair():
                return pipeline.run_lineage(
                    store, ["NEW"], identity_client=client, evidence_client=evidence_client, context=context
                )

            with (
                patch.object(pipeline, "prepare_group_evidence_and_source_work", side_effect=capture),
                patch.object(pipeline, "load_pricing", side_effect=selected_prices),
                patch.object(observations, "_select_new_observations", side_effect=new_prefix),
                patch.object(
                    transform, "correlate", side_effect=AssertionError("accepted row cascade repeated")
                ),
                patch.object(
                    public_common_continuation, "certify_group", wraps=public_common_continuation.certify_group
                ) as certify,
            ):
                if interruption:

                    class Interrupted(BaseException):
                        pass

                    with (
                        patch.object(storage, "publish", side_effect=Interrupted()),
                        self.assertRaises(Interrupted),
                    ):
                        repair()
                    with ExitStack() as guards:
                        for owner, name in (
                            (public_common_continuation, "certify_group"),
                            (documented_refinement, "refine"),
                            (observations, "_select_new_observations"),
                            (pipeline, "load_pricing"),
                            (source_work, "project_closed_source"),
                            (transform, "make_publication"),
                            (storage, "selected_document"),
                        ):
                            guards.enter_context(
                                patch.object(
                                    owner,
                                    name,
                                    side_effect=AssertionError("completed producer repeated: " + name),
                                )
                            )
                        result = repair()
                else:
                    result = repair()
                self.assertEqual(1, certify.call_count)
            self.assertEqual(0, result["failed"], result)
            self.assertEqual(["NEW"], loaded)
            self.assertTrue(all(plan.dispositions == {"NEW": "B", "OLD": "A"} for plan in plans))
            current = {
                name: storage.read_receipt(root, name, storage.read_index(root)) for name in ("OLD", "NEW")
            }
            self.assertEqual(0, current["NEW"]["unresolved_rows"])
            self.assertIsNone(current["OLD"]["membership_only_rebind"])
            self.assertEqual(3, current["NEW"]["membership_only_rebind"]["version"])
            public_id = current["NEW"]["identity_memberships"][0]["security_id"]
            for name, receipt in current.items():
                for kind, document in source_work.assignment_documents(root, receipt, {}).items():
                    for entry in document["adjustments"]:
                        for segment in entry["segments"]:
                            if kind == "current_year" and segment["from"][:7] >= receipt["anchor_month"]:
                                continue
                            self.assertEqual(public_id, segment["security_identity"]["security_id"])
                            self.assertEqual(public_id, segment["canonical_security"]["security_id"])
                            self.assertEqual(
                                "superseded" if name == "OLD" else "selected", segment["observation_selection"]
                            )
                            expected_cusip = (
                                "111111111"
                                if name == "OLD"
                                else "333333333"
                                if segment["from"] >= "2024-01-10"
                                else "222222222"
                                if segment["from"] >= "2024-01-08"
                                else "111111111"
                            )
                            self.assertEqual(expected_cusip, segment["canonical_security"]["cusip"])
            check = policy._bar_reasons

            def fresh_only(bar, adjustment):
                self.assertLess(bar["session"], "2024-01-08", "accepted later numerical outcome repeated")
                return check(bar, adjustment)

            with (
                patch.object(policy, "_bar_reasons", side_effect=fresh_only),
                patch.object(
                    standalone,
                    "read_artifact",
                    side_effect=AssertionError("held NEW or unselected OLD prices reopened"),
                ),
            ):
                served = maintain(store)
                self.assertTrue(served["complete"], served)
                self.assertEqual(32, served["checked_bars"])
                library = DataLibrary(store=store)
                securities = library.list_securities()
                self.assertEqual(1, len(securities))
                security = securities[0]
                self.assertEqual({"111111111", "222222222", "333333333"}, set(security.cusips))
                self.assertEqual(8, len(library.get_history(security.public_id, "all").bars))
                exported = library.write_csv_export(
                    Path(directory) / "export", public_ids=[security.public_id], workers=1
                )
            self.assertEqual(8, exported["rows"])
            manifest = json.loads(Path(exported["manifest"]).read_text())
            entry = manifest["identities"][security.public_id]
            digest = json.loads((Path(exported["output"]) / entry["digest"]).read_text())
            self.assertEqual(
                [security.public_id], [item.public_id for item in library.resolve_ticker("OLD", on="2024-01-05")]
            )
            self.assertEqual((), library.resolve_ticker("OLD", on="2024-01-08"))
            self.assertEqual(
                [security.public_id], [item.public_id for item in library.resolve_ticker("NEW", on="2024-01-08")]
            )
            self.assertEqual(2, len(digest["security"]["ticker_ranges"]))


class DocumentedContinuityAuthorityTests(TestCase):
    def test_shared_entry_does_not_drop_a_component_or_recalculate_an_accepted_containing_component(self):
        narrow = security_identity_for_alpaca_assets(
            ["a", "b"], [], tickers=["OLD", "NEW"], basis="earlier_scope"
        ).as_report_object()
        whole = security_identity_for_alpaca_assets(
            ["a", "b", "c"], [], tickers=["OLD", "NEW"], basis="completed_containing_scope"
        ).as_report_object()
        prior = {"a": narrow, "b": narrow, "c": whole}
        self.assertEqual(prior, source_work.compose_memberships(prior, []))
        edge = SimpleNamespace(
            predecessor_asset_id="a", successor_asset_id="b", predecessor_symbol="OLD", successor_symbol="NEW"
        )
        with patch.object(
            source_work_contract,
            "security_identity_for_alpaca_assets",
            side_effect=AssertionError("completed public ID recalculated"),
        ):
            selected = source_work.compose_memberships(prior, [edge])
        self.assertTrue(all(value is whole for value in selected.values()))

    def test_accepted_continuation_does_not_repeat_certification_or_membership_calculation(self):
        fact = accept_supplements(continuity_facts())[0]
        evidence = {"sourced_public_common_continuations": {"schema_version": 1, "facts": [fact]}}
        certificate = {"source_fact_id": fact["match"]["source_id"], "accepted": "prior-proof"}
        controls = {
            "OLD": SimpleNamespace(
                previous={
                    "evidence": evidence,
                    "public_common_continuation_membership": {
                        "version": 1,
                        "certificates": [certificate],
                        "issues": [],
                    },
                }
            )
        }
        with (
            patch.object(
                public_common_continuation,
                "certify_group",
                side_effect=AssertionError("accepted proof repeated"),
            ),
            patch.object(
                source_work,
                "compose_memberships",
                side_effect=AssertionError("accepted membership calculated again"),
            ),
        ):
            common, members = documented_continuity.bind(controls, {"OLD": evidence}, {})
        self.assertEqual([certificate], common["certificates"])
        self.assertIsNone(members)

    def test_actual_changed_accepted_fact_requires_owning_reconciliation(self):
        before = continuity_facts()[0]
        after = deepcopy(before)
        after["facts"]["legal_effective_date"] = "2024-01-04"
        left = {
            "sourced_public_common_continuations": {
                "schema_version": 1,
                "facts": [accept_supplements([before])[0]],
            }
        }
        right = {
            "sourced_public_common_continuations": {
                "schema_version": 1,
                "facts": [accept_supplements([after])[0]],
            }
        }
        controls = {"OLD": SimpleNamespace(previous={"evidence": left})}
        with self.assertRaisesRegex(LineageError, "changed accepted public continuation"):
            documented_continuity.bind(controls, {"OLD": right}, {})

    def test_conflicting_new_assertions_do_not_select_the_last_peer(self):
        before = continuity_facts()[0]
        after = deepcopy(before)
        after["facts"]["legal_effective_date"] = "2024-01-04"
        controls = {"OLD": SimpleNamespace(previous={"evidence": {}})}
        evidence = {
            name: {
                "sourced_public_common_continuations": {
                    "schema_version": 1,
                    "facts": [accept_supplements([fact])[0]],
                }
            }
            for name, fact in (("OLD", before), ("NEW", after))
        }
        with patch.object(
            public_common_continuation,
            "certify_group",
            side_effect=AssertionError("conflicting assertions reached certification"),
        ):
            with self.assertRaisesRegex(LineageError, "conflicting public continuation assertions"):
                documented_continuity.bind(controls, evidence, {})

    def test_frozen_policy_cannot_execute_new_refinement_plan(self):
        frozen = load_policy("resources/alpaca_security_correlation_v90_legacy_policy.yaml")
        plan = source_work.SourceWorkPlan(
            {}, {}, {}, {}, {}, {}, {}, False, refine_accepted_dates=True, refinement_policy_version=91
        )
        with self.assertRaisesRegex(LineageError, "policy 91"):
            source_work.execute_source_work(
                None,
                None,
                {},
                {},
                {},
                plan,
                frozen,
                {},
                loader=lambda *a, **k: self.fail("frozen source work opened prices"),
            )

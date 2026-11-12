"""Actual payload-free metadata closure, publication and serving transfer."""

import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations
from test_alpaca_universe_update import CatalogueClient

from alpaca_end_of_day import DataLibrary, standalone
from libs.alpaca_end_of_day_security_correlation import (
    public_common_continuation,
    security_identity,
    security_relations,
)
from libs.alpaca_lineage_build import pipeline, pricing, source_work, sourced_rules, storage, universe_update
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_pricing_download.contract import timestamp
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.http import BarsClient
from libs.alpaca_serving_catalogue import policy as serving_policy
from libs.alpaca_serving_catalogue import reuse as serving_reuse
from libs.alpaca_serving_catalogue import storage as serving_storage
from libs.alpaca_serving_catalogue import work as serving_work
from libs.alpaca_serving_catalogue.producer import maintain
from tests.test_alpaca_public_common_continuation import common_fact, parent_fact
from tests.test_alpaca_public_common_continuation import evidence as common_evidence
from tests.test_alpaca_security_relations import conversion_fact
from tests.test_alpaca_security_relations import evidence as relation_evidence


class PublicCommonPipelineTests(unittest.TestCase):
    def test_new_public_parent_joins_common_donors_without_reopening_accepted_values(self):
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted common prices opened")),
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted common values reread")),
            patch.object(
                serving_policy, "_bar_reasons", side_effect=AssertionError("accepted numbers rechecked")
            ),
        ):
            result = pipeline.run_lineage(
                self.store,
                ["BUSINESS"],
                identity_client=IdentityClient(self.assets),
                event_supplements=[parent_fact()],
            )
            self.assertEqual(0, result["failed"], result)
            served = maintain(self.store)
        self.assertTrue(served["complete"], served)
        self.assertEqual(0, served["checked_bars"])
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

    def test_daily_update_itself_publishes_scoped_rule_facts_and_skips_completed_application(self):
        registry = self.store.parent / "ticker.csv"
        registry.write_text("BUSINESS,SPAC\n")
        from libs.alpaca_lineage_build.check_summary import rebuild

        self.assertEqual(
            0, pipeline.run_lineage(self.store, identity_client=IdentityClient(self.assets))["failed"]
        )
        rebuild(self.store)
        universe_update.rebuild_universe(self.store)
        facts = self.store.parent / "rule-facts.json"
        facts.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "applications": [
                        {"id": "public-common-test", "symbols": ["SPAC"], "supplements": [common_fact()]}
                    ],
                }
            )
        )
        identity = CatalogueClient([self.assets["SPAC"], self.assets["BUSINESS"]], exact=self.assets)
        client = BarsClient("fake", "fake")
        rows = {
            "SPAC": informative_bars("2024-01-02", "2024-01-05"),
            "BUSINESS": informative_bars("2024-01-05", "2024-01-09"),
        }

        def response(params):
            return {
                "bars": {
                    name: [
                        row
                        for row in rows[name]
                        if timestamp(params["start"]) <= timestamp(row["t"]) <= timestamp(params["end"])
                    ]
                    for name in params["symbols"].split(",")
                }
            }

        with (
            patch.object(
                universe_update, "load_applications", side_effect=lambda: sourced_rules.load_applications(facts)
            ),
            patch.object(client, "_get", side_effect=response),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("accepted prices materialized")),
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted serving values replayed")),
        ):
            result = universe_update.run_update(
                self.store, "2024-01-09", pricing_client=client, identity_client=identity, registry=registry
            )
            self.assertTrue(result["complete"], result)
            self.assertEqual(0, result["pending_work"])
            index = storage.read_index(self.root)
            marker = index["symbols"]["SPAC"]["projection"]["sourced_fact_applications"]
            self.assertEqual(["public-common-test"], list(marker))
            self.assertEqual(1, DataLibrary(store=self.store).online_count())
            with (
                patch.object(
                    storage, "read_receipt", side_effect=AssertionError("completed fact receipt reopened")
                ),
                patch.object(
                    public_common_continuation,
                    "certify_group",
                    side_effect=AssertionError("completed fact recertified"),
                ),
            ):
                repeated = universe_update.run_update(
                    self.store, "2024-01-09", pricing_client=client, identity_client=identity, registry=registry
                )
            self.assertTrue(repeated["complete"], repeated)
            self.assertNotIn("SPAC", repeated["plan"]["work"])
            document = json.loads(facts.read_text())
            document["applications"][0]["id"] = "same-facts-new-application"
            facts.write_text(json.dumps(document))
            with patch.object(
                public_common_continuation, "certify_group", side_effect=AssertionError("accepted fact replay")
            ):
                reprojected = universe_update.run_update(
                    self.store, "2024-01-09", pricing_client=client, identity_client=identity, registry=registry
                )
            self.assertTrue(reprojected["complete"], reprojected)
        from libs.alpaca_lineage_build.work_contract import satisfied

        required = {
            "required_sourced_facts": {"public-common-test": "changed-input"},
            "obligation_id": "new-obligation",
            "end_exclusive": "2024-01-09",
        }
        self.assertFalse(satisfied(required, index["symbols"]["SPAC"], False, approved=True))

    def test_new_source_unknown_count_control_publishes_real_nonzero_owners(self):
        rows = informative_bars("2024-01-02", "2024-01-09")
        download_pricing(self.store, "FRESH", "2024-01-09", from_date="2024-01-01", client=FakeClient(rows))
        self.assets["FRESH"] = self.assets["777777777"] = asset("FRESH", "fresh-id")
        fact = conversion_fact()
        fact["facts"]["successor"].update(symbol="FRESH", cusip="777777777")
        fact["facts"]["first_successor_trading"]["symbol"] = "FRESH"
        fact["facts"]["consideration"][0]["recipient_cusip"] = "777777777"
        result = pipeline.run_lineage(
            self.store, ["FRESH"], identity_client=IdentityClient(self.assets), event_supplements=[fact]
        )
        self.assertEqual(0, result["failed"], result)
        receipt = storage.read_receipt(self.root, "FRESH", storage.read_index(self.root))
        self.assertEqual(8 * len(rows), receipt["source_rows"])
        self.assertEqual(receipt["source_rows"], receipt["correlated_rows"])
        self.assertEqual(["fresh-id"], receipt["identity_memberships"][0]["member_alpaca_asset_ids"])

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.assets = {"SPAC": asset("SPAC", "predecessor-id"), "BUSINESS": asset("BUSINESS", "successor-id")}
        self.assets.update({"111111111": self.assets["SPAC"], "222222222": self.assets["BUSINESS"]})
        negative = self._testMethodName == "test_disjoint_negative_donor_preserves_unaligned_view_capability"

        class UnalignedDonor(FakeClient):
            def fetch(self, symbol, adjustment, *args, **kwargs):
                rows = super().fetch(symbol, adjustment, *args, **kwargs)
                return [row for row in rows if row["t"][:10] != "2024-01-03"] if adjustment == "raw" else rows

        overlap = self._testMethodName in {
            "test_changed_owner_uses_only_selected_frozen_counterpart",
            "test_negative_partial_overlap_stays_pending_without_old_history_replay",
            "test_actual_disjoint_append_keeps_common_and_unresolved_sources_closed",
            "test_prepublication_peer_failure_keeps_owner_and_metadata_handoffs",
        }
        partial = (
            self._testMethodName == "test_negative_partial_overlap_stays_pending_without_old_history_replay"
        )
        for name, first, end in (
            ("SPAC", "2024-01-02", "2024-01-05"),
            ("BUSINESS", "2024-01-03" if partial else "2024-01-02" if overlap else "2024-01-05", "2024-01-09"),
        ):
            client = UnalignedDonor if negative and name == "SPAC" else FakeClient
            bars = informative_bars(first, end)
            if partial and name == "SPAC":
                bars[0]["h"] = 1
            with direct_adjustment_observations():
                download_pricing(self.store, name, "2024-01-09", from_date="2024-01-01", client=client(bars))
            result = pipeline.run_lineage(self.store, [name], identity_client=IdentityClient(self.assets))
            self.assertEqual(0, result["failed"], result)
        self.assertEqual(0, maintain(self.store, bootstrap=True)["failed"])

    def test_owner_plan_retains_foreign_additions_but_invalidates_real_changes(self):
        index = storage.read_index(self.root)
        controls, evidence = {}, {}
        extension = common_evidence()
        for name in ("SPAC", "BUSINESS"):
            prior = storage.read_receipt(self.root, name, index)
            source = pricing.load_source(self.store / "end_of_day/pricing" / name, name)
            controls[name] = pricing.metadata_source_control(
                self.store, name, source, prior["source_sha256"], prior
            )
            evidence[name] = {
                **copy.deepcopy(prior["evidence"]),
                "sourced_public_common_continuations": extension["sourced_public_common_continuations"],
            }
            evidence[name]["asset_lookups"].extend(copy.deepcopy(extension["asset_lookups"]))
        foreign = {
            "id": "foreign-event",
            "action_type": "stock_merger",
            "acquiree_symbol": "TARGET",
            "acquiree_cusip": "999999999",
            "acquirer_symbol": "OTHER",
            "acquirer_cusip": "888888888",
            "process_date": "2024-01-03",
        }
        base = evidence["SPAC"]
        base["corporate_actions"] = [foreign]
        base["corporate_action_count"] = 1
        base["unresolved_cusips"] = ["999999999"]
        base["symbol_occupancy_evidence"] = [
            {"symbol": "OTHER", "cusip": "888888888", "asset_id": "foreign-id", "evidence": [foreign]}
        ]
        cases = (
            "foreign_addition",
            "nested_own_action",
            "own_unresolved_occupancy",
            "changed_mapping",
            "changed_generation",
            "unchanged_ancestry",
            "changed_pinned_mapping",
            "removed_old_action",
            "changed_old_action",
        )
        for case in cases:
            with self.subTest(case=case):
                selected, observed = copy.deepcopy(controls), copy.deepcopy(evidence)
                owner = selected["SPAC"]
                facts = observed["SPAC"]
                if case == "nested_own_action":
                    facts["corporate_actions"][0] = {
                        **foreign,
                        "recipient": {"symbol": "SPAC", "cusip": "111111111"},
                    }
                elif case == "own_unresolved_occupancy":
                    facts["symbol_occupancy_evidence"].append(
                        {"symbol": "SPAC", "cusip": "777777777", "asset_id": None, "evidence": []}
                    )
                elif case == "changed_mapping":
                    facts["asset_lookups"][0]["asset"]["id"] = "replacement-id"
                elif case in {"changed_generation", "unchanged_ancestry"}:
                    owner.source_sha256 = "new-selected-generation"
                    if case == "unchanged_ancestry":
                        owner.source["history_changes"] = [
                            {"source_sha256": owner.previous["source_sha256"], "changed_from": None}
                        ]
                elif case == "changed_pinned_mapping":
                    owner.source["latest_request"]["asof"] = "2024-01-03"
                elif case in {"removed_old_action", "changed_old_action"}:
                    owner.previous["evidence"]["corporate_actions"] = [copy.deepcopy(foreign)]
                    owner.previous["evidence"]["corporate_action_count"] = 1
                    if case == "removed_old_action":
                        facts["corporate_actions"] = []
                        facts["corporate_action_count"] = 0
                    else:
                        facts["corporate_actions"][0]["acquirer_cusip"] = "777777777"
                plan = source_work.prepare_group_evidence_and_source_work(selected, observed, {}, cache={})
                self.assertEqual(
                    {
                        "SPAC": "A" if case in {"foreign_addition", "unchanged_ancestry"} else "C",
                        "BUSINESS": "A",
                    },
                    plan.dispositions,
                )

    def test_post_union_foreign_label_does_not_reopen_predecessor_owner(self):
        result = pipeline.run_lineage(
            self.store,
            ["BUSINESS"],
            identity_client=IdentityClient(self.assets),
            event_supplements=[common_fact()],
        )
        self.assertEqual(0, result["failed"], result)
        index = storage.read_index(self.root)
        prior = storage.read_receipt(self.root, "SPAC", index)
        observed = copy.deepcopy(prior["evidence"])
        observed["corporate_actions"].append(
            {
                "id": "later-peer-event",
                "action_type": "stock_merger",
                "acquirer_symbol": "BUSINESS",
                "acquirer_cusip": "222222222",
                "acquiree_symbol": "FOREIGN",
                "acquiree_cusip": "999999999",
                "process_date": "2024-01-03",
            }
        )
        observed["corporate_action_count"] += 1
        observed["provider_corporate_actions"] = copy.deepcopy(observed["corporate_actions"])
        self.assertTrue(source_work._accepted_owners_apply(prior, observed))
        controls, evidence = {}, {}
        for name in ("SPAC", "BUSINESS"):
            receipt = prior if name == "SPAC" else storage.read_receipt(self.root, name, index)
            source = pricing.load_source(self.store / "end_of_day/pricing" / name, name)
            controls[name] = pricing.metadata_source_control(
                self.store, name, source, receipt["source_sha256"], receipt
            )
            evidence[name] = observed if name == "SPAC" else receipt["evidence"]
        plan = source_work.prepare_group_evidence_and_source_work(controls, evidence, {}, cache={})
        self.assertEqual({"SPAC": "retained", "BUSINESS": "retained"}, plan.dispositions)
        observed["corporate_actions"][-1]["acquiree_symbol"] = "SPAC"
        observed["provider_corporate_actions"] = copy.deepcopy(observed["corporate_actions"])
        self.assertFalse(source_work._accepted_owners_apply(prior, observed))
        plan = source_work.prepare_group_evidence_and_source_work(controls, evidence, {}, cache={})
        self.assertEqual({"SPAC": "C", "BUSINESS": "retained"}, plan.dispositions)

    def test_prepublication_peer_failure_keeps_owner_and_metadata_handoffs(self):
        load = pipeline.load_pricing
        loaded = []

        def fail_once(store, name, **kwargs):
            self.assertEqual("BUSINESS", name, "accepted predecessor pricing reopened")
            loaded.append(name)
            if len(loaded) == 1:
                raise LineageError("selected peer interrupted before publication")
            return load(store, name, **kwargs)

        with (
            patch.object(pipeline, "load_pricing", side_effect=fail_once),
            patch.object(
                public_common_continuation, "certify_group", wraps=public_common_continuation.certify_group
            ) as certify,
            patch.object(security_relations, "project", wraps=security_relations.project) as project,
        ):
            result = pipeline.run_lineage(
                self.store,
                ["BUSINESS", "SPAC"],
                identity_client=IdentityClient(self.assets),
                event_supplements=[common_fact()],
            )
        self.assertEqual(1, result["failed"], result)
        self.assertEqual(["BUSINESS", "BUSINESS"], loaded)
        self.assertEqual(1, certify.call_count)
        self.assertEqual(1, project.call_count)
        receipt = storage.read_receipt(self.root, "SPAC", storage.read_index(self.root))
        self.assertEqual(24, receipt["correlated_rows"])
        self.assertEqual(0, receipt["unresolved_rows"])

    def test_membership_projection_has_real_counts_without_loading_old_prices(self):
        before = {
            name: storage.read_receipt(self.root, name, storage.read_index(self.root))
            for name in ("SPAC", "BUSINESS")
        }
        with (
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("closed pricing loaded")),
            patch.object(pricing, "load_pricing", side_effect=AssertionError("closed pricing loaded")),
            patch.object(
                pricing, "load_published_boundary", side_effect=AssertionError("closed boundary loaded")
            ),
            patch.object(pricing, "load_published_views", side_effect=AssertionError("closed history loaded")),
            patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing provider")),
            patch.object(
                public_common_continuation, "source_history", side_effect=AssertionError("old source proof")
            ),
            patch.object(security_relations, "project", wraps=security_relations.project) as projection,
        ):
            result = pipeline.run_lineage(
                self.store,
                ["BUSINESS"],
                identity_client=IdentityClient(self.assets),
                event_supplements=[common_fact()],
            )
        self.assertEqual(0, result["failed"], result)
        self.assertEqual(1, projection.call_count)
        after = {name: storage.read_receipt(self.root, name, storage.read_index(self.root)) for name in before}
        self.assertEqual(
            {name: item["source_rows"] for name, item in before.items()},
            {name: item["source_rows"] for name, item in after.items()},
        )
        self.assertTrue(all(item["membership_only_rebind"] for item in after.values()))
        expected = security_identity.security_id_for_alpaca_asset_ids(["predecessor-id", "successor-id"])
        self.assertEqual(
            {expected},
            {identity["security_id"] for item in after.values() for identity in item["identity_memberships"]},
        )
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("old serving payload")),
            patch.object(serving_policy, "_bar_reasons", side_effect=AssertionError("old numeric check")),
            patch.object(
                security_identity,
                "security_id_for_alpaca_asset_ids",
                side_effect=AssertionError("old public ID rehash"),
            ),
        ):
            served = maintain(self.store)
        self.assertEqual(0, served["failed"], served)
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

    def test_negative_partial_overlap_stays_pending_without_old_history_replay(self):
        before = serving_storage.controls(self.store)
        before_groups = serving_storage.read_groups(
            self.store, before["groups"], generation=before["state"]["generation"]
        )
        result = pipeline.run_lineage(
            self.store,
            ["BUSINESS"],
            identity_client=IdentityClient(self.assets),
            event_supplements=[common_fact()],
        )
        self.assertEqual(0, result["failed"], result)
        pending = serving_work.read(self.store)
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("old history replay")),
            patch.object(serving_policy, "_bar_reasons", side_effect=AssertionError("old numerical replay")),
        ):
            result = maintain(self.store)
        self.assertEqual(1, result["failed"], result)
        self.assertIn("bounded serving projection pending", str(result))
        self.assertIn("retained_date_scopes=[('2024-01-05', '2024-01-08')]", str(result))
        self.assertIn("arbitrary-run recovery is unavailable", str(result))
        self.assertEqual(pending, serving_work.read(self.store))
        after = serving_storage.controls(self.store)
        after_groups = serving_storage.read_groups(
            self.store, after["groups"], generation=after["state"]["generation"]
        )
        self.assertEqual(before_groups, after_groups)

    def test_required_receipt_envelopes_fail_at_selected_ingress_once(self):
        result = pipeline.run_lineage(
            self.store,
            ["BUSINESS"],
            identity_client=IdentityClient(self.assets),
            event_supplements=[common_fact()],
        )
        self.assertEqual(0, result["failed"], result)
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "SPAC", index)
        receipt["public_common_continuation_ownership"] = {
            "version": 1,
            "symbol": "SPAC",
            "source_fact_id": "public-holder-transition",
            "route": [],
            "first": "2024-01-02",
            "last": "2024-01-04",
        }
        receipt["security_relations"] = security_relations.project(
            security_relations.certify_group([relation_evidence()]), {}
        )
        paths = [
            ("public_common_continuation_membership", "certificates", 0),
            ("public_common_continuation_ownership",),
            ("security_relations", "relations", 0),
            ("membership_only_rebind",),
            ("membership_only_rebind", "runs", 0),
        ]

        def at(document, path):
            for key in path:
                document = document[key]
            return document

        with (
            patch.object(
                public_common_continuation,
                "certify_group",
                side_effect=AssertionError("repeated membership proof"),
            ),
            patch.object(
                security_relations, "certify_group", side_effect=AssertionError("repeated relation proof")
            ),
        ):
            storage.read_receipt(self.root, "SPAC", index, selected_loader=lambda *args: receipt)
            for path in paths:
                for key in at(receipt, path):
                    with self.subTest(path=path, key=key):
                        altered = copy.deepcopy(receipt)
                        del at(altered, path)[key]
                        with self.assertRaisesRegex(LineageError, "owning lineage recovery"):
                            storage.read_receipt(self.root, "SPAC", index, selected_loader=lambda *args: altered)

    def test_public_relation_projection_all_evidence_paths_and_mutation_isolation(self):
        fact = conversion_fact()
        fact["facts"]["successor"]["symbol"] = "SPAC"
        fact["facts"]["first_successor_trading"]["symbol"] = "SPAC"
        result = pipeline.run_lineage(
            self.store, ["SPAC"], identity_client=IdentityClient(self.assets), event_supplements=[fact]
        )
        self.assertEqual(0, result["failed"], result)
        reader = standalone.StandaloneReader(self.store)
        with (
            patch.object(
                security_relations, "certify_group", side_effect=AssertionError("consumer relation proof")
            ),
            patch.object(
                security_relations, "project", side_effect=AssertionError("consumer relation projection")
            ),
        ):
            evidence = reader.get_evidence("SPAC")
            relation = evidence["security_relations"]["relations"][0]
            self.assertIsNone(relation["predecessor"]["asset_id"])
            self.assertIsNone(relation["predecessor"]["security_id"])
            self.assertIsNotNone(relation["successor"]["security_id"])
            expected = copy.deepcopy(evidence["security_relations"])
            evidence["security_relations"]["relations"].clear()
            self.assertEqual(expected, reader.get_evidence("SPAC")["security_relations"])
            with reader.open_group("SPAC") as group:
                self.assertEqual(expected, group.evidence["security_relations"])
                changed = group.evidence
                changed["security_relations"]["relations"].clear()
                self.assertEqual(expected, group.evidence["security_relations"])
            included = reader.get_histories("SPAC", include_evidence=True)
            self.assertEqual(expected, included["evidence"]["security_relations"])
            with reader.open_published_evidence() as selected:
                groups = list(selected.iter_groups())
            self.assertIn(expected, [group["security_relations"] for group in groups])

    def test_actual_disjoint_append_keeps_common_and_unresolved_sources_closed(self):
        result = pipeline.run_lineage(
            self.store,
            ["BUSINESS"],
            identity_client=IdentityClient(self.assets),
            event_supplements=[common_fact()],
        )
        self.assertEqual(0, result["failed"], result)
        peer_rows = informative_bars("2024-01-02", "2024-01-09")
        download_pricing(self.store, "PEER", "2024-01-09", from_date="2024-01-01", client=FakeClient(peer_rows))
        result = pipeline.run_lineage(self.store, ["PEER"], identity_client=IdentityClient(self.assets))
        self.assertEqual(0, result["failed"], result)
        index = storage.read_index(self.root)
        receipts = {name: storage.read_receipt(self.root, name, index) for name in ("SPAC", "BUSINESS", "PEER")}
        self.assertGreater(receipts["PEER"]["unresolved_rows"], 0)
        generations = {name: receipt["source_sha256"] for name, receipt in receipts.items()}
        for name, receipt in receipts.items():
            receipt["input_sources"] = generations
            receipt["dependencies"] = sorted(set(receipts) - {name})
        with storage.pricing_locks(self.store, receipts):
            storage.publish(self.store, self.root, index, receipts, {name: {} for name in receipts}, {})
        for name, first, end in (
            ("SPAC", "2024-01-02", "2024-01-05"),
            ("BUSINESS", "2024-01-02", "2024-01-10"),
            ("PEER", "2024-01-02", "2024-01-10"),
        ):
            download_pricing(self.store, name, "2024-01-10", client=FakeClient(informative_bars(first, end)))
        original = pipeline.load_pricing
        loads = []

        def load(store, name, **kwargs):
            loads.append((name, kwargs.get("resume_from")))
            self.assertIsNotNone(kwargs.get("resume_from"), "accepted peer full loader")
            return original(store, name, **kwargs)

        history = public_common_continuation.source_history

        def source_history(name, *args, **kwargs):
            self.assertNotEqual("BUSINESS", name, "accepted common source proof repeated")
            return history(name, *args, **kwargs)

        with (
            patch.object(pipeline, "load_pricing", side_effect=load),
            patch.object(public_common_continuation, "source_history", side_effect=source_history),
        ):
            result = pipeline.run_lineage(self.store, ["BUSINESS"], identity_client=IdentityClient(self.assets))
        self.assertEqual(0, result["failed"], result)
        self.assertIn(("PEER", "2024-01-09"), loads)
        after = storage.read_receipt(self.root, "BUSINESS", storage.read_index(self.root))
        self.assertEqual(
            receipts["BUSINESS"]["public_common_continuation_ownership"],
            after["public_common_continuation_ownership"],
        )

    def test_lineage_journal_and_serving_commit_interruption_keep_completed_handoffs(self):
        class Interrupted(BaseException):
            pass

        with patch.object(storage, "_install", side_effect=Interrupted()), self.assertRaises(Interrupted):
            pipeline.run_lineage(
                self.store,
                ["BUSINESS"],
                identity_client=IdentityClient(self.assets),
                event_supplements=[common_fact()],
            )
        self.assertTrue((self.root / ".pending.json").exists())
        with (
            patch.object(
                public_common_continuation,
                "certify_group",
                side_effect=AssertionError("recovery membership proof"),
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("recovery old history")),
        ):
            result = pipeline.run_lineage(self.store, ["BUSINESS"], identity_client=IdentityClient(self.assets))
        self.assertEqual(0, result["failed"], result)
        self.assertFalse((self.root / ".pending.json").exists())
        with (
            patch.object(serving_work, "acknowledge", side_effect=Interrupted()),
            self.assertRaises(Interrupted),
        ):
            maintain(self.store)
        with (
            patch.object(
                serving_reuse,
                "compose_membership_rebind",
                side_effect=AssertionError("completed serving composition"),
            ),
            patch.object(
                standalone.StandaloneReader,
                "_open_group",
                side_effect=AssertionError("completed serving inputs"),
            ),
            patch.object(
                serving_policy, "_bar_reasons", side_effect=AssertionError("completed numerical checks")
            ),
        ):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(1, result["reused_groups"])

    def test_disjoint_negative_donor_preserves_unaligned_view_capability(self):
        result = pipeline.run_lineage(
            self.store,
            ["BUSINESS"],
            identity_client=IdentityClient(self.assets),
            event_supplements=[common_fact()],
        )
        self.assertEqual(0, result["failed"], result)
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted numerical payload")),
            patch.object(serving_policy, "_bar_reasons", side_effect=AssertionError("accepted numeric check")),
        ):
            served = maintain(self.store)
        self.assertEqual(0, served["failed"], served)
        self.assertEqual(0, DataLibrary(store=self.store).online_count())
        controls = serving_storage.controls(self.store)
        groups = serving_storage.read_groups(
            self.store, set(controls["members"].values()), generation=controls["state"]["generation"]
        )
        self.assertEqual(1, len(groups))
        completed = next(iter(groups.values()))
        self.assertIn("incompatible_sessions", completed["decisions"][0]["reasons"])
        record = completed["history_validation"]["prefix_records"][0]
        self.assertEqual(["2024-01-02", "2024-01-04", "2024-01-05", "2024-01-08"], record["session_dates"])

    def test_changed_owner_uses_only_selected_frozen_counterpart(self):
        original = pipeline.load_pricing
        loaded = []

        def load(store, name, **kwargs):
            self.assertEqual("BUSINESS", name)
            loaded.append(name)
            return original(store, name, **kwargs)

        with patch.object(pipeline, "load_pricing", side_effect=load):
            result = pipeline.run_lineage(
                self.store,
                ["BUSINESS"],
                identity_client=IdentityClient(self.assets),
                event_supplements=[common_fact()],
            )
        self.assertEqual(0, result["failed"], result)
        self.assertEqual(["BUSINESS"], loaded)
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "BUSINESS", index)
        self.assertIsNotNone(receipt["public_common_continuation_ownership"])
        documents = storage.read_boundaries(self.root, receipt)
        raw = next(item for item in documents["current_month"]["adjustments"] if item["adjustment"] == "raw")
        self.assertEqual(["SPAC", "BUSINESS"], [segment["historical_symbol"] for segment in raw["segments"]])
        with (
            patch.object(standalone, "_rows", side_effect=AssertionError("accepted overlap history")),
            patch.object(
                serving_policy, "_bar_reasons", side_effect=AssertionError("accepted overlap numeric check")
            ),
        ):
            served = maintain(self.store)
        self.assertEqual(0, served["failed"], served)
        self.assertEqual(1, DataLibrary(store=self.store).online_count())

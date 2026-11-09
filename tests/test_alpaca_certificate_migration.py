"""Version migration preserves decisions through both durable owner boundaries."""

import copy
import unittest
from pathlib import Path
from unittest.mock import patch

import test_alpaca_end_of_day_api as fixture
from test_alpaca_succession_compatibility import retained_policy_runtime

from alpaca_end_of_day import standalone
from libs.alpaca_lineage_build import certificate_migration, pipeline, storage
from libs.alpaca_lineage_build.contract import versions
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_pricing_download.update_checks import complete_checks, read_checks
from libs.alpaca_serving_catalogue import producer, work
from libs.alpaca_serving_catalogue import storage as catalogue


def install_v76(case, *, serving=True, correlation=76):
    """Install a complete synthetic frozen publication, not production relabelling."""
    policy = load_policy(
        Path(__file__).resolve().parents[1]
        / f"resources/alpaca_security_correlation_v{correlation}_legacy_policy.yaml"
    )
    index = storage.read_index(case.root)
    prior_versions = {
        **versions(),
        "correlation": correlation,
        "correlation_policy_sha256": policy.semantic_sha256,
    }
    receipts, entries = {}, {}
    for symbol, entry in index["symbols"].items():
        receipt = copy.deepcopy(storage.read_receipt(case.root, symbol, index))
        receipt.update(versions=prior_versions, correlation_policy=policy.provenance())
        receipts[symbol] = receipt
        entries[symbol] = {
            **entry,
            "sha256": storage.document_digest(receipt),
            "versions": prior_versions,
            "correlation_policy": policy.provenance(),
        }
    journal = {
        "schema_version": 34,
        "correlation_policy": policy.provenance(),
        "retire": [],
        "documents": {f"{s}/lineage.manifest.json": r for s, r in receipts.items()},
        "index_updates": entries,
    }
    storage._install(case.root, journal)
    if serving:
        controls = catalogue.controls(case.store)
        groups = catalogue.read_groups(
            case.store, controls["groups"], generation=controls["state"]["generation"]
        )
        for group in groups.values():
            group["references"]["lineage"] = {
                name: entries[name]["sha256"] if name in entries else None
                for name in group["references"]["lineage"]
            }
        catalogue.commit(case.store, groups.keys(), list(groups.values()))
        work.acknowledge(case.store, work.read(case.store))
    checks = read_checks(case.store)
    complete_checks(case.store, checks["epoch"])
    return receipts


class CertificateMigrationTests(unittest.TestCase):
    def setUp(self):
        # This exact 76/77 -> 79 transition must not follow future production policy.
        retained_policy_runtime(self, computation_revision=2)
        self.case = fixture.NativeCase()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        self.case.bootstrap()
        install_v76(self.case)

    def forbidden_work(self):
        for owner, name in (
            (pipeline, "load_pricing"),
            (pipeline, "correlate"),
            (storage, "read_boundaries"),
            (standalone, "_rows"),
            (producer, "evaluate"),
            (catalogue, "read_groups"),
        ):
            self.enterContext(
                patch.object(owner, name, side_effect=AssertionError(f"migration repeated {name}"))
            )

    def test_v77_header_promotion_and_conservative_related_query_selection(self):
        install_v76(self.case, correlation=77)
        index = storage.read_index(self.case.root)
        receipt = storage.read_receipt(self.case.root, "TEST", index)
        self.assertTrue(certificate_migration.can_promote_group({"TEST": receipt}, {"TEST"}))
        changed = copy.deepcopy(receipt)
        changed["evidence"]["corporate_actions"].append(
            {
                "id": "rename",
                "action_type": "name_change",
                "old_symbol": "EARLIER",
                "new_symbol": "TEST",
            }
        )
        changed["evidence"]["queried_symbols"] = ["EARLIER", "TEST"]
        changed["evidence"]["query"]["corporate_actions_data_quality"] = "complete"
        self.assertIn(
            "completed_related_query_coverage",
            certificate_migration.affected_inputs({"TEST": changed})["reasons"],
        )
        self.forbidden_work()
        self.assertEqual(certificate_migration.migrate(self.case.store)["promoted_groups"], 1)

    def test_exact_header_promotion_retargets_completed_serving_without_payloads(self):
        before = self.case.catalogue_groups("TEST")["TEST"]
        self.forbidden_work()
        preview = certificate_migration.preview(self.case.store)
        self.assertEqual(preview["promotable_groups"], 1)
        self.assertEqual(preview["metadata_receipts"], 0)
        result = certificate_migration.migrate(self.case.store)
        self.assertEqual(result["promoted_groups"], 1)
        self.assertEqual(result["affected_symbols"], [])
        maintained = producer.maintain(self.case.store)
        self.assertTrue(maintained["complete"])
        self.assertEqual(maintained["reused_groups"], 1)
        self.assertEqual(maintained["checked_bars"], 0)
        self.assertEqual(work.read(self.case.store), {})
        self.assertEqual(
            catalogue.controls(self.case.store)["groups"]["TEST"]["decision_generation"],
            before["decision_generation"],
        )
        self.assertEqual(certificate_migration.migrate(self.case.store)["promoted_groups"], 0)

    def test_interruption_after_lineage_and_serving_commits_keeps_exact_handoff(self):
        self.forbidden_work()
        with patch.object(storage, "_acknowledge", side_effect=RuntimeError("stopped after lineage commit")):
            with self.assertRaisesRegex(RuntimeError, "lineage commit"):
                certificate_migration.migrate(self.case.store)
        self.assertTrue((self.case.root / ".pending.json").exists())
        recovered = certificate_migration.migrate(self.case.store)
        self.assertTrue(recovered["recovered_publication"])
        self.assertTrue(work.read_handoffs(self.case.store, work.read(self.case.store)))
        with patch.object(work, "acknowledge", side_effect=RuntimeError("stopped after serving commit")):
            first = producer.maintain(self.case.store)
        self.assertEqual(first["failed"], 1)
        with patch.object(catalogue, "retarget_decision", side_effect=AssertionError("already committed")):
            repeated = producer.maintain(self.case.store)
        self.assertTrue(repeated["complete"])
        self.assertEqual(work.read(self.case.store), {})

    def test_changed_evidence_cannot_issue_unchanged_publication_handoff(self):
        index = storage.read_index(self.case.root)
        prior = storage.read_receipt(self.case.root, "TEST", index)
        from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy

        new = copy.deepcopy(prior)
        new.update(versions=versions(), correlation_policy=load_authorized_policy().provenance())
        new["evidence"]["warnings"].append("materially changed")
        with self.assertRaisesRegex(RuntimeError, "changed serving inputs"):
            storage.publish(
                self.case.store,
                self.case.root,
                index,
                {"TEST": new},
                {"TEST": {}},
                {},
                previous={"TEST": prior},
                unchanged_decision=True,
            )

    def test_exact_transition_expires_and_changed_rule_prerequisites_select_evaluation(self):
        index = storage.read_index(self.case.root)
        receipt = storage.read_receipt(self.case.root, "TEST", index)
        self.assertTrue(certificate_migration.can_promote_group({"TEST": receipt}, {"TEST"}))
        changed = copy.deepcopy(receipt)
        changed["versions"]["correlation"] = 75
        self.assertFalse(certificate_migration.can_promote_group({"TEST": changed}, {"TEST"}))
        changed = copy.deepcopy(receipt)
        changed["evidence"]["corporate_actions"].append(
            {
                "id": "handoff",
                "action_type": "name_change",
                "old_symbol": "TEST",
                "new_symbol": "TEST",
                "old_cusip": "old",
                "new_cusip": "new",
            }
        )
        changed["evidence"].setdefault("symbol_occupancy_evidence", []).append(
            {"symbol": "TEST", "asset_id": None}
        )
        # Missing endpoints cannot execute this certificate; ambiguity alone
        # does not authorize an otherwise unnecessary policy replay.
        self.assertTrue(certificate_migration.can_promote_group({"TEST": changed}, {"TEST"}))
        changed["evidence"]["asset_lookups"].extend(
            {"lookup_kind": "cusip", "lookup": cusip, "asset": {"id": identity, "symbol": "TEST"}}
            for cusip, identity in (("old", "prior-id"), ("new", "TEST-id"))
        )
        self.assertFalse(certificate_migration.can_promote_group({"TEST": changed}, {"TEST"}))

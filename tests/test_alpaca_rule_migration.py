"""A version migration publishes its proof once without redoing unaffected work."""

from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import snapshot

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.check_summary import rebuild as rebuild_check_summary
from libs.alpaca_lineage_build.contract import LineageError, versions
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy, load_policy
from libs.alpaca_lineage_build.migration import (
    can_promote_observation_decision,
    migrate_correlation_policy,
)
from libs.alpaca_lineage_build.pipeline import run_lineage
from libs.alpaca_pricing_download.check_summary import invalidate, read_status
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_pricing_download.update_checks import complete_checks, read_checks


class RuleMigrationProofTests(TestCase):
    def setUp(self):
        self.policy = load_authorized_policy()

    def receipt(self, *, schema_version=33, lineage_version=33, correlation_version=59):
        recorded_versions = {
            **versions(),
            "lineage": lineage_version,
            "correlation": correlation_version,
        }
        recorded_versions.pop("correlation_policy_sha256", None)
        return {
            "schema_version": schema_version,
            "versions": recorded_versions,
            "symbol": "TEST",
            "asset_ids": ["id"],
            "unresolved_rows": 0,
        }

    def promotable(self, receipt):
        return can_promote_observation_decision(receipt, self.policy)

    @staticmethod
    def asset(asset_id, symbol, *, active=True):
        return {
            "id": asset_id,
            "symbol": symbol,
            "class": "us_equity",
            "exchange": "NYSE",
            "status": "active" if active else "inactive",
            "tradable": active,
        }

    @staticmethod
    def lookup(kind, value, selected_asset):
        return {
            "lookup_kind": kind,
            "lookup": value,
            "found": True,
            "outcome": "found",
            "http_status": 200,
            "asset": selected_asset,
        }

    @staticmethod
    def dividend(action_id, cusip, when):
        return {
            "id": action_id,
            "action_type": "cash_dividend",
            "symbol": "TEST",
            "cusip": cusip,
            "ex_date": when,
            "record_date": when,
            "payable_date": when,
            "process_date": when,
            "rate": 0.1,
        }

    def reused_label_evidence(self):
        old = self.asset("old-id", "OLD", active=False)
        current = self.asset("current-id", "TEST")
        return {
            "corporate_actions": [
                self.dividend("old-action", "111111111", "2024-01-03"),
                self.dividend("current-action", "222222222", "2024-01-08"),
            ],
            "identities": [old, current],
            "symbol_occupancy_evidence": [
                {
                    "symbol": "TEST",
                    "cusip": "111111111",
                    "asset_id": "old-id",
                    "evidence": [{"action_id": "old-action"}],
                },
                {
                    "symbol": "TEST",
                    "cusip": "222222222",
                    "asset_id": "current-id",
                    "evidence": [{"action_id": "current-action"}],
                },
            ],
            "asset_lookups": [
                self.lookup("symbol", "TEST", current),
                self.lookup("cusip", "111111111", old),
                self.lookup("cusip", "222222222", current),
            ],
        }

    def test_completed_single_uuid_decision_needs_no_evidence_replay(self):
        receipt = self.receipt()
        before = deepcopy(receipt)
        self.assertTrue(self.promotable(receipt))
        self.assertEqual(receipt, before)

    def test_only_lineage_33_decision_uses_the_exact_proof(self):
        receipt = self.receipt(schema_version=31, lineage_version=31)
        self.assertFalse(self.promotable(receipt))

    def test_unresolved_shape_does_not_block_a_behavior_identical_transition(self):
        receipt = self.receipt()
        receipt["unresolved_rows"] = 8
        self.assertTrue(self.promotable(receipt))

    def test_empty_identity_decision_needs_no_evidence_replay(self):
        receipt = self.receipt()
        receipt.update(
            asset_ids=[],
            unresolved_rows=8,
            evidence={
                "corporate_actions": [],
                "identities": [],
                "symbol_occupancy_evidence": [],
                "asset_lookups": [],
            },
        )
        self.assertTrue(self.promotable(receipt))

    def test_action_anchored_current_suffix_is_behavior_identical(self):
        receipt = self.receipt()
        receipt.update(
            asset_ids=["old-id", "current-id"],
            unresolved_rows=8,
            evidence=self.reused_label_evidence(),
        )
        self.assertTrue(self.promotable(receipt))

    def test_completed_decision_with_suffix_shape_is_promoted_unchanged(self):
        receipt = self.receipt()
        receipt.update(
            asset_ids=["old-id", "current-id"],
            evidence=self.reused_label_evidence(),
        )
        self.assertTrue(self.promotable(receipt))

    def test_possible_foreign_occupancy_duplicate_is_behavior_identical(self):
        receipt = self.receipt()
        evidence = self.reused_label_evidence()
        evidence["corporate_actions"] = [
            self.dividend("current-before", "222222222", "2024-01-02"),
            self.dividend("foreign-copy", "111111111", "2024-01-03"),
            self.dividend("current-after", "222222222", "2024-01-08"),
        ]
        evidence["symbol_occupancy_evidence"] = [
            {
                "symbol": "TEST",
                "cusip": "222222222",
                "asset_id": "current-id",
                "evidence": [
                    {"action_id": "current-before"},
                    {"action_id": "current-after"},
                ],
            }
        ]
        receipt.update(
            asset_ids=["old-id", "current-id"],
            unresolved_rows=8,
            evidence=evidence,
        )
        self.assertTrue(self.promotable(receipt))

    def test_unaffected_multi_uuid_shape_is_promotable(self):
        receipt = self.receipt()
        receipt.update(
            asset_ids=["old-id", "current-id"],
            unresolved_rows=8,
            evidence={
                "corporate_actions": [],
                "identities": [],
                "symbol_occupancy_evidence": [],
                "asset_lookups": [],
            },
        )
        self.assertTrue(self.promotable(receipt))

    def test_proof_is_specific_to_one_complete_version_transition(self):
        for field, value in (
            ("lineage", 31),
            ("correlation", 57),
            ("identity_report", -1),
            ("calendar_sha256", "different"),
        ):
            with self.subTest(field=field):
                receipt = self.receipt()
                receipt["versions"][field] = value
                self.assertFalse(self.promotable(receipt))
        receipt = self.receipt()
        receipt["schema_version"] = 31
        self.assertFalse(self.promotable(receipt))
        with patch("libs.alpaca_lineage_build.migration.versions", return_value={**versions(), "lineage": 32}):
            self.assertFalse(self.promotable(self.receipt()))
        with (
            patch("libs.alpaca_lineage_build.migration.PRODUCTION_LINEAGE_VERSION", 35),
            patch("libs.alpaca_lineage_build.migration.PRODUCTION_CORRELATION_VERSION", 61),
        ):
            self.assertFalse(self.promotable(self.receipt()))


class RuleMigrationPublicationTests(NativeCase):
    def downgrade(self):
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "TEST", index)
        legacy_versions = {**versions(), "lineage": 33, "correlation": 59}
        legacy_versions.pop("correlation_policy_sha256")
        receipt.update(schema_version=33, versions=legacy_versions)
        receipt.pop("correlation_policy")
        entry = {
            **index["symbols"]["TEST"],
            "sha256": storage.document_digest(receipt),
            "versions": legacy_versions,
        }
        entry.pop("correlation_policy")
        journal = {
            "schema_version": 33,
            "boundary": "alpaca_lineage_transaction",
            "documents": {"TEST/lineage.manifest.json": receipt},
            "retire": [],
            "index_updates": {"TEST": entry},
            "sources": {"TEST": receipt["source_sha256"]},
            "unavailable_pricing": [],
            "notices": {},
        }
        with (
            storage.pricing_locks(self.store, receipt["input_sources"]),
            storage.publication_lock(self.root, exclusive=True),
        ):
            storage._install(self.root, journal, compare=False)

    def test_fresh_publication_rejects_candidate_policy_before_writing_journal(self):
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "TEST", index)
        boundaries = storage.read_boundaries(self.root, receipt)
        candidate = load_policy(
            Path(__file__).resolve().parents[1]
            / "resources/alpaca_security_correlation_evidence_quality_policy.yaml"
        )
        receipt["correlation_policy"] = candidate.provenance()
        with (
            storage.pricing_locks(self.store, receipt["input_sources"]),
            self.assertRaisesRegex(LineageError, "authorized current policy"),
        ):
            storage.publish(
                self.store,
                self.root,
                index,
                {"TEST": receipt},
                {"TEST": boundaries},
                {},
            )
        self.assertFalse((self.root / ".pending.json").exists())

    def test_dedicated_policy_migration_uses_only_the_fast_path_and_is_idempotent(self):
        self.downgrade()
        checks = read_checks(self.store)
        complete_checks(self.store, checks["epoch"])
        pricing = snapshot(self.store / "end_of_day/pricing")
        with (
            patch(
                "libs.alpaca_lineage_build.pipeline.load_pricing",
                side_effect=AssertionError("decoded prices"),
            ),
            patch(
                "libs.alpaca_lineage_build.pipeline.correlate",
                side_effect=AssertionError("replayed rules"),
            ),
            patch(
                "libs.alpaca_lineage_build.observations.select_observations",
                side_effect=AssertionError("selected observations"),
            ),
            patch(
                "libs.alpaca_lineage_build.storage.read_boundaries",
                side_effect=AssertionError("opened assignment boundaries"),
            ),
        ):
            migrated = migrate_correlation_policy(self.store)
            repeated = migrate_correlation_policy(self.store)
        self.assertEqual(1, migrated["promoted_groups"])
        self.assertEqual(1, migrated["promoted_receipts"])
        self.assertEqual(0, migrated["failed"])
        self.assertEqual(0, migrated["provider_requests"])
        self.assertEqual(0, migrated["identity_rule_executions"])
        self.assertEqual(0, migrated["pricing_payload_reads"])
        self.assertEqual(0, migrated["pricing_writes"])
        self.assertEqual("pending_update", migrated["serving_catalogue_action"])
        self.assertEqual(0, repeated["promoted_groups"])
        self.assertEqual(1, repeated["already_current_groups"])
        self.assertEqual("pending_update", repeated["serving_catalogue_action"])
        self.assertEqual(pricing, snapshot(self.store / "end_of_day/pricing"))

    def test_dedicated_policy_migration_recovers_its_frozen_journal_before_resuming(self):
        self.downgrade()
        checks = read_checks(self.store)
        complete_checks(self.store, checks["epoch"])
        with (
            patch(
                "libs.alpaca_lineage_build.storage.finish_publication",
                side_effect=RuntimeError("interrupted policy migration"),
            ),
            self.assertRaisesRegex(RuntimeError, "interrupted policy migration"),
        ):
            migrate_correlation_policy(self.store)
        self.assertTrue((self.root / ".pending.json").exists())
        resumed = migrate_correlation_policy(self.store)
        self.assertTrue(resumed["recovered_publication"])
        self.assertEqual(0, resumed["promoted_groups"])
        self.assertEqual(1, resumed["already_current_groups"])
        self.assertFalse((self.root / ".pending.json").exists())

    def test_dedicated_policy_migration_preserves_pending_summary_ownership(self):
        rebuild_check_summary(self.store)
        self.downgrade()
        checks = read_checks(self.store)
        complete_checks(self.store, checks["epoch"])
        index = storage.read_index(self.root)
        receipt = storage.read_receipt(self.root, "TEST", index)
        with storage.pricing_locks(self.store, receipt["input_sources"]):
            invalidate(self.store, {"TEST"}, owner="pricing")
        before = read_status(self.store)["pending_inputs"]
        with (
            patch(
                "libs.alpaca_lineage_build.storage.publish",
                side_effect=AssertionError("published despite pending summary work"),
            ) as publish,
            self.assertRaisesRegex(DownloadError, "pending inputs"),
        ):
            migrate_correlation_policy(self.store)
        publish.assert_not_called()
        self.assertEqual(before, read_status(self.store)["pending_inputs"])
        receipt = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))
        self.assertEqual(33, receipt["schema_version"])

    def test_promote_then_repeat_without_payload_decode_or_repeated_migration_proof(self):
        self.downgrade()
        before = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))
        pricing = snapshot(self.store / "end_of_day/pricing/TEST")
        client = IdentityClient({"TEST": asset("TEST")})
        with (
            patch(
                "libs.alpaca_lineage_build.pipeline.load_pricing", side_effect=AssertionError("decoded prices")
            ),
            patch(
                "libs.alpaca_lineage_build.pipeline.correlate", side_effect=AssertionError("repeated decisions")
            ),
        ):
            migrated = run_lineage(self.store, ["TEST"], identity_client=client)
            self.assertEqual(migrated["failed"], 0, migrated)
            self.assertTrue(migrated["results"][0]["reused_decision"])
            after = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))
            self.assertEqual(after["versions"], versions())
            self.assertEqual(after["boundaries"], before["boundaries"])
            with patch(
                "libs.alpaca_lineage_build.pipeline.can_promote_observation_decision",
                side_effect=AssertionError("repeated migration"),
            ):
                repeat = run_lineage(self.store, ["TEST"], identity_client=client)
        self.assertEqual(repeat["results"][0]["status"], "unchanged")
        self.assertEqual(client.request_count, 0)
        self.assertEqual(snapshot(self.store / "end_of_day/pricing/TEST"), pricing)

    def test_interrupted_promotion_recovers_through_existing_journal(self):
        self.downgrade()
        client = IdentityClient({"TEST": asset("TEST")})
        with patch(
            "libs.alpaca_lineage_build.storage.finish_publication",
            side_effect=RuntimeError("interrupted promotion"),
        ):
            result = run_lineage(self.store, ["TEST"], identity_client=client)
        self.assertEqual(result["failed"], 1, result)
        self.assertTrue((self.root / ".pending.json").exists())
        with patch(
            "libs.alpaca_lineage_build.pipeline.load_pricing",
            side_effect=AssertionError("decoded recovered prices"),
        ):
            repeat = run_lineage(self.store, ["TEST"], identity_client=client)
        self.assertEqual(repeat["failed"], 0, repeat)
        self.assertFalse((self.root / ".pending.json").exists())
        self.assertEqual(client.request_count, 0)
        receipt = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))
        self.assertEqual(receipt["versions"], versions())

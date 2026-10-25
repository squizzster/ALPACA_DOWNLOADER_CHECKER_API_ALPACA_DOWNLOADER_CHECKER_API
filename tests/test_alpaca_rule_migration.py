"""A version migration publishes its proof once without redoing unaffected work."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import snapshot
from test_alpaca_terminal_occupancy import terminal_fixture

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.contract import versions
from libs.alpaca_lineage_build.migration import can_promote_observation_decision
from libs.alpaca_lineage_build.pipeline import run_lineage


class RuleMigrationProofTests(TestCase):
    def receipt(self, *, schema_version=30, lineage_version=30, correlation_version=56):
        return {
            "schema_version": schema_version,
            "versions": {
                **versions(),
                "lineage": lineage_version,
                "correlation": correlation_version,
            },
            "asset_ids": ["old-id", "live-id"],
            "evidence": terminal_fixture(),
            "unresolved_rows": 0,
        }

    def prior_receipt(self):
        return self.receipt(schema_version=31, lineage_version=31, correlation_version=57)

    def test_changed_terminal_interval_requires_group_recorrelation(self):
        self.assertFalse(can_promote_observation_decision(self.receipt()))

    def test_unaffected_legacy_decision_is_promoted_through_both_changes(self):
        receipt = self.receipt()
        receipt["asset_ids"] = ["live-id"]
        before = deepcopy(receipt)
        self.assertTrue(can_promote_observation_decision(receipt))
        self.assertEqual(receipt, before)

    def test_completed_single_uuid_prior_decision_needs_no_evidence_replay(self):
        receipt = self.prior_receipt()
        receipt["asset_ids"] = ["id"]
        receipt.pop("evidence")  # The compact authoritative projection is sufficient.
        self.assertTrue(can_promote_observation_decision(receipt))

    def test_unresolved_single_uuid_without_shape_evidence_fails_closed(self):
        receipt = self.prior_receipt()
        receipt.update(asset_ids=["id"], unresolved_rows=8)
        receipt.pop("evidence")
        self.assertFalse(can_promote_observation_decision(receipt))

    def test_disconnected_same_label_occupants_require_recorrelation(self):
        self.assertFalse(can_promote_observation_decision(self.prior_receipt()))

    def test_cross_uuid_name_change_requires_recorrelation(self):
        receipt = self.prior_receipt()
        receipt["evidence"] = {
            "corporate_actions": [
                {
                    "id": "rename",
                    "action_type": "name_change",
                    "effective_date": "2024-01-02",
                    "process_date": "2024-01-02",
                    "old_symbol": "OLD",
                    "old_cusip": "old-cusip",
                    "new_symbol": "NEW",
                    "new_cusip": "new-cusip",
                }
            ],
            "identities": [
                {"id": "old-id", "symbol": "OLD"},
                {"id": "live-id", "symbol": "NEW"},
            ],
            "symbol_occupancy_evidence": [
                {"asset_id": "old-id", "symbol": "OLD", "cusip": "old-cusip"},
                {"asset_id": "live-id", "symbol": "NEW", "cusip": "new-cusip"},
            ],
            "asset_lookups": [],
        }
        self.assertFalse(can_promote_observation_decision(receipt))

    def test_incomplete_active_request_horizon_requires_recorrelation(self):
        receipt = self.prior_receipt()
        receipt.update(
            symbol="LIVE",
            asset_ids=["live-id"],
            unresolved_rows=8,
            assignment_summary={"raw": {"last_session": "2026-10-22"}},
            pricing_request={"end_exclusive": "2026-10-24T00:00:00Z"},
        )
        receipt["evidence"] = {
            "corporate_actions": [],
            "identities": [{"id": "live-id", "symbol": "LIVE", "status": "active"}],
            "symbol_occupancy_evidence": [],
            "asset_lookups": [
                {
                    "lookup_kind": "symbol",
                    "lookup": "LIVE",
                    "found": True,
                    "asset": {"id": "live-id", "symbol": "LIVE", "status": "active"},
                }
            ],
        }
        self.assertFalse(can_promote_observation_decision(receipt))
        receipt["pricing_request"]["end_exclusive"] = "2026-10-23T00:00:00Z"
        self.assertTrue(can_promote_observation_decision(receipt))

    def test_proof_is_specific_to_one_complete_version_transition(self):
        for field, value in (
            ("lineage", 29),
            ("correlation", 55),
            ("identity_report", -1),
            ("calendar_sha256", "different"),
        ):
            with self.subTest(field=field):
                receipt = self.receipt()
                receipt["versions"][field] = value
                self.assertFalse(can_promote_observation_decision(receipt))
        receipt = self.receipt()
        receipt["schema_version"] = 29
        self.assertFalse(can_promote_observation_decision(receipt))
        with patch(
            "libs.alpaca_lineage_build.migration.versions", return_value={**versions(), "correlation": 58}
        ):
            self.assertFalse(can_promote_observation_decision(self.receipt()))


class RuleMigrationPublicationTests(NativeCase):
    def downgrade(self):
        def change(receipt, _):
            receipt.update(schema_version=30, versions={**versions(), "lineage": 30, "correlation": 56})

        self.publish_change(change)

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

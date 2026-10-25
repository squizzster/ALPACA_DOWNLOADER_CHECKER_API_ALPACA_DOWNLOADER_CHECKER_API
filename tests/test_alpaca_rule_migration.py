"""A version migration publishes its proof once without redoing unaffected work."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import snapshot

from libs.alpaca_lineage_build import storage
from libs.alpaca_lineage_build.contract import versions
from libs.alpaca_lineage_build.migration import can_promote_observation_decision
from libs.alpaca_lineage_build.pipeline import run_lineage


class RuleMigrationProofTests(TestCase):
    def receipt(self, *, schema_version=32, lineage_version=32, correlation_version=58):
        return {
            "schema_version": schema_version,
            "versions": {
                **versions(),
                "lineage": lineage_version,
                "correlation": correlation_version,
            },
            "symbol": "TEST",
            "asset_ids": ["id"],
            "unresolved_rows": 0,
        }

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
        self.assertTrue(can_promote_observation_decision(receipt))
        self.assertEqual(receipt, before)

    def test_compatible_lineage_31_decision_uses_the_same_exact_proof(self):
        receipt = self.receipt(schema_version=31, lineage_version=31)
        self.assertTrue(can_promote_observation_decision(receipt))

    def test_unresolved_single_uuid_without_shape_evidence_fails_closed(self):
        receipt = self.receipt()
        receipt["unresolved_rows"] = 8
        self.assertFalse(can_promote_observation_decision(receipt))

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
        self.assertTrue(can_promote_observation_decision(receipt))

    def test_action_anchored_current_suffix_requires_group_recorrelation(self):
        receipt = self.receipt()
        receipt.update(
            asset_ids=["old-id", "current-id"],
            unresolved_rows=8,
            evidence=self.reused_label_evidence(),
        )
        self.assertFalse(can_promote_observation_decision(receipt))

    def test_completed_decision_with_suffix_shape_is_promoted_unchanged(self):
        receipt = self.receipt()
        receipt.update(
            asset_ids=["old-id", "current-id"],
            evidence=self.reused_label_evidence(),
        )
        self.assertTrue(can_promote_observation_decision(receipt))

    def test_possible_foreign_occupancy_duplicate_requires_group_recorrelation(self):
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
        self.assertFalse(can_promote_observation_decision(receipt))

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
        self.assertTrue(can_promote_observation_decision(receipt))

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
                self.assertFalse(can_promote_observation_decision(receipt))
        receipt = self.receipt()
        receipt["schema_version"] = 31
        self.assertFalse(can_promote_observation_decision(receipt))
        with patch("libs.alpaca_lineage_build.migration.versions", return_value={**versions(), "lineage": 32}):
            self.assertFalse(can_promote_observation_decision(self.receipt()))


class RuleMigrationPublicationTests(NativeCase):
    def downgrade(self):
        def change(receipt, _):
            receipt.update(schema_version=32, versions={**versions(), "lineage": 32, "correlation": 58})

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

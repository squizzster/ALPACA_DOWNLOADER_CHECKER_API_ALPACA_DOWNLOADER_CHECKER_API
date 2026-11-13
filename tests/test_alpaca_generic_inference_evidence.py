"""Research cannot enter fresh inference; completed provider work remains closed."""

import copy
import unittest
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset

from libs.alpaca_end_of_day_security_correlation.row_correlation import CorrelationCache
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_lineage_build.policy_engine import evaluate_policy
from libs.alpaca_security_lineage.inference_evidence import (
    ProviderEvidence,
    accept_provider_evidence,
    provider_evidence,
)
from libs.alpaca_security_lineage.lineage import build_identity_report


class GenericInferenceEvidenceTests(unittest.TestCase):
    local = {"present": True, "downloads": [], "counts_known": False}

    def report(self):
        return build_identity_report(
            "ALPHA",
            self.local,
            IdentityClient({"ALPHA": asset("ALPHA", "owner-a")}),
            query_dates=("2026-01-01", "2026-02-01"),
        )

    def test_external_supplement_rejected_before_client_access(self):
        class ForbiddenClient:
            def __getattribute__(self, name):
                raise AssertionError("provider access before rejection")

        with self.assertRaisesRegex(ValueError, "inadmissible"):
            build_identity_report("ALPHA", self.local, ForbiddenClient(), event_supplements=[{}])

    def test_fresh_inference_rejects_assertions_before_policy_or_storage(self):
        for field in ("sourced_market_occupancy", "event_supplements", "research_certificates"):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "injection"):
                evaluate_policy(None, {}, {"ALPHA": {field: ["authored"]}}, policy=None)

    def test_raw_provider_observations_replace_legacy_derived_assertions_once(self):
        legacy = dict(self.report())
        legacy.update(
            event_supplements=[{"legacy": "authored endpoint"}],
            provider_corporate_actions=[],
            corporate_actions=[
                {
                    "action_type": "name_change",
                    "id": "authored",
                    "old_symbol": "BETA",
                    "old_cusip": "invented",
                    "new_symbol": "ALPHA",
                    "new_cusip": "invented",
                }
            ],
            symbol_changes=[{"id": "authored"}],
            input_symbol_occupants=[{"cusip": "invented"}],
        )
        original = copy.deepcopy(legacy)
        client = EvidenceClient(IdentityClient())
        from libs.alpaca_security_lineage import lineage

        with patch.object(
            lineage, "report_from_observations", wraps=lineage.report_from_observations
        ) as assemble:
            accepted = client.provider_view(legacy, local_report=self.local)
            self.assertIs(client.provider_view(legacy, local_report=self.local), accepted)
            self.assertEqual(assemble.call_count, 1)
        self.assertIsInstance(accepted, ProviderEvidence)
        self.assertEqual(accepted["corporate_actions"], [])
        self.assertEqual(accepted["symbol_changes"], [])
        self.assertEqual(accepted["input_symbol_occupants"], [])
        self.assertEqual(accepted["query"], legacy["query"])
        self.assertEqual(legacy, original)
        self.assertEqual(client.request_count, 0)

    def test_missing_raw_provider_observations_requires_metadata_recovery(self):
        legacy = dict(self.report())
        legacy["event_supplements"] = ["authored"]
        with self.assertRaisesRegex(ValueError, "metadata recovery"):
            provider_evidence(legacy)

    def test_completed_provider_acceptance_is_not_repeated(self):
        accepted = self.report()
        with patch(
            "libs.alpaca_security_lineage.inference_evidence.reject_assertions",
            side_effect=AssertionError("repeat producer validation"),
        ):
            self.assertIs(accept_provider_evidence(accepted), accepted)
            self.assertIs(provider_evidence(accepted), accepted)

    def test_legacy_certificate_capabilities_cannot_enable_fresh_rules(self):
        for option in (
            "sourced_succession_certificates",
            "sourced_rename_certificates",
            "public_common_certificates",
            "new_public_parent_continuations",
        ):
            with self.subTest(option=option), self.assertRaisesRegex(RuntimeError, "inadmissible"):
                CorrelationCache(**{option: True})

    def test_auxiliary_assertions_cannot_bypass_retired_market_ingress(self):
        from libs.alpaca_end_of_day_security_correlation import market_occupancy

        evidence = {"UNPRICED": {"sourced_market_occupancy": {"facts": ["authored"]}}}
        for execute in (
            lambda: market_occupancy.bind({}, {}, auxiliary=evidence),
            lambda: market_occupancy.project({}, {}, {}, auxiliary=evidence),
        ):
            with self.subTest(execute=execute), self.assertRaisesRegex(ValueError, "injection"):
                execute()

    def test_retired_direct_recipes_cannot_establish_fresh_authority(self):
        from types import SimpleNamespace

        from libs.alpaca_end_of_day_security_correlation import market_occupancy, sourced_rename

        with self.assertRaisesRegex(ValueError, "inadmissible"):
            market_occupancy.apply_adjustment_authority({}, {}, SimpleNamespace(adjustment_bridges=[{}]))
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            sourced_rename.mapped_certificate("ALPHA", {}, [object()], object())
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            market_occupancy.adjustment_bridges(
                [],
                {
                    "authored": {
                        "kind": "dated_market_occupancy",
                        "match": {},
                        "facts": {},
                        "sources": [],
                    }
                },
                [],
            )

    def test_fresh_shared_observations_are_traversed_once_at_acceptance(self):
        from libs.alpaca_security_lineage.inference_evidence import reject_assertions

        class Observation(dict):
            traversals = 0

            def items(self):
                self.traversals += 1
                return super().items()

        asset = Observation(id="provider-id", symbol="ALPHA")
        reject_assertions({"catalogue": {"ALPHA": asset}, "records": [asset], "exact": {"ALPHA": asset}})
        self.assertEqual(asset.traversals, 1)

    def test_retained_ticker_snapshot_is_removed_from_fresh_provider_view(self):
        legacy = dict(self.report())
        legacy["symbol_disposition"] = {
            **legacy["symbol_disposition"],
            "classification": "exchange_test_symbol",
            "test_symbol_evidence": {"source_url": "https://example.test/external-symbol-snapshot"},
        }
        selected = provider_evidence(legacy, local_report=self.local)
        self.assertEqual(selected["symbol_disposition"]["classification"], "alpaca_asset_identity_available")
        self.assertIsNone(selected["symbol_disposition"]["test_symbol_evidence"])
        self.assertEqual(legacy["symbol_disposition"]["classification"], "exchange_test_symbol")

    def test_bare_legacy_ticker_exclusion_is_not_fresh_source_evidence(self):
        with self.assertRaisesRegex(ValueError, "test-symbol classification is inadmissible"):
            accept_provider_evidence({"symbol_disposition": {"classification": "exchange_test_symbol"}})


class GenericPublicationOutcomeTests(unittest.TestCase):
    def test_owned_and_ownerless_publications_complete_independently(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from test_alpaca_pricing_download import FakeClient, bar

        from alpaca_end_of_day import DataLibrary
        from libs.alpaca_lineage_build import pipeline, storage
        from libs.alpaca_pricing_download.download import download_pricing
        from libs.alpaca_serving_catalogue.producer import maintain

        with TemporaryDirectory() as directory:
            store = Path(directory) / "store"
            for symbol in ("ALPHA", "DARK"):
                download_pricing(
                    store,
                    symbol,
                    "2026-01-06",
                    from_date="2026-01-01",
                    client=FakeClient([bar("2026-01-02"), bar("2026-01-05")]),
                )
            result = pipeline.run_lineage(
                store,
                ["ALPHA", "DARK"],
                identity_client=IdentityClient({"ALPHA": asset("ALPHA", "owner-a")}),
            )
            self.assertEqual(result["failed"], 0, result)
            root = store / "end_of_day/lineage"
            index = storage.read_index(root)
            owned = storage.read_receipt(root, "ALPHA", index)
            ownerless = storage.read_receipt(root, "DARK", index)
            self.assertEqual(owned["correlated_rows"], 16)
            self.assertEqual(ownerless["unresolved_rows"], 16)
            self.assertEqual(ownerless["correlated_rows"], 0)
            for receipt in (owned, ownerless):
                self.assertTrue(receipt["row_partition_complete"])
                self.assertEqual(receipt["source_rows"], 16)
            served = maintain(store, bootstrap=True)
            self.assertTrue(served["complete"], served)
            library = DataLibrary(store=store)
            self.assertEqual(library.online_count(), 1)
            self.assertEqual(library.resolve_ticker("DARK"), ())

"""New sourced facts are acquired once, preserved, and invalidated by event changes."""

import copy
import json
import unittest
from unittest.mock import patch

import test_alpaca_lineage_build as fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import snapshot

from libs.alpaca_lineage_build import pipeline
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_security_lineage.event_supplements import (
    load_supplements,
    project_actions,
    validate_supplements,
)


def split_supplement():
    return {
        "kind": "split_endpoints",
        "match": {
            "id": "split",
            "action_type": "forward_split",
            "symbol": "GOOG",
            "cusip": "111111111",
            "ex_date": "2026-10-01",
            "process_date": "2026-10-01",
            "old_rate": 1,
            "new_rate": 10,
        },
        "facts": {"old_cusip": "111111111", "new_cusip": "222222222", "effective_date": "2026-10-01"},
        "sources": [
            {
                "url": "https://issuer.example/split.pdf",
                "title": "Split identifier transition",
                "authority": "issuer",
                "document_date": "2026-09-20",
            }
        ],
    }


def derivative_supplement():
    return {
        "kind": "derivative_only",
        "match": {
            "id": "root",
            "action_type": "name_change",
            "old_symbol": "1GOOG",
            "new_symbol": "GOOG",
            "old_cusip": "111111111",
            "new_cusip": "111111111",
            "process_date": "2026-10-01",
        },
        "facts": {
            "instrument_type": "option",
            "underlying_cusip": "111111111",
            "effective_date": "2026-10-01",
            "series": [{"type": "put", "expiration": "2026-10-02", "strike": 9}],
        },
        "sources": [
            {
                "url": "https://clearinghouse.example/root.pdf",
                "title": "FLEX root consolidation",
                "authority": "clearinghouse",
                "document_date": "2026-09-20",
            }
        ],
    }


class SupplementTests(unittest.TestCase):
    def test_listing_transfer_keeps_the_real_rename_and_rejects_changed_security(self):
        item = derivative_supplement()
        item["kind"] = "listing_transfer_only"
        item["facts"] = {
            "instrument_type": "equity",
            "unchanged_symbol": "GOOG",
            "effective_date": "2026-10-01",
            "from_exchange": "NASDAQ",
            "to_exchange": "NYSE",
        }
        item["sources"][0]["authority"] = "issuer"
        validate_supplements([item])
        real_rename = {**item["match"], "id": "earlier-rename", "process_date": "2026-01-02"}
        projected, applied = project_actions([real_rename, item["match"]], [item])
        self.assertEqual([real_rename], projected)
        self.assertEqual([item], applied)
        for key, value in (("new_cusip", "222222222"), ("process_date", "2026-10-02")):
            changed = copy.deepcopy(item)
            changed["match"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_supplements([changed])
        changed = copy.deepcopy(item)
        changed["facts"]["unchanged_symbol"] = "OTHER"
        with self.assertRaises(ValueError):
            validate_supplements([changed])

    def test_retained_supplement_schema_requires_an_explicit_supported_version(self):
        for version in (None, True, 2):
            with self.subTest(version=version), self.assertRaisesRegex(LineageError, "schema"):
                EvidenceClient(IdentityClient()).retain_event_supplements(
                    [{"event_supplement_schema_version": version, "event_supplements": [split_supplement()]}]
                )

    def test_split_projection_preserves_raw_evidence_and_provider_dates(self):
        item = split_supplement()
        validate_supplements([item])
        raw = copy.deepcopy(item["match"])
        projected, applied = project_actions([raw], [item])
        self.assertEqual(raw, item["match"])
        self.assertEqual(projected[0]["new_cusip"], "222222222")
        self.assertEqual(projected[0]["ex_date"], raw["ex_date"])
        self.assertEqual(applied, [item])
        for changed in ({**raw, "new_rate": 20}, {**raw, "new_cusip": "333333333"}):
            with self.assertRaises(ValueError):
                project_actions([changed], [item])

    def test_derivative_scope_is_exact_and_does_not_filter_similarly_named_equity(self):
        item = derivative_supplement()
        validate_supplements([item])
        equity = {**item["match"], "id": "different-equity-event"}
        projected, applied = project_actions([item["match"], equity], [item])
        self.assertEqual(projected, [equity])
        self.assertEqual(applied, [item])
        with self.assertRaisesRegex(ValueError, "changed"):
            project_actions([{**item["match"], "process_date": "2026-10-02"}], [item])

    def test_invalid_and_speculative_facts_fail_at_input_boundary(self):
        changes = [
            ("sources", []),
            ("kind", "merge_these_ids"),
            ("facts", {**split_supplement()["facts"], "effective_date": "2026-10-02"}),
            ("facts", {**split_supplement()["facts"], "new_cusip": "111111111"}),
            ("match", {**split_supplement()["match"], "new_rate": "NaN"}),
            ("match", {**split_supplement()["match"], "new_rate": 0}),
        ]
        for key, value in changes:
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                validate_supplements([{**split_supplement(), key: value}])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_supplements([split_supplement(), split_supplement()])


class SupplementPipelineTests(unittest.TestCase):
    setUp = fixtures.LineageBuildTests.setUp
    download = fixtures.LineageBuildTests.download
    run_build = fixtures.LineageBuildTests.run_build
    receipt = fixtures.LineageBuildTests.receipt

    def test_new_split_endpoint_acquires_only_missing_mapping_and_survives_reuse(self):
        self.download()
        item = split_supplement()
        old, new = {**asset("GOOG", "old-id"), "status": "inactive"}, asset("GOOG", "new-id")
        assets = {"GOOG": new, "111111111": old, "222222222": new}
        self.run_build(["GOOG"], client=IdentityClient(assets, [item["match"]]))
        raw = copy.deepcopy(self.receipt()["evidence"]["corporate_actions"])
        pricing = snapshot(self.store / "end_of_day/pricing")
        client = IdentityClient(assets)
        with patch.object(pipeline, "download_pricing", side_effect=AssertionError("pricing changed")):
            result = self.run_build(["GOOG"], client=client, event_supplements=[item])
        self.assertEqual(result["unmatched_event_supplements"], [])
        self.assertEqual(client.request_count, 1)
        self.assertEqual(client.calls, [])
        evidence = self.receipt()["evidence"]
        self.assertEqual(evidence["event_supplements"], [item])
        self.assertEqual(evidence["provider_corporate_actions"], raw)
        self.assertEqual(evidence["corporate_actions"][0]["new_cusip"], "222222222")
        client = IdentityClient()
        for kwargs in ({}, {"event_supplements": [item]}):
            self.run_build(["GOOG"], client=client, **kwargs)
            self.assertEqual(self.receipt()["evidence"], evidence)
        self.assertEqual(client.request_count, 0)
        self.assertEqual(snapshot(self.store / "end_of_day/pricing"), pricing)
        # Explicit metadata refresh must preserve the fact and original action.
        client = IdentityClient(assets, [item["match"]])
        self.run_build(["GOOG"], client=client, refresh=True)
        self.assertEqual(self.receipt()["evidence"]["event_supplements"], [item])
        self.assertEqual(self.receipt()["evidence"]["provider_corporate_actions"], raw)
        retained = snapshot(self.root)
        changed = IdentityClient(assets, [{**item["match"], "new_rate": 20}])
        failed = pipeline.run_lineage(self.store, ["GOOG"], identity_client=changed, refresh=True)
        self.assertEqual(failed["failed"], 1)
        self.assertEqual(snapshot(self.root), retained)

    def test_derivative_action_retained_without_creating_equity_dependency(self):
        self.download()
        item = derivative_supplement()
        client = IdentityClient({"GOOG": asset("GOOG"), "111111111": asset("GOOG")}, [item["match"]])
        self.run_build(["GOOG"], client=client, event_supplements=[item])
        evidence = self.receipt()["evidence"]
        self.assertEqual(evidence["corporate_actions"], [])
        self.assertEqual(evidence["provider_corporate_actions"], [item["match"]])
        self.assertEqual(evidence["queried_symbols"], ["GOOG"])
        self.assertEqual(set(self.receipt()["input_sources"]), {"GOOG"})

    def test_unmatched_supplement_is_reported_and_requires_explicit_selection(self):
        self.download()
        with self.assertRaises(LineageError):
            pipeline.run_lineage(
                self.store, identity_client=IdentityClient(), event_supplements=[split_supplement()]
            )
        result = pipeline.run_lineage(
            self.store, ["GOOG"], identity_client=IdentityClient(), event_supplements=[split_supplement()]
        )
        self.assertEqual(result["unmatched_event_supplements"], ["split"])
        self.assertEqual(result["failed"], 1)

    def test_document_schema_is_strict(self):
        path = self.store.parent / "facts.json"
        path.write_text(json.dumps({"schema_version": 1, "supplements": [split_supplement()]}))
        self.assertEqual(load_supplements(path), [split_supplement()])
        path.write_text(json.dumps({"schema_version": True, "supplements": [split_supplement()]}))
        with self.assertRaises(ValueError):
            load_supplements(path)

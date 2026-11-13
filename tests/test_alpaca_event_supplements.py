"""Retired assertion inputs reject before work; legacy provenance stays inspectable."""

import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_lineage_build import pipeline
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.metadata import EvidenceClient
from libs.alpaca_security_lineage import event_supplements, lineage
from libs.alpaca_security_lineage.event_supplements import (
    accept_supplements,
    load_supplements,
    project_actions,
    supplement_key,
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


def succession_supplement():
    return {
        "kind": "holding_company_succession",
        "match": {"source_id": "parent-substitution", "symbol": "GOOG"},
        "facts": {
            "schema_version": 1,
            "fact_kind": "holding_company_succession",
            "old_cusip": "111111111",
            "new_cusip": "222222222",
            "legal_effective_date": "2026-09-30",
            "exchange_effective_date": "2026-10-01",
            "old_rate": 1,
            "new_rate": 1,
            "share_class": "common_stock",
            "automatic_exchange": True,
            "unchanged_rights": True,
        },
        "sources": [
            {
                "url": "https://issuer.example/parent-substitution.html",
                "title": "Automatic common-share substitution",
                "authority": "issuer",
                "document_date": "2026-09-30",
            }
        ],
    }


class SupplementTests(unittest.TestCase):
    def test_all_well_formed_legacy_kinds_are_rejected(self):
        for item in (split_supplement(), derivative_supplement(), succession_supplement()):
            with self.subTest(kind=item["kind"]), self.assertRaisesRegex(ValueError, "inadmissible"):
                accept_supplements([item])

    def test_validation_cannot_authorize_an_authored_fact(self):
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            validate_supplements([split_supplement()])

    def test_old_accepted_container_cannot_bypass_rejection(self):
        old = event_supplements._AcceptedSupplements([event_supplements._freeze(succession_supplement())])
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            accept_supplements(old)

    def test_empty_placeholder_is_immutable_and_reused(self):
        empty = accept_supplements([])
        self.assertIs(empty, accept_supplements(empty))
        self.assertIs(empty, copy.deepcopy(empty))
        with self.assertRaisesRegex(TypeError, "immutable"):
            empty.append(split_supplement())

    def test_file_input_rejects_before_path_access(self):
        with patch.object(Path, "open", side_effect=AssertionError("assertion file opened")):
            for path in ("/does-not-exist.json", "", object()):
                with self.subTest(path=type(path)), self.assertRaisesRegex(ValueError, "inadmissible"):
                    load_supplements(path)

    def test_projection_rejects_before_consuming_provider_actions(self):
        class ForbiddenActions:
            def __iter__(self):
                raise AssertionError("provider actions consumed before rejection")

        with self.assertRaisesRegex(ValueError, "inadmissible"):
            project_actions(ForbiddenActions(), [split_supplement()])

    def test_empty_projection_preserves_provider_observations(self):
        actions = [split_supplement()["match"], derivative_supplement()["match"]]
        original = copy.deepcopy(actions)
        projected, applied = project_actions(actions, [])
        self.assertEqual(original, projected)
        self.assertEqual(original, actions)
        self.assertEqual([], applied)

    def test_retained_negative_projection_cannot_filter_new_inference(self):
        fact = derivative_supplement()
        owner = event_supplements.EventProjection()
        owner.retain(fact["match"], fact, None)
        with patch.object(event_supplements, "_project_action", side_effect=AssertionError("overlay replay")):
            self.assertEqual(([fact["match"]], []), owner.bind([])([fact["match"]]))

    def test_retained_positive_projection_cannot_supply_new_endpoint(self):
        fact = split_supplement()
        owner = event_supplements.EventProjection()
        owner.retain(fact["match"], fact, {**fact["match"], "new_cusip": "222222222"})
        self.assertEqual(([fact["match"]], []), owner.bind([])([fact["match"]]))

    def test_private_projection_entry_also_rejects(self):
        fact = split_supplement()
        with self.assertRaisesRegex(ValueError, "inadmissible"):
            event_supplements._project_action(fact["match"], fact)

    def test_legacy_key_and_succession_readers_preserve_literal_provenance(self):
        item = succession_supplement()
        report = {"sourced_successions": {"schema_version": 1, "facts": [item]}}
        with patch.object(
            event_supplements, "accept_supplements", side_effect=AssertionError("read admitted facts")
        ):
            self.assertEqual("sourced:parent-substitution", supplement_key(item))
            self.assertEqual("split", supplement_key(split_supplement()))
            self.assertEqual([item], event_supplements.succession_items(report))
        self.assertEqual(report, json.loads(json.dumps(report)))

    def test_raw_provider_union_is_selected_without_overlay(self):
        raw = [split_supplement()["match"]]
        self.assertIs(
            raw, event_supplements.provider_actions({"provider_corporate_actions": raw, "corporate_actions": []})
        )
        self.assertEqual(raw, event_supplements.provider_actions({"corporate_actions": raw}))

    def test_metadata_constructor_rejects_before_client_access(self):
        class ForbiddenClient:
            def __getattribute__(self, name):
                raise AssertionError("provider accessed")

        with self.assertRaisesRegex(LineageError, "inadmissible"):
            EvidenceClient(ForbiddenClient(), event_supplements=[succession_supplement()])

    def test_identity_report_rejects_before_query_or_provider_work(self):
        with patch.object(lineage, "local_query_dates", side_effect=AssertionError("query planning")):
            with self.assertRaisesRegex(ValueError, "inadmissible"):
                lineage.build_identity_report("GOOG", {}, None, event_supplements=[split_supplement()])

    def test_pipeline_rejects_before_context_storage_or_provider_work(self):
        with patch.object(pipeline, "accept_context", side_effect=AssertionError("store accessed")):
            with self.assertRaisesRegex(LineageError, "inadmissible"):
                pipeline.run_lineage(
                    "/unused", ["GOOG"], identity_client=None, event_supplements=[split_supplement()]
                )

    def test_retained_research_is_projected_once_from_raw_provider_inputs(self):
        from tests.test_alpaca_sourced_rename import case

        evidence, _ = case()
        client = EvidenceClient(None)
        with patch.object(
            lineage, "report_from_observations", wraps=lineage.report_from_observations
        ) as produce:
            selected = client.provider_view(evidence)
            self.assertIs(selected, client.provider_view(evidence))
        self.assertEqual(1, produce.call_count)
        self.assertNotIn("event_supplements", selected)
        self.assertEqual(evidence["provider_corporate_actions"], selected["corporate_actions"])
        self.assertTrue(evidence["event_supplements"])


if __name__ == "__main__":
    unittest.main()

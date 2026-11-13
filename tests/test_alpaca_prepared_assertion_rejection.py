"""Fresh JSON candidates cannot introduce researched producer capabilities."""

import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation.uuid_lineage import UuidLineageEdge
from libs.alpaca_lineage_build import prepared_recovery
from libs.alpaca_lineage_build.contract import COMPUTATION_REVISION, LineageError


def provider_edge(kind="name_change"):
    return UuidLineageEdge(
        kind,
        "actual-provider-action",
        date(2024, 1, 5),
        "old-id",
        "OLD",
        "111111111",
        "new-id",
        "NEW",
        "222222222",
    ).as_report_object()


def researched_outputs():
    return {
        "public_common_continuation_membership": {
            "version": 1,
            "certificates": [{"source_fact_id": "authored-holder-treatment"}],
            "issues": [],
        },
        "public_common_continuation_ownership": {
            "version": 1,
            "source_fact_id": "authored-holder-treatment",
            "first": "2024-01-02",
            "last": "2024-01-04",
        },
        "security_relations": {
            "version": 1,
            "relations": [
                {
                    "relation_kind": "distinct_security_conversion",
                    "source_fact_id": "authored-conversion",
                    "identity_union": False,
                }
            ],
            "issues": [],
        },
        "uuid_lineage_edges": [{**provider_edge(), "sourced_fact_id": "authored-rename"}],
    }


class PreparedAssertionRejectionTests(TestCase):
    def require(self, new, old=None):
        prepared_recovery.require_assertion_free_outputs(
            {"AAA": {"evidence": {}, **new}}, {"AAA": {"evidence": {}, **(old or {})}}, {}
        )

    def test_new_public_common_certificates_cannot_enter_a_candidate(self):
        value = researched_outputs()["public_common_continuation_membership"]
        with self.assertRaisesRegex(LineageError, "researched ownership"):
            self.require({"public_common_continuation_membership": value})

    def test_new_source_bounds_cannot_extend_a_legacy_common_recipe(self):
        field = "public_common_continuation_ownership"
        old = researched_outputs()[field]
        changed = {**old, "last": "2024-01-08"}
        with self.assertRaisesRegex(LineageError, "researched ownership"):
            self.require({field: changed}, {field: old})

    def test_new_nonunion_conversion_terms_also_reject(self):
        value = researched_outputs()["security_relations"]
        with self.assertRaisesRegex(LineageError, "researched ownership"):
            self.require({"security_relations": value})

    def test_researched_edge_annotations_and_kinds_cannot_disguise_provider_edges(self):
        for change in (
            {"sourced_fact_id": "fact"},
            {"source_fact_id": "fact"},
            *(
                {"edge_type": kind}
                for kind in ("holding_company_succession", "public_common_continuation", "sourced_ticker_change")
            ),
        ):
            with self.subTest(change=change), self.assertRaisesRegex(LineageError, "researched UUID edge"):
                self.require({"uuid_lineage_edges": [{**provider_edge(), **change}]})

    def test_changed_researched_edge_cannot_extend_its_accepted_dates(self):
        old = {**provider_edge(), "sourced_fact_id": "fact"}
        changed = {**old, "effective_date": "2024-01-08"}
        with self.assertRaisesRegex(LineageError, "researched UUID edge"):
            self.require({"uuid_lineage_edges": [changed]}, {"uuid_lineage_edges": [old]})

    def test_unchanged_closed_outputs_remain_accepted_without_recertification(self):
        old = researched_outputs()
        frozen = deepcopy(old)
        with (
            patch.object(
                prepared_recovery, "validate_handoff", side_effect=AssertionError("closed handoff revalidated")
            ),
            patch.object(
                prepared_recovery,
                "accept_observation_choices",
                side_effect=AssertionError("closed numerical choice reaccepted"),
            ),
        ):
            self.require(deepcopy(old), old)
        self.assertEqual(frozen, old)

    def test_new_generic_provider_edges_and_empty_diagnostics_remain_admissible(self):
        for kind in ("name_change", "forward_split", "reverse_split", "same_symbol_stock_merger"):
            with self.subTest(kind=kind):
                self.require(
                    {
                        "uuid_lineage_edges": [provider_edge(kind)],
                        "sourced_rename_membership": {"version": 1, "certificates": [], "issues": []},
                        "public_common_continuation_membership": {
                            "version": 1,
                            "certificates": [],
                            "issues": [],
                        },
                        "security_relations": {"version": 1, "relations": [], "issues": []},
                    }
                )

    def test_raw_json_candidate_rejects_before_assignment_acceptance_or_io(self):
        base = {
            "boundary": prepared_recovery.CANDIDATE_BOUNDARY,
            "version": 1,
            "store": "/unused",
            "members": ["AAA"],
            "policy": {"policy_role": "production", "semantic_sha256": "c" * 64},
            "computation_revision": COMPUTATION_REVISION,
            "receipt_generations": {"AAA": "a" * 64},
            "pricing_generations": {"AAA": "b" * 64},
            "baseline_receipts": {"AAA": {"evidence": {}}},
            "candidate_receipts": {"AAA": {"evidence": {}}},
            "candidate_boundaries": {"AAA": {}},
            "comparison": {"comparison": {"mapping_corrections": {}, "candidate_partition_complete": True}},
            "review": {"version": 1, "status": "approved", "conclusion": "Generic completed result."},
        }
        for field, value in researched_outputs().items():
            candidate = deepcopy(base)
            candidate["candidate_receipts"]["AAA"][field] = value
            candidate = json.loads(json.dumps(candidate))
            with (
                self.subTest(field=field),
                patch.object(Path, "open", side_effect=AssertionError("store or provider opened")),
                patch.object(
                    prepared_recovery,
                    "validate_handoff",
                    side_effect=AssertionError("assignment accepted before rejection"),
                ),
                patch.object(
                    prepared_recovery,
                    "accept_observation_choices",
                    side_effect=AssertionError("numerical decision accepted before rejection"),
                ),
            ):
                with self.assertRaisesRegex(LineageError, "researched"):
                    prepared_recovery.accept_candidate(candidate, "/unused")

"""Provider-only source planning preserves accepted work and rejects new facts."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import (
    public_common_continuation,
    security_relations,
    sourced_rename,
)
from libs.alpaca_lineage_build import source_work
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_lineage_build.work_contract import WorkPolicy, obligation, restriction, satisfied


def evidence(identifier="uuid-aaa"):
    asset = {"id": identifier, "symbol": "AAA", "class": "us_equity", "exchange": "NASDAQ"}
    return {
        "asset_lookups": [{"lookup_kind": "symbol", "lookup": "AAA", "asset": asset}],
        "identities": [asset],
        "corporate_actions": [],
        "corporate_action_count": 0,
        "symbol_changes": [],
        "symbol_occupancy_evidence": [],
        "unresolved_cusips": [],
        "query": {},
        "queried_symbols": ["AAA"],
    }


def control():
    held = evidence()
    identity = {"security_id": "accepted-id", "member_alpaca_asset_ids": ["uuid-aaa"], "member_tickers": ["AAA"]}
    prior = {"symbol": "AAA", "evidence": held, "identity_memberships": [identity]}
    return SimpleNamespace(
        store=Path("/synthetic"),
        symbol="AAA",
        source_sha256="generation",
        previous_receipt_generation="receipt",
        previous=prior,
        local={"counts_known": True},
        accepted_reuse=(True, True),
        accepted_provider_evidence=held,
    )


def plan(**options):
    return SourceWorkPlan({}, {}, {}, {}, {}, {}, {}, False, **options)


class GenericSourceWorkTests(TestCase):
    def test_accepted_owners_use_held_provider_view_and_never_research_certifiers(self):
        selected = control()
        after = deepcopy(selected.accepted_provider_evidence)
        cache = {}
        with (
            patch.object(
                source_work, "provider_evidence", side_effect=AssertionError("repeated provider projection")
            ),
            patch.object(
                public_common_continuation, "certify_group", side_effect=AssertionError("research common")
            ),
            patch.object(security_relations, "certify_group", side_effect=AssertionError("research relations")),
            patch.object(sourced_rename, "certify_group", side_effect=AssertionError("research rename")),
        ):
            result = source_work.prepare_group_evidence_and_source_work(
                {"AAA": selected}, {"AAA": after}, {}, cache=cache
            )
        self.assertEqual(result.dispositions, {"AAA": "retained"})
        self.assertIs(result.memberships["uuid-aaa"], selected.previous["identity_memberships"][0])
        with (
            patch.object(
                source_work, "_accepted_owners_apply", side_effect=AssertionError("repeated owner comparison")
            ),
            patch.object(source_work, "reject_assertions", side_effect=AssertionError("repeated acceptance")),
        ):
            self.assertIs(
                source_work.prepare_group_evidence_and_source_work(
                    {"AAA": selected}, {"AAA": after}, {}, cache=cache
                ),
                result,
            )

    def test_changed_provider_mapping_selects_owning_correlation(self):
        selected = control()
        result = source_work.prepare_group_evidence_and_source_work(
            {"AAA": selected}, {"AAA": evidence("competitor")}, {}, cache={}
        )
        self.assertEqual(result.dispositions, {"AAA": "C"})

    def test_authored_facts_cannot_be_new_source_work(self):
        selected = control()
        after = evidence()
        after["sourced_market_occupancy"] = {"facts": [{"authored": True}]}
        with self.assertRaisesRegex(ValueError, "fact injection"):
            source_work.prepare_group_evidence_and_source_work({"AAA": selected}, {"AAA": after}, {}, cache={})

    def test_completed_output_survives_original_provenance_and_actual_invalidation(self):
        selected = control()
        after = evidence()
        after["sourced_market_occupancy"] = {"facts": [{"legacy": True}]}
        recipe = plan(documented_membership={"owners": []})
        policy = SimpleNamespace(semantic_sha256="policy")
        outputs = ({"AAA": {"completed": True}}, {}, {}, {})
        scope = source_work._source_work_scope({"AAA": selected}, {"AAA": after}, {}, policy, recipe)
        cache = {"completed_source_work": {scope: {"outputs": outputs}}}
        with (
            patch.object(
                source_work, "_execute_source_work", side_effect=AssertionError("completed output replay")
            ),
            patch.object(
                source_work, "reject_assertions", side_effect=AssertionError("completed provenance audit")
            ),
        ):
            self.assertIs(
                source_work.execute_source_work(
                    "/synthetic",
                    "/root",
                    {"AAA": selected},
                    {"AAA": after},
                    {},
                    recipe,
                    policy,
                    cache,
                    loader=None,
                ),
                outputs,
            )
        selected.source_sha256 = "changed"
        with self.assertRaisesRegex(LineageError, "research recipe"):
            source_work.execute_source_work(
                "/synthetic", "/root", {"AAA": selected}, {"AAA": after}, {}, recipe, policy, cache, loader=None
            )

    def test_completed_negative_scope_is_consumed_but_added_unknown_rows_reach_inference(self):
        from libs.alpaca_lineage_build import transform
        from libs.alpaca_pricing_download.contract import ADJUSTMENTS

        class CompletedStage(Exception):
            pass

        class FreshStage(Exception):
            pass

        for current_rows in (1, 2):
            selected = control()
            selected.source = {}
            report = {
                "source_row_count": len(ADJUSTMENTS),
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": len(ADJUSTMENTS), "segments": []},
            }
            empty = {"version": 1, "certificates": [], "issues": []}
            relations = {"version": 1, "relations": [], "issues": []}
            prepared = SourceWorkPlan(
                {"AAA": "B"}, {}, empty, empty, relations, relations, {}, False, prepared_reports={"AAA": report}
            )
            view = SimpleNamespace(local={"downloads": [{"rows": current_rows} for _ in ADJUSTMENTS]})
            retained = SimpleNamespace(accepted_report=report)
            with (
                self.subTest(current_rows=current_rows),
                patch.object(source_work, "closed_source", return_value=retained),
                patch.object(transform, "correlate", side_effect=FreshStage("unassessed new scope")) as infer,
                patch.object(
                    source_work.source_observations,
                    "reconcile",
                    side_effect=CompletedStage("completed negative"),
                ) as consume,
                self.assertRaises(CompletedStage if current_rows == 1 else FreshStage),
            ):
                source_work._decide_source_work(
                    "/synthetic",
                    "/root",
                    {"AAA": selected},
                    {"AAA": evidence()},
                    {},
                    prepared,
                    None,
                    {},
                    loader=lambda *a, **k: view,
                )
            self.assertEqual(current_rows != 1, bool(infer.call_count))
            self.assertEqual(current_rows == 1, bool(consume.call_count))
            self.assertEqual(current_rows == 1, getattr(view, "completed_source_metadata", False))

    def test_append_choice_scope_consumes_pricing_approval_and_its_invalidation(self):
        class SelectedStage(Exception):
            pass

        for accepted in (True, False):
            selected = control()
            selected.source = {"latest_request": {"asof": "new-mapping"}}
            selected.source_sha256 = "new-generation"
            selected.accepted_reuse = False, accepted
            selected.previous["serving_ownership"] = {"completed": True}
            report = {
                "source_row_count": 1,
                "correlated": {"row_count": 0, "segments": []},
                "uncorrelated": {"row_count": 1, "segments": []},
            }
            empty = {"version": 1, "certificates": [], "issues": []}
            relations = {"version": 1, "relations": [], "issues": []}
            prepared = SourceWorkPlan(
                {"AAA": "B"}, {}, empty, empty, relations, relations, {}, False, prepared_reports={"AAA": report}
            )
            view = SimpleNamespace(local={"downloads": [{"rows": 1}]})
            with (
                self.subTest(accepted_prefix=accepted),
                patch.object(source_work, "closed_source", return_value=SimpleNamespace(accepted_report=report)),
                patch.object(
                    source_work, "unchanged_rows", side_effect=AssertionError("pricing approval repeated")
                ),
                patch.object(
                    source_work, "continuing_request", side_effect=AssertionError("mapping approval repeated")
                ),
                patch.object(source_work, "accepted_donor_grids", return_value=set()),
                patch.object(source_work, "accepted_choices", return_value="completed choices") as choices,
                patch.object(source_work.source_observations, "reconcile", side_effect=SelectedStage) as consume,
                self.assertRaises(SelectedStage),
            ):
                source_work._decide_source_work(
                    "/synthetic",
                    "/root",
                    {"AAA": selected},
                    {"AAA": evidence()},
                    {},
                    prepared,
                    None,
                    {},
                    loader=lambda *a, **k: view,
                )
            self.assertEqual({"AAA"} if accepted else set(), set(consume.call_args.kwargs["retained"]))
            self.assertEqual(accepted, bool(choices.call_count))


class GenericWorkObligationTests(TestCase):
    def test_unfinished_research_requirement_blocks_only_affected_group(self):
        item = {
            "status": "pending",
            "end_exclusive": "2026-11-13",
            "required_sourced_facts": {"legacy": "accepted"},
        }
        entries = {
            name: {"dependencies": [], "asset_ids": [], "group_members": [name]} for name in ("AAA", "BBB")
        }
        policy = WorkPolicy("/synthetic", {"symbols": entries}, {"AAA": item}, [], "2026-11-13")
        self.assertEqual(
            restriction(item, "2026-11-13", retry_blocked=True), "generic_lineage_recovery_required"
        )
        self.assertFalse(policy.allowed({"AAA"}))
        self.assertTrue(policy.allowed({"BBB"}))
        with self.assertRaisesRegex(LineageError, "explicit generic lineage recovery"):
            obligation(item)

    def test_completed_ownerless_publication_acknowledges_without_fact_inventory(self):
        item = {
            "status": "pending",
            "obligation_id": "accepted",
            "end_exclusive": "2026-11-13",
            "required_sourced_facts": {"legacy": "accepted"},
        }
        entry = {
            "projection": {
                "processed_through": "2026-11-13",
                "update_obligation_id": "accepted",
                "unresolved_rows": 8,
            }
        }
        self.assertTrue(satisfied(item, entry, False))


class ServingAcceptedStatusTests(TestCase):
    def test_status_consumes_the_selected_pending_map_without_boundary_reread(self):
        from libs.alpaca_serving_catalogue import producer

        controls = {
            "state": {"generation": "selected", "complete": True},
            "groups": {"held": {"online_identities": 1, "ineligible_identities": 0}},
        }
        with patch.object(producer.work, "read", side_effect=AssertionError("completed serving work reread")):
            result = producer.status("/synthetic", controls=controls, pending={"AAA": "accepted"})
        self.assertEqual(["AAA"], result["pending_symbols"])
        self.assertEqual("selected", result["generation"])

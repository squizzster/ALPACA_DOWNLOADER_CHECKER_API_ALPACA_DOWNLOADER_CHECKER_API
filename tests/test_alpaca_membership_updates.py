"""Generic containing-component updates retain scoped producer decisions."""

import copy
import json
import tempfile
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_component_refinement as fixtures
from test_alpaca_lineage_build import IdentityClient, asset
from test_alpaca_pricing_download import FakeClient, bar, direct_adjustment_observations

from libs.alpaca_end_of_day_security_correlation import component_refinement
from libs.alpaca_end_of_day_security_correlation.security_identity import security_identity_for_alpaca_assets
from libs.alpaca_lineage_build import (
    membership_prices,
    membership_updates,
    metadata,
    observations,
    pipeline,
    source_work,
    storage,
    transform,
)
from libs.alpaca_lineage_build.context import OperationContext
from libs.alpaca_lineage_build.contract import LineageError
from libs.alpaca_lineage_build.pricing import metadata_source_control
from libs.alpaca_lineage_build.recovery_handoffs import operation
from libs.alpaca_lineage_build.source_observations import _protect_reports
from libs.alpaca_lineage_build.source_work_contract import SourceWorkPlan
from libs.alpaca_lineage_debug.paired import _retained
from libs.alpaca_pipeline.codec import Decoder, Encoder
from libs.alpaca_pipeline.state import export_state, import_state
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.download import download_pricing
from libs.alpaca_pricing_download.storage import load_source


def fixture():
    reports, evidence, selected = fixtures.fixture()
    controls = {
        name: SimpleNamespace(
            symbol=name,
            store="/synthetic",
            source={},
            source_sha256=selected["pricing_generations"][name],
            previous=selected["receipts"][name],
            previous_receipt_generation=selected["receipt_generations"][name],
            accepted_reuse=(True, True),
            local={},
        )
        for name in reports
    }
    identities = {
        identifier: identity
        for receipt in selected["receipts"].values()
        for identity in receipt["identity_memberships"]
        for identifier in identity["member_alpaca_asset_ids"]
    }
    plan = SourceWorkPlan(dict.fromkeys(reports, "retained"), identities, {}, {}, {}, {}, {}, False)
    ruleset = SimpleNamespace(membership_refinement=component_refinement.NAME, execution_sha256="enabled")
    return reports, evidence, selected, controls, plan, ruleset


class MembershipUpdateTests(TestCase):
    def test_bound_unavailable_peer_does_not_erase_supported_retained_pair(self):
        reports, evidence, selected, controls, plan, ruleset = fixture()
        pairs = component_refinement.membership_pairs(selected["receipts"])
        auxiliary = {"RETIRED": {"asset_lookups": []}}
        current = membership_updates.prepare(
            controls, evidence, auxiliary, plan, ruleset, {}, reports.__getitem__, pairs=pairs
        )
        self.assertEqual({change["source"] for change in current.membership_refinement["changes"]}, {"OLD"})
        # An unassessed auxiliary or genuinely changed source receipt still
        # needs normal group work before a new retained decision can be made.
        self.assertIs(
            membership_updates.prepare(controls, evidence, auxiliary, plan, ruleset, {}, self.fail), plan
        )
        controls["NEW"].accepted_reuse = (False, False)
        self.assertIs(
            membership_updates.prepare(controls, evidence, auxiliary, plan, ruleset, {}, self.fail, pairs=pairs),
            plan,
        )

    def test_fresh_completed_edges_feed_the_shared_hook_and_exact_invalidation(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        days = ("2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20")
        rows = tuple((day + "T04:00:00Z",) for day in days)
        inputs = {"OLD": SimpleNamespace(views=dict.fromkeys(ADJUSTMENTS, rows))}
        with (
            patch(
                "libs.alpaca_end_of_day_security_correlation.uuid_lineage.build_uuid_lineage_edges",
                side_effect=AssertionError("fresh edge recertified"),
            ),
            patch(
                "libs.alpaca_end_of_day_security_correlation.security_identity.security_identity_for_alpaca_assets",
                side_effect=AssertionError("fresh component rebuilt"),
            ),
        ):
            actual, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
            invalidated = membership_updates.invalidations(inputs, actual, trace)
        self.assertEqual(set(invalidated), set(days))
        self.assertTrue(all(value == {"old", "current"} for value in invalidated.values()))
        self.assertEqual(
            actual["OLD"]["correlated"]["segments"][0]["canonical_security"]["alpaca_asset_id"], "a"
        )

    def test_invalidation_uses_new_york_session_and_one_shared_timestamp_projection(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        actual, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
        rows = tuple(
            (value,)
            for value in (
                "2023-06-15T01:00:00Z",
                "2023-06-16T01:00:00Z",
                "2023-06-17T01:00:00Z",
                "2023-06-21T01:00:00Z",
            )
        )
        inputs = {"OLD": SimpleNamespace(views=dict.fromkeys(ADJUSTMENTS, rows))}
        instants = {}
        with patch.object(membership_updates, "timestamp", wraps=membership_updates.timestamp) as parse:
            result = membership_updates.invalidations(inputs, actual, trace, instants=instants)
        self.assertEqual(set(result), {"2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20"})
        self.assertEqual(parse.call_count, 4)
        with patch.object(
            membership_updates, "timestamp", side_effect=AssertionError("held timestamp reparsed")
        ):
            self.assertEqual(result, membership_updates.invalidations(inputs, actual, trace, instants=instants))

    def test_disabled_producer_performs_no_screen_proof_or_report_read(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        ruleset.membership_refinement = None
        with patch.object(membership_updates, "membership_pairs", side_effect=AssertionError("screen")):
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, {}, lambda _: self.fail("assignment read")
            )
        self.assertIs(actual, plan)
        self.assertEqual(set(reports), set(actual.dispositions))

    def test_optional_missing_membership_capability_preserves_good_reports_and_reports_unknown_scope(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        selected["receipts"]["OLD"].pop("identity_memberships")
        with patch.object(membership_updates, "refine", side_effect=AssertionError("legacy membership rebuilt")):
            actual, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertIs(actual, reports)
        self.assertFalse(trace["changes"])
        self.assertEqual(trace["applicability"], "unknown")
        self.assertEqual(trace["issues"], [{"reason": "accepted_membership_scope_unknown", "sources": ["OLD"]}])
        with self.assertRaises(LineageError):
            membership_updates.update_reports(reports, evidence, selected, ruleset, completed={})

    def test_retained_containing_component_is_scoped_and_cached(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        cache = {}
        with patch.object(component_refinement, "_extends", wraps=component_refinement._extends) as proof:
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, cache, reports.__getitem__
            )
        self.assertEqual(1, proof.call_count)
        self.assertEqual(actual.dispositions, {"OLD": "B", "NEW": "retained"})
        self.assertEqual(actual.membership_refinement["changed_source_view_rows"], {"OLD": 32})
        with patch.object(membership_updates, "refine", side_effect=AssertionError("completed proof repeated")):
            repeated = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, cache, lambda _: self.fail("assignment repeated")
            )
        self.assertIs(repeated, actual)
        self.assertEqual(actual.membership_refinement["input_evidence"], evidence)

    def test_completed_applicability_crosses_json_without_repeated_proof(self):
        reports, evidence, selected, controls, plan, ruleset = fixture()
        pair_scope = tuple(sorted(selected["receipt_generations"].items()))
        edge_scope = "NEW", selected["receipt_generations"]["NEW"], id(evidence["NEW"])
        cache = {
            "legacy_membership_pairs": {pair_scope: component_refinement.membership_pairs(selected["receipts"])},
            "legacy_membership_edges": {edge_scope: (evidence["NEW"], reports["NEW"]["uuid_lineage_edges"])},
        }
        membership_updates.prepare(controls, evidence, {}, plan, ruleset, cache, reports.__getitem__)
        context = OperationContext("/synthetic")
        owner = object()
        context.sourced_operations[owner] = {"source_work": cache}
        encoder = Encoder()
        packed = export_state(context, None, encoder.encode, evidence=owner)
        objects, packed = json.loads(json.dumps([encoder.objects, packed]))
        replacement = object()
        restored, _ = import_state("/synthetic", packed, Decoder(objects).decode, evidence=replacement)
        cache = restored.sourced_operations[replacement]["source_work"]
        accepted = next(iter(cache["membership_plans"].values()))
        evidence = accepted.membership_refinement["input_evidence"]
        restored_edge_scope = "NEW", selected["receipt_generations"]["NEW"], id(evidence["NEW"])
        self.assertIs(cache["legacy_membership_edges"][restored_edge_scope][0], evidence["NEW"])
        self.assertEqual(cache["legacy_membership_pairs"][pair_scope][0]["donor"], "NEW")
        with patch.object(membership_updates, "refine", side_effect=AssertionError("transport repeated proof")):
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, cache, lambda _: self.fail("transport reread assignments")
            )
        self.assertIs(actual, accepted)

    def test_explicit_research_plan_consumes_no_route_or_edge_proof(self):
        reports, evidence, selected, controls, plan, ruleset = fixture()
        expected, trace = component_refinement.refine(reports, evidence, selected)
        completed = {"store": "/synthetic", "plan": trace}
        with (
            patch.object(membership_updates, "refine", side_effect=AssertionError("rule replay")),
            patch.object(membership_updates, "membership_pairs", side_effect=AssertionError("screen replay")),
            patch.object(component_refinement, "_extends", side_effect=AssertionError("edge replay")),
            patch.object(component_refinement, "_same_issue_route", side_effect=AssertionError("route replay")),
        ):
            actual = membership_updates.prepare(
                controls, evidence, {}, plan, ruleset, {}, reports.__getitem__, completed=completed
            )
        for old, new in zip(
            expected["OLD"]["correlated"]["segments"], actual.prepared_reports["OLD"]["correlated"]["segments"]
        ):
            self.assertEqual(
                old, {key: value for key, value in new.items() if key != "completed_membership_refinement"}
            )

    def test_actual_changed_store_or_generation_rejects_import_without_reproof(self):
        reports, _, selected, _, _, _ = fixture()
        _, trace = component_refinement.refine(*fixtures.fixture())
        selected["store"] = "/synthetic"
        for changed in ("store", "pricing", "receipt", "donor"):
            current = copy.deepcopy(selected)
            completed = {"store": "/elsewhere" if changed == "store" else "/synthetic", "plan": trace}
            if changed == "pricing":
                current["pricing_generations"]["OLD"] = "changed"
            elif changed == "receipt":
                current["receipt_generations"]["OLD"] = "changed"
            elif changed == "donor":
                current["receipt_generations"]["NEW"] = "changed"
            with self.subTest(changed=changed), self.assertRaises(LineageError):
                membership_updates.apply_completed(reports, current, completed)

    def test_missing_edges_preserve_accepted_owner_and_cached_negative(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        reports["NEW"]["uuid_lineage_edges"] = []
        cache = {}
        actual = membership_updates.prepare(controls, evidence, {}, plan, ruleset, cache, reports.__getitem__)
        self.assertFalse(actual.membership_refinement["changes"])
        self.assertEqual(actual.dispositions, plan.dispositions)
        with patch.object(membership_updates, "refine", side_effect=AssertionError("negative replay")):
            self.assertIs(
                actual,
                membership_updates.prepare(
                    controls, evidence, {}, plan, ruleset, cache, lambda _: self.fail("negative read")
                ),
            )

    def test_partial_source_scope_is_not_a_whole_identity_rebind(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        late = copy.deepcopy(reports["OLD"]["correlated"]["segments"][0])
        late.update(row_start=5, row_end=5, first_session_date="2023-06-21", last_session_date="2023-06-21")
        reports["OLD"]["correlated"]["segments"].append(late)
        actual = membership_updates.prepare(controls, evidence, {}, plan, ruleset, {}, reports.__getitem__)
        selected = source_work.closed_source(controls["OLD"], actual).accepted_report
        self.assertEqual(selected["correlated"]["segments"][-1]["security_identity"]["security_id"], "old")
        self.assertEqual(selected["correlated"]["segments"][0]["security_identity"]["security_id"], "current")

    def test_only_changed_source_positions_reopen_numerical_selection(self):
        reports, evidence, selected, controls, plan, ruleset = fixture()
        actual = membership_updates.prepare(controls, evidence, {}, plan, ruleset, {}, reports.__getitem__)
        choices = _retained(reports, [], selected)
        retained = {
            name: (
                {
                    "views": {
                        adjustment: {"ownership": [{"rows": 4, "owner": [owner, [name]]}]}
                        for adjustment in ADJUSTMENTS
                    }
                },
                {owner: receipt["identity_memberships"][0]},
                set(),
                choices[name],
            )
            for name, receipt in selected["receipts"].items()
            for owner in (receipt["identity_memberships"][0]["security_id"],)
        }
        active, protected = _protect_reports(actual.prepared_reports, retained)
        self.assertEqual(len(active["OLD"]["correlated"]["segments"]), 8)
        self.assertFalse(protected["OLD"])
        self.assertFalse(active["NEW"]["correlated"]["segments"])
        self.assertEqual(len(protected["NEW"]), 8)

    def test_public_source_work_entry_invokes_opt_in_owner(self):
        reports, evidence, _, controls, plan, ruleset = fixture()
        ruleset.policy = SimpleNamespace()
        with (
            patch.object(source_work, "_prepare_group_evidence_and_source_work", return_value=plan),
            patch.object(
                source_work,
                "retained_ownership_report",
                side_effect=lambda prior, *_: reports["OLD" if prior is controls["OLD"].previous else "NEW"],
            ),
            patch.object(
                source_work,
                "_membership_sessions",
                return_value=lambda name, segment: tuple(
                    (day, day + "T04:00:00Z") for day in ("2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20")
                ),
            ),
            patch("libs.alpaca_lineage_build.transform.retained_decision_report", return_value={}),
        ):
            actual = source_work.prepare_group_evidence_and_source_work(
                controls, evidence, {}, cache={}, ruleset=ruleset
            )
        self.assertEqual(actual.dispositions["OLD"], "B")
        self.assertEqual(Path(controls["OLD"].store), Path("/synthetic"))


class OldRowsClient(FakeClient):
    def fetch(self, symbol, adjustment, start, end, *, asof=None):
        rows = super().fetch(symbol, adjustment, start, end, asof=asof)
        return [row for row in rows if symbol != "OLD" or row["t"][:10] < "2024-01-08"]


class CurrentDonorTests(TestCase):
    def fixture(self):
        reports, evidence, selected, _, _, ruleset = fixture()
        selected["store"] = "/synthetic"
        days = ["2023-06-14", "2023-06-15", "2023-06-16", "2023-06-20"]
        raw = tuple((day + "T04:00:00Z",) for day in days)
        selected["inputs"] = {name: SimpleNamespace(views=dict.fromkeys(ADJUSTMENTS, raw)) for name in reports}
        reports["NEW"]["lineage_evidence_sha256"] = "current-donor-evidence"
        return reports, evidence, selected, ruleset

    def test_same_issue_alias_continues_and_real_branch_competes(self):
        reports, evidence, selected, ruleset = self.fixture()
        split = reports["NEW"]["uuid_lineage_edges"][0]
        earlier = {**split["predecessor"], "symbol": "OLD"}
        alias = {**split, "edge_type": "name_change", "predecessor": earlier, "successor": split["predecessor"]}
        reports["NEW"]["uuid_lineage_edges"].append(alias)
        _, current = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(current["changed_source_view_rows"], {"OLD": 32})
        initial = {**earlier, "symbol": "INITIAL"}
        first_alias = {
            **alias,
            "predecessor": initial,
            "successor": earlier,
            "effective_date": "2026-11-02",
        }
        reports["NEW"]["uuid_lineage_edges"].append(first_alias)
        _, chain = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(chain["changed_source_view_rows"], {"OLD": 32})
        first_alias["successor"] = split["predecessor"]
        _, branch = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(branch["changes"], [])
        reports["NEW"]["uuid_lineage_edges"].pop()
        reports["NEW"]["uuid_lineage_edges"].append(
            {**split, "successor": {**split["successor"], "asset_id": "elsewhere"}}
        )
        _, competing = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(competing["changes"], [])

    def test_completed_new_donor_survives_first_seen_or_expanded_membership(self):
        for previous in (None, {"identity_memberships": [fixtures.identity("old", ["a", "b"])]}):
            with self.subTest(previous=previous):
                reports, evidence, selected, ruleset = self.fixture()
                selected["receipts"]["NEW"] = previous
                selected["receipt_generations"]["NEW"] = None if previous is None else "earlier"
                candidate, trace = membership_updates.update_reports(reports, evidence, selected, ruleset)
                self.assertEqual(trace["changed_source_view_rows"], {"OLD": 32})
                handoff = {"store": "/synthetic", "plan": json.loads(json.dumps(trace))}
                with patch.object(component_refinement, "_extends", side_effect=AssertionError("reproved")):
                    recovered, _ = membership_updates.update_reports(
                        reports, evidence, selected, ruleset, completed=handoff
                    )
                self.assertEqual(candidate, recovered)
                replacement = {**reports, "NEW": {**reports["NEW"], "lineage_evidence_sha256": "changed"}}
                with self.assertRaisesRegex(LineageError, "metadata changed"):
                    membership_updates.apply_completed(replacement, selected, handoff)

    def test_append_uses_fresh_split_to_unite_accepted_prefix_with_only_its_new_suffix(self):
        prefix, evidence, selected, ruleset = self.fixture()
        selected["components"] = dict.fromkeys(prefix, set(prefix))
        old = prefix["OLD"]["correlated"]["segments"][0]["security_identity"]
        target = prefix["NEW"]["correlated"]["segments"][0]["security_identity"]
        for name in prefix:
            selected["receipts"][name] = {"identity_memberships": [old], "source_rows": 32}
        for segment in prefix["NEW"]["correlated"]["segments"]:
            segment["security_identity"] = old
        suffix = copy.deepcopy(prefix)
        for name, report in suffix.items():
            if name == "OLD":
                report["correlated"] = {"segments": [], "row_count": 0}
            else:
                for segment in report["correlated"]["segments"]:
                    segment.update(
                        row_start=1,
                        row_end=1,
                        first_session_date="2026-11-10",
                        last_session_date="2026-11-10",
                        historical_symbol="NEW",
                        security_identity=target,
                    )
        full = selected["inputs"]
        full["NEW"].views = {
            view: rows + (("2026-11-10T05:00:00Z",),) for view, rows in full["NEW"].views.items()
        }
        inputs = {
            name: SimpleNamespace(
                materialization_scope="retained_suffix",
                previous_assignment_summary={adj: {"rows": 4} for adj in ADJUSTMENTS},
            )
            for name in full
        }
        # An unrelated group member can never cause an unrelated prefix read.
        controls = []

        def prices(name, retained):
            controls.append(name)
            return full[name]

        combined, materialized = membership_updates.compose_growing_prefixes(
            suffix, selected, evidence, inputs, load_prefix=prefix.__getitem__, load_prices=prices
        )
        self.assertEqual(controls, ["NEW", "OLD"])
        actual, trace = membership_updates.update_reports(
            combined, evidence, {**selected, "inputs": materialized}, ruleset
        )
        self.assertEqual(trace["changed_source_view_rows"], {"OLD": 32})
        self.assertEqual(
            {s["security_identity"]["security_id"] for s in actual["NEW"]["correlated"]["segments"]}, {"current"}
        )
        self.assertEqual(
            [
                (s["row_start"], s["row_end"])
                for s in actual["NEW"]["correlated"]["segments"]
                if s["adjustment"] == "raw"
            ],
            [(1, 4), (5, 5)],
        )
        selected["receipts"]["NEW"]["identity_memberships"] = [target]
        unchanged = membership_updates.compose_growing_prefixes(
            suffix,
            selected,
            evidence,
            inputs,
            load_prefix=lambda _: self.fail("unchanged prefix inspected"),
            load_prices=lambda *_: self.fail("unchanged prices reopened"),
        )
        self.assertIs(unchanged[1], inputs)

    def test_advancing_legacy_group_and_disjoint_dates(self):
        prefix, evidence, selected, ruleset = self.fixture()
        selected["components"] = dict.fromkeys(prefix, set(prefix))
        suffix = copy.deepcopy(prefix)
        for report in suffix.values():
            report["correlated"] = {"segments": [], "row_count": 0}
        edges = suffix["NEW"].pop("uuid_lineage_edges")
        for prior in selected["receipts"].values():
            prior.update(
                versions={"correlation": 93}, input_sources={"OLD": "before", "NEW": "before"}, source_rows=32
            )
        full = selected["inputs"]
        sources = {
            name: SimpleNamespace(
                materialization_scope="retained_suffix",
                previous_assignment_summary={adj: {"rows": 4} for adj in ADJUSTMENTS},
            )
            for name in full
        }
        recoveries = []

        def recover(report, donor):
            recoveries.append(donor)
            return {**report, "uuid_lineage_edges": edges}

        combined, materialized = membership_updates.compose_growing_prefixes(
            suffix,
            selected,
            evidence,
            sources,
            load_prefix=prefix.__getitem__,
            load_prices=lambda name, retained: full[name],
            recover_edges=recover,
        )
        actual, trace = membership_updates.update_reports(
            combined, evidence, {**selected, "inputs": materialized}, ruleset
        )
        self.assertEqual(recoveries, ["NEW"])
        self.assertEqual(trace["changed_source_view_rows"], {"OLD": 32})
        self.assertEqual(actual["NEW"]["uuid_lineage_edges"], edges)
        for segment in prefix["NEW"]["correlated"]["segments"]:
            segment.update(first_session_date="2026-11-01", last_session_date="2026-11-09")

        def no_prices(*_):
            self.fail("disjoint prefix opened pricing")

        for fresh_target in (False, True):
            if fresh_target:
                suffix["NEW"] = prefix["NEW"]
                selected["receipts"]["NEW"]["identity_memberships"] = [
                    prefix["OLD"]["correlated"]["segments"][0]["security_identity"]
                ]
            _, inputs = membership_updates.compose_growing_prefixes(
                suffix,
                selected,
                evidence,
                sources,
                load_prefix=prefix.__getitem__,
                load_prices=no_prices,
                recover_edges=lambda *args: self.fail("disjoint prefix rebuilt edge"),
            )
            self.assertIs(inputs, sources)

    def test_retained_membership_uses_market_day_at_midnight(self):
        stamp = "2024-07-03T01:00:00Z"
        controls = {
            "OLD": SimpleNamespace(
                store="/s",
                previous={
                    "anchor_month": "2024-07",
                    "serving_ownership": {"offsets": {"raw": {"current_month": 0}}},
                },
            )
        }
        rows = (("2024-07-01T04:00:00Z",), (stamp,), ("2024-07-03T04:00:00Z",))
        read = source_work._membership_sessions(controls, {})
        seg = dict(
            adjustment="raw",
            row_start=1,
            row_end=3,
            first_timestamp=rows[0][0],
            last_timestamp=rows[-1][0],
            first_session_date="2024-07-01",
            last_session_date="2024-07-03",
        )
        with patch.object(source_work.source_observations, "counterpart_partition", return_value={"raw": rows}):
            self.assertEqual(read("OLD", seg)[1], ("2024-07-02", stamp))

    def test_legacy_researched_edge_cannot_authorize_a_new_membership(self):
        reports, evidence, selected, ruleset = self.fixture()
        edge = reports["NEW"]["uuid_lineage_edges"][0]
        edge["sourced_fact_id"] = edge.pop("corporate_action_id")
        _, decision = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(decision["changes"], [])

    def test_reclassified_prefix_reads_only_the_contested_partition_across_transport(self):
        cold, first, middle, last, added = (
            (day + "T05:00:00Z", value)
            for value, day in enumerate(("2023-06-01", "2024-01-10", "2024-01-11", "2024-01-12", "2024-02-10"))
        )
        prior = {"correlated": {"segments": []}, "uncorrelated": {"segments": []}}
        for adj in ADJUSTMENTS:
            for start, end, left, right in ((1, 1, cold, cold), (2, 4, first, last)):
                prior["correlated"]["segments"].append(
                    dict(
                        adjustment=adj,
                        row_start=start,
                        row_end=end,
                        first_session_date=left[0][:10],
                        last_session_date=right[0][:10],
                        first_timestamp=left[0],
                        last_timestamp=right[0],
                    )
                )
        suffix = SimpleNamespace(
            symbol="NEW",
            source={
                "anchor_month": "2024-02",
                "boundaries": {
                    kind: {"sha256": kind, "manifest": kind}
                    for kind in ("completed_years", "current_year", "current_month")
                },
            },
            views=dict.fromkeys(ADJUSTMENTS, (added,)),
            previous_assignment_summary=dict.fromkeys(
                ADJUSTMENTS, {"rows": 4, "last_timestamp": last[0], "last_session": last[0][:10]}
            ),
        )
        reads = []

        def selected_partition(root, source, kind, *, selected, documents):
            self.assertEqual(kind, "current_year")
            reads.append(kind)
            documents[kind] = {"selected": kind}
            return dict.fromkeys(ADJUSTMENTS, (first, middle, last))

        with patch.object(source_work.source_observations, "load_published_boundary", selected_partition):
            view = membership_prices.compose("/synthetic", suffix, prior, {})
            indexes = transform._row_indexes(view)
            for adj, rows in view.views.items():
                self.assertEqual((rows.timestamp_at(0), rows.timestamp_at(3)), (cold[0], last[0]))
                self.assertEqual(indexes[adj].months[0], "2023-06")
                self.assertEqual(rows[4], added)
            self.assertEqual(reads, [])
            self.assertEqual(view.views["raw"][2], middle)
            encoder = Encoder()
            token = encoder.encode(view.views)
            held = Decoder(json.loads(json.dumps(encoder.objects))).decode(token)
            for rows in held.values():
                self.assertEqual(rows[2], middle)
                self.assertEqual(rows.timestamp_at(0), cold[0])
            self.assertEqual(reads, ["current_year"])

    def test_segment_boundaries_and_missing_interior_session(self):
        reports, evidence, selected, ruleset = self.fixture()
        fragments = []
        for segment in reports["NEW"]["correlated"]["segments"]:
            fragments.extend(
                [
                    {**segment, "row_end": 2, "last_session_date": "2023-06-15"},
                    {**segment, "row_start": 3, "first_session_date": "2023-06-16"},
                ]
            )
        reports["NEW"]["correlated"]["segments"] = fragments
        _, full = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(full["changed_source_view_rows"], {"OLD": 32})
        # A longer enclosing donor range lacks the source's June 15 session.
        rows = tuple((day + "T04:00:00Z",) for day in ("2023-06-13", "2023-06-14", "2023-06-16", "2023-06-20"))
        selected["inputs"]["NEW"].views = dict.fromkeys(ADJUSTMENTS, rows)
        for segment in fragments[::2]:
            segment.update(first_session_date="2023-06-13", last_session_date="2023-06-14")
        result, partial = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(partial["changed_source_view_rows"], {"OLD": 24})
        raw = [s for s in result["OLD"]["correlated"]["segments"] if s["adjustment"] == "raw"]
        self.assertEqual(
            [(s["row_start"], s["row_end"], s["security_identity"]["security_id"]) for s in raw],
            [(1, 1, "current"), (2, 2, "old"), (3, 4, "current")],
        )
        recovered, _ = membership_updates.update_reports(
            reports, evidence, selected, ruleset, completed={"store": "/synthetic", "plan": partial}
        )
        self.assertEqual(recovered, result)
        self.assertEqual(
            set(membership_updates.invalidations({}, result, partial)),
            {"2023-06-14", "2023-06-16", "2023-06-20"},
        )
        selected["components"] = {"OLD": {"OLD"}, "NEW": {"NEW"}}
        _, separate = membership_updates.update_reports(reports, evidence, selected, ruleset)
        self.assertEqual(separate["changes"], [])


class ProductionMembershipTests(TestCase):
    def test_researched_four_session_plan_publishes_and_retry_keeps_completed_choices(self):
        self.exercise()

    def test_construction_interruption_keeps_completed_applicability_and_numerical_decisions(self):
        self.exercise(interrupted=True)

    def test_subsequent_append_preserves_the_repaired_membership(self):
        self.exercise(append=True)

    def test_context_plan_is_consumed_before_unchanged_repeat_and_append(self):
        self.exercise(controller=True, append=True)

    def test_context_plan_consumption_survives_postcommit_json_resume(self):
        self.exercise(controller=True, delivery_json=True)

    def test_success_preserves_a_replaced_context_plan(self):
        self.exercise(controller=True, renewed=True)

    def test_normal_lineage_adopts_legacy_containing_donor_once(self):
        self.exercise(legacy_adoption=True)

    def test_first_ordinary_append_adopts_preexisting_superset_before_publish(self):
        self.exercise(legacy_adoption=True, append=True)

    def test_same_cutoff_update_selects_existing_duplicate_and_closes_it_once(self):
        self.exercise(legacy_adoption=True, same_cutoff=True)

    def test_same_cutoff_update_acknowledges_unsupported_transfer_without_publication(self):
        self.exercise(legacy_adoption=True, same_cutoff=True, declined=True)

    def test_interrupted_negative_assessment_is_acknowledged_without_reassessment(self):
        self.exercise(legacy_adoption=True, same_cutoff=True, declined=True, interrupt_negative=True)

    def exercise(
        self,
        *,
        interrupted=False,
        append=False,
        controller=False,
        delivery_json=False,
        renewed=False,
        legacy_adoption=False,
        same_cutoff=False,
        declined=False,
        interrupt_negative=False,
    ):
        with tempfile.TemporaryDirectory() as directory:
            store = Path(directory) / "store"
            root = store / "end_of_day/lineage"
            old = security_identity_for_alpaca_assets(
                ["a"], [], tickers=["OLD", "NEW"], basis="accepted"
            ).as_report_object()
            target = security_identity_for_alpaca_assets(
                ["a", "b"], [], tickers=["OLD", "NEW"], basis="accepted"
            ).as_report_object()
            days = (
                "2024-01-02",
                "2024-01-03",
                "2024-01-04",
                "2024-01-05",
                "2024-01-08",
                "2024-01-09",
                "2024-01-10",
                "2024-01-11",
            )
            bars = [bar(day, "10") for day in days]
            client = IdentityClient(
                {
                    "OLD": asset("OLD", "a"),
                    "NEW": asset("NEW", "b"),
                    "111111111": asset("NEW" if legacy_adoption else "OLD", "a"),
                    "222222222": asset("NEW", "b"),
                },
                [
                    {
                        "id": "rename",
                        "action_type": "name_change",
                        "old_symbol": "OLD",
                        "new_symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "111111111",
                        "process_date": "2024-01-08",
                    },
                    {
                        "id": "split",
                        "action_type": "reverse_split",
                        "symbol": "NEW",
                        "old_cusip": "111111111",
                        "new_cusip": "222222222",
                        "old_rate": 7,
                        "new_rate": 1,
                        "effective_date": "2024-01-10",
                        "process_date": "2024-01-10",
                    },
                ],
            )
            if declined:
                client.actions = [row for row in client.actions if row["action_type"] != "reverse_split"]
            with (
                patch(
                    "libs.alpaca_pricing_download.download.utc_now",
                    return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                ),
                direct_adjustment_observations(),
            ):
                for name in ("OLD", "NEW"):
                    cutoff = (
                        "2024-01-08"
                        if legacy_adoption and not (append or same_cutoff) and name == "OLD"
                        else "2024-01-12"
                    )
                    download_pricing(store, name, cutoff, from_date="2024-01-01", client=OldRowsClient(bars))
            context, held = OperationContext(store), {}
            if legacy_adoption and (append or same_cutoff):
                from test_alpaca_lineage_batching import Provider

                provider = Provider(actions=client.actions, catalogue=[client.assets["NEW"]])
                provider.assets = client.assets
                client = provider.client
            evidence_client = metadata.EvidenceClient(client)
            if legacy_adoption and (append or same_cutoff):
                evidence_client.observe_catalogue()
            correlate = pipeline.correlate

            def prior_fixture(*args, **kwargs):
                reports = correlate(*args, **kwargs)
                for name, report in reports.items():
                    segments = []
                    identity = old if name == "OLD" else target
                    for adjustment, rows in args[1][name].views.items():
                        for position, row in enumerate(rows, 1):
                            earlier = row[0][:10] < "2024-01-10"
                            label = "OLD" if same_cutoff and row[0][:10] < "2024-01-08" else name
                            segments.append(
                                {
                                    "adjustment": adjustment,
                                    "row_start": position,
                                    "row_end": position,
                                    "row_count": 1,
                                    "first_timestamp": row[0],
                                    "last_timestamp": row[0],
                                    "first_session_date": row[0][:10],
                                    "last_session_date": row[0][:10],
                                    "historical_symbol": label,
                                    "security_identity": identity,
                                    "canonical_security": {
                                        "security_id": identity["security_id"],
                                        "alpaca_asset_id": "a" if earlier else "b",
                                        "cusip": "111111111" if earlier else "222222222",
                                    },
                                    "ticker_ownership_interval": {
                                        "historical_symbol": label,
                                        "start_date_inclusive": "2024-01-01",
                                        "end_date_exclusive": "2024-01-08" if name == "OLD" else None,
                                    },
                                    "correlation_rule": {"name": "accepted-fixture"},
                                    "warnings": [],
                                }
                            )
                    report["correlated"] = {"row_count": len(segments), "segments": segments}
                    report["uncorrelated"] = {"row_count": 0, "segments": []}
                    held[name] = report
                return reports

            initial_dispatch = nullcontext()
            if legacy_adoption and not (append or same_cutoff):
                from libs.alpaca_lineage_build.contract import document_digest
                from libs.alpaca_pricing_download.contract import timestamp

                # The older publication accepted the pair at unequal pricing
                # cutoffs after the retired source's suffix returned no bars.
                start, end = (timestamp(day + "T00:00:00Z") for day in ("2024-01-08", "2024-01-12"))
                self.assertEqual([], OldRowsClient(bars).fetch("OLD", "all", start, end))
                source = load_source(store / "end_of_day/pricing/OLD", "OLD")
                initial_group = pipeline.process_group

                def with_accepted_suffix(*args, **kwargs):
                    kwargs["empty_suffixes"].add(("OLD", document_digest(source), "2024-01-12"))
                    return initial_group(*args, **kwargs)

                initial_dispatch = patch.object(pipeline, "process_group", side_effect=with_accepted_suffix)
            with initial_dispatch, patch.object(pipeline, "correlate", side_effect=prior_fixture):
                initial = pipeline.run_lineage(
                    store,
                    None if legacy_adoption and (append or same_cutoff) else ["NEW", "OLD"],
                    identity_client=client,
                    pricing_client=OldRowsClient(bars),
                    evidence_client=evidence_client,
                    context=context,
                )
            self.assertEqual(initial["failed"], 0, initial)
            index = storage.read_index(root)
            receipts = {name: storage.read_receipt(root, name, index) for name in ("OLD", "NEW")}
            if legacy_adoption:
                from libs.alpaca_lineage_build.contract import document_digest
                from libs.alpaca_lineage_build.ruleset import CANONICAL_POLICY_PATH, load_ruleset

                frozen = load_ruleset(
                    CANONICAL_POLICY_PATH.with_name("alpaca_security_correlation_v93_legacy_policy.yaml")
                ).policy
                entries = {}
                for name, receipt in receipts.items():
                    receipt.pop("uuid_lineage_edges", None)
                    receipt.pop("retained_membership_assessed", None)
                    receipt["versions"] = {
                        **receipt["versions"],
                        "correlation": 93,
                        "correlation_policy_sha256": frozen.semantic_sha256,
                    }
                    receipt["correlation_policy"] = frozen.provenance()
                    storage.atomic_json(root / name / "lineage.manifest.json", receipt)
                    entries[name] = {
                        **index["symbols"][name],
                        "sha256": document_digest(receipt),
                        "versions": receipt["versions"],
                        "correlation_policy": frozen.provenance(),
                    }
                storage._update_index(root, entries)
                if append or same_cutoff:
                    from test_alpaca_universe_update import UniverseUpdateTests

                    from alpaca_end_of_day import DataLibrary
                    from libs.alpaca_lineage_build.check_summary import rebuild
                    from libs.alpaca_lineage_build.universe_update import (
                        plan_update,
                        rebuild_universe,
                        run_update,
                    )
                    from libs.alpaca_pricing_download.http import BarsClient
                    from libs.alpaca_serving_catalogue.producer import maintain

                    rebuild(store)
                    rebuild_universe(store)
                    self.assertTrue(maintain(store, bootstrap=True)["complete"])
                    before = DataLibrary(store=store)
                    original = {
                        view: before.get_history(target["security_id"], view).bars for view in ADJUSTMENTS
                    }
                    if append:
                        bars = [*bars, bar("2024-01-12", "10")]
                    registry = Path(directory) / "ticker.csv"
                    registry.write_text("NEW,OLD\n")
                    if same_cutoff:
                        with patch.object(
                            storage,
                            "publish_membership_screens",
                            side_effect=AssertionError("preview wrote scheduling metadata"),
                        ):
                            preview = plan_update(
                                store, "2024-01-12", identity_client=provider.client, registry=registry
                            )
                        self.assertIn("legacy_membership_overlap", preview["work"]["NEW"]["reasons"])
                    prices = BarsClient("fixture", "fixture")
                    market = UniverseUpdateTests()
                    market.pages, market.live = [], {"OLD": bars[:4], "NEW": bars}
                    with (
                        patch.object(prices, "_get", side_effect=market.page),
                        patch.object(membership_prices, "compose", wraps=membership_prices.compose) as composed,
                        patch.object(
                            pipeline, "scoped_identity_memberships", wraps=pipeline.scoped_identity_memberships
                        ) as selected_memberships,
                    ):

                        def update():
                            return run_update(
                                store,
                                "2024-01-13" if append else "2024-01-12",
                                registry=registry,
                                identity_client=provider.client,
                                pricing_client=prices,
                            )

                        if interrupt_negative:
                            from libs.alpaca_lineage_build import retained_membership

                            accept_negative = retained_membership.complete_negative

                            def interrupt(*args):
                                accept_negative(*args)
                                raise KeyboardInterrupt

                            with (
                                patch.object(retained_membership, "complete_negative", side_effect=interrupt),
                                self.assertRaises(KeyboardInterrupt),
                            ):
                                update()
                            market.pages.clear()
                            with (
                                patch.object(
                                    retained_membership,
                                    "membership_pairs",
                                    side_effect=AssertionError("repeated completed pair selection"),
                                ),
                                patch.object(
                                    source_work,
                                    "prepare_group_evidence_and_source_work",
                                    side_effect=AssertionError("repeated completed ownership assessment"),
                                ),
                            ):
                                adopted = update()
                        else:
                            adopted = update()
                    self.assertTrue(adopted["complete"], adopted)
                    self.assertEqual(adopted["pending_work"], 0)
                    self.assertCountEqual(
                        [(p["symbols"], p["adjustment"], p["start"][:10], p["end"][:10]) for p in market.pages],
                        [
                            ("NEW,OLD", "all", "2024-01-02", "2024-01-03"),
                            ("OLD", "all", "2024-01-03", "2024-01-06"),
                            ("NEW", "all", "2024-01-03", "2024-01-11"),
                            *(
                                [
                                    (label, view, "2024-01-12", "2024-01-12")
                                    for label in ("NEW", "OLD")
                                    for view in ("raw", "all")
                                ]
                                if append
                                else []
                            ),
                        ],
                    )
                    if same_cutoff:
                        self.assertIn("legacy_membership_overlap", adopted["plan"]["work"]["NEW"]["reasons"])
                    decision = (
                        adopted["stages"]["maintenance"]["stages"]["lineage"]["results"]
                        if append
                        else adopted["stages"]["discovery_lineage"][0]["results"]
                    )[0]
                    if declined:
                        self.assertEqual(decision["status"], "unchanged")
                        self.assertEqual(adopted["stages"]["serving_catalogue"]["published_components"], 0)
                        self.assertEqual(
                            storage.read_index(root)["symbols"]["NEW"]["legacy_membership_screen_v94"][
                                "assessed"
                            ],
                            "retained_ownership",
                        )
                    else:
                        self.assertEqual(
                            decision["membership_refinement"]["changed_source_view_rows"], {"OLD": 32}
                        )
                        self.assertEqual(selected_memberships.call_count, int(append))
                        self.assertEqual(
                            {call.args[1].symbol for call in composed.call_args_list},
                            {"OLD", "NEW"} if append else set(),
                            adopted,
                        )
                        current_index = storage.read_index(root)
                        for name in ("NEW", "OLD"):
                            receipt = storage.read_receipt(root, name, current_index)
                            self.assertEqual(
                                {item["security_id"] for item in receipt["identity_memberships"]},
                                {target["security_id"]},
                            )
                        self.assertEqual(adopted["stages"]["serving_catalogue"]["published_components"], 1)
                        after = DataLibrary(store=store)
                        self.assertEqual(
                            [item.public_id for item in after.list_securities()], [target["security_id"]]
                        )
                        for view in ADJUSTMENTS:
                            self.assertEqual(
                                (
                                    after.get_history(target["security_id"], view).bars[:-1]
                                    if append
                                    else after.get_history(target["security_id"], view).bars
                                ),
                                original[view],
                            )
                    if same_cutoff:
                        market.pages.clear()
                        with (
                            patch.object(storage, "publish", side_effect=AssertionError("republished lineage")),
                            patch.object(
                                source_work,
                                "prepare_group_evidence_and_source_work",
                                side_effect=AssertionError("repeated membership assessment"),
                            ),
                            patch(
                                "libs.alpaca_serving_catalogue.membership_selection.read_legacy_overlaps",
                                side_effect=AssertionError("repeated serving range selection"),
                            ),
                            patch.object(prices, "_get", side_effect=market.page),
                        ):
                            repeated = run_update(
                                store,
                                "2024-01-12",
                                registry=registry,
                                identity_client=provider.client,
                                pricing_client=prices,
                            )
                        self.assertTrue(repeated["complete"], repeated)
                        self.assertEqual(repeated["plan"]["work"], {})
                        self.assertEqual(repeated["stages"]["serving_catalogue"]["published_components"], 0)
                        self.assertEqual(len(market.pages), 3)  # Ordinary retained all comparisons only.
                    return
                with (
                    patch.object(
                        pipeline, "acquire", side_effect=AssertionError("retained metadata reacquired")
                    ),
                    patch.object(
                        pipeline, "correlate", side_effect=AssertionError("retained row assignments replayed")
                    ),
                ):
                    adopted = pipeline.run_lineage(store, ["OLD"], identity_client=client)
                    self.assertEqual(adopted["failed"], 0, adopted)
                    changes = adopted["results"][0]["membership_refinement"]["changes"]
                    self.assertEqual(len(changes), 4 * len(ADJUSTMENTS))
                    self.assertEqual({item["new_public_id"] for item in changes}, {target["security_id"]})
                    with (
                        patch.object(
                            storage, "publish", side_effect=AssertionError("unchanged lineage republished")
                        ),
                        patch.object(
                            pipeline, "load_pricing", side_effect=AssertionError("accepted prices reopened")
                        ),
                        patch.object(
                            membership_updates,
                            "refine",
                            side_effect=AssertionError("accepted membership repeated"),
                        ),
                        patch(
                            "libs.alpaca_lineage_build.retained_membership.donor_edges",
                            side_effect=AssertionError("accepted edges reconstructed"),
                        ),
                    ):
                        repeated = pipeline.run_lineage(store, ["OLD"], identity_client=client)
                    self.assertEqual(repeated["failed"], 0, repeated)
                    self.assertTrue(repeated["results"][0]["reused_decision"])
                return
            controls = {}
            for name in receipts:
                source = load_source(store / "end_of_day/pricing" / name, name)
                controls[name] = metadata_source_control(
                    store,
                    name,
                    source,
                    receipts[name]["source_sha256"],
                    receipts[name],
                    accepted_reuse=(True, True),
                )
                controls[name].previous_receipt_generation = index["symbols"][name]["sha256"]
            selected = membership_updates._selected(controls)
            reports = {
                name: {
                    **receipt,
                    **source_work.retained_ownership_report(receipt, controls[name].local, {}, root, {}),
                }
                for name, receipt in receipts.items()
            }
            reports["NEW"]["uuid_lineage_edges"] = [
                {
                    "edge_type": "reverse_split",
                    "corporate_action_id": "split",
                    "effective_date": "2024-01-10",
                    "predecessor": {"asset_id": "a", "symbol": "NEW", "cusip": "111111111"},
                    "successor": {"asset_id": "b", "symbol": "NEW", "cusip": "222222222"},
                }
            ]
            evidence = {name: receipt["evidence"] for name, receipt in receipts.items()}
            _, research = component_refinement.refine(reports, evidence, selected)
            self.assertEqual(len(research["changes"]), 32)
            ruleset = replace(
                context.accept_ruleset(),
                membership_refinement=component_refinement.NAME,
                execution_sha256="selected-membership",
            )
            if controller:
                context._ruleset = ruleset
                key = tuple(sorted(controls))
                accepted_plan = {"store": str(store), "plan": research}
                context.completed_membership_plans[key] = accepted_plan

                def controlled_run():
                    return pipeline.run_lineage(
                        store,
                        ["NEW"],
                        identity_client=client,
                        evidence_client=evidence_client,
                        context=context,
                    )

                with (
                    operation(store),
                    patch.object(
                        membership_updates,
                        "refine",
                        side_effect=AssertionError("research applicability repeated"),
                    ),
                    patch.object(
                        component_refinement,
                        "_extends",
                        side_effect=AssertionError("accepted extension proof repeated"),
                    ),
                    patch.object(
                        transform, "correlate", side_effect=AssertionError("accepted owner cascade repeated")
                    ),
                ):
                    if delivery_json:
                        with patch.object(
                            storage.ReceiptCache,
                            "published",
                            side_effect=OSError("postcommit delivery interrupted"),
                        ):
                            failed = controlled_run()
                        self.assertEqual(failed["failed"], 1, failed)
                        self.assertIs(context.completed_membership_plans[key], accepted_plan)
                        held = next(
                            value
                            for value in context.group_completions.values()
                            if value.get("effects", {}).get("published")
                        )
                        self.assertIs(held["accepted_membership_plan"][1], accepted_plan)
                        from libs.alpaca_pipeline.codec import Decoder, Encoder
                        from libs.alpaca_pipeline.state import export_state, import_state

                        encoder = Encoder()
                        state = export_state(context, None, encoder.encode, evidence=evidence_client)
                        document = json.loads(json.dumps({"state": state, "objects": encoder.objects}))
                        decoder = Decoder(document["objects"])
                        context, _ = import_state(
                            store, document["state"], decoder.decode, evidence=evidence_client
                        )
                        accepted_plan = context.completed_membership_plans[key]
                        held = next(
                            value
                            for value in context.group_completions.values()
                            if value.get("effects", {}).get("published")
                        )
                        self.assertIs(held["accepted_membership_plan"][1], accepted_plan)
                        with (
                            patch.object(
                                storage, "publish", side_effect=AssertionError("completed publication repeated")
                            ),
                            patch.object(
                                pipeline, "load_pricing", side_effect=AssertionError("completed prices reopened")
                            ),
                            patch.object(
                                observations,
                                "_select_new_observations",
                                side_effect=AssertionError("completed numerical decision repeated"),
                            ),
                        ):
                            result = controlled_run()
                    elif renewed:
                        replacement_plan = dict(accepted_plan)
                        published = storage.ReceiptCache.published

                        def renew_after_publication(cache, receipts):
                            context.completed_membership_plans[key] = replacement_plan
                            return published(cache, receipts)

                        with patch.object(storage.ReceiptCache, "published", new=renew_after_publication):
                            result = controlled_run()
                    else:
                        result = controlled_run()
                    self.assertEqual(result["failed"], 0, result)
                    if renewed:
                        self.assertIs(context.completed_membership_plans[key], replacement_plan)
                    else:
                        self.assertNotIn(key, context.completed_membership_plans)
                    self.assertFalse(
                        any("accepted_membership_plan" in value for value in context.group_completions.values())
                    )
                    if not renewed:
                        with (
                            patch.object(
                                membership_updates,
                                "apply_completed",
                                side_effect=AssertionError("consumed plan reapplied"),
                            ),
                            patch.object(
                                pipeline, "load_pricing", side_effect=AssertionError("unchanged prices reopened")
                            ),
                        ):
                            repeated = controlled_run()
                        self.assertEqual(repeated["failed"], 0, repeated)
            else:
                cache = {}
                numerical = observations._select_new_observations

                def new_contest_only(inputs, reports, **kwargs):
                    for report in reports.values():
                        for segment in report["correlated"]["segments"]:
                            self.assertLessEqual(segment["last_timestamp"][:10], "2024-01-05")
                    return numerical(inputs, reports, **kwargs)

                with (
                    operation(store),
                    patch.object(
                        membership_updates,
                        "refine",
                        side_effect=AssertionError("research applicability repeated"),
                    ),
                    patch.object(
                        transform, "correlate", side_effect=AssertionError("accepted ownership repeated")
                    ),
                    patch.object(observations, "_select_new_observations", side_effect=new_contest_only),
                ):
                    plan = source_work.prepare_group_evidence_and_source_work(
                        controls,
                        evidence,
                        {},
                        cache=cache,
                        ruleset=ruleset,
                        accepted_membership_plan={"store": str(store), "plan": research},
                    )
                    if interrupted:

                        class Interrupted(BaseException):
                            pass

                        with patch.object(transform, "make_publication", side_effect=Interrupted):
                            with self.assertRaises(Interrupted):
                                source_work.execute_source_work(
                                    store,
                                    root,
                                    controls,
                                    evidence,
                                    {},
                                    plan,
                                    ruleset.policy,
                                    cache,
                                    loader=pipeline.load_pricing,
                                )
                    decisions = (
                        patch.object(
                            source_work,
                            "_decide_source_work",
                            side_effect=AssertionError("completed numerical work repeated"),
                        )
                        if interrupted
                        else nullcontext()
                    )
                    with decisions:
                        outputs = source_work.execute_source_work(
                            store,
                            root,
                            controls,
                            evidence,
                            {},
                            plan,
                            ruleset.policy,
                            cache,
                            loader=pipeline.load_pricing,
                        )
                    with patch.object(
                        source_work,
                        "_execute_source_work",
                        side_effect=AssertionError("completed result repeated"),
                    ):
                        self.assertIs(
                            outputs,
                            source_work.execute_source_work(
                                store,
                                root,
                                controls,
                                evidence,
                                {},
                                plan,
                                ruleset.policy,
                                cache,
                                loader=lambda *_args, **_kwargs: self.fail("accepted rows reread"),
                            ),
                        )
                    new_receipts, documents, _, _ = outputs
                    for receipt in new_receipts.values():
                        receipt["input_sources"] = selected["pricing_generations"]
                    storage.publish(
                        store,
                        root,
                        index,
                        new_receipts,
                        documents,
                        {},
                        previous=receipts,
                        sources={name: control.source for name, control in controls.items()},
                    )
            actual = storage.read_receipt(root, "OLD", storage.read_index(root))
            self.assertEqual(
                {item["security_id"] for item in actual["identity_memberships"]}, {target["security_id"]}
            )
            self.assertEqual(actual["source_sha256"], receipts["OLD"]["source_sha256"])
            if append:
                appended = [*bars, bar("2024-01-12", "10"), bar("2024-01-16", "10")]
                with (
                    patch(
                        "libs.alpaca_pricing_download.download.utc_now",
                        return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
                    ),
                    direct_adjustment_observations(),
                ):
                    for name in ("OLD", "NEW"):
                        download_pricing(
                            store, name, "2024-01-17", from_date="2024-01-01", client=OldRowsClient(appended)
                        )
                resumed = context if controller else OperationContext(store)
                resumed._correlation_policy = ruleset.policy
                resumed._ruleset = ruleset
                with patch.object(
                    component_refinement,
                    "_extends",
                    side_effect=AssertionError("accepted extension proof repeated"),
                ):
                    continued = pipeline.run_lineage(
                        store,
                        ["NEW"],
                        identity_client=client,
                        pricing_client=OldRowsClient(appended),
                        context=resumed,
                    )
                self.assertEqual(continued["failed"], 0, continued)
                after = storage.read_receipt(root, "OLD", storage.read_index(root))
                self.assertEqual(
                    {item["security_id"] for item in after["identity_memberships"]}, {target["security_id"]}
                )

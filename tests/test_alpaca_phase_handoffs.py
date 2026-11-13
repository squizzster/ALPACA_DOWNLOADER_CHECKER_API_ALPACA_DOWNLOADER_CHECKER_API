"""Established allocation is an input authority for unresolved-only fallback."""

import copy
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import row_correlation as rows
from libs.alpaca_end_of_day_security_correlation.errors import SecurityCorrelationError
from libs.alpaca_lineage_build import policy_engine as engine
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from tests.test_alpaca_end_of_day_security_correlation import asset, correlation_source, occupancy
from tests.test_alpaca_name_change_evidence import bars, inputs_for, rename


class PhaseHandoffTests(unittest.TestCase):
    days = ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08")

    def reused_rename(self):
        evidence = rename()
        current = {**asset("later-id", "OLD", "Later issue"), "lookup_cusips": ["later-cusip"]}
        evidence["identities"].append(current)
        evidence["asset_lookups"] = [x for x in evidence["asset_lookups"] if x["lookup"] != "OLD"]
        evidence["asset_lookups"].extend(
            {
                "lookup_kind": kind,
                "lookup": label,
                "asset": current,
                "found": True,
                "outcome": "found",
                "http_status": 200,
            }
            for kind, label in (("symbol", "OLD"), ("cusip", "later-cusip"))
        )
        evidence["symbol_occupancy_evidence"].append(
            occupancy(
                "OLD",
                "later-cusip",
                "later-id",
                action_type="cash_dividend",
                action_id="anchor",
                process_date="2024-01-08",
                role="symbol",
            )
        )
        evidence["corporate_actions"].append(
            {
                "id": "anchor",
                "action_type": "cash_dividend",
                "symbol": "OLD",
                "cusip": "later-cusip",
                "ex_date": "2024-01-08",
                "process_date": "2024-01-08",
                "rate": "0.1",
            }
        )
        inputs = inputs_for({"OLD": bars(self.days), "NEW": bars(self.days)}, asofs={"OLD": "2024-01-04"})
        return evidence, inputs

    def test_continuity_is_settled_once_before_freeze_without_early_fallback_rows(self):
        evidence, inputs = self.reused_rename()
        captured = []
        original = engine._validate_phase_monotonicity

        def guard(before, after):
            captured.append((before, after))
            return original(before, after)

        with (
            patch.object(rows, "assess_name_change_history", wraps=rows.assess_name_change_history) as assess,
            patch.object(engine, "_validate_phase_monotonicity", guard),
        ):
            engine.evaluate_policy(
                Path("."), inputs, dict.fromkeys(inputs, evidence), policy=load_authorized_policy()
            )
        self.assertEqual(1, assess.call_count)
        before, after = captured[0]
        self.assertTrue(before["NEW"]["uuid_lineage_edges"])
        self.assertEqual(before["NEW"], after["NEW"])
        self.assertFalse(any(i["asset_id"] == "later-id" for i in before["OLD"]["ticker_ownership_intervals"]))
        self.assertFalse(
            any(
                s["canonical_security"]["alpaca_asset_id"] == "later-id"
                for s in before["OLD"]["correlated"]["segments"]
            )
        )

    def test_uncertain_or_divergent_rename_keeps_the_continuity_veto(self):
        for change in ("undated", "overlap", "extra", "divergent", "incomplete"):
            with self.subTest(change=change):
                evidence, inputs = self.reused_rename()
                if change == "undated":
                    evidence["corporate_actions"].pop()
                elif change == "overlap":
                    evidence["corporate_actions"][-1].update(ex_date="2024-01-04", process_date="2024-01-04")
                elif change == "extra":
                    evidence["identities"].append(asset("third", "OLD", "Unplaced issue"))
                elif change == "divergent":
                    for download in inputs["NEW"].downloads.values():
                        download.rows[0]["c"] = "999"
                else:
                    inputs["OLD"].local["downloads"][0]["rows"] += 1
                result = engine.evaluate_policy(
                    Path("."), inputs, dict.fromkeys(inputs, evidence), policy=load_authorized_policy()
                )
                self.assertFalse(result["NEW"]["uuid_lineage_edges"])

    def test_reserved_segments_are_not_evaluated_or_merged_with_new_neighbours(self):
        values = bars(self.days[:3])
        source = correlation_source("SYN", asof="2024-01-05")
        lineage = {"corporate_actions": [], "identities": []}

        def initial(context):
            return (
                rows._RowDecision(branch="correlated", asset_id="owner", cusip="cusip")
                if context.session_date.isoformat() == self.days[1]
                else rows._RowDecision(
                    branch="uncorrelated", reason_codes=("ambiguous_security_identity_candidates",)
                )
            )

        baseline = rows.correlate_download_rows(
            values, source, lineage, [], row_rules=(rows.RowCorrelationRule("initial", initial),)
        )
        reserved = baseline["correlated"][0]
        visited = []

        def fallback(context):
            visited.append(context.session_date.isoformat())
            self.assertNotEqual(self.days[1], context.session_date.isoformat())
            return rows._RowDecision(branch="correlated", asset_id="owner", cusip="cusip")

        final = rows.correlate_download_rows(
            values,
            source,
            lineage,
            [],
            row_rules=(rows.RowCorrelationRule("fallback", fallback),),
            established_segments=(reserved,),
        )
        self.assertEqual([self.days[0], self.days[2]], visited)
        self.assertEqual([(1, 1), (2, 2), (3, 3)], [(x["row_start"], x["row_end"]) for x in final["correlated"]])
        self.assertIs(reserved, final["correlated"][1])
        unresolved = rows.correlate_download_rows(
            values,
            source,
            lineage,
            [],
            row_rules=(rows.RowCorrelationRule("initial", initial),),
            established_segments=(reserved,),
        )
        self.assertFalse(
            unresolved["uncorrelated"][0]["terminal_correlation_fallback"]["criteria"][
                "all_rows_initially_uncorrelated"
            ]
        )

    def test_certified_policy_overlay_reuses_completed_continuity(self):
        evidence, inputs = self.reused_rename()
        policy = load_authorized_policy()
        first = engine._correlation_cache(policy)
        engine._register_sources(first, inputs)
        expected = first.lineage_analysis(evidence)
        second = engine._correlation_cache(policy)
        second.inherit_continuity(first)
        engine._register_sources(second, inputs)
        with (
            patch.object(rows, "assess_name_change_history", side_effect=AssertionError("continuity replayed")),
            patch.object(rows, "prepare_ticker_ownership", side_effect=AssertionError("chronology replayed")),
        ):
            actual = second.lineage_analysis(evidence, include_action_anchored_current_suffix=True)
        self.assertEqual(expected[2], actual[2])
        self.assertIs(
            first.name_change_assessment(evidence),
            second.name_change_assessment(evidence, include_action_anchored_current_suffix=True),
        )

    def test_completed_source_never_reenters_builder(self):
        inputs = inputs_for({"DONE": bars(self.days)})
        completed = {"DONE": {"uncorrelated": {"row_count": 0}}}
        with patch.object(
            engine, "build_security_correlation_report", side_effect=AssertionError("completed source reopened")
        ):
            result = engine._correlate_until_stable(
                Path("."),
                inputs,
                {},
                rows.CorrelationCache(),
                completed,
                policy=load_authorized_policy(),
                established_reports=completed,
            )
        self.assertIs(completed["DONE"], result["DONE"])

    def test_conflicting_new_interval_is_rejected_before_reserving_rows(self):
        evidence, inputs = self.reused_rename()
        policy = load_authorized_policy()
        cache = engine._correlation_cache(policy)
        engine._register_sources(cache, inputs)
        baseline = engine._correlate_until_stable(
            Path("."), inputs, dict.fromkeys(inputs, evidence), cache, {}, policy=policy
        )
        # A corrupt proposal is a new producer output, not an accepted handoff.
        complete = cache.lineage_analysis(evidence, include_action_anchored_current_suffix=True)
        anchor = next(i for i in complete[0] if i.asset_id == "later-id")
        conflicting = replace(anchor, start_date_inclusive=anchor.start_date_inclusive.replace(day=2))
        key = (id(cache.provider_evidence(evidence)), True)
        stored = cache._lineage[key]
        cache._lineage[key] = (stored[0], [conflicting if i == anchor else i for i in stored[1]], *stored[2:])
        with self.assertRaisesRegex(SecurityCorrelationError, "conflicts with established"):
            engine._correlate_until_stable(
                Path("."),
                inputs,
                dict.fromkeys(inputs, evidence),
                cache,
                baseline,
                policy=policy,
                suffix_sources={"OLD"},
                established_reports=baseline,
            )

    def test_original_integrity_guard_still_rejects_changed_protected_provenance(self):
        evidence, inputs = self.reused_rename()
        result = engine.evaluate_policy(
            Path("."), inputs, dict.fromkeys(inputs, evidence), policy=load_authorized_policy()
        )
        for field, value in (("warnings", ["changed"]), ("segment_id", "changed"), ("security_identity", {})):
            altered = copy.deepcopy(result)
            altered["OLD"]["correlated"]["segments"][0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(Exception, "established correlated row"):
                engine._validate_phase_monotonicity(result, altered)

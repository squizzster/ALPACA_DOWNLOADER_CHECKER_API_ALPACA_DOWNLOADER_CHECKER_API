"""Scoped action evidence must restore valid history without joining other equity."""

import copy
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from libs.alpaca_end_of_day_security_correlation import name_change as nc
from libs.alpaca_end_of_day_security_correlation import ownership_interval_rules as oi
from libs.alpaca_end_of_day_security_correlation.uuid_lineage import UuidLineageEdge
from libs.alpaca_lineage_build.correlation_policy import load_policy
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_lineage_policy_lab.audit import _event_scope_prefilter
from tests.test_alpaca_name_change_evidence import bars, exact_same_symbol_handoff, inputs_for, raw_segments, run


class EventScopeTests(unittest.TestCase):
    def test_audit_selects_completed_related_queries_before_price_reads(self):
        evidence = {
            "query": {"input_symbol": "OLD", "corporate_actions_data_quality": "complete"},
            "queried_symbols": ["OLD", "NEW"],
            "corporate_actions": [
                {"id": "rename", "action_type": "name_change", "old_symbol": "OLD", "new_symbol": "NEW"}
            ],
        }
        group = SimpleNamespace(receipts={"OLD": {"versions": {"correlation": 77}, "evidence": evidence}})
        self.assertIn(
            "completed_related_query_coverage",
            _event_scope_prefilter(group, SimpleNamespace(implementation_correlation_version=78)),
        )
        self.assertEqual(
            _event_scope_prefilter(group, SimpleNamespace(implementation_correlation_version=77)), []
        )

    def test_earlier_acquisition_preserves_only_the_continuing_acquirer(self):
        merger = {
            "id": "earlier-acquisition",
            "action_type": "stock_merger",
            "acquirer_symbol": "HOLD",
            "acquirer_cusip": "old-cusip",
            "acquiree_symbol": "TARGET",
            "acquiree_cusip": "target-cusip",
            "acquiree_rate": 1,
            "acquirer_rate": 0.84,
            "effective_date": "2024-01-03",
            "process_date": "2024-01-03",
        }
        variants = {
            "continuing_acquirer": {},
            "wrong_acquirer_cusip": {"acquirer_cusip": "other-cusip"},
            "same_label_target": {"acquiree_symbol": "HOLD"},
            "target_is_endpoint": {"acquiree_cusip": "new-cusip"},
            "same_day_handoff": {"effective_date": "2024-01-05", "process_date": "2024-01-05"},
            "later_event": {"effective_date": "2024-01-08", "process_date": "2024-01-08"},
            "unknown_process_date": {"process_date": None},
            "zero_ratio": {"acquirer_rate": 0},
            "nan_ratio": {"acquirer_rate": "NaN"},
            "acquiree_role": {
                "acquirer_symbol": "TARGET",
                "acquirer_cusip": "target-cusip",
                "acquiree_symbol": "HOLD",
                "acquiree_cusip": "old-cusip",
            },
            "cash_merger": {"action_type": "cash_merger"},
        }
        prices = {"HOLD": bars(("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"))}
        for name, changes in variants.items():
            with self.subTest(name=name):
                evidence = exact_same_symbol_handoff()
                evidence["corporate_actions"].append({**merger, **changes})
                report = run(evidence, prices)["HOLD"]
                joined = any(
                    s["security_identity"]["member_alpaca_asset_ids"] == ["new-id", "old-id"]
                    for s in raw_segments(report)
                )
                self.assertEqual(joined, name == "continuing_acquirer")
        policy = load_policy(
            Path(__file__).resolve().parents[1] / "resources/alpaca_security_correlation_v77_legacy_policy.yaml"
        )
        evidence = exact_same_symbol_handoff()
        evidence["corporate_actions"].append(merger)
        historical = correlate(Path("."), inputs_for(prices), {"HOLD": evidence}, policy=policy)["HOLD"]
        self.assertFalse(
            any(len(s["security_identity"]["member_alpaca_asset_ids"]) > 1 for s in raw_segments(historical))
        )

    def terminal(self, existing=(), edges=(), *, current=True, positive=(), occupancy=()):
        action = {
            "id": "terminal",
            "action_type": "cash_merger",
            "acquiree_symbol": "REUSED",
            "acquiree_cusip": "later-cusip",
            "effective_date": "2026-10-28",
        }
        context = oi._OwnershipIntervalRuleContext(
            (action, *positive),
            {("REUSED", "later-cusip"): "later"},
            {
                "corporate_actions": [action, *positive],
                "symbol_occupancy_evidence": list(occupancy),
                "asset_lookups": [
                    {
                        "lookup_kind": "cusip",
                        "lookup": "later-cusip",
                        "found": True,
                        "outcome": "found",
                        "http_status": 200,
                        "asset": {
                            "id": "later",
                            "symbol": "REUSED",
                            "exchange": "ARCA",
                            "class": "us_equity",
                            "status": "inactive",
                            "tradable": False,
                        },
                    }
                ],
            },
            edges,
            closed_prior_terminal_conflicts=current,
        )
        return oi._apply_cash_merger_terminal_rule(
            context, existing, 3, "cash_merger_acquiree_terminal_boundary"
        )

    def interval(self, *, symbol="REUSED", asset="prior", start=None, end="2021-06-11"):
        rule = oi.AppliedOwnershipIntervalRule(1, "name_change_ticker_transition", ("rename",))
        return oi.TickerOwnershipInterval(
            symbol,
            "later-cusip" if asset == "later" else "prior-cusip",
            asset,
            date.fromisoformat(start) if start else None,
            date.fromisoformat(end) if end else None,
            (rule,),
        )

    def test_terminal_inference_does_not_extend_back_across_unrelated_closed_owner(self):
        prior = self.interval()
        result = self.terminal((prior,))
        self.assertEqual(result.intervals, (prior,))
        self.assertEqual(len(self.terminal((prior,), current=False).intervals), 2)
        self.assertEqual(len(self.terminal().intervals), 1)
        self.assertEqual(len(self.terminal((self.interval(symbol="OTHER"),)).intervals), 2)
        self.assertEqual(len(self.terminal((self.interval(end=None),)).intervals), 2)
        supported = self.interval(asset="later", start="2025-09-03", end=None)
        result = self.terminal((supported,))
        self.assertEqual(result.intervals[0].start_date_inclusive, date(2025, 9, 3))
        self.assertEqual(result.intervals[0].end_date_exclusive, date(2026, 10, 28))
        edge = UuidLineageEdge(
            "forward_split",
            "split",
            date(2022, 1, 3),
            "prior",
            "REUSED",
            "prior-cusip",
            "later",
            "REUSED",
            "later-cusip",
        )
        self.assertEqual(len(self.terminal((prior,), (edge,)).intervals), 2)

    def test_closed_prior_veto_preserves_a_positively_anchored_inactive_suffix(self):
        prior = self.interval()
        positive = {
            "id": "dividend",
            "action_type": "cash_dividend",
            "symbol": "REUSED",
            "cusip": "later-cusip",
            "ex_date": "2025-05-02",
            "rate": "0.08",
        }
        result = self.terminal((prior,), positive=(positive,))
        later = next(i for i in result.intervals if i.asset_id == "later")
        self.assertEqual(later.start_date_inclusive, date(2025, 5, 2))
        self.assertEqual(later.end_date_exclusive, date(2026, 10, 28))
        self.assertIn("dividend", later.applied_rules[0].supporting_action_ids)
        self.assertIn(prior, result.intervals)
        for changes in ({"ex_date": "2020-01-02"}, {"ex_date": "2026-10-29"}, {"rate": 0}, {"cusip": "other"}):
            with self.subTest(changes=changes):
                self.assertEqual(
                    self.terminal((prior,), positive=({**positive, **changes},)).intervals, (prior,)
                )
        for owner in ("rival", None):
            conflict = {
                "symbol": "REUSED",
                "cusip": "unknown",
                "asset_id": owner,
                "evidence": [{"process_date": "2025-07-01"}],
            }
            self.assertEqual(
                self.terminal((prior,), positive=(positive,), occupancy=(conflict,)).intervals, (prior,)
            )
        legacy = self.terminal((prior,), positive=(positive,), current=False)
        self.assertIsNone(next(i for i in legacy.intervals if i.asset_id == "later").start_date_inclusive)

    def test_query_coverage_consumes_only_completed_in_range_exact_labels(self):
        evidence = {
            "query": {
                "input_symbol": "OLD",
                "corporate_actions_data_quality": "complete",
                "corporate_actions_start": "2020-01-01",
                "corporate_actions_end": "2026-10-31",
            },
            "queried_symbols": ["OLD", "NEW"],
        }
        self.assertTrue(nc._complete_action_query_covers(evidence, "NEW", date(2025, 1, 1)))
        self.assertTrue(nc._complete_action_query_covers(evidence, "OLD", date(2025, 1, 1)))
        self.assertFalse(nc._complete_action_query_covers(evidence, "UNSEEN", date(2025, 1, 1)))
        self.assertFalse(nc._complete_action_query_covers(evidence, "NEW", date(2019, 1, 1)))
        partial = copy.deepcopy(evidence)
        partial["query"]["corporate_actions_data_quality"] = "partial"
        self.assertFalse(nc._complete_action_query_covers(partial, "NEW", date(2025, 1, 1)))
        del evidence["queried_symbols"]
        self.assertFalse(nc._complete_action_query_covers(evidence, "NEW", date(2025, 1, 1)))

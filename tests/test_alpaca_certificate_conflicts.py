"""Unresolved evidence must remain a veto at the certificate producer."""

import unittest

from tests.test_alpaca_end_of_day_security_correlation import occupancy
from tests.test_alpaca_name_change_evidence import (
    bars,
    component_anchored_early_successor_case,
    exact_same_symbol_handoff,
    pre_entry_carry_forward_case,
    raw_segments,
    run,
    terminal_carry_forward_evidence,
    terminal_carry_forward_histories,
)


class CertificateConflictTests(unittest.TestCase):
    def test_same_symbol_certificate_rejects_other_structural_actions(self):
        for kind in ("spin_off", "reorganization", "reverse_split", "unknown_structural_action"):
            with self.subTest(kind=kind):
                evidence = exact_same_symbol_handoff()
                evidence["corporate_actions"].append(
                    {"id": "conflict", "action_type": kind, "symbol": "HOLD", "process_date": "2024-01-05"}
                )
                report = run(
                    evidence,
                    {"HOLD": bars(("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"))},
                )["HOLD"]
                self.assertFalse(report["uuid_lineage_edges"])

    def test_malformed_component_alternate_symbol_stays_unresolved(self):
        for value in (None, "", [], {}):
            with self.subTest(value=value):
                evidence, histories, _ = component_anchored_early_successor_case()
                action = next(a for a in evidence["corporate_actions"] if a["action_type"] == "unit_split")
                if value is None:
                    del action["alternate_symbol"]
                else:
                    action["alternate_symbol"] = value
                report = run(evidence, histories)["NEW.WS"]
                self.assertEqual(3, sum(s["row_count"] for s in raw_segments(report, "uncorrelated")))

    def test_non_observation_does_not_hide_an_unresolved_occupant_or_action(self):
        for kind in ("pre_entry", "terminal"):
            for evidence_kind in ("occupancy", "action", "both"):
                for when_kind in ("inside", "outside", "undated"):
                    with self.subTest(kind=kind, evidence=evidence_kind, when=when_kind):
                        if kind == "pre_entry":
                            evidence, histories, first, _, _ = pre_entry_carry_forward_case()
                            symbol, reason = "NEW", "certified_pre_entry_carry_forward_non_observation"
                        else:
                            evidence, histories = (
                                terminal_carry_forward_evidence(),
                                terminal_carry_forward_histories(),
                            )
                            first, symbol, reason = "2024-01-05", "OLD", "terminal_nontrading_carry_forward"
                        when = (
                            first if when_kind == "inside" else "2022-01-01" if when_kind == "outside" else None
                        )
                        if evidence_kind in ("occupancy", "both"):
                            evidence["symbol_occupancy_evidence"].append(
                                occupancy(
                                    symbol,
                                    "unknown-cusip",
                                    None,
                                    action_type="reverse_split",
                                    action_id="conflict",
                                    process_date=when,
                                    role="symbol",
                                )
                            )
                        if evidence_kind in ("action", "both"):
                            evidence["corporate_actions"].append(
                                {
                                    "id": "conflict",
                                    "action_type": "reverse_split",
                                    "symbol": symbol,
                                    "old_cusip": "unknown-cusip",
                                    "new_cusip": "another-unknown",
                                    "process_date": when,
                                }
                            )
                        report = run(evidence, histories)[symbol]
                        certified = any(
                            s["reason_codes"] == [reason] for s in raw_segments(report, "uncorrelated")
                        )
                        self.assertEqual(when_kind == "outside", certified)

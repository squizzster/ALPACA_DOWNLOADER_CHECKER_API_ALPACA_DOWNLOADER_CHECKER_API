"""Rebuild certificate evidence through the real metadata normalizer, in memory."""

import copy
import hashlib
import unittest
from pathlib import Path

from test_alpaca_lineage_build import IdentityClient, asset

from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy, load_policy
from libs.alpaca_lineage_build.metadata import EvidenceClient, acquire
from libs.alpaca_lineage_build.policy_engine import evaluate_policy
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.evidence import PublishedPricingEvidence
from tests.test_alpaca_name_change_evidence import (
    bars,
    component_anchored_early_successor_case,
    early_successor_bridge_evidence,
    exact_same_symbol_handoff,
    inputs_for,
)


def fixture(shape):
    allrows = bars(
        ("2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09", "2024-01-10")
    )
    if shape == "same_symbol":
        return exact_same_symbol_handoff(), {"HOLD": allrows[:5]}, "HOLD", "2024-01-04"
    if shape == "component":
        evidence, histories, _ = component_anchored_early_successor_case()
        return evidence, histories, "NEW.WS", "2024-01-05"
    return (
        early_successor_bridge_evidence(),
        {"OLD": allrows[:3], "NEW": allrows[3:], "CURRENT": allrows},
        "NEW",
        "2024-01-05",
    )


def normalized_case(shape, variant, *, policy=None, conflict_symbol=None, conflict_day=None):
    prior, histories, target, day = fixture(shape)
    day = conflict_day or day
    actions = copy.deepcopy(prior["corporate_actions"])
    assets = {item["lookup"]: copy.deepcopy(item["asset"]) for item in prior["asset_lookups"]}
    for item in prior["identities"]:
        assets.setdefault(item["symbol"], copy.deepcopy(item))
    if variant in ("unresolved_distribution", "resolved_distribution"):
        actions.append(
            {
                "id": "conflict",
                "action_type": "cash_dividend",
                "symbol": conflict_symbol or target,
                "cusip": "third-cusip",
                "process_date": day,
                "ex_date": day,
                "record_date": day,
                "payable_date": day,
                "rate": "0.2",
            }
        )
        if variant == "resolved_distribution":
            assets["third-cusip"] = asset(target, "third-id")
    elif variant != "baseline":
        actions.append(
            {"id": "conflict", "action_type": variant, "symbol": conflict_symbol or target, "process_date": day}
        )
    client = EvidenceClient(IdentityClient(assets, actions))
    inputs = inputs_for(histories)
    evidence = {}
    for symbol, view in inputs.items():
        view.symbol = symbol
        view.source_sha256 = hashlib.sha256(symbol.encode()).hexdigest()
        view.source["latest_request"]["end_exclusive"] = "2024-02-01"
        for download in view.downloads.values():
            download.manifest["request_to"] = "2024-02-01"
        view.local["published_pricing"] = PublishedPricingEvidence(
            symbol,
            view.source_sha256,
            "2024-01-01",
            "2024-02-01",
            "2024-02-01",
            "sip",
            "1Day",
            tuple((a, len(histories[symbol])) for a in ADJUSTMENTS),
        ).to_report()
        evidence[symbol] = acquire(view, client)
    reports = evaluate_policy(
        Path(__file__).resolve().parents[1], inputs, evidence, policy=policy or load_authorized_policy()
    )
    report = reports[target]
    return report, evidence


class NormalizedCertificateTests(unittest.TestCase):
    def test_frozen_v76_keeps_its_original_outcomes_but_is_not_production_authorized(self):
        from libs.alpaca_lineage_build.correlation_policy import authorize_production_policy

        policy = load_policy(
            Path(__file__).resolve().parents[1] / "resources/alpaca_security_correlation_v76_legacy_policy.yaml"
        )
        with self.assertRaises(RuntimeError):
            authorize_production_policy(policy)
        for shape in ("same_symbol", "component", "source_bridge"):
            report, _ = normalized_case(shape, "unresolved_distribution", policy=policy)
            self.assertEqual(report["uncorrelated"]["row_count"], 0)

    def test_evidence_outside_early_certificate_span_does_not_remove_supported_prefix(self):
        for shape in ("component", "source_bridge"):
            with self.subTest(shape=shape):
                report, _ = normalized_case(shape, "unresolved_distribution", conflict_day="2024-01-25")
                self.assertEqual(report["uncorrelated"]["row_count"], 0)

    def test_bridge_current_source_unknown_occupant_vetoes_early_certificate(self):
        report, _ = normalized_case("source_bridge", "unresolved_distribution", conflict_symbol="CURRENT")
        self.assertEqual(report["uncorrelated"]["row_count"], 16)

    def test_valid_complete_groups_keep_supported_history(self):
        for shape, rows in (("same_symbol", 40), ("component", 48), ("source_bridge", 32)):
            with self.subTest(shape=shape):
                report, _ = normalized_case(shape, "baseline")
                self.assertEqual(report["correlated"]["row_count"], rows)
                self.assertEqual(report["uncorrelated"]["row_count"], 0)

    def test_unresolved_competing_distribution_vetoes_certificate(self):
        for shape in ("same_symbol", "component", "source_bridge"):
            with self.subTest(shape=shape):
                report, evidence = normalized_case(shape, "unresolved_distribution")
                self.assertTrue(any("third-cusip" in e["unresolved_cusips"] for e in evidence.values()))
                self.assertGreater(report["uncorrelated"]["row_count"], 0)

    def test_incomplete_structural_actions_veto_early_source_bridge(self):
        for kind in ("spin_off", "reorganization", "reverse_split"):
            with self.subTest(kind=kind):
                report, _ = normalized_case("source_bridge", kind)
                self.assertEqual(report["uncorrelated"]["row_count"], 16)
                self.assertEqual(report["correlated"]["row_count"], 16)

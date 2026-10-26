"""Optional diagnostics cannot expand a completed group's rows in ordinary work."""

import unittest
from unittest.mock import patch

import test_alpaca_standalone_reader as fixtures

from libs.alpaca_lineage_build import policy_engine
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.pricing import load_pricing
from libs.alpaca_pricing_download import storage


class PolicyTraceTests(unittest.TestCase):
    setUp = fixtures.StandaloneReaderTests.setUp
    download = fixtures.StandaloneReaderTests.download
    build = fixtures.StandaloneReaderTests.build
    publication = fixtures.StandaloneReaderTests.publication

    def test_no_trace_skips_diagnostic_expansion_and_hashing_but_preserves_reports(self):
        receipt, _ = self.publication()
        root = self.store / "end_of_day/pricing/TEST"
        with storage.ticker_lock(root, exclusive=True):
            inputs = {"TEST": load_pricing(self.store, "TEST")}
        evidence = {"TEST": receipt["evidence"]}
        policy = load_authorized_policy()
        with (
            patch.object(policy_engine, "_report_rows", side_effect=AssertionError("diagnostic row expansion")),
            patch.object(policy_engine, "_digest", side_effect=AssertionError("diagnostic hash")),
            patch.object(policy_engine, "document_digest", side_effect=AssertionError("diagnostic pass hash")),
            patch.object(
                policy_engine, "_validate_phase_monotonicity", wraps=policy_engine._validate_phase_monotonicity
            ) as invariants,
        ):
            ordinary = policy_engine.evaluate_policy(self.store, inputs, evidence, policy=policy)
        invariants.assert_called_once()
        trace = {}
        with patch.object(policy_engine, "_report_rows", wraps=policy_engine._report_rows) as rows:
            traced = policy_engine.evaluate_policy(
                self.store, inputs, evidence, policy=policy, execution_trace=trace
            )
        self.assertEqual(ordinary, traced)
        rows.assert_called_once()
        self.assertEqual(trace["phase_one"]["row_counts"]["TEST"]["correlated"], 8 * len(self.bars))
        self.assertEqual(len(trace["phase_one"]["protected_correlated_rows_sha256"]), 64)

    def test_no_trace_still_rejects_invalid_phase_results(self):
        policy = load_authorized_policy()
        with patch.object(
            policy_engine,
            "_validate_phase_monotonicity",
            side_effect=policy_engine.CorrelationPolicyError("invalid phase result"),
        ):
            with self.assertRaisesRegex(policy_engine.CorrelationPolicyError, "invalid phase result"):
                policy_engine.evaluate_policy(self.store, {}, {}, policy=policy)

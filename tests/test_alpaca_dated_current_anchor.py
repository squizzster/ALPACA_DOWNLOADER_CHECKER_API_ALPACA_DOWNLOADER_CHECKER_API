import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import closed_departure, dated_current_anchor
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.transform import correlate
from libs.common.us_equity_market_session_calendar import us_market_hours_sha256
from tests.test_alpaca_closed_departure_source import prior_claim_case
from tests.test_alpaca_name_change_evidence import bars, inputs_for


def dated_current_case(symbol, kind):
    evidence, inputs = prior_claim_case(symbol)
    current = evidence["identities"][-1]
    evidence["asset_lookups"].append(
        {**evidence["asset_lookups"][0], "lookup_kind": "cusip", "lookup": "222222222", "asset": current}
    )
    if kind == "split":
        evidence["corporate_actions"].append(
            {
                "action_type": "reverse_split",
                "id": "current-split",
                "symbol": symbol,
                "old_cusip": "111111111",
                "new_cusip": "222222222",
                "ex_date": "2024-06-14",
                "process_date": "2024-06-14",
                "old_rate": 4,
                "new_rate": 1,
            }
        )
    else:
        for day in ("2024-06-14", "2024-06-17"):
            evidence["corporate_actions"].append(
                {
                    "action_type": "cash_dividend",
                    "id": f"current-dividend-{day}",
                    "symbol": symbol,
                    "cusip": "222222222",
                    "rate": 0.25,
                    "ex_date": day,
                    "process_date": day,
                }
            )
    return evidence, inputs


class DatedCurrentAnchorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = load_authorized_policy()
        us_market_hours_sha256()

    def evaluate(self, symbol, evidence, inputs, policy=None):
        with patch.object(Path, "open", side_effect=AssertionError("unexpected storage read")):
            return correlate(
                Path("/unused"), inputs, {name: evidence for name in inputs}, policy=policy or self.policy
            )[symbol]

    def test_current_dividends_and_completed_split_each_bound_later_rows(self):
        for symbol, kind in (("ZLVQ", "dividend"), ("ZXQT", "split")):
            with self.subTest(kind=kind):
                evidence, sources = dated_current_case(symbol, kind)
                with patch.object(
                    closed_departure, "current_anchor_bounds", wraps=dated_current_anchor.current_anchor_bounds
                ) as selected:
                    report = self.evaluate(symbol, evidence, sources)
                self.assertEqual(1, selected.call_count)
                self.assertEqual(
                    {
                        "version": 4,
                        "symbol": symbol,
                        "asset_id": "current-id",
                        "first": "2024-06-14",
                        "last": "2024-06-18",
                        "cusip": "222222222",
                        "anchor_action_id": "current-split"
                        if kind == "split"
                        else "current-dividend-2024-06-14",
                    },
                    report["current_source_ownership"],
                )
                evidence, sources = dated_current_case(symbol, kind)
                old = self.evaluate(
                    symbol,
                    evidence,
                    sources,
                    replace(self.policy, implementation_correlation_version=98),
                )
                self.assertIsNone(old["current_source_ownership"])
                self.assertEqual(old["uuid_lineage_edges"], report["uuid_lineage_edges"])

    def test_conflicts_keep_the_ordinary_ownership_result(self):
        def rival_dividend(evidence):
            evidence["corporate_actions"].append(
                {**evidence["corporate_actions"][-1], "id": "competing-dividend", "cusip": "111111111"}
            )

        for change in (
            lambda evidence: evidence["corporate_actions"].pop(),
            lambda evidence: evidence["identities"][0].update(status="active"),
            rival_dividend,
        ):
            evidence, inputs = dated_current_case("ZLVQ", "dividend")
            change(evidence)
            self.assertIsNone(self.evaluate("ZLVQ", evidence, inputs)["current_source_ownership"])

    def test_overlap_or_missing_view_in_later_span_prevents_allocation(self):
        evidence, inputs = dated_current_case("ZLVQ", "dividend")
        inputs.update(inputs_for({"ZPPE": bars(("2024-06-17", "2024-06-18"))}))
        self.assertIsNone(self.evaluate("ZLVQ", evidence, inputs)["current_source_ownership"])

        evidence, inputs = dated_current_case("ZLVQ", "dividend")
        after = inputs["ZLVQ"].downloads["split.csv.gz"]
        after.rows.pop(3)
        after.manifest["rows"] -= 1
        self.assertIsNone(self.evaluate("ZLVQ", evidence, inputs)["current_source_ownership"])

    def test_explicit_unit_component_can_trade_alongside_current_common(self):
        evidence, inputs = dated_current_case("ZLVQ", "dividend")
        inputs.update(inputs_for({"ZPPE": bars(("2024-06-17", "2024-06-18"))}))
        evidence["corporate_actions"].append(
            {
                "action_type": "unit_split",
                "id": "unit-separation",
                "old_symbol": "ZPPU",
                "old_cusip": "333333333",
                "old_rate": 1,
                "new_symbol": "ZLVQ",
                "new_cusip": "111111111",
                "new_rate": 1,
                "alternate_symbol": "ZPPE",
                "alternate_cusip": "444444444",
                "alternate_rate": 1,
                "effective_date": "2024-04-22",
                "process_date": "2024-04-22",
            }
        )
        witness = {**evidence["identities"][0], "id": "warrant-id", "symbol": "ZPPE"}
        evidence["identities"].append(witness)
        evidence["asset_lookups"].append(
            {**evidence["asset_lookups"][-1], "lookup": "444444444", "asset": witness}
        )
        report = self.evaluate("ZLVQ", evidence, inputs)
        self.assertEqual("current-id", report["current_source_ownership"]["asset_id"])
        self.assertTrue(
            all(
                "warrant-id" not in segment["security_identity"]["member_alpaca_asset_ids"]
                for segment in report["correlated"]["segments"]
            )
        )

    def test_bounded_handoff_extends_without_repeating_action_or_grid_checks(self):
        evidence, inputs = dated_current_case("ZLVQ", "dividend")
        proof = self.evaluate("ZLVQ", evidence, inputs)["current_source_ownership"]
        validate_handoff({"schema_version": 35, "symbol": "ZLVQ", "current_source_ownership": proof})
        with patch.object(
            closed_departure, "current_anchor_bounds", side_effect=AssertionError("proof repeated")
        ):
            carried = closed_departure.continue_current_source(proof, evidence, "2024-06-20").to_report()
        self.assertEqual({**proof, "last": "2024-06-20"}, carried)

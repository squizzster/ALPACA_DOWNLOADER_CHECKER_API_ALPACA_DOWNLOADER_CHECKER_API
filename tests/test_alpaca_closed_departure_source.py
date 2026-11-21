"""Current-source ownership after a closed departure, across the complete group."""

import copy
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from test_alpaca_lineage_build import IdentityClient, asset, informative_bars
from test_alpaca_name_change_evidence import bars, inputs_for, raw_segments, rename
from test_alpaca_pricing_download import FakeClient, direct_adjustment_observations

from libs.alpaca_end_of_day_security_correlation import closed_departure, correlation, row_correlation
from libs.alpaca_end_of_day_security_correlation.correlation_version import validate_correlation_change_log
from libs.alpaca_lineage_build import pipeline, policy_engine, pricing, storage
from libs.alpaca_lineage_build.continuation import validate_handoff
from libs.alpaca_lineage_build.correlation_policy import load_authorized_policy
from libs.alpaca_lineage_build.evidence import EvidenceCapture
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.download import download_pricing
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests.test_alpaca_end_of_day_security_correlation import occupancy


def case():
    evidence = rename(same_uuid=True, same_cusip=True, old="REUS", new="KEEP", when="2024-04-08")
    old = {**asset("KEEP", "old-id"), "lookup_cusips": ["old-cusip"]}
    current = {**asset("REUS", "current-id"), "lookup_cusips": []}
    evidence.update(
        identities=[old, current],
        asset_lookups=[
            {
                "lookup_kind": kind,
                "lookup": value,
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": item,
            }
            for kind, value, item in (
                ("symbol", "REUS", current),
                ("symbol", "KEEP", old),
                ("cusip", "old-cusip", old),
            )
        ],
        query={
            "input_symbol": "REUS",
            "corporate_actions_start": "2024-01-01",
            "corporate_actions_end": "2024-06-19",
            "corporate_actions_data_quality": "complete",
        },
        queried_symbols=["REUS", "KEEP"],
        unresolved_cusips=[],
    )
    histories = {
        "REUS": bars(("2024-06-12", "2024-06-13", "2024-06-14", "2024-06-17", "2024-06-18"), 100),
        "KEEP": bars(expected_market_session_coverage("2024-04-05", "2024-06-19").session_dates, 20),
    }
    inputs = inputs_for(histories, asofs=dict.fromkeys(histories, "2024-06-19"))
    for view in inputs.values():
        view.source["latest_request"]["end_exclusive"] = "2024-06-19"
        for download in view.downloads.values():
            download.manifest["request_to"] = "2024-06-19"
    return evidence, inputs


def terminal_case(*, keep_rows=()):
    evidence, former = case()
    retired = evidence["identities"][0]
    earlier = {**asset("REUS", "prior-split"), "status": "inactive", "tradable": False}
    retired.update(status="inactive", tradable=False)
    evidence["identities"].append(earlier)
    evidence["asset_lookups"].append(
        {
            "lookup_kind": "cusip",
            "lookup": "earlier-cusip",
            "asset": earlier,
            "found": True,
            "outcome": "found",
            "http_status": 200,
        }
    )
    split = {
        "action_type": "reverse_split",
        "id": "split",
        "symbol": "REUS",
        "ex_date": "2024-04-01",
        "process_date": "2024-04-01",
        "old_cusip": "earlier-cusip",
        "new_cusip": "old-cusip",
        "old_rate": 23,
        "new_rate": 1,
    }
    end = {
        "action_type": "worthless_removal",
        "id": "terminal",
        "symbol": "KEEP",
        "cusip": "old-cusip",
        "process_date": "2024-05-15",
    }
    evidence["corporate_actions"].extend((split, end))
    evidence["symbol_occupancy_evidence"].extend(
        occupancy(
            "REUS",
            cusip,
            owner,
            action_type="reverse_split",
            action_id="split",
            process_date="2024-04-01",
            role=role,
        )
        for cusip, owner, role in (("earlier-cusip", "prior-split", "old"), ("old-cusip", "old-id", "new"))
    )
    return evidence, terminal_inputs(former, keep_rows)


def cash_terminal_case():
    evidence, former = case()
    rename_evidence = rename(same_uuid=True, same_cusip=True, old="KEEP", new="REUS", when="2024-04-08")
    evidence.update(
        corporate_actions=rename_evidence["corporate_actions"],
        symbol_occupancy_evidence=rename_evidence["symbol_occupancy_evidence"],
    )
    retired = evidence["identities"][0]
    retired.update(symbol="REUS", status="inactive", tradable=False)
    evidence["asset_lookups"].pop(1)  # The former ticker no longer has an exact lookup.
    evidence["corporate_actions"].append(
        {
            "action_type": "cash_merger",
            "id": "terminal",
            "acquiree_symbol": "REUS",
            "acquiree_cusip": "old-cusip",
            "effective_date": "2024-05-15",
            "process_date": "2024-05-15",
            "rate": 12.25,
        }
    )
    evidence["symbol_occupancy_evidence"].append(
        occupancy(
            "REUS",
            "old-cusip",
            "old-id",
            action_type="cash_merger",
            action_id="terminal",
            process_date="2024-05-15",
            role="acquiree",
        )
    )
    return evidence, terminal_inputs(former, bars(("2024-04-05",), 20))


def terminal_inputs(former, keep_rows):
    inputs = inputs_for(
        {"REUS": former["REUS"].downloads["raw.csv.gz"].rows, "KEEP": keep_rows},
        asofs={"REUS": "2024-06-19", "KEEP": "2024-06-19"},
    )
    for view in inputs.values():
        view.source["latest_request"]["end_exclusive"] = "2024-06-19"
        for download in view.downloads.values():
            download.manifest["request_to"] = "2024-06-19"
    return inputs


def prior_claim_actions(symbol, cusip="111111111"):
    return [
        {
            "action_type": "cash_dividend",
            "id": f"dividend-{ex}",
            "symbol": symbol,
            "cusip": identifier,
            "rate": 0.25,
            "ex_date": ex,
            "process_date": process,
        }
        for identifier, ex, process in (
            ("", "2024-01-25", "2024-01-31"),
            (cusip, "2024-02-20", "2024-02-23"),
            (cusip, "2024-04-22", "2024-04-26"),
        )
    ]


def prior_claim_case(symbol):
    prior = {**asset(symbol, "old-id"), "status": "inactive", "tradable": False}
    current = asset(symbol, "current-id")
    actions = prior_claim_actions(symbol)
    evidence = {
        "identities": [prior, current],
        "asset_lookups": [
            {
                "lookup_kind": kind,
                "lookup": value,
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": item,
            }
            for kind, value, item in (("symbol", symbol, current), ("cusip", "111111111", prior))
        ],
        "query": {
            "input_symbol": symbol,
            "corporate_actions_start": "2024-01-01",
            "corporate_actions_end": "2024-06-19",
            "corporate_actions_data_quality": "complete",
        },
        "corporate_actions": actions,
        "symbol_occupancy_evidence": [
            occupancy(
                symbol,
                action["cusip"],
                "old-id",
                action_type="cash_dividend",
                action_id=action["id"],
                process_date=action["process_date"],
                role="symbol",
            )
            for action in actions
            if action["cusip"]
        ],
        "queried_symbols": [symbol],
        "unresolved_cusips": [],
    }
    histories = {symbol: bars(("2024-06-12", "2024-06-13", "2024-06-14", "2024-06-17", "2024-06-18"), 100)}
    inputs = inputs_for(histories, asofs={symbol: "2024-06-19"})
    view = inputs[symbol]
    view.source["latest_request"]["end_exclusive"] = "2024-06-19"
    for download in view.downloads.values():
        download.manifest["request_to"] = "2024-06-19"
    return evidence, inputs


def split_suffix_case(symbol):
    evidence, inputs = prior_claim_case(symbol)
    evidence["corporate_actions"].append(
        {
            "action_type": "forward_split",
            "id": "split",
            "symbol": symbol,
            "cusip": "111111111",
            "ex_date": "2024-06-14",
            "process_date": "2024-06-14",
            "old_rate": 1,
            "new_rate": 2,
        }
    )
    adjusted = inputs[symbol].downloads["split.csv.gz"]
    for row in adjusted.rows[:2]:
        row.update({field: str(int(row[field]) / 2) for field in ("o", "h", "l", "c", "vw")})
        row["v"] = "200"
    return evidence, inputs


class ClosedDepartureSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy = load_authorized_policy()
        validate_correlation_change_log()

    def evaluate(self, evidence, inputs, policy=None, *, store=Path("/unused"), **scope):
        with patch.object(Path, "open", side_effect=AssertionError("unexpected storage read")):
            return correlate(
                store, inputs, {name: evidence for name in inputs}, policy=policy or self.policy, **scope
            )

    def test_complete_current_source_resolves_without_current_cusip_or_action(self):
        evidence, inputs = case()
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            reports = self.evaluate(evidence, inputs)
        self.assertEqual(2, assess.call_count)  # one result, including negative KEEP, per complete source
        self.assertEqual(40, reports["REUS"]["correlated"]["row_count"])
        self.assertEqual(0, reports["REUS"]["uncorrelated"]["row_count"])
        self.assertEqual("current-id", raw_segments(reports["REUS"])[0]["canonical_security"]["alpaca_asset_id"])
        certificate = reports["REUS"]["current_source_ownership"]
        self.assertEqual(("2024-06-12", "2024-06-18"), (certificate["first"], certificate["last"]))
        self.assertEqual(
            [],
            reports["REUS"]["uuid_lineage_edges"]
            if not reports["REUS"]["uuid_lineage_edges"]
            else [edge for edge in reports["REUS"]["uuid_lineage_edges"] if "current-id" in str(edge)],
        )
        previous = self.evaluate(
            evidence,
            inputs,
            replace(self.policy, implementation_correlation_version=79, implementation_lineage_version=34),
        )
        self.assertEqual(40, previous["REUS"]["uncorrelated"]["row_count"])
        self.assertEqual(previous["KEEP"]["correlated"], reports["KEEP"]["correlated"])
        self.assertEqual(previous["KEEP"]["uncorrelated"], reports["KEEP"]["uncorrelated"])

    def test_terminal_branch_closes_before_a_distinct_tape(self):
        without_terminal_rule = replace(
            self.policy, implementation_correlation_version=closed_departure.TERMINAL_DEPARTURE_VERSION - 1
        )
        for shape in (terminal_case, cash_terminal_case):
            with self.subTest(shape=shape.__name__):
                evidence, inputs = shape()
                previous = self.evaluate(evidence, inputs, without_terminal_rule)
                reports = self.evaluate(evidence, inputs)
                self.assertEqual(40, previous["REUS"]["uncorrelated"]["row_count"])
                self.assertEqual(0, reports["REUS"]["uncorrelated"]["row_count"])
                segments = raw_segments(reports["REUS"])
                self.assertEqual(
                    ["current-id"], [row["canonical_security"]["alpaca_asset_id"] for row in segments]
                )
                self.assertEqual(["current-id"], segments[0]["security_identity"]["member_alpaca_asset_ids"])
                certificate = reports["REUS"]["current_source_ownership"]
                self.assertEqual(
                    ("terminal", "2024-05-15"),
                    (certificate["departure_action_id"], certificate["departure_date"]),
                )
                self.assertEqual(("2024-06-12", "2024-06-18"), (certificate["first"], certificate["last"]))
                self.assertEqual(previous["KEEP"]["correlated"], reports["KEEP"]["correlated"])
                self.assertEqual(previous["KEEP"]["uncorrelated"], reports["KEEP"]["uncorrelated"])

    def test_terminal_branch_has_no_authority_while_old_issue_is_possible(self):
        for label, mutate in (
            ("missing extinction", lambda evidence: evidence["corporate_actions"].pop()),
            (
                "closure in current tape",
                lambda evidence: evidence["corporate_actions"][-1].update(process_date="2024-06-17"),
            ),
            (
                "conflicting expiry dates",
                lambda evidence: evidence["corporate_actions"][-1].update(effective_date="2024-05-16"),
            ),
            ("unknown former split", lambda evidence: evidence["corporate_actions"].pop(-2)),
            ("active earlier issue", lambda evidence: evidence["identities"][-1].update(status="active")),
        ):
            with self.subTest(label=label):
                evidence, inputs = terminal_case()
                mutate(evidence)
                report = self.evaluate(evidence, inputs)["REUS"]
                self.assertIsNone(report["current_source_ownership"])
                self.assertEqual(40, report["uncorrelated"]["row_count"])

    def test_terminal_branch_needs_complete_noncompeting_peer_tapes(self):
        evidence, inputs = terminal_case(keep_rows=bars(("2024-06-13",), 20))
        report = self.evaluate(evidence, inputs)["REUS"]
        self.assertIsNone(report["current_source_ownership"])
        self.assertEqual(40, report["uncorrelated"]["row_count"])

        evidence, inputs = terminal_case()
        inputs["KEEP"].local["downloads"][0]["rows"] = 1
        report = self.evaluate(evidence, inputs)["REUS"]
        self.assertIsNone(report["current_source_ownership"])
        self.assertEqual(40, report["uncorrelated"]["row_count"])

    def test_disjoint_historical_claims_support_only_current_observed_rows(self):
        control = replace(
            self.policy, implementation_correlation_version=closed_departure.PRIOR_CLAIM_VERSION - 1
        )
        for symbol in ("NQZK", "ZQPN"):
            with self.subTest(symbol=symbol):
                evidence, inputs = prior_claim_case(symbol)
                self.assertEqual(
                    40, self.evaluate(evidence, inputs, control)[symbol]["uncorrelated"]["row_count"]
                )
                report = self.evaluate(evidence, inputs)[symbol]
                self.assertEqual(40, report["correlated"]["row_count"])
                self.assertEqual([], report["ticker_ownership_intervals"])
                self.assertEqual([], report["uuid_lineage_edges"])
                self.assertEqual(
                    {
                        "version": 2,
                        "symbol": symbol,
                        "asset_id": "current-id",
                        "first": "2024-06-12",
                        "last": "2024-06-18",
                        "prior_asset_id": "old-id",
                        "latest_prior_activity_date": "2024-04-26",
                    },
                    report["current_source_ownership"],
                )
                segments = report["correlated"]["segments"]
                self.assertEqual(
                    {closed_departure.PRIOR_CLAIM_METHOD}, {s["correlation_method"] for s in segments}
                )
                self.assertTrue(
                    all(s["security_identity"]["member_alpaca_asset_ids"] == ["current-id"] for s in segments)
                )

    def test_observed_split_allocates_only_the_current_suffix(self):
        control = replace(
            self.policy, implementation_correlation_version=closed_departure.SPLIT_SUFFIX_VERSION - 1
        )
        suffix_policy = replace(
            self.policy, implementation_correlation_version=closed_departure.SPLIT_CONTINUITY_VERSION - 1
        )
        for symbol in ("QZAM", "QZBM"):
            with self.subTest(symbol=symbol):
                evidence, inputs = split_suffix_case(symbol)
                self.assertEqual(
                    40, self.evaluate(evidence, inputs, control)[symbol]["uncorrelated"]["row_count"]
                )
                with patch.object(
                    closed_departure, "_assess_split_suffix", wraps=closed_departure._assess_split_suffix
                ) as assess:
                    report = self.evaluate(evidence, inputs, suffix_policy)[symbol]
                self.assertEqual(1, assess.call_count)
                self.assertEqual(24, report["correlated"]["row_count"])
                self.assertEqual(16, report["uncorrelated"]["row_count"])
                self.assertEqual([], report["uuid_lineage_edges"])
                self.assertEqual([], report["ticker_ownership_intervals"])
                proof = report["current_source_ownership"]
                self.assertEqual(
                    {
                        "version": 3,
                        "symbol": symbol,
                        "asset_id": "current-id",
                        "prior_asset_id": "old-id",
                        "split_action_id": "split",
                        "first": "2024-06-14",
                        "last": "2024-06-18",
                    },
                    proof,
                )
                validate_handoff({"schema_version": 35, "symbol": symbol, "current_source_ownership": proof})
                self.assertTrue(
                    all(
                        item["security_identity"]["member_alpaca_asset_ids"] == ["current-id"]
                        and item["correlation_method"] == closed_departure.SPLIT_SUFFIX_METHOD
                        for item in report["correlated"]["segments"]
                    )
                )

    def test_single_endpoint_split_connects_earlier_rows_to_the_same_public_identity(self):
        suffix_policy = replace(
            self.policy, implementation_correlation_version=closed_departure.SPLIT_CONTINUITY_VERSION - 1
        )
        for symbol in ("QZAM", "QZBM"):
            with self.subTest(symbol=symbol):
                evidence, inputs = split_suffix_case(symbol)
                baseline = self.evaluate(evidence, inputs, suffix_policy)[symbol]
                candidate = self.evaluate(evidence, inputs)[symbol]
                self.assertEqual(40, candidate["correlated"]["row_count"])
                self.assertEqual(0, candidate["uncorrelated"]["row_count"])
                self.assertEqual(baseline["uuid_lineage_edges"], candidate["uuid_lineage_edges"])
                self.assertEqual(
                    {
                        tuple(item["security_identity"]["member_alpaca_asset_ids"])
                        for item in baseline["correlated"]["segments"]
                    },
                    {
                        tuple(item["security_identity"]["member_alpaca_asset_ids"])
                        for item in candidate["correlated"]["segments"]
                    },
                )
                proof = candidate["current_source_ownership"]
                suffix = baseline["current_source_ownership"]
                self.assertEqual(
                    {**suffix, "version": 6, "first": "2024-06-12", "split_date": suffix["first"]}, proof
                )
                validate_handoff({"schema_version": 35, "symbol": symbol, "current_source_ownership": proof})
                decide = row_correlation._decide_row

                def only_earlier_rows(context, **kwargs):
                    self.assertLess(context.session_date.isoformat(), suffix["first"])
                    return decide(context, **kwargs)

                identity = baseline["correlated"]["segments"][0]["security_identity"]
                inputs[symbol].retained_split = pricing.RetainedSplit(suffix, baseline, {"current-id": identity})
                with (
                    patch.object(
                        closed_departure,
                        "_split_observation",
                        side_effect=AssertionError("repeated split comparison"),
                    ),
                    patch.object(row_correlation, "_decide_row", side_effect=only_earlier_rows),
                ):
                    retained = self.evaluate(evidence, inputs)[symbol]
                    self.assertEqual(candidate["correlated"], retained["correlated"])
                    self.assertEqual(proof, retained["current_source_ownership"])
                    self.assertEqual(
                        {**proof, "last": "2024-06-20"},
                        closed_departure.continue_current_source(proof, evidence, "2024-06-20").to_report(),
                    )

    def test_prefix_conflicts_preserve_only_the_already_supported_suffix(self):
        symbol = "QZAM"
        for label, mutate in (
            (
                "contradictory mapping",
                lambda evidence: evidence.update(
                    mapping_endpoint_observations={"111111111": {symbol: "different-id"}}
                ),
            ),
            (
                "earlier unresolved occupant",
                lambda evidence: evidence["symbol_occupancy_evidence"].append(
                    {
                        "symbol": symbol,
                        "asset_id": None,
                        "cusip": "222222222",
                        "evidence": [{"process_date": "2024-06-13"}],
                    }
                ),
            ),
        ):
            with self.subTest(label=label):
                evidence, inputs = split_suffix_case(symbol)
                mutate(evidence)
                self.assertEqual(24, self.evaluate(evidence, inputs)[symbol]["correlated"]["row_count"])

    def test_more_than_one_split_can_bound_a_suffix_without_dating_the_uuid_change(self):
        evidence, inputs = split_suffix_case("QZAM")
        latest = evidence["corporate_actions"][-1]
        evidence["corporate_actions"].append(
            {**latest, "id": "first-split", "ex_date": "2024-03-01", "process_date": "2024-03-01"}
        )
        report = self.evaluate(evidence, inputs)["QZAM"]
        self.assertEqual("2024-06-14", report["current_source_ownership"]["first"])
        self.assertEqual([], report["uuid_lineage_edges"])
        evidence, inputs = split_suffix_case("QZAM")
        evidence["corporate_actions"].append(
            {**latest, "id": "first-split", "cusip": "", "ex_date": "2024-03-01", "process_date": "2024-03-01"}
        )
        self.assertIsNone(self.evaluate(evidence, inputs)["QZAM"]["current_source_ownership"])

    def test_split_suffix_does_not_consume_unconfirmed_basis_or_rival_ownership(self):
        for description, mutate in (
            ("different observed ratio", lambda evidence: evidence["corporate_actions"][-1].update(new_rate=3)),
            (
                "incompatible asset class",
                lambda evidence: evidence["asset_lookups"][1]["asset"].update({"class": "not_equity"}),
            ),
            (
                "later rival dividend",
                lambda evidence: evidence["corporate_actions"][1].update(ex_date="2024-06-17"),
            ),
            (
                "alternate security form",
                lambda evidence: evidence["asset_lookups"][1]["asset"].update(name="Related ADS"),
            ),
            (
                "date role conflict",
                lambda evidence: evidence["corporate_actions"][-1].update(effective_date="2024-06-17"),
            ),
        ):
            with self.subTest(description=description):
                evidence, inputs = split_suffix_case("QZAM")
                mutate(evidence)
                self.assertIsNone(self.evaluate(evidence, inputs)["QZAM"]["current_source_ownership"])

    def test_split_suffix_uses_completed_continuation_without_reopening_proof(self):
        evidence, inputs = split_suffix_case("QZAM")
        first = self.evaluate(evidence, inputs)["QZAM"]
        carried = first["current_source_ownership"]
        with patch.object(closed_departure, "_split_observation", side_effect=AssertionError("proof repeated")):
            result = closed_departure.continue_current_source(carried, evidence, "2024-06-20").to_report()
        self.assertEqual({**carried, "last": "2024-06-20"}, result)

    def test_lower_bound_is_the_resumed_session_when_old_claims_precede_inert_tape(self):
        symbol = "QZAM"
        control = replace(
            self.policy, implementation_correlation_version=closed_departure.RESUMED_CURRENT_SOURCE_VERSION - 1
        )

        def fixture():
            evidence, _ = prior_claim_case(symbol)
            days = expected_market_session_coverage("2024-01-02", "2024-11-06").session_dates
            rows = bars(days)
            opening = days.index("2024-05-01")
            resumed = days.index("2024-10-28")
            close = rows[opening - 1]["c"]
            for row in rows[opening:resumed]:
                row.update(dict.fromkeys(("o", "h", "l", "c"), close), n="0", v="0", vw="0")
            evidence["query"]["corporate_actions_end"] = "2024-11-06"
            inputs = inputs_for({symbol: rows}, asofs={symbol: "2024-11-06"})
            for download in inputs[symbol].downloads.values():
                download.manifest["request_to"] = "2024-11-06"
            inputs[symbol].source["latest_request"]["end_exclusive"] = "2024-11-06"
            return evidence, inputs, opening, len(days) - resumed

        evidence, inputs, _, _ = fixture()
        self.assertIsNone(self.evaluate(evidence, inputs, control)[symbol]["current_source_ownership"])
        evidence, inputs, _, rows = fixture()
        report = self.evaluate(evidence, inputs)[symbol]
        self.assertEqual(rows * 8, report["correlated"]["row_count"])
        self.assertEqual("2024-10-28", report["current_source_ownership"]["first"])
        self.assertEqual([], report["uuid_lineage_edges"])
        evidence, inputs, opening, _ = fixture()
        inputs[symbol].downloads["all.csv.gz"].rows[opening]["n"] = "1"
        self.assertIsNone(self.evaluate(evidence, inputs)[symbol]["current_source_ownership"])
        evidence, inputs, _, _ = fixture()
        evidence["corporate_actions"][-1]["ex_date"] = "2024-11-01"
        self.assertIsNone(self.evaluate(evidence, inputs)[symbol]["current_source_ownership"])

    def test_contemporaneous_active_observation_selects_one_session_and_preserves_earlier_uncertainty(self):
        symbol = "QZBM"

        def fixture():
            evidence, inputs = prior_claim_case(symbol)
            evidence["asset_lookups"][0]["source"] = "asset_catalogue"
            evidence["catalogue_observed_at_utc"] = "2024-06-19T12:00:00+00:00"
            evidence["corporate_actions"].append(
                {
                    "action_type": "forward_split",
                    "id": "old-split",
                    "symbol": symbol,
                    "cusip": "111111111",
                    "ex_date": "2024-04-03",
                    "old_rate": 1,
                    "new_rate": 2,
                }
            )
            return evidence, inputs

        control = replace(
            self.policy, implementation_correlation_version=closed_departure.OBSERVED_CURRENT_SESSION_VERSION - 1
        )
        evidence, inputs = fixture()
        self.assertEqual(0, self.evaluate(evidence, inputs, control)[symbol]["correlated"]["row_count"])
        evidence, inputs = fixture()
        result = self.evaluate(evidence, inputs)[symbol]
        self.assertEqual(8, result["correlated"]["row_count"])
        self.assertEqual(32, result["uncorrelated"]["row_count"])
        self.assertEqual([], result["uuid_lineage_edges"])
        proof = result["current_source_ownership"]
        self.assertEqual(
            (5, "2024-06-18", "2024-06-18", "current-id"),
            (
                proof["version"],
                proof["first"],
                proof["last"],
                proof["asset_id"],
            ),
        )
        validate_handoff({"schema_version": 35, "symbol": symbol, "current_source_ownership": proof})
        for mutate in (
            lambda e: e.update(catalogue_observed_at_utc="2024-06-17T12:00:00+00:00"),
            lambda e: e.update(catalogue_observed_at_utc="2024-06-20T15:00:00+00:00"),
            lambda e: e["corporate_actions"][-1].update(ex_date="2024-06-19"),
            lambda e: e["identities"][0].update(status="active"),
        ):
            evidence, inputs = fixture()
            mutate(evidence)
            self.assertIsNone(self.evaluate(evidence, inputs)[symbol]["current_source_ownership"])

    def test_historical_claim_authority_rejects_rival_overlap_and_structural_evidence(self):
        def next_split(evidence):
            evidence["corporate_actions"].append(
                {
                    "action_type": "forward_split",
                    "id": "one-sided",
                    "symbol": "NQZK",
                    "cusip": "111111111",
                    "ex_date": "2024-05-29",
                    "process_date": "2024-05-29",
                    "old_rate": 1,
                    "new_rate": 2,
                }
            )

        for description, mutate in (
            ("rival in tape", lambda e: e["corporate_actions"][-1].update(ex_date="2024-06-13")),
            ("unidentified dividend in tape", lambda e: e["corporate_actions"][0].update(ex_date="2024-06-13")),
            ("structural action", next_split),
            ("undated old claim", lambda e: e["corporate_actions"][-1].pop("ex_date")),
            ("rival still active", lambda e: e["identities"][0].update(status="active")),
            ("cropped action query", lambda e: e["query"].update(corporate_actions_start="2024-02-01")),
            ("single witnessed dividend date", lambda e: e["corporate_actions"].pop()),
        ):
            with self.subTest(description=description):
                evidence, inputs = prior_claim_case("NQZK")
                mutate(evidence)
                report = self.evaluate(evidence, inputs)["NQZK"]
                self.assertIsNone(report["current_source_ownership"])
                self.assertEqual(40, report["uncorrelated"]["row_count"])

    def test_historical_claim_does_not_repeat_an_accepted_source_decision(self):
        evidence, inputs = prior_claim_case("NQZK")
        eligible = self.evaluate(evidence, inputs)["NQZK"]
        competing, retained = prior_claim_case("NQZK")
        retained["NQZK"].accepted_report = eligible
        with patch.object(
            closed_departure, "_assess_prior_claim_source", side_effect=AssertionError("repeated proof")
        ):
            report = self.evaluate(competing, retained)["NQZK"]
        self.assertEqual(eligible["correlated"], report["correlated"])
        self.assertEqual(eligible["current_source_ownership"], report["current_source_ownership"])

    def test_v2_handoff_bounds_and_identity_fields_are_validated_at_ingress(self):
        evidence, inputs = prior_claim_case("NQZK")
        certificate = self.evaluate(evidence, inputs)["NQZK"]["current_source_ownership"]
        receipt = {"schema_version": 35, "symbol": "NQZK", "current_source_ownership": certificate}
        validate_handoff(receipt)
        for corrupt in (
            {"latest_prior_activity_date": "2024-06-12"},
            {"last": "2024-06-11"},
            {"prior_asset_id": "current-id"},
            {"departure_date": "2024-04-26"},
            {"version": True},
        ):
            with self.subTest(corrupt=corrupt), self.assertRaisesRegex(Exception, "owning lineage recovery"):
                validate_handoff({**receipt, "current_source_ownership": {**certificate, **corrupt}})

    def test_nearest_vetoes_leave_the_new_source_unassigned(self):
        def change_sources(inputs, key, value):
            for download in inputs["REUS"].downloads.values():
                download.manifest[key] = value

        def remove_row(inputs, adjustment=None):
            for name, download in inputs["REUS"].downloads.items():
                if adjustment is None or download.manifest["adjustment"] == adjustment:
                    download.rows.pop(1)
                    download.manifest["rows"] -= 1
                    next(item for item in inputs["REUS"].local["downloads"] if item["file"] == name)["rows"] -= 1

        mutations = {
            "historical mapping": lambda e, i: change_sources(i, "asof", "2024-04-05"),
            "disabled mapping": lambda e, i: change_sources(i, "asof", "-"),
            "cropped request": lambda e, i: change_sources(i, "request_from", "2024-06-12"),
            "missing session": lambda e, i: remove_row(i),
            "incomplete view": lambda e, i: remove_row(i, "all"),
            "partial source": lambda e, i: i["REUS"].local["downloads"][0].update(rows=6),
            "unqueried destination": lambda e, i: e.update(queried_symbols=["REUS"]),
            "partial actions": lambda e, i: e["query"].update(corporate_actions_data_quality="partial"),
            "unknown CUSIP": lambda e, i: e.update(unresolved_cusips=["unknown"]),
            "third occupant": lambda e, i: e["identities"].append(asset("REUS", "third-id")),
            "third successor occupant": lambda e, i: e["identities"].append(asset("KEEP", "third-id")),
            "missing complete counterpart": lambda e, i: i.pop("KEEP"),
            "historically mapped counterpart": lambda e, i: [
                download.manifest.update(asof="2024-04-05") for download in i["KEEP"].downloads.values()
            ],
            "duplicate current lookup": lambda e, i: e["asset_lookups"].append(
                {**e["asset_lookups"][0], "asset": {**e["asset_lookups"][0]["asset"], "status": "inactive"}}
            ),
            "failed exact lookup": lambda e, i: e["asset_lookups"][0].update(http_status=503),
            "inactive current": lambda e, i: e["identities"][-1].update(status="inactive"),
            "current not tradable": lambda e, i: e["identities"][-1].update(tradable=False),
            "third structural event": lambda e, i: e["corporate_actions"].append(
                {
                    "id": "unknown-event",
                    "action_type": "reorganization",
                    "process_date": "2024-06-13",
                    "recipients": [{"symbol": "REUS"}],
                }
            ),
            "continued old occupancy": lambda e, i: e["symbol_occupancy_evidence"].append(
                {
                    "symbol": "REUS",
                    "cusip": "old-cusip",
                    "asset_id": "old-id",
                    "evidence": [
                        {
                            "action_id": "late-dividend",
                            "action_type": "cash_dividend",
                            "process_date": "2024-06-13",
                        }
                    ],
                }
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                evidence, inputs = case()
                mutate(evidence, inputs)
                reports = self.evaluate(evidence, inputs)
                self.assertIsNone(reports["REUS"]["current_source_ownership"])
                self.assertFalse(
                    any(
                        segment.get("correlation_method") == closed_departure.METHOD
                        for segment in reports["REUS"]["correlated"]["segments"]
                    )
                )

    def test_exact_successor_copy_cannot_claim_current_identity(self):
        evidence, inputs = case()
        for name, download in inputs["REUS"].downloads.items():
            donor = {row["t"]: row for row in inputs["KEEP"].downloads[name].rows}
            download.rows[:] = [copy.deepcopy(donor[row["t"]]) for row in download.rows]
        report = self.evaluate(evidence, inputs)["REUS"]
        self.assertIsNone(report["current_source_ownership"])
        self.assertEqual(40, report["uncorrelated"]["row_count"])

    def test_completed_certificate_survives_peer_failure_and_phase_transition(self):
        evidence, inputs = case()
        from libs.alpaca_end_of_day_security_correlation.correlation import build_security_correlation_report
        from libs.alpaca_lineage_build.policy_engine import _correlation_cache, _register_sources

        cache = _correlation_cache(self.policy)
        _register_sources(cache, inputs)
        view = inputs["REUS"]
        kwargs = dict(
            authenticated_download_reader=view.reader,
            verified_session_exclusions_reader=lambda *_: {},
            authenticated_symbol_sessions=lambda _: None,
            cache=cache,
        )
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            first = build_security_correlation_report(
                {"symbol": "REUS", "local": view.local, "lineage": evidence}, Path("/unused"), **kwargs
            )
            # A later peer failure must not erase this completed source authority.
            later = _correlation_cache(self.policy)
            _register_sources(later, inputs)
            later.inherit_continuity(cache)
            second = build_security_correlation_report(
                {"symbol": "REUS", "local": view.local, "lineage": evidence},
                Path("/unused"),
                **{**kwargs, "cache": later},
            )
        self.assertEqual(1, assess.call_count)
        self.assertEqual(first["current_source_ownership"], second["current_source_ownership"])

    def test_missing_required_receipt_handoff_fails_closed(self):
        with self.assertRaisesRegex(Exception, "owning lineage recovery"):
            validate_handoff({"schema_version": 35, "symbol": "REUS"})
        evidence, inputs = case()
        certificate = self.evaluate(evidence, inputs)["REUS"]["current_source_ownership"]
        for key, value in (("version", True), ("first", "nonsense"), ("asset_id", "old-id")):
            with self.subTest(key=key), self.assertRaisesRegex(Exception, "owning lineage recovery"):
                validate_handoff(
                    {
                        "schema_version": 35,
                        "symbol": "REUS",
                        "current_source_ownership": {**certificate, key: value},
                    }
                )

    def test_actual_evaluator_retry_retains_completed_proof_until_inputs_change(self):
        evidence, inputs = case()
        original = policy_engine.build_security_correlation_report

        def fail_peer(report, *args, **kwargs):
            result = original(report, *args, **kwargs)
            if report["symbol"] == "REUS":
                self.assertIsNotNone(result["current_source_ownership"])
                raise RuntimeError("interrupted peer")
            return result

        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            with (
                patch.object(policy_engine, "build_security_correlation_report", side_effect=fail_peer),
                self.assertRaisesRegex(RuntimeError, "interrupted peer"),
            ):
                self.evaluate(evidence, inputs)
            retried = self.evaluate(evidence, inputs)
            self.assertEqual(40, retried["REUS"]["correlated"]["row_count"])
            self.assertEqual(1, sum(call.args[0] == "REUS" for call in assess.call_args_list))
            inputs["REUS"].source_sha256 = "changed-generation"
            self.evaluate(evidence, inputs)
            self.assertEqual(2, sum(call.args[0] == "REUS" for call in assess.call_args_list))

    def test_cache_is_bound_to_store_generation_and_complete_selected_scope(self):
        evidence, inputs = case()
        owner = inputs[min(inputs)]
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            self.evaluate(evidence, inputs)
            first = owner._completed_correlation_producers[3]
            self.evaluate(evidence, inputs)
            self.assertIs(first, owner._completed_correlation_producers[3])
            self.assertEqual(1, sum(call.args[0] == "REUS" for call in assess.call_args_list))
            self.evaluate(evidence, inputs, store=Path("/another-unused-store"))
            second = owner._completed_correlation_producers[3]
            self.assertIsNot(first, second)
            self.assertEqual(2, sum(call.args[0] == "REUS" for call in assess.call_args_list))
            scope = dict(store=Path("/another-unused-store"), selected_group=["REUS", "KEEP", "MISSING"])
            incomplete = self.evaluate(evidence, inputs, **scope)
            third = owner._completed_correlation_producers[3]
            self.assertIsNot(second, third)
            self.assertIsNone(incomplete["REUS"]["current_source_ownership"])
            unavailable = self.evaluate(evidence, inputs, unavailable_pricing=["MISSING"], **scope)
            self.assertIsNot(third, owner._completed_correlation_producers[3])
            self.assertIsNone(unavailable["REUS"]["current_source_ownership"])

    def test_unbound_legacy_adapter_does_not_retain_cross_call_cache(self):
        evidence, inputs = case()
        inputs["REUS"].source_sha256 = None
        facts = {name: evidence for name in inputs}
        arguments = dict(selected_group=sorted(inputs), unavailable_pricing=[])
        first = policy_engine._selected_group_cache(Path("/unused"), self.policy, inputs, facts, **arguments)
        second = policy_engine._selected_group_cache(Path("/unused"), self.policy, inputs, facts, **arguments)
        self.assertIsNot(first, second)


class ClosedDeparturePipelineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.store = Path(temporary.name) / "store"
        self.root = self.store / "end_of_day/lineage"
        self.enterContext(
            patch(
                "libs.alpaca_pricing_download.download.utc_now",
                return_value=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
        )
        self.current = asset("REUS", "current-id")
        self.old = asset("KEEP", "old-id")
        self.actions = [
            {
                "id": "departure",
                "action_type": "name_change",
                "old_symbol": "REUS",
                "new_symbol": "KEEP",
                "old_cusip": "111111111",
                "new_cusip": "111111111",
                "process_date": "2024-04-08",
            }
        ]
        self.rows = {
            "REUS": informative_bars("2024-06-12", "2024-06-19"),
            "KEEP": informative_bars("2024-04-05", "2024-06-19"),
        }
        for row in self.rows["REUS"]:
            for field in ("o", "h", "l", "c", "vw"):
                row[field] += 100
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-19", from_date="2024-01-01", client=FakeClient(rows))

    def run_lineage(self, *, expected_failures=0, **kwargs):
        with patch.object(pipeline, "download_pricing", side_effect=AssertionError("provider pricing")):
            result = pipeline.run_lineage(
                self.store,
                ["REUS"],
                identity_client=IdentityClient(
                    {"REUS": self.current, "KEEP": self.old, "111111111": self.old}, self.actions
                ),
                **kwargs,
            )
        self.assertEqual(expected_failures, result["failed"], result)
        return result

    def receipt(self, symbol="REUS"):
        return storage.read_receipt(self.root, symbol, storage.read_index(self.root))

    def test_split_history_stops_at_carry_and_append_recovers_without_replaying_either_scope(self):
        self.old.update(symbol="REUS", name=self.current["name"], status="inactive", tradable=False)
        self.actions = [
            {
                "action_type": "forward_split",
                "id": "split",
                "symbol": "REUS",
                "cusip": "111111111",
                "ex_date": "2024-06-14",
                "process_date": "2024-06-14",
                "old_rate": 1,
                "new_rate": 2,
            }
        ]
        rows = informative_bars("2024-01-02", "2024-06-19")
        for row in rows[2:102]:
            row.update(o=13, h=13, l=13, c=13, v=0, n=0, vw=0)

        class SplitClient(FakeClient):
            def fetch(self, symbol, adjustment, start, end, *, asof=None):
                selected = super().fetch(symbol, adjustment, start, end, asof=asof)
                if "split" not in adjustment:
                    return selected
                return [
                    {
                        **row,
                        **{field: Decimal(row[field]) / 2 for field in ("o", "h", "l", "c", "vw")},
                        "v": row["v"] * 2,
                    }
                    if row["t"][:10] < "2024-06-14"
                    else row
                    for row in selected
                ]

        with direct_adjustment_observations():
            download_pricing(self.store, "REUS", "2024-06-19", client=SplitClient(rows), refresh_all=True)
        self.run_lineage()
        prior = self.receipt()
        proof = prior["current_source_ownership"]
        self.assertEqual(6, proof["version"])
        self.assertEqual(rows[102]["t"][:10], proof["first"])
        self.assertEqual(102 * 8, prior["unresolved_rows"])

        added = informative_bars("2024-06-20", "2024-06-21")
        with direct_adjustment_observations():
            download_pricing(self.store, "REUS", "2024-06-21", client=SplitClient([*rows, *added]))
        original = row_correlation._decide_row

        def reject_old_rows(context, **kwargs):
            self.assertGreater(context.session_date.isoformat(), proof["last"])
            return original(context, **kwargs)

        with (
            patch.object(row_correlation, "assess_current_source", side_effect=AssertionError("source replay")),
            patch.object(
                row_correlation, "extend_split_ownership", side_effect=AssertionError("episode replay")
            ),
            patch.object(row_correlation, "_decide_row", side_effect=reject_old_rows),
            patch.object(storage, "finish_publication", side_effect=RuntimeError("publication interruption")),
        ):
            result = self.run_lineage(expected_failures=1)
        self.assertIn("publication interruption", str(result))
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("journal correlation replay")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("journal payload replay")),
        ):
            self.run_lineage()
        current = self.receipt()
        self.assertEqual({**proof, "last": "2024-06-20"}, current["current_source_ownership"])
        self.assertEqual(prior["unresolved_rows"], current["unresolved_rows"])
        self.assertEqual(prior["correlated_rows"] + 8, current["correlated_rows"])
        self.assertEqual(prior["identity_memberships"], current["identity_memberships"])

    def test_actual_pipeline_keeps_proof_and_disjoint_unresolved_peer_history(self):
        self.run_lineage()
        prior = self.receipt()
        self.assertEqual(0, prior["unresolved_rows"])
        self.assertGreater(self.receipt("KEEP")["unresolved_rows"], 0)
        retained_rows = prior["source_rows"] + self.receipt("KEEP")["source_rows"]
        for symbol, rows in self.rows.items():
            added = informative_bars("2024-06-20", "2024-06-21")
            download_pricing(self.store, symbol, "2024-06-21", client=FakeClient([*rows, *added]))
        original = row_correlation.assess_current_source
        decide = row_correlation._decide_row
        informative = correlation.has_informative_trading

        def reject_reproof(symbol, *args, **kwargs):
            if symbol == "REUS":
                raise AssertionError("completed current-source certificate was repeated")
            return original(symbol, *args, **kwargs)

        def reject_old_decision(context, **kwargs):
            if context.requested_symbol == "REUS" and context.session_date.isoformat() <= "2024-06-18":
                raise AssertionError("accepted current-source row decision was repeated")
            return decide(context, **kwargs)

        def reject_old_activity(rows):
            if rows and rows[0]["t"] == self.rows["REUS"][0]["t"] and rows[0]["c"] > 100:
                raise AssertionError("accepted historical activity was repeated")
            return informative(rows)

        with (
            patch.object(row_correlation, "assess_current_source", side_effect=reject_reproof),
            patch.object(row_correlation, "_decide_row", side_effect=reject_old_decision),
            patch.object(correlation, "has_informative_trading", side_effect=reject_old_activity),
        ):
            result = self.run_lineage()
        self.assertEqual(retained_rows, result["results"][0]["historical_rows_reused"])
        after = self.receipt()
        self.assertEqual(0, after["unresolved_rows"])
        self.assertEqual("2024-06-20", after["current_source_ownership"]["last"])
        self.assertEqual(prior["current_source_ownership"]["first"], after["current_source_ownership"]["first"])
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("unchanged calculation")),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("unchanged payloads")),
        ):
            self.run_lineage()

    def test_peer_fallback_does_not_open_certified_sources_closed_pricing(self):
        for symbol, start in (("REUS", "2024-06-12"), ("KEEP", "2024-04-05")):
            self.rows[symbol] = informative_bars(start, "2024-07-01")
            if symbol == "REUS":
                for row in self.rows[symbol]:
                    for field in ("o", "h", "l", "c", "vw"):
                        row[field] += 100
            download_pricing(
                self.store, symbol, "2024-07-01", client=FakeClient(self.rows[symbol]), refresh_all=True
            )
        self.run_lineage()
        prior = self.receipt()
        for symbol, rows in self.rows.items():
            download_pricing(
                self.store,
                symbol,
                "2024-07-02",
                client=FakeClient([*rows, *informative_bars("2024-07-01", "2024-07-02")]),
            )
        load = pricing.load_published_boundary

        def reject_closed_source(root, source, kind, **kwargs):
            if source["symbol"] == "REUS" and kind != "current_month":
                raise AssertionError("certified source's closed pricing was reopened")
            return load(root, source, kind, **kwargs)

        with patch.object(pricing, "load_published_boundary", side_effect=reject_closed_source):
            self.run_lineage()
        after = self.receipt()
        self.assertEqual(prior["correlated_rows"] + 8, after["correlated_rows"])
        self.assertEqual("2024-07-01", after["current_source_ownership"]["last"])
        self.assertEqual(prior["identity_memberships"], after["identity_memberships"])

    def test_publication_interruption_recovers_completed_proof(self):
        with patch.object(storage, "finish_publication", side_effect=RuntimeError("interrupted journal")):
            failed = pipeline.run_lineage(
                self.store,
                ["REUS"],
                identity_client=IdentityClient(
                    {"REUS": self.current, "KEEP": self.old, "111111111": self.old}, self.actions
                ),
            )
        self.assertEqual(1, failed["failed"], failed)
        with (
            patch.object(
                row_correlation, "assess_current_source", side_effect=AssertionError("journal proof repeated")
            ),
            patch.object(pipeline, "load_pricing", side_effect=AssertionError("journal prices repeated")),
        ):
            self.run_lineage()
        self.assertEqual(0, self.receipt()["unresolved_rows"])

    def test_diagnostic_marks_mixed_source_reuse_without_replaying_decisions(self):
        self.run_lineage()
        for symbol, rows in self.rows.items():
            download_pricing(
                self.store,
                symbol,
                "2024-06-21",
                client=FakeClient([*rows, *informative_bars("2024-06-20", "2024-06-21")]),
            )
        capture = EvidenceCapture()
        self.run_lineage(observer=capture)
        current, old = capture.sources["REUS"], capture.sources["KEEP"]
        self.assertEqual("added_sessions", current["actual_calculation_scope"])
        self.assertEqual("calculated_with_closed_reuse", current["status"])
        self.assertEqual("added_sessions", old["actual_calculation_scope"])
        self.assertEqual("calculated_with_closed_reuse", old["status"])
        self.assertTrue(any(item["decision_origin"] == "reused_publication" for item in old["assignments"]))
        self.assertTrue(any(item["decision_origin"] == "reused_publication" for item in current["assignments"]))
        self.assertEqual(6, len(current["pricing"]["session_grids"][0]["session_dates"]))

    def test_normal_append_and_empty_advance_consume_completed_ownership(self):
        self.rows["KEEP"] = informative_bars("2024-04-08", "2024-06-19")
        download_pricing(
            self.store, "KEEP", "2024-06-19", client=FakeClient(self.rows["KEEP"]), refresh_all=True
        )
        self.run_lineage()
        self.assertEqual(0, self.receipt("KEEP")["unresolved_rows"])
        # June 19 is an exchange holiday: coverage advances with no new row.
        for symbol, rows in self.rows.items():
            download_pricing(self.store, symbol, "2024-06-20", client=FakeClient(rows))
        with (
            patch.object(pipeline, "correlate", side_effect=AssertionError("empty append calculation")),
            patch.object(pricing, "load_published_boundary", side_effect=AssertionError("empty append prices")),
        ):
            self.run_lineage()
        for symbol, rows in self.rows.items():
            added = informative_bars("2024-06-20", "2024-06-21")
            download_pricing(self.store, symbol, "2024-06-21", client=FakeClient([*rows, *added]))
        original = row_correlation.assess_current_source

        def reject_reproof(symbol, *args, **kwargs):
            if symbol == "REUS":
                raise AssertionError("append repeated source certificate")
            return original(symbol, *args, **kwargs)

        with patch.object(row_correlation, "assess_current_source", side_effect=reject_reproof):
            result = self.run_lineage()
        self.assertEqual(16, result["results"][0]["pricing_rows_processed"])
        self.assertEqual("2024-06-20", self.receipt()["current_source_ownership"]["last"])

    def test_changed_mapping_cannot_keep_current_source_ownership(self):
        self.run_lineage()
        download_pricing(
            self.store,
            "REUS",
            "2024-06-19",
            client=FakeClient(self.rows["REUS"]),
            refresh_all=True,
            mapping_asof="2024-04-05",
        )
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            self.run_lineage()
        self.assertTrue(any(call.args[0] == "REUS" for call in assess.call_args_list))
        after = self.receipt()
        self.assertIsNone(after["current_source_ownership"])
        self.assertFalse(
            any("current-id" in item["member_alpaca_asset_ids"] for item in after["identity_memberships"])
        )

    def test_actual_changed_history_selects_a_new_source_certificate(self):
        self.run_lineage()
        replacement = copy.deepcopy(self.rows["REUS"])
        for field in ("o", "h", "l", "c", "vw"):
            replacement[1][field] += 1
        download_pricing(self.store, "REUS", "2024-06-19", client=FakeClient(replacement), refresh_all=True)
        with patch.object(
            row_correlation, "assess_current_source", wraps=closed_departure.assess_current_source
        ) as assess:
            self.run_lineage()
        self.assertEqual(1, sum(call.args[0] == "REUS" for call in assess.call_args_list))
        self.assertEqual(0, self.receipt()["unresolved_rows"])

    def test_historical_claim_append_consumes_v2_handoff_without_reproof(self):
        self.old.update(symbol="REUS", status="inactive", tradable=False)
        self.actions = prior_claim_actions("REUS")
        self.run_lineage()
        baseline = self.receipt()
        self.assertEqual(2, baseline["current_source_ownership"]["version"])
        download_pricing(
            self.store,
            "REUS",
            "2024-06-21",
            client=FakeClient([*self.rows["REUS"], *informative_bars("2024-06-20", "2024-06-21")]),
        )
        decide = row_correlation._decide_row

        def reject_old_decision(context, **kwargs):
            self.assertGreater(context.session_date.isoformat(), "2024-06-18")
            return decide(context, **kwargs)

        with (
            patch.object(row_correlation, "assess_current_source", side_effect=AssertionError("repeated proof")),
            patch.object(row_correlation, "_decide_row", side_effect=reject_old_decision),
        ):
            result = self.run_lineage()
        receipt = self.receipt()
        self.assertEqual(8, result["results"][0]["pricing_rows_processed"])
        self.assertEqual(
            {**baseline["current_source_ownership"], "last": "2024-06-20"}, receipt["current_source_ownership"]
        )
        self.assertEqual(baseline["identity_memberships"], receipt["identity_memberships"])
        self.assertEqual(0, receipt["unresolved_rows"])


if __name__ == "__main__":
    unittest.main()

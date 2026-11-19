"""Borrow only the completed traded episode across a split-adjusted rename."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from libs.alpaca_end_of_day_security_correlation import mapped_rename_chain
from libs.alpaca_end_of_day_security_correlation.mapped_rename_chain import (
    CLOSED_ENTRY_RENAME_PREFIX,
    TERMINAL_RENAME_PREFIX,
)
from libs.alpaca_end_of_day_security_correlation.ticker_reuse_gap import DATED_TERMINAL_SUFFIX_RULE_NAME
from libs.alpaca_lineage_build.adjustment_basis import INCOMPATIBLE_SPLIT_BASIS
from libs.alpaca_lineage_build.observations import select_observations
from libs.alpaca_lineage_build.transform import correlate
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.common.us_equity_market_session_calendar import expected_market_session_coverage
from tests.test_alpaca_end_of_day_security_correlation import add_name_change_edge
from tests.test_alpaca_name_change_evidence import (
    PRODUCTION_POLICY,
    active_successor_prefix_inputs,
    active_successor_prefix_reused_ticker_case,
    bars,
)
from tests.test_alpaca_split_adjusted_rename_prefix import DAYS, rename_chain, run_group


def terminal_case():
    facts, histories, boundary, count = active_successor_prefix_reused_ticker_case()
    old, retired = facts["identities"]
    old.update(status="inactive", tradable=False)
    subsequent = {
        **old,
        "id": "post-split-id",
        "lookup_cusips": ["new-cusip"],
        "status": "active",
        "tradable": True,
    }
    facts["identities"].append(subsequent)
    facts["asset_lookups"][0]["asset"] = subsequent
    facts["asset_lookups"].append({**facts["asset_lookups"][1], "lookup": "new-cusip", "asset": subsequent})
    split = histories["NEW"][-2]["t"][:10]
    facts["corporate_actions"].append(
        {
            "action_type": "reverse_split",
            "id": "reverse",
            "symbol": "NEW",
            "old_cusip": "old-cusip",
            "new_cusip": "new-cusip",
            "old_rate": 10,
            "new_rate": 1,
            "effective_date": split,
            "ex_date": split,
            "process_date": split,
        }
    )
    facts["corporate_actions"] = [a for a in facts["corporate_actions"] if a["id"] != "retired-old-exit"]
    facts["symbol_occupancy_evidence"] = [
        item for item in facts["symbol_occupancy_evidence"] if item["asset_id"] != retired["id"]
    ]
    resumed = histories["OLD"][-count]["t"][:10]
    earlier = bars(expected_market_session_coverage("2023-06-01", resumed).session_dates)
    for row in earlier[3:]:
        row.update(dict.fromkeys(("o", "h", "l", "c", "vw"), "12"), v="0", n="0")
    histories["OLD"] = [*earlier, *histories["OLD"][-count:]]
    histories["NEW"] = [*deepcopy(earlier), *histories["NEW"]]
    return facts, histories, len(earlier), boundary


def closed_entry_case():
    facts, histories = rename_chain()
    facts["corporate_actions"] = [a for a in facts["corporate_actions"] if a["id"] != "rename-1"]
    facts["symbol_occupancy_evidence"] = [
        e for e in facts["symbol_occupancy_evidence"] if e["evidence"][0]["action_id"] != "rename-1"
    ]
    add_name_change_edge(
        facts,
        action_id="original-owner-exits",
        process_date=DAYS[3],
        predecessor=("prior-id", "MIDDLE", "prior-cusip", "Previous security"),
        successor=("id-A", "MIDDLE", "CUSIP-A", "Entering security"),
    )
    prior = next(a for a in facts["identities"] if a["id"] == "prior-id")
    prior["status"] = "inactive"
    facts["asset_lookups"].append({**facts["asset_lookups"][0], "lookup": "prior-cusip", "asset": prior})
    facts["queried_symbols"] = ["MIDDLE", "LAST"]
    del histories["FIRST"]
    return facts, histories


def inputs_with_scales(histories, scales):
    adjusted = deepcopy(histories["NEW"])
    for i, row in enumerate(adjusted):
        factor = scales(i)
        for field in ("o", "h", "l", "c", "vw"):
            row[field] = str(int(row[field]) * factor)
        row["v"] = str(int(row["v"]) // factor)
    views = {mode: adjusted for mode in ADJUSTMENTS if mode == "all" or "split" in mode.split(",")}
    return active_successor_prefix_inputs(histories, view_histories={"NEW": views})


def run(facts, inputs, version):
    return correlate(
        Path("."),
        inputs,
        dict.fromkeys(inputs, facts),
        policy=replace(PRODUCTION_POLICY, implementation_correlation_version=version),
    )


class TerminalRenamePrefixTests(TestCase):
    def test_borrows_only_the_completed_episode_and_preserves_the_earlier_owner(self):
        facts, histories, before, boundary = terminal_case()
        inputs = inputs_with_scales(histories, lambda _: 10)
        prior = run(facts, inputs, 92)
        reports = run(facts, inputs, 93)
        for mode in ADJUSTMENTS:
            donor = [s for s in reports["OLD"]["correlated"]["segments"] if s["adjustment"] == mode]
            mapped = [
                s
                for s in reports["NEW"]["correlated"]["segments"]
                if s["adjustment"] == mode and TERMINAL_RENAME_PREFIX in s["warnings"]
            ]
            self.assertEqual(
                [(before + 1, len(histories["OLD"]))], [(s["row_start"], s["row_end"]) for s in mapped]
            )
            self.assertEqual("OLD", mapped[0]["historical_symbol"])
            self.assertEqual(boundary, mapped[0]["ticker_ownership_interval"]["end_date_exclusive"])
            self.assertEqual(
                histories["NEW"][before]["t"][:10],
                mapped[0]["ticker_ownership_interval"]["start_date_inclusive"],
            )
            self.assertEqual(DATED_TERMINAL_SUFFIX_RULE_NAME, donor[-1]["correlation_method"])
            self.assertEqual(donor[-1]["canonical_security"], mapped[0]["canonical_security"])
            self.assertEqual(donor[-1]["security_identity"], mapped[0]["security_identity"])
            unresolved = [s for s in reports["NEW"]["uncorrelated"]["segments"] if s["adjustment"] == mode]
            self.assertEqual([(1, before)], [(s["row_start"], s["row_end"]) for s in unresolved])
        self.assertEqual(prior["OLD"]["correlated"], reports["OLD"]["correlated"])
        self.assertFalse(
            any(TERMINAL_RENAME_PREFIX in s["warnings"] for s in prior["NEW"]["correlated"]["segments"])
        )

    def test_scoped_raw_tape_is_compared_once_and_only_after_ownership_is_known(self):
        facts, histories, before, _ = terminal_case()
        # A difference elsewhere in the old label's history cannot spill into its dated run.
        histories["NEW"][0]["vw"] = "999"
        inputs = inputs_with_scales(histories, lambda _: 10)
        compared = []
        original = mapped_rename_chain._same_transaction

        def compare(left, right):
            self.assertNotIn(left["t"], compared)
            compared.append(left["t"])
            return original(left, right)

        with patch.object(mapped_rename_chain, "_same_transaction", compare):
            run(facts, inputs, 93)
        self.assertEqual([row["t"] for row in histories["OLD"][before:]], compared)

    def test_a_single_within_scope_raw_difference_rejects_the_whole_borrow(self):
        for field in ("n", "vw"):
            with self.subTest(field=field):
                facts, histories, before, _ = terminal_case()
                histories["NEW"][before + 2][field] = "999"
                reports = run(facts, inputs_with_scales(histories, lambda _: 10), 93)
                for mode in ADJUSTMENTS:
                    unowned = [s for s in reports["NEW"]["uncorrelated"]["segments"] if s["adjustment"] == mode]
                    self.assertEqual(
                        [(1, len(histories["OLD"]))], [(s["row_start"], s["row_end"]) for s in unowned]
                    )

    def test_each_adjustment_must_hand_off_the_same_dated_owner_and_generation(self):
        facts, histories, _, _ = terminal_case()
        baseline = run(facts, inputs_with_scales(histories, lambda _: 10), 92)
        for case in ("different owner", "another generation", "different request scope", "different method"):
            with self.subTest(case=case):
                inputs = inputs_with_scales(histories, lambda _: 10)
                donor = deepcopy(baseline["OLD"])
                episode = next(
                    s
                    for s in donor["correlated"]["segments"]
                    if s["adjustment"] == "spin-off"
                    and s["correlation_method"] == DATED_TERMINAL_SUFFIX_RULE_NAME
                )
                if case == "different owner":
                    episode["canonical_security"] = {
                        **episode["canonical_security"],
                        "alpaca_asset_id": "retired-old-id",
                    }
                elif case == "another generation":
                    episode["download_sha256"] = "replacement"
                elif case == "different request scope":
                    episode["request_to"] = "2024-01-01"
                else:
                    episode["correlation_method"] = "requested_symbol_dated_uuid_interval"
                inputs["OLD"].accepted_report = donor
                with patch.object(
                    mapped_rename_chain, "_same_transaction", side_effect=AssertionError("read unowned tape")
                ):
                    reports = run(facts, inputs, 93)
                unresolved = reports["NEW"]["uncorrelated"]
                self.assertEqual(len(histories["OLD"]) * len(ADJUSTMENTS), unresolved["row_count"])

    def test_two_applicable_rename_intervals_cannot_lend_the_donor(self):
        facts, histories, _, _ = terminal_case()
        original = mapped_rename_chain._terminal_scope

        def competing(report, raw_file, edge, intervals, stop, members):
            matching = next(
                i
                for i in intervals
                if i.historical_symbol == "OLD" and i.end_date_exclusive == edge.effective_date
            )
            second = replace(matching, start_date_inclusive=edge.effective_date.replace(day=1))
            return original(report, raw_file, edge, [*intervals, second], stop, members)

        with (
            patch.object(mapped_rename_chain, "_terminal_scope", competing),
            patch.object(
                mapped_rename_chain, "_same_transaction", side_effect=AssertionError("read ambiguous tape")
            ),
        ):
            reports = run(facts, inputs_with_scales(histories, lambda _: 10), 93)
        self.assertEqual(len(histories["OLD"]) * len(ADJUSTMENTS), reports["NEW"]["uncorrelated"]["row_count"])

    def test_earlier_basis_is_superseded_only_after_two_consistent_traded_opening_sessions(self):
        for case in (
            "common scale",
            "same basis",
            "single correction",
            "different scales",
            "zero trades",
            "zero volume",
            "rounded split volume",
        ):
            with self.subTest(case=case):
                facts, histories, before, _ = terminal_case()

                def scale(i):
                    if case == "same basis":
                        return 1
                    if case == "single correction":
                        return 10 if i == before else 1
                    if case == "different scales" and i == before + 1:
                        return 20
                    return 10

                for rows in histories.values():
                    if case == "zero trades":
                        rows[before + 1]["n"] = "0"
                    elif case == "zero volume":
                        rows[before + 1]["v"] = "0"
                    elif case == "rounded split volume":
                        rows[before + 1].update(v="1", n="1")
                inputs = inputs_with_scales(histories, scale)
                reports = run(facts, inputs, 93)
                selected, corrections = select_observations(inputs, reports)
                self.assertEqual({}, corrections)
                for mode in ADJUSTMENTS:
                    prefix = next(
                        s
                        for s in selected["OLD"]["correlated"]["segments"]
                        if s["adjustment"] == mode and s["row_start"] == 1
                    )
                    self.assertEqual(before, prefix["row_end"])
                    self.assertEqual("old-id", prefix["canonical_security"]["alpaca_asset_id"])
                    excluded = case in ("common scale", "rounded split volume")
                    self.assertEqual("superseded" if excluded else "selected", prefix["observation_selection"])
                    self.assertEqual(excluded, INCOMPATIBLE_SPLIT_BASIS in prefix["warnings"])
                    mapped = [
                        s
                        for s in selected["NEW"]["correlated"]["segments"]
                        if s["adjustment"] == mode and TERMINAL_RENAME_PREFIX in s["warnings"]
                    ]
                    self.assertEqual(
                        ["selected" if excluded else "superseded"], [s["observation_selection"] for s in mapped]
                    )


class ClosedEntryRenamePrefixTests(TestCase):
    def test_entry_starts_a_new_basis_without_absorbing_the_earlier_security(self):
        facts, histories = closed_entry_case()
        for version in (92, 93):
            with self.subTest(version=version):
                inputs, reports = run_group(facts, histories, version)
                selected, corrections = select_observations(inputs, reports)
                self.assertEqual({}, corrections)
                for mode in ADJUSTMENTS:
                    last = [s for s in selected["LAST"]["correlated"]["segments"] if s["adjustment"] == mode]
                    earlier = [
                        s for s in selected["LAST"]["uncorrelated"]["segments"] if s["adjustment"] == mode
                    ]
                    donor = [s for s in selected["MIDDLE"]["correlated"]["segments"] if s["adjustment"] == mode]
                    self.assertEqual(
                        [(1, 3 if version == 93 else 7)], [(s["row_start"], s["row_end"]) for s in earlier]
                    )
                    self.assertEqual(
                        (8, 11, "selected", "LAST"),
                        (
                            last[-1]["row_start"],
                            last[-1]["row_end"],
                            last[-1]["observation_selection"],
                            last[-1]["historical_symbol"],
                        ),
                    )
                    self.assertEqual(1, len(donor))
                    self.assertEqual(
                        "superseded" if version == 93 else "selected", donor[0]["observation_selection"]
                    )
                    mapped = [s for s in last if CLOSED_ENTRY_RENAME_PREFIX in s["warnings"]]
                    if version == 93:
                        self.assertEqual(
                            [(4, 7, "MIDDLE", "selected")],
                            [
                                (
                                    s["row_start"],
                                    s["row_end"],
                                    s["historical_symbol"],
                                    s["observation_selection"],
                                )
                                for s in mapped
                            ],
                        )
                        self.assertEqual(donor[0]["security_identity"], mapped[0]["security_identity"])
                        self.assertNotIn("prior-id", mapped[0]["security_identity"]["member_alpaca_asset_ids"])
                    else:
                        self.assertEqual([], mapped)

    def test_a_missing_entry_endpoint_disconnected_split_or_raw_difference_keeps_donor_prices(self):
        for case in (
            "missing prior lookup",
            "missing entering lookup",
            "missing connecting split",
            "unequal transaction",
        ):
            with self.subTest(case=case):
                facts, histories = closed_entry_case()
                if case in ("missing prior lookup", "missing entering lookup"):
                    target = "prior-cusip" if case == "missing prior lookup" else "CUSIP-A"
                    facts["asset_lookups"] = [
                        record for record in facts["asset_lookups"] if record["lookup"] != target
                    ]
                elif case == "missing connecting split":
                    facts["corporate_actions"] = [
                        action for action in facts["corporate_actions"] if action["id"] != "split-B"
                    ]
                else:
                    histories["LAST"][5]["vw"] = "99"
                inputs, reports = run_group(facts, histories, 93)
                selected, corrections = select_observations(inputs, reports)
                self.assertEqual({}, corrections)
                for mode in ADJUSTMENTS:
                    unowned = [
                        s for s in selected["LAST"]["uncorrelated"]["segments"] if s["adjustment"] == mode
                    ]
                    donor = [s for s in selected["MIDDLE"]["correlated"]["segments"] if s["adjustment"] == mode]
                    self.assertEqual([(1, 7)], [(s["row_start"], s["row_end"]) for s in unowned])
                    self.assertEqual(
                        [(1, 4, "selected")],
                        [(s["row_start"], s["row_end"], s["observation_selection"]) for s in donor],
                    )

    def test_prior_public_sessions_block_only_the_new_numerical_switch(self):
        inputs, reports = run_group(*closed_entry_case(), 93)
        # The same public identity also has completed assignments from an earlier label.
        previous_inputs, previous_reports = run_group(*rename_chain(), 91)
        inputs["FIRST"], reports["FIRST"] = previous_inputs["FIRST"], previous_reports["FIRST"]
        selected, corrections = select_observations(inputs, reports)
        self.assertEqual({}, corrections)
        for mode in ADJUSTMENTS:
            for symbol, expected in (("FIRST", "selected"), ("MIDDLE", "selected"), ("LAST", "superseded")):
                segment = next(s for s in selected[symbol]["correlated"]["segments"] if s["adjustment"] == mode)
                self.assertEqual(expected, segment["observation_selection"])
                self.assertNotIn(INCOMPATIBLE_SPLIT_BASIS, segment["warnings"])
                if symbol == "LAST":
                    self.assertIn(CLOSED_ENTRY_RENAME_PREFIX, segment["warnings"])

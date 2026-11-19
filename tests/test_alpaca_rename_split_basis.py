"""Derive split-only prices from independent native action terms and a bounded mapped-route contest."""

import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_alpaca_carried_rename import DAYS, bar

from libs.alpaca_lineage_build import dated_price_publication as published
from libs.alpaca_lineage_build import rename_split_basis as source
from libs.alpaca_pricing_download import rename_split_basis as basis
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval

CUT = DAYS[2]
SPLIT = DAYS[8]


class RenameBasisTests(unittest.TestCase):
    def setUp(self):
        self.views = {}
        self.ranges = [
            dict(ticker="OLD", first_session=DAYS[0], last_session=DAYS[1]),
            dict(ticker="FUT", first_session=CUT, last_session=DAYS[-1]),
        ]
        self.name = dict(
            id="rename", action_type="name_change", old_symbol="OLD", new_symbol="FUT", process_date=CUT
        )
        self.split = dict(
            id="split", action_type="reverse_split", symbol="FUT", old_rate=3, new_rate=1, ex_date=SPLIT
        )
        for mode in ADJUSTMENTS:
            rows = [bar(d, "OLD" if d < CUT else "FUT", "0.5125", True) for d in DAYS[:5]]
            if mode in basis._SPLIT:
                for r in rows[2:]:
                    r.update(
                        **{field: "1.537" for field in ("open", "high", "low", "close", "vwap")}, volume="26"
                    )
            self.views[mode] = {"bars": rows}
        self.inputs = dict(views=self.views, ranges=self.ranges, actions=[self.name, self.split])
        self.raw = {r["session"]: r for r in self.views["raw"]["bars"]}
        self.mapped = {CUT: self.raw[CUT]}
        self.observations = []

    def observe(self, ticker, asof, mode, first, last):
        key = ticker, asof, mode, first, last
        self.assertNotIn(key, self.observations)
        self.observations.append(key)
        self.assertEqual(key, ("FUT", CUT, "raw", DAYS[1], CUT))
        return self.mapped

    def test_future_labels_derive_provider_precision_preserving_raw_and_later_sessions(self):
        bundles, desc = basis.assess(self.inputs, CUT, self.observe)
        self.assertEqual(desc["share_basis"], "3")
        self.assertEqual(desc["raw_revisions"], [])
        self.assertEqual(desc["basis_action_scope"]["symbols"], ["FUT", "OLD"])
        self.assertEqual(desc["basis_action_scope"]["through"], SPLIT)
        for mode in ADJUSTMENTS:
            self.assertEqual(list(bundles[mode]), DAYS[:2])
            self.assertEqual(bundles[mode][DAYS[0]]["close"], "1.537" if mode in basis._SPLIT else "0.5125")
            self.assertEqual(bundles[mode][DAYS[0]]["tickers"], ["OLD"])
        self.assertEqual(len(self.observations), 1)

    def test_changed_cash_view_split_terms_and_competing_outgoing_occupants_decline(self):
        for issue in (
            "cash_before",
            "cash_after",
            "wrong_factor",
            "prior_trade",
            "successor_trade",
            "terms",
            "no_rename",
            "same_label_later_occupant",
            "unwitnessed_earlier_split",
            "true_rename_split",
            "adr_units",
            "missing_conversion",
            "new_cusip",
        ):
            with self.subTest(issue=issue):
                self.setUp()
                if issue.startswith("cash_"):
                    self.views["dividend"]["bars"][0 if issue == "cash_before" else 2]["close"] = "0.5"
                elif issue == "wrong_factor":
                    self.split["old_rate"] = 4
                elif issue == "prior_trade":
                    self.mapped[DAYS[1]] = bar(DAYS[1], "FUT", "1.2", True)
                elif issue == "successor_trade":
                    self.mapped[CUT] = bar(CUT, "FUT", "0.3", True)
                elif issue == "terms":
                    self.inputs["actions"].append(
                        dict(
                            id="merger",
                            action_type="stock_merger",
                            acquiree_symbol="OLD",
                            acquiree_rate=2,
                            acquirer_rate=1,
                            effective_date=DAYS[1],
                        )
                    )
                elif issue == "true_rename_split":
                    self.inputs["actions"].append(
                        {**self.split, "id": "resume", "ex_date": CUT, "symbol": "OLD"}
                    )
                elif issue == "adr_units":
                    self.inputs["actions"].append(
                        dict(id="newunits", action_type="reorganization", symbol="FUT", effective_date=CUT)
                    )
                elif issue == "missing_conversion":
                    self.inputs["actions"].append(
                        dict(
                            id="merger",
                            action_type="stock_merger",
                            acquiree_symbol="OLD",
                            acquirer_symbol="FUT",
                            effective_date=DAYS[1],
                            acquirer_rate=1,
                        )
                    )
                elif issue == "new_cusip":
                    self.name.update(old_cusip="old", new_cusip="new")
                elif issue == "no_rename":
                    self.inputs["actions"].remove(self.name)
                elif issue == "same_label_later_occupant":
                    self.ranges[1]["last_session"] = DAYS[6]
                elif issue == "unwitnessed_earlier_split":
                    self.inputs["actions"].append(
                        {**self.split, "id": "earlier", "ex_date": DAYS[1], "symbol": "OLD"}
                    )
                with self.assertRaises(UnsupportedInterval):
                    basis.assess(self.inputs, CUT, self.observe)

    def test_both_tapes_traded_incompatible_horizons_and_direct_mapping(self):
        self.mapped[DAYS[1]] = self.raw[DAYS[1]]

        def observe(ticker, asof, view, first, last):
            if view == "raw":
                return self.observe(ticker, asof, view, first, last)
            self.assertEqual((ticker, asof, view, first, last), ("FUT", CUT, "all", CUT, CUT))
            return {CUT: self.views["all"]["bars"][2]}

        with self.assertRaisesRegex(UnsupportedInterval, "ordinary dated mapping"):
            basis.assess(self.inputs, CUT, observe)
        self.observations.clear()

        def different(*key):
            return self.observe(*key) if key[2] == "raw" else {CUT: bar(CUT, "FUT", "61.48", True)}

        _, desc = basis.assess(self.inputs, CUT, different)
        self.assertEqual(desc["share_basis"], "3")

    def test_contested_carry_requires_live_literal_and_resumes_without_provider_recheck(self):
        self.mapped[DAYS[1]] = bar(DAYS[1], "FUT", "30", False)
        state = {}

        def interrupt(key, result):
            state[key] = result
            if key == "mapping":
                raise OSError("after mapped contest")

        with self.assertRaises(OSError):
            basis.assess(self.inputs, CUT, self.observe, completed=state, checkpoint=interrupt)

        def literal(ticker, asof, mode, lo, hi):
            self.assertEqual((ticker, asof, mode, lo, hi), ("OLD", "-", "raw", DAYS[1], DAYS[1]))
            return {lo: self.raw[lo]}

        with patch.object(basis, "_plan", side_effect=AssertionError("repeated completed action comparison")):
            bundles, desc = basis.assess(self.inputs, CUT, literal, completed=state)
        with patch.object(
            basis, "_prices", side_effect=AssertionError("repeated accepted numerical construction")
        ):
            self.assertEqual(
                basis.assess(None, CUT, Mock(side_effect=AssertionError("repeated HTTP")), completed=state),
                [bundles, desc],
            )

    def test_membership_action_invalidation_and_provider_only_inputs(self):
        _, interval = basis.assess(self.inputs, CUT, self.observe)
        interval.update(members=["FUT", "OLD"], unavailable=[])
        self.assertTrue(source.valid_scope(interval))
        old = dict(
            source_sha256="before",
            boundaries={"part": 1},
            input_sources={"FUT": "x", "OLD": "y"},
            evidence=dict(corporate_actions=[self.name, self.split]),
            dated_price_intervals={"pid": interval},
        )
        for changed in ("rate", "unrelated_action", "old_future_ownership", "later_append"):
            new = deepcopy(old)
            new.pop(published.FIELD)
            if changed == "rate":
                new["evidence"]["corporate_actions"][1]["old_rate"] = 5
            if changed == "unrelated_action":
                new["evidence"]["corporate_actions"].append(
                    dict(action_type="cash_dividend", symbol="FUT", id="cash", ex_date=DAYS[4])
                )
            if changed in ("old_future_ownership", "later_append"):
                new["boundaries"] = {"part": 2}
                published.continue_closed(old, new, SPLIT if changed == "old_future_ownership" else DAYS[-1])
            published.retain(old, new, continued=True)
            self.assertEqual(published.FIELD in new, changed in ("later_append", "unrelated_action"))
        evidence = {"provider_corporate_actions": [{**self.split, "research_certificates": ["forbidden"]}]}
        with self.assertRaisesRegex(ValueError, "injection"):
            source.selected_actions(evidence, {"FUT"}, DAYS[0])
        other = dict(security_identity={"security_id": "other"}, historical_symbol="OLD")
        other.update({"from": DAYS[0], "to": DAYS[1]})
        snap = SimpleNamespace(
            _selected_receipts={},
            _assignments={"else": [("raw", "part", other)]},
            dated_acquisition_inputs=Mock(side_effect=AssertionError("opened ambiguous prices")),
        )
        from datetime import date

        who = SimpleNamespace(public_id="this", first_session=date.fromisoformat(DAYS[0]))
        with self.assertRaisesRegex(UnsupportedInterval, "another selected raw"):
            source.inputs(snap, who, CUT, self.ranges)

    def test_native_action_selection_carries_competing_nonstandard_roles(self):
        from datetime import date

        owner = SimpleNamespace(public_id="id", first_session=date.fromisoformat(DAYS[0]))
        for action, accepted in (
            (dict(action_type="unit_split", old_symbol="OLD", new_symbol="SHARES"), False),
            (dict(action_type="stock_dividend", stock_movements=[{"recipient_symbol": "OLD"}]), False),
            (
                dict(action_type="unit_split", old_symbol="THIRD", new_symbol="OLD", alternate_symbol="RIGHT"),
                True,
            ),
            (
                dict(
                    action_type="stock_merger",
                    acquiree_symbol="OLD",
                    acquirer_symbol="FUT",
                    acquiree_rate=None,
                    acquirer_rate=1,
                ),
                False,
            ),
        ):
            with self.subTest(action=action):
                self.observations.clear()
                action = dict(action, id="event", effective_date=DAYS[1])
                snapshot = SimpleNamespace(
                    _assignments={},
                    _selected_receipts={
                        "OLD": {"evidence": {"corporate_actions": [self.name, self.split, action]}}
                    },
                    dated_acquisition_inputs=Mock(return_value=self.views),
                )
                inputs = source.inputs(snapshot, owner, CUT, self.ranges)
                self.assertIn(action, inputs["actions"])
                if accepted:
                    _, result = basis.assess(inputs, CUT, self.observe)
                    self.assertEqual(result["share_basis"], "3")
                else:
                    with self.assertRaises(UnsupportedInterval):
                        basis.assess(inputs, CUT, self.observe)
                    self.assertFalse(self.observations)

    def test_future_equivalent_share_horizon_preserves_prior_numerical_scope(self):
        self.ranges[1]["last_session"] = "2033-01-01"
        earlier = deepcopy(self.inputs)
        self.inputs["actions"] += [
            {**self.split, "id": "next", "old_rate": 2, "ex_date": "2032-02-01"},
            {
                **self.split,
                "id": "inverse",
                "action_type": "forward_split",
                "old_rate": 1,
                "new_rate": 2,
                "ex_date": "2032-03-01",
            },
        ]
        _, desc = basis.assess(self.inputs, CUT, self.observe)
        self.assertEqual(desc["basis_action_scope"]["through"], SPLIT)
        self.assertEqual(
            source.relevant_actions({"corporate_actions": earlier["actions"]}, desc),
            source.relevant_actions({"corporate_actions": self.inputs["actions"]}, desc),
        )

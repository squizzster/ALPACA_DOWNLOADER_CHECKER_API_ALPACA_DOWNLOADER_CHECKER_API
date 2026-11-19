"""Only a dated provider route can distinguish two uses of a ticker from a temporary halt."""

import unittest
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from libs.alpaca_lineage_build import occupation_storage
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.occupation_boundary import select
from libs.alpaca_lineage_build.occupation_repair import has_prior_anchor
from libs.alpaca_lineage_build.occupation_review import OccupationReview

D = [(date(2024, 4, 1) + timedelta(days=i)).isoformat() for i in range(9)]


def row(day, close, volume):
    return {
        "timestamp": day + "T04:00:00Z",
        **dict.fromkeys(("open", "high", "low", "close"), str(close)),
        "vwap": str(close if volume else 0),
        "volume": str(volume),
        "trade_count": "2" if volume else "0",
    }


class OccupationTests(unittest.TestCase):
    def setUp(self):
        self.literal = {
            d: row(d, p, v)
            for d, p, v in zip(D, [9, 10, 10, 10, 10, 10, 11, 12, 14], [100, 120, 0, 0, 0, 0, 200, 201, 202])
        }
        self.current = {d: r for d, r in self.literal.items() if d >= D[2]}
        self.old = {d: r if d < D[2] else row(d, 50 + i, 300) for i, (d, r) in enumerate(self.literal.items())}
        self.forbidden = Mock(side_effect=AssertionError("unexpected historical route read"))

    def test_mapping_excludes_all_earlier_trades_even_when_it_echoes_departed_price(self):
        prior = Mock(return_value=self.old)
        result = select(self.literal, self.literal, self.current, prior)
        self.assertEqual((D[6], D[1]), (result["first"], result["last_prior_trade"]))
        prior.assert_called_once_with(D[1])

    def test_two_dated_concurrent_tapes_distinguish_same_label(self):
        prior = Mock(return_value=self.old)
        result = select(self.literal, self.literal, self.literal, prior)
        self.assertEqual(result["first"], D[6])
        self.assertEqual(result["reason"], "historical_route_keeps_trading_against_different_later_tape")
        prior.assert_called_once_with(D[1])

    def test_resumed_listing_or_identical_route_remains_assigned(self):
        self.assertIsNone(select(self.literal, self.literal, self.literal, lambda _: self.literal))
        self.assertIsNone(select(self.literal, self.literal, self.literal, lambda _: self.current))

    def test_selected_owner_must_cover_every_new_traded_date(self):
        self.current.pop(D[7])
        self.assertIsNone(select(self.literal, self.literal, self.current, self.forbidden))

    def test_trade_information_on_one_session_cannot_be_carried_as_a_quote(self):
        self.current[D[4]] = row(D[4], 10, 10)
        self.assertIsNone(select(self.literal, self.literal, self.current, self.forbidden))

    def test_prices_inactive_before_the_restart_must_be_the_departed_price(self):
        self.current[D[4]] = row(D[4], 9, 0)
        self.assertIsNone(select(self.literal, self.literal, self.current, self.forbidden))

    def test_old_scope_duplicate_or_changed_raw_fields_are_recorded_at_the_correct_boundary(self):
        old = {**self.literal, D[0]: row(D[0], 55, 200)}
        self.assertIsNone(select(old, self.literal, self.current, lambda _: self.old))
        later = {**self.literal, D[8]: row(D[8], 14, 200000)}
        result = select(later, self.literal, self.current, lambda _: self.old)
        self.assertEqual(result["price_revision_dates"], [D[8]])

    def test_component_does_not_replace_independent_close_matching_without_trade_volume(self):
        later = {**self.current, D[8]: row(D[8], 14, 200000)}
        self.assertIsNone(select(self.literal, self.literal, later, self.forbidden))

    def test_remove_old_scope_from_compact_registration(self):
        self.assertEqual(
            occupation_storage.index_fields({"symbol": "A"}, {occupation_storage.PIVOTS: {"pid": D[1]}}),
            {occupation_storage.PIVOTS: {}},
        )
        self.assertEqual(occupation_storage.index_fields({"symbol": "A"}, {}), {})

    def test_digest_only_snapshot_accepts_the_bound_occupation(self):
        pid, token = "a" * 64, "b" * 32
        decision = {
            "version": 1,
            "public_id": pid,
            "token": token,
            "symbol": "EARLY",
            "locator": "EARLY",
            "members": ["EARLY"],
            "unavailable": [],
            "source_decision": "c" * 32,
            "source_pricing": {"EARLY": "d" * 64},
            "current_symbol": "LATER",
            "prior_asof": D[1],
            "current_asof": D[-1],
            "prior_first_trade": D[0],
            "last_prior_trade": D[1],
            "first": D[3],
            "price_revision_dates": [],
            "reason": "current_route_excludes_previous_trades",
        }
        receipt = {"symbol": "EARLY", occupation_storage.FIELD: {pid: decision}}
        occupation_storage.accept(receipt, {"sha256": "e" * 64})
        with self.assertRaises(RecoveryRequired):
            occupation_storage.accept(receipt, {"sha256": "e" * 64, occupation_storage.TOKENS: {}})
        with self.assertRaises(RecoveryRequired):
            occupation_storage.accept(receipt, {"sha256": "e" * 64, "source_sha256": "d" * 64})
        decision["last_prior_trade"] = D[-1]
        with self.assertRaises(RecoveryRequired):
            occupation_storage.accept(receipt, {"sha256": "e" * 64})

    def test_generation_or_declared_token_mismatch_fails_before_payload_read(self):
        with self.assertRaises(RecoveryRequired):
            occupation_storage.accept({"symbol": "A"}, {occupation_storage.TOKENS: {"p": "missing"}})
        with TemporaryDirectory() as root:
            with self.assertRaises((ValueError, RecoveryRequired)):
                occupation_storage.earlier_rows(Path(root), {"locator": "A", "token": "../../outside/file"})


class OccupationControls(unittest.TestCase):
    setUp = OccupationTests.setUp

    def test_reciprocal_truncation_across_short_halt_keeps_owner(self):
        historical = {d: self.literal[d] for d in D[:2]}
        self.assertIsNone(select(self.literal, self.literal, self.current, lambda _: historical))

    def test_actual_native_owner_anchor_vetoes_both_route_patterns(self):
        for current in (self.current, self.literal):
            self.assertIsNone(
                select(self.literal, self.literal, current, self.forbidden, anchored_before=lambda _: True)
            )
        self.forbidden.assert_not_called()

    def test_only_contemporary_same_label_and_uuid_support_veto(self):
        op = {
            "symbol": "TEC",
            "asset_id": "security-one",
            "cusip": "000111",
            "evidence": [{"process_date": D[0]}],
        }
        proof = {"entry": {"evidence": {"symbol_occupancy_evidence": [op]}}}
        args = (proof, {"member_alpaca_asset_ids": ["security-one"]}, "TEC", D[0], D[1])
        self.assertTrue(has_prior_anchor(*args))
        for changes in (
            {"symbol": "PREF"},
            {"cusip": ""},
            {"asset_id": "other"},
            {"evidence": [{"process_date": D[7]}]},
        ):
            with self.subTest(changes=changes):
                proof["entry"]["evidence"]["symbol_occupancy_evidence"] = [{**op, **changes}]
                self.assertFalse(has_prior_anchor(*args))

    def test_native_rename_into_symbol_before_price_coverage_protects_existing_owner(self):
        event = {"role": "new", "process_date": "2020-08-07"}
        observation = {"symbol": "TEC", "asset_id": "one", "cusip": "000111", "evidence": [event]}
        receipts = {"TEC": {"evidence": {"symbol_occupancy_evidence": [observation]}}}
        scope = {"ticker": "TEC", "cusip": "000111", "start_inclusive": "2020-08-10", "end_exclusive": None}
        args = receipts, {"member_alpaca_asset_ids": ["one"]}, "TEC", D[0], D[1]
        self.assertTrue(has_prior_anchor(*args, [scope]))
        self.assertFalse(has_prior_anchor(*args, [{**scope, "end_exclusive": "2023-01-04"}]))
        event["role"] = "old"
        self.assertFalse(has_prior_anchor(*args, [scope]))

    def test_only_long_reciprocal_campaigns_select_without_concurrent_competing_tape(self):
        days = [(date(2024, 1, 1) + timedelta(days=i)).isoformat() for i in range(30)]
        raw = {d: row(d, 10 if i < 28 else 20 + i, 100 if i < 2 or i >= 28 else 0) for i, d in enumerate(days)}
        current = {d: raw[d] for d in days[28:]}
        for history, accepted in (
            ({d: raw[d] for d in days[:2]}, True),
            ({days[0]: raw[days[0]]}, False),
            (raw, False),
        ):
            self.assertEqual(select(raw, raw, current, lambda _: history) is not None, accepted)

    def test_no_cross_label_or_out_of_scope_removal(self):
        from libs.alpaca_lineage_build.occupation_assignments import OccupationProjection, UnsupportedOverlap

        projection = OccupationProjection(
            self.literal,
            {"security_id": "p"},
            {"selection": [{"first": D[6]}]},
            "TEC",
            {"first_session": D[0], "last_session": D[-1]},
            [],
            [],
        )
        for sym, day in (("PREF", D[1]), ("TEC", "2019-01-01")):
            with self.assertRaises(UnsupportedOverlap):
                projection.segments(
                    {
                        "branch": "correlated",
                        "security_identity": {"security_id": "p"},
                        "from": day,
                        "to": day,
                        "historical_symbol": sym,
                    },
                    "raw",
                )


class OccupationRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(TemporaryDirectory()))
        self.group = {
            "locator": "ACME",
            "references": {"pricing": {"ACME": "p"}, "lineage": {"ACME": "a"}},
            "decision_generation": "g",
        }
        self.pid = "b" * 64
        self.forbidden = Mock(side_effect=AssertionError("repeated completed work"))

    def open(self):
        return OccupationReview(self.root, self.group, self.pid)

    def test_partial_acquisition_recovers_only_remaining_route(self):
        review = self.open()
        self.assertEqual(review.observe("literal", lambda: {"first": "accepted"}), {"first": "accepted"})
        with self.assertRaises(OSError):
            review.observe("new", Mock(side_effect=OSError("provider interrupted")))
        again = self.open()
        self.assertEqual(again.observe("literal", self.forbidden), {"first": "accepted"})
        self.assertEqual(again.observe("new", lambda: {"later": "new"}), {"later": "new"})
        decision = again.decide(lambda: {"owner": "new"})
        self.assertEqual(self.open().decide(self.forbidden), decision)

    def test_terminal_negative_reuse_and_actual_generation_invalidation(self):
        from contextlib import nullcontext

        from libs.alpaca_lineage_build import duplicate_repair as repair

        expected = {"status": "unresolved", "complete": True, "failed": 0}
        self.open().finish(expected)
        entries = {"ACME": {"group_members": ["ACME"], "source_sha256": "p", "sha256": "a"}}
        with (
            patch.object(repair.storage, "writer_lock", return_value=nullcontext(self.root)),
            patch.object(repair.storage, "pricing_locks", return_value=nullcontext()),
            patch.object(repair.storage, "read_index", return_value={"symbols": entries}),
            patch.object(repair.storage, "read_receipt", self.forbidden),
            patch.object(repair, "maintain", self.forbidden),
        ):
            self.assertEqual(
                repair.run_selected(
                    self.root, self.group, {}, occupation_id=self.pid, client_factory=self.forbidden
                ),
                expected,
            )
            entries["ACME"]["source_sha256"] = "next"
            with self.assertRaises(RecoveryRequired):
                repair.run_selected(
                    self.root, self.group, {}, occupation_id=self.pid, client_factory=self.forbidden
                )
        self.group["references"]["pricing"]["ACME"] = "next"
        self.assertNotIn("outcome", self.open().state)

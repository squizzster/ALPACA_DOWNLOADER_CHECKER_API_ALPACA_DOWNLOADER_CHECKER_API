import unittest
from contextlib import nullcontext
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from libs.alpaca_lineage_build import duplicate_repair as repair
from libs.alpaca_lineage_build.contract import RecoveryRequired
from libs.alpaca_lineage_build.duplicate_history import AcceptedHistory, select_group
from libs.alpaca_serving_catalogue.duplicate_slices import prefix


@dataclass(frozen=True)
class Bar:
    close: Decimal
    volume: int
    trades: int


D = tuple(date(2024, 6, i) for i in (10, 11, 12, 13, 14))
RAW = {d: Bar(Decimal(i), 10 + i, 6 + i) for i, d in enumerate(D, start=5)}


def history(key, ids, labels):
    data = {d: RAW[d] for d in labels}
    return AcceptedHistory(key, frozenset(ids), labels, data)


def evidence(symbol, new, old_id, new_id):
    facts = [("CUSIPOLD", old_id, symbol), ("CUSIPNEW", new_id, new)]
    return {
        "corporate_actions": [
            {
                "action_type": "reverse_split",
                "id": "act",
                "symbol": symbol,
                "new_symbol": new,
                "old_cusip": "CUSIPOLD",
                "new_cusip": "CUSIPNEW",
                "old_rate": 4,
                "new_rate": 1,
                "ex_date": D[3].isoformat(),
            }
        ],
        "asset_lookups": [
            {
                "lookup": cusip,
                "lookup_kind": "cusip",
                "found": True,
                "outcome": "found",
                "http_status": 200,
                "asset": {"id": i, "symbol": sym, "class": "us_equity"},
            }
            for cusip, i, sym in facts
        ],
    }


class Tests(unittest.TestCase):
    def nested(self):
        short = history("o", ["1", "2"], {d: ("START",) for d in D[:3]})
        long = history("n", ["1", "2", "3"], {d: ("START",) if d < D[3] else ("NEXT",) for d in D})
        return short, long, evidence("NEXT", "NEXT", "2", "3")

    def disjoint(self):
        short = history("o", ["1", "2"], {d: ("START",) if d < D[1] else ("LATER",) for d in D[:3]})
        long = history("n", ["3"], {d: ("REUSED",) for d in D})
        return short, long, evidence("LATER", "REUSED", "2", "3")

    def test_later_same_security_component_preserves_complete_selected_source(self):
        a, b, e = self.nested()
        (v,) = select_group([a, b], [e])
        self.assertEqual([(s.source_id, s.first, s.last) for s in v.selection], [("n", D[0], D[-1])])

    def test_cross_symbol_split_produces_dated_composite_with_reused_label(self):
        a, b, e = self.disjoint()
        (v,) = select_group([a, b], [e])
        self.assertEqual(
            [(s.source_id, s.first, s.last) for s in v.selection], [("o", D[0], D[2]), ("n", D[3], D[4])]
        )

    def test_no_price_only_link(self):
        a, b, _ = self.disjoint()
        self.assertFalse(select_group([a, b], []))

    def test_sibling_uuid(self):
        a, b, e = self.nested()
        a = replace(a, members=frozenset(("1", "foreign")))
        self.assertFalse(select_group([a, b], [e]))

    def test_partial_dates_and_changed_raw_leave_prior_selections(self):
        a, b, e = self.disjoint()
        self.assertFalse(select_group([replace(a, raw={D[1]: RAW[D[1]]}), b], [e]))
        bad = replace(RAW[D[1]], trades=200)
        self.assertFalse(select_group([replace(a, raw={**a.raw, D[1]: bad}), b], [e]))

    def test_cancelled_and_unknown_cusips_are_insufficient(self):
        a, b, e = self.disjoint()
        e["asset_lookups"][0]["found"] = False
        self.assertFalse(select_group([a, b], [e]))

    def test_ambiguous_group(self):
        a, b, e = self.nested()
        self.assertFalse(select_group([a, b, replace(b, public_id="competing")], [e]))

    def test_other_occupant_trades_do_not_assign_by_label(self):
        a, b, e = self.disjoint()
        x = replace(b, raw={**b.raw, D[0]: Bar(Decimal("0.33"), 1100, 11)})
        self.assertFalse(select_group([a, x], [e]))

    def test_authored_assertions_cannot_supply_an_ownership_route(self):
        a, b, facts = self.disjoint()
        facts["assertions"] = ["invented"]
        with self.assertRaisesRegex(ValueError, "research fact injection"):
            select_group([a, b], [facts])

    def test_common_and_warrant_with_zero_trade_carries(self):
        a, b, e = self.disjoint()
        tape = {day: replace(bar, close=Decimal("0.01"), volume=0, trades=0) for day, bar in RAW.items()}
        a = replace(a, raw={day: tape[day] for day in a.raw})
        b = replace(b, raw=tape)
        self.assertFalse(select_group([a, b], [e]))

    def test_two_share_classes_with_distinct_volume(self):
        a, b, e = self.nested()
        b = replace(b, raw={**b.raw, D[2]: replace(b.raw[D[2]], volume=31000)})
        self.assertFalse(select_group([a, b], [e]))

    def test_reused_spelling_without_an_identity_preserving_action(self):
        a, b, e = self.disjoint()
        e["corporate_actions"][0]["action_type"] = "spin_off"
        self.assertFalse(select_group([a, b], [e]))

    def test_another_resolved_split_exit_blocks_both_claimants(self):
        a, b, e = self.disjoint()
        f = evidence("LATER", "DISTINCT", "2", "outside")
        f["corporate_actions"][0]["id"] = "second-event"
        self.assertFalse(select_group([a, b], [e, f]))

    def test_outside_same_ticker_class_not_admissible_by_spelling(self):
        a, b, e = self.disjoint()
        e["asset_lookups"][1]["asset"]["id"] = "another-public-issuer"
        self.assertFalse(select_group([a, b], [e]))

    def test_cancelled_action_and_conflicting_event_revisions_do_not_assign(self):
        short, long, facts = self.disjoint()
        event = facts["corporate_actions"][0]
        for variant in ({"status": "cancelled"}, {"old_rate": 0}, {"new_rate": "NaN"}):
            changed = {**facts, "corporate_actions": [{**event, **variant}]}
            self.assertFalse(select_group([short, long], [changed]))
            self.assertFalse(select_group([short, long], [facts, changed]))


class OperationTests(unittest.TestCase):
    def setUp(self):
        context = self.enterContext(TemporaryDirectory())
        self.root = Path(context)
        self.refs = {"lineage": {"A": "accepted"}, "pricing": {"A": "prices"}}
        self.group = {
            "references": self.refs,
            "locator": "A",
            "securities": [{"public_id": "p"}, {"public_id": "q"}],
            "decision_generation": "selection",
        }
        self.entries = {"A": {"group_members": ["A"], "sha256": "accepted", "source_sha256": "prices"}}
        self.dirty = {}
        self.enterContext(patch.object(repair, "exclusive_owned", return_value=True))
        self.enterContext(patch.object(repair, "_select_group", return_value=(self.group, self.dirty)))
        self.enterContext(patch.object(repair.storage, "writer_lock", return_value=nullcontext(self.root)))
        self.enterContext(patch.object(repair.storage, "pricing_locks", return_value=nullcontext()))
        self.enterContext(patch.object(repair.storage, "read_index", return_value={"symbols": self.entries}))
        self.enterContext(
            patch.object(
                repair.storage, "read_receipt", side_effect=AssertionError("reopened a completed receipt")
            )
        )
        self.enterContext(
            patch.object(repair, "_read_views", side_effect=AssertionError("reopened accepted history"))
        )
        self.enterContext(
            patch.object(repair, "_decision", side_effect=AssertionError("repeated ownership comparison"))
        )
        self.maintain = self.enterContext(
            patch.object(repair, "maintain", return_value={"complete": True, "failed": 0})
        )

    def test_exclusion_required_before_selecting_data(self):
        with patch.object(repair, "exclusive_owned", return_value=False):
            with self.assertRaises(RecoveryRequired):
                repair.reconcile(self.root, "A")

    def test_valid_retries_do_not_open_receipts_or_payloads(self):
        self.entries["A"][repair.MARKER] = "selection"
        self.assertEqual(repair.reconcile(self.root, "A")["status"], "already_published")
        self.maintain.assert_not_called()

    def test_changed_pricing_invalidates_without_new_reads(self):
        self.entries["A"]["source_sha256"] = "other"
        with self.assertRaisesRegex(RecoveryRequired, "inputs changed"):
            repair.reconcile(self.root, "A")

    def test_interruption_after_lineage_before_serving_borrows_accepted_output(self):
        self.entries["A"].update({repair.MARKER: "selection", "sha256": "new"})
        self.dirty["A"] = "new"
        self.assertEqual(repair.reconcile(self.root, "A")["status"], "recovered")
        self.maintain.assert_called_once()

    def test_incomplete_unrelated_transaction_is_left_to_owner(self):
        (self.root / ".pending.json").write_text("{}")
        with patch.object(repair.storage, "recover_journal", side_effect=AssertionError("foreign journal")):
            with self.assertRaisesRegex(RecoveryRequired, "different lineage"):
                repair.reconcile(self.root, "A")

    def test_selected_interrupted_journal_installs_without_comparison(self):
        (self.root / ".pending.json").write_text(
            '{"selected_owner_repair":"selection","export_input_changes":{}}'
        )
        with patch.object(repair.storage, "recover_journal") as recover:
            self.assertEqual(repair.reconcile(self.root, "A")["status"], "recovered")
            recover.assert_called_once()
        self.maintain.assert_called_once()

    def test_serving_rejects_an_expired_payload_generation_without_opening_anything(self):
        from unittest.mock import Mock

        group = Mock()
        group.duplicate_source_slices.return_value = {"source_pricing": {"A": "old"}}
        group.references = {"pricing": {"A": "new"}}
        self.assertEqual(prefix(group, [], None, None), {})


if __name__ == "__main__":
    unittest.main()

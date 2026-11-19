"""Provider-only acceptance for a bounded correction of an already owned series."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

from libs.alpaca_pricing_download import dated_publication
from libs.alpaca_pricing_download.contract import ADJUSTMENTS
from libs.alpaca_pricing_download.dated_rename import UnsupportedInterval, acquire, provider_rows

DAYS = [(date(2031, 3, 3) + timedelta(days=i)).isoformat() for i in range(5)] + ["2031-03-10", "2031-03-11"]
CUT = DAYS[3]


def bar(day, price):
    return {
        "timestamp": day + "T05:00:00Z",
        "session": day,
        "tickers": ["PRE" if day < CUT else "POST"],
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": "10",
        "trade_count": "2",
        "vwap": price,
    }


class AcquisitionTests(unittest.TestCase):
    def setUp(self):
        self.ranges = [
            dict(ticker="PRE", first_session=DAYS[0], last_session=DAYS[2]),
            dict(ticker="POST", first_session=CUT, last_session=DAYS[-1]),
        ]
        self.held = {}
        self.mapped = {}
        for view in ADJUSTMENTS:
            scale = "100" if "split" in view or view == "all" else "1"
            self.held[view] = {"bars": [bar(d, scale if d >= CUT else "1") for d in DAYS]}
            self.mapped[view] = {d: bar(d, scale) for d in DAYS}
        self.original = deepcopy(self.mapped["raw"])
        self.calls = []

    def observe(self, symbol, asof, view, first, last):
        self.calls.append((symbol, asof, view, first, last))
        if symbol == "PRE":
            self.assertEqual((asof, view), (DAYS[2], "raw"))
            rows = self.original
        else:
            self.assertEqual((symbol, asof), ("POST", CUT))
            rows = self.mapped[view]
        return {d: b for d, b in rows.items() if first <= d <= last}

    def run_trial(self):
        return acquire(self.held, self.ranges, CUT, self.observe)

    def test_same_label_prefix_requires_every_raw_field_and_stable_dated_ownership(self):
        for symbol in ("UNSEEN", "NEW.U"):
            with self.subTest(symbol=symbol):
                held = deepcopy(self.held)
                for view in held.values():
                    for item in view["bars"]:
                        item["tickers"] = [symbol]
                ranges = [dict(ticker=symbol, first_session=DAYS[0], last_session=DAYS[-1])]
                mapped = deepcopy(self.mapped)

                def observe(ticker, asof, view, first, last):
                    self.assertEqual((ticker, asof), (symbol, CUT))
                    return mapped[view]

                bundles, outcome = acquire(held, ranges, CUT, observe, same_label=True)
                self.assertEqual(outcome["repair_kind"], "same_label_basis")
                self.assertEqual(outcome["raw_revisions"], [])
                self.assertEqual(bundles["split"][DAYS[0]]["close"], "100")
                self.assertEqual(bundles["raw"][DAYS[0]]["tickers"], [symbol])
                opening = held["raw"]["bars"][3]
                opening["open"] = opening["high"] = "100"
                with self.assertRaisesRegex(UnsupportedInterval, "traded opening"):
                    acquire(
                        held, ranges, CUT, Mock(side_effect=AssertionError("provider accessed")), same_label=True
                    )
                opening["open"] = opening["high"] = "1"
                mapped["raw"][DAYS[1]]["trade_count"] = "4"
                with self.assertRaisesRegex(UnsupportedInterval, "mapped raw"):
                    acquire(held, ranges, CUT, observe, same_label=True)
                held["raw"]["bars"][0]["tickers"] = ["COMPETING"]
                with self.assertRaisesRegex(UnsupportedInterval, "one dated ticker"):
                    acquire(
                        held, ranges, CUT, Mock(side_effect=AssertionError("provider accessed")), same_label=True
                    )

    def staged_selection(self):
        store = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (store / "end_of_day" / "pricing").mkdir(parents=True)
        binding = {"cut": CUT, "public_id": "1" * 64, "decision_generation": "first"}
        inputs = Mock(return_value={"views": self.held, "ranges": self.ranges})
        return store, binding, inputs

    def test_sql_resume_reads_only_inputs_required_by_remaining_proof(self):
        store, binding, inputs = self.staged_selection()

        def interrupt(client, *scope):
            if scope[2] == "dividend":
                raise OSError("interrupt after raw, all, split and basis")
            return self.observe(*scope)

        with (
            patch.object(dated_publication, "provider_rows", side_effect=interrupt),
            self.assertRaisesRegex(OSError, "interrupt"),
        ):
            dated_publication.acquire_selected(store, binding, inputs, None)
        with closing(sqlite3.connect(dated_publication.database(store))) as db, db:
            db.execute(
                "UPDATE dated_price_stage SET payload=x'00' WHERE step IN "
                "('input:raw','input:all','input:split','input:ranges')"
            )
        with (
            patch.object(dated_publication, "provider_rows", side_effect=lambda _, *scope: self.observe(*scope)),
            patch("uuid.uuid4", side_effect=OSError("interrupt after completed proof")),
            self.assertRaisesRegex(OSError, "completed proof"),
        ):
            dated_publication.acquire_selected(store, binding, inputs, None)
        self.assertCountEqual([c[2] for c in self.calls], ADJUSTMENTS)
        with closing(sqlite3.connect(dated_publication.database(store))) as db, db:
            db.execute(
                "UPDATE dated_price_stage SET payload=x'00' WHERE step LIKE 'input:%' OR step IN "
                "('proof:scope','proof:basis')"
            )
        with patch.object(dated_publication, "provider_rows", side_effect=AssertionError("repeated download")):
            result = dated_publication.acquire_selected(store, binding, inputs, None)
            self.assertEqual(result["sessions"], 3)
            self.assertEqual(result, dated_publication.acquire_selected(store, binding, inputs, None))
            with self.assertRaisesRegex(AssertionError, "changed binding"):
                dated_publication.acquire_selected(
                    store,
                    {**binding, "decision_generation": "second"},
                    Mock(side_effect=AssertionError("changed binding")),
                    None,
                )
        inputs.assert_called_once()

    def test_sql_decline_reloads_only_the_reason(self):
        store, binding, inputs = self.staged_selection()
        self.mapped["raw"].pop(DAYS[1])
        with (
            patch.object(dated_publication, "provider_rows", side_effect=lambda _, *scope: self.observe(*scope)),
            self.assertRaisesRegex(UnsupportedInterval, "lacks a published"),
        ):
            dated_publication.acquire_selected(store, binding, inputs, None)
        with closing(sqlite3.connect(dated_publication.database(store))) as db, db:
            db.execute("UPDATE dated_price_stage SET payload=x'00' WHERE step!='proof:declined'")
        with (
            patch.object(dated_publication, "provider_rows", side_effect=AssertionError("repeated download")),
            self.assertRaisesRegex(UnsupportedInterval, "lacks a published"),
        ):
            dated_publication.acquire_selected(store, binding, inputs, None)
        inputs.assert_called_once()

    def test_all_views_use_one_bounded_request_each_and_keep_dated_labels(self):
        # Provider text representation and timestamps can differ with identical values/instants.
        self.mapped["raw"][DAYS[0]]["volume"] = "10.00"
        self.mapped["raw"][CUT]["timestamp"] = CUT + "T00:00:00-05:00"
        bundles, interval = self.run_trial()
        self.assertEqual(len(self.calls), len(ADJUSTMENTS))
        self.assertEqual({c[2] for c in self.calls}, set(ADJUSTMENTS))
        self.assertTrue(all(c[3:] == (DAYS[0], DAYS[5]) for c in self.calls))
        self.assertEqual(interval["raw_revisions"], [])
        self.assertEqual(interval["witnesses"], DAYS[3:6])
        self.assertEqual(list(bundles["all"]), DAYS[:3])
        self.assertEqual(bundles["all"][DAYS[0]]["tickers"], ["PRE"])

    def test_each_view_inherits_its_accepted_labels(self):
        self.held["spin-off"]["bars"][0]["tickers"] = ["PRE", "OTHER"]
        bundles, _ = self.run_trial()
        self.assertEqual(bundles["spin-off"][DAYS[0]]["tickers"], ["PRE", "OTHER"])
        self.assertEqual(bundles["raw"][DAYS[0]]["tickers"], ["PRE"])

    def test_no_basis_jump_prevents_all_provider_requests(self):
        for view in ("all", "split"):
            self.held[view]["bars"][2].update(close="100")
        with self.assertRaisesRegex(UnsupportedInterval, "no comparable"):
            self.run_trial()
        self.assertFalse(self.calls)

    def test_ambiguous_predecessor_prevents_all_provider_requests(self):
        self.held["raw"]["bars"][2]["tickers"] = ["PRE", "OTHER"]
        with self.assertRaisesRegex(UnsupportedInterval, "ambiguous"):
            self.run_trial()
        self.assertFalse(self.calls)

    def test_missing_session_and_changed_successor_stop_at_first_view(self):
        for failure in ("missing", "wrong_timestamp", "wrong_volume"):
            with self.subTest(failure=failure):
                self.setUp()
                if failure == "missing":
                    self.mapped["raw"].pop(DAYS[1])
                elif failure == "wrong_timestamp":
                    self.mapped["raw"][CUT]["timestamp"] = CUT + "T06:00:00Z"
                else:
                    self.mapped["raw"][DAYS[5]]["volume"] = "1"
                with self.assertRaises(UnsupportedInterval):
                    self.run_trial()
                self.assertEqual([c[2] for c in self.calls], ["raw"])

    def test_no_new_earlier_split_basis_jump(self):
        for view in ("split", "all"):
            self.mapped[view][DAYS[0]] = bar(DAYS[0], "1")
        with self.assertRaisesRegex(UnsupportedInterval, "introduce"):
            self.run_trial()
        self.assertEqual([c[2] for c in self.calls], ["raw", "all", "split"])

    def test_existing_earlier_split_step_can_be_retained(self):
        for view in ("split", "all"):
            self.held[view]["bars"][0] = bar(DAYS[0], "0.01")
            self.mapped[view][DAYS[0]] = bar(DAYS[0], "1")
        _, interval = self.run_trial()
        self.assertEqual(interval["sessions"], 3)

    def test_join_rejects_residual_scale_before_other_views(self):
        self.mapped["split"][DAYS[2]] = bar(DAYS[2], "90")
        with self.assertRaisesRegex(UnsupportedInterval, "incompatible"):
            self.run_trial()
        self.assertEqual(len(self.calls), 3)

    def test_original_route_checks_only_changed_sessions(self):
        for dates, windows in [
            ([DAYS[0], DAYS[1]], [(DAYS[0], DAYS[1])]),
            ([DAYS[0], DAYS[2]], [(DAYS[0], DAYS[0]), (DAYS[2], DAYS[2])]),
        ]:
            with self.subTest(dates=dates):
                self.setUp()
                for d in dates:
                    self.original[d]["volume"] = self.mapped["raw"][d]["volume"] = "99"
                _, interval = self.run_trial()
                self.assertEqual([r["session"] for r in interval["raw_revisions"]], dates)
                self.assertEqual([c[3:] for c in self.calls if c[0] == "PRE"], windows)

    def test_disputed_raw_fails_before_dependent_views(self):
        self.mapped["raw"][DAYS[1]]["volume"] = "99"
        with self.assertRaisesRegex(UnsupportedInterval, "uncorroborated"):
            self.run_trial()
        self.assertEqual(len(self.calls), 4)

    def test_eligibility_of_new_views(self):
        self.mapped["dividend"][DAYS[1]]["low"] = "-3"
        with self.assertRaisesRegex(UnsupportedInterval, "ineligible"):
            self.run_trial()

    def test_rejects_ambiguous_provider_day(self):
        from libs.alpaca_pricing_download.dated_rename import _window
        from libs.alpaca_pricing_download.responses import CompletedBars

        client = Mock()
        bar_row = (DAYS[0] + "T05:00:00Z", *("1",) * 7)
        client.fetch.return_value = CompletedBars(
            "POST", "raw", _window(DAYS[0]), _window(DAYS[1]), CUT, (bar_row, bar_row)
        )
        with self.assertRaisesRegex(UnsupportedInterval, "ambiguous daily"):
            provider_rows(client, "POST", CUT, "raw", DAYS[0], DAYS[0])

    def test_interrupted_producer_keeps_accepted_view_and_range_decisions(self):
        state = {}
        self.original[DAYS[0]]["volume"] = self.mapped["raw"][DAYS[0]]["volume"] = "99"
        self.original[DAYS[2]]["volume"] = self.mapped["raw"][DAYS[2]]["volume"] = "77"

        def interrupt(key, value):
            state[key] = json.loads(json.dumps(value))
            if key == "confirmation:0":
                raise OSError("interrupted after journal commit")

        with self.assertRaisesRegex(OSError, "interrupted"):
            acquire(self.held, self.ranges, CUT, self.observe, completed={}, checkpoint=interrupt)
        self.assertCountEqual(state, ["scope", "view:all", "view:raw", "view:split", "basis", "confirmation:0"])
        first_calls = set(self.calls)
        self.calls.clear()
        with (
            patch("libs.alpaca_pricing_download.dated_rename._scope", side_effect=AssertionError("replanned")),
            patch(
                "libs.alpaca_pricing_download.dated_rename._check_basis",
                side_effect=AssertionError("recomputed basis"),
            ),
        ):
            held = {k: v for k, v in self.held.items() if k not in {"raw", "split", "all"}}
            result = acquire(held, None, CUT, self.observe, completed=state)
        self.assertTrue(first_calls.isdisjoint(self.calls))
        self.assertEqual(
            result[1]["raw_revisions"],
            [{"session": day, "ticker": "PRE", "asof": DAYS[2]} for day in (DAYS[0], DAYS[2])],
        )
        held = Mock()
        self.assertEqual(acquire(held, None, CUT, Mock(side_effect=AssertionError), completed=state), result)
        held.assert_not_called()

    def test_completed_negative_survives_restart_without_any_observation(self):
        state = {}
        self.mapped["split"].pop(DAYS[0])
        with self.assertRaisesRegex(UnsupportedInterval, "split lacks"):
            acquire(self.held, self.ranges, CUT, self.observe, completed=state)
        self.assertIn("declined", state)
        with self.assertRaisesRegex(UnsupportedInterval, "split lacks"):
            acquire(None, None, CUT, Mock(side_effect=AssertionError), completed=json.loads(json.dumps(state)))

    def test_transport_failure_can_retry_only_the_unfinished_request(self):
        state = {}

        def fetch(*args):
            if args[2] == "all":
                raise OSError("timeout")
            return self.observe(*args)

        with self.assertRaisesRegex(OSError, "timeout"):
            acquire(self.held, self.ranges, CUT, fetch, completed=state)
        self.assertNotIn("declined", state)
        acquire(self.held, self.ranges, CUT, self.observe, completed=state)
        self.assertEqual(sum(1 for c in self.calls if c[2] == "raw"), 1)

    def test_only_zero_volume_successor_cannot_select(self):
        for bar_row in self.held["raw"]["bars"][3:]:
            bar_row["volume"] = str(Decimal(0))
        with self.assertRaisesRegex(UnsupportedInterval, "insufficient traded"):
            self.run_trial()
        self.assertFalse(self.calls)

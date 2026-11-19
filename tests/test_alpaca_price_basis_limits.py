"""Native observations, durable checkpoints and dated serving coverage across consumers."""

import csv
import io
import tempfile
from copy import deepcopy
from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from test_alpaca_end_of_day_api import NativeCase

from alpaca_end_of_day import DataLibrary, StandaloneReader, standalone
from libs.alpaca_lineage_build import price_basis_limits as limits
from libs.alpaca_lineage_build.contract import LineageError, RecoveryRequired
from libs.alpaca_pricing_download import price_basis_review as pricing
from libs.alpaca_pricing_download.contract import DownloadError
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import operation_lock

DAYS = ("2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08")
CUT = DAYS[2]
PID = "a" * 64


def evidence():
    def bar(day, close):
        return dict(
            session=day,
            tickers=["FUTURE"],
            open=close,
            close=close,
            high=close,
            low=close,
            trade_count="50",
            volume="400",
        )

    views = {
        mode: {
            "bars": [
                bar(day, "40" if ("split" in mode or mode == "all") and day < CUT else "10")
                for day in (DAYS if mode == "raw" else DAYS[1:3])
            ]
        }
        for mode in pricing.MODES
    }
    return {
        "dates": DAYS,
        "views": views,
        "actions": [
            {"action_type": "reverse_split", "ex_date": CUT, "symbol": "FUTURE", "old_rate": 4, "new_rate": 1}
        ],
    }


class PricingReviewTests(TestCase):
    def test_competing_price_evidence_and_native_alternatives(self):
        data = evidence()
        p = pricing.plan(data, CUT, False)
        self.assertEqual(p["outcome"], "check_regular_trades")
        traded = dict(price="10", minutes=3, trades=9)
        self.assertEqual(pricing.finish(p, traded, traded, False)["reason"], limits.RESTRICT)
        self.assertEqual(pricing.finish(p, {**traded, "minutes": 1}, traded, False)["outcome"], "unsupported")
        self.assertEqual(pricing.finish(p, traded, {**traded, "price": "13"}, False)["outcome"], "unsupported")

        moved = deepcopy(data)
        moved["views"]["raw"]["bars"][3]["high"] = "41"
        self.assertIn("traded price range", pricing.plan(moved, CUT, False)["reason"])
        other = deepcopy(data)
        other["views"]["spin-off"]["bars"][0]["close"] = "8"
        self.assertIn("independent", pricing.plan(other, CUT, False)["reason"])
        data["views"]["raw"]["bars"][2]["open"] = "40"
        self.assertEqual(pricing.plan(data, CUT, False)["outcome"], "unchanged")
        p = pricing.plan(data, CUT, True)
        self.assertEqual(pricing.finish(p, traded, {**traded, "price": "40"}, True)["outcome"], "restored")
        data["views"]["raw"]["bars"][2]["open"] = "10"
        data["views"]["raw"]["bars"][3]["open"] = "40"
        self.assertIn("nearby", pricing.plan(data, CUT, False)["reason"])
        data["views"]["raw"]["bars"][2]["trade_count"] = "0"
        self.assertIn("carried", pricing.plan(data, CUT, False)["reason"])

    def test_literal_regular_session_and_early_close(self):
        client = Mock()
        day = "2025-12-24"

        def bar(stamp):
            return dict(t=stamp, o=2, h=2, l=2, c=2, v=15, n=4)

        client._get.return_value = {"bars": {"FUTURE": [bar("2025-12-24T17:59:00Z")]}}
        self.assertEqual(
            pricing.regular_minutes(client, "FUTURE", day, "close"), {"price": "2", "minutes": 1, "trades": 4}
        )
        request = client._get.call_args.args[0]
        self.assertEqual(request["asof"], "-")
        self.assertEqual(request["start"], "2025-12-24T12:50:00-05:00")
        client._get.return_value = {"bars": {"FUTURE": [bar("2025-12-24T18:01:00Z")]}}
        with self.assertRaises(DownloadError):
            pricing.regular_minutes(client, "FUTURE", day, "close")
        client._get.reset_mock()
        with self.assertRaises(DownloadError):
            pricing.regular_minutes(client, "FUTURE,OTHER", day, "close")
        client._get.assert_not_called()

    def test_thin_session_continues_only_the_unobserved_regular_window(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp)
            (store / "end_of_day/pricing").mkdir(parents=True)
            binding = dict(
                cut=CUT, public_id=PID, numerical_token=None, source_decision="selected", recheck=False
            )
            enough = dict(price="10", minutes=5, trades=20)
            quote = dict(price="10", minutes=1, trades=2)
            with patch.object(
                pricing, "regular_minutes", side_effect=[enough, quote, RuntimeError("before remainder")]
            ) as observe:
                with self.assertRaisesRegex(RuntimeError, "before remainder"):
                    pricing.review(store, binding, evidence, None)
                self.assertEqual([c.args[-1] for c in observe.call_args_list], ["close", "open", "open"])
            # The later minute corroborates activity. The previously accepted
            # opening still supplies the actual endpoint price.
            with patch.object(pricing, "regular_minutes", return_value={**enough, "price": "11"}) as observe:
                outcome = pricing.review(
                    store, binding, Mock(side_effect=AssertionError("reopened source")), None
                )
                self.assertEqual(outcome["outcome"], "restricted")
                self.assertEqual(outcome["regular_open"], dict(price="10", minutes=6, trades=22))
                observe.assert_called_once_with(None, "FUTURE", CUT, "open", remainder=True)

    def test_interrupted_and_completed_reviews_consume_their_stage_once(self):
        with tempfile.TemporaryDirectory() as temp:
            store = Path(temp)
            (store / "end_of_day/pricing").mkdir(parents=True)
            binding = dict(
                cut=CUT, public_id=PID, numerical_token=None, source_decision="selected", recheck=False
            )
            load = Mock(return_value=evidence())
            print_ = dict(price="10", trades=25, minutes=10)
            with patch.object(
                pricing, "regular_minutes", side_effect=[print_, RuntimeError("interrupted")]
            ) as get:
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    pricing.review(store, binding, load, None)
                self.assertEqual([c.args[-1] for c in get.call_args_list], ["close", "open"])
            with patch.object(pricing, "regular_minutes", return_value=print_) as get:
                outcome = pricing.review(store, binding, load, None)
                self.assertEqual(outcome["outcome"], "restricted")
                self.assertEqual([c.args[-1] for c in get.call_args_list], ["open"])
            with patch.object(pricing, "finish", side_effect=AssertionError("repeated decision")):
                self.assertEqual(pricing.review(store, binding, load, None, completed_only=True), outcome)
            load.assert_called_once()


class LimitsTests(NativeCase):
    def setUp(self):
        super().setUp()
        self.data = self.bootstrap()
        self.pid = self.data.list_securities()[0].public_id

    def select(self, reason, *, token):
        group = self.catalogue_groups("TEST")["TEST"]
        item = dict(
            public_id=self.pid,
            source_decision=group["decision_generation"],
            symbol="TEST",
            previous_session=DAYS[1],
            witness_start=DAYS[0],
            witness_end=DAYS[-1],
            cut=CUT,
            numerical_token=None,
            reason=reason,
            token=token,
        )
        with operation_lock(self.store, exclusive=True):
            limits.publish(self.store, group, item)
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("replayed numerical approval")):
            result = maintain(self.store)
        self.assertTrue(result["complete"], result)
        self.assertEqual(result["checked_bars"], 0)
        return DataLibrary(store=self.store)

    def test_positive_limited_range_serves_every_export_and_restores_full_range(self):
        full = self.data.get_histories(self.pid)
        data = self.select(limits.RESTRICT, token="b" * 32)
        self.assertEqual(data.get_security(self.pid).first_session, date.fromisoformat(CUT))
        self.assertFalse(data.resolve_ticker("TEST", on=DAYS[0]))
        self.assertTrue(data.resolve_ticker("TEST", on=CUT))
        direct = StandaloneReader(self.store).get_histories("TEST", public_id=self.pid)["views"]
        self.assertEqual(len(direct["all"]["bars"]), len(self.bars))
        self.assertEqual(len(data.get_history(self.pid, "raw", end=DAYS[1]).bars), 0)
        for mode, history in data.get_histories(self.pid).items():
            self.assertEqual(history.bars, tuple(b for b in full[mode].bars if b.session.isoformat() >= CUT))
        with patch.object(standalone, "_history", wraps=standalone._history) as read:
            with data.open_history_export(public_ids=[self.pid]) as run:
                item = next(run.iter_histories())
            self.assertEqual(item.views["raw"]["bars"][0]["session"], CUT)
            self.assertTrue(read.called)
            self.assertTrue(all(c.kwargs.get("minimum_session") == CUT for c in read.call_args_list))
        rows = io.StringIO()
        data.write_flat_online_csv(rows)
        self.assertEqual(
            [r["date"] for r in csv.DictReader(io.StringIO(rows.getvalue()))],
            [b.session.isoformat() for b in data.get_history(self.pid, "raw").bars],
        )
        result = data.write_csv_export(self.directory / "csv", public_ids=[self.pid], workers=1)
        self.assertTrue(result)
        restored = self.select(limits.RESTORED, token="c" * 32)
        self.assertEqual(restored.get_history(self.pid, "raw").bars, full["raw"].bars)
        self.assertTrue(restored.resolve_ticker("TEST", on=DAYS[0]))

    def test_retention_rejects_replacement_and_marks_only_material_changes(self):
        self.select(limits.RESTRICT, token="d" * 32)
        from libs.alpaca_lineage_build import storage

        receipt = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))
        key, item = next(iter(receipt[limits.FIELD].items()))
        fresh = deepcopy(receipt)
        fresh["source_sha256"] = "changed"
        limits.continue_closed(receipt, fresh, "2026-10-09")
        limits.retain(receipt, fresh, continued=True)
        self.assertEqual(fresh[limits.FIELD][key], item)
        limits.continue_closed(receipt, fresh, DAYS[0])
        limits.retain(receipt, fresh, continued=True)
        self.assertTrue(fresh[limits.FIELD][key]["pending"])
        changed = deepcopy(receipt)
        changed["evidence"]["provider_corporate_actions"] = [
            {"id": "changed", "symbol": "TEST", "ex_date": CUT, "action_type": "reverse_split"}
        ]
        limits.retain(receipt, changed)
        self.assertTrue(changed[limits.FIELD][key]["pending"])
        fresh[limits.FIELD][key] = {**item, "symbol": "INJECTED"}
        with self.assertRaises(LineageError):
            limits.retain(receipt, fresh, continued=True)
        numerical = {self.pid: {"token": "e" * 32, "first": DAYS[0], "cut": CUT}}
        self.assertTrue(limits.group_limits({"TEST": receipt}, set(), numerical)[self.pid][CUT]["pending"])
        self.assertEqual(limits.group_limits({"TEST": receipt}, {"UNKNOWN"}, {}), {})
        self.assertIsNone(
            limits.numerical_binding(
                {"token": "e" * 32, "first": "2026-10-20", "cut": "2026-10-21"}, DAYS[0], DAYS[-1]
            )
        )
        with self.assertRaises(RecoveryRequired):
            limits.accept(receipt, {limits.TOKENS: {}})

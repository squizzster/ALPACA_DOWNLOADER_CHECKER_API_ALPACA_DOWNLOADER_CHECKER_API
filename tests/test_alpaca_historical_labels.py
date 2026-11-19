"""Dated ticker occupation from a completed historical review, independent of price eligibility."""

from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

import test_alpaca_price_basis_limits as limits_fixture
from test_alpaca_end_of_day_api import NativeCase
from test_alpaca_price_basis_limits import CUT, DAYS

from alpaca_end_of_day import DataLibrary, StandaloneReader
from libs.alpaca_lineage_build import historical_labels as labels
from libs.alpaca_lineage_build import price_basis_limits
from libs.alpaca_lineage_build.dated_price_publication import publish_selection
from libs.alpaca_pricing_download import historical_labels as prices
from libs.alpaca_pricing_download.contract import COLUMNS
from libs.alpaca_serving_catalogue import policy
from libs.alpaca_serving_catalogue.producer import maintain
from libs.common.store_lock import operation_lock


class LiteralOccupationTests(TestCase):
    def setUp(self):
        self.basis = dict(
            symbol="OLD",
            original_first_session="2020-01-02",
            witness_start="2020-01-03",
            witness_end="2020-01-06",
        )
        self.dates = ["2020-01-02", "2020-01-07", "2020-01-08", "2020-01-09"]
        self.stage = {}

        def row(d):
            return dict(
                session=d, timestamp=d + "T05:00:00Z", tickers=["NEW"], **dict.fromkeys(COLUMNS[1:], "2")
            )

        self.owner = {d: row(d) for d in self.dates}
        self.tapes = {"OLD": {d: row(d) for d in self.dates[:2]}, "NEW": {d: row(d) for d in self.dates[2:]}}
        self.peer = []

    def assess(self):
        def acquire(client, symbol, asof, mode, first, last):
            self.assertTrue(last < self.basis["witness_start"] or first > self.basis["witness_end"])
            self.assertEqual((asof, mode), ("-", "raw"))
            return {d: b for d, b in self.tapes[symbol].items() if first <= d <= last}

        def load(first, last):
            self.assertTrue(last < self.basis["witness_start"] or first > self.basis["witness_end"])
            return [b for d, b in self.owner.items() if first <= d <= last]

        def save(k, v):
            self.stage[k] = v
            return v

        with patch.object(prices, "provider_rows", side_effect=acquire):
            return prices.assess(
                self.basis, "NEW", "2020-01-10", load, lambda: self.peer, None, self.stage.get, save
            )

    def test_projected_and_genuine_competing_trades(self):
        result = self.assess()
        self.assertEqual(result["outcome"], "projected")
        self.assertEqual(
            result["change"],
            dict(
                old="NEW",
                new="OLD",
                prior_session="2020-01-01",
                first="2020-01-02",
                last="2020-01-07",
                next_session="2020-01-08",
            ),
        )
        with patch.object(prices, "_matches", side_effect=AssertionError("recompared completed scope")):
            self.assertEqual(self.assess(), result)
        self.stage.clear()
        self.tapes["NEW"]["2020-01-07"] = self.owner["2020-01-07"]
        self.assertIn("unique handover", self.assess()["reason"])

    def test_stop_at_earlier_ambiguity_preserve_independent_recent_reuse(self):
        self.tapes["NEW"]["2020-01-02"] = self.owner["2020-01-02"]
        self.peer = [dict(first_session="2021-01-01", last_session="2022-01-01")]
        self.assertEqual(self.assess()["change"]["first"], "2020-01-03")
        self.peer = [dict(first_session="2019-01-01", last_session="2020-01-05")]
        self.assertIn("independent published", self.assess()["reason"])

    def test_interruption_reuses_completed_block(self):
        self.basis["witness_end"] = "2019-10-01"
        self.basis["witness_start"] = "2019-09-30"
        self.basis["original_first_session"] = "2019-09-30"
        self.owner["2019-10-02"] = {
            **self.owner["2020-01-02"],
            "session": "2019-10-02",
            "timestamp": "2019-10-02T04:00:00Z",
        }
        self.tapes["OLD"]["2019-10-02"] = self.owner["2019-10-02"]
        original = prices._matches
        seen = []

        def fail(left, right):
            if left["session"] == "2020-01-07":
                raise RuntimeError("later block")
            seen.append(left["session"])
            return original(left, right)

        with (
            patch.object(prices, "_matches", side_effect=fail),
            self.assertRaisesRegex(RuntimeError, "later block"),
        ):
            self.assess()
        self.assertIn("2019-10-02", seen)

        def guard(left, right):
            self.assertNotEqual(left["session"], "2019-10-02")
            return original(left, right)

        with patch.object(prices, "_matches", side_effect=guard):
            self.assertEqual(self.assess()["outcome"], "projected")


class PublicLabelTests(TestCase):
    def test_all_views_and_ambiguity_are_projected_on_only_selected_dates(self):
        rows = [dict(session=d, tickers=["NEW", "OTHER"], foo=1) for d in DAYS]
        result = labels.project_views(
            {"raw": {"bars": rows}, "all": {"bars": rows}},
            dict(first=DAYS[1], last=DAYS[2], old="NEW", new="OLD"),
        )
        marked = result["raw"]["bars"]
        self.assertEqual(marked[1]["tickers"], ["OLD", "OTHER"])
        self.assertEqual(marked[0], rows[0])
        self.assertIs(marked, result["all"]["bars"])
        self.assertEqual(rows[1]["tickers"], ["NEW", "OTHER"])


class ActualLabelTests(NativeCase):
    select = limits_fixture.LimitsTests.select

    def setUp(self):
        super().setUp()
        self.data = self.bootstrap()
        self.pid = self.data.list_securities()[0].public_id

    def test_lineage_label_publication_serves_all_views_without_replaying_price_decisions(self):
        data = self.select(price_basis_limits.RESTRICT, token="f" * 32)
        before = data.get_histories(self.pid)
        group = self.catalogue_groups("TEST")["TEST"]
        descriptor = dict(
            schema_version=1,
            public_id=self.pid,
            cut=CUT,
            basis_token="f" * 32,
            token="e" * 32,
            source_decision=group["decision_generation"],
            members=["TEST"],
            unavailable=[],
            witness_end=DAYS[-1],
            change=dict(
                old="TEST",
                new="FORMER",
                first=DAYS[0],
                last=DAYS[2],
                next_session=DAYS[3],
                prior_session="2026-09-30",
            ),
        )
        with operation_lock(self.store, exclusive=True):
            publish_selection(self.store, group, descriptor, self.pid, labels.FIELD, labels.TOKENS)
        with patch.object(policy, "_bar_reasons", side_effect=AssertionError("replayed numerical approval")):
            self.assertTrue(maintain(self.store)["complete"])
        data = DataLibrary(store=self.store)
        self.assertTrue(data.resolve_ticker("FORMER", on=CUT))
        self.assertFalse(data.resolve_ticker("TEST", on=CUT))
        self.assertTrue(data.resolve_ticker("TEST", on=DAYS[3]))
        self.assertIn("FORMER", StandaloneReader(self.store).resolve_ticker("TEST")["matches"][0]["tickers"])
        with data.open_history_export(public_ids=[self.pid]) as run:
            item = next(run.iter_histories())
            for view in item.views.values():
                self.assertEqual(view["bars"][0]["tickers"], ["FORMER"])
        for mode, hist in data.get_histories(self.pid).items():
            self.assertEqual(hist.bars, before[mode].bars)
        from libs.alpaca_lineage_build import storage

        receipt = storage.read_receipt(self.root, "TEST", storage.read_index(self.root))
        self.assertTrue(labels.selected({"TEST": receipt}, ()))
        self.assertFalse(labels.selected({"TEST": receipt}, ("OTHER",)))
        changed = deepcopy(receipt)
        changed["source_sha256"] = "changed"
        labels.continue_closed(receipt, changed, "2026-10-09")
        labels.retain(receipt, changed, continued=True)
        self.assertEqual(changed[labels.FIELD][self.pid], descriptor)
        labels.continue_closed(receipt, changed, CUT)
        labels.retain(receipt, changed, continued=True)
        self.assertNotIn(labels.FIELD, changed)
        changed = deepcopy(receipt)
        changed["dated_price_intervals"] = {self.pid: {"token": "c" * 32, "first": DAYS[0], "cut": CUT}}
        self.assertFalse(labels.selected({"TEST": changed}, ()))
        # Lifting the unsupported price prefix replaces its certificate. Online
        # resolution drops the dependent label, while the original identity stays.
        restored = self.select(price_basis_limits.RESTORED, token="1" * 32)
        self.assertTrue(restored.resolve_ticker("TEST", on=CUT))
        self.assertFalse(restored.resolve_ticker("FORMER", on=CUT))
